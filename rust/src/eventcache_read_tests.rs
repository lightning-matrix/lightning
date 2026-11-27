use super::*;
use crate::ectrace::{Counted, Snapshot, Stats};
use matrix_sdk::ruma::{api::client::sync::sync_events::v5, room_id, serde::Raw};
use matrix_sdk::test_utils::mocks::MatrixMockServer;
use matrix_sdk_base::RequestedRequiredStates;
use std::time::{Duration, Instant};

const ROOM: &str = "!reads:localhost";
const IDLE: Duration = Duration::from_secs(3);
const IDLE_READS_ALLOWED: u64 = 5;

fn message(id: &str, sender: &str, ts: u64) -> serde_json::Value {
    json!({"type": "m.room.message", "event_id": id, "sender": sender, "origin_server_ts": ts,
        "content": {"msgtype": "m.text", "body": id}})
}

fn history() -> Vec<serde_json::Value> {
    let alice = "@alice:localhost";
    let bob = "@bob:localhost";
    let mut ts = 1_000_000u64;
    let mut tick = || {
        ts += 1_000;
        ts
    };
    let mut events = Vec::new();
    for n in 0..40 {
        events.push(message(&format!("$m{n}"), alice, tick()));
    }
    for root in 0..12 {
        for k in 0..3 {
            let mut reply = message(&format!("$t{root}_{k}"), bob, tick());
            reply["content"]["m.relates_to"] = json!({"rel_type": "m.thread",
                "event_id": format!("$m{root}"), "is_falling_back": true,
                "m.in_reply_to": {"event_id": format!("$m{root}")}});
            events.push(reply);
        }
    }
    for n in 0..6 {
        let mut reply = message(&format!("$r{n}"), bob, tick());
        let target = if n < 4 { format!("$m{}", 20 + n) } else { "$uncached".to_owned() };
        reply["content"]["m.relates_to"] = json!({"m.in_reply_to": {"event_id": target}});
        events.push(reply);
    }
    for n in 0..4 {
        let mut edit = message(&format!("$e{n}"), alice, tick());
        edit["content"]["m.new_content"] = json!({"msgtype": "m.text", "body": "edited"});
        edit["content"]["m.relates_to"] =
            json!({"rel_type": "m.replace", "event_id": format!("$m{}", 30 + n)});
        events.push(edit);
    }
    for n in 0..6 {
        events.push(json!({"type": "m.reaction", "event_id": format!("$x{n}"), "sender": bob,
            "origin_server_ts": tick(),
            "content": {"m.relates_to": {"rel_type": "m.annotation", "event_id": "$m35",
                "key": format!("{n}")}}}));
    }
    events
}

async fn counted_client(server: &MatrixMockServer, dir: &std::path::Path, stats: Arc<Stats>) -> Client {
    let store = Arc::new(matrix_sdk::SqliteEventCacheStore::open(dir, None).await.unwrap());
    let config = matrix_sdk::config::StoreConfig::new(
        matrix_sdk::cross_process_lock::CrossProcessLockConfig::SingleProcess,
    )
    .event_cache_store(Arc::new(Counted::new(store, stats)));
    let client = server
        .client_builder()
        .on_builder(move |b| {
            b.store_config(config).with_threading_support(matrix_sdk::ThreadingSupport::Enabled {
                with_subscriptions: false,
            })
        })
        .build()
        .await;
    client.event_cache().subscribe().unwrap();
    client
}

async fn sync_history(client: &Client) {
    let mut response = v5::Response::new("1".to_owned());
    let mut room = v5::response::Room::default();
    room.timeline = history()
        .into_iter()
        .map(|v| Raw::from_json_string(v.to_string()).unwrap())
        .collect();
    response.rooms.insert(room_id!("!reads:localhost").to_owned(), room);
    client
        .process_sliding_sync_test_helper(&response, &RequestedRequiredStates::default())
        .await
        .unwrap();
    let room = client.get_room(room_id!("!reads:localhost")).unwrap();
    let (cache, _handles) = room.event_cache().await.unwrap();
    let last = EventId::parse("$x5").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while cache.find_event(&last).await.unwrap().is_none() {
        assert!(Instant::now() < deadline, "the history never reached the event cache");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for(events: &EventQueue, kind: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let seen = {
            let mut queue = events.lock().unwrap();
            let mut seen = false;
            while let Some(raw) = queue.pop_front() {
                if serde_json::from_str::<serde_json::Value>(&raw).unwrap()["type"] == kind {
                    seen = true;
                }
            }
            seen
        };
        if seen {
            return;
        }
        assert!(Instant::now() < deadline, "no {kind} was emitted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// What the store was asked while nothing happened: no sync, no user input.
async fn idle_reads(stats: &Stats) -> (Snapshot, String) {
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let before = stats.snapshot();
    let _ = stats.take_top();
    tokio::time::sleep(IDLE).await;
    let delta = stats.snapshot().since(&before);
    let (top, distinct) = stats.take_top();
    (delta, crate::ectrace::trace_line(IDLE.as_secs(), &delta, &top, distinct))
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("lightning-ec-reads-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn an_open_room_leaves_the_event_cache_store_alone_while_idle() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = scratch("room");
    let stats = Arc::new(Stats::default());
    let (_server, client) = runtime.block_on(async {
        let server = MatrixMockServer::new().await;
        let client = counted_client(&server, &dir, Arc::clone(&stats)).await;
        sync_history(&client).await;
        (server, client)
    });
    let events: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
    let registry = Arc::new(TimelineRegistry::new(Arc::clone(&events)));
    registry.open_room(&runtime, client.clone(), ROOM.to_owned());
    let (delta, line) = runtime.block_on(async {
        wait_for(&events, "timeline_reset").await;
        idle_reads(&stats).await
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(delta.reads() <= IDLE_READS_ALLOWED, "an idle open room kept reading: {line}");
}

#[test]
fn an_open_thread_list_leaves_the_event_cache_store_alone_while_idle() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = scratch("list");
    let stats = Arc::new(Stats::default());
    let (_server, client) = runtime.block_on(async {
        let server = MatrixMockServer::new().await;
        let client = counted_client(&server, &dir, Arc::clone(&stats)).await;
        sync_history(&client).await;
        (server, client)
    });
    let events: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
    let registry = Arc::new(TimelineRegistry::new(Arc::clone(&events)));
    registry.open_room(&runtime, client.clone(), ROOM.to_owned());
    registry.thread_list_gen.store(1, Ordering::SeqCst);
    *registry.active_thread_list.lock().unwrap() = Some(ActiveThreadList {
        room_id: ROOM.to_owned(),
        list_gen: 1,
        service: None,
        tally: Arc::default(),
        task: None,
        pagination_busy: Arc::new(AtomicBool::new(false)),
    });
    let lifecycle = registry.lifecycle_gen.load(Ordering::SeqCst);
    let (delta, line) = runtime.block_on(async {
        wait_for(&events, "timeline_reset").await;
        let task = tokio::spawn(open_thread_list_task(
            Arc::clone(&registry), client.clone(), ROOM.to_owned(), 1, lifecycle,
            Arc::clone(&events)));
        wait_for(&events, "thread_list_reset").await;
        let measured = idle_reads(&stats).await;
        task.abort();
        measured
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(delta.reads() <= IDLE_READS_ALLOWED, "an idle thread list kept reading: {line}");
}

#[test]
fn an_open_thread_leaves_the_event_cache_store_alone_while_idle() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = scratch("thread");
    let stats = Arc::new(Stats::default());
    let (_server, client) = runtime.block_on(async {
        let server = MatrixMockServer::new().await;
        let client = counted_client(&server, &dir, Arc::clone(&stats)).await;
        sync_history(&client).await;
        (server, client)
    });
    let events: EventQueue = Arc::new(Mutex::new(VecDeque::new()));
    let registry = Arc::new(TimelineRegistry::new(Arc::clone(&events)));
    registry.open_room(&runtime, client.clone(), ROOM.to_owned());
    registry.thread_gen.store(1, Ordering::SeqCst);
    *registry.active_thread.lock().unwrap() = Some(ActiveThread {
        room_id: ROOM.to_owned(),
        root_event_id: "$m0".to_owned(),
        thread_gen: 1,
        timeline: None,
        task: None,
        pagination_busy: Arc::new(AtomicBool::new(false)),
        reached_start: Arc::new(AtomicBool::new(false)),
        clear_epoch: Arc::new(AtomicU64::new(0)),
    });
    let lifecycle = registry.lifecycle_gen.load(Ordering::SeqCst);
    let (delta, line) = runtime.block_on(async {
        wait_for(&events, "timeline_reset").await;
        let task = tokio::spawn(open_thread_task(
            Arc::clone(&registry), client.clone(), ROOM.to_owned(), "$m0".to_owned(), 1,
            lifecycle, Arc::clone(&events)));
        wait_for(&events, "thread_reset").await;
        let measured = idle_reads(&stats).await;
        task.abort();
        measured
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(delta.reads() <= IDLE_READS_ALLOWED, "an idle open thread kept reading: {line}");
}
