use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::Duration;

use matrix_sdk::ruma::{events::relation::RelationType, EventId, OwnedEventId, RoomId};
use matrix_sdk_base::cross_process_lock::CrossProcessLockGeneration;
use matrix_sdk_base::event_cache::store::EventCacheStore;
use matrix_sdk_base::event_cache::{Event, Gap};
use matrix_sdk_base::linked_chunk::{
    ChunkContent, ChunkIdentifier, ChunkIdentifierGenerator, ChunkMetadata, LinkedChunkId,
    Position, RawChunk, Update,
};

pub(crate) const ENV: &str = "LIGHTNING_EVENTCACHE_TRACE";
const PERIOD: Duration = Duration::from_secs(5);
const TOP_KEYS: usize = 8;
const KEYS_KEPT: usize = 4096;
const OPS: usize = 12;

pub(crate) fn enabled() -> bool {
    std::env::var_os(ENV).is_some()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    FindEvent,
    FindRelations,
    RoomEvents,
    LastChunk,
    PreviousChunk,
    AllChunks,
    ChunksMetadata,
    Duplicates,
    Updates,
    SaveEvent,
    Lock,
    Other,
}

const ALL: [Op; OPS] = [
    Op::FindEvent,
    Op::FindRelations,
    Op::RoomEvents,
    Op::LastChunk,
    Op::PreviousChunk,
    Op::AllChunks,
    Op::ChunksMetadata,
    Op::Duplicates,
    Op::Updates,
    Op::SaveEvent,
    Op::Lock,
    Op::Other,
];

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::FindEvent => "find_event",
            Op::FindRelations => "find_event_relations",
            Op::RoomEvents => "get_room_events",
            Op::LastChunk => "load_last_chunk",
            Op::PreviousChunk => "load_previous_chunk",
            Op::AllChunks => "load_all_chunks",
            Op::ChunksMetadata => "load_all_chunks_metadata",
            Op::Duplicates => "filter_duplicated_events",
            Op::Updates => "handle_linked_chunk_updates",
            Op::SaveEvent => "save_event",
            Op::Lock => "try_take_leased_lock",
            Op::Other => "other",
        }
    }

    fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn is_read(self) -> bool {
        !matches!(self, Op::Updates | Op::SaveEvent | Op::Lock | Op::Other)
    }
}

/// Calls into one event cache store, and the rows each kind of call returned.
#[derive(Default)]
pub(crate) struct Stats {
    calls: [AtomicU64; OPS],
    rows: [AtomicU64; OPS],
    keys: Mutex<HashMap<String, u64>>,
    updates: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    calls: [u64; OPS],
    rows: [u64; OPS],
    updates: u64,
}

impl Snapshot {
    pub(crate) fn calls(&self, op: Op) -> u64 {
        self.calls[op.index()]
    }

    pub(crate) fn rows(&self, op: Op) -> u64 {
        self.rows[op.index()]
    }

    pub(crate) fn reads(&self) -> u64 {
        ALL.iter().filter(|op| op.is_read()).map(|op| self.calls(*op)).sum()
    }

    pub(crate) fn since(&self, earlier: &Snapshot) -> Snapshot {
        let mut out = Snapshot {
            updates: self.updates.saturating_sub(earlier.updates),
            ..Snapshot::default()
        };
        for i in 0..OPS {
            out.calls[i] = self.calls[i].saturating_sub(earlier.calls[i]);
            out.rows[i] = self.rows[i].saturating_sub(earlier.rows[i]);
        }
        out
    }

    pub(crate) fn summary(&self) -> String {
        let mut parts: Vec<String> = ALL
            .iter()
            .filter(|op| self.calls(**op) > 0)
            .map(|op| format!("{}={}/{}", op.name(), self.calls(*op), self.rows(*op)))
            .collect();
        if self.updates > 0 {
            parts.push(format!("room_updates={}", self.updates));
        }
        parts.join(" ")
    }
}

impl Stats {
    fn note(&self, op: Op, key: String, rows: usize) {
        self.calls[op.index()].fetch_add(1, Ordering::Relaxed);
        self.rows[op.index()].fetch_add(rows as u64, Ordering::Relaxed);
        self.note_key(format!("{} {key}", op.name()));
    }

    fn note_key(&self, key: String) {
        if let Ok(mut keys) = self.keys.lock() {
            let key = if keys.len() >= KEYS_KEPT && !keys.contains_key(&key) {
                "<more keys>".to_owned()
            } else {
                key
            };
            *keys.entry(key).or_default() += 1;
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let mut out = Snapshot {
            updates: self.updates.load(Ordering::Relaxed),
            ..Snapshot::default()
        };
        for i in 0..OPS {
            out.calls[i] = self.calls[i].load(Ordering::Relaxed);
            out.rows[i] = self.rows[i].load(Ordering::Relaxed);
        }
        out
    }

    /// The most repeated keys since the last call, and how many distinct keys there were.
    pub(crate) fn take_top(&self) -> (Vec<(String, u64)>, usize) {
        let keys = match self.keys.lock() {
            Ok(mut keys) => std::mem::take(&mut *keys),
            Err(_) => return (Vec::new(), 0),
        };
        let distinct = keys.len();
        let mut top: Vec<(String, u64)> = keys.into_iter().collect();
        top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        top.truncate(TOP_KEYS);
        (top, distinct)
    }
}

pub(crate) fn global() -> Arc<Stats> {
    static STATS: OnceLock<Arc<Stats>> = OnceLock::new();
    Arc::clone(STATS.get_or_init(|| Arc::new(Stats::default())))
}

fn redact(id: &str) -> String {
    crate::redact_matrix_id(id)
}

fn chunk_key(id: &LinkedChunkId<'_>) -> String {
    match id {
        LinkedChunkId::Room(room) => format!("room={}", redact(room.as_str())),
        LinkedChunkId::Thread(room, root) => {
            format!("thread={} root={}", redact(room.as_str()), redact(root.as_str()))
        }
        LinkedChunkId::PinnedEvents(room) => format!("pinned={}", redact(room.as_str())),
        LinkedChunkId::EventFocused(room, event) => {
            format!("focused={} event={}", redact(room.as_str()), redact(event.as_str()))
        }
    }
}

fn chunk_rows(chunk: &RawChunk<Event, Gap>) -> usize {
    match &chunk.content {
        ChunkContent::Items(items) => items.len(),
        ChunkContent::Gap(_) => 0,
    }
}

fn relation_names(filter: Option<&[RelationType]>) -> String {
    match filter {
        None => "any".to_owned(),
        Some(types) => types.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(","),
    }
}

/// An event cache store that counts what is asked of the one it wraps.
pub(crate) struct Counted<T> {
    inner: Arc<T>,
    stats: Arc<Stats>,
}

impl<T> Counted<T> {
    pub(crate) fn new(inner: Arc<T>, stats: Arc<Stats>) -> Self {
        Self { inner, stats }
    }
}

impl<T> std::fmt::Debug for Counted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counted").finish_non_exhaustive()
    }
}

#[matrix_sdk::async_trait]
impl<T: EventCacheStore> EventCacheStore for Counted<T> {
    type Error = T::Error;

    async fn try_take_leased_lock(
        &self,
        lease_duration_ms: u32,
        key: &str,
        holder: &str,
    ) -> Result<Option<CrossProcessLockGeneration>, Self::Error> {
        self.stats.calls[Op::Lock.index()].fetch_add(1, Ordering::Relaxed);
        self.inner.try_take_leased_lock(lease_duration_ms, key, holder).await
    }

    async fn handle_linked_chunk_updates(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
        updates: Vec<Update<Event, Gap>>,
    ) -> Result<(), Self::Error> {
        self.stats.note(Op::Updates, chunk_key(&linked_chunk_id), updates.len());
        self.inner.handle_linked_chunk_updates(linked_chunk_id, updates).await
    }

    async fn load_all_chunks(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
    ) -> Result<Vec<RawChunk<Event, Gap>>, Self::Error> {
        let out = self.inner.load_all_chunks(linked_chunk_id).await;
        let rows = out.as_ref().map_or(0, |chunks| chunks.iter().map(chunk_rows).sum());
        self.stats.note(Op::AllChunks, chunk_key(&linked_chunk_id), rows);
        out
    }

    async fn load_all_chunks_metadata(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
    ) -> Result<Vec<ChunkMetadata>, Self::Error> {
        let out = self.inner.load_all_chunks_metadata(linked_chunk_id).await;
        let rows = out.as_ref().map_or(0, Vec::len);
        self.stats.note(Op::ChunksMetadata, chunk_key(&linked_chunk_id), rows);
        out
    }

    async fn load_last_chunk(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
    ) -> Result<(Option<RawChunk<Event, Gap>>, ChunkIdentifierGenerator), Self::Error> {
        let out = self.inner.load_last_chunk(linked_chunk_id).await;
        let rows = out.as_ref().ok().and_then(|(chunk, _)| chunk.as_ref()).map_or(0, chunk_rows);
        self.stats.note(Op::LastChunk, chunk_key(&linked_chunk_id), rows);
        out
    }

    async fn load_previous_chunk(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
        before_chunk_identifier: ChunkIdentifier,
    ) -> Result<Option<RawChunk<Event, Gap>>, Self::Error> {
        let out = self.inner.load_previous_chunk(linked_chunk_id, before_chunk_identifier).await;
        let rows = out.as_ref().ok().and_then(Option::as_ref).map_or(0, chunk_rows);
        self.stats.note(Op::PreviousChunk, chunk_key(&linked_chunk_id), rows);
        out
    }

    async fn clear_all_linked_chunks(&self) -> Result<(), Self::Error> {
        self.stats.calls[Op::Other.index()].fetch_add(1, Ordering::Relaxed);
        self.inner.clear_all_linked_chunks().await
    }

    async fn filter_duplicated_events(
        &self,
        linked_chunk_id: LinkedChunkId<'_>,
        events: Vec<OwnedEventId>,
    ) -> Result<Vec<(OwnedEventId, Position)>, Self::Error> {
        self.stats.note(Op::Duplicates, chunk_key(&linked_chunk_id), events.len());
        self.inner.filter_duplicated_events(linked_chunk_id, events).await
    }

    async fn find_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
    ) -> Result<Option<Event>, Self::Error> {
        let out = self.inner.find_event(room_id, event_id).await;
        let rows = out.as_ref().map_or(0, |found| usize::from(found.is_some()));
        self.stats.note(Op::FindEvent, redact(event_id.as_str()), rows);
        out
    }

    async fn find_event_relations(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        filter: Option<&[RelationType]>,
    ) -> Result<Vec<(Event, Option<Position>)>, Self::Error> {
        let out = self.inner.find_event_relations(room_id, event_id, filter).await;
        let rows = out.as_ref().map_or(0, Vec::len);
        let key = format!("[{}] {}", relation_names(filter), redact(event_id.as_str()));
        self.stats.note(Op::FindRelations, key, rows);
        out
    }

    async fn get_room_events(
        &self,
        room_id: &RoomId,
        event_type: Option<&str>,
        session_id: Option<&str>,
    ) -> Result<Vec<Event>, Self::Error> {
        let out = self.inner.get_room_events(room_id, event_type, session_id).await;
        let rows = out.as_ref().map_or(0, Vec::len);
        let key = format!(
            "[{}] room={} session={}",
            event_type.unwrap_or("any"),
            redact(room_id.as_str()),
            session_id.map_or_else(|| "any".to_owned(), redact),
        );
        self.stats.note(Op::RoomEvents, key, rows);
        out
    }

    async fn save_event(&self, room_id: &RoomId, event: Event) -> Result<(), Self::Error> {
        self.stats.note(Op::SaveEvent, format!("room={}", redact(room_id.as_str())), 1);
        self.inner.save_event(room_id, event).await
    }

    async fn close(&self) -> Result<(), Self::Error> {
        self.inner.close().await
    }

    async fn reopen(&self) -> Result<(), Self::Error> {
        self.inner.reopen().await
    }

    async fn optimize(&self) -> Result<(), Self::Error> {
        self.inner.optimize().await
    }

    async fn get_size(&self) -> Result<Option<usize>, Self::Error> {
        self.inner.get_size().await
    }
}

/// Counts the SDK's per-room event cache updates into `stats`.
pub(crate) fn watch_client(client: &matrix_sdk::Client, stats: Arc<Stats>) {
    use tokio::sync::broadcast::error::RecvError;
    let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };
    let mut updates = client.event_cache().subscribe_to_room_generic_updates();
    runtime.spawn(async move {
        loop {
            match updates.recv().await {
                Ok(update) => {
                    stats.updates.fetch_add(1, Ordering::Relaxed);
                    stats.note_key(format!("room_update {}", redact(update.room_id.as_str())));
                }
                Err(RecvError::Lagged(missed)) => {
                    stats.updates.fetch_add(missed, Ordering::Relaxed);
                }
                Err(RecvError::Closed) => break,
            }
        }
    });
}

pub(crate) fn trace_line(t_secs: u64, delta: &Snapshot, top: &[(String, u64)], distinct: usize) -> String {
    let top: Vec<String> = top.iter().map(|(key, n)| format!("{key} x{n}")).collect();
    format!(
        "eventcache-trace t={t_secs}s reads={} {} distinct_keys={distinct} top=[{}]",
        delta.reads(),
        delta.summary(),
        top.join("; "),
    )
}

static SINK: Mutex<Option<crate::EventQueueRef>> = Mutex::new(None);

/// With `LIGHTNING_EVENTCACHE_TRACE` set, logs every few seconds what the event
/// cache store was asked and its most repeated keys, so a read storm names its caller.
pub(crate) fn start_if_enabled(client: &matrix_sdk::Client, events: crate::EventQueueRef) {
    static ONCE: Once = Once::new();
    if !enabled() {
        return;
    }
    if let Ok(mut sink) = SINK.lock() {
        *sink = Some(events);
    }
    watch_client(client, global());
    ONCE.call_once(|| {
        let _ = std::thread::Builder::new().name("lightning-ectrace".into()).spawn(|| {
            let stats = global();
            let mut prev = stats.snapshot();
            let mut tick: u64 = 0;
            loop {
                std::thread::sleep(PERIOD);
                tick += 1;
                let now = stats.snapshot();
                let (top, distinct) = stats.take_top();
                let line = trace_line(tick * PERIOD.as_secs(), &now.since(&prev), &top, distinct);
                prev = now;
                let sink = SINK.lock().ok().and_then(|s| s.clone());
                if let Some(events) = sink {
                    crate::enqueue(&events, serde_json::json!({ "type": "rust_trace", "line": line }));
                }
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trace_line_names_the_calls_and_the_most_repeated_keys() {
        let stats = Stats::default();
        let before = stats.snapshot();
        for _ in 0..3 {
            stats.note(Op::FindRelations, "[m.thread] $abc".to_owned(), 40);
        }
        stats.note(Op::FindEvent, "$def".to_owned(), 1);
        stats.note(Op::Updates, "room=!x".to_owned(), 2);
        let delta = stats.snapshot().since(&before);
        assert_eq!(delta.reads(), 4, "a write is not a read");
        assert_eq!(delta.calls(Op::FindRelations), 3);
        assert_eq!(delta.rows(Op::FindRelations), 120);
        let (top, distinct) = stats.take_top();
        assert_eq!(distinct, 3);
        assert_eq!(top[0], ("find_event_relations [m.thread] $abc".to_owned(), 3));
        let line = trace_line(5, &delta, &top, distinct);
        assert!(line.starts_with("eventcache-trace t=5s reads=4 find_event=1/1 find_event_relations=3/120"), "{line}");
        assert!(line.contains("find_event_relations [m.thread] $abc x3"), "{line}");
        assert_eq!(stats.take_top().1, 0, "each report starts a new window");
    }

    #[test]
    fn the_key_table_stays_bounded() {
        let stats = Stats::default();
        for n in 0..KEYS_KEPT + 50 {
            stats.note(Op::FindEvent, format!("${n}"), 0);
        }
        let (top, distinct) = stats.take_top();
        assert_eq!(distinct, KEYS_KEPT + 1);
        assert_eq!(top[0], ("<more keys>".to_owned(), 50));
    }
}
