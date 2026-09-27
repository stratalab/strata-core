//! Space-reclamation contract §3.5 (slice 14, #3600): the storage footprint
//! ratchet. Each primitive loads a fixed dataset through the engine onto two
//! user branches (beside the engine's own `_system_` branch), in batched
//! commits with flushes between them, then walks the session boundaries the
//! 952 MB database fell through:
//!
//! 1. load, then close at once;
//! 2. reopen and close with no writes (the idle reopen);
//! 3. reopen, write once, close.
//!
//! After every reopen the audit-tier footprint must be clean: no table object
//! the manifests do not reference, an empty quarantine, at most one superseded
//! snapshot, no WAL below the retention watermark, and a WAL tail of at most
//! one segment. The total bytes on disk over the logical bytes written must stay
//! under a named per-primitive ceiling. The facts come from
//! `Database::storage_footprint`, the same path `strata admin storage` prints;
//! nothing here walks the directory except the planted-orphan twin.
//!
//! The ceilings are ratchets: each is the measured ratio of this exact
//! workload plus a small margin. A change that moves a ratio up past its
//! ceiling is a footprint regression; a change that moves it down should
//! lower the ceiling in the same PR.
#![cfg(all(feature = "localfs", feature = "testkit"))]

mod common;

use common::{branch, space};
use serde_json::json;
use strata_engine::{
    Database, DatabaseOpenOutcome, DurableLocalOpenOptions, EventBatchAppendEntry, EventPayload,
    EventType, FootprintDetail, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData,
    GraphNodeId, GraphProperties, JsonDocumentId, JsonPath, JsonSetEntry, JsonValue, KvKey,
    KvValue, MaintenanceScheduling, StorageFootprint, VectorCollectionName, VectorConfig,
    VectorDistanceMetric, VectorEmbedding, VectorKey, VectorMetadata, VectorUpsertEntry,
};

/// Batches per branch; each batch is flushed before the next, so every load
/// leaves several table generations for compaction and reclaim to settle.
const BATCHES: usize = 4;
/// Records per batch, per branch.
const PER_BATCH: usize = 256;
/// The user branches each workload writes: the product default and a second
/// root, so the checkpoint and recovery paths see the multi-branch topology.
const BRANCHES: [&str; 2] = ["default", "feature"];

/// The WAL segment size every ratchet database opens with: small enough that
/// each load spans several segments, so WAL reclaim is observable. It is also
/// the most the tail above the retention watermark may hold.
const WAL_SEGMENT_BYTES: u64 = 256 * 1024;

/// Ratchet ceilings: total bytes on disk over logical bytes written, after a
/// clean close and reopen. Each is the measured ratio of this workload
/// (recorded beside it; deterministic under the inline scheduler) plus about
/// five percent. Flushed tables are Zstd-compressed, so ordinary text lands
/// well under 1x.
const KV_CEILING: f64 = 0.15; // measured 0.140
const JSON_CEILING: f64 = 0.21; // measured 0.196
const EVENT_CEILING: f64 = 0.66; // measured 0.627
const VECTOR_CEILING: f64 = 0.73; // measured 0.690
const GRAPH_CEILING: f64 = 0.35; // measured 0.326
/// The never-flushed event log on the production scheduler: every row lives in
/// the close checkpoint's snapshot, which is stored uncompressed (#3625).
/// Measured 3.473-3.474 across ten runs. Lower it when #3625 lands.
const NEVER_FLUSHED_EVENT_CEILING: f64 = 3.6;

#[derive(Clone, Copy, Debug)]
enum Primitive {
    Kv,
    Json,
    Event,
    Vector,
    Graph,
}

impl Primitive {
    const fn ceiling(self) -> f64 {
        match self {
            Self::Kv => KV_CEILING,
            Self::Json => JSON_CEILING,
            Self::Event => EVENT_CEILING,
            Self::Vector => VECTOR_CEILING,
            Self::Graph => GRAPH_CEILING,
        }
    }
}

fn open_with(path: &std::path::Path, scheduling: MaintenanceScheduling) -> Database {
    Database::open_local(
        path,
        DurableLocalOpenOptions::new()
            .with_maintenance_scheduling_policy_for_test(scheduling)
            .with_wal_segment_size_for_test(WAL_SEGMENT_BYTES),
    )
    .map(DatabaseOpenOutcome::into_database)
    .expect("durable open")
}

fn open_inline(path: &std::path::Path) -> Database {
    open_with(path, MaintenanceScheduling::DeterministicInline)
}

/// A deterministic sentence of ordinary text: the payload shape of an agent
/// transcript, neither incompressible noise nor one repeated byte.
fn prose(index: usize) -> String {
    const WORDS: [&str; 16] = [
        "branch", "commit", "vector", "event", "graph", "table", "reclaim", "snapshot", "manifest",
        "window", "session", "agent", "tool", "result", "query", "answer",
    ];
    (0..24)
        .map(|word| WORDS[(index.wrapping_mul(31) + word * 7 + word * word) % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

/// One batch of `primitive` records on `branch_name`; returns the logical
/// bytes the caller wrote (keys, identifiers and payloads — never anything
/// the engine derives).
fn load_batch(db: &mut Database, primitive: Primitive, branch_name: &str, batch: usize) -> u64 {
    let range = batch * PER_BATCH..(batch + 1) * PER_BATCH;
    match primitive {
        Primitive::Kv => load_kv(db, branch_name, range),
        Primitive::Json => load_json(db, branch_name, range),
        Primitive::Event => load_events(db, branch_name, range),
        Primitive::Vector => load_vectors(db, branch_name, range, batch == 0),
        Primitive::Graph => load_graph(db, branch_name, range, batch == 0),
    }
}

fn load_kv(db: &mut Database, branch_name: &str, range: std::ops::Range<usize>) -> u64 {
    let entries: Vec<(KvKey, KvValue)> = range
        .map(|i| {
            (
                KvKey::new(format!("turn-{i:06}")).expect("key"),
                KvValue::new(prose(i).into_bytes()),
            )
        })
        .collect();
    let bytes = entries
        .iter()
        .map(|(k, v)| (k.as_bytes().len() + v.as_bytes().len()) as u64)
        .sum();
    db.kv(branch(branch_name), space("default"))
        .expect("kv")
        .put_batch(entries)
        .expect("kv batch");
    bytes
}

fn load_json(db: &mut Database, branch_name: &str, range: std::ops::Range<usize>) -> u64 {
    let mut bytes = 0u64;
    let entries: Vec<JsonSetEntry> = range
        .map(|i| {
            let id = format!("doc-{i:06}");
            let doc = json!({"turn": i, "role": "assistant", "text": prose(i)});
            bytes += (id.len() + serde_json::to_vec(&doc).expect("json").len()) as u64;
            JsonSetEntry::new(
                JsonDocumentId::new(id).expect("id"),
                JsonPath::root(),
                JsonValue::new(doc).expect("value"),
            )
        })
        .collect();
    db.json(branch(branch_name), space("default"))
        .expect("json")
        .batch_set_or_create(entries)
        .expect("json batch");
    bytes
}

fn load_events(db: &mut Database, branch_name: &str, range: std::ops::Range<usize>) -> u64 {
    let mut bytes = 0u64;
    let entries: Vec<EventBatchAppendEntry> = range
        .map(|i| {
            let kind = "tool_call";
            let payload = json!({"turn": i, "tool": "search", "text": prose(i)});
            bytes += (kind.len() + serde_json::to_vec(&payload).expect("json").len()) as u64;
            EventBatchAppendEntry::new(
                EventType::new(kind).expect("type"),
                EventPayload::new(payload).expect("payload"),
            )
        })
        .collect();
    db.event(branch(branch_name), space("default"))
        .expect("event")
        .batch_append(entries)
        .expect("event batch");
    bytes
}

fn load_vectors(
    db: &mut Database,
    branch_name: &str,
    range: std::ops::Range<usize>,
    first: bool,
) -> u64 {
    const DIMENSION: usize = 64;
    let collection = VectorCollectionName::new("embeddings").expect("name");
    let mut vectors = db
        .vector(branch(branch_name), space("default"))
        .expect("vector");
    if first {
        vectors
            .create_collection(
                collection.clone(),
                VectorConfig::new(DIMENSION, VectorDistanceMetric::Cosine).expect("config"),
            )
            .expect("collection");
    }
    let mut bytes = 0u64;
    let entries: Vec<VectorUpsertEntry> = range
        .map(|i| {
            let key = format!("chunk-{i:06}");
            let metadata = json!({"doc": i / 8, "section": i % 8});
            bytes += (key.len()
                + DIMENSION * 4
                + serde_json::to_vec(&metadata).expect("json").len()) as u64;
            let embedding: Vec<f32> = (0..DIMENSION)
                .map(|d| {
                    let step = u16::try_from((i * 131 + d * 17) % 997).expect("below 997");
                    f32::from(step) / 997.0 - 0.5
                })
                .collect();
            VectorUpsertEntry::new(
                VectorKey::new(key).expect("key"),
                VectorEmbedding::new(embedding).expect("embedding"),
                Some(VectorMetadata::new(metadata).expect("metadata")),
            )
        })
        .collect();
    vectors
        .batch_upsert(&collection, &entries)
        .expect("vector batch");
    bytes
}

fn load_graph(
    db: &mut Database,
    branch_name: &str,
    range: std::ops::Range<usize>,
    first: bool,
) -> u64 {
    let name = GraphName::new("knowledge").expect("graph");
    let mut graph = db
        .graph(branch(branch_name), space("default"))
        .expect("graph");
    if first {
        graph.create_graph(name.clone()).expect("graph created");
    }
    let mut bytes = 0u64;
    let nodes: Vec<(GraphNodeId, GraphNodeData)> = range
        .clone()
        .map(|i| {
            let id = format!("entity-{i:06}");
            let properties = json!({"label": prose(i)});
            bytes += (id.len() + serde_json::to_vec(&properties).expect("json").len()) as u64;
            (
                GraphNodeId::new(id).expect("id"),
                GraphNodeData::new(
                    Some(GraphProperties::new(properties).expect("properties")),
                    None,
                ),
            )
        })
        .collect();
    // Each node links to its predecessor in the batch: one edge per
    // node after the first.
    let edges: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> = range
        .skip(1)
        .map(|i| {
            let src = format!("entity-{i:06}");
            let dst = format!("entity-{:06}", i - 1);
            let kind = "mentions";
            bytes += (src.len() + kind.len() + dst.len() + 8) as u64;
            (
                GraphNodeId::new(src).expect("src"),
                GraphEdgeType::new(kind).expect("type"),
                GraphNodeId::new(dst).expect("dst"),
                GraphEdgeData::new(1.0, None).expect("edge data"),
            )
        })
        .collect();
    graph
        .bulk_insert(&name, &nodes, &edges, None)
        .expect("graph bulk insert");
    bytes
}

/// Every way a footprint can hold reclaimable debt, as readable violations.
/// Empty means clean.
fn debt_violations(footprint: &StorageFootprint) -> Vec<String> {
    let mut violations = Vec::new();
    if footprint.detail != FootprintDetail::Audit {
        violations.push("not an audit-tier footprint".to_owned());
    }
    if footprint.unreferenced_objects != Some(0) || footprint.unreferenced_bytes != Some(0) {
        violations.push(format!(
            "unreferenced table objects: {:?} ({:?} bytes)",
            footprint.unreferenced_objects, footprint.unreferenced_bytes
        ));
    }
    if footprint.quarantined_objects != Some(0) || footprint.quarantined_bytes != Some(0) {
        violations.push(format!(
            "quarantine not empty: {:?} ({:?} bytes)",
            footprint.quarantined_objects, footprint.quarantined_bytes
        ));
    }
    if footprint.superseded_snapshots.is_none_or(|count| count > 1) {
        violations.push(format!(
            "superseded snapshots: {:?}",
            footprint.superseded_snapshots
        ));
    }
    // A listing count independent of the superseded predicate: the live
    // snapshot plus at most the one superseded snapshot allowed above.
    if footprint.snapshot_objects.is_none_or(|count| count > 2) {
        violations.push(format!(
            "snapshot objects on disk: {:?}",
            footprint.snapshot_objects
        ));
    }
    if footprint.wal_reclaimable_bytes != Some(0) {
        violations.push(format!(
            "WAL below the retention watermark: {:?} bytes",
            footprint.wal_reclaimable_bytes
        ));
    }
    // The tail above the watermark is at most one WAL segment. (The audit's
    // own listing: the runtime's cached segment count is cold on a reopen
    // until the session's first commit.)
    if footprint
        .wal_tail_bytes
        .is_none_or(|bytes| bytes > WAL_SEGMENT_BYTES)
    {
        violations.push(format!(
            "WAL tail above one segment: {:?} bytes",
            footprint.wal_tail_bytes
        ));
    }
    violations
}

fn audit(db: &mut Database) -> StorageFootprint {
    db.storage_footprint(None, FootprintDetail::Audit)
        .expect("audit footprint")
}

fn assert_within_ratchet(
    primitive: Primitive,
    ceiling: f64,
    stage: &str,
    footprint: &StorageFootprint,
    logical: u64,
) {
    let violations = debt_violations(footprint);
    assert!(
        violations.is_empty(),
        "{primitive:?} after {stage}: {violations:?}\n{footprint:?}"
    );
    let total = footprint.total_bytes.expect("the audit totals every part");
    #[allow(clippy::cast_precision_loss, reason = "a ratio of byte counts")]
    let ratio = total as f64 / logical as f64;
    eprintln!("RATCHET {primitive:?} {stage}: total={total} logical={logical} ratio={ratio:.3}");
    assert!(
        ratio <= ceiling,
        "{primitive:?} after {stage}: {total} bytes on disk for {logical} logical bytes \
         is {ratio:.3}x, over the {ceiling:.3}x ceiling\n{footprint:?}"
    );
}

/// The full session walk for one primitive.
fn ratchet(primitive: Primitive) {
    let dir = tempfile::tempdir().expect("tmp");
    let mut logical = 0u64;
    {
        let mut db = open_inline(dir.path());
        db.branches()
            .expect("branches")
            .create(branch("feature"))
            .expect("create feature");
        for batch in 0..BATCHES {
            for branch_name in BRANCHES {
                logical += load_batch(&mut db, primitive, branch_name, batch);
                db.flush_storage_branch_for_test(&branch(branch_name))
                    .expect("flush");
            }
        }
        // Non-vacuity: the load spans several WAL segments, so a close that
        // failed to release them would show up as reclaimable WAL below.
        let loaded = db
            .storage_footprint(None, FootprintDetail::Live)
            .expect("live footprint");
        assert!(
            loaded.wal_retained_segments.is_some_and(|count| count >= 2),
            "{primitive:?}: the load must span several WAL segments: {loaded:?}"
        );
        db.close().expect("clean close");
    }
    // 1. The close right after the load.
    {
        let mut db = open_inline(dir.path());
        assert_within_ratchet(
            primitive,
            primitive.ceiling(),
            "load and close",
            &audit(&mut db),
            logical,
        );
        db.close().expect("clean close");
    }
    // 2. An idle session: reopen and close with no writes.
    {
        let mut db = open_inline(dir.path());
        db.close().expect("clean close");
        let mut db = open_inline(dir.path());
        assert_within_ratchet(
            primitive,
            primitive.ceiling(),
            "idle reopen",
            &audit(&mut db),
            logical,
        );
        db.close().expect("clean close");
    }
    // 3. A session with one write.
    {
        let mut db = open_inline(dir.path());
        logical += load_single(&mut db);
        db.close().expect("clean close");
        let mut db = open_inline(dir.path());
        assert_within_ratchet(
            primitive,
            primitive.ceiling(),
            "one-write session",
            &audit(&mut db),
            logical,
        );
        db.close().expect("clean close");
    }
}

/// The one write of the third session: a KV put on the default branch.
fn load_single(db: &mut Database) -> u64 {
    let key = KvKey::new("last-turn").expect("key");
    let value = KvValue::new(prose(0).into_bytes());
    let bytes = (key.as_bytes().len() + value.as_bytes().len()) as u64;
    db.kv(branch("default"), space("default"))
        .expect("kv")
        .put(key, value)
        .expect("put");
    bytes
}

#[test]
fn kv_footprint_holds_its_ratchet_across_sessions() {
    ratchet(Primitive::Kv);
}

#[test]
fn json_footprint_holds_its_ratchet_across_sessions() {
    ratchet(Primitive::Json);
}

#[test]
fn event_footprint_holds_its_ratchet_across_sessions() {
    ratchet(Primitive::Event);
}

#[test]
fn vector_footprint_holds_its_ratchet_across_sessions() {
    ratchet(Primitive::Vector);
}

#[test]
fn graph_footprint_holds_its_ratchet_across_sessions() {
    ratchet(Primitive::Graph);
}

/// The `strata-turn` shape on the production scheduler: batched event
/// appends, a clean close, and a reopen whose background worker settles the
/// prior session's debt within a bounded wait.
#[test]
fn event_log_load_settles_on_the_background_scheduler() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut logical = 0u64;
    {
        let mut db = open_with(dir.path(), MaintenanceScheduling::Background);
        for batch in 0..BATCHES * 2 {
            logical += load_batch(&mut db, Primitive::Event, "default", batch);
        }
        db.close().expect("clean close");
    }
    let mut db = open_with(dir.path(), MaintenanceScheduling::Background);
    let footprint = settle(&mut db);
    assert_within_ratchet(
        Primitive::Event,
        NEVER_FLUSHED_EVENT_CEILING,
        "background reopen",
        &footprint,
        logical,
    );
    db.close().expect("clean close");
}

/// The first control run of the 952 MB investigation on the production
/// scheduler: batched event appends flushed between batches (so compaction
/// supersedes tables while the worker is still reclaiming), then an immediate
/// close. The close drains what it can; the reopen's worker must settle the
/// rest within a bounded wait.
#[test]
fn flushed_event_load_closed_at_once_settles_on_the_background_scheduler() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut logical = 0u64;
    {
        let mut db = open_with(dir.path(), MaintenanceScheduling::Background);
        for batch in 0..BATCHES * 2 {
            logical += load_batch(&mut db, Primitive::Event, "default", batch);
            db.flush_storage_branch_for_test(&branch("default"))
                .expect("flush");
        }
        db.close().expect("clean close");
    }
    let mut db = open_with(dir.path(), MaintenanceScheduling::Background);
    let footprint = settle(&mut db);
    assert_within_ratchet(
        Primitive::Event,
        EVENT_CEILING,
        "flushed load, immediate close, background reopen",
        &footprint,
        logical,
    );
    db.close().expect("clean close");
}

/// Poll the audit until the background worker has nothing queued and the
/// footprint holds no debt, or 30 s pass; returns the last reading either way.
fn settle(db: &mut Database) -> StorageFootprint {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let footprint = audit(db);
        let settled = footprint.reclaim.pending_reclaim_tasks == Some(0)
            && debt_violations(&footprint).is_empty();
        if settled || std::time::Instant::now() >= deadline {
            return footprint;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Non-vacuity twin: a stray table object on disk — a byte copy of a live
/// one under a name no manifest references — must fail the ratchet's debt
/// check. Proves the audit lists what is on disk rather than what the
/// catalogue believes.
#[test]
fn a_planted_orphan_table_object_fails_the_ratchet() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let mut db = open_inline(dir.path());
        for batch in 0..2 {
            load_batch(&mut db, Primitive::Kv, "default", batch);
            db.flush_storage_branch_for_test(&branch("default"))
                .expect("flush");
        }
        db.close().expect("clean close");
    }
    let mut db = open_inline(dir.path());
    assert!(
        debt_violations(&audit(&mut db)).is_empty(),
        "clean before the plant"
    );
    let live = table_objects(dir.path());
    let source = live.first().expect("a flushed table object on disk");
    let planted = source.with_file_name(format!(
        "ffffffffffffff00{}",
        source
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.find('.').map(|dot| &name[dot..]))
            .expect("object suffix")
    ));
    std::fs::copy(source, &planted).expect("plant an orphan");
    let violations = debt_violations(&audit(&mut db));
    assert!(
        violations
            .iter()
            .any(|v| v.starts_with("unreferenced table objects")),
        "a planted orphan passed the ratchet: {violations:?} (planted {})",
        planted.display()
    );
    db.close().expect("clean close");
}

/// Every table data object under the database directory.
fn table_objects(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path
                .components()
                .any(|component| component.as_os_str() == "tables")
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".object@"))
            {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out.sort();
    out
}
