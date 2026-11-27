//! Property tests: temporal reads for the keyed-mutable capabilities (KV, JSON
//! documents, graph nodes) match a reference timeline oracle, and a branch
//! forked at a version equals the source read as-of that version.
//!
//! All three capabilities are keyed, mutable (put/delete, upsert/remove), and
//! read through the same MVCC `read_row` + `ReadSelector::{AtVersion,
//! AtTimestamp}` path, so a single generic oracle covers them via the
//! `TemporalFixture` trait. A random sequence of put/delete commits is applied
//! to a small key set; each commit's version, timestamp, and resulting value
//! are recorded into a per-key timeline. The engine's latest / as-of-version /
//! as-of-timestamp reads are then checked against that oracle across every
//! observed commit point plus the out-of-range boundaries (version 0 and MAX,
//! timestamp EPOCH and MAX), which must be `history_unavailable` diagnostics.
//!
//! Event temporal reads are append-only (monotonic, no tombstones) and do not
//! fit this keyed-mutable oracle; they are covered separately (TCP3.6b-ii).

#![allow(clippy::result_large_err)]

mod common;

use std::collections::BTreeSet;

use common::{branch, key, open_cache_database, space, value};
use proptest::prelude::*;
use serde_json::{json, Value};
use strata_core::{CommitVersion, Timestamp};
use strata_engine::{
    Database, EngineResult, GraphName, GraphNode, GraphNodeData, GraphNodeId, GraphProperties,
    JsonDocumentId, JsonPath, JsonValue,
};

const N_KEYS: usize = 3;
const HISTORY_CODE: &str = "history_unavailable.engine.persistence_history";
const BEFORE_FIRST_CODE: &str = "history_unavailable.engine.before_first_commit";
const AFTER_LATEST_CODE: &str = "history_unavailable.engine.after_latest_commit";

#[derive(Clone, Copy, Debug)]
enum Op {
    Put(usize, u8),
    Delete(usize),
}

fn any_op() -> impl Strategy<Value = Op> {
    let keys = 0usize..N_KEYS;
    prop_oneof![
        (keys.clone(), any::<u8>()).prop_map(|(index, byte)| Op::Put(index, byte)),
        keys.prop_map(Op::Delete),
    ]
}

/// One committed change to a key; `value` is `None` for a tombstone.
#[derive(Clone)]
struct Event {
    version: CommitVersion,
    timestamp: Timestamp,
    value: Option<Vec<u8>>,
}

/// The version and timestamp a write committed at.
#[derive(Clone, Copy)]
struct Commit {
    version: CommitVersion,
    timestamp: Timestamp,
}

/// Value of the latest event at or before `version` (events are in version
/// order, so the last one within bound wins).
fn oracle_at_version(events: &[Event], version: CommitVersion) -> Option<Vec<u8>> {
    events
        .iter()
        .rfind(|event| event.version <= version)
        .and_then(|event| event.value.clone())
}

/// Value of the highest-version event at or before `timestamp` (MVCC as-of).
fn oracle_at_timestamp(events: &[Event], timestamp: Timestamp) -> Option<Vec<u8>> {
    events
        .iter()
        .filter(|event| event.timestamp <= timestamp)
        .max_by_key(|event| event.version)
        .and_then(|event| event.value.clone())
}

fn oracle_latest(events: &[Event]) -> Option<Vec<u8>> {
    events.last().and_then(|event| event.value.clone())
}

/// Decodes the one byte a capability round-trips through its stored value.
fn seed_from_json(value: &Value) -> Vec<u8> {
    let seed = value
        .get("seed")
        .and_then(Value::as_u64)
        .expect("stored value carries a seed");
    vec![u8::try_from(seed).expect("seed byte")]
}

/// A keyed-mutable capability the shared temporal oracle can exercise. Each
/// fixture encodes a `byte` on write and decodes it on read, so the oracle can
/// compare `Option<Vec<u8>>` uniformly.
trait TemporalFixture {
    fn label() -> &'static str;

    /// Creates any container the capability needs before writes. Idempotent.
    fn ensure(db: &mut Database, branch_name: &str) -> EngineResult<()>;

    /// Writes `byte` to key `index`, overwriting any prior value; always commits.
    fn put(db: &mut Database, branch_name: &str, index: usize, byte: u8) -> EngineResult<Commit>;

    /// Removes key `index`. Commits a tombstone only if the key existed.
    fn delete(db: &mut Database, branch_name: &str, index: usize) -> EngineResult<Option<Commit>>;

    /// Latest value for key `index`, or `None` if absent/tombstoned.
    fn get_latest(
        db: &mut Database,
        branch_name: &str,
        index: usize,
    ) -> EngineResult<Option<Vec<u8>>>;

    /// Value for key `index` visible at `version`.
    fn get_at_version(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        version: CommitVersion,
    ) -> EngineResult<Option<Vec<u8>>>;

    /// Value for key `index` visible at `timestamp`.
    fn get_at(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        timestamp: Timestamp,
    ) -> EngineResult<Option<Vec<u8>>>;
}

const KV_KEYS: [&[u8]; N_KEYS] = [b"ka", b"kb", b"kc"];

struct KvTemporal;

impl TemporalFixture for KvTemporal {
    fn label() -> &'static str {
        "kv"
    }

    fn ensure(_db: &mut Database, _branch_name: &str) -> EngineResult<()> {
        Ok(())
    }

    fn put(db: &mut Database, branch_name: &str, index: usize, byte: u8) -> EngineResult<Commit> {
        let outcome = db
            .kv(branch(branch_name), space("default"))?
            .put(key(KV_KEYS[index]), value(&[byte]))?
            .commit();
        Ok(Commit {
            version: outcome.version(),
            timestamp: outcome.timestamp(),
        })
    }

    fn delete(db: &mut Database, branch_name: &str, index: usize) -> EngineResult<Option<Commit>> {
        let outcome = db
            .kv(branch(branch_name), space("default"))?
            .delete(key(KV_KEYS[index]))?;
        Ok(outcome.commit().map(|commit| Commit {
            version: commit.version(),
            timestamp: commit.timestamp(),
        }))
    }

    fn get_latest(
        db: &mut Database,
        branch_name: &str,
        index: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .kv(branch(branch_name), space("default"))?
            .get(&key(KV_KEYS[index]))?
            .map(|read| read.as_bytes().to_vec()))
    }

    fn get_at_version(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        version: CommitVersion,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .kv(branch(branch_name), space("default"))?
            .get_at_version(&key(KV_KEYS[index]), version)?
            .map(|read| read.as_bytes().to_vec()))
    }

    fn get_at(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        timestamp: Timestamp,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .kv(branch(branch_name), space("default"))?
            .get_at(&key(KV_KEYS[index]), timestamp)?
            .map(|read| read.as_bytes().to_vec()))
    }
}

struct JsonTemporal;

impl JsonTemporal {
    fn doc_id(index: usize) -> EngineResult<JsonDocumentId> {
        JsonDocumentId::new(format!("doc-{index}"))
    }
}

impl TemporalFixture for JsonTemporal {
    fn label() -> &'static str {
        "json"
    }

    fn ensure(_db: &mut Database, _branch_name: &str) -> EngineResult<()> {
        Ok(())
    }

    fn put(db: &mut Database, branch_name: &str, index: usize, byte: u8) -> EngineResult<Commit> {
        let id = Self::doc_id(index)?;
        let document = JsonValue::new(json!({ "seed": byte }))?;
        let mut json = db.json(branch(branch_name), space("default"))?;
        // `set` does not create the document and `create` rejects an existing
        // one; together they give put's create-or-overwrite semantics.
        let outcome = if json.exists(&id)? {
            json.set(id, &JsonPath::root(), document)?
        } else {
            json.create(id, document)?
        };
        let commit = outcome.commit();
        Ok(Commit {
            version: commit.version(),
            timestamp: commit.timestamp(),
        })
    }

    fn delete(db: &mut Database, branch_name: &str, index: usize) -> EngineResult<Option<Commit>> {
        let id = Self::doc_id(index)?;
        let outcome = db
            .json(branch(branch_name), space("default"))?
            .delete(id, &JsonPath::root())?;
        Ok(outcome.commit().map(|commit| Commit {
            version: commit.version(),
            timestamp: commit.timestamp(),
        }))
    }

    fn get_latest(
        db: &mut Database,
        branch_name: &str,
        index: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::doc_id(index)?;
        Ok(db
            .json(branch(branch_name), space("default"))?
            .get(&id, &JsonPath::root())?
            .map(|document| seed_from_json(document.as_inner())))
    }

    fn get_at_version(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        version: CommitVersion,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::doc_id(index)?;
        Ok(db
            .json(branch(branch_name), space("default"))?
            .get_at_version(&id, &JsonPath::root(), version)?
            .map(|document| seed_from_json(document.as_inner())))
    }

    fn get_at(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        timestamp: Timestamp,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::doc_id(index)?;
        Ok(db
            .json(branch(branch_name), space("default"))?
            .get_at(&id, &JsonPath::root(), timestamp)?
            .map(|document| seed_from_json(document.as_inner())))
    }
}

const GRAPH_NAME: &str = "temporal";

struct GraphTemporal;

impl GraphTemporal {
    fn graph() -> EngineResult<GraphName> {
        GraphName::new(GRAPH_NAME)
    }

    fn node_id(index: usize) -> EngineResult<GraphNodeId> {
        GraphNodeId::new(format!("n-{index}"))
    }
}

impl TemporalFixture for GraphTemporal {
    fn label() -> &'static str {
        "graph"
    }

    fn ensure(db: &mut Database, branch_name: &str) -> EngineResult<()> {
        match db
            .graph(branch(branch_name), space("default"))?
            .create_graph(Self::graph()?)
        {
            Ok(_) => Ok(()),
            Err(error) if error.code() == "already_exists.engine.graph" => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn put(db: &mut Database, branch_name: &str, index: usize, byte: u8) -> EngineResult<Commit> {
        let properties = GraphProperties::new(json!({ "seed": byte }))?;
        let data = GraphNodeData::new(Some(properties), None);
        let outcome = db
            .graph(branch(branch_name), space("default"))?
            .upsert_node(&Self::graph()?, Self::node_id(index)?, data)?;
        let commit = outcome.commit();
        Ok(Commit {
            version: commit.version(),
            timestamp: commit.timestamp(),
        })
    }

    fn delete(db: &mut Database, branch_name: &str, index: usize) -> EngineResult<Option<Commit>> {
        let outcome = db
            .graph(branch(branch_name), space("default"))?
            .delete_node(&Self::graph()?, &Self::node_id(index)?)?;
        Ok(outcome.commit().map(|commit| Commit {
            version: commit.version(),
            timestamp: commit.timestamp(),
        }))
    }

    fn get_latest(
        db: &mut Database,
        branch_name: &str,
        index: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .graph(branch(branch_name), space("default"))?
            .get_node(&Self::graph()?, &Self::node_id(index)?)?
            .map(|node| node_seed(&node)))
    }

    fn get_at_version(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        version: CommitVersion,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .graph(branch(branch_name), space("default"))?
            .get_node_at_version(&Self::graph()?, &Self::node_id(index)?, version)?
            .map(|node| node_seed(&node)))
    }

    fn get_at(
        db: &mut Database,
        branch_name: &str,
        index: usize,
        timestamp: Timestamp,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(db
            .graph(branch(branch_name), space("default"))?
            .get_node_at(&Self::graph()?, &Self::node_id(index)?, timestamp)?
            .map(|node| node_seed(&node)))
    }
}

fn node_seed(node: &GraphNode) -> Vec<u8> {
    let properties = node
        .data()
        .properties()
        .expect("temporal graph node carries properties");
    seed_from_json(properties.as_inner())
}

/// Applies `ops`, records a per-key reference timeline, then checks the
/// capability's latest / as-of-version / as-of-timestamp reads against the
/// oracle at every observed commit point and the out-of-range boundaries.
fn temporal_reads_match_oracle<F: TemporalFixture>(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let mut db = open_cache_database().expect("cache database opens");
    F::ensure(&mut db, "default").expect("ensure container");

    let mut timelines: Vec<Vec<Event>> = vec![Vec::new(); N_KEYS];
    let mut max_version = 0u64;
    for op in ops {
        match op {
            Op::Put(index, byte) => {
                let commit = F::put(&mut db, "default", index, byte).expect("put commits");
                timelines[index].push(Event {
                    version: commit.version,
                    timestamp: commit.timestamp,
                    value: Some(vec![byte]),
                });
                max_version = max_version.max(commit.version.as_u64());
            }
            Op::Delete(index) => {
                if let Some(commit) = F::delete(&mut db, "default", index).expect("delete") {
                    timelines[index].push(Event {
                        version: commit.version,
                        timestamp: commit.timestamp,
                        value: None,
                    });
                    max_version = max_version.max(commit.version.as_u64());
                }
            }
        }
    }

    // Only recorded commit points are guaranteed to resolve to a retained
    // frontier (control-plane commits interleave, so versions are not
    // contiguous). Out-of-range boundaries are checked separately below.
    let mut commit_versions: BTreeSet<CommitVersion> = BTreeSet::new();
    let mut commit_timestamps: BTreeSet<Timestamp> = BTreeSet::new();
    for timeline in &timelines {
        for event in timeline {
            commit_versions.insert(event.version);
            commit_timestamps.insert(event.timestamp);
        }
    }

    for (index, timeline) in timelines.iter().enumerate() {
        let latest = F::get_latest(&mut db, "default", index).expect("latest read");
        prop_assert_eq!(latest, oracle_latest(timeline), "{} latest", F::label());

        for &version in &commit_versions {
            let actual = F::get_at_version(&mut db, "default", index, version)
                .expect("in-range version read succeeds");
            prop_assert_eq!(
                actual,
                oracle_at_version(timeline, version),
                "{}",
                F::label()
            );
        }
        for &timestamp in &commit_timestamps {
            let actual = F::get_at(&mut db, "default", index, timestamp)
                .expect("in-range timestamp read succeeds");
            prop_assert_eq!(
                actual,
                oracle_at_timestamp(timeline, timestamp),
                "{}",
                F::label()
            );
        }

        // Out-of-range reads are diagnostics, never clamp-to-latest or ordinary
        // absence: before the retained floor (version 0 / EPOCH) and after the
        // latest retained commit (version MAX / Timestamp::MAX). A timestamp
        // names the end it missed; a version keeps the retained-history code.
        if max_version >= 1 {
            let boundaries = [
                (
                    F::get_at_version(&mut db, "default", index, CommitVersion::new(0)),
                    HISTORY_CODE,
                ),
                (
                    F::get_at_version(&mut db, "default", index, CommitVersion::MAX),
                    HISTORY_CODE,
                ),
                (
                    F::get_at(&mut db, "default", index, Timestamp::EPOCH),
                    BEFORE_FIRST_CODE,
                ),
                (
                    F::get_at(&mut db, "default", index, Timestamp::MAX),
                    AFTER_LATEST_CODE,
                ),
            ];
            for (boundary, code) in boundaries {
                let error = boundary.expect_err("out-of-range read is a diagnostic");
                prop_assert_eq!(error.code(), code, "{}", F::label());
            }
        }
    }
    Ok(())
}

/// A branch forked at a version reads exactly like the source read as-of that
/// version.
fn fork_at_version_equals_source_as_of<F: TemporalFixture>(
    bytes: &[u8],
    fork_point: usize,
) -> Result<(), TestCaseError> {
    let mut db = open_cache_database().expect("cache database opens");
    F::ensure(&mut db, "default").expect("ensure container");

    let mut versions: Vec<CommitVersion> = Vec::new();
    for &byte in bytes {
        let commit = F::put(&mut db, "default", 0, byte).expect("put commits");
        versions.push(commit.version);
    }

    let fork_version = versions[fork_point % versions.len()];
    db.branches()
        .expect("branch service")
        .fork_at_version(&branch("default"), branch("snapshot"), fork_version)
        .expect("fork at version");

    let child_latest = F::get_latest(&mut db, "snapshot", 0).expect("child latest read");
    let source_as_of =
        F::get_at_version(&mut db, "default", 0, fork_version).expect("source as-of read");
    prop_assert_eq!(child_latest, source_as_of, "{}", F::label());
    Ok(())
}

proptest! {
    #[test]
    fn kv_temporal_reads_match_oracle(ops in prop::collection::vec(any_op(), 1..30)) {
        temporal_reads_match_oracle::<KvTemporal>(ops)?;
    }

    #[test]
    fn json_temporal_reads_match_oracle(ops in prop::collection::vec(any_op(), 1..30)) {
        temporal_reads_match_oracle::<JsonTemporal>(ops)?;
    }

    #[test]
    fn graph_temporal_reads_match_oracle(ops in prop::collection::vec(any_op(), 1..30)) {
        temporal_reads_match_oracle::<GraphTemporal>(ops)?;
    }

    #[test]
    fn kv_fork_at_version_equals_source_as_of(
        bytes in prop::collection::vec(any::<u8>(), 1..15),
        fork_point in 0usize..15,
    ) {
        fork_at_version_equals_source_as_of::<KvTemporal>(&bytes, fork_point)?;
    }

    #[test]
    fn json_fork_at_version_equals_source_as_of(
        bytes in prop::collection::vec(any::<u8>(), 1..15),
        fork_point in 0usize..15,
    ) {
        fork_at_version_equals_source_as_of::<JsonTemporal>(&bytes, fork_point)?;
    }

    #[test]
    fn graph_fork_at_version_equals_source_as_of(
        bytes in prop::collection::vec(any::<u8>(), 1..15),
        fork_point in 0usize..15,
    ) {
        fork_at_version_equals_source_as_of::<GraphTemporal>(&bytes, fork_point)?;
    }
}
