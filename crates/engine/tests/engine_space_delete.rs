//! #3574: a space that bounded writes populated must be deletable through
//! the public API, whatever its size — deletion may span several commits,
//! the space is unregistered at the first, and the rows are gone at the
//! last. The sibling contract of #3477 for graphs. The resume paths (a mark
//! left by a crash finished by the next delete, create, or write of the
//! name) need the engine's test seam and are pinned in-crate in
//! `api/space.rs`, where the default-feature mutation lane can see them.

mod common;

use strata_core::CommitVersion;
use strata_engine::{
    Database, EventPayload, EventType, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData,
    GraphNodeId, JsonDocumentId, JsonValue, KvKey, KvValue, ProductSpace, VectorCollectionName,
    VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
};

use common::{branch, open_cache_database, open_durable_database, space};

/// Rows tombstoned per sweep commit: the chunk `GraphService::DELETE_CHUNK_ROWS`
/// shares with space deletion, half the storage commit budget.
const CHUNK: usize = 2_048;

fn run_database_modes(exercise: fn(Database)) {
    exercise(open_cache_database().expect("cache open succeeds"));

    let tempdir = tempfile::tempdir().expect("tempdir");
    exercise(open_durable_database(tempdir.path()).expect("durable open succeeds"));
}

fn key(index: usize) -> KvKey {
    KvKey::new(format!("k{index:06}")).expect("key")
}

/// Writes `count` KV keys into `target` in commits under the storage budget.
fn fill_kv(database: &mut Database, target: &ProductSpace, count: usize) {
    for start in (0..count).step_by(1_000) {
        let entries = (start..(start + 1_000).min(count))
            .map(|index| (key(index), KvValue::new(b"v".to_vec())));
        database
            .kv(branch("default"), target.clone())
            .expect("kv service")
            .put_batch(entries)
            .expect("bounded put");
    }
}

/// The issue's shape, widened to every capability: KV, JSON and a graph in
/// one space, each written in bounded commits.
fn fill_every_capability(database: &mut Database, target: &ProductSpace) -> u64 {
    fill_kv(database, target, 5_000);
    let mut json = database
        .json(branch("default"), target.clone())
        .expect("json service");
    for index in 0..200 {
        json.create(
            JsonDocumentId::new(format!("doc:{index}")).expect("doc id"),
            JsonValue::new(serde_json::json!({"n": index})).expect("value"),
        )
        .expect("json create");
    }
    let mut graph = database
        .graph(branch("default"), target.clone())
        .expect("graph service");
    let roads = GraphName::new("roads").expect("graph");
    graph.create_graph(roads.clone()).expect("graph created");
    let nodes: Vec<_> = (0..200)
        .map(|index| {
            (
                GraphNodeId::new(format!("n:{index}")).expect("node"),
                GraphNodeData::default(),
            )
        })
        .collect();
    let next = GraphEdgeType::new("next").expect("type");
    let edges: Vec<_> = (0..200)
        .map(|index| {
            (
                GraphNodeId::new(format!("n:{index}")).expect("node"),
                next.clone(),
                GraphNodeId::new(format!("n:{}", (index + 1) % 200)).expect("node"),
                GraphEdgeData::default(),
            )
        })
        .collect();
    graph
        .bulk_insert(&roads, &nodes, &edges, Some(128))
        .expect("graph bulk import");
    // KV + JSON + graph metadata + nodes + edges + reverse edges.
    5_000 + 200 + 1 + 200 + 200 + 200
}

fn latest_version(database: &mut Database) -> CommitVersion {
    // A no-op registration of the default space commits nothing, so read the
    // head through a throwaway write's neighbour: the last visible KV commit.
    database
        .kv(branch("default"), space("default"))
        .expect("kv service")
        .put(
            KvKey::new("clock").expect("key"),
            KvValue::new(b"t".to_vec()),
        )
        .expect("clock write")
        .commit()
        .version()
}

fn assert_space_gone(database: &mut Database, target: &ProductSpace) {
    let mut spaces = database.spaces(branch("default")).expect("space service");
    assert!(
        !spaces.exists(target).expect("exists reads"),
        "unregistered"
    );
    let listed = spaces.list().expect("list reads");
    assert!(!listed.contains(target), "not listed");
    assert!(
        listed.contains(&space("default")),
        "the default space survives every deletion"
    );
    let usage = spaces.usage(target).expect("usage reads");
    assert_eq!(
        (
            usage.kv_count(),
            usage.json_count(),
            usage.graph_count(),
            usage.graph_node_count(),
            usage.graph_edge_count()
        ),
        (0, 0, 0, 0, 0),
        "no row of the space remains"
    );
    assert_eq!(
        database
            .kv(branch("default"), target.clone())
            .expect("kv service")
            .count(None)
            .expect("count"),
        0
    );
    assert!(database
        .graph(branch("default"), target.clone())
        .expect("graph service")
        .list_graphs(None, 10)
        .expect("graphs list")
        .graphs()
        .is_empty());
}

#[test]
fn a_space_larger_than_one_commit_can_be_force_deleted_in_cache_and_durable_modes() {
    run_database_modes(exercise_large_delete);
}

/// The issue's case: far more rows than one commit admits, force-deleted
/// through the public API. The rows go in as many commits as the chunk
/// needs, plus the mark before them and the catalog tombstone after; every
/// registration-based view says the space is gone; every row is gone; and a
/// read pinned before the deletion still sees the space as it was.
fn exercise_large_delete(mut database: Database) {
    let target = space("bulk");
    database
        .spaces(branch("default"))
        .expect("space service")
        .create(target.clone())
        .expect("space create");
    let rows = fill_every_capability(&mut database, &target);
    let before = latest_version(&mut database);

    let outcome = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, true)
        .expect("a space that bounded writes could fill can be deleted");
    assert!(outcome.deleted());
    assert!(outcome.force());
    assert_eq!(outcome.deleted_rows(), rows, "every row is reported");
    let last = outcome.version().expect("the deletion commits");
    let chunks = u64::try_from(usize::try_from(rows).expect("fits").div_ceil(CHUNK)).expect("fits");
    assert_eq!(
        last.as_u64() - before.as_u64(),
        1 + chunks + 1,
        "the mark, one commit per chunk, then the catalog tombstone"
    );

    assert_space_gone(&mut database, &target);

    // MVCC-001: the version before the deletion still reads the space whole.
    let mut kv = database
        .kv(branch("default"), target.clone())
        .expect("kv service");
    assert!(kv
        .get_at_version(&key(4_999), before)
        .expect("historical read")
        .is_some());
    assert!(kv.get(&key(4_999)).expect("latest read").is_none());
}

#[test]
fn a_space_with_events_and_vectors_deletes_via_the_large_path() {
    run_database_modes(exercise_events_and_vectors_delete);
}

/// The large path must sweep event and vector rows too, not only the KV, JSON
/// and graph rows `exercise_large_delete` fills. The reorder tombstones the
/// vector-collection row among the first, and an event log carries its metadata
/// and per-event type index alongside the events. Enough KV rows to force the
/// chunked path, then an event log and a vector collection on top; after the
/// force-delete every one is gone, and the collection reads back absent, not
/// corrupt.
fn exercise_events_and_vectors_delete(mut database: Database) {
    let target = space("mixed");
    database
        .spaces(branch("default"))
        .expect("space service")
        .create(target.clone())
        .expect("space create");
    fill_kv(&mut database, &target, CHUNK + 100);
    {
        let mut events = database
            .event(branch("default"), target.clone())
            .expect("event service");
        for index in 0..50 {
            events
                .append(
                    EventType::new("tick").expect("event type"),
                    EventPayload::new(serde_json::json!({ "n": index })).expect("payload"),
                )
                .expect("event append");
        }
    }
    let docs = VectorCollectionName::new("docs").expect("collection");
    {
        let mut vectors = database
            .vector(branch("default"), target.clone())
            .expect("vector service");
        vectors
            .create_collection(
                docs.clone(),
                VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("config"),
            )
            .expect("collection create");
        for index in 0..10 {
            vectors
                .upsert(
                    docs.clone(),
                    VectorKey::new(format!("v{index}")).expect("key"),
                    VectorEmbedding::new([1.0_f32, 0.0]).expect("embedding"),
                    None,
                )
                .expect("upsert");
        }
    }

    let outcome = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, true)
        .expect("a mixed-capability space deletes");
    assert!(outcome.deleted());
    assert!(outcome.force());
    assert!(
        outcome.deleted_rows() > u64::try_from(CHUNK).expect("fits"),
        "the chunked path swept KV, events and vectors together"
    );

    assert_space_gone(&mut database, &target);
    let usage = database
        .spaces(branch("default"))
        .expect("space service")
        .usage(&target)
        .expect("usage reads");
    assert_eq!(
        (
            usage.event_count(),
            usage.vector_collection_count(),
            usage.vector_entry_count()
        ),
        (0, 0, 0),
        "no event or vector row remains"
    );
    assert!(database
        .event(branch("default"), target.clone())
        .expect("event service")
        .list(None, None, None)
        .expect("event list reads")
        .is_empty());
    let error = database
        .vector(branch("default"), target)
        .expect("vector service")
        .get(&docs, &VectorKey::new("v0").expect("key"))
        .expect_err("the swept collection reads back absent, not corrupt");
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

#[test]
fn a_space_that_fits_the_chunk_still_deletes_in_one_commit() {
    run_database_modes(exercise_single_commit);
}

/// Below the chunk the catalog and the rows go together in the single
/// commit they always did; at the chunk the deletion takes the mark, one
/// chunk, and the tombstone.
fn exercise_single_commit(mut database: Database) {
    for (label, rows, expected_commits) in [
        ("under-the-chunk", CHUNK - 1, 1),
        ("at-the-chunk", CHUNK, 3),
    ] {
        let target = space(label);
        database
            .spaces(branch("default"))
            .expect("space service")
            .create(target.clone())
            .expect("space create");
        fill_kv(&mut database, &target, rows);
        let before = latest_version(&mut database);
        let outcome = database
            .spaces(branch("default"))
            .expect("space service")
            .delete(&target, true)
            .expect("delete succeeds");
        assert_eq!(
            outcome.deleted_rows(),
            u64::try_from(rows).expect("fits"),
            "{label}"
        );
        assert_eq!(
            outcome.version().expect("commits").as_u64() - before.as_u64(),
            expected_commits,
            "{label}: commit count"
        );
        assert_space_gone(&mut database, &target);
    }
}

#[test]
fn an_empty_space_and_a_missing_space_delete_as_before() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let target = space("hollow");
    database
        .spaces(branch("default"))
        .expect("space service")
        .create(target.clone())
        .expect("space create");
    let before = latest_version(&mut database);
    let outcome = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, false)
        .expect("an empty space deletes without force");
    assert!(outcome.deleted());
    assert_eq!(outcome.deleted_rows(), 0);
    assert_eq!(
        outcome.version().expect("commits").as_u64() - before.as_u64(),
        1
    );

    let missing = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, true)
        .expect("a missing space is not an error");
    assert!(!missing.deleted());
    assert!(missing.commit().is_none());
}

#[test]
fn a_populated_space_still_refuses_deletion_without_force() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let target = space("keep");
    fill_kv(&mut database, &target, CHUNK + 10);
    let error = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, false)
        .expect_err("a populated space refuses without force, whatever its size");
    assert_eq!(error.code(), "failed_precondition.engine.space_not_empty");
    assert!(database
        .spaces(branch("default"))
        .expect("space service")
        .exists(&target)
        .expect("exists reads"));
    assert_eq!(
        database
            .kv(branch("default"), target)
            .expect("kv service")
            .count(None)
            .expect("count"),
        u64::try_from(CHUNK + 10).expect("fits")
    );
}
