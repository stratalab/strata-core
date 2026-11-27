//! A graph's adjacency index reads its nodes and edges at one version
//! (#3758).
//!
//! `adjacency_index` promises "one consistent read" of the graph's nodes and
//! edges, and the analytics and traversal verbs run on it. These tests commit
//! a node together with an edge into it while the build is between its node
//! scan and its edge scan, and hold the build to that promise: it sees the
//! node and the edge or neither, and never reports a healthy database as
//! `data_loss.engine.graph_index`.
//!
//! They live in the crate, not under `tests/`, so the test-only scan hook
//! they pause on is built under default features: the mutation lane
//! compiles and runs them.

use std::sync::mpsc;
use std::time::Duration;

use crate::{
    BranchName, CacheOpenOptions, Database, DurableLocalOpenOptions, GraphAdjacencyIndex,
    GraphAnalyticsBudget, GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId,
    ProductSpace,
};

fn branch() -> BranchName {
    BranchName::new("default").expect("valid branch name")
}

fn space() -> ProductSpace {
    ProductSpace::new("default").expect("valid product space")
}

fn graph_name() -> GraphName {
    GraphName::new("deps").expect("valid graph name")
}

fn node(id: &str) -> GraphNodeId {
    GraphNodeId::new(id).expect("valid node id")
}

fn edge_type() -> GraphEdgeType {
    GraphEdgeType::new("uses").expect("valid edge type")
}

fn open_cache_database() -> Database {
    Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database()
}

fn add_node(database: &Database, id: &str) {
    database
        .graph(branch(), space())
        .expect("graph service opens")
        .upsert_node(&graph_name(), node(id), GraphNodeData::default())
        .expect("the node commits");
}

fn add_edge(database: &Database, src: &str, dst: &str) {
    database
        .graph(branch(), space())
        .expect("graph service opens")
        .upsert_edge(
            &graph_name(),
            node(src),
            edge_type(),
            node(dst),
            GraphEdgeData::default(),
        )
        .expect("the edge commits");
}

fn adjacency(database: &Database) -> GraphAdjacencyIndex {
    database
        .graph(branch(), space())
        .expect("graph service opens")
        .adjacency_index(&graph_name(), &GraphAnalyticsBudget::default())
        .expect("a healthy graph builds its adjacency index")
}

/// Whether the index holds an edge from `src` to `dst`.
fn has_edge(index: &GraphAdjacencyIndex, src: &str, dst: &str) -> bool {
    let (Some(src), Some(dst)) = (index.node_index(&node(src)), index.node_index(&node(dst)))
    else {
        return false;
    };
    index
        .outgoing(src)
        .iter()
        .any(|edge| edge.neighbor() == dst)
}

/// Arms a pause at the next scan: the scanning thread signals and then waits
/// to be resumed. Returns the pause signal and the resume handle.
fn pause_at_next_scan(database: &Database) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    pause_at_scan(database, 0)
}

/// Arms a pause at the scan that follows `skip` further scans.
fn pause_at_scan(database: &Database, skip: usize) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    database.on_scan_for_test(skip, move || {
        paused_tx.send(()).expect("the test waits for the pause");
        resume_rx.recv().expect("the test resumes the build");
    });
    (paused_rx, resume_tx)
}

/// Holds the build at each of its two scans and commits at each pause: a
/// lone node at the first, then a node and an edge into it at the second,
/// so the second commit lands between the node scan and the edge scan. The
/// build must succeed and see the edge's endpoint exactly when it sees the
/// edge.
fn build_reads_one_version_across_concurrent_commits(database: &Database) {
    database
        .graph(branch(), space())
        .expect("graph service opens")
        .create_graph(graph_name())
        .expect("the graph commits");
    add_node(database, "m");

    let first_pause = pause_at_next_scan(database);
    let index = std::thread::scope(|scope| {
        let reader = scope.spawn(|| adjacency(database));
        let (paused, resume) = first_pause;
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the build pauses at its node scan");
        add_node(database, "early");
        // Armed only now: a hook fires on whichever thread scans next, and
        // the commit above may scan too.
        let (paused_again, resume_again) = pause_at_next_scan(database);
        resume.send(()).expect("the build is waiting");
        paused_again
            .recv_timeout(Duration::from_secs(30))
            .expect("the build pauses at its edge scan");
        add_node(database, "n");
        add_edge(database, "m", "n");
        resume_again.send(()).expect("the build is waiting");
        reader.join().expect("the reader thread")
    });

    assert_eq!(
        index.node_index(&node("n")).is_some(),
        has_edge(&index, "m", "n"),
        "the build sees the node and the edge into it, or neither"
    );
    assert_eq!(
        index.node_ids(),
        &[node("m")],
        "the build reads the graph as it was when the build began"
    );
    // Direction control: a build that begins after the commits sees them.
    let fresh = adjacency(database);
    assert!(fresh.node_index(&node("early")).is_some());
    assert!(has_edge(&fresh, "m", "n"));
}

#[test]
fn test_graph_adjacency_reads_one_version_across_concurrent_commits_in_cache_mode() {
    build_reads_one_version_across_concurrent_commits(&open_cache_database());
}

/// The other interleaving: a node and the edge into it, deleted in one
/// commit while the build is between its node scan and its edge scan. A
/// build that read the edges after the delete would keep the node and lose
/// its edge, a graph no version ever held. The build must keep both.
fn build_keeps_a_node_and_edge_deleted_mid_build(database: &Database) {
    database
        .graph(branch(), space())
        .expect("graph service opens")
        .create_graph(graph_name())
        .expect("the graph commits");
    add_node(database, "m");
    add_node(database, "n");
    add_edge(database, "m", "n");

    // The node scan passes; the build pauses at its edge scan. Nothing else
    // scans until the pause fires: the main thread only waits.
    let (paused, resume) = pause_at_scan(database, 1);
    let index = std::thread::scope(|scope| {
        let reader = scope.spawn(|| adjacency(database));
        paused
            .recv_timeout(Duration::from_secs(30))
            .expect("the build pauses at its edge scan");
        // Removes the node and every edge on it in one commit.
        database
            .graph(branch(), space())
            .expect("graph service opens")
            .delete_node(&graph_name(), &node("n"))
            .expect("the delete commits");
        resume.send(()).expect("the build is waiting");
        reader.join().expect("the reader thread")
    });

    assert!(
        index.node_index(&node("n")).is_some(),
        "the node the build began with"
    );
    assert!(
        has_edge(&index, "m", "n"),
        "the build keeps the edge into a node it kept"
    );
    // Direction control: a build that begins after the delete loses both.
    let fresh = adjacency(database);
    assert!(fresh.node_index(&node("n")).is_none());
    assert!(!has_edge(&fresh, "m", "n"));
}

#[test]
fn test_graph_adjacency_keeps_a_node_and_edge_deleted_mid_build() {
    build_keeps_a_node_and_edge_deleted_mid_build(&open_cache_database());
}

#[test]
fn test_graph_adjacency_reads_one_version_across_concurrent_commits_in_durable_mode() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = Database::open_local(tempdir.path(), DurableLocalOpenOptions::new())
        .expect("durable open succeeds")
        .into_database();
    build_reads_one_version_across_concurrent_commits(&database);
}
