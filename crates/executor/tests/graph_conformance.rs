//! TCP4.4d — LDBC Graphalytics reference-output conformance for the six
//! graph analytics kernels.
//!
//! The vendored validation graphs (see `conformance/graphalytics/README.md`)
//! carry the official reference outputs the LDBC framework uses to validate
//! platform implementations. Strata ships exactly the six Graphalytics
//! kernels — BFS, `PageRank`, WCC, CDLP, LCC, SSSP — all hand-rolled, so this
//! is their first external correctness oracle.
//!
//! Mapping decisions (each verified or adjudicated in this file):
//! - Nodes are inserted in ascending numeric id order, so the engine's
//!   index order coincides with id order: WCC's canonical-smallest-index
//!   representative becomes the spec's smallest-member id, and CDLP's
//!   index-initialized labels are order-isomorphic to id-initialized ones.
//! - Undirected inputs list each edge in both directions; the loader
//!   dedupes to one stored edge (src < dst) and kernels run with
//!   `GraphDirection::Both`, which counts each undirected neighbor once.
//!   Directed inputs load as-is and traverse `Outgoing` (BFS/SSSP) or
//!   `Both` (CDLP, per the spec's in-plus-out neighbor multiset).
//! - Unreachable vertices: the reference prints `9223372036854775807`
//!   (BFS) / `Infinity` (SSSP); Strata omits them from the result map.
//! - `PageRank` runs the reference parameters (0.85; 14 directed / 26
//!   undirected iterations) with a tolerance far below any step delta so
//!   the iteration counts match; scores compare at the reference's own
//!   relative epsilon (1e-4). SSSP compares at the same epsilon.
//! - LCC: Strata's documented contract is the coefficient over the
//!   UNDIRECTED view. The undirected reference must therefore match
//!   exactly; the directed reference uses the spec's directed definition
//!   (ordered neighbor pairs), a deliberately different function —
//!   `lcc_directed_reference_uses_a_different_definition` asserts the
//!   divergence so the difference stays visible rather than assumed.
//! - CDLP: synchronous propagation per the spec (#3024, fixed) — both
//!   variants assert the LDBC reference outputs directly.

use std::collections::BTreeMap;
use std::path::PathBuf;

use strata_executor::{Command, Executor, GraphDirection, Output};

const EPSILON: f64 = 1e-4;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/graphalytics")
}

fn read(name: &str) -> String {
    std::fs::read_to_string(data_dir().join(name))
        .unwrap_or_else(|err| panic!("vendored file {name}: {err}"))
}

/// Adjacency-list input: `vertex neighbor…` per line, one line per vertex.
/// Returns sorted vertex ids and directed edge pairs.
fn parse_adjacency(name: &str) -> (Vec<u64>, Vec<(u64, u64)>) {
    let mut vertices = std::collections::BTreeSet::new();
    let mut edges = Vec::new();
    for line in read(name).lines() {
        let mut fields = line.split_whitespace();
        let Some(vertex) = fields.next() else {
            continue;
        };
        let vertex: u64 = vertex.parse().expect("vertex id");
        vertices.insert(vertex);
        for neighbor in fields {
            let neighbor: u64 = neighbor.parse().expect("neighbor id");
            // Neighbor-only vertices (no line of their own) still exist.
            vertices.insert(neighbor);
            edges.push((vertex, neighbor));
        }
    }
    (vertices.into_iter().collect(), edges)
}

/// Reference output: `vertex value` per line.
fn parse_output(name: &str) -> BTreeMap<u64, String> {
    read(name)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split_whitespace();
            let vertex: u64 = fields.next().expect("vertex").parse().expect("vertex id");
            let value = fields.next().expect("value").to_owned();
            (vertex, value)
        })
        .collect()
}

/// Loads a graph at the wire: ascending-id node inserts, then edges,
/// exactly as listed (undirected inputs list each edge in both directions,
/// and BOTH are stored — `PageRank` flows rank along stored edges only, so
/// the undirected view needs the reverse edges materialized; the other
/// kernels then traverse `Outgoing` so each undirected neighbor counts
/// once).
fn load_graph(
    executor: &mut Executor,
    graph: &str,
    vertices: &[u64],
    edges: &[(u64, u64)],
    weights: Option<&BTreeMap<(u64, u64), f64>>,
) {
    executor
        .execute(Command::GraphCreate {
            branch: None,
            space: None,
            graph: graph.to_owned(),
        })
        .expect("graph creates");
    for vertex in vertices {
        executor
            .execute(Command::GraphAddNode {
                branch: None,
                space: None,
                graph: graph.to_owned(),
                node_id: vertex.to_string(),
                properties: None,
                binding: None,
                object_type: None,
            })
            .expect("node adds");
    }
    for (src, dst) in edges {
        let weight = weights.map(|map| map[&(*src, *dst)]);
        executor
            .execute(Command::GraphAddEdge {
                branch: None,
                space: None,
                graph: graph.to_owned(),
                src: src.to_string(),
                edge_type: "e".to_owned(),
                dst: dst.to_string(),
                weight,
                properties: None,
            })
            .expect("edge adds");
    }
}

fn assert_f64_map(
    kernel: &str,
    observed: &BTreeMap<String, f64>,
    reference: &BTreeMap<u64, String>,
    epsilon: f64,
) {
    for (vertex, expected) in reference {
        let expected: f64 = expected.parse().expect("reference value");
        let observed_value = observed
            .get(&vertex.to_string())
            .unwrap_or_else(|| panic!("{kernel}: vertex {vertex} missing from the result"));
        let tolerance = epsilon * expected.abs().max(f64::MIN_POSITIVE);
        assert!(
            (observed_value - expected).abs() <= tolerance,
            "{kernel}: vertex {vertex} diverges from the LDBC reference \
             (observed {observed_value}, reference {expected})"
        );
    }
    assert_eq!(
        observed.len(),
        reference.len(),
        "{kernel}: result size diverges from the reference"
    );
}

#[test]
fn bfs_matches_the_ldbc_reference() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for variant in ["dir", "undir"] {
        let direction = GraphDirection::Outgoing;
        let graph = format!("bfs-{variant}");
        let (vertices, edges) = parse_adjacency(&format!("bfs_{variant}-input"));
        load_graph(&mut executor, &graph, &vertices, &edges, None);
        let Output::GraphBfsResult(result) = executor
            .execute(Command::GraphBfs {
                branch: None,
                space: None,
                graph,
                start: "1".to_owned(),
                max_depth: None,
                max_nodes: None,
                edge_types: None,
                direction: Some(direction),
                budget: None,
                as_of: None,
                as_of_time: None,
            })
            .expect("bfs runs")
        else {
            panic!("unexpected bfs output");
        };
        let reference = parse_output(&format!("bfs_{variant}-output"));
        for (vertex, expected) in &reference {
            let key = vertex.to_string();
            if expected == "9223372036854775807" {
                assert!(
                    !result.depths().contains_key(&key),
                    "bfs {variant}: unreachable vertex {vertex} appears in the result"
                );
            } else {
                let expected: u64 = expected.parse().expect("depth");
                assert_eq!(
                    result.depths().get(&key),
                    Some(&expected),
                    "bfs {variant}: vertex {vertex} depth diverges from the LDBC reference"
                );
            }
        }
    }
}

#[test]
fn wcc_matches_the_ldbc_reference() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for variant in ["dir", "undir"] {
        let graph = format!("wcc-{variant}");
        let (vertices, edges) = parse_adjacency(&format!("wcc_{variant}-input"));
        load_graph(&mut executor, &graph, &vertices, &edges, None);
        let Output::GraphWccResult(result) = executor
            .execute(Command::GraphWcc {
                branch: None,
                space: None,
                graph,
                budget: None,
                as_of: None,
                as_of_time: None,
            })
            .expect("wcc runs")
        else {
            panic!("unexpected wcc output");
        };
        let reference = parse_output(&format!("wcc_{variant}-output"));
        for (vertex, expected) in &reference {
            assert_eq!(
                result.components().get(&vertex.to_string()),
                Some(expected),
                "wcc {variant}: vertex {vertex} component diverges from the LDBC reference"
            );
        }
        assert_eq!(result.components().len(), reference.len());
    }
}

#[test]
fn sssp_matches_the_ldbc_reference() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for variant in ["dir", "undir"] {
        let direction = GraphDirection::Outgoing;
        let graph = format!("sssp-{variant}");
        let vertices: Vec<u64> = read(&format!("sssp_{variant}-input.v"))
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.trim().parse().expect("vertex id"))
            .collect();
        let mut edges = Vec::new();
        let mut weights = BTreeMap::new();
        for line in read(&format!("sssp_{variant}-input.e")).lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.is_empty() {
                continue;
            }
            let (src, dst): (u64, u64) = (
                fields[0].parse().expect("src"),
                fields[1].parse().expect("dst"),
            );
            let weight: f64 = fields[2].parse().expect("weight");
            edges.push((src, dst));
            weights.insert((src, dst), weight);
            if variant == "undir" && src != dst {
                edges.push((dst, src));
                weights.entry((dst, src)).or_insert(weight);
            }
        }
        let mut sorted = vertices.clone();
        sorted.sort_unstable();
        load_graph(&mut executor, &graph, &sorted, &edges, Some(&weights));
        let Output::GraphSsspResult(result) = executor
            .execute(Command::GraphSssp {
                branch: None,
                space: None,
                graph,
                source: "1".to_owned(),
                direction: Some(direction),
                edge_types: None,
                budget: None,
                as_of: None,
                as_of_time: None,
            })
            .expect("sssp runs")
        else {
            panic!("unexpected sssp output");
        };
        let reference = parse_output(&format!("sssp_{variant}-output"));
        for (vertex, expected) in &reference {
            let key = vertex.to_string();
            if expected == "Infinity" {
                assert!(
                    !result.distances().contains_key(&key),
                    "sssp {variant}: unreachable vertex {vertex} appears in the result"
                );
            } else {
                let expected: f64 = expected.parse().expect("distance");
                let observed = result
                    .distances()
                    .get(&key)
                    .unwrap_or_else(|| panic!("sssp {variant}: vertex {vertex} missing"));
                assert!(
                    (observed - expected).abs() <= EPSILON * expected.abs().max(f64::MIN_POSITIVE),
                    "sssp {variant}: vertex {vertex} diverges (observed {observed}, \
                     reference {expected})"
                );
            }
        }
    }
}

#[test]
fn pagerank_matches_the_ldbc_reference() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for (variant, iterations) in [("dir", 14u64), ("undir", 26u64)] {
        let graph = format!("pr-{variant}");
        let (vertices, edges) = parse_adjacency(&format!("pr_{variant}-input"));
        load_graph(&mut executor, &graph, &vertices, &edges, None);
        let Output::GraphPagerankResult(result) = executor
            .execute(Command::GraphPagerank {
                branch: None,
                space: None,
                graph,
                damping: Some(0.85),
                max_iterations: Some(iterations),
                // Far below any step delta: the run must complete all
                // reference iterations rather than converging early.
                tolerance: Some(1e-15),
                personalization: None,
                budget: None,
                as_of: None,
                as_of_time: None,
            })
            .expect("pagerank runs")
        else {
            panic!("unexpected pagerank output");
        };
        assert_eq!(
            result.iterations(),
            iterations,
            "pagerank {variant}: early convergence would desync from the reference"
        );
        let reference = parse_output(&format!("pr_{variant}-output"));
        assert_f64_map(
            &format!("pagerank {variant}"),
            result.ranks(),
            &reference,
            EPSILON,
        );
    }
}

#[test]
fn cdlp_matches_the_ldbc_reference() {
    // Promoted from the #3024 gate-7 pin by the synchronous-propagation
    // fix: CDLP now matches the LDBC reference outputs on both variants.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for variant in ["dir", "undir"] {
        let graph = format!("cdlp-{variant}");
        let (vertices, edges) = parse_adjacency(&format!("cdlp_{variant}-input"));
        load_graph(&mut executor, &graph, &vertices, &edges, None);
        let direction = if variant == "dir" {
            GraphDirection::Both
        } else {
            GraphDirection::Outgoing
        };
        let Output::GraphCdlpResult(result) = executor
            .execute(Command::GraphCdlp {
                branch: None,
                space: None,
                graph,
                max_iterations: Some(5),
                direction: Some(direction),
                budget: None,
                as_of: None,
                as_of_time: None,
            })
            .expect("cdlp runs")
        else {
            panic!("unexpected cdlp output");
        };
        let reference = parse_output(&format!("cdlp_{variant}-output"));
        for (vertex, expected) in &reference {
            assert_eq!(
                result.labels().get(&vertex.to_string()),
                Some(expected),
                "cdlp {variant}: vertex {vertex} label diverges from the LDBC reference"
            );
        }
        assert_eq!(result.labels().len(), reference.len());
    }
}

#[test]
fn lcc_matches_the_ldbc_reference_on_the_undirected_graph() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (vertices, edges) = parse_adjacency("lcc_undir-input");
    load_graph(&mut executor, "lcc-undir", &vertices, &edges, None);
    let Output::GraphLccResult(result) = executor
        .execute(Command::GraphLcc {
            branch: None,
            space: None,
            graph: "lcc-undir".to_owned(),
            budget: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("lcc runs")
    else {
        panic!("unexpected lcc output");
    };
    let reference = parse_output("lcc_undir-output");
    assert_f64_map("lcc undir", result.coefficients(), &reference, EPSILON);
}

#[test]
fn lcc_directed_reference_uses_a_different_definition() {
    // Strata's LCC contract is the coefficient over the UNDIRECTED view
    // (documented on `GraphAdjacencyIndex::lcc`); the LDBC directed LCC
    // counts ordered neighbor pairs — a deliberately different function.
    // This assert keeps the difference visible: if the two ever agree on
    // the directed validation graph, the distinction has collapsed and the
    // directed variant belongs in the conformance matrix above.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (vertices, edges) = parse_adjacency("lcc_dir-input");
    load_graph(&mut executor, "lcc-dir", &vertices, &edges, None);
    let Output::GraphLccResult(result) = executor
        .execute(Command::GraphLcc {
            branch: None,
            space: None,
            graph: "lcc-dir".to_owned(),
            budget: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("lcc runs")
    else {
        panic!("unexpected lcc output");
    };
    let reference = parse_output("lcc_dir-output");
    let diverging = reference
        .iter()
        .filter(|(vertex, expected)| {
            let expected: f64 = expected.parse().expect("reference value");
            result
                .coefficients()
                .get(&vertex.to_string())
                .is_none_or(|observed| (observed - expected).abs() > EPSILON * expected.max(1e-9))
        })
        .count();
    assert!(
        diverging > 0,
        "directed LCC now matches the LDBC directed definition — move it into \
         the conformance matrix"
    );
}
