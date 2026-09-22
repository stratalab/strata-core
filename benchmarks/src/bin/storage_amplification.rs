//! Storage-amplification benchmark (#3492 / #3499).
//!
//! Measures on-disk physical bytes vs logical payload bytes per data
//! primitive, settled to steady state, with a per-component breakdown
//! (WAL / tables / snapshots). This is the "before/after" proof harness for
//! block compression: run it on `main` (uncompressed) for the baseline, then
//! again with compression enabled to show the reduction.
//!
//! The amplification the graph app surfaced (#3492) is dominated by the tiny
//! per-record payload against the fixed row envelope, so KV (one row/key,
//! lowest multiplier) and Graph (authored + derived rows, highest multiplier)
//! bracket the spectrum. JSON / vector / event extend the same harness.
//!
//! Usage:
//!   storage-amplification --primitive all --fill real --settle-secs 60
//!   storage-amplification --primitive graph --nodes 12862 --edges 28802

use std::path::Path;
use std::time::{Duration, Instant};

use strata_engine::{
    BranchName, Database, DurableLocalOpenOptions, GraphEdgeData, GraphEdgeType, GraphName,
    GraphNodeData, GraphNodeId, GraphProperties, KvKey, KvValue, ProductSpace, VersionRetention,
};

#[allow(dead_code)]
#[path = "../../benches/ycsb_workloads.rs"]
mod ycsb_workloads;
use ycsb_workloads::dir_size_bytes;

const SETTLE_PROBE_SECS: u64 = 3;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fill {
    /// Structured, repetitive payloads — what a compressor can shrink.
    Real,
    /// Incompressible random payloads — the envelope floor a compressor cannot touch.
    Random,
}

struct Components {
    total: u64,
    wal: u64,
    tables: u64,
    snapshots: u64,
}

fn component_sizes(path: &Path) -> Components {
    let sub = |name: &str| {
        let p = path.join(name);
        if p.exists() {
            dir_size_bytes(&p)
        } else {
            0
        }
    };
    Components {
        total: dir_size_bytes(path),
        wal: sub("wal"),
        tables: sub("tables"),
        snapshots: sub("snapshots"),
    }
}

/// Let background maintenance (flush + compaction) reach steady state, then
/// return the settled component sizes. Stops early once the total stops
/// shrinking across two consecutive probes.
fn settle(path: &Path, settle_secs: u64) -> Components {
    let start = Instant::now();
    let mut prev = dir_size_bytes(path);
    let mut stable_probes = 0u32;
    while start.elapsed().as_secs() < settle_secs {
        std::thread::sleep(Duration::from_secs(SETTLE_PROBE_SECS));
        let now = dir_size_bytes(path);
        if now >= prev {
            stable_probes += 1;
            if stable_probes >= 2 {
                break;
            }
        } else {
            stable_probes = 0;
        }
        prev = now;
    }
    component_sizes(path)
}

fn fill_bytes(fill: Fill, seed: u64, len: usize) -> Vec<u8> {
    match fill {
        // A repeating structured pattern: highly compressible, like real keys/props.
        Fill::Real => (0..len).map(|i| b"strata-"[i % 7]).collect(),
        // splitmix64 stream: incompressible.
        Fill::Random => {
            let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            (0..len)
                .map(|_| {
                    x ^= x >> 30;
                    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                    x ^= x >> 27;
                    (x >> 24) as u8
                })
                .collect()
        }
    }
}

fn report(primitive: &str, fill: Fill, logical: u64, c: &Components, note: &str) {
    let amp = |phys: u64| {
        if logical == 0 {
            0.0
        } else {
            phys as f64 / logical as f64
        }
    };
    let fill_label = if fill == Fill::Real { "real" } else { "random" };
    println!(
        "{primitive:<8} fill={fill_label:<6} logical={:>10} total={:>10} ({:.2}x)  \
         tables={:>10} ({:.2}x)  wal={:>10}  snap={:>8}  {note}",
        logical,
        c.total,
        amp(c.total),
        c.tables,
        amp(c.tables),
        c.wal,
        c.snapshots,
    );
}

/// Percentage reduction from `before` to `after` (positive = shrank).
fn pct_drop(before: u64, after: u64) -> f64 {
    if before == 0 {
        0.0
    } else {
        (before as f64 - after as f64) / before as f64 * 100.0
    }
}

/// Runs `f` against a fresh throwaway database directory, returning its result.
fn with_dir<T>(f: impl FnOnce(&Path) -> T) -> T {
    let dir = tempfile::tempdir().expect("tempdir");
    f(dir.path())
}

/// Opens a durable database, optionally opting into MVCC version pruning
/// (#3502): `None` keeps every version (`KeepAll`, the control arm), `Some(w)`
/// retains versions newer than `visible - w` per key and prunes the rest during
/// compaction (the treatment arm that demonstrates the layer-2 disk win).
fn open_db(root: &Path, retention: Option<u64>) -> Database {
    let mut options = DurableLocalOpenOptions::new();
    if let Some(window) = retention {
        options = options.with_version_retention(VersionRetention::KeepRecentVersions { window });
    }
    Database::open_local(root, options)
        .expect("open localfs database")
        .into_database()
}

// --- KV: one physical row per key (control, lowest multiplier). ---
//
// `rewrites` is the layer-2 (MVCC version-retention) knob: after the initial
// load, every key is overwritten `rewrites` more times, so each key accrues
// `rewrites + 1` committed versions. `logical` stays the LIVE dataset size (one
// value per key) — the extra versions are pure amplification. Under `KeepAll`
// (`retention = None`) every version is retained; under `KeepRecentVersions`
// (`retention = Some(w)`) compaction prunes versions older than the window, so
// the physical size collapses toward the live set. Each key is flushed and
// compacted so the table path (not just the WAL) carries the versions.
fn bench_kv(
    root: &Path,
    fill: Fill,
    keys: usize,
    value_bytes: usize,
    rewrites: usize,
    retention: Option<u64>,
    settle_secs: u64,
) -> (u64, Components) {
    let mut database = open_db(root, retention);
    let branch: BranchName = database.default_branch().clone();
    let space = ProductSpace::new("amp").expect("space");

    let mut logical = 0u64;
    // One write-and-flush pass over the whole key set. `pass` seeds distinct
    // values so every overwrite is a genuinely new version, and the flush after
    // each pass lands that generation in its own L0 table.
    let mut write_pass = |database: &mut Database, pass: usize, count_logical: bool| {
        let mut kv = database
            .kv(branch.clone(), space.clone())
            .expect("kv service");
        let mut batch: Vec<(KvKey, KvValue)> = Vec::with_capacity(256);
        for i in 0..keys {
            let key = format!("key:{i:012}").into_bytes();
            let value = fill_bytes(fill, (i + pass * keys) as u64, value_bytes);
            if count_logical {
                logical += (key.len() + value.len()) as u64;
            }
            batch.push((KvKey::new(key).expect("key"), KvValue::new(value)));
            if batch.len() == 256 {
                kv.put_batch(std::mem::take(&mut batch))
                    .expect("kv put_batch");
            }
        }
        if !batch.is_empty() {
            kv.put_batch(batch).expect("kv put_batch");
        }
    };

    // Pass 0 is the live dataset (counts toward logical); passes 1..=rewrites
    // are overwrites that only add versions.
    for pass in 0..=rewrites {
        write_pass(&mut database, pass, pass == 0);
        force_flush(&mut database, &branch);
    }
    // Deterministically drive the pruning dispatch so the treatment arm's win
    // is measured, not raced against background pressure.
    force_compact(&mut database, &branch);
    let c = settle(root, settle_secs);
    (logical, c)
}

/// #3522: the (src, dst) node indices for edge submission `index`. The graph
/// dedups edges by (src, type, dst), so colliding endpoints collapse to one
/// live edge while the `logical` denominator counts every submission — which
/// deflated the reported graph amplification. Each submission must map to a
/// DISTINCT (src, dst) pair (no duplicate live edge), while keeping an
/// island-like shape (each node links to a few nearby neighbours).
fn graph_edge_endpoints(index: usize, node_count: usize) -> (usize, usize) {
    let n = node_count.max(1);
    let src = index % n;
    // `index / n` grows the neighbour offset each time `src` wraps, so
    // (src, dst) is unique for every submission below `n * (n - 1)` (the base-n
    // decomposition of `index` is a bijection): distinct live edges, one per
    // submission, still island-like (each node links to a few nearby nodes).
    let dst = (src + 1 + index / n) % n;
    (src, dst)
}

// --- Graph: authored node/edge rows + derived index rows (highest multiplier). ---
fn bench_graph(root: &Path, fill: Fill, node_count: usize, edge_count: usize, settle_secs: u64) {
    let mut database = open_db(root, None);
    let branch: BranchName = database.default_branch().clone();
    let space = ProductSpace::new("amp").expect("space");
    let mut graph = database
        .graph(branch.clone(), space)
        .expect("graph service");
    let graph_name = GraphName::new("city").expect("graph name");
    graph
        .create_graph(graph_name.clone())
        .expect("create graph");

    // Island-like shape: node id "n:<i>" with {x,y}; edge src->dst type "street"
    // weight + {name}. `logical` is the compact-JSON size of the dataset, the
    // same denominator #3492 measured against.
    let mut logical_json = serde_json::json!({ "nodes": [], "edges": [] });
    let nodes: Vec<(GraphNodeId, GraphNodeData)> = (0..node_count)
        .map(|i| {
            let id = format!("n:{i}");
            let x = (i * 37 % 100_000) as i64;
            let y = (i * 53 % 100_000) as i64;
            let props = serde_json::json!({ "x": x, "y": y });
            logical_json["nodes"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({ "id": id, "x": x, "y": y }));
            (
                GraphNodeId::new(id).expect("node id"),
                GraphNodeData::new(Some(GraphProperties::new(props).expect("node props")), None),
            )
        })
        .collect();
    let name_pool = [
        "West 106th Street",
        "Broadway",
        "Amsterdam Ave",
        "Columbus Ave",
    ];
    let edges: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> = (0..edge_count)
        .map(|i| {
            let (src_index, dst_index) = graph_edge_endpoints(i, node_count);
            let src = format!("n:{src_index}");
            let dst = format!("n:{dst_index}");
            let name = if fill == Fill::Real {
                name_pool[i % name_pool.len()].to_string()
            } else {
                String::from_utf8_lossy(&fill_bytes(fill, i as u64, 16)).into_owned()
            };
            let weight = (i % 500 + 1) as f64;
            let props = serde_json::json!({ "name": name });
            logical_json["edges"].as_array_mut().unwrap().push(
                serde_json::json!({ "src": src, "dst": dst, "length_m": weight, "name": name }),
            );
            (
                GraphNodeId::new(src).expect("src"),
                GraphEdgeType::new("street").expect("edge type"),
                GraphNodeId::new(dst).expect("dst"),
                GraphEdgeData::new(
                    weight,
                    Some(GraphProperties::new(props).expect("edge props")),
                )
                .expect("edge data"),
            )
        })
        .collect();
    let logical = serde_json::to_vec(&logical_json)
        .expect("logical json")
        .len() as u64;

    graph
        .bulk_insert(&graph_name, &nodes, &edges, Some(1024))
        .expect("graph bulk_insert");
    drop(graph);
    force_flush(&mut database, &branch);
    let c = settle(root, settle_secs);
    report("graph", fill, logical, &c, "");
}

/// Force the imported data out of the WAL into L0 tables so the table path is
/// actually exercised — small datasets never cross the flush threshold on
/// their own, so without this the benchmark measures only WAL amplification.
fn force_flush(database: &mut Database, branch: &BranchName) {
    for _ in 0..8 {
        match database.flush_storage_branch_for_test(branch) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("[warn] flush failed: {e:?}");
                break;
            }
        }
    }
}

/// Deterministically drive the pruning compaction dispatch (#3502) so the
/// treatment arm prunes at a known point instead of racing background pressure.
/// A no-op for the `KeepAll` control (compaction runs, drops nothing).
fn force_compact(database: &mut Database, branch: &BranchName) {
    if let Err(e) = database.force_storage_branch_compaction_for_test(branch) {
        eprintln!("[warn] forced compaction failed: {e:?}");
    }
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let primitive = arg_value(&args, "--primitive").unwrap_or_else(|| "all".to_string());
    let fill = match arg_value(&args, "--fill").as_deref() {
        Some("random") => Fill::Random,
        _ => Fill::Real,
    };
    let settle_secs = arg_value(&args, "--settle-secs")
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let keys = arg_value(&args, "--keys")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let value_bytes = arg_value(&args, "--value-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let nodes = arg_value(&args, "--nodes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(12_862);
    let edges = arg_value(&args, "--edges")
        .and_then(|s| s.parse().ok())
        .unwrap_or(28_802);
    // #3502 Slice E layer-2 knobs: `--rewrites R` overwrites every key R more
    // times to build MVCC version churn; `--retention-window W` runs a second
    // (treatment) KV arm with pruning opted in, so the A/B shows the disk win.
    let rewrites = arg_value(&args, "--rewrites")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let retention_window: Option<u64> =
        arg_value(&args, "--retention-window").and_then(|s| s.parse().ok());

    println!(
        "# storage-amplification  fill={}  settle_secs={}  rewrites={}  retention_window={}",
        if fill == Fill::Real { "real" } else { "random" },
        settle_secs,
        rewrites,
        retention_window.map_or_else(|| "none".to_string(), |w| w.to_string()),
    );
    println!(
        "{:<8} {:<11} {:>18} {:>18} {:>18} {:>13} {:>13}",
        "primitive", "fill", "logical", "total(amp)", "tables(amp)", "wal", "snapshots"
    );

    if primitive == "kv" || primitive == "all" {
        match retention_window {
            // Layer-2 A/B: identical re-write-heavy workload under KeepAll
            // (control) vs KeepRecentVersions{window} (treatment).
            Some(window) if rewrites > 0 => {
                let (_, ctl) = with_dir(|p| {
                    let (logical, c) =
                        bench_kv(p, fill, keys, value_bytes, rewrites, None, settle_secs);
                    report("kv", fill, logical, &c, "[KeepAll]");
                    (logical, c)
                });
                let (_, trt) = with_dir(|p| {
                    let (logical, c) = bench_kv(
                        p,
                        fill,
                        keys,
                        value_bytes,
                        rewrites,
                        Some(window),
                        settle_secs,
                    );
                    report("kv", fill, logical, &c, &format!("[Keep w={window}]"));
                    (logical, c)
                });
                println!(
                    "# layer-2 pruning win (rewrites={rewrites}, w={window}): \
                     tables {} -> {} ({:.1}% smaller)  total {} -> {} ({:.1}% smaller)",
                    ctl.tables,
                    trt.tables,
                    pct_drop(ctl.tables, trt.tables),
                    ctl.total,
                    trt.total,
                    pct_drop(ctl.total, trt.total),
                );
            }
            // Single run (optionally with a window but no churn, or plain).
            other => with_dir(|p| {
                let (logical, c) =
                    bench_kv(p, fill, keys, value_bytes, rewrites, other, settle_secs);
                report("kv", fill, logical, &c, "");
            }),
        }
    }
    if primitive == "graph" || primitive == "all" {
        with_dir(|p| bench_graph(p, fill, nodes, edges, settle_secs));
    }
}

#[cfg(test)]
mod tests {
    use super::graph_edge_endpoints;
    use std::collections::HashSet;

    /// #3522: every edge submission must map to a DISTINCT (src, dst) pair, so
    /// the number of live edges equals the number submitted and the `logical`
    /// denominator is accurate. The default workload (nodes=12862, edges=28802)
    /// previously collapsed to 12862 distinct live edges. No self-loops either
    /// (a self-loop is not an island-like street).
    #[test]
    fn graph_edge_endpoints_are_distinct_without_self_loops() {
        for &(node_count, edge_count) in &[(100usize, 250usize), (12_862, 28_802)] {
            let mut seen = HashSet::with_capacity(edge_count);
            for i in 0..edge_count {
                let (src, dst) = graph_edge_endpoints(i, node_count);
                assert!(src < node_count && dst < node_count, "endpoints in range");
                assert_ne!(src, dst, "no self-loop at edge {i}");
                assert!(
                    seen.insert((src, dst)),
                    "duplicate live edge ({src}, {dst}) at submission {i} \
                     (nodes={node_count}, edges={edge_count})",
                );
            }
            assert_eq!(
                seen.len(),
                edge_count,
                "every submission must be a distinct live edge",
            );
        }
    }
}
