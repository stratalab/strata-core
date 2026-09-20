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
    GraphNodeData, GraphNodeId, GraphProperties, KvKey, KvValue, ProductSpace,
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

fn report(primitive: &str, fill: Fill, logical: u64, c: &Components) {
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
         tables={:>10} ({:.2}x)  wal={:>10}  snap={:>8}",
        logical,
        c.total,
        amp(c.total),
        c.tables,
        amp(c.tables),
        c.wal,
        c.snapshots,
    );
}

fn open_db(root: &Path) -> Database {
    Database::open_local(root, DurableLocalOpenOptions::new())
        .expect("open localfs database")
        .into_database()
}

// --- KV: one physical row per key (control, lowest multiplier). ---
fn bench_kv(root: &Path, fill: Fill, keys: usize, value_bytes: usize, settle_secs: u64) {
    let mut database = open_db(root);
    let branch: BranchName = database.default_branch().clone();
    let space = ProductSpace::new("amp").expect("space");

    let mut logical = 0u64;
    {
        let mut kv = database.kv(branch.clone(), space).expect("kv service");
        let mut batch: Vec<(KvKey, KvValue)> = Vec::with_capacity(256);
        for i in 0..keys {
            let key = format!("key:{i:012}").into_bytes();
            let value = fill_bytes(fill, i as u64, value_bytes);
            logical += (key.len() + value.len()) as u64;
            batch.push((KvKey::new(key).expect("key"), KvValue::new(value)));
            if batch.len() == 256 {
                kv.put_batch(std::mem::take(&mut batch)).expect("kv put_batch");
            }
        }
        if !batch.is_empty() {
            kv.put_batch(batch).expect("kv put_batch");
        }
    }
    force_flush(&mut database, &branch);
    let c = settle(root, settle_secs);
    report("kv", fill, logical, &c);
}

// --- Graph: authored node/edge rows + derived index rows (highest multiplier). ---
fn bench_graph(root: &Path, fill: Fill, node_count: usize, edge_count: usize, settle_secs: u64) {
    let mut database = open_db(root);
    let branch: BranchName = database.default_branch().clone();
    let space = ProductSpace::new("amp").expect("space");
    let mut graph = database.graph(branch.clone(), space).expect("graph service");
    let graph_name = GraphName::new("city").expect("graph name");
    graph.create_graph(graph_name.clone()).expect("create graph");

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
                GraphNodeData::new(
                    Some(GraphProperties::new(props).expect("node props")),
                    None,
                ),
            )
        })
        .collect();
    let name_pool = ["West 106th Street", "Broadway", "Amsterdam Ave", "Columbus Ave"];
    let edges: Vec<(GraphNodeId, GraphEdgeType, GraphNodeId, GraphEdgeData)> = (0..edge_count)
        .map(|i| {
            let src = format!("n:{}", i % node_count.max(1));
            let dst = format!("n:{}", (i * 7 + 1) % node_count.max(1));
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
                GraphEdgeData::new(weight, Some(GraphProperties::new(props).expect("edge props")))
                    .expect("edge data"),
            )
        })
        .collect();
    let logical = serde_json::to_vec(&logical_json).expect("logical json").len() as u64;

    graph
        .bulk_insert(&graph_name, &nodes, &edges, Some(1024))
        .expect("graph bulk_insert");
    drop(graph);
    force_flush(&mut database, &branch);
    let c = settle(root, settle_secs);
    report("graph", fill, logical, &c);
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

    println!(
        "# storage-amplification  fill={}  settle_secs={}",
        if fill == Fill::Real { "real" } else { "random" },
        settle_secs
    );
    println!(
        "{:<8} {:<11} {:>18} {:>18} {:>18} {:>13} {:>13}",
        "primitive", "fill", "logical", "total(amp)", "tables(amp)", "wal", "snapshots"
    );

    let run = |name: &str, f: &dyn Fn(&Path)| {
        let dir = tempfile::tempdir().expect("tempdir");
        f(dir.path());
        let _ = name;
    };
    if primitive == "kv" || primitive == "all" {
        run("kv", &|p| bench_kv(p, fill, keys, value_bytes, settle_secs));
    }
    if primitive == "graph" || primitive == "all" {
        run("graph", &|p| bench_graph(p, fill, nodes, edges, settle_secs));
    }
}
