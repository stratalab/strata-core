//! Regression benchmark runner for the Strata cleanup program.
//!
//! Runs three benchmark suites (redb, ycsb, beir), captures results as JSON,
//! compares against baseline, and appends to history for round-by-round tracking.
//!
//! Usage:
//!   cargo run --release -p strata-benchmarks --bin regression
//!   cargo run --release -p strata-benchmarks --bin regression -- --capture-baseline
//!   cargo run --release -p strata-benchmarks --bin regression -- --tranche 2 --epic "RC-1"
//!   cargo run --release -p strata-benchmarks --bin regression -- --quick

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

// =============================================================================
// Result types for regression tracking
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegressionRun {
    /// When this run was captured.
    timestamp: String,
    /// Git commit hash.
    git_commit: String,
    /// Git branch.
    git_branch: String,
    /// Tranche number (0-9).
    tranche: Option<u32>,
    /// Epic identifier (e.g., "RC-1: OpenSpec canonicalization").
    epic: Option<String>,
    /// Hardware fingerprint for apples-to-apples comparison.
    hardware: String,
    /// Results from each suite.
    suites: HashMap<String, SuiteResult>,
    /// Comparison against baseline (if baseline exists).
    comparison: Option<Comparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SuiteResult {
    /// Suite name: "redb", "ycsb", "beir".
    suite: String,
    /// Duration of the suite run in seconds.
    duration_secs: f64,
    /// Key metrics extracted from the suite.
    metrics: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Comparison {
    baseline_commit: String,
    baseline_timestamp: String,
    verdict: Verdict,
    regressions: Vec<Regression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Verdict {
    Pass,
    Fail,
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Regression {
    suite: String,
    metric: String,
    baseline_value: f64,
    current_value: f64,
    delta_pct: f64,
    threshold_pct: f64,
    severity: String,
}

// =============================================================================
// Thresholds from non-regression-requirements.md
// =============================================================================

struct Threshold {
    metric_pattern: &'static str,
    max_regression_pct: f64,
}

const THRESHOLDS: &[Threshold] = &[
    // Throughput: max 5% regression (negative = regression for ops/sec)
    Threshold {
        metric_pattern: "ops_per_sec",
        max_regression_pct: -5.0,
    },
    Threshold {
        metric_pattern: "load_ops_sec",
        max_regression_pct: -5.0,
    },
    Threshold {
        metric_pattern: "run_ops_sec",
        max_regression_pct: -5.0,
    },
    // Median latency: max 5% regression (positive = regression for latency)
    Threshold {
        metric_pattern: "p50",
        max_regression_pct: 5.0,
    },
    // Tail latency: max 10% regression
    Threshold {
        metric_pattern: "p99",
        max_regression_pct: 10.0,
    },
    // BEIR nDCG@10: max 0.01 absolute drop (handled specially)
    Threshold {
        metric_pattern: "ndcg",
        max_regression_pct: -100.0,
    },
];

/// BEIR nDCG absolute threshold (not percentage-based).
const BEIR_NDCG_MAX_ABSOLUTE_DROP: f64 = 0.01;

// =============================================================================
// YCSB runner (inline — runs Strata only for regression tracking)
// =============================================================================

fn run_ycsb_strata(records: usize, ops: usize) -> SuiteResult {
    use stratadb::{Strata, Value};

    let start = Instant::now();
    let tmpdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let db = Strata::open(tmpdir.path()).expect("failed to open strata");

    let value = vec![0x42u8; 4];
    let update_value = vec![0x43u8; 4];

    // Load phase
    let load_start = Instant::now();
    for i in 0..records {
        db.kv_put(&format!("{:0>20}", i), Value::Bytes(value.clone()))
            .unwrap();
    }
    let load_elapsed = load_start.elapsed();
    let load_ops_sec = records as f64 / load_elapsed.as_secs_f64();

    // Run phase — simplified Workload A (50% read, 50% update)
    let mut rng = fastrand::Rng::with_seed(0xABCD_2026);
    let mut latencies = Vec::with_capacity(ops);
    let run_start = Instant::now();
    for _ in 0..ops {
        let idx = rng.usize(..records);
        let key = format!("{:0>20}", idx);
        let t = Instant::now();
        if rng.bool() {
            let _ = db.kv_get(&key);
        } else {
            db.kv_put(&key, Value::Bytes(update_value.clone())).unwrap();
        }
        latencies.push(t.elapsed());
    }
    let run_elapsed = run_start.elapsed();
    let run_ops_sec = ops as f64 / run_elapsed.as_secs_f64();

    latencies.sort_unstable();
    let len = latencies.len();
    let p50_us = latencies[len * 50 / 100].as_nanos() as f64 / 1000.0;
    let p99_us = latencies[(len * 99 / 100).min(len - 1)].as_nanos() as f64 / 1000.0;

    let duration_secs = start.elapsed().as_secs_f64();

    let mut metrics = HashMap::new();
    metrics.insert("load_ops_sec".into(), load_ops_sec);
    metrics.insert("run_ops_sec".into(), run_ops_sec);
    metrics.insert("p50_us".into(), p50_us);
    metrics.insert("p99_us".into(), p99_us);

    SuiteResult {
        suite: "ycsb".into(),
        duration_secs,
        metrics,
    }
}

// =============================================================================
// Branch suite — locks B1 baseline for branch operation latency / throughput.
// Per docs/design/branching/branching-execution-plan.md (B9 benchmark matrix)
// and B1 deliverable: "branch benchmark baselines exist for later comparison".
// Local-only; benchmarks/ is intentionally untracked.
// =============================================================================

fn percentiles_us(timings: &mut [std::time::Duration]) -> (f64, f64, f64) {
    timings.sort_unstable();
    let len = timings.len();
    if len == 0 {
        return (0.0, 0.0, 0.0);
    }
    let p = |pct: usize| {
        let idx = (len * pct / 100).min(len - 1);
        timings[idx].as_nanos() as f64 / 1000.0
    };
    (p(50), p(95), p(99))
}

fn run_branch_strata(iterations: usize, large_merge_keys: usize) -> SuiteResult {
    use stratadb::{MergeStrategy, Strata, Value};

    let start = Instant::now();
    let tmpdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let path = tmpdir.path().to_path_buf();
    let mut db = Strata::open(&path).expect("failed to open strata for branch suite");

    let mut create_lat = Vec::with_capacity(iterations);
    let mut fork_lat = Vec::with_capacity(iterations);
    let mut diff_lat = Vec::with_capacity(iterations);
    let mut delete_lat = Vec::with_capacity(iterations);

    let small_value = Value::Bytes(vec![0x42u8; 64]);

    // -- create
    //   Write one key per branch after create so storage registers the
    //   branch (storage.fork_branch refuses sources with no writes).
    for i in 0..iterations {
        let name = format!("bench-create-{:08}", i);
        let t = Instant::now();
        db.branches()
            .create(&name)
            .expect("branches().create must succeed");
        create_lat.push(t.elapsed());

        db.set_branch(&name).unwrap();
        db.kv_put("seed", small_value.clone()).unwrap();
    }
    db.set_branch("default").unwrap();

    // -- fork (each iteration forks bench-create-i into bench-fork-i)
    for i in 0..iterations {
        let src = format!("bench-create-{:08}", i);
        let dst = format!("bench-fork-{:08}", i);
        let t = Instant::now();
        db.branches()
            .fork(&src, &dst)
            .expect("branches().fork must succeed");
        fork_lat.push(t.elapsed());
    }

    // -- diff (compare each fork pair)
    for i in 0..iterations {
        let a = format!("bench-create-{:08}", i);
        let b = format!("bench-fork-{:08}", i);
        let t = Instant::now();
        let _ = db.branches().diff(&a, &b);
        diff_lat.push(t.elapsed());
    }

    // -- merge small (10 keys per side, fresh fork each iteration)
    let mut merge_small_lat = Vec::with_capacity(iterations.min(64));
    for i in 0..iterations.min(64) {
        let parent = format!("ms-parent-{:04}", i);
        let child = format!("ms-child-{:04}", i);
        db.branches().create(&parent).unwrap();
        // Seed parent so storage will fork from it.
        db.set_branch(&parent).unwrap();
        db.kv_put("seed", small_value.clone()).unwrap();
        db.branches().fork(&parent, &child).unwrap();
        db.set_branch(&child).unwrap();
        for k in 0..10 {
            db.kv_put(&format!("k{}", k), small_value.clone()).unwrap();
        }
        let t = Instant::now();
        db.branches()
            .merge(&child, &parent, MergeStrategy::LastWriterWins)
            .expect("branches().merge (small) must succeed for valid baseline");
        merge_small_lat.push(t.elapsed());
    }

    // -- merge large (large_merge_keys per side, single iteration — slow)
    let merge_large_us = {
        let parent = "ml-parent";
        let child = "ml-child";
        db.branches().create(parent).unwrap();
        db.set_branch(parent).unwrap();
        db.kv_put("seed", small_value.clone()).unwrap();
        db.branches().fork(parent, child).unwrap();
        db.set_branch(child).unwrap();
        for k in 0..large_merge_keys {
            db.kv_put(&format!("ml-k{:08}", k), small_value.clone())
                .unwrap();
        }
        let t = Instant::now();
        db.branches()
            .merge(child, parent, MergeStrategy::LastWriterWins)
            .expect("branches().merge (large) must succeed for valid baseline");
        t.elapsed().as_nanos() as f64 / 1000.0
    };

    // -- delete (delete every bench-fork-* branch we created)
    db.set_branch("default").unwrap();
    for i in 0..iterations {
        let name = format!("bench-fork-{:08}", i);
        let t = Instant::now();
        db.branches()
            .delete(&name)
            .expect("branches().delete must succeed");
        delete_lat.push(t.elapsed());
    }

    // -- reopen-after-N-branches: drop and re-open the same path
    drop(db);
    let reopen_after = {
        let t = Instant::now();
        let _db = Strata::open(&path).expect("reopen must succeed");
        t.elapsed().as_millis() as f64
    };

    // -- materialize (B6 smoke): per-iteration `storage().materialize_layer(child, 0)`
    //   on freshly-forked children with an inherited layer from a
    //   rewritten parent. Uses `strata_engine::Database` directly —
    //   the `Strata` executor does not expose materialize.
    let mat_iters = iterations.min(64);
    let mut materialize_lat = Vec::with_capacity(mat_iters);
    {
        use strata_core::{BranchId as CoreBranchId, Value as CoreValue};
        use strata_engine::{open_product_database, KVStore, OpenOptions, ProductOpenOutcome};

        let mat_tmpdir = tempfile::TempDir::new().expect("materialize tmpdir");
        let mat_outcome = open_product_database(mat_tmpdir.path(), OpenOptions::default())
            .expect("materialize product open");
        let ProductOpenOutcome::Local { db: mat_db, .. } = mat_outcome else {
            panic!("fresh materialize benchmark database should open locally");
        };
        for i in 0..mat_iters {
            let parent = format!("mat-parent-{:08}", i);
            let child = format!("mat-child-{:08}", i);
            let pid = CoreBranchId::from_user_name(&parent);
            let cid = CoreBranchId::from_user_name(&child);

            mat_db.branches().create(&parent).unwrap();
            KVStore::new(mat_db.clone())
                .put(&pid, "default", "seed", CoreValue::Int(i as i64))
                .unwrap();
            mat_db.storage().rotate_memtable(&pid);
            mat_db.storage().flush_oldest_frozen(&pid).unwrap();
            mat_db.branches().fork(&parent, &child).unwrap();
            // Rewrite parent so child inherits a real layer to materialize.
            KVStore::new(mat_db.clone())
                .put(&pid, "default", "seed", CoreValue::Int(i as i64 + 1))
                .unwrap();
            mat_db.storage().rotate_memtable(&pid);
            mat_db.storage().flush_oldest_frozen(&pid).unwrap();

            let t = Instant::now();
            mat_db
                .storage()
                .materialize_layer(&cid, 0)
                .expect("materialize layer 0 must succeed for valid baseline");
            materialize_lat.push(t.elapsed());
        }
    }

    let duration_secs = start.elapsed().as_secs_f64();

    let (create_p50, create_p95, create_p99) = percentiles_us(&mut create_lat);
    let (fork_p50, fork_p95, fork_p99) = percentiles_us(&mut fork_lat);
    let (diff_p50, diff_p95, diff_p99) = percentiles_us(&mut diff_lat);
    let (delete_p50, delete_p95, delete_p99) = percentiles_us(&mut delete_lat);
    let (merge_s_p50, merge_s_p95, merge_s_p99) = percentiles_us(&mut merge_small_lat);
    let (materialize_p50, materialize_p95, materialize_p99) =
        percentiles_us(&mut materialize_lat);

    let mut metrics = HashMap::new();
    metrics.insert("create_p50_us".into(), create_p50);
    metrics.insert("create_p95_us".into(), create_p95);
    metrics.insert("create_p99_us".into(), create_p99);
    metrics.insert("fork_p50_us".into(), fork_p50);
    metrics.insert("fork_p95_us".into(), fork_p95);
    metrics.insert("fork_p99_us".into(), fork_p99);
    metrics.insert("diff_p50_us".into(), diff_p50);
    metrics.insert("diff_p95_us".into(), diff_p95);
    metrics.insert("diff_p99_us".into(), diff_p99);
    metrics.insert("delete_p50_us".into(), delete_p50);
    metrics.insert("delete_p95_us".into(), delete_p95);
    metrics.insert("delete_p99_us".into(), delete_p99);
    metrics.insert("merge_small_p50_us".into(), merge_s_p50);
    metrics.insert("merge_small_p95_us".into(), merge_s_p95);
    metrics.insert("merge_small_p99_us".into(), merge_s_p99);
    metrics.insert("merge_large_us".into(), merge_large_us);
    // B6 materialize smoke. `materialize_p50_us` + `materialize_p99_us`
    // are automatically gated by the `THRESHOLDS` table (metric-name
    // substring match on "p50" and "p99"). `materialize_p95_us` is
    // visibility-only and is not in any gated pattern.
    metrics.insert("materialize_p50_us".into(), materialize_p50);
    metrics.insert("materialize_p95_us".into(), materialize_p95);
    metrics.insert("materialize_p99_us".into(), materialize_p99);
    // Use a stable key name (independent of iteration count) so baselines can
    // be compared across runs with different `--quick` settings. The actual
    // branch population is implicit in the suite definition above.
    metrics.insert("reopen_after_branch_heavy_history_ms".into(), reopen_after);

    SuiteResult {
        suite: "branch".into(),
        duration_secs,
        metrics,
    }
}

// =============================================================================
// redb runner (inline — Strata only for regression tracking)
// =============================================================================

fn run_redb_strata(record_count: usize) -> SuiteResult {
    use stratadb::{Strata, Value};

    let start = Instant::now();
    let tmpdir = tempfile::TempDir::new().expect("failed to create temp dir");
    let db = Strata::open(tmpdir.path()).expect("failed to open strata");

    let key_size = 24;
    let value_size = 150;

    // Bulk load
    let bulk_start = Instant::now();
    for i in 0..record_count {
        let key = format!("{:0>width$}", i, width = key_size);
        db.kv_put(&key, Value::Bytes(vec![0x42; value_size]))
            .unwrap();
    }
    let bulk_elapsed = bulk_start.elapsed();
    let bulk_ops_sec = record_count as f64 / bulk_elapsed.as_secs_f64();

    // Random reads (1M or record_count, whichever is smaller)
    let read_count = record_count.min(1_000_000);
    let mut rng = fastrand::Rng::with_seed(0xDEAD_BEEF);
    let read_start = Instant::now();
    for _ in 0..read_count {
        let idx = rng.usize(..record_count);
        let key = format!("{:0>width$}", idx, width = key_size);
        let _ = db.kv_get(&key);
    }
    let read_elapsed = read_start.elapsed();
    let read_ops_sec = read_count as f64 / read_elapsed.as_secs_f64();

    // Range scans (500K or record_count/10)
    let scan_count = (record_count / 10).min(500_000);
    let scan_start = Instant::now();
    for _ in 0..scan_count {
        let idx = rng.usize(..record_count);
        let key = format!("{:0>width$}", idx, width = key_size);
        let _ = db.kv_scan(Some(&key), Some(10));
    }
    let scan_elapsed = scan_start.elapsed();
    let scan_ops_sec = scan_count as f64 / scan_elapsed.as_secs_f64();

    let duration_secs = start.elapsed().as_secs_f64();

    let mut metrics = HashMap::new();
    metrics.insert("bulk_load_ops_sec".into(), bulk_ops_sec);
    metrics.insert("random_read_ops_sec".into(), read_ops_sec);
    metrics.insert("range_scan_ops_sec".into(), scan_ops_sec);
    metrics.insert(
        "bulk_load_duration_ms".into(),
        bulk_elapsed.as_millis() as f64,
    );

    SuiteResult {
        suite: "redb".into(),
        duration_secs,
        metrics,
    }
}

// =============================================================================
// BEIR runner (inline — nDCG@10 on 4 small datasets)
// =============================================================================

fn run_beir_strata(data_dir: &Path) -> SuiteResult {
    use stratadb::{SearchQuery, Strata, Value};

    let start = Instant::now();
    let datasets = ["nfcorpus", "scifact", "arguana", "scidocs"];
    let mut metrics = HashMap::new();
    let mut total_ndcg = 0.0;
    let mut dataset_count = 0;

    for dataset_name in &datasets {
        let dataset_path = data_dir.join(dataset_name);
        if !dataset_path.exists() {
            eprintln!(
                "  SKIP {} (not found at {})",
                dataset_name,
                dataset_path.display()
            );
            continue;
        }

        let corpus = load_beir_corpus(&dataset_path.join("corpus.jsonl"));
        let queries = load_beir_queries(&dataset_path.join("queries.jsonl"));
        let qrels = load_beir_qrels(&dataset_path.join("qrels/test.tsv"));

        // Index
        let tmpdir = tempfile::TempDir::new().unwrap();
        let db = Strata::open(tmpdir.path()).unwrap();
        for (doc_id, (title, text)) in &corpus {
            let content = if title.is_empty() {
                text.clone()
            } else {
                format!("{} {}", title, text)
            };
            db.kv_put(doc_id, Value::String(content)).unwrap();
        }

        // Search and compute nDCG@10
        let k = 10;
        let mut ndcg_sum = 0.0;
        let mut query_count = 0;
        for (qid, query_text) in &queries {
            if !qrels.contains_key(qid) {
                continue;
            }
            let results = db.search(SearchQuery {
                query: query_text.clone(),
                recipe: Some(serde_json::json!("keyword")),
                precomputed_embedding: None,
                k: Some(k as u64),
                as_of: None,
                diff: None,
            });
            match results {
                Ok((hits, _stats)) => {
                    let ranked: Vec<String> = hits
                        .iter()
                        .filter_map(|h| {
                            h.entity_ref
                                .key
                                .as_deref()
                                .filter(|k| *k != qid)
                                .map(String::from)
                        })
                        .take(k)
                        .collect();
                    let ranked_refs: Vec<&str> = ranked.iter().map(|s| s.as_str()).collect();
                    let ndcg = compute_ndcg(&ranked_refs, qrels.get(qid).unwrap(), k);
                    ndcg_sum += ndcg;
                    query_count += 1;
                }
                Err(_) => {}
            }
        }

        if query_count > 0 {
            let avg_ndcg = ndcg_sum / query_count as f64;
            eprintln!(
                "  {} nDCG@{}: {:.4} ({} queries)",
                dataset_name, k, avg_ndcg, query_count
            );
            metrics.insert(format!("{}_ndcg_at_{}", dataset_name, k), avg_ndcg);
            total_ndcg += avg_ndcg;
            dataset_count += 1;
        }
    }

    if dataset_count > 0 {
        metrics.insert("avg_ndcg_at_10".into(), total_ndcg / dataset_count as f64);
    }

    let duration_secs = start.elapsed().as_secs_f64();
    SuiteResult {
        suite: "beir".into(),
        duration_secs,
        metrics,
    }
}

// BEIR helpers

fn load_beir_corpus(path: &Path) -> HashMap<String, (String, String)> {
    let file = std::fs::File::open(path).expect("Cannot open corpus");
    let reader = BufReader::new(file);
    let mut docs = HashMap::new();
    for line in reader.lines() {
        let line = line.unwrap();
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        let id = v["_id"].as_str().unwrap().to_string();
        let title = v["title"].as_str().unwrap_or("").to_string();
        let text = v["text"].as_str().unwrap().to_string();
        docs.insert(id, (title, text));
    }
    docs
}

fn load_beir_queries(path: &Path) -> HashMap<String, String> {
    let file = std::fs::File::open(path).expect("Cannot open queries");
    let reader = BufReader::new(file);
    let mut queries = HashMap::new();
    for line in reader.lines() {
        let line = line.unwrap();
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        let id = v["_id"].as_str().unwrap().to_string();
        let text = v["text"].as_str().unwrap().to_string();
        queries.insert(id, text);
    }
    queries
}

fn load_beir_qrels(path: &Path) -> HashMap<String, HashMap<String, u32>> {
    let file = std::fs::File::open(path).expect("Cannot open qrels");
    let reader = BufReader::new(file);
    let mut qrels: HashMap<String, HashMap<String, u32>> = HashMap::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        let qid = parts[0].to_string();
        let doc_id = parts[1].to_string();
        let score: u32 = parts[2].parse().unwrap_or(0);
        if qid == "query-id" {
            continue;
        } // skip header
        qrels.entry(qid).or_default().insert(doc_id, score);
    }
    qrels
}

fn compute_ndcg(ranked: &[&str], qrels: &HashMap<String, u32>, k: usize) -> f64 {
    let dcg: f64 = ranked
        .iter()
        .enumerate()
        .take(k)
        .map(|(i, doc_id)| {
            let rel = *qrels.get(*doc_id).unwrap_or(&0) as f64;
            (2f64.powf(rel) - 1.0) / (2.0 + i as f64).log2()
        })
        .sum();

    // Ideal DCG
    let mut ideal_rels: Vec<u32> = qrels.values().copied().collect();
    ideal_rels.sort_unstable_by(|a, b| b.cmp(a));
    let idcg: f64 = ideal_rels
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, &rel)| (2f64.powf(rel as f64) - 1.0) / (2.0 + i as f64).log2())
        .sum();

    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

// =============================================================================
// Comparison logic
// =============================================================================

fn compare_against_baseline(
    current: &HashMap<String, SuiteResult>,
    baseline: &RegressionRun,
) -> Comparison {
    let mut regressions = Vec::new();

    for (suite_name, current_suite) in current {
        let baseline_suite = match baseline.suites.get(suite_name) {
            Some(s) => s,
            None => continue,
        };

        for (metric_name, &current_val) in &current_suite.metrics {
            let baseline_val = match baseline_suite.metrics.get(metric_name) {
                Some(&v) => v,
                None => continue,
            };

            // Special case: BEIR nDCG uses absolute threshold
            if metric_name.contains("ndcg") {
                let drop = baseline_val - current_val;
                if drop > BEIR_NDCG_MAX_ABSOLUTE_DROP {
                    regressions.push(Regression {
                        suite: suite_name.clone(),
                        metric: metric_name.clone(),
                        baseline_value: baseline_val,
                        current_value: current_val,
                        delta_pct: -drop / baseline_val * 100.0,
                        threshold_pct: -100.0 * BEIR_NDCG_MAX_ABSOLUTE_DROP / baseline_val,
                        severity: "FAIL".into(),
                    });
                }
                continue;
            }

            // Percentage-based thresholds
            if baseline_val == 0.0 {
                continue;
            }
            let delta_pct = (current_val - baseline_val) / baseline_val * 100.0;

            for threshold in THRESHOLDS {
                if !metric_name.contains(threshold.metric_pattern) {
                    continue;
                }

                let is_regression = if threshold.max_regression_pct < 0.0 {
                    // For throughput: negative delta = regression
                    delta_pct < threshold.max_regression_pct
                } else {
                    // For latency: positive delta = regression
                    delta_pct > threshold.max_regression_pct
                };

                if is_regression {
                    regressions.push(Regression {
                        suite: suite_name.clone(),
                        metric: metric_name.clone(),
                        baseline_value: baseline_val,
                        current_value: current_val,
                        delta_pct,
                        threshold_pct: threshold.max_regression_pct,
                        severity: "FAIL".into(),
                    });
                }
                break;
            }
        }
    }

    let verdict = if regressions.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail
    };

    Comparison {
        baseline_commit: baseline.git_commit.clone(),
        baseline_timestamp: baseline.timestamp.clone(),
        verdict,
        regressions,
    }
}

// =============================================================================
// History management
// =============================================================================

fn history_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("history")
        .join("history.json")
}

fn baseline_path() -> PathBuf {
    baseline_path_named("pre-cleanup.json")
}

fn baseline_path_named(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("baselines")
        .join(name)
}

fn load_history() -> Vec<RegressionRun> {
    let path = history_path();
    if !path.exists() {
        return Vec::new();
    }
    let contents = std::fs::read_to_string(&path).unwrap_or_default();
    serde_json::from_str(&contents).unwrap_or_default()
}

fn save_history(history: &[RegressionRun]) {
    let path = history_path();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let json = serde_json::to_string_pretty(history).unwrap();
    std::fs::write(&path, json).unwrap();
}

#[allow(dead_code)] // Convenience wrapper for the default baseline path.
fn load_baseline() -> Option<RegressionRun> {
    load_baseline_at(&baseline_path())
}

fn load_baseline_at(path: &Path) -> Option<RegressionRun> {
    if !path.exists() {
        return None;
    }
    let contents = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&contents).ok()
}

#[allow(dead_code)] // Convenience wrapper for the default baseline path.
fn save_baseline(run: &RegressionRun) {
    save_baseline_at(run, &baseline_path());
}

fn save_baseline_at(run: &RegressionRun, path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let json = serde_json::to_string_pretty(run).unwrap();
    std::fs::write(path, json).unwrap();
    eprintln!("Baseline saved to {}", path.display());
}

// =============================================================================
// Git/hardware helpers
// =============================================================================

fn git_short_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn git_branch() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn hardware_fingerprint() -> String {
    let cpu = read_cpu_model();
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let ram_gb = read_total_ram_gb();
    format!("{} ({} cores, {} GB)", cpu, cores, ram_gb)
}

fn read_cpu_model() -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in contents.lines() {
                if line.starts_with("model name") {
                    if let Some(val) = line.split(':').nth(1) {
                        return val.trim().to_string();
                    }
                }
            }
        }
    }
    "unknown".to_string()
}

fn read_total_ram_gb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return kb / 1_048_576;
                        }
                    }
                }
            }
        }
    }
    0
}

fn iso8601_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

// =============================================================================
// CLI
// =============================================================================

struct Config {
    capture_baseline: bool,
    tranche: Option<u32>,
    epic: Option<String>,
    quick: bool,
    beir_data_dir: PathBuf,
    skip_redb: bool,
    skip_ycsb: bool,
    skip_beir: bool,
    skip_branch: bool,
    /// Custom baseline filename inside `benchmarks/baselines/`. Default
    /// is `pre-cleanup.json`. Use `pre-branch-cleanup.json` for branch
    /// suite isolation (B1 deliverable).
    baseline_name: Option<String>,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config {
        capture_baseline: false,
        tranche: None,
        epic: None,
        quick: false,
        beir_data_dir: PathBuf::from("../strata-benchmarks/datasets/beir"),
        skip_redb: false,
        skip_ycsb: false,
        skip_beir: false,
        skip_branch: false,
        baseline_name: None,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--capture-baseline" => config.capture_baseline = true,
            "--quick" | "-q" => config.quick = true,
            "--tranche" => {
                i += 1;
                if i < args.len() {
                    config.tranche = args[i].parse().ok();
                }
            }
            "--epic" => {
                i += 1;
                if i < args.len() {
                    config.epic = Some(args[i].clone());
                }
            }
            "--beir-data-dir" => {
                i += 1;
                if i < args.len() {
                    config.beir_data_dir = PathBuf::from(&args[i]);
                }
            }
            "--baseline-name" => {
                i += 1;
                if i < args.len() {
                    config.baseline_name = Some(args[i].clone());
                }
            }
            "--skip-redb" => config.skip_redb = true,
            "--skip-ycsb" => config.skip_ycsb = true,
            "--skip-beir" => config.skip_beir = true,
            "--skip-branch" => config.skip_branch = true,
            _ => {}
        }
        i += 1;
    }
    config
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let config = parse_args();

    let (redb_records, ycsb_records, ycsb_ops, branch_iters, branch_large_merge) = if config.quick {
        (100_000, 10_000, 10_000, 100, 1_000)
    } else {
        (5_000_000, 100_000, 100_000, 1_000, 10_000)
    };

    eprintln!("=== Strata Regression Benchmark ===");
    eprintln!("Hardware: {}", hardware_fingerprint());
    eprintln!("Commit:   {}", git_short_commit());
    eprintln!("Branch:   {}", git_branch());
    if let Some(t) = config.tranche {
        eprintln!("Tranche:  {}", t);
    }
    if let Some(ref e) = config.epic {
        eprintln!("Epic:     {}", e);
    }
    if config.quick {
        eprintln!("Mode:     QUICK (reduced dataset sizes)");
    }
    eprintln!();

    let mut suites: HashMap<String, SuiteResult> = HashMap::new();

    // Run redb suite
    if !config.skip_redb {
        eprintln!("--- redb ({} records) ---", redb_records);
        let result = run_redb_strata(redb_records);
        eprintln!(
            "  bulk load:   {:.0} ops/s",
            result.metrics["bulk_load_ops_sec"]
        );
        eprintln!(
            "  random read: {:.0} ops/s",
            result.metrics["random_read_ops_sec"]
        );
        eprintln!(
            "  range scan:  {:.0} ops/s",
            result.metrics["range_scan_ops_sec"]
        );
        eprintln!("  duration:    {:.1}s", result.duration_secs);
        eprintln!();
        suites.insert("redb".into(), result);
    }

    // Run YCSB suite
    if !config.skip_ycsb {
        eprintln!("--- ycsb ({} records, {} ops) ---", ycsb_records, ycsb_ops);
        let result = run_ycsb_strata(ycsb_records, ycsb_ops);
        eprintln!("  load:   {:.0} ops/s", result.metrics["load_ops_sec"]);
        eprintln!("  run:    {:.0} ops/s", result.metrics["run_ops_sec"]);
        eprintln!("  p50:    {:.1} us", result.metrics["p50_us"]);
        eprintln!("  p99:    {:.1} us", result.metrics["p99_us"]);
        eprintln!("  duration: {:.1}s", result.duration_secs);
        eprintln!();
        suites.insert("ycsb".into(), result);
    }

    // Run branch suite — B1 baseline lock for branching closure plan.
    if !config.skip_branch {
        eprintln!(
            "--- branch ({} iters, large_merge={} keys) ---",
            branch_iters, branch_large_merge
        );
        let result = run_branch_strata(branch_iters, branch_large_merge);
        eprintln!("  create  p50: {:.1} us", result.metrics["create_p50_us"]);
        eprintln!("  fork    p50: {:.1} us", result.metrics["fork_p50_us"]);
        eprintln!("  diff    p50: {:.1} us", result.metrics["diff_p50_us"]);
        eprintln!(
            "  merge_s p50: {:.1} us",
            result.metrics["merge_small_p50_us"]
        );
        eprintln!(
            "  merge_l    : {:.1} us",
            result.metrics["merge_large_us"]
        );
        eprintln!("  delete  p50: {:.1} us", result.metrics["delete_p50_us"]);
        eprintln!(
            "  materialize p50: {:.1} us",
            result.metrics["materialize_p50_us"]
        );
        for (k, v) in result.metrics.iter() {
            if k.starts_with("reopen_after_") {
                eprintln!("  {}: {:.1} ms", k, v);
            }
        }
        eprintln!("  duration:    {:.1}s", result.duration_secs);
        eprintln!();
        suites.insert("branch".into(), result);
    }

    // Run BEIR suite
    if !config.skip_beir {
        if config.beir_data_dir.exists() {
            eprintln!("--- beir (4 datasets) ---");
            let result = run_beir_strata(&config.beir_data_dir);
            if let Some(&avg) = result.metrics.get("avg_ndcg_at_10") {
                eprintln!("  avg nDCG@10: {:.4}", avg);
            }
            eprintln!("  duration:    {:.1}s", result.duration_secs);
            eprintln!();
            suites.insert("beir".into(), result);
        } else {
            eprintln!(
                "--- beir: SKIPPED (datasets not found at {}) ---",
                config.beir_data_dir.display()
            );
            eprintln!("  Use --beir-data-dir to specify the BEIR datasets path");
            eprintln!();
        }
    }

    // Build the run record
    let run = RegressionRun {
        timestamp: iso8601_now(),
        git_commit: git_short_commit(),
        git_branch: git_branch(),
        tranche: config.tranche,
        epic: config.epic,
        hardware: hardware_fingerprint(),
        suites: suites.clone(),
        comparison: None,
    };

    // Capture baseline if requested
    if config.capture_baseline {
        let path = match &config.baseline_name {
            Some(name) => baseline_path_named(name),
            None => baseline_path(),
        };
        save_baseline_at(&run, &path);
        eprintln!("=== Baseline captured ===");
        // Also output as JSON to stdout
        println!("{}", serde_json::to_string_pretty(&run).unwrap());
        return;
    }

    // Compare against baseline
    let mut run = run;
    let baseline = match &config.baseline_name {
        Some(name) => load_baseline_at(&baseline_path_named(name)),
        None => load_baseline(),
    };
    if let Some(baseline) = baseline {
        eprintln!(
            "=== Comparison vs baseline ({} @ {}) ===\n",
            baseline.git_commit, baseline.timestamp
        );
        let comparison = compare_against_baseline(&suites, &baseline);

        // Print comparison table
        for (suite_name, current_suite) in &suites {
            if let Some(baseline_suite) = baseline.suites.get(suite_name) {
                eprintln!("  {} suite:", suite_name);
                for (metric, &current_val) in &current_suite.metrics {
                    if let Some(&baseline_val) = baseline_suite.metrics.get(metric) {
                        let delta_pct = if baseline_val != 0.0 {
                            (current_val - baseline_val) / baseline_val * 100.0
                        } else {
                            0.0
                        };
                        let marker = if comparison
                            .regressions
                            .iter()
                            .any(|r| r.suite == *suite_name && r.metric == *metric)
                        {
                            " REGRESSION"
                        } else {
                            ""
                        };
                        eprintln!(
                            "    {:<30} {:>12.2} → {:>12.2} ({:+.1}%){}",
                            metric, baseline_val, current_val, delta_pct, marker
                        );
                    }
                }
                eprintln!();
            }
        }

        match comparison.verdict {
            Verdict::Pass => eprintln!("VERDICT: PASS"),
            Verdict::Fail => {
                eprintln!("VERDICT: FAIL");
                eprintln!();
                for r in &comparison.regressions {
                    eprintln!(
                        "  {} {} {}: {:.2} → {:.2} ({:+.1}%, threshold: {:.1}%)",
                        r.severity,
                        r.suite,
                        r.metric,
                        r.baseline_value,
                        r.current_value,
                        r.delta_pct,
                        r.threshold_pct
                    );
                }
            }
            Verdict::Warn => eprintln!("VERDICT: WARN"),
        }
        eprintln!();

        run.comparison = Some(comparison);
    } else {
        eprintln!("No baseline found. Run with --capture-baseline first.");
        eprintln!();
    }

    // Append to history
    let mut history = load_history();
    history.push(run.clone());
    save_history(&history);
    eprintln!("History updated ({} entries)", history.len());

    // Output JSON to stdout
    println!("{}", serde_json::to_string_pretty(&run).unwrap());

    // Exit with non-zero if regression detected
    if let Some(ref comparison) = run.comparison {
        if comparison.verdict == Verdict::Fail {
            std::process::exit(1);
        }
    }
}
