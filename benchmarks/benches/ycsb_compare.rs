//! YCSB Competitive Comparison — Strata vs RocksDB vs SQLite
//!
//! Runs standard YCSB workloads A-F against multiple databases and prints a
//! comparison table. Reuses workload definitions from workloads.rs.
//!
//! Usage:
//!   cargo bench --bench ycsb_compare
//!   cargo bench --bench ycsb_compare -- -q                    # quick (10K records)
//!   cargo bench --bench ycsb_compare -- --workload a,c        # specific workloads
//!   cargo bench --bench ycsb_compare -- --records 1000000     # 1M records

use strata_benchmarks::harness;

#[path = "ycsb_workloads.rs"]
mod workloads;

use harness::{create_db, print_hardware_info, DurabilityConfig};
use std::path::Path;
use std::time::Instant;
use tempfile::{NamedTempFile, TempDir};
use workloads::{workload_by_label, ycsb_key, FastRng, KeyChooser, Operation, WorkloadSpec};

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_RECORDS: usize = 100_000;
const DEFAULT_OPS: usize = 100_000;
const QUICK_RECORDS: usize = 10_000;
const QUICK_OPS: usize = 10_000;
const VALUE_SIZE: usize = 4;

// =============================================================================
// Database trait
// =============================================================================

trait YcsbDb {
    fn name(&self) -> &'static str;
    fn put(&mut self, key: &str, value: &[u8]);
    fn get(&mut self, key: &str) -> Option<Vec<u8>>;
    fn scan(&mut self, start_key: &str, count: usize) -> Vec<Vec<u8>>;
}

// =============================================================================
// Strata implementation
// =============================================================================

struct StrataYcsb {
    _db: harness::BenchDb,
}

impl StrataYcsb {
    fn new() -> Self {
        Self {
            _db: create_db(DurabilityConfig::Standard),
        }
    }

    fn db(&self) -> &stratadb::Strata {
        &self._db.db
    }
}

impl YcsbDb for StrataYcsb {
    fn name(&self) -> &'static str {
        "strata"
    }

    fn put(&mut self, key: &str, value: &[u8]) {
        self.db()
            .kv_put(key, stratadb::Value::Bytes(value.to_vec()))
            .unwrap();
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        match self.db().kv_get(key) {
            Ok(Some(stratadb::Value::Bytes(b))) => Some(b),
            _ => None,
        }
    }

    fn scan(&mut self, start_key: &str, count: usize) -> Vec<Vec<u8>> {
        self.db()
            .kv_scan(Some(start_key), Some(count as u64))
            .unwrap_or_default()
            .into_iter()
            .map(|(_, v)| match v {
                stratadb::Value::Bytes(b) => b,
                _ => Vec::new(),
            })
            .collect()
    }
}

// =============================================================================
// RocksDB implementation
// =============================================================================

struct RocksYcsb {
    db: rocksdb::DB,
    _dir: TempDir,
}

impl RocksYcsb {
    fn new(tmpdir: &Path) -> Self {
        let dir = tempfile::tempdir_in(tmpdir).unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, dir.path()).unwrap();
        Self { db, _dir: dir }
    }
}

impl YcsbDb for RocksYcsb {
    fn name(&self) -> &'static str {
        "rocksdb"
    }

    fn put(&mut self, key: &str, value: &[u8]) {
        self.db.put(key.as_bytes(), value).unwrap();
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        self.db.get(key.as_bytes()).unwrap()
    }

    fn scan(&mut self, start_key: &str, count: usize) -> Vec<Vec<u8>> {
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            start_key.as_bytes(),
            rocksdb::Direction::Forward,
        ));
        iter.take(count)
            .filter_map(|r| r.ok().map(|(_, v)| v.to_vec()))
            .collect()
    }
}

// =============================================================================
// SQLite implementation
// =============================================================================

struct SqliteYcsb {
    conn: rusqlite::Connection,
    _file: NamedTempFile,
}

impl SqliteYcsb {
    fn new(tmpdir: &Path) -> Self {
        let file = NamedTempFile::new_in(tmpdir).unwrap();
        let conn = rusqlite::Connection::open(file.path()).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        Self { conn, _file: file }
    }
}

impl YcsbDb for SqliteYcsb {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn put(&mut self, key: &str, value: &[u8]) {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                rusqlite::params![key, value],
            )
            .unwrap();
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        self.conn
            .query_row(
                "SELECT value FROM kv WHERE key = ?1",
                rusqlite::params![key],
                |row| row.get(0),
            )
            .ok()
    }

    fn scan(&mut self, start_key: &str, count: usize) -> Vec<Vec<u8>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key >= ?1 ORDER BY key LIMIT ?2")
            .unwrap();
        stmt.query_map(rusqlite::params![start_key, count as i64], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }
}

// =============================================================================
// Workload runner
// =============================================================================

struct WorkloadResult {
    engine: String,
    workload_label: char,
    load_ops_sec: f64,
    run_ops_sec: f64,
    p50_us: f64,
    p99_us: f64,
}

fn run_workload(
    db: &mut dyn YcsbDb,
    workload: &WorkloadSpec,
    record_count: usize,
    op_count: usize,
) -> WorkloadResult {
    let value = vec![0x42u8; VALUE_SIZE];
    let update_value = vec![0x43u8; VALUE_SIZE];

    // Load phase
    let load_start = Instant::now();
    for i in 0..record_count {
        db.put(&ycsb_key(i), &value);
    }
    let load_elapsed = load_start.elapsed();
    let load_ops_sec = record_count as f64 / load_elapsed.as_secs_f64();

    // Run phase
    let mut rng = FastRng::new(0xABCD_2026);
    let mut key_chooser = KeyChooser::new(workload.distribution, record_count);
    let mut insert_counter = record_count;
    let mut latencies = Vec::with_capacity(op_count);

    let wall_start = Instant::now();
    for _ in 0..op_count {
        let op = workload.choose_operation(rng.next_f64());
        let t = Instant::now();
        match op {
            Operation::Read => {
                let idx = key_chooser.next(&mut rng);
                let _ = db.get(&ycsb_key(idx));
            }
            Operation::Update => {
                let idx = key_chooser.next(&mut rng);
                db.put(&ycsb_key(idx), &update_value);
            }
            Operation::Insert => {
                db.put(&ycsb_key(insert_counter), &value);
                insert_counter += 1;
                key_chooser.set_max_key(insert_counter);
            }
            Operation::Scan => {
                let idx = key_chooser.next(&mut rng);
                let _ = db.scan(&ycsb_key(idx), 10);
            }
            Operation::ReadModifyWrite => {
                let idx = key_chooser.next(&mut rng);
                let key = ycsb_key(idx);
                let _ = db.get(&key);
                db.put(&key, &update_value);
            }
        }
        latencies.push(t.elapsed());
    }
    let wall_elapsed = wall_start.elapsed();

    latencies.sort_unstable();
    let len = latencies.len();
    let p50 = latencies[len * 50 / 100];
    let p99 = latencies[(len * 99 / 100).min(len - 1)];

    WorkloadResult {
        engine: db.name().to_string(),
        workload_label: workload.label,
        load_ops_sec,
        run_ops_sec: op_count as f64 / wall_elapsed.as_secs_f64(),
        p50_us: p50.as_nanos() as f64 / 1000.0,
        p99_us: p99.as_nanos() as f64 / 1000.0,
    }
}

// =============================================================================
// CLI
// =============================================================================

struct Config {
    workloads: Vec<char>,
    records: usize,
    ops: usize,
    quiet: bool,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config {
        workloads: vec!['a', 'b', 'c', 'd', 'e', 'f'],
        records: DEFAULT_RECORDS,
        ops: DEFAULT_OPS,
        quiet: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--workload" | "-w" => {
                i += 1;
                if i < args.len() {
                    config.workloads = args[i]
                        .split(',')
                        .filter_map(|s| s.trim().chars().next())
                        .map(|c| c.to_ascii_lowercase())
                        .collect();
                }
            }
            "--records" => {
                i += 1;
                if i < args.len() {
                    config.records = args[i].parse().unwrap_or(DEFAULT_RECORDS);
                }
            }
            "--ops" => {
                i += 1;
                if i < args.len() {
                    config.ops = args[i].parse().unwrap_or(DEFAULT_OPS);
                }
            }
            "-q" | "--quick" => {
                config.records = QUICK_RECORDS;
                config.ops = QUICK_OPS;
                config.quiet = true;
            }
            _ => {}
        }
        i += 1;
    }
    config
}

// =============================================================================
// Output
// =============================================================================

fn print_comparison_table(results: &[WorkloadResult], engines: &[&str]) {
    // Group by workload
    let workload_labels: Vec<char> = {
        let mut labels = Vec::new();
        for r in results {
            if !labels.contains(&r.workload_label) {
                labels.push(r.workload_label);
            }
        }
        labels
    };

    // Print header
    eprint!("  {:<12}", "workload");
    for engine in engines {
        eprint!("  {:>14}", format!("{} ops/s", engine));
        eprint!("  {:>10}", "p99(us)");
    }
    eprintln!();
    eprint!("  {:<12}", "--------");
    for _ in engines {
        eprint!("  {:>14}", "-----------");
        eprint!("  {:>10}", "-------");
    }
    eprintln!();

    for label in &workload_labels {
        eprint!(
            "  {:<12}",
            format!("Workload {}", label.to_ascii_uppercase())
        );
        for engine in engines {
            let result = results
                .iter()
                .find(|r| r.workload_label == *label && r.engine == *engine);
            match result {
                Some(r) => {
                    eprint!("  {:>14}", format!("{:.0}", r.run_ops_sec));
                    eprint!("  {:>10}", format!("{:.1}", r.p99_us));
                }
                None => {
                    eprint!("  {:>14}", "—");
                    eprint!("  {:>10}", "—");
                }
            }
        }
        eprintln!();
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    print_hardware_info();
    let config = parse_args();

    let tmpdir = std::env::current_dir().unwrap().join(".benchmark");
    std::fs::create_dir_all(&tmpdir).unwrap();

    eprintln!(
        "\n=== YCSB Competitive Comparison ({} records, {} ops) ===\n",
        config.records, config.ops,
    );

    let mut all_results = Vec::new();
    for &label in &config.workloads {
        let workload = match workload_by_label(label) {
            Some(w) => w,
            None => continue,
        };

        eprintln!(
            "--- Workload {}: {} ({}) ---",
            workload.label.to_ascii_uppercase(),
            workload.name,
            workload.mix_label(),
        );

        // Strata
        {
            let mut db = StrataYcsb::new();
            eprint!("  strata...");
            let r = run_workload(&mut db, workload, config.records, config.ops);
            eprintln!(
                " load={:.0} ops/s, run={:.0} ops/s, p50={:.1}us, p99={:.1}us",
                r.load_ops_sec, r.run_ops_sec, r.p50_us, r.p99_us
            );
            all_results.push(r);
        }

        // RocksDB
        {
            let mut db = RocksYcsb::new(&tmpdir);
            eprint!("  rocksdb...");
            let r = run_workload(&mut db, workload, config.records, config.ops);
            eprintln!(
                " load={:.0} ops/s, run={:.0} ops/s, p50={:.1}us, p99={:.1}us",
                r.load_ops_sec, r.run_ops_sec, r.p50_us, r.p99_us
            );
            all_results.push(r);
        }

        // SQLite (skip at 10M+ — too slow)
        if config.records <= 1_000_000 {
            let mut db = SqliteYcsb::new(&tmpdir);
            eprint!("  sqlite...");
            let r = run_workload(&mut db, workload, config.records, config.ops);
            eprintln!(
                " load={:.0} ops/s, run={:.0} ops/s, p50={:.1}us, p99={:.1}us",
                r.load_ops_sec, r.run_ops_sec, r.p50_us, r.p99_us
            );
            all_results.push(r);
        }

        eprintln!();
    }

    let mut engine_names = vec!["strata", "rocksdb"];
    if all_results.iter().any(|r| r.engine == "sqlite") {
        engine_names.push("sqlite");
    }

    // Print comparison table
    eprintln!("=== Summary ===\n");
    print_comparison_table(&all_results, &engine_names);

    let _ = std::fs::remove_dir_all(&tmpdir);
}
