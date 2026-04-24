//! WAL latency microbenchmark for blind KV writes.
//!
//! Measures per-operation latency for a single fresh-key `kv_put` across
//! cache, standard, and always durability modes, and records WAL counter
//! deltas so the benchmark shows whether the write path appended and/or
//! synced on each operation.
//!
//! Usage:
//!   cargo bench --manifest-path benchmarks/Cargo.toml --bench wal_latency
//!   cargo bench --manifest-path benchmarks/Cargo.toml --bench wal_latency -- -q
//!   cargo bench --manifest-path benchmarks/Cargo.toml --bench wal_latency -- --samples 2000
//!   cargo bench --manifest-path benchmarks/Cargo.toml --bench wal_latency -- --warmup 200
//!   cargo bench --manifest-path benchmarks/Cargo.toml --bench wal_latency -- --no-save

use std::collections::HashMap;
use std::time::Instant;

use strata_benchmarks::harness::recorder::ResultRecorder;
use strata_benchmarks::harness::{
    create_db, kv_key, kv_value, measure_with_counters, print_hardware_info, report_counters,
    report_percentiles, DurabilityConfig, PERCENTILE_SAMPLES,
};
use strata_benchmarks::schema::{BenchmarkMetrics, BenchmarkResult};

const DEFAULT_SAMPLES: usize = PERCENTILE_SAMPLES;
const DEFAULT_WARMUP: usize = 100;
const QUICK_SAMPLES: usize = 250;
const QUICK_WARMUP: usize = 25;
const VALUE_SIZE_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct Config {
    samples: usize,
    warmup: usize,
    save: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            warmup: DEFAULT_WARMUP,
            save: true,
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-q" | "--quick" => {
                config.samples = QUICK_SAMPLES;
                config.warmup = QUICK_WARMUP;
            }
            "--samples" => {
                i += 1;
                if i < args.len() {
                    config.samples = args[i].parse().unwrap_or(DEFAULT_SAMPLES);
                }
            }
            "--warmup" => {
                i += 1;
                if i < args.len() {
                    config.warmup = args[i].parse().unwrap_or(DEFAULT_WARMUP);
                }
            }
            "--no-save" => {
                config.save = false;
            }
            _ => {}
        }
        i += 1;
    }

    config
}

fn main() {
    print_hardware_info();
    let config = parse_args();

    eprintln!(
        "\n=== WAL Latency ({} samples, {} warmup) ===\n",
        config.samples, config.warmup
    );

    let mut recorder = ResultRecorder::new("wal-latency");
    for mode in DurabilityConfig::ALL {
        run_mode(mode, config, &mut recorder);
    }

    if config.save {
        if let Err(err) = recorder.save() {
            eprintln!("Failed to save WAL latency results: {err}");
            std::process::exit(1);
        }
    }
}

fn run_mode(mode: DurabilityConfig, config: Config, recorder: &mut ResultRecorder) {
    let bench_db = create_db(mode);
    let total_keys = config.warmup + config.samples;
    let keys: Vec<String> = (0..total_keys as u64).map(kv_key).collect();
    let value = kv_value();

    eprintln!("--- {} ---", mode.label());

    for key in keys.iter().take(config.warmup) {
        bench_db.db.kv_put(key, value.clone()).unwrap();
    }

    let mut idx = config.warmup;
    let started = Instant::now();
    let (percentiles, wal_delta) = measure_with_counters(&bench_db, config.samples, || {
        let key = &keys[idx];
        idx += 1;
        bench_db.db.kv_put(key, value.clone()).unwrap();
    });
    let elapsed = started.elapsed();
    let ops_per_sec = config.samples as f64 / elapsed.as_secs_f64();
    let label = format!("blind_put ({})", mode.label());

    report_percentiles(&label, &percentiles);
    report_counters(&label, &wal_delta, config.samples as u64);
    eprintln!("  {:<45} ops/s={:.0}", label, ops_per_sec);
    eprintln!();

    let mut parameters = HashMap::new();
    parameters.insert("durability".to_string(), serde_json::json!(mode.label()));
    parameters.insert(
        "value_size_bytes".to_string(),
        serde_json::json!(VALUE_SIZE_BYTES),
    );
    parameters.insert("samples".to_string(), serde_json::json!(config.samples));
    parameters.insert("warmup_ops".to_string(), serde_json::json!(config.warmup));

    let wal_metrics = if wal_delta.wal_appends > 0 || wal_delta.sync_calls > 0 {
        (
            Some(wal_delta.wal_appends as f64 / config.samples as f64),
            Some(wal_delta.sync_calls as f64 / config.samples as f64),
        )
    } else {
        (None, None)
    };

    recorder.record(BenchmarkResult {
        benchmark: format!("wal/blind_put/{}", mode.label()),
        category: "latency".to_string(),
        parameters,
        metrics: BenchmarkMetrics {
            ops_per_sec: Some(ops_per_sec),
            p50_ns: Some(percentiles.p50.as_nanos() as u64),
            p95_ns: Some(percentiles.p95.as_nanos() as u64),
            p99_ns: Some(percentiles.p99.as_nanos() as u64),
            min_ns: Some(percentiles.min.as_nanos() as u64),
            max_ns: Some(percentiles.max.as_nanos() as u64),
            avg_ns: Some((elapsed.as_nanos() / config.samples as u128) as u64),
            samples: Some(percentiles.samples as u64),
            wal_appends_per_op: wal_metrics.0,
            wal_syncs_per_op: wal_metrics.1,
            ..Default::default()
        },
    });
}
