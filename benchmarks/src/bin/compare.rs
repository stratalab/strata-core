//! Benchmark comparison tool.
//!
//! Compares two JSON result files and prints a table showing performance deltas.
//!
//! Usage: `cargo run --bin bench-compare -- <baseline.json> <candidate.json>`

use std::collections::HashMap;
use strata_benchmarks::schema::{BenchmarkReport, BenchmarkResult};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <baseline.json> <candidate.json>", args[0]);
        std::process::exit(1);
    }

    let baseline = load_report(&args[1]);
    let candidate = load_report(&args[2]);

    // Build lookup by benchmark name
    let base_map: HashMap<&str, &BenchmarkResult> = baseline
        .results
        .iter()
        .map(|r| (r.benchmark.as_str(), r))
        .collect();

    let cand_map: HashMap<&str, &BenchmarkResult> = candidate
        .results
        .iter()
        .map(|r| (r.benchmark.as_str(), r))
        .collect();

    // Header
    eprintln!("Baseline: {} ({})", args[1], baseline.metadata.timestamp);
    eprintln!("Candidate: {} ({})", args[2], candidate.metadata.timestamp);
    eprintln!();

    println!(
        "{:<40} | {:>12} | {:>12} | {:>12}",
        "Benchmark", "Base p50", "New p50", "Delta"
    );
    println!("{}", "-".repeat(84));

    let mut matched = 0u32;
    let mut only_base = 0u32;
    let mut only_cand = 0u32;

    // Iterate over candidate results to find matches
    for cand in &candidate.results {
        if let Some(base) = base_map.get(cand.benchmark.as_str()) {
            matched += 1;
            print_comparison(&cand.benchmark, &base.metrics, &cand.metrics);
        } else {
            only_cand += 1;
        }
    }

    // Count base-only
    for base in &baseline.results {
        if !cand_map.contains_key(base.benchmark.as_str()) {
            only_base += 1;
        }
    }

    println!("{}", "-".repeat(84));
    println!(
        "Compared: {} | Baseline only: {} | Candidate only: {}",
        matched, only_base, only_cand
    );
}

fn load_report(path: &str) -> BenchmarkReport {
    let contents = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading {}: {}", path, e);
        std::process::exit(1);
    });
    serde_json::from_str(&contents).unwrap_or_else(|e| {
        eprintln!("Error parsing {}: {}", path, e);
        std::process::exit(1);
    })
}

fn print_comparison(
    name: &str,
    base: &strata_benchmarks::schema::BenchmarkMetrics,
    cand: &strata_benchmarks::schema::BenchmarkMetrics,
) {
    // Compare p50 latency
    if let (Some(base_p50), Some(cand_p50)) = (base.p50_ns, cand.p50_ns) {
        let delta_pct = if base_p50 > 0 {
            ((cand_p50 as f64 - base_p50 as f64) / base_p50 as f64) * 100.0
        } else {
            0.0
        };

        let hint = if delta_pct < -1.0 {
            "faster"
        } else if delta_pct > 1.0 {
            "slower"
        } else {
            "~same"
        };

        println!(
            "{:<40} | {:>12} | {:>12} | {:>+.1}% ({})",
            name,
            format_ns(base_p50),
            format_ns(cand_p50),
            delta_pct,
            hint,
        );
    } else if let (Some(base_ops), Some(cand_ops)) = (base.ops_per_sec, cand.ops_per_sec) {
        // Fallback to ops/sec comparison
        let delta_pct = if base_ops > 0.0 {
            ((cand_ops - base_ops) / base_ops) * 100.0
        } else {
            0.0
        };

        let hint = if delta_pct > 1.0 {
            "faster"
        } else if delta_pct < -1.0 {
            "slower"
        } else {
            "~same"
        };

        println!(
            "{:<40} | {:>10} ops/s | {:>10} ops/s | {:>+.1}% ({})",
            name,
            format_num(base_ops as u64),
            format_num(cand_ops as u64),
            delta_pct,
            hint,
        );
    }
}

fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{} ns", ns)
    } else if ns < 1_000_000 {
        format!("{:.2} us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
