//! Vector ANN Performance Benchmarks
//!
//! Measures QPS and recall@10 for the HNSW index at various scales.
//!
//! Usage:
//!   cargo bench --bench vector_benchmarks
//!   cargo bench --bench vector_benchmarks -- --scales 10000
//!   cargo bench --bench vector_benchmarks -- search_100k

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashSet;
use strata_vector::backend::{SegmentCapable, VectorIndexBackend};
use strata_vector::hnsw::HnswConfig;
use strata_vector::segmented::{SegmentedHnswBackend, SegmentedHnswConfig};
use strata_vector::{DistanceMetric, VectorConfig, VectorId};

/// Deterministic pseudo-random embedding generator
fn make_embedding(dim: usize, seed: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| ((seed * dim + j) as f32 / 1000.0).sin())
        .collect()
}

/// Build a SegmentedHnswBackend with n vectors of dimension dim
fn build_index(n: usize, dim: usize) -> SegmentedHnswBackend {
    let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
    let seg_config = SegmentedHnswConfig {
        hnsw: HnswConfig::default(),
        seal_threshold: 50_000,
        heap_flush_threshold: 0, // disable mmap flushing in benchmarks
        auto_compact_threshold: usize::MAX, // disable auto-compact in benchmarks
    };
    let mut backend = SegmentedHnswBackend::new(&config, seg_config);

    for i in 1..=n {
        let emb = make_embedding(dim, i);
        backend.insert(VectorId::new(i as u64), &emb).unwrap();
    }

    backend
}

/// Compute recall@k between two result sets
fn recall_at_k(approx: &[(VectorId, f32)], exact: &[(VectorId, f32)], k: usize) -> f64 {
    let exact_ids: HashSet<VectorId> = exact.iter().take(k).map(|(id, _)| *id).collect();
    let approx_ids: HashSet<VectorId> = approx.iter().take(k).map(|(id, _)| *id).collect();
    let overlap = exact_ids.intersection(&approx_ids).count();
    overlap as f64 / k as f64
}

/// Brute-force search for ground truth
fn brute_force_search(
    backend: &SegmentedHnswBackend,
    query: &[f32],
    k: usize,
) -> Vec<(VectorId, f32)> {
    use strata_vector::brute_force::BruteForceBackend;

    let config = backend.config();
    let mut bf = BruteForceBackend::new(&config);

    for id in backend.vector_ids() {
        if let Some(emb) = backend.get(id) {
            bf.insert(id, emb).unwrap();
        }
    }

    bf.search(query, k)
}

fn bench_search(c: &mut Criterion) {
    let dim = 128;
    let k = 10;
    let num_queries = 50;

    for &n in &[10_000, 100_000] {
        let mut group = c.benchmark_group(format!("ann_search_{}k_{}d", n / 1000, dim));
        group.throughput(Throughput::Elements(num_queries as u64));
        group.sample_size(10);

        let backend = build_index(n, dim);

        // Pre-generate queries
        let queries: Vec<Vec<f32>> = (0..num_queries)
            .map(|q| {
                (0..dim)
                    .map(|i| ((q * dim + i) as f32 / 200.0).cos())
                    .collect()
            })
            .collect();

        group.bench_function(BenchmarkId::new("search", n), |b| {
            b.iter(|| {
                for query in &queries {
                    let _results = backend.search(query, k);
                }
            });
        });

        // Measure recall (not timed)
        if n <= 100_000 {
            let mut total_recall = 0.0;
            for query in &queries[..5.min(queries.len())] {
                let approx = backend.search(query, k);
                let exact = brute_force_search(&backend, query, k);
                total_recall += recall_at_k(&approx, &exact, k);
            }
            let avg_recall = total_recall / 5.0;
            eprintln!("[{n} vectors, {dim}d] Average recall@{k}: {avg_recall:.3}");
        }

        group.finish();
    }
}

fn bench_build(c: &mut Criterion) {
    let dim = 128;

    for &n in &[10_000, 100_000] {
        let mut group = c.benchmark_group(format!("ann_build_{}k_{}d", n / 1000, dim));
        group.sample_size(10);

        group.bench_function(BenchmarkId::new("build", n), |b| {
            b.iter(|| {
                let _backend = build_index(n, dim);
            });
        });

        group.finish();
    }
}

fn bench_compact(c: &mut Criterion) {
    let dim = 128;
    let n = 50_000;
    let k = 10;

    let mut group = c.benchmark_group(format!("ann_compact_{}k_{}d", n / 1000, dim));
    group.sample_size(10);

    group.bench_function("compact_and_search", |b| {
        b.iter_with_setup(
            || build_index(n, dim),
            |mut backend| {
                backend.compact();
                let query = make_embedding(dim, 0);
                let _results = backend.search(&query, k);
            },
        );
    });

    group.finish();
}

criterion_group!(benches, bench_search, bench_build, bench_compact);
criterion_main!(benches);
