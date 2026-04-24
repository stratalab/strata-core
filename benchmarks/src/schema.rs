//! Shared benchmark result types for cross-SDK compatibility.
//!
//! All SDKs (Rust, Python, Node) should produce JSON files matching these types
//! so results can be compared across implementations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level benchmark report written to a JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Metadata about this run (hardware, git, timestamp).
    pub metadata: RunMetadata,
    /// Individual benchmark results.
    pub results: Vec<BenchmarkResult>,
}

/// Metadata captured at the start of a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    /// ISO 8601 timestamp of the run start.
    pub timestamp: String,
    /// Short git commit hash (empty if not in a git repo).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    /// Git branch name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_branch: Option<String>,
    /// Whether the working tree had uncommitted changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_dirty: Option<bool>,
    /// SDK identifier (e.g. "rust", "python", "node").
    pub sdk: String,
    /// SDK/crate version.
    pub sdk_version: String,
    /// Hardware information.
    pub hardware: HardwareInfo,
}

/// Hardware information for reproducibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareInfo {
    /// CPU model string.
    pub cpu: String,
    /// Number of logical cores.
    pub cores: usize,
    /// Total RAM in GB.
    pub ram_gb: u64,
    /// Operating system.
    pub os: String,
    /// CPU architecture.
    pub arch: String,
}

/// A single benchmark measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Benchmark name (e.g. "kv/put/128B/cache").
    pub benchmark: String,
    /// Category (e.g. "latency", "concurrency", "redis-compare", "fill-level").
    pub category: String,
    /// Benchmark-specific parameters.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub parameters: HashMap<String, serde_json::Value>,
    /// Measured metrics.
    pub metrics: BenchmarkMetrics,
}

/// Metrics collected from a benchmark measurement.
///
/// All fields are optional to support different benchmark types and cross-SDK
/// compatibility. Fields that don't apply are omitted from JSON output.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BenchmarkMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_appends_per_op: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_syncs_per_op: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_rate_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_level: Option<usize>,
}
