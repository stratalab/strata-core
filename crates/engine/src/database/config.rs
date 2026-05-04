//! Database configuration via `strata.toml`
//!
//! Replaces the builder pattern with a simple config file in the data directory.
//! On first open, a default `strata.toml` is created. To change settings,
//! edit the file and restart — same model as Redis.

use crate::{StrataError, StrataResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use strata_security::SensitiveString;
use strata_storage::durability::wal::DurabilityMode;
use strata_storage::runtime_config::StorageRuntimeConfig;

// ============================================================================
// Shadow Collection Names
// ============================================================================

/// Shadow vector collection name for KV store auto-embeddings.
pub const SHADOW_KV: &str = "_system_embed_kv";
/// Shadow vector collection name for JSON store auto-embeddings.
pub const SHADOW_JSON: &str = "_system_embed_json";
/// Shadow vector collection name for event log auto-embeddings.
pub const SHADOW_EVENT: &str = "_system_embed_event";

/// Config file name placed in the database data directory.
pub const CONFIG_FILE_NAME: &str = "strata.toml";

/// Configuration for an external inference model endpoint.
///
/// When present in `StrataConfig`, the search handler uses it to construct
/// a query expander and re-ranker. Persisted in `strata.toml` under the
/// `[model]` section.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelConfig {
    /// OpenAI-compatible API endpoint (e.g. "http://localhost:11434/v1")
    pub endpoint: String,
    /// Model name (e.g. "qwen3:1.7b")
    pub model: String,
    /// Optional API key for authenticated endpoints
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<SensitiveString>,
    /// Request timeout in milliseconds (default: 5000)
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5000
}

/// Public storage configuration.
///
/// Engine owns this serialized product configuration and adapts the
/// storage-facing fields into `StorageRuntimeConfig`. Storage owns the runtime
/// mechanics and effective-value derivation for storage knobs; transaction and
/// database lifecycle policy fields remain engine-owned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    /// Unified public memory budget in bytes for storage data structures.
    ///
    /// When set (> 0), storage derives effective block-cache, write-buffer,
    /// and immutable-memtable runtime values from this unified budget.
    /// Individual storage resource fields are ignored for that derivation.
    ///
    /// Default: 0 (disabled — individual fields take effect).
    #[serde(default)]
    pub memory_budget: usize,
    /// Public advisory branch-limit setting. Default: 1024. Set to 0 for unlimited.
    ///
    /// Storage records this runtime value on the store, but current storage
    /// mechanics do not enforce a branch-creation limit.
    #[serde(default = "default_max_branches")]
    pub max_branches: usize,
    /// Maximum entries in a single transaction's write buffer. Default: 500_000.
    /// Set to 0 for unlimited.
    #[serde(default = "default_max_write_buffer_entries")]
    pub max_write_buffer_entries: usize,
    /// Maximum versions to retain per key. Default: 0 (unlimited).
    /// When set, standard MVCC writes (WriteMode::Append) will prune
    /// versions exceeding this limit. Explicit KeepLast(n) overrides.
    #[serde(default)]
    pub max_versions_per_key: usize,
    /// Public block-cache size in bytes.
    ///
    /// Default: 0 (storage auto-detects at runtime after engine profile
    /// adjustments). Set a non-zero value to request an explicit cache size.
    /// Set `memory_budget` to derive from a unified budget; that takes
    /// precedence over this field.
    #[serde(default)]
    pub block_cache_size: usize,
    /// Public memtable write-buffer size in bytes.
    ///
    /// Storage applies this as the active memtable rotation threshold when
    /// `memory_budget == 0`.
    /// Default: 128 MiB. Set to 0 to disable automatic rotation.
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    /// Public frozen-memtable limit per branch.
    ///
    /// Storage applies this to memtable rotation when `memory_budget == 0`.
    /// Engine also uses the effective storage value to decide when transaction
    /// write-pressure handling should ask storage to flush/compact.
    /// Default: 4. Set to 0 for unlimited.
    #[serde(default = "default_max_immutable_memtables")]
    pub max_immutable_memtables: usize,
    /// L0 file count that triggers engine transaction slowdown.
    ///
    /// This is transaction runtime policy, not storage runtime configuration.
    /// Default: 0 (disabled).
    #[serde(default = "default_l0_slowdown_writes_trigger")]
    pub l0_slowdown_writes_trigger: usize,
    /// L0 file count that makes engine transaction commits wait for compaction.
    ///
    /// This is transaction runtime policy, not storage runtime configuration.
    /// Default: 0 (disabled).
    #[serde(default = "default_l0_stop_writes_trigger")]
    pub l0_stop_writes_trigger: usize,
    /// Number of engine background worker threads for compaction, flush, and
    /// maintenance.
    /// Default: min(4, available CPU cores). On single-core devices set to 1.
    ///
    /// Class: open-time-only. See `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`.
    #[serde(default = "default_background_threads")]
    pub background_threads: usize,
    /// Target size for a single storage output segment file in bytes.
    /// Default: 64 MiB. On embedded devices (Pi), use 4–8 MiB.
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,
    /// Target storage L1 size in bytes. Higher levels are multiplied by 10×.
    /// Default: 256 MiB. On embedded devices (Pi), use 32 MiB.
    #[serde(default = "default_level_base_bytes")]
    pub level_base_bytes: u64,
    /// Storage segment data block size in bytes.
    /// Default: 4096 (4 KiB). Larger blocks improve throughput at the cost of
    /// read amplification.
    #[serde(default = "default_data_block_size")]
    pub data_block_size: usize,
    /// Storage bloom-filter bits per key. Higher values reduce false positives
    /// but use more memory.
    /// Default: 10.
    #[serde(default = "default_bloom_bits_per_key")]
    pub bloom_bits_per_key: usize,
    /// Storage compaction I/O rate limit in bytes per second. 0 = unlimited
    /// (default).
    /// On slow storage (SD cards), set to e.g. 5–10 MB/s to avoid starving user I/O.
    #[serde(default)]
    pub compaction_rate_limit: u64,
    /// Maximum time (milliseconds) an engine transaction can wait for L0 compaction.
    ///
    /// This is transaction runtime policy, not storage runtime configuration.
    /// If exceeded, the write returns an error instead of blocking indefinitely.
    /// Default: 30000 (30 seconds). Set to 0 for unlimited (not recommended).
    #[serde(default = "default_write_stall_timeout_ms")]
    pub write_stall_timeout_ms: u64,
    /// Storage codec for encryption at rest. Default: "identity" (no encryption).
    /// Set to "aes-gcm-256" for AES-256-GCM authenticated encryption.
    /// When using "aes-gcm-256", set the `STRATA_ENCRYPTION_KEY` environment
    /// variable to a 64-character hex string (32 bytes).
    ///
    /// **Warning:** A database created with one codec cannot be opened with a
    /// different codec — the MANIFEST records the codec ID and validates on open.
    /// A second opener with a mismatched codec gets [`StrataError::IncompatibleReuse`].
    ///
    /// Class: open-time-only. See `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`.
    #[serde(default = "default_codec")]
    pub codec: String,
}

fn default_max_branches() -> usize {
    1024
}

fn default_max_write_buffer_entries() -> usize {
    500_000
}

fn default_write_buffer_size() -> usize {
    128 * 1024 * 1024 // 128 MiB
}

fn default_max_immutable_memtables() -> usize {
    4
}

fn default_l0_slowdown_writes_trigger() -> usize {
    0 // disabled — enabling with a rate-limited compaction creates a deadlock:
      // writes stall waiting for L0 to drain, but compaction is throttled.
      // Revisit when adaptive rate limiting is implemented (throttle only under
      // read pressure, run at full speed during write-only phases).
}

fn default_l0_stop_writes_trigger() -> usize {
    0 // disabled — see l0_slowdown rationale above
}

fn default_background_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(4))
        .unwrap_or(1)
}

fn default_target_file_size() -> u64 {
    64 << 20 // 64 MiB
}

fn default_level_base_bytes() -> u64 {
    256 << 20 // 256 MiB
}

fn default_data_block_size() -> usize {
    4096 // 4 KiB
}

fn default_bloom_bits_per_key() -> usize {
    10
}

fn default_write_stall_timeout_ms() -> u64 {
    30_000 // 30 seconds
}

fn default_codec() -> String {
    "identity".to_string()
}

impl StorageConfig {
    /// Effective block cache size, accounting for `memory_budget`.
    pub fn effective_block_cache_size(&self) -> usize {
        storage_runtime_config_from(self).block_cache_configured_bytes()
    }

    /// Effective write buffer size, accounting for `memory_budget`.
    pub fn effective_write_buffer_size(&self) -> usize {
        storage_runtime_config_from(self).write_buffer_size
    }

    /// Effective max immutable memtables, accounting for `memory_budget`.
    pub fn effective_max_immutable_memtables(&self) -> usize {
        storage_runtime_config_from(self).max_immutable_memtables
    }
}

/// Adapt engine-owned public storage config into storage-owned runtime config.
///
/// This passes raw public field values to storage. Storage owns the
/// effective-value derivation for storage resources.
pub(crate) fn storage_runtime_config_from(config: &StorageConfig) -> StorageRuntimeConfig {
    StorageRuntimeConfig::builder()
        .memory_budget(config.memory_budget)
        .block_cache_size(config.block_cache_size)
        .write_buffer_size(config.write_buffer_size)
        .max_branches(config.max_branches)
        .max_versions_per_key(config.max_versions_per_key)
        .max_immutable_memtables(config.max_immutable_memtables)
        .target_file_size(config.target_file_size)
        .level_base_bytes(config.level_base_bytes)
        .data_block_size(config.data_block_size)
        .bloom_bits_per_key(config.bloom_bits_per_key)
        .compaction_rate_limit(config.compaction_rate_limit)
        .build()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memory_budget: 0,
            max_branches: default_max_branches(),
            max_write_buffer_entries: default_max_write_buffer_entries(),
            max_versions_per_key: 0,
            block_cache_size: 0,
            write_buffer_size: default_write_buffer_size(),
            max_immutable_memtables: default_max_immutable_memtables(),
            l0_slowdown_writes_trigger: default_l0_slowdown_writes_trigger(),
            l0_stop_writes_trigger: default_l0_stop_writes_trigger(),
            background_threads: default_background_threads(),
            target_file_size: default_target_file_size(),
            level_base_bytes: default_level_base_bytes(),
            data_block_size: default_data_block_size(),
            bloom_bits_per_key: default_bloom_bits_per_key(),
            compaction_rate_limit: 0,
            write_stall_timeout_ms: default_write_stall_timeout_ms(),
            codec: default_codec(),
        }
    }
}

/// Snapshot retention policy.
///
/// Controls how many on-disk checkpoint snapshots (`snap-NNNNNN.chk`) are kept
/// after each successful checkpoint. The snapshot referenced by the live
/// MANIFEST is always retained regardless of `retain_count` so recovery is
/// never broken by pruning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotRetentionPolicy {
    /// Maximum number of snapshot files to keep.
    ///
    /// Storage pruning preserves at least one snapshot; `0` is accepted for
    /// config-file compatibility and is treated as `1` by the pruning runtime.
    /// Default: 10.
    #[serde(default = "default_snapshot_retain_count")]
    pub retain_count: usize,
}

fn default_snapshot_retain_count() -> usize {
    10
}

impl Default for SnapshotRetentionPolicy {
    fn default() -> Self {
        Self {
            retain_count: default_snapshot_retain_count(),
        }
    }
}

/// Database configuration loaded from `strata.toml`.
///
/// # Example
///
/// ```toml
/// # Durability mode: "standard" (default) or "always"
/// # "standard" = periodic fsync (~100ms), may lose last interval on crash
/// # "always" = fsync every commit, zero data loss
/// durability = "standard"
///
/// # [model]
/// # endpoint = "http://localhost:11434/v1"
/// # model = "qwen3:1.7b"
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrataConfig {
    /// Durability mode: `"standard"` or `"always"` (switchable at runtime).
    /// Cache behavior is selected by opening a cache database, not by this
    /// disk-backed durability setting.
    ///
    /// Class: live-safe (Standard ↔ Always).
    /// See `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`.
    #[serde(default = "default_durability_str")]
    pub durability: String,
    /// Enable automatic text embedding for semantic search.
    #[serde(default)]
    pub auto_embed: bool,
    /// Optional model configuration for query expansion and re-ranking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelConfig>,
    /// Embedding batch size for auto-embed. Default: 512.
    #[serde(
        default = "default_embed_batch_size",
        skip_serializing_if = "Option::is_none"
    )]
    pub embed_batch_size: Option<usize>,

    /// Embedding model name. Default: "miniLM".
    /// Changing after data is indexed requires re-indexing.
    #[serde(default = "default_embed_model")]
    pub embed_model: String,

    // -- Generation provider configuration --
    /// Default generation provider: "local", "anthropic", "openai", or "google".
    #[serde(default = "default_provider")]
    pub provider: String,
    /// Default model name for generation (e.g., "gpt2", "claude-sonnet-4-20250514").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    /// Anthropic (Claude) API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anthropic_api_key: Option<SensitiveString>,
    /// OpenAI (GPT) API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openai_api_key: Option<SensitiveString>,
    /// Google (Gemini) API key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub google_api_key: Option<SensitiveString>,
    /// Storage layer resource limits.
    #[serde(default)]
    pub storage: StorageConfig,
    /// If true, silently start with empty state when WAL recovery fails.
    /// Default: false (refuse to open — safer).
    ///
    /// Class: open-time-only. See `docs/design/architecture-cleanup/durability-recovery-config-matrix.md`.
    #[serde(default)]
    pub allow_lossy_recovery: bool,
    /// If true, open is permitted when storage recovery only observed
    /// `DegradationClass::PolicyDowngrade` (currently emitted by the
    /// no-`segments.manifest` legacy-L0-promotion fallback). `DataLoss`
    /// is still refused unless `allow_lossy_recovery` is also set.
    /// Default: false.
    ///
    /// Class: open-time-only.
    #[serde(default)]
    pub allow_missing_manifest: bool,
    /// Whether the user opted in to anonymous usage telemetry.
    /// Default: false (opt-in, never on by default).
    #[serde(default)]
    pub telemetry: bool,
    /// Default storage data type for new vector collections.
    /// "f32" (default) or "int8" (scalar quantization, 4x memory savings).
    /// Individual collections can override via VectorConfig.
    #[serde(default = "default_vector_dtype")]
    pub default_vector_dtype: String,
    /// Snapshot retention policy. Engine owns the public retention setting;
    /// storage owns pruning mechanics. The live MANIFEST snapshot is always
    /// retained.
    #[serde(default)]
    pub snapshot_retention: SnapshotRetentionPolicy,
}

fn default_durability_str() -> String {
    "standard".to_string()
}

fn default_embed_batch_size() -> Option<usize> {
    Some(512)
}

fn default_embed_model() -> String {
    "miniLM".to_string()
}

fn default_provider() -> String {
    "local".to_string()
}

fn default_vector_dtype() -> String {
    "f32".to_string()
}

impl Default for StrataConfig {
    fn default() -> Self {
        Self {
            durability: default_durability_str(),
            auto_embed: false,
            model: None,
            embed_batch_size: Some(512),
            embed_model: default_embed_model(),
            provider: default_provider(),
            default_model: None,
            anthropic_api_key: None,
            openai_api_key: None,
            google_api_key: None,
            storage: StorageConfig::default(),
            allow_lossy_recovery: false,
            allow_missing_manifest: false,
            telemetry: false,
            default_vector_dtype: default_vector_dtype(),
            snapshot_retention: SnapshotRetentionPolicy::default(),
        }
    }
}

/// Restrict config file to owner-only read/write (0o600).
///
/// The config file may contain API keys, so we propagate permission
/// errors rather than ignoring them.
#[cfg(unix)]
fn restrict_config_permissions(path: &std::path::Path) -> StrataResult<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        StrataError::internal(format!(
            "Failed to restrict permissions on '{}': {}",
            path.display(),
            e
        ))
    })
}

#[cfg(not(unix))]
fn restrict_config_permissions(_path: &std::path::Path) -> StrataResult<()> {
    Ok(())
}

impl StrataConfig {
    /// Parse the default_vector_dtype string into a StorageDtype.
    /// Returns F32 for unrecognized values.
    pub fn vector_storage_dtype(&self) -> crate::StorageDtype {
        match self.default_vector_dtype.to_lowercase().as_str() {
            "int8" | "sq8" => crate::StorageDtype::Int8,
            "binary" | "rabitq" => crate::StorageDtype::Binary,
            _ => crate::StorageDtype::F32,
        }
    }

    /// Parse the durability string into a `DurabilityMode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not `"standard"` or `"always"`.
    pub fn durability_mode(&self) -> StrataResult<DurabilityMode> {
        match self.durability.as_str() {
            "standard" => Ok(DurabilityMode::standard_default()),
            "always" => Ok(DurabilityMode::Always),
            other => Err(StrataError::invalid_input(format!(
                "Invalid durability mode '{}' in strata.toml. Expected \"standard\" or \"always\". Cache mode is selected via Database::cache() or OpenSpec::cache().",
                other
            ))),
        }
    }

    /// Returns the default config file content with comments.
    pub fn default_toml() -> &'static str {
        r#"# Strata database configuration
#
# Durability mode: "standard" (default) or "always"
#   "standard" = periodic fsync (~100ms), may lose last interval on crash
#   "always"   = fsync every commit, zero data loss
# Switchable at runtime via CONFIG SET durability.
# Cache mode is selected at open time via Database::cache(), not here.
durability = "standard"

# Auto-embed: automatically generate embeddings for text data (default: false)
# Requires the "embed" feature to be compiled in.
auto_embed = false

# Embedding batch size for auto-embed (default: 512).
# Increase for bulk ingestion, decrease for interactive use.
# embed_batch_size = 512

# BM25 scoring parameters (k1, b) are configured per-recipe, not globally.
# See the recipe system: db.set_recipe("name", { "retrieve": { "bm25": { "k1": 0.9, "b": 0.4 } } })

# Embedding model: "miniLM" (384d, default), "nomic-embed" (768d),
# "bge-m3" (1024d), or "gemma-embed" (768d).
# Changing after data is indexed requires re-indexing.
# embed_model = "miniLM"

# Model configuration for query expansion and re-ranking.
# Uncomment and configure to enable intelligent search features.
# [model]
# endpoint = "http://localhost:11434/v1"
# model = "qwen3:1.7b"
# api_key = "your-api-key"      # optional
# timeout_ms = 5000              # optional, default 5000

# Generation provider: "local" (default), "anthropic", "openai", or "google".
# provider = "local"
# default_model = "gpt2"
# anthropic_api_key = "sk-ant-..."
# openai_api_key = "sk-..."
# google_api_key = "AIza..."

# Default storage type for new vector collections: "f32" (default), "int8", or "binary".
# "int8" uses scalar quantization (SQ8) for 4x memory savings (~1-2% recall loss).
# "binary" uses RaBitQ binary quantization for 32x compression (~5% recall loss).
# Embedded profile auto-sets "int8". Individual collections can override.
# default_vector_dtype = "f32"

# Storage resource limits.
# [storage]
# memory_budget = 0             # 0 = disabled; when >0, storage derives cache/buffer/immutable values.
# max_branches = 1024          # advisory; stored by storage, not yet enforced
# max_write_buffer_entries = 500000 # per-transaction coordinator limit
# max_versions_per_key = 0    # 0 = unlimited; set to e.g. 100 to cap MVCC history
# block_cache_size = 0          # 0 = storage auto-detect; nonzero = explicit bytes
# write_buffer_size = 134217728  # 128 MiB; memtable rotation threshold
# max_immutable_memtables = 4   # max frozen memtables per branch before write stalling
# l0_slowdown_writes_trigger = 0 # engine transaction slowdown disabled by default
# l0_stop_writes_trigger = 0     # engine transaction stall disabled by default
# background_threads = 4        # compaction/flush workers; default min(4, CPU cores)
# target_file_size = 67108864   # 64 MiB; segment file target (Pi: 4-8 MiB)
# level_base_bytes = 268435456  # 256 MiB; L1 target size (Pi: 32 MiB)
# data_block_size = 4096        # 4 KiB; segment data block size
# bloom_bits_per_key = 10       # bloom filter bits per key
# compaction_rate_limit = 0     # 0 = unlimited; bytes/sec cap for compaction I/O
# write_stall_timeout_ms = 30000 # engine transaction L0 stall timeout

# Snapshot retention. After each successful checkpoint, snapshot files older
# than the retain window are pruned. The snapshot referenced by the live
# MANIFEST is always retained.
# [snapshot_retention]
# retain_count = 10             # keep last N snap-NNNNNN.chk files
"#
    }

    /// Read and parse config from a file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file(path: &Path) -> StrataResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            StrataError::internal(format!(
                "Failed to read config file '{}': {}",
                path.display(),
                e
            ))
        })?;
        let mut config: StrataConfig = toml::from_str(&content).map_err(|e| {
            StrataError::invalid_input(format!(
                "Failed to parse config file '{}': {}",
                path.display(),
                e
            ))
        })?;
        // Validate the durability value eagerly
        config.durability_mode()?;
        // Environment variables override config file values (12-factor app pattern).
        // This avoids storing secrets in plaintext config files.
        config.apply_env_overrides();
        Ok(config)
    }

    /// Override API key fields with environment variables, if set.
    ///
    /// Recognized variables: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`.
    fn apply_env_overrides(&mut self) {
        for (env_var, field) in [
            ("ANTHROPIC_API_KEY", &mut self.anthropic_api_key),
            ("OPENAI_API_KEY", &mut self.openai_api_key),
            ("GOOGLE_API_KEY", &mut self.google_api_key),
        ] {
            if let Ok(val) = std::env::var(env_var) {
                if !val.is_empty() {
                    *field = Some(SensitiveString::from(val));
                }
            }
        }
    }

    /// Write the default config file if it does not already exist.
    ///
    /// Returns `Ok(())` whether the file was created or already existed.
    pub fn write_default_if_missing(path: &Path) -> StrataResult<()> {
        if !path.exists() {
            atomic_write_config(path, Self::default_toml().as_bytes())?;
            restrict_config_permissions(path)?;
        }
        Ok(())
    }

    /// Serialize this config to TOML and write it to the given path.
    pub fn write_to_file(&self, path: &Path) -> StrataResult<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| StrataError::internal(format!("Failed to serialize config: {}", e)))?;
        atomic_write_config(path, content.as_bytes())?;
        restrict_config_permissions(path)
    }
}

/// Crash-safe write of `strata.toml` content via temp-file + atomic rename
/// (closes DG-018: atomicity + durability).
///
/// **Atomicity:** Reader of `strata.toml` never observes a partially-written
/// file — the rename is atomic. Crash mid-write leaves either the previous
/// content or the new content, never garbage. This matches the publish
/// pattern used by MANIFEST and segment files
/// (`crates/storage/src/manifest.rs`, `crates/storage/src/segment_builder.rs`).
fn atomic_write_config(path: &Path, content: &[u8]) -> StrataResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            StrataError::internal(format!(
                "Failed to create config directory '{}': {}",
                parent.display(),
                e
            ))
        })?;
    }

    let tmp_path = path.with_extension("toml.tmp");

    {
        use std::io::Write;

        let mut file = std::fs::File::create(&tmp_path).map_err(|e| {
            StrataError::internal(format!(
                "Failed to create temp config file '{}': {}",
                tmp_path.display(),
                e
            ))
        })?;
        file.write_all(content).map_err(|e| {
            StrataError::internal(format!(
                "Failed to write temp config file '{}': {}",
                tmp_path.display(),
                e
            ))
        })?;
        file.sync_all().map_err(|e| {
            StrataError::internal(format!(
                "Failed to fsync temp config file '{}': {}",
                tmp_path.display(),
                e
            ))
        })?;
    }

    std::fs::rename(&tmp_path, path).map_err(|e| {
        StrataError::internal(format!(
            "Failed to rename temp config '{}' to '{}': {}",
            tmp_path.display(),
            path.display(),
            e
        ))
    })?;

    let dir_path = path.parent().unwrap_or(Path::new("."));
    std::fs::File::open(dir_path)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| {
            StrataError::internal(format!(
                "Failed to fsync config directory '{}': {}",
                dir_path.display(),
                e
            ))
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn default_config_is_standard() {
        let config = StrataConfig::default();
        assert_eq!(config.durability, "standard");
        let mode = config.durability_mode().unwrap();
        assert!(matches!(mode, DurabilityMode::Standard { .. }));
    }

    #[test]
    fn parse_standard() {
        let config: StrataConfig = toml::from_str("durability = \"standard\"").unwrap();
        assert!(matches!(
            config.durability_mode().unwrap(),
            DurabilityMode::Standard { .. }
        ));
    }

    #[test]
    fn parse_always() {
        let config: StrataConfig = toml::from_str("durability = \"always\"").unwrap();
        assert_eq!(config.durability_mode().unwrap(), DurabilityMode::Always);
    }

    #[test]
    fn parse_invalid_mode_returns_error() {
        let config: StrataConfig = toml::from_str("durability = \"turbo\"").unwrap();
        assert!(config.durability_mode().is_err());
    }

    #[test]
    fn default_toml_parses_correctly() {
        let config: StrataConfig = toml::from_str(StrataConfig::default_toml()).unwrap();
        assert_eq!(config.durability, "standard");
    }

    #[test]
    fn default_toml_documents_residual_storage_config_owners() {
        let default_toml = StrataConfig::default_toml();

        assert!(default_toml.contains("# l0_slowdown_writes_trigger = 0"));
        assert!(default_toml.contains("# l0_stop_writes_trigger = 0"));
        assert!(default_toml.contains("# write_stall_timeout_ms = 30000"));
        assert!(default_toml.contains("per-transaction coordinator limit"));
        assert!(default_toml.contains("storage auto-detect"));
        assert!(!default_toml.contains(concat!("Actual", " RSS")));
        assert!(!default_toml.contains(concat!("Embedded  (< 1 GiB RAM): ", "min")));
    }

    #[test]
    fn write_default_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);
        assert!(!path.exists());

        StrataConfig::write_default_if_missing(&path).unwrap();
        assert!(path.exists());

        let config = StrataConfig::from_file(&path).unwrap();
        assert_eq!(config.durability, "standard");
    }

    #[test]
    fn write_default_does_not_overwrite() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        // Write custom config
        std::fs::write(&path, "durability = \"always\"\n").unwrap();

        // write_default_if_missing should not overwrite
        StrataConfig::write_default_if_missing(&path).unwrap();

        let config = StrataConfig::from_file(&path).unwrap();
        assert_eq!(config.durability, "always");
    }

    /// DG-018: atomic write must not leave a `*.toml.tmp` artifact behind.
    #[test]
    fn write_to_file_leaves_no_tmp_artifact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        let cfg = StrataConfig::default();
        cfg.write_to_file(&path).unwrap();
        assert!(path.exists(), "config file must be present");

        let tmp = path.with_extension("toml.tmp");
        assert!(!tmp.exists(), "atomic write must rename away the .tmp file");

        // Re-write with mutated content; same invariant.
        let mut cfg2 = StrataConfig::default();
        cfg2.snapshot_retention.retain_count = 42;
        cfg2.write_to_file(&path).unwrap();
        assert!(!tmp.exists());
        let reloaded = StrataConfig::from_file(&path).unwrap();
        assert_eq!(reloaded.snapshot_retention.retain_count, 42);
    }

    /// DG-018: `write_default_if_missing` also takes the atomic path.
    #[test]
    fn write_default_leaves_no_tmp_artifact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        StrataConfig::write_default_if_missing(&path).unwrap();
        assert!(path.exists());
        let tmp = path.with_extension("toml.tmp");
        assert!(!tmp.exists());
    }

    #[test]
    fn from_file_with_missing_field_uses_default() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        // Empty config file — all fields should use defaults
        std::fs::write(&path, "").unwrap();

        let config = StrataConfig::from_file(&path).unwrap();
        assert_eq!(config.durability, "standard");
    }

    #[test]
    fn model_config_round_trip() {
        let config = StrataConfig {
            durability: "always".to_string(),
            auto_embed: true,
            model: Some(ModelConfig {
                endpoint: "http://localhost:11434/v1".to_string(),
                model: "qwen3:1.7b".to_string(),
                api_key: Some(SensitiveString::from("sk-test")),
                timeout_ms: 3000,
            }),
            ..StrataConfig::default()
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.durability, "always");
        assert!(parsed.auto_embed);
        let model = parsed.model.unwrap();
        assert_eq!(model.endpoint, "http://localhost:11434/v1");
        assert_eq!(model.model, "qwen3:1.7b");
        assert_eq!(model.api_key.as_deref(), Some("sk-test"));
        assert_eq!(model.timeout_ms, 3000);
    }

    #[test]
    fn model_config_round_trip_without_model() {
        let config = StrataConfig {
            durability: "standard".to_string(),
            auto_embed: false,
            model: None,
            ..StrataConfig::default()
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.durability, "standard");
        assert!(!parsed.auto_embed);
        assert!(parsed.model.is_none());
        // Verify the [model] section is not present in serialized output
        assert!(!toml_str.contains("[model]"));
    }

    #[test]
    fn backward_compat_old_config_without_model() {
        // Old config files won't have a [model] section
        let old_toml = r#"
durability = "always"
auto_embed = true
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.durability, "always");
        assert!(config.auto_embed);
        assert!(config.model.is_none());
    }

    #[test]
    fn model_config_default_timeout() {
        let toml_str = r#"
[model]
endpoint = "http://localhost:11434/v1"
model = "qwen3:1.7b"
"#;
        let config: StrataConfig = toml::from_str(toml_str).unwrap();
        let model = config.model.unwrap();
        assert_eq!(model.timeout_ms, 5000); // default
        assert!(model.api_key.is_none());
    }

    #[test]
    fn write_to_file_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        let config = StrataConfig {
            durability: "always".to_string(),
            auto_embed: true,
            model: Some(ModelConfig {
                endpoint: "http://localhost:8080/v1".to_string(),
                model: "llama3".to_string(),
                api_key: None,
                timeout_ms: 5000,
            }),
            ..StrataConfig::default()
        };

        config.write_to_file(&path).unwrap();
        let loaded = StrataConfig::from_file(&path).unwrap();
        assert_eq!(loaded.durability, "always");
        assert!(loaded.auto_embed);
        let model = loaded.model.unwrap();
        assert_eq!(model.endpoint, "http://localhost:8080/v1");
        assert_eq!(model.model, "llama3");
    }

    // -----------------------------------------------------------------------
    // Generation provider config
    // -----------------------------------------------------------------------

    #[test]
    fn default_provider_is_local() {
        let config = StrataConfig::default();
        assert_eq!(config.provider, "local");
        assert!(config.default_model.is_none());
        assert!(config.anthropic_api_key.is_none());
        assert!(config.openai_api_key.is_none());
        assert!(config.google_api_key.is_none());
    }

    #[test]
    fn provider_config_round_trip() {
        let config = StrataConfig {
            provider: "anthropic".to_string(),
            default_model: Some("claude-sonnet-4-20250514".to_string()),
            anthropic_api_key: Some(SensitiveString::from("sk-ant-test-key")),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.provider, "anthropic");
        assert_eq!(
            parsed.default_model.as_deref(),
            Some("claude-sonnet-4-20250514")
        );
        assert_eq!(parsed.anthropic_api_key.as_deref(), Some("sk-ant-test-key"));
        assert!(parsed.openai_api_key.is_none());
        assert!(parsed.google_api_key.is_none());
    }

    #[test]
    fn provider_config_all_keys_round_trip() {
        let config = StrataConfig {
            provider: "openai".to_string(),
            default_model: Some("gpt-4".to_string()),
            anthropic_api_key: Some(SensitiveString::from("sk-ant-key")),
            openai_api_key: Some(SensitiveString::from("sk-openai-key")),
            google_api_key: Some(SensitiveString::from("AIza-key")),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.provider, "openai");
        assert_eq!(parsed.default_model.as_deref(), Some("gpt-4"));
        assert_eq!(parsed.anthropic_api_key.as_deref(), Some("sk-ant-key"));
        assert_eq!(parsed.openai_api_key.as_deref(), Some("sk-openai-key"));
        assert_eq!(parsed.google_api_key.as_deref(), Some("AIza-key"));
    }

    #[test]
    fn provider_config_backward_compat() {
        // Old config files without provider fields should parse fine
        let old_toml = r#"
durability = "standard"
auto_embed = false
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.provider, "local");
        assert!(config.default_model.is_none());
        assert!(config.anthropic_api_key.is_none());
    }

    #[test]
    fn provider_config_omits_none_keys_in_serialization() {
        let config = StrataConfig::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        assert!(!toml_str.contains("default_model"));
        assert!(!toml_str.contains("anthropic_api_key"));
        assert!(!toml_str.contains("openai_api_key"));
        assert!(!toml_str.contains("google_api_key"));
        // provider should always be present (not Option)
        assert!(toml_str.contains("provider"));
    }

    // -----------------------------------------------------------------------
    // Embed model config
    // -----------------------------------------------------------------------

    #[test]
    fn default_embed_model_is_minilm() {
        let config = StrataConfig::default();
        assert_eq!(config.embed_model, "miniLM");
    }

    #[test]
    fn embed_model_backward_compat() {
        // Old config files without embed_model should parse and default to "miniLM"
        let old_toml = r#"
durability = "standard"
auto_embed = true
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.embed_model, "miniLM");
    }

    #[test]
    fn embed_model_round_trip() {
        let config = StrataConfig {
            embed_model: "nomic-embed".to_string(),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.embed_model, "nomic-embed");
    }

    #[test]
    fn embed_model_persists_to_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        let config = StrataConfig {
            embed_model: "bge-m3".to_string(),
            ..StrataConfig::default()
        };
        config.write_to_file(&path).unwrap();

        let loaded = StrataConfig::from_file(&path).unwrap();
        assert_eq!(loaded.embed_model, "bge-m3");
    }

    #[test]
    fn embed_model_always_serialized() {
        // embed_model is a String (not Option), so it's always in TOML output
        let config = StrataConfig::default();
        let toml_str = toml::to_string_pretty(&config).unwrap();
        assert!(
            toml_str.contains("embed_model"),
            "embed_model should always be serialized: {}",
            toml_str
        );
        assert!(toml_str.contains("miniLM"));
    }

    #[test]
    fn provider_config_persists_to_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        let config = StrataConfig {
            provider: "google".to_string(),
            default_model: Some("gemini-pro".to_string()),
            google_api_key: Some(SensitiveString::from("AIza-test")),
            ..StrataConfig::default()
        };
        config.write_to_file(&path).unwrap();

        let loaded = StrataConfig::from_file(&path).unwrap();
        assert_eq!(loaded.provider, "google");
        assert_eq!(loaded.default_model.as_deref(), Some("gemini-pro"));
        assert_eq!(loaded.google_api_key.as_deref(), Some("AIza-test"));
    }

    // -----------------------------------------------------------------------
    // Cache is an open mode, not a disk durability setting
    // -----------------------------------------------------------------------

    #[test]
    fn parse_cache_durability_is_rejected() {
        let config: StrataConfig = toml::from_str("durability = \"cache\"").unwrap();
        let error = config.durability_mode().unwrap_err();
        assert!(error.to_string().contains("Database::cache()"));
    }

    #[test]
    fn cache_durability_round_trip_still_rejects_runtime_mode() {
        let config = StrataConfig {
            durability: "cache".to_string(),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.durability, "cache");
        assert!(parsed.durability_mode().is_err());
    }

    // -----------------------------------------------------------------------
    // embed_batch_size default
    // -----------------------------------------------------------------------

    #[test]
    fn default_embed_batch_size_is_512() {
        let config = StrataConfig::default();
        assert_eq!(config.embed_batch_size, Some(512));
    }

    #[test]
    fn embed_batch_size_backward_compat_missing_field() {
        // Old config files without embed_batch_size should default to Some(512)
        let old_toml = r#"
durability = "standard"
auto_embed = false
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.embed_batch_size, Some(512));
    }

    #[test]
    fn embed_batch_size_explicit_override() {
        let toml_str = "embed_batch_size = 256\n";
        let config: StrataConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.embed_batch_size, Some(256));
    }

    #[test]
    fn embed_batch_size_persists_to_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);

        let config = StrataConfig {
            embed_batch_size: Some(1024),
            ..StrataConfig::default()
        };
        config.write_to_file(&path).unwrap();

        let loaded = StrataConfig::from_file(&path).unwrap();
        assert_eq!(loaded.embed_batch_size, Some(1024));
    }

    // -----------------------------------------------------------------------
    // Storage config
    // -----------------------------------------------------------------------

    #[test]
    fn test_storage_config_defaults() {
        let config = StrataConfig::default();
        assert_eq!(config.storage.max_branches, 1024);
        assert_eq!(config.storage.max_write_buffer_entries, 500_000);
        assert_eq!(config.storage.max_versions_per_key, 0);
        assert_eq!(config.storage.write_buffer_size, 128 * 1024 * 1024);
        assert_eq!(config.storage.max_immutable_memtables, 4);
        assert_eq!(config.storage.l0_slowdown_writes_trigger, 0);
        assert_eq!(config.storage.l0_stop_writes_trigger, 0);
        assert_eq!(config.storage.write_stall_timeout_ms, 30_000);
    }

    #[test]
    fn test_storage_config_backward_compat() {
        // Old config files without [storage] section should parse fine
        let old_toml = r#"
durability = "standard"
auto_embed = false
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.storage.max_branches, 1024);
        assert_eq!(config.storage.max_write_buffer_entries, 500_000);
        assert_eq!(config.storage.max_versions_per_key, 0);
        assert_eq!(config.storage.write_buffer_size, 128 * 1024 * 1024);
        assert_eq!(config.storage.max_immutable_memtables, 4);
    }

    #[test]
    fn test_write_buffer_size_round_trip() {
        let toml_str = r#"
[storage]
write_buffer_size = 67108864
max_immutable_memtables = 8
"#;
        let config: StrataConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.write_buffer_size, 64 * 1024 * 1024);
        assert_eq!(config.storage.max_immutable_memtables, 8);

        let serialized = toml::to_string_pretty(&config).unwrap();
        let reparsed: StrataConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.storage.write_buffer_size, 64 * 1024 * 1024);
        assert_eq!(reparsed.storage.max_immutable_memtables, 8);
    }

    #[test]
    fn test_max_versions_per_key_default_zero() {
        let config = StrataConfig::default();
        assert_eq!(
            config.storage.max_versions_per_key, 0,
            "default must be 0 (unlimited) for backward compat"
        );
    }

    #[test]
    fn storage_runtime_adapter_ignores_engine_owned_and_non_runtime_fields() {
        let base = StorageConfig::default();
        let mut changed = base.clone();
        changed.max_write_buffer_entries = base.max_write_buffer_entries.saturating_add(17);
        changed.l0_slowdown_writes_trigger = 3;
        changed.l0_stop_writes_trigger = 7;
        changed.background_threads = base.background_threads.saturating_add(5).max(1);
        changed.write_stall_timeout_ms = base.write_stall_timeout_ms.saturating_add(11);
        changed.codec = "aes-gcm-256".to_string();

        assert_eq!(
            storage_runtime_config_from(&base),
            storage_runtime_config_from(&changed),
            "engine-owned policy and non-runtime fields must not be absorbed into StorageRuntimeConfig"
        );
    }

    // -----------------------------------------------------------------------
    // allow_lossy_recovery config
    // -----------------------------------------------------------------------

    #[test]
    fn test_allow_lossy_recovery_defaults_to_false() {
        let config = StrataConfig::default();
        assert!(!config.allow_lossy_recovery);
    }

    #[test]
    fn test_allow_lossy_recovery_backward_compat() {
        // Old config files without allow_lossy_recovery should default to false
        let old_toml = r#"
durability = "standard"
auto_embed = false
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert!(
            !config.allow_lossy_recovery,
            "Missing allow_lossy_recovery should default to false (safe)"
        );
    }

    #[test]
    fn test_allow_lossy_recovery_round_trip() {
        let config = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert!(parsed.allow_lossy_recovery);
    }

    // -----------------------------------------------------------------------
    // Issue #1737: Scale-span config gaps
    // -----------------------------------------------------------------------

    #[test]
    fn test_issue_1737_background_threads_default_bounded() {
        // S-H2: background_threads must default to min(4, available_parallelism)
        let config = StorageConfig::default();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let expected = cpus.min(4);
        assert_eq!(
            config.background_threads, expected,
            "background_threads should default to min(4, available_parallelism()={})",
            cpus
        );
    }

    #[test]
    fn test_issue_1737_storage_constants_configurable() {
        // S-H8: critical storage constants must be configurable via StorageConfig
        let config = StorageConfig::default();
        assert_eq!(config.target_file_size, 64 * 1024 * 1024); // 64 MiB
        assert_eq!(config.level_base_bytes, 256 * 1024 * 1024); // 256 MiB
        assert_eq!(config.data_block_size, 4096); // 4 KiB
        assert_eq!(config.bloom_bits_per_key, 10);
    }

    #[test]
    fn test_issue_1737_compaction_rate_limit_in_config() {
        // S-M7: compaction_rate_limit must be configurable via strata.toml
        let config = StorageConfig::default();
        assert_eq!(config.compaction_rate_limit, 0, "default 0 = unlimited");
    }

    #[test]
    fn test_issue_1737_config_round_trip() {
        // All new fields must survive TOML serialization round-trip
        let toml_str = r#"
[storage]
background_threads = 2
target_file_size = 8388608
level_base_bytes = 33554432
data_block_size = 8192
bloom_bits_per_key = 12
compaction_rate_limit = 10485760
"#;
        let config: StrataConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.background_threads, 2);
        assert_eq!(config.storage.target_file_size, 8 * 1024 * 1024);
        assert_eq!(config.storage.level_base_bytes, 32 * 1024 * 1024);
        assert_eq!(config.storage.data_block_size, 8192);
        assert_eq!(config.storage.bloom_bits_per_key, 12);
        assert_eq!(config.storage.compaction_rate_limit, 10 * 1024 * 1024);

        let serialized = toml::to_string_pretty(&config).unwrap();
        let reparsed: StrataConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.storage.background_threads, 2);
        assert_eq!(reparsed.storage.target_file_size, 8 * 1024 * 1024);
        assert_eq!(reparsed.storage.level_base_bytes, 32 * 1024 * 1024);
        assert_eq!(reparsed.storage.data_block_size, 8192);
        assert_eq!(reparsed.storage.bloom_bits_per_key, 12);
        assert_eq!(reparsed.storage.compaction_rate_limit, 10 * 1024 * 1024);
    }

    #[test]
    fn test_issue_1737_backward_compat_missing_new_fields() {
        // Old config files without the new fields must still parse with correct defaults
        let old_toml = r#"
durability = "standard"
[storage]
max_branches = 512
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.storage.max_branches, 512);
        // New fields should have their defaults
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(config.storage.background_threads, cpus.min(4));
        assert_eq!(config.storage.target_file_size, 64 * 1024 * 1024);
        assert_eq!(config.storage.level_base_bytes, 256 * 1024 * 1024);
        assert_eq!(config.storage.data_block_size, 4096);
        assert_eq!(config.storage.bloom_bits_per_key, 10);
        assert_eq!(config.storage.compaction_rate_limit, 0);
    }

    #[cfg(unix)]
    #[test]
    fn write_to_file_sets_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);
        let config = StrataConfig::default();
        config.write_to_file(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "strata.toml should be owner-only (0o600)");
    }

    #[test]
    fn env_var_overrides_api_keys() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CONFIG_FILE_NAME);
        std::fs::write(
            &path,
            "durability = \"standard\"\nanthropic_api_key = \"file-key\"\n",
        )
        .unwrap();

        // Set env vars
        unsafe {
            std::env::set_var("ANTHROPIC_API_KEY", "env-key");
            std::env::set_var("OPENAI_API_KEY", "env-openai");
        }

        let config = StrataConfig::from_file(&path).unwrap();

        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("OPENAI_API_KEY");
        }

        // Env var overrides file value
        assert_eq!(config.anthropic_api_key.as_deref(), Some("env-key"));
        // Env var sets value even when file had none
        assert_eq!(config.openai_api_key.as_deref(), Some("env-openai"));
        // Unset env var leaves file value alone
        assert!(config.google_api_key.is_none());
    }

    // -----------------------------------------------------------------------
    // memory_budget (issue #2184)
    // -----------------------------------------------------------------------

    #[test]
    fn memory_budget_default_zero() {
        let cfg = StorageConfig::default();
        assert_eq!(cfg.memory_budget, 0);
    }

    #[test]
    fn memory_budget_derives_effective_values() {
        let cfg = StorageConfig {
            memory_budget: 32 << 20, // 32 MiB
            ..StorageConfig::default()
        };
        assert_eq!(cfg.effective_block_cache_size(), 16 << 20);
        assert_eq!(cfg.effective_write_buffer_size(), 8 << 20);
        assert_eq!(cfg.effective_max_immutable_memtables(), 1);
        // Total: 16 + 8*2 = 32 MiB = budget
        let total = cfg.effective_block_cache_size()
            + cfg.effective_write_buffer_size() * (1 + cfg.effective_max_immutable_memtables());
        assert_eq!(total, 32 << 20);
    }

    #[test]
    fn memory_budget_zero_uses_individual_fields() {
        let cfg = StorageConfig {
            memory_budget: 0,
            block_cache_size: 64 << 20,
            write_buffer_size: 32 << 20,
            max_immutable_memtables: 3,
            ..StorageConfig::default()
        };
        assert_eq!(cfg.effective_block_cache_size(), 64 << 20);
        assert_eq!(cfg.effective_write_buffer_size(), 32 << 20);
        assert_eq!(cfg.effective_max_immutable_memtables(), 3);
    }

    #[test]
    fn memory_budget_toml_round_trip() {
        let toml_str = r#"
[storage]
memory_budget = 33554432
"#;
        let config: StrataConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.memory_budget, 32 << 20);

        let serialized = toml::to_string_pretty(&config).unwrap();
        let reparsed: StrataConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(reparsed.storage.memory_budget, 32 << 20);
    }

    #[test]
    fn memory_budget_backward_compat() {
        let old_toml = r#"
durability = "standard"
[storage]
max_branches = 512
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert_eq!(config.storage.memory_budget, 0);
        // All effective values match raw fields when budget is 0
        assert_eq!(
            config.storage.effective_write_buffer_size(),
            config.storage.write_buffer_size
        );
    }
}
