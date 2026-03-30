//! Database configuration via `strata.toml`
//!
//! Replaces the builder pattern with a simple config file in the data directory.
//! On first open, a default `strata.toml` is created. To change settings,
//! edit the file and restart — same model as Redis.

use serde::{Deserialize, Serialize};
use std::path::Path;
use strata_core::{StrataError, StrataResult};
use strata_durability::wal::DurabilityMode;
use strata_security::SensitiveString;

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

/// Storage layer resource limits.
///
/// Controls maximum branches and per-transaction write buffer sizes.
/// All limits default to generous values suitable for production use.
/// Set to `0` for unlimited (backward-compatible default).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    /// Maximum number of branches allowed. Default: 1024. Set to 0 for unlimited.
    #[serde(default = "default_max_branches")]
    pub max_branches: usize,
    /// Maximum entries in a single transaction's write buffer. Default: 500_000. Set to 0 for unlimited.
    #[serde(default = "default_max_write_buffer_entries")]
    pub max_write_buffer_entries: usize,
    /// Maximum versions to retain per key. Default: 0 (unlimited).
    /// When set, standard MVCC writes (WriteMode::Append) will prune
    /// versions exceeding this limit. Explicit KeepLast(n) overrides.
    #[serde(default)]
    pub max_versions_per_key: usize,
    /// Block cache size in bytes. Caches decompressed segment data blocks.
    /// Default: 0 (auto-detect from available RAM: max(256 MiB, available / 4)).
    #[serde(default)]
    pub block_cache_size: usize,
    /// Memtable write buffer size in bytes. When a branch's active memtable
    /// exceeds this threshold, it is frozen and a new active memtable is swapped in.
    /// Default: 128 MiB. Set to 0 to disable automatic rotation.
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    /// Maximum number of frozen (immutable) memtables allowed per branch before
    /// write stalling kicks in. Default: 4. Set to 0 for unlimited.
    #[serde(default = "default_max_immutable_memtables")]
    pub max_immutable_memtables: usize,
    /// L0 file count that triggers write slowdown (1 ms yield per write).
    /// Mirrors RocksDB's `level0_slowdown_writes_trigger`. Default: 20.
    #[serde(default = "default_l0_slowdown_writes_trigger")]
    pub l0_slowdown_writes_trigger: usize,
    /// L0 file count that completely stalls writes until compaction catches up.
    /// Mirrors RocksDB's `level0_stop_writes_trigger`. Default: 36.
    #[serde(default = "default_l0_stop_writes_trigger")]
    pub l0_stop_writes_trigger: usize,
    /// Number of background worker threads for compaction, flush, and maintenance.
    /// Default: min(4, available CPU cores). On single-core devices set to 1.
    #[serde(default = "default_background_threads")]
    pub background_threads: usize,
    /// Target size for a single output segment file in bytes.
    /// Default: 64 MiB. On embedded devices (Pi), use 4–8 MiB.
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,
    /// Target total size for L1 in bytes. Higher levels are multiplied by 10×.
    /// Default: 256 MiB. On embedded devices (Pi), use 32 MiB.
    #[serde(default = "default_level_base_bytes")]
    pub level_base_bytes: u64,
    /// Data block size in bytes for segment files.
    /// Default: 4096 (4 KiB). Larger blocks improve throughput at the cost of read amplification.
    #[serde(default = "default_data_block_size")]
    pub data_block_size: usize,
    /// Bloom filter bits per key. Higher values reduce false positives but use more memory.
    /// Default: 10.
    #[serde(default = "default_bloom_bits_per_key")]
    pub bloom_bits_per_key: usize,
    /// Compaction I/O rate limit in bytes per second. 0 = unlimited (default).
    /// On slow storage (SD cards), set to e.g. 5–10 MB/s to avoid starving user I/O.
    #[serde(default)]
    pub compaction_rate_limit: u64,
    /// Maximum time (milliseconds) a write can be stalled waiting for L0 compaction.
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
    0 // disabled by default until compaction throughput improves
}

fn default_l0_stop_writes_trigger() -> usize {
    0 // disabled by default until compaction throughput improves
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
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
    /// `"cache"` is valid in strata.toml for backward compat but cannot be set at runtime.
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
    /// BM25 k1 parameter (term frequency saturation).
    /// Default: 0.9 (Anserini/Pyserini BEIR standard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25_k1: Option<f32>,
    /// BM25 b parameter (document length normalization).
    /// Default: 0.4 (Anserini/Pyserini BEIR standard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25_b: Option<f32>,

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
    #[serde(default)]
    pub allow_lossy_recovery: bool,
    /// Whether the user opted in to anonymous usage telemetry.
    /// Default: false (opt-in, never on by default).
    #[serde(default)]
    pub telemetry: bool,
    /// Default storage data type for new vector collections.
    /// "f32" (default) or "int8" (scalar quantization, 4x memory savings).
    /// Individual collections can override via VectorConfig.
    #[serde(default = "default_vector_dtype")]
    pub default_vector_dtype: String,
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
            bm25_k1: None,
            bm25_b: None,
            embed_model: default_embed_model(),
            provider: default_provider(),
            default_model: None,
            anthropic_api_key: None,
            openai_api_key: None,
            google_api_key: None,
            storage: StorageConfig::default(),
            allow_lossy_recovery: false,
            telemetry: false,
            default_vector_dtype: default_vector_dtype(),
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
    pub fn vector_storage_dtype(&self) -> strata_core::primitives::StorageDtype {
        match self.default_vector_dtype.to_lowercase().as_str() {
            "int8" | "sq8" => strata_core::primitives::StorageDtype::Int8,
            "binary" | "rabitq" => strata_core::primitives::StorageDtype::Binary,
            _ => strata_core::primitives::StorageDtype::F32,
        }
    }

    /// Build a BM25 scorer using configured parameters (or defaults).
    pub fn bm25_scorer(&self) -> crate::search::BM25LiteScorer {
        let mut scorer = crate::search::BM25LiteScorer::default();
        if let Some(k1) = self.bm25_k1 {
            scorer.k1 = k1;
        }
        if let Some(b) = self.bm25_b {
            scorer.b = b;
        }
        scorer
    }

    /// Parse the durability string into a `DurabilityMode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not `"standard"`, `"always"`, or `"cache"`.
    pub fn durability_mode(&self) -> StrataResult<DurabilityMode> {
        match self.durability.as_str() {
            "standard" => Ok(DurabilityMode::standard_default()),
            "always" => Ok(DurabilityMode::Always),
            "cache" => Ok(DurabilityMode::Cache),
            other => Err(StrataError::invalid_input(format!(
                "Invalid durability mode '{}' in strata.toml. Expected \"standard\", \"always\", or \"cache\".",
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

# DEPRECATED: BM25 parameters have moved to the recipe system.
# Use `RECIPE SET '{"retrieve":{"bm25":{"k1":0.9,"b":0.4}}}'` instead.
# These values are auto-migrated to the default recipe on first open.
# bm25_k1 = 0.9
# bm25_b = 0.4

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
# max_branches = 1024
# max_write_buffer_entries = 500000
# max_versions_per_key = 0    # 0 = unlimited; set to e.g. 100 to cap MVCC history
# block_cache_size = 0          # 0 = auto (max(256 MiB, available_ram / 4))
# write_buffer_size = 134217728  # 128 MiB; memtable rotation threshold
# max_immutable_memtables = 4   # max frozen memtables per branch before write stalling
# background_threads = 4        # compaction/flush workers; default min(4, CPU cores)
# target_file_size = 67108864   # 64 MiB; segment file target (Pi: 4-8 MiB)
# level_base_bytes = 268435456  # 256 MiB; L1 target size (Pi: 32 MiB)
# data_block_size = 4096        # 4 KiB; segment data block size
# bloom_bits_per_key = 10       # bloom filter bits per key
# compaction_rate_limit = 0     # 0 = unlimited; bytes/sec cap for compaction I/O
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
            std::fs::write(path, Self::default_toml()).map_err(|e| {
                StrataError::internal(format!(
                    "Failed to write default config file '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            restrict_config_permissions(path)?;
        }
        Ok(())
    }

    /// Serialize this config to TOML and write it to the given path.
    pub fn write_to_file(&self, path: &Path) -> StrataResult<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| StrataError::internal(format!("Failed to serialize config: {}", e)))?;
        std::fs::write(path, content).map_err(|e| {
            StrataError::internal(format!(
                "Failed to write config file '{}': {}",
                path.display(),
                e
            ))
        })?;
        restrict_config_permissions(path)
    }
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

    #[test]
    fn bm25_scorer_uses_defaults_when_none() {
        let config = StrataConfig::default();
        let scorer = config.bm25_scorer();
        assert!((scorer.k1 - 0.9).abs() < 0.001);
        assert!((scorer.b - 0.4).abs() < 0.001);
    }

    #[test]
    fn bm25_scorer_uses_config_overrides() {
        let config = StrataConfig {
            bm25_k1: Some(1.2),
            bm25_b: Some(0.75),
            ..StrataConfig::default()
        };
        let scorer = config.bm25_scorer();
        assert!((scorer.k1 - 1.2).abs() < 0.001);
        assert!((scorer.b - 0.75).abs() < 0.001);
    }

    #[test]
    fn bm25_config_backward_compat() {
        // Old config files without bm25 fields should parse fine
        let old_toml = r#"
durability = "standard"
auto_embed = false
"#;
        let config: StrataConfig = toml::from_str(old_toml).unwrap();
        assert!(config.bm25_k1.is_none());
        assert!(config.bm25_b.is_none());
        let scorer = config.bm25_scorer();
        assert!((scorer.k1 - 0.9).abs() < 0.001);
        assert!((scorer.b - 0.4).abs() < 0.001);
    }

    #[test]
    fn bm25_config_round_trip() {
        let config = StrataConfig {
            bm25_k1: Some(1.5),
            bm25_b: Some(0.6),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert!((parsed.bm25_k1.unwrap() - 1.5).abs() < 0.001);
        assert!((parsed.bm25_b.unwrap() - 0.6).abs() < 0.001);
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
    // Cache durability mode
    // -----------------------------------------------------------------------

    #[test]
    fn parse_cache_durability() {
        let config: StrataConfig = toml::from_str("durability = \"cache\"").unwrap();
        assert_eq!(config.durability_mode().unwrap(), DurabilityMode::Cache);
    }

    #[test]
    fn cache_durability_round_trip() {
        let config = StrataConfig {
            durability: "cache".to_string(),
            ..StrataConfig::default()
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.durability, "cache");
        assert_eq!(parsed.durability_mode().unwrap(), DurabilityMode::Cache);
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
}
