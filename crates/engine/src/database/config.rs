//! Database configuration via `strata.toml`
//!
//! Replaces the builder pattern with a simple config file in the data directory.
//! On first open, a default `strata.toml` is created. To change settings,
//! edit the file and restart — same model as Redis.

use serde::{Deserialize, Serialize};
use std::path::Path;
use strata_core::{StrataError, StrataResult};
use strata_durability::wal::DurabilityMode;

// ============================================================================
// Shadow Collection Names
// ============================================================================

/// Shadow vector collection name for KV store auto-embeddings.
pub const SHADOW_KV: &str = "_system_embed_kv";
/// Shadow vector collection name for JSON store auto-embeddings.
pub const SHADOW_JSON: &str = "_system_embed_json";
/// Shadow vector collection name for event log auto-embeddings.
pub const SHADOW_EVENT: &str = "_system_embed_event";
/// Shadow vector collection name for state cell auto-embeddings.
pub const SHADOW_STATE: &str = "_system_embed_state";

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
    pub api_key: Option<String>,
    /// Request timeout in milliseconds (default: 5000)
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    5000
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
    /// Durability mode: `"standard"` or `"always"`.
    #[serde(default = "default_durability_str")]
    pub durability: String,
    /// Enable automatic text embedding for semantic search.
    #[serde(default)]
    pub auto_embed: bool,
    /// Optional model configuration for query expansion and re-ranking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelConfig>,
    /// Embedding batch size for auto-embed.
    /// When opened via `OpenOptions`, defaults to 512.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_batch_size: Option<usize>,
    /// BM25 k1 parameter (term frequency saturation).
    /// Default: 0.9 (Anserini/Pyserini BEIR standard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25_k1: Option<f32>,
    /// BM25 b parameter (document length normalization).
    /// Default: 0.4 (Anserini/Pyserini BEIR standard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25_b: Option<f32>,
}

fn default_durability_str() -> String {
    "standard".to_string()
}

impl Default for StrataConfig {
    fn default() -> Self {
        Self {
            durability: default_durability_str(),
            auto_embed: false,
            model: None,
            embed_batch_size: None,
            bm25_k1: None,
            bm25_b: None,
        }
    }
}

impl StrataConfig {
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
    /// Returns an error if the string is not `"standard"` or `"always"`.
    pub fn durability_mode(&self) -> StrataResult<DurabilityMode> {
        match self.durability.as_str() {
            "standard" => Ok(DurabilityMode::standard_default()),
            "always" => Ok(DurabilityMode::Always),
            other => Err(StrataError::invalid_input(format!(
                "Invalid durability mode '{}' in strata.toml. Expected \"standard\" or \"always\".",
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
durability = "standard"

# Auto-embed: automatically generate embeddings for text data (default: false)
# Requires the "embed" feature to be compiled in.
auto_embed = false

# Embedding batch size for auto-embed (default: 512 via OpenOptions).
# Increase for bulk ingestion, decrease for interactive use.
# embed_batch_size = 512

# BM25 scoring parameters (defaults: k1=0.9, b=0.4 per Anserini/Pyserini).
# Increase k1 for more term-frequency sensitivity, increase b for more
# length normalization. Lucene defaults are k1=1.2, b=0.75.
# bm25_k1 = 0.9
# bm25_b = 0.4

# Model configuration for query expansion and re-ranking.
# Uncomment and configure to enable intelligent search features.
# [model]
# endpoint = "http://localhost:11434/v1"
# model = "qwen3:1.7b"
# api_key = "your-api-key"      # optional
# timeout_ms = 5000              # optional, default 5000
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
        let config: StrataConfig = toml::from_str(&content).map_err(|e| {
            StrataError::invalid_input(format!(
                "Failed to parse config file '{}': {}",
                path.display(),
                e
            ))
        })?;
        // Validate the durability value eagerly
        config.durability_mode()?;
        Ok(config)
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
        })
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
                api_key: Some("sk-test".to_string()),
                timeout_ms: 3000,
            }),
            embed_batch_size: None,
            bm25_k1: None,
            bm25_b: None,
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: StrataConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.durability, "always");
        assert!(parsed.auto_embed);
        let model = parsed.model.unwrap();
        assert_eq!(model.endpoint, "http://localhost:11434/v1");
        assert_eq!(model.model, "qwen3:1.7b");
        assert_eq!(model.api_key, Some("sk-test".to_string()));
        assert_eq!(model.timeout_ms, 3000);
    }

    #[test]
    fn model_config_round_trip_without_model() {
        let config = StrataConfig {
            durability: "standard".to_string(),
            auto_embed: false,
            model: None,
            embed_batch_size: None,
            bm25_k1: None,
            bm25_b: None,
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
            embed_batch_size: None,
            bm25_k1: None,
            bm25_b: None,
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
}
