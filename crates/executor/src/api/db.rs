//! Database operations: ping, info, health, flush, compact, configuration.

use super::Strata;
use crate::output::EmbedStatusInfo;
use crate::types::*;
use crate::{Command, Error, Output, Result};
use strata_engine::{HealthReport, StrataConfig};

impl Strata {
    // =========================================================================
    // Database Operations (5)
    // =========================================================================

    /// Ping the database.
    pub fn ping(&self) -> Result<String> {
        match self.execute_cmd(Command::Ping)? {
            Output::Pong { version } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Ping".into(),
            }),
        }
    }

    /// Get database info.
    pub fn info(&self) -> Result<DatabaseInfo> {
        match self.execute_cmd(Command::Info)? {
            Output::DatabaseInfo(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Info".into(),
            }),
        }
    }

    /// Run health checks on all subsystems.
    pub fn health(&self) -> Result<HealthReport> {
        match self.execute_cmd(Command::Health)? {
            Output::Health(report) => Ok(report),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Health".into(),
            }),
        }
    }

    /// Flush the database to disk.
    pub fn flush(&self) -> Result<()> {
        match self.execute_cmd(Command::Flush)? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Flush".into(),
            }),
        }
    }

    /// Compact the database.
    pub fn compact(&self) -> Result<()> {
        match self.execute_cmd(Command::Compact)? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Compact".into(),
            }),
        }
    }

    // =========================================================================
    // Bundle Operations (3)
    // =========================================================================

    /// Export a branch to a .branchbundle.tar.zst archive.
    pub fn branch_export(&self, branch_id: &str, path: &str) -> Result<BranchExportResult> {
        match self.execute_cmd(Command::BranchExport {
            branch_id: branch_id.to_string(),
            path: path.to_string(),
        })? {
            Output::BranchExported(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchExport".into(),
            }),
        }
    }

    /// Import a branch from a .branchbundle.tar.zst archive.
    pub fn branch_import(&self, path: &str) -> Result<BranchImportResult> {
        match self.execute_cmd(Command::BranchImport {
            path: path.to_string(),
        })? {
            Output::BranchImported(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchImport".into(),
            }),
        }
    }

    /// Validate a .branchbundle.tar.zst archive without importing.
    pub fn branch_validate_bundle(&self, path: &str) -> Result<BundleValidateResult> {
        match self.execute_cmd(Command::BranchBundleValidate {
            path: path.to_string(),
        })? {
            Output::BundleValidated(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for BranchBundleValidate".into(),
            }),
        }
    }

    // =========================================================================
    // Configuration (6)
    // =========================================================================

    /// Set a configuration key.
    ///
    /// All 15 keys are supported: `provider`, `default_model`,
    /// `anthropic_api_key`, `openai_api_key`, `google_api_key`, `embed_model`,
    /// `durability`, `auto_embed`, `bm25_k1`, `bm25_b`, `embed_batch_size`,
    /// `model_endpoint`, `model_name`, `model_api_key`, `model_timeout_ms`.
    ///
    /// Values are validated at the handler level.  Changes are persisted to
    /// `strata.toml` for disk-backed databases.
    pub fn config_set(&self, key: &str, value: &str) -> Result<()> {
        match self.execute_cmd(Command::ConfigureSet {
            key: key.to_string(),
            value: value.to_string(),
        })? {
            Output::ConfigSetResult { .. } => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ConfigureSet".into(),
            }),
        }
    }

    /// Get a configuration value by key.
    ///
    /// Returns `Some(value)` for keys that are set, or `None` for optional
    /// keys that have no value (e.g. `default_model` when unset).
    pub fn config_get(&self, key: &str) -> Result<Option<String>> {
        match self.execute_cmd(Command::ConfigureGetKey {
            key: key.to_string(),
        })? {
            Output::ConfigValue(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ConfigureGetKey".into(),
            }),
        }
    }

    /// Get the current database configuration.
    ///
    /// Returns a snapshot of the unified config (durability, auto_embed, model).
    pub fn config(&self) -> Result<StrataConfig> {
        match self.execute_cmd(Command::ConfigGet)? {
            Output::Config(cfg) => Ok(cfg),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ConfigGet".into(),
            }),
        }
    }

    /// Configure an external LLM model for query expansion and re-ranking.
    ///
    /// The configuration is persisted to `strata.toml` for disk-backed databases.
    pub fn configure_model(
        &self,
        endpoint: &str,
        model: &str,
        api_key: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<()> {
        match self.execute_cmd(Command::ConfigureModel {
            endpoint: endpoint.to_string(),
            model: model.to_string(),
            api_key: api_key.map(|s| s.to_string()),
            timeout_ms,
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ConfigureModel".into(),
            }),
        }
    }

    /// Embed a single text string into a vector.
    ///
    /// Returns the embedding vector produced by the configured model.
    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        match self.execute_cmd(Command::Embed {
            text: text.to_string(),
        })? {
            Output::Embedding(vec) => Ok(vec),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Embed".into(),
            }),
        }
    }

    /// Embed a batch of text strings into vectors.
    ///
    /// Returns one embedding vector per input text.
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        match self.execute_cmd(Command::EmbedBatch {
            texts: texts.iter().map(|s| s.to_string()).collect(),
        })? {
            Output::Embeddings(vecs) => Ok(vecs),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EmbedBatch".into(),
            }),
        }
    }

    /// Get a snapshot of the embedding pipeline status.
    pub fn embed_status(&self) -> Result<EmbedStatusInfo> {
        match self.execute_cmd(Command::EmbedStatus)? {
            Output::EmbedStatus(info) => Ok(info),
            _ => Err(Error::Internal {
                reason: "Unexpected output for EmbedStatus".into(),
            }),
        }
    }

    /// Check whether auto-embedding is enabled.
    pub fn auto_embed_enabled(&self) -> Result<bool> {
        match self.execute_cmd(Command::AutoEmbedStatus)? {
            Output::Bool(enabled) => Ok(enabled),
            _ => Err(Error::Internal {
                reason: "Unexpected output for AutoEmbedStatus".into(),
            }),
        }
    }

    /// Enable or disable auto-embedding of text values.
    ///
    /// Persisted to `strata.toml` for disk-backed databases.
    pub fn set_auto_embed(&self, enabled: bool) -> Result<()> {
        match self.execute_cmd(Command::ConfigSetAutoEmbed { enabled })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ConfigSetAutoEmbed".into(),
            }),
        }
    }

    // =========================================================================
    // Introspection / Analytics
    // =========================================================================

    /// Return a structured snapshot of the database for introspection.
    pub fn describe(&self) -> Result<DescribeResult> {
        match self.execute_cmd(Command::Describe {
            branch: self.branch_id(),
        })? {
            Output::Described(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Describe".into(),
            }),
        }
    }

    /// Get the available time range for the current branch.
    ///
    /// Returns (oldest_ts, latest_ts) in microseconds since epoch.
    /// Either value may be `None` if the branch has no data.
    pub fn time_range(&self) -> Result<(Option<u64>, Option<u64>)> {
        match self.execute_cmd(Command::TimeRange {
            branch: self.branch_id(),
        })? {
            Output::TimeRange {
                oldest_ts,
                latest_ts,
            } => Ok((oldest_ts, latest_ts)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for TimeRange".into(),
            }),
        }
    }

    /// Search across multiple primitives using a structured query.
    pub fn search(&self, query: SearchQuery) -> Result<(Vec<SearchResultHit>, SearchStatsOutput)> {
        match self.execute_cmd(Command::Search {
            branch: self.branch_id(),
            space: self.space_id(),
            search: query,
        })? {
            Output::SearchResults { hits, stats } => Ok((hits, stats)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Search".into(),
            }),
        }
    }

    /// Count KV keys, optionally filtered by prefix.
    pub fn kv_count(&self, prefix: Option<&str>) -> Result<u64> {
        match self.execute_cmd(Command::KvCount {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
        })? {
            Output::Uint(count) => Ok(count),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvCount".into(),
            }),
        }
    }

    /// Count JSON documents, optionally filtered by prefix.
    pub fn json_count(&self, prefix: Option<&str>) -> Result<u64> {
        match self.execute_cmd(Command::JsonCount {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
        })? {
            Output::Uint(count) => Ok(count),
            _ => Err(Error::Internal {
                reason: "Unexpected output for JsonCount".into(),
            }),
        }
    }

    // =========================================================================
    // Sampling
    // =========================================================================

    /// Sample KV entries for shape discovery.
    pub fn kv_sample(&self, count: Option<usize>) -> Result<(u64, Vec<SampleItem>)> {
        match self.execute_cmd(Command::KvSample {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: None,
            count,
        })? {
            Output::SampleResult { total_count, items } => Ok((total_count, items)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvSample".into(),
            }),
        }
    }

    /// Sample JSON documents for shape discovery.
    pub fn json_sample(&self, count: Option<usize>) -> Result<(u64, Vec<SampleItem>)> {
        match self.execute_cmd(Command::JsonSample {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: None,
            count,
        })? {
            Output::SampleResult { total_count, items } => Ok((total_count, items)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for JsonSample".into(),
            }),
        }
    }

    /// Sample vector entries for shape discovery (returns metadata, not embeddings).
    pub fn vector_sample(
        &self,
        collection: &str,
        count: Option<usize>,
    ) -> Result<(u64, Vec<SampleItem>)> {
        match self.execute_cmd(Command::VectorSample {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            count,
        })? {
            Output::SampleResult { total_count, items } => Ok((total_count, items)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorSample".into(),
            }),
        }
    }

    // =========================================================================
    // Retention
    // =========================================================================

    /// Apply retention policy (trigger garbage collection) on the current branch.
    pub fn retention_apply(&self) -> Result<()> {
        match self.execute_cmd(Command::RetentionApply {
            branch: self.branch_id(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for RetentionApply".into(),
            }),
        }
    }

    /// Get retention statistics for the current branch.
    ///
    /// Note: this command is not yet implemented in the executor and will
    /// return an error.
    pub fn retention_stats(&self) -> Result<()> {
        match self.execute_cmd(Command::RetentionStats {
            branch: self.branch_id(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for RetentionStats".into(),
            }),
        }
    }

    /// Preview what would be deleted by the retention policy.
    ///
    /// Note: this command is not yet implemented in the executor and will
    /// return an error.
    pub fn retention_preview(&self) -> Result<()> {
        match self.execute_cmd(Command::RetentionPreview {
            branch: self.branch_id(),
        })? {
            Output::Unit => Ok(()),
            _ => Err(Error::Internal {
                reason: "Unexpected output for RetentionPreview".into(),
            }),
        }
    }
}
