//! Config and observability command handlers.
//!
//! Handles ConfigGet, ConfigSetAutoEmbed, AutoEmbedStatus, DurabilityCounters,
//! ConfigureSet, and ConfigureGetKey.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};
use strata_security::SensitiveString;

/// Handle ConfigGet command: return the current database configuration.
pub fn config_get(p: &Arc<Primitives>) -> Result<Output> {
    Ok(Output::Config(p.db.config()))
}

/// Handle ConfigSetAutoEmbed command: enable or disable auto-embedding.
pub fn config_set_auto_embed(p: &Arc<Primitives>, enabled: bool) -> Result<Output> {
    p.db.update_config(|cfg| {
        cfg.auto_embed = enabled;
    })
    .map_err(crate::Error::from)?;
    Ok(Output::Unit)
}

/// Handle AutoEmbedStatus command: check if auto-embedding is enabled.
pub fn auto_embed_status(p: &Arc<Primitives>) -> Result<Output> {
    Ok(Output::Bool(p.db.auto_embed_enabled()))
}

/// Handle DurabilityCounters command: return WAL counters (default if None).
pub fn durability_counters(p: &Arc<Primitives>) -> Result<Output> {
    let counters = p.db.durability_counters().unwrap_or_default();
    Ok(Output::DurabilityCounters(counters))
}

/// Supported configuration key names for ConfigureSet/ConfigureGetKey.
const KNOWN_KEYS: &[&str] = &[
    "provider",
    "default_model",
    "anthropic_api_key",
    "openai_api_key",
    "google_api_key",
    "embed_model",
    "durability",
    "auto_embed",
    "embed_batch_size",
    "model_endpoint",
    "model_name",
    "model_api_key",
    "model_timeout_ms",
    // Storage parameters (hot-swappable at runtime)
    "max_branches",
    "max_write_buffer_entries",
    "max_versions_per_key",
    "block_cache_size",
    "write_buffer_size",
    "max_immutable_memtables",
    "l0_slowdown_writes_trigger",
    "l0_stop_writes_trigger",
    "target_file_size",
    "level_base_bytes",
    "data_block_size",
    "bloom_bits_per_key",
    "compaction_rate_limit",
    // Open-time only (rejected at runtime with clear error)
    "background_threads",
    "allow_lossy_recovery",
    "allow_missing_manifest",
];

/// Keys that can only be set at database open time.
const OPEN_TIME_ONLY_KEYS: &[&str] = &[
    "background_threads",
    "allow_lossy_recovery",
    "allow_missing_manifest",
];

/// Storage keys that accept usize values (≥ 0).
const STORAGE_USIZE_KEYS: &[&str] = &[
    "max_branches",
    "max_write_buffer_entries",
    "max_versions_per_key",
    "block_cache_size",
    "write_buffer_size",
    "max_immutable_memtables",
    "l0_slowdown_writes_trigger",
    "l0_stop_writes_trigger",
    "data_block_size",
    "bloom_bits_per_key",
];

/// Storage keys that accept u64 values (≥ 0).
const STORAGE_U64_KEYS: &[&str] = &[
    "target_file_size",
    "level_base_bytes",
    "compaction_rate_limit",
];

/// Handle ConfigureSet command: set a named configuration key.
pub fn configure_set(p: &Arc<Primitives>, key: String, value: String) -> Result<Output> {
    let key_lower = key.trim().to_ascii_lowercase();

    if !KNOWN_KEYS.contains(&key_lower.as_str()) {
        let candidates: Vec<String> = KNOWN_KEYS.iter().map(|s| s.to_string()).collect();
        let hint = crate::suggest::format_hint("config keys", &candidates, key.trim(), 2);
        return Err(Error::InvalidInput {
            reason: format!("Unknown configuration key: {:?}", key.trim()),
            hint,
        });
    }

    // Reject open-time-only keys
    if OPEN_TIME_ONLY_KEYS.contains(&key_lower.as_str()) {
        return Err(Error::InvalidInput {
            reason: format!(
                "{:?} can only be set at database open time (in strata.toml), not at runtime",
                key_lower
            ),
            hint: None,
        });
    }

    // Validate provider name
    if key_lower == "provider" {
        let v = value.trim().to_ascii_lowercase();
        if v.is_empty() {
            return Err(Error::InvalidInput {
                reason: "Provider cannot be empty. Valid values: local, anthropic, openai, google"
                    .to_string(),
                hint: None,
            });
        }
        let valid_providers = ["local", "anthropic", "openai", "google"];
        if !valid_providers.contains(&v.as_str()) {
            return Err(Error::InvalidInput {
                reason: format!(
                    "Unknown provider: {:?}. Valid providers: {}",
                    value.trim(),
                    valid_providers.join(", ")
                ),
                hint: None,
            });
        }
    }

    // Validate embed_model name and normalize to canonical casing.
    let mut canonical_embed_model = None;
    if key_lower == "embed_model" {
        let v = value.trim().to_ascii_lowercase();
        if v.is_empty() {
            return Err(Error::InvalidInput {
                reason: "embed_model cannot be empty. Valid models: miniLM, nomic-embed, bge-m3, gemma-embed".to_string(),
                hint: None,
            });
        }
        // Cloud model specs (e.g., "openai:text-embedding-3-small") pass through as-is.
        let canonical = if v.contains(':') {
            value.trim()
        } else {
            match v.as_str() {
                "minilm" => "miniLM",
                "nomic-embed" => "nomic-embed",
                "bge-m3" => "bge-m3",
                "gemma-embed" => "gemma-embed",
                _ => {
                    return Err(Error::InvalidInput {
                        reason: format!(
                            "Unknown embed_model: {:?}. Valid: miniLM, nomic-embed, bge-m3, gemma-embed, or provider:model (e.g., openai:text-embedding-3-small)",
                            value.trim()
                        ),
                        hint: None,
                    });
                }
            }
        };
        canonical_embed_model = Some(canonical.to_string());
        tracing::warn!(
            target: "strata::config",
            new_model = %canonical,
            "embed_model changed; takes effect on next database open. Existing data must be re-indexed."
        );
    }

    // Validate durability (only standard/always at runtime; cache is open-time only)
    if key_lower == "durability" {
        let v = value.trim().to_ascii_lowercase();
        let valid = ["standard", "always"];
        if !valid.contains(&v.as_str()) {
            let reason = if v == "cache" {
                "Cannot switch to cache mode at runtime. Cache mode has no WAL \
                 infrastructure and must be selected at open time via Database::cache()."
                    .to_string()
            } else {
                format!(
                    "Invalid durability mode: {:?}. Valid values: standard, always",
                    value.trim()
                )
            };
            return Err(Error::InvalidInput { reason, hint: None });
        }
    }

    // Validate auto_embed (bool)
    if key_lower == "auto_embed" {
        let v = value.trim().to_ascii_lowercase();
        if v != "true" && v != "false" {
            return Err(Error::InvalidInput {
                reason: format!(
                    "Invalid auto_embed value: {:?}. Expected \"true\" or \"false\"",
                    value.trim()
                ),
                hint: None,
            });
        }
    }

    // Validate embed_batch_size (usize, > 0)
    if key_lower == "embed_batch_size" {
        let v: usize = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid embed_batch_size value: {:?}. Expected a positive integer",
                value.trim()
            ),
            hint: None,
        })?;
        if v == 0 {
            return Err(Error::InvalidInput {
                reason: "embed_batch_size must be greater than 0".to_string(),
                hint: None,
            });
        }
    }

    // Validate model_timeout_ms (u64)
    if key_lower == "model_timeout_ms" {
        let _: u64 = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid model_timeout_ms value: {:?}. Expected a positive integer",
                value.trim()
            ),
            hint: None,
        })?;
    }

    // Validate storage usize keys
    if STORAGE_USIZE_KEYS.contains(&key_lower.as_str()) {
        let _: usize = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid {} value: {:?}. Expected a non-negative integer",
                key_lower,
                value.trim()
            ),
            hint: None,
        })?;
    }

    // Validate storage u64 keys
    if STORAGE_U64_KEYS.contains(&key_lower.as_str()) {
        let _: u64 = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid {} value: {:?}. Expected a non-negative integer",
                key_lower,
                value.trim()
            ),
            hint: None,
        })?;
    }

    // Durability: one canonical path — `Database::set_durability_mode`
    // handles the WAL switch, signature update, config-string update, and
    // strata.toml persist atomically. A prior version of this handler also
    // called `update_config`; that was redundant after the engine-side
    // persist landed, and is now rejected by `update_config` itself.
    if key_lower == "durability" {
        let v = value.trim().to_ascii_lowercase();
        let mode = match v.as_str() {
            "standard" => crate::DurabilityMode::standard_default(),
            "always" => crate::DurabilityMode::Always,
            _ => unreachable!(), // validated above
        };
        if let Err(e) = p.db.set_durability_mode(mode) {
            // Ephemeral databases have no WAL — surface the engine's
            // rejection rather than pretending the change took effect.
            return Err(crate::Error::from(e));
        }
        return Ok(Output::ConfigSetResult {
            key: key_lower,
            new_value: v,
        });
    }

    // Storage parameter keys — route through update_config (which applies live)
    if STORAGE_USIZE_KEYS.contains(&key_lower.as_str())
        || STORAGE_U64_KEYS.contains(&key_lower.as_str())
    {
        let effective = value.trim().to_string();
        p.db.update_config(|cfg| match key_lower.as_str() {
            "max_branches" => {
                cfg.storage.max_branches = value.trim().parse().unwrap_or(cfg.storage.max_branches)
            }
            "max_write_buffer_entries" => {
                cfg.storage.max_write_buffer_entries = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.max_write_buffer_entries)
            }
            "max_versions_per_key" => {
                cfg.storage.max_versions_per_key = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.max_versions_per_key)
            }
            "block_cache_size" => {
                cfg.storage.block_cache_size =
                    value.trim().parse().unwrap_or(cfg.storage.block_cache_size)
            }
            "write_buffer_size" => {
                cfg.storage.write_buffer_size = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.write_buffer_size)
            }
            "max_immutable_memtables" => {
                cfg.storage.max_immutable_memtables = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.max_immutable_memtables)
            }
            "l0_slowdown_writes_trigger" => {
                cfg.storage.l0_slowdown_writes_trigger = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.l0_slowdown_writes_trigger)
            }
            "l0_stop_writes_trigger" => {
                cfg.storage.l0_stop_writes_trigger = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.l0_stop_writes_trigger)
            }
            "target_file_size" => {
                cfg.storage.target_file_size =
                    value.trim().parse().unwrap_or(cfg.storage.target_file_size)
            }
            "level_base_bytes" => {
                cfg.storage.level_base_bytes =
                    value.trim().parse().unwrap_or(cfg.storage.level_base_bytes)
            }
            "data_block_size" => {
                cfg.storage.data_block_size =
                    value.trim().parse().unwrap_or(cfg.storage.data_block_size)
            }
            "bloom_bits_per_key" => {
                cfg.storage.bloom_bits_per_key = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.bloom_bits_per_key)
            }
            "compaction_rate_limit" => {
                cfg.storage.compaction_rate_limit = value
                    .trim()
                    .parse()
                    .unwrap_or(cfg.storage.compaction_rate_limit)
            }
            _ => unreachable!(),
        })
        .map_err(crate::Error::from)?;
        return Ok(Output::ConfigSetResult {
            key: key_lower,
            new_value: effective,
        });
    }

    // Model-related keys may need to create the model section
    let use_model_config = matches!(
        key_lower.as_str(),
        "model_endpoint" | "model_name" | "model_api_key" | "model_timeout_ms"
    );

    if use_model_config {
        let effective = if key_lower == "model_api_key" {
            mask_api_key(&value)
        } else {
            value.clone()
        };
        p.db.update_config(|cfg| {
            let model =
                cfg.model
                    .get_or_insert_with(|| strata_engine::database::config::ModelConfig {
                        endpoint: String::new(),
                        model: String::new(),
                        api_key: None,
                        timeout_ms: 5000,
                    });
            match key_lower.as_str() {
                "model_endpoint" => model.endpoint = value.clone(),
                "model_name" => model.model = value.clone(),
                "model_api_key" => model.api_key = Some(SensitiveString::from(value.clone())),
                "model_timeout_ms" => {
                    model.timeout_ms = value.trim().parse().unwrap_or(5000);
                }
                _ => unreachable!(),
            }
        })
        .map_err(crate::Error::from)?;
        return Ok(Output::ConfigSetResult {
            key: key_lower,
            new_value: effective,
        });
    }

    // Compute effective value for the response (normalized/masked as appropriate).
    // Must match what ConfigureGetKey would return after the write.
    let effective = match key_lower.as_str() {
        "embed_model" => canonical_embed_model
            .clone()
            .unwrap_or_else(|| value.clone()),
        "anthropic_api_key" | "openai_api_key" | "google_api_key" => mask_api_key(&value),
        "auto_embed" => value.trim().eq_ignore_ascii_case("true").to_string(),
        // Pre-existing validators above already reject non-numeric values,
        // so parse() here is guaranteed to succeed for valid inputs.
        "embed_batch_size" => value.trim().parse::<usize>().unwrap().to_string(),
        _ => value.clone(),
    };

    p.db.update_config(|cfg| match key_lower.as_str() {
        "provider" => cfg.provider = value.clone(),
        "default_model" => cfg.default_model = Some(value.clone()),
        "anthropic_api_key" => cfg.anthropic_api_key = Some(SensitiveString::from(value.clone())),
        "openai_api_key" => cfg.openai_api_key = Some(SensitiveString::from(value.clone())),
        "google_api_key" => cfg.google_api_key = Some(SensitiveString::from(value.clone())),
        "embed_model" => {
            cfg.embed_model = canonical_embed_model
                .clone()
                .unwrap_or_else(|| value.clone())
        }
        "auto_embed" => {
            cfg.auto_embed = value.trim().eq_ignore_ascii_case("true");
        }
        "embed_batch_size" => {
            cfg.embed_batch_size = value.trim().parse().ok();
        }
        _ => unreachable!(),
    })
    .map_err(crate::Error::from)?;

    Ok(Output::ConfigSetResult {
        key: key_lower,
        new_value: effective,
    })
}

/// Handle ConfigureGetKey command: get the value of a named configuration key.
pub fn configure_get_key(p: &Arc<Primitives>, key: String) -> Result<Output> {
    let key_lower = key.trim().to_ascii_lowercase();

    if !KNOWN_KEYS.contains(&key_lower.as_str()) {
        let candidates: Vec<String> = KNOWN_KEYS.iter().map(|s| s.to_string()).collect();
        let hint = crate::suggest::format_hint("config keys", &candidates, key.trim(), 2);
        return Err(Error::InvalidInput {
            reason: format!("Unknown configuration key: {:?}", key.trim()),
            hint,
        });
    }

    let cfg = p.db.config();
    let value = match key_lower.as_str() {
        "provider" => Some(cfg.provider.clone()),
        "default_model" => cfg.default_model.clone(),
        "anthropic_api_key" => cfg.anthropic_api_key.as_deref().map(mask_api_key),
        "openai_api_key" => cfg.openai_api_key.as_deref().map(mask_api_key),
        "google_api_key" => cfg.google_api_key.as_deref().map(mask_api_key),
        "embed_model" => Some(cfg.embed_model.clone()),
        "durability" => Some(cfg.durability.clone()),
        "auto_embed" => Some(cfg.auto_embed.to_string()),
        "embed_batch_size" => cfg.embed_batch_size.map(|v| v.to_string()),
        "model_endpoint" => cfg.model.as_ref().map(|m| m.endpoint.clone()),
        "model_name" => cfg.model.as_ref().map(|m| m.model.clone()),
        "model_api_key" => cfg
            .model
            .as_ref()
            .and_then(|m| m.api_key.as_deref().map(mask_api_key)),
        "model_timeout_ms" => cfg.model.as_ref().map(|m| m.timeout_ms.to_string()),
        // Storage parameters
        "max_branches" => Some(cfg.storage.max_branches.to_string()),
        "max_write_buffer_entries" => Some(cfg.storage.max_write_buffer_entries.to_string()),
        "max_versions_per_key" => Some(cfg.storage.max_versions_per_key.to_string()),
        "block_cache_size" => Some(cfg.storage.block_cache_size.to_string()),
        "write_buffer_size" => Some(cfg.storage.write_buffer_size.to_string()),
        "max_immutable_memtables" => Some(cfg.storage.max_immutable_memtables.to_string()),
        "l0_slowdown_writes_trigger" => Some(cfg.storage.l0_slowdown_writes_trigger.to_string()),
        "l0_stop_writes_trigger" => Some(cfg.storage.l0_stop_writes_trigger.to_string()),
        "target_file_size" => Some(cfg.storage.target_file_size.to_string()),
        "level_base_bytes" => Some(cfg.storage.level_base_bytes.to_string()),
        "data_block_size" => Some(cfg.storage.data_block_size.to_string()),
        "bloom_bits_per_key" => Some(cfg.storage.bloom_bits_per_key.to_string()),
        "compaction_rate_limit" => Some(cfg.storage.compaction_rate_limit.to_string()),
        // Open-time only keys still readable
        "background_threads" => Some(cfg.storage.background_threads.to_string()),
        "allow_lossy_recovery" => Some(cfg.allow_lossy_recovery.to_string()),
        _ => unreachable!(),
    };

    Ok(Output::ConfigValue(value))
}

/// Mask an API key for display: show first 4 characters + "***".
fn mask_api_key(key: &str) -> String {
    if key.chars().count() <= 4 {
        "***".to_string()
    } else {
        let prefix: String = key.chars().take(4).collect();
        format!("{}***", prefix)
    }
}
