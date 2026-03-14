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
    "bm25_k1",
    "bm25_b",
    "embed_batch_size",
    "model_endpoint",
    "model_name",
    "model_api_key",
    "model_timeout_ms",
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
        let canonical = match v.as_str() {
            "minilm" => "miniLM",
            "nomic-embed" => "nomic-embed",
            "bge-m3" => "bge-m3",
            "gemma-embed" => "gemma-embed",
            _ => {
                return Err(Error::InvalidInput {
                    reason: format!(
                        "Unknown embed_model: {:?}. Valid models: miniLM, nomic-embed, bge-m3, gemma-embed",
                        value.trim()
                    ),
                    hint: None,
                });
            }
        };
        canonical_embed_model = Some(canonical.to_string());
        tracing::warn!(
            target: "strata::config",
            new_model = %canonical,
            "embed_model changed; takes effect on next database open. Existing data must be re-indexed."
        );
    }

    // Validate durability
    if key_lower == "durability" {
        let v = value.trim().to_ascii_lowercase();
        let valid = ["standard", "always", "cache"];
        if !valid.contains(&v.as_str()) {
            return Err(Error::InvalidInput {
                reason: format!(
                    "Invalid durability mode: {:?}. Valid values: standard, always, cache",
                    value.trim()
                ),
                hint: None,
            });
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

    // Validate bm25_k1 (f32, finite, > 0)
    if key_lower == "bm25_k1" {
        let v: f32 = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid bm25_k1 value: {:?}. Expected a positive number",
                value.trim()
            ),
            hint: None,
        })?;
        if !v.is_finite() || v <= 0.0 {
            return Err(Error::InvalidInput {
                reason: "bm25_k1 must be a finite number greater than 0".to_string(),
                hint: None,
            });
        }
    }

    // Validate bm25_b (f32, finite, 0..=1)
    if key_lower == "bm25_b" {
        let v: f32 = value.trim().parse().map_err(|_| Error::InvalidInput {
            reason: format!(
                "Invalid bm25_b value: {:?}. Expected a number between 0 and 1",
                value.trim()
            ),
            hint: None,
        })?;
        if !v.is_finite() || !(0.0..=1.0).contains(&v) {
            return Err(Error::InvalidInput {
                reason: "bm25_b must be a finite number between 0 and 1 (inclusive)".to_string(),
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

    // Durability uses persist_config_deferred (allows changing durability)
    if key_lower == "durability" {
        let v = value.trim().to_ascii_lowercase();
        let effective = v.clone();
        p.db.persist_config_deferred(|cfg| {
            cfg.durability = v;
        })
        .map_err(crate::Error::from)?;
        tracing::info!(
            target: "strata::config",
            "durability changed; takes effect on next restart"
        );
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
        "bm25_k1" => value
            .trim()
            .parse::<f32>()
            .map(|v| v.to_string())
            .unwrap_or_else(|_| value.clone()),
        "bm25_b" => value
            .trim()
            .parse::<f32>()
            .map(|v| v.to_string())
            .unwrap_or_else(|_| value.clone()),
        "embed_batch_size" => value
            .trim()
            .parse::<usize>()
            .map(|v| v.to_string())
            .unwrap_or_else(|_| value.clone()),
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
        "bm25_k1" => {
            cfg.bm25_k1 = value.trim().parse().ok();
        }
        "bm25_b" => {
            cfg.bm25_b = value.trim().parse().ok();
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
        "bm25_k1" => cfg.bm25_k1.map(|v| v.to_string()),
        "bm25_b" => cfg.bm25_b.map(|v| v.to_string()),
        "embed_batch_size" => cfg.embed_batch_size.map(|v| v.to_string()),
        "model_endpoint" => cfg.model.as_ref().map(|m| m.endpoint.clone()),
        "model_name" => cfg.model.as_ref().map(|m| m.model.clone()),
        "model_api_key" => cfg
            .model
            .as_ref()
            .and_then(|m| m.api_key.as_deref().map(mask_api_key)),
        "model_timeout_ms" => cfg.model.as_ref().map(|m| m.timeout_ms.to_string()),
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
