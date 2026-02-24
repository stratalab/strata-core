//! Config and observability command handlers.
//!
//! Handles ConfigGet, ConfigSetAutoEmbed, AutoEmbedStatus, DurabilityCounters,
//! ConfigureSet, and ConfigureGetKey.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};

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
];

/// Handle ConfigureSet command: set a named configuration key.
pub fn configure_set(p: &Arc<Primitives>, key: String, value: String) -> Result<Output> {
    let key_lower = key.trim().to_ascii_lowercase();

    if !KNOWN_KEYS.contains(&key_lower.as_str()) {
        return Err(Error::InvalidInput {
            reason: format!(
                "Unknown configuration key: {:?}. Valid keys: {}",
                key.trim(),
                KNOWN_KEYS.join(", ")
            ),
        });
    }

    // Validate provider name
    if key_lower == "provider" {
        let v = value.trim().to_ascii_lowercase();
        if v.is_empty() {
            return Err(Error::InvalidInput {
                reason: "Provider cannot be empty. Valid values: local, anthropic, openai, google"
                    .to_string(),
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
            });
        }
    }

    p.db.update_config(|cfg| match key_lower.as_str() {
        "provider" => cfg.provider = value.clone(),
        "default_model" => cfg.default_model = Some(value.clone()),
        "anthropic_api_key" => cfg.anthropic_api_key = Some(value.clone()),
        "openai_api_key" => cfg.openai_api_key = Some(value.clone()),
        "google_api_key" => cfg.google_api_key = Some(value.clone()),
        _ => unreachable!(),
    })
    .map_err(crate::Error::from)?;

    Ok(Output::Unit)
}

/// Handle ConfigureGetKey command: get the value of a named configuration key.
pub fn configure_get_key(p: &Arc<Primitives>, key: String) -> Result<Output> {
    let key_lower = key.trim().to_ascii_lowercase();

    if !KNOWN_KEYS.contains(&key_lower.as_str()) {
        return Err(Error::InvalidInput {
            reason: format!(
                "Unknown configuration key: {:?}. Valid keys: {}",
                key.trim(),
                KNOWN_KEYS.join(", ")
            ),
        });
    }

    let cfg = p.db.config();
    let value = match key_lower.as_str() {
        "provider" => Some(cfg.provider.clone()),
        "default_model" => cfg.default_model.clone(),
        "anthropic_api_key" => cfg.anthropic_api_key.clone(),
        "openai_api_key" => cfg.openai_api_key.clone(),
        "google_api_key" => cfg.google_api_key.clone(),
        _ => unreachable!(),
    };

    Ok(Output::ConfigValue(value))
}
