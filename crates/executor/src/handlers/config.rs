//! Config and observability command handlers.
//!
//! Handles ConfigGet, ConfigSetAutoEmbed, AutoEmbedStatus, and DurabilityCounters.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Output, Result};

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
