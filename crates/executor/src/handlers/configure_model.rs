//! ConfigureModel command handler.
//!
//! Stores model configuration in the unified StrataConfig for use by the search handler.
//! The configuration is persisted to `strata.toml` for disk-backed databases.

use std::sync::Arc;

use strata_engine::ModelConfig;

use crate::bridge::Primitives;
use crate::{Output, Result};

/// Handle ConfigureModel command: store model endpoint configuration.
pub fn configure_model(
    p: &Arc<Primitives>,
    endpoint: String,
    model: String,
    api_key: Option<String>,
    timeout_ms: Option<u64>,
) -> Result<Output> {
    p.db.update_config(|cfg| {
        cfg.model = Some(ModelConfig {
            endpoint,
            model,
            api_key: api_key.map(strata_security::SensitiveString::from),
            timeout_ms: timeout_ms.unwrap_or(5000),
        });
    })
    .map_err(crate::Error::from)?;

    Ok(Output::Unit)
}
