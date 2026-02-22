//! Handlers for model management commands (ModelsList, ModelsPull).

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};

/// Handle `Command::ModelsList`.
#[cfg(feature = "embed")]
pub fn models_list(_p: &Arc<Primitives>) -> Result<Output> {
    use crate::types::ModelInfoOutput;

    let registry = strata_intelligence::ModelRegistry::new();
    let models = registry.list_available();

    let output: Vec<ModelInfoOutput> = models
        .into_iter()
        .map(|m| ModelInfoOutput {
            name: m.name,
            task: format!("{:?}", m.task),
            architecture: m.architecture,
            default_quant: m.default_quant,
            embedding_dim: m.embedding_dim,
            is_local: m.is_local,
            size_bytes: m.size_bytes,
        })
        .collect();

    Ok(Output::ModelsList(output))
}

/// Handle `Command::ModelsPull { name }`.
#[cfg(feature = "embed")]
pub fn models_pull(_p: &Arc<Primitives>, name: String) -> Result<Output> {
    let registry = strata_intelligence::ModelRegistry::new();
    let path = registry.pull(&name).map_err(|e| Error::Internal {
        reason: format!("Failed to pull model '{}': {}", name, e),
    })?;

    Ok(Output::ModelsPulled {
        name,
        path: path.display().to_string(),
    })
}

/// Handle `Command::ModelsLocal`.
#[cfg(feature = "embed")]
pub fn models_local(_p: &Arc<Primitives>) -> Result<Output> {
    use crate::types::ModelInfoOutput;

    let registry = strata_intelligence::ModelRegistry::new();
    let models = registry.list_local();

    let output: Vec<ModelInfoOutput> = models
        .into_iter()
        .map(|m| ModelInfoOutput {
            name: m.name,
            task: format!("{:?}", m.task),
            architecture: m.architecture,
            default_quant: m.default_quant,
            embedding_dim: m.embedding_dim,
            is_local: m.is_local,
            size_bytes: m.size_bytes,
        })
        .collect();

    Ok(Output::ModelsList(output))
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn models_local(_p: &Arc<Primitives>) -> Result<Output> {
    Err(Error::Internal {
        reason: "Model management not available: compile with --features embed".to_string(),
    })
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn models_list(_p: &Arc<Primitives>) -> Result<Output> {
    Err(Error::Internal {
        reason: "Model management not available: compile with --features embed".to_string(),
    })
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn models_pull(_p: &Arc<Primitives>, _name: String) -> Result<Output> {
    Err(Error::Internal {
        reason: "Model management not available: compile with --features embed".to_string(),
    })
}
