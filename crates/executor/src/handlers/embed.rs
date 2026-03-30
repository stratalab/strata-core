//! Handlers for direct embedding commands (Embed, EmbedBatch).
//!
//! These expose embedding via the executor command interface, using the
//! same EmbedModelState lifecycle as the auto-embed hook.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};

/// Handle `Command::Embed { text }`.
#[cfg(feature = "embed")]
pub fn embed(p: &Arc<Primitives>, text: String) -> Result<Output> {
    use strata_intelligence::embed::EmbedModelState;

    let model_dir = p.db.model_dir();
    let model_name = p.db.embed_model();
    let state =
        p.db.extension::<EmbedModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get embed model state: {}", e),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            })?;

    let shared = state
        .get_or_load(&model_dir, &model_name)
        .map_err(|e| Error::Internal {
            reason: format!("Failed to load embedding model: {}", e),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
    let embedding = engine.embed(&text).map_err(|e| Error::Internal {
        reason: format!("Embedding failed: {}", e),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })?;

    Ok(Output::Embedding(embedding))
}

/// Handle `Command::EmbedBatch { texts }`.
#[cfg(feature = "embed")]
pub fn embed_batch(p: &Arc<Primitives>, texts: Vec<String>) -> Result<Output> {
    use strata_intelligence::embed::EmbedModelState;

    let model_dir = p.db.model_dir();
    let model_name = p.db.embed_model();
    let state =
        p.db.extension::<EmbedModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get embed model state: {}", e),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            })?;

    let shared = state
        .get_or_load(&model_dir, &model_name)
        .map_err(|e| Error::Internal {
            reason: format!("Failed to load embedding model: {}", e),
            hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
        })?;

    let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
    let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
    let embeddings = engine.embed_batch(&refs).map_err(|e| Error::Internal {
        reason: format!("Batch embedding failed: {}", e),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })?;

    Ok(Output::Embeddings(embeddings))
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn embed(_p: &Arc<Primitives>, _text: String) -> Result<Output> {
    Err(Error::Internal {
        reason: "Embedding not available: compile with --features embed".to_string(),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub fn embed_batch(_p: &Arc<Primitives>, _texts: Vec<String>) -> Result<Output> {
    Err(Error::Internal {
        reason: "Embedding not available: compile with --features embed".to_string(),
        hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
    })
}
