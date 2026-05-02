//! Handlers for direct embedding commands (Embed, EmbedBatch).
//!
//! These expose embedding via the executor command interface, using the
//! same EmbedModelState lifecycle as the auto-embed hook.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};

#[cfg(feature = "embed")]
const BUG_REPORT_HINT: &str =
    "This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues";

#[cfg(feature = "embed")]
fn internal_bug(reason: impl Into<String>) -> Error {
    Error::Internal {
        reason: reason.into(),
        hint: Some(BUG_REPORT_HINT.to_string()),
    }
}

#[cfg(feature = "embed")]
fn embed_failed(model: &str, reason: impl Into<String>) -> Error {
    Error::EmbedFailed {
        model: model.to_string(),
        reason: reason.into(),
        hint: None,
    }
}

/// Handle `Command::Embed { text }`.
#[cfg(feature = "embed")]
pub(crate) fn embed(p: &Arc<Primitives>, text: String) -> Result<Output> {
    use strata_intelligence::embed::EmbedModelState;

    let model_dir = p.db.model_dir();
    let model_name = p.db.embed_model();
    let api_key = strata_intelligence::embed::resolve_api_key_for_model(&p.db, &model_name);
    let state =
        p.db.extension::<EmbedModelState>()
            .map_err(|e| internal_bug(format!("Failed to get embed model state: {}", e)))?;

    let shared = state
        .get_or_load(&model_dir, &model_name, api_key.as_deref())
        .map_err(|e| Error::ModelLoadFailed {
            model: model_name.clone(),
            reason: e.to_string(),
            hint: Some("Check that the configured embedding model is available".to_string()),
        })?;

    let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
    let embedding = engine
        .embed(&text)
        .map_err(|e| embed_failed(&model_name, e.to_string()))?;

    Ok(Output::Embedding(embedding))
}

/// Handle `Command::EmbedBatch { texts }`.
#[cfg(feature = "embed")]
pub(crate) fn embed_batch(p: &Arc<Primitives>, texts: Vec<String>) -> Result<Output> {
    use strata_intelligence::embed::EmbedModelState;

    let model_dir = p.db.model_dir();
    let model_name = p.db.embed_model();
    let api_key = strata_intelligence::embed::resolve_api_key_for_model(&p.db, &model_name);
    let state =
        p.db.extension::<EmbedModelState>()
            .map_err(|e| internal_bug(format!("Failed to get embed model state: {}", e)))?;

    let shared = state
        .get_or_load(&model_dir, &model_name, api_key.as_deref())
        .map_err(|e| Error::ModelLoadFailed {
            model: model_name.clone(),
            reason: e.to_string(),
            hint: Some("Check that the configured embedding model is available".to_string()),
        })?;

    let engine = shared.lock().unwrap_or_else(|e| e.into_inner());
    let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
    let embeddings = engine
        .embed_batch(&refs)
        .map_err(|e| embed_failed(&model_name, e.to_string()))?;

    Ok(Output::Embeddings(embeddings))
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub(crate) fn embed(_p: &Arc<Primitives>, _text: String) -> Result<Output> {
    Err(Error::NotImplemented {
        feature: "Embed".to_string(),
        reason: "embedding commands require compiling with --features embed".to_string(),
    })
}

/// No-op when the embed feature is not compiled in.
#[cfg(not(feature = "embed"))]
pub(crate) fn embed_batch(_p: &Arc<Primitives>, _texts: Vec<String>) -> Result<Output> {
    Err(Error::NotImplemented {
        feature: "EmbedBatch".to_string(),
        reason: "embedding commands require compiling with --features embed".to_string(),
    })
}
