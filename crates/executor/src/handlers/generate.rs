//! Handlers for text generation commands (Generate, Tokenize, Detokenize, GenerateUnload).

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::{Error, Output, Result};

/// Handle `Command::Generate { model, prompt, ... }`.
#[cfg(feature = "embed")]
#[allow(clippy::too_many_arguments)]
pub fn generate(
    p: &Arc<Primitives>,
    model: String,
    prompt: String,
    max_tokens: Option<usize>,
    temperature: Option<f32>,
    top_k: Option<usize>,
    top_p: Option<f32>,
    seed: Option<u64>,
    stop_tokens: Option<Vec<u32>>,
) -> Result<Output> {
    use crate::types::GenerationResult;
    use strata_intelligence::generate::GenerateModelState;

    if prompt.is_empty() {
        return Err(Error::InvalidInput {
            reason: "Prompt must not be empty".to_string(),
        });
    }

    let state =
        p.db.extension::<GenerateModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get generate model state: {}", e),
            })?;

    let entry = state
        .get_or_load(&model)
        .map_err(|e| Error::Internal { reason: e })?;

    let gen_config = strata_intelligence::GenerationConfig {
        max_tokens: max_tokens.unwrap_or(256),
        stop_tokens: stop_tokens.unwrap_or_default(),
        sampling: strata_intelligence::SamplingConfig {
            temperature: temperature.unwrap_or(0.0),
            top_k: top_k.unwrap_or(0),
            top_p: top_p.unwrap_or(1.0),
            seed,
        },
    };

    let (text, stop_reason, prompt_tokens, completion_tokens) =
        strata_intelligence::generate::with_engine(&entry, |engine| {
            let output = engine.generate_full(&prompt, &gen_config)?;
            let text = engine.decode(&output.token_ids);
            let completion_tokens = output.token_ids.len();
            Ok::<_, strata_intelligence::InferenceError>((
                text,
                output.stop_reason.to_string(),
                output.prompt_tokens,
                completion_tokens,
            ))
        })
        .map_err(|e| Error::Internal { reason: e })?
        .map_err(|e| Error::Internal {
            reason: format!("Generation failed: {}", e),
        })?;

    Ok(Output::Generated(GenerationResult {
        text,
        stop_reason,
        prompt_tokens,
        completion_tokens,
        model,
    }))
}

/// Handle `Command::Tokenize { model, text, add_special_tokens }`.
#[cfg(feature = "embed")]
pub fn tokenize(
    p: &Arc<Primitives>,
    model: String,
    text: String,
    add_special_tokens: Option<bool>,
) -> Result<Output> {
    use crate::types::TokenizeResult;
    use strata_intelligence::generate::GenerateModelState;

    let state =
        p.db.extension::<GenerateModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get generate model state: {}", e),
            })?;

    let entry = state
        .get_or_load(&model)
        .map_err(|e| Error::Internal { reason: e })?;

    let add_special = add_special_tokens.unwrap_or(true);

    let ids = strata_intelligence::generate::with_engine(&entry, |engine| {
        engine.encode(&text, add_special)
    })
    .map_err(|e| Error::Internal { reason: e })?;

    let count = ids.len();
    Ok(Output::TokenIds(TokenizeResult { ids, count, model }))
}

/// Handle `Command::Detokenize { model, ids }`.
#[cfg(feature = "embed")]
pub fn detokenize(p: &Arc<Primitives>, model: String, ids: Vec<u32>) -> Result<Output> {
    use strata_intelligence::generate::GenerateModelState;

    let state =
        p.db.extension::<GenerateModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get generate model state: {}", e),
            })?;

    let entry = state
        .get_or_load(&model)
        .map_err(|e| Error::Internal { reason: e })?;

    let text = strata_intelligence::generate::with_engine(&entry, |engine| engine.decode(&ids))
        .map_err(|e| Error::Internal { reason: e })?;

    Ok(Output::Text(text))
}

/// Handle `Command::GenerateUnload { model }`.
#[cfg(feature = "embed")]
pub fn generate_unload(p: &Arc<Primitives>, model: String) -> Result<Output> {
    use strata_intelligence::generate::GenerateModelState;

    let state =
        p.db.extension::<GenerateModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get generate model state: {}", e),
            })?;

    let was_loaded = state.unload(&model);
    Ok(Output::Bool(was_loaded))
}

// =========================================================================
// No-op stubs when embed feature is not compiled in
// =========================================================================

#[cfg(not(feature = "embed"))]
#[allow(clippy::too_many_arguments)]
pub fn generate(
    _p: &Arc<Primitives>,
    _model: String,
    _prompt: String,
    _max_tokens: Option<usize>,
    _temperature: Option<f32>,
    _top_k: Option<usize>,
    _top_p: Option<f32>,
    _seed: Option<u64>,
    _stop_tokens: Option<Vec<u32>>,
) -> Result<Output> {
    Err(Error::Internal {
        reason: "Generation not available: compile with --features embed".to_string(),
    })
}

#[cfg(not(feature = "embed"))]
pub fn tokenize(
    _p: &Arc<Primitives>,
    _model: String,
    _text: String,
    _add_special_tokens: Option<bool>,
) -> Result<Output> {
    Err(Error::Internal {
        reason: "Generation not available: compile with --features embed".to_string(),
    })
}

#[cfg(not(feature = "embed"))]
pub fn detokenize(_p: &Arc<Primitives>, _model: String, _ids: Vec<u32>) -> Result<Output> {
    Err(Error::Internal {
        reason: "Generation not available: compile with --features embed".to_string(),
    })
}

#[cfg(not(feature = "embed"))]
pub fn generate_unload(_p: &Arc<Primitives>, _model: String) -> Result<Output> {
    Err(Error::Internal {
        reason: "Generation not available: compile with --features embed".to_string(),
    })
}
