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
    stop_sequences: Option<Vec<String>>,
) -> Result<Output> {
    use crate::types::GenerationResult;
    use strata_intelligence::generate::GenerateModelState;

    if prompt.is_empty() {
        return Err(Error::InvalidInput {
            reason: "Prompt must not be empty".to_string(),
            hint: None,
        });
    }

    let state =
        p.db.extension::<GenerateModelState>()
            .map_err(|e| Error::Internal {
                reason: format!("Failed to get generate model state: {}", e),
            })?;

    // Read provider config from the database
    let cfg = p.db.config();
    let provider_name = cfg.provider.trim().to_ascii_lowercase();

    let entry = if provider_name == "local" {
        // Local inference: load from the model registry
        state
            .get_or_load(&model)
            .map_err(|e| Error::Internal { reason: e })?
    } else {
        // Cloud provider: parse provider kind, look up API key, dispatch
        let provider_kind: strata_intelligence::ProviderKind =
            provider_name.parse().map_err(|_| Error::InvalidInput {
                reason: format!(
                    "Unknown provider: {:?}. Valid providers: local, anthropic, openai, google",
                    cfg.provider
                ),
                hint: None,
            })?;

        // Resolve model name: use the command-supplied model, or fall back to default_model
        let resolved_model = if model.is_empty() {
            cfg.default_model.clone().unwrap_or_default()
        } else {
            model.clone()
        };

        if resolved_model.is_empty() {
            return Err(Error::InvalidInput {
                reason: "No model specified and no default_model configured. \
                     Use: CONFIGURE SET default_model \"model-name\""
                    .to_string(),
                hint: None,
            });
        }

        // Look up the API key for this provider
        let api_key = match provider_kind {
            strata_intelligence::ProviderKind::Anthropic => cfg.anthropic_api_key.clone(),
            strata_intelligence::ProviderKind::OpenAI => cfg.openai_api_key.clone(),
            strata_intelligence::ProviderKind::Google => cfg.google_api_key.clone(),
            strata_intelligence::ProviderKind::Local => unreachable!(),
        };

        let api_key = api_key.filter(|k| !k.is_empty()).ok_or_else(|| {
            let key_name = format!("{}_api_key", provider_kind);
            Error::InvalidInput {
                reason: format!(
                    "No API key configured for {} provider. \
                     Use: CONFIGURE SET {} \"your-api-key\"",
                    provider_kind, key_name
                ),
                hint: None,
            }
        })?;

        GenerateModelState::create_cloud_engine(
            provider_kind,
            api_key.into_inner(),
            &resolved_model,
        )
        .map_err(|e| Error::Internal { reason: e })?
    };

    let request = strata_intelligence::GenerateRequest {
        prompt,
        max_tokens: max_tokens.unwrap_or(256),
        temperature: temperature.unwrap_or(0.0),
        top_k: top_k.unwrap_or(0),
        top_p: top_p.unwrap_or(1.0),
        seed,
        stop_sequences: stop_sequences.unwrap_or_default(),
        stop_tokens: stop_tokens.unwrap_or_default(),
    };

    // Use the resolved model name in the output (may differ from input for cloud)
    let output_model = if provider_name != "local" && model.is_empty() {
        cfg.default_model.unwrap_or(model)
    } else {
        model
    };

    let (text, stop_reason, prompt_tokens, completion_tokens) =
        strata_intelligence::generate::with_engine(&entry, |engine| {
            let response = engine.generate(&request)?;
            Ok::<_, strata_intelligence::InferenceError>((
                response.text,
                response.stop_reason.to_string(),
                response.prompt_tokens,
                response.completion_tokens,
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
        model: output_model,
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
    .map_err(|e| Error::Internal { reason: e })?
    .map_err(|e| Error::Internal {
        reason: format!("Tokenization failed: {}", e),
    })?;

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
        .map_err(|e| Error::Internal { reason: e })?
        .map_err(|e| Error::Internal {
            reason: format!("Detokenization failed: {}", e),
        })?;

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
    _stop_sequences: Option<Vec<String>>,
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
