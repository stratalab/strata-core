//! Inference and model management operations.

use super::Strata;
use crate::types::*;
use crate::{Command, Error, Output, Result};

impl Strata {
    // =========================================================================
    // Generation
    // =========================================================================

    /// Generate text from a prompt using a local model.
    pub fn generate(
        &self,
        model: &str,
        prompt: &str,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
    ) -> Result<GenerationResult> {
        match self.execute_cmd(Command::Generate {
            model: model.to_string(),
            prompt: prompt.to_string(),
            max_tokens,
            temperature,
            top_k: None,
            top_p: None,
            seed: None,
            stop_tokens: None,
            stop_sequences: None,
        })? {
            Output::Generated(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Generate".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Generate text with full control over sampling parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_with_options(
        &self,
        model: &str,
        prompt: &str,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_k: Option<usize>,
        top_p: Option<f32>,
        seed: Option<u64>,
        stop_tokens: Option<Vec<u32>>,
        stop_sequences: Option<Vec<String>>,
    ) -> Result<GenerationResult> {
        match self.execute_cmd(Command::Generate {
            model: model.to_string(),
            prompt: prompt.to_string(),
            max_tokens,
            temperature,
            top_k,
            top_p,
            seed,
            stop_tokens,
            stop_sequences,
        })? {
            Output::Generated(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Generate".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Tokenize text into token IDs using a model's tokenizer.
    pub fn tokenize(
        &self,
        model: &str,
        text: &str,
        add_special_tokens: Option<bool>,
    ) -> Result<TokenizeResult> {
        match self.execute_cmd(Command::Tokenize {
            model: model.to_string(),
            text: text.to_string(),
            add_special_tokens,
        })? {
            Output::TokenIds(result) => Ok(result),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Tokenize".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Detokenize token IDs back to text.
    pub fn detokenize(&self, model: &str, ids: Vec<u32>) -> Result<String> {
        match self.execute_cmd(Command::Detokenize {
            model: model.to_string(),
            ids,
        })? {
            Output::Text(text) => Ok(text),
            _ => Err(Error::Internal {
                reason: "Unexpected output for Detokenize".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Unload a generation model from memory.
    ///
    /// Returns `true` if the model was loaded and has been unloaded.
    pub fn generate_unload(&self, model: &str) -> Result<bool> {
        match self.execute_cmd(Command::GenerateUnload {
            model: model.to_string(),
        })? {
            Output::Bool(was_loaded) => Ok(was_loaded),
            _ => Err(Error::Internal {
                reason: "Unexpected output for GenerateUnload".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    // =========================================================================
    // Model Management
    // =========================================================================

    /// List all available models in the registry.
    pub fn models_list(&self) -> Result<Vec<ModelInfoOutput>> {
        match self.execute_cmd(Command::ModelsList)? {
            Output::ModelsList(models) => Ok(models),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ModelsList".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Download a model by name.
    ///
    /// Returns (name, local_path) of the pulled model.
    pub fn models_pull(&self, name: &str) -> Result<(String, String)> {
        match self.execute_cmd(Command::ModelsPull {
            name: name.to_string(),
        })? {
            Output::ModelsPulled { name, path } => Ok((name, path)),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ModelsPull".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List locally downloaded models.
    pub fn models_local(&self) -> Result<Vec<ModelInfoOutput>> {
        match self.execute_cmd(Command::ModelsLocal)? {
            Output::ModelsList(models) => Ok(models),
            _ => Err(Error::Internal {
                reason: "Unexpected output for ModelsLocal".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }
}
