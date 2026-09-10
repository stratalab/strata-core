//! Runtime facade for model execution.

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
use std::sync::{Mutex, MutexGuard};

// Only the build that cannot download raises `download_disabled` here; the
// build that can lets the registry speak.
#[cfg(not(feature = "download"))]
use crate::error::RegistryFailure;
use crate::resolve::{AvailabilityKind, ModelSource, ModelUse, PullAction, ResolvedModel};
use crate::{
    generation_provider_feature_enabled, EnvProviderSettings, GenerateRequest, GenerateResponse,
    InferenceError, ModelInfo, ModelRegistry, ModelTask, ProviderKey, ProviderKind,
    ProviderSettings, UnsupportedKind,
};

#[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
use crate::error::ProviderFailure;

use crate::InferenceEngine;

// Ungated: `status` names every provider's key variable, including for
// providers this build cannot call — that is how it says what to set.
use crate::api_key_env_var;

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
use crate::GenerationEngine;

#[cfg(any(feature = "openai", feature = "google"))]
use crate::CloudEmbeddingEngine;

#[cfg(feature = "local")]
use crate::{EmbeddingEngine, RankingEngine};

/// Runtime configuration for model execution.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct InferenceRuntimeConfig {
    /// Optional model directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models_dir: Option<PathBuf>,
    /// Whether provider network calls and model downloads are allowed.
    pub network_enabled: bool,
}

impl Default for InferenceRuntimeConfig {
    fn default() -> Self {
        Self {
            models_dir: None,
            network_enabled: true,
        }
    }
}

/// Model cache facts exposed for diagnostics.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct ModelCacheStatus {
    /// Cached generation model specs.
    pub generation_models: Vec<String>,
    /// Cached embedding model specs.
    pub embedding_models: Vec<String>,
    /// Cached ranking model specs.
    pub ranking_models: Vec<String>,
}

/// Whether one provider can be used right now, and if not, why not.
///
/// Never carries a key — only where one was found (#3124/D11).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct ProviderStatus {
    /// Which provider this row describes.
    pub provider: ProviderKind,
    /// Whether this binary was compiled with the provider.
    pub feature_enabled: bool,
    /// Whether this provider needs an API key at all.
    pub requires_api_key: bool,
    /// Whether a key was found. Never the key itself.
    pub key_present: bool,
    /// The environment variable this provider reads its key from, whether or
    /// not one is set — so a caller can say what to do about a missing key.
    /// `None` for providers that need no key.
    pub key_env_var: Option<String>,
    /// Where the key was found, when one was. Never a value. `None` when no
    /// key is present.
    ///
    /// The runtime's [`ProviderSettings`] names the place: the environment
    /// variable, or the config file's path for a key stored with `strata
    /// config set <provider>.api_key` — so a caller is told the file, not a
    /// variable it never exported.
    pub key_source: Option<String>,
    /// Whether a call could be attempted right now.
    pub ready: bool,
    /// The model-spec prefix that selects this provider, e.g. `"openai:"`.
    ///
    /// A caller that finds a ready provider can use this directly: prefix any
    /// model name with it. That is the actionable form for a coding agent,
    /// which reads this over the human table.
    pub model_prefix: String,
}

/// What this binary can do before anything is attempted (D11).
///
/// #3124: every part of this was knowable up front and reported nowhere, so a
/// user learned their build's limits by watching an operation fail.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct InferenceStatus {
    /// Whether this binary can execute local models.
    pub local_execution: bool,
    /// Whether this binary can download model artifacts.
    pub model_download: bool,
    /// Every provider, in a stable order.
    pub providers: Vec<ProviderStatus>,
    /// The model directory, shared by every database on this machine.
    pub models_dir: PathBuf,
    /// Catalogued models with at least one variant downloaded — the models
    /// `models local` lists, judged the way resolution judges (a non-empty
    /// file; an interrupted download's zero-length leftover does not count).
    pub models_downloaded: usize,
    /// Catalogued models in total.
    pub models_catalogued: usize,
    /// What to do when local execution is needed and absent. `None` when this
    /// build already has it.
    pub local_remedy: Option<String>,
}

/// Provider/model capability facts.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct InferenceCapability {
    /// Provider kind.
    pub provider: ProviderKind,
    /// Model name or path after provider parsing.
    pub model: String,
    /// Whether the model can be used right now, and if not, the one reason
    /// why — the same answer a refusal carries in its `details`, given here
    /// before anything is attempted (#3226). Capability *locates* the model:
    /// a cloud model is `ready` from here even without its key (`status`
    /// reports keys), and a local model reports `not_downloaded` or
    /// `not_in_catalog` on the same basis a `pull` or a load would.
    pub availability: AvailabilityKind,
    /// The spec `strata inference models pull` takes to fetch the model.
    /// Present only when `availability` is `not_downloaded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_spec: Option<String>,
    /// The download size in bytes. Present only when `availability` is
    /// `not_downloaded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Whether **this binary** can generate with this model right now.
    ///
    /// False when the provider's feature is compiled out, even for a model
    /// that inherently generates — see `provider_feature_enabled` for why. The
    /// model's own task is in the catalog (`inference models list`).
    pub can_generate: bool,
    /// Whether **this binary** can tokenize with this model right now.
    pub can_tokenize: bool,
    /// Whether **this binary** can embed with this model right now.
    pub can_embed: bool,
    /// Whether **this binary** can rank with this model right now.
    pub can_rank: bool,
    /// Whether the operation requires network access.
    pub requires_network: bool,
    /// Whether the provider requires an API key.
    pub requires_api_key: bool,
    /// Whether this binary was compiled with the provider feature needed for execution.
    pub provider_feature_enabled: bool,
    /// Whether this runtime configuration currently allows network access.
    pub network_enabled: bool,
    /// Known embedding dimension, if available.
    pub embedding_dim: usize,
    /// Whether chat requests may offer `tools` (function calling).
    pub supports_tools: bool,
    /// Whether `response_format: json_object` is honored.
    pub supports_json_object: bool,
    /// Whether `response_format: json_schema` (structured output) is honored.
    pub supports_json_schema: bool,
    /// Whether `logprobs` are returned in the response.
    pub supports_logprobs: bool,
}

/// Pull-model command output.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct PullModelOutput {
    /// Requested model spec.
    pub model: String,
    /// Local path containing the resolved GGUF file.
    pub path: PathBuf,
}

/// Embedding request.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct EmbedRequest {
    /// Text to embed.
    pub text: String,
}

/// Batch embedding response.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct EmbedResponse {
    /// Ordered embedding item outcomes.
    pub items: Vec<EmbedRuntimeOutcome>,
    /// Embedding dimension when known.
    pub dimension: usize,
}

/// Embedding item outcome.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EmbedRuntimeOutcome {
    /// Successful embedding item.
    Ok {
        /// Embedding vector.
        vector: Vec<f32>,
    },
    /// Failed embedding item.
    Error {
        /// Stable error code.
        code: String,
        /// Redacted public error message.
        message: String,
    },
}

/// Ranking request.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct RankRequest {
    /// Query text.
    pub query: String,
    /// Candidate passages.
    pub passages: Vec<String>,
}

/// Ranking response.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct RankResponse {
    /// Ordered ranking item outcomes.
    pub items: Vec<RankRuntimeOutcome>,
}

/// Ranking item outcome.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RankRuntimeOutcome {
    /// Successful ranking score.
    Ok {
        /// Passage index.
        index: usize,
        /// Relevance score.
        score: f32,
    },
    /// Failed ranking item.
    Error {
        /// Passage index.
        index: usize,
        /// Stable error code.
        code: String,
        /// Redacted public error message.
        message: String,
    },
}

/// Public runtime facade.
#[derive(Debug)]
pub struct InferenceRuntime {
    config: InferenceRuntimeConfig,
    /// The one registry (R5). Built once from `config.models_dir`; every
    /// resolution, listing and download reads this instance, so there is no
    /// second registry somewhere that could be looking at a different
    /// directory (#3260).
    registry: ModelRegistry,
    /// The one source of provider keys (R4): resolution, `status` and the
    /// provider call all ask this instance, so they cannot disagree.
    settings: Settings,
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    generation: Mutex<HashMap<String, GenerationEngine>>,
    #[cfg(feature = "local")]
    embeddings: Mutex<HashMap<String, EmbeddingEngine>>,
    #[cfg(feature = "local")]
    rankers: Mutex<HashMap<String, RankingEngine>>,
}

/// The injected [`ProviderSettings`], opaque to `Debug` so a runtime dump can
/// never show a key an implementation happens to hold.
struct Settings(Arc<dyn ProviderSettings>);

impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderSettings")
    }
}

impl Default for InferenceRuntime {
    fn default() -> Self {
        Self::new(InferenceRuntimeConfig::default())
    }
}

impl InferenceRuntime {
    /// Creates a runtime facade that reads provider keys from the process
    /// environment ([`EnvProviderSettings`]).
    pub fn new(config: InferenceRuntimeConfig) -> Self {
        Self::with_settings(config, Arc::new(EnvProviderSettings))
    }

    /// Creates a runtime facade with an injected source of provider settings
    /// (R4). This is how an embedding application adds places to look for a
    /// key beyond the environment — the executor composes the environment
    /// with the user's config file — and how a test fixes the answer without
    /// touching the environment.
    pub fn with_settings(
        config: InferenceRuntimeConfig,
        settings: Arc<dyn ProviderSettings>,
    ) -> Self {
        let registry = config
            .models_dir
            .clone()
            .map_or_else(ModelRegistry::new, ModelRegistry::with_dir);
        Self {
            config,
            registry,
            settings: Settings(settings),
            #[cfg(any(
                feature = "local",
                feature = "anthropic",
                feature = "openai",
                feature = "google"
            ))]
            generation: Mutex::new(HashMap::new()),
            #[cfg(feature = "local")]
            embeddings: Mutex::new(HashMap::new()),
            #[cfg(feature = "local")]
            rankers: Mutex::new(HashMap::new()),
        }
    }

    /// Returns catalog model information.
    pub fn list_models(&self) -> Vec<ModelInfo> {
        self.registry().list_available()
    }

    /// Returns locally available catalog model information.
    pub fn list_local_models(&self) -> Vec<ModelInfo> {
        self.registry().list_local()
    }

    /// Downloads a catalogued model into the local model directory, or reports
    /// the file that is already there.
    ///
    /// Goes through the resolver like every other verb (R6), so a pull answers
    /// the same questions in the same order as a load: a malformed spec is
    /// `invalid_request`, an unknown name `unknown_model`, a cloud model
    /// `unsupported_operation` — not "requires network access" for all three
    /// (#3255). What pull does NOT ask is whether the model can *run* here:
    /// fetching a file needs no local execution and no particular task.
    ///
    /// D8: this is the one place that downloads; loading never does.
    pub fn pull_model(&self, model: &str) -> Result<PullModelOutput, InferenceError> {
        let resolved = self.resolve(model, None)?;
        match resolved.pull_action(self.config.network_enabled)? {
            // Already on disk: no network needed to say so.
            PullAction::Present(path) => Ok(PullModelOutput {
                model: resolved.spec.clone(),
                path: path.to_path_buf(),
            }),
            PullAction::Download { entry, variant } => {
                #[cfg(feature = "download")]
                {
                    let path = self.registry.pull_variant(entry, variant, |_, _| {})?;
                    Ok(PullModelOutput {
                        model: resolved.spec.clone(),
                        path,
                    })
                }
                #[cfg(not(feature = "download"))]
                {
                    // The code is `download_disabled` because the kind says
                    // so, not because of any word in the text. The remedy
                    // names the exact file: this build cannot fetch it, so the
                    // reader has to.
                    Err(InferenceError::RegistryFailed {
                        kind: RegistryFailure::DownloadDisabled,
                        message: format!(
                            "model download is not built into this binary: \
                             {LOCAL_UNAVAILABLE_REMEDY} To use `{spec}` anyway, place \
                             `{file}` from https://huggingface.co/{repo} in the models \
                             directory yourself — `strata inference status` shows the \
                             directory.",
                            spec = resolved.spec,
                            file = variant.hf_file,
                            repo = entry.hf_repo,
                        ),
                        details: Some(Box::new(resolved.details())),
                    })
                }
            }
        }
    }

    /// Reports what this binary can do, before anything is attempted.
    ///
    /// Answers in one place the questions that previously had to be inferred
    /// from a failure: which providers are compiled in, which have a key and
    /// where it came from, whether local execution exists in this build, and
    /// how many catalogued models are already on disk.
    pub fn status(&self) -> InferenceStatus {
        let providers = [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
            ProviderKind::Local,
        ]
        .into_iter()
        .map(|provider| {
            let feature_enabled = generation_provider_feature_enabled(provider)
                || embedding_provider_feature_enabled_for_capability(provider);
            let requires_api_key = provider != ProviderKind::Local;
            let key_env_var = requires_api_key.then(|| api_key_env_var(provider).to_owned());
            let key_source =
                reported_key_source(requires_api_key, self.settings.0.key(provider).as_ref());
            let key_present = key_source.is_some();
            ProviderStatus {
                provider,
                feature_enabled,
                requires_api_key,
                key_present,
                key_env_var,
                key_source,
                ready: provider_is_ready(feature_enabled, key_present, requires_api_key),
                model_prefix: format!("{provider}:"),
            }
        })
        .collect();

        let registry = self.registry();
        InferenceStatus {
            local_execution: cfg!(feature = "local"),
            model_download: cfg!(feature = "download"),
            providers,
            models_dir: registry.models_dir().to_path_buf(),
            // What `models local` lists: any downloaded variant counts, not
            // only the default quant that `list_available` reports on.
            models_downloaded: registry.list_local().len(),
            models_catalogued: registry.list_available().len(),
            local_remedy: (!cfg!(feature = "local")).then(|| LOCAL_UNAVAILABLE_REMEDY.to_owned()),
        }
    }

    /// Returns capability facts for a model spec.
    pub fn capability(&self, model_spec: &str) -> Result<InferenceCapability, InferenceError> {
        // Located, never loaded: capability answers for unknown names and
        // missing files too, with nothing claimed for them.
        let resolved = self.resolve(model_spec, None)?;
        let provider = resolved.provider;
        // The one answer a refusal would carry, reported here instead: the
        // availability and, when a download would fix it, what to pull.
        let details = resolved.details();
        // The resolver's own name resolution, so an alias or a quant suffix
        // reports the same entry it would load.
        let entry = match resolved.source {
            ModelSource::Catalog { entry, .. } => Some(entry),
            ModelSource::GgufPath(_) | ModelSource::Uncatalogued { .. } | ModelSource::Cloud => {
                None
            }
        };
        let task = entry.map(|entry| entry.task);
        let abilities = ModelAbilities::of(provider, task);
        Ok(InferenceCapability {
            provider,
            model: resolved.name,
            availability: details.availability,
            pull_spec: details.pull_spec,
            size_bytes: details.size_bytes,
            // #3124: `can_*` answers "can THIS BINARY do this, now" — not
            // "does the model support it". The two diverge whenever a provider
            // feature is compiled out, and a released binary has `local` off:
            // reporting `can_embed: true` beside `provider_feature_enabled:
            // false` made every caller responsible for ANDing the two, and the
            // more prominent field was the wrong one. What the model inherently
            // is stays discoverable through the catalog's `task`.
            //
            // The two halves are separated deliberately: `ModelAbilities` is
            // what the model supports, decided by a pure function with its own
            // truth table, and the feature check is what this build can run.
            // Folded together, every branch of the first half was unobservable
            // in a build with the feature off (`x && false` is false whatever
            // `x` is), so the mutation gate could not distinguish them.
            can_generate: abilities.generate && generation_provider_feature_enabled(provider),
            can_tokenize: abilities.tokenize && cfg!(feature = "local"),
            can_embed: abilities.embed
                && embedding_provider_feature_enabled_for_capability(provider),
            can_rank: abilities.rank && cfg!(feature = "local"),
            requires_network: provider != ProviderKind::Local,
            requires_api_key: provider != ProviderKind::Local,
            provider_feature_enabled: generation_provider_feature_enabled(provider)
                || embedding_provider_feature_enabled_for_capability(provider),
            network_enabled: self.config.network_enabled,
            embedding_dim: entry.map_or(0, |entry| entry.embedding_dim),
            // Chat feature support per provider. `json_object` is unsupported by
            // Anthropic (use json_schema); `logprobs` are unsupported by
            // Anthropic and local (deferred). Local structured outputs and tool
            // calling are grammar-based.
            supports_tools: true,
            supports_json_object: provider != ProviderKind::Anthropic,
            supports_json_schema: true,
            supports_logprobs: matches!(provider, ProviderKind::OpenAI | ProviderKind::Google),
        })
    }

    /// Generates text from a model spec.
    pub fn generate(
        &self,
        model_spec: &str,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Run(ModelTask::Generate)))?;
        resolved.require_ready()?;

        #[cfg(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        ))]
        {
            if resolved.source.is_cloud() {
                let mut engine = self.cloud_generation_engine(&resolved)?;
                return engine.generate(request);
            }
            let mut cache = self.lock_generation();
            let engine = self.cached_generation_engine(&mut cache, &resolved, None)?;
            engine.generate(request)
        }

        #[cfg(not(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        )))]
        {
            // There is no engine in this build to hand the request to.
            let _ = request;
            unreachable!(
                "`require_ready` refuses every model in a build with no provider feature: `{}`",
                resolved.spec
            )
        }
    }

    /// Runs a chat/generation request (the OpenAI-shaped body).
    ///
    /// Local applies the model's chat template and the full sampler chain; cloud
    /// providers map messages natively (system prompt, multi-turn history,
    /// assistant prefill) and forward every knob the provider supports.
    /// `model_config` is threaded into local model loading (cache-keyed).
    pub fn chat(
        &self,
        model_spec: &str,
        request: &crate::wire::ChatRequest,
    ) -> Result<crate::wire::ChatResponse, InferenceError> {
        request.validate()?;
        let resolved = self.resolve(model_spec, Some(ModelUse::Run(ModelTask::Generate)))?;
        resolved.require_ready()?;

        #[cfg(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        ))]
        {
            let mut response = if resolved.source.is_cloud() {
                let mut engine = self.cloud_generation_engine(&resolved)?;
                engine.generate_chat(request)?
            } else {
                let mut cache = self.lock_generation();
                let engine = self.cached_generation_engine(
                    &mut cache,
                    &resolved,
                    request.model_config.as_ref(),
                )?;
                engine.generate_chat(request)?
            };
            response.model = model_spec.to_string();
            Ok(response)
        }

        #[cfg(not(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        )))]
        {
            unreachable!(
                "`require_ready` refuses every model in a build with no provider feature: `{}`",
                resolved.spec
            )
        }
    }

    /// Runs an embeddings request (single or batch, OpenAI-shaped).
    ///
    /// Phase A bridges to [`Self::embed_batch`]; `dimensions`/`normalize`/
    /// `input_type` are accepted but applied in a later phase.
    pub fn embeddings(
        &self,
        model_spec: &str,
        request: &crate::wire::EmbeddingsRequest,
    ) -> Result<crate::wire::EmbeddingsResponse, InferenceError> {
        let texts = request.input.to_vec();
        let batch = self.embed_batch(model_spec, &texts)?;
        let mut data = Vec::with_capacity(batch.items.len());
        for (index, item) in batch.items.into_iter().enumerate() {
            match item {
                EmbedRuntimeOutcome::Ok { vector } => data.push(crate::wire::EmbeddingItem {
                    index: index as u32,
                    embedding: vector,
                }),
                EmbedRuntimeOutcome::Error { code, message } => {
                    return Err(InferenceError::Provider(format!(
                        "embedding item {index} failed [{code}]: {message}"
                    )));
                }
            }
        }
        Ok(crate::wire::EmbeddingsResponse {
            model: model_spec.to_string(),
            dimension: batch.dimension,
            data,
            usage: crate::wire::Usage::default(),
        })
    }

    /// Tokenizes text with a local generation model.
    pub fn tokenize(
        &self,
        model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Tokenize))?;
        resolved.require_ready()?;

        #[cfg(feature = "local")]
        {
            let mut cache = self.lock_generation();
            let engine = self.cached_generation_engine(&mut cache, &resolved, None)?;
            engine.encode(text, add_special)
        }

        #[cfg(not(feature = "local"))]
        {
            // Tokenizing is a property of a loaded GGUF, which this build
            // cannot load.
            let _ = (text, add_special);
            unreachable!(
                "`require_ready` refuses every tokenize in a build without local execution: `{}`",
                resolved.spec
            )
        }
    }

    /// Detokenizes local token ids.
    pub fn detokenize(&self, model_spec: &str, ids: &[u32]) -> Result<String, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Tokenize))?;
        resolved.require_ready()?;

        #[cfg(feature = "local")]
        {
            let mut cache = self.lock_generation();
            let engine = self.cached_generation_engine(&mut cache, &resolved, None)?;
            engine.decode(ids)
        }

        #[cfg(not(feature = "local"))]
        {
            // Detokenizing is a property of a loaded GGUF, which this build
            // cannot load.
            let _ = ids;
            unreachable!(
                "`require_ready` refuses every detokenize in a build without local execution: `{}`",
                resolved.spec
            )
        }
    }

    /// Embeds one text.
    pub fn embed(
        &self,
        model_spec: &str,
        request: &EmbedRequest,
    ) -> Result<Vec<f32>, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Run(ModelTask::Embed)))?;
        resolved.require_ready()?;
        self.with_embedding_engine(&resolved, |engine| engine.embed(&request.text))
    }

    /// Embeds a batch of texts.
    pub fn embed_batch(
        &self,
        model_spec: &str,
        texts: &[String],
    ) -> Result<EmbedResponse, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Run(ModelTask::Embed)))?;
        resolved.require_ready()?;

        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let embeddings =
            self.with_embedding_engine(&resolved, |engine| engine.embed_batch(&refs))?;
        let dimension = embeddings.first().map_or(0, Vec::len);
        Ok(EmbedResponse {
            dimension,
            items: embeddings
                .into_iter()
                .map(|vector| EmbedRuntimeOutcome::Ok { vector })
                .collect(),
        })
    }

    /// Ranks passages against a query.
    pub fn rank(
        &self,
        model_spec: &str,
        request: &RankRequest,
    ) -> Result<RankResponse, InferenceError> {
        let resolved = self.resolve(model_spec, Some(ModelUse::Run(ModelTask::Rank)))?;
        resolved.require_ready()?;

        let refs: Vec<&str> = request.passages.iter().map(String::as_str).collect();
        let scores =
            self.with_ranking_engine(&resolved, |engine| engine.rank(&request.query, &refs))?;
        Ok(RankResponse {
            items: scores
                .into_iter()
                .enumerate()
                .map(|(index, score)| RankRuntimeOutcome::Ok { index, score })
                .collect(),
        })
    }

    /// Unloads cached local models. When `model_spec` is `None`, all caches are cleared.
    ///
    /// Cache keys are resolved specs (trimmed), so the argument is trimmed
    /// the same way before it is matched.
    pub fn unload(&self, model_spec: Option<&str>) -> Result<bool, InferenceError> {
        let _model_spec = model_spec.map(str::trim);
        #[cfg(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        ))]
        let mut unloaded = false;
        #[cfg(not(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        )))]
        let unloaded = false;
        #[cfg(any(
            feature = "local",
            feature = "anthropic",
            feature = "openai",
            feature = "google"
        ))]
        {
            let mut cache = self.lock_generation();
            unloaded |= remove_matching(&mut cache, _model_spec);
        }
        #[cfg(feature = "local")]
        {
            let mut cache = self.lock_embeddings();
            unloaded |= remove_matching(&mut cache, _model_spec);
            let mut cache = self.lock_rankers();
            unloaded |= remove_matching(&mut cache, _model_spec);
        }
        Ok(unloaded)
    }

    /// Returns model cache status.
    pub fn cache_status(&self) -> Result<ModelCacheStatus, InferenceError> {
        Ok(ModelCacheStatus {
            generation_models: {
                #[cfg(any(
                    feature = "local",
                    feature = "anthropic",
                    feature = "openai",
                    feature = "google"
                ))]
                {
                    let cache = self.lock_generation();
                    sorted_keys(&cache)
                }
                #[cfg(not(any(
                    feature = "local",
                    feature = "anthropic",
                    feature = "openai",
                    feature = "google"
                )))]
                {
                    Vec::new()
                }
            },
            embedding_models: {
                #[cfg(feature = "local")]
                {
                    let cache = self.lock_embeddings();
                    sorted_keys(&cache)
                }
                #[cfg(not(feature = "local"))]
                {
                    Vec::new()
                }
            },
            ranking_models: {
                #[cfg(feature = "local")]
                {
                    let cache = self.lock_rankers();
                    sorted_keys(&cache)
                }
                #[cfg(not(feature = "local"))]
                {
                    Vec::new()
                }
            },
        })
    }

    /// The runtime's one registry (R5).
    pub(crate) fn registry(&self) -> &ModelRegistry {
        &self.registry
    }

    /// Whether provider network calls and model downloads are allowed.
    pub(crate) fn network_enabled(&self) -> bool {
        self.config.network_enabled
    }

    /// Runs `op` on the embedding engine for a resolved, ready model: the
    /// cloud engine for a cloud spec, the cached local engine otherwise.
    ///
    /// The one place the build's shape shows: `require_ready` has already
    /// refused every model this build cannot serve, so the twin for a missing
    /// feature is unreachable rather than a second, differently-worded
    /// refusal.
    fn with_embedding_engine<T>(
        &self,
        resolved: &ResolvedModel,
        op: impl FnOnce(&dyn InferenceEngine) -> Result<T, InferenceError>,
    ) -> Result<T, InferenceError> {
        if resolved.source.is_cloud() {
            #[cfg(any(feature = "openai", feature = "google"))]
            {
                let engine = self.cloud_embedding_engine(resolved)?;
                return op(&engine);
            }
            #[cfg(not(any(feature = "openai", feature = "google")))]
            {
                unreachable!(
                    "`require_ready` refuses every cloud embedding in a build without an \
                     embedding provider: `{}`",
                    resolved.spec
                )
            }
        }
        #[cfg(feature = "local")]
        {
            let mut cache = self.lock_embeddings();
            let engine = self.cached_embedding_engine(&mut cache, resolved)?;
            op(engine)
        }
        #[cfg(not(feature = "local"))]
        {
            // No local engine exists in this build to run `op` on.
            let _ = op;
            unreachable!(
                "`require_ready` refuses every local embedding in a build without local \
                 execution: `{}`",
                resolved.spec
            )
        }
    }

    /// Runs `op` on the cached local ranking engine for a resolved, ready
    /// model. Ranking is local-only; see [`Self::with_embedding_engine`] on
    /// why the other twin is unreachable.
    fn with_ranking_engine<T>(
        &self,
        resolved: &ResolvedModel,
        op: impl FnOnce(&dyn InferenceEngine) -> Result<T, InferenceError>,
    ) -> Result<T, InferenceError> {
        #[cfg(feature = "local")]
        {
            let mut cache = self.lock_rankers();
            let engine = self.cached_ranking_engine(&mut cache, resolved)?;
            op(engine)
        }
        #[cfg(not(feature = "local"))]
        {
            // No ranking engine exists in this build to run `op` on.
            let _ = op;
            unreachable!(
                "`require_ready` refuses every rank in a build without local execution: `{}`",
                resolved.spec
            )
        }
    }

    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn lock_generation(&self) -> MutexGuard<'_, HashMap<String, GenerationEngine>> {
        lock_model_cache(&self.generation, "generation")
    }

    #[cfg(feature = "local")]
    fn lock_embeddings(&self) -> MutexGuard<'_, HashMap<String, EmbeddingEngine>> {
        lock_model_cache(&self.embeddings, "embedding")
    }

    #[cfg(feature = "local")]
    fn lock_rankers(&self) -> MutexGuard<'_, HashMap<String, RankingEngine>> {
        lock_model_cache(&self.rankers, "ranking")
    }

    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn cached_generation_engine<'a>(
        &self,
        cache: &'a mut HashMap<String, GenerationEngine>,
        resolved: &ResolvedModel,
        config: Option<&crate::wire::ModelConfig>,
    ) -> Result<&'a mut GenerationEngine, InferenceError> {
        let key = engine_cache_key(&resolved.spec, config);
        if !cache.contains_key(&key) {
            let engine = local_generation_engine(resolved, config)?;
            cache.insert(key.clone(), engine);
        }
        Ok(cache
            .get_mut(&key)
            .expect("generation engine inserted before lookup"))
    }

    #[cfg(feature = "local")]
    fn cached_embedding_engine<'a>(
        &self,
        cache: &'a mut HashMap<String, EmbeddingEngine>,
        resolved: &ResolvedModel,
    ) -> Result<&'a EmbeddingEngine, InferenceError> {
        let key = resolved.spec.as_str();
        if !cache.contains_key(key) {
            let engine = load_local(resolved, |path| EmbeddingEngine::from_gguf(path))?;
            cache.insert(key.to_owned(), engine);
        }
        Ok(cache
            .get(key)
            .expect("embedding engine inserted before lookup"))
    }

    #[cfg(feature = "local")]
    fn cached_ranking_engine<'a>(
        &self,
        cache: &'a mut HashMap<String, RankingEngine>,
        resolved: &ResolvedModel,
    ) -> Result<&'a RankingEngine, InferenceError> {
        let key = resolved.spec.as_str();
        if !cache.contains_key(key) {
            let engine = load_local(resolved, |path| RankingEngine::from_gguf(path))?;
            cache.insert(key.to_owned(), engine);
        }
        Ok(cache
            .get(key)
            .expect("ranking engine inserted before lookup"))
    }
}

impl InferenceRuntime {
    /// Builds a cloud generation engine for a resolved, ready cloud model. The
    /// key is read here, after `require_ready` has already established it is
    /// set.
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn cloud_generation_engine(
        &self,
        resolved: &ResolvedModel,
    ) -> Result<GenerationEngine, InferenceError> {
        #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
        {
            let api_key = self.api_key(resolved.provider)?;
            GenerationEngine::cloud(resolved.provider, api_key, resolved.name.clone())
        }
        #[cfg(not(any(feature = "anthropic", feature = "openai", feature = "google")))]
        {
            unreachable!(
                "`require_ready` refuses every cloud model in a build without a cloud provider: `{}`",
                resolved.spec
            )
        }
    }

    #[cfg(any(feature = "openai", feature = "google"))]
    fn cloud_embedding_engine(
        &self,
        resolved: &ResolvedModel,
    ) -> Result<CloudEmbeddingEngine, InferenceError> {
        let api_key = self.api_key(resolved.provider)?;
        CloudEmbeddingEngine::new(resolved.provider, api_key, resolved.name.clone())
    }

    /// The provider's key, from this runtime's [`ProviderSettings`].
    ///
    /// The resolver has already refused a missing key by the time an engine is
    /// built, so the error here is the same `missing_api_key` with the same
    /// text — one authoring site, `missing_api_key_message` — for the window
    /// in which the key disappears between the two reads.
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn api_key(&self, provider: ProviderKind) -> Result<String, InferenceError> {
        if provider == ProviderKind::Local {
            return Err(InferenceError::Unsupported {
                kind: UnsupportedKind::Provider,
                message: "the local provider does not use API keys".to_owned(),
                details: None,
            });
        }
        self.settings.0.key(provider).map_or_else(
            || {
                Err(InferenceError::ProviderFailed {
                    kind: ProviderFailure::MissingApiKey,
                    message: crate::resolve::missing_api_key_message(
                        provider,
                        api_key_env_var(provider),
                    ),
                    details: None,
                })
            },
            |key| Ok(key.secret().to_owned()),
        )
    }

    /// Whether this runtime's settings hold a key for `provider`: the answer
    /// the resolver folds into `Availability::KeyMissing`.
    pub(crate) fn key_present(&self, provider: ProviderKind) -> bool {
        self.settings.0.key(provider).is_some()
    }
}

/// Loads a local generation engine from the resolved model's file.
#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
fn local_generation_engine(
    resolved: &ResolvedModel,
    config: Option<&crate::wire::ModelConfig>,
) -> Result<GenerationEngine, InferenceError> {
    #[cfg(feature = "local")]
    {
        load_local(resolved, |path| {
            GenerationEngine::from_gguf_with_config(path, config)
        })
    }
    #[cfg(not(feature = "local"))]
    {
        // No loader exists in this build to hand the config to.
        let _ = config;
        unreachable!(
            "`require_ready` refuses every local model in a build without local execution: `{}`",
            resolved.spec
        )
    }
}

/// Loads a local engine from the file a ready, resolved model names.
///
/// A catalogued model whose load fails is checked for corruption: a file
/// whose size is not the catalogued size is deleted so the next
/// `strata inference models pull` fetches a fresh copy instead of tripping
/// over it forever. A caller-supplied GGUF path is never deleted — nothing is
/// known about what size it should be.
#[cfg(feature = "local")]
fn load_local<T>(
    resolved: &ResolvedModel,
    load: impl FnOnce(&std::path::Path) -> Result<T, InferenceError>,
) -> Result<T, InferenceError> {
    match &resolved.source {
        ModelSource::Catalog { variant, path, .. } => load(path).inspect_err(|_| {
            crate::registry::discard_if_corrupt(variant, path);
        }),
        ModelSource::GgufPath(path) => load(path),
        ModelSource::Uncatalogued { .. } | ModelSource::Cloud => unreachable!(
            "`require_ready` passes only models with a file to load: `{}`",
            resolved.spec
        ),
    }
}

// Delegates to the inherent methods above — Rust resolves `self.method()` to the
// inherent method (preferred over trait methods), so there is no recursion.
impl crate::InferenceService for InferenceRuntime {
    fn list_models(&self) -> Vec<crate::ModelInfo> {
        self.list_models()
    }

    fn list_local_models(&self) -> Vec<crate::ModelInfo> {
        self.list_local_models()
    }

    fn pull_model(&self, model: &str) -> Result<crate::PullModelOutput, InferenceError> {
        self.pull_model(model)
    }

    fn capability(&self, model_spec: &str) -> Result<crate::InferenceCapability, InferenceError> {
        self.capability(model_spec)
    }

    fn chat(
        &self,
        model_spec: &str,
        request: &crate::wire::ChatRequest,
    ) -> Result<crate::wire::ChatResponse, InferenceError> {
        self.chat(model_spec, request)
    }

    fn tokenize(
        &self,
        model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError> {
        self.tokenize(model_spec, text, add_special)
    }

    fn detokenize(&self, model_spec: &str, ids: &[u32]) -> Result<String, InferenceError> {
        self.detokenize(model_spec, ids)
    }

    fn embeddings(
        &self,
        model_spec: &str,
        request: &crate::wire::EmbeddingsRequest,
    ) -> Result<crate::wire::EmbeddingsResponse, InferenceError> {
        self.embeddings(model_spec, request)
    }

    fn rank(
        &self,
        model_spec: &str,
        request: &crate::RankRequest,
    ) -> Result<crate::RankResponse, InferenceError> {
        self.rank(model_spec, request)
    }

    fn unload(&self, model_spec: Option<&str>) -> Result<bool, InferenceError> {
        self.unload(model_spec)
    }

    fn cache_status(&self) -> Result<crate::ModelCacheStatus, InferenceError> {
        self.cache_status()
    }

    fn status(&self) -> InferenceStatus {
        self.status()
    }
}

/// Cache key for a loaded generation engine. A default or absent config shares
/// the plain-spec key (so tokenize/generate and a config-less chat reuse one
/// engine); a non-default config keys a distinct engine so its load params
/// (`n_ctx`/`n_gpu_layers`/`n_batch`/`n_threads`) take effect.
#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
fn engine_cache_key(model_spec: &str, config: Option<&crate::wire::ModelConfig>) -> String {
    match config {
        Some(c) if *c != crate::wire::ModelConfig::default() => format!("{model_spec}\u{0}{c:?}"),
        _ => model_spec.to_owned(),
    }
}

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
fn sorted_keys<T>(map: &HashMap<String, T>) -> Vec<String> {
    let mut keys: Vec<String> = map.keys().cloned().collect();
    keys.sort();
    keys
}

#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
fn remove_matching<T>(map: &mut HashMap<String, T>, model_spec: Option<&str>) -> bool {
    match model_spec {
        Some(model_spec) => map.remove(model_spec).is_some(),
        None => {
            let had_entries = !map.is_empty();
            map.clear();
            had_entries
        }
    }
}

/// Locks a model cache, recovering if a panic on another thread poisoned it.
///
/// The lock is held across every engine load and call, so a poisoned lock
/// means a thread unwound while it had one entry borrowed — a local
/// llama.cpp context possibly mid-decode. `PoisonError` does not say which
/// entry, so the whole map is treated as suspect: every cached engine is
/// dropped rather than reused (the engine precedents in `persistence/` keep
/// their data because it holds no invariant a panic can break; a native
/// context does), the poison flag is cleared, and the next caller sees an
/// empty, working cache. A model reload is the price of one panic. Before
/// #3249 the poison was reported as `inference.io_failure` ("inspect
/// filesystem permissions") on every later call for the rest of the process.
#[cfg(any(
    feature = "local",
    feature = "anthropic",
    feature = "openai",
    feature = "google"
))]
fn lock_model_cache<'a, T>(
    cache: &'a Mutex<HashMap<String, T>>,
    kind: &'static str,
) -> MutexGuard<'a, HashMap<String, T>> {
    cache.lock().unwrap_or_else(|poisoned| {
        let mut guard = poisoned.into_inner();
        tracing::warn!(
            cache = kind,
            dropped_models = guard.len(),
            "model cache lock was poisoned by a panic on another thread; dropping its cached models"
        );
        // Clear before un-poisoning: if an engine's Drop panics mid-clear,
        // the guard's unwind re-poisons the lock and the next caller recovers
        // again from the half-cleared map. The other order would let a clean
        // flag outlive a failed recovery.
        guard.clear();
        cache.clear_poison();
        guard
    })
}

/// What to tell a caller who needs local model execution and does not have it.
///
/// **One authoring site on purpose.** Seven refusal messages, the status
/// renderer, and the model listing all said this, and #3124 was partly about
/// them disagreeing. It is also the string that changes when D2 lands
/// `strata inference install-local` — at which point this becomes a command,
/// and only here.
///
/// It names `strata inference install-local` and never `cargo install`: the
/// callers of this surface are mostly coding agents, which have no Rust
/// toolchain and cannot answer a build prompt. A remediation an agent cannot
/// execute is the same dead end as no remediation at all.
///
/// It also states that **a bare model name means a local model**, because
/// without that the advice to "use a cloud model" is unactionable: the reader
/// has just named a model and is being told to name one, with no way to see
/// what should change. `miniLM` and `local:miniLM` are the same spec; the
/// prefix is the whole difference.
pub const LOCAL_UNAVAILABLE_REMEDY: &str =
    "this build runs cloud models only, and a bare model name means a local \
     model. Either name a cloud model instead — `openai:<model>`, \
     `google:<model>` or `anthropic:<model>` — or run `strata inference \
     install-local` to add local execution.";

/// The `key_source` a `status` row reports: where the key was found, for a
/// provider that needs one and has one; nothing otherwise.
///
/// A pure function on purpose: the result is the key's source LABEL, never
/// its value, so `status` cannot leak a key — and a provider that needs no
/// key reports no source however the settings answer, which the truth table
/// pins directly instead of through a settings fake that lies about the local
/// provider.
fn reported_key_source(requires_api_key: bool, key: Option<&ProviderKey>) -> Option<String> {
    if !requires_api_key {
        return None;
    }
    key.map(|key| key.source().label())
}

/// A provider can be called when the build has it and it has whatever key it
/// needs.
///
/// Extracted for the same reason as `env_holds_a_key`: in CI `local` is off and
/// no cloud key is set, so `feature_enabled` is false for the keyless provider
/// and `key_present` is false for every other one. Both branches of the
/// disjunction are false together, which makes it indistinguishable from a
/// conjunction — the mutant survived on the environment, not on the logic.
/// Decided on values here, it has a truth table.
fn provider_is_ready(feature_enabled: bool, key_present: bool, requires_api_key: bool) -> bool {
    feature_enabled && (key_present || !requires_api_key)
}

/// What a model and provider inherently support, before any question of what
/// this binary was compiled with.
///
/// Split out of `capability` (#3124) so the decision has a truth table that can
/// be tested directly. Inside the `can_*` expressions each branch was ANDed
/// with a feature check, so in a build with that feature off the whole
/// expression was false regardless — every mutation of this logic was
/// equivalent, and the mutation gate rightly could not tell them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ModelAbilities {
    pub(crate) generate: bool,
    pub(crate) tokenize: bool,
    pub(crate) embed: bool,
    pub(crate) rank: bool,
}

impl ModelAbilities {
    /// `task` is the catalogued task for a local model, and `None` for a cloud
    /// spec (where the provider alone decides) or a local spec the catalog
    /// does not know.
    ///
    /// A local model does exactly what its catalogued task says: a reranker
    /// ranks, an embedding model embeds, a generation model generates, and
    /// each of them tokenizes. An uncatalogued local spec claims nothing —
    /// the registry cannot load it, so this binary can do nothing with it.
    pub(crate) fn of(provider: ProviderKind, task: Option<ModelTask>) -> Self {
        let local = provider == ProviderKind::Local;
        Self {
            // Every cloud provider generates; a local model only when that is
            // its task. `!= Some(Embed)` here once claimed generation for a
            // reranker (#3124).
            generate: !local || task == Some(ModelTask::Generate),
            // Tokenization is a property of a local GGUF; cloud providers do
            // not expose it.
            tokenize: local && task.is_some(),
            // OpenAI and Google serve embedding endpoints; Anthropic does not.
            // A local model embeds when that is its catalogued task.
            embed: matches!(provider, ProviderKind::OpenAI | ProviderKind::Google)
                || task == Some(ModelTask::Embed),
            // Reranking is local-only, and only for a rank model.
            rank: local && task == Some(ModelTask::Rank),
        }
    }
}

fn embedding_provider_feature_enabled_for_capability(provider: ProviderKind) -> bool {
    match provider {
        ProviderKind::Local => cfg!(feature = "local"),
        ProviderKind::OpenAI => cfg!(feature = "openai"),
        ProviderKind::Google => cfg!(feature = "google"),
        ProviderKind::Anthropic => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Readiness, over every combination rather than the one CI happens to be in.
    ///
    /// The two rows that matter are the ones the environment cannot produce: a
    /// keyless provider that is built in is ready with no key, and a keyed
    /// provider that is built in is ready once its key arrives.
    #[test]
    fn a_provider_is_ready_when_built_in_and_holding_any_key_it_needs() {
        // Keyless: readiness is the feature alone.
        assert!(
            provider_is_ready(true, false, false),
            "built in, needs no key"
        );
        assert!(provider_is_ready(true, true, false));
        assert!(!provider_is_ready(false, false, false), "not built in");
        assert!(!provider_is_ready(false, true, false));

        // Keyed: the feature and the key together.
        assert!(provider_is_ready(true, true, true), "built in, key present");
        assert!(
            !provider_is_ready(true, false, true),
            "built in, key missing"
        );
        assert!(
            !provider_is_ready(false, true, true),
            "key without the feature"
        );
        assert!(!provider_is_ready(false, false, true));
    }

    /// `api_key` reports exactly what the runtime's settings hold: the key
    /// verbatim when there is one, `missing_api_key` when there is not.
    #[test]
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn the_reported_key_is_the_one_the_settings_hold() {
        let with_keys = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::FakeKeys),
        );
        let without = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::NoKeys),
        );
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
        ] {
            assert_eq!(
                with_keys.api_key(provider).expect("a key is held"),
                crate::testkit::FAKE_KEY,
                "{provider} must report the held key verbatim"
            );
            let error = without.api_key(provider).expect_err("no key is held");
            assert_eq!(error.code(), "inference.missing_api_key", "{provider}");
        }
    }

    /// A run's resolution asks the runtime's settings for the key: with one
    /// held, a cloud generate resolves `ready`; without, `key_missing` naming
    /// the variable and config key that would hold it. This observes the
    /// resolver's key check through the runtime — the resolver's own truth
    /// table injects the answer — and it is the only offline observer of the
    /// key-present side: a run with a key would send a request (#3270), and
    /// the engine's own `missing_api_key` masks the key-absent side one step
    /// later.
    #[test]
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn a_run_resolves_the_key_the_settings_hold() {
        use crate::resolve::Availability;

        let models_dir = tempfile::tempdir().expect("tempdir");
        let config = || InferenceRuntimeConfig {
            models_dir: Some(models_dir.path().to_path_buf()),
            network_enabled: true,
        };
        let with_keys =
            InferenceRuntime::with_settings(config(), Arc::new(crate::testkit::FakeKeys));
        let without = InferenceRuntime::with_settings(config(), Arc::new(crate::testkit::NoKeys));
        let generate = Some(ModelUse::Run(ModelTask::Generate));
        for provider in [
            ProviderKind::OpenAI,
            ProviderKind::Anthropic,
            ProviderKind::Google,
        ] {
            if !generation_provider_feature_enabled(provider) {
                continue;
            }
            let spec = format!("{provider}:m");
            let held = with_keys.resolve(&spec, generate).expect("well-formed");
            assert_eq!(held.availability, Availability::Ready, "{provider}");
            let missing = without.resolve(&spec, generate).expect("well-formed");
            assert_eq!(
                missing.availability,
                Availability::KeyMissing {
                    env_var: api_key_env_var(provider),
                    config_key: format!("{provider}.api_key"),
                },
                "{provider}"
            );
        }
    }

    /// The local provider has no key to report, and says so as a refusal rather
    /// than as an empty string that would read like a key — whatever the
    /// settings would answer for it.
    ///
    /// The code is chosen by `UnsupportedKind::Provider`, not by the wording of
    /// the message: this used to be classified by substring-matching the text
    /// (#3216), so rewording the sentence would have moved the code.
    #[test]
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn the_local_provider_has_no_api_key() {
        let runtime = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::FakeKeys),
        );
        let error = runtime
            .api_key(ProviderKind::Local)
            .expect_err("local uses no key");
        assert_eq!(error.code(), "inference.unsupported_provider");
    }

    #[test]
    fn default_config_allows_network() {
        let config = InferenceRuntimeConfig::default();
        assert!(config.network_enabled);
        assert!(config.models_dir.is_none());
    }

    /// The key source is where the key was found, never its value (D11), and
    /// only for a provider that needs a key.
    #[test]
    fn key_source_reports_the_place_never_the_value() {
        const SECRET: &str = "sk-do-not-leak-this-value";
        let key = ProviderKey::new(
            SECRET,
            crate::KeySource::Environment("OPENAI_API_KEY".into()),
        );

        // Held: the source is the PLACE. If this ever returned the value, a
        // key would ride out on every `inference status`.
        let found = reported_key_source(true, Some(&key));
        assert_eq!(found.as_deref(), Some("OPENAI_API_KEY"));
        assert_ne!(found.as_deref(), Some(SECRET));

        // Not held: no source at all.
        assert_eq!(reported_key_source(true, None), None);

        // A provider that needs no key never reports a source, however the
        // settings answer.
        assert_eq!(reported_key_source(false, Some(&key)), None);
        assert_eq!(reported_key_source(false, None), None);
    }

    /// `status` asks the runtime's settings, and reports every cloud provider
    /// keyed or keyless together with them — the end-to-end form of the two
    /// pure rules above, through the one call site.
    #[test]
    fn status_reports_keys_from_the_runtime_settings() {
        let with_keys = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::FakeKeys),
        )
        .status();
        let without = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::NoKeys),
        )
        .status();
        for (keyed, keyless) in with_keys.providers.iter().zip(&without.providers) {
            assert_eq!(keyed.provider, keyless.provider);
            if keyed.provider == ProviderKind::Local {
                assert!(!keyed.key_present && keyed.key_source.is_none());
                continue;
            }
            assert!(keyed.key_present, "{:?}", keyed.provider);
            assert_eq!(
                keyed.key_source.as_deref(),
                Some("application"),
                "{:?}",
                keyed.provider
            );
            assert!(!keyless.key_present, "{:?}", keyless.provider);
            assert_eq!(keyless.key_source, None);
            assert_eq!(
                keyed.ready, keyed.feature_enabled,
                "a keyed provider is ready exactly when built in: {:?}",
                keyed.provider
            );
            assert!(!keyless.ready, "{:?}", keyless.provider);
        }
    }

    /// A runtime dump names its settings and never a key they hold: the
    /// injected source is opaque to `Debug` (Rule 31), so a runtime logged at
    /// `trace` or in a panic message cannot leak what an application handed
    /// it.
    #[test]
    fn a_runtime_dump_names_its_settings_and_never_a_key() {
        let runtime = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::FakeKeys),
        );
        let dumped = format!("{runtime:?}");
        assert!(
            !dumped.contains(crate::testkit::FAKE_KEY),
            "a held key must not appear in a runtime dump: {dumped}"
        );
        assert!(
            dumped.contains("settings: ProviderSettings"),
            "the settings are named, not omitted: {dumped}"
        );
    }

    /// The truth table for what a model inherently supports (#3124).
    ///
    /// Observable regardless of which provider features are compiled in, which
    /// is the point: folded into the `can_*` expressions this logic was
    /// unreachable in a build with the feature off.
    #[test]
    fn model_abilities_truth_table() {
        use ProviderKind::{Anthropic, Google, Local, OpenAI};

        // Local embedding model: embeds and tokenizes, does not generate.
        let embed = ModelAbilities::of(Local, Some(ModelTask::Embed));
        assert_eq!(
            embed,
            ModelAbilities {
                generate: false,
                tokenize: true,
                embed: true,
                rank: false
            }
        );

        // Local generation model: generates and tokenizes, does not embed.
        let generate = ModelAbilities::of(Local, Some(ModelTask::Generate));
        assert_eq!(
            generate,
            ModelAbilities {
                generate: true,
                tokenize: true,
                embed: false,
                rank: false
            }
        );

        // Local rank model: ranks and tokenizes; neither generates nor embeds.
        let rank = ModelAbilities::of(Local, Some(ModelTask::Rank));
        assert_eq!(
            rank,
            ModelAbilities {
                generate: false,
                tokenize: true,
                embed: false,
                rank: true
            }
        );

        // An uncatalogued local spec has no task, and the registry cannot load
        // it: nothing is claimed.
        let unknown = ModelAbilities::of(Local, None);
        assert_eq!(
            unknown,
            ModelAbilities {
                generate: false,
                tokenize: false,
                embed: false,
                rank: false
            }
        );

        // Cloud providers never tokenize or rank here. OpenAI and Google embed;
        // Anthropic does not.
        for provider in [OpenAI, Google] {
            assert_eq!(
                ModelAbilities::of(provider, None),
                ModelAbilities {
                    generate: true,
                    tokenize: false,
                    embed: true,
                    rank: false
                },
                "{provider:?}"
            );
        }
        assert_eq!(
            ModelAbilities::of(Anthropic, None),
            ModelAbilities {
                generate: true,
                tokenize: false,
                embed: false,
                rank: false
            }
        );
    }

    #[test]
    fn capability_for_local_embed_model_reports_embedding() {
        let runtime = InferenceRuntime::default();
        let capability = runtime.capability("local:miniLM").expect("capability");
        assert_eq!(capability.provider, ProviderKind::Local);
        // #3124: `can_*` reports what THIS BINARY can do, so these follow the
        // feature rather than the model's declared task. The model's own shape
        // stays visible through `embedding_dim`.
        assert_eq!(capability.can_embed, cfg!(feature = "local"));
        assert_eq!(capability.can_tokenize, cfg!(feature = "local"));
        assert_eq!(capability.provider_feature_enabled, cfg!(feature = "local"));
        assert!(!capability.requires_api_key);
        assert_eq!(capability.embedding_dim, 384);
    }

    /// `capability` answers the same locate a refusal would carry, so a
    /// caller can learn what a model needs *before* trying it: a catalogued
    /// model that is not on disk reports `not_downloaded` beside the exact
    /// `pull` spec and the download size (#3226). A cloud model is located,
    /// never reached, so it is `ready` with no pull fields — the key and
    /// network checks belong to a run, and `status` reports the key.
    #[test]
    fn capability_reports_availability_and_what_a_pull_needs() {
        let models_dir = tempfile::tempdir().expect("tempdir");
        let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
            models_dir: Some(models_dir.path().to_path_buf()),
            network_enabled: false,
        });

        let capability = runtime.capability("miniLM").expect("capability");
        assert_eq!(capability.availability, AvailabilityKind::NotDownloaded);
        assert_eq!(capability.pull_spec.as_deref(), Some("miniLM"));
        assert!(
            capability.size_bytes.is_some_and(|bytes| bytes > 0),
            "{:?}",
            capability.size_bytes
        );

        // A non-default quant pulls by its full name.
        let capability = runtime.capability("phi3.5:q8_0").expect("capability");
        assert_eq!(capability.availability, AvailabilityKind::NotDownloaded);
        assert_eq!(capability.pull_spec.as_deref(), Some("phi3.5:q8_0"));

        let capability = runtime
            .capability("openai:text-embedding-3-small")
            .expect("capability");
        assert_eq!(capability.availability, AvailabilityKind::Ready);
        assert_eq!(capability.pull_spec, None);
        assert_eq!(capability.size_bytes, None);

        // Not in the catalog: located, with nothing to pull.
        let capability = runtime.capability("nope").expect("capability");
        assert_eq!(capability.availability, AvailabilityKind::NotInCatalog);
        assert_eq!(capability.pull_spec, None);
        assert_eq!(capability.size_bytes, None);
    }

    #[test]
    fn capability_for_openai_reports_network_and_api_key() {
        let runtime = InferenceRuntime::default();
        let capability = runtime
            .capability("openai:text-embedding-3-small")
            .expect("capability");
        assert_eq!(capability.provider, ProviderKind::OpenAI);
        assert!(capability.can_generate);
        assert!(capability.can_embed);
        assert!(capability.requires_network);
        assert!(capability.requires_api_key);
    }

    /// Call site of the #3222 parser rule: the catalog's own colon-shaped
    /// names reach the registry through `capability` instead of dying in the
    /// parser as "unknown provider". `miniLM:f16` is a `name:quant` form and
    /// `qwen3:1.7b` a `family:size` catalog name. `embedding_dim` is filled
    /// from the registry entry whatever features are compiled, so 384 proves
    /// the name resolved, not merely parsed.
    #[test]
    fn test_capability_resolves_catalog_names_that_contain_colons() {
        let runtime = InferenceRuntime::default();
        let capability = runtime
            .capability("miniLM:f16")
            .expect("a name:quant catalog form is a local model");
        assert_eq!(capability.provider, ProviderKind::Local);
        assert_eq!(capability.model, "miniLM:f16");
        assert_eq!(capability.embedding_dim, 384);

        for spec in ["qwen3:1.7b", "tinyllama:q8_0"] {
            let capability = runtime.capability(spec).expect(spec);
            assert_eq!(capability.provider, ProviderKind::Local, "{spec}");
            assert_eq!(capability.model, spec, "{spec}");
            assert!(!capability.requires_api_key, "{spec}");
            assert!(!capability.requires_network, "{spec}");
            // A catalogued generation model generates when this build can
            // run it; an unresolved name would claim nothing even then.
            assert_eq!(capability.can_generate, cfg!(feature = "local"), "{spec}");
        }
    }

    /// The compute path dispatches a colon-shaped catalog name to the local
    /// engine like a bare name. This build has no local execution, so the
    /// refusal is the local path's `unsupported_operation`; before #3222 the
    /// parser refused first with `invalid_request`. Gated off `local` because
    /// there the call would load a real model from the registry.
    #[test]
    #[cfg(all(
        any(feature = "anthropic", feature = "openai", feature = "google"),
        not(feature = "local")
    ))]
    fn test_generate_dispatches_a_colon_shaped_catalog_name_to_the_local_path() {
        let runtime = InferenceRuntime::default();
        let request = GenerateRequest::default();
        let error = runtime
            .generate("qwen3:1.7b", &request)
            .expect_err("no local execution in this build");
        assert_eq!(error.code(), "inference.unsupported_operation");
        let error = runtime
            .generate("tinyllama:q8_0", &request)
            .expect_err("no local execution in this build");
        assert_eq!(error.code(), "inference.unsupported_operation");
    }

    #[test]
    fn cache_status_is_empty_by_default() {
        let runtime = InferenceRuntime::default();
        assert_eq!(
            runtime.cache_status().expect("cache status"),
            ModelCacheStatus::default()
        );
    }

    /// Poisons a lock the way production does: a panic on another thread
    /// while it holds the guard.
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn poison<T: Send>(lock: &Mutex<T>) {
        let outcome = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _guard = lock.lock().expect("lock is clean before poisoning");
                    panic!("simulated panic while holding a model cache lock");
                })
                .join()
        });
        assert!(outcome.is_err(), "the poisoning thread panicked");
        assert!(lock.is_poisoned());
    }

    /// Caches an engine under `spec` the way a loaded local model is cached.
    ///
    /// A cloud engine stands in because it constructs offline; a local one
    /// needs a model file. Production caches only local engines here —
    /// `generate` builds cloud engines per call — so the stand-in is a test
    /// convenience, not a production state.
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn cache_stand_in_engine(runtime: &InferenceRuntime, spec: &str) {
        #[cfg(feature = "openai")]
        let provider = ProviderKind::OpenAI;
        #[cfg(all(feature = "anthropic", not(feature = "openai")))]
        let provider = ProviderKind::Anthropic;
        #[cfg(all(
            feature = "google",
            not(feature = "openai"),
            not(feature = "anthropic")
        ))]
        let provider = ProviderKind::Google;
        let engine =
            GenerationEngine::cloud(provider, "test-key".to_owned(), "test-model".to_owned())
                .expect("cloud engine constructs offline");
        runtime
            .generation
            .lock()
            .expect("lock is clean")
            .insert(spec.to_owned(), engine);
    }

    /// A panic on one thread while it held a model cache must not fail every
    /// later caller of the runtime (#3249): the runtime kept reporting the
    /// poisoned lock as `inference.io_failure` — "inspect filesystem
    /// permissions" — on every call for the rest of the process. The engines
    /// that thread was using are dropped and the lock is cleaned instead, so
    /// the next caller sees an empty, working cache.
    #[test]
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn test_a_poisoned_model_cache_does_not_fail_later_callers() {
        let runtime = InferenceRuntime::default();
        // A local-only build exercises recovery of an empty cache.
        #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
        cache_stand_in_engine(&runtime, "test-model");
        poison(&runtime.generation);

        let status = runtime
            .cache_status()
            .expect("cache status survives a poisoned generation cache");
        assert!(
            status.generation_models.is_empty(),
            "models cached during the panic are dropped: {status:?}"
        );
        assert!(
            !runtime.generation.is_poisoned(),
            "recovery leaves the lock clean for the next caller"
        );
        let unloaded = runtime
            .unload(None)
            .expect("unload survives a poisoned generation cache");
        assert!(!unloaded, "nothing was cached to unload");
    }

    /// `unload` is the other public entry point onto the cache; it must
    /// recover on its own, not only after `cache_status` already has.
    #[test]
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn test_unload_recovers_a_poisoned_model_cache_first() {
        let runtime = InferenceRuntime::default();
        poison(&runtime.generation);

        let unloaded = runtime
            .unload(None)
            .expect("unload survives a poisoned generation cache");
        assert!(
            !unloaded,
            "the panicking thread's models were already dropped"
        );
        assert!(!runtime.generation.is_poisoned());
    }

    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn populated_cache() -> Mutex<HashMap<String, u32>> {
        Mutex::new(HashMap::from([
            ("tinyllama".to_owned(), 1),
            ("minilm".to_owned(), 2),
        ]))
    }

    /// Recovery drops what the panicking thread was using and cleans the lock,
    /// so the first caller after the panic and every caller after that see the
    /// same empty, unpoisoned cache.
    #[test]
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn test_lock_model_cache_recovers_a_poisoned_cache_by_dropping_its_models() {
        let cache = populated_cache();
        poison(&cache);

        let guard = lock_model_cache(&cache, "test");
        assert!(guard.is_empty(), "models held during the panic are dropped");
        drop(guard);
        assert!(!cache.is_poisoned(), "the poison flag is cleared");

        let guard = lock_model_cache(&cache, "test");
        assert!(
            guard.is_empty(),
            "the cache stays empty for the next caller"
        );
    }

    /// Direction control: a clean lock is passed through untouched. Recovery
    /// must never drop the cached models of a runtime that did not panic.
    #[test]
    #[cfg(any(
        feature = "local",
        feature = "anthropic",
        feature = "openai",
        feature = "google"
    ))]
    fn test_lock_model_cache_leaves_a_clean_cache_alone() {
        let cache = populated_cache();

        let guard = lock_model_cache(&cache, "test");
        assert_eq!(sorted_keys(&guard), ["minilm", "tinyllama"]);
        assert_eq!(guard.get("tinyllama"), Some(&1));
        drop(guard);
        assert!(!cache.is_poisoned());
    }

    /// `unload` says whether it dropped anything, so a caller can tell
    /// "unloaded" from "nothing was loaded". Nothing else observes the fold
    /// across the caches: with it broken, every unload reports nothing dropped.
    #[test]
    #[cfg(any(feature = "anthropic", feature = "openai", feature = "google"))]
    fn unload_reports_whether_it_dropped_a_cached_model() {
        let runtime = InferenceRuntime::default();
        cache_stand_in_engine(&runtime, "test-model");

        assert!(
            !runtime.unload(Some("other-model")).expect("unload"),
            "a spec that is not cached drops nothing"
        );
        assert!(
            runtime.unload(Some("test-model")).expect("unload"),
            "the cached model is dropped"
        );
        let status = runtime.cache_status().expect("cache status");
        assert!(status.generation_models.is_empty(), "{status:?}");
        assert!(
            !runtime.unload(None).expect("unload"),
            "nothing is left to drop"
        );
    }

    /// `generate` and `chat` fork on the spec's provider: a local spec goes
    /// to the model cache, anything else to a cloud engine. A flipped fork
    /// still fails both calls in a keyless build, so each side is pinned by
    /// the code its own path produces, not by failure alone.
    ///
    /// A cloud spec reaches the provider path, which asks for a key before
    /// anything else; sent down the local path instead it is refused as not
    /// a local model (`inference.unsupported_operation`).
    #[test]
    #[cfg(feature = "openai")]
    fn a_cloud_spec_is_served_by_its_provider_not_the_model_cache() {
        let runtime = InferenceRuntime::with_settings(
            InferenceRuntimeConfig::default(),
            Arc::new(crate::testkit::NoKeys),
        );
        let request = GenerateRequest::default();
        let chat = crate::wire::ChatRequest {
            prompt: Some("hi".to_owned()),
            ..Default::default()
        };

        let error = runtime
            .generate("openai:gpt-test", &request)
            .expect_err("no key is held");
        assert_eq!(error.code(), "inference.missing_api_key");
        let error = runtime
            .chat("openai:gpt-test", &chat)
            .expect_err("no key is held");
        assert_eq!(error.code(), "inference.missing_api_key");
    }

    /// The other side of the fork: a local spec reaches the local loader,
    /// which this build does not have (`inference.unsupported_operation`);
    /// sent down the provider path instead it is refused as a provider this
    /// build lacks (`inference.unsupported_provider`).
    #[test]
    #[cfg(all(
        any(feature = "anthropic", feature = "openai", feature = "google"),
        not(feature = "local")
    ))]
    fn a_local_spec_is_served_by_the_model_cache_not_a_provider() {
        let runtime = InferenceRuntime::default();
        let request = GenerateRequest::default();
        let chat = crate::wire::ChatRequest {
            prompt: Some("hi".to_owned()),
            ..Default::default()
        };

        let error = runtime
            .generate("tinyllama", &request)
            .expect_err("no local execution in this build");
        assert_eq!(error.code(), "inference.unsupported_operation");
        let error = runtime
            .chat("tinyllama", &chat)
            .expect_err("no local execution in this build");
        assert_eq!(error.code(), "inference.unsupported_operation");
    }
}
