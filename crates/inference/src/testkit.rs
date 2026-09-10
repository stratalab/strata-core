//! Deterministic fake inference engine — the harness the test plan calls
//! for: command contracts, error branches, redaction, and partial-failure
//! handling become cheap and repeatable, with zero network, zero model
//! files, and zero sleeps.
//!
//! This is NOT behavioral proof of provider quality, tokenization, local
//! runtime lifecycle, embedding semantics, or model output. Any claim that
//! real inference works must come from the gated GGUF/provider integration
//! lanes.

#![doc(hidden)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::registry::ModelRegistry;
use crate::resolve::{
    pull_spec, resolve, Availability, ModelSource, ModelUse, PullAction, ResolvedModel,
};
use crate::runtime::ModelAbilities;
use crate::{
    GenerateRequest, GenerateResponse, InferenceEngine, InferenceError, ModelTask, ProviderBaseUrl,
    ProviderKey, ProviderKind, ProviderSettings, RegistryFailure, SettingSource, StopReason,
};

/// The key [`FakeKeys`] holds for every cloud provider. Not a real key: a
/// provider call made with it is refused by the provider, and the resolution
/// lanes never make one.
pub const FAKE_KEY: &str = "sk-fake-key-for-tests";

/// Provider settings holding no key for any provider — a scrubbed
/// environment, without touching the environment.
///
/// Injected through [`crate::InferenceRuntime::with_settings`] so a test can
/// assert the missing-key path (`Availability::KeyMissing`,
/// `inference.missing_api_key`) deterministically, whatever the developer
/// running it has exported.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoKeys;

impl ProviderSettings for NoKeys {
    fn key(&self, _provider: ProviderKind) -> Option<ProviderKey> {
        None
    }
}

/// Provider settings holding [`FAKE_KEY`] for every cloud provider, sourced
/// from the application. The local provider has no key, as ever.
#[derive(Clone, Copy, Debug, Default)]
pub struct FakeKeys;

impl ProviderSettings for FakeKeys {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
        (provider != ProviderKind::Local)
            .then(|| ProviderKey::new(FAKE_KEY, SettingSource::Application))
    }
}

/// [`FakeKeys`] with every cloud provider's requests pointed at one base URL,
/// sourced from the application: a test double's address, or a closed
/// loopback port when the test wants the call to fail before it leaves the
/// machine (`inference.provider_unavailable`) — so a keyed cloud run can be
/// driven end to end without a network and without a real key ever being
/// sent anywhere (#3270).
#[derive(Clone, Debug)]
pub struct FakeKeysAt {
    base_url: String,
}

impl FakeKeysAt {
    /// Fake keys, and every cloud provider reached at `base_url`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl ProviderSettings for FakeKeysAt {
    fn key(&self, provider: ProviderKind) -> Option<ProviderKey> {
        FakeKeys.key(provider)
    }

    fn base_url(&self, provider: ProviderKind) -> Option<ProviderBaseUrl> {
        (provider != ProviderKind::Local)
            .then(|| ProviderBaseUrl::new(&self.base_url, SettingSource::Application))
    }
}

/// A scripted failure the fake engine raises on every call, phrased in the
/// same `InferenceError` vocabulary the real providers use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScriptedFailure {
    MissingModel,
    InvalidRequest,
    UnsupportedParameter,
    UnsupportedOperation,
    MissingApiKey,
    AuthFailure,
    RateLimit,
    Timeout,
    ProviderUnavailable,
    MalformedResponse,
}

impl ScriptedFailure {
    fn to_error(self) -> InferenceError {
        match self {
            // The kind carries the code; no word in the message does (#3216).
            Self::MissingModel => InferenceError::RegistryFailed {
                kind: RegistryFailure::MissingModel,
                message: "fake: model fake:missing is not downloaded".to_string(),
                details: None,
            },
            Self::InvalidRequest => {
                InferenceError::InvalidSpec("fake: request is invalid".to_string())
            }
            Self::UnsupportedParameter => InferenceError::NotSupported(
                "fake: parameter is not supported by this engine".to_string(),
            ),
            Self::UnsupportedOperation => InferenceError::NotSupported(
                "fake: operation is not supported by this engine".to_string(),
            ),
            Self::MissingApiKey => InferenceError::Provider(
                "fake: FAKE_API_KEY is not set for this provider".to_string(),
            ),
            Self::AuthFailure => InferenceError::Provider(
                "fake: invalid API key sk-fake-1234567890 (401)".to_string(),
            ),
            Self::RateLimit => {
                InferenceError::Provider("fake: rate limited (too many requests)".to_string())
            }
            Self::Timeout => InferenceError::Provider("fake: request timed out".to_string()),
            Self::ProviderUnavailable => {
                InferenceError::Provider("fake: provider unavailable".to_string())
            }
            Self::MalformedResponse => {
                InferenceError::Provider("fake: malformed provider response".to_string())
            }
        }
    }
}

/// Deterministic fake engine: same inputs, same outputs, forever.
#[derive(Debug)]
pub struct FakeInferenceEngine {
    embedding_dim: usize,
    latency: Duration,
    failure: Option<ScriptedFailure>,
    failing_embed_items: BTreeSet<usize>,
    failing_rank_items: BTreeSet<usize>,
    healthy: bool,
    calls: usize,
}

impl Default for FakeInferenceEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeInferenceEngine {
    #[must_use]
    pub fn new() -> Self {
        Self {
            embedding_dim: 8,
            latency: Duration::from_millis(0),
            failure: None,
            failing_embed_items: BTreeSet::new(),
            failing_rank_items: BTreeSet::new(),
            healthy: true,
            calls: 0,
        }
    }

    /// Configure the latency the engine *reports*. Nothing sleeps: tests
    /// assert the configured value through [`Self::latency`].
    #[must_use]
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    #[must_use]
    pub fn with_embedding_dim(mut self, dim: usize) -> Self {
        self.embedding_dim = dim;
        self
    }

    /// Script every subsequent call to fail with the given class.
    #[must_use]
    pub fn with_failure(mut self, failure: ScriptedFailure) -> Self {
        self.failure = Some(failure);
        self
    }

    /// Script specific batch-embedding items to fail (partial failure).
    #[must_use]
    pub fn with_failing_embed_items(mut self, items: impl IntoIterator<Item = usize>) -> Self {
        self.failing_embed_items = items.into_iter().collect();
        self
    }

    /// Script specific ranking passages to fail (partial failure).
    #[must_use]
    pub fn with_failing_rank_items(mut self, items: impl IntoIterator<Item = usize>) -> Self {
        self.failing_rank_items = items.into_iter().collect();
        self
    }

    #[must_use]
    pub fn with_health(mut self, healthy: bool) -> Self {
        self.healthy = healthy;
        self
    }

    /// The configured (reported, never slept) latency.
    #[must_use]
    pub const fn latency(&self) -> Duration {
        self.latency
    }

    /// Calls observed across all operations, for interaction assertions.
    #[must_use]
    pub const fn calls(&self) -> usize {
        self.calls
    }

    fn scripted(&self) -> Result<(), InferenceError> {
        match self.failure {
            Some(failure) => Err(failure.to_error()),
            None => Ok(()),
        }
    }

    fn deterministic_vector(&self, text: &str) -> Vec<f32> {
        // A tiny splitmix-style fold: pure function of (text, position),
        // spread over [-1, 1], identical across runs and platforms.
        let seed = text.bytes().fold(0xdead_beef_u64, |acc, byte| {
            acc.wrapping_mul(31).wrapping_add(u64::from(byte))
        });
        (0..self.embedding_dim)
            .map(|position| {
                let mut z =
                    seed.wrapping_add(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(position as u64 + 1));
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^= z >> 31;
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "deterministic pseudo-embedding; exact float value is the contract"
                )]
                let unit = (z % 2_000_003) as f32 / 1_000_001.5 - 1.0;
                unit
            })
            .collect()
    }
}

impl InferenceEngine for FakeInferenceEngine {
    fn generate(&mut self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        self.calls += 1;
        self.scripted()?;
        let prompt_tokens = request.prompt.split_whitespace().count();
        // Deterministic text: a pure function of prompt, seed, and grammar.
        let seed = request.seed.unwrap_or(0);
        let mut text = format!("fake(seed={seed}):{}", request.prompt);
        if request.grammar.is_some() {
            text = format!("{{\"fake\":\"{seed}\"}}");
        }
        for stop in &request.stop_sequences {
            if let Some(index) = text.find(stop.as_str()) {
                text.truncate(index);
            }
        }
        let natural_tokens = text.split_whitespace().count().max(1);
        let completion_tokens = natural_tokens.min(request.max_tokens);
        let stop_reason = if completion_tokens == request.max_tokens {
            StopReason::MaxTokens
        } else {
            StopReason::StopToken
        };
        Ok(GenerateResponse {
            text,
            stop_reason,
            prompt_tokens,
            completion_tokens,
        })
    }

    fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError> {
        self.scripted()?;
        Ok(self.deterministic_vector(text))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        self.scripted()?;
        if let Some(failed) = texts
            .iter()
            .enumerate()
            .find_map(|(index, _)| self.failing_embed_items.contains(&index).then_some(index))
        {
            return Err(InferenceError::Provider(format!(
                "fake: embedding item {failed} of {} failed",
                texts.len()
            )));
        }
        Ok(texts
            .iter()
            .map(|text| self.deterministic_vector(text))
            .collect())
    }

    fn rank(&self, query: &str, passages: &[&str]) -> Result<Vec<f32>, InferenceError> {
        self.scripted()?;
        if let Some(failed) = passages
            .iter()
            .enumerate()
            .find_map(|(index, _)| self.failing_rank_items.contains(&index).then_some(index))
        {
            return Err(InferenceError::Provider(format!(
                "fake: ranking item {failed} of {} failed",
                passages.len()
            )));
        }
        // Deterministic score: shared-whitespace-token overlap, scaled by
        // passage position so ties break stably.
        let query_tokens: BTreeSet<&str> = query.split_whitespace().collect();
        Ok(passages
            .iter()
            .enumerate()
            .map(|(position, passage)| {
                let overlap = passage
                    .split_whitespace()
                    .filter(|token| query_tokens.contains(token))
                    .count();
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "deterministic fake score; small values"
                )]
                let score = overlap as f32 - position as f32 / 1_000.0;
                score
            })
            .collect())
    }

    fn supports_generate(&self) -> bool {
        true
    }

    fn supports_embed(&self) -> bool {
        true
    }

    fn supports_rank(&self) -> bool {
        true
    }

    fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }

    fn is_healthy(&self) -> bool {
        self.healthy
    }
}

/// The directory the fake's registry sits over. It does not exist, and the
/// fake never reads it: presence is the fake world's to decide.
const FAKE_MODELS_DIR: &str = "/fake/models";

/// The fake's own models, beside the real catalog. Each does every task, so
/// a test can name a model that is unquestionably the fake's without
/// depending on what the real catalog holds.
const FAKE_MODELS: [(&str, ModelTask); 3] = [
    ("fake-embed", ModelTask::Embed),
    ("fake-generate", ModelTask::Generate),
    ("fake-rank", ModelTask::Rank),
];

/// Deterministic runtime-level fake for the executor's
/// [`crate::InferenceService`] surface: model management plus the wire-typed
/// compute paths, with zero network, model files, or sleeps. Same inputs, same
/// outputs, forever — so fixture replays are stable.
///
/// It fakes *execution only*. Resolution is the real resolver over the real
/// catalog (`crate::resolve`), so a fixture replayed here meets the refusals
/// a caller meets — a name the catalog does not know, a malformed spec, a
/// model asked for a task it does not do — with the same codes and details.
/// What the environment decides is fixed in the fake's favour
/// ([`fake_availability`]): every catalogued model is downloaded — unless
/// [`Self::with_undownloaded`] says otherwise — every provider is built in
/// and keyed, the network is on. Beside the real catalog it has
/// [`FAKE_MODELS`], three models of its own that do everything.
#[derive(Debug)]
pub struct FakeInferenceService {
    embedding_dim: usize,
    /// The real catalog over a directory that does not exist: identity is
    /// real, presence is the fake's to decide.
    registry: ModelRegistry,
    /// The pull specs of the catalogued variants this world has not
    /// downloaded. A pull takes one out, so the refuse → pull → retry loop
    /// a consumer runs (the CLI's download offer) plays out in-process
    /// against the resolver's real answer.
    undownloaded: Mutex<BTreeSet<String>>,
}

impl Default for FakeInferenceService {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeInferenceService {
    #[must_use]
    pub fn new() -> Self {
        Self {
            embedding_dim: 8,
            registry: ModelRegistry::with_dir(PathBuf::from(FAKE_MODELS_DIR)),
            undownloaded: Mutex::new(BTreeSet::new()),
        }
    }

    #[must_use]
    pub fn with_embedding_dim(mut self, dim: usize) -> Self {
        self.embedding_dim = dim;
        self
    }

    /// A world where the catalogued variant `pull_spec` names (`miniLM`,
    /// `tinyllama:q8_0`) is not on disk until it is pulled: `capability`
    /// reports it `not_downloaded` with the pull spec and size, a run refuses
    /// it as `missing_model` carrying the same answer, a pull makes it
    /// present, and a run after that succeeds.
    #[must_use]
    pub fn with_undownloaded(self, pull_spec: impl Into<String>) -> Self {
        self.lock_undownloaded().insert(pull_spec.into());
        self
    }

    fn lock_undownloaded(&self) -> MutexGuard<'_, BTreeSet<String>> {
        // A poisoned lock still holds a whole set — nothing here writes it
        // in more than one step — so the fake world goes on.
        self.undownloaded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Resolves `spec` the way the runtime does, then applies the fake world.
    ///
    /// Errs for what the resolver errs for (a malformed spec); every other
    /// refusal is an availability the caller turns into an error with
    /// [`ResolvedModel::require_ready`], details and all.
    fn locate(&self, spec: &str, use_: Option<ModelUse>) -> Result<ResolvedModel, InferenceError> {
        // Network on and every key present are the two environment facts the
        // resolver takes as inputs; the ones it reads from the build are
        // fixed by `fake_availability` afterwards.
        let mut resolved = resolve(&self.registry, true, &|_| true, spec, use_)?;
        if is_fakes_own(&resolved) {
            resolved.source = ModelSource::GgufPath(
                self.registry
                    .models_dir()
                    .join(format!("{}.gguf", resolved.name)),
            );
            resolved.availability = Availability::Ready;
        } else if let Some(absent) = self.not_downloaded(&resolved) {
            resolved.availability = absent;
        } else {
            resolved.availability = fake_availability(resolved.availability);
        }
        Ok(resolved)
    }

    /// The runtime's answer for a catalogued variant that is not on disk,
    /// when this world holds it undownloaded: the same pull spec and size
    /// the resolver reports. `None` for anything else, and for a variant the
    /// resolver already refused on identity or task — those checks come
    /// before presence in the runtime, so they do here.
    fn not_downloaded(&self, resolved: &ResolvedModel) -> Option<Availability> {
        let ModelSource::Catalog { entry, variant, .. } = &resolved.source else {
            return None;
        };
        if matches!(resolved.availability, Availability::TaskNotSupported { .. }) {
            return None;
        }
        let pull_spec = pull_spec(entry, variant);
        let absent = self.lock_undownloaded().contains(&pull_spec);
        absent.then_some(Availability::NotDownloaded {
            pull_spec,
            size_bytes: variant.size_bytes,
        })
    }

    /// A pure, platform-stable pseudo-embedding (mirrors the engine fake).
    fn embed_vector(&self, text: &str) -> Vec<f32> {
        let seed = text.bytes().fold(0xdead_beef_u64, |acc, byte| {
            acc.wrapping_mul(31).wrapping_add(u64::from(byte))
        });
        (0..self.embedding_dim)
            .map(|position| {
                let mut z =
                    seed.wrapping_add(0x9e37_79b9_7f4a_7c15_u64.wrapping_mul(position as u64 + 1));
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^= z >> 31;
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "deterministic pseudo-embedding; exact float value is the contract"
                )]
                let unit = (z % 2_000_003) as f32 / 1_000_001.5 - 1.0;
                unit
            })
            .collect()
    }
}

/// The fake world's answer, from the real resolver's.
///
/// Identity stays real: the catalog, whether a path names a file, what a
/// catalogued model does. What the build decides is fixed instead of read —
/// local execution and every provider built in, every catalogued model
/// downloaded (a world that holds one back says so first, in
/// [`FakeInferenceService::not_downloaded`]). Fixed rather than left to
/// `cfg!` so a fixture replays the same under every feature set: in a build
/// without `local` the resolver answers `LocalExecutionNotBuilt` before it
/// looks for the file, where a build with it answers `NotDownloaded` — two
/// codes for one fixture.
fn fake_availability(real: Availability) -> Availability {
    match real {
        Availability::Ready
        | Availability::NotInCatalog
        | Availability::PathMissing
        | Availability::TaskNotSupported { .. } => real,
        Availability::LocalExecutionNotBuilt
        | Availability::ProviderNotBuilt
        | Availability::NotDownloaded { .. } => Availability::Ready,
        // Never resolved: `locate` passes the network on and every key
        // present. Listed so the match stays total when a variant is added.
        Availability::NetworkDisabled | Availability::KeyMissing { .. } => Availability::Ready,
    }
}

/// Whether a resolved local name is one of [`FAKE_MODELS`].
fn is_fakes_own(resolved: &ResolvedModel) -> bool {
    resolved.provider == ProviderKind::Local
        && FAKE_MODELS.iter().any(|(name, _)| *name == resolved.name)
}

/// What a resolved model does in the fake world: the real ability table for
/// a catalogued or cloud model, everything for one of the fake's own.
fn fake_abilities(resolved: &ResolvedModel) -> ModelAbilities {
    if is_fakes_own(resolved) {
        return ModelAbilities {
            generate: true,
            tokenize: true,
            embed: true,
            rank: true,
        };
    }
    let task = match resolved.source {
        ModelSource::Catalog { entry, .. } => Some(entry.task),
        ModelSource::GgufPath(_) | ModelSource::Uncatalogued { .. } | ModelSource::Cloud => None,
    };
    ModelAbilities::of(resolved.provider, task)
}

fn fake_model(name: &str, task: crate::ModelTask, embedding_dim: usize) -> crate::ModelInfo {
    crate::ModelInfo {
        name: name.to_owned(),
        task,
        architecture: "fake".to_owned(),
        default_quant: "q8_0".to_owned(),
        embedding_dim,
        is_local: false,
        // The fake provider executes in-process, so its models really are
        // runnable regardless of which real provider features are compiled in.
        runnable: true,
        local_path: None,
        size_bytes: 0,
        hf_repo: "fake/fake".to_owned(),
    }
}

/// The prompt text a chat request carries (prompt field, else the last message).
fn chat_prompt(request: &crate::ChatRequest) -> String {
    if let Some(prompt) = &request.prompt {
        return prompt.clone();
    }
    request
        .messages
        .as_ref()
        .and_then(|messages| messages.last())
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

impl crate::InferenceService for FakeInferenceService {
    fn list_models(&self) -> Vec<crate::ModelInfo> {
        FAKE_MODELS
            .iter()
            .map(|&(name, task)| {
                let embedding_dim = if task == ModelTask::Embed {
                    self.embedding_dim
                } else {
                    0
                };
                fake_model(name, task, embedding_dim)
            })
            .collect()
    }

    fn list_local_models(&self) -> Vec<crate::ModelInfo> {
        Vec::new()
    }

    /// The runtime's own pull decision over the fake world: a present model
    /// reports its file, an undownloaded one ([`Self::with_undownloaded`])
    /// becomes present, and a cloud or unknown spec is refused as the
    /// runtime refuses it.
    fn pull_model(&self, model: &str) -> Result<crate::PullModelOutput, InferenceError> {
        let resolved = self.locate(model, None)?;
        let path = match resolved.pull_action(true)? {
            PullAction::Present(path) => path.to_path_buf(),
            PullAction::Download { entry, variant } => {
                self.lock_undownloaded().remove(&pull_spec(entry, variant));
                resolved
                    .local_path()
                    .expect("a catalogued variant has a path in the models directory")
                    .to_path_buf()
            }
        };
        Ok(crate::PullModelOutput {
            model: resolved.spec,
            path,
        })
    }

    fn capability(&self, model_spec: &str) -> Result<crate::InferenceCapability, InferenceError> {
        // Located, never loaded, like the runtime: an unknown name is
        // reported, not refused.
        let resolved = self.locate(model_spec, None)?;
        let provider = resolved.provider;
        let details = resolved.details();
        let abilities = fake_abilities(&resolved);
        Ok(crate::InferenceCapability {
            provider,
            model: resolved.name,
            availability: details.availability,
            pull_spec: details.pull_spec,
            size_bytes: details.size_bytes,
            // Every feature is built into the fake world, so what the model
            // does is what this fake can do with it.
            can_generate: abilities.generate,
            can_tokenize: abilities.tokenize,
            can_embed: abilities.embed,
            can_rank: abilities.rank,
            requires_network: provider != ProviderKind::Local,
            requires_api_key: provider != ProviderKind::Local,
            provider_feature_enabled: true,
            network_enabled: true,
            embedding_dim: if abilities.embed {
                self.embedding_dim
            } else {
                0
            },
            supports_tools: false,
            supports_json_object: true,
            supports_json_schema: false,
            supports_logprobs: false,
        })
    }

    fn chat(
        &self,
        model_spec: &str,
        request: &crate::ChatRequest,
    ) -> Result<crate::ChatResponse, InferenceError> {
        let resolved = self.locate(model_spec, Some(ModelUse::Run(ModelTask::Generate)))?;
        resolved.require_ready()?;
        let prompt = chat_prompt(request);
        let content = format!("fake:{prompt}");
        let prompt_tokens = u32::try_from(prompt.split_whitespace().count()).unwrap_or(u32::MAX);
        let completion_tokens =
            u32::try_from(content.split_whitespace().count()).unwrap_or(u32::MAX);
        Ok(crate::ChatResponse {
            model: resolved.spec,
            choices: vec![crate::ChatChoice {
                index: 0,
                message: crate::ChatMessage {
                    role: crate::Role::Assistant,
                    content,
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                },
                finish_reason: crate::FinishReason::Stop,
                logprobs: None,
            }],
            usage: crate::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens.saturating_add(completion_tokens),
            },
        })
    }

    fn tokenize(
        &self,
        model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError> {
        self.locate(model_spec, Some(ModelUse::Tokenize))?
            .require_ready()?;
        let mut ids: Vec<u32> = text.bytes().map(u32::from).collect();
        if add_special {
            ids.insert(0, 1);
            ids.push(2);
        }
        Ok(ids)
    }

    fn detokenize(&self, model_spec: &str, ids: &[u32]) -> Result<String, InferenceError> {
        self.locate(model_spec, Some(ModelUse::Tokenize))?
            .require_ready()?;
        Ok(ids
            .iter()
            .filter_map(|&id| u8::try_from(id).ok())
            .map(char::from)
            .collect())
    }

    fn embeddings(
        &self,
        model_spec: &str,
        request: &crate::EmbeddingsRequest,
    ) -> Result<crate::EmbeddingsResponse, InferenceError> {
        let resolved = self.locate(model_spec, Some(ModelUse::Run(ModelTask::Embed)))?;
        resolved.require_ready()?;
        let texts = request.input.to_vec();
        let prompt_tokens = texts
            .iter()
            .map(|text| u32::try_from(text.split_whitespace().count()).unwrap_or(u32::MAX))
            .fold(0u32, u32::saturating_add);
        let data = texts
            .iter()
            .enumerate()
            .map(|(index, text)| crate::EmbeddingItem {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                embedding: self.embed_vector(text),
            })
            .collect();
        Ok(crate::EmbeddingsResponse {
            model: resolved.spec,
            data,
            dimension: self.embedding_dim,
            usage: crate::Usage {
                prompt_tokens,
                completion_tokens: 0,
                total_tokens: prompt_tokens,
            },
        })
    }

    fn rank(
        &self,
        model_spec: &str,
        request: &crate::RankRequest,
    ) -> Result<crate::RankResponse, InferenceError> {
        self.locate(model_spec, Some(ModelUse::Run(ModelTask::Rank)))?
            .require_ready()?;
        let query: BTreeSet<&str> = request.query.split_whitespace().collect();
        let items = request
            .passages
            .iter()
            .enumerate()
            .map(|(index, passage)| {
                let overlap = passage
                    .split_whitespace()
                    .filter(|word| query.contains(word))
                    .count();
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "deterministic pseudo-score; exact value is the contract"
                )]
                let score = overlap as f32;
                crate::RankRuntimeOutcome::Ok { index, score }
            })
            .collect();
        Ok(crate::RankResponse { items })
    }

    fn unload(&self, _model_spec: Option<&str>) -> Result<bool, InferenceError> {
        Ok(false)
    }

    fn cache_status(&self) -> Result<crate::ModelCacheStatus, InferenceError> {
        Ok(crate::ModelCacheStatus {
            generation_models: Vec::new(),
            embedding_models: Vec::new(),
            ranking_models: Vec::new(),
        })
    }

    /// The fake executes in-process and needs no key or network, so every
    /// provider it reports is ready and every catalogued model is runnable.
    /// Keeping this deterministic is what lets fixtures replay it.
    fn status(&self) -> crate::InferenceStatus {
        crate::InferenceStatus {
            local_execution: true,
            model_download: false,
            providers: vec![crate::ProviderStatus {
                provider: crate::ProviderKind::Local,
                feature_enabled: true,
                requires_api_key: false,
                key_present: false,
                key_env_var: None,
                key_source: None,
                base_url: None,
                base_url_env_var: None,
                base_url_source: None,
                ready: true,
                model_prefix: "local:".to_owned(),
            }],
            models_dir: self.registry.models_dir().to_path_buf(),
            // The same two listings the real runtime counts.
            models_downloaded: self.list_local_models().len(),
            models_catalogued: self.list_models().len(),
            local_remedy: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- deterministic outputs (cases 1-5) ---

    #[test]
    fn generation_text_and_token_counts_are_deterministic() {
        let mut first = FakeInferenceEngine::new();
        let mut second = FakeInferenceEngine::new();
        let request = GenerateRequest {
            prompt: "the quick brown fox".to_string(),
            seed: Some(7),
            max_tokens: 64,
            ..GenerateRequest::default()
        };
        let a = first.generate(&request).expect("generate");
        let b = second.generate(&request).expect("generate");
        assert_eq!(a, b, "same request must yield identical responses");
        assert_eq!(a.prompt_tokens, 4);
        assert!(a.text.contains("seed=7") && a.text.contains("the quick brown fox"));
        assert_eq!(a.stop_reason, StopReason::StopToken);

        // Token budget engages deterministically.
        let clipped = first
            .generate(&GenerateRequest {
                prompt: "one two three four five six".to_string(),
                max_tokens: 2,
                ..GenerateRequest::default()
            })
            .expect("generate");
        assert_eq!(clipped.completion_tokens, 2);
        assert_eq!(clipped.stop_reason, StopReason::MaxTokens);
    }

    #[test]
    fn embedding_vectors_are_deterministic_and_input_sensitive() {
        let engine = FakeInferenceEngine::new().with_embedding_dim(16);
        let a = engine.embed("alpha").expect("embed");
        let b = engine.embed("alpha").expect("embed");
        let c = engine.embed("beta").expect("embed");
        assert_eq!(a.len(), 16);
        assert_eq!(a, b, "same text, same vector");
        assert_ne!(a, c, "different text, different vector");
        assert_eq!(
            engine.embed_batch(&["alpha", "beta"]).expect("batch"),
            vec![a, c],
            "batch agrees with single-item embedding"
        );
    }

    #[test]
    fn ranking_scores_are_deterministic_and_relevance_ordered() {
        let engine = FakeInferenceEngine::new();
        let scores = engine
            .rank(
                "storage engine durability",
                &[
                    "a storage engine with durability guarantees",
                    "cooking with induction stoves",
                ],
            )
            .expect("rank");
        assert_eq!(scores.len(), 2);
        assert!(
            scores[0] > scores[1],
            "the on-topic passage must outrank the off-topic one: {scores:?}"
        );
        let again = engine
            .rank(
                "storage engine durability",
                &[
                    "a storage engine with durability guarantees",
                    "cooking with induction stoves",
                ],
            )
            .expect("rank");
        assert_eq!(scores, again);
    }

    #[test]
    fn stop_sequences_truncate_deterministically() {
        let mut engine = FakeInferenceEngine::new();
        let response = engine
            .generate(&GenerateRequest {
                prompt: "hello STOP world".to_string(),
                stop_sequences: vec!["STOP".to_string()],
                ..GenerateRequest::default()
            })
            .expect("generate");
        assert!(
            !response.text.contains("STOP") && !response.text.contains("world"),
            "stop sequence must truncate: {:?}",
            response.text
        );
    }

    #[test]
    fn configured_latency_is_reported_without_sleeping() {
        let engine =
            FakeInferenceEngine::new().with_latency(std::time::Duration::from_millis(1_500));
        let started = std::time::Instant::now();
        let _ = engine.embed("no sleeping").expect("embed");
        assert!(
            started.elapsed() < std::time::Duration::from_millis(200),
            "the fake must never sleep its configured latency"
        );
        assert_eq!(engine.latency(), std::time::Duration::from_millis(1_500));
    }

    // --- scripted failures (cases 6-15) ---

    fn failing(failure: ScriptedFailure) -> FakeInferenceEngine {
        FakeInferenceEngine::new().with_failure(failure)
    }

    #[test]
    fn scripted_failures_classify_to_the_stable_error_codes() {
        let cases: [(ScriptedFailure, &str); 10] = [
            (ScriptedFailure::MissingModel, "inference.missing_model"),
            (ScriptedFailure::InvalidRequest, "inference.invalid_request"),
            (
                ScriptedFailure::UnsupportedParameter,
                "inference.unsupported_parameter",
            ),
            (
                ScriptedFailure::UnsupportedOperation,
                "inference.unsupported_operation",
            ),
            (ScriptedFailure::MissingApiKey, "inference.missing_api_key"),
            (
                ScriptedFailure::AuthFailure,
                "inference.provider_auth_failed",
            ),
            (
                ScriptedFailure::RateLimit,
                "inference.provider_rate_limited",
            ),
            (ScriptedFailure::Timeout, "inference.provider_timeout"),
            (
                ScriptedFailure::ProviderUnavailable,
                "inference.provider_unavailable",
            ),
            (
                ScriptedFailure::MalformedResponse,
                "inference.provider_malformed_response",
            ),
        ];
        for (failure, code) in cases {
            let mut engine = failing(failure);
            let error = engine
                .generate(&GenerateRequest::default())
                .expect_err("scripted failure must fail generation");
            assert_eq!(error.code(), code, "{failure:?}: {error:?}");
            // The same script gates every capability, not just generation.
            failing(failure)
                .embed("x")
                .expect_err("scripted failure must fail embedding");
            failing(failure)
                .rank("q", &["p"])
                .expect_err("scripted failure must fail ranking");
        }
    }

    // --- partial failures (cases 16-17) ---

    #[test]
    fn partial_embedding_failure_names_the_failed_item() {
        let engine = FakeInferenceEngine::new().with_failing_embed_items([1]);
        assert!(engine.embed("solo").is_ok(), "single-item path unaffected");
        let error = engine
            .embed_batch(&["ok", "bad", "ok"])
            .expect_err("scripted item failure");
        assert!(
            error.to_string().contains("item 1 of 3"),
            "the failed item must be identifiable: {error}"
        );
    }

    #[test]
    fn partial_ranking_failure_names_the_failed_item() {
        let engine = FakeInferenceEngine::new().with_failing_rank_items([0]);
        let error = engine
            .rank("q", &["bad", "ok"])
            .expect_err("scripted item failure");
        assert!(
            error.to_string().contains("item 0 of 2"),
            "the failed item must be identifiable: {error}"
        );
    }

    // --- redaction (case 18) ---

    #[test]
    fn scripted_auth_failure_redacts_the_secret() {
        let mut engine = failing(ScriptedFailure::AuthFailure);
        let error = engine
            .generate(&GenerateRequest::default())
            .expect_err("auth failure");
        let display = error.to_string();
        let debug = format!("{error:?}");
        for rendered in [&display, &debug] {
            assert!(
                !rendered.contains("sk-fake-1234567890"),
                "the raw secret must never render: {rendered}"
            );
        }
    }

    // --- capability and health surface ---

    #[test]
    fn capability_flags_and_health_are_reported() {
        let engine = FakeInferenceEngine::new().with_health(false);
        assert!(engine.supports_generate() && engine.supports_embed() && engine.supports_rank());
        assert_eq!(engine.embedding_dim(), 8);
        assert!(!engine.is_healthy());
    }

    // --- the fake service resolves like the runtime (S2b) ---

    use crate::{
        AvailabilityKind, ChatRequest, EmbedInput, EmbeddingsRequest, InferenceService, RankRequest,
    };

    const CLOUD: &str = "anthropic:claude-3-5-haiku-latest";

    fn embed(text: &str) -> EmbeddingsRequest {
        EmbeddingsRequest {
            input: EmbedInput::One(text.to_owned()),
            dimensions: None,
            normalize: None,
            input_type: None,
            instruction: None,
        }
    }

    fn chat(prompt: &str) -> ChatRequest {
        ChatRequest {
            prompt: Some(prompt.to_owned()),
            ..ChatRequest::default()
        }
    }

    fn rank(query: &str) -> RankRequest {
        RankRequest {
            query: query.to_owned(),
            passages: vec!["a passage".to_owned()],
        }
    }

    /// The code and the resolver's answer an error carries.
    fn refusal(error: &InferenceError) -> (&str, AvailabilityKind) {
        let details = error
            .availability()
            .unwrap_or_else(|| panic!("a resolution refusal carries details: {error:?}"));
        (error.code(), details.availability)
    }

    #[test]
    fn the_fake_service_refuses_what_the_catalog_does_not_know() {
        let service = FakeInferenceService::new();
        // Identity is the real catalog's: a name it does not know is unknown
        // here too, with the same code and the same answer.
        for spec in ["nope", "local:nope", "nope:q4_k_m"] {
            let error = service
                .embeddings(spec, &embed("hi"))
                .expect_err("not in the catalog");
            assert_eq!(
                refusal(&error),
                ("inference.unknown_model", AvailabilityKind::NotInCatalog),
                "{spec}: {error:?}"
            );
        }
        // A malformed spec is the resolver's refusal, before any catalog.
        let error = service
            .embeddings("openai:", &embed("hi"))
            .expect_err("malformed spec");
        assert_eq!(error.code(), "inference.invalid_request", "{error:?}");
        assert_eq!(error.availability(), None);
        // A path that names no file is missing here as it is there.
        let error = service
            .chat("/fake/models/absent.gguf", &chat("hi"))
            .expect_err("no such file");
        assert_eq!(
            refusal(&error),
            ("inference.unknown_model", AvailabilityKind::PathMissing),
            "{error:?}"
        );
    }

    #[test]
    fn the_fake_service_holds_a_model_to_its_catalogued_task() {
        let service = FakeInferenceService::new();
        let mismatches: [(&str, Result<(), InferenceError>); 4] = [
            (
                "chat miniLM",
                service.chat("miniLM", &chat("hi")).map(|_| ()),
            ),
            (
                "rank tinyllama",
                service.rank("tinyllama", &rank("q")).map(|_| ()),
            ),
            (
                "tokenize cloud",
                service.tokenize(CLOUD, "hi", false).map(|_| ()),
            ),
            (
                "embed cloud",
                service.embeddings(CLOUD, &embed("hi")).map(|_| ()),
            ),
        ];
        for (what, result) in mismatches {
            let error = result.expect_err(what);
            assert_eq!(
                refusal(&error),
                (
                    "inference.unsupported_operation",
                    AvailabilityKind::TaskNotSupported
                ),
                "{what}: {error:?}"
            );
        }
    }

    #[test]
    fn the_fake_world_has_every_catalogued_model_present_and_every_provider_ready() {
        let service = FakeInferenceService::new();
        // Execution, on a local model that is not downloaded anywhere and a
        // cloud model with no key in any environment.
        let response = service
            .embeddings("miniLM", &embed("hi"))
            .expect("every catalogued model is present");
        assert_eq!(response.model, "miniLM");
        assert_eq!(response.data[0].embedding.len(), 8);
        let response = service
            .chat(CLOUD, &chat("hi"))
            .expect("every provider is built, on the network, with its key");
        assert_eq!(response.model, CLOUD);
        service
            .chat("tinyllama", &chat("hi"))
            .expect("local generation is built");
        service
            .rank("jina-reranker-v1-tiny", &rank("q"))
            .expect("local ranking is built");
        service
            .tokenize("tinyllama", "hi", true)
            .expect("local tokenization is built");

        // A pull, decided the way the runtime decides it.
        let pulled = service
            .pull_model("miniLM")
            .expect("present, nothing to fetch");
        assert_eq!(pulled.model, "miniLM");
        assert_eq!(
            pulled.path,
            PathBuf::from("/fake/models/all-MiniLM-L6-v2.F16.gguf")
        );
        let error = service
            .pull_model("openai:gpt-4o")
            .expect_err("nothing to pull for a cloud model");
        assert_eq!(error.code(), "inference.unsupported_operation", "{error:?}");
        let error = service.pull_model("nope").expect_err("unknown");
        assert_eq!(error.code(), "inference.unknown_model", "{error:?}");
    }

    #[test]
    fn the_fake_service_reports_capability_from_the_catalog() {
        let service = FakeInferenceService::new();

        let cloud = service.capability(CLOUD).expect("located, never loaded");
        assert_eq!(cloud.provider, ProviderKind::Anthropic);
        assert_eq!(cloud.model, "claude-3-5-haiku-latest");
        assert_eq!(cloud.availability, AvailabilityKind::Ready);
        assert!(cloud.requires_network && cloud.requires_api_key);
        assert!(cloud.network_enabled && cloud.provider_feature_enabled);
        assert!(cloud.can_generate && !cloud.can_embed && !cloud.can_rank && !cloud.can_tokenize);
        assert_eq!(cloud.embedding_dim, 0);

        let local = service.capability("miniLM").expect("located");
        assert_eq!(local.provider, ProviderKind::Local);
        assert_eq!(local.availability, AvailabilityKind::Ready);
        assert_eq!(local.pull_spec, None);
        assert_eq!(local.size_bytes, None);
        assert!(!local.requires_network && !local.requires_api_key);
        assert!(local.can_embed && local.can_tokenize && !local.can_generate && !local.can_rank);
        assert_eq!(local.embedding_dim, 8);

        // Reported, not refused, like the runtime.
        let unknown = service.capability("nope").expect("reported");
        assert_eq!(unknown.availability, AvailabilityKind::NotInCatalog);
        assert!(
            !unknown.can_generate
                && !unknown.can_embed
                && !unknown.can_rank
                && !unknown.can_tokenize
        );
    }

    #[test]
    fn the_fakes_own_models_do_everything() {
        let service = FakeInferenceService::new();
        service
            .chat("fake-generate", &chat("hi"))
            .expect("own model generates");
        service
            .embeddings("fake-embed", &embed("hi"))
            .expect("own model embeds");
        service
            .rank("fake-rank", &rank("q"))
            .expect("own model ranks");
        service
            .tokenize("fake-embed", "hi", false)
            .expect("own model tokenizes");
        // Whatever the catalog would say about the name, the fake's own do
        // every task: they exist so a consumer can pin a fixed name.
        service
            .chat("fake-embed", &chat("hi"))
            .expect("own model does every task");

        let own = service.capability("fake-embed").expect("located");
        assert_eq!(own.provider, ProviderKind::Local);
        assert_eq!(own.availability, AvailabilityKind::Ready);
        assert!(own.can_generate && own.can_embed && own.can_rank && own.can_tokenize);
        assert_eq!(own.embedding_dim, 8);

        let pulled = service.pull_model("fake-embed").expect("present");
        assert_eq!(pulled.path, PathBuf::from("/fake/models/fake-embed.gguf"));
    }

    #[test]
    fn the_fake_lists_its_own_models_with_the_embedding_dim_it_was_built_with() {
        let service = FakeInferenceService::new().with_embedding_dim(5);
        let listed = service.list_models();

        // Every own model, once, by the name a consumer pins.
        let names: Vec<&str> = listed.iter().map(|m| m.name.as_str()).collect();
        let own: Vec<&str> = FAKE_MODELS.iter().map(|&(name, _)| name).collect();
        assert_eq!(names, own);
        assert_eq!(
            service.status().models_catalogued,
            FAKE_MODELS.len(),
            "status counts the listing"
        );

        // The embedding model carries the fake's dimension; the others none.
        for model in &listed {
            let expected = if model.task == ModelTask::Embed { 5 } else { 0 };
            assert_eq!(model.embedding_dim, expected, "{}", model.name);
        }
        assert!(
            listed.iter().any(|m| m.task == ModelTask::Embed),
            "the catalog holds an embedding model"
        );
    }

    #[test]
    fn detokenize_reverses_tokenize_on_a_located_model_and_refuses_otherwise() {
        let service = FakeInferenceService::new().with_undownloaded("miniLM");

        // Own model: the bytes come back as the text they were.
        let ids = service
            .tokenize("fake-embed", "hi", false)
            .expect("own model tokenizes");
        assert_eq!(
            service.detokenize("fake-embed", &ids).expect("round trip"),
            "hi"
        );
        // Ids that are not bytes are dropped rather than invented.
        assert_eq!(
            service
                .detokenize("fake-embed", &[u32::from(b'o'), 0x1_0000, u32::from(b'k')])
                .expect("round trip"),
            "ok"
        );

        // Resolution first, as everywhere: an unknown name and a model not on
        // disk refuse with the code and answer the runtime would give.
        let unknown = service.detokenize("nope", &ids).expect_err("unknown");
        assert_eq!(
            refusal(&unknown),
            ("inference.unknown_model", AvailabilityKind::NotInCatalog)
        );
        let absent = service.detokenize("miniLM", &ids).expect_err("not on disk");
        assert_eq!(
            refusal(&absent),
            ("inference.missing_model", AvailabilityKind::NotDownloaded)
        );
    }

    #[test]
    fn a_world_with_a_model_undownloaded_reports_refuses_then_pulls_it() {
        let service = FakeInferenceService::new().with_undownloaded("miniLM");

        // Reported with the runtime's answer: the pull spec and the size.
        let capability = service.capability("miniLM").expect("located");
        assert_eq!(capability.availability, AvailabilityKind::NotDownloaded);
        assert_eq!(capability.pull_spec.as_deref(), Some("miniLM"));
        let size = capability
            .size_bytes
            .expect("a catalogued variant has a size");
        assert!(size > 0);

        // Refused for a run, carrying the same answer.
        let error = service
            .embeddings("miniLM", &embed("hi"))
            .expect_err("not on disk");
        assert_eq!(
            refusal(&error),
            ("inference.missing_model", AvailabilityKind::NotDownloaded),
            "{error:?}"
        );
        let details = error.availability().expect("carried");
        assert_eq!(details.pull_spec.as_deref(), Some("miniLM"));
        assert_eq!(details.size_bytes, Some(size));

        // Presence comes after identity and task, as in the runtime: the
        // model is held to its task before anyone looks for its file.
        let error = service
            .chat("miniLM", &chat("hi"))
            .expect_err("an embedding model does not generate");
        assert_eq!(refusal(&error).1, AvailabilityKind::TaskNotSupported);

        // Every other catalogued model is still present.
        service
            .embeddings("nomic-embed", &embed("hi"))
            .expect("only miniLM is held back");

        // A pull makes it present, at the path the runtime would use …
        let pulled = service.pull_model("miniLM").expect("fetched");
        assert_eq!(
            pulled.path,
            PathBuf::from("/fake/models/all-MiniLM-L6-v2.F16.gguf")
        );
        // … and the same run now succeeds.
        let response = service
            .embeddings("miniLM", &embed("hi"))
            .expect("present after the pull");
        assert_eq!(response.model, "miniLM");
        assert_eq!(
            service.capability("miniLM").expect("located").availability,
            AvailabilityKind::Ready
        );
    }

    #[test]
    fn fake_availability_fixes_the_build_and_keeps_identity() {
        let requested = ModelUse::Run(ModelTask::Embed);
        let kept = [
            Availability::Ready,
            Availability::NotInCatalog,
            Availability::PathMissing,
            Availability::TaskNotSupported { requested },
        ];
        for real in kept {
            assert_eq!(fake_availability(real.clone()), real);
        }
        let fixed = [
            Availability::LocalExecutionNotBuilt,
            Availability::ProviderNotBuilt,
            Availability::NotDownloaded {
                pull_spec: "miniLM".to_owned(),
                size_bytes: 1,
            },
            Availability::NetworkDisabled,
            Availability::KeyMissing {
                env_var: "OPENAI_API_KEY",
                config_key: "openai.api_key".to_owned(),
            },
        ];
        for real in fixed {
            assert_eq!(
                fake_availability(real.clone()),
                Availability::Ready,
                "{real:?}"
            );
        }
    }
}
