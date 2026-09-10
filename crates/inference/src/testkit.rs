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
use std::time::Duration;

use crate::{GenerateRequest, GenerateResponse, InferenceEngine, InferenceError, StopReason};

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
            Self::MissingModel => {
                InferenceError::Registry("fake: unknown model: fake:missing".to_string())
            }
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

/// Deterministic runtime-level fake for the executor's
/// [`crate::InferenceService`] surface: model management plus the wire-typed
/// compute paths, with zero network, model files, or sleeps. Same inputs, same
/// outputs, forever — so fixture replays are stable.
#[derive(Clone, Debug)]
pub struct FakeInferenceService {
    embedding_dim: usize,
}

impl Default for FakeInferenceService {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeInferenceService {
    #[must_use]
    pub fn new() -> Self {
        Self { embedding_dim: 8 }
    }

    #[must_use]
    pub fn with_embedding_dim(mut self, dim: usize) -> Self {
        self.embedding_dim = dim;
        self
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
        vec![
            fake_model("fake-embed", crate::ModelTask::Embed, self.embedding_dim),
            fake_model("fake-generate", crate::ModelTask::Generate, 0),
            fake_model("fake-rank", crate::ModelTask::Rank, 0),
        ]
    }

    fn list_local_models(&self) -> Vec<crate::ModelInfo> {
        Vec::new()
    }

    fn pull_model(&self, model: &str) -> Result<crate::PullModelOutput, InferenceError> {
        Ok(crate::PullModelOutput {
            model: model.to_owned(),
            path: std::path::PathBuf::from(format!("fake-models/{model}.gguf")),
        })
    }

    fn capability(&self, model_spec: &str) -> Result<crate::InferenceCapability, InferenceError> {
        Ok(crate::InferenceCapability {
            provider: crate::ProviderKind::Local,
            model: model_spec.to_owned(),
            availability: crate::AvailabilityKind::Ready,
            pull_spec: None,
            size_bytes: None,
            can_generate: true,
            can_tokenize: true,
            can_embed: true,
            can_rank: true,
            requires_network: false,
            requires_api_key: false,
            provider_feature_enabled: true,
            network_enabled: false,
            embedding_dim: self.embedding_dim,
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
        let prompt = chat_prompt(request);
        let content = format!("fake:{prompt}");
        let prompt_tokens = u32::try_from(prompt.split_whitespace().count()).unwrap_or(u32::MAX);
        let completion_tokens =
            u32::try_from(content.split_whitespace().count()).unwrap_or(u32::MAX);
        Ok(crate::ChatResponse {
            model: model_spec.to_owned(),
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
        _model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError> {
        let mut ids: Vec<u32> = text.bytes().map(u32::from).collect();
        if add_special {
            ids.insert(0, 1);
            ids.push(2);
        }
        Ok(ids)
    }

    fn detokenize(&self, _model_spec: &str, ids: &[u32]) -> Result<String, InferenceError> {
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
            model: model_spec.to_owned(),
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
        _model_spec: &str,
        request: &crate::RankRequest,
    ) -> Result<crate::RankResponse, InferenceError> {
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
                ready: true,
                model_prefix: "local:".to_owned(),
            }],
            models_dir: std::path::PathBuf::from("/fake/models"),
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
}
