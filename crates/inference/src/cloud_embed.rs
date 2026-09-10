//! Cloud embedding engine: text → dense vector via OpenAI or Google APIs.
//!
//! [`CloudEmbeddingEngine`] provides the same `embed()` / `embed_batch()`
//! interface as the local [`EmbeddingEngine`](crate::EmbeddingEngine), but
//! sends requests to cloud provider APIs instead of running a local model.
//!
//! Anthropic does not offer an embedding API — attempting to construct a
//! `CloudEmbeddingEngine` with `ProviderKind::Anthropic` returns `NotSupported`.

#[cfg(any(feature = "openai", feature = "google"))]
use crate::provider::cloud::{post_json, CloudPost};
use crate::{embedding_provider_feature_enabled, InferenceError, ProviderKind};

/// Embedding engine backed by a cloud provider (OpenAI or Google).
///
/// Thread-safe (`Send`) — no interior mutability needed since each embed
/// call is a stateless HTTP request.
pub struct CloudEmbeddingEngine {
    provider: ProviderKind,
    api_key: String,
    model: String,
    /// Where requests go; the provider's own API root outside tests.
    api_base: String,
}

impl std::fmt::Debug for CloudEmbeddingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudEmbeddingEngine")
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl CloudEmbeddingEngine {
    /// Create a new cloud embedding engine.
    ///
    /// # Errors
    ///
    /// - `NotSupported` if provider is `Anthropic` or `Local`
    /// - `Provider` if api_key or model is empty
    pub fn new(
        provider: ProviderKind,
        api_key: String,
        model: String,
    ) -> Result<Self, InferenceError> {
        match provider {
            ProviderKind::Anthropic => {
                return Err(InferenceError::NotSupported(
                    "Anthropic does not offer an embedding API".to_string(),
                ));
            }
            ProviderKind::Local => {
                return Err(InferenceError::NotSupported(
                    "use EmbeddingEngine for local models, not CloudEmbeddingEngine".to_string(),
                ));
            }
            ProviderKind::OpenAI | ProviderKind::Google => {
                if !embedding_provider_feature_enabled(provider) {
                    return Err(InferenceError::NotSupported(format!(
                        "{provider} embedding provider not enabled"
                    )));
                }
            }
        }

        if api_key.trim().is_empty() {
            return Err(InferenceError::Provider(format!(
                "{} API key is empty",
                provider
            )));
        }
        if model.trim().is_empty() {
            return Err(InferenceError::Provider(format!(
                "{} model name is empty",
                provider
            )));
        }

        Ok(Self {
            api_base: api_base_for(provider).to_string(),
            provider,
            api_key,
            model,
        })
    }

    /// Point requests at `base_url` instead of the provider's public API: an
    /// OpenAI-compatible server, a proxy, or a test's canned-response server.
    pub(crate) fn with_base_url(mut self, base_url: &str) -> Self {
        self.api_base = base_url.to_string();
        self
    }

    /// Which provider this engine uses.
    pub fn provider(&self) -> ProviderKind {
        self.provider
    }

    /// The model name.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// How long one embedding request may take.
    #[cfg(any(feature = "openai", feature = "google"))]
    const SINGLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    /// How long a batch (up to [`Self::CLOUD_BATCH_CHUNK_SIZE`] texts) may take.
    #[cfg(any(feature = "openai", feature = "google"))]
    const BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

    /// POST `body` to `url` as this engine's provider, with its credential.
    #[cfg(any(feature = "openai", feature = "google"))]
    fn post(
        &self,
        url: &str,
        body: &str,
        timeout: std::time::Duration,
    ) -> Result<String, InferenceError> {
        let bearer = format!("Bearer {}", self.api_key);
        let (provider, credential) = match self.provider {
            ProviderKind::OpenAI => ("OpenAI", ("Authorization", bearer.as_str())),
            ProviderKind::Google => ("Google", ("x-goog-api-key", self.api_key.as_str())),
            // `new` admits only the two providers above.
            other => {
                return Err(InferenceError::NotSupported(format!(
                    "cloud embedding not supported for provider: {other}"
                )))
            }
        };
        post_json(&CloudPost {
            provider,
            url,
            headers: &[credential],
            body,
            timeout,
        })
    }

    /// Embed a single text via the cloud API.
    fn embed_single(&self, text: &str) -> Result<Vec<f32>, InferenceError> {
        match self.provider {
            #[cfg(feature = "openai")]
            ProviderKind::OpenAI => {
                let body = crate::provider::openai::build_embed_request_json(&self.model, &[text]);
                let response_body = self.post(
                    &crate::provider::openai::embed_url(&self.api_base),
                    &body,
                    Self::SINGLE_TIMEOUT,
                )?;

                let mut embeddings =
                    crate::provider::openai::parse_embed_response_json(&response_body)?;
                if embeddings.is_empty() {
                    return Err(InferenceError::Provider(
                        "OpenAI: no embeddings returned".to_string(),
                    ));
                }
                let embedding = embeddings.swap_remove(0);
                Ok(l2_normalize(embedding))
            }

            #[cfg(feature = "google")]
            ProviderKind::Google => {
                let url = crate::provider::google::build_embed_url(&self.api_base, &self.model);
                let body = crate::provider::google::build_embed_request_json(text);
                let response_body = self.post(&url, &body, Self::SINGLE_TIMEOUT)?;

                let embedding = crate::provider::google::parse_embed_response_json(&response_body)?;
                Ok(l2_normalize(embedding))
            }

            other => Err(InferenceError::NotSupported(format!(
                "cloud embedding not supported for provider: {other}"
            ))),
        }
    }

    /// Embed a batch of texts via the cloud API.
    /// Maximum texts per HTTP request to avoid API limits and timeouts.
    const CLOUD_BATCH_CHUNK_SIZE: usize = 64;

    fn embed_many(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        // Chunk large batches to stay within API limits.
        if texts.len() > Self::CLOUD_BATCH_CHUNK_SIZE {
            let mut all_embeddings = Vec::with_capacity(texts.len());
            for chunk in texts.chunks(Self::CLOUD_BATCH_CHUNK_SIZE) {
                all_embeddings.extend(self.embed_many(chunk)?);
            }
            return Ok(all_embeddings);
        }

        match self.provider {
            #[cfg(feature = "openai")]
            ProviderKind::OpenAI => {
                let body = crate::provider::openai::build_embed_request_json(&self.model, texts);
                let response_body = self.post(
                    &crate::provider::openai::embed_url(&self.api_base),
                    &body,
                    Self::BATCH_TIMEOUT,
                )?;

                let embeddings =
                    crate::provider::openai::parse_embed_response_json(&response_body)?;
                Ok(embeddings.into_iter().map(l2_normalize).collect())
            }

            #[cfg(feature = "google")]
            ProviderKind::Google => {
                let url =
                    crate::provider::google::build_batch_embed_url(&self.api_base, &self.model);
                let body =
                    crate::provider::google::build_batch_embed_request_json(&self.model, texts);
                let response_body = self.post(&url, &body, Self::BATCH_TIMEOUT)?;

                let embeddings =
                    crate::provider::google::parse_batch_embed_response_json(&response_body)?;
                Ok(embeddings.into_iter().map(l2_normalize).collect())
            }

            other => Err(InferenceError::NotSupported(format!(
                "cloud embedding not supported for provider: {other}"
            ))),
        }
    }
}

impl crate::InferenceEngine for CloudEmbeddingEngine {
    fn embed(&self, text: &str) -> Result<Vec<f32>, InferenceError> {
        self.embed_single(text)
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, InferenceError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if texts.len() == 1 {
            return self.embed_single(texts[0]).map(|e| vec![e]);
        }
        self.embed_many(texts)
    }

    fn supports_embed(&self) -> bool {
        true
    }

    fn embedding_dim(&self) -> usize {
        match self.model.as_str() {
            "text-embedding-3-small" => 1536,
            "text-embedding-3-large" => 3072,
            "text-embedding-ada-002" => 1536,
            "text-embedding-004" => 768, // Google
            "gemini-embedding-001" => 3072,
            "gemini-embedding-2" => 3072,
            _ => 0,
        }
    }

    fn is_healthy(&self) -> bool {
        true
    }
}

// Compile-time verify Send.
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_both() {
        assert_send::<CloudEmbeddingEngine>();
    }
    let _ = assert_both;
};

/// The API root for `provider` when nothing overrides it.
///
/// Only reached for a provider `new` has admitted, and `new` admits exactly
/// the cloud providers whose feature is on — every one of which has a
/// default — so the empty fallback (the local provider's "no endpoint")
/// never serves a request.
fn api_base_for(provider: ProviderKind) -> &'static str {
    crate::default_base_url(provider).unwrap_or_default()
}

/// L2-normalize an embedding vector.
///
/// Returns the input unchanged if the norm is zero (all-zero vector).
fn l2_normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    #[cfg(feature = "openai")]
    #[test]
    fn new_openai_with_valid_args() {
        let engine = CloudEmbeddingEngine::new(
            ProviderKind::OpenAI,
            "sk-test-key".into(),
            "text-embedding-3-small".into(),
        );
        assert!(engine.is_ok());
        let engine = engine.unwrap();
        assert_eq!(engine.provider(), ProviderKind::OpenAI);
        assert_eq!(engine.model(), "text-embedding-3-small");
        assert_eq!(engine.api_base, crate::provider::openai::API_BASE);
    }

    #[cfg(feature = "google")]
    #[test]
    fn new_google_with_valid_args() {
        let engine = CloudEmbeddingEngine::new(
            ProviderKind::Google,
            "AIza-test-key".into(),
            "text-embedding-004".into(),
        );
        assert!(engine.is_ok());
        let engine = engine.unwrap();
        assert_eq!(engine.provider(), ProviderKind::Google);
        assert_eq!(engine.model(), "text-embedding-004");
        assert_eq!(engine.api_base, crate::provider::google::API_BASE);
    }

    #[test]
    fn new_anthropic_returns_not_supported() {
        let result =
            CloudEmbeddingEngine::new(ProviderKind::Anthropic, "sk-ant-key".into(), "model".into());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, InferenceError::NotSupported(_)),
            "should be NotSupported, got: {err}"
        );
        assert!(err.to_string().contains("Anthropic"));
    }

    #[test]
    fn new_local_returns_not_supported() {
        let result = CloudEmbeddingEngine::new(ProviderKind::Local, "key".into(), "model".into());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            InferenceError::NotSupported(_)
        ));
    }

    #[cfg(any(feature = "openai", feature = "google"))]
    #[test]
    fn new_empty_key_returns_provider_error() {
        let provider = if cfg!(feature = "openai") {
            ProviderKind::OpenAI
        } else {
            ProviderKind::Google
        };
        let result = CloudEmbeddingEngine::new(provider, "".into(), "model".into());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Provider(_)));
    }

    #[cfg(any(feature = "openai", feature = "google"))]
    #[test]
    fn new_whitespace_key_returns_provider_error() {
        let provider = if cfg!(feature = "openai") {
            ProviderKind::OpenAI
        } else {
            ProviderKind::Google
        };
        let result = CloudEmbeddingEngine::new(provider, "  \t".into(), "model".into());
        assert!(result.is_err());
    }

    #[cfg(any(feature = "openai", feature = "google"))]
    #[test]
    fn new_empty_model_returns_provider_error() {
        let provider = if cfg!(feature = "openai") {
            ProviderKind::OpenAI
        } else {
            ProviderKind::Google
        };
        let result = CloudEmbeddingEngine::new(provider, "key".into(), "".into());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Provider(_)));
    }

    // -----------------------------------------------------------------------
    // InferenceEngine trait
    // -----------------------------------------------------------------------

    #[cfg(feature = "openai")]
    #[test]
    fn supports_embed_returns_true() {
        let engine =
            CloudEmbeddingEngine::new(ProviderKind::OpenAI, "sk-key".into(), "model".into())
                .unwrap();
        assert!(crate::InferenceEngine::supports_embed(&engine));
    }

    #[cfg(feature = "openai")]
    #[test]
    fn supports_generate_returns_false() {
        let engine =
            CloudEmbeddingEngine::new(ProviderKind::OpenAI, "sk-key".into(), "model".into())
                .unwrap();
        assert!(!crate::InferenceEngine::supports_generate(&engine));
    }

    #[cfg(feature = "openai")]
    #[test]
    fn generate_returns_not_supported() {
        let mut engine =
            CloudEmbeddingEngine::new(ProviderKind::OpenAI, "sk-key".into(), "model".into())
                .unwrap();
        let req = crate::GenerateRequest::default();
        let err = crate::InferenceEngine::generate(&mut engine, &req).unwrap_err();
        assert!(matches!(err, InferenceError::NotSupported(_)));
    }

    #[cfg(feature = "openai")]
    #[test]
    fn embed_batch_empty_returns_empty() {
        let engine =
            CloudEmbeddingEngine::new(ProviderKind::OpenAI, "sk-key".into(), "model".into())
                .unwrap();
        let result = crate::InferenceEngine::embed_batch(&engine, &[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Debug output
    // -----------------------------------------------------------------------

    #[cfg(feature = "openai")]
    #[test]
    fn debug_redacts_api_key() {
        let engine = CloudEmbeddingEngine::new(
            ProviderKind::OpenAI,
            "sk-secret-key-do-not-leak".into(),
            "text-embedding-3-small".into(),
        )
        .unwrap();
        let dbg = format!("{:?}", engine);
        assert!(
            !dbg.contains("sk-secret-key-do-not-leak"),
            "API key leaked in Debug: {dbg}"
        );
        assert!(dbg.contains("[REDACTED]"), "should show [REDACTED]: {dbg}");
        assert!(
            dbg.contains("text-embedding-3-small"),
            "should show model: {dbg}"
        );
        assert!(dbg.contains("OpenAI"), "should show provider: {dbg}");
    }

    // -----------------------------------------------------------------------
    // L2 normalization
    // -----------------------------------------------------------------------

    #[test]
    fn l2_normalize_unit_vector() {
        let v = l2_normalize(vec![1.0, 0.0, 0.0]);
        assert!((v[0] - 1.0).abs() < 1e-6);
        assert!((v[1]).abs() < 1e-6);
        assert!((v[2]).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_non_unit_vector() {
        let v = l2_normalize(vec![3.0, 4.0]);
        // norm = 5.0, normalized = [0.6, 0.8]
        assert!((v[0] - 0.6).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_zero_vector_unchanged() {
        let v = l2_normalize(vec![0.0, 0.0, 0.0]);
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn l2_normalize_already_normalized() {
        let v = l2_normalize(vec![0.6, 0.8]);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_empty_vector() {
        let v = l2_normalize(vec![]);
        assert!(v.is_empty());
    }

    #[test]
    fn l2_normalize_negative_values() {
        let v = l2_normalize(vec![-3.0, 4.0]);
        // norm = 5.0, normalized = [-0.6, 0.8]
        assert!((v[0] - (-0.6)).abs() < 1e-6);
        assert!((v[1] - 0.8).abs() < 1e-6);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // The request path, end to end against a local stand-in for the API
    // -----------------------------------------------------------------------

    #[cfg(any(feature = "openai", feature = "google"))]
    use crate::provider::cloud::test_server::CannedResponse;
    #[cfg(any(feature = "openai", feature = "google"))]
    use crate::InferenceEngine;

    #[cfg(any(feature = "openai", feature = "google"))]
    fn engine_at(provider: ProviderKind, server: &CannedResponse) -> CloudEmbeddingEngine {
        CloudEmbeddingEngine::new(provider, "test-key".into(), "embed-test".into())
            .expect("a valid engine")
            .with_base_url(server.base_url())
    }

    fn assert_unit(actual: &[f32], expected: &[f32]) {
        assert_eq!(actual.len(), expected.len(), "{actual:?} vs {expected:?}");
        for (a, e) in actual.iter().zip(expected) {
            assert!((a - e).abs() < 1e-6, "{actual:?} vs {expected:?}");
        }
    }

    /// A single OpenAI embedding round-trips: the endpoint under the base, the
    /// bearer credential, and the vector normalised on the way out.
    #[cfg(feature = "openai")]
    #[test]
    fn an_openai_embedding_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"data":[{"index":0,"embedding":[3.0,4.0]}],"usage":{"prompt_tokens":1,"total_tokens":1}}"#,
        );
        let vector = engine_at(ProviderKind::OpenAI, &server)
            .embed("hi")
            .expect("an embedding");
        assert_unit(&vector, &[0.6, 0.8]);

        let request = server.request();
        assert!(
            request.starts_with("POST /embeddings "),
            "the endpoint under the base: {request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-key"),
            "the credential travels as a bearer token: {request}"
        );
    }

    /// A batch of OpenAI embeddings round-trips in one request.
    #[cfg(feature = "openai")]
    #[test]
    fn an_openai_batch_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"data":[{"index":1,"embedding":[0.0,2.0]},{"index":0,"embedding":[3.0,4.0]}],"usage":{"prompt_tokens":2,"total_tokens":2}}"#,
        );
        let vectors = engine_at(ProviderKind::OpenAI, &server)
            .embed_batch(&["a", "b"])
            .expect("two embeddings");
        assert_eq!(vectors.len(), 2);
        assert_unit(&vectors[0], &[0.6, 0.8]);
        assert_unit(&vectors[1], &[0.0, 1.0]);
        assert!(server.request().starts_with("POST /embeddings "));
    }

    /// An embedding model OpenAI does not know is a 404 `model_not_found`
    /// (#3236). By status alone it was "provider unavailable, retry".
    #[cfg(feature = "openai")]
    #[test]
    fn an_unknown_openai_embedding_model_is_model_not_found() {
        let server = CannedResponse::serve(
            404,
            r#"{"error":{"message":"The model `text-embedding-nope` does not exist or you do not have access to it.","type":"invalid_request_error","param":null,"code":"model_not_found"}}"#,
        );
        let error = engine_at(ProviderKind::OpenAI, &server)
            .embed("hi")
            .expect_err("unknown model");
        assert_eq!(error.code(), "inference.provider_model_not_found");
    }

    /// No credits on a batch embed is quota, not a rate limit (#3236).
    #[cfg(feature = "openai")]
    #[test]
    fn no_credits_on_a_batch_is_quota_exhausted() {
        let server = CannedResponse::serve(
            429,
            r#"{"error":{"message":"You have no credits remaining.","type":"insufficient_quota","param":null,"code":"insufficient_quota"}}"#,
        );
        let error = engine_at(ProviderKind::OpenAI, &server)
            .embed_batch(&["a", "b"])
            .expect_err("no credits");
        assert_eq!(error.code(), "inference.provider_quota_exhausted");
    }

    /// A single Google embedding round-trips: the model's `embedContent`
    /// endpoint under the base, the key in a header, the vector normalised.
    #[cfg(feature = "google")]
    #[test]
    fn a_google_embedding_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(200, r#"{"embedding":{"values":[3.0,4.0]}}"#);
        let vector = engine_at(ProviderKind::Google, &server)
            .embed("hi")
            .expect("an embedding");
        assert_unit(&vector, &[0.6, 0.8]);

        let request = server.request();
        assert!(
            request.starts_with("POST /v1beta/models/embed-test:embedContent "),
            "the model's endpoint under the base: {request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("x-goog-api-key: test-key"),
            "the credential travels in x-goog-api-key: {request}"
        );
    }

    /// A batch of Google embeddings uses `batchEmbedContents` in one request.
    #[cfg(feature = "google")]
    #[test]
    fn a_google_batch_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"embeddings":[{"values":[3.0,4.0]},{"values":[0.0,2.0]}]}"#,
        );
        let vectors = engine_at(ProviderKind::Google, &server)
            .embed_batch(&["a", "b"])
            .expect("two embeddings");
        assert_eq!(vectors.len(), 2);
        assert_unit(&vectors[0], &[0.6, 0.8]);
        assert_unit(&vectors[1], &[0.0, 1.0]);
        assert!(server
            .request()
            .starts_with("POST /v1beta/models/embed-test:batchEmbedContents "));
    }

    /// A retired Google embedding model is a 404 `NOT_FOUND` (#3236) —
    /// `text-embedding-004` today. By status alone it was an outage.
    #[cfg(feature = "google")]
    #[test]
    fn a_retired_google_embedding_model_is_model_not_found() {
        let server = CannedResponse::serve(
            404,
            r#"{"error":{"code":404,"message":"models/text-embedding-004 is not found for API version v1beta, or is not supported for embedContent. Call ModelService.ListModels to see the list of available models and their supported methods.","status":"NOT_FOUND"}}"#,
        );
        let error = engine_at(ProviderKind::Google, &server)
            .embed("hi")
            .expect_err("retired model");
        assert_eq!(error.code(), "inference.provider_model_not_found");
        assert!(
            error.to_string().contains("text-embedding-004"),
            "names the model the provider rejected: {error}"
        );
    }
}
