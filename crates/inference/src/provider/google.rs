//! Google (Gemini) cloud generation provider.
//!
//! Sends generation requests to
//! `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
//! and maps the response to [`GenerateResponse`].
//!
//! The API key is passed via the `x-goog-api-key` header for security
//! (avoids leaking credentials in URL logs).

use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

const API_BASE: &str = "https://generativelanguage.googleapis.com/v1beta/models";

/// Google cloud provider state.
pub(crate) struct GoogleProvider {
    api_key: String,
    model: String,
}

impl std::fmt::Debug for GoogleProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl GoogleProvider {
    pub fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
        if api_key.trim().is_empty() {
            return Err(InferenceError::Provider(
                "Google API key is empty".to_string(),
            ));
        }
        if model.trim().is_empty() {
            return Err(InferenceError::Provider(
                "Google model name is empty".to_string(),
            ));
        }
        Ok(Self { api_key, model })
    }

    pub fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        if request.max_tokens == 0 {
            return Err(InferenceError::Provider(
                "max_tokens must be greater than 0".to_string(),
            ));
        }

        let url = build_url(&self.model);
        let body = build_request_json(request);

        let agent = ureq::Agent::new_with_config(
            ureq::config::Config::builder()
                .timeout_global(Some(std::time::Duration::from_secs(30)))
                .build(),
        );
        let mut response = agent
            .post(&url)
            .header("x-goog-api-key", &self.api_key)
            .header("content-type", "application/json")
            .send(&body)
            .map_err(|e| map_http_error("Google", e))?;

        let response_body = response.body_mut().read_to_string().map_err(|e| {
            InferenceError::Provider(format!("Google: failed to read response: {e}"))
        })?;

        parse_response_json(&response_body)
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

/// Build the full URL with the model name (API key sent via header).
pub(crate) fn build_url(model: &str) -> String {
    format!("{API_BASE}/{model}:generateContent")
}

/// Build the Google Gemini API request JSON.
///
/// Includes `topK` (supported by Gemini). Silently ignores `seed` and
/// `stop_tokens` (not supported).
pub(crate) fn build_request_json(request: &GenerateRequest) -> String {
    let mut gen_config = serde_json::json!({
        "maxOutputTokens": request.max_tokens
    });

    // Include temperature
    gen_config["temperature"] = serde_json::json!(request.temperature);

    // Include top_p if not default
    if request.top_p < 1.0 {
        gen_config["topP"] = serde_json::json!(request.top_p);
    }

    // Gemini supports top_k
    if request.top_k > 0 {
        gen_config["topK"] = serde_json::json!(request.top_k);
    }

    // Include stop sequences if non-empty
    if !request.stop_sequences.is_empty() {
        gen_config["stopSequences"] = serde_json::json!(request.stop_sequences);
    }

    // seed: silently ignored (not supported by Gemini)
    // stop_tokens: silently ignored (token-level, local only)

    // Disable thinking for Gemini 2.5+ models — without this, the model
    // spends the entire token budget on internal reasoning and returns no text.
    gen_config["thinkingConfig"] = serde_json::json!({"thinkingBudget": 0});

    let obj = serde_json::json!({
        "contents": [
            {
                "parts": [
                    {
                        "text": request.prompt
                    }
                ]
            }
        ],
        "generationConfig": gen_config
    });

    obj.to_string()
}

/// Parse the Google Gemini API response JSON into a `GenerateResponse`.
pub(crate) fn parse_response_json(body: &str) -> Result<GenerateResponse, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("Google: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        let code = error
            .get("code")
            .and_then(|c| c.as_u64())
            .map(|c| format!(" (code {c})"))
            .unwrap_or_default();
        return Err(InferenceError::Provider(format!(
            "Google API error{code}: {msg}"
        )));
    }

    // Extract from candidates array
    let candidates = json
        .get("candidates")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("Google: missing or invalid 'candidates' array".to_string())
        })?;

    if candidates.is_empty() {
        return Err(InferenceError::Provider(
            "Google: empty candidates array in response".to_string(),
        ));
    }

    let candidate = &candidates[0];

    // Extract text from content.parts
    let text = candidate
        .get("content")
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .ok_or_else(|| {
            InferenceError::Provider("Google: candidate missing content.parts".to_string())
        })?;

    // Map finishReason
    let stop_reason = match candidate.get("finishReason").and_then(|r| r.as_str()) {
        Some("STOP") => StopReason::StopToken,
        Some("MAX_TOKENS") => StopReason::MaxTokens,
        Some("SAFETY") => StopReason::Cancelled,
        Some("RECITATION") => StopReason::Cancelled,
        Some(other) => {
            tracing::warn!(reason = ?other, "Unknown stop reason from Google, defaulting to StopToken");
            StopReason::StopToken
        }
        None => StopReason::StopToken,
    };

    // Extract usage metadata
    let usage = json.get("usageMetadata");
    let prompt_tokens = usage
        .and_then(|u| u.get("promptTokenCount"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let completion_tokens = usage
        .and_then(|u| u.get("candidatesTokenCount"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    Ok(GenerateResponse {
        text,
        stop_reason,
        prompt_tokens,
        completion_tokens,
    })
}

// =========================================================================
// Embedding API
// =========================================================================

/// Build the URL for the Google embedContent API (single text).
pub(crate) fn build_embed_url(model: &str) -> String {
    format!("{API_BASE}/{model}:embedContent")
}

/// Build the URL for the Google batchEmbedContents API (multiple texts).
pub(crate) fn build_batch_embed_url(model: &str) -> String {
    format!("{API_BASE}/{model}:batchEmbedContents")
}

/// Build the Google embedContent request JSON for a single text.
pub(crate) fn build_embed_request_json(text: &str) -> String {
    serde_json::json!({
        "content": {
            "parts": [{"text": text}]
        }
    })
    .to_string()
}

/// Build the Google batchEmbedContents request JSON for multiple texts.
pub(crate) fn build_batch_embed_request_json(model: &str, texts: &[&str]) -> String {
    let requests: Vec<serde_json::Value> = texts
        .iter()
        .map(|text| {
            serde_json::json!({
                "model": format!("models/{model}"),
                "content": {
                    "parts": [{"text": text}]
                }
            })
        })
        .collect();

    serde_json::json!({ "requests": requests }).to_string()
}

/// Parse the Google embedContent response JSON into a single embedding vector.
pub(crate) fn parse_embed_response_json(body: &str) -> Result<Vec<f32>, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("Google: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        let code = error
            .get("code")
            .and_then(|c| c.as_u64())
            .map(|c| format!(" (code {c})"))
            .unwrap_or_default();
        return Err(InferenceError::Provider(format!(
            "Google embedding API error{code}: {msg}"
        )));
    }

    let values = json
        .get("embedding")
        .and_then(|e| e.get("values"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("Google: missing 'embedding.values' in response".to_string())
        })?;

    Ok(values
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0) as f32)
        .collect())
}

/// Parse the Google batchEmbedContents response JSON into embedding vectors.
pub(crate) fn parse_batch_embed_response_json(body: &str) -> Result<Vec<Vec<f32>>, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("Google: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        let code = error
            .get("code")
            .and_then(|c| c.as_u64())
            .map(|c| format!(" (code {c})"))
            .unwrap_or_default();
        return Err(InferenceError::Provider(format!(
            "Google embedding API error{code}: {msg}"
        )));
    }

    let embeddings = json
        .get("embeddings")
        .and_then(|e| e.as_array())
        .ok_or_else(|| {
            InferenceError::Provider(
                "Google: missing or invalid 'embeddings' array in batch response".to_string(),
            )
        })?;

    if embeddings.is_empty() {
        return Err(InferenceError::Provider(
            "Google: empty embeddings array in batch response".to_string(),
        ));
    }

    embeddings
        .iter()
        .map(|item| {
            let values = item
                .get("values")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    InferenceError::Provider(
                        "Google: batch embedding item missing 'values'".to_string(),
                    )
                })?;
            Ok(values
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect())
        })
        .collect()
}

/// Map ureq HTTP errors to InferenceError::Provider with descriptive messages.
fn map_http_error(provider: &str, err: ureq::Error) -> InferenceError {
    match &err {
        ureq::Error::StatusCode(status) => {
            let code = *status;
            let description = match code {
                400 => "bad request (check model name and parameters)",
                401 | 403 => "invalid or unauthorized API key",
                429 => "rate limited (too many requests)",
                500 => "server error",
                503 => "service unavailable",
                _ => "HTTP error",
            };
            InferenceError::Provider(format!("{provider}: {description} (HTTP {code})"))
        }
        _ => InferenceError::Provider(format!("{provider}: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_with_valid_key_and_model() {
        let p = GoogleProvider::new("AIza-test-key".into(), "gemini-pro".into());
        assert!(p.is_ok());
        assert_eq!(p.unwrap().model(), "gemini-pro");
    }

    #[test]
    fn new_with_empty_key_returns_error() {
        let p = GoogleProvider::new("".into(), "gemini-pro".into());
        assert!(p.is_err());
        assert!(p.unwrap_err().to_string().contains("key"));
    }

    #[test]
    fn new_with_whitespace_key_returns_error() {
        let p = GoogleProvider::new("  ".into(), "gemini-pro".into());
        assert!(p.is_err());
    }

    #[test]
    fn new_with_empty_model_returns_error() {
        let p = GoogleProvider::new("key".into(), "".into());
        assert!(p.is_err());
        assert!(p.unwrap_err().to_string().contains("model"));
    }

    // -----------------------------------------------------------------------
    // URL building
    // -----------------------------------------------------------------------

    #[test]
    fn url_contains_model_not_key() {
        let url = build_url("gemini-pro");
        assert!(url.contains("gemini-pro"));
        assert!(!url.contains("key="), "API key should not appear in URL");
        assert!(url.contains("generateContent"));
    }

    #[test]
    fn url_has_no_query_params() {
        let url = build_url("model");
        assert!(!url.contains('?'), "URL should have no query parameters");
    }

    // -----------------------------------------------------------------------
    // Request JSON building
    // -----------------------------------------------------------------------

    #[test]
    fn request_json_basic_structure() {
        let req = GenerateRequest {
            prompt: "Hello".into(),
            max_tokens: 100,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["contents"][0]["parts"][0]["text"], "Hello");
        assert_eq!(json["generationConfig"]["maxOutputTokens"], 100);
    }

    #[test]
    fn request_json_temperature_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            temperature: 0.7,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let temp = json["generationConfig"]["temperature"].as_f64().unwrap();
        assert!((temp - 0.7).abs() < 0.01);
    }

    #[test]
    fn request_json_top_k_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_k: 40,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["generationConfig"]["topK"], 40);
    }

    #[test]
    fn request_json_top_k_zero_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_k: 0,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json["generationConfig"].get("topK").is_none());
    }

    #[test]
    fn request_json_top_p_custom_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 0.9,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let top_p = json["generationConfig"]["topP"].as_f64().unwrap();
        assert!((top_p - 0.9).abs() < 0.01);
    }

    #[test]
    fn request_json_top_p_default_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 1.0,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json["generationConfig"].get("topP").is_none());
    }

    #[test]
    fn request_json_seed_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            seed: Some(42),
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json["generationConfig"].get("seed").is_none());
    }

    #[test]
    fn request_json_stop_sequences_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_sequences: vec!["END".into()],
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let seqs = json["generationConfig"]["stopSequences"]
            .as_array()
            .unwrap();
        assert_eq!(seqs.len(), 1);
        assert_eq!(seqs[0], "END");
    }

    #[test]
    fn request_json_stop_sequences_empty_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json["generationConfig"].get("stopSequences").is_none());
    }

    #[test]
    fn request_json_stop_tokens_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_tokens: vec![1, 2],
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("stop_tokens").is_none());
        assert!(json["generationConfig"].get("stop_tokens").is_none());
    }

    // -----------------------------------------------------------------------
    // Response JSON parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_normal_completion() {
        let body = r#"{
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello world"}]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {
                "promptTokenCount": 5,
                "candidatesTokenCount": 2
            }
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "Hello world");
        assert_eq!(resp.stop_reason, StopReason::StopToken);
        assert_eq!(resp.prompt_tokens, 5);
        assert_eq!(resp.completion_tokens, 2);
    }

    #[test]
    fn parse_max_tokens_stop() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "truncated"}]},
                "finishReason": "MAX_TOKENS"
            }],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 256}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::MaxTokens);
    }

    #[test]
    fn parse_safety_stop() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": ""}]},
                "finishReason": "SAFETY"
            }],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 0}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::Cancelled);
    }

    #[test]
    fn parse_multiple_parts_concatenated() {
        let body = r#"{
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Hello "},
                        {"text": "world"}
                    ]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 2}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "Hello world");
    }

    #[test]
    fn parse_empty_candidates_returns_error() {
        let body = r#"{
            "candidates": [],
            "usageMetadata": {"promptTokenCount": 1}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("empty candidates"));
    }

    #[test]
    fn parse_missing_candidates_returns_error() {
        let body = r#"{"usageMetadata": {}}"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("candidates"));
    }

    #[test]
    fn parse_candidate_missing_content_returns_error() {
        let body = r#"{
            "candidates": [{"finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 1}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("content.parts"));
    }

    #[test]
    fn parse_api_error_response() {
        let body = r#"{
            "error": {
                "code": 400,
                "message": "API key not valid. Please pass a valid API key.",
                "status": "INVALID_ARGUMENT"
            }
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("API key not valid"));
        assert!(err.to_string().contains("400"));
    }

    #[test]
    fn parse_missing_usage_defaults_to_zero() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hi"}]},
                "finishReason": "STOP"
            }]
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.prompt_tokens, 0);
        assert_eq!(resp.completion_tokens, 0);
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        let err = parse_response_json("not json").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }

    // -----------------------------------------------------------------------
    // HTTP error mapping
    // -----------------------------------------------------------------------

    #[test]
    fn map_400_mentions_bad_request() {
        let err = map_http_error("Google", ureq::Error::StatusCode(400));
        assert!(err.to_string().contains("bad request"));
    }

    #[test]
    fn map_403_mentions_unauthorized() {
        let err = map_http_error("Google", ureq::Error::StatusCode(403));
        assert!(err.to_string().contains("unauthorized"));
    }

    #[test]
    fn map_429_mentions_rate_limit() {
        let err = map_http_error("Google", ureq::Error::StatusCode(429));
        assert!(err.to_string().contains("rate limited"));
    }

    #[test]
    fn map_error_includes_provider_name() {
        let err = map_http_error("Google", ureq::Error::StatusCode(500));
        assert!(err.to_string().contains("Google"));
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_missing_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hello"}]}
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_null_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hello"}]},
                "finishReason": null
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_unknown_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hello"}]},
                "finishReason": "SOME_FUTURE_REASON"
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_recitation_maps_to_cancelled() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "copied text"}]},
                "finishReason": "RECITATION"
            }],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 3}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::Cancelled);
    }

    #[test]
    fn parse_parts_with_non_text_entries_filtered() {
        // Gemini can return inline_data parts (images etc.) — only text parts
        // should be extracted.
        let body = r#"{
            "candidates": [{
                "content": {
                    "parts": [
                        {"inline_data": {"mime_type": "image/png", "data": "abc"}},
                        {"text": "only this"}
                    ]
                },
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "only this");
    }

    #[test]
    fn parse_api_error_without_code() {
        let body = r#"{
            "error": {
                "message": "Something went wrong"
            }
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("Something went wrong"));
        // Should NOT contain "code" since none was provided
        assert!(!err.to_string().contains("code"), "err: {err}");
    }

    #[test]
    fn debug_redacts_api_key() {
        let p = GoogleProvider::new("AIza-secret-key-123".into(), "gemini-pro".into()).unwrap();
        let dbg = format!("{:?}", p);
        assert!(
            !dbg.contains("AIza-secret-key-123"),
            "API key leaked in Debug output: {dbg}"
        );
        assert!(
            dbg.contains("[REDACTED]"),
            "Debug should show [REDACTED]: {dbg}"
        );
        assert!(
            dbg.contains("gemini-pro"),
            "Debug should show model name: {dbg}"
        );
    }

    #[test]
    fn request_json_prompt_with_special_chars() {
        let req = GenerateRequest {
            prompt: "Hello \"world\" \n\ttab & <html>".into(),
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            json["contents"][0]["parts"][0]["text"],
            "Hello \"world\" \n\ttab & <html>"
        );
    }

    #[test]
    fn generate_max_tokens_zero_returns_error() {
        let provider = GoogleProvider::new("key".into(), "gemini-pro".into()).unwrap();
        let request = GenerateRequest {
            prompt: "test".into(),
            max_tokens: 0,
            ..Default::default()
        };
        let err = provider.generate(&request).unwrap_err();
        assert!(
            err.to_string().contains("max_tokens"),
            "Error should mention max_tokens: {err}"
        );
    }

    #[test]
    fn request_json_temperature_zero_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            temperature: 0.0,
            ..Default::default()
        };
        let json_str = build_request_json(&req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["generationConfig"]["temperature"], 0.0);
    }

    // -----------------------------------------------------------------------
    // Embedding URL building
    // -----------------------------------------------------------------------

    #[test]
    fn embed_url_single_contains_model() {
        let url = build_embed_url("text-embedding-004");
        assert!(url.contains("text-embedding-004"));
        assert!(url.contains("embedContent"));
        assert!(!url.contains("batch"));
    }

    #[test]
    fn batch_embed_url_contains_model() {
        let url = build_batch_embed_url("text-embedding-004");
        assert!(url.contains("text-embedding-004"));
        assert!(url.contains("batchEmbedContents"));
    }

    // -----------------------------------------------------------------------
    // Embedding request JSON building
    // -----------------------------------------------------------------------

    #[test]
    fn embed_request_single_text() {
        let json_str = build_embed_request_json("hello world");
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["content"]["parts"][0]["text"], "hello world");
    }

    #[test]
    fn batch_embed_request_multiple_texts() {
        let texts = &["hello", "world"];
        let json_str = build_batch_embed_request_json("text-embedding-004", texts);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let requests = json["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["model"], "models/text-embedding-004");
        assert_eq!(requests[0]["content"]["parts"][0]["text"], "hello");
        assert_eq!(requests[1]["content"]["parts"][0]["text"], "world");
    }

    #[test]
    fn embed_request_special_chars() {
        let json_str = build_embed_request_json("Hello \"world\" \n\ttab");
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(
            json["content"]["parts"][0]["text"],
            "Hello \"world\" \n\ttab"
        );
    }

    // -----------------------------------------------------------------------
    // Embedding response JSON parsing
    // -----------------------------------------------------------------------

    #[test]
    fn embed_response_single() {
        let body = r#"{
            "embedding": {
                "values": [0.1, 0.2, 0.3]
            }
        }"#;
        let embedding = parse_embed_response_json(body).unwrap();
        assert_eq!(embedding.len(), 3);
        assert!((embedding[0] - 0.1).abs() < 1e-6);
        assert!((embedding[1] - 0.2).abs() < 1e-6);
        assert!((embedding[2] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn batch_embed_response_preserves_order() {
        let body = r#"{
            "embeddings": [
                {"values": [0.1, 0.1]},
                {"values": [0.9, 0.9]}
            ]
        }"#;
        let embeddings = parse_batch_embed_response_json(body).unwrap();
        assert_eq!(embeddings.len(), 2);
        assert!((embeddings[0][0] - 0.1).abs() < 1e-6);
        assert!((embeddings[1][0] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn embed_response_api_error() {
        let body = r#"{
            "error": {
                "code": 400,
                "message": "Invalid model"
            }
        }"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("Invalid model"));
    }

    #[test]
    fn embed_response_missing_embedding_returns_error() {
        let body = r#"{}"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("embedding"));
    }

    #[test]
    fn embed_response_invalid_json() {
        let err = parse_embed_response_json("not json").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }

    #[test]
    fn batch_embed_response_missing_embeddings_returns_error() {
        let body = r#"{}"#;
        let err = parse_batch_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("embeddings"));
    }

    #[test]
    fn batch_embed_response_empty_embeddings_returns_error() {
        let body = r#"{"embeddings": []}"#;
        let err = parse_batch_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn batch_embed_response_invalid_json() {
        let err = parse_batch_embed_response_json("{bad}").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }
}
