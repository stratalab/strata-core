//! OpenAI (GPT) cloud generation provider.
//!
//! Sends generation requests to `https://api.openai.com/v1/chat/completions`
//! and maps the response to [`GenerateResponse`].

use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

const API_URL: &str = "https://api.openai.com/v1/chat/completions";

/// OpenAI cloud provider state.
pub(crate) struct OpenAIProvider {
    api_key: String,
    model: String,
}

impl std::fmt::Debug for OpenAIProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAIProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl OpenAIProvider {
    pub fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
        if api_key.trim().is_empty() {
            return Err(InferenceError::Provider(
                "OpenAI API key is empty".to_string(),
            ));
        }
        if model.trim().is_empty() {
            return Err(InferenceError::Provider(
                "OpenAI model name is empty".to_string(),
            ));
        }
        Ok(Self { api_key, model })
    }

    pub fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        let body = build_request_json(&self.model, request);

        let mut response = ureq::post(API_URL)
            .header("Authorization", &format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .send(&body)
            .map_err(|e| map_http_error("OpenAI", e))?;

        let response_body = response
            .body_mut()
            .read_to_string()
            .map_err(|e| InferenceError::Provider(format!("OpenAI: failed to read response: {e}")))?;

        parse_response_json(&response_body)
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

/// Build the OpenAI Chat Completions API request JSON.
///
/// Silently ignores `top_k` and `stop_tokens` (not supported).
/// Includes `seed` when provided. Omits `top_p` when 1.0 (disabled).
pub(crate) fn build_request_json(model: &str, request: &GenerateRequest) -> String {
    let mut obj = serde_json::json!({
        "model": model,
        "max_tokens": request.max_tokens,
        "messages": [
            {
                "role": "user",
                "content": request.prompt
            }
        ]
    });

    // Include temperature (OpenAI accepts 0.0)
    obj["temperature"] = serde_json::json!(request.temperature);

    // Only include top_p if not the default
    if request.top_p < 1.0 {
        obj["top_p"] = serde_json::json!(request.top_p);
    }

    // OpenAI supports seed
    if let Some(seed) = request.seed {
        obj["seed"] = serde_json::json!(seed);
    }

    // Include stop sequences if non-empty
    if !request.stop_sequences.is_empty() {
        obj["stop"] = serde_json::json!(request.stop_sequences);
    }

    // top_k: silently ignored (not supported by OpenAI)
    // stop_tokens: silently ignored (token-level, local only)

    obj.to_string()
}

/// Parse the OpenAI Chat Completions API response JSON into a `GenerateResponse`.
pub(crate) fn parse_response_json(body: &str) -> Result<GenerateResponse, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        return Err(InferenceError::Provider(format!("OpenAI API error: {msg}")));
    }

    // Extract from choices array
    let choices = json
        .get("choices")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("OpenAI: missing or invalid 'choices' array".to_string())
        })?;

    if choices.is_empty() {
        return Err(InferenceError::Provider(
            "OpenAI: empty choices array in response".to_string(),
        ));
    }

    let choice = &choices[0];

    // Extract text
    let text = choice
        .get("message")
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();

    // Map finish_reason
    let stop_reason = match choice.get("finish_reason").and_then(|r| r.as_str()) {
        Some("stop") => StopReason::StopToken,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::Cancelled,
        _ => StopReason::StopToken, // default fallback
    };

    // Extract usage
    let usage = json.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("prompt_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let completion_tokens = usage
        .and_then(|u| u.get("completion_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    Ok(GenerateResponse {
        text,
        stop_reason,
        prompt_tokens,
        completion_tokens,
    })
}

/// Map ureq HTTP errors to InferenceError::Provider with descriptive messages.
fn map_http_error(provider: &str, err: ureq::Error) -> InferenceError {
    match &err {
        ureq::Error::StatusCode(status) => {
            let code = *status;
            let description = match code {
                401 => "invalid API key",
                403 => "forbidden (check API key permissions)",
                429 => "rate limited (too many requests)",
                500 => "server error",
                502 => "bad gateway",
                503 => "service unavailable",
                _ => "HTTP error",
            };
            InferenceError::Provider(format!(
                "{provider}: {description} (HTTP {code})"
            ))
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
        let p = OpenAIProvider::new("sk-test-key".into(), "gpt-4".into());
        assert!(p.is_ok());
        assert_eq!(p.unwrap().model(), "gpt-4");
    }

    #[test]
    fn new_with_empty_key_returns_error() {
        let p = OpenAIProvider::new("".into(), "gpt-4".into());
        assert!(p.is_err());
        assert!(p.unwrap_err().to_string().contains("key"));
    }

    #[test]
    fn new_with_whitespace_key_returns_error() {
        let p = OpenAIProvider::new("  \t".into(), "gpt-4".into());
        assert!(p.is_err());
    }

    #[test]
    fn new_with_empty_model_returns_error() {
        let p = OpenAIProvider::new("sk-test".into(), "".into());
        assert!(p.is_err());
        assert!(p.unwrap_err().to_string().contains("model"));
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
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["model"], "gpt-4");
        assert_eq!(json["max_tokens"], 100);
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["content"], "Hello");
    }

    #[test]
    fn request_json_temperature_always_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            temperature: 0.0,
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // OpenAI always gets temperature (even 0.0)
        assert_eq!(json["temperature"], 0.0);
    }

    #[test]
    fn request_json_top_p_default_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 1.0,
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("top_p").is_none());
    }

    #[test]
    fn request_json_top_p_custom_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 0.95,
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let top_p = json["top_p"].as_f64().unwrap();
        assert!((top_p - 0.95).abs() < 0.01);
    }

    #[test]
    fn request_json_seed_included_when_present() {
        let req = GenerateRequest {
            prompt: "test".into(),
            seed: Some(42),
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["seed"], 42);
    }

    #[test]
    fn request_json_seed_omitted_when_none() {
        let req = GenerateRequest {
            prompt: "test".into(),
            seed: None,
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("seed").is_none());
    }

    #[test]
    fn request_json_top_k_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_k: 40,
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("top_k").is_none());
    }

    #[test]
    fn request_json_stop_sequences_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_sequences: vec!["STOP".into()],
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let stop = json["stop"].as_array().unwrap();
        assert_eq!(stop.len(), 1);
        assert_eq!(stop[0], "STOP");
    }

    #[test]
    fn request_json_stop_sequences_empty_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("stop").is_none());
    }

    // -----------------------------------------------------------------------
    // Response JSON parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_normal_completion() {
        let body = r#"{
            "choices": [{
                "message": {"content": "Hello world"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "Hello world");
        assert_eq!(resp.stop_reason, StopReason::StopToken);
        assert_eq!(resp.prompt_tokens, 5);
        assert_eq!(resp.completion_tokens, 2);
    }

    #[test]
    fn parse_length_stop() {
        let body = r#"{
            "choices": [{
                "message": {"content": "truncated"},
                "finish_reason": "length"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 256}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::MaxTokens);
    }

    #[test]
    fn parse_content_filter() {
        let body = r#"{
            "choices": [{
                "message": {"content": ""},
                "finish_reason": "content_filter"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 0}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::Cancelled);
    }

    #[test]
    fn parse_empty_choices_returns_error() {
        let body = r#"{
            "choices": [],
            "usage": {"prompt_tokens": 1, "completion_tokens": 0}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("empty choices"));
    }

    #[test]
    fn parse_missing_choices_returns_error() {
        let body = r#"{"usage": {"prompt_tokens": 1}}"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("choices"));
    }

    #[test]
    fn parse_api_error_response() {
        let body = r#"{
            "error": {
                "message": "Incorrect API key provided",
                "type": "invalid_request_error"
            }
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("Incorrect API key"));
    }

    #[test]
    fn parse_null_content_returns_empty_string() {
        let body = r#"{
            "choices": [{
                "message": {"content": null},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 0}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "");
    }

    #[test]
    fn parse_missing_usage_defaults_to_zero() {
        let body = r#"{
            "choices": [{
                "message": {"content": "hi"},
                "finish_reason": "stop"
            }]
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.prompt_tokens, 0);
        assert_eq!(resp.completion_tokens, 0);
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        let err = parse_response_json("{not json}").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }

    // -----------------------------------------------------------------------
    // HTTP error mapping
    // -----------------------------------------------------------------------

    #[test]
    fn map_401_mentions_api_key() {
        let err = map_http_error("OpenAI", ureq::Error::StatusCode(401));
        assert!(err.to_string().contains("invalid API key"));
        assert!(err.to_string().contains("401"));
    }

    #[test]
    fn map_429_mentions_rate_limit() {
        let err = map_http_error("OpenAI", ureq::Error::StatusCode(429));
        assert!(err.to_string().contains("rate limited"));
    }

    #[test]
    fn map_error_includes_provider_name() {
        let err = map_http_error("OpenAI", ureq::Error::StatusCode(500));
        assert!(err.to_string().contains("OpenAI"));
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_missing_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "choices": [{
                "message": {"content": "hello"}
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_null_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": null
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_unknown_finish_reason_defaults_to_stop_token() {
        let body = r#"{
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": "some_future_reason"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_missing_message_content_returns_empty_string() {
        let body = r#"{
            "choices": [{
                "message": {},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 0}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "");
    }

    #[test]
    fn debug_redacts_api_key() {
        let p = OpenAIProvider::new("sk-secret-key-123".into(), "gpt-4".into()).unwrap();
        let dbg = format!("{:?}", p);
        assert!(!dbg.contains("sk-secret-key-123"), "API key leaked in Debug output: {dbg}");
        assert!(dbg.contains("[REDACTED]"), "Debug should show [REDACTED]: {dbg}");
        assert!(dbg.contains("gpt-4"), "Debug should show model name: {dbg}");
    }

    #[test]
    fn request_json_prompt_with_special_chars() {
        let req = GenerateRequest {
            prompt: "Hello \"world\" \n\ttab & <html>".into(),
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            json["messages"][0]["content"],
            "Hello \"world\" \n\ttab & <html>"
        );
    }

    #[test]
    fn request_json_stop_tokens_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_tokens: vec![1, 2, 3],
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("stop_tokens").is_none());
    }
}
