//! Anthropic (Claude) cloud generation provider.
//!
//! Sends generation requests to `https://api.anthropic.com/v1/messages` and
//! maps the response to [`GenerateResponse`].

use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";

/// Anthropic cloud provider state.
pub(crate) struct AnthropicProvider {
    api_key: String,
    model: String,
}

impl std::fmt::Debug for AnthropicProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl AnthropicProvider {
    pub fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
        if api_key.trim().is_empty() {
            return Err(InferenceError::Provider(
                "Anthropic API key is empty".to_string(),
            ));
        }
        if model.trim().is_empty() {
            return Err(InferenceError::Provider(
                "Anthropic model name is empty".to_string(),
            ));
        }
        Ok(Self { api_key, model })
    }

    pub fn generate(&self, request: &GenerateRequest) -> Result<GenerateResponse, InferenceError> {
        let body = build_request_json(&self.model, request);

        let mut response = ureq::post(API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .send(&body)
            .map_err(|e| map_http_error("Anthropic", e))?;

        let response_body = response
            .body_mut()
            .read_to_string()
            .map_err(|e| InferenceError::Provider(format!("Anthropic: failed to read response: {e}")))?;

        parse_response_json(&response_body)
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

/// Build the Anthropic Messages API request JSON.
///
/// Silently ignores `top_k`, `seed`, `stop_tokens` (not supported by this API).
/// Omits `top_p` when it equals 1.0 (disabled).
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

    // Always include temperature — Anthropic defaults to 1.0 when omitted,
    // which would break greedy decoding (temperature=0.0).
    obj["temperature"] = serde_json::json!(request.temperature);

    // Only include top_p if it's not the default (disabled) value
    if request.top_p < 1.0 {
        obj["top_p"] = serde_json::json!(request.top_p);
    }

    // Include stop_sequences if non-empty
    if !request.stop_sequences.is_empty() {
        obj["stop_sequences"] = serde_json::json!(request.stop_sequences);
    }

    // top_k: silently ignored (not supported)
    // seed: silently ignored (not supported)
    // stop_tokens: silently ignored (token-level, local only)

    obj.to_string()
}

/// Parse the Anthropic Messages API response JSON into a `GenerateResponse`.
pub(crate) fn parse_response_json(body: &str) -> Result<GenerateResponse, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("Anthropic: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(err_type) = json.get("type").and_then(|v| v.as_str()) {
        if err_type == "error" {
            let msg = json
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            return Err(InferenceError::Provider(format!("Anthropic API error: {msg}")));
        }
    }

    // Extract text from content array
    let content = json
        .get("content")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("Anthropic: missing or invalid 'content' array".to_string())
        })?;

    if content.is_empty() {
        return Err(InferenceError::Provider(
            "Anthropic: empty content array in response".to_string(),
        ));
    }

    let text = content
        .iter()
        .filter_map(|block| {
            if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                block.get("text").and_then(|t| t.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("");

    // Map stop_reason
    let stop_reason = match json.get("stop_reason").and_then(|s| s.as_str()) {
        Some("end_turn") => StopReason::StopToken,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("stop_sequence") => StopReason::StopToken,
        _ => StopReason::StopToken, // default fallback
    };

    // Extract usage
    let usage = json.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let completion_tokens = usage
        .and_then(|u| u.get("output_tokens"))
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
        let p = AnthropicProvider::new("sk-ant-test".into(), "claude-sonnet-4-20250514".into());
        assert!(p.is_ok());
        assert_eq!(p.unwrap().model(), "claude-sonnet-4-20250514");
    }

    #[test]
    fn new_with_empty_key_returns_error() {
        let p = AnthropicProvider::new("".into(), "claude-sonnet-4-20250514".into());
        assert!(p.is_err());
        let err = p.unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)));
        assert!(err.to_string().contains("key"), "error: {err}");
    }

    #[test]
    fn new_with_whitespace_key_returns_error() {
        let p = AnthropicProvider::new("  \t  ".into(), "model".into());
        assert!(p.is_err());
    }

    #[test]
    fn new_with_empty_model_returns_error() {
        let p = AnthropicProvider::new("sk-test".into(), "".into());
        assert!(p.is_err());
        assert!(p.unwrap_err().to_string().contains("model"));
    }

    #[test]
    fn new_with_whitespace_model_returns_error() {
        let p = AnthropicProvider::new("sk-test".into(), "   ".into());
        assert!(p.is_err());
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
        let json_str = build_request_json("claude-sonnet-4-20250514", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["model"], "claude-sonnet-4-20250514");
        assert_eq!(json["max_tokens"], 100);
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["content"], "Hello");
    }

    #[test]
    fn request_json_temperature_zero_included() {
        // Temperature must always be sent — Anthropic defaults to 1.0 when
        // omitted, which would silently break greedy decoding.
        let req = GenerateRequest {
            prompt: "test".into(),
            temperature: 0.0,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["temperature"], 0.0);
    }

    #[test]
    fn request_json_temperature_nonzero_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            temperature: 0.7,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let temp = json["temperature"].as_f64().unwrap();
        assert!((temp - 0.7).abs() < 0.01);
    }

    #[test]
    fn request_json_top_p_default_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 1.0,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("top_p").is_none());
    }

    #[test]
    fn request_json_top_p_custom_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 0.9,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let top_p = json["top_p"].as_f64().unwrap();
        assert!((top_p - 0.9).abs() < 0.01);
    }

    #[test]
    fn request_json_top_k_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            top_k: 40,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("top_k").is_none(), "top_k should be omitted");
    }

    #[test]
    fn request_json_seed_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            seed: Some(42),
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("seed").is_none(), "seed should be omitted");
    }

    #[test]
    fn request_json_stop_sequences_included() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_sequences: vec!["STOP".into(), "\n\n".into()],
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        let seqs = json["stop_sequences"].as_array().unwrap();
        assert_eq!(seqs.len(), 2);
        assert_eq!(seqs[0], "STOP");
        assert_eq!(seqs[1], "\n\n");
    }

    #[test]
    fn request_json_stop_sequences_empty_omitted() {
        let req = GenerateRequest {
            prompt: "test".into(),
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("stop_sequences").is_none());
    }

    #[test]
    fn request_json_stop_tokens_silently_ignored() {
        let req = GenerateRequest {
            prompt: "test".into(),
            stop_tokens: vec![1, 2, 3],
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("stop_tokens").is_none());
    }

    #[test]
    fn request_json_prompt_with_special_chars() {
        let req = GenerateRequest {
            prompt: "Hello \"world\" \n\ttab & <html>".into(),
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(
            json["messages"][0]["content"],
            "Hello \"world\" \n\ttab & <html>"
        );
    }

    // -----------------------------------------------------------------------
    // Response JSON parsing
    // -----------------------------------------------------------------------

    #[test]
    fn parse_normal_completion() {
        let body = r#"{
            "content": [{"type": "text", "text": "Hello world"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 5, "output_tokens": 2}
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
            "content": [{"type": "text", "text": "truncated"}],
            "stop_reason": "max_tokens",
            "usage": {"input_tokens": 10, "output_tokens": 256}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::MaxTokens);
        assert_eq!(resp.completion_tokens, 256);
    }

    #[test]
    fn parse_stop_sequence_reason() {
        let body = r#"{
            "content": [{"type": "text", "text": "before stop"}],
            "stop_reason": "stop_sequence",
            "usage": {"input_tokens": 3, "output_tokens": 5}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_multiple_content_blocks() {
        let body = r#"{
            "content": [
                {"type": "text", "text": "Hello "},
                {"type": "text", "text": "world"}
            ],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 2}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "Hello world");
    }

    #[test]
    fn parse_empty_content_array_returns_error() {
        let body = r#"{
            "content": [],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 0}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("empty content"));
    }

    #[test]
    fn parse_missing_content_returns_error() {
        let body = r#"{"stop_reason": "end_turn"}"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("content"));
    }

    #[test]
    fn parse_api_error_response() {
        let body = r#"{
            "type": "error",
            "error": {
                "type": "authentication_error",
                "message": "invalid x-api-key"
            }
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("invalid x-api-key"));
    }

    #[test]
    fn parse_missing_usage_defaults_to_zero() {
        let body = r#"{
            "content": [{"type": "text", "text": "hi"}],
            "stop_reason": "end_turn"
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.prompt_tokens, 0);
        assert_eq!(resp.completion_tokens, 0);
    }

    #[test]
    fn parse_invalid_json_returns_error() {
        let err = parse_response_json("not json at all").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }

    #[test]
    fn parse_content_with_non_text_blocks_filtered() {
        let body = r#"{
            "content": [
                {"type": "tool_use", "id": "x"},
                {"type": "text", "text": "only this"}
            ],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "only this");
    }

    // -----------------------------------------------------------------------
    // HTTP error mapping
    // -----------------------------------------------------------------------

    #[test]
    fn map_401_mentions_api_key() {
        let err = map_http_error("Anthropic", ureq::Error::StatusCode(401));
        assert!(err.to_string().contains("invalid API key"));
        assert!(err.to_string().contains("401"));
    }

    #[test]
    fn map_429_mentions_rate_limit() {
        let err = map_http_error("Anthropic", ureq::Error::StatusCode(429));
        assert!(err.to_string().contains("rate limited"));
    }

    #[test]
    fn map_500_mentions_server_error() {
        let err = map_http_error("Anthropic", ureq::Error::StatusCode(500));
        assert!(err.to_string().contains("server error"));
    }

    #[test]
    fn map_error_includes_provider_name() {
        let err = map_http_error("Anthropic", ureq::Error::StatusCode(503));
        assert!(err.to_string().contains("Anthropic"));
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_all_non_text_blocks_returns_empty_string() {
        // When response only contains tool_use blocks (no text), we should
        // get an empty string, not an error.
        let body = r#"{
            "content": [
                {"type": "tool_use", "id": "x", "name": "get_weather", "input": {}},
                {"type": "tool_use", "id": "y", "name": "search", "input": {}}
            ],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 5, "output_tokens": 10}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.text, "");
    }

    #[test]
    fn parse_missing_stop_reason_defaults_to_stop_token() {
        let body = r#"{
            "content": [{"type": "text", "text": "hello"}],
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_null_stop_reason_defaults_to_stop_token() {
        let body = r#"{
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": null,
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn parse_unknown_stop_reason_defaults_to_stop_token() {
        let body = r#"{
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": "some_future_reason",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_response_json(body).unwrap();
        assert_eq!(resp.stop_reason, StopReason::StopToken);
    }

    #[test]
    fn debug_redacts_api_key() {
        let p = AnthropicProvider::new("sk-ant-secret-key-123".into(), "claude-sonnet-4-20250514".into()).unwrap();
        let dbg = format!("{:?}", p);
        assert!(!dbg.contains("sk-ant-secret-key-123"), "API key leaked in Debug output: {dbg}");
        assert!(dbg.contains("[REDACTED]"), "Debug should show [REDACTED]: {dbg}");
        assert!(dbg.contains("claude-sonnet-4-20250514"), "Debug should show model name: {dbg}");
    }
}
