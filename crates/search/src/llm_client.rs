//! Shared LLM client infrastructure for expand and rerank modules
//!
//! Provides a unified error type, HTTP call helper, and retry logic
//! to avoid duplication between ApiExpander and ApiReranker.

use std::fmt;

// ============================================================================
// Unified Error Type
// ============================================================================

/// Errors that can occur when calling an external LLM endpoint
#[non_exhaustive]
#[derive(Debug)]
pub enum LlmClientError {
    /// HTTP request failed (network unreachable, connection refused, etc.)
    Network(String),
    /// Failed to parse model response
    Parse(String),
    /// Model request timed out
    Timeout,
    /// Required cargo feature (expand/rerank) is not enabled
    FeatureDisabled(&'static str),
}

impl fmt::Display for LlmClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LlmClientError::Network(msg) => write!(f, "network error: {}", msg),
            LlmClientError::Parse(msg) => write!(f, "parse error: {}", msg),
            LlmClientError::Timeout => write!(f, "model request timed out"),
            LlmClientError::FeatureDisabled(feat) => {
                write!(f, "feature '{}' not enabled", feat)
            }
        }
    }
}

impl std::error::Error for LlmClientError {}

// ============================================================================
// Shared HTTP Client
// ============================================================================

/// Call an OpenAI-compatible chat completions endpoint and extract the response content.
///
/// Handles:
/// - ureq agent construction with timeout
/// - Bearer token auth header
/// - Sending the request body
/// - Parsing `choices[0].message.content` from the response
///
/// Returns the extracted content string on success.
#[cfg(any(feature = "expand", feature = "rerank"))]
pub fn call_chat_completions(
    url: &str,
    api_key: Option<&str>,
    timeout: std::time::Duration,
    body: &serde_json::Value,
) -> Result<String, LlmClientError> {
    let body_bytes = serde_json::to_vec(body)
        .map_err(|e| LlmClientError::Parse(format!("failed to serialize request: {}", e)))?;

    let config = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .build();
    let agent = ureq::Agent::new_with_config(config);

    let mut request = agent.post(url).header("Content-Type", "application/json");

    if let Some(key) = api_key {
        request = request.header("Authorization", &format!("Bearer {}", key));
    }

    let mut response = request.send(&body_bytes[..]).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("timed out") || msg.contains("Timeout") {
            LlmClientError::Timeout
        } else {
            LlmClientError::Network(msg)
        }
    })?;

    let response_text = response
        .body_mut()
        .read_to_string()
        .map_err(|e| LlmClientError::Network(format!("failed to read response: {}", e)))?;

    let json: serde_json::Value = serde_json::from_str(&response_text)
        .map_err(|e| LlmClientError::Parse(format!("invalid JSON response: {}", e)))?;

    let content = json
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| {
            LlmClientError::Parse(format!(
                "unexpected response format: {}",
                &response_text[..response_text.len().min(200)]
            ))
        })?;

    Ok(content.to_string())
}

// ============================================================================
// Retry Helper
// ============================================================================

/// Execute an LLM call with a single retry on failure or empty results.
///
/// 1. Calls `call_fn()` to get raw text
/// 2. Calls `parse_fn()` to parse it
/// 3. If parsing succeeds but `is_empty_fn()` returns true, retries once
/// 4. If the call itself fails, retries once
///
/// `operation` is a label for tracing messages (e.g. "expand" or "rerank").
pub fn retry_once<T>(
    call_fn: impl Fn() -> Result<String, LlmClientError>,
    parse_fn: impl Fn(&str) -> T,
    is_empty_fn: impl Fn(&T) -> bool,
    on_empty_err: impl Fn() -> LlmClientError,
    operation: &str,
) -> Result<T, LlmClientError> {
    // First attempt
    match call_fn() {
        Ok(text) => {
            let result = parse_fn(&text);
            if !is_empty_fn(&result) {
                return Ok(result);
            }
            tracing::warn!(
                target: "strata::llm_client",
                op = operation,
                "First call returned no valid results, retrying"
            );
        }
        Err(e) => {
            tracing::warn!(
                target: "strata::llm_client",
                op = operation,
                error = %e,
                "First call failed, retrying"
            );
        }
    }

    // Retry once
    match call_fn() {
        Ok(text) => {
            let result = parse_fn(&text);
            if is_empty_fn(&result) {
                tracing::warn!(
                    target: "strata::llm_client",
                    op = operation,
                    "Retry also returned no valid results, falling back"
                );
                Err(on_empty_err())
            } else {
                Ok(result)
            }
        }
        Err(e) => {
            tracing::warn!(
                target: "strata::llm_client",
                op = operation,
                error = %e,
                "Retry also failed, falling back"
            );
            Err(e)
        }
    }
}
