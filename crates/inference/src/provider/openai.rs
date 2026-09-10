//! OpenAI (GPT) cloud generation provider.
//!
//! Sends generation requests to `https://api.openai.com/v1/chat/completions`
//! and maps the response to [`GenerateResponse`].

use crate::provider::cloud::{post_json, reject_local_only, CloudPost};
use crate::wire::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, FinishReason, LogProbs, ResponseFormat,
    Role, TokenLogProb, ToolCall, ToolCallFunction, TopLogProb, Usage,
};
use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

/// The API root every OpenAI endpoint hangs off when nothing overrides it.
pub(crate) const API_BASE: &str = crate::settings::OPENAI_BASE_URL;

/// The chat-completions endpoint under `api_base`.
fn chat_url(api_base: &str) -> String {
    format!("{api_base}/chat/completions")
}

/// The embeddings endpoint under `api_base`.
pub(crate) fn embed_url(api_base: &str) -> String {
    format!("{api_base}/embeddings")
}

/// OpenAI cloud provider state.
pub(crate) struct OpenAIProvider {
    api_key: String,
    model: String,
    /// Where requests go: [`API_BASE`], or the base URL the runtime's
    /// settings override it with.
    api_base: String,
}

impl std::fmt::Debug for OpenAIProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAIProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl OpenAIProvider {
    pub(crate) fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
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
        Ok(Self {
            api_key,
            model,
            api_base: API_BASE.to_string(),
        })
    }

    /// Point requests at `base_url` instead of the public API: an
    /// OpenAI-compatible server, a proxy, or a test's canned-response server.
    pub(crate) fn with_base_url(mut self, base_url: &str) -> Self {
        self.api_base = base_url.to_string();
        self
    }

    pub(crate) fn generate(
        &self,
        request: &GenerateRequest,
    ) -> Result<GenerateResponse, InferenceError> {
        if request.max_tokens == 0 {
            return Err(InferenceError::Provider(
                "max_tokens must be greater than 0".to_string(),
            ));
        }

        let body = build_request_json(&self.model, request);
        self.post(body)
    }

    /// Generate from an OpenAI-shaped chat request, mapping messages natively
    /// (system/user/assistant/tool turns preserved), forwarding supported
    /// sampling knobs, tools/tool_choice, structured-output `response_format`,
    /// and `logprobs`, and parsing tool calls + log-probabilities back into the
    /// [`ChatResponse`].
    pub(crate) fn generate_chat(
        &self,
        request: &ChatRequest,
    ) -> Result<ChatResponse, InferenceError> {
        let body = build_chat_request_json(&self.model, request)?;
        let response_body = self.send(body)?;
        parse_chat_response_json(&response_body, &self.model)
    }

    /// Send a prepared completion request body and parse the response.
    fn post(&self, body: String) -> Result<GenerateResponse, InferenceError> {
        let response_body = self.send(body)?;
        parse_response_json(&response_body)
    }

    /// POST a prepared JSON body to the chat-completions endpoint and return the
    /// raw response body (shared by the completion and chat parsers).
    fn send(&self, body: String) -> Result<String, InferenceError> {
        post_json(&CloudPost {
            provider: "OpenAI",
            url: &chat_url(&self.api_base),
            headers: &[("Authorization", &format!("Bearer {}", self.api_key))],
            body: &body,
            timeout: std::time::Duration::from_secs(30),
        })
    }

    pub(crate) fn model(&self) -> &str {
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

    // Enable JSON mode when grammar is specified
    if request.grammar.is_some() {
        obj["response_format"] = serde_json::json!({"type": "json_object"});
    }

    // top_k: silently ignored (not supported by OpenAI)
    // stop_tokens: silently ignored (token-level, local only)
    // grammar: mapped to response_format above (GBNF string itself is not sent)

    obj.to_string()
}

/// Build the OpenAI Chat Completions API request JSON from an OpenAI-shaped
/// chat request.
///
/// Turns map directly to `messages` (roles serialize to their wire strings).
/// Every knob OpenAI honors — `max_tokens`, `temperature`, `top_p`, `seed`,
/// `frequency_penalty`, `presence_penalty`, `logit_bias`, `stop`,
/// `response_format` — is forwarded when set. `top_k` and the llama.cpp
/// extensions have no OpenAI equivalent and are ignored.
pub(crate) fn build_chat_request_json(
    model: &str,
    request: &ChatRequest,
) -> Result<String, InferenceError> {
    reject_local_only(request, "OpenAI")?;

    // The wire `ChatMessage` is already OpenAI-shaped (snake_case roles;
    // `tool_calls` / `tool_call_id` serialize to OpenAI's exact format), so map
    // each message directly — this preserves multi-turn tool calls and tool
    // results, which the lossy `(role, content)` projection dropped. A raw
    // `prompt` becomes a single user turn.
    let messages: Vec<serde_json::Value> = if let Some(prompt) = &request.prompt {
        vec![serde_json::json!({ "role": "user", "content": prompt })]
    } else {
        request
            .messages
            .iter()
            .flatten()
            .map(|message| {
                serde_json::to_value(message)
                    .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid message: {e}")))
            })
            .collect::<Result<_, _>>()?
    };

    let mut obj = serde_json::json!({
        "model": model,
        "messages": messages,
    });

    if let Some(max_tokens) = request.max_tokens {
        obj["max_tokens"] = serde_json::json!(max_tokens);
    }
    if let Some(temperature) = request.temperature {
        obj["temperature"] = serde_json::json!(temperature);
    }
    if let Some(top_p) = request.top_p {
        obj["top_p"] = serde_json::json!(top_p);
    }
    if let Some(seed) = request.seed {
        obj["seed"] = serde_json::json!(seed);
    }
    if let Some(frequency_penalty) = request.frequency_penalty {
        obj["frequency_penalty"] = serde_json::json!(frequency_penalty);
    }
    if let Some(presence_penalty) = request.presence_penalty {
        obj["presence_penalty"] = serde_json::json!(presence_penalty);
    }
    if let Some(logit_bias) = &request.logit_bias {
        obj["logit_bias"] = serde_json::json!(logit_bias);
    }
    if let Some(stop) = &request.stop {
        if !stop.is_empty() {
            obj["stop"] = serde_json::json!(stop);
        }
    }

    // response_format: text (omitted) | json_object | json_schema.
    match &request.response_format {
        Some(ResponseFormat::JsonObject) => {
            obj["response_format"] = serde_json::json!({ "type": "json_object" });
        }
        Some(ResponseFormat::JsonSchema { json_schema }) => {
            let mut spec = serde_json::json!({
                "name": json_schema.name,
                "schema": json_schema.schema,
            });
            if let Some(description) = &json_schema.description {
                spec["description"] = serde_json::json!(description);
            }
            if let Some(strict) = json_schema.strict {
                spec["strict"] = serde_json::json!(strict);
            }
            obj["response_format"] = serde_json::json!({
                "type": "json_schema",
                "json_schema": spec,
            });
        }
        Some(ResponseFormat::Text) | None => {}
    }

    // tools / tool_choice: the wire DTOs are already OpenAI-shaped, so they
    // serialize straight through.
    if let Some(tools) = &request.tools {
        if !tools.is_empty() {
            obj["tools"] = serde_json::to_value(tools)
                .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid tools: {e}")))?;
        }
    }
    if let Some(tool_choice) = &request.tool_choice {
        obj["tool_choice"] = serde_json::to_value(tool_choice)
            .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid tool_choice: {e}")))?;
    }

    // logprobs (+ optional per-token alternatives).
    if request.logprobs == Some(true) {
        obj["logprobs"] = serde_json::json!(true);
        if let Some(top_logprobs) = request.top_logprobs {
            obj["top_logprobs"] = serde_json::json!(top_logprobs);
        }
    }

    Ok(obj.to_string())
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
        .ok_or_else(|| {
            InferenceError::Provider("OpenAI: response missing message content".to_string())
        })?
        .to_string();

    // Map finish_reason
    let stop_reason = match choice.get("finish_reason").and_then(|r| r.as_str()) {
        Some("stop") => StopReason::StopToken,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::Cancelled,
        Some(other) => {
            tracing::warn!(reason = ?other, "Unknown stop reason from OpenAI, defaulting to StopToken");
            StopReason::StopToken
        }
        None => StopReason::StopToken,
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

/// Parse the OpenAI Chat Completions response JSON into a rich [`ChatResponse`]
/// (content, tool calls, finish reason, log-probabilities, and usage).
pub(crate) fn parse_chat_response_json(
    body: &str,
    fallback_model: &str,
) -> Result<ChatResponse, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid JSON response: {e}")))?;

    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        return Err(InferenceError::Provider(format!("OpenAI API error: {msg}")));
    }

    let choices = json
        .get("choices")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("OpenAI: missing or invalid 'choices' array".to_string())
        })?;
    let choice = choices.first().ok_or_else(|| {
        InferenceError::Provider("OpenAI: empty choices array in response".to_string())
    })?;

    let message = choice
        .get("message")
        .ok_or_else(|| InferenceError::Provider("OpenAI: choice missing 'message'".to_string()))?;
    let content = message
        .get("content")
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();
    let tool_calls = parse_tool_calls(message.get("tool_calls"));

    // A choice must carry either content or tool calls.
    if content.is_empty() && tool_calls.is_none() {
        return Err(InferenceError::Provider(
            "OpenAI: response missing message content".to_string(),
        ));
    }

    let finish_reason = match choice.get("finish_reason").and_then(|r| r.as_str()) {
        Some("stop") => FinishReason::Stop,
        Some("length") => FinishReason::Length,
        Some("tool_calls") => FinishReason::ToolCalls,
        Some("content_filter") => FinishReason::ContentFilter,
        Some(other) => {
            tracing::warn!(reason = ?other, "Unknown finish_reason from OpenAI, defaulting to Stop");
            FinishReason::Stop
        }
        None if tool_calls.is_some() => FinishReason::ToolCalls,
        None => FinishReason::Stop,
    };

    let logprobs = parse_logprobs(choice.get("logprobs"));

    let usage = json.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("prompt_tokens"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;
    let completion_tokens = usage
        .and_then(|u| u.get("completion_tokens"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;

    let model = json
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or(fallback_model)
        .to_string();

    Ok(ChatResponse {
        model,
        choices: vec![ChatChoice {
            index: 0,
            message: ChatMessage {
                role: Role::Assistant,
                content,
                name: None,
                tool_calls,
                tool_call_id: None,
            },
            finish_reason,
            logprobs,
        }],
        usage: Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        },
    })
}

/// Parse an OpenAI `tool_calls` array into [`ToolCall`]s (None when absent/empty).
fn parse_tool_calls(value: Option<&serde_json::Value>) -> Option<Vec<ToolCall>> {
    let array = value?.as_array()?;
    let calls: Vec<ToolCall> = array
        .iter()
        .filter_map(|call| {
            let id = call.get("id").and_then(|i| i.as_str())?.to_string();
            let function = call.get("function")?;
            let name = function.get("name").and_then(|n| n.as_str())?.to_string();
            let arguments = function
                .get("arguments")
                .and_then(|a| a.as_str())
                .unwrap_or("")
                .to_string();
            Some(ToolCall::Function {
                id,
                function: ToolCallFunction { name, arguments },
            })
        })
        .collect();
    (!calls.is_empty()).then_some(calls)
}

/// Parse an OpenAI chat `logprobs` object into [`LogProbs`] (None when absent).
fn parse_logprobs(value: Option<&serde_json::Value>) -> Option<LogProbs> {
    let content = value?.get("content")?.as_array()?;
    let tokens = content
        .iter()
        .map(|entry| TokenLogProb {
            token: token_str(entry),
            logprob: logprob_f32(entry),
            bytes: parse_token_bytes(entry.get("bytes")),
            top_logprobs: entry
                .get("top_logprobs")
                .and_then(|t| t.as_array())
                .map(|alts| {
                    alts.iter()
                        .map(|alt| TopLogProb {
                            token: token_str(alt),
                            logprob: logprob_f32(alt),
                            bytes: parse_token_bytes(alt.get("bytes")),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
        .collect();
    Some(LogProbs { content: tokens })
}

fn token_str(entry: &serde_json::Value) -> String {
    entry
        .get("token")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string()
}

fn logprob_f32(entry: &serde_json::Value) -> f32 {
    entry
        .get("logprob")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0) as f32
}

fn parse_token_bytes(value: Option<&serde_json::Value>) -> Option<Vec<u8>> {
    let array = value?.as_array()?;
    Some(
        array
            .iter()
            .filter_map(|b| b.as_u64().map(|n| n as u8))
            .collect(),
    )
}

// =========================================================================
// Embedding API
// =========================================================================

/// Build the OpenAI Embeddings API request JSON.
///
/// Uses the array `input` format for native batch support.
pub(crate) fn build_embed_request_json(model: &str, texts: &[&str]) -> String {
    serde_json::json!({
        "model": model,
        "input": texts
    })
    .to_string()
}

/// Parse the OpenAI Embeddings API response JSON into embedding vectors.
///
/// Returns embeddings sorted by `index` (OpenAI may return out of order).
pub(crate) fn parse_embed_response_json(body: &str) -> Result<Vec<Vec<f32>>, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("OpenAI: invalid JSON response: {e}")))?;

    // Check for API error response
    if let Some(error) = json.get("error") {
        let msg = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        return Err(InferenceError::Provider(format!(
            "OpenAI embedding API error: {msg}"
        )));
    }

    let data = json.get("data").and_then(|d| d.as_array()).ok_or_else(|| {
        InferenceError::Provider(
            "OpenAI: missing or invalid 'data' array in embedding response".to_string(),
        )
    })?;

    if data.is_empty() {
        return Err(InferenceError::Provider(
            "OpenAI: empty data array in embedding response".to_string(),
        ));
    }

    // Collect (index, embedding) pairs and sort by index
    let mut indexed: Vec<(usize, Vec<f32>)> = data
        .iter()
        .map(|item| {
            let index = item.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as usize;
            let embedding = item
                .get("embedding")
                .and_then(|e| e.as_array())
                .ok_or_else(|| {
                    InferenceError::Provider(
                        "OpenAI: data item missing 'embedding' array".to_string(),
                    )
                })?
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect();
            Ok((index, embedding))
        })
        .collect::<Result<Vec<_>, InferenceError>>()?;

    indexed.sort_by_key(|(idx, _)| *idx);

    Ok(indexed.into_iter().map(|(_, emb)| emb).collect())
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
    // Chat request JSON building (Phase C)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_roles_map_to_wire_strings() {
        let req = ChatRequest {
            messages: Some(vec![
                crate::wire::ChatMessage::new(Role::System, "sys"),
                crate::wire::ChatMessage::new(Role::User, "u"),
                crate::wire::ChatMessage::new(Role::Assistant, "a"),
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(
            messages.len(),
            3,
            "system stays inline as a message for OpenAI"
        );
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "sys");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[2]["content"], "a");
        // max_tokens omitted → not sent.
        assert!(json.get("max_tokens").is_none());
    }

    #[test]
    fn chat_json_prompt_becomes_single_user_turn() {
        let req = ChatRequest {
            prompt: Some("just this".into()),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "just this");
    }

    #[test]
    fn chat_json_preserves_tool_calls_and_tool_results() {
        use crate::wire::{ToolCall, ToolCallFunction};
        // A multi-turn tool exchange: user → assistant(tool_calls) → tool(result).
        // The assistant turn must keep `tool_calls` and the tool turn must keep
        // `tool_call_id`, or OpenAI rejects the request (400).
        let req = ChatRequest {
            messages: Some(vec![
                ChatMessage::new(Role::User, "weather?"),
                ChatMessage {
                    role: Role::Assistant,
                    content: String::new(),
                    name: None,
                    tool_calls: Some(vec![ToolCall::Function {
                        id: "call_1".into(),
                        function: ToolCallFunction {
                            name: "get_weather".into(),
                            arguments: r#"{"city":"Paris"}"#.into(),
                        },
                    }]),
                    tool_call_id: None,
                },
                ChatMessage {
                    role: Role::Tool,
                    content: "18C".into(),
                    name: None,
                    tool_calls: None,
                    tool_call_id: Some("call_1".into()),
                },
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(msgs[1]["tool_calls"][0]["type"], "function");
        assert_eq!(msgs[1]["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "call_1");
        assert_eq!(msgs[2]["content"], "18C");
    }

    #[test]
    fn chat_json_includes_seed_and_penalties() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            seed: Some(42),
            frequency_penalty: Some(0.5),
            presence_penalty: Some(0.25),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert_eq!(json["seed"], 42);
        let fp = json["frequency_penalty"].as_f64().unwrap();
        assert!((fp - 0.5).abs() < 1e-6);
        let pp = json["presence_penalty"].as_f64().unwrap();
        assert!((pp - 0.25).abs() < 1e-6);
    }

    #[test]
    fn chat_json_response_format_json_object() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::JsonObject),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert_eq!(json["response_format"]["type"], "json_object");
    }

    #[test]
    fn chat_json_response_format_text_omitted() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::Text),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert!(json.get("response_format").is_none());
    }

    #[test]
    fn chat_json_logit_bias_serialized() {
        let mut bias = std::collections::BTreeMap::new();
        bias.insert(1234u32, -1.5f32);
        let req = ChatRequest {
            prompt: Some("hi".into()),
            logit_bias: Some(bias),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        let val = json["logit_bias"]["1234"].as_f64().unwrap();
        assert!((val + 1.5).abs() < 1e-6, "logit_bias: {json}");
    }

    #[test]
    fn chat_json_stop_and_top_p_forwarded_top_k_ignored() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            top_p: Some(0.8),
            top_k: Some(40),
            stop: Some(vec!["\n\n".into()]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        let top_p = json["top_p"].as_f64().unwrap();
        assert!((top_p - 0.8).abs() < 1e-6);
        assert_eq!(json["stop"][0], "\n\n");
        assert!(json.get("top_k").is_none(), "top_k unsupported by OpenAI");
    }

    #[test]
    fn chat_json_grammar_rejected() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            grammar: Some("g".into()),
            ..Default::default()
        };
        let err = build_chat_request_json("gpt-4", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    // -----------------------------------------------------------------------
    // Chat request: tools / structured outputs / logprobs (Phase G2)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_tools_pass_through() {
        use crate::wire::{FunctionDef, Tool};
        let req = ChatRequest {
            prompt: Some("weather?".into()),
            tools: Some(vec![Tool::Function {
                function: FunctionDef {
                    name: "get_weather".into(),
                    description: Some("look up weather".into()),
                    parameters: Some(serde_json::json!({
                        "type": "object",
                        "properties": { "city": { "type": "string" } },
                    })),
                    strict: None,
                },
            }]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert_eq!(json["tools"][0]["type"], "function");
        assert_eq!(json["tools"][0]["function"]["name"], "get_weather");
        assert_eq!(json["tools"][0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn chat_json_tool_choice_modes() {
        use crate::wire::{NamedToolChoice, ToolChoice, ToolChoiceFunction, ToolChoiceMode};
        let auto = ChatRequest {
            prompt: Some("hi".into()),
            tool_choice: Some(ToolChoice::Mode(ToolChoiceMode::Auto)),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &auto).unwrap()).unwrap();
        assert_eq!(json["tool_choice"], "auto");

        let named = ChatRequest {
            prompt: Some("hi".into()),
            tool_choice: Some(ToolChoice::Named(NamedToolChoice::Function {
                function: ToolChoiceFunction {
                    name: "get_weather".into(),
                },
            })),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &named).unwrap()).unwrap();
        assert_eq!(json["tool_choice"]["type"], "function");
        assert_eq!(json["tool_choice"]["function"]["name"], "get_weather");
    }

    #[test]
    fn chat_json_response_format_json_schema() {
        use crate::wire::JsonSchemaSpec;
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::JsonSchema {
                json_schema: JsonSchemaSpec {
                    name: "person".into(),
                    description: Some("a person".into()),
                    schema: serde_json::json!({
                        "type": "object",
                        "properties": { "name": { "type": "string" } },
                    }),
                    strict: Some(true),
                },
            }),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert_eq!(json["response_format"]["type"], "json_schema");
        assert_eq!(json["response_format"]["json_schema"]["name"], "person");
        assert_eq!(json["response_format"]["json_schema"]["strict"], true);
        assert_eq!(
            json["response_format"]["json_schema"]["schema"]["type"],
            "object"
        );
    }

    #[test]
    fn chat_json_logprobs_forwarded() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            logprobs: Some(true),
            top_logprobs: Some(5),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert_eq!(json["logprobs"], true);
        assert_eq!(json["top_logprobs"], 5);
    }

    #[test]
    fn chat_json_extensions_omitted_when_unset() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("gpt-4", &req).unwrap()).unwrap();
        assert!(json.get("logprobs").is_none());
        assert!(json.get("tools").is_none());
        assert!(json.get("tool_choice").is_none());
        assert!(json.get("response_format").is_none());
    }

    // -----------------------------------------------------------------------
    // Chat response parsing: tool calls / logprobs (Phase G2)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_chat_response_text() {
        let body = r#"{
            "model": "gpt-4o-mini",
            "choices": [{"index":0,"message":{"role":"assistant","content":"Hello world"},"finish_reason":"stop"}],
            "usage": {"prompt_tokens":5,"completion_tokens":2}
        }"#;
        let resp = parse_chat_response_json(body, "gpt-4").unwrap();
        assert_eq!(resp.model, "gpt-4o-mini");
        assert_eq!(resp.choices[0].message.content, "Hello world");
        assert_eq!(resp.choices[0].finish_reason, FinishReason::Stop);
        assert_eq!(resp.usage.total_tokens, 7);
        assert!(resp.choices[0].message.tool_calls.is_none());
    }

    #[test]
    fn parse_chat_response_tool_calls() {
        let body = r#"{
            "choices": [{
                "index":0,
                "message":{"role":"assistant","content":null,"tool_calls":[
                    {"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}
                ]},
                "finish_reason":"tool_calls"
            }],
            "usage":{"prompt_tokens":10,"completion_tokens":8}
        }"#;
        let resp = parse_chat_response_json(body, "gpt-4").unwrap();
        assert_eq!(resp.choices[0].finish_reason, FinishReason::ToolCalls);
        let calls = resp.choices[0].message.tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 1);
        let ToolCall::Function { id, function } = &calls[0];
        assert_eq!(id, "call_1");
        assert_eq!(function.name, "get_weather");
        assert!(function.arguments.contains("Paris"));
        // content null alongside tool_calls is valid.
        assert_eq!(resp.choices[0].message.content, "");
    }

    #[test]
    fn parse_chat_response_logprobs() {
        let body = r#"{
            "choices":[{
                "index":0,
                "message":{"role":"assistant","content":"hi"},
                "finish_reason":"stop",
                "logprobs":{"content":[
                    {"token":"hi","logprob":-0.05,"bytes":[104,105],"top_logprobs":[{"token":"hey","logprob":-2.3,"bytes":[104,101,121]}]}
                ]}
            }],
            "usage":{"prompt_tokens":1,"completion_tokens":1}
        }"#;
        let resp = parse_chat_response_json(body, "gpt-4").unwrap();
        let lp = resp.choices[0].logprobs.as_ref().unwrap();
        assert_eq!(lp.content.len(), 1);
        assert_eq!(lp.content[0].token, "hi");
        assert!((lp.content[0].logprob + 0.05).abs() < 1e-6);
        assert_eq!(lp.content[0].bytes.as_ref().unwrap(), &vec![104u8, 105]);
        assert_eq!(lp.content[0].top_logprobs[0].token, "hey");
    }

    #[test]
    fn parse_chat_response_missing_content_and_tools_errors() {
        let body = r#"{"choices":[{"message":{"role":"assistant"},"finish_reason":"stop"}]}"#;
        let err = parse_chat_response_json(body, "gpt-4").unwrap_err();
        assert!(err.to_string().contains("missing message content"));
    }

    #[test]
    fn parse_chat_response_api_error() {
        let body = r#"{"error":{"message":"bad key"}}"#;
        let err = parse_chat_response_json(body, "gpt-4").unwrap_err();
        assert!(err.to_string().contains("bad key"));
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
    fn parse_null_content_returns_error() {
        let body = r#"{
            "choices": [{
                "message": {"content": null},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 0}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("missing message content"));
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
    // The request path, end to end against a local stand-in for the API
    // -----------------------------------------------------------------------

    use crate::provider::cloud::test_server::CannedResponse;

    fn provider_at(server: &CannedResponse) -> OpenAIProvider {
        OpenAIProvider::new("sk-test-key".into(), "gpt-4o-mini".into())
            .expect("a valid provider")
            .with_base_url(server.base_url())
    }

    fn short_request() -> GenerateRequest {
        GenerateRequest {
            prompt: "hi".into(),
            max_tokens: 8,
            ..GenerateRequest::default()
        }
    }

    /// The whole path works: the request reaches the endpoint with the
    /// credential, and a good answer parses. The control for the failure
    /// tests below, and what keeps `send` from being replaced wholesale.
    #[test]
    fn a_completion_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":1,"completion_tokens":1}}"#,
        );
        let response = provider_at(&server)
            .generate(&short_request())
            .expect("a parsed completion");
        assert_eq!(response.text, "hello");

        let request = server.request();
        assert!(
            request.starts_with("POST /chat/completions "),
            "the endpoint under the base: {request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer sk-test-key"),
            "the credential travels as a bearer token: {request}"
        );
    }

    /// OpenAI's no-credits answer is a 429 whose body says `insufficient_quota`
    /// (#3236). Read by status alone it is "rate limited", which tells the
    /// user to wait for something that will never reset on its own.
    #[test]
    fn no_credits_is_quota_exhausted_not_rate_limited() {
        let server = CannedResponse::serve(
            429,
            r#"{"error":{"message":"You have no credits remaining. Add credits to continue using the API at https://platform.openai.com/settings/organization/billing.",
                "type":"insufficient_quota","param":null,"code":"credit_balance_exhausted"}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("no credits");
        assert_eq!(error.code(), "inference.provider_quota_exhausted");
        assert!(
            error.to_string().contains("no credits remaining"),
            "the provider's own explanation reaches the user: {error}"
        );
    }

    /// A model OpenAI does not know is a 404 with `model_not_found` (#3236).
    /// By status alone it was "provider unavailable, retry".
    #[test]
    fn an_unknown_model_is_model_not_found_not_an_outage() {
        let server = CannedResponse::serve(
            404,
            r#"{"error":{"message":"The model `gpt-4o-minx` does not exist or you do not have access to it.",
                "type":"invalid_request_error","param":null,"code":"model_not_found"}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("unknown model");
        assert_eq!(error.code(), "inference.provider_model_not_found");
        assert!(
            error.to_string().contains("gpt-4o-minx"),
            "names the model the provider rejected: {error}"
        );
    }

    /// Direction control: a rejected key still reads as an auth failure, and
    /// the provider's own wording rides along.
    #[test]
    fn a_rejected_key_is_auth_failed() {
        let server = CannedResponse::serve(
            401,
            r#"{"error":{"message":"Incorrect API key provided: sk-demo-*************rong. You can find your API key at https://platform.openai.com/account/api-keys.",
                "type":"invalid_request_error","code":"invalid_api_key","param":null},"status":401}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("rejected key");
        assert_eq!(error.code(), "inference.provider_auth_failed");
        let shown = error.to_string();
        assert!(
            shown.contains("Incorrect API key"),
            "the provider's own explanation reaches the user: {shown}"
        );
        assert!(
            !shown.contains("sk-demo"),
            "a key echoed back by the provider is redacted: {shown}"
        );
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
    fn parse_missing_message_content_returns_error() {
        let body = r#"{
            "choices": [{
                "message": {},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 0}
        }"#;
        let err = parse_response_json(body).unwrap_err();
        assert!(err.to_string().contains("missing message content"));
    }

    #[test]
    fn debug_redacts_api_key() {
        let p = OpenAIProvider::new("sk-secret-key-123".into(), "gpt-4".into()).unwrap();
        let dbg = format!("{:?}", p);
        assert!(
            !dbg.contains("sk-secret-key-123"),
            "API key leaked in Debug output: {dbg}"
        );
        assert!(
            dbg.contains("[REDACTED]"),
            "Debug should show [REDACTED]: {dbg}"
        );
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
    fn generate_max_tokens_zero_returns_error() {
        let provider = OpenAIProvider::new("sk-key".into(), "gpt-4".into()).unwrap();
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

    #[test]
    fn request_json_grammar_enables_json_mode() {
        let req = GenerateRequest {
            prompt: "test".into(),
            grammar: Some("root ::= \"{\" ... \"}\"".into()),
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["response_format"]["type"], "json_object");
    }

    #[test]
    fn request_json_no_grammar_no_response_format() {
        let req = GenerateRequest {
            prompt: "test".into(),
            ..Default::default()
        };
        let json_str = build_request_json("gpt-4", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(json.get("response_format").is_none());
    }

    // -----------------------------------------------------------------------
    // Embedding request JSON building
    // -----------------------------------------------------------------------

    #[test]
    fn embed_request_single_text() {
        let json_str = build_embed_request_json("text-embedding-3-small", &["hello world"]);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["model"], "text-embedding-3-small");
        let input = json["input"].as_array().unwrap();
        assert_eq!(input.len(), 1);
        assert_eq!(input[0], "hello world");
    }

    #[test]
    fn embed_request_batch() {
        let texts = &["hello", "world", "foo"];
        let json_str = build_embed_request_json("text-embedding-3-large", texts);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(json["model"], "text-embedding-3-large");
        let input = json["input"].as_array().unwrap();
        assert_eq!(input.len(), 3);
        assert_eq!(input[0], "hello");
        assert_eq!(input[1], "world");
        assert_eq!(input[2], "foo");
    }

    #[test]
    fn embed_request_special_chars() {
        let json_str = build_embed_request_json("model", &["Hello \"world\" \n\ttab & <html>"]);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(json["input"][0], "Hello \"world\" \n\ttab & <html>");
    }

    // -----------------------------------------------------------------------
    // Embedding response JSON parsing
    // -----------------------------------------------------------------------

    #[test]
    fn embed_response_single_embedding() {
        let body = r#"{
            "data": [
                {"embedding": [0.1, 0.2, 0.3], "index": 0}
            ],
            "usage": {"prompt_tokens": 2, "total_tokens": 2}
        }"#;
        let embeddings = parse_embed_response_json(body).unwrap();
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), 3);
        assert!((embeddings[0][0] - 0.1).abs() < 1e-6);
        assert!((embeddings[0][1] - 0.2).abs() < 1e-6);
        assert!((embeddings[0][2] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn embed_response_batch_preserves_order() {
        // OpenAI may return embeddings out of order — we must sort by index
        let body = r#"{
            "data": [
                {"embedding": [0.9, 0.9], "index": 1},
                {"embedding": [0.1, 0.1], "index": 0}
            ],
            "usage": {"prompt_tokens": 4, "total_tokens": 4}
        }"#;
        let embeddings = parse_embed_response_json(body).unwrap();
        assert_eq!(embeddings.len(), 2);
        // index 0 should be first
        assert!((embeddings[0][0] - 0.1).abs() < 1e-6);
        // index 1 should be second
        assert!((embeddings[1][0] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn embed_response_api_error() {
        let body = r#"{
            "error": {
                "message": "Invalid model specified",
                "type": "invalid_request_error"
            }
        }"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("Invalid model"));
    }

    #[test]
    fn embed_response_missing_data_returns_error() {
        let body = r#"{"usage": {"prompt_tokens": 1}}"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("data"));
    }

    #[test]
    fn embed_response_empty_data_returns_error() {
        let body = r#"{"data": [], "usage": {"prompt_tokens": 0}}"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn embed_response_invalid_json() {
        let err = parse_embed_response_json("{not json}").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"));
    }

    #[test]
    fn embed_response_missing_embedding_field() {
        let body = r#"{
            "data": [{"index": 0}],
            "usage": {"prompt_tokens": 1}
        }"#;
        let err = parse_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("embedding"));
    }
}
