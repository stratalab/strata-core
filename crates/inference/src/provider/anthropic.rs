//! Anthropic (Claude) cloud generation provider.
//!
//! Sends generation requests to `https://api.anthropic.com/v1/messages` and
//! maps the response to [`GenerateResponse`] (completion) or [`ChatResponse`]
//! (chat, with native tools and structured outputs).

use crate::provider::cloud::{post_json, reject_local_only, CloudPost};
use crate::wire::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, FinishReason, FunctionDef, NamedToolChoice,
    ResponseFormat, Role, Tool, ToolCall, ToolCallFunction, ToolChoice, ToolChoiceMode, Usage,
};
use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

/// The origin every Anthropic endpoint hangs off when nothing overrides it.
/// The `/v1` segment belongs to the endpoint path, as in the Anthropic SDK,
/// so an `ANTHROPIC_BASE_URL` written for the SDK works here unchanged.
const API_BASE: &str = crate::settings::ANTHROPIC_BASE_URL;
const API_VERSION: &str = "2023-06-01";

/// The Messages endpoint under `api_base`.
fn messages_url(api_base: &str) -> String {
    format!("{api_base}/v1/messages")
}

/// Anthropic cloud provider state.
pub(crate) struct AnthropicProvider {
    api_key: String,
    model: String,
    /// Where requests go: [`API_BASE`], or the base URL the runtime's
    /// settings override it with.
    api_base: String,
}

impl std::fmt::Debug for AnthropicProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl AnthropicProvider {
    pub(crate) fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
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
        Ok(Self {
            api_key,
            model,
            api_base: API_BASE.to_string(),
        })
    }

    /// Point requests at `base_url` instead of the public API: a proxy, or a
    /// test's canned-response server. The `/v1/messages` path is appended.
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

    /// Generate from an OpenAI-shaped chat request, mapping messages natively:
    /// system prompt hoisted, multi-turn preserved, assistant `tool_calls`
    /// rendered as `tool_use` blocks, `tool` results batched into user turns,
    /// tools/tool_choice forwarded, and `response_format: json_schema` realised
    /// as a forced single-tool call. The native response (text + tool calls) is
    /// parsed back into the [`ChatResponse`].
    pub(crate) fn generate_chat(
        &self,
        request: &ChatRequest,
    ) -> Result<ChatResponse, InferenceError> {
        let body = build_chat_request_json(&self.model, request)?;
        // When `response_format: json_schema` is used, structured output is
        // implemented as a forced tool; the parser folds that tool's input back
        // into message content instead of surfacing it as a tool call.
        let forced = forced_structured_output_tool(request);
        let raw = self.send(body)?;
        parse_chat_response_json(&raw, &self.model, forced.as_deref())
    }

    /// Send a prepared completion request body and parse it as a completion.
    fn post(&self, body: String) -> Result<GenerateResponse, InferenceError> {
        parse_response_json(&self.send(body)?)
    }

    /// POST a prepared JSON body to the Messages endpoint and return the raw
    /// response body (shared by the completion and chat parsers).
    fn send(&self, body: String) -> Result<String, InferenceError> {
        post_json(&CloudPost {
            provider: "Anthropic",
            url: &messages_url(&self.api_base),
            headers: &[
                ("x-api-key", &self.api_key),
                ("anthropic-version", API_VERSION),
            ],
            body: &body,
            timeout: std::time::Duration::from_secs(30),
        })
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }
}

/// Build the Anthropic Messages API request JSON.
///
/// Silently ignores `top_k`, `seed`, `stop_tokens` (not supported by this API).
///
/// **temperature vs top_p mutual exclusion:** newer Claude models (sonnet 4.5+,
/// opus 4.6+, haiku 4.5+) reject requests that specify *both* `temperature`
/// and `top_p` with `400 invalid_request_error`. We always send `temperature`
/// (Anthropic defaults to 1.0 if omitted, which would silently break greedy
/// decoding) and drop `top_p` when both would otherwise be sent. Callers that
/// want top_p instead of temperature should set temperature to NaN or rely on
/// Anthropic's default by passing top_p explicitly — but most callers prefer
/// the temperature knob, so dropping top_p is the safer default.
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
    // which would break greedy decoding (temperature=0.0). top_p is dropped
    // because Anthropic's API rejects requests that set both.
    obj["temperature"] = serde_json::json!(request.temperature);

    // Include stop_sequences if non-empty
    if !request.stop_sequences.is_empty() {
        obj["stop_sequences"] = serde_json::json!(request.stop_sequences);
    }

    // top_p: dropped to satisfy Anthropic's mutual-exclusion constraint with temperature
    // top_k: silently ignored (not supported)
    // seed: silently ignored (not supported)
    // stop_tokens: silently ignored (token-level, local only)

    obj.to_string()
}

/// Build the Anthropic Messages API request JSON from an OpenAI-shaped chat
/// request.
///
/// System turns are hoisted to the top-level `system` field (multiple joined
/// with "\n"). User and assistant turns become `messages`; an assistant turn
/// that carries `tool_calls` is rendered as `tool_use` content blocks, and
/// consecutive `tool` results are batched into a single user turn of
/// `tool_result` blocks (Anthropic requires tool results grouped this way).
///
/// `tools`/`tool_choice` map to Anthropic's native shapes.
/// `response_format: json_schema` is realised as a *forced tool*: the schema
/// becomes a tool whose invocation is required, which is how Claude produces
/// guaranteed-shape JSON. `json_object` has no Anthropic equivalent and is
/// rejected. `temperature`/`top_p` remain mutually exclusive; `top_p` is now
/// forwarded when set. Seed, penalties, logit_bias, and the llama.cpp
/// extensions have no Anthropic equivalent and are ignored.
///
/// Messages are iterated directly (not via `chat_turns`, which is lossy — it
/// drops `tool_calls`/`tool_call_id`, both required for the tool round-trip).
pub(crate) fn build_chat_request_json(
    model: &str,
    request: &ChatRequest,
) -> Result<String, InferenceError> {
    reject_local_only(request, "Anthropic")?;

    if request.temperature.is_some() && request.top_p.is_some() {
        return Err(InferenceError::Provider(
            "Anthropic: set `temperature` or `top_p`, not both".to_string(),
        ));
    }

    // A raw `prompt` is a single user turn with no tools; otherwise iterate the
    // full messages directly to preserve tool_calls/tool_call_id.
    let prompt_fallback: Vec<ChatMessage>;
    let messages_in: &[ChatMessage] = if let Some(msgs) = &request.messages {
        msgs.as_slice()
    } else if let Some(prompt) = &request.prompt {
        prompt_fallback = vec![ChatMessage::new(Role::User, prompt.clone())];
        prompt_fallback.as_slice()
    } else {
        &[]
    };

    let (system, messages) = build_anthropic_messages(messages_in)?;

    let max_tokens = request.max_tokens.unwrap_or(1024);
    let mut obj = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
    });

    if !system.is_empty() {
        obj["system"] = serde_json::json!(system);
    }
    if let Some(temperature) = request.temperature {
        obj["temperature"] = serde_json::json!(temperature);
    }
    // top_p is forwarded; the mutual-exclusion with temperature is rejected
    // above rather than silently dropped.
    if let Some(top_p) = request.top_p {
        obj["top_p"] = serde_json::json!(top_p);
    }
    if let Some(top_k) = request.top_k {
        obj["top_k"] = serde_json::json!(top_k);
    }
    if let Some(stop) = &request.stop {
        if !stop.is_empty() {
            obj["stop_sequences"] = serde_json::json!(stop);
        }
    }

    // --- tools / tool_choice / response_format ---
    let mut tools_json: Vec<serde_json::Value> = Vec::new();
    if let Some(tools) = &request.tools {
        for tool in tools {
            let Tool::Function { function } = tool;
            tools_json.push(anthropic_tool_json(function));
        }
    }
    let mut tool_choice_json = request.tool_choice.as_ref().map(map_tool_choice);

    match &request.response_format {
        Some(ResponseFormat::JsonSchema { json_schema }) => {
            // A forced structured-output tool cannot coexist with caller-supplied
            // tools or a tool_choice — the intent would be ambiguous.
            if request.tools.is_some() || request.tool_choice.is_some() {
                return Err(InferenceError::Provider(
                    "Anthropic: response_format json_schema cannot be combined with `tools` or \
                     `tool_choice`"
                        .to_string(),
                ));
            }
            let mut tool = serde_json::json!({
                "name": json_schema.name,
                "input_schema": json_schema.schema,
            });
            if let Some(description) = &json_schema.description {
                tool["description"] = serde_json::json!(description);
            }
            tools_json.push(tool);
            tool_choice_json = Some(serde_json::json!({
                "type": "tool",
                "name": json_schema.name,
            }));
        }
        Some(ResponseFormat::JsonObject) => {
            return Err(InferenceError::Provider(
                "Anthropic: json_object is unsupported; use response_format json_schema or prompt \
                 for JSON"
                    .to_string(),
            ));
        }
        Some(ResponseFormat::Text) | None => {}
    }

    if !tools_json.is_empty() {
        obj["tools"] = serde_json::Value::Array(tools_json);
    }
    if let Some(tool_choice) = tool_choice_json {
        obj["tool_choice"] = tool_choice;
    }

    // logprobs: Anthropic's Messages API has no log-probability support, so
    // `request.logprobs` is silently ignored (never sent, never an error).

    Ok(obj.to_string())
}

/// Map OpenAI-shaped chat turns into Anthropic's `(system, messages)` pair.
///
/// System turns are joined (with "\n") into the returned system string. User,
/// assistant, and tool turns become message objects: an assistant turn with
/// `tool_calls` renders as `tool_use` blocks, and consecutive `tool` results
/// are batched into a single user turn of `tool_result` blocks. A `tool`
/// message without a `tool_call_id` is an error.
fn build_anthropic_messages(
    messages_in: &[ChatMessage],
) -> Result<(String, Vec<serde_json::Value>), InferenceError> {
    let mut system = String::new();
    let mut messages: Vec<serde_json::Value> = Vec::new();
    // Consecutive `tool` results must be delivered as a single user turn.
    let mut pending_tool_results: Vec<serde_json::Value> = Vec::new();

    for message in messages_in {
        // Any non-tool message closes an open run of tool results.
        if message.role != Role::Tool && !pending_tool_results.is_empty() {
            messages.push(serde_json::json!({
                "role": "user",
                "content": serde_json::Value::Array(std::mem::take(&mut pending_tool_results)),
            }));
        }

        match message.role {
            Role::System => {
                if !system.is_empty() {
                    system.push('\n');
                }
                system.push_str(&message.content);
            }
            Role::User => {
                messages.push(serde_json::json!({
                    "role": "user",
                    "content": message.content,
                }));
            }
            Role::Assistant => {
                messages.push(assistant_message_json(message));
            }
            Role::Tool => {
                let tool_use_id = message.tool_call_id.as_deref().ok_or_else(|| {
                    InferenceError::Provider(
                        "Anthropic: `tool` message is missing `tool_call_id`".to_string(),
                    )
                })?;
                pending_tool_results.push(serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": message.content,
                }));
            }
        }
    }

    // Flush a trailing run of tool results.
    if !pending_tool_results.is_empty() {
        messages.push(serde_json::json!({
            "role": "user",
            "content": serde_json::Value::Array(pending_tool_results),
        }));
    }

    Ok((system, messages))
}

/// Render an assistant turn as an Anthropic message. A turn with `tool_calls`
/// becomes a content-block array (an optional leading `text` block, then one
/// `tool_use` block per call); otherwise it is a plain-text assistant message.
fn assistant_message_json(message: &ChatMessage) -> serde_json::Value {
    let tool_calls = match &message.tool_calls {
        Some(calls) if !calls.is_empty() => calls,
        _ => {
            return serde_json::json!({
                "role": "assistant",
                "content": message.content,
            });
        }
    };

    let mut blocks: Vec<serde_json::Value> = Vec::new();
    // Omit the text block for a pure tool-call turn.
    if !message.content.is_empty() {
        blocks.push(serde_json::json!({
            "type": "text",
            "text": message.content,
        }));
    }
    for call in tool_calls {
        let ToolCall::Function { id, function } = call;
        // `arguments` is a JSON-encoded string; Anthropic wants a JSON object
        // for `input`. Fall back to `{}` if it does not parse.
        let input: serde_json::Value =
            serde_json::from_str(&function.arguments).unwrap_or_else(|_| serde_json::json!({}));
        blocks.push(serde_json::json!({
            "type": "tool_use",
            "id": id,
            "name": function.name,
            "input": input,
        }));
    }
    serde_json::json!({
        "role": "assistant",
        "content": serde_json::Value::Array(blocks),
    })
}

/// Map a [`FunctionDef`] to an Anthropic tool object (`name`, optional
/// `description`, and `input_schema`). A missing parameter schema becomes an
/// empty object schema.
fn anthropic_tool_json(function: &FunctionDef) -> serde_json::Value {
    let input_schema = function
        .parameters
        .clone()
        .unwrap_or_else(|| serde_json::json!({"type": "object", "properties": {}}));
    let mut tool = serde_json::json!({
        "name": function.name,
        "input_schema": input_schema,
    });
    if let Some(description) = &function.description {
        tool["description"] = serde_json::json!(description);
    }
    tool
}

/// Map an OpenAI-shaped [`ToolChoice`] to Anthropic's `tool_choice` object:
/// `auto` → `auto`, `required` → `any`, `none` → `none`, and a named function
/// → `{"type":"tool","name":…}`.
fn map_tool_choice(choice: &ToolChoice) -> serde_json::Value {
    match choice {
        ToolChoice::Mode(ToolChoiceMode::Auto) => serde_json::json!({"type": "auto"}),
        ToolChoice::Mode(ToolChoiceMode::Required) => serde_json::json!({"type": "any"}),
        ToolChoice::Mode(ToolChoiceMode::None) => serde_json::json!({"type": "none"}),
        ToolChoice::Named(NamedToolChoice::Function { function }) => {
            serde_json::json!({"type": "tool", "name": function.name})
        }
    }
}

/// The forced-tool name that implements `response_format: json_schema`, or
/// `None` for any other response-format setting. Used by the parser to fold the
/// forced tool's output back into message content.
fn forced_structured_output_tool(request: &ChatRequest) -> Option<String> {
    match &request.response_format {
        Some(ResponseFormat::JsonSchema { json_schema }) => Some(json_schema.name.clone()),
        _ => None,
    }
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
            return Err(InferenceError::Provider(format!(
                "Anthropic API error: {msg}"
            )));
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
        Some(other) => {
            tracing::warn!(reason = ?other, "Unknown stop reason from Anthropic, defaulting to StopToken");
            StopReason::StopToken
        }
        None => StopReason::StopToken,
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

/// Parse the Anthropic Messages API response JSON into a rich [`ChatResponse`]
/// (concatenated text, tool calls, finish reason, and usage).
///
/// `forced_tool_name` is the tool that stands in for `response_format:
/// json_schema`. When a `tool_use` block matches it, its input is folded into
/// the message content (the structured JSON output) rather than surfaced as a
/// tool call, and the turn reads as a normal stop — mirroring OpenAI's
/// json_schema behaviour.
pub(crate) fn parse_chat_response_json(
    body: &str,
    fallback_model: &str,
    forced_tool_name: Option<&str>,
) -> Result<ChatResponse, InferenceError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| InferenceError::Provider(format!("Anthropic: invalid JSON response: {e}")))?;

    // API error response.
    if json.get("type").and_then(|v| v.as_str()) == Some("error") {
        let msg = json
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        return Err(InferenceError::Provider(format!(
            "Anthropic API error: {msg}"
        )));
    }

    let content = json
        .get("content")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("Anthropic: missing or invalid 'content' array".to_string())
        })?;

    let mut text = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    let mut structured_output: Option<String> = None;

    for block in content {
        match block.get("type").and_then(|t| t.as_str()) {
            Some("text") => {
                if let Some(chunk) = block.get("text").and_then(|t| t.as_str()) {
                    text.push_str(chunk);
                }
            }
            Some("tool_use") => {
                let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let input = block
                    .get("input")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                // The forced structured-output tool: surface its input as the
                // message content instead of as a tool call.
                if forced_tool_name == Some(name) {
                    structured_output = Some(serde_json::to_string(&input).unwrap_or_default());
                    continue;
                }
                let id = block
                    .get("id")
                    .and_then(|i| i.as_str())
                    .unwrap_or("")
                    .to_string();
                let arguments = serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string());
                tool_calls.push(ToolCall::Function {
                    id,
                    function: ToolCallFunction {
                        name: name.to_string(),
                        arguments,
                    },
                });
            }
            _ => {}
        }
    }

    let (content, tool_calls, finish_reason) = match structured_output {
        // Forced structured output: the JSON becomes the message content and the
        // turn reads as a normal stop, not a tool call.
        Some(json_content) => (json_content, None, FinishReason::Stop),
        None => {
            let finish = match json.get("stop_reason").and_then(|s| s.as_str()) {
                Some("max_tokens") => FinishReason::Length,
                Some("tool_use") => FinishReason::ToolCalls,
                // "end_turn", "stop_sequence", null, and unknown → Stop.
                _ => FinishReason::Stop,
            };
            let calls = (!tool_calls.is_empty()).then_some(tool_calls);
            (text, calls, finish)
        }
    };

    let usage = json.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("input_tokens"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;
    let completion_tokens = usage
        .and_then(|u| u.get("output_tokens"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;

    Ok(ChatResponse {
        model: fallback_model.to_string(),
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
            logprobs: None,
        }],
        usage: Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        },
    })
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
    fn request_json_top_p_always_omitted_with_temperature() {
        // Anthropic rejects requests that set both temperature and top_p.
        // We always send temperature, so top_p must always be dropped — even
        // when the caller explicitly requests it.
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
    fn request_json_top_p_custom_dropped_to_satisfy_anthropic() {
        // Even a non-default top_p is dropped, because Anthropic's API rejects
        // `temperature` + `top_p` together. We always send temperature, so
        // top_p loses. This was a 400-causing latent bug pre-2026-04.
        let req = GenerateRequest {
            prompt: "test".into(),
            top_p: 0.9,
            ..Default::default()
        };
        let json_str = build_request_json("model", &req);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert!(
            json.get("top_p").is_none(),
            "top_p must be dropped from Anthropic requests to avoid HTTP 400"
        );
        // Temperature must still be present.
        assert!(json.get("temperature").is_some());
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
    fn generate_max_tokens_zero_returns_error() {
        let provider =
            AnthropicProvider::new("sk-key".into(), "claude-sonnet-4-20250514".into()).unwrap();
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
    // Chat request JSON building (Phase C)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_system_hoisted_and_multi_turn_mapped() {
        let req = ChatRequest {
            messages: Some(vec![
                crate::wire::ChatMessage::new(Role::System, "be terse"),
                crate::wire::ChatMessage::new(Role::User, "hi"),
                crate::wire::ChatMessage::new(Role::Assistant, "hello"),
            ]),
            ..Default::default()
        };
        let json_str = build_chat_request_json("claude-sonnet-4-20250514", &req).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // System is hoisted out of `messages` into the top-level `system` field.
        assert_eq!(json["system"], "be terse");
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2, "only user + assistant are messages");
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "hi");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"], "hello");
        // Default max_tokens when the caller omits it.
        assert_eq!(json["max_tokens"], 1024);
    }

    #[test]
    fn chat_json_multiple_system_turns_joined() {
        let req = ChatRequest {
            messages: Some(vec![
                crate::wire::ChatMessage::new(Role::System, "line one"),
                crate::wire::ChatMessage::new(Role::System, "line two"),
                crate::wire::ChatMessage::new(Role::User, "hi"),
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        assert_eq!(json["system"], "line one\nline two");
    }

    #[test]
    fn chat_json_prompt_becomes_single_user_turn() {
        let req = ChatRequest {
            prompt: Some("just this".into()),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "just this");
        assert!(json.get("system").is_none());
    }

    #[test]
    fn chat_json_sends_top_p_when_set() {
        // The regression this phase fixes: top_p used to be silently dropped.
        let req = ChatRequest {
            prompt: Some("hi".into()),
            top_p: Some(0.9),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let top_p = json["top_p"].as_f64().unwrap();
        assert!((top_p - 0.9).abs() < 1e-6, "top_p must be sent: {json}");
    }

    #[test]
    fn chat_json_temperature_and_top_p_together_errors() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            temperature: Some(0.7),
            top_p: Some(0.9),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_forwards_temperature_top_k_and_stop() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            temperature: Some(0.0),
            top_k: Some(40),
            stop: Some(vec!["STOP".into()]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        assert_eq!(json["temperature"], 0.0);
        assert_eq!(json["top_k"], 40);
        assert_eq!(json["stop_sequences"][0], "STOP");
    }

    #[test]
    fn chat_json_response_format_errors() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(crate::wire::ResponseFormat::JsonObject),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_grammar_rejected() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            grammar: Some("root ::= \"x\"".into()),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_tool_message_errors() {
        let req = ChatRequest {
            messages: Some(vec![crate::wire::ChatMessage::new(Role::Tool, "result")]),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
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
    // The request path, end to end against a local stand-in for the API
    // -----------------------------------------------------------------------

    use crate::provider::cloud::test_server::CannedResponse;

    fn provider_at(server: &CannedResponse) -> AnthropicProvider {
        AnthropicProvider::new("sk-ant-test-key".into(), "claude-test".into())
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
    /// credential and version headers, and a good answer parses.
    #[test]
    fn a_completion_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn",
                "usage":{"input_tokens":1,"output_tokens":1}}"#,
        );
        let response = provider_at(&server)
            .generate(&short_request())
            .expect("a parsed completion");
        assert_eq!(response.text, "hello");

        let request = server.request().to_ascii_lowercase();
        assert!(
            request.starts_with("post /v1/messages "),
            "the versioned endpoint under the origin: {request}"
        );
        assert!(
            request.contains("x-api-key: sk-ant-test-key"),
            "the credential travels in x-api-key: {request}"
        );
        assert!(
            request.contains(&format!("anthropic-version: {API_VERSION}")),
            "the API version is pinned: {request}"
        );
    }

    /// A model Anthropic does not know is a 404 with `not_found_error`
    /// (#3236). By status alone it was "provider unavailable, retry".
    #[test]
    fn an_unknown_model_is_model_not_found_not_an_outage() {
        let server = CannedResponse::serve(
            404,
            r#"{"type":"error","error":{"type":"not_found_error","message":"model: claude-nope"},"request_id":"req_x"}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("unknown model");
        assert_eq!(error.code(), "inference.provider_model_not_found");
        assert!(
            error.to_string().contains("claude-nope"),
            "names the model the provider rejected: {error}"
        );
    }

    /// Anthropic reports an empty balance as a 400 `invalid_request_error`
    /// whose message is the only place the cause appears (#3236). By status
    /// alone it was "invalid request", which sends the user to their prompt.
    #[test]
    fn an_empty_balance_is_quota_exhausted_not_an_invalid_request() {
        let server = CannedResponse::serve(
            400,
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("empty balance");
        assert_eq!(error.code(), "inference.provider_quota_exhausted");
        assert!(
            error.to_string().contains("credit balance is too low"),
            "the provider's own explanation reaches the user: {error}"
        );
    }

    /// Direction control: a rejected key still reads as an auth failure.
    #[test]
    fn a_rejected_key_is_auth_failed() {
        let server = CannedResponse::serve(
            401,
            r#"{"type":"error","error":{"type":"authentication_error","message":"API key is invalid."},"request_id":null}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("rejected key");
        assert_eq!(error.code(), "inference.provider_auth_failed");
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
        let p = AnthropicProvider::new(
            "sk-ant-secret-key-123".into(),
            "claude-sonnet-4-20250514".into(),
        )
        .unwrap();
        let dbg = format!("{:?}", p);
        assert!(
            !dbg.contains("sk-ant-secret-key-123"),
            "API key leaked in Debug output: {dbg}"
        );
        assert!(
            dbg.contains("[REDACTED]"),
            "Debug should show [REDACTED]: {dbg}"
        );
        assert!(
            dbg.contains("claude-sonnet-4-20250514"),
            "Debug should show model name: {dbg}"
        );
    }

    // -----------------------------------------------------------------------
    // Chat request: tools (G3a)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_tools_map_to_input_schema() {
        use crate::wire::{FunctionDef, Tool};
        let req = ChatRequest {
            messages: Some(vec![ChatMessage::new(Role::User, "weather?")]),
            tools: Some(vec![
                Tool::Function {
                    function: FunctionDef {
                        name: "get_weather".into(),
                        description: Some("look up weather".into()),
                        parameters: Some(serde_json::json!({
                            "type": "object",
                            "properties": { "city": { "type": "string" } },
                        })),
                        strict: None,
                    },
                },
                Tool::Function {
                    function: FunctionDef {
                        name: "no_params".into(),
                        description: None,
                        parameters: None,
                        strict: None,
                    },
                },
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let tools = json["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        // First tool: parameters become `input_schema`; description passed through.
        assert_eq!(tools[0]["name"], "get_weather");
        assert_eq!(tools[0]["description"], "look up weather");
        assert_eq!(tools[0]["input_schema"]["type"], "object");
        assert_eq!(
            tools[0]["input_schema"]["properties"]["city"]["type"],
            "string"
        );
        // Anthropic uses `input_schema`, never OpenAI's `parameters`.
        assert!(tools[0].get("parameters").is_none());
        // Second tool: no params → default empty object schema; no description.
        assert_eq!(tools[1]["name"], "no_params");
        assert!(tools[1].get("description").is_none());
        assert_eq!(
            tools[1]["input_schema"],
            serde_json::json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn chat_json_tool_choice_all_variants() {
        use crate::wire::{NamedToolChoice, ToolChoice, ToolChoiceFunction, ToolChoiceMode};
        let choice_json = |tc: ToolChoice| -> serde_json::Value {
            let req = ChatRequest {
                prompt: Some("hi".into()),
                tool_choice: Some(tc),
                ..Default::default()
            };
            serde_json::from_str::<serde_json::Value>(&build_chat_request_json("m", &req).unwrap())
                .unwrap()["tool_choice"]
                .clone()
        };
        assert_eq!(
            choice_json(ToolChoice::Mode(ToolChoiceMode::Auto))["type"],
            "auto"
        );
        assert_eq!(
            choice_json(ToolChoice::Mode(ToolChoiceMode::Required))["type"],
            "any"
        );
        assert_eq!(
            choice_json(ToolChoice::Mode(ToolChoiceMode::None))["type"],
            "none"
        );
        let named = choice_json(ToolChoice::Named(NamedToolChoice::Function {
            function: ToolChoiceFunction {
                name: "get_weather".into(),
            },
        }));
        assert_eq!(named["type"], "tool");
        assert_eq!(named["name"], "get_weather");
    }

    // -----------------------------------------------------------------------
    // Chat request: tool round-trip (G3a)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_assistant_tool_calls_become_tool_use_blocks() {
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: "let me check".into(),
            name: None,
            tool_calls: Some(vec![ToolCall::Function {
                id: "call_1".into(),
                function: ToolCallFunction {
                    name: "get_weather".into(),
                    arguments: r#"{"city":"Paris"}"#.into(),
                },
            }]),
            tool_call_id: None,
        };
        let req = ChatRequest {
            messages: Some(vec![ChatMessage::new(Role::User, "weather?"), assistant]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1]["role"], "assistant");
        let content = messages[1]["content"].as_array().unwrap();
        // Text block first (content non-empty), then the tool_use block.
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "let me check");
        assert_eq!(content[1]["type"], "tool_use");
        assert_eq!(content[1]["id"], "call_1");
        assert_eq!(content[1]["name"], "get_weather");
        // The arguments string is parsed into a JSON object for `input`.
        assert_eq!(content[1]["input"]["city"], "Paris");
    }

    #[test]
    fn chat_json_assistant_pure_tool_call_omits_text_and_tolerates_bad_arguments() {
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: String::new(),
            name: None,
            tool_calls: Some(vec![ToolCall::Function {
                id: "call_x".into(),
                function: ToolCallFunction {
                    name: "f".into(),
                    arguments: "not json".into(),
                },
            }]),
            tool_call_id: None,
        };
        let req = ChatRequest {
            messages: Some(vec![assistant]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let content = json["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1, "no text block for an empty-content turn");
        assert_eq!(content[0]["type"], "tool_use");
        // Unparseable arguments fall back to an empty object.
        assert_eq!(content[0]["input"], serde_json::json!({}));
    }

    #[test]
    fn chat_json_consecutive_tool_results_batched_into_one_user_turn() {
        let tool_msg = |id: &str, content: &str| ChatMessage {
            role: Role::Tool,
            content: content.into(),
            name: None,
            tool_calls: None,
            tool_call_id: Some(id.into()),
        };
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: String::new(),
            name: None,
            tool_calls: Some(vec![
                ToolCall::Function {
                    id: "call_a".into(),
                    function: ToolCallFunction {
                        name: "get_weather".into(),
                        arguments: "{}".into(),
                    },
                },
                ToolCall::Function {
                    id: "call_b".into(),
                    function: ToolCallFunction {
                        name: "get_weather".into(),
                        arguments: "{}".into(),
                    },
                },
            ]),
            tool_call_id: None,
        };
        let req = ChatRequest {
            messages: Some(vec![
                ChatMessage::new(Role::User, "weather in two cities?"),
                assistant,
                tool_msg("call_a", "sunny"),
                tool_msg("call_b", "rainy"),
                ChatMessage::new(Role::User, "thanks"),
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        let messages = json["messages"].as_array().unwrap();
        // user, assistant(tool_use x2), user(tool_result x2), user("thanks").
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[1]["role"], "assistant");
        // The two tool results are batched into ONE user turn.
        assert_eq!(messages[2]["role"], "user");
        let results = messages[2]["content"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["type"], "tool_result");
        assert_eq!(results[0]["tool_use_id"], "call_a");
        assert_eq!(results[0]["content"], "sunny");
        assert_eq!(results[1]["tool_use_id"], "call_b");
        assert_eq!(results[1]["content"], "rainy");
        // The trailing user message is its own turn (tool-result run flushed).
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"], "thanks");
    }

    #[test]
    fn chat_json_tool_message_missing_id_errors() {
        let req = ChatRequest {
            messages: Some(vec![ChatMessage {
                role: Role::Tool,
                content: "result".into(),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            }]),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
        assert!(err.to_string().contains("tool_call_id"), "err: {err}");
    }

    // -----------------------------------------------------------------------
    // Chat request: structured outputs (G3a)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_response_format_json_schema_forces_tool() {
        use crate::wire::JsonSchemaSpec;
        let req = ChatRequest {
            prompt: Some("extract a person".into()),
            response_format: Some(ResponseFormat::JsonSchema {
                json_schema: JsonSchemaSpec {
                    name: "person".into(),
                    description: Some("a person record".into()),
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
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        // The schema becomes a single tool whose call is forced.
        let tools = json["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "person");
        assert_eq!(tools[0]["description"], "a person record");
        assert_eq!(
            tools[0]["input_schema"]["properties"]["name"]["type"],
            "string"
        );
        assert_eq!(json["tool_choice"]["type"], "tool");
        assert_eq!(json["tool_choice"]["name"], "person");
        // No `response_format` field is sent to Anthropic.
        assert!(json.get("response_format").is_none());
    }

    #[test]
    fn chat_json_json_schema_with_user_tools_errors() {
        use crate::wire::{FunctionDef, JsonSchemaSpec, Tool};
        let req = ChatRequest {
            prompt: Some("hi".into()),
            tools: Some(vec![Tool::Function {
                function: FunctionDef {
                    name: "get_weather".into(),
                    description: None,
                    parameters: None,
                    strict: None,
                },
            }]),
            response_format: Some(ResponseFormat::JsonSchema {
                json_schema: JsonSchemaSpec {
                    name: "person".into(),
                    description: None,
                    schema: serde_json::json!({ "type": "object" }),
                    strict: None,
                },
            }),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_json_schema_with_tool_choice_errors() {
        use crate::wire::{JsonSchemaSpec, ToolChoice, ToolChoiceMode};
        let req = ChatRequest {
            prompt: Some("hi".into()),
            tool_choice: Some(ToolChoice::Mode(ToolChoiceMode::Auto)),
            response_format: Some(ResponseFormat::JsonSchema {
                json_schema: JsonSchemaSpec {
                    name: "person".into(),
                    description: None,
                    schema: serde_json::json!({ "type": "object" }),
                    strict: None,
                },
            }),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_json_object_error_mentions_json_schema() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::JsonObject),
            ..Default::default()
        };
        let err = build_chat_request_json("m", &req).unwrap_err();
        assert!(err.to_string().contains("json_schema"), "err: {err}");
    }

    #[test]
    fn chat_json_logprobs_ignored() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            logprobs: Some(true),
            top_logprobs: Some(5),
            ..Default::default()
        };
        // Building must not error just because logprobs was requested...
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json("m", &req).unwrap()).unwrap();
        // ...and nothing logprob-related is sent (Anthropic has no such support).
        assert!(json.get("logprobs").is_none());
        assert!(json.get("top_logprobs").is_none());
    }

    // -----------------------------------------------------------------------
    // Chat response parsing (G3a)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_chat_response_text() {
        let body = r#"{
            "content": [{"type": "text", "text": "Hello world"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 5, "output_tokens": 2}
        }"#;
        let resp = parse_chat_response_json(body, "fallback", None).unwrap();
        assert_eq!(resp.model, "fallback");
        assert_eq!(resp.choices[0].message.content, "Hello world");
        assert_eq!(resp.choices[0].finish_reason, FinishReason::Stop);
        assert!(resp.choices[0].message.tool_calls.is_none());
        assert_eq!(resp.usage.prompt_tokens, 5);
        assert_eq!(resp.usage.completion_tokens, 2);
        assert_eq!(resp.usage.total_tokens, 7);
    }

    #[test]
    fn parse_chat_response_stop_sequence_is_stop() {
        let body = r#"{
            "content": [{"type": "text", "text": "x"}],
            "stop_reason": "stop_sequence",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_chat_response_json(body, "m", None).unwrap();
        assert_eq!(resp.choices[0].finish_reason, FinishReason::Stop);
    }

    #[test]
    fn parse_chat_response_max_tokens_is_length() {
        let body = r#"{
            "content": [{"type": "text", "text": "truncated"}],
            "stop_reason": "max_tokens",
            "usage": {"input_tokens": 3, "output_tokens": 256}
        }"#;
        let resp = parse_chat_response_json(body, "m", None).unwrap();
        assert_eq!(resp.choices[0].finish_reason, FinishReason::Length);
    }

    #[test]
    fn parse_chat_response_tool_use() {
        let body = r#"{
            "model": "claude-sonnet-4-20250514",
            "content": [
                {"type": "text", "text": "Let me check."},
                {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 12, "output_tokens": 9}
        }"#;
        let resp = parse_chat_response_json(body, "fallback", None).unwrap();
        // Spec: model is the caller's fallback, not the response's model field.
        assert_eq!(resp.model, "fallback");
        let choice = &resp.choices[0];
        assert_eq!(choice.finish_reason, FinishReason::ToolCalls);
        assert_eq!(choice.message.content, "Let me check.");
        let calls = choice.message.tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 1);
        let ToolCall::Function { id, function } = &calls[0];
        assert_eq!(id, "toolu_1");
        assert_eq!(function.name, "get_weather");
        // The input object is re-encoded as an arguments string.
        assert_eq!(function.arguments, r#"{"city":"Paris"}"#);
        assert_eq!(resp.usage.total_tokens, 21);
    }

    #[test]
    fn parse_chat_response_forced_structured_output() {
        let body = r#"{
            "content": [
                {"type": "tool_use", "id": "toolu_2", "name": "person", "input": {"name": "Ada"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 5, "output_tokens": 7}
        }"#;
        let resp = parse_chat_response_json(body, "m", Some("person")).unwrap();
        let choice = &resp.choices[0];
        // The forced tool's input becomes the JSON message content.
        assert_eq!(choice.message.content, r#"{"name":"Ada"}"#);
        // Not surfaced as a tool call, and reads as a normal stop.
        assert!(choice.message.tool_calls.is_none());
        assert_eq!(choice.finish_reason, FinishReason::Stop);
    }

    #[test]
    fn parse_chat_response_forced_name_mismatch_is_a_tool_call() {
        // forced tool name is set, but the model called a different tool.
        let body = r#"{
            "content": [
                {"type": "tool_use", "id": "toolu_3", "name": "other_tool", "input": {"q": 1}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        }"#;
        let resp = parse_chat_response_json(body, "m", Some("person")).unwrap();
        let choice = &resp.choices[0];
        assert_eq!(choice.finish_reason, FinishReason::ToolCalls);
        assert!(choice.message.tool_calls.is_some());
        assert_eq!(choice.message.content, "");
    }

    #[test]
    fn parse_chat_response_api_error() {
        let body = r#"{
            "type": "error",
            "error": {"type": "authentication_error", "message": "invalid x-api-key"}
        }"#;
        let err = parse_chat_response_json(body, "m", None).unwrap_err();
        assert!(err.to_string().contains("invalid x-api-key"), "err: {err}");
    }

    #[test]
    fn parse_chat_response_invalid_json() {
        let err = parse_chat_response_json("not json", "m", None).unwrap_err();
        assert!(err.to_string().contains("invalid JSON"), "err: {err}");
    }

    #[test]
    fn parse_chat_response_missing_content_errors() {
        let body = r#"{"stop_reason": "end_turn"}"#;
        let err = parse_chat_response_json(body, "m", None).unwrap_err();
        assert!(err.to_string().contains("content"), "err: {err}");
    }
}
