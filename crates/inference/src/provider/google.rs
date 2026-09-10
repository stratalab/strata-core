//! Google (Gemini) cloud generation provider.
//!
//! Sends generation requests to
//! `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
//! and maps the response to [`GenerateResponse`].
//!
//! The API key is passed via the `x-goog-api-key` header for security
//! (avoids leaking credentials in URL logs).

use std::collections::HashMap;

use crate::provider::cloud::{post_json, reject_local_only, CloudPost};
use crate::wire::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, FinishReason, FunctionDef, LogProbs,
    NamedToolChoice, ResponseFormat, Role, TokenLogProb, Tool, ToolCall, ToolCallFunction,
    ToolChoice, ToolChoiceMode, TopLogProb, Usage,
};
use crate::{GenerateRequest, GenerateResponse, InferenceError, StopReason};

/// The origin every Gemini model endpoint hangs off when nothing overrides
/// it. The `/v1beta/models` segments belong to the endpoint path, as in the
/// Google GenAI SDK, so a base URL written for the SDK works here unchanged.
pub(crate) const API_BASE: &str = crate::settings::GOOGLE_BASE_URL;

/// Google cloud provider state.
pub(crate) struct GoogleProvider {
    api_key: String,
    model: String,
    /// Where requests go: [`API_BASE`], or the base URL the runtime's
    /// settings override it with.
    api_base: String,
}

impl std::fmt::Debug for GoogleProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleProvider")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl GoogleProvider {
    pub(crate) fn new(api_key: String, model: String) -> Result<Self, InferenceError> {
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
        Ok(Self {
            api_key,
            model,
            api_base: API_BASE.to_string(),
        })
    }

    /// Point requests at `base_url` instead of the public API: a proxy, or a
    /// test's canned-response server. The `/v1beta/models/…` path is appended.
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

        let body = build_request_json(request);
        self.post(body)
    }

    /// Generate from an OpenAI-shaped chat request, mapping messages natively:
    /// system prompt hoisted to `system_instruction`, multi-turn preserved,
    /// assistant `tool_calls` rendered as `functionCall` parts, `tool` results
    /// batched into user turns of `functionResponse` parts (name recovered from
    /// the id→name correlation map), tools/tool_choice/response_format/logprobs
    /// forwarded, and the native response (text + tool calls + logprobs) parsed
    /// back into the [`ChatResponse`].
    pub(crate) fn generate_chat(
        &self,
        request: &ChatRequest,
    ) -> Result<ChatResponse, InferenceError> {
        let body = build_chat_request_json(request)?;
        let raw = self.send(body)?;
        parse_chat_response_json(&raw, &self.model)
    }

    /// Send a prepared completion request body and parse it as a completion.
    fn post(&self, body: String) -> Result<GenerateResponse, InferenceError> {
        parse_response_json(&self.send(body)?)
    }

    /// POST a prepared JSON body to the `generateContent` endpoint and return
    /// the raw response body (shared by the completion and chat parsers). The
    /// URL carries the model name, so it is built here from `self.model`.
    fn send(&self, body: String) -> Result<String, InferenceError> {
        post_json(&CloudPost {
            provider: "Google",
            url: &build_url(&self.api_base, &self.model),
            headers: &[("x-goog-api-key", &self.api_key)],
            body: &body,
            timeout: std::time::Duration::from_secs(30),
        })
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }
}

/// Build the full URL with the model name (API key sent via header).
pub(crate) fn build_url(api_base: &str, model: &str) -> String {
    model_endpoint(api_base, model, "generateContent")
}

/// One model method's endpoint under `api_base`:
/// `{api_base}/v1beta/models/{model}:{method}`.
fn model_endpoint(api_base: &str, model: &str, method: &str) -> String {
    format!("{api_base}/v1beta/models/{}:{method}", model_path(model))
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

/// Build the Google Gemini API request JSON from an OpenAI-shaped chat request.
///
/// System-role turns are hoisted to `system_instruction` (joined with "\n");
/// user turns become `user` `contents` and assistant turns become `model`
/// `contents`. An assistant turn carrying `tool_calls` is rendered as
/// `functionCall` parts, and consecutive `tool` results are batched into a
/// single user turn of `functionResponse` parts.
///
/// `tools`/`tool_choice` map to Gemini's `functionDeclarations`/`toolConfig`;
/// `response_format` maps to `responseMimeType` (+ `responseSchema` for
/// json_schema); `logprobs`/`top_logprobs` map to `responseLogprobs`/`logprobs`.
/// Supported sampling knobs (`max_tokens`, `temperature`, `top_p`, `top_k`,
/// `stop`) are forwarded when set. Seed, penalties, logit_bias, and the
/// llama.cpp extensions have no Gemini equivalent and are ignored.
///
/// **Impedance mismatch:** OpenAI keys tool results by call `id`, but Gemini
/// keys `functionResponse` by function *name*. Messages are iterated directly
/// (not via the lossy `chat_turns`, which drops `tool_calls`/`tool_call_id`) so
/// an id→name map can be built from assistant `tool_calls` and used to recover
/// each `tool` result's function name.
pub(crate) fn build_chat_request_json(request: &ChatRequest) -> Result<String, InferenceError> {
    reject_local_only(request, "Google")?;

    // A raw `prompt` is a single user turn; otherwise iterate the full messages
    // directly to preserve tool_calls/tool_call_id (chat_turns is lossy).
    let prompt_fallback: Vec<ChatMessage>;
    let messages_in: &[ChatMessage] = if let Some(msgs) = &request.messages {
        msgs.as_slice()
    } else if let Some(prompt) = &request.prompt {
        prompt_fallback = vec![ChatMessage::new(Role::User, prompt.clone())];
        prompt_fallback.as_slice()
    } else {
        &[]
    };

    let (system, contents) = build_google_contents(messages_in)?;

    let mut gen_config = serde_json::json!({});
    if let Some(max_tokens) = request.max_tokens {
        gen_config["maxOutputTokens"] = serde_json::json!(max_tokens);
    }
    if let Some(temperature) = request.temperature {
        gen_config["temperature"] = serde_json::json!(temperature);
    }
    if let Some(top_p) = request.top_p {
        gen_config["topP"] = serde_json::json!(top_p);
    }
    if let Some(top_k) = request.top_k {
        gen_config["topK"] = serde_json::json!(top_k);
    }
    if let Some(stop) = &request.stop {
        if !stop.is_empty() {
            gen_config["stopSequences"] = serde_json::json!(stop);
        }
    }
    apply_response_format(&mut gen_config, request);
    apply_logprobs(&mut gen_config, request);

    // Default: disable thinking. Without this, gemini-2.5 spends the whole
    // budget on internal reasoning and returns no text; a `thinking` knob is a
    // later phase.
    gen_config["thinkingConfig"] = serde_json::json!({"thinkingBudget": 0});

    let mut obj = serde_json::json!({
        "contents": contents,
        "generationConfig": gen_config,
    });
    if !system.is_empty() {
        obj["system_instruction"] = serde_json::json!({"parts": [{"text": system}]});
    }
    if let Some(tools) = build_tools_json(request) {
        obj["tools"] = tools;
    }
    if let Some(tool_config) = build_tool_config_json(request.tool_choice.as_ref()) {
        obj["toolConfig"] = tool_config;
    }

    Ok(obj.to_string())
}

/// Map OpenAI-shaped chat turns into Gemini's `(system_instruction, contents)`
/// pair. System turns are joined (with "\n"); user turns become `user`
/// contents; assistant turns become `model` contents (`functionCall` parts when
/// they carry `tool_calls`); and consecutive `tool` results are batched into a
/// single `user` content of `functionResponse` parts.
///
/// The function name of a `functionResponse` is recovered from the assistant
/// `tool_calls` seen earlier via a `tool_call_id → name` map. A `tool` message
/// with a missing or unrecognized `tool_call_id` is an error.
fn build_google_contents(
    messages_in: &[ChatMessage],
) -> Result<(String, Vec<serde_json::Value>), InferenceError> {
    let mut system = String::new();
    let mut contents: Vec<serde_json::Value> = Vec::new();
    // tool_call_id → function name, populated from assistant tool_calls and read
    // back when a `tool` result must be keyed by name for Gemini.
    let mut call_names: HashMap<String, String> = HashMap::new();
    // Consecutive `tool` results are delivered as a single user turn.
    let mut pending_tool_results: Vec<serde_json::Value> = Vec::new();

    for message in messages_in {
        // Any non-tool message closes an open run of tool results.
        if message.role != Role::Tool && !pending_tool_results.is_empty() {
            contents.push(serde_json::json!({
                "role": "user",
                "parts": serde_json::Value::Array(std::mem::take(&mut pending_tool_results)),
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
                contents.push(serde_json::json!({
                    "role": "user",
                    "parts": [{"text": message.content}],
                }));
            }
            Role::Assistant => {
                if let Some(calls) = &message.tool_calls {
                    for call in calls {
                        let ToolCall::Function { id, function } = call;
                        call_names.insert(id.clone(), function.name.clone());
                    }
                }
                contents.push(model_message_json(message));
            }
            Role::Tool => {
                let call_id = message.tool_call_id.as_deref().ok_or_else(|| {
                    InferenceError::Provider(
                        "Google: `tool` message is missing `tool_call_id`".to_string(),
                    )
                })?;
                let name = call_names.get(call_id).ok_or_else(|| {
                    InferenceError::Provider(format!(
                        "Google: `tool` message references unknown tool_call_id `{call_id}`; no \
                         preceding assistant tool_call declared it"
                    ))
                })?;
                pending_tool_results.push(function_response_part(name, &message.content));
            }
        }
    }

    // Flush a trailing run of tool results.
    if !pending_tool_results.is_empty() {
        contents.push(serde_json::json!({
            "role": "user",
            "parts": serde_json::Value::Array(pending_tool_results),
        }));
    }

    Ok((system, contents))
}

/// Render an assistant turn as a Gemini `model` content. A turn with
/// `tool_calls` becomes a parts array (an optional leading `text` part, then one
/// `functionCall` part per call); otherwise a plain-text `model` content. The
/// `arguments` JSON string is parsed into an object for `args` (falling back to
/// `{}` when it does not parse).
fn model_message_json(message: &ChatMessage) -> serde_json::Value {
    let tool_calls = match &message.tool_calls {
        Some(calls) if !calls.is_empty() => calls,
        _ => {
            return serde_json::json!({
                "role": "model",
                "parts": [{"text": message.content}],
            });
        }
    };

    let mut parts: Vec<serde_json::Value> = Vec::new();
    // Omit the text part for a pure tool-call turn.
    if !message.content.is_empty() {
        parts.push(serde_json::json!({"text": message.content}));
    }
    for call in tool_calls {
        let ToolCall::Function { id: _, function } = call;
        let args: serde_json::Value =
            serde_json::from_str(&function.arguments).unwrap_or_else(|_| serde_json::json!({}));
        parts.push(serde_json::json!({
            "functionCall": {
                "name": function.name,
                "args": args,
            }
        }));
    }
    serde_json::json!({
        "role": "model",
        "parts": serde_json::Value::Array(parts),
    })
}

/// Build a Gemini `functionResponse` part. The tool-result content is parsed as
/// a JSON object and used verbatim; anything that is not a JSON object (or does
/// not parse) is wrapped as `{"result": <content>}`.
fn function_response_part(name: &str, content: &str) -> serde_json::Value {
    let response = match serde_json::from_str::<serde_json::Value>(content) {
        Ok(value @ serde_json::Value::Object(_)) => value,
        _ => serde_json::json!({ "result": content }),
    };
    serde_json::json!({
        "functionResponse": {
            "name": name,
            "response": response,
        }
    })
}

/// Apply `response_format` to `generationConfig`: `json_object` sets
/// `responseMimeType`; `json_schema` sets `responseMimeType` + `responseSchema`;
/// text/none add nothing.
fn apply_response_format(gen_config: &mut serde_json::Value, request: &ChatRequest) {
    match &request.response_format {
        Some(ResponseFormat::JsonObject) => {
            gen_config["responseMimeType"] = serde_json::json!("application/json");
        }
        Some(ResponseFormat::JsonSchema { json_schema }) => {
            gen_config["responseMimeType"] = serde_json::json!("application/json");
            gen_config["responseSchema"] = json_schema.schema.clone();
        }
        Some(ResponseFormat::Text) | None => {}
    }
}

/// Apply log-probability knobs to `generationConfig`: `logprobs == Some(true)`
/// sets `responseLogprobs`, and `top_logprobs` sets Gemini's `logprobs` count.
fn apply_logprobs(gen_config: &mut serde_json::Value, request: &ChatRequest) {
    if request.logprobs == Some(true) {
        gen_config["responseLogprobs"] = serde_json::json!(true);
        if let Some(top_logprobs) = request.top_logprobs {
            gen_config["logprobs"] = serde_json::json!(top_logprobs);
        }
    }
}

/// Build Gemini's `tools` value: a single-element array holding one
/// `functionDeclarations` list for all offered functions. Returns `None` when
/// the request offers no tools.
fn build_tools_json(request: &ChatRequest) -> Option<serde_json::Value> {
    let tools = request.tools.as_ref()?;
    if tools.is_empty() {
        return None;
    }
    let declarations: Vec<serde_json::Value> = tools
        .iter()
        .map(|tool| {
            let Tool::Function { function } = tool;
            function_declaration_json(function)
        })
        .collect();
    Some(serde_json::json!([{ "functionDeclarations": declarations }]))
}

/// Map a [`FunctionDef`] to a Gemini `functionDeclarations` entry (`name`,
/// optional `description`, and `parameters`). A missing parameter schema becomes
/// a bare object schema.
fn function_declaration_json(function: &FunctionDef) -> serde_json::Value {
    let parameters = function
        .parameters
        .clone()
        .unwrap_or_else(|| serde_json::json!({ "type": "object" }));
    let mut decl = serde_json::json!({
        "name": function.name,
        "parameters": parameters,
    });
    if let Some(description) = &function.description {
        decl["description"] = serde_json::json!(description);
    }
    decl
}

/// Map an OpenAI-shaped [`ToolChoice`] to Gemini's `toolConfig`: `auto`→AUTO,
/// `required`→ANY, `none`→NONE, and a named function→ANY with
/// `allowedFunctionNames`. Returns `None` when no tool_choice is set.
fn build_tool_config_json(choice: Option<&ToolChoice>) -> Option<serde_json::Value> {
    let config = match choice? {
        ToolChoice::Mode(ToolChoiceMode::Auto) => serde_json::json!({ "mode": "AUTO" }),
        ToolChoice::Mode(ToolChoiceMode::Required) => serde_json::json!({ "mode": "ANY" }),
        ToolChoice::Mode(ToolChoiceMode::None) => serde_json::json!({ "mode": "NONE" }),
        ToolChoice::Named(NamedToolChoice::Function { function }) => serde_json::json!({
            "mode": "ANY",
            "allowedFunctionNames": [function.name],
        }),
    };
    Some(serde_json::json!({ "functionCallingConfig": config }))
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

/// Parse the Google Gemini `generateContent` response JSON into a rich
/// [`ChatResponse`] (concatenated text, tool calls, finish reason,
/// log-probabilities, and usage).
///
/// `functionCall` parts become [`ToolCall`]s with synthesized ids (`call_0`, …,
/// Gemini assigns none), and their presence forces `finish_reason =
/// ToolCalls`. The model is always the caller's `fallback_model`. API-error
/// bodies map to `Err`.
pub(crate) fn parse_chat_response_json(
    body: &str,
    fallback_model: &str,
) -> Result<ChatResponse, InferenceError> {
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

    let candidates = json
        .get("candidates")
        .and_then(|c| c.as_array())
        .ok_or_else(|| {
            InferenceError::Provider("Google: missing or invalid 'candidates' array".to_string())
        })?;
    let candidate = candidates.first().ok_or_else(|| {
        InferenceError::Provider("Google: empty candidates array in response".to_string())
    })?;

    let parts = candidate
        .get("content")
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.as_array())
        .map(Vec::as_slice);
    let (content, tool_calls) = parse_candidate_parts(parts);

    // Gemini returns finishReason "STOP" even when it emits function calls, so
    // the presence of tool calls decides ToolCalls regardless of finishReason.
    let finish_reason = if tool_calls.is_some() {
        FinishReason::ToolCalls
    } else {
        match candidate.get("finishReason").and_then(|r| r.as_str()) {
            Some("STOP") => FinishReason::Stop,
            Some("MAX_TOKENS") => FinishReason::Length,
            Some("SAFETY" | "RECITATION") => FinishReason::ContentFilter,
            Some(other) => {
                tracing::warn!(reason = ?other, "Unknown finish reason from Google, defaulting to Stop");
                FinishReason::Stop
            }
            None => FinishReason::Stop,
        }
    };

    let logprobs = parse_logprobs(candidate.get("logprobsResult"));

    let usage = json.get("usageMetadata");
    let prompt_tokens = usage
        .and_then(|u| u.get("promptTokenCount"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;
    let completion_tokens = usage
        .and_then(|u| u.get("candidatesTokenCount"))
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
            logprobs,
        }],
        usage: Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        },
    })
}

/// Split a candidate's `parts` array into concatenated text and tool calls.
/// Gemini assigns no call id, so ids are synthesized by tool-call index
/// (`call_0`, `call_1`, …).
fn parse_candidate_parts(parts: Option<&[serde_json::Value]>) -> (String, Option<Vec<ToolCall>>) {
    let mut content = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    for part in parts.into_iter().flatten() {
        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
            content.push_str(text);
        } else if let Some(call) = part.get("functionCall") {
            let name = call
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let args = call
                .get("args")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let arguments = serde_json::to_string(&args).unwrap_or_else(|_| "{}".to_string());
            let id = format!("call_{}", tool_calls.len());
            tool_calls.push(ToolCall::Function {
                id,
                function: ToolCallFunction { name, arguments },
            });
        }
    }
    let tool_calls = (!tool_calls.is_empty()).then_some(tool_calls);
    (content, tool_calls)
}

/// Parse Gemini's `logprobsResult` into [`LogProbs`] (best-effort; `None` when
/// absent or malformed). `chosenCandidates` become the per-token entries, and
/// the matching `topCandidates[i].candidates` become each token's alternatives.
fn parse_logprobs(value: Option<&serde_json::Value>) -> Option<LogProbs> {
    let result = value?;
    let chosen = result.get("chosenCandidates").and_then(|c| c.as_array())?;
    let top = result.get("topCandidates").and_then(|c| c.as_array());
    let content = chosen
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let top_logprobs = top
                .and_then(|t| t.get(i))
                .and_then(|c| c.get("candidates"))
                .and_then(|c| c.as_array())
                .map(|alts| {
                    alts.iter()
                        .map(|alt| TopLogProb {
                            token: gemini_token(alt),
                            logprob: gemini_logprob(alt),
                            bytes: None,
                        })
                        .collect()
                })
                .unwrap_or_default();
            TokenLogProb {
                token: gemini_token(entry),
                logprob: gemini_logprob(entry),
                bytes: None,
                top_logprobs,
            }
        })
        .collect();
    Some(LogProbs { content })
}

fn gemini_token(entry: &serde_json::Value) -> String {
    entry
        .get("token")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string()
}

fn gemini_logprob(entry: &serde_json::Value) -> f32 {
    entry
        .get("logProbability")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0) as f32
}

// =========================================================================
// Embedding API
// =========================================================================

/// Build the URL for the Google embedContent API (single text).
pub(crate) fn build_embed_url(api_base: &str, model: &str) -> String {
    model_endpoint(api_base, model, "embedContent")
}

/// Build the URL for the Google batchEmbedContents API (multiple texts).
pub(crate) fn build_batch_embed_url(api_base: &str, model: &str) -> String {
    model_endpoint(api_base, model, "batchEmbedContents")
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
    let model = format!("models/{}", model_name(model));
    let requests: Vec<serde_json::Value> = texts
        .iter()
        .map(|text| {
            serde_json::json!({
                "model": model,
                "content": {
                    "parts": [{"text": text}]
                }
            })
        })
        .collect();

    serde_json::json!({ "requests": requests }).to_string()
}

fn model_path(model: &str) -> &str {
    model.strip_prefix("models/").unwrap_or(model)
}

fn model_name(model: &str) -> &str {
    model.strip_prefix("models/").unwrap_or(model)
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
        let url = build_url(API_BASE, "gemini-pro");
        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
            "the versioned model path under the origin"
        );
        assert!(!url.contains("key="), "API key should not appear in URL");
        assert_eq!(
            build_url("http://127.0.0.1:8000", "models/gemini-pro"),
            "http://127.0.0.1:8000/v1beta/models/gemini-pro:generateContent",
            "an override is rooted the same way and a `models/` prefix folds in"
        );
    }

    #[test]
    fn url_has_no_query_params() {
        let url = build_url(API_BASE, "model");
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
    // Chat request JSON building (Phase C)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_system_hoisted_and_assistant_is_model() {
        let req = ChatRequest {
            messages: Some(vec![
                crate::wire::ChatMessage::new(Role::System, "sys"),
                crate::wire::ChatMessage::new(Role::User, "u"),
                crate::wire::ChatMessage::new(Role::Assistant, "a"),
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();

        assert_eq!(json["system_instruction"]["parts"][0]["text"], "sys");
        let contents = json["contents"].as_array().unwrap();
        assert_eq!(contents.len(), 2, "system is not a content turn");
        assert_eq!(contents[0]["role"], "user");
        assert_eq!(contents[0]["parts"][0]["text"], "u");
        // Assistant maps to Gemini's `model` role.
        assert_eq!(contents[1]["role"], "model");
        assert_eq!(contents[1]["parts"][0]["text"], "a");
    }

    #[test]
    fn chat_json_prompt_becomes_single_user_turn() {
        let req = ChatRequest {
            prompt: Some("just this".into()),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let contents = json["contents"].as_array().unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0]["role"], "user");
        assert_eq!(contents[0]["parts"][0]["text"], "just this");
        assert!(json.get("system_instruction").is_none());
    }

    #[test]
    fn chat_json_does_not_force_top_k_and_forwards_user_knobs() {
        // Gemini must NOT invent a topK, and must forward the caller's knobs.
        let req = ChatRequest {
            prompt: Some("hi".into()),
            max_tokens: Some(128),
            temperature: Some(0.3),
            top_p: Some(0.8),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let cfg = &json["generationConfig"];
        assert!(cfg.get("topK").is_none(), "topK must not be forced: {json}");
        assert_eq!(cfg["maxOutputTokens"], 128);
        let temp = cfg["temperature"].as_f64().unwrap();
        assert!((temp - 0.3).abs() < 1e-6);
        let top_p = cfg["topP"].as_f64().unwrap();
        assert!((top_p - 0.8).abs() < 1e-6);
        // Thinking is disabled by default so 2.5 models return text.
        assert_eq!(cfg["thinkingConfig"]["thinkingBudget"], 0);
    }

    #[test]
    fn chat_json_top_k_forwarded_when_set() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            top_k: Some(20),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        assert_eq!(json["generationConfig"]["topK"], 20);
    }

    #[test]
    fn chat_json_response_format_sets_mime() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::JsonObject),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        assert_eq!(
            json["generationConfig"]["responseMimeType"],
            "application/json"
        );
    }

    #[test]
    fn chat_json_stop_sequences_forwarded() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            stop: Some(vec!["END".into()]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        assert_eq!(json["generationConfig"]["stopSequences"][0], "END");
    }

    #[test]
    fn chat_json_grammar_rejected() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            grammar: Some("g".into()),
            ..Default::default()
        };
        let err = build_chat_request_json(&req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    #[test]
    fn chat_json_tool_message_errors() {
        // A bare `tool` message has no `tool_call_id` to correlate → error.
        let req = ChatRequest {
            messages: Some(vec![crate::wire::ChatMessage::new(Role::Tool, "r")]),
            ..Default::default()
        };
        let err = build_chat_request_json(&req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
    }

    // -----------------------------------------------------------------------
    // Chat request: tools / tool_choice (G3b)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_tools_become_function_declarations() {
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
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        // One `tools` entry holding a single functionDeclarations array.
        let tools = json["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        let decls = tools[0]["functionDeclarations"].as_array().unwrap();
        assert_eq!(decls.len(), 2);
        assert_eq!(decls[0]["name"], "get_weather");
        assert_eq!(decls[0]["description"], "look up weather");
        assert_eq!(
            decls[0]["parameters"]["properties"]["city"]["type"],
            "string"
        );
        // Gemini uses `parameters`, never OpenAI's nested `function` wrapper.
        assert!(tools[0].get("type").is_none());
        // No-params fn defaults to a bare object schema; no description key.
        assert_eq!(decls[1]["name"], "no_params");
        assert!(decls[1].get("description").is_none());
        assert_eq!(
            decls[1]["parameters"],
            serde_json::json!({ "type": "object" })
        );
    }

    #[test]
    fn chat_json_tool_config_all_modes() {
        use crate::wire::ToolChoiceFunction;
        let config_json = |tc: ToolChoice| -> serde_json::Value {
            let req = ChatRequest {
                prompt: Some("hi".into()),
                tool_choice: Some(tc),
                ..Default::default()
            };
            serde_json::from_str::<serde_json::Value>(&build_chat_request_json(&req).unwrap())
                .unwrap()["toolConfig"]["functionCallingConfig"]
                .clone()
        };
        assert_eq!(
            config_json(ToolChoice::Mode(ToolChoiceMode::Auto))["mode"],
            "AUTO"
        );
        assert_eq!(
            config_json(ToolChoice::Mode(ToolChoiceMode::Required))["mode"],
            "ANY"
        );
        assert_eq!(
            config_json(ToolChoice::Mode(ToolChoiceMode::None))["mode"],
            "NONE"
        );
        let named = config_json(ToolChoice::Named(NamedToolChoice::Function {
            function: ToolChoiceFunction {
                name: "get_weather".into(),
            },
        }));
        assert_eq!(named["mode"], "ANY");
        assert_eq!(named["allowedFunctionNames"][0], "get_weather");
    }

    // -----------------------------------------------------------------------
    // Chat request: tool round-trip (G3b)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_assistant_tool_calls_become_function_call_parts() {
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
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        assert_eq!(json["contents"][1]["role"], "model");
        let parts = json["contents"][1]["parts"].as_array().unwrap();
        // Text part first (content non-empty), then the functionCall part.
        assert_eq!(parts[0]["text"], "let me check");
        assert_eq!(parts[1]["functionCall"]["name"], "get_weather");
        // The arguments string is parsed into a JSON object for `args`.
        assert_eq!(parts[1]["functionCall"]["args"]["city"], "Paris");
    }

    #[test]
    fn chat_json_assistant_pure_tool_call_omits_text_and_tolerates_bad_arguments() {
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: String::new(),
            name: None,
            tool_calls: Some(vec![ToolCall::Function {
                id: "c".into(),
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
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let parts = json["contents"][0]["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 1, "no text part for an empty-content turn");
        // Unparseable arguments fall back to an empty object.
        assert_eq!(parts[0]["functionCall"]["args"], serde_json::json!({}));
    }

    #[test]
    fn chat_json_tool_result_correlates_name_by_id() {
        let assistant = ChatMessage {
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
        };
        let tool = ChatMessage {
            role: Role::Tool,
            content: r#"{"temp":20}"#.into(),
            name: None,
            tool_calls: None,
            tool_call_id: Some("call_1".into()),
        };
        let req = ChatRequest {
            messages: Some(vec![
                ChatMessage::new(Role::User, "weather?"),
                assistant,
                tool,
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let contents = json["contents"].as_array().unwrap();
        // user, model(functionCall), user(functionResponse).
        assert_eq!(contents.len(), 3);
        assert_eq!(contents[2]["role"], "user");
        let response = &contents[2]["parts"][0]["functionResponse"];
        // Name recovered from the id→name map, not from the tool message.
        assert_eq!(response["name"], "get_weather");
        // A JSON-object result is used verbatim.
        assert_eq!(response["response"]["temp"], 20);
    }

    #[test]
    fn chat_json_tool_result_non_object_wrapped() {
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: String::new(),
            name: None,
            tool_calls: Some(vec![ToolCall::Function {
                id: "c1".into(),
                function: ToolCallFunction {
                    name: "f".into(),
                    arguments: "{}".into(),
                },
            }]),
            tool_call_id: None,
        };
        let tool = ChatMessage {
            role: Role::Tool,
            content: "plain text".into(),
            name: None,
            tool_calls: None,
            tool_call_id: Some("c1".into()),
        };
        let req = ChatRequest {
            messages: Some(vec![assistant, tool]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let response = &json["contents"][1]["parts"][0]["functionResponse"];
        assert_eq!(response["name"], "f");
        // Non-object content is wrapped under `result`.
        assert_eq!(response["response"]["result"], "plain text");
    }

    #[test]
    fn chat_json_consecutive_tool_results_batched() {
        let assistant = ChatMessage {
            role: Role::Assistant,
            content: String::new(),
            name: None,
            tool_calls: Some(vec![
                ToolCall::Function {
                    id: "call_a".into(),
                    function: ToolCallFunction {
                        name: "wa".into(),
                        arguments: "{}".into(),
                    },
                },
                ToolCall::Function {
                    id: "call_b".into(),
                    function: ToolCallFunction {
                        name: "wb".into(),
                        arguments: "{}".into(),
                    },
                },
            ]),
            tool_call_id: None,
        };
        let tool = |id: &str, content: &str| ChatMessage {
            role: Role::Tool,
            content: content.into(),
            name: None,
            tool_calls: None,
            tool_call_id: Some(id.into()),
        };
        let req = ChatRequest {
            messages: Some(vec![
                ChatMessage::new(Role::User, "?"),
                assistant,
                tool("call_a", r#"{"v":1}"#),
                tool("call_b", r#"{"v":2}"#),
                ChatMessage::new(Role::User, "thanks"),
            ]),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let contents = json["contents"].as_array().unwrap();
        // user, model(functionCall x2), user(functionResponse x2), user("thanks").
        assert_eq!(contents.len(), 4);
        assert_eq!(contents[2]["role"], "user");
        let responses = contents[2]["parts"].as_array().unwrap();
        assert_eq!(responses.len(), 2, "both tool results in one user turn");
        assert_eq!(responses[0]["functionResponse"]["name"], "wa");
        assert_eq!(responses[1]["functionResponse"]["name"], "wb");
        // The trailing user message flushes the tool-result run into its own turn.
        assert_eq!(contents[3]["role"], "user");
        assert_eq!(contents[3]["parts"][0]["text"], "thanks");
    }

    #[test]
    fn chat_json_tool_result_unknown_id_errors() {
        let tool = ChatMessage {
            role: Role::Tool,
            content: "{}".into(),
            name: None,
            tool_calls: None,
            tool_call_id: Some("nope".into()),
        };
        let req = ChatRequest {
            messages: Some(vec![ChatMessage::new(Role::User, "hi"), tool]),
            ..Default::default()
        };
        let err = build_chat_request_json(&req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
        assert!(err.to_string().contains("nope"), "err: {err}");
    }

    #[test]
    fn chat_json_tool_result_missing_id_errors() {
        let req = ChatRequest {
            messages: Some(vec![ChatMessage {
                role: Role::Tool,
                content: "r".into(),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            }]),
            ..Default::default()
        };
        let err = build_chat_request_json(&req).unwrap_err();
        assert!(matches!(err, InferenceError::Provider(_)), "err: {err}");
        assert!(err.to_string().contains("tool_call_id"), "err: {err}");
    }

    // -----------------------------------------------------------------------
    // Chat request: structured outputs / logprobs (G3b)
    // -----------------------------------------------------------------------

    #[test]
    fn chat_json_response_format_json_schema_sets_response_schema() {
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
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let cfg = &json["generationConfig"];
        assert_eq!(cfg["responseMimeType"], "application/json");
        assert_eq!(cfg["responseSchema"]["type"], "object");
        assert_eq!(
            cfg["responseSchema"]["properties"]["name"]["type"],
            "string"
        );
    }

    #[test]
    fn chat_json_json_object_sets_mime_only() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            response_format: Some(ResponseFormat::JsonObject),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let cfg = &json["generationConfig"];
        assert_eq!(cfg["responseMimeType"], "application/json");
        assert!(cfg.get("responseSchema").is_none());
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
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let cfg = &json["generationConfig"];
        assert_eq!(cfg["responseLogprobs"], true);
        assert_eq!(cfg["logprobs"], 5);
    }

    #[test]
    fn chat_json_logprobs_true_without_top_logprobs() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            logprobs: Some(true),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        let cfg = &json["generationConfig"];
        assert_eq!(cfg["responseLogprobs"], true);
        assert!(cfg.get("logprobs").is_none());
    }

    #[test]
    fn chat_json_advanced_knobs_omitted_when_unset() {
        let req = ChatRequest {
            prompt: Some("hi".into()),
            ..Default::default()
        };
        let json: serde_json::Value =
            serde_json::from_str(&build_chat_request_json(&req).unwrap()).unwrap();
        assert!(json.get("tools").is_none());
        assert!(json.get("toolConfig").is_none());
        let cfg = &json["generationConfig"];
        assert!(cfg.get("responseMimeType").is_none());
        assert!(cfg.get("responseSchema").is_none());
        assert!(cfg.get("responseLogprobs").is_none());
        assert!(cfg.get("logprobs").is_none());
    }

    // -----------------------------------------------------------------------
    // Chat response parsing (G3b)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_chat_response_text() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "Hello world"}]},
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 2}
        }"#;
        let resp = parse_chat_response_json(body, "gemini-pro").unwrap();
        assert_eq!(resp.model, "gemini-pro");
        assert_eq!(resp.choices[0].message.content, "Hello world");
        assert_eq!(resp.choices[0].finish_reason, FinishReason::Stop);
        assert!(resp.choices[0].message.tool_calls.is_none());
        assert!(resp.choices[0].logprobs.is_none());
        assert_eq!(resp.usage.total_tokens, 7);
    }

    #[test]
    fn parse_chat_response_function_calls() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [
                    {"functionCall": {"name": "get_weather", "args": {"city": "Paris"}}},
                    {"functionCall": {"name": "get_time", "args": {}}}
                ]},
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 8}
        }"#;
        let resp = parse_chat_response_json(body, "gemini-pro").unwrap();
        // functionCall parts force ToolCalls even though Gemini reports STOP.
        assert_eq!(resp.choices[0].finish_reason, FinishReason::ToolCalls);
        let calls = resp.choices[0].message.tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 2);
        let ToolCall::Function { id, function } = &calls[0];
        assert_eq!(id, "call_0", "ids synthesized by index");
        assert_eq!(function.name, "get_weather");
        assert!(function.arguments.contains("Paris"));
        let ToolCall::Function { id: id1, .. } = &calls[1];
        assert_eq!(id1, "call_1");
        // No text parts → empty content.
        assert_eq!(resp.choices[0].message.content, "");
    }

    #[test]
    fn parse_chat_response_text_and_function_call() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [
                    {"text": "let me check"},
                    {"functionCall": {"name": "get_weather", "args": {"city": "Paris"}}}
                ]},
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 4}
        }"#;
        let resp = parse_chat_response_json(body, "m").unwrap();
        assert_eq!(resp.choices[0].message.content, "let me check");
        let calls = resp.choices[0].message.tool_calls.as_ref().unwrap();
        // Ids are indexed by tool-call count, so a leading text part is call_0.
        let ToolCall::Function { id, .. } = &calls[0];
        assert_eq!(id, "call_0");
        assert_eq!(resp.choices[0].finish_reason, FinishReason::ToolCalls);
    }

    #[test]
    fn parse_chat_response_finish_reasons() {
        let finish = |reason: &str| {
            let body = format!(
                r#"{{"candidates":[{{"content":{{"parts":[{{"text":"x"}}]}},"finishReason":"{reason}"}}],"usageMetadata":{{"promptTokenCount":1,"candidatesTokenCount":1}}}}"#
            );
            parse_chat_response_json(&body, "m").unwrap().choices[0].finish_reason
        };
        assert_eq!(finish("STOP"), FinishReason::Stop);
        assert_eq!(finish("MAX_TOKENS"), FinishReason::Length);
        assert_eq!(finish("SAFETY"), FinishReason::ContentFilter);
        assert_eq!(finish("RECITATION"), FinishReason::ContentFilter);
        assert_eq!(finish("SOME_FUTURE_REASON"), FinishReason::Stop);
    }

    #[test]
    fn parse_chat_response_logprobs() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hi"}]},
                "finishReason": "STOP",
                "logprobsResult": {
                    "chosenCandidates": [
                        {"token": "hi", "logProbability": -0.05}
                    ],
                    "topCandidates": [
                        {"candidates": [
                            {"token": "hi", "logProbability": -0.05},
                            {"token": "hey", "logProbability": -2.3}
                        ]}
                    ]
                }
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_chat_response_json(body, "m").unwrap();
        let lp = resp.choices[0].logprobs.as_ref().unwrap();
        assert_eq!(lp.content.len(), 1);
        assert_eq!(lp.content[0].token, "hi");
        assert!((lp.content[0].logprob + 0.05).abs() < 1e-6);
        assert_eq!(lp.content[0].top_logprobs.len(), 2);
        assert_eq!(lp.content[0].top_logprobs[1].token, "hey");
        assert!((lp.content[0].top_logprobs[1].logprob + 2.3).abs() < 1e-6);
    }

    #[test]
    fn parse_chat_response_logprobs_absent_is_none() {
        let body = r#"{
            "candidates": [{
                "content": {"parts": [{"text": "hi"}]},
                "finishReason": "STOP"
            }],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1}
        }"#;
        let resp = parse_chat_response_json(body, "m").unwrap();
        assert!(resp.choices[0].logprobs.is_none());
    }

    #[test]
    fn parse_chat_response_api_error() {
        let body = r#"{"error": {"code": 400, "message": "API key not valid"}}"#;
        let err = parse_chat_response_json(body, "gemini-pro").unwrap_err();
        assert!(err.to_string().contains("API key not valid"), "err: {err}");
        assert!(err.to_string().contains("400"), "err: {err}");
    }

    #[test]
    fn parse_chat_response_invalid_json() {
        let err = parse_chat_response_json("not json", "m").unwrap_err();
        assert!(err.to_string().contains("invalid JSON"), "err: {err}");
    }

    #[test]
    fn parse_chat_response_empty_candidates_errors() {
        let body = r#"{"candidates": []}"#;
        let err = parse_chat_response_json(body, "m").unwrap_err();
        assert!(err.to_string().contains("empty candidates"), "err: {err}");
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
    // The request path, end to end against a local stand-in for the API
    // -----------------------------------------------------------------------

    use crate::provider::cloud::test_server::CannedResponse;

    fn provider_at(server: &CannedResponse) -> GoogleProvider {
        GoogleProvider::new("AIzaTestKey".into(), "gemini-test".into())
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

    /// The whole path works: the request reaches the model's endpoint with
    /// the credential in a header, not the URL, and a good answer parses.
    #[test]
    fn a_completion_round_trips_through_the_request_path() {
        let server = CannedResponse::serve(
            200,
            r#"{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}],
                "usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}"#,
        );
        let response = provider_at(&server)
            .generate(&short_request())
            .expect("a parsed completion");
        assert_eq!(response.text, "hello");

        let request = server.request();
        assert!(
            request.starts_with("POST /v1beta/models/gemini-test:generateContent "),
            "the model's versioned endpoint under the origin: {request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("x-goog-api-key: aizatestkey"),
            "the credential travels in x-goog-api-key: {request}"
        );
        assert!(
            !request.contains("key="),
            "the credential never rides in the URL: {request}"
        );
    }

    /// Google rejects a bad key with a 400 whose body says `API_KEY_INVALID`
    /// (#3236). By status alone it was "invalid request", which told the user
    /// to check their model name.
    #[test]
    fn a_rejected_key_is_auth_failed_not_an_invalid_request() {
        let server = CannedResponse::serve(
            400,
            r#"{"error":{"code":400,"message":"API key not valid. Please pass a valid API key.","status":"INVALID_ARGUMENT",
                "details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_INVALID","domain":"googleapis.com","metadata":{"service":"generativelanguage.googleapis.com"}},
                           {"@type":"type.googleapis.com/google.rpc.LocalizedMessage","locale":"en-US","message":"API key not valid. Please pass a valid API key."}]}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("rejected key");
        assert_eq!(error.code(), "inference.provider_auth_failed");
        assert!(
            error.to_string().contains("API key not valid"),
            "the provider's own explanation reaches the user: {error}"
        );
    }

    /// A model Google does not know is a 404 `NOT_FOUND` (#3236). By status
    /// alone it was "provider unavailable, retry".
    #[test]
    fn an_unknown_model_is_model_not_found_not_an_outage() {
        let server = CannedResponse::serve(
            404,
            r#"{"error":{"code":404,"message":"models/gemini-nope is not found for API version v1beta, or is not supported for generateContent. Call ModelService.ListModels to see the list of available models and their supported methods.","status":"NOT_FOUND"}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("unknown model");
        assert_eq!(error.code(), "inference.provider_model_not_found");
        assert!(
            error.to_string().contains("gemini-nope"),
            "names the model the provider rejected: {error}"
        );
    }

    /// Direction control: a genuine rate limit is still one. Google's
    /// `RESOURCE_EXHAUSTED` is its per-minute quota, which does reset.
    #[test]
    fn a_rate_limit_is_still_rate_limited() {
        let server = CannedResponse::serve(
            429,
            r#"{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}"#,
        );
        let error = provider_at(&server)
            .generate(&short_request())
            .expect_err("rate limited");
        assert_eq!(error.code(), "inference.provider_rate_limited");
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
        let url = build_embed_url(API_BASE, "text-embedding-004");
        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent"
        );
    }

    #[test]
    fn batch_embed_url_contains_model() {
        let url = build_batch_embed_url(API_BASE, "text-embedding-004");
        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents"
        );
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

    #[test]
    fn batch_embed_response_api_error() {
        let body = r#"{
            "error": {
                "code": 429,
                "message": "Resource exhausted"
            }
        }"#;
        let err = parse_batch_embed_response_json(body).unwrap_err();
        assert!(err.to_string().contains("Resource exhausted"));
        assert!(err.to_string().contains("429"));
    }
}
