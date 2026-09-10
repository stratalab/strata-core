//! Shared helpers for cloud providers: the one HTTP path every provider call
//! goes through, and the classifier that reads a provider's error body.
use std::time::Duration;

use crate::wire::ChatRequest;
use crate::{InferenceError, ProviderFailure};

/// Rejects knobs no cloud provider can honor. GBNF `grammar` is a llama.cpp
/// constraint; cloud callers should use `response_format` instead.
pub(crate) fn reject_local_only(
    request: &ChatRequest,
    provider: &str,
) -> Result<(), InferenceError> {
    if request.grammar.is_some() {
        return Err(InferenceError::Provider(format!(
            "{provider}: GBNF `grammar` is local-only; use `response_format` for cloud models"
        )));
    }
    Ok(())
}

/// One JSON POST to a cloud provider.
pub(crate) struct CloudPost<'a> {
    /// How diagnostics name the provider ("OpenAI").
    pub(crate) provider: &'a str,
    pub(crate) url: &'a str,
    /// The credential and any protocol headers; the content type is added here.
    pub(crate) headers: &'a [(&'a str, &'a str)],
    pub(crate) body: &'a str,
    pub(crate) timeout: Duration,
}

/// POST a JSON body and return the response body on success, or the failure
/// the provider described on anything else.
///
/// Non-2xx responses are read, not raised (#3236): ureq's default turns them
/// into a bare status before the body can be seen, and the body is where a
/// provider says *why* — no credits, unknown model, bad key — which a status
/// alone conflates with "wait and retry".
pub(crate) fn post_json(post: &CloudPost<'_>) -> Result<String, InferenceError> {
    let agent = ureq::Agent::new_with_config(
        ureq::config::Config::builder()
            .timeout_global(Some(post.timeout))
            .http_status_as_error(false)
            .build(),
    );
    let mut request = agent
        .post(post.url)
        .header("content-type", "application/json");
    for (name, value) in post.headers {
        request = request.header(*name, *value);
    }
    let mut response = request
        .send(post.body)
        .map_err(|e| transport_error(post.provider, e))?;

    let status = response.status().as_u16();
    let body = response.body_mut().read_to_string().map_err(|e| {
        InferenceError::Provider(format!("{}: failed to read response: {e}", post.provider))
    })?;
    if response.status().is_success() {
        Ok(body)
    } else {
        Err(provider_error(post.provider, status, &body))
    }
}

/// A failure the transport itself reported: nothing came back to read.
fn transport_error(provider: &str, err: ureq::Error) -> InferenceError {
    let kind = match err {
        // The transport already told us it timed out.
        ureq::Error::Timeout(_) => ProviderFailure::Timeout,
        // Statuses are never raised here (`http_status_as_error(false)`), so
        // everything else is the network: DNS, connect, TLS, protocol.
        _ => ProviderFailure::Unavailable,
    };
    InferenceError::ProviderFailed {
        kind,
        message: format!("{provider}: {err}"),
        details: None,
    }
}

/// The failure a non-2xx response describes, classified from its body first
/// and its status second, carrying the provider's own explanation.
pub(crate) fn provider_error(provider: &str, status: u16, body: &str) -> InferenceError {
    let described = ErrorBody::parse(body);
    let kind = classify(status, &described);
    let detail = described
        .message
        .unwrap_or_else(|| describe_status(status).to_string());
    InferenceError::ProviderFailed {
        kind,
        message: format!("{provider}: {detail} (HTTP {status})"),
        details: None,
    }
}

/// What a provider's error body says about a failure, in provider-neutral
/// form.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ErrorBody {
    /// The discriminators the body carried, lower-cased: OpenAI's
    /// `error.type` and `error.code`, Anthropic's `error.type`, Google's
    /// `error.status` and `error.details[].reason`. One table reads all three.
    tokens: Vec<String>,
    /// The provider's human-readable explanation, if any.
    message: Option<String>,
}

impl ErrorBody {
    /// Read the `error` object all three providers wrap their failures in.
    /// Anything unreadable — HTML from a proxy, an empty body — is a body that
    /// says nothing, and the status decides alone.
    pub(crate) fn parse(body: &str) -> Self {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(body) else {
            return Self::default();
        };
        let error = &json["error"];
        let mut tokens = Vec::new();
        for field in ["type", "code", "status"] {
            if let Some(token) = error[field].as_str() {
                tokens.push(token.to_ascii_lowercase());
            }
        }
        if let Some(details) = error["details"].as_array() {
            tokens.extend(
                details
                    .iter()
                    .filter_map(|detail| detail["reason"].as_str())
                    .map(str::to_ascii_lowercase),
            );
        }
        let message = error["message"]
            .as_str()
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .map(str::to_owned);
        Self { tokens, message }
    }

    fn says_any(&self, candidates: &[&str]) -> bool {
        self.tokens
            .iter()
            .any(|token| candidates.contains(&token.as_str()))
    }

    fn message_mentions(&self, phrase: &str) -> bool {
        self.message
            .as_deref()
            .is_some_and(|message| message.to_ascii_lowercase().contains(phrase))
    }
}

/// Billing, not throttling: waiting will not clear it.
const QUOTA: &[&str] = &[
    "insufficient_quota",
    "credit_balance_exhausted",
    "billing_hard_limit_reached",
    "billing_not_active",
];
const AUTH: &[&str] = &[
    "invalid_api_key",
    "authentication_error",
    "permission_error",
    "unauthenticated",
    "permission_denied",
    "api_key_invalid",
];
const MODEL_NOT_FOUND: &[&str] = &["model_not_found", "not_found_error", "not_found"];
const RATE_LIMITED: &[&str] = &[
    "rate_limit_exceeded",
    "rate_limit_error",
    "resource_exhausted",
];
const TIMEOUT: &[&str] = &["deadline_exceeded"];
const UNAVAILABLE: &[&str] = &[
    "overloaded_error",
    "api_error",
    "server_error",
    "unavailable",
    "internal",
];

/// The failure a non-2xx response means.
///
/// The body's own account outranks the status, most specific first: a 429
/// that says `insufficient_quota` is a billing problem, not a rate limit, and
/// a 400 that says `API_KEY_INVALID` is a rejected key, not a bad request.
/// Anthropic reports an empty balance with no token at all, only prose, so
/// that one phrase is read too. A body that says nothing leaves the status
/// to decide, as before.
pub(crate) fn classify(status: u16, body: &ErrorBody) -> ProviderFailure {
    if body.says_any(QUOTA) || body.message_mentions("credit balance") {
        ProviderFailure::QuotaExhausted
    } else if body.says_any(AUTH) {
        ProviderFailure::AuthFailed
    } else if body.says_any(MODEL_NOT_FOUND) {
        ProviderFailure::ModelNotFound
    } else if body.says_any(RATE_LIMITED) {
        ProviderFailure::RateLimited
    } else if body.says_any(TIMEOUT) {
        ProviderFailure::Timeout
    } else if body.says_any(UNAVAILABLE) {
        ProviderFailure::Unavailable
    } else {
        ProviderFailure::from_http_status(status)
    }
}

/// What to say about a status when the provider said nothing readable.
fn describe_status(status: u16) -> &'static str {
    match status {
        400 => "bad request",
        401 => "invalid API key",
        403 => "forbidden (check API key permissions)",
        404 => "not found",
        429 => "rate limited (too many requests)",
        500 => "server error",
        502 => "bad gateway",
        503 => "service unavailable",
        _ => "HTTP error",
    }
}

#[cfg(test)]
mod tests {
    use super::test_server::CannedResponse;
    use super::*;

    fn body(json: &str) -> ErrorBody {
        ErrorBody::parse(json)
    }

    /// The message a classified failure carries, without Display's framing.
    fn message_of(error: &InferenceError) -> &str {
        match error {
            InferenceError::ProviderFailed { message, .. } => message,
            other => panic!("not a classified provider failure: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Reading a body
    // -----------------------------------------------------------------------

    /// Every field the three providers use as a discriminator lands in
    /// `tokens`, lower-cased, and the prose lands in `message`.
    #[test]
    fn a_body_yields_its_discriminators_and_message() {
        let openai = body(
            r#"{"error":{"message":" The model does not exist. ","type":"invalid_request_error","code":"model_not_found","param":null}}"#,
        );
        assert_eq!(openai.tokens, ["invalid_request_error", "model_not_found"]);
        assert_eq!(openai.message.as_deref(), Some("The model does not exist."));

        let anthropic = body(
            r#"{"type":"error","error":{"type":"authentication_error","message":"API key is invalid."}}"#,
        );
        assert_eq!(anthropic.tokens, ["authentication_error"]);
        assert_eq!(anthropic.message.as_deref(), Some("API key is invalid."));

        let google = body(
            r#"{"error":{"code":400,"message":"API key not valid.","status":"INVALID_ARGUMENT","details":[{"@type":"x","reason":"API_KEY_INVALID","domain":"googleapis.com"},{"@type":"y"}]}}"#,
        );
        // Google's numeric `code` is not a token; the reason under `details` is.
        assert_eq!(google.tokens, ["invalid_argument", "api_key_invalid"]);
        assert_eq!(google.message.as_deref(), Some("API key not valid."));
    }

    /// A body that is not a provider error object says nothing: HTML from a
    /// proxy, an empty body, JSON without `error`, an empty message.
    #[test]
    fn an_unreadable_body_says_nothing() {
        for unreadable in [
            "",
            "<html><body>502 Bad Gateway</body></html>",
            r#"{"detail":"not the provider shape"}"#,
            r#"{"error":"a bare string"}"#,
            r#"{"error":{"message":"   ","code":7}}"#,
        ] {
            assert_eq!(body(unreadable), ErrorBody::default(), "{unreadable:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Classifying
    // -----------------------------------------------------------------------

    /// The body outranks the status: each row is a real provider response
    /// whose status alone would have said something else.
    #[test]
    fn a_body_that_names_its_cause_outranks_the_status() {
        for (status, json, expected) in [
            // OpenAI, out of credits, on a 429 that reads as a rate limit.
            (
                429,
                r#"{"error":{"message":"You have no credits remaining.","type":"insufficient_quota","code":"credit_balance_exhausted"}}"#,
                ProviderFailure::QuotaExhausted,
            ),
            // Anthropic, out of credits, on a 400 with no token: prose only.
            (
                400,
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}"#,
                ProviderFailure::QuotaExhausted,
            ),
            // Google, bad key, on a 400 that reads as a bad request.
            (
                400,
                r#"{"error":{"code":400,"message":"API key not valid.","status":"INVALID_ARGUMENT","details":[{"reason":"API_KEY_INVALID"}]}}"#,
                ProviderFailure::AuthFailed,
            ),
            // OpenAI, bad key, on a 401 — the body and status agree.
            (
                401,
                r#"{"error":{"message":"Incorrect API key provided.","type":"invalid_request_error","code":"invalid_api_key"}}"#,
                ProviderFailure::AuthFailed,
            ),
            // OpenAI, unknown model, on a 404.
            (
                404,
                r#"{"error":{"message":"The model `x` does not exist.","type":"invalid_request_error","code":"model_not_found"}}"#,
                ProviderFailure::ModelNotFound,
            ),
            // Anthropic, unknown model.
            (
                404,
                r#"{"type":"error","error":{"type":"not_found_error","message":"model: x"}}"#,
                ProviderFailure::ModelNotFound,
            ),
            // Google, retired model.
            (
                404,
                r#"{"error":{"code":404,"message":"models/x is not found","status":"NOT_FOUND"}}"#,
                ProviderFailure::ModelNotFound,
            ),
            // Google, throttled: still a rate limit.
            (
                429,
                r#"{"error":{"code":429,"message":"Quota exceeded","status":"RESOURCE_EXHAUSTED"}}"#,
                ProviderFailure::RateLimited,
            ),
            // OpenAI, throttled: still a rate limit.
            (
                429,
                r#"{"error":{"message":"Rate limit reached","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#,
                ProviderFailure::RateLimited,
            ),
            // Anthropic, overloaded, on a 529 no status table knows.
            (
                529,
                r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
                ProviderFailure::Unavailable,
            ),
            // Google, deadline, on a 504.
            (
                504,
                r#"{"error":{"code":504,"message":"Deadline expired","status":"DEADLINE_EXCEEDED"}}"#,
                ProviderFailure::Timeout,
            ),
        ] {
            assert_eq!(
                classify(status, &body(json)),
                expected,
                "HTTP {status} {json}"
            );
        }
    }

    /// Quota is checked before auth and model: a body carrying several
    /// tokens resolves to the most specific one, whatever their order.
    #[test]
    fn the_most_specific_token_wins() {
        let stacked = body(
            r#"{"error":{"type":"not_found_error","code":"insufficient_quota","status":"UNAUTHENTICATED"}}"#,
        );
        assert_eq!(classify(200, &stacked), ProviderFailure::QuotaExhausted);

        let auth_and_model =
            body(r#"{"error":{"type":"not_found_error","code":"invalid_api_key"}}"#);
        assert_eq!(classify(200, &auth_and_model), ProviderFailure::AuthFailed);

        let model_and_rate =
            body(r#"{"error":{"type":"rate_limit_error","code":"model_not_found"}}"#);
        assert_eq!(
            classify(200, &model_and_rate),
            ProviderFailure::ModelNotFound
        );
    }

    /// A body that says nothing leaves the status to decide, exactly as the
    /// status-only path always did.
    #[test]
    fn a_silent_body_leaves_the_status_to_decide() {
        for status in [400, 401, 403, 404, 408, 418, 429, 500, 502, 503, 504] {
            assert_eq!(
                classify(status, &ErrorBody::default()),
                ProviderFailure::from_http_status(status),
                "HTTP {status}"
            );
        }
        // A body with tokens the table does not know is a silent body.
        let unknown = body(r#"{"error":{"type":"something_new","code":"we_have_not_seen"}}"#);
        assert_eq!(classify(429, &unknown), ProviderFailure::RateLimited);
        assert_eq!(classify(401, &unknown), ProviderFailure::AuthFailed);
    }

    /// The whole error, from a status and a body: the kind and the
    /// provider's own words, with the status kept for the log.
    #[test]
    fn a_provider_error_carries_the_providers_words() {
        let error = provider_error(
            "P",
            404,
            r#"{"error":{"message":"The model `x` does not exist.","code":"model_not_found"}}"#,
        );
        assert_eq!(error.code(), "inference.provider_model_not_found");
        assert_eq!(
            message_of(&error),
            "P: The model `x` does not exist. (HTTP 404)"
        );
    }

    /// With nothing readable the message describes the status instead.
    #[test]
    fn a_provider_error_describes_the_status_when_the_body_is_silent() {
        for (status, expected_code, described) in [
            (400, "inference.invalid_request", "bad request"),
            (401, "inference.provider_auth_failed", "invalid API key"),
            (403, "inference.provider_auth_failed", "forbidden"),
            (404, "inference.provider_model_not_found", "not found"),
            (429, "inference.provider_rate_limited", "rate limited"),
            (500, "inference.provider_unavailable", "server error"),
            (502, "inference.provider_unavailable", "bad gateway"),
            (503, "inference.provider_unavailable", "service unavailable"),
            (418, "inference.provider_unavailable", "HTTP error"),
        ] {
            let error = provider_error("P", status, "<html>not json</html>");
            assert_eq!(error.code(), expected_code, "HTTP {status}");
            let message = message_of(&error);
            assert!(
                message.starts_with("P: ") && message.contains(described),
                "HTTP {status}: {message}"
            );
            assert!(
                message.ends_with(&format!("(HTTP {status})")),
                "the message names the status: {message}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // The transport
    // -----------------------------------------------------------------------

    /// The transport's own verdict decides the kind (D6).
    ///
    /// Deleting the `Timeout` arm would let a timeout fall through to the
    /// catch-all and report as an outage, which is the exact collapse D6
    /// exists to prevent — an outage invites a retry against a server that
    /// is fine, while a timeout points at the deadline.
    #[test]
    fn a_transport_timeout_is_a_timeout_not_an_outage() {
        let timeout = transport_error("p", ureq::Error::Timeout(ureq::Timeout::Global));
        assert_eq!(timeout.code(), "inference.provider_timeout");

        // The catch-all still reports an outage, so the assertion above is
        // about the arm and not about every error becoming a timeout.
        let other = transport_error("p", ureq::Error::HostNotFound);
        assert_eq!(other.code(), "inference.provider_unavailable");
        assert!(message_of(&other).starts_with("p: "));
    }

    fn post(server: &CannedResponse, timeout: Duration) -> Result<String, InferenceError> {
        post_json(&CloudPost {
            provider: "P",
            url: &format!("{}/endpoint", server.base_url()),
            headers: &[("x-credential", "secret")],
            body: r#"{"k":"v"}"#,
            timeout,
        })
    }

    /// A 2xx hands back the body; the request carried the JSON content type,
    /// the caller's headers, and the caller's body.
    #[test]
    fn a_success_returns_the_body_and_the_request_is_shaped_right() {
        let server = CannedResponse::serve(200, r#"{"ok":true}"#);
        let body = post(&server, Duration::from_secs(5)).expect("a 2xx succeeds");
        assert_eq!(body, r#"{"ok":true}"#);

        let request = server.request();
        assert!(request.starts_with("POST /endpoint "), "{request}");
        let head = request.to_ascii_lowercase();
        assert!(head.contains("content-type: application/json"), "{request}");
        assert!(head.contains("x-credential: secret"), "{request}");
        assert!(request.ends_with(r#"{"k":"v"}"#), "{request}");
    }

    /// A non-2xx is read, not raised: the failure it describes comes back
    /// classified, with the provider's own words.
    #[test]
    fn a_failure_status_is_read_and_classified() {
        let server = CannedResponse::serve(
            402,
            r#"{"error":{"type":"billing_not_active","message":"Payment required."}}"#,
        );
        let error = post(&server, Duration::from_secs(5)).expect_err("a 402 fails");
        assert_eq!(error.code(), "inference.provider_quota_exhausted");
        assert_eq!(message_of(&error), "P: Payment required. (HTTP 402)");
    }

    /// Nothing listening is an outage, not a timeout.
    #[test]
    fn a_refused_connection_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a port");
        let url = format!(
            "http://{}/endpoint",
            listener.local_addr().expect("the address")
        );
        // Closing the listener frees the port with nothing behind it.
        drop(listener);
        let error = post_json(&CloudPost {
            provider: "P",
            url: &url,
            headers: &[],
            body: "{}",
            timeout: Duration::from_secs(5),
        })
        .expect_err("nothing is listening");
        assert_eq!(error.code(), "inference.provider_unavailable");
    }

    /// A server that never answers is a timeout, through the real transport.
    #[test]
    fn a_silent_server_is_a_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a port");
        let url = format!(
            "http://{}/endpoint",
            listener.local_addr().expect("the address")
        );
        // Accept the connection so the client gets past connect, then say
        // nothing until the client gives up.
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept the client");
            stream
        });
        let error = post_json(&CloudPost {
            provider: "P",
            url: &url,
            headers: &[],
            body: "{}",
            timeout: Duration::from_millis(300),
        })
        .expect_err("the server never answers");
        assert_eq!(error.code(), "inference.provider_timeout");
        drop(server.join().expect("the server thread"));
    }
}

/// A stand-in for a provider's API: one loopback HTTP/1.1 server that answers
/// its first request with a canned status and body, and hands back what the
/// client sent. Lets a test drive the real request path — URL, headers, body,
/// status handling, parsing — without a network or a key.
#[cfg(test)]
pub(crate) mod test_server {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::JoinHandle;

    pub(crate) struct CannedResponse {
        base_url: String,
        request: JoinHandle<String>,
    }

    impl CannedResponse {
        /// Start serving `status` + `body` on a fresh loopback port.
        pub(crate) fn serve(status: u16, body: &str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
            let base_url = format!(
                "http://{}",
                listener.local_addr().expect("the bound address")
            );
            let body = body.to_string();
            let request = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept the client");
                let request = read_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status} Canned\r\n\
                     content-type: application/json\r\n\
                     content-length: {}\r\n\
                     connection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write the response");
                request
            });
            Self { base_url, request }
        }

        /// The URL to hand a provider as its API root.
        pub(crate) fn base_url(&self) -> &str {
            &self.base_url
        }

        /// The raw request the client sent — head and body — once it has.
        pub(crate) fn request(self) -> String {
            self.request.join().expect("the server thread")
        }
    }

    /// Read one HTTP/1.1 request: the head up to the blank line, then as many
    /// body bytes as `content-length` promises.
    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            let read = stream.read(&mut chunk).expect("read the request head");
            assert!(read > 0, "client closed before sending a request head");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(blank) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                break blank + 4;
            }
        };
        let head = String::from_utf8_lossy(&bytes[..head_end]).into_owned();
        let content_length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length").then(|| {
                    value
                        .trim()
                        .parse::<usize>()
                        .expect("a numeric content-length")
                })
            })
            // A request without the header has no body to wait for.
            .unwrap_or(0);
        while bytes.len() < head_end + content_length {
            let read = stream.read(&mut chunk).expect("read the request body");
            assert!(read > 0, "client closed mid-body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }
}
