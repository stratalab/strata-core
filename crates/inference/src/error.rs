use std::error::Error;
use std::fmt;

use serde::ser::Serializer;

use crate::resolve::AvailabilityDetails;

/// Errors that can occur during inference operations.
///
/// Serializes externally tagged (`{"RegistryFailed": {"kind": …, "message":
/// …}}`) with every message redacted on the way out; `details` appears only
/// when present.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InferenceError {
    /// Local llama.cpp runtime failure.
    LlamaCpp(#[serde(serialize_with = "serialize_redacted")] String),

    /// Cloud provider failure.
    Provider(#[serde(serialize_with = "serialize_redacted")] String),

    /// Model registry or model download failure.
    Registry(#[serde(serialize_with = "serialize_redacted")] String),

    /// Filesystem or process IO failure.
    Io(#[serde(serialize_with = "serialize_redacted")] String),

    /// Requested provider, model, or operation is unavailable.
    NotSupported(#[serde(serialize_with = "serialize_redacted")] String),

    /// The model spec itself is malformed (empty, or a provider prefix with
    /// no model name after it) — caller input error, not a provider outage.
    /// A prefix that is not a provider name is not malformed: the spec is a
    /// local model name and the registry decides whether it exists (#3222).
    InvalidSpec(#[serde(serialize_with = "serialize_redacted")] String),

    /// A model-registry failure whose classification was decided **where the
    /// failure happened**.
    ///
    /// D8/#3216. Rewording the not-downloaded message from "not found locally"
    /// to "is not downloaded" silently reclassified it from `missing_model` to
    /// `download_failed`, because `registry_code` matches "download" — and the
    /// D8 download offer keys off `missing_model`, so the feature would have
    /// been dead on arrival. The fourth time this design bit during one change.
    RegistryFailed {
        /// What went wrong, decided at the raise site.
        kind: RegistryFailure,
        /// Human-readable detail. Carries no classification weight.
        #[serde(serialize_with = "serialize_redacted")]
        message: String,
        /// The resolver's answer, when this refusal came from resolution.
        /// Boxed so the payload of one arm does not widen every `Result`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Box<AvailabilityDetails>>,
    },

    /// A cloud provider failure whose classification was decided **where the
    /// failure happened**, instead of guessed from the message afterwards.
    ///
    /// D6/#3216. `Provider(String)` is classified by substring-matching its
    /// text, which gets the answer wrong for most ways of phrasing a key
    /// problem: "API key missing" reports as corrupt provider data, and a
    /// rejected key is indistinguishable from an outage. The information was
    /// never missing — an HTTP mapper knows it saw a 401 — it was thrown away
    /// and reconstructed by guessing.
    ///
    /// New provider failures should use this. `Provider` remains for the paths
    /// not yet converted.
    ProviderFailed {
        /// What went wrong, decided at the raise site.
        kind: ProviderFailure,
        /// Human-readable detail. Carries no classification weight.
        #[serde(serialize_with = "serialize_redacted")]
        message: String,
        /// The resolver's answer, when this refusal came from resolution.
        /// Boxed so the payload of one arm does not widen every `Result`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Box<AvailabilityDetails>>,
    },

    /// A refusal whose classification was decided **where it was raised**.
    ///
    /// `NotSupported(String)` is classified by substring: the word "provider"
    /// in the message makes it `unsupported_provider`, "download" makes it
    /// `download_disabled`. That is workable for fixed text and not for a
    /// message that names the spec the caller typed — `provider.gguf` would
    /// reclassify its own refusal. The resolver (`crate::resolve`) names the
    /// spec, so it raises this instead.
    Unsupported {
        /// What is unsupported, decided at the raise site.
        kind: UnsupportedKind,
        /// Human-readable detail. Carries no classification weight.
        #[serde(serialize_with = "serialize_redacted")]
        message: String,
        /// The resolver's answer, when this refusal came from resolution.
        /// Boxed so the payload of one arm does not widen every `Result`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Box<AvailabilityDetails>>,
    },
}

/// Redacts a message on its way to the wire, so a provider's error text
/// cannot carry a key out in a serialized error (Rule 31).
fn serialize_redacted<S: Serializer>(message: &str, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&redact_secrets(message))
}

/// What a typed refusal is about, as known at the point of refusal.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedKind {
    /// The operation cannot be performed here: the build lacks it, the
    /// runtime forbids it, or the model does not do it.
    Operation,
    /// The provider is not compiled into this binary.
    Provider,
}

impl UnsupportedKind {
    /// The stable error code for this refusal.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Operation => "inference.unsupported_operation",
            Self::Provider => "inference.unsupported_provider",
        }
    }
}

/// Why a model-registry lookup failed, as known at the point of failure.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryFailure {
    /// The name is not in the catalog, or the path has no file: nothing to
    /// pull, nothing to load. The other half of what `MissingModel` used to
    /// mean (#3256).
    UnknownModel,
    /// The model is catalogued but its artifact is not on disk — a `pull`
    /// away from ready.
    MissingModel,
    /// This build cannot download, or the runtime forbids network access.
    DownloadDisabled,
    /// A download was attempted and failed.
    DownloadFailed,
    /// A downloaded artifact did not match its expected hash.
    VerificationFailed,
    /// The registry's own state is unreadable.
    Corrupt,
}

impl RegistryFailure {
    /// The stable error code for this failure.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownModel => "inference.unknown_model",
            Self::MissingModel => "inference.missing_model",
            Self::DownloadDisabled => "inference.download_disabled",
            Self::DownloadFailed => "inference.download_failed",
            Self::VerificationFailed => "inference.download_verification_failed",
            Self::Corrupt => "inference.registry_corrupt",
        }
    }
}

/// Why a cloud provider call failed, as known at the point of failure.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailure {
    /// No API key was available for the provider.
    MissingApiKey,
    /// A key was supplied and the provider rejected it (401/403).
    AuthFailed,
    /// The provider refused the request itself (400).
    InvalidRequest,
    /// The provider asked us to slow down (429).
    RateLimited,
    /// The account behind the key has no credit or quota left to spend.
    ///
    /// Arrives as a 429 or a 400 whose body names billing (#3236); unlike a
    /// rate limit it does not clear by waiting.
    QuotaExhausted,
    /// The provider does not serve the requested model (404).
    ///
    /// Distinct from `inference.missing_model`, which is a local model not on
    /// disk.
    ModelNotFound,
    /// The request did not complete in time.
    Timeout,
    /// The provider is unreachable or erroring (5xx, transport failure).
    Unavailable,
    /// The provider answered, but not with something we can read.
    MalformedResponse,
}

impl ProviderFailure {
    /// The stable error code for this failure.
    ///
    /// A direct mapping, which is the entire point: no message is consulted,
    /// so rewording a diagnostic cannot change what it is.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingApiKey => "inference.missing_api_key",
            Self::AuthFailed => "inference.provider_auth_failed",
            Self::InvalidRequest => "inference.invalid_request",
            Self::RateLimited => "inference.provider_rate_limited",
            Self::QuotaExhausted => "inference.provider_quota_exhausted",
            Self::ModelNotFound => "inference.provider_model_not_found",
            Self::Timeout => "inference.provider_timeout",
            Self::Unavailable => "inference.provider_unavailable",
            Self::MalformedResponse => "inference.provider_malformed_response",
        }
    }

    /// The failure a cloud HTTP status means when the response body says
    /// nothing more specific.
    ///
    /// The status is the authority. Reading it here rather than describing it
    /// in prose and matching the prose back is the whole of D6. A body that
    /// names its cause outranks it (#3236): see `provider::cloud::classify`.
    #[must_use]
    pub const fn from_http_status(status: u16) -> Self {
        match status {
            400 => Self::InvalidRequest,
            401 | 403 => Self::AuthFailed,
            // Every cloud endpoint Strata calls is fixed; the only thing that
            // can be missing is the model.
            404 => Self::ModelNotFound,
            429 => Self::RateLimited,
            408 | 504 => Self::Timeout,
            _ => Self::Unavailable,
        }
    }
}

impl fmt::Debug for InferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LlamaCpp(message) => formatter
                .debug_tuple("LlamaCpp")
                .field(&redact_secrets(message))
                .finish(),
            Self::Provider(message) => formatter
                .debug_tuple("Provider")
                .field(&redact_secrets(message))
                .finish(),
            Self::Registry(message) => formatter
                .debug_tuple("Registry")
                .field(&redact_secrets(message))
                .finish(),
            Self::Io(message) => formatter
                .debug_tuple("Io")
                .field(&redact_secrets(message))
                .finish(),
            Self::NotSupported(message) => formatter
                .debug_tuple("NotSupported")
                .field(&redact_secrets(message))
                .finish(),
            Self::InvalidSpec(message) => formatter
                .debug_tuple("InvalidSpec")
                .field(&redact_secrets(message))
                .finish(),
            Self::RegistryFailed {
                kind,
                message,
                details,
            } => formatter
                .debug_struct("RegistryFailed")
                .field("kind", kind)
                .field("message", &redact_secrets(message))
                .field("details", details)
                .finish(),
            Self::ProviderFailed {
                kind,
                message,
                details,
            } => formatter
                .debug_struct("ProviderFailed")
                .field("kind", kind)
                .field("message", &redact_secrets(message))
                .field("details", details)
                .finish(),
            Self::Unsupported {
                kind,
                message,
                details,
            } => formatter
                .debug_struct("Unsupported")
                .field("kind", kind)
                .field("message", &redact_secrets(message))
                .field("details", details)
                .finish(),
        }
    }
}

impl fmt::Display for InferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LlamaCpp(message) => write!(formatter, "llama.cpp: {}", redact_secrets(message)),
            Self::Provider(message) => {
                write!(formatter, "provider error: {}", redact_secrets(message))
            }
            Self::Registry(message) => {
                write!(formatter, "registry error: {}", redact_secrets(message))
            }
            Self::Io(message) => write!(formatter, "IO error: {}", redact_secrets(message)),
            Self::NotSupported(message) => {
                write!(formatter, "not supported: {}", redact_secrets(message))
            }
            Self::InvalidSpec(message) => {
                write!(formatter, "invalid model spec: {}", redact_secrets(message))
            }
            Self::RegistryFailed { message, .. } => {
                write!(formatter, "registry error: {}", redact_secrets(message))
            }
            Self::ProviderFailed { message, .. } => {
                write!(formatter, "provider error: {}", redact_secrets(message))
            }
            Self::Unsupported { message, .. } => {
                write!(formatter, "not supported: {}", redact_secrets(message))
            }
        }
    }
}

impl Error for InferenceError {}

impl From<std::io::Error> for InferenceError {
    fn from(e: std::io::Error) -> Self {
        InferenceError::Io(e.to_string())
    }
}

impl InferenceError {
    /// Returns the stable public error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::LlamaCpp(message) => llama_code(message),
            Self::Provider(message) => provider_code(message),
            Self::Registry(message) => registry_code(message),
            Self::Io(_) => "inference.io_failure",
            Self::NotSupported(message) => not_supported_code(message),
            Self::InvalidSpec(_) => "inference.invalid_request",
            Self::ProviderFailed { kind, .. } => kind.code(),
            Self::RegistryFailed { kind, .. } => kind.code(),
            Self::Unsupported { kind, .. } => kind.code(),
        }
    }

    /// The resolver's answer this error carries, when it came from
    /// resolution: which model, who serves it, and why it cannot be used.
    ///
    /// The code says what class of thing went wrong; this says what to do
    /// about it. `None` for errors raised past resolution (a provider call
    /// that failed, a file that would not load) and for the string variants.
    ///
    /// The code's class and retry policy are not here on purpose: they are
    /// facts of the executor's error registry, which this crate cannot read
    /// (Rule 3), so a copy here could only drift from it (#3286).
    #[must_use]
    pub fn availability(&self) -> Option<&AvailabilityDetails> {
        match self {
            Self::RegistryFailed { details, .. }
            | Self::ProviderFailed { details, .. }
            | Self::Unsupported { details, .. } => details.as_deref(),
            Self::LlamaCpp(_)
            | Self::Provider(_)
            | Self::Registry(_)
            | Self::Io(_)
            | Self::NotSupported(_)
            | Self::InvalidSpec(_) => None,
        }
    }

    /// Returns the public redacted message.
    pub fn public_message(&self) -> String {
        redact_secrets(&self.to_string())
    }
}

fn llama_code(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("load") || lower.contains("not found") || lower.contains("no such file") {
        "inference.model_load_failed"
    } else {
        "inference.local_runtime_failed"
    }
}

fn provider_code(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    let mentions_api_key =
        lower.contains("api key") || lower.contains("api_key") || lower.contains("_api_key");
    if lower.contains("invalid request") || lower.contains("bad request") || lower.contains("400") {
        "inference.invalid_request"
    } else if mentions_api_key && (lower.contains("not set") || lower.contains("empty")) {
        "inference.missing_api_key"
    } else if lower.contains("invalid api key")
        || lower.contains("auth")
        || lower.contains("401")
        || lower.contains("403")
    {
        "inference.provider_auth_failed"
    } else if lower.contains("rate limit") || lower.contains("429") {
        "inference.provider_rate_limited"
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "inference.provider_timeout"
    } else if lower.contains("500")
        || lower.contains("502")
        || lower.contains("503")
        || lower.contains("unavailable")
    {
        "inference.provider_unavailable"
    } else if lower.contains("invalid json")
        || lower.contains("missing")
        || lower.contains("empty choices")
        || lower.contains("empty candidates")
        || lower.contains("malformed")
    {
        "inference.provider_malformed_response"
    } else if lower.contains("unsupported") {
        "inference.unsupported_parameter"
    } else {
        "inference.provider_unavailable"
    }
}

fn registry_code(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("unknown model") || lower.contains("not found locally") {
        "inference.missing_model"
    } else if lower.contains("download disabled") || lower.contains("network access") {
        "inference.download_disabled"
    } else if lower.contains("sha-256") || lower.contains("hash mismatch") {
        "inference.download_verification_failed"
    } else if lower.contains("download") {
        "inference.download_failed"
    } else if lower.contains("corrupt") {
        "inference.registry_corrupt"
    } else {
        "inference.registry_corrupt"
    }
}

fn not_supported_code(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("provider") {
        "inference.unsupported_provider"
    } else if lower.contains("parameter") || lower.contains("knob") {
        "inference.unsupported_parameter"
    } else if lower.contains("download") {
        "inference.download_disabled"
    } else {
        "inference.unsupported_operation"
    }
}

fn redact_secrets(message: &str) -> String {
    let mut redacted = String::with_capacity(message.len());
    let mut index = 0;
    while index < message.len() {
        let rest = &message[index..];
        let lower = rest.to_ascii_lowercase();
        let secret_prefix = ["sk-ant", "sk_ant", "sk-", "aiza"]
            .iter()
            .find(|prefix| lower.starts_with(**prefix));
        if secret_prefix.is_some() {
            redacted.push_str("[REDACTED]");
            index += secret_len(rest);
            continue;
        }

        let ch = rest
            .chars()
            .next()
            .expect("index is within string boundary");
        redacted.push(ch);
        index += ch.len_utf8();
    }
    redacted
}

fn secret_len(value: &str) -> usize {
    value
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        .last()
        .map_or(value.len(), |(index, ch)| index + ch.len_utf8())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Display tests: verify exact format, not just substring ---

    #[test]
    fn display_llamacpp_error() {
        let e = InferenceError::LlamaCpp("model not found".into());
        assert_eq!(e.to_string(), "llama.cpp: model not found");
    }

    #[test]
    fn display_provider_error() {
        let e = InferenceError::Provider("rate limited".into());
        assert_eq!(e.to_string(), "provider error: rate limited");
    }

    #[test]
    fn display_registry_error() {
        let e = InferenceError::Registry("unknown model".into());
        assert_eq!(e.to_string(), "registry error: unknown model");
    }

    #[test]
    fn display_io_error() {
        let e = InferenceError::Io("file not found".into());
        assert_eq!(e.to_string(), "IO error: file not found");
    }

    #[test]
    fn display_not_supported() {
        let e = InferenceError::NotSupported("tokenize on cloud".into());
        assert_eq!(e.to_string(), "not supported: tokenize on cloud");
    }

    // --- Each variant has a distinct prefix (no ambiguity) ---

    #[test]
    fn all_variants_have_distinct_prefixes() {
        let errors = [
            InferenceError::LlamaCpp("x".into()),
            InferenceError::Provider("x".into()),
            InferenceError::Registry("x".into()),
            InferenceError::Io("x".into()),
            InferenceError::NotSupported("x".into()),
        ];
        let messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        // Every pair should be different despite same inner "x"
        for i in 0..messages.len() {
            for j in (i + 1)..messages.len() {
                assert_ne!(messages[i], messages[j], "variants {i} and {j} collide");
            }
        }
    }

    // --- io::Error conversion ---

    #[test]
    fn io_error_conversion_preserves_full_message() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e: InferenceError = io_err.into();
        assert!(matches!(e, InferenceError::Io(_)));
        // Verify the full formatted string, not just a substring
        assert_eq!(e.to_string(), "IO error: gone");
    }

    #[test]
    fn io_error_conversion_preserves_kind_description() {
        // When io::Error has no custom message, to_string() includes the kind
        let io_err = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot read /etc/shadow",
        );
        let e: InferenceError = io_err.into();
        assert!(
            e.to_string().contains("cannot read /etc/shadow"),
            "got: {}",
            e
        );
    }

    #[test]
    fn io_error_from_real_fs_operation() {
        // Exercise the From impl with a real filesystem error
        let result: Result<Vec<u8>, InferenceError> =
            std::fs::read("/nonexistent/path/that/does/not/exist").map_err(Into::into);
        let e = result.unwrap_err();
        assert!(matches!(e, InferenceError::Io(_)));
        assert!(!e.to_string().is_empty());
    }

    // --- Clone across all variants ---

    #[test]
    fn clone_all_variants() {
        let cases: Vec<InferenceError> = vec![
            InferenceError::LlamaCpp("llama msg".into()),
            InferenceError::Provider("provider msg".into()),
            InferenceError::Registry("registry msg".into()),
            InferenceError::Io("io msg".into()),
            InferenceError::NotSupported("not supported msg".into()),
        ];
        for original in &cases {
            let cloned = original.clone();
            assert_eq!(original.to_string(), cloned.to_string());
            // Verify the clone is structurally the same variant
            assert_eq!(
                std::mem::discriminant(original),
                std::mem::discriminant(&cloned)
            );
        }
    }

    // --- Error messages contain original context ---

    #[test]
    fn every_variant_preserves_context_string() {
        let context = "specific context string with special chars: !@#$%";
        let cases: Vec<InferenceError> = vec![
            InferenceError::LlamaCpp(context.into()),
            InferenceError::Provider(context.into()),
            InferenceError::Registry(context.into()),
            InferenceError::Io(context.into()),
            InferenceError::NotSupported(context.into()),
        ];
        for e in &cases {
            assert!(
                e.to_string().contains(context),
                "variant {:?} lost context: {}",
                std::mem::discriminant(e),
                e
            );
        }
    }

    // --- Empty string edge case ---

    #[test]
    fn empty_string_variants_still_have_prefix() {
        // Empty inner string shouldn't produce empty Display output
        let cases: Vec<(InferenceError, &str)> = vec![
            (InferenceError::LlamaCpp(String::new()), "llama.cpp: "),
            (InferenceError::Provider(String::new()), "provider error: "),
            (InferenceError::Registry(String::new()), "registry error: "),
            (InferenceError::Io(String::new()), "IO error: "),
            (
                InferenceError::NotSupported(String::new()),
                "not supported: ",
            ),
        ];
        for (e, expected) in &cases {
            assert_eq!(&e.to_string(), expected);
        }
    }

    // --- std::error::Error trait ---

    #[test]
    fn implements_std_error_trait() {
        let e = InferenceError::Provider("test".into());
        // Verify it can be used as &dyn std::error::Error
        let dyn_err: &dyn std::error::Error = &e;
        assert!(!dyn_err.to_string().is_empty());
        // source() should be None for our string-wrapping variants
        assert!(dyn_err.source().is_none());
    }

    #[test]
    fn usable_with_question_mark_operator() {
        fn fallible() -> Result<(), InferenceError> {
            let _data = std::fs::read("/nonexistent/file/for/test")?;
            Ok(())
        }
        let result = fallible();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Io(_)));
    }
}
