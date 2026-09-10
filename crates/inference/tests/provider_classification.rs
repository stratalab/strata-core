//! What a provider failure is classified as, pinned (D6, toward #3216).
//!
//! `InferenceError::code()` derives the code for `Provider` by
//! substring-matching the human message — twenty-eight checks across four
//! functions decide the whole taxonomy. That makes prose load-bearing: three
//! separate message edits in this PR silently reclassified errors, and each was
//! caught only because a pin existed.
//!
//! This file is that pin for the provider half. It captures the current
//! mapping **before** any of it is changed, so a refactor that carries codes at
//! the raise site can prove it moved only what it meant to.
//!
//! The `misclassified_*` tests are deliberately asserting **wrong** answers.
//! They are not approval — they are the record of what ships today, so the fix
//! that flips them is visible in a diff rather than lost in a rewrite.

use strata_inference::{InferenceError, ProviderFailure};

fn provider(message: &str) -> InferenceError {
    InferenceError::Provider(message.to_owned())
}

/// The real messages the HTTP mappers emit, one per status they special-case.
/// These are the paths that matter: they are what a caller actually hits.
#[test]
fn http_status_messages_classify_as_intended() {
    let cases = [
        (
            "openai: invalid API key (HTTP 401)",
            "inference.provider_auth_failed",
        ),
        (
            "openai: forbidden (check API key permissions) (HTTP 403)",
            "inference.provider_auth_failed",
        ),
        (
            "openai: rate limited (too many requests) (HTTP 429)",
            "inference.provider_rate_limited",
        ),
        (
            "openai: server error (HTTP 500)",
            "inference.provider_unavailable",
        ),
        (
            "openai: bad gateway (HTTP 502)",
            "inference.provider_unavailable",
        ),
        (
            "openai: service unavailable (HTTP 503)",
            "inference.provider_unavailable",
        ),
        (
            "google: bad request (check model name and parameters) (HTTP 400)",
            "inference.invalid_request",
        ),
        (
            "google: invalid or unauthorized API key (HTTP 401)",
            "inference.provider_auth_failed",
        ),
    ];
    for (message, expected) in cases {
        assert_eq!(provider(message).code(), expected, "message: {message}");
    }
}

/// The message the missing-key path emits today.
#[test]
fn the_missing_key_message_classifies_as_a_missing_key() {
    let error = provider("OPENAI_API_KEY not set (required for openai:gpt-4o-mini)");
    assert_eq!(error.code(), "inference.missing_api_key");
}

/// Non-key provider failures keep their meaning.
#[test]
fn transport_and_response_failures_classify_as_intended() {
    let cases = [
        (
            "openai: invalid JSON response: expected value",
            "inference.provider_malformed_response",
        ),
        ("anthropic: request timed out", "inference.provider_timeout"),
        (
            "openai: empty choices in response",
            "inference.provider_malformed_response",
        ),
    ];
    for (message, expected) in cases {
        assert_eq!(provider(message).code(), expected, "message: {message}");
    }
}

/// **Known wrong.** A missing API key phrased with the word "missing" lands in
/// the malformed-response branch, because `contains("missing")` is checked for
/// corrupt responses and nothing catches it first.
///
/// The registry classes that code as corruption: a caller is told their
/// provider returned corrupt data when in fact they never set a key. D6 fixes
/// this by deciding the code where the failure happens.
#[test]
fn misclassified_a_missing_key_reads_as_provider_corruption() {
    let error = provider("API key missing for openai");
    assert_eq!(
        error.code(),
        "inference.provider_malformed_response",
        "pinning today's wrong answer so the fix is visible"
    );
}

/// **Known wrong.** A rejected key — the case D6 exists for — is
/// indistinguishable from an outage unless the message happens to contain
/// "401", "auth", or the exact phrase "invalid api key".
#[test]
fn misclassified_a_rejected_key_reads_as_an_outage() {
    let error = provider("the configured API key was rejected");
    assert_eq!(error.code(), "inference.provider_unavailable");
}

/// **Known wrong.** So does a key that was never found, when phrased without
/// the words "not set" or "empty".
#[test]
fn misclassified_an_absent_key_reads_as_an_outage() {
    assert_eq!(
        provider("no API key found for openai").code(),
        "inference.provider_unavailable"
    );
}

/// The fallback: anything unrecognised is reported as an outage, which is the
/// safest default but means every gap in the substring table looks retryable.
#[test]
fn an_unrecognised_failure_falls_back_to_unavailable() {
    assert_eq!(
        provider("connection refused reaching api.openai.com").code(),
        "inference.provider_unavailable"
    );
    assert_eq!(
        provider("something nobody anticipated").code(),
        "inference.provider_unavailable"
    );
}

// ---------------------------------------------------------------------------
// D6: the converted paths. These decide the code where the failure happens, so
// none of the answers below depends on how the message is worded.
// ---------------------------------------------------------------------------

fn failed(kind: ProviderFailure, message: &str) -> InferenceError {
    InferenceError::ProviderFailed {
        kind,
        message: message.to_owned(),
        details: None,
    }
}

/// The code follows the kind, whatever the message says.
///
/// This is the property the substring classifier cannot have: the same text
/// under two kinds gives two codes, and the same kind under two texts gives
/// one. Prose stops being load-bearing.
#[test]
fn the_code_follows_the_kind_not_the_message() {
    // Identical text, different kinds → different codes.
    assert_eq!(
        failed(ProviderFailure::AuthFailed, "something went wrong").code(),
        "inference.provider_auth_failed"
    );
    assert_eq!(
        failed(ProviderFailure::Timeout, "something went wrong").code(),
        "inference.provider_timeout"
    );

    // Text that the legacy classifier would misread, now classified correctly:
    // "missing" no longer drags a key problem into the corruption class.
    let missing = failed(ProviderFailure::MissingApiKey, "API key missing for openai");
    assert_eq!(
        missing.code(),
        "inference.missing_api_key",
        "a key problem must never read as corrupt provider data"
    );

    // And a rejected key is no longer an outage.
    assert_eq!(
        failed(
            ProviderFailure::AuthFailed,
            "the configured API key was rejected"
        )
        .code(),
        "inference.provider_auth_failed"
    );
}

/// Every kind has a distinct code, so no two failures collapse together.
#[test]
fn every_failure_kind_has_its_own_code() {
    use ProviderFailure::{
        AuthFailed, InvalidRequest, MalformedResponse, MissingApiKey, ModelNotFound,
        QuotaExhausted, RateLimited, Timeout, Unavailable,
    };
    let kinds = [
        MissingApiKey,
        AuthFailed,
        InvalidRequest,
        RateLimited,
        QuotaExhausted,
        ModelNotFound,
        Timeout,
        Unavailable,
        MalformedResponse,
    ];
    let mut codes: Vec<&str> = kinds.iter().map(|kind| kind.code()).collect();
    codes.sort_unstable();
    let distinct = codes.len();
    codes.dedup();
    assert_eq!(codes.len(), distinct, "two kinds share a code: {codes:?}");
}

/// The HTTP status decides the kind when the body says nothing. This is the
/// fallback the one cloud transport uses instead of describing the status in
/// prose and matching the prose back; a body that names its cause outranks it.
#[test]
fn http_status_decides_the_kind() {
    use ProviderFailure::{
        AuthFailed, InvalidRequest, ModelNotFound, RateLimited, Timeout, Unavailable,
    };
    let cases = [
        (400, InvalidRequest),
        (401, AuthFailed),
        (403, AuthFailed),
        (404, ModelNotFound), // every endpoint is fixed; only the model can be missing
        (408, Timeout),
        (429, RateLimited),
        (500, Unavailable),
        (502, Unavailable),
        (503, Unavailable),
        (504, Timeout),
        (418, Unavailable), // anything unrecognised is an outage, not a guess
    ];
    for (status, expected) in cases {
        assert_eq!(
            ProviderFailure::from_http_status(status),
            expected,
            "HTTP {status}"
        );
    }
}

/// A redacted message still classifies, because the code was never in the text.
///
/// Redaction rewrites the message — under the substring classifier that could
/// change an error's code and class on its way out.
#[test]
fn redaction_cannot_change_a_classified_code() {
    let error = failed(
        ProviderFailure::AuthFailed,
        "openai rejected key sk-abcdefghijklmnopqrstuvwxyz",
    );
    let serialized = serde_json::to_string(&error).expect("serializes");
    assert!(
        !serialized.contains("sk-abcdefghijklmnopqrstuvwxyz"),
        "the key value must be redacted: {serialized}"
    );
    assert_eq!(
        error.code(),
        "inference.provider_auth_failed",
        "redaction rewrites the message; the code must not move with it"
    );
}

// ---------------------------------------------------------------------------
// D8: the registry half. The same design bit here, and this is the pin.
// ---------------------------------------------------------------------------

/// A model that is catalogued but not downloaded reports `missing_model`,
/// whatever the message says.
///
/// The wording changed during D8 from "not found locally" to "is not
/// downloaded", which silently reclassified it to `download_failed` — because
/// `registry_code` matches "download". The CLI's download offer keys off
/// `missing_model`, so the feature was dead until the code was carried
/// explicitly. Fourth time this design bit inside one change.
#[test]
fn a_not_downloaded_model_reports_missing_model_whatever_the_wording() {
    use strata_inference::RegistryFailure;

    let worded_to_trip_the_old_classifier = InferenceError::RegistryFailed {
        kind: RegistryFailure::MissingModel,
        message: "Model 'tinyllama' is not downloaded. To download it: \
                  strata inference models pull tinyllama"
            .to_owned(),
        details: None,
    };
    assert_eq!(
        worded_to_trip_the_old_classifier.code(),
        "inference.missing_model",
        "the message mentions downloading twice; the code must not follow it"
    );
}

/// Every registry failure kind has its own code.
#[test]
fn every_registry_failure_kind_has_its_own_code() {
    use strata_inference::RegistryFailure::{
        Corrupt, DownloadDisabled, DownloadFailed, MissingModel, VerificationFailed,
    };
    let kinds = [
        MissingModel,
        DownloadDisabled,
        DownloadFailed,
        VerificationFailed,
        Corrupt,
    ];
    let mut codes: Vec<&str> = kinds.iter().map(|kind| kind.code()).collect();
    codes.sort_unstable();
    let distinct = codes.len();
    codes.dedup();
    assert_eq!(codes.len(), distinct, "two kinds share a code: {codes:?}");
}
