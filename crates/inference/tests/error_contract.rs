//! Stable inference error contract tests.

#![allow(clippy::too_many_lines)]

use serde::de::DeserializeOwned;
use serde::Serialize;
use strata_inference::{
    AvailabilityKind, InferenceError, InferenceRuntime, InferenceRuntimeConfig, ProviderFailure,
    RegistryFailure,
};

fn round_trip<T>(value: &T) -> T
where
    T: Clone + PartialEq + std::fmt::Debug + Serialize + DeserializeOwned,
{
    let encoded = serde_json::to_string(value).expect("serializes");
    serde_json::from_str(&encoded).expect("deserializes")
}

#[test]
fn every_stable_error_code_is_carried_through_serialization() {
    // The code is the whole contract this crate owns: its class and retry
    // policy are rows of the executor's error registry, which the executor
    // wire test pins against every live envelope. A copy of those rows here
    // could only drift (#3286).
    let cases = [
        (
            InferenceError::Provider("HTTP 400 bad request".to_owned()),
            "inference.invalid_request",
        ),
        (
            InferenceError::Registry("unknown model miniLM".to_owned()),
            "inference.missing_model",
        ),
        (
            InferenceError::LlamaCpp("model load failed".to_owned()),
            "inference.model_load_failed",
        ),
        (
            InferenceError::NotSupported("openai provider not enabled".to_owned()),
            "inference.unsupported_provider",
        ),
        (
            InferenceError::NotSupported("ranking requires local feature".to_owned()),
            "inference.unsupported_operation",
        ),
        (
            InferenceError::NotSupported("unsupported parameter top_k".to_owned()),
            "inference.unsupported_parameter",
        ),
        (
            InferenceError::Provider("OPENAI_API_KEY not set".to_owned()),
            "inference.missing_api_key",
        ),
        (
            InferenceError::Provider("invalid API key".to_owned()),
            "inference.provider_auth_failed",
        ),
        (
            InferenceError::Provider("HTTP 429 rate limit".to_owned()),
            "inference.provider_rate_limited",
        ),
        (
            InferenceError::Provider("request timed out".to_owned()),
            "inference.provider_timeout",
        ),
        (
            InferenceError::Provider("HTTP 503 unavailable".to_owned()),
            "inference.provider_unavailable",
        ),
        (
            InferenceError::Provider("invalid JSON response".to_owned()),
            "inference.provider_malformed_response",
        ),
        // Billing is not throttling (#3236).
        (
            InferenceError::ProviderFailed {
                kind: ProviderFailure::QuotaExhausted,
                message: "OpenAI: You have no credits remaining. (HTTP 429)".to_owned(),
                details: None,
            },
            "inference.provider_quota_exhausted",
        ),
        // A model the provider does not serve is the caller's to fix (#3236).
        (
            InferenceError::ProviderFailed {
                kind: ProviderFailure::ModelNotFound,
                message: "Anthropic: model: claude-nope (HTTP 404)".to_owned(),
                details: None,
            },
            "inference.provider_model_not_found",
        ),
        // A name the catalog does not know is not a download away (#3256).
        (
            InferenceError::RegistryFailed {
                kind: RegistryFailure::UnknownModel,
                message: "Unknown model `nope`.".to_owned(),
                details: None,
            },
            "inference.unknown_model",
        ),
        (
            InferenceError::Registry("network access disabled".to_owned()),
            "inference.download_disabled",
        ),
        (
            InferenceError::Registry("download failed".to_owned()),
            "inference.download_failed",
        ),
        (
            InferenceError::Registry("sha-256 mismatch".to_owned()),
            "inference.download_verification_failed",
        ),
        (
            InferenceError::Registry("download sha-256 hash mismatch".to_owned()),
            "inference.download_verification_failed",
        ),
        (
            InferenceError::LlamaCpp("decode failed".to_owned()),
            "inference.local_runtime_failed",
        ),
        (
            InferenceError::Registry("corrupt registry".to_owned()),
            "inference.registry_corrupt",
        ),
        (
            InferenceError::Io("disk failed".to_owned()),
            "inference.io_failure",
        ),
    ];

    for (error, code) in cases {
        assert_eq!(error.code(), code, "{error:?}");
        assert_eq!(round_trip(&error), error, "{error:?}");
    }
}

/// A refusal from resolution carries the resolver's answer, and the answer
/// survives serialization with the message and the code beside it.
#[test]
fn a_resolution_refusal_round_trips_with_its_details() {
    let models = tempfile::tempdir().expect("tempdir");
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models.path().to_path_buf()),
        network_enabled: true,
    });
    let resolved = runtime.resolve("miniLM", None).expect("well-formed spec");
    let error = resolved.require_ready().expect_err("not downloaded");
    assert_eq!(error.code(), "inference.missing_model");

    let details = error
        .availability()
        .expect("a resolution refusal carries details");
    assert_eq!(details.availability, AvailabilityKind::NotDownloaded);
    assert_eq!(details.model, "miniLM");
    assert_eq!(details.pull_spec.as_deref(), Some("miniLM"));

    let value = serde_json::to_value(&error).expect("serializes");
    let body = &value["RegistryFailed"];
    assert_eq!(body["kind"], "missing_model");
    assert_eq!(body["details"]["availability"], "not_downloaded");
    assert_eq!(body["details"]["pull_spec"], "miniLM");
    assert_eq!(round_trip(&error), error);

    // An error raised past resolution has no answer to carry, and the wire
    // does not pretend otherwise.
    let plain = InferenceError::RegistryFailed {
        kind: RegistryFailure::Corrupt,
        message: "registry.json is unreadable".to_owned(),
        details: None,
    };
    assert_eq!(plain.availability(), None);
    let value = serde_json::to_value(&plain).expect("serializes");
    assert_eq!(value["RegistryFailed"].get("details"), None);
    assert_eq!(round_trip(&plain), plain);
}

#[test]
fn public_error_surfaces_redact_provider_secrets() {
    let secrets =
        "failed with key=sk-test-secret and url=/v1?key=AIzaabc123 and sk-ant-provider-token";
    // One string variant and every struct variant with a message: the struct
    // variants serialize by derive, so each `message` field carries its own
    // redaction attribute, and a dropped attribute would leak on the wire
    // while the string variant still passed (rule 31).
    let errors = [
        InferenceError::Provider(secrets.to_owned()),
        InferenceError::ProviderFailed {
            kind: ProviderFailure::AuthFailed,
            message: secrets.to_owned(),
            details: None,
        },
        InferenceError::RegistryFailed {
            kind: RegistryFailure::DownloadFailed,
            message: secrets.to_owned(),
            details: None,
        },
        InferenceError::Unsupported {
            kind: strata_inference::UnsupportedKind::Provider,
            message: secrets.to_owned(),
            details: None,
        },
    ];

    for error in errors {
        let display = error.to_string();
        let debug = format!("{error:?}");
        let public_message = error.public_message();
        let serialized = serde_json::to_string(&error).expect("serializes");

        for rendered in [display, debug, public_message, serialized] {
            assert!(
                !rendered.contains("sk-test-secret"),
                "secret leaked through {rendered}"
            );
            assert!(
                !rendered.contains("AIzaabc123"),
                "secret leaked through {rendered}"
            );
            assert!(
                !rendered.contains("sk-ant-provider-token"),
                "secret leaked through {rendered}"
            );
            assert!(
                rendered.contains("[REDACTED]"),
                "redaction marker missing from {rendered}"
            );
        }
    }
}

#[test]
fn invalid_model_spec_is_invalid_input_not_a_provider_error() {
    // A malformed model spec is caller input error, not a provider outage.
    // Classifying it as `provider_unavailable` (retryable in the registry)
    // would invite a client to retry a request that can never succeed.
    // (`bogus:model` is not malformed: a non-provider prefix makes the spec a
    // local model name and the registry decides whether it exists — #3222.)
    for spec in ["", "   ", "anthropic:", "local:"] {
        let error = strata_inference::parse_model_spec(spec).expect_err("invalid spec is rejected");
        assert_eq!(
            error.code(),
            "inference.invalid_request",
            "spec {spec:?} classified as invalid request"
        );
        assert!(
            matches!(error, InferenceError::InvalidSpec(_)),
            "spec {spec:?} is caller input error: {error:?}"
        );
    }
}
