//! Public inference API contract tests.

#![allow(clippy::float_cmp)]

use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use strata_inference::{
    parse_model_spec, AvailabilityKind, EmbedRequest, EmbedResponse, EmbedRuntimeOutcome,
    GenerateRequest, GenerateResponse, InferenceCapability, InferenceRuntime,
    InferenceRuntimeConfig, ModelCacheStatus, ModelInfo, ModelTask, ProviderKind, PullModelOutput,
    RankRequest, RankResponse, RankRuntimeOutcome, StopReason,
};

fn round_trip<T>(value: &T) -> T
where
    T: Clone + PartialEq + std::fmt::Debug + Serialize + DeserializeOwned,
{
    let encoded = serde_json::to_string(value).expect("serializes");
    serde_json::from_str(&encoded).expect("deserializes")
}

#[test]
fn request_response_dtos_round_trip_through_json() {
    let generation_request = GenerateRequest {
        prompt: "prompt with\nunicode marker".to_owned(),
        max_tokens: 0,
        temperature: 0.25,
        top_k: 40,
        top_p: 0.9,
        seed: Some(42),
        stop_sequences: vec!["stop".to_owned()],
        stop_tokens: vec![1, 2, 3],
        grammar: Some("root ::= object".to_owned()),
    };
    assert_eq!(round_trip(&generation_request), generation_request);

    let generation_response = GenerateResponse {
        text: "answer".to_owned(),
        stop_reason: StopReason::MaxTokens,
        prompt_tokens: 7,
        completion_tokens: 3,
    };
    assert_eq!(round_trip(&generation_response), generation_response);

    let embed_request = EmbedRequest {
        text: "embedded database".to_owned(),
    };
    assert_eq!(round_trip(&embed_request), embed_request);

    let embed_response = EmbedResponse {
        dimension: 3,
        items: vec![
            EmbedRuntimeOutcome::Ok {
                vector: vec![0.1, 0.2, 0.3],
            },
            EmbedRuntimeOutcome::Error {
                code: "inference.provider_unavailable".to_owned(),
                message: "provider unavailable".to_owned(),
            },
        ],
    };
    assert_eq!(round_trip(&embed_response), embed_response);

    let rank_request = RankRequest {
        query: "embedded database".to_owned(),
        passages: vec!["database".to_owned(), "compiler".to_owned()],
    };
    assert_eq!(round_trip(&rank_request), rank_request);

    let rank_response = RankResponse {
        items: vec![
            RankRuntimeOutcome::Ok {
                index: 0,
                score: 0.9,
            },
            RankRuntimeOutcome::Error {
                index: 1,
                code: "inference.local_runtime_failed".to_owned(),
                message: "runtime failed".to_owned(),
            },
        ],
    };
    assert_eq!(round_trip(&rank_response), rank_response);
}

#[test]
fn diagnostics_dtos_round_trip_without_provider_payloads() {
    let capability = InferenceCapability {
        provider: ProviderKind::OpenAI,
        model: "text-embedding-3-small".to_owned(),
        availability: AvailabilityKind::Ready,
        pull_spec: None,
        size_bytes: None,
        can_generate: true,
        can_tokenize: false,
        can_embed: true,
        can_rank: false,
        requires_network: true,
        requires_api_key: true,
        provider_feature_enabled: cfg!(feature = "openai"),
        network_enabled: true,
        embedding_dim: 1536,
        supports_tools: true,
        supports_json_object: true,
        supports_json_schema: true,
        supports_logprobs: true,
    };
    assert_eq!(round_trip(&capability), capability);

    let info = ModelInfo {
        name: "miniLM".to_owned(),
        task: ModelTask::Embed,
        architecture: "bert".to_owned(),
        default_quant: "f16".to_owned(),
        embedding_dim: 384,
        is_local: false,
        runnable: false,
        local_path: None,
        size_bytes: 45_000_000,
        hf_repo: "stratalab-org/all-MiniLM-L6-v2-GGUF".to_owned(),
    };
    assert_eq!(round_trip(&info), info);

    let pull = PullModelOutput {
        model: "miniLM".to_owned(),
        path: PathBuf::from("/tmp/miniLM.gguf"),
    };
    assert_eq!(round_trip(&pull), pull);

    let cache_status = ModelCacheStatus {
        generation_models: vec!["local:qwen3:1.7b:q8_0".to_owned()],
        embedding_models: vec!["openai:text-embedding-3-small".to_owned()],
        ranking_models: vec!["local:jina-reranker-v1-tiny".to_owned()],
    };
    assert_eq!(round_trip(&cache_status), cache_status);
}

#[test]
fn defaults_deserialize_without_network_or_download() {
    let request: GenerateRequest = serde_json::from_value(json!({
        "prompt": "hello"
    }))
    .expect("defaulted generation request");
    assert_eq!(request.prompt, "hello");
    assert_eq!(request.max_tokens, 256);
    assert_eq!(request.temperature, 0.0);
    assert_eq!(request.top_k, 0);
    assert_eq!(request.top_p, 1.0);
    assert!(request.seed.is_none());
    assert!(request.stop_sequences.is_empty());
    assert!(request.stop_tokens.is_empty());
    assert!(request.grammar.is_none());

    // An empty models directory, so the model is not on disk: a pull of a
    // downloaded model is a no-op success whatever the network says.
    let models_dir = tempfile::tempdir().expect("temp models dir");
    let config = InferenceRuntimeConfig {
        models_dir: Some(models_dir.path().to_path_buf()),
        network_enabled: false,
    };
    let runtime = InferenceRuntime::new(config);
    let err = runtime
        .pull_model("miniLM")
        .expect_err("network-disabled runtime refuses downloads");
    assert_eq!(err.code(), "inference.download_disabled");
}

#[test]
fn stable_enum_strings_are_part_of_the_wire_contract() {
    assert_eq!(serde_json::to_value(ProviderKind::Local).unwrap(), "local");
    assert_eq!(
        serde_json::to_value(ProviderKind::Anthropic).unwrap(),
        "anthropic"
    );
    assert_eq!(
        serde_json::to_value(ProviderKind::OpenAI).unwrap(),
        "openai"
    );
    assert_eq!(
        serde_json::to_value(ProviderKind::Google).unwrap(),
        "google"
    );

    assert_eq!(serde_json::to_value(ModelTask::Embed).unwrap(), "embed");
    assert_eq!(
        serde_json::to_value(ModelTask::Generate).unwrap(),
        "generate"
    );
    assert_eq!(serde_json::to_value(ModelTask::Rank).unwrap(), "rank");

    assert_eq!(
        serde_json::to_value(StopReason::StopToken).unwrap(),
        "stop_token"
    );
    assert_eq!(
        serde_json::to_value(StopReason::MaxTokens).unwrap(),
        "max_tokens"
    );
    assert_eq!(
        serde_json::to_value(StopReason::ContextLength).unwrap(),
        "context_length"
    );
    assert_eq!(
        serde_json::to_value(StopReason::Cancelled).unwrap(),
        "cancelled"
    );
}

#[test]
fn model_spec_parser_keeps_provider_model_ids_opaque_after_first_colon() {
    let cases = [
        ("miniLM", ProviderKind::Local, "miniLM"),
        ("local:miniLM", ProviderKind::Local, "miniLM"),
        (
            "local:qwen3:1.7b:q8_0",
            ProviderKind::Local,
            "qwen3:1.7b:q8_0",
        ),
        (
            "anthropic:claude-sonnet-4-6",
            ProviderKind::Anthropic,
            "claude-sonnet-4-6",
        ),
        ("openai:gpt-4o-mini", ProviderKind::OpenAI, "gpt-4o-mini"),
        (
            "google:models/gemini-embedding-001",
            ProviderKind::Google,
            "models/gemini-embedding-001",
        ),
        (
            "openai:org/model.name:revision-1",
            ProviderKind::OpenAI,
            "org/model.name:revision-1",
        ),
    ];

    for (spec, expected_provider, expected_model) in cases {
        let (provider, model) = parse_model_spec(spec).expect(spec);
        assert_eq!(provider, expected_provider, "{spec}");
        assert_eq!(model, expected_model, "{spec}");
    }

    let suffixes = [
        "name.with.dots",
        "repo/model-name",
        "family:variant:quant",
        "org/model.name:revision-1",
        "path/with/slash-and-dash:q8_0",
    ];
    for suffix in suffixes {
        let spec = format!("openai:{suffix}");
        let (provider, model) = parse_model_spec(&spec).expect(&spec);
        assert_eq!(provider, ProviderKind::OpenAI);
        assert_eq!(model, suffix);
    }
}

#[test]
fn model_spec_parser_trims_outer_whitespace_and_accepts_provider_casing() {
    let (provider, model) = parse_model_spec("  OpenAI:gpt-4o-mini  ").expect("trimmed spec");
    assert_eq!(provider, ProviderKind::OpenAI);
    assert_eq!(model, "gpt-4o-mini");
}

#[test]
fn model_spec_parser_rejects_malformed_specs() {
    for spec in ["", "   ", "openai:", "local:"] {
        assert!(parse_model_spec(spec).is_err(), "{spec:?} should fail");
    }
}

/// A first segment that is not a provider name is the start of a local model
/// name — the catalog's own `family:size` and `name:quant` forms are
/// colon-shaped, and `models list` prints them (#3222). Existence is the
/// registry's call, so `unknown:model` parses too.
#[test]
fn test_model_spec_parser_treats_a_non_provider_prefix_as_a_local_name() {
    for spec in ["qwen3:1.7b", "tinyllama:q8_0", "unknown:model", ":model"] {
        let (provider, model) = parse_model_spec(spec).expect(spec);
        assert_eq!(provider, ProviderKind::Local, "{spec}");
        assert_eq!(model, spec, "{spec}");
    }
}

#[test]
fn capability_reports_runtime_and_feature_availability_separately() {
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: None,
        network_enabled: false,
    });
    let capability = runtime
        .capability("openai:text-embedding-3-small")
        .expect("capability is metadata-only");

    assert_eq!(capability.provider, ProviderKind::OpenAI);
    assert!(capability.can_generate);
    assert!(capability.can_embed);
    assert!(!capability.can_tokenize);
    assert!(capability.requires_network);
    assert!(capability.requires_api_key);
    assert_eq!(
        capability.provider_feature_enabled,
        cfg!(feature = "openai")
    );
    assert!(!capability.network_enabled);
}
