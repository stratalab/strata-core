//! Executor inference command behavior tests.

#![cfg(feature = "inference")]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use strata_executor::cli_metadata::{CliCommandCatalog, CliCommandEntry};
use strata_executor::{
    public_error_code_entries, Command, CommitOutcomeStatus, ErrorClass, Executor, ExecutorError,
    ExecutorErrorClass, Output, PageInfo, RetryPolicy,
};
use strata_inference::{
    ChatChoice, ChatMessage, ChatRequest, ChatResponse, EmbedInput, EmbeddingItem,
    EmbeddingsRequest, EmbeddingsResponse, FinishReason, InferenceCapability, InferenceError,
    InferenceRuntime, InferenceRuntimeConfig, ModelCacheStatus, ModelInfo, ModelTask,
    ProviderFailure, ProviderKey, ProviderKind, ProviderSettings, PullModelOutput, RankRequest,
    RankResponse, RankRuntimeOutcome, RegistryFailure, Role, Usage,
};

fn output_round_trip(value: &Output) -> Output {
    let encoded = serde_json::to_string(value).expect("output serializes");
    serde_json::from_str(&encoded).expect("output deserializes")
}

#[test]
fn inference_commands_round_trip_through_json() {
    let cases = vec![
        Command::InferenceModelsList {},
        Command::InferenceModelsLocal {},
        Command::InferenceModelsPull {
            model: "miniLM".to_owned(),
        },
        Command::InferenceModelCapability {
            model: "openai:gpt-4o-mini".to_owned(),
        },
        Command::InferenceGenerate {
            model: "openai:gpt-4o-mini".to_owned(),
            request: ChatRequest {
                prompt: Some("hello".to_owned()),
                max_tokens: Some(8),
                ..ChatRequest::default()
            },
        },
        Command::InferenceTokenize {
            model: "local:gpt2".to_owned(),
            text: "hello".to_owned(),
            add_special: true,
        },
        Command::InferenceDetokenize {
            model: "local:gpt2".to_owned(),
            ids: vec![1, 2, 3],
        },
        Command::InferenceEmbed {
            model: "openai:text-embedding-3-small".to_owned(),
            request: EmbeddingsRequest {
                input: EmbedInput::One("hello".to_owned()),
                dimensions: None,
                normalize: None,
                input_type: None,
                instruction: None,
            },
        },
        Command::InferenceEmbed {
            model: "openai:text-embedding-3-small".to_owned(),
            request: EmbeddingsRequest {
                input: EmbedInput::Many(vec!["a".to_owned(), "b".to_owned()]),
                dimensions: None,
                normalize: None,
                input_type: None,
                instruction: None,
            },
        },
        Command::InferenceRank {
            model: "local:/models/reranker.gguf".to_owned(),
            request: RankRequest {
                query: "q".to_owned(),
                passages: vec!["p".to_owned()],
            },
        },
        Command::InferenceUnload {
            model: Some("local:gpt2".to_owned()),
        },
        Command::InferenceCacheStatus {},
    ];

    for command in cases {
        let encoded = serde_json::to_string(&command).expect("command serializes");
        let decoded: Command = serde_json::from_str(&encoded).expect("command deserializes");
        assert_eq!(decoded, command);
    }
}

#[test]
fn inference_outputs_round_trip_through_json() {
    let model = ModelInfo {
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
    let capability = InferenceCapability {
        provider: ProviderKind::OpenAI,
        model: "gpt-4o-mini".to_owned(),
        availability: strata_inference::AvailabilityKind::Ready,
        pull_spec: None,
        size_bytes: None,
        can_generate: true,
        can_tokenize: false,
        can_embed: false,
        can_rank: false,
        requires_network: true,
        requires_api_key: true,
        provider_feature_enabled: true,
        network_enabled: true,
        embedding_dim: 0,
        supports_tools: true,
        supports_json_object: true,
        supports_json_schema: true,
        supports_logprobs: true,
    };
    let cases = vec![
        Output::InferenceModels {
            items: vec![model],
            page: PageInfo::terminal(),
        },
        Output::InferenceModelPulled(PullModelOutput {
            model: "miniLM".to_owned(),
            path: PathBuf::from("/tmp/miniLM.gguf"),
        }),
        Output::InferenceCapability(capability),
        Output::InferenceGeneration(ChatResponse {
            model: "openai:gpt-4o-mini".to_owned(),
            choices: vec![ChatChoice {
                index: 0,
                message: ChatMessage::new(Role::Assistant, "hello"),
                finish_reason: FinishReason::Stop,
                logprobs: None,
            }],
            usage: Usage {
                prompt_tokens: 2,
                completion_tokens: 1,
                total_tokens: 3,
            },
        }),
        Output::InferenceTokenIds(vec![1, 2, 3]),
        Output::InferenceText("hello".to_owned()),
        Output::InferenceEmbeddings(EmbeddingsResponse {
            model: "openai:text-embedding-3-small".to_owned(),
            data: vec![
                EmbeddingItem {
                    index: 0,
                    embedding: vec![0.1, 0.2, 0.3],
                },
                EmbeddingItem {
                    index: 1,
                    embedding: vec![0.4, 0.5, 0.6],
                },
            ],
            dimension: 3,
            usage: Usage {
                prompt_tokens: 4,
                completion_tokens: 0,
                total_tokens: 4,
            },
        }),
        Output::InferenceRanking(RankResponse {
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
        }),
        Output::InferenceUnloadResult { unloaded: true },
        Output::InferenceCacheStatus(ModelCacheStatus {
            generation_models: vec!["local:qwen3".to_owned()],
            embedding_models: vec!["openai:text-embedding-3-small".to_owned()],
            ranking_models: vec!["local:reranker".to_owned()],
        }),
    ];

    for output in cases {
        assert_eq!(output_round_trip(&output), output);
    }
}

#[test]
fn inference_errors_preserve_stable_code_class_and_redaction_through_executor() {
    let error: ExecutorError = InferenceError::Provider("OPENAI_API_KEY not set".to_owned()).into();
    assert_eq!(error.code(), "inference.missing_api_key");
    assert_eq!(error.class(), ExecutorErrorClass::Unavailable);
    assert_eq!(error.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(error.retry_policy(), RetryPolicy::AfterStateChange);
    assert_eq!(error.commit_outcome(), CommitOutcomeStatus::NotApplicable);
    assert!(error.retryable());

    let secret_error: ExecutorError =
        InferenceError::Provider("request failed for sk-test-secret".to_owned()).into();
    let encoded = serde_json::to_string(&secret_error).expect("error serializes");
    assert!(!secret_error.message().contains("sk-test-secret"));
    assert!(!encoded.contains("sk-test-secret"));
    assert!(encoded.contains("[REDACTED]"));
}

#[test]
fn inference_error_retry_policies_match_v1_contract() {
    let verification_error: ExecutorError =
        InferenceError::Registry("sha-256 hash mismatch".to_owned()).into();
    assert_eq!(
        verification_error.code(),
        "inference.download_verification_failed"
    );
    assert_eq!(
        verification_error.retry_policy(),
        RetryPolicy::AfterStateChange
    );

    let local_runtime_error: ExecutorError =
        InferenceError::LlamaCpp("context allocation failed".to_owned()).into();
    assert_eq!(local_runtime_error.code(), "inference.local_runtime_failed");
    assert_eq!(local_runtime_error.retry_policy(), RetryPolicy::Unknown);
}

/// One `InferenceError` per way the inference crate can fail. The string
/// variants pick their code from the message, so each message here is chosen
/// to land on a distinct code; the structured variants carry theirs.
///
/// The list is hand-maintained over the inference crate's `#[non_exhaustive]`
/// enums, so it grows by hand: a kind added without a registry row is caught
/// only once it is listed here (the sweep then finds no row for its code),
/// while a registry row added without a producer is caught unconditionally.
fn every_constructible_inference_error() -> Vec<InferenceError> {
    let message = || "probe".to_owned();
    let mut errors = vec![
        InferenceError::LlamaCpp("context allocation failed".to_owned()),
        InferenceError::LlamaCpp("model load failed".to_owned()),
        InferenceError::Provider(message()),
        InferenceError::Registry(message()),
        InferenceError::Io(message()),
        InferenceError::NotSupported(message()),
        InferenceError::NotSupported("provider probe".to_owned()),
        InferenceError::NotSupported("parameter probe".to_owned()),
        InferenceError::InvalidSpec(message()),
    ];
    errors.extend(
        [
            RegistryFailure::UnknownModel,
            RegistryFailure::MissingModel,
            RegistryFailure::DownloadDisabled,
            RegistryFailure::DownloadFailed,
            RegistryFailure::VerificationFailed,
            RegistryFailure::Corrupt,
        ]
        .into_iter()
        .map(|kind| InferenceError::RegistryFailed {
            kind,
            message: message(),
            details: None,
        }),
    );
    errors.extend(
        [
            ProviderFailure::MissingApiKey,
            ProviderFailure::AuthFailed,
            ProviderFailure::InvalidRequest,
            ProviderFailure::RateLimited,
            ProviderFailure::QuotaExhausted,
            ProviderFailure::ModelNotFound,
            ProviderFailure::Timeout,
            ProviderFailure::Unavailable,
            ProviderFailure::MalformedResponse,
        ]
        .into_iter()
        .map(|kind| InferenceError::ProviderFailed {
            kind,
            message: message(),
            details: None,
        }),
    );
    errors
}

/// The registry is the single authority for a code's retry policy and
/// suggested fix: what an inference error carries onto the wire must be the
/// row `strata agents errors` documents, for every code the inference crate
/// can produce (#3243). The sweep also proves every `inference.*` row is
/// reachable and that every listed error keeps its own code (an unregistered
/// code would be rewritten to `internal.executor.unregistered_code`).
///
/// This pins wire == registry, not the registry's values themselves; those
/// literal contract values stay pinned by
/// `inference_error_retry_policies_match_v1_contract` above, which is why
/// that test is not folded into this one.
#[test]
fn test_inference_errors_carry_the_registry_retry_policy_and_suggested_fix() {
    let entries: Vec<_> = public_error_code_entries().collect();
    let mut reached = BTreeSet::new();
    let mut mismatches = Vec::new();
    for inference_error in every_constructible_inference_error() {
        // Look the row up by the inference crate's own code, before the
        // conversion can normalize an unregistered one away.
        let code = inference_error.code();
        let entry = entries
            .iter()
            .find(|entry| entry.code == code)
            .unwrap_or_else(|| panic!("{code} is not a registered code"));
        let error: ExecutorError = inference_error.into();
        assert_eq!(error.code(), code, "conversion changed the code");
        reached.insert(entry.code);
        if error.retry_policy() != entry.retry_policy {
            mismatches.push(format!(
                "{}: retry_policy {:?} on the wire, {:?} in the registry",
                entry.code,
                error.retry_policy(),
                entry.retry_policy
            ));
        }
        if error.suggested_fix() != entry.suggested_fix {
            mismatches.push(format!(
                "{}: suggested_fix {:?} on the wire, {:?} in the registry",
                entry.code,
                error.suggested_fix(),
                entry.suggested_fix
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "inference errors disagree with the registry:\n{}",
        mismatches.join("\n")
    );

    let unreached: Vec<_> = entries
        .iter()
        .map(|entry| entry.code)
        .filter(|code| code.starts_with("inference.") && !reached.contains(code))
        .collect();
    assert!(
        unreached.is_empty(),
        "registry rows no constructible inference error reaches: {unreached:?}"
    );
}

/// The executor renders every field of the resolver's answer, including the
/// one that is not a string: `size_bytes` lands as its decimal text, not as
/// an absent or empty entry. Pinned on the real resolver's not-downloaded
/// answer, the only availability that carries a number; the wire test's
/// relation 4 mirrors the flattening and so cannot see this arm alone.
#[test]
fn a_not_downloaded_refusal_renders_its_size_as_decimal_text() {
    let models = tempfile::tempdir().expect("models dir");
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models.path().to_path_buf()),
        network_enabled: true,
    });
    let resolved = runtime.resolve("miniLM", None).expect("well-formed spec");
    let refusal = resolved.require_ready().expect_err("not downloaded");
    let size_bytes = refusal
        .availability()
        .and_then(|details| details.size_bytes)
        .expect("a catalogued model has a size");
    assert!(size_bytes > 0, "the catalog records a real size");

    let error = ExecutorError::from(refusal);
    assert_eq!(error.code(), "inference.missing_model");
    let details: std::collections::BTreeMap<&str, &str> = error
        .status()
        .details()
        .iter()
        .map(|detail| (detail.key(), detail.value()))
        .collect();
    assert_eq!(
        details.get("size_bytes").copied(),
        Some(size_bytes.to_string().as_str())
    );
    assert_eq!(details.get("availability").copied(), Some("not_downloaded"));
    assert_eq!(details.get("pull_spec").copied(), Some("miniLM"));
    // Nothing named a collection, so the refusal does not invent one.
    assert_eq!(details.get("collection"), None);
}

/// One transport, one classifier, one error set: every cloud provider call
/// goes through the same request path, so a command that reaches a provider
/// can fail with any `ProviderFailure`. The IDL must therefore declare either
/// none of those codes or all of them for each command — `inference.embed`
/// declared five of nine, and the generated reference and every SDK built from
/// it under-documented what `embed` can fail with (#3239).
///
/// The provider codes come from the same hand-maintained error list the
/// registry sweep above keeps complete: a kind without a registry row, or a
/// row without a producer, fails there first.
#[test]
fn test_commands_that_reach_a_cloud_provider_declare_every_provider_failure_code() {
    let provider_codes: BTreeSet<&str> = every_constructible_inference_error()
        .iter()
        .filter_map(|error| match error {
            InferenceError::ProviderFailed { kind, .. } => Some(kind.code()),
            _ => None,
        })
        .collect();
    assert!(
        provider_codes.len() > 1,
        "the error list names more than one provider failure"
    );
    // A malformed model spec maps to the same code as a provider's 400, so
    // that code alone does not say a command reached a provider
    // (`inference.capability` declares it and never leaves the process).
    let spec_code = InferenceError::InvalidSpec("probe".to_owned()).code();
    assert!(provider_codes.contains(spec_code));

    let catalog = CliCommandCatalog::embedded().expect("embedded command catalog loads");
    let mut reaches_provider = BTreeSet::new();
    let mut partial = Vec::new();
    for command in catalog.commands() {
        let declared: BTreeSet<&str> = command
            .errors
            .iter()
            .map(|error| error.code.as_str())
            .collect();
        if !declared
            .iter()
            .any(|code| *code != spec_code && provider_codes.contains(code))
        {
            continue;
        }
        reaches_provider.insert(command.id.as_str());
        let missing: Vec<_> = provider_codes.difference(&declared).copied().collect();
        if !missing.is_empty() {
            partial.push(format!("{}: missing {missing:?}", command.id));
        }
    }
    assert!(
        partial.is_empty(),
        "commands that reach a cloud provider declare only some of its failure codes:\n{}",
        partial.join("\n")
    );
    // Every cloud-calling command must have been held to the rule, or the
    // pass above is vacuous. The classifier is declaration-driven — a command
    // that reaches a provider but declares no provider code is invisible to
    // it — so a new command that calls the cloud transport is added here.
    // The two vector commands reach it when given `--text` (#3247).
    for id in [
        "inference.generate",
        "inference.embed",
        "vector.upsert",
        "vector.query",
    ] {
        assert!(
            reaches_provider.contains(id),
            "{id} was not classified as reaching a provider: {reaches_provider:?}"
        );
    }
}

/// `vector.upsert --text` and `vector.query --text` embed the text through the
/// same `Inference::embeddings` call `inference.embed` dispatches to, so
/// everything `embed` can fail with, they can fail with — yet they declared
/// only the engine's missing-model precondition and no `inference.*` code at
/// all (#3247). The rule: a command on the text-embedding path declares
/// exactly the `inference.*` codes `inference.embed` declares — no fewer, and
/// no extra ones either, since it has no inference path `embed` lacks. Each
/// such command references the `inference.model_runtime` error set
/// (`error-sets.yaml`, #3250), so the authored lists cannot drift apart; this
/// test is what keeps the set itself aligned with the runtime, and what
/// catches a text-embedding command that stops referencing it.
///
/// `failed_precondition.engine.embedding_model_missing` is the marker: it is
/// raised only when a command resolves a collection's recorded model in order
/// to embed text, so declaring it *is* being on that path.
#[test]
fn test_commands_that_embed_text_declare_every_inference_embed_error_code() {
    const MARKER: &str = "failed_precondition.engine.embedding_model_missing";

    fn inference_codes(command: &CliCommandEntry) -> BTreeSet<&str> {
        command
            .errors
            .iter()
            .map(|error| error.code.as_str())
            .filter(|code| code.starts_with("inference."))
            .collect()
    }

    let catalog = CliCommandCatalog::embedded().expect("embedded command catalog loads");
    let embed = catalog
        .commands()
        .iter()
        .find(|command| command.id == "inference.embed")
        .expect("inference.embed is in the embedded catalog");
    let embed_codes = inference_codes(embed);
    // The set being copied has to be a real one, or a hollowed-out `embed`
    // would make every text-embedding command pass trivially. `embed`
    // declares exactly what the runtime can construct, less the three
    // download outcomes: `embed` never downloads (D8 offers `pull`, a
    // separate command, which declares them), so a not-downloaded model is
    // its `missing_model`, not a download that failed.
    let runtime_codes: BTreeSet<&str> = every_constructible_inference_error()
        .iter()
        .filter(|error| {
            !matches!(
                error,
                InferenceError::RegistryFailed {
                    kind: RegistryFailure::DownloadDisabled
                        | RegistryFailure::DownloadFailed
                        | RegistryFailure::VerificationFailed,
                    ..
                }
            )
        })
        .map(InferenceError::code)
        .collect();
    let undeclared: Vec<_> = runtime_codes.difference(&embed_codes).copied().collect();
    let unproducible: Vec<_> = embed_codes.difference(&runtime_codes).copied().collect();
    assert!(
        undeclared.is_empty() && unproducible.is_empty(),
        "inference.embed declares exactly the codes the runtime can produce on its path; \
         undeclared {undeclared:?}, declared without a producer {unproducible:?}"
    );

    let mut embeds_text = BTreeSet::new();
    let mut misaligned = Vec::new();
    for command in catalog.commands() {
        if !command.errors.iter().any(|error| error.code == MARKER) {
            continue;
        }
        embeds_text.insert(command.id.as_str());
        let codes = inference_codes(command);
        let missing: Vec<_> = embed_codes.difference(&codes).copied().collect();
        let extra: Vec<_> = codes.difference(&embed_codes).copied().collect();
        if !missing.is_empty() || !extra.is_empty() {
            misaligned.push(format!(
                "{}: missing {missing:?}, extra {extra:?}",
                command.id
            ));
        }
    }
    assert!(
        misaligned.is_empty(),
        "commands that embed text do not declare inference.embed's inference.* codes exactly:\n{}",
        misaligned.join("\n")
    );
    // Both text-embedding commands must have been held to the rule, or the
    // pass above is vacuous. A new command that embeds text through the
    // collection's model declares the marker and is added here.
    for id in ["vector.upsert", "vector.query"] {
        assert!(
            embeds_text.contains(id),
            "{id} was not classified as embedding text: {embeds_text:?}"
        );
    }
}

#[test]
fn model_list_and_capability_execute_with_default_cloud_providers() {
    let mut executor = Executor::open_cache().expect("executor opens");
    let output = executor
        .execute(Command::InferenceModelsList {})
        .expect("model list succeeds");
    let Output::InferenceModels { items: models, .. } = output else {
        panic!("expected inference model output");
    };
    assert!(models.iter().any(|model| model.name == "miniLM"));

    let output = executor
        .execute(Command::InferenceModelCapability {
            model: "openai:text-embedding-3-small".to_owned(),
        })
        .expect("capability succeeds");
    let Output::InferenceCapability(InferenceCapability {
        requires_api_key,
        requires_network,
        can_embed,
        provider_feature_enabled,
        network_enabled,
        ..
    }) = output
    else {
        panic!("expected capability output");
    };
    assert!(requires_api_key);
    assert!(requires_network);
    assert!(can_embed);
    assert!(provider_feature_enabled);
    assert!(network_enabled);
}

/// A models directory the filesystem will not read is not an empty one. The
/// resolver refuses with `inference.io_failure` instead of answering "not
/// downloaded" — which would have offered to download a model that may be
/// sitting right there — and the code reaches the wire through both the
/// locating verb (`capability`) and a running one (`embed`) (#3252). A
/// symlink to itself fails `metadata` with ELOOP for root too, so the test
/// does not depend on permissions.
#[cfg(unix)]
#[test]
fn an_unreadable_models_directory_is_an_io_failure_on_every_resolving_verb() {
    let dir = tempfile::tempdir().expect("tempdir");
    let looped = dir.path().join("loop");
    std::os::unix::fs::symlink(&looped, &looped).expect("symlink loop");
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(looped),
        network_enabled: false,
    });
    let mut executor = Executor::open_cache()
        .expect("executor opens")
        .with_inference_runtime(runtime);

    let commands = [
        Command::InferenceModelCapability {
            model: "miniLM".to_owned(),
        },
        Command::InferenceEmbed {
            model: "miniLM".to_owned(),
            request: EmbeddingsRequest {
                input: EmbedInput::One("hello".to_owned()),
                dimensions: None,
                normalize: None,
                input_type: None,
                instruction: None,
            },
        },
    ];
    for command in commands {
        let name = format!("{command:?}");
        let err = executor
            .execute(command)
            .expect_err("the models directory cannot be read");
        assert_eq!(err.code(), "inference.io_failure", "{name}");
        assert_eq!(err.public_class(), ErrorClass::Io, "{name}");
    }
}

/// Provider settings that hold no key for any provider: the runtime learns
/// keys from what it was constructed with (#3221), so the test states the
/// world instead of editing the process environment.
struct NoKeys;

impl ProviderSettings for NoKeys {
    fn key(&self, _provider: ProviderKind) -> Option<ProviderKey> {
        None
    }
}

#[test]
fn cloud_generate_reports_missing_api_key_without_a_key() {
    let runtime = InferenceRuntime::with_settings(
        InferenceRuntimeConfig {
            models_dir: None,
            network_enabled: true,
        },
        Arc::new(NoKeys),
    );
    let mut executor = Executor::open_cache()
        .expect("executor opens")
        .with_inference_runtime(runtime);

    let err = executor
        .execute(Command::InferenceGenerate {
            model: "openai:gpt-4o-mini".to_owned(),
            request: ChatRequest {
                prompt: Some("hello".to_owned()),
                max_tokens: Some(4),
                ..ChatRequest::default()
            },
        })
        .expect_err("missing API key is reported before provider call");
    assert_eq!(err.code(), "inference.missing_api_key");
    assert_eq!(err.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(err.retry_policy(), RetryPolicy::AfterStateChange);
}
