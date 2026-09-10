//! Hermetic executor-level inference coverage (TCP3.9c).
//!
//! The executor's only other inference tests run the real `InferenceRuntime`
//! and need live provider keys + network (`inference_integration.rs`). This
//! drives every inference command through the executor dispatch against the
//! deterministic `FakeInferenceService` — no network, no model files, no keys —
//! proving the injection seam works end-to-end and pinning each command's
//! mapped output.

#![cfg(all(feature = "testkit", feature = "inference"))]

use strata_executor::{Command, Executor, Output};
use strata_inference::testkit::FakeInferenceService;
use strata_inference::{
    ChatRequest, EmbedInput, EmbeddingsRequest, RankRequest, RankRuntimeOutcome,
};

fn executor() -> Executor {
    Executor::open_cache()
        .expect("cache executor opens")
        .with_inference_runtime(FakeInferenceService::new())
}

#[test]
fn models_list_returns_the_fake_catalog() {
    let mut executor = executor();
    let Output::InferenceModels { items, .. } = executor
        .execute(Command::InferenceModelsList {})
        .expect("models list")
    else {
        panic!("unexpected output");
    };
    assert_eq!(items.len(), 3);
    assert!(items.iter().any(|model| model.name == "fake-embed"));
    assert!(items.iter().all(|model| !model.is_local));
}

#[test]
fn models_local_is_empty() {
    let mut executor = executor();
    let Output::InferenceModels { items, .. } = executor
        .execute(Command::InferenceModelsLocal {})
        .expect("models local")
    else {
        panic!("unexpected output");
    };
    assert!(items.is_empty());
}

#[test]
fn pull_returns_a_path_under_the_directory_status_names() {
    let mut executor = executor();
    let Output::InferenceStatus(status) = executor
        .execute(Command::InferenceStatus {})
        .expect("status")
    else {
        panic!("unexpected output");
    };
    let Output::InferenceModelPulled(output) = executor
        .execute(Command::InferenceModelsPull {
            model: "fake-generate".to_owned(),
        })
        .expect("pull")
    else {
        panic!("unexpected output");
    };
    assert_eq!(output.model, "fake-generate");
    // The fake's world is a directory that does not exist on any machine, so
    // nothing here reads a real file — but it is one directory, and pull and
    // status must agree on it.
    assert_eq!(output.path, status.models_dir.join("fake-generate.gguf"));
    assert!(!output.path.exists(), "the fake world is not on disk");
}

#[test]
fn capability_reports_a_local_all_capable_model() {
    let mut executor = executor();
    let Output::InferenceCapability(capability) = executor
        .execute(Command::InferenceModelCapability {
            model: "fake-generate".to_owned(),
        })
        .expect("capability")
    else {
        panic!("unexpected output");
    };
    assert!(capability.can_generate && capability.can_embed && capability.can_rank);
    assert!(!capability.requires_network && !capability.requires_api_key);
}

#[test]
fn generate_echoes_the_prompt_deterministically() {
    let mut executor = executor();
    let request = ChatRequest {
        prompt: Some("hello".to_owned()),
        ..Default::default()
    };
    let Output::InferenceGeneration(response) = executor
        .execute(Command::InferenceGenerate {
            model: "fake-generate".to_owned(),
            request,
        })
        .expect("generate")
    else {
        panic!("unexpected output");
    };
    assert_eq!(response.model, "fake-generate");
    assert_eq!(response.choices[0].message.content, "fake:hello");
}

#[test]
fn tokenize_then_detokenize_round_trips() {
    let mut executor = executor();
    let Output::InferenceTokenIds(ids) = executor
        .execute(Command::InferenceTokenize {
            model: "fake-generate".to_owned(),
            text: "hi".to_owned(),
            add_special: false,
        })
        .expect("tokenize")
    else {
        panic!("unexpected output");
    };
    assert_eq!(ids, vec![104, 105]); // bytes of "hi"

    let Output::InferenceText(text) = executor
        .execute(Command::InferenceDetokenize {
            model: "fake-generate".to_owned(),
            ids,
        })
        .expect("detokenize")
    else {
        panic!("unexpected output");
    };
    assert_eq!(text, "hi");
}

#[test]
fn embed_produces_a_fixed_dimension_vector() {
    let mut executor = executor();
    let request = EmbeddingsRequest {
        input: EmbedInput::One("hello".to_owned()),
        dimensions: None,
        normalize: None,
        input_type: None,
        instruction: None,
    };
    let Output::InferenceEmbeddings(response) = executor
        .execute(Command::InferenceEmbed {
            model: "fake-embed".to_owned(),
            request,
        })
        .expect("embed")
    else {
        panic!("unexpected output");
    };
    assert_eq!(response.dimension, 8);
    assert_eq!(response.data.len(), 1);
    assert_eq!(response.data[0].embedding.len(), 8);
}

// The fake scores are integer overlap counts cast to f32 (0.0 / 1.0 exactly),
// so the exact value is the contract.
#[allow(clippy::float_cmp)]
#[test]
fn rank_scores_passages_by_query_overlap() {
    let mut executor = executor();
    let request = RankRequest {
        query: "vector database".to_owned(),
        passages: vec!["vector search".to_owned(), "unrelated text".to_owned()],
    };
    let Output::InferenceRanking(response) = executor
        .execute(Command::InferenceRank {
            model: "fake-rank".to_owned(),
            request,
        })
        .expect("rank")
    else {
        panic!("unexpected output");
    };
    // "vector search" shares one token ("vector") with the query; "unrelated
    // text" shares none.
    match (&response.items[0], &response.items[1]) {
        (
            RankRuntimeOutcome::Ok { score: first, .. },
            RankRuntimeOutcome::Ok { score: second, .. },
        ) => {
            assert_eq!(*first, 1.0);
            assert_eq!(*second, 0.0);
        }
        other => panic!("unexpected rank items: {other:?}"),
    }
}

#[test]
fn unload_and_cache_status_report_an_empty_fake_cache() {
    let mut executor = executor();
    let Output::InferenceUnloadResult { unloaded } = executor
        .execute(Command::InferenceUnload { model: None })
        .expect("unload")
    else {
        panic!("unexpected output");
    };
    assert!(!unloaded);

    let Output::InferenceCacheStatus(status) = executor
        .execute(Command::InferenceCacheStatus {})
        .expect("cache status")
    else {
        panic!("unexpected output");
    };
    assert!(status.generation_models.is_empty());
    assert!(status.embedding_models.is_empty());
    assert!(status.ranking_models.is_empty());
}
