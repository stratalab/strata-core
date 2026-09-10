//! Text goes through the model the collection records, and nowhere else
//! (D9 + D10), driven hermetically.
//!
//! `vector_text_embedding.rs` proves the *quality* claim against the real
//! local embedder and so only runs where that model is installed. This file
//! pins the *routing* claims with the deterministic `FakeInferenceService`, so
//! they run on every PR:
//!
//! - the model handed to the provider is the one the collection records, and
//!   a stored text is embedded as a document while a searched text is embedded
//!   as a query;
//! - a collection with no recorded model refuses text with a code of its own
//!   that names the command which declares one, and declaring it turns the
//!   same request into a success;
//! - a declaration is one-time: repeating it is a no-op, changing it is a
//!   mismatch;
//! - an embedding that fails, or a vector the collection rejects, leaves
//!   nothing written;
//! - a refusal from model resolution names the collection whose recorded
//!   model was refused, beside the resolver's own answer (#3226).

#![cfg(all(feature = "testkit", feature = "inference"))]
// `ExecutorResult` mirrors the wire error type; the helpers below return it.
#![allow(clippy::result_large_err)]

use std::sync::{Arc, Mutex};

use strata_executor::{
    Command, ErrorClass, Executor, ExecutorResult, Output, VectorDistanceMetric,
};
use strata_inference::testkit::FakeInferenceService;
use strata_inference::{
    ChatRequest, ChatResponse, EmbeddingsRequest, EmbeddingsResponse, InferenceCapability,
    InferenceError, InferenceRuntime, InferenceRuntimeConfig, InferenceService, InferenceStatus,
    InputType, ModelCacheStatus, ModelInfo, PullModelOutput, RankRequest, RankResponse,
};

/// The fake embeds into 8 dimensions.
const DIMENSION: u64 = 8;
const WIDTH: usize = 8;

/// One embedding call as the provider saw it: which model, and whether the
/// text was presented as a document or a query.
type EmbedCall = (String, Option<InputType>);

/// Wraps the fake so a test can see what the executor asked the provider for,
/// and can make the provider fail.
struct Recording {
    inner: FakeInferenceService,
    calls: Arc<Mutex<Vec<EmbedCall>>>,
    fail_embeddings: bool,
}

impl Recording {
    fn new(calls: Arc<Mutex<Vec<EmbedCall>>>) -> Self {
        Self {
            inner: FakeInferenceService::new(),
            calls,
            fail_embeddings: false,
        }
    }

    fn failing(calls: Arc<Mutex<Vec<EmbedCall>>>) -> Self {
        Self {
            fail_embeddings: true,
            ..Self::new(calls)
        }
    }
}

impl InferenceService for Recording {
    fn list_models(&self) -> Vec<ModelInfo> {
        self.inner.list_models()
    }

    fn list_local_models(&self) -> Vec<ModelInfo> {
        self.inner.list_local_models()
    }

    fn pull_model(&self, model: &str) -> Result<PullModelOutput, InferenceError> {
        self.inner.pull_model(model)
    }

    fn capability(&self, model_spec: &str) -> Result<InferenceCapability, InferenceError> {
        self.inner.capability(model_spec)
    }

    fn chat(
        &self,
        model_spec: &str,
        request: &ChatRequest,
    ) -> Result<ChatResponse, InferenceError> {
        self.inner.chat(model_spec, request)
    }

    fn tokenize(
        &self,
        model_spec: &str,
        text: &str,
        add_special: bool,
    ) -> Result<Vec<u32>, InferenceError> {
        self.inner.tokenize(model_spec, text, add_special)
    }

    fn detokenize(&self, model_spec: &str, ids: &[u32]) -> Result<String, InferenceError> {
        self.inner.detokenize(model_spec, ids)
    }

    fn embeddings(
        &self,
        model_spec: &str,
        request: &EmbeddingsRequest,
    ) -> Result<EmbeddingsResponse, InferenceError> {
        self.calls
            .lock()
            .expect("call log is not poisoned")
            .push((model_spec.to_owned(), request.input_type));
        if self.fail_embeddings {
            return Err(InferenceError::Provider(
                "fake: provider unavailable".to_owned(),
            ));
        }
        self.inner.embeddings(model_spec, request)
    }

    fn rank(
        &self,
        model_spec: &str,
        request: &RankRequest,
    ) -> Result<RankResponse, InferenceError> {
        self.inner.rank(model_spec, request)
    }

    fn unload(&self, model_spec: Option<&str>) -> Result<bool, InferenceError> {
        self.inner.unload(model_spec)
    }

    fn cache_status(&self) -> Result<ModelCacheStatus, InferenceError> {
        self.inner.cache_status()
    }

    fn status(&self) -> InferenceStatus {
        self.inner.status()
    }
}

struct Harness {
    executor: Executor,
    calls: Arc<Mutex<Vec<EmbedCall>>>,
}

impl Harness {
    fn new() -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let executor = Executor::open_cache()
            .expect("cache executor opens")
            .with_inference_runtime(Recording::new(Arc::clone(&calls)));
        Self { executor, calls }
    }

    fn with_failing_provider() -> Self {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let executor = Executor::open_cache()
            .expect("cache executor opens")
            .with_inference_runtime(Recording::failing(Arc::clone(&calls)));
        Self { executor, calls }
    }

    fn calls(&self) -> Vec<EmbedCall> {
        self.calls.lock().expect("call log is not poisoned").clone()
    }

    fn create(&mut self, collection: &str, dimension: u64, model: Option<&str>) {
        self.executor
            .execute(Command::VectorCreateCollection {
                branch: None,
                space: None,
                collection: collection.to_owned(),
                dimension,
                metric: VectorDistanceMetric::Cosine,
                embedding_model: model.map(str::to_owned),
            })
            .expect("collection creates");
    }

    fn upsert_text(&mut self, collection: &str, key: &str, text: &str) -> ExecutorResult<Output> {
        self.executor.execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            key: key.to_owned(),
            vector: Vec::new(),
            text: Some(text.to_owned()),
            metadata: None,
        })
    }

    fn query_text(&mut self, collection: &str, text: &str) -> ExecutorResult<Output> {
        self.query_text_as_of(collection, text, None)
    }

    fn query_text_as_of(
        &mut self,
        collection: &str,
        text: &str,
        as_of: Option<u64>,
    ) -> ExecutorResult<Output> {
        self.executor.execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            query: Vec::new(),
            text: Some(text.to_owned()),
            k: 5,
            filter: None,
            as_of,
            as_of_time: None,
        })
    }

    fn query_vector_as_of(
        &mut self,
        collection: &str,
        query: Vec<f64>,
        as_of: Option<u64>,
    ) -> ExecutorResult<Output> {
        self.executor.execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: collection.to_owned(),
            query,
            text: None,
            k: 5,
            filter: None,
            as_of,
            as_of_time: None,
        })
    }

    /// Stores a raw vector and returns the commit's logical timestamp — the
    /// value `as_of` takes.
    fn upsert_vector(&mut self, collection: &str, key: &str, vector: Vec<f32>) -> u64 {
        match self
            .executor
            .vector_upsert(collection, key, vector, None)
            .expect("raw vector upserts")
        {
            Output::VectorWriteResult { commit, .. } => commit.timestamp(),
            other => panic!("unexpected upsert output: {other:?}"),
        }
    }

    fn declare(&mut self, collection: &str, model: &str) -> ExecutorResult<Output> {
        self.executor.vector_set_embedding_model(collection, model)
    }

    fn count(&mut self, collection: &str) -> u64 {
        match self.executor.vector_count(collection).expect("count") {
            Output::Uint(count) => count,
            other => panic!("unexpected count output: {other:?}"),
        }
    }

    fn recorded_model(&mut self, collection: &str) -> Option<String> {
        match self
            .executor
            .vector_collection_stats(collection)
            .expect("stats")
        {
            Output::VectorCollectionList { items, .. } => {
                assert_eq!(items.len(), 1);
                items[0].embedding_model().map(str::to_owned)
            }
            other => panic!("unexpected stats output: {other:?}"),
        }
    }
}

fn matched_keys(output: Output) -> Vec<String> {
    match output {
        Output::VectorMatches(matches) => matches
            .into_iter()
            .map(|entry| entry.key().to_owned())
            .collect(),
        other => panic!("unexpected query output: {other:?}"),
    }
}

#[test]
fn text_is_embedded_with_the_recorded_model_as_document_then_query() {
    let mut harness = Harness::new();
    harness.create("docs", DIMENSION, Some("fake-embed"));

    harness
        .upsert_text("docs", "n1", "the quick brown fox")
        .expect("text upserts");
    harness
        .upsert_text("docs", "n2", "an unrelated sentence")
        .expect("text upserts");
    let keys = matched_keys(
        harness
            .query_text("docs", "the quick brown fox")
            .expect("text queries"),
    );

    // The same text embeds to the same vector, so it is its own nearest
    // neighbour — the round trip went through one model.
    assert_eq!(keys.first().map(String::as_str), Some("n1"));
    assert_eq!(harness.count("docs"), 2);
    assert_eq!(
        harness.calls(),
        vec![
            ("fake-embed".to_owned(), Some(InputType::Document)),
            ("fake-embed".to_owned(), Some(InputType::Document)),
            ("fake-embed".to_owned(), Some(InputType::Query)),
        ],
        "every call names the collection's model; stores are documents, searches are queries"
    );
}

#[test]
fn a_collection_without_a_model_refuses_text_and_names_the_remedy() {
    let mut harness = Harness::new();
    harness.create("raw", DIMENSION, None);

    for error in [
        harness
            .upsert_text("raw", "k", "hello")
            .expect_err("text upsert is refused"),
        harness
            .query_text("raw", "hello")
            .expect_err("text query is refused"),
    ] {
        assert_eq!(error.public_class(), ErrorClass::FailedPrecondition);
        assert_eq!(
            error.code(),
            "failed_precondition.engine.embedding_model_missing"
        );
        assert!(
            error
                .to_string()
                .contains("vector collection set-embedding-model raw"),
            "the refusal names the command that declares a model: {error}"
        );
    }
    assert!(
        harness.calls().is_empty(),
        "no provider is consulted when there is no model to consult"
    );
    assert_eq!(harness.count("raw"), 0);
}

#[test]
fn declaring_a_model_turns_the_refusal_into_a_store() {
    let mut harness = Harness::new();
    harness.create("raw", DIMENSION, None);
    harness
        .upsert_text("raw", "k", "hello")
        .expect_err("text is refused before a model is declared");

    let Output::VectorCollectionList { items, .. } = harness
        .declare("raw", "fake-embed")
        .expect("a model can be declared on a model-less collection")
    else {
        panic!("declare returns the collection's facts");
    };
    assert_eq!(items[0].embedding_model(), Some("fake-embed"));

    harness
        .upsert_text("raw", "k", "hello")
        .expect("the same request now stores");
    assert_eq!(
        matched_keys(harness.query_text("raw", "hello").expect("text queries")),
        vec!["k".to_owned()]
    );
    assert_eq!(harness.recorded_model("raw"), Some("fake-embed".to_owned()));
    assert!(harness
        .calls()
        .iter()
        .all(|(model, _)| model == "fake-embed"));
}

/// A text search at a snapshot embeds with the model the collection recorded
/// *at that snapshot*, not the one it records now.
///
/// The two can differ in exactly one way, since a model is declared once and
/// never changed: the snapshot predates the declaration. The vectors visible
/// then were vouched for by nobody — the declaration speaks for the vectors
/// present when it was made — so the search is refused, as it would have been
/// at the time, and the caller is pointed at a vector query rather than at a
/// declaration that cannot reach the past. The same snapshot stays searchable
/// with a vector, and a snapshot from after the declaration searches with it.
#[test]
fn a_text_query_at_a_snapshot_uses_the_model_recorded_then() {
    let mut harness = Harness::new();
    harness.create("legacy", DIMENSION, None);
    // Written before any model existed: a vector nobody vouched for.
    let before = harness.upsert_vector("legacy", "orphan", vec![1.0; WIDTH]);
    harness
        .declare("legacy", "fake-embed")
        .expect("a model-less collection accepts a declaration");
    let after = match harness
        .upsert_text("legacy", "k", "hello")
        .expect("text stores once a model is declared")
    {
        Output::VectorWriteResult { commit, .. } => commit.timestamp(),
        other => panic!("unexpected upsert output: {other:?}"),
    };
    let calls_before_the_searches = harness.calls().len();

    // Before the declaration: refused, and the provider is never asked.
    let error = harness
        .query_text_as_of("legacy", "hello", Some(before))
        .expect_err("a snapshot older than the declaration has no model to embed with");
    assert_eq!(error.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_missing"
    );
    assert!(
        !error.to_string().contains("set-embedding-model"),
        "declaring a model now cannot change what that snapshot recorded: {error}"
    );
    assert_eq!(
        harness.calls().len(),
        calls_before_the_searches,
        "no provider is consulted for a snapshot that recorded no model"
    );

    // The refusal is about text needing a model, not about the snapshot: a
    // vector still searches it.
    assert_eq!(
        matched_keys(
            harness
                .query_vector_as_of("legacy", vec![1.0; WIDTH], Some(before))
                .expect("a vector query searches the old snapshot")
        ),
        vec!["orphan".to_owned()]
    );

    // From the declaration on: embedded with the model recorded then.
    let keys = matched_keys(
        harness
            .query_text_as_of("legacy", "hello", Some(after))
            .expect("a snapshot after the declaration searches with its model"),
    );
    assert_eq!(keys.first().map(String::as_str), Some("k"));
    assert_eq!(
        harness.calls().last(),
        Some(&("fake-embed".to_owned(), Some(InputType::Query)))
    );
    assert_eq!(
        matched_keys(
            harness
                .query_text("legacy", "hello")
                .expect("the head of the branch searches with its model")
        )
        .first()
        .map(String::as_str),
        Some("k")
    );
}

#[test]
fn a_declaration_is_one_time() {
    let mut harness = Harness::new();
    harness.create("docs", DIMENSION, Some("fake-embed"));
    harness
        .upsert_text("docs", "k", "hello")
        .expect("text upserts");

    // Re-declaring the recorded model is a no-op, not an error.
    harness
        .declare("docs", "fake-embed")
        .expect("the recorded model can be declared again");

    // Declaring another one is refused: the stored vector came from
    // `fake-embed`, and provenance is not a setting to flip.
    let error = harness
        .declare("docs", "other-embed")
        .expect_err("a different model is refused");
    assert_eq!(error.public_class(), ErrorClass::FailedPrecondition);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_mismatch"
    );
    assert_eq!(
        harness.recorded_model("docs"),
        Some("fake-embed".to_owned())
    );

    let error = harness
        .declare("docs", "")
        .expect_err("an empty model id is refused");
    assert_eq!(error.code(), "invalid_argument.engine.embedding_model");

    let error = harness
        .declare("absent", "fake-embed")
        .expect_err("an absent collection is refused");
    assert_eq!(error.public_class(), ErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

#[test]
fn a_vector_and_a_text_together_or_neither_is_an_input_error() {
    let mut harness = Harness::new();
    harness.create("docs", DIMENSION, Some("fake-embed"));

    let error = harness
        .executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "k".to_owned(),
            vector: vec![1.0; WIDTH],
            text: Some("hello".to_owned()),
            metadata: None,
        })
        .expect_err("both is ambiguous");
    assert_eq!(error.code(), "invalid_argument.executor.vector_input");

    let error = harness
        .executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: Vec::new(),
            text: None,
            k: 5,
            filter: None,
            as_of: None,
            as_of_time: None,
        })
        .expect_err("neither is nothing to search with");
    assert_eq!(error.code(), "invalid_argument.executor.vector_input");

    assert!(harness.calls().is_empty());
    assert_eq!(harness.count("docs"), 0);
}

#[test]
fn a_failed_embedding_writes_nothing() {
    let mut harness = Harness::with_failing_provider();
    harness.create("docs", DIMENSION, Some("fake-embed"));

    let error = harness
        .upsert_text("docs", "k", "hello")
        .expect_err("the provider is down");
    assert_eq!(error.public_class(), ErrorClass::Unavailable);
    assert_eq!(error.code(), "inference.provider_unavailable");
    assert_eq!(harness.calls().len(), 1, "the provider was asked once");
    assert_eq!(
        harness.count("docs"),
        0,
        "the embedding precedes the write; when it fails, nothing is written"
    );
}

#[test]
fn an_embedding_the_collection_rejects_writes_nothing() {
    let mut harness = Harness::new();
    // The fake embeds into 8 dimensions; a 3-wide collection cannot hold it.
    harness.create("narrow", 3, Some("fake-embed"));

    let error = harness
        .upsert_text("narrow", "k", "hello")
        .expect_err("the embedding is the wrong width");
    assert_eq!(error.code(), "invalid_argument.engine.vector_dimension");
    assert_eq!(harness.calls().len(), 1);
    assert_eq!(harness.count("narrow"), 0);
}

/// The model a `--text` call is refused for came from the collection, not
/// the command, so the refusal says which collection recorded it — beside
/// the resolver's answer, so a client can tell "not in the catalog" from
/// "not downloaded" without parsing the message (#3226, #3256).
#[test]
fn a_resolution_refusal_names_the_collection_and_carries_the_answer() {
    // The real runtime, so the refusal is the resolver's: a name the catalog
    // does not know is `unknown_model` in every build, before the build's
    // local-execution or key checks get a say. The models directory is a
    // fresh tempdir, so nothing on this machine can make the name resolve.
    let models = tempfile::tempdir().expect("models dir");
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models.path().to_path_buf()),
        network_enabled: false,
    });
    let mut executor = Executor::open_cache()
        .expect("cache executor opens")
        .with_inference_runtime(runtime);
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            dimension: DIMENSION,
            metric: VectorDistanceMetric::Cosine,
            embedding_model: Some("local:nope".to_owned()),
        })
        .expect("collection creates");

    let error = executor
        .execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            key: "k".to_owned(),
            vector: Vec::new(),
            text: Some("hello".to_owned()),
            metadata: None,
        })
        .expect_err("the recorded model is not a model this binary knows");
    assert_eq!(error.code(), "inference.unknown_model");
    assert_eq!(error.public_class(), ErrorClass::NotFound);

    let details: std::collections::BTreeMap<&str, &str> = error
        .status()
        .details()
        .iter()
        .map(|detail| (detail.key(), detail.value()))
        .collect();
    assert_eq!(
        details,
        [
            ("availability", "not_in_catalog"),
            ("collection", "docs"),
            ("model", "nope"),
            ("provider", "local"),
        ]
        .into_iter()
        .collect()
    );

    // The same refusal for a query, which reads the model from the same
    // record.
    let error = executor
        .execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: "docs".to_owned(),
            query: Vec::new(),
            text: Some("hello".to_owned()),
            k: 5,
            filter: None,
            as_of: None,
            as_of_time: None,
        })
        .expect_err("the recorded model is not a model this binary knows");
    assert_eq!(error.code(), "inference.unknown_model");
    assert!(
        error
            .status()
            .details()
            .iter()
            .any(|detail| detail.key() == "collection" && detail.value() == "docs"),
        "{:?}",
        error.status().details()
    );
}

/// A failure past resolution — the provider itself — has no answer to carry,
/// and the wire does not invent one.
#[test]
fn a_provider_failure_carries_no_resolution_details() {
    let mut harness = Harness::with_failing_provider();
    harness.create("docs", DIMENSION, Some("fake-embed"));

    let error = harness
        .upsert_text("docs", "k", "hello")
        .expect_err("the provider is down");
    assert_eq!(error.code(), "inference.provider_unavailable");
    assert!(
        error.status().details().is_empty(),
        "{:?}",
        error.status().details()
    );
}
