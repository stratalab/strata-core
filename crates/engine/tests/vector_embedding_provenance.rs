//! A collection remembers which model produced its vectors (D9, rule 24).
//!
//! CLAUDE.md hard rule 24 has promised
//! `failed_precondition.embedding_model_mismatch` since V1 was designed. It
//! existed in neither the code nor `errors.yaml`, so the guarantee was a
//! sentence. This is the behaviour behind it.
//!
//! The failure it prevents is the quiet one. Dimension checks catch a 768-wide
//! vector going into a 384-wide collection. They cannot catch `nomic-embed`
//! vectors going into a `miniLM` collection at the same width — that returns
//! neighbours which are ranked, confident, and meaningless, and nothing
//! downstream can tell.

mod common;

use strata_engine::{
    EmbeddingModelId, EngineErrorClass, VectorCollectionName, VectorConfig, VectorDistanceMetric,
    VectorEmbedding, VectorKey,
};

use common::{
    branch, open_cache_database, open_durable_database, reopen_durable_database_after_drop, space,
};

fn collection(name: &str) -> VectorCollectionName {
    VectorCollectionName::new(name).expect("collection name")
}

fn model(name: &str) -> EmbeddingModelId {
    EmbeddingModelId::new(name).expect("model id")
}

fn config(dimension: usize) -> VectorConfig {
    VectorConfig::new(dimension, VectorDistanceMetric::Cosine).expect("config")
}

/// A recorded model survives the round trip through storage.
#[test]
fn a_collection_remembers_the_model_it_was_created_with() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    let created = vectors
        .create_collection(
            collection("docs"),
            config(384).with_embedding_model(model("miniLM")),
        )
        .expect("collection is created");
    assert_eq!(
        created
            .config()
            .embedding_model()
            .map(EmbeddingModelId::as_str),
        Some("miniLM")
    );

    // Read back through a fresh lookup, not the creation outcome.
    let info = vectors
        .collection_info(&collection("docs"))
        .expect("info read succeeds")
        .expect("collection exists");
    assert_eq!(
        info.config()
            .embedding_model()
            .map(EmbeddingModelId::as_str),
        Some("miniLM"),
        "the model is stored, not just returned from create"
    );
}

/// The provenance survives a reopen, which is what makes it provenance rather
/// than a runtime hint.
#[test]
fn the_model_survives_a_durable_reopen() {
    let directory = tempfile::tempdir().expect("tempdir");

    {
        let mut database = open_durable_database(directory.path()).expect("durable open");
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .create_collection(
                collection("docs"),
                config(384).with_embedding_model(model("openai:text-embedding-3-small")),
            )
            .expect("collection is created");
    }

    // Dropped, not closed: wait out a detached worker's writer lock (#2837).
    let mut database =
        reopen_durable_database_after_drop(directory.path()).expect("durable reopen");
    let info = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens")
        .collection_info(&collection("docs"))
        .expect("info read succeeds")
        .expect("collection survived the reopen");
    assert_eq!(
        info.config()
            .embedding_model()
            .map(EmbeddingModelId::as_str),
        Some("openai:text-embedding-3-small")
    );
}

/// The case dimension cannot catch: two models, same width.
#[test]
fn a_second_model_at_the_same_width_is_refused() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    vectors
        .create_collection(
            collection("docs"),
            config(384).with_embedding_model(model("miniLM")),
        )
        .expect("collection is created");

    // Same dimension, same metric — nothing but the recorded model can tell
    // these apart, which is the whole reason it is recorded.
    let error = vectors
        .require_embedding_model(&collection("docs"), &model("nomic-embed"))
        .expect_err("a different model is refused");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_mismatch"
    );

    // The matching model is accepted, so the refusal is about disagreement and
    // not about the check being on.
    vectors
        .require_embedding_model(&collection("docs"), &model("miniLM"))
        .expect("the recorded model is accepted");
}

/// Collections created before provenance existed keep working.
///
/// Every collection in every database today is in this state: the field
/// defaults to absent, so the check has nothing to disagree with. Refusing
/// them here would break existing data to enforce a rule they predate.
#[test]
fn a_collection_without_a_recorded_model_accepts_any_model() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    vectors
        .create_collection(collection("legacy"), config(384))
        .expect("collection is created");

    let info = vectors
        .collection_info(&collection("legacy"))
        .expect("info read succeeds")
        .expect("collection exists");
    assert!(
        info.config().embedding_model().is_none(),
        "no model is recorded, which is a legal state"
    );

    for any in ["miniLM", "nomic-embed", "openai:text-embedding-3-small"] {
        vectors
            .require_embedding_model(&collection("legacy"), &model(any))
            .expect("a collection with no recorded model accepts any");
    }
}

/// A model identifier is validated, but its shape is not the engine's business:
/// engine never speaks to a provider, so it cannot know which names are real.
#[test]
fn model_identifiers_are_validated_without_knowing_any_provider() {
    // Accepted: bare names and provider-prefixed specs alike.
    for accepted in ["miniLM", "openai:text-embedding-3-small", "some/vendor:v2"] {
        EmbeddingModelId::new(accepted).expect(accepted);
    }

    // Refused: the shapes that would corrupt a stored record.
    let empty = EmbeddingModelId::new("").expect_err("empty is refused");
    assert_eq!(empty.class(), EngineErrorClass::InvalidInput);
    assert_eq!(empty.code(), "invalid_argument.engine.embedding_model");

    EmbeddingModelId::new("has\0nul").expect_err("a NUL byte is refused");
    EmbeddingModelId::new("has\nnewline").expect_err("a newline is refused");
    EmbeddingModelId::new("x".repeat(257)).expect_err("an over-long id is refused");
}

/// Provenance is per collection, so one space can hold collections from
/// different models without either contaminating the other.
#[test]
fn two_collections_can_record_different_models() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    vectors
        .create_collection(
            collection("small"),
            config(384).with_embedding_model(model("miniLM")),
        )
        .expect("first collection");
    vectors
        .create_collection(
            collection("large"),
            config(768).with_embedding_model(model("nomic-embed")),
        )
        .expect("second collection");

    vectors
        .require_embedding_model(&collection("small"), &model("miniLM"))
        .expect("first collection keeps its own model");
    vectors
        .require_embedding_model(&collection("large"), &model("nomic-embed"))
        .expect("second collection keeps its own model");
    vectors
        .require_embedding_model(&collection("small"), &model("nomic-embed"))
        .expect_err("the other collection's model is still refused here");
}

/// Text has to be embedded with *some* model, and the collection is where that
/// is recorded. A collection that records none refuses with its own code, and
/// the refusal names the command that declares one — a caller that reads only
/// the code can tell "no model" from "wrong model", and one that reads the
/// message knows what to run.
#[test]
fn text_needs_a_recorded_model() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    vectors
        .create_collection(collection("legacy"), config(8))
        .expect("collection is created");
    vectors
        .create_collection(
            collection("docs"),
            config(8).with_embedding_model(model("miniLM")),
        )
        .expect("collection is created");

    let error = vectors
        .recorded_embedding_model(&collection("legacy"))
        .expect_err("a collection without a model cannot embed text");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_missing"
    );
    assert!(
        error
            .to_string()
            .contains("vector collection set-embedding-model legacy"),
        "the refusal names the command that declares a model: {error}"
    );

    assert_eq!(
        vectors
            .recorded_embedding_model(&collection("docs"))
            .expect("a collection with a model reports it"),
        model("miniLM")
    );

    let error = vectors
        .recorded_embedding_model(&collection("absent"))
        .expect_err("a missing collection is not a missing model");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

/// Every collection that predates provenance is model-less, so there has to be
/// a way to declare a model after the fact. Declaring is one-time: it takes
/// the caller's word for the vectors already present, re-declaring the same
/// model changes nothing, and declaring a different one is the mixing rule 24
/// forbids.
#[test]
fn a_model_is_declared_once() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    vectors
        .create_collection(collection("legacy"), config(2))
        .expect("collection is created");
    vectors
        .upsert(
            collection("legacy"),
            VectorKey::new("v1").expect("key"),
            VectorEmbedding::new(vec![1.0, 0.0]).expect("embedding"),
            None,
        )
        .expect("a vector is stored before any model is declared");

    let declared = vectors
        .declare_embedding_model(&collection("legacy"), model("miniLM"))
        .expect("a model-less collection accepts a declaration");
    assert_eq!(
        declared
            .config()
            .embedding_model()
            .map(EmbeddingModelId::as_str),
        Some("miniLM")
    );
    assert_eq!(declared.config().dimension(), 2, "the shape is untouched");
    assert_eq!(declared.count(), 1, "the existing vector is kept");

    // Stored, not just returned: a fresh lookup and the text path both see it.
    let info = vectors
        .collection_info(&collection("legacy"))
        .expect("info read succeeds")
        .expect("collection exists");
    assert_eq!(
        info.config()
            .embedding_model()
            .map(EmbeddingModelId::as_str),
        Some("miniLM")
    );
    assert_eq!(
        vectors
            .recorded_embedding_model(&collection("legacy"))
            .expect("the declared model is what text embeds with"),
        model("miniLM")
    );

    // Re-declaring the recorded model is a no-op: nothing is committed, so the
    // config row's version does not move.
    let again = vectors
        .declare_embedding_model(&collection("legacy"), model("miniLM"))
        .expect("re-declaring the same model is accepted");
    assert_eq!(again, info, "a repeated declaration commits nothing");

    let error = vectors
        .declare_embedding_model(&collection("legacy"), model("nomic-embed"))
        .expect_err("a different model is refused");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_mismatch"
    );
    assert_eq!(
        vectors
            .recorded_embedding_model(&collection("legacy"))
            .expect("the recorded model is unchanged by the refusal"),
        model("miniLM")
    );

    let error = vectors
        .declare_embedding_model(&collection("absent"), model("miniLM"))
        .expect_err("there is nothing to declare on");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

/// A snapshot read sees the model the collection recorded *then*.
///
/// A declaration vouches for the vectors present when it is made. Vectors the
/// collection held before — since overwritten or deleted — were vouched for by
/// nobody, so a text search at a snapshot older than the declaration has no
/// model to embed with, exactly as it had none at the time. Reading the model
/// from the head of the branch would quietly compare an embedding made now
/// against vectors of unknown origin.
#[test]
fn the_recorded_model_is_read_at_the_snapshot_being_searched() {
    let mut database = open_cache_database().expect("cache open succeeds");
    let mut vectors = database
        .vector(branch("default"), space("default"))
        .expect("vector service opens");

    // A commit from before `legacy` existed, to read it at.
    let earlier = vectors
        .create_collection(collection("other"), config(2))
        .expect("collection is created")
        .created_timestamp();
    vectors
        .create_collection(collection("legacy"), config(2))
        .expect("collection is created");
    let before = vectors
        .upsert(
            collection("legacy"),
            VectorKey::new("v1").expect("key"),
            VectorEmbedding::new(vec![1.0, 0.0]).expect("embedding"),
            None,
        )
        .expect("a vector is stored before any model is declared")
        .commit()
        .timestamp();
    // The info a declaration returns carries the commit that rewrote the
    // config row — the declaration's own position on the timeline.
    let declared = vectors
        .declare_embedding_model(&collection("legacy"), model("miniLM"))
        .expect("a model-less collection accepts a declaration")
        .created_timestamp();
    assert!(before < declared, "the declaration is a later commit");

    // Before the declaration: no model, and no remedy either — the message
    // must not send the caller to declare one, since that would not change
    // what this snapshot recorded.
    let error = vectors
        .recorded_embedding_model_at(&collection("legacy"), before)
        .expect_err("a snapshot older than the declaration has no model");
    assert_eq!(error.class(), EngineErrorClass::Conflict);
    assert_eq!(
        error.code(),
        "failed_precondition.engine.embedding_model_missing"
    );
    assert!(
        !error.to_string().contains("set-embedding-model"),
        "declaring a model now cannot fix a snapshot that predates it: {error}"
    );

    // At and after the declaration: the model it recorded.
    assert_eq!(
        vectors
            .recorded_embedding_model_at(&collection("legacy"), declared)
            .expect("the declaration's own commit records the model"),
        model("miniLM")
    );
    assert_eq!(
        vectors
            .recorded_embedding_model(&collection("legacy"))
            .expect("the head of the branch records the model"),
        model("miniLM")
    );

    // A snapshot before the collection existed is a missing collection, the
    // same answer a vector search at that snapshot gives.
    let error = vectors
        .recorded_embedding_model_at(&collection("legacy"), earlier)
        .expect_err("the collection did not exist yet");
    assert_eq!(error.class(), EngineErrorClass::NotFound);
    assert_eq!(error.code(), "not_found.engine.vector_collection");
}

/// A declaration is provenance like any other: it survives a reopen.
#[test]
fn a_declared_model_survives_a_durable_reopen() {
    let directory = tempfile::tempdir().expect("tempdir");

    {
        let mut database = open_durable_database(directory.path()).expect("durable open");
        let mut vectors = database
            .vector(branch("default"), space("default"))
            .expect("vector service opens");
        vectors
            .create_collection(collection("legacy"), config(384))
            .expect("collection is created");
        vectors
            .declare_embedding_model(&collection("legacy"), model("miniLM"))
            .expect("declaration succeeds");
    }

    // Dropped, not closed: wait out a detached worker's writer lock (#2837).
    let mut database =
        reopen_durable_database_after_drop(directory.path()).expect("durable reopen");
    assert_eq!(
        database
            .vector(branch("default"), space("default"))
            .expect("vector service opens")
            .recorded_embedding_model(&collection("legacy"))
            .expect("the declaration survived the reopen"),
        model("miniLM")
    );
}

/// The registry entries are what a caller sees *without* hitting the error —
/// the docs page, `agents errors`, an MCP tool description. No registry keyword
/// arm matches `embedding_model`, so without their own entries these three
/// codes fell through to the class-generic sentence and a fix of "reload
/// current state and retry", which does not fix any of them. Each entry must
/// say what the code means and name the actual remedy.
#[test]
fn the_embedding_model_codes_document_their_own_remedy() {
    let entry = |code: &str| {
        strata_engine::error_code_registry_entry(code)
            .unwrap_or_else(|| panic!("{code} is registered"))
    };

    let missing = entry("failed_precondition.engine.embedding_model_missing");
    assert!(
        missing.message_template.contains("no embedding model"),
        "message says what is missing: {}",
        missing.message_template
    );
    assert!(
        missing
            .suggested_fix
            .contains("vector collection set-embedding-model"),
        "fix names the declaring command: {}",
        missing.suggested_fix
    );

    let mismatch = entry("failed_precondition.engine.embedding_model_mismatch");
    assert!(
        mismatch.message_template.contains("does not match"),
        "message says the model disagrees: {}",
        mismatch.message_template
    );
    assert!(
        mismatch.suggested_fix.contains("vector collection stats"),
        "fix names where the recorded model is shown: {}",
        mismatch.suggested_fix
    );

    let invalid = entry("invalid_argument.engine.embedding_model");
    assert!(
        invalid.message_template.contains("model id"),
        "message names the field: {}",
        invalid.message_template
    );
    assert!(
        invalid.suggested_fix.contains("non-empty model id"),
        "fix says what a valid id looks like: {}",
        invalid.suggested_fix
    );
}
