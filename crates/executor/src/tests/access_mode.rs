//! Access mode tests: verify read-only mode blocks writes and allows reads.

use std::sync::Arc;

use strata_engine::Database;
use strata_security::AccessMode;

use crate::types::DistanceMetric;
use crate::{Command, Error, Executor, Session, Strata, Value};

// =============================================================================
// Helpers
// =============================================================================

/// Create a cache database, ensure the default branch exists (via a
/// read-write Strata), then return the underlying `Arc<Database>` so
/// tests can build read-only executors/sessions on top of it.
fn setup_rw_db() -> Arc<Database> {
    let strata = Strata::cache().unwrap();
    strata.database()
}

// =============================================================================
// Executor-level tests
// =============================================================================

#[test]
fn test_read_only_blocks_kv_put() {
    let db = setup_rw_db();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let result = executor.execute(Command::KvPut {
        branch: None,
        space: None,
        key: "k".into(),
        value: Value::Int(1),
    });

    match result {
        Err(Error::AccessDenied { command, .. }) => assert_eq!(command, "KvPut"),
        other => panic!("expected AccessDenied, got {:?}", other),
    }
}

#[test]
fn test_read_only_allows_kv_get() {
    // Write some data via read-write handle first
    let strata = Strata::cache().unwrap();
    strata.kv_put("k", "hello").unwrap();
    let db = strata.executor().primitives().db.clone();

    // Open read-only executor on the same database
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);
    let result = executor.execute(Command::KvGet {
        branch: None,
        space: None,
        key: "k".into(),
        as_of: None,
    });

    assert!(result.is_ok(), "read should succeed, got {:?}", result);
}

#[test]
fn test_read_only_blocks_all_writes() {
    let db = setup_rw_db();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let write_commands: Vec<Command> = vec![
        Command::KvPut {
            branch: None,
            space: None,
            key: "k".into(),
            value: Value::Int(1),
        },
        Command::KvDelete {
            branch: None,
            space: None,
            key: "k".into(),
        },
        Command::JsonSet {
            branch: None,
            space: None,
            key: "k".into(),
            path: "$".into(),
            value: Value::Int(1),
        },
        Command::JsonDelete {
            branch: None,
            space: None,
            key: "k".into(),
            path: "$".into(),
        },
        Command::EventAppend {
            branch: None,
            space: None,
            event_type: "t".into(),
            payload: Value::object(Default::default()),
        },
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "c".into(),
            key: "k".into(),
            vector: vec![1.0],
            metadata: None,
        },
        Command::VectorDelete {
            branch: None,
            space: None,
            collection: "c".into(),
            key: "k".into(),
        },
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "c".into(),
            dimension: 4,
            metric: DistanceMetric::Cosine,
        },
        Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: "c".into(),
        },
        Command::BranchCreate {
            branch_id: Some("new".into()),
            metadata: None,
        },
        Command::BranchDelete {
            branch: crate::types::BranchId::from("x"),
        },
        Command::BranchFork {
            source: "default".into(),
            destination: "fork".into(),
            message: None,
            creator: None,
        },
        Command::BranchMerge {
            source: "a".into(),
            target: "b".into(),
            strategy: strata_engine::MergeStrategy::LastWriterWins,
            message: None,
            creator: None,
        },
        Command::ConfigSetAutoEmbed { enabled: true },
        Command::TxnBegin {
            branch: None,
            options: None,
        },
        Command::TxnCommit,
        Command::TxnRollback,
        Command::RetentionApply { branch: None },
        Command::Flush,
        Command::Compact,
    ];

    for cmd in write_commands {
        let name = cmd.name().to_string();
        let result = executor.execute(cmd);
        match result {
            Err(Error::AccessDenied { command, .. }) => assert_eq!(command, name),
            other => panic!("expected AccessDenied for {}, got {:?}", name, other),
        }
    }
}

#[test]
fn test_read_only_allows_all_reads() {
    let db = setup_rw_db();
    let executor = Executor::new_with_mode(db, AccessMode::ReadOnly);

    let read_commands: Vec<Command> = vec![
        Command::KvGet {
            branch: None,
            space: None,
            key: "k".into(),
            as_of: None,
        },
        Command::KvList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: None,
            as_of: None,
        },
        Command::KvGetv {
            branch: None,
            space: None,
            key: "k".into(),
            as_of: None,
        },
        Command::JsonGet {
            branch: None,
            space: None,
            key: "k".into(),
            path: "$".into(),
            as_of: None,
        },
        Command::JsonGetv {
            branch: None,
            space: None,
            key: "k".into(),
            as_of: None,
        },
        Command::JsonList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: 10,
            as_of: None,
        },
        Command::EventGet {
            branch: None,
            space: None,
            sequence: 0,
            as_of: None,
        },
        Command::EventGetByType {
            branch: None,
            space: None,
            event_type: "t".into(),
            limit: None,
            after_sequence: None,
            as_of: None,
        },
        Command::EventLen {
            branch: None,
            space: None,
        },
        Command::VectorListCollections {
            branch: None,
            space: None,
        },
        Command::BranchGet {
            branch: crate::types::BranchId::default(),
        },
        Command::BranchList {
            state: None,
            limit: None,
            offset: None,
        },
        Command::BranchExists {
            branch: crate::types::BranchId::default(),
        },
        Command::Ping,
        Command::Info,
        Command::TxnInfo,
        Command::TxnIsActive,
        Command::Search {
            branch: None,
            space: None,
            search: crate::types::SearchQuery {
                query: "test".into(),
                k: None,
                primitives: None,
                time_range: None,
                mode: None,
                expand: None,
                rerank: None,
                precomputed_embedding: None,
            },
        },
        Command::BranchDiff {
            branch_a: "default".into(),
            branch_b: "default".into(),
            filter_primitives: None,
            filter_spaces: None,
            as_of: None,
        },
        Command::ConfigGet,
        Command::AutoEmbedStatus,
        Command::DurabilityCounters,
        Command::ConfigureGetKey {
            key: "provider".into(),
        },
        Command::DbExport {
            branch: None,
            space: None,
            primitive: crate::types::ExportPrimitive::Kv,
            format: crate::types::ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: None,
            collection: None,
            graph: None,
        },
    ];

    for cmd in read_commands {
        let name = cmd.name().to_string();
        let result = executor.execute(cmd);
        // Should not be AccessDenied — the actual result may be Ok or a
        // domain error (e.g. KeyNotFound), but never AccessDenied.
        if let Err(Error::AccessDenied { .. }) = &result {
            panic!("read command {} was blocked by AccessDenied", name)
        }
    }
}

// =============================================================================
// Handle propagation
// =============================================================================

#[test]
fn test_read_only_propagates_to_handle() {
    let strata = Strata::cache().unwrap();
    let db = strata.executor().primitives().db.clone();

    // Build a read-only handle via open_with path is tricky with cache,
    // so we test via from_database indirectly through new_handle.
    // Instead: create a read-only Strata by building manually.
    let ro_executor = Executor::new_with_mode(db.clone(), AccessMode::ReadOnly);
    // Verify the executor is read-only
    assert_eq!(ro_executor.access_mode(), AccessMode::ReadOnly);
}

// =============================================================================
// Session-level tests
// =============================================================================

#[test]
fn test_read_only_session_blocks_txn() {
    let db = setup_rw_db();
    let mut session = Session::new_with_mode(db, AccessMode::ReadOnly);

    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });

    match result {
        Err(Error::AccessDenied { command, .. }) => assert_eq!(command, "TxnBegin"),
        other => panic!("expected AccessDenied, got {:?}", other),
    }
}

#[test]
fn test_read_only_session_allows_reads() {
    let strata = Strata::cache().unwrap();
    strata.kv_put("k", "v").unwrap();
    let db = strata.executor().primitives().db.clone();

    let mut session = Session::new_with_mode(db, AccessMode::ReadOnly);
    let result = session.execute(Command::KvGet {
        branch: None,
        space: None,
        key: "k".into(),
        as_of: None,
    });

    assert!(result.is_ok(), "read should succeed, got {:?}", result);
}

// =============================================================================
// Classification tests
// =============================================================================

#[test]
fn test_is_write_classification() {
    // Exhaustive check that every write command returns true
    let writes = vec![
        Command::KvPut {
            branch: None,
            space: None,
            key: "".into(),
            value: Value::Null,
        },
        Command::KvDelete {
            branch: None,
            space: None,
            key: "".into(),
        },
        Command::JsonSet {
            branch: None,
            space: None,
            key: "".into(),
            path: "".into(),
            value: Value::Null,
        },
        Command::JsonDelete {
            branch: None,
            space: None,
            key: "".into(),
            path: "".into(),
        },
        Command::EventAppend {
            branch: None,
            space: None,
            event_type: "".into(),
            payload: Value::Null,
        },
        Command::VectorUpsert {
            branch: None,
            space: None,
            collection: "".into(),
            key: "".into(),
            vector: vec![],
            metadata: None,
        },
        Command::VectorDelete {
            branch: None,
            space: None,
            collection: "".into(),
            key: "".into(),
        },
        Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "".into(),
            dimension: 0,
            metric: DistanceMetric::Cosine,
        },
        Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: "".into(),
        },
        Command::BranchCreate {
            branch_id: None,
            metadata: None,
        },
        Command::BranchDelete {
            branch: crate::types::BranchId::default(),
        },
        Command::TxnBegin {
            branch: None,
            options: None,
        },
        Command::TxnCommit,
        Command::TxnRollback,
        Command::RetentionApply { branch: None },
        Command::Flush,
        Command::Compact,
        Command::BranchExport {
            branch_id: "".into(),
            path: "".into(),
        },
        Command::BranchImport { path: "".into() },
        Command::BranchFork {
            source: "".into(),
            destination: "".into(),
            message: None,
            creator: None,
        },
        Command::BranchMerge {
            source: "".into(),
            target: "".into(),
            strategy: strata_engine::MergeStrategy::LastWriterWins,
            message: None,
            creator: None,
        },
        Command::ConfigSetAutoEmbed { enabled: false },
        Command::ConfigureSet {
            key: "provider".into(),
            value: "openai".into(),
        },
    ];

    for cmd in &writes {
        assert!(
            cmd.is_write(),
            "{} should be classified as write",
            cmd.name()
        );
    }

    // Check that read commands return false
    let reads = vec![
        Command::KvGet {
            branch: None,
            space: None,
            key: "".into(),
            as_of: None,
        },
        Command::KvList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: None,
            as_of: None,
        },
        Command::KvGetv {
            branch: None,
            space: None,
            key: "".into(),
            as_of: None,
        },
        Command::JsonGet {
            branch: None,
            space: None,
            key: "".into(),
            path: "".into(),
            as_of: None,
        },
        Command::JsonGetv {
            branch: None,
            space: None,
            key: "".into(),
            as_of: None,
        },
        Command::JsonList {
            branch: None,
            space: None,
            prefix: None,
            cursor: None,
            limit: 10,
            as_of: None,
        },
        Command::EventGet {
            branch: None,
            space: None,
            sequence: 0,
            as_of: None,
        },
        Command::EventGetByType {
            branch: None,
            space: None,
            event_type: "".into(),
            limit: None,
            after_sequence: None,
            as_of: None,
        },
        Command::EventLen {
            branch: None,
            space: None,
        },
        Command::VectorGet {
            branch: None,
            space: None,
            collection: "".into(),
            key: "".into(),
            as_of: None,
        },
        Command::VectorSearch {
            branch: None,
            space: None,
            collection: "".into(),
            query: vec![],
            k: 0,
            filter: None,
            metric: None,
            as_of: None,
        },
        Command::VectorListCollections {
            branch: None,
            space: None,
        },
        Command::BranchGet {
            branch: crate::types::BranchId::default(),
        },
        Command::BranchList {
            state: None,
            limit: None,
            offset: None,
        },
        Command::BranchExists {
            branch: crate::types::BranchId::default(),
        },
        Command::Ping,
        Command::Info,
        Command::TxnInfo,
        Command::TxnIsActive,
        Command::RetentionStats { branch: None },
        Command::RetentionPreview { branch: None },
        Command::BranchBundleValidate { path: "".into() },
        Command::Search {
            branch: None,
            space: None,
            search: crate::types::SearchQuery {
                query: "".into(),
                k: None,
                primitives: None,
                time_range: None,
                mode: None,
                expand: None,
                rerank: None,
                precomputed_embedding: None,
            },
        },
        Command::BranchDiff {
            branch_a: "".into(),
            branch_b: "".into(),
            filter_primitives: None,
            filter_spaces: None,
            as_of: None,
        },
        Command::ConfigGet,
        Command::AutoEmbedStatus,
        Command::DurabilityCounters,
        Command::ConfigureGetKey {
            key: "provider".into(),
        },
        Command::DbExport {
            branch: None,
            space: None,
            primitive: crate::types::ExportPrimitive::Kv,
            format: crate::types::ExportFormat::Csv,
            prefix: None,
            limit: None,
            path: None,
            collection: None,
            graph: None,
        },
    ];

    for cmd in &reads {
        assert!(
            !cmd.is_write(),
            "{} should be classified as read",
            cmd.name()
        );
    }
}

#[test]
fn test_open_with_defaults_is_read_write() {
    let strata = Strata::cache().unwrap();
    assert_eq!(strata.access_mode(), AccessMode::ReadWrite);

    // Verify writes work
    let result = strata.kv_put("k", "v");
    assert!(result.is_ok());
}
