//! Retention and maintenance command behavior contract tests.
//!
//! These tests lock in current executor/session semantics so architecture docs
//! and code cannot silently drift.

use crate::common::*;
use strata_executor::{Command, Error, Output};

#[test]
fn retention_apply_is_implemented_and_returns_unit() {
    let executor = create_executor();
    let out = executor
        .execute(Command::RetentionApply { branch: None })
        .unwrap();
    assert!(matches!(out, Output::Unit));
}

#[test]
fn retention_stats_and_preview_are_not_yet_implemented() {
    let executor = create_executor();

    let stats = executor.execute(Command::RetentionStats { branch: None });
    let preview = executor.execute(Command::RetentionPreview { branch: None });

    match stats {
        Err(Error::Internal { reason }) => {
            assert!(
                reason.contains("not yet implemented"),
                "unexpected reason: {reason}"
            );
        }
        other => panic!(
            "expected Internal not implemented for stats, got {:?}",
            other
        ),
    }

    match preview {
        Err(Error::Internal { reason }) => {
            assert!(
                reason.contains("not yet implemented"),
                "unexpected reason: {reason}"
            );
        }
        other => panic!(
            "expected Internal not implemented for preview, got {:?}",
            other
        ),
    }
}

#[test]
fn session_flush_and_compact_remain_available_during_transaction() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let flush = session.execute(Command::Flush).unwrap();
    assert!(matches!(flush, Output::Unit));

    let compact = session.execute(Command::Compact).unwrap();
    assert!(matches!(compact, Output::Unit));

    session.execute(Command::TxnRollback).unwrap();
}
