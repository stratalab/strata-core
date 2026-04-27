use strata_core::Value;
use strata_executor::{BranchId, Command, Error, Output};

use crate::common::{create_db, create_executor};

const TEST_BRANCH: &str = "metadata-mainline";

fn ensure_branch(executor: &strata_executor::Executor) {
    match executor
        .execute(Command::BranchCreate {
            branch_id: Some(TEST_BRANCH.into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => assert_eq!(info.id.as_str(), TEST_BRANCH),
        other => panic!("Expected BranchWithVersion, got {other:?}"),
    }
}

#[test]
fn tags_round_trip_through_executor() {
    let executor = create_executor();
    ensure_branch(&executor);

    executor
        .execute(Command::KvPut {
            branch: Some(TEST_BRANCH.into()),
            space: None,
            key: "tag-anchor".into(),
            value: Value::String("anchor".into()),
        })
        .unwrap();

    let created = executor
        .execute(Command::TagCreate {
            branch: TEST_BRANCH.into(),
            name: "release-1".into(),
            version: None,
            message: Some("release candidate".into()),
            creator: Some("tester".into()),
        })
        .unwrap();
    let tagged_version = match created {
        Output::TagCreated(info) => {
            assert_eq!(info.name, "release-1");
            assert_eq!(info.branch, TEST_BRANCH);
            assert_eq!(info.message.as_deref(), Some("release candidate"));
            assert_eq!(info.creator.as_deref(), Some("tester"));
            info.version
        }
        other => panic!("Expected TagCreated, got {other:?}"),
    };

    let resolved = executor
        .execute(Command::TagResolve {
            branch: TEST_BRANCH.into(),
            name: "release-1".into(),
        })
        .unwrap();
    match resolved {
        Output::MaybeTag(Some(info)) => {
            assert_eq!(info.version, tagged_version);
            assert_eq!(info.name, "release-1");
        }
        other => panic!("Expected MaybeTag(Some(_)), got {other:?}"),
    }

    let listed = executor
        .execute(Command::TagList {
            branch: TEST_BRANCH.into(),
        })
        .unwrap();
    match listed {
        Output::TagList(tags) => {
            assert!(tags.iter().any(|tag| tag.name == "release-1"));
        }
        other => panic!("Expected TagList, got {other:?}"),
    }

    assert_eq!(
        executor
            .execute(Command::TagDelete {
                branch: TEST_BRANCH.into(),
                name: "release-1".into(),
            })
            .unwrap(),
        Output::Bool(true)
    );
    assert_eq!(
        executor
            .execute(Command::TagResolve {
                branch: TEST_BRANCH.into(),
                name: "release-1".into(),
            })
            .unwrap(),
        Output::MaybeTag(None)
    );
}

#[test]
fn notes_round_trip_through_executor() {
    let executor = create_executor();
    ensure_branch(&executor);

    let added = executor
        .execute(Command::NoteAdd {
            branch: TEST_BRANCH.into(),
            version: 1,
            message: "ship it".into(),
            author: Some("tester".into()),
            metadata: Some(Value::object(
                [("status".to_string(), Value::String("approved".into()))]
                    .into_iter()
                    .collect(),
            )),
        })
        .unwrap();
    match added {
        Output::NoteAdded(note) => {
            assert_eq!(note.branch, TEST_BRANCH);
            assert_eq!(note.version, 1);
            assert_eq!(note.message, "ship it");
            assert_eq!(note.author.as_deref(), Some("tester"));
            assert!(note.metadata.is_some());
        }
        other => panic!("Expected NoteAdded, got {other:?}"),
    }

    let listed = executor
        .execute(Command::NoteGet {
            branch: TEST_BRANCH.into(),
            version: None,
        })
        .unwrap();
    match listed {
        Output::NoteList(notes) => {
            assert!(notes.iter().any(|note| {
                note.version == 1
                    && note.message == "ship it"
                    && note.author.as_deref() == Some("tester")
            }));
        }
        other => panic!("Expected NoteList, got {other:?}"),
    }

    let exact = executor
        .execute(Command::NoteGet {
            branch: TEST_BRANCH.into(),
            version: Some(1),
        })
        .unwrap();
    match exact {
        Output::NoteList(notes) => {
            assert_eq!(notes.len(), 1);
            assert_eq!(notes[0].version, 1);
        }
        other => panic!("Expected NoteList, got {other:?}"),
    }

    assert_eq!(
        executor
            .execute(Command::NoteDelete {
                branch: TEST_BRANCH.into(),
                version: 1,
            })
            .unwrap(),
        Output::Bool(true)
    );
    assert_eq!(
        executor
            .execute(Command::NoteGet {
                branch: TEST_BRANCH.into(),
                version: Some(1),
            })
            .unwrap(),
        Output::NoteList(Vec::new())
    );
}

#[test]
fn branch_metadata_rejects_system_branch() {
    let executor = create_executor();

    let tag_error = executor
        .execute(Command::TagCreate {
            branch: "_system_".into(),
            name: "release-1".into(),
            version: Some(1),
            message: None,
            creator: None,
        })
        .unwrap_err();
    assert!(matches!(
        tag_error,
        Error::InvalidInput { ref reason, .. } if reason.contains("reserved for system use")
    ));

    let note_error = executor
        .execute(Command::NoteAdd {
            branch: "_system_".into(),
            version: 1,
            message: "hidden".into(),
            author: None,
            metadata: None,
        })
        .unwrap_err();
    assert!(matches!(
        note_error,
        Error::InvalidInput { ref reason, .. } if reason.contains("reserved for system use")
    ));
}

#[test]
fn branch_metadata_rejects_deleted_branch() {
    let executor = create_executor();
    ensure_branch(&executor);

    executor
        .execute(Command::BranchDelete {
            branch: TEST_BRANCH.into(),
        })
        .unwrap();

    let tag_error = executor
        .execute(Command::TagCreate {
            branch: TEST_BRANCH.into(),
            name: "release-1".into(),
            version: Some(1),
            message: None,
            creator: None,
        })
        .unwrap_err();
    assert!(matches!(
        tag_error,
        Error::BranchNotFound { ref branch, .. } if branch == TEST_BRANCH
    ));

    let note_error = executor
        .execute(Command::NoteAdd {
            branch: TEST_BRANCH.into(),
            version: 1,
            message: "ship it".into(),
            author: None,
            metadata: None,
        })
        .unwrap_err();
    assert!(matches!(
        note_error,
        Error::BranchNotFound { ref branch, .. } if branch == TEST_BRANCH
    ));
}

#[test]
fn branch_metadata_commands_bypass_active_transaction() {
    let db = create_db();
    let mut session = strata_executor::Session::new(db.clone());
    session
        .execute(Command::BranchCreate {
            branch_id: Some(TEST_BRANCH.into()),
            metadata: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: Some(TEST_BRANCH.into()),
            space: None,
            key: "committed".into(),
            value: Value::String("visible".into()),
        })
        .unwrap();
    let committed_version = db.current_version().as_u64();

    session
        .execute(Command::TxnBegin {
            branch: Some(TEST_BRANCH.into()),
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: Some(TEST_BRANCH.into()),
            space: None,
            key: "pending".into(),
            value: Value::String("staged".into()),
        })
        .unwrap();

    let tagged = session
        .execute(Command::TagCreate {
            branch: TEST_BRANCH.into(),
            name: "txn-bypass".into(),
            version: None,
            message: None,
            creator: None,
        })
        .unwrap();
    match tagged {
        Output::TagCreated(info) => {
            assert_eq!(info.version, committed_version);
        }
        other => panic!("Expected TagCreated, got {other:?}"),
    }

    session
        .execute(Command::NoteAdd {
            branch: TEST_BRANCH.into(),
            version: committed_version,
            message: "metadata survives rollback".into(),
            author: None,
            metadata: None,
        })
        .unwrap();

    session.execute(Command::TxnRollback).unwrap();

    match session
        .execute(Command::TagResolve {
            branch: TEST_BRANCH.into(),
            name: "txn-bypass".into(),
        })
        .unwrap()
    {
        Output::MaybeTag(Some(info)) => assert_eq!(info.version, committed_version),
        other => panic!("Expected MaybeTag(Some(_)), got {other:?}"),
    }

    match session
        .execute(Command::NoteGet {
            branch: TEST_BRANCH.into(),
            version: Some(committed_version),
        })
        .unwrap()
    {
        Output::NoteList(notes) => {
            assert_eq!(notes.len(), 1);
            assert_eq!(notes[0].message, "metadata survives rollback");
        }
        other => panic!("Expected NoteList, got {other:?}"),
    }
}

#[test]
fn branch_metadata_can_target_other_branch_during_active_transaction() {
    let db = create_db();
    let mut session = strata_executor::Session::new(db.clone());
    session
        .execute(Command::BranchCreate {
            branch_id: Some(TEST_BRANCH.into()),
            metadata: None,
        })
        .unwrap();
    session
        .execute(Command::BranchCreate {
            branch_id: Some("metadata-secondary".into()),
            metadata: None,
        })
        .unwrap();

    session
        .execute(Command::KvPut {
            branch: Some(BranchId::from("metadata-secondary")),
            space: None,
            key: "committed".into(),
            value: Value::String("visible".into()),
        })
        .unwrap();
    let committed_version = db.current_version().as_u64();

    session
        .execute(Command::TxnBegin {
            branch: Some(TEST_BRANCH.into()),
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: Some(TEST_BRANCH.into()),
            space: None,
            key: "pending".into(),
            value: Value::String("staged".into()),
        })
        .unwrap();

    let tagged = session
        .execute(Command::TagCreate {
            branch: "metadata-secondary".into(),
            name: "cross-branch".into(),
            version: None,
            message: None,
            creator: None,
        })
        .unwrap();
    match tagged {
        Output::TagCreated(info) => {
            assert_eq!(info.branch, "metadata-secondary");
            assert_eq!(info.version, committed_version);
        }
        other => panic!("Expected TagCreated, got {other:?}"),
    }

    session
        .execute(Command::NoteAdd {
            branch: "metadata-secondary".into(),
            version: committed_version,
            message: "survives rollback".into(),
            author: None,
            metadata: None,
        })
        .unwrap();

    session.execute(Command::TxnRollback).unwrap();

    match session
        .execute(Command::TagResolve {
            branch: "metadata-secondary".into(),
            name: "cross-branch".into(),
        })
        .unwrap()
    {
        Output::MaybeTag(Some(info)) => assert_eq!(info.version, committed_version),
        other => panic!("Expected MaybeTag(Some(_)), got {other:?}"),
    }

    match session
        .execute(Command::NoteGet {
            branch: "metadata-secondary".into(),
            version: Some(committed_version),
        })
        .unwrap()
    {
        Output::NoteList(notes) => {
            assert_eq!(notes.len(), 1);
            assert_eq!(notes[0].message, "survives rollback");
        }
        other => panic!("Expected NoteList, got {other:?}"),
    }
}
