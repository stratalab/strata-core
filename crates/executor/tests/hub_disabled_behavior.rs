//! `HubClone` behavior when the hub feature is not compiled (e.g. the
//! wasm build, which cannot carry the native HTTP client stack).

#![cfg(not(feature = "hub"))]

use strata_executor::{Command, Executor, ExecutorError, ExecutorErrorClass, Output};

fn assert_hub_feature_disabled(error: &ExecutorError) {
    // A build without the hub feature is `unsupported` (#2750); the compat
    // class for `unsupported` is `Unavailable`.
    assert_eq!(error.class(), ExecutorErrorClass::Unavailable);
    assert_eq!(error.code(), "unsupported.executor.hub_feature_disabled");
}

#[test]
fn hub_clone_returns_a_stable_feature_disabled_error_without_feature() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::HubClone {
            dataset: "titanic".to_owned(),
            branch: None,
            dest: "unused".to_owned(),
            hub_url: None,
        })
        .expect_err("feature disabled clone fails");
    assert_hub_feature_disabled(&error);
}

#[test]
fn hub_clone_progress_returns_a_stable_feature_disabled_error_without_feature() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let mut progress_events = Vec::<Output>::new();
    let error = executor
        .execute_hub_clone_with_progress("titanic", None, "unused", None, &mut |output| {
            progress_events.push(output);
        })
        .expect_err("feature disabled clone progress fails");
    assert_hub_feature_disabled(&error);
    assert!(
        progress_events.is_empty(),
        "disabled hub clone must not emit progress events"
    );
}

#[test]
fn hub_browse_commands_return_stable_feature_disabled_errors_without_feature() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let commands = [
        Command::HubInfo { hub_url: None },
        Command::HubListDatasets {
            hub_url: None,
            tasks: Vec::new(),
            tags: Vec::new(),
            primitives: Vec::new(),
            license: None,
            size_min_bytes: None,
            size_max_bytes: None,
            sort: None,
            limit: None,
            offset: None,
        },
        Command::HubGetDataset {
            name: "titanic".to_owned(),
            hub_url: None,
        },
        Command::HubListRefs {
            dataset: "titanic".to_owned(),
            hub_url: None,
        },
        Command::HubListYanked {
            since: None,
            hub_url: None,
        },
    ];

    for command in commands {
        let error = executor
            .execute(command)
            .expect_err("feature disabled hub browse command fails");
        assert_hub_feature_disabled(&error);
    }
}
