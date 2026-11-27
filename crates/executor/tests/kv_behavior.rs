//! Executor KV behavior tests.

use strata_engine::{CacheOpenOptions, Database};
use strata_executor::{
    BatchItemStatus, BatchKvEntry, BatchStatus, Bytes, Command, Executor, ExecutorErrorClass,
    MutationEffectKind, Output, VersionedValue, DEFAULT_BRANCH,
};
use tempfile::TempDir;

#[test]
fn cache_executor_runs_complete_kv_command_suite() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    run_kv_command_suite(&mut executor);
}

#[test]
fn kv_write_outputs_report_commit_receipts_and_effects() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let created = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes("effect-key"),
            value: bytes("one"),
        })
        .expect("create succeeds");
    let Output::WriteResult { effect, commit, .. } = created else {
        panic!("unexpected create output: {created:?}");
    };
    assert_eq!(effect.kind(), MutationEffectKind::Created);
    assert!(effect.applied());
    assert!(!effect.matched());
    assert_eq!(effect.affected_count(), 1);
    assert_eq!(commit.put_count(), 1);
    assert_eq!(commit.delete_count(), 0);

    let updated = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes("effect-key"),
            value: bytes("two"),
        })
        .expect("update succeeds");
    let Output::WriteResult { effect, commit, .. } = updated else {
        panic!("unexpected update output: {updated:?}");
    };
    assert_eq!(effect.kind(), MutationEffectKind::Updated);
    assert!(effect.applied());
    assert!(effect.matched());
    assert_eq!(commit.put_count(), 1);
    assert_eq!(commit.delete_count(), 0);

    let deleted = executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: bytes("effect-key"),
        })
        .expect("delete succeeds");
    let Output::DeleteResult { effect, commit, .. } = deleted else {
        panic!("unexpected delete output: {deleted:?}");
    };
    assert_eq!(effect.kind(), MutationEffectKind::Deleted);
    assert!(effect.applied());
    assert!(effect.matched());
    assert!(commit.is_some());

    let missing = executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: bytes("effect-key"),
        })
        .expect("missing delete succeeds");
    let Output::DeleteResult { effect, commit, .. } = missing else {
        panic!("unexpected missing delete output: {missing:?}");
    };
    assert_eq!(effect.kind(), MutationEffectKind::NotFound);
    assert!(!effect.applied());
    assert!(!effect.matched());
    assert_eq!(effect.affected_count(), 0);
    assert!(commit.is_none());
}

#[test]
fn write_ack_carries_a_wall_clock_committed_at_distinct_from_the_logical_clock() {
    // #3112: a write ack's `committed_at` is a real wall-clock instant (UTC
    // epoch micros), NOT the logical commit clock. On a fresh database the
    // logical `timestamp` is a tiny per-commit counter, so a client must read
    // `committed_at` to render a real date (a client that formatted `timestamp`
    // as Unix micros showed 1969). Assert the field is populated with an actual
    // recent time, and that the logical clock is not that.
    // 2020-01-01T00:00:00Z in epoch micros: any real clock is far above it, and
    // the logical commit counter (seeded near 1) is far below it.
    const YEAR_2020_MICROS: u64 = 1_577_836_800_000_000;
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let output = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes("when-key"),
            value: bytes("v"),
        })
        .expect("put succeeds");
    let Output::WriteResult { commit, .. } = output else {
        panic!("unexpected output: {output:?}");
    };
    let committed_at = commit
        .committed_at()
        .expect("write ack carries a committed_at");
    assert!(
        committed_at > YEAR_2020_MICROS,
        "committed_at {committed_at} is not a wall-clock instant"
    );
    assert!(
        commit.timestamp() < YEAR_2020_MICROS,
        "logical timestamp {} should be a small counter, not wall-clock",
        commit.timestamp()
    );
}

/// #3112 S3a: the defining contract of wall-clock time travel — `as_of_time`
/// is EXACTLY an `as_of` at the instant's resolved commit. Reading at a
/// commit's own `committed_at` must return precisely what reading at that
/// commit's logical `timestamp` returns, for every commit in the history.
///
/// This is the property that keeps the two clocks from drifting: wall-clock
/// as-of inherits at-or-before, MVCC visibility, and tombstone handling from
/// the logical path rather than reimplementing them.
#[test]
fn as_of_time_at_each_commit_matches_as_of_at_that_commits_logical_timestamp() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    // A history with an overwrite and a delete, so visibility is genuinely
    // exercised rather than every read returning the same row.
    let mut stamps = Vec::new();
    for value in ["one", "two"] {
        stamps.push(spaced(|e| put_stamp(e, "drift-key", value), &mut executor));
    }
    stamps.push(spaced(|e| delete_stamp(e, "drift-key"), &mut executor));
    stamps.push(spaced(
        |e| put_stamp(e, "drift-key", "three"),
        &mut executor,
    ));
    assert_distinct_instants(&stamps);

    for (index, (timestamp, committed_at)) in stamps.iter().copied().enumerate() {
        let by_logical = kv_get_at(&mut executor, "drift-key", Some(timestamp), None)
            .expect("logical as_of read succeeds");
        let by_wall_clock = kv_get_at(&mut executor, "drift-key", None, Some(committed_at))
            .expect("wall-clock as_of read succeeds");
        assert_eq!(
            by_logical, by_wall_clock,
            "commit {index}: as_of_time({committed_at}) diverged from as_of({timestamp})"
        );
    }

    // And the reads are actually distinguishing commits, not all returning the
    // same thing — otherwise the equality above would be vacuous.
    let first =
        kv_get_at(&mut executor, "drift-key", None, Some(stamps[0].1)).expect("first commit reads");
    let last =
        kv_get_at(&mut executor, "drift-key", None, Some(stamps[3].1)).expect("last commit reads");
    assert_ne!(
        first, last,
        "the history must be observably different across commits"
    );
}

/// A target between two commits resolves to the earlier one — the at-or-before
/// rule, in wall-clock terms.
#[test]
fn as_of_time_between_commits_resolves_to_the_earlier_commit() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let first = spaced(|e| put_stamp(e, "between-key", "one"), &mut executor);
    let second = spaced(|e| put_stamp(e, "between-key", "two"), &mut executor);
    assert_distinct_instants(&[first, second]);
    let (_, first_instant) = first;
    let (second_timestamp, second_instant) = second;

    let between = first_instant + (second_instant - first_instant) / 2;
    assert!(between > first_instant && between < second_instant);
    assert_eq!(
        kv_get_at(&mut executor, "between-key", None, Some(between)).expect("resolves"),
        kv_get_at(&mut executor, "between-key", None, Some(first_instant)).expect("resolves"),
        "a target between commits must resolve to the earlier boundary"
    );
    assert_ne!(
        kv_get_at(&mut executor, "between-key", None, Some(between)).expect("resolves"),
        kv_get_at(&mut executor, "between-key", Some(second_timestamp), None).expect("resolves"),
        "and must NOT reach the later commit"
    );
}

/// D3: past the tip raises rather than clamping to current state, matching the
/// locked temporal contract's logical form.
#[test]
fn as_of_time_past_the_latest_commit_raises_instead_of_clamping() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (_, instant) = put_stamp(&mut executor, "tip-key", "one");

    let error = kv_get_at(&mut executor, "tip-key", None, Some(instant + 60_000_000))
        .expect_err("a target past the tip must raise");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    assert_eq!(
        error.code(),
        "history_unavailable.engine.after_latest_commit"
    );
}

/// F3: "before the database existed" is a real answer, and distinct from a
/// successful read of empty history. Nothing was pruned, so it names the first
/// commit rather than a retention window.
#[test]
fn as_of_time_before_the_first_commit_raises() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (_, instant) = put_stamp(&mut executor, "floor-key", "one");

    let error = kv_get_at(&mut executor, "floor-key", None, Some(instant - 60_000_000))
        .expect_err("a target before the first dated commit must raise");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    assert_eq!(
        error.code(),
        "history_unavailable.engine.before_first_commit"
    );
}

/// A designed safety property: instants are epoch MICROS, and every other
/// plausible unit for the same moment lands outside the dated range in one
/// direction or the other — and the refusal names which. No unit confusion can
/// quietly resolve to a wrong commit — a class of bug that would otherwise be
/// invisible.
#[test]
fn as_of_time_in_the_wrong_time_unit_always_raises() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (_, micros) = put_stamp(&mut executor, "unit-key", "one");

    for (unit, instant, code) in [
        (
            "seconds",
            micros / 1_000_000,
            "history_unavailable.engine.before_first_commit",
        ),
        (
            "millis",
            micros / 1_000,
            "history_unavailable.engine.before_first_commit",
        ),
        (
            "nanos",
            micros.saturating_mul(1_000),
            "history_unavailable.engine.after_latest_commit",
        ),
    ] {
        let Err(error) = kv_get_at(&mut executor, "unit-key", None, Some(instant)) else {
            panic!("{unit} must not silently resolve to a commit");
        };
        assert_eq!(
            error.code(),
            code,
            "{unit} landed outside the dated range, as intended"
        );
    }
}

/// Both clocks at once is ambiguous, so it is refused rather than given a
/// silent precedence rule.
#[test]
fn as_of_and_as_of_time_together_are_rejected() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (timestamp, committed_at) = put_stamp(&mut executor, "both-key", "one");

    let error = kv_get_at(
        &mut executor,
        "both-key",
        Some(timestamp),
        Some(committed_at),
    )
    .expect_err("supplying both clocks must be refused");
    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(error.code(), "invalid_argument.executor.as_of_conflict");

    // The refusal is about the pair, not about either field: each alone works.
    kv_get_at(&mut executor, "both-key", Some(timestamp), None).expect("as_of alone works");
    kv_get_at(&mut executor, "both-key", None, Some(committed_at)).expect("as_of_time alone works");
}

#[test]
fn kv_batch_write_outputs_report_per_item_commit_receipts_and_effects() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    write(&mut executor, None, None, "batch-effect-existing", "old");

    let output = executor
        .execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries: vec![
                BatchKvEntry::new(bytes("batch-effect-created"), bytes("new")),
                BatchKvEntry::new(bytes("batch-effect-existing"), bytes("updated")),
            ],
        })
        .expect("batch put succeeds");
    let Output::BatchResults(results) = output else {
        panic!("unexpected batch put output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    let created = results[0].effect().expect("created effect");
    assert_eq!(created.kind(), MutationEffectKind::Created);
    assert!(created.applied());
    assert!(!created.matched());
    let updated = results[1].effect().expect("updated effect");
    assert_eq!(updated.kind(), MutationEffectKind::Updated);
    assert!(updated.applied());
    assert!(updated.matched());
    let batch_commit = results[0].commit().expect("created commit");
    assert_eq!(batch_commit.put_count(), 2);
    assert_eq!(batch_commit.delete_count(), 0);
    assert_eq!(
        results[1].commit().expect("updated commit").version(),
        batch_commit.version()
    );

    let output = executor
        .execute(Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: vec![bytes("batch-effect-created"), bytes("batch-effect-missing")],
        })
        .expect("batch delete succeeds");
    let Output::BatchResults(results) = output else {
        panic!("unexpected batch delete output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    let deleted = results[0].effect().expect("deleted effect");
    assert_eq!(deleted.kind(), MutationEffectKind::Deleted);
    assert!(deleted.applied());
    assert!(deleted.matched());
    let missing = results[1].effect().expect("missing effect");
    assert_eq!(missing.kind(), MutationEffectKind::NotFound);
    assert!(!missing.applied());
    assert!(!missing.matched());
    assert!(results[0].commit().is_some());
    assert!(results[1].commit().is_none());
}

#[test]
fn kv_batch_exists_reports_invalid_keys_as_positional_errors() {
    // A malformed key is a positional item error, not a whole-batch abort —
    // matching kv_batch_get. Valid answers (present/absent) are ok items.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    write(&mut executor, None, None, "present", "v");

    let Output::BatchExistsResults(results) = executor
        .execute(Command::KvBatchExists {
            branch: None,
            space: None,
            keys: vec![bytes("present"), bytes("absent"), Bytes::new(Vec::new())],
        })
        .expect("batch exists does not abort on a bad key")
    else {
        panic!("unexpected batch exists output");
    };
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].status(), BatchItemStatus::Ok);
    assert!(results[0].exists());
    assert_eq!(results[1].status(), BatchItemStatus::Ok);
    assert!(!results[1].exists());
    assert_eq!(
        results[2].status(),
        BatchItemStatus::Error,
        "an empty key is a positional error, not a whole-batch abort"
    );
    assert_eq!(
        results[2].error_status().expect("item error").code(),
        "invalid_argument.engine.kv_key"
    );
    assert_eq!(results.status(), BatchStatus::Partial);
}

#[test]
fn durable_executor_reopens_values_lists_and_history() {
    let temp = TempDir::new().expect("temp dir");
    let path = temp.path().join("db");

    {
        let mut executor = Executor::open_durable_local(&path).expect("durable executor opens");
        run_kv_command_suite(&mut executor);
        executor.close().expect("durable executor closes");
    }

    let mut reopened = Executor::open_durable_local(&path).expect("durable executor reopens");
    assert_eq!(
        execute_get(&mut reopened, "alpha"),
        Some(bytes("one-updated"))
    );
    assert_eq!(execute_count(&mut reopened, None), 6);
    assert_history_has_tombstone(&mut reopened, "delete-me");
}

#[test]
fn branch_and_space_defaults_are_isolated() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .branch_fork_current(DEFAULT_BRANCH, "feature")
        .expect("branch creates");

    write(&mut executor, None, None, "shared", "default-branch");
    write(
        &mut executor,
        Some("feature"),
        None,
        "shared",
        "feature-branch",
    );
    write(&mut executor, None, Some("tenant-a"), "shared", "space-a");

    assert_eq!(
        execute_get(&mut executor, "shared"),
        Some(bytes("default-branch"))
    );
    assert_eq!(
        execute_get_in(&mut executor, Some("feature"), None, "shared"),
        Some(bytes("feature-branch"))
    );
    assert_eq!(
        execute_get_in(&mut executor, None, Some("tenant-a"), "shared"),
        Some(bytes("space-a"))
    );
}

#[test]
fn executor_inherits_configured_database_default_branch() {
    let options = CacheOpenOptions::new()
        .with_default_branch("main")
        .expect("valid branch");
    let database = Database::open_cache(options)
        .expect("cache database opens")
        .into_database();
    let mut executor = Executor::from_database(database);

    assert_eq!(executor.default_branch(), "main");
    write(&mut executor, None, None, "shared", "main-value");
    assert_eq!(
        execute_get(&mut executor, "shared"),
        Some(bytes("main-value"))
    );

    let error = executor
        .execute(Command::KvGet {
            branch: Some(DEFAULT_BRANCH.to_owned()),
            space: None,
            key: bytes("shared"),
            as_of: None,
            as_of_time: None,
        })
        .expect_err("literal default branch is absent");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
}

#[test]
fn branch_commands_delegate_to_engine_branch_service() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    write(&mut executor, None, None, "shared", "base");

    let created = executor
        .execute(Command::BranchCreate {
            branch: "scratch".to_owned(),
        })
        .expect("branch create succeeds");
    let Output::Branch(created) = created else {
        panic!("branch create output");
    };
    assert_eq!(created.name(), "scratch");
    assert!(created.parent().is_none());

    let forked = executor
        .execute(Command::BranchForkCurrent {
            source: DEFAULT_BRANCH.to_owned(),
            branch: "feature".to_owned(),
        })
        .expect("branch fork succeeds");
    let Output::Branch(forked) = forked else {
        panic!("branch fork output");
    };
    assert_eq!(forked.name(), "feature");
    assert_eq!(
        forked.parent().expect("parent facts").name(),
        DEFAULT_BRANCH
    );
    assert_eq!(
        execute_get_in(&mut executor, Some("feature"), None, "shared"),
        Some(bytes("base"))
    );

    let listed = executor
        .execute(Command::BranchList {})
        .expect("branch list succeeds");
    let Output::Branches {
        items: branches, ..
    } = listed
    else {
        panic!("branch list output");
    };
    assert!(branches
        .iter()
        .any(|branch| branch.name() == DEFAULT_BRANCH));
    assert!(branches.iter().any(|branch| branch.name() == "scratch"));
    assert!(branches.iter().any(|branch| branch.name() == "feature"));

    let deleted = executor
        .execute(Command::BranchDelete {
            branch: "scratch".to_owned(),
        })
        .expect("branch delete succeeds");
    let Output::BranchDeleteResult {
        deleted,
        effect,
        branch,
        ..
    } = deleted
    else {
        panic!("branch delete output");
    };
    assert!(deleted);
    assert_eq!(effect.kind(), MutationEffectKind::Deleted);
    assert!(effect.applied());
    assert!(effect.matched());
    assert_eq!(branch.name(), "scratch");

    let error = executor
        .execute(Command::KvPut {
            branch: Some("scratch".to_owned()),
            space: None,
            key: bytes("blocked"),
            value: bytes("blocked"),
        })
        .expect_err("deleted branch write fails");
    assert_eq!(error.class(), ExecutorErrorClass::NotFound);
}

#[test]
fn command_to_output_mapping_is_explicit_for_every_variant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    write(&mut executor, None, None, "map-a", "one");
    write(&mut executor, None, None, "map-b", "two");

    let commands = vec![
        Command::KvPut {
            branch: None,
            space: None,
            key: bytes("map-put"),
            value: bytes("value"),
        },
        Command::KvGet {
            branch: None,
            space: None,
            key: bytes("map-a"),
            as_of: None,
            as_of_time: None,
        },
        Command::KvDelete {
            branch: None,
            space: None,
            key: bytes("map-delete-missing"),
        },
        Command::KvList {
            branch: None,
            space: None,
            prefix: Some(bytes("map-")),
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        },
        Command::KvScan {
            branch: None,
            space: None,
            start: Some(bytes("map-")),
            limit: Some(2),
        },
        Command::KvBatchPut {
            branch: None,
            space: None,
            entries: vec![BatchKvEntry::new(bytes("map-c"), bytes("three"))],
        },
        Command::KvBatchGet {
            branch: None,
            space: None,
            keys: vec![bytes("map-a"), bytes("missing")],
            as_of: None,
            as_of_time: None,
        },
        Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: vec![bytes("map-c"), bytes("missing")],
        },
        Command::KvBatchExists {
            branch: None,
            space: None,
            keys: vec![bytes("map-a"), bytes("missing")],
        },
        Command::KvExists {
            branch: None,
            space: None,
            key: bytes("map-a"),
        },
        Command::KvHistory {
            branch: None,
            space: None,
            key: bytes("map-a"),
        },
        Command::KvCount {
            branch: None,
            space: None,
            prefix: Some(bytes("map-")),
            as_of: None,
            as_of_time: None,
        },
        Command::KvSample {
            branch: None,
            space: None,
            prefix: Some(bytes("map-")),
            count: Some(1),
        },
    ];

    let outputs = commands
        .into_iter()
        .map(|command| executor.execute(command).expect("command succeeds"))
        .collect::<Vec<_>>();

    assert!(matches!(outputs[0], Output::WriteResult { .. }));
    assert!(matches!(outputs[1], Output::KvVersionedValue(_)));
    assert!(matches!(outputs[2], Output::DeleteResult { .. }));
    assert!(matches!(outputs[3], Output::KeysPage { .. }));
    assert!(matches!(outputs[4], Output::KvScanResult { .. }));
    assert!(matches!(outputs[5], Output::BatchResults(_)));
    assert!(matches!(outputs[6], Output::BatchGetResults(_)));
    assert!(matches!(outputs[7], Output::BatchResults(_)));
    assert!(matches!(outputs[8], Output::BatchExistsResults(_)));
    assert!(matches!(outputs[9], Output::Bool(_)));
    assert!(matches!(outputs[10], Output::VersionHistory(_)));
    assert!(matches!(outputs[11], Output::Uint(_)));
    assert!(matches!(outputs[12], Output::SampleResult { .. }));
}

#[test]
fn duplicate_batch_writes_fail_before_partial_application() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let error = executor
        .execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries: vec![
                BatchKvEntry::new(bytes("dupe"), bytes("one")),
                BatchKvEntry::new(bytes("dupe"), bytes("two")),
            ],
        })
        .expect_err("duplicate batch put fails");

    assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
    assert!(execute_get(&mut executor, "dupe").is_none());
}

#[test]
fn empty_batches_return_empty_outputs() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    assert!(matches!(
        executor
            .execute(Command::KvBatchPut {
                branch: None,
                space: None,
                entries: Vec::new(),
            })
            .expect("empty batch put succeeds"),
        Output::BatchResults(results) if results.is_empty() && !results.applied()
    ));
    assert!(matches!(
        executor
            .execute(Command::KvBatchDelete {
                branch: None,
                space: None,
                keys: Vec::new(),
            })
            .expect("empty batch delete succeeds"),
        Output::BatchResults(results) if results.is_empty() && !results.applied()
    ));
    assert!(matches!(
        executor
            .execute(Command::KvBatchGet {
                branch: None,
                space: None,
                keys: Vec::new(),
                as_of: None,
                as_of_time: None,
            })
            .expect("empty batch get succeeds"),
        Output::BatchGetResults(results) if results.is_empty() && !results.applied()
    ));
}

#[test]
fn invalid_batch_items_are_positional_errors() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let output = executor
        .execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries: vec![
                BatchKvEntry::new(Bytes::new(Vec::new()), bytes("bad")),
                BatchKvEntry::new(bytes("valid"), bytes("good")),
            ],
        })
        .expect("batch put returns positional errors");
    let Output::BatchResults(results) = output else {
        panic!("unexpected batch put output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(!results[0].applied());
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.kv_key"
    );
    assert!(results[1].applied());
    assert_eq!(execute_get(&mut executor, "valid"), Some(bytes("good")));

    let output = executor
        .execute(Command::KvBatchGet {
            branch: None,
            space: None,
            keys: vec![Bytes::new(Vec::new()), bytes("valid")],
            as_of: None,
            as_of_time: None,
        })
        .expect("batch get returns positional errors");
    let Output::BatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.kv_key"
    );
    assert_eq!(results[1].value(), Some(&bytes("good")));

    let output = executor
        .execute(Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: vec![Bytes::new(Vec::new()), bytes("valid")],
        })
        .expect("batch delete returns positional errors");
    let Output::BatchResults(results) = output else {
        panic!("unexpected batch delete output: {output:?}");
    };
    assert_eq!(results.len(), 2);
    assert!(results[0].error().is_some());
    assert_eq!(
        results[0].error_status().expect("item error status").code(),
        "invalid_argument.engine.kv_key"
    );
    assert!(results[1].applied());
    assert!(execute_get(&mut executor, "valid").is_none());
}

#[test]
fn batch_commands_validate_branch_before_returning_item_results() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    for command in [
        Command::KvBatchPut {
            branch: Some("missing".to_owned()),
            space: None,
            entries: Vec::new(),
        },
        Command::KvBatchPut {
            branch: Some("missing".to_owned()),
            space: None,
            entries: vec![BatchKvEntry::new(Bytes::new(Vec::new()), bytes("bad"))],
        },
        Command::KvBatchGet {
            branch: Some("missing".to_owned()),
            space: None,
            keys: Vec::new(),
            as_of: None,
            as_of_time: None,
        },
        Command::KvBatchDelete {
            branch: Some("missing".to_owned()),
            space: None,
            keys: Vec::new(),
        },
        Command::KvBatchExists {
            branch: Some("missing".to_owned()),
            space: None,
            keys: Vec::new(),
        },
    ] {
        let error = executor.execute(command).expect_err("missing branch fails");
        assert_eq!(error.class(), ExecutorErrorClass::NotFound);
    }
}

fn run_kv_command_suite(executor: &mut Executor) {
    let first = write(executor, None, None, "alpha", "one");
    let second = write(executor, None, None, "alpha", "one-updated");
    assert!(second.version > first.version);

    write(executor, None, None, "bravo", "two");
    write(executor, None, None, "prefix-a", "three");
    write(executor, None, None, "prefix-b", "four");
    write(executor, None, None, "delete-me", "gone");

    assert_eq!(execute_get(executor, "alpha"), Some(bytes("one-updated")));
    let first_as_of =
        execute_get_as_of(executor, "alpha", first.timestamp).expect("historical value exists");
    assert_eq!(first_as_of.value(), &bytes("one"));
    assert_eq!(first_as_of.version(), first.version);
    assert_eq!(first_as_of.timestamp(), first.timestamp);
    assert!(execute_exists(executor, "alpha"));
    assert!(!execute_exists(executor, "missing"));

    assert_eq!(
        execute_list(executor, Some("prefix-")),
        vec![bytes("prefix-a"), bytes("prefix-b")]
    );
    assert_eq!(
        execute_list_page(executor, Some("prefix-"), None, 1),
        (vec![bytes("prefix-a")], true)
    );
    assert_eq!(
        execute_list_as_of(executor, None, first.timestamp),
        vec![bytes("alpha")]
    );

    assert_eq!(
        execute_scan(executor, Some("prefix-"), Some(10)),
        vec![
            (bytes("prefix-a"), bytes("three")),
            (bytes("prefix-b"), bytes("four"))
        ]
    );

    batch_put(
        executor,
        vec![
            ("batch-a", "five"),
            ("batch-b", "six"),
            ("sample-a", "seven"),
        ],
    );
    assert_eq!(
        execute_batch_get(executor, vec!["batch-a", "missing", "batch-b"]),
        vec![Some(bytes("five")), None, Some(bytes("six"))]
    );
    assert_eq!(
        execute_batch_exists(executor, vec!["batch-a", "missing", "batch-b"]),
        vec![true, false, true]
    );

    let deleted = execute_delete(executor, "delete-me");
    assert!(deleted);
    let missing_deleted = execute_delete(executor, "delete-me");
    assert!(!missing_deleted);
    assert_history_has_tombstone(executor, "delete-me");

    assert_eq!(execute_count(executor, Some("batch-")), 2);
    let sample = execute_sample(executor, Some("batch-"), 1);
    assert_eq!(sample.0, 2);
    assert_eq!(sample.1.len(), 1);
    assert!(sample.1[0].as_slice().starts_with(b"batch-"));

    let results = execute_batch_delete(executor, vec!["batch-a", "missing"]);
    assert_eq!(results, vec![true, false]);
    assert!(!execute_exists(executor, "batch-a"));
}

fn write(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: &str,
    value: &str,
) -> WriteFacts {
    match executor
        .execute(Command::KvPut {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key: bytes(key),
            value: bytes(value),
        })
        .expect("put succeeds")
    {
        Output::WriteResult { effect, commit, .. } => {
            assert!(effect.applied());
            WriteFacts {
                version: commit.version(),
                timestamp: commit.timestamp(),
            }
        }
        output => panic!("unexpected put output: {output:?}"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WriteFacts {
    version: u64,
    timestamp: u64,
}

fn batch_put(executor: &mut Executor, entries: Vec<(&str, &str)>) {
    let entries = entries
        .into_iter()
        .map(|(key, value)| BatchKvEntry::new(bytes(key), bytes(value)))
        .collect();
    match executor
        .execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries,
        })
        .expect("batch put succeeds")
    {
        Output::BatchResults(results) => {
            assert!(results.iter().all(strata_executor::BatchItem::applied));
        }
        output => panic!("unexpected batch put output: {output:?}"),
    }
}

#[test]
fn commands_resolve_omitted_space_from_the_executor_session_default() {
    // CLI-4: the executor owns space session context. A command that omits its
    // space resolves to the session default — the mechanism `command run`
    // relies on so raw JSON honors --space — not the literal "default" space.
    let mut executor = Executor::open_cache()
        .expect("cache executor opens")
        .with_default_space("app")
        .expect("session space set");
    assert_eq!(executor.default_space(), "app");

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes("k"),
            value: bytes("v"),
        })
        .expect("put succeeds");

    // The write landed in the session space, not the literal "default" space.
    assert_eq!(
        execute_get_in(&mut executor, None, Some("app"), "k"),
        Some(bytes("v"))
    );
    assert_eq!(
        execute_get_in(&mut executor, None, Some("default"), "k"),
        None
    );
    // A read that also omits the space resolves to "app" and finds it.
    assert_eq!(execute_get(&mut executor, "k"), Some(bytes("v")));
}

fn execute_get(executor: &mut Executor, key: &str) -> Option<Bytes> {
    match executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: bytes(key),
            as_of: None,
            as_of_time: None,
        })
        .expect("get succeeds")
    {
        Output::KvVersionedValue(value) => value.into_option().map(|v| v.value().clone()),
        output => panic!("unexpected get output: {output:?}"),
    }
}

fn execute_get_in(
    executor: &mut Executor,
    branch: Option<&str>,
    space: Option<&str>,
    key: &str,
) -> Option<Bytes> {
    match executor
        .execute(Command::KvGet {
            branch: branch.map(str::to_owned),
            space: space.map(str::to_owned),
            key: bytes(key),
            as_of: None,
            as_of_time: None,
        })
        .expect("get succeeds")
    {
        Output::KvVersionedValue(value) => value.into_option().map(|v| v.value().clone()),
        output => panic!("unexpected get output: {output:?}"),
    }
}

fn execute_get_as_of(executor: &mut Executor, key: &str, as_of: u64) -> Option<VersionedValue> {
    match executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: bytes(key),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("historical get succeeds")
    {
        Output::KvVersionedValue(value) => value.into_option(),
        output => panic!("unexpected historical get output: {output:?}"),
    }
}

fn execute_delete(executor: &mut Executor, key: &str) -> bool {
    match executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: bytes(key),
        })
        .expect("delete succeeds")
    {
        Output::DeleteResult { effect, .. } => effect.applied(),
        output => panic!("unexpected delete output: {output:?}"),
    }
}

fn execute_list(executor: &mut Executor, prefix: Option<&str>) -> Vec<Bytes> {
    match executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        })
        .expect("list succeeds")
    {
        Output::KeysPage { items: keys, .. } => keys,
        output => panic!("unexpected list output: {output:?}"),
    }
}

fn execute_list_page(
    executor: &mut Executor,
    prefix: Option<&str>,
    cursor: Option<&str>,
    limit: u64,
) -> (Vec<Bytes>, bool) {
    match executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            cursor: cursor.map(bytes),
            limit: Some(limit),
            as_of: None,
            as_of_time: None,
        })
        .expect("list page succeeds")
    {
        Output::KeysPage { items: keys, page } => (keys, page.has_more()),
        output => panic!("unexpected list page output: {output:?}"),
    }
}

fn execute_list_as_of(executor: &mut Executor, prefix: Option<&str>, as_of: u64) -> Vec<Bytes> {
    match executor
        .execute(Command::KvList {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            cursor: None,
            limit: None,
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("historical list succeeds")
    {
        Output::KeysPage { items: keys, .. } => keys,
        output => panic!("unexpected historical list output: {output:?}"),
    }
}

#[test]
fn kv_scan_paginates_honestly_with_a_cursor() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for i in 0..6 {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: bytes(&format!("k{i}")),
                value: bytes("v"),
            })
            .expect("put succeeds");
    }

    // DSGN-2: page through in chunks of 2. The union of pages must be all six
    // keys with no overlap and no gap, and only the final page ends (cursor
    // None). Previously every scan lied with a terminal page (cursor None),
    // truncating callers to the first page.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut start: Option<Bytes> = None;
    let mut pages = 0;
    loop {
        let (keys, cursor) = scan_page(&mut executor, start, 2);
        assert!(keys.len() <= 2);
        seen.extend(keys.iter().map(|key| key.as_slice().to_vec()));
        pages += 1;
        assert!(pages <= 6, "pagination did not terminate");
        match cursor {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    seen.sort();
    let mut deduped = seen.clone();
    deduped.dedup();
    assert_eq!(deduped.len(), seen.len(), "no duplicate keys across pages");
    let mut expected: Vec<Vec<u8>> = (0..6).map(|i| format!("k{i}").into_bytes()).collect();
    expected.sort();
    assert_eq!(seen, expected);
}

#[test]
fn kv_list_as_of_paginates_in_the_engine() {
    // Time-travel listing paginates via the engine (list_at_page), so a
    // historical page reflects only keys visible at that timestamp and never
    // materializes the whole keyset in the executor.
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let mut historical = 0;
    for i in 0..4 {
        historical = write(&mut executor, None, None, &format!("h{i}"), "v").timestamp;
    }
    // Keys added after the snapshot must not appear in the historical pages.
    write(&mut executor, None, None, "h4", "v");
    write(&mut executor, None, None, "h5", "v");

    let mut cursor: Option<Bytes> = None;
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut pages = 0;
    loop {
        let Output::KeysPage { items, page } = executor
            .execute(Command::KvList {
                branch: None,
                space: None,
                prefix: None,
                cursor: cursor.clone(),
                limit: Some(2),
                as_of: Some(historical),
                as_of_time: None,
            })
            .expect("historical page succeeds")
        else {
            panic!("unexpected list output");
        };
        assert!(items.len() <= 2);
        seen.extend(items.iter().map(|key| key.as_slice().to_vec()));
        pages += 1;
        assert!(pages <= 4, "pagination did not terminate");
        match page.cursor() {
            Some(next) => cursor = Some(next.clone()),
            None => break,
        }
    }
    seen.sort();
    let expected: Vec<Vec<u8>> = (0..4).map(|i| format!("h{i}").into_bytes()).collect();
    assert_eq!(
        seen, expected,
        "only the 4 keys visible at the snapshot, paginated"
    );
    assert_eq!(pages, 2, "4 keys at limit 2 = two pages");
}

fn scan_page(
    executor: &mut Executor,
    start: Option<Bytes>,
    limit: u64,
) -> (Vec<Bytes>, Option<Bytes>) {
    match executor
        .execute(Command::KvScan {
            branch: None,
            space: None,
            start,
            limit: Some(limit),
        })
        .expect("scan succeeds")
    {
        Output::KvScanResult { items, page } => (
            items.iter().map(|item| item.key().clone()).collect(),
            page.cursor().cloned(),
        ),
        output => panic!("unexpected scan output: {output:?}"),
    }
}

fn execute_scan(
    executor: &mut Executor,
    start: Option<&str>,
    limit: Option<u64>,
) -> Vec<(Bytes, Bytes)> {
    match executor
        .execute(Command::KvScan {
            branch: None,
            space: None,
            start: start.map(bytes),
            limit,
        })
        .expect("scan succeeds")
    {
        Output::KvScanResult { items: rows, .. } => rows
            .into_iter()
            .take_while(|row| row.key().as_slice().starts_with(b"prefix-"))
            .map(|row| (row.key().clone(), row.value().clone()))
            .collect(),
        output => panic!("unexpected scan output: {output:?}"),
    }
}

fn execute_batch_get(executor: &mut Executor, keys: Vec<&str>) -> Vec<Option<Bytes>> {
    match executor
        .execute(Command::KvBatchGet {
            branch: None,
            space: None,
            keys: keys.into_iter().map(bytes).collect(),
            as_of: None,
            as_of_time: None,
        })
        .expect("batch get succeeds")
    {
        Output::BatchGetResults(results) => results
            .into_iter()
            .map(|result| result.value().cloned())
            .collect(),
        output => panic!("unexpected batch get output: {output:?}"),
    }
}

fn execute_batch_delete(executor: &mut Executor, keys: Vec<&str>) -> Vec<bool> {
    match executor
        .execute(Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: keys.into_iter().map(bytes).collect(),
        })
        .expect("batch delete succeeds")
    {
        Output::BatchResults(results) => {
            results.into_iter().map(|result| result.applied()).collect()
        }
        output => panic!("unexpected batch delete output: {output:?}"),
    }
}

fn execute_batch_exists(executor: &mut Executor, keys: Vec<&str>) -> Vec<bool> {
    match executor
        .execute(Command::KvBatchExists {
            branch: None,
            space: None,
            keys: keys.into_iter().map(bytes).collect(),
        })
        .expect("batch exists succeeds")
    {
        Output::BatchExistsResults(results) => {
            results.into_iter().map(|item| item.exists()).collect()
        }
        output => panic!("unexpected batch exists output: {output:?}"),
    }
}

fn execute_exists(executor: &mut Executor, key: &str) -> bool {
    match executor
        .execute(Command::KvExists {
            branch: None,
            space: None,
            key: bytes(key),
        })
        .expect("exists succeeds")
    {
        Output::Bool(value) => value,
        output => panic!("unexpected exists output: {output:?}"),
    }
}

fn execute_count(executor: &mut Executor, prefix: Option<&str>) -> u64 {
    match executor
        .execute(Command::KvCount {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            as_of: None,
            as_of_time: None,
        })
        .expect("count succeeds")
    {
        Output::Uint(count) => count,
        output => panic!("unexpected count output: {output:?}"),
    }
}

fn execute_count_as_of(executor: &mut Executor, prefix: Option<&str>, as_of: u64) -> u64 {
    match executor
        .execute(Command::KvCount {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            as_of: Some(as_of),
            as_of_time: None,
        })
        .expect("count as_of succeeds")
    {
        Output::Uint(count) => count,
        output => panic!("unexpected count output: {output:?}"),
    }
}

#[test]
fn kv_count_as_of_returns_historical_count() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let first = write(&mut executor, None, None, "a", "1");
    write(&mut executor, None, None, "b", "2");
    write(&mut executor, None, None, "c", "3");
    assert_eq!(execute_count(&mut executor, None), 3);
    assert_eq!(
        execute_count_as_of(&mut executor, None, first.timestamp),
        1,
        "count as_of the first write sees only the first key"
    );
}

fn execute_sample(executor: &mut Executor, prefix: Option<&str>, count: u64) -> (u64, Vec<Bytes>) {
    match executor
        .execute(Command::KvSample {
            branch: None,
            space: None,
            prefix: prefix.map(bytes),
            count: Some(count),
        })
        .expect("sample succeeds")
    {
        Output::SampleResult {
            total_count, items, ..
        } => (
            total_count,
            items.into_iter().map(|item| item.key().clone()).collect(),
        ),
        output => panic!("unexpected sample output: {output:?}"),
    }
}

fn assert_history_has_tombstone(executor: &mut Executor, key: &str) {
    match executor
        .execute(Command::KvHistory {
            branch: None,
            space: None,
            key: bytes(key),
        })
        .expect("history succeeds")
    {
        Output::VersionHistory(Some(history)) => assert!(
            history
                .items()
                .iter()
                .any(strata_executor::HistoryItem::is_tombstone),
            "history should include a tombstone"
        ),
        output => panic!("unexpected history output: {output:?}"),
    }
}

fn bytes(value: &str) -> Bytes {
    Bytes::from(value)
}

/// #3112 S3a: separates commits in WALL-CLOCK time before committing.
///
/// Wall-clock resolution addresses commit BOUNDARIES, so two commits inside one
/// microsecond are deliberately indistinguishable by instant — the greatest
/// version at or before the target wins, matching the temporal contract's rule
/// for duplicate timestamps. A fixture that wants to address each commit
/// separately must therefore give each its own microsecond.
///
/// The wait happens BEFORE the commit on purpose. Retrying a commit until the
/// clock ticks would leave the rejected attempts sitting at instants the test
/// then reads at, and those extra commits — not the recorded one — would be
/// what a target resolved to.
fn spaced<T>(commit: impl FnOnce(&mut Executor) -> T, executor: &mut Executor) -> T {
    std::thread::sleep(std::time::Duration::from_millis(2));
    commit(executor)
}

/// Fails loudly if the platform clock was too coarse to separate the commits,
/// rather than letting the surrounding assertions flake.
fn assert_distinct_instants(stamps: &[(u64, u64)]) {
    for pair in stamps.windows(2) {
        assert!(
            pair[1].1 > pair[0].1,
            "fixture needs strictly increasing wall-clock instants, got {} then {}",
            pair[0].1,
            pair[1].1
        );
    }
}

/// #3112 S3a: both of a commit's clocks — `(logical timestamp, committed_at)`.
/// The pair is what lets a test ask the same temporal question twice, once in
/// each clock, and demand the same answer.
fn put_stamp(executor: &mut Executor, key: &str, value: &str) -> (u64, u64) {
    let output = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes(key),
            value: bytes(value),
        })
        .expect("put succeeds");
    commit_stamp(&output)
}

fn delete_stamp(executor: &mut Executor, key: &str) -> (u64, u64) {
    let output = executor
        .execute(Command::KvDelete {
            branch: None,
            space: None,
            key: bytes(key),
        })
        .expect("delete succeeds");
    commit_stamp(&output)
}

fn commit_stamp(output: &Output) -> (u64, u64) {
    let commit = match output {
        Output::WriteResult { commit, .. } => commit,
        Output::DeleteResult { commit, .. } => commit
            .as_ref()
            .expect("a delete that applied carries a commit receipt"),
        other => panic!("unexpected write output: {other:?}"),
    };
    (
        commit.timestamp(),
        commit
            .committed_at()
            .expect("a live commit records a wall-clock instant"),
    )
}

#[allow(clippy::result_large_err)] // ExecutorResult mirrors the wire error type
fn kv_get_at(
    executor: &mut Executor,
    key: &str,
    as_of: Option<u64>,
    as_of_time: Option<u64>,
) -> Result<Output, strata_executor::ExecutorError> {
    executor.execute(Command::KvGet {
        branch: None,
        space: None,
        key: bytes(key),
        as_of,
        as_of_time,
    })
}

/// A put's commit as `(version, logical timestamp)`, each read from its own
/// receipt field, so a test can tell the two apart.
fn put_receipt(executor: &mut Executor, key: &str, value: &str) -> (u64, u64) {
    let output = executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: bytes(key),
            value: bytes(value),
        })
        .expect("put succeeds");
    let Output::WriteResult { commit, .. } = output else {
        panic!("unexpected put output: {output:?}");
    };
    (commit.version(), commit.timestamp())
}

/// Each item of a KV batch read as `(found, value, version, timestamp)`.
type KvBatchAnswers = Vec<(bool, Option<Bytes>, Option<u64>, Option<u64>)>;

fn kv_batch_get_at(
    executor: &mut Executor,
    keys: &[&str],
    as_of: Option<u64>,
    as_of_time: Option<u64>,
) -> Result<KvBatchAnswers, Box<strata_executor::ExecutorError>> {
    let output = executor.execute(Command::KvBatchGet {
        branch: None,
        space: None,
        keys: keys.iter().copied().map(bytes).collect(),
        as_of,
        as_of_time,
    })?;
    let Output::BatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    Ok(results
        .into_iter()
        .map(|item| {
            let result = item.result().expect("every read item carries a result");
            (
                result.found(),
                result.value().cloned(),
                result.version(),
                result.timestamp(),
            )
        })
        .collect())
}

/// #3581: `as_of` pins every key of a batch to one commit — the batch at a
/// position says what that position said, in request order, with a key
/// deleted later still present and a key written later still absent, while
/// the unpinned batch says the latest.
#[test]
fn kv_batch_get_as_of_pins_every_key_to_one_commit() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (v_a1, t_a1) = put_receipt(&mut executor, "a", "one");
    let (v_b, t_b) = put_receipt(&mut executor, "b", "bee");
    let (v_a2, t_a2) = put_receipt(&mut executor, "a", "two");
    delete_stamp(&mut executor, "b");
    put_receipt(&mut executor, "c", "sea");
    let keys = ["a", "b", "c", "a"];

    // `as_of` addresses the logical timeline, so the read takes B's
    // timestamp; each answer carries its row's own version and timestamp.
    let at_b = kv_batch_get_at(&mut executor, &keys, Some(t_b), None).expect("as_of read");
    assert_eq!(
        at_b,
        vec![
            (true, Some(bytes("one")), Some(v_a1), Some(t_a1)),
            (true, Some(bytes("bee")), Some(v_b), Some(t_b)),
            (false, None, None, None),
            (true, Some(bytes("one")), Some(v_a1), Some(t_a1)),
        ],
        "at B's commit: A's first value, B present, C not yet written"
    );

    let latest = kv_batch_get_at(&mut executor, &keys, None, None).expect("latest read");
    assert_eq!(
        latest[0],
        (true, Some(bytes("two")), Some(v_a2), Some(t_a2))
    );
    assert_eq!(
        latest[1],
        (false, None, None, None),
        "B is gone at the latest"
    );
    assert!(latest[2].0, "C exists at the latest");
    assert_eq!(latest[3], latest[0], "a duplicate key is answered again");
}

/// #3581: `as_of_time` on a batch is exactly `as_of` at the instant's commit,
/// for every commit — the same contract the single read keeps (#3112).
#[test]
fn kv_batch_get_as_of_time_matches_as_of_at_each_commit() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let stamps = vec![
        spaced(|e| put_stamp(e, "drift", "one"), &mut executor),
        spaced(|e| put_stamp(e, "drift", "two"), &mut executor),
        spaced(|e| put_stamp(e, "other", "elsewhere"), &mut executor),
        spaced(|e| delete_stamp(e, "drift"), &mut executor),
    ];
    assert_distinct_instants(&stamps);
    let keys = ["drift", "other"];
    for (index, (timestamp, committed_at)) in stamps.iter().copied().enumerate() {
        let by_logical = kv_batch_get_at(&mut executor, &keys, Some(timestamp), None)
            .expect("logical as_of batch");
        let by_wall_clock = kv_batch_get_at(&mut executor, &keys, None, Some(committed_at))
            .expect("wall-clock as_of batch");
        assert_eq!(by_logical, by_wall_clock, "commit {index}");
    }
    let first =
        kv_batch_get_at(&mut executor, &keys, None, Some(stamps[0].1)).expect("first commit");
    let last = kv_batch_get_at(&mut executor, &keys, None, Some(stamps[3].1)).expect("last commit");
    assert_ne!(
        first, last,
        "the history is observably different across commits"
    );
}

/// #3581: the batch keeps the single read's refusals — both clocks at once
/// is a conflict, and a position outside the branch's history is a
/// diagnostic for the whole batch, never a fallback to the latest state.
#[test]
fn kv_batch_get_as_of_refusals_match_the_single_read() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (timestamp, committed_at) = put_stamp(&mut executor, "a", "one");
    let keys = ["a", "missing"];

    let conflict = kv_batch_get_at(&mut executor, &keys, Some(timestamp), Some(committed_at))
        .expect_err("both clocks at once is refused");
    let single_conflict = kv_get_at(&mut executor, "a", Some(timestamp), Some(committed_at))
        .expect_err("the single read refuses both clocks too");
    assert_eq!(conflict.class(), ExecutorErrorClass::InvalidInput);
    assert_eq!(conflict.code(), "invalid_argument.executor.as_of_conflict");
    assert_eq!(conflict.code(), single_conflict.code());

    for (as_of, as_of_time, what) in [
        (
            Some(u64::MAX),
            None,
            "a timeline position after the latest commit",
        ),
        (None, Some(u64::MAX), "an instant after the latest commit"),
        (None, Some(0), "an instant before the first commit"),
    ] {
        let batch = kv_batch_get_at(&mut executor, &keys, as_of, as_of_time)
            .expect_err("an out-of-history batch is refused");
        let single =
            kv_get_at(&mut executor, "a", as_of, as_of_time).expect_err("the single read too");
        assert_eq!(batch.class(), single.class(), "{what}");
        assert_eq!(batch.code(), single.code(), "{what}");
    }
    let after_latest = kv_batch_get_at(&mut executor, &keys, Some(u64::MAX), None)
        .expect_err("a timeline position after the latest commit is a diagnostic");
    assert_eq!(
        after_latest.code(),
        "history_unavailable.engine.after_latest_commit"
    );
}

/// The number of items a KV batch read answers, whatever each item says.
fn kv_batch_get_items(
    executor: &mut Executor,
    keys: &[&str],
    as_of: Option<u64>,
    as_of_time: Option<u64>,
) -> Result<usize, Box<strata_executor::ExecutorError>> {
    let output = executor.execute(Command::KvBatchGet {
        branch: None,
        space: None,
        keys: keys.iter().copied().map(bytes).collect(),
        as_of,
        as_of_time,
    })?;
    let Output::BatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    Ok(results.into_iter().count())
}

/// #3763: a pinned batch checks its position even when it reads no key —
/// one that names none, and one whose every key is refused item by item.
/// Each clock refuses an out-of-history position exactly as the single read
/// does, so the two clocks agree at each end of history whatever the batch
/// holds. An empty key is the refused key.
#[test]
fn kv_batch_get_checks_its_position_even_when_it_reads_no_key() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (timestamp, committed_at) = put_stamp(&mut executor, "a", "one");

    for (keys, shape) in [
        (&[][..], "a batch naming no key"),
        (&[""][..], "a batch whose every key is refused"),
    ] {
        for (code, logical, wall_clock) in [
            ("history_unavailable.engine.before_first_commit", 0, 0),
            (
                "history_unavailable.engine.after_latest_commit",
                u64::MAX,
                u64::MAX,
            ),
        ] {
            for (as_of, as_of_time) in [(Some(logical), None), (None, Some(wall_clock))] {
                let what = format!("{shape}, as_of {as_of:?}, as_of_time {as_of_time:?}");
                let batch = kv_batch_get_items(&mut executor, keys, as_of, as_of_time)
                    .expect_err("an out-of-history position fails the whole batch");
                let single = kv_get_at(&mut executor, "a", as_of, as_of_time)
                    .expect_err("the single read is refused too");
                assert_eq!(batch.class(), single.class(), "{what}");
                assert_eq!(batch.code(), single.code(), "{what}");
                assert_eq!(batch.code(), code, "{what}");
            }
        }

        // Direction control: inside history the same batch answers, one item
        // per key, on either clock.
        for (as_of, as_of_time) in [(Some(timestamp), None), (None, Some(committed_at))] {
            let items = kv_batch_get_items(&mut executor, keys, as_of, as_of_time)
                .expect("an in-history batch answers");
            assert_eq!(items, keys.len(), "{shape}");
        }
    }
}

/// The `Executor::kv_batch_get` facade is the unpinned command on the default
/// branch and space: it answers the latest value of every key, in order, and
/// says the same thing the `KvBatchGet` command says.
#[test]
fn kv_batch_get_facade_reads_the_latest_state_of_the_default_branch() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let (v_a1, t_a1) = put_receipt(&mut executor, "a", "one");
    put_receipt(&mut executor, "a", "two");
    let (v_b, t_b) = put_receipt(&mut executor, "b", "bee");
    let keys = ["a", "missing", "b"];

    let output = executor
        .kv_batch_get(keys.iter().copied().map(bytes).collect())
        .expect("facade batch get succeeds");
    let Output::BatchGetResults(results) = output else {
        panic!("unexpected batch get output: {output:?}");
    };
    let answers: KvBatchAnswers = results
        .into_iter()
        .map(|item| {
            let result = item.result().expect("every read item carries a result");
            (
                result.found(),
                result.value().cloned(),
                result.version(),
                result.timestamp(),
            )
        })
        .collect();
    assert!(v_a1 < v_b && t_a1 < t_b, "the commits are ordered");
    assert_eq!(answers[0].1, Some(bytes("two")), "the latest value of A");
    assert_eq!(answers[1], (false, None, None, None), "an absent key");
    assert_eq!(answers[2], (true, Some(bytes("bee")), Some(v_b), Some(t_b)));
    assert_eq!(
        answers,
        kv_batch_get_at(&mut executor, &keys, None, None).expect("command batch get"),
        "the facade answers what the command answers"
    );
}

/// Pins the `KvPut` contract: each put is one atomic commit with a strictly
/// increasing version, and an acknowledged write is immediately visible to
/// subsequent reads from the same database — never transiently invisible.
#[test]
fn kv_put_is_atomic_with_read_after_write_visibility() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let mut last_version = 0;
    for (round, value) in ["one", "two", "three"].iter().enumerate() {
        let output = executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: bytes("raw-key"),
                value: bytes(value),
            })
            .expect("put succeeds");
        let Output::WriteResult { effect, commit, .. } = output else {
            panic!("unexpected put output: {output:?}");
        };
        assert!(effect.applied());
        assert!(
            commit.version() > last_version,
            "commit versions increase monotonically"
        );
        last_version = commit.version();

        // Read-after-write: the acknowledged value is immediately visible.
        let read = match executor
            .execute(Command::KvGet {
                branch: None,
                space: None,
                key: bytes("raw-key"),
                as_of: None,
                as_of_time: None,
            })
            .expect("get succeeds")
        {
            Output::KvVersionedValue(value) => {
                value.into_option().expect("value present after write")
            }
            output => panic!("unexpected get output on round {round}: {output:?}"),
        };
        assert_eq!(read.value(), &bytes(value));
        assert!(read.version() >= last_version);
    }
}
