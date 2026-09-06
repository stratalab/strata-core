//! #3112 S4: `history` reports each commit's wall-clock instant.
//!
//! Before this slice `history` returned `{version, timestamp, value}` and
//! nothing else, so the natural loop — *look at what changed, see when, read
//! as of then* — broke at step two: the only thing to point at was a
//! commit-timeline counter.
//!
//! The load-bearing assertion here is not "a `committed_at` field is present"
//! but "**the right** instant is on **the right** row". Instants are joined
//! onto rows by commit version after the read, so a shifted or mis-ordered
//! join would date every row plausibly and wrongly — a failure no
//! presence-check would notice. Every test below compares against the instant
//! each write ack itself reported.

use strata_executor::{Bytes, Command, Executor, Output, VectorDistanceMetric};

/// Separates commits in wall-clock time before committing, so each lands in
/// its own microsecond and mis-attribution is detectable.
fn spaced<T>(executor: &mut Executor, commit: impl FnOnce(&mut Executor) -> T) -> T {
    std::thread::sleep(std::time::Duration::from_millis(2));
    commit(executor)
}

/// The `(version, committed_at)` a write ack reported — the ground truth a
/// history row is checked against.
fn ack(executor: &mut Executor, command: Command) -> (u64, u64) {
    let output = executor.execute(command).expect("write succeeds");
    let commit = match &output {
        Output::WriteResult { commit, .. }
        | Output::JsonWriteResult { commit, .. }
        | Output::VectorWriteResult { commit, .. } => commit,
        Output::DeleteResult { commit, .. } | Output::JsonDeleteResult { commit, .. } => commit
            .as_ref()
            .expect("an applied delete carries a commit receipt"),
        other => panic!("unexpected write output: {other:?}"),
    };
    (
        commit.version(),
        commit
            .committed_at()
            .expect("a live commit records a wall-clock instant"),
    )
}

#[test]
fn kv_history_dates_every_row_with_its_own_commits_instant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");

    let put_one = spaced(&mut executor, |e| {
        ack(
            e,
            Command::KvPut {
                branch: None,
                space: None,
                key: Bytes::from("k"),
                value: Bytes::from("one"),
            },
        )
    });
    let put_two = spaced(&mut executor, |e| {
        ack(
            e,
            Command::KvPut {
                branch: None,
                space: None,
                key: Bytes::from("k"),
                value: Bytes::from("two"),
            },
        )
    });
    // A delete is a commit too, and its history row must carry a date like any
    // other — "when was this removed" is exactly the question people ask.
    let deleted = spaced(&mut executor, |e| {
        ack(
            e,
            Command::KvDelete {
                branch: None,
                space: None,
                key: Bytes::from("k"),
            },
        )
    });

    let output = executor
        .execute(Command::KvHistory {
            branch: None,
            space: None,
            key: Bytes::from("k"),
        })
        .expect("history succeeds");
    let Output::VersionHistory(Some(history)) = output else {
        panic!("unexpected history output: {output:?}");
    };

    // Three distinct instants, so a join that shifted by one row would show up.
    assert_eq!(history.count(), 3);
    let by_version: Vec<_> = history
        .items()
        .iter()
        .map(|item| (item.version(), item.committed_at()))
        .collect();
    assert_eq!(
        by_version,
        vec![
            (deleted.0, Some(deleted.1)),
            (put_two.0, Some(put_two.1)),
            (put_one.0, Some(put_one.1)),
        ],
        "each row must carry the instant its own write ack reported"
    );

    // And the dates are strictly increasing oldest-to-newest, so the history
    // reads as a timeline rather than a set of unrelated stamps.
    assert!(put_one.1 < put_two.1 && put_two.1 < deleted.1);

    // The tombstone row is the delete, and it is dated.
    let tombstone = history
        .items()
        .iter()
        .find(|item| item.is_tombstone())
        .expect("the delete appears in history");
    assert_eq!(tombstone.committed_at(), Some(deleted.1));
}

#[test]
fn json_history_dates_every_row_with_its_own_commits_instant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let set = |value: &str| Command::JsonSet {
        branch: None,
        space: None,
        key: "doc".to_owned(),
        path: "$.field".to_owned(),
        value: serde_json::json!(value),
    };
    let first = spaced(&mut executor, |e| ack(e, set("one")));
    let second = spaced(&mut executor, |e| ack(e, set("two")));

    let output = executor
        .execute(Command::JsonHistory {
            branch: None,
            space: None,
            key: "doc".to_owned(),
        })
        .expect("history succeeds");
    let Output::JsonVersionHistory(Some(items)) = output else {
        panic!("unexpected history output: {output:?}");
    };
    assert_eq!(
        items
            .iter()
            .map(|item| (item.version(), item.committed_at()))
            .collect::<Vec<_>>(),
        vec![(second.0, Some(second.1)), (first.0, Some(first.1))]
    );
}

#[test]
fn vector_history_dates_every_row_with_its_own_commits_instant() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    executor
        .execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: "c".to_owned(),
            dimension: 2,
            metric: VectorDistanceMetric::Cosine,
        })
        .expect("collection created");
    let upsert = |vector: Vec<f64>| Command::VectorUpsert {
        branch: None,
        space: None,
        collection: "c".to_owned(),
        key: "v".to_owned(),
        vector,
        metadata: None,
    };
    let first = spaced(&mut executor, |e| ack(e, upsert(vec![1.0, 0.0])));
    let second = spaced(&mut executor, |e| ack(e, upsert(vec![0.0, 1.0])));

    let output = executor
        .execute(Command::VectorHistory {
            branch: None,
            space: None,
            collection: "c".to_owned(),
            key: "v".to_owned(),
        })
        .expect("history succeeds");
    let Output::VectorVersionHistory(Some(history)) = output else {
        panic!("unexpected history output: {output:?}");
    };
    assert_eq!(
        history
            .items()
            .iter()
            .map(|item| (item.version(), item.committed_at()))
            .collect::<Vec<_>>(),
        vec![(second.0, Some(second.1)), (first.0, Some(first.1))]
    );
}

/// The loop this slice exists to enable: read history, take a date off a row,
/// and use it to read the value as of that point. Before S4 there was nothing
/// dated to point at.
#[test]
fn a_date_from_history_can_be_handed_straight_back_to_as_of_time() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    for value in ["one", "two"] {
        spaced(&mut executor, |e| {
            ack(
                e,
                Command::KvPut {
                    branch: None,
                    space: None,
                    key: Bytes::from("k"),
                    value: Bytes::from(value),
                },
            )
        });
    }

    let output = executor
        .execute(Command::KvHistory {
            branch: None,
            space: None,
            key: Bytes::from("k"),
        })
        .expect("history succeeds");
    let Output::VersionHistory(Some(history)) = output else {
        panic!("unexpected history output: {output:?}");
    };

    // The OLDEST row's date, fed straight back as a read bound.
    let oldest = history.items().last().expect("history has an oldest row");
    let instant = oldest.committed_at().expect("history rows carry dates");

    let at_that_date = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: Bytes::from("k"),
            as_of: None,
            as_of_time: Some(instant),
        })
        .expect("reading at a date taken from history succeeds");
    let by_logical = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: Bytes::from("k"),
            as_of: Some(oldest.timestamp()),
            as_of_time: None,
        })
        .expect("the same row's logical position also reads");
    assert_eq!(
        at_that_date, by_logical,
        "a row's date and its logical position must name the same point"
    );

    // And it is genuinely the older value, not current state.
    let current = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: Bytes::from("k"),
            as_of: None,
            as_of_time: None,
        })
        .expect("current read succeeds");
    assert_ne!(
        at_that_date, current,
        "the date from the oldest row must not resolve to current state"
    );
}
