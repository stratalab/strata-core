//! Property tests: event-log temporal reads match an append-only reference
//! oracle, and a branch forked at a version sees exactly the events committed
//! up to that version.
//!
//! Events are append-only: immutable once written, monotonic, no tombstones or
//! overwrites. So they do not fit the keyed-mutable timeline oracle
//! (`temporal_timeline_model.rs`, TCP3.6b-i). Their temporal surface is
//! timestamp-based MVCC visibility on the branch commit timeline (not the
//! event's own occurrence time): `len_at(ts)` is the number of events committed
//! by `ts`, and `get_at(sequence, ts)` yields the event iff its append
//! committed by `ts`. `len_at` is lenient before the first commit (returns 0);
//! `get_at` out-of-range (before the first commit / after the latest commit)
//! is a `history_unavailable` diagnostic naming that end, matching the KV
//! boundary contract.

#![allow(clippy::result_large_err)]

mod common;

use common::{branch, open_cache_database, space};
use proptest::prelude::*;
use serde_json::json;
use strata_core::{CommitVersion, Timestamp};
use strata_engine::{Database, EventPayload, EventSequence, EventType};

const BEFORE_FIRST_CODE: &str = "history_unavailable.engine.before_first_commit";
const AFTER_LATEST_CODE: &str = "history_unavailable.engine.after_latest_commit";

fn event_type() -> EventType {
    EventType::new("conformance.event").expect("valid event type")
}

fn payload(index: usize) -> EventPayload {
    EventPayload::new(json!({ "i": index })).expect("valid payload")
}

/// Appends `count` events on `branch_name`, returning each append's commit
/// (version, timestamp) in sequence order.
fn append_events(
    db: &mut Database,
    branch_name: &str,
    count: usize,
) -> Vec<(CommitVersion, Timestamp)> {
    let mut events = db
        .event(branch(branch_name), space("default"))
        .expect("event service");
    (0..count)
        .map(|index| {
            let commit = events
                .append(event_type(), payload(index))
                .expect("append commits")
                .commit();
            (commit.version(), commit.timestamp())
        })
        .collect()
}

proptest! {
    #[test]
    fn event_temporal_reads_match_append_oracle(count in 1usize..20) {
        let mut db = open_cache_database().expect("cache database opens");
        let commits = append_events(&mut db, "default", count);

        let mut events = db
            .event(branch("default"), space("default"))
            .expect("event service");

        // The log length is the number of appends, and the as-of length at each
        // commit timestamp is the number of events committed by then.
        prop_assert_eq!(events.len().expect("len").count(), count as u64);
        for &(_, timestamp) in &commits {
            let expected = commits.iter().filter(|(_, ts)| *ts <= timestamp).count() as u64;
            prop_assert_eq!(events.len_at(timestamp).expect("len_at").count(), expected);
        }

        // Each event is visible at latest, and as-of a timestamp iff its append
        // committed by then. A sequence past the end is absent.
        for sequence in 0..count {
            let seq = EventSequence::new(sequence as u64);
            prop_assert!(events.get(seq).expect("latest get").is_some());
            let (_, seq_ts) = commits[sequence];
            for &(_, timestamp) in &commits {
                let visible = seq_ts <= timestamp;
                prop_assert_eq!(
                    events.get_at(seq, timestamp).expect("as-of get").is_some(),
                    visible
                );
            }
        }
        prop_assert!(events
            .get(EventSequence::new(count as u64))
            .expect("past-end get")
            .is_none());

        // Out-of-range as-of reads are diagnostics, not absence or clamping.
        let before = events
            .get_at(EventSequence::new(0), Timestamp::EPOCH)
            .expect_err("before-history read is a diagnostic");
        prop_assert_eq!(before.code(), BEFORE_FIRST_CODE);
        let after = events
            .get_at(EventSequence::new(0), Timestamp::MAX)
            .expect_err("after-latest read is a diagnostic");
        prop_assert_eq!(after.code(), AFTER_LATEST_CODE);
    }

    #[test]
    fn event_fork_at_version_sees_events_up_to_that_version(
        count in 1usize..15,
        fork_point in 0usize..15,
    ) {
        let mut db = open_cache_database().expect("cache database opens");
        let commits = append_events(&mut db, "default", count);

        // Forking at event `index`'s append version captures the log through
        // that event: sequences 0..=index, i.e. index + 1 events.
        let index = fork_point % count;
        let fork_version = commits[index].0;
        db.branches()
            .expect("branch service")
            .fork_at_version(&branch("default"), branch("snapshot"), fork_version)
            .expect("fork at version");

        let child_len = db
            .event(branch("snapshot"), space("default"))
            .expect("child event service")
            .len()
            .expect("child len")
            .count();
        prop_assert_eq!(child_len, (index + 1) as u64);
    }
}
