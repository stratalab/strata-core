//! TCP4.7 axis: `as_of` × read-command parity (#2702).
//!
//! Time-travel should be one capability with one coverage rule, but `as_of`
//! is accepted by most reads and rejected as an unknown field by a scattered
//! remainder. The coverage gap is ledgered; the semantic contract that holds
//! (a read as-of the write's own commit timestamp equals the live read) is
//! asserted permanently.

use serde_json::json;
use strata_executor::Command;

#[path = "parity/support.rs"]
mod support;

/// Contract that holds today: reading as-of the write's own commit
/// timestamp equals the live read (per the temporal contract, `as_of` at the
/// latest commit is in-window; only beyond-latest raises).
#[test]
fn as_of_at_the_write_timestamp_agrees_with_the_live_read() {
    let mut executor = support::executor();
    let put = support::run(
        &mut executor,
        &json!({"type": "kv_put", "key": "YQ==", "value": "b25l"}),
    );
    let timestamp = put["data"]["commit"]["timestamp"]
        .as_u64()
        .expect("write receipt carries a commit timestamp");

    let live = support::run(&mut executor, &json!({"type": "kv_get", "key": "YQ=="}));
    let as_of = support::run(
        &mut executor,
        &json!({"type": "kv_get", "key": "YQ==", "as_of": timestamp}),
    );
    assert_eq!(
        live["data"], as_of["data"],
        "a read as-of the write's commit timestamp equals the live read"
    );
}

/// Contract (#3145): the event range reads are latest-only. The log as of a
/// commit is read with `event_list`'s `as_of` and sequence cursor; the
/// temporal contract's conformance rule 1 says why a range is not.
#[test]
fn event_ranges_take_no_as_of() {
    let ranges = [
        json!({"type": "event_range", "start_seq": 0, "direction": "forward"}),
        json!({"type": "event_range_by_time", "start_ts": 0, "direction": "forward"}),
    ];
    for range in ranges {
        // The unbounded request is well-formed, so each rejection below can
        // only be the bound itself.
        serde_json::from_value::<Command>(range.clone())
            .unwrap_or_else(|err| panic!("the unbounded range is accepted ({range}): {err}"));
        for bound in ["as_of", "as_of_time"] {
            let mut wire = range.clone();
            wire[bound] = json!(5);
            let err = serde_json::from_value::<Command>(wire.clone())
                .expect_err("an event range is latest-only by contract")
                .to_string();
            assert!(
                err.contains(&format!("unknown field `{bound}`")),
                "{wire} must be refused for its `{bound}`, not for another \
                 reason; adding a commit-timeline bound needs the temporal \
                 contract amended first: {err}"
            );
        }
    }
}

/// PIN #2702: `as_of` coverage is patchy — accepted by `kv_get` and
/// `event_list`, rejected as an unknown field by `kv_exists` and
/// `event_verify_chain`.
#[test]
fn pin_2702_as_of_coverage_is_patchy_across_reads() {
    support::pinned("as_of_coverage", 2702);

    let accepts = |wire: serde_json::Value| {
        serde_json::from_value::<Command>(wire.clone())
            .unwrap_or_else(|err| panic!("must accept as_of ({wire}): {err}"));
    };
    let rejects = |wire: serde_json::Value| {
        assert!(
            serde_json::from_value::<Command>(wire.clone()).is_err(),
            "today: must reject as_of as an unknown field ({wire}); if this \
             fails, the command gained as_of — update the ledger entry"
        );
    };

    accepts(json!({"type": "kv_get", "key": "YQ==", "as_of": 5}));
    accepts(json!({"type": "event_list", "as_of": 5}));

    rejects(json!({"type": "kv_exists", "key": "YQ==", "as_of": 5}));
    rejects(json!({"type": "event_verify_chain", "as_of": 5}));
}

/// Ledger guard (entry ⇒ pin): every `as_of*` ledger entry is pinned here.
#[test]
fn every_as_of_ledger_entry_is_pinned_here() {
    support::assert_ledger_entries_all_pinned("as_of", &[("as_of_coverage", 2702)]);
}
