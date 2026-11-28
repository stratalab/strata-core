//! Concurrency conformance (#3776 program, Phase A2).
//!
//! The capability services borrow `&self`, so threads sharing one
//! `Database` run engine operations at once. An operation makes several
//! persistence calls (reads, scans, commits). This harness holds the
//! **contract**: any two operations run at once must leave the state, and
//! give the answers, of one of their two serial orders.
//!
//! For each ordered pair `(first, second)` of a catalog's operations it
//! runs, from the catalog's seed:
//! - both serial orders, which are the references;
//! - `first` held before each of its persistence calls in turn (every point
//!   read, scan and commit it makes), with `second` run to completion while
//!   `first` is held. A `second` that has to wait for `first`'s turn
//!   (ACID-008) queues there instead, and `first` is resumed.
//!
//! A run whose answers and digest match neither reference is a
//! counterexample.
//!
//! Known counterexamples live in [`ledger::KNOWN_GAPS`], which can only
//! shrink. A new counterexample fails the suite, and so does a listed
//! scenario that has become serializable: delete its entry.
//!
//! The harness lives in the crate, not under `tests/`, so the test-only
//! hooks it pauses on are built under default features.

mod event;
mod graph;
mod harness;
mod json;
mod kv;
mod ledger;
mod spaces;

use std::collections::BTreeMap;
use std::fmt::Write;

use harness::{check_catalog, Catalog, Verdict};

/// Every catalog the harness runs.
const CATALOGS: &[&Catalog] = &[
    &kv::CATALOG,
    &json::CATALOG,
    &event::CATALOG,
    &graph::CATALOG,
    &spaces::CATALOG,
];

/// Holds `catalog` to the ledger: every red pair is listed, and every
/// listed pair of this catalog is still red.
fn assert_catalog_matches_ledger(catalog: &Catalog) {
    let verdicts = check_catalog(catalog);
    // A by-contract pair is known with issue 0: expected red, not a gap.
    let known = ledger::KNOWN_GAPS
        .iter()
        .copied()
        .chain(
            ledger::BY_CONTRACT
                .iter()
                .map(|(scenario, _)| (*scenario, 0)),
        )
        .collect::<BTreeMap<&str, u32>>();
    let mut unexpected = Vec::new();
    let mut healed = Vec::new();
    for verdict in &verdicts {
        match (
            verdict.is_serializable(),
            known.get(verdict.scenario.as_str()),
        ) {
            (false, None) => unexpected.push(verdict),
            (true, Some(issue)) => healed.push((verdict, *issue)),
            _ => {}
        }
    }
    eprintln!("{}", report(catalog, &verdicts, &known));
    let mut failures = String::new();
    for verdict in &unexpected {
        let (window, outcome, forward, reverse) = verdict
            .counterexample
            .as_ref()
            .expect("a red verdict carries its counterexample");
        write!(
            failures,
            "\nNEW GAP {}\n  held at {window:?}\n  got      {outcome:?}\n  serial   {forward:?}\n  reverse  {reverse:?}\n",
            verdict.scenario
        )
        .expect("writing to a String does not fail");
    }
    for (verdict, issue) in &healed {
        write!(
            failures,
            "\nHEALED {} (#{issue}): serializable at all {} windows; delete its ledger entry\n",
            verdict.scenario, verdict.windows
        )
        .expect("writing to a String does not fail");
    }
    assert!(
        failures.is_empty(),
        "{} conformance:{failures}",
        catalog.family
    );
}

/// The family's matrix: one line per ordered pair.
fn report(catalog: &Catalog, verdicts: &[Verdict], known: &BTreeMap<&str, u32>) -> String {
    let mut lines = vec![format!("== {} ({} pairs)", catalog.family, verdicts.len())];
    for verdict in verdicts {
        let mark = match (
            verdict.is_serializable(),
            known.get(verdict.scenario.as_str()),
        ) {
            (true, _) => "ok   ".to_owned(),
            (false, Some(0)) => "CONTRACT".to_owned(),
            (false, Some(issue)) => format!("GAP #{issue}"),
            (false, None) => "NEW  ".to_owned(),
        };
        let window = verdict
            .counterexample
            .as_ref()
            .map_or(String::new(), |(window, ..)| format!(" at {window:?}"));
        lines.push(format!(
            "  {mark} {}  [{} windows]{window}",
            verdict.scenario, verdict.windows
        ));
    }
    lines.join("\n")
}

#[test]
fn kv_operations_are_serializable_or_known_gaps() {
    assert_catalog_matches_ledger(&kv::CATALOG);
}

#[test]
fn json_operations_are_serializable_or_known_gaps() {
    assert_catalog_matches_ledger(&json::CATALOG);
}

#[test]
fn event_operations_are_serializable_or_known_gaps() {
    assert_catalog_matches_ledger(&event::CATALOG);
}

#[test]
fn graph_operations_are_serializable_or_known_gaps() {
    assert_catalog_matches_ledger(&graph::CATALOG);
}

#[test]
fn first_writes_to_new_spaces_are_serializable_or_known_gaps() {
    assert_catalog_matches_ledger(&spaces::CATALOG);
}

/// Every ledger entry names a pair some catalog runs, so the ledger cannot
/// keep an entry for an operation that was renamed or removed.
#[test]
fn every_known_gap_names_a_scenario_the_harness_runs() {
    let scenarios = CATALOGS
        .iter()
        .flat_map(|catalog| {
            catalog.ops.iter().flat_map(|first| {
                catalog
                    .ops
                    .iter()
                    .map(|second| harness::scenario_name(first, second))
            })
        })
        .collect::<std::collections::BTreeSet<_>>();
    let unknown = ledger::KNOWN_GAPS
        .iter()
        .map(|(scenario, _)| *scenario)
        .chain(ledger::BY_CONTRACT.iter().map(|(scenario, _)| *scenario))
        .filter(|scenario| !scenarios.contains(*scenario))
        .collect::<Vec<_>>();
    assert!(
        unknown.is_empty(),
        "ledger entries for no scenario: {unknown:?}"
    );
}

/// Every mutating IDL command that two threads can run at once has an
/// operation in some catalog. A command is excluded only with the reason it
/// cannot race, and the exclusions are checked against the IDL too.
#[test]
fn every_concurrent_mutating_command_has_a_harness_operation() {
    let index_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../executor/idl/v1/generated/command-index.json"
    );
    let index: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(index_path).expect("the IDL command index reads"),
    )
    .expect("the IDL command index parses");
    let mutating = index["commands"]
        .as_array()
        .expect("commands")
        .iter()
        .filter(|command| {
            let kind = command["kind"].as_str().unwrap_or_default();
            kind.starts_with("mutation.") || kind.ends_with("_mutation")
        })
        .map(|command| command["id"].as_str().expect("command id").to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let covered = CATALOGS
        .iter()
        .flat_map(|catalog| catalog.ops.iter().map(|op| op.command.to_owned()))
        .collect::<std::collections::BTreeSet<_>>();
    let excluded = ledger::NOT_CONCURRENT
        .iter()
        .chain(ledger::NOT_YET_CATALOGUED)
        .map(|(command, _)| (*command).to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = mutating
        .iter()
        .filter(|command| !covered.contains(*command) && !excluded.contains(*command))
        .collect::<Vec<_>>();
    let stale = excluded
        .iter()
        .filter(|command| !mutating.contains(*command) || covered.contains(*command))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "mutating commands with no harness operation: {missing:?}"
    );
    assert!(
        stale.is_empty(),
        "exclusions that are not mutating commands, or are now covered: {stale:?}"
    );
}
