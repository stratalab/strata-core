//! W0c — machine-readable STH status ledger + status-lint (remediation for the
//! 2026-09-02 test-coverage audit, Phase-1 finding 3).
//!
//! The existence-only charter guard (`testing_charter_guard.rs`) checks that the
//! backticked PATHS cited in the charter docs resolve to real files. It cannot
//! catch stale STATUS facts — a fuzz-target count that fell behind (the docs said
//! 28/30 while the tree had grown), a coverage gate that was superseded (the old
//! 73.0% workspace floor vs today's per-crate product-only floors), or a renamed /
//! removed CI lane. Those are exactly the drifts the audit found.
//!
//! This file records the STH facts as machine-readable consts and lints them
//! against the real repo, so a drift fails a test and forces the ledger update
//! instead of rotting in prose. It deliberately does NOT parse charter prose or
//! duplicate the path-existence guard — it checks a small set of load-bearing,
//! mechanically-verifiable facts.

use std::path::{Path, PathBuf};

/// Number of libFuzzer targets under `crates/storage/fuzz/fuzz_targets/`.
/// `fuzz.yml` enumerates them dynamically (`cargo fuzz list`), so this is an
/// OBSERVED fact the lint keeps honest — never a normative cap. A new target
/// must bump this line (which is the point: the count stops silently drifting).
const STH_FUZZ_TARGET_COUNT: usize = 39;

/// The STH / Phase-1 nightly jobs. Each MUST exist as a job in `nightly.yml`;
/// a renamed or removed lane fails the lint (so the ledger cannot claim a lane
/// that no longer runs).
const STH_NIGHTLY_JOBS: &[&str] = &[
    "memory-safety-miri",
    "memory-safety-address-sanitizer",
    "memory-safety-thread-sanitizer",
    "failure-during-failure-soak",
    "storage-soak-lanes",
    "coverage-baseline",
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

fn read(root: &Path, relative: &str) -> String {
    std::fs::read_to_string(root.join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

/// The recorded fuzz-target count must equal the on-disk inventory, and the count
/// must stay an observed fact (dynamic enumeration), not a fixed number CI trusts.
#[test]
fn sth_fuzz_target_count_matches_the_inventory() {
    let root = repo_root();
    let dir = root.join("crates/storage/fuzz/fuzz_targets");
    let on_disk = std::fs::read_dir(&dir)
        .expect("fuzz_targets dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
        .count();
    assert_eq!(
        on_disk, STH_FUZZ_TARGET_COUNT,
        "STH fuzz-target count drifted: {on_disk} `.rs` targets on disk vs ledger \
         {STH_FUZZ_TARGET_COUNT}. Update STH_FUZZ_TARGET_COUNT — this is exactly the stale \
         fixed-count (the old 28/30) the status-lint exists to catch."
    );
    // CI enumerates dynamically (`cargo [+nightly] fuzz list`), so no workflow
    // hard-codes a target count.
    assert!(
        read(&root, ".github/workflows/fuzz.yml").contains("fuzz list"),
        "fuzz.yml must enumerate targets dynamically via `cargo fuzz list`, so new targets join \
         the nightly automatically and no fixed count can go stale"
    );
}

/// Every STH nightly job the ledger names must actually be defined in the workflow.
#[test]
fn sth_nightly_jobs_are_defined() {
    let nightly = read(&repo_root(), ".github/workflows/nightly.yml");
    for job in STH_NIGHTLY_JOBS {
        assert!(
            nightly.contains(&format!("\n  {job}:")),
            "STH nightly job `{job}` is not defined in nightly.yml (renamed or removed?); restore \
             the lane or update STH_NIGHTLY_JOBS"
        );
    }
}

/// The coverage gate is per-crate product-only floors (`coverage_floors.py`), not
/// the superseded 73.0% workspace floor the STH-7 as-built prose still described.
#[test]
fn sth_coverage_gate_is_the_per_crate_product_only_floor() {
    let root = repo_root();
    assert!(
        root.join("scripts/coverage_floors.py").exists(),
        "the per-crate product-only coverage floor gate (scripts/coverage_floors.py) must exist"
    );
    assert!(
        read(&root, ".github/workflows/nightly.yml").contains("coverage_floors.py"),
        "nightly.yml must run the per-crate product-only floor gate (scripts/coverage_floors.py); \
         the old workspace-wide floor was superseded by TCP3.0"
    );
}
