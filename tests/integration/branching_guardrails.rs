//! Tripwire test that counts known transitional branch-truth shapes
//! across the workspace and compares against a locked snapshot, per
//! epic B1 of the branching execution plan
//! (`docs/design/branching/branching-execution-plan.md`).
//!
//! ## How it works
//!
//! For each pattern in `LOCKED_PATTERNS`, the test walks every `*.rs` file
//! under `crates/`, `src/`, and `tests/` (skipping `target/`,
//! `benchmarks/`, `.git/`, `.claude/`) and counts substring matches. The
//! totals are compared against the snapshot at
//! `tests/integration/data/branching_transitional_shapes.json`.
//!
//! Drift in either direction fails the test:
//!
//! - **count went up**  → someone added a new violation; either fix it or
//!   bump the snapshot if the addition is intentional and acknowledged.
//! - **count went down** → someone removed a transitional shape (good!);
//!   bump the snapshot to acknowledge the cleanup so the new lower count
//!   becomes the floor.
//!
//! On first capture, set `STRATA_GUARDRAIL_CAPTURE=1` and run the test —
//! it writes the snapshot from observation rather than asserting against
//! it. Commit the resulting JSON.
//!
//! ## What's tracked today
//!
//! - `BRANCH_NAMESPACE` const sites — duplicated in
//!   `crates/engine/src/primitives/branch/index.rs` and
//!   `crates/executor/src/bridge.rs`. B2 collapses to one canonical
//!   `BranchId::from_user_name` in `strata_core`.
//! - `merge_base_override` occurrences — the executor → engine override
//!   plumbing B3 removes when lineage moves into `BranchControlStore`.
//! - `MergeStrategy::LastWriterWins` literals — B5 renames to a name
//!   that matches runtime behavior (per Lockstep Set 5 in the plan).
//! - `BranchId::new()` random-construction sites — proxy for places where
//!   a generation-aware identity will be needed once B3 lands `BranchRef`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

// =============================================================================
// Patterns
// =============================================================================

/// One transitional shape to track.
struct Pattern {
    /// Stable key written into the snapshot JSON.
    snapshot_key: &'static str,
    /// Substring searched on each line.
    needle: &'static str,
    /// One-line note for failure messages.
    rationale: &'static str,
}

const LOCKED_PATTERNS: &[Pattern] = &[
    Pattern {
        snapshot_key: "branch_namespace_const_sites",
        // Trailing `:` so we match only `const BRANCH_NAMESPACE: <ty> = ...`
        // declarations, not test fixtures named `BRANCH_NAMESPACE_BYTES`.
        needle: "const BRANCH_NAMESPACE:",
        rationale: "B2 will collapse the duplicated BRANCH_NAMESPACE consts \
                    (engine + executor) into one canonical derivation in strata_core.",
    },
    Pattern {
        snapshot_key: "merge_base_override_occurrences",
        needle: "merge_base_override",
        rationale: "B3 removes merge_base_override when lineage moves into the \
                    engine-owned BranchControlStore. Each occurrence is a callsite \
                    or signature B3 will rewrite.",
    },
    Pattern {
        snapshot_key: "merge_strategy_lastwriterwins_literals",
        needle: "MergeStrategy::LastWriterWins",
        rationale: "B5 renames merge policy to match runtime behavior \
                    (Lockstep Set 5). Each literal is a callsite to update.",
    },
    Pattern {
        snapshot_key: "branch_id_random_new_sites",
        needle: "BranchId::new()",
        rationale: "Random branch-id construction is incompatible with B3's \
                    generation-safe BranchRef. Many of these are test fixtures \
                    that will need to migrate to deterministic ids.",
    },
];

// =============================================================================
// Snapshot
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    schema_version: u32,
    description: String,
    /// Pattern key → count.
    shapes: BTreeMap<String, u64>,
}

fn snapshot_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
        .join("branching_transitional_shapes.json")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// =============================================================================
// Walker
// =============================================================================

/// Directories to skip entirely. Match on directory name only.
const SKIP_DIR_NAMES: &[&str] = &[
    "target",
    "benchmarks",
    ".git",
    ".claude",
    "node_modules",
    "dist",
    ".venv",
    "venv",
    ".pytest_cache",
    "__pycache__",
];

/// Files to skip. The guardrail file itself contains the patterns it
/// searches for, so it must not be self-counted.
const SKIP_FILE_NAMES: &[&str] = &["branching_guardrails.rs"];

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else { continue };
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if ft.is_dir() {
            if SKIP_DIR_NAMES.contains(&name) {
                continue;
            }
            collect_rs_files(&path, out)?;
        } else if ft.is_file()
            && path.extension().and_then(|e| e.to_str()) == Some("rs")
            && !SKIP_FILE_NAMES.contains(&name)
        {
            out.push(path);
        }
    }
    Ok(())
}

/// Count substring occurrences across all `.rs` files under the workspace
/// root, line by line (so multi-occurrence lines count multiply).
fn measure_pattern(needle: &str) -> u64 {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files).expect("walk workspace");
    let mut total = 0u64;
    for path in files {
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        for line in content.lines() {
            // Multiple occurrences on one line still count multiply —
            // this catches macro-expanded patterns that might reuse
            // a needle several times in a single physical line.
            let mut idx = 0;
            while let Some(pos) = line[idx..].find(needle) {
                total += 1;
                idx += pos + needle.len();
            }
        }
    }
    total
}

fn measure_all() -> BTreeMap<String, u64> {
    LOCKED_PATTERNS
        .iter()
        .map(|p| (p.snapshot_key.to_string(), measure_pattern(p.needle)))
        .collect()
}

// =============================================================================
// Test
// =============================================================================

/// Capture-or-assert: `STRATA_GUARDRAIL_CAPTURE=1` writes a fresh snapshot
/// and exits; otherwise, the test asserts the current measurements match
/// the committed snapshot.
#[test]
fn branching_transitional_shapes_match_snapshot() {
    let measured = measure_all();
    let path = snapshot_path();

    if std::env::var("STRATA_GUARDRAIL_CAPTURE").is_ok() {
        let snapshot = Snapshot {
            schema_version: 1,
            description: "Locked counts of branching transitional shapes. \
                          See tests/integration/branching_guardrails.rs for \
                          what each key tracks and why."
                .to_string(),
            shapes: measured.clone(),
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("mkdir snapshot dir");
        }
        let json = serde_json::to_string_pretty(&snapshot).expect("serialize snapshot") + "\n";
        fs::write(&path, json).expect("write snapshot");
        eprintln!(
            "STRATA_GUARDRAIL_CAPTURE: wrote snapshot to {}",
            path.display()
        );
        for p in LOCKED_PATTERNS {
            eprintln!("  {} = {}", p.snapshot_key, measured[p.snapshot_key]);
        }
        return;
    }

    let raw = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "Branching guardrail snapshot missing at {}: {e}\n\
             First-time setup: run with STRATA_GUARDRAIL_CAPTURE=1 to create it.",
            path.display()
        )
    });
    let snapshot: Snapshot = serde_json::from_str(&raw).expect("parse snapshot");

    let mut drift: Vec<String> = Vec::new();
    for pattern in LOCKED_PATTERNS {
        let key = pattern.snapshot_key;
        let actual = *measured.get(key).unwrap_or(&0);
        let expected = *snapshot.shapes.get(key).unwrap_or(&0);
        if actual != expected {
            drift.push(format!(
                "  {key}: expected {expected}, found {actual}\n    why we track it: {rationale}",
                rationale = pattern.rationale
            ));
        }
    }

    if !drift.is_empty() {
        let drift_msg = drift.join("\n");
        panic!(
            "Branching guardrail drift detected.\n\n\
             {drift_msg}\n\n\
             If this is intentional cleanup or an acknowledged addition, update\n\
             the snapshot at {} by running:\n\
             \n  STRATA_GUARDRAIL_CAPTURE=1 cargo test -p stratadb --test integration \\\n    \
                 branching_guardrails::branching_transitional_shapes_match_snapshot\n\
             \n\
             Then commit the updated JSON. The snapshot is the locked baseline.",
            path.display()
        );
    }
}

/// Sanity: the snapshot file declares `schema_version` 1.
#[test]
fn snapshot_schema_version_is_one() {
    let Ok(raw) = fs::read_to_string(snapshot_path()) else {
        return; // first-time setup; main test panics with capture instructions
    };
    let snapshot: Snapshot = serde_json::from_str(&raw).expect("parse snapshot");
    assert_eq!(snapshot.schema_version, 1);
}

/// Sanity: every locked pattern has an entry in the snapshot. Catches
/// the case where someone adds a pattern without bumping the snapshot.
#[test]
fn snapshot_covers_every_locked_pattern() {
    let Ok(raw) = fs::read_to_string(snapshot_path()) else {
        return; // first-time setup
    };
    let snapshot: Snapshot = serde_json::from_str(&raw).expect("parse snapshot");
    for pattern in LOCKED_PATTERNS {
        assert!(
            snapshot.shapes.contains_key(pattern.snapshot_key),
            "snapshot at {} is missing key {:?}.\n\
             Either bump the snapshot via STRATA_GUARDRAIL_CAPTURE=1 or remove\n\
             the pattern from LOCKED_PATTERNS.",
            snapshot_path().display(),
            pattern.snapshot_key
        );
    }
}
