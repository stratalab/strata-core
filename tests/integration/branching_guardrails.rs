//! Inventory test that counts known branch-structure patterns across the
//! workspace and compares them against a locked snapshot.
//!
//! ## How it works
//!
//! For each pattern in `LOCKED_PATTERNS`, the test walks production `*.rs`
//! files under `crates/**/src/**` and `src/**` (skipping `tests/`,
//! `benches/`, `target/`, `benchmarks/`, `.git/`, `.claude/`) and counts
//! substring matches in code, not comments or `#[cfg(test)]` modules that
//! live inside `src/` files. The totals are compared against the snapshot at
//! `tests/integration/data/branching_shape_inventory.json`.
//!
//! Drift in either direction fails the test:
//!
//! - **count went up**  → someone added a new violation; either fix it or
//!   bump the snapshot if the addition is intentional and acknowledged.
//! - **count went down** → someone reduced the pattern count;
//!   bump the snapshot so the new lower count becomes the floor.
//!
//! On first capture, set `STRATA_GUARDRAIL_CAPTURE=1` and run the test —
//! it writes the snapshot from observation rather than asserting against
//! it. Commit the resulting JSON.
//!
//! ## What's tracked today
//!
//! - `BRANCH_NAMESPACE` const sites — expected to remain at a single
//!   canonical declaration.
//! - `merge_base_override` occurrences — explicit merge-base override wiring.
//! - `MergeStrategy::LastWriterWins` literals — runtime-visible merge policy
//!   naming sites.
//! - `BranchId::new()` random-construction sites — useful for tracking where
//!   tests or helpers create fresh branch ids.
//!
//! These are intentionally **production-code** tripwires. Characterization
//! tests cover public behavior; this guardrail measures implementation
//! patterns, so comment text and test-only references must not move the baseline.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

// =============================================================================
// Patterns
// =============================================================================

/// One tracked branch-structure pattern.
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
        rationale: "The codebase should keep a single canonical \
                    BRANCH_NAMESPACE declaration. The floor of 1 catches any \
                    regression that reintroduces a second site.",
    },
    Pattern {
        snapshot_key: "merge_base_override_occurrences",
        needle: "merge_base_override",
        rationale: "Each occurrence marks a callsite or signature carrying an \
                    explicit merge-base override.",
    },
    Pattern {
        snapshot_key: "merge_strategy_lastwriterwins_literals",
        needle: "MergeStrategy::LastWriterWins",
        rationale: "Each literal marks a callsite that depends on the \
                    current merge-policy name.",
    },
    Pattern {
        snapshot_key: "branch_id_random_new_sites",
        needle: "BranchId::new()",
        rationale: "Random branch-id construction is worth tracking because \
                    many of these sites are fixtures or helpers rather than \
                    data-driven identifiers.",
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
        .join("branching_shape_inventory.json")
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
        } else if ft.is_file() && is_production_rs_file(&path) && !SKIP_FILE_NAMES.contains(&name) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_production_rs_file(path: &Path) -> bool {
    if path.extension().and_then(|e| e.to_str()) != Some("rs") {
        return false;
    }

    let components: Vec<&str> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();

    let has_src = components.contains(&"src");
    let in_tests = components.contains(&"tests");
    let in_benches = components.contains(&"benches");

    has_src && !in_tests && !in_benches
}

fn strip_comments_from_line<'a>(line: &'a str, in_block_comment: &mut bool) -> Option<&'a str> {
    let mut rest = line.trim_start();

    loop {
        if *in_block_comment {
            if let Some(end) = rest.find("*/") {
                rest = &rest[end + 2..];
                *in_block_comment = false;
                rest = rest.trim_start();
                continue;
            }
            return None;
        }

        if rest.is_empty() || rest.starts_with("//") {
            return None;
        }

        if rest.starts_with("/*") {
            rest = &rest[2..];
            *in_block_comment = true;
            continue;
        }

        if let Some(start) = rest.find("/*") {
            let prefix = &rest[..start];
            if prefix.trim().is_empty() {
                rest = &rest[start + 2..];
                *in_block_comment = true;
                continue;
            }
            return Some(prefix.trim_end());
        }

        if let Some(start) = rest.find("//") {
            let prefix = &rest[..start];
            if prefix.trim().is_empty() {
                return None;
            }
            return Some(prefix.trim_end());
        }

        return Some(rest);
    }
}

fn brace_delta(line: &str) -> isize {
    let opens = line.chars().filter(|c| *c == '{').count() as isize;
    let closes = line.chars().filter(|c| *c == '}').count() as isize;
    opens - closes
}

/// Count substring occurrences across a Rust source string while ignoring
/// comment-only text and `#[cfg(test)] mod ...` blocks that live in `src/`.
fn count_pattern_in_source(needle: &str, content: &str) -> u64 {
    let mut total = 0u64;
    let mut in_block_comment = false;
    let mut pending_cfg_test_module = false;
    let mut skipped_test_module_depth: Option<isize> = None;

    for line in content.lines() {
        let Some(code) = strip_comments_from_line(line, &mut in_block_comment) else {
            continue;
        };
        let trimmed = code.trim_start();

        if let Some(depth) = &mut skipped_test_module_depth {
            *depth += brace_delta(code);
            if *depth <= 0 {
                skipped_test_module_depth = None;
            }
            continue;
        }

        if pending_cfg_test_module {
            if trimmed.starts_with("#[") {
                continue;
            }
            if looks_like_module_start(trimmed) {
                let depth = brace_delta(code);
                if depth > 0 {
                    skipped_test_module_depth = Some(depth);
                }
                pending_cfg_test_module = false;
                continue;
            }
            pending_cfg_test_module = false;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test_module = true;
            continue;
        }

        // Multiple occurrences on one line still count multiply —
        // this catches macro-expanded patterns that might reuse
        // a needle several times in a single physical line.
        let mut idx = 0;
        while let Some(pos) = code[idx..].find(needle) {
            total += 1;
            idx += pos + needle.len();
        }
    }

    total
}

fn looks_like_module_start(line: &str) -> bool {
    (line.starts_with("mod ") || line.starts_with("pub ") || line.starts_with("pub(crate) "))
        && line.contains("mod ")
        && line.contains('{')
}

/// Count substring occurrences across production `.rs` files under the
/// workspace root, ignoring comment-only text and `#[cfg(test)]` modules that
/// live in `src/`. Multiple occurrences on one code line still count multiply.
fn measure_pattern(needle: &str) -> u64 {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files).expect("walk workspace");
    let mut total = 0u64;
    for path in files {
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        total += count_pattern_in_source(needle, &content);
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
fn branching_shape_inventory_matches_snapshot() {
    let measured = measure_all();
    let path = snapshot_path();

    if std::env::var("STRATA_GUARDRAIL_CAPTURE").is_ok() {
        let snapshot = Snapshot {
            schema_version: 1,
            description: "Locked counts of branching shape inventory. \
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
             If this is an expected reduction or an acknowledged addition, update\n\
             the snapshot at {} by running:\n\
             \n  STRATA_GUARDRAIL_CAPTURE=1 cargo test -p stratadb --test integration \\\n    \
                 branching_guardrails::branching_shape_inventory_matches_snapshot\n\
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

#[test]
fn production_file_filter_excludes_tests_and_benches() {
    assert!(is_production_rs_file(Path::new(
        "crates/executor/src/bridge.rs"
    )));
    assert!(!is_production_rs_file(Path::new(
        "tests/integration/branching_guardrails.rs"
    )));
    assert!(!is_production_rs_file(Path::new(
        "crates/engine/benches/transaction_benchmarks.rs"
    )));
}

#[test]
fn strip_comments_ignores_comment_only_occurrences() {
    let mut in_block = false;

    assert_eq!(
        strip_comments_from_line("let x = merge_base_override;", &mut in_block),
        Some("let x = merge_base_override;")
    );
    assert_eq!(
        strip_comments_from_line("// merge_base_override", &mut in_block),
        None
    );
    assert_eq!(
        strip_comments_from_line("let x = 1; // merge_base_override", &mut in_block),
        Some("let x = 1;")
    );

    assert_eq!(
        strip_comments_from_line("/* merge_base_override", &mut in_block),
        None
    );
    assert!(in_block);
    assert_eq!(
        strip_comments_from_line(
            "still comment */ let y = merge_base_override;",
            &mut in_block
        ),
        Some("let y = merge_base_override;")
    );
    assert!(!in_block);
}

#[test]
fn cfg_test_modules_do_not_contribute_to_pattern_counts() {
    let content = r#"
fn production_site() {
    let _ = BranchId::new();
}

#[cfg(test)]
mod tests {
    fn helper() {
        let _ = BranchId::new();
    }
}
"#;

    assert_eq!(count_pattern_in_source("BranchId::new()", content), 1);
}

#[test]
fn cfg_test_modules_with_extra_attributes_are_skipped() {
    let content = r#"
fn production_site() {
    let _ = BranchId::new();
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    fn helper() {
        let _ = BranchId::new();
    }
}
"#;

    assert_eq!(count_pattern_in_source("BranchId::new()", content), 1);
}

// =============================================================================
// B4.3 tripwire: branch_ops low-level mutation surface is empty
// =============================================================================

/// Known `pub fn` items in `crates/engine/src/branch_ops/mod.rs` that are
/// deliberately left on the public surface. Read-only diff / merge-base /
/// annotation query helpers plus storage-maintenance (`materialize_branch`)
/// are the full allow-list. Every other `pub fn` in that file must go
/// through `BranchService`.
const BRANCH_OPS_PUB_FN_ALLOWLIST: &[&str] = &[
    "diff_branches",
    "diff_branches_with_options",
    "diff_three_way",
    "get_merge_base",
    "get_notes",
    "list_tags",
    "materialize_branch",
    "resolve_tag",
];

/// Low-level branch mutation helpers that must never be re-exposed on the
/// engine crate's public surface. B4.3 tightens these to `pub(crate)` and
/// routes external callers through `BranchService`.
const FORBIDDEN_BRANCH_OPS_EXPORT_NAMES: &[&str] = &[
    "fork_branch",
    "fork_branch_with_metadata",
    "merge_branches",
    "merge_branches_with_metadata",
    "revert_version_range",
    "revert_version_range_with_metadata",
    "cherry_pick_keys",
    "cherry_pick_from_diff",
    "create_tag",
    "delete_tag",
    "add_note",
    "delete_note",
];

fn branch_ops_mod_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("crates")
        .join("engine")
        .join("src")
        .join("branch_ops")
        .join("mod.rs")
}

fn engine_lib_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("crates")
        .join("engine")
        .join("src")
        .join("lib.rs")
}

/// Extract the identifier after `pub fn ` on a code line, if any. Returns
/// `None` for non-matching lines or lines that are inside a comment (those
/// are already stripped by `strip_comments_from_line`).
fn parse_pub_fn_name(code: &str) -> Option<&str> {
    let after = code.strip_prefix("pub fn ")?;
    let end = after
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(after.len());
    if end == 0 {
        None
    } else {
        Some(&after[..end])
    }
}

/// B4.3 tripwire. After bypass collapse, every `pub fn` in
/// `crates/engine/src/branch_ops/mod.rs` must either be a declared
/// read-only helper on the allow-list or be tightened to `pub(crate)`.
/// Any new unlisted `pub fn` is a regression — either move the callsite
/// to `BranchService` or bump the allow-list with justification.
#[test]
fn branch_ops_pub_mutation_surface_is_empty() {
    let path = branch_ops_mod_path();
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    let mut in_block_comment = false;
    let mut violations: Vec<String> = Vec::new();

    for (lineno, line) in src.lines().enumerate() {
        let Some(code) = strip_comments_from_line(line, &mut in_block_comment) else {
            continue;
        };
        let Some(name) = parse_pub_fn_name(code) else {
            continue;
        };
        if !BRANCH_OPS_PUB_FN_ALLOWLIST.contains(&name) {
            violations.push(format!(
                "  {}:{}: `pub fn {name}` is not on the branch_ops allow-list",
                path.display(),
                lineno + 1,
            ));
        }
    }

    if !violations.is_empty() {
        let joined = violations.join("\n");
        panic!(
            "B4.3 tripwire: unlisted public mutation surface in branch_ops/mod.rs:\n\n\
             {joined}\n\n\
             If this is a new read-only helper, add its name to\n\
             `BRANCH_OPS_PUB_FN_ALLOWLIST` in\n\
             tests/integration/branching_guardrails.rs.\n\
             Otherwise move the callers to BranchService and tighten the\n\
             helper to `pub(crate)`."
        );
    }
}

/// B4.3 re-export audit. The crate root may still re-export branch-op types
/// such as `ForkInfo` or `MergeInfo`, but it must not re-expose the low-level
/// mutation helpers themselves. This complements the `pub fn` scan above by
/// catching `pub use branch_ops::...` regressions at the engine crate root.
#[test]
fn branch_ops_mutators_are_not_reexported_at_crate_root() {
    let path = engine_lib_path();
    let src = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

    let mut in_block_comment = false;
    let mut in_branch_ops_reexport = false;
    let mut violations: Vec<String> = Vec::new();

    for (lineno, line) in src.lines().enumerate() {
        let Some(code) = strip_comments_from_line(line, &mut in_block_comment) else {
            continue;
        };

        if code.contains("pub use branch_ops") {
            in_branch_ops_reexport = true;
        }

        if !in_branch_ops_reexport {
            continue;
        }

        for name in FORBIDDEN_BRANCH_OPS_EXPORT_NAMES {
            if code.contains(name) {
                violations.push(format!(
                    "  {}:{}: crate root re-exports forbidden branch_ops helper `{name}`",
                    path.display(),
                    lineno + 1,
                ));
            }
        }

        if code.contains(';') {
            in_branch_ops_reexport = false;
        }
    }

    if !violations.is_empty() {
        let joined = violations.join("\n");
        panic!(
            "B4.3 tripwire: forbidden branch_ops helper re-export at crate root:\n\n\
             {joined}\n\n\
             Remove the public re-export or route callers through BranchService."
        );
    }
}

#[test]
fn parse_pub_fn_name_extracts_identifier() {
    assert_eq!(
        parse_pub_fn_name("pub fn diff_branches("),
        Some("diff_branches")
    );
    assert_eq!(
        parse_pub_fn_name("pub fn materialize_branch(db: &Arc<Database>) {"),
        Some("materialize_branch")
    );
    assert_eq!(
        parse_pub_fn_name("pub(crate) fn fork_branch_with_metadata"),
        None
    );
    assert_eq!(parse_pub_fn_name("fn private_helper()"), None);
    assert_eq!(parse_pub_fn_name("pub fn "), None);
}
