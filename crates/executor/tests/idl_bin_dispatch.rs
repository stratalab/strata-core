//! The `strata-idl` bin dispatch for the generation verbs, exercised
//! hermetically: `STRATA_IDL_REPO_ROOT` points the bin at a scratch copy of
//! the IDL tree, so `generate` and `generate-tests` never write into the real
//! repository. Kills the match-arm mutants `cargo test` alone cannot see
//! (a deleted arm falls through to the unknown-verb exit 2).
#![cfg(all(feature = "idl-tooling", feature = "inference", feature = "testkit"))]

use std::path::{Path, PathBuf};
use std::process::Command;

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).expect("scratch mkdir");
    for entry in std::fs::read_dir(source).expect("scratch read_dir") {
        let entry = entry.expect("scratch entry");
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("scratch file_type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("scratch copy");
        }
    }
}

/// A scratch copy of everything the bin reads: the IDL tree, the fixtures,
/// and the enum sources the resolver scans for variant coverage. Returns the
/// real repo root beside it for restoring single files.
fn scratch_tree() -> (PathBuf, tempfile::TempDir) {
    let real = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("executor lives under crates/")
        .to_path_buf();
    let scratch = tempfile::tempdir().expect("scratch root");
    for relative in ["crates/executor/idl/v1", "crates/executor/tests/fixtures"] {
        copy_tree(&real.join(relative), &scratch.path().join(relative));
    }
    let src = scratch.path().join("crates/executor/src");
    std::fs::create_dir_all(&src).expect("scratch src dir");
    for file in ["command.rs", "output.rs"] {
        std::fs::copy(real.join("crates/executor/src").join(file), src.join(file))
            .expect("copy enum source");
    }
    (real, scratch)
}

fn run(scratch: &Path, verb: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_strata-idl"))
        .arg(verb)
        .env("STRATA_IDL_REPO_ROOT", scratch)
        .output()
        .expect("run strata-idl")
}

#[test]
fn the_test_generation_verbs_dispatch_and_the_unknown_verb_refuses() {
    let (_, scratch) = scratch_tree();
    let generated = scratch
        .path()
        .join("crates/executor/tests/generated/conformance_cases.rs");
    std::fs::create_dir_all(generated.parent().expect("parent")).expect("mkdir generated");
    std::fs::write(&generated, "// bogus\n").expect("seed bogus file");

    // Stale scratch file: `check-tests` dispatches and reports failure.
    let stale = run(scratch.path(), "check-tests");
    assert_eq!(stale.status.code(), Some(1), "stale check must exit 1");

    // `generate-tests` dispatches and repairs the scratch copy...
    let generate = run(scratch.path(), "generate-tests");
    assert!(
        generate.status.success(),
        "generate-tests failed: {}",
        String::from_utf8_lossy(&generate.stderr)
    );
    // ...after which `check-tests` passes.
    let fresh = run(scratch.path(), "check-tests");
    assert!(
        fresh.status.success(),
        "fresh check failed: {}",
        String::from_utf8_lossy(&fresh.stderr)
    );

    // A verb the dispatch does not know exits 2 (the usage error) — the
    // failure mode a deleted match arm would collapse real verbs into.
    let unknown = run(scratch.path(), "definitely-not-a-verb");
    assert_eq!(unknown.status.code(), Some(2), "unknown verb must exit 2");
}

/// `generate` is the writer behind the SDK index and every schema document,
/// and `check` is the gate that reads them back. Corrupt both in the scratch
/// copy: the verb must repair them, so a `generate` that returns without
/// writing leaves the gate red.
#[test]
fn generate_rewrites_the_index_and_the_schema_documents() {
    let (_, scratch) = scratch_tree();
    let generated = scratch.path().join("crates/executor/idl/v1/generated");
    let index = generated.join("command-index.json");
    let schema = generated.join("schemas/kv.get.json");
    std::fs::write(&index, "{}\n").expect("corrupt the index");
    std::fs::remove_file(&schema).expect("drop a schema document");

    let stale = run(scratch.path(), "check");
    assert_eq!(
        stale.status.code(),
        Some(1),
        "a corrupt index must fail check"
    );

    let generate = run(scratch.path(), "generate");
    assert!(
        generate.status.success(),
        "generate failed: {}",
        String::from_utf8_lossy(&generate.stderr)
    );
    assert!(
        schema.is_file(),
        "generate must rewrite the dropped document"
    );
    assert_ne!(
        std::fs::read_to_string(&index).expect("read the index"),
        "{}\n",
        "generate must rewrite the index"
    );

    let fresh = run(scratch.path(), "check");
    assert!(
        fresh.status.success(),
        "fresh check failed: {}",
        String::from_utf8_lossy(&fresh.stderr)
    );
}

/// Bump the `budget:` line in a debt-ledger YAML by one, so its budget no longer
/// equals its entry count.
fn bump_budget(path: &Path) {
    let text = std::fs::read_to_string(path).expect("read debt yaml");
    let bumped: Vec<String> = text
        .lines()
        .map(|line| match line.strip_prefix("budget: ") {
            Some(n) => {
                let n: usize = n.trim().parse().expect("budget is a usize");
                format!("budget: {}", n + 1)
            }
            None => line.to_owned(),
        })
        .collect();
    std::fs::write(path, format!("{}\n", bumped.join("\n"))).expect("write debt yaml");
}

/// W0b call-site coverage: the debt-count budget gates must actually fire when a
/// ledger's `budget` no longer matches its entry count. On the CONSISTENT real
/// tree a disabled gate is indistinguishable from a live one, so this feeds
/// INCONSISTENT data through the real binary — `check` runs the replay-skip
/// ratchet (via `resolve_index`) and `verify-fixtures` runs the error-replay
/// coverage guard, so each budget is exercised through its own lane. Kills a
/// call-site mutant that drops or neuters either `enforce_debt_budget` call.
#[test]
fn the_debt_budget_gates_reject_a_count_mismatch() {
    let (real, scratch) = scratch_tree();

    let idl = scratch.path().join("crates/executor/idl/v1");
    let skip_yaml = idl.join("replay-skipped-commands.yaml");
    let unreplayed_yaml = idl.join("unreplayed-error-codes.yaml");
    let real_skip = real.join("crates/executor/idl/v1/replay-skipped-commands.yaml");

    // Clean scratch: both gates pass — proves the copied tree is valid and the
    // committed budgets already equal their counts.
    assert!(
        run(scratch.path(), "check").status.success(),
        "clean check must pass"
    );
    assert!(
        run(scratch.path(), "verify-fixtures").status.success(),
        "clean verify-fixtures must pass"
    );

    // Skip-list budget above its count -> `check` must reject.
    bump_budget(&skip_yaml);
    let skip = run(scratch.path(), "check");
    assert_eq!(
        skip.status.code(),
        Some(1),
        "a replay-skip budget mismatch must fail check"
    );
    assert!(
        String::from_utf8_lossy(&skip.stderr).contains("budget"),
        "check must name the budget: {}",
        String::from_utf8_lossy(&skip.stderr)
    );
    // Restore the skip ledger so it does not confound the unreplayed check.
    std::fs::copy(&real_skip, &skip_yaml).expect("restore skip yaml");

    // Unreplayed budget above its count -> `verify-fixtures` must reject.
    bump_budget(&unreplayed_yaml);
    let unreplayed = run(scratch.path(), "verify-fixtures");
    assert_eq!(
        unreplayed.status.code(),
        Some(1),
        "an unreplayed-error-code budget mismatch must fail verify-fixtures"
    );
    assert!(
        String::from_utf8_lossy(&unreplayed.stderr).contains("budget"),
        "verify-fixtures must name the budget: {}",
        String::from_utf8_lossy(&unreplayed.stderr)
    );
}
