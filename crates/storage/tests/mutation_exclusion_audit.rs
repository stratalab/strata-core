//! Guards the decision logic of `scripts/verify_mutation_exclusions.py` (#3220).
//!
//! That script is the CI gate that turns a mutation lane's "not compiled in this
//! lane" `exclude_glob` justifications into a checked fact: it builds each lane
//! with `cargo check --tests` and fails a `crates/*/src/**` glob that excludes a
//! file the lane actually compiled, unless the glob is on a reasoned allowlist
//! of deliberate compiled-code exclusions. The real per-lane builds run in the
//! dedicated `mutation-exclusion-audit` CI job; this test runs the script's
//! `--self-test`, which exercises the classification (not compiled / partition /
//! allowlisted / unjustified-compiled) and the glob matcher on fabricated
//! inputs, so a logic regression reds `cargo test` here and not only in that
//! job. Same pattern as `mutation_gate_verdict.rs` running `mutation-verdict.sh`.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    // crates/storage -> repo root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/storage has a repo root two levels up")
        .to_path_buf()
}

#[test]
#[cfg_attr(miri, ignore = "spawns python3; Miri cannot spawn processes")]
fn exclusion_audit_self_test_passes() {
    let root = repo_root();
    let script = root.join("scripts/verify_mutation_exclusions.py");
    assert!(
        script.exists(),
        "verify_mutation_exclusions.py must exist at {}",
        script.display()
    );

    let output = Command::new("python3")
        .arg(&script)
        .arg("--self-test")
        .current_dir(&root)
        .output()
        .expect("python3 should run the exclusion-audit self-test");

    assert!(
        output.status.success(),
        "exclusion-audit self-test failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
