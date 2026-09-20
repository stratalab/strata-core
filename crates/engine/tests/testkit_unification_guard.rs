//! #3290: engine's `testkit` seams must never unify into default workspace
//! builds. With resolver 2, a DEV-DEPENDENCY feature declared anywhere in
//! the workspace (`strata-executor = { features = ["testkit"] }` as a
//! dev-dep, say) flips `strata-engine/testkit` on for every `cargo test
//! --workspace` and `cargo clippy --workspace` — engine's test seams
//! silently become part of every crate's build, and the dedicated testkit
//! lanes stop meaning anything. S2b hit exactly this and backed it out into
//! named features with explicit lanes; this guard keeps it backed out.

#![deny(unsafe_code)]

use std::path::Path;

/// The issue's diagnostic, which must print nothing: any line naming the
/// engine `testkit` feature in the workspace-wide reverse feature tree is a
/// manifest-declared unification. The version is pinned because an old
/// `strata-engine` also sits in the lock via the benchmarks git deps, and
/// the unversioned spec is ambiguous.
#[test]
fn engine_testkit_never_unifies_into_default_workspace_builds() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/engine");
    let spec = format!("strata-engine@{}", env!("CARGO_PKG_VERSION"));
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--locked",
            "--workspace",
            "-e",
            "features",
            "-i",
            &spec,
        ])
        .current_dir(root)
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let unified: Vec<&str> = stdout
        .lines()
        .filter(|line| line.contains("strata-engine feature \"testkit\""))
        .collect();
    assert!(
        unified.is_empty(),
        "strata-engine/testkit is unified into default workspace builds — a \
         dev-dep feature declaration must move to a named feature with its \
         own explicit lane (#3290):\n{}",
        unified.join("\n")
    );
}
