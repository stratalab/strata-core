//! The license text must travel with every binary we distribute.
//!
//! The workspace is Apache-2.0 and `LICENSE` sits at the repo root, but the
//! release's package step shipped `strata` and `README.md` and nothing else.
//! Every tarball from v1.0.0 to v1.2.2 went out without the license text
//! (#3010) — a redistribution term, not a nicety, and invisible because the
//! release pipeline was green the whole time: no job asserts what is *inside*
//! an archive.
//!
//! The failure has two halves, and fixing one without the other is silent. The
//! package step copies from a SPARSE checkout, so a `cp LICENSE staging/`
//! whose file was never fetched fails the job, while a `tar` that lists
//! `LICENSE` without the copy packages nothing. This guard pins both, plus the
//! file's existence, so the three cannot drift apart.
//!
//! Co-located with the other program-level guards (`mutation_partition_guard`,
//! `testing_charter_guard`) in the storage test tree, which is the program's
//! home crate.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

fn release_workflow() -> String {
    let path = repo_root().join(".github/workflows/release.yml");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// The one line that decides what a downloaded tarball contains.
fn tar_command(workflow: &str) -> &str {
    workflow
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("tar czf"))
        .expect("the release packages each target with a `tar czf` line")
}

#[test]
fn the_release_tarball_carries_the_license() {
    let workflow = release_workflow();

    assert!(
        repo_root().join("LICENSE").is_file(),
        "LICENSE is missing from the repo root, so nothing can package it"
    );

    let tar = tar_command(&workflow);
    assert!(
        tar.split_whitespace().any(|word| word == "LICENSE"),
        "the release tarball does not list LICENSE, so an Apache-2.0 binary \
         ships without its license text (#3010): {tar}"
    );

    assert!(
        workflow.contains("cp LICENSE staging/"),
        "LICENSE is listed in the tar but never staged, so packaging fails or \
         omits it"
    );
}

/// The package step checks out sparsely, so a staged file the checkout never
/// fetched is a broken release job rather than a missing file in the archive.
/// Any path that step copies has to appear in its sparse list.
///
/// Scoped to that one step on purpose: the `release` job later copies
/// `CHANGELOG.md` under a FULL checkout, and a whole-file scan reads that as a
/// violation. The first draft of this guard did exactly that.
#[test]
fn every_staged_file_is_fetched_by_the_sparse_checkout() {
    let workflow = release_workflow();
    let start = workflow
        .find("sparse-checkout: |")
        .expect("the package job checks out sparsely");
    let end = workflow[start..]
        .find("- name: Upload packaged artifact")
        .map(|offset| start + offset)
        .expect("the package job uploads what it packaged");
    let package_job = &workflow[start..end];

    let sparse: Vec<&str> = package_job
        .lines()
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with("sparse-checkout-cone-mode"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();

    let staged: Vec<&str> = package_job
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("cp "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect();

    assert!(
        !staged.is_empty(),
        "no `cp` into the staging directory found; this guard is watching nothing"
    );
    for file in staged {
        assert!(
            sparse.contains(&file),
            "the package step copies `{file}` but its sparse checkout does not \
             fetch it, so the release job would fail on a missing file. \
             Sparse list: {sparse:?}"
        );
    }
}

/// The step asserts its own output, rather than trusting the `tar` line.
#[test]
fn the_package_step_verifies_the_archive_it_built() {
    let workflow = release_workflow();
    assert!(
        workflow.contains("tar tzf \"${ARCHIVE_NAME}\" | grep -qx 'LICENSE'"),
        "the package step does not look inside the archive it just built, which \
         is why every release since v1.0.0 shipped without a license and the \
         pipeline stayed green (#3010)"
    );
}

/// Every target the release BUILDS is also PACKAGED.
///
/// The two are separate lists in one file — `build.strategy.matrix.include`
/// names a target per runner, `package.strategy.matrix.target` names them
/// again — so a target added to one and not the other compiles a binary that
/// is silently never shipped, or packages an artifact that was never built.
///
/// This is the in-repo half of the drift that made `install.sh` resolve six
/// targets while the release published three (#3011, #3060). The other half
/// lives in stratadb.org, which serves the installer; nothing here can see it,
/// which is precisely why the lists that ARE both here should be pinned.
#[test]
fn every_built_target_is_packaged() {
    let workflow = release_workflow();

    let built: std::collections::BTreeSet<&str> = workflow
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("- target: "))
        .collect();

    let package_start = workflow
        .find("  package:")
        .expect("the release has a package job");
    let packaged: std::collections::BTreeSet<&str> = workflow[package_start..]
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("target:"))
        .skip(1)
        .take_while(|line| line.trim_start().starts_with("- "))
        .map(|line| line.trim().trim_start_matches("- "))
        .collect();

    assert!(
        !built.is_empty() && !packaged.is_empty(),
        "no targets parsed from one of the matrices; this guard is watching nothing \
         (built: {built:?}, packaged: {packaged:?})"
    );
    assert_eq!(
        built, packaged,
        "the build and package target matrices disagree. A target built but not \
         packaged ships nothing; a target packaged but not built fails the job. \
         built: {built:?}, packaged: {packaged:?}"
    );
}

/// Every placeholder in the Homebrew formula template has a substitution.
///
/// The template is Ruby with `SHA_*_PLACEHOLDER` tokens that a later `sed`
/// replaces. Adding a platform block without its `sed` line ships a formula
/// whose `sha256` is the literal word `SHA_INTEL64_PLACEHOLDER`, and
/// `brew install` fails for everyone on that platform — exactly the mistake
/// available while adding the Intel-Mac block (#3011).
#[test]
fn every_formula_placeholder_is_substituted() {
    let workflow = release_workflow();

    let declared: std::collections::BTreeSet<&str> = workflow
        .match_indices("_PLACEHOLDER")
        .filter_map(|(at, _)| {
            let before = &workflow[..at];
            // Uppercase and underscore only: the tokens are `SHA_*` and
            // `VERSION`, and the URL writes `vVERSION_PLACEHOLDER`, whose
            // lowercase `v` is part of the tag rather than the token.
            let start = before
                .rfind(|c: char| !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))?;
            Some(&workflow[start + 1..at + "_PLACEHOLDER".len()])
        })
        .collect();

    assert!(
        declared.len() >= 4,
        "expected the formula's version and per-platform placeholders, found {declared:?}"
    );
    for placeholder in declared {
        let substitution = format!("s/{placeholder}/");
        assert!(
            workflow.contains(&substitution),
            "`{placeholder}` appears in the formula template but no `sed` replaces it, \
             so the published formula would carry the literal token"
        );
    }
}
