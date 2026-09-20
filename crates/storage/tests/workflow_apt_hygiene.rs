//! #3272: an `apt-get update` that refreshes the runner image's third-party
//! repos fails the job on ANY of their transient index mismatches (the
//! chrome-stable Hash Sum failure cost a perf-gates cycle; on release.yml
//! it would cost a tag). Every package we install is in Ubuntu
//! main/universe, so updates must refresh only the Ubuntu archives — via
//! `.github/actions/apt-install`, or inline with `sources.list.d` disabled
//! where a composite action cannot reach (release.yml's shell branch).

#![deny(unsafe_code)]

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

#[test]
fn workflows_never_refresh_third_party_apt_repos() {
    let workflows = repo_root().join(".github").join("workflows");
    for entry in std::fs::read_dir(&workflows).expect("read workflows dir") {
        let path = entry.expect("workflow entry").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("yml") {
            continue;
        }
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} unreadable: {err}", path.display()));
        for (index, line) in body.lines().enumerate() {
            let unsafe_update =
                line.contains("apt-get update") && !line.contains("Dir::Etc::sourceparts=-");
            assert!(
                !unsafe_update,
                "{}:{}: `apt-get update` without `-o Dir::Etc::sourceparts=-` \
                 refreshes the image's third-party repos and inherits their \
                 flakes — use .github/actions/apt-install or the inline \
                 Ubuntu-archives-only shape (#3272): {line}",
                path.display(),
                index + 1
            );
        }
    }
    let action = repo_root().join(".github/actions/apt-install/action.yml");
    let body = std::fs::read_to_string(&action).expect("apt-install action exists");
    assert!(
        body.contains("Dir::Etc::sourceparts=-") && body.contains("Acquire::Retries=3"),
        "the apt-install action must keep the Ubuntu-archives-only update with retries"
    );
}
