//! A mutation lane's verdict comes from its artifacts, never from its exit code.
//!
//! cargo-mutants decides its exit code with timeout **ahead of** missed, so a
//! run that both hung on one mutant and left others alive reports exit 3 —
//! "timeout" — and says nothing about the survivors. The gate used to read that
//! as "timeout only, nothing missed" and pass, which meant any PR whose diff
//! carried a hanging mutant got a free pass on every mutant it missed (#3225).
//! Verified against cargo-mutants 27.1.0, the version
//! `taiki-e/install-action@cargo-mutants` installs: a run with 5 missed and 1
//! timeout exits 3.
//!
//! The survivors are in `mutants.out/missed.txt` whatever the exit code, so
//! that file is the authority and `scripts/mutation-verdict.sh` is the one
//! place that reads it. The tests below run that script against fabricated
//! artifact directories — they judge the decision itself, not a description of
//! it, which is the whole reason the decision does not live in the YAML.
//!
//! The second defect they pin is quieter. Nothing clears `mutants.out`: a run
//! that finds no mutants exits 0 and leaves the previous run's `missed.txt`
//! untouched. The three lanes share one directory, so without a clear between
//! them a lane inherits whatever the lane before it left behind.

#![deny(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// The workflow whose mutation step runs the lanes.
const WORKFLOW: &str = ".github/workflows/ci.yml";

/// The script that owns the pass/fail decision for one lane.
const VERDICT: &str = "scripts/mutation-verdict.sh";

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("{relative} unreadable: {err}"))
}

/// The shell body of the workflow's mutation step.
fn mutation_step() -> String {
    let workflow = read(WORKFLOW);
    workflow
        .split_once("- name: Mutants on the PR diff")
        .expect("the mutation step exists")
        .1
        .split_once("- name: Upload mutants report")
        .expect("the mutation step ends")
        .0
        .to_owned()
}

/// A fabricated `mutants.out`: `missed` and `timeout` are the file bodies, and
/// `None` means cargo-mutants never wrote that file at all.
struct Artifacts {
    directory: tempfile::TempDir,
}

impl Artifacts {
    fn new(missed: Option<&str>, timeout: Option<&str>) -> Self {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let out = directory.path().join("mutants.out");
        std::fs::create_dir_all(&out).expect("mutants.out is creatable");
        if let Some(body) = missed {
            std::fs::write(out.join("missed.txt"), body).expect("missed.txt is writable");
        }
        if let Some(body) = timeout {
            std::fs::write(out.join("timeout.txt"), body).expect("timeout.txt is writable");
        }
        Self { directory }
    }

    /// Run the verdict script over these artifacts, as the lane named `lane`
    /// with cargo-mutants having exited `code`. Returns (exit code, output).
    fn verdict(&self, lane: &str, code: i32) -> (i32, String) {
        let out = self.directory.path().join("mutants.out");
        let script = repo_root().join(VERDICT);
        let result = Command::new("bash")
            .arg(&script)
            .arg(lane)
            .arg(code.to_string())
            .arg(&out)
            .output()
            .unwrap_or_else(|err| panic!("{VERDICT} is runnable: {err}"));
        let mut text = String::from_utf8_lossy(&result.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&result.stderr));
        (
            result.status.code().expect("the script exited normally"),
            text,
        )
    }
}

/// Five missed mutants and one timeout — the shape measured on cargo-mutants
/// 27.1.0, and exactly the run that used to pass.
const MISSED_FIVE: &str = "src/lib.rs:3:5: replace untested -> i32 with 0\n\
                           src/lib.rs:3:5: replace untested -> i32 with 1\n\
                           src/lib.rs:3:5: replace untested -> i32 with -1\n\
                           src/lib.rs:3:7: replace + with - in untested\n\
                           src/lib.rs:3:7: replace + with * in untested\n";
const TIMEOUT_ONE: &str = "src/lib.rs:8:5: replace keep_going -> bool with true\n";

/// The regression this issue is about: exit 3 alongside survivors must fail.
#[test]
fn a_timeout_does_not_excuse_the_mutants_that_survived_beside_it() {
    let artifacts = Artifacts::new(Some(MISSED_FIVE), Some(TIMEOUT_ONE));
    let (code, output) = artifacts.verdict("b", 3);
    assert_eq!(
        code, 2,
        "cargo-mutants exited 3 with 5 mutants alive and the gate passed it. \
         Exit 3 means `at least one mutant hung`; it says nothing about missed, \
         because timeout is reported ahead of missed (#3225).\n{output}"
    );
    assert!(
        output.contains("replace + with * in untested"),
        "a failing lane must print the survivors — the reader should not have to \
         scroll the log to learn which mutants lived.\n{output}"
    );
}

/// The case the exit-3 allowance was actually written for, which must still pass.
#[test]
fn a_hang_with_nothing_alive_behind_it_is_not_a_failure() {
    let artifacts = Artifacts::new(None, Some(TIMEOUT_ONE));
    let (code, output) = artifacts.verdict("a", 3);
    assert_eq!(
        code, 0,
        "a mutant killed by hang was still detected; only its assertion is \
         missing. Failing here would make every slow test a red gate.\n{output}"
    );
    assert!(
        output.contains("replace keep_going"),
        "the hung mutant must be named, so a reviewer can judge whether `killed \
         by hang` is really true of it.\n{output}"
    );
}

/// An empty `missed.txt` is the same statement as an absent one.
#[test]
fn an_empty_missed_file_is_not_a_survivor() {
    let artifacts = Artifacts::new(Some(""), Some(TIMEOUT_ONE));
    let (code, output) = artifacts.verdict("a", 3);
    assert_eq!(code, 0, "an empty missed.txt names no survivor\n{output}");
}

#[test]
fn a_clean_run_passes() {
    let artifacts = Artifacts::new(None, None);
    let (code, output) = artifacts.verdict("a", 0);
    assert_eq!(
        code, 0,
        "exit 0 with no artifacts is a clean lane\n{output}"
    );
}

/// The path that already worked, and must keep working.
#[test]
fn survivors_without_a_hang_still_fail() {
    let artifacts = Artifacts::new(Some(MISSED_FIVE), None);
    let (code, output) = artifacts.verdict("c", 2);
    assert_eq!(
        code, 2,
        "exit 2 is cargo-mutants reporting survivors\n{output}"
    );
    assert!(
        output.contains("5 mutant"),
        "the count belongs in the error line\n{output}"
    );
}

/// A tool failure is not a verdict, and must not be read as one in either
/// direction — the lane judged nothing, so it cannot pass.
#[test]
fn a_tool_error_is_not_swallowed() {
    let artifacts = Artifacts::new(None, None);
    for code in [1, 4] {
        let (actual, output) = artifacts.verdict("a", code);
        assert_eq!(
            actual, code,
            "cargo-mutants exited {code} — a build or invocation failure. \
             Passing that would mean a broken lane reads as a clean one.\n{output}"
        );
    }
}

/// The workflow must not re-decide what the script decides.
#[test]
fn the_workflow_takes_its_verdict_from_the_script() {
    let step = mutation_step();
    assert!(
        step.contains(VERDICT),
        "the mutation step no longer calls {VERDICT}; the gate's decision has \
         moved back into the workflow, where no test can reach it."
    );
    for forbidden in ["-ne 3", "-eq 3"] {
        assert!(
            !step.contains(forbidden),
            "the mutation step tests the exit code against 3 ({forbidden}). \
             That is the #3225 bug: cargo-mutants reports timeout ahead of \
             missed, so exit 3 is also what a run with survivors returns. \
             The verdict belongs to {VERDICT}, which reads missed.txt."
        );
    }
}

/// Nothing clears `mutants.out`, and the lanes share it.
///
/// A lane whose slice holds no mutants exits 0 without touching the directory,
/// so the previous lane's `missed.txt` is still sitting there. Since the
/// verdict now reads that file, a lane must start from a directory that is its
/// own — otherwise lane B can be failed by lane A's survivors, or (with the
/// lanes reordered) pass on lane A's silence.
#[test]
fn every_lane_starts_from_an_empty_artifact_directory() {
    let step = mutation_step();
    let lanes = step
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("cargo mutants"))
        .count();
    let clears = step
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("rm -rf mutants.out"))
        .count();
    assert!(lanes >= 3, "the mutation step moved: {lanes} lanes found");
    assert_eq!(
        clears, lanes,
        "{lanes} lanes and {clears} clears. A run that finds no mutants exits 0 \
         and leaves the previous run's missed.txt in place, so a lane that does \
         not clear mutants.out first inherits the lane before it."
    );
}
