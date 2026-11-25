//! A red nightly lane must notify someone (#3309).
//!
//! Five streaks (6-22 nights: #3307/#2898/#2900/#2764/#2763) were found only
//! by someone happening to open the Actions tab. The reporter job at the end
//! of nightly.yml closes that gap — and these tests keep the reporter itself
//! from silently rotting the same way:
//!
//! - the wiring guards derive both sides from the workflow file (the
//!   reporter's `needs` list must equal every other job id — a hand-kept lane
//!   list is exactly the drift #3317 documents), and pin the `always()` shape
//!   (a plain `if: failure()` is skipped whenever a lane is skipped, and a
//!   reporter that can be skipped is the original gap again) — including a
//!   `cancelled` lane, which is how GitHub concludes a lane that hit its
//!   `timeout-minutes` (#3740: Miri timed out 60 nights, three reported);
//! - the behavior tests run `scripts/nightly-red-reporter.sh` against a fake
//!   `gh` on PATH — they judge the decision itself, the same falsifiable-gate
//!   shape as `mutation_gate_verdict.rs` (#3225).

#![deny(unsafe_code)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

const WORKFLOW: &str = ".github/workflows/nightly.yml";
const REPORTER: &str = "scripts/nightly-red-reporter.sh";
const REPORTER_JOB: &str = "nightly-red-reporter";

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

/// Top-level job ids: two-space-indented `name:` lines after `jobs:`.
fn workflow_job_ids() -> BTreeSet<String> {
    let workflow = read(WORKFLOW);
    let jobs = workflow
        .split_once("\njobs:\n")
        .expect("nightly.yml has a jobs block")
        .1;
    jobs.lines()
        .filter_map(|line| {
            let name = line.strip_prefix("  ")?.strip_suffix(':')?;
            (!name.starts_with(' ') && !name.contains(' ')).then(|| name.to_owned())
        })
        .collect()
}

/// The reporter job's block, from its id to the end of the file (it is the
/// last job by construction; the guards below fail loudly if it is absent).
fn reporter_block() -> String {
    let workflow = read(WORKFLOW);
    workflow
        .split_once(&format!("\n  {REPORTER_JOB}:\n"))
        .expect("nightly.yml has the reporter job")
        .1
        .to_owned()
}

#[test]
fn the_reporter_needs_every_other_lane() {
    let mut lanes = workflow_job_ids();
    assert!(
        lanes.remove(REPORTER_JOB),
        "the reporter job must exist in {WORKFLOW}"
    );
    let block = reporter_block();
    let needs: BTreeSet<String> = block
        .split_once("needs:\n")
        .expect("the reporter has a needs list")
        .1
        .lines()
        .map_while(|line| line.trim().strip_prefix("- ").map(str::to_owned))
        .collect();
    assert_eq!(
        needs, lanes,
        "the reporter's `needs` must name every other job in {WORKFLOW} — \
         a lane missing here can finish after the reporter ran and go unreported"
    );
}

#[test]
fn the_reporter_runs_on_failed_or_cancelled_lanes_even_when_others_were_skipped() {
    let block = reporter_block();
    assert!(
        block.contains(
            "if: always() && (contains(needs.*.result, 'failure') \
             || contains(needs.*.result, 'cancelled'))"
        ),
        "the reporter's condition must be `always()` with explicit failure and \
         cancelled checks — a plain `if: failure()` is skipped when any lane is \
         skipped, and a lane that hits its `timeout-minutes` concludes \
         `cancelled`, so a failure-only check never runs the reporter for it"
    );
}

#[test]
fn the_reporter_has_the_permissions_its_calls_need() {
    let block = reporter_block();
    for permission in [
        "contents: read",
        "actions: read",
        "checks: read",
        "issues: write",
    ] {
        assert!(
            block.contains(permission),
            "the reporter job must declare `{permission}`: job-level \
             permissions replace the defaults wholesale, so omitting one \
             breaks checkout, the jobs query, the timeout annotation read, \
             or the issue write"
        );
    }
}

#[test]
fn the_workflow_delegates_reporting_to_the_script() {
    let block = reporter_block();
    assert!(
        block.contains(REPORTER),
        "the reporter job must run {REPORTER} — the decision lives in the \
         script so these tests can execute it, not a YAML transcription of it"
    );
}

/// A scratch area holding a fake `gh` plus its canned outputs and call log.
struct FakeGh {
    dir: PathBuf,
}

impl FakeGh {
    /// `jobs_json` is the raw jobs-API payload; `issues_json` the raw open
    /// `nightly-red` issue list. The shim logs every invocation to calls.log;
    /// a job's check-run annotations come from [`FakeGh::annotate`], and a
    /// job without any reads as an empty list.
    fn new(name: &str, jobs_json: &str, issues_json: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "nightly-red-reporter-{name}-{}",
            std::process::id()
        ));
        // A leftover from a previous run of this same test would replay stale
        // canned outputs; start clean.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        std::fs::write(dir.join("jobs.json"), jobs_json).expect("jobs.json");
        std::fs::write(dir.join("issues.json"), issues_json).expect("issues.json");
        let shim = r#"#!/usr/bin/env bash
here="$(cd "$(dirname "$0")" && pwd)"
printf '%s\n' "$*" >> "$here/calls.log"
case "$1 $2" in
  "api "*"/jobs"*)
    # Apply the CALLER's --jq expression: hardcoding the filter here would
    # make the script's own conclusion filter invisible to these tests.
    expr=""
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "--jq" ]; then expr="$arg"; fi
      prev="$arg"
    done
    jq -r "$expr" "$here/jobs.json"
    ;;
  "api "*"/annotations"*)
    # repos/<owner>/<repo>/check-runs/<job id>/annotations, filtered by the
    # caller's --jq like the jobs call above.
    id="${2#*/check-runs/}"
    id="${id%%/*}"
    expr=""
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "--jq" ]; then expr="$arg"; fi
      prev="$arg"
    done
    if [ -f "$here/annotations-$id.json" ]; then
      jq -r "$expr" "$here/annotations-$id.json"
    else
      echo '[]' | jq -r "$expr"
    fi
    ;;
  "api "*)
    echo "cafe1234"
    ;;
  "label create")
    ;;
  "issue list")
    cat "$here/issues.json"
    ;;
  "issue comment" | "issue create")
    ;;
  *)
    echo "fake gh: unexpected call: $*" >&2
    exit 64
    ;;
esac
"#;
        let gh = dir.join("gh");
        std::fs::write(&gh, shim).expect("gh shim");
        Command::new("chmod")
            .args(["+x"])
            .arg(&gh)
            .status()
            .expect("chmod");
        Self { dir }
    }

    /// Gives job `id` check-run annotations with these messages.
    fn annotate(&self, id: usize, messages: &[&str]) {
        let entries: Vec<String> = messages
            .iter()
            .map(|message| format!(r#"{{"annotation_level":"failure","message":"{message}"}}"#))
            .collect();
        std::fs::write(
            self.dir.join(format!("annotations-{id}.json")),
            format!("[{}]", entries.join(",")),
        )
        .expect("annotations json");
    }

    fn run_reporter(&self) -> std::process::Output {
        let path = format!(
            "{}:{}",
            self.dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        Command::new("bash")
            .arg(repo_root().join(REPORTER))
            .args(["stratalab/strata-core", "42"])
            .env("PATH", path)
            .output()
            .expect("run reporter")
    }

    fn calls(&self) -> String {
        std::fs::read_to_string(self.dir.join("calls.log")).unwrap_or_default()
    }
}

/// Each job's id is its 1-based position in `entries`.
fn jobs_json(entries: &[(&str, &str)]) -> String {
    let jobs: Vec<String> = entries
        .iter()
        .enumerate()
        .map(|(index, (name, conclusion))| {
            let id = index + 1;
            format!(r#"{{"id":{id},"name":"{name}","conclusion":"{conclusion}"}}"#)
        })
        .collect();
    format!(r#"{{"jobs":[{}]}}"#, jobs.join(","))
}

/// What GitHub annotates on a job that ran into its `timeout-minutes`
/// (verbatim from the 2026-11-25 Miri lane).
const TIMEOUT_ANNOTATION: &str = "The job has exceeded the maximum execution time of 2h0m0s";
/// What a cancelled job is annotated with, timeout or not.
const CANCEL_ANNOTATION: &str = "The operation was canceled.";

#[test]
fn a_run_with_no_failures_reports_nothing() {
    let fake = FakeGh::new(
        "green",
        &jobs_json(&[
            ("storage-soak-lanes", "success"),
            ("differential", "success"),
        ]),
        "[]",
    );
    let output = fake.run_reporter();
    assert!(output.status.success(), "green run must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        !calls.contains("issue "),
        "a green run must not touch issues, called: {calls}"
    );
}

#[test]
fn a_new_red_lane_files_a_labelled_issue_named_after_it() {
    let fake = FakeGh::new(
        "new-red",
        &jobs_json(&[
            ("storage-soak-lanes", "failure"),
            ("differential", "success"),
        ]),
        "[]",
    );
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("issue create")
            && calls.contains("nightly-red: storage-soak-lanes")
            && calls.contains("bug,nightly-red")
            && calls.contains("`storage-soak-lanes` failed."),
        "a new red lane files a labelled issue named after it, called: {calls}"
    );
    assert!(
        !calls.contains("issue comment"),
        "nothing to extend on a first red, called: {calls}"
    );
}

#[test]
fn a_streak_extends_the_open_issue_instead_of_filing_another() {
    let fake = FakeGh::new(
        "streak",
        &jobs_json(&[("storage-soak-lanes", "failure")]),
        r#"[{"number":77,"title":"nightly-red: storage-soak-lanes"}]"#,
    );
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("issue comment 77"),
        "a streak comments the open issue, called: {calls}"
    );
    assert!(
        !calls.contains("issue create"),
        "a streak must not file a second issue, called: {calls}"
    );
}

#[test]
fn each_red_lane_reports_separately_and_matching_is_exact() {
    // The open issue names a DIFFERENT lane: `differential` must extend it,
    // `storage-soak-lanes` must file fresh — substring matching would pair
    // the wrong streaks.
    let fake = FakeGh::new(
        "two-lanes",
        &jobs_json(&[
            ("storage-soak-lanes", "failure"),
            ("differential", "failure"),
        ]),
        r#"[{"number":88,"title":"nightly-red: differential"}]"#,
    );
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("issue create") && calls.contains("nightly-red: storage-soak-lanes"),
        "the un-issued lane files fresh, called: {calls}"
    );
    assert!(
        calls.contains("issue comment 88"),
        "the already-issued lane extends its issue, called: {calls}"
    );
}

#[test]
fn a_manually_cancelled_or_skipped_lane_is_not_a_finding() {
    let fake = FakeGh::new(
        "cancelled",
        &jobs_json(&[
            ("storage-soak-lanes", "cancelled"),
            ("differential", "skipped"),
        ]),
        "[]",
    );
    fake.annotate(1, &[CANCEL_ANNOTATION]);
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("check-runs/1/annotations"),
        "a cancelled lane is judged by its annotations, called: {calls}"
    );
    assert!(
        !calls.contains("issue "),
        "a cancel without a timeout, or a skip, is not a finding, called: {calls}"
    );
}

#[test]
fn a_lane_cancelled_by_its_timeout_files_an_issue_saying_so() {
    let fake = FakeGh::new(
        "timed-out",
        &jobs_json(&[
            ("differential", "success"),
            ("memory-safety-miri", "cancelled"),
        ]),
        "[]",
    );
    fake.annotate(2, &[TIMEOUT_ANNOTATION, CANCEL_ANNOTATION]);
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("issue create")
            && calls.contains("nightly-red: memory-safety-miri")
            && calls.contains("`memory-safety-miri` timed out."),
        "a lane that hit its timeout-minutes is red and says so, called: {calls}"
    );
}

#[test]
fn a_timed_out_streak_extends_the_open_issue() {
    let fake = FakeGh::new(
        "timed-out-streak",
        &jobs_json(&[("memory-safety-miri", "cancelled")]),
        r#"[{"number":91,"title":"nightly-red: memory-safety-miri"}]"#,
    );
    fake.annotate(1, &[TIMEOUT_ANNOTATION]);
    let output = fake.run_reporter();
    assert!(output.status.success(), "reporter must exit 0: {output:?}");
    let calls = fake.calls();
    assert!(
        calls.contains("issue comment 91") && calls.contains("Still red (timed out)"),
        "a timeout streak comments the open issue, called: {calls}"
    );
    assert!(
        !calls.contains("issue create"),
        "a timeout streak must not file a second issue, called: {calls}"
    );
}
