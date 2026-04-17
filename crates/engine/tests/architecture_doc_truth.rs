//! Regression test for T3-E8 / D-DR-12: `docs/architecture/durability-and-recovery.md`
//! must describe the shipped durability path. Any forward-looking claim must
//! be explicitly labeled with `[TARGET STATE]` on the same line so readers
//! never confuse a planned behavior for a shipped one.
//!
//! The acceptance criterion from `durability-recovery-scope.md` Phase DR-10
//! and the tranche-3 implementation plan Epic 8 is:
//!
//! > Add a grep-style regression test that fails on unlabeled target-state
//! > phrases in the durability architecture doc.
//!
//! Adding a target-state phrase without the `[TARGET STATE]` label will fail
//! this test. Removing the "current state as of" banner will also fail.
//!
//! If a legitimate use of a trigger word appears in prose where it is not
//! claiming target-state (for example, "not yet" meaning "not until some
//! runtime condition"), rephrase the sentence — the trigger list is narrow
//! enough that the false-positive rate is intentionally low.

use std::fs;
use std::path::PathBuf;

/// Phrases that, without an accompanying `[TARGET STATE]` marker on the same
/// line, indicate the doc is describing unshipped behavior as if it had
/// shipped. Matched case-insensitively against the line content.
const TARGET_STATE_TRIGGERS: &[&str] = &[
    "not yet",
    "to be implemented",
    "will be implemented",
    "is planned",
    "future work",
    "in the future",
    "eventually",
    "upcoming",
    "tbd",
    "todo",
    "fixme",
    "will be added",
    "will land",
    "planned for",
];

/// The literal marker that labels a target-state claim as such.
const TARGET_STATE_MARKER: &str = "[TARGET STATE]";

/// The banner prefix that every architecture doc under this regime must carry
/// so the reader knows the "as of" commit/date anchor.
const CURRENT_STATE_BANNER: &str = "Current state as of";

fn doc_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("docs/architecture/durability-and-recovery.md")
}

fn read_doc() -> String {
    let path = doc_path();
    let display = path.display();
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {display}: {e}"))
}

#[test]
fn durability_doc_has_current_state_banner() {
    let doc = read_doc();
    assert!(
        doc.contains(CURRENT_STATE_BANNER),
        "architecture doc at {} must contain a \"{CURRENT_STATE_BANNER} <commit/date>\" banner \
         so readers know what state the doc describes",
        doc_path().display()
    );
}

#[test]
fn durability_doc_has_no_unlabeled_target_state_claims() {
    let doc = read_doc();
    let mut violations: Vec<(usize, String, &str)> = Vec::new();

    for (lineno, line) in doc.lines().enumerate() {
        let lower = line.to_lowercase();
        for trigger in TARGET_STATE_TRIGGERS {
            if !lower.contains(trigger) {
                continue;
            }
            if line.contains(TARGET_STATE_MARKER) {
                continue;
            }
            violations.push((lineno + 1, line.to_string(), trigger));
        }
    }

    if violations.is_empty() {
        return;
    }

    let mut msg = format!(
        "architecture doc at {} contains target-state phrases without a `{TARGET_STATE_MARKER}` label on the same line.\n\
         Either remove the forward-looking phrasing or add `{TARGET_STATE_MARKER}` to make the target-state claim explicit.\n\n\
         Offending lines:\n",
        doc_path().display()
    );
    for (lineno, line, trigger) in &violations {
        msg.push_str(&format!(
            "  line {lineno}: trigger=\"{trigger}\"\n    {line}\n"
        ));
    }
    panic!("{msg}");
}
