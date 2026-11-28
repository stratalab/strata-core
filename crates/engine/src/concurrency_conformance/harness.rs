//! The commit-seam harness: run two operations from one seeded state with the
//! first held at each of its persistence calls in turn, and hold the result
//! to the serial orders.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::{CacheOpenOptions, Database, EngineError};

/// What an operation reports, normalized so two runs of the same history
/// compare equal: commit versions, timestamps and hashes are left out.
pub(super) type Answer = Value;

/// One engine operation a thread sharing the handle can run.
#[derive(Clone, Copy)]
pub(super) struct Op {
    /// A short label naming the operation and its target, unique within its
    /// catalog, e.g. `json.set(d, $.x)`.
    pub(super) label: &'static str,
    /// The IDL command the operation exercises, for the coverage guard.
    pub(super) command: &'static str,
    pub(super) run: fn(&Database) -> Answer,
}

/// A family of operations over shared state: a seed, the operations, and
/// the observable state they leave.
pub(super) struct Catalog {
    pub(super) family: &'static str,
    pub(super) seed: fn(&mut Database),
    pub(super) ops: &'static [Op],
    /// Reads the state the operations touch, after both have finished. Runs
    /// with the handle to itself, so it may use `&mut` services.
    pub(super) digest: fn(&mut Database) -> Answer,
}

/// Both answers and the state, from one run.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Outcome {
    pub(super) first: Answer,
    pub(super) second: Answer,
    pub(super) state: Answer,
}

/// Where the first operation is held: before its `n`th (from 0) call of
/// one kind reaches storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Window {
    Read(u64),
    Scan(u64),
    Commit(u64),
}

/// The verdict on one ordered pair.
#[derive(Debug)]
pub(super) struct Verdict {
    pub(super) scenario: String,
    pub(super) windows: usize,
    /// The first window whose outcome matched neither serial order, with
    /// that outcome and the two serial ones.
    pub(super) counterexample: Option<(Window, Outcome, Outcome, Outcome)>,
}

impl Verdict {
    pub(super) const fn is_serializable(&self) -> bool {
        self.counterexample.is_none()
    }
}

pub(super) fn open_cache_database() -> Database {
    Database::open_cache(CacheOpenOptions::new())
        .expect("cache open succeeds")
        .into_database()
}

/// An operation's answer: what `ok` makes of a success, or the error's code.
pub(super) fn answer<T>(result: Result<T, EngineError>, ok: impl FnOnce(T) -> Answer) -> Answer {
    match result {
        Ok(value) => ok(value),
        Err(error) => json!({ "error": error.code() }),
    }
}

/// Runs `first` then `second` from the seed, and counts `first`'s calls.
fn run_serial(catalog: &Catalog, first: &Op, second: &Op) -> (Outcome, (u64, u64, u64)) {
    let mut database = open_cache_database();
    (catalog.seed)(&mut database);
    let before = database.persistence_calls_for_test();
    let first_answer = (first.run)(&database);
    let after = database.persistence_calls_for_test();
    let second_answer = (second.run)(&database);
    let state = (catalog.digest)(&mut database);
    (
        Outcome {
            first: first_answer,
            second: second_answer,
            state,
        },
        (after.0 - before.0, after.1 - before.1, after.2 - before.2),
    )
}

/// Arms the pause for `window`: the thread that reaches it signals and then
/// waits to be resumed.
fn arm(database: &Database, window: Window) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let hook = move || {
        // The receiver outlives the run unless the run failed; either way the
        // signal's only reader is gone, so there is nothing to report.
        let _ = paused_tx.send(());
        // Dropped without a resume only when the run panicked; the paused
        // operation then simply continues.
        let _ = resume_rx.recv();
    };
    match window {
        Window::Read(skip) => database.on_point_read_for_test(to_usize(skip), hook),
        Window::Scan(skip) => database.on_scan_for_test(to_usize(skip), hook),
        Window::Commit(skip) => database.on_commit_for_test(to_usize(skip), hook),
    }
    (paused_rx, resume_tx)
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).expect("a call count fits in usize")
}

/// How long a run waits for a thread before declaring the harness stuck.
const STUCK: Duration = Duration::from_secs(30);

/// Runs `first` held at `window`, runs `second` to completion (or until it
/// queues behind `first`'s branch writer), then resumes `first`. `None` when `first`
/// never reaches the window.
fn run_interleaved(catalog: &Catalog, first: &Op, second: &Op, window: Window) -> Option<Outcome> {
    let mut database = open_cache_database();
    (catalog.seed)(&mut database);
    let (paused, resume) = arm(&database, window);
    let shared = &database;
    let answers = std::thread::scope(|scope| {
        // Owned by the scope, so a panic below drops the resume handle and the
        // held operation runs on instead of waiting forever.
        let resume = resume;
        let held = scope.spawn(|| (first.run)(shared));
        let started = Instant::now();
        loop {
            if paused.recv_timeout(Duration::from_millis(5)).is_ok() {
                break;
            }
            if held.is_finished() {
                // The window does not exist on this run.
                return None;
            }
            assert!(
                started.elapsed() < STUCK,
                "{} never reached {window:?}",
                first.label
            );
        }
        let waits_before = shared.writer_waits_for_test();
        let racing = scope.spawn(|| (second.run)(shared));
        let started = Instant::now();
        while !racing.is_finished() && shared.writer_waits_for_test() == waits_before {
            assert!(
                started.elapsed() < STUCK,
                "{} neither finished nor queued behind {}",
                second.label,
                first.label
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        resume.send(()).expect("the held operation is waiting");
        let first_answer = held.join().expect("the held operation does not panic");
        let second_answer = racing.join().expect("the racing operation does not panic");
        Some((first_answer, second_answer))
    })?;
    let state = (catalog.digest)(&mut database);
    Some(Outcome {
        first: answers.0,
        second: answers.1,
        state,
    })
}

/// Checks the ordered pair `(first, second)`: at every window of `first`,
/// the outcome must equal one of the two serial orders.
pub(super) fn check(catalog: &Catalog, first: &Op, second: &Op) -> Verdict {
    let scenario = scenario_name(first, second);
    let (first_then_second, (reads, scans, commits)) = run_serial(catalog, first, second);
    let (second_then_first, _) = run_serial(catalog, second, first);
    // Order the reverse run's answers by operation, not by who ran first.
    let second_then_first = Outcome {
        first: second_then_first.second,
        second: second_then_first.first,
        state: second_then_first.state,
    };
    let windows = (0..reads)
        .map(Window::Read)
        .chain((0..scans).map(Window::Scan))
        .chain((0..commits).map(Window::Commit))
        .collect::<Vec<_>>();
    let mut checked = 0;
    for window in windows {
        let Some(outcome) = run_interleaved(catalog, first, second, window) else {
            continue;
        };
        checked += 1;
        if outcome != first_then_second && outcome != second_then_first {
            return Verdict {
                scenario,
                windows: checked,
                counterexample: Some((window, outcome, first_then_second, second_then_first)),
            };
        }
    }
    Verdict {
        scenario,
        windows: checked,
        counterexample: None,
    }
}

pub(super) fn scenario_name(first: &Op, second: &Op) -> String {
    format!("{} || {}", first.label, second.label)
}

/// Every ordered pair of a catalog's operations, the same operation twice
/// included.
pub(super) fn check_catalog(catalog: &Catalog) -> Vec<Verdict> {
    let mut verdicts = Vec::new();
    for first in catalog.ops {
        for second in catalog.ops {
            verdicts.push(check(catalog, first, second));
        }
    }
    verdicts
}
