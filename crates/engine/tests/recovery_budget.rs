//! TCP4.9b — recovery budget-adherence oracle.
//!
//! The memory budget is a product contract (`with_memory_budget` bounds the
//! database's working memory; graded admission enforces it on the write
//! path), but #2567 showed recovery is outside it: a 1B-key crash-recovery
//! open consumed ~56 GB RSS and was OOM-killed. This harness re-finds that
//! at CI scale (gate 7) and pins it: recovering a ~64 MB database under a
//! 16 MB budget peaks at ~200 MB RSS — ~12× the budget — and the peak is
//! byte-identical with **no budget at all**, so the budget is not leaked
//! past but ignored entirely on the recovery path.
//!
//! Measurement: each phase runs in its own subprocess (the TCP2.1
//! re-invoke-self pattern) so `VmHWM` — the kernel's own high-water mark —
//! is a clean per-phase peak, unpolluted by the seeding phase's allocations.
//! The seed itself is deliberately **unbudgeted**: the contract under test
//! is recovery's memory, and seeding under a small budget just measures
//! write-path back-pressure (which BS5's graded admission already covers).
//!
//! The pin (`pin_2567_*`) asserts today's violation exactly, shrink-only:
//! when the fix lands, recovery peaks must land within the budget envelope,
//! the pin breaks, and it must be replaced by the contract assertion its
//! failure message spells out. The correctness half is permanent: recovery
//! under a small budget must still recover *all* the data, however much
//! memory it uses.
#![cfg(all(feature = "localfs", target_os = "linux"))]

use std::path::Path;

use strata_engine::{
    BranchName, Database, DatabaseOpenOutcome, DurableLocalOpenOptions, KvKey, KvValue,
    ProductSpace,
};

/// ~64 MB of KV data: 4× the recovery budget, large enough that an
/// unbounded replay is unmistakable and small enough for a per-PR lane.
const KEYS: u32 = 2000;
const VALUE_BYTES: usize = 32 * 1024;
const RECOVERY_BUDGET: u64 = 16 * 1024 * 1024;

/// Today recovery peaks at ~12× the budget; the pin trips at >4× so the
/// assertion is nowhere near the noise floor in either direction.
const PINNED_VIOLATION_FACTOR: u64 = 4;

const DIR_ENV: &str = "STRATA_RECOVERY_BUDGET_DIR";

fn branch() -> BranchName {
    BranchName::new("default").expect("valid branch name")
}

fn space() -> ProductSpace {
    ProductSpace::new("default").expect("valid product space")
}

fn kv_key(index: u32) -> KvKey {
    KvKey::new(format!("scale-{index:06}").as_bytes()).expect("valid key")
}

/// Reads a numeric field (kB) from `/proc/self/status`.
fn vm_kb(name: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    status
        .lines()
        .find(|line| line.starts_with(name))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|kb| kb.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{name} missing from /proc/self/status"))
}

fn open_unbudgeted(root: &Path) -> Database {
    Database::open_local(root, DurableLocalOpenOptions::new())
        .map(DatabaseOpenOutcome::into_database)
        .expect("durable open")
}

fn open_budgeted(root: &Path) -> Database {
    Database::open_local(
        root,
        DurableLocalOpenOptions::new().with_memory_budget(RECOVERY_BUDGET),
    )
    .map(DatabaseOpenOutcome::into_database)
    .expect("budgeted durable open")
}

// ---------------------------------------------------------------------------
// Subprocess phases. Each early-returns when the env var is absent, so a
// bare `--ignored` sweep cannot fail on them.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "subprocess phase: re-invoked by the parent oracle with STRATA_RECOVERY_BUDGET_DIR"]
fn phase_seed() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        return;
    };
    let root = Path::new(&dir).join("db");
    let mut db = open_unbudgeted(&root);
    let payload = vec![b'v'; VALUE_BYTES];
    {
        let mut kv = db.kv(branch(), space()).expect("kv opens");
        for index in 0..KEYS {
            kv.put(kv_key(index), KvValue::new(payload.clone()))
                .expect("unbudgeted seed write");
        }
    }
    db.close().expect("clean close flushes the WAL");
    println!("PHASE-SEED-OK");
}

/// Reports the recovery-phase RSS peak and proves the recovered state is
/// complete: budgeted recovery must never trade data for memory.
#[test]
#[ignore = "subprocess phase: re-invoked by the parent oracle with STRATA_RECOVERY_BUDGET_DIR"]
fn phase_recover_budgeted() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        return;
    };
    let root = Path::new(&dir).join("db");
    let before = vm_kb("VmHWM");
    let db = open_budgeted(&root);
    let after_open = vm_kb("VmHWM");

    let mut kv = db.kv(branch(), space()).expect("kv opens after recovery");
    for index in [0, KEYS / 2, KEYS - 1] {
        let row = kv.get(&kv_key(index)).expect("read recovers");
        let value = row.unwrap_or_else(|| panic!("key {index} lost by budgeted recovery"));
        assert_eq!(
            value.as_bytes().len(),
            VALUE_BYTES,
            "key {index} damaged by budgeted recovery"
        );
    }
    println!("PHASE-RECOVER-BUDGETED before_kb={before} after_open_kb={after_open}");
}

#[test]
#[ignore = "subprocess phase: re-invoked by the parent oracle with STRATA_RECOVERY_BUDGET_DIR"]
fn phase_recover_unbudgeted() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        return;
    };
    let root = Path::new(&dir).join("db");
    let before = vm_kb("VmHWM");
    let _db = open_unbudgeted(&root);
    let after_open = vm_kb("VmHWM");
    println!("PHASE-RECOVER-UNBUDGETED before_kb={before} after_open_kb={after_open}");
}

// ---------------------------------------------------------------------------
// The parent oracle.
// ---------------------------------------------------------------------------

fn run_phase(test_name: &str, dir: &Path) -> String {
    let exe = std::env::current_exe().expect("current test binary");
    let output = std::process::Command::new(exe)
        .args([test_name, "--exact", "--ignored", "--nocapture"])
        .env(DIR_ENV, dir)
        .output()
        .expect("spawn subprocess phase");
    assert!(
        output.status.success(),
        "{test_name} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Extracts `key=value` kB fields from a phase's marker line.
fn phase_kb(output: &str, marker: &str, field: &str) -> u64 {
    let line = output
        .lines()
        .find(|line| line.starts_with(marker))
        .unwrap_or_else(|| panic!("{marker} line missing from phase output:\n{output}"));
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(&format!("{field}=")))
        .and_then(|kb| kb.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{marker} line missing {field}: {line}"))
}

/// Sums the on-disk WAL bytes the recovery phases will replay.
fn wal_bytes(dir: &Path) -> u64 {
    let wal = dir.join("db").join("wal");
    std::fs::read_dir(&wal)
        .unwrap_or_else(|err| panic!("read {}: {err}", wal.display()))
        .map(|entry| {
            entry
                .expect("wal entry")
                .metadata()
                .expect("wal metadata")
                .len()
        })
        .sum()
}

/// #3319 / #2567 S3a: the recovery TRANSIENT is bounded — replaying a WAL
/// tail must not materialize whole-tail decode buffers on top of the
/// installed state.
///
/// Today `recover_wal` decodes the entire tail into one `Vec`, copies it,
/// and carries it to bootstrap replay: the peak is ~3x the tail (~200 MB
/// for a ~64 MB WAL). With streamed, windowed replay the peak is the
/// installed state (the replayed rows' memtables, ~1x the tail — bounding
/// THAT under the budget is the S3b replay-flush slice, tracked by
/// `pin_2567_*`) plus a small window. The 1.5x ceiling sits between the
/// two regimes with margin on both sides.
#[test]
fn recovery_transient_stays_within_a_small_multiple_of_the_wal_tail() {
    let dir = tempfile::tempdir().expect("tmp");
    run_phase("phase_seed", dir.path());
    let tail_kb = wal_bytes(dir.path()) / 1024;
    assert!(
        tail_kb > 3 * RECOVERY_BUDGET / 1024,
        "seed must leave a WAL tail several times the budget (got {tail_kb} kB)"
    );

    let budgeted = run_phase("phase_recover_budgeted", dir.path());
    let budgeted_peak_kb = phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "after_open_kb")
        - phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "before_kb");

    let ceiling_kb = tail_kb + tail_kb / 2;
    assert!(
        budgeted_peak_kb <= ceiling_kb,
        "recovery transient is unbounded: replaying a {tail_kb} kB WAL tail \
         peaked at {budgeted_peak_kb} kB (> {ceiling_kb} kB = 1.5x the tail) — \
         the tail is being materialized wholesale instead of streamed"
    );
}

/// #2567 pin (shrink-only): recovery memory ignores the configured budget.
///
/// Asserts today's violation exactly: the recovery-phase RSS peak exceeds
/// the budget several times over, and an unbudgeted recovery peaks at the
/// same height — the budget changes nothing. When the fix lands this pin
/// breaks; delete it and assert the contract instead: the budgeted
/// recovery peak stays within the budget envelope (budget + a fixed
/// process-overhead allowance), while `phase_recover_budgeted`'s read-back
/// (already permanent) keeps proving no data was traded away.
#[test]
fn pin_2567_recovery_rss_ignores_the_memory_budget() {
    let dir = tempfile::tempdir().expect("tmp");
    run_phase("phase_seed", dir.path());

    let budgeted = run_phase("phase_recover_budgeted", dir.path());
    let budgeted_peak_kb = phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "after_open_kb")
        - phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "before_kb");

    let unbudgeted = run_phase("phase_recover_unbudgeted", dir.path());
    let unbudgeted_peak_kb = phase_kb(&unbudgeted, "PHASE-RECOVER-UNBUDGETED", "after_open_kb")
        - phase_kb(&unbudgeted, "PHASE-RECOVER-UNBUDGETED", "before_kb");

    let budget_kb = RECOVERY_BUDGET / 1024;
    assert!(
        budgeted_peak_kb > budget_kb * PINNED_VIOLATION_FACTOR,
        "#2567 pin: budgeted recovery peaked at {budgeted_peak_kb} kB, within \
         {PINNED_VIOLATION_FACTOR}x of the {budget_kb} kB budget — the fix landed: \
         delete this pin and assert the budget envelope instead"
    );
    // The stronger half: the budget is ignored, not merely exceeded — the
    // budgeted peak matches the unbudgeted peak. (Both replay the same ~64 MB
    // WAL; a budget-aware recovery would bound the budgeted run well below.)
    let (low, high) = (
        budgeted_peak_kb.min(unbudgeted_peak_kb),
        budgeted_peak_kb.max(unbudgeted_peak_kb),
    );
    assert!(
        high - low < high / 4,
        "#2567 pin: budgeted ({budgeted_peak_kb} kB) and unbudgeted \
         ({unbudgeted_peak_kb} kB) recovery peaks diverged by more than 25% — the \
         budget now influences recovery: re-triage this pin against the fix"
    );
}
