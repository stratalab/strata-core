//! TCP4.9b — recovery budget-adherence oracle.
//!
//! The memory budget is a product contract (`with_memory_budget` bounds the
//! database's working memory; graded admission enforces it on the write
//! path), and #2567 showed recovery was outside it: a 1B-key crash-recovery
//! open consumed ~56 GB RSS and was OOM-killed. This harness re-found that
//! at CI scale (gate 7) — recovering a ~64 MB database under a 16 MB budget
//! peaked at ~200 MB RSS, byte-identical with no budget at all — and now
//! holds the fixed contract: the envelope below.
//!
//! Measurement: each phase runs in its own subprocess (the TCP2.1
//! re-invoke-self pattern) so `VmHWM` — the kernel's own high-water mark —
//! is a clean per-phase peak, unpolluted by the seeding phase's allocations.
//! The seed itself is deliberately **unbudgeted**: the contract under test
//! is recovery's memory, and seeding under a small budget just measures
//! write-path back-pressure (which BS5's graded admission already covers).
//!
//! History: the `pin_2567_*` gate-7 pin held today's violation exactly
//! (budgeted peak ~12x the budget, byte-identical unbudgeted) until the fix
//! landed in two slices — the streamed replay transient (S3a, #3478) and
//! the replay-time flush of the installed state (S3b) — at which point the
//! pin broke as designed and was replaced by the envelope contract below.
//! The correctness half is permanent: recovery under a small budget must
//! still recover *all* the data, however much memory it uses.
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

    {
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
        // The recovered store must be fully OPERABLE: a write plus the clean
        // close's flush drives a manifest publish over the tables the
        // budgeted recovery flushed mid-replay — a catalog gap there
        // (an unrecorded replay-flushed table) fails right here.
        kv.put(kv_key(KEYS), KvValue::new(vec![b'w'; 64]))
            .expect("post-recovery write");
    }
    let mut db = db;
    db.close()
        .expect("post-recovery close flushes and publishes over replay-flushed tables");
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
/// installed state (the replayed rows' memtables, ~1x the tail — S3b's
/// replay-time flush bounds that under the budget; the envelope test below
/// asserts it) plus a small window. The 1.5x ceiling sits between the
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

/// The S3b envelope allowance on top of the budget: the streamed read chunk,
/// one in-flight flush artifact (itself budget-checked), disk-resident
/// reader metadata, and allocator slack. Fixed and documented — not a knob
/// to grow when the assertion gets tight.
const ENVELOPE_ALLOWANCE_KB: u64 = 32 * 1024;

/// The #2567/#3319 contract, complete (S3a + S3b): budgeted recovery peaks
/// within the budget envelope — the transient is streamed (S3a) and the
/// replayed state rotates and flushes to disk-resident tables under the
/// budget-derived threshold (S3b) — while the read-back in
/// `phase_recover_budgeted` keeps proving no data was traded away.
///
/// Order matters: the UNBUDGETED control runs first because a budgeted
/// recovery now MUTATES the store (its mid-replay flushes publish tables
/// and manifests), and the control must see the original WAL-heavy shape.
/// At this scale the unbudgeted run flushes nothing (its budget-derived
/// rotation threshold exceeds the whole tail), so the store reaches the
/// budgeted phase unchanged.
#[test]
fn budgeted_recovery_peak_stays_within_the_budget_envelope() {
    let dir = tempfile::tempdir().expect("tmp");
    run_phase("phase_seed", dir.path());

    let unbudgeted = run_phase("phase_recover_unbudgeted", dir.path());
    let unbudgeted_peak_kb = phase_kb(&unbudgeted, "PHASE-RECOVER-UNBUDGETED", "after_open_kb")
        - phase_kb(&unbudgeted, "PHASE-RECOVER-UNBUDGETED", "before_kb");

    let budgeted = run_phase("phase_recover_budgeted", dir.path());
    let budgeted_peak_kb = phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "after_open_kb")
        - phase_kb(&budgeted, "PHASE-RECOVER-BUDGETED", "before_kb");

    let budget_kb = RECOVERY_BUDGET / 1024;
    assert!(
        budgeted_peak_kb <= budget_kb + ENVELOPE_ALLOWANCE_KB,
        "budgeted recovery peaked at {budgeted_peak_kb} kB — outside the \
         envelope ({budget_kb} kB budget + {ENVELOPE_ALLOWANCE_KB} kB allowance): \
         the replayed state is not being flushed under the budget"
    );
    // The budget must INFLUENCE recovery: the same store recovered without a
    // budget keeps the whole replayed state resident and peaks well above.
    assert!(
        budgeted_peak_kb < unbudgeted_peak_kb - unbudgeted_peak_kb / 4,
        "budgeted ({budgeted_peak_kb} kB) is not materially below unbudgeted \
         ({unbudgeted_peak_kb} kB) — the budget is not shaping recovery"
    );
}
