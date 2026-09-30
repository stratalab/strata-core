//! Space-reclamation contract §3.1 (slice 7): a clean close checkpoints every
//! branch — the product default and the engine's `_system_` root, which never
//! flushes — and truncates the WAL behind the snapshot, so the next open
//! replays nothing and still reads every row.
#![cfg(feature = "localfs")]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{branch, key, open_durable_database, space, value};
use strata_engine::{Database, DatabaseOpenOutcome, DurableLocalOpenOptions};

const ROWS: u32 = 512;

/// Every WAL segment file under `wal/`, by name, with its size in bytes.
fn wal_segments(root: &Path) -> Vec<(String, u64)> {
    let Ok(entries) = std::fs::read_dir(root.join("wal")) else {
        return Vec::new();
    };
    let mut segments: Vec<(String, u64)> = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_owned();
            let id = name.strip_suffix(".object@")?;
            (id.len() == 16 && id.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(())?;
            let size = std::fs::metadata(&path).ok()?.len();
            Some((name, size))
        })
        .collect();
    segments.sort();
    segments
}

fn row_key(index: u32) -> Vec<u8> {
    format!("row-{index:05}").into_bytes()
}

/// The oracle for "nothing to replay" is byte-level: after a clean close the
/// WAL holds at most one segment, and it is no larger than the header-only
/// segment a database that never wrote anything leaves behind.
#[test]
fn clean_close_then_reopen_replays_nothing() {
    let dir = tempfile::tempdir().expect("tmp");

    let control_root = dir.path().join("control");
    {
        let mut db = open_durable_database(&control_root).expect("durable open");
        db.close().expect("clean close");
    }
    let control = wal_segments(&control_root);
    assert!(
        control.len() <= 1,
        "a fresh database leaves at most one segment: {control:?}"
    );
    let header_only_bytes = control.iter().map(|(_, bytes)| *bytes).max().unwrap_or(0);

    let root = dir.path().join("db");
    {
        let mut db = open_durable_database(&root).expect("durable open");
        let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
        for index in 0..ROWS {
            kv.put(key(&row_key(index)), value(&[b'v'; 128]))
                .expect("seed write");
        }
        drop(kv);
        assert!(
            wal_segments(&root)
                .iter()
                .any(|(_, bytes)| *bytes > header_only_bytes),
            "the session's rows live in the WAL before the close"
        );
        db.close().expect("clean close checkpoints and truncates");
    }
    let after_close = wal_segments(&root);
    assert!(
        after_close.len() <= 1,
        "the close truncated every covered segment: {after_close:?}"
    );
    assert!(
        after_close
            .iter()
            .all(|(_, bytes)| *bytes <= header_only_bytes),
        "the remaining segment holds no record to replay: {after_close:?} (header-only = {header_only_bytes})"
    );

    {
        let mut db = open_durable_database(&root).expect("reopen after a clean close");
        let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
        for index in 0..ROWS {
            let read = kv
                .get(&key(&row_key(index)))
                .expect("read succeeds")
                .unwrap_or_else(|| panic!("row {index} survives through the snapshot"));
            assert_eq!(read.as_bytes(), &[b'v'; 128]);
        }
        drop(kv);
        db.close().expect("a second clean close with nothing new");
    }
    let after_second_close = wal_segments(&root);
    assert!(
        after_second_close.len() <= 1
            && after_second_close
                .iter()
                .all(|(_, bytes)| *bytes <= header_only_bytes),
        "a session that wrote nothing leaves the WAL record-free: {after_second_close:?}"
    );
}

/// The user branches holding a durable table-manifest base: one `manifest`
/// object per flushed branch under `tables/`.
fn table_manifests(root: &Path) -> usize {
    let Ok(branches) = std::fs::read_dir(root.join("tables")) else {
        return 0;
    };
    branches
        .flatten()
        .filter(|entry| entry.path().join("manifest.object@").is_file())
        .count()
}

/// The island shape (#3493): two user branches that each hold a durable
/// table-manifest base beside the never-flushed `_system_` root. Before the
/// per-branch orphan recovery (space-reclamation contract §3.2, slice 12) no
/// checkpoint could complete over such a database, so its WAL only grew; now
/// a clean close checkpoints every branch, truncates the WAL, and the reopen
/// replays nothing yet reads every row of both branches.
#[test]
fn two_user_branches_with_durable_bases_checkpoint_and_reclaim_wal() {
    const MEMORY_BUDGET: u64 = 8 * 1024 * 1024;
    const VALUE_BYTES: usize = 32 * 1024;
    const ROWS_PER_ROUND: u32 = 16;
    const MAX_ROUNDS: u32 = 32;
    let dir = tempfile::tempdir().expect("tmp");

    let control_root = dir.path().join("control");
    {
        let mut db = open_durable_database(&control_root).expect("durable open");
        db.close().expect("clean close");
    }
    let header_only_bytes = wal_segments(&control_root)
        .iter()
        .map(|(_, bytes)| *bytes)
        .max()
        .unwrap_or(0);

    let root = dir.path().join("db");
    let second = branch("second");
    let payload = vec![b'w'; VALUE_BYTES];
    let mut rows_written = 0u32;
    {
        let mut db = Database::open_local(
            &root,
            DurableLocalOpenOptions::new().with_memory_budget(MEMORY_BUDGET),
        )
        .map(DatabaseOpenOutcome::into_database)
        .expect("budgeted durable open");
        db.branches()
            .expect("branches")
            .create(second.clone())
            .expect("create the second user branch");
        // Write both user branches in rounds until each holds a durable base
        // (its table manifest exists): the budget bounds every memtable, so
        // the background flush publishes each branch's base well within the
        // round cap.
        let deadline = Instant::now() + Duration::from_secs(60);
        for round in 0..MAX_ROUNDS {
            for index in round * ROWS_PER_ROUND..(round + 1) * ROWS_PER_ROUND {
                for name in [branch("default"), second.clone()] {
                    let mut kv = db.kv(name, space("app")).expect("kv opens");
                    kv.put(key(&row_key(index)), value(&payload))
                        .expect("budgeted write");
                }
                rows_written = index + 1;
            }
            if table_manifests(&root) >= 2 {
                break;
            }
        }
        while table_manifests(&root) < 2 {
            assert!(
                Instant::now() < deadline,
                "both user branches flush a durable base under the budget \
                 (manifests={}, rows per branch={rows_written})",
                table_manifests(&root)
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        db.close()
            .expect("clean close checkpoints every branch and truncates");
    }
    let after_close = wal_segments(&root);
    assert!(
        after_close.len() <= 1
            && after_close
                .iter()
                .all(|(_, bytes)| *bytes <= header_only_bytes),
        "the close checkpointed the island shape and truncated the WAL: \
         {after_close:?} (header-only = {header_only_bytes})"
    );
    assert!(
        table_manifests(&root) >= 2,
        "both durable bases survive the close"
    );

    {
        let mut db = open_durable_database(&root).expect("reopen after a clean close");
        for name in [branch("default"), second] {
            let mut kv = db.kv(name.clone(), space("app")).expect("kv opens");
            for index in 0..rows_written {
                let read = kv
                    .get(&key(&row_key(index)))
                    .expect("read succeeds")
                    .unwrap_or_else(|| {
                        panic!("row {index} on {name:?} survives through its base and the snapshot")
                    });
                assert_eq!(read.as_bytes().len(), VALUE_BYTES);
            }
        }
        db.close().expect("a second clean close with nothing new");
    }
}

/// Every empty directory below a family root (`timeline/<id>`, `tables/<b>`,
/// ...), relative to `root`. Family roots themselves are permanent.
fn empty_dirs_below_family_roots(root: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, depth: usize, out: &mut Vec<String>) {
        let entries: Vec<_> = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|entry| entry.expect("dir entry"))
            .collect();
        if entries.is_empty() && depth >= 2 {
            out.push(
                dir.strip_prefix(root)
                    .expect("under root")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        for entry in entries {
            if entry.file_type().expect("file type").is_dir() {
                walk(root, &entry.path(), depth + 1, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, 0, &mut out);
    out.sort();
    out
}

/// #3692: the issue's repro — a loop of single-put sessions, each a clean
/// close that checkpoints, seals timeline segments under a new
/// `timeline/<sealing id>/` and prunes the superseded ones. In 1.2.6 every
/// pruned segment left its directory behind (4 KiB each on ext4: 45 KB of data
/// in 1.7 MB after 401 puts). Now no emptied directory survives, and the
/// timeline keeps only the directories that hold a live segment.
#[test]
fn put_close_loop_leaves_no_empty_timeline_directories() {
    const SESSIONS: u32 = 20;
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    for index in 0..SESSIONS {
        let mut db = open_durable_database(&root).expect("durable open");
        let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
        kv.put(key(b"k"), value(format!("v{index}").as_bytes()))
            .expect("put");
        drop(kv);
        db.close().expect("clean close");
    }

    let timeline_dirs: Vec<_> = std::fs::read_dir(root.join("timeline"))
        .expect("the loop sealed timeline segments")
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .collect();
    assert!(
        !timeline_dirs.is_empty(),
        "the loop sealed timeline segments (the test is not vacuous)"
    );
    assert!(
        timeline_dirs.len() < 4,
        "only live sealing directories remain after {SESSIONS} checkpoints: {} dirs",
        timeline_dirs.len()
    );
    assert_eq!(
        empty_dirs_below_family_roots(&root),
        Vec::<String>::new(),
        "no emptied directory survives the loop"
    );

    let mut db = open_durable_database(&root).expect("reopen");
    let mut kv = db.kv(branch("default"), space("app")).expect("kv opens");
    let read = kv.get(&key(b"k")).expect("read").expect("row");
    assert_eq!(read.as_bytes(), format!("v{}", SESSIONS - 1).as_bytes());
    drop(kv);
    db.close().expect("close");
}
