//! Space-reclamation contract §3.1 (slice 7): a clean close checkpoints every
//! branch — the product default and the engine's `_system_` root, which never
//! flushes — and truncates the WAL behind the snapshot, so the next open
//! replays nothing and still reads every row.
#![cfg(feature = "localfs")]

mod common;

use std::path::Path;

use common::{branch, key, open_durable_database, space, value};

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
