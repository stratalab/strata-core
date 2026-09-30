//! #3500 — the engine's durable table-compression opt-out, end to end.
//!
//! `DurableLocalOpenOptions::with_table_compression` selects the codec durable
//! tables are written with: `Zstd` (the default since #3499) or
//! `Uncompressed`. Reads succeed under either codec — each block frame records
//! its own — so the only observable proof that the setting reached the table
//! builder is the on-disk size of the tables a flush writes, read here through
//! `Database::storage_footprint` (the path `strata admin storage` prints).
//!
//! Flushing is a `testkit` seam and table files are a durable-only behavior, so
//! the binary compiles away without both features.
#![cfg(all(feature = "testkit", feature = "localfs"))]

mod common;

use common::{assert_branch_value, branch, key, space, value};
use strata_engine::{
    Database, DatabaseOpenOutcome, DurableLocalOpenOptions, FootprintDetail, KvKey, KvValue,
    MaintenanceScheduling, TableCompression,
};

/// Rows per batch; each value is highly compressible, so the two codecs'
/// table sizes differ by far more than any metadata overhead.
const ROWS: usize = 128;
const VALUE_BYTES: usize = 4096;

/// Opens with the inline maintenance scheduler, so no background compaction
/// can rewrite a table between the two footprint reads a size check takes.
fn open_with(path: &std::path::Path, options: DurableLocalOpenOptions) -> Database {
    Database::open_local(
        path,
        options.with_maintenance_scheduling_policy_for_test(
            MaintenanceScheduling::DeterministicInline,
        ),
    )
    .map(DatabaseOpenOutcome::into_database)
    .expect("durable open")
}

fn open(path: &std::path::Path, compression: TableCompression) -> Database {
    open_with(
        path,
        DurableLocalOpenOptions::new().with_table_compression(compression),
    )
}

fn open_default(path: &std::path::Path) -> Database {
    open_with(path, DurableLocalOpenOptions::new())
}

fn row_key(prefix: &str, index: usize) -> Vec<u8> {
    format!("{prefix}-{index:04}").into_bytes()
}

/// A compressible but row-distinct value: a repeated phrase tagged with the
/// row index, so a read back proves the right row came back.
fn row_value(prefix: &str, index: usize) -> Vec<u8> {
    let mut bytes = format!("{prefix}-{index:04}:").into_bytes();
    bytes.extend(
        b"branch commit table snapshot "
            .iter()
            .copied()
            .cycle()
            .take(VALUE_BYTES),
    );
    bytes
}

/// Writes one batch under `prefix` on the default branch and flushes it into
/// a table; returns the logical value bytes written.
fn write_and_flush(db: &mut Database, prefix: &str) -> u64 {
    let entries: Vec<(KvKey, KvValue)> = (0..ROWS)
        .map(|i| (key(&row_key(prefix, i)), value(&row_value(prefix, i))))
        .collect();
    let bytes = entries.iter().map(|(_, v)| v.as_bytes().len() as u64).sum();
    db.kv(branch("default"), space("default"))
        .expect("kv opens")
        .put_batch(entries)
        .expect("batch commits");
    let flushed = db
        .flush_storage_branch_for_test(&branch("default"))
        .expect("flush");
    assert!(flushed > 0, "the flush must build a table");
    bytes
}

fn assert_batch_reads(db: &mut Database, prefix: &str) {
    for i in 0..ROWS {
        assert_branch_value(
            db,
            "default",
            "default",
            &row_key(prefix, i),
            &row_value(prefix, i),
        );
    }
}

fn live_table_bytes(db: &mut Database) -> u64 {
    db.storage_footprint(None, FootprintDetail::Live)
        .expect("footprint")
        .live_table_bytes
}

/// The opt-out writes uncompressed tables and the default writes Zstd ones.
/// Both databases are open at once in the same process, so the codec is a
/// per-database setting, not process-global state (Hard Rule 9).
#[test]
fn opt_out_writes_uncompressed_tables_and_the_default_stays_zstd() {
    let zstd_dir = tempfile::tempdir().expect("tempdir");
    let raw_dir = tempfile::tempdir().expect("tempdir");
    let mut zstd_db = open_default(zstd_dir.path());
    let mut raw_db = open(raw_dir.path(), TableCompression::Uncompressed);

    let zstd_before = live_table_bytes(&mut zstd_db);
    let raw_before = live_table_bytes(&mut raw_db);
    // Interleave the two databases' writes: a process-global codec would
    // leak whichever was set last into both.
    let logical = write_and_flush(&mut raw_db, "a");
    write_and_flush(&mut zstd_db, "a");
    let zstd_table = live_table_bytes(&mut zstd_db) - zstd_before;
    let raw_table = live_table_bytes(&mut raw_db) - raw_before;

    // An uncompressed table stores every value byte verbatim.
    assert!(
        raw_table >= logical,
        "Uncompressed tables must hold the payload verbatim: {raw_table} table bytes for \
         {logical} logical bytes"
    );
    // Zstd shrinks this repetitive payload by far more than 4x.
    assert!(
        zstd_table.saturating_mul(4) < raw_table,
        "the default must stay Zstd: {zstd_table} Zstd table bytes vs {raw_table} uncompressed"
    );

    assert_batch_reads(&mut zstd_db, "a");
    assert_batch_reads(&mut raw_db, "a");
    zstd_db.close().expect("close");
    raw_db.close().expect("close");
}

/// Asserts a freshly flushed table's size matches `codec` for a batch of
/// `logical` value bytes: verbatim for `Uncompressed`, under a quarter for
/// `Zstd` on this repetitive payload.
fn assert_table_codec(codec: TableCompression, table_bytes: u64, logical: u64) {
    match codec {
        TableCompression::Uncompressed => assert!(
            table_bytes >= logical,
            "expected an uncompressed table: {table_bytes} bytes for {logical} logical"
        ),
        TableCompression::Zstd => assert!(
            table_bytes.saturating_mul(4) < logical,
            "expected a Zstd table: {table_bytes} bytes for {logical} logical"
        ),
        // `TableCompression` is `#[non_exhaustive]`; a new codec needs an
        // expectation here.
        _ => panic!("unhandled TableCompression {codec:?}"),
    }
}

/// A database written under one codec reopens and reads under the other, and
/// keeps reading after a table of the second codec lands beside the first:
/// the codec is recorded per block, so tables of both codecs coexist.
fn switch_codec_and_read_back(first: TableCompression, second: TableCompression) {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let mut db = open(dir.path(), first);
        let before = live_table_bytes(&mut db);
        let logical = write_and_flush(&mut db, "first");
        assert_table_codec(first, live_table_bytes(&mut db) - before, logical);
        db.close().expect("close");
    }
    {
        let mut db = open(dir.path(), second);
        assert_batch_reads(&mut db, "first");
        let before = live_table_bytes(&mut db);
        let logical = write_and_flush(&mut db, "second");
        // The reopened setting governs the new table even though the
        // database already holds a table of the other codec.
        assert_table_codec(second, live_table_bytes(&mut db) - before, logical);
        assert_batch_reads(&mut db, "first");
        assert_batch_reads(&mut db, "second");
        db.close().expect("close");
    }
    // And back again: both tables stay readable under the original setting.
    let mut db = open(dir.path(), first);
    assert_batch_reads(&mut db, "first");
    assert_batch_reads(&mut db, "second");
    db.close().expect("close");
}

#[test]
fn uncompressed_database_reopens_and_reads_under_zstd() {
    switch_codec_and_read_back(TableCompression::Uncompressed, TableCompression::Zstd);
}

#[test]
fn zstd_database_reopens_and_reads_uncompressed() {
    switch_codec_and_read_back(TableCompression::Zstd, TableCompression::Uncompressed);
}
