//! Industry-standard embedded database benchmark (redb suite).
//!
//! Compares Strata against redb, lmdb, rocksdb, sled, fjall, and sqlite
//! on bulk load, individual writes, batch writes, random reads, range scans,
//! multi-threaded reads, and deletes.
//!
//! Adapted from: https://github.com/cberner/redb/tree/master/crates/redb-bench

use std::env::current_dir;
use std::{fs, process};
use tempfile::{NamedTempFile, TempDir};

#[path = "redb_common.rs"]
mod common;
use common::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir_all(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    println!("=== redb benchmark: {} records ===\n", BULK_ELEMENTS);

    // ── Strata ──────────────────────────────────────────────────────────────
    let strata_results = {
        let tmpdir_strata: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        // Write config to strata.toml so Strata::open picks it up
        let config = stratadb::StrataConfig {
            durability: "always".into(),
            ..stratadb::StrataConfig::default()
        };
        let config_path = tmpdir_strata.path().join("strata.toml");
        config.write_to_file(&config_path).unwrap();
        // Open using the product path with full subsystem set
        let db = stratadb::Strata::open(tmpdir_strata.path()).unwrap();
        let table = StrataBenchDatabase::new(db);
        benchmark(table, tmpdir_strata.path())
    };

    // ── redb ────────────────────────────────────────────────────────────────
    let redb_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let mut db = redb::Database::builder()
            .set_cache_size(CACHE_SIZE)
            .create(tmpfile.path())
            .unwrap();
        let table = RedbBenchDatabase::new(&mut db);
        benchmark(table, tmpfile.path())
    };

    // ── lmdb (heed) ─────────────────────────────────────────────────────────
    let lmdb_results = {
        let tempdir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(4096 * 1024 * 1024)
                .open(tempdir.path())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(env);
        benchmark(table, tempdir.path())
    };

    // ── rocksdb ─────────────────────────────────────────────────────────────
    let rocksdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let cache = rocksdb::Cache::new_lru_cache(CACHE_SIZE);
        let write_buffer = rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
            CACHE_SIZE / 2,
            false,
            cache.clone(),
        );

        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&cache);
        bb.set_bloom_filter(10.0, false);
        bb.set_cache_index_and_filter_blocks(true);
        bb.set_pin_l0_filter_and_index_blocks_in_cache(false);
        bb.set_pin_top_level_index_and_filter(false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.set_write_buffer_manager(&write_buffer);
        opts.set_max_write_buffer_size_to_maintain((CACHE_SIZE / 2) as i64);
        opts.create_if_missing(true);
        opts.increase_parallelism(
            std::thread::available_parallelism().map_or(1, |n| n.get()) as i32
        );

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table, tmpfile.path())
    };

    // ── sled ────────────────────────────────────────────────────────────────
    let sled_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let db = sled::Config::new()
            .path(tmpfile.path())
            .cache_capacity(CACHE_SIZE as u64)
            .open()
            .unwrap();

        let table = SledBenchDatabase::new(&db, tmpfile.path());
        benchmark(table, tmpfile.path())
    };

    // ── fjall ───────────────────────────────────────────────────────────────
    let fjall_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let mut db = fjall::Config::new(tmpfile.path())
            .cache_size(CACHE_SIZE.try_into().unwrap())
            .open_transactional()
            .unwrap();

        let table = FjallBenchDatabase::new(&mut db);
        benchmark(table, tmpfile.path())
    };

    // ── sqlite ──────────────────────────────────────────────────────────────
    let sqlite_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let table = SqliteBenchDatabase::new(tmpfile.path());
        benchmark(table, tmpfile.path())
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows = Vec::new();

    for (benchmark, _duration) in &strata_results {
        rows.push(vec![benchmark.to_string()]);
    }

    let results = [
        strata_results,
        redb_results,
        lmdb_results,
        rocksdb_results,
        sled_results,
        fjall_results,
        sqlite_results,
    ];

    let mut identified_smallests = vec![vec![false; results.len()]; rows.len()];
    for (i, identified_smallests_row) in identified_smallests.iter_mut().enumerate() {
        let mut smallest = None;
        for (j, _) in identified_smallests_row.iter().enumerate() {
            let (_, rt) = &results[j][i];
            smallest = match smallest {
                Some((_, prev)) if rt < prev => Some((j, rt)),
                Some((pi, prev)) => Some((pi, prev)),
                None => Some((j, rt)),
            };
        }
        let (j, _rt) = smallest.unwrap();
        identified_smallests_row[j] = true;
    }

    for (j, results) in results.iter().enumerate() {
        for (i, (_benchmark, result_type)) in results.iter().enumerate() {
            rows[i].push(if identified_smallests[i][j] {
                format!("**{result_type}**")
            } else {
                result_type.to_string()
            });
        }
    }

    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_width(120);
    table.set_header([
        "", "strata", "redb", "lmdb", "rocksdb", "sled", "fjall", "sqlite",
    ]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
