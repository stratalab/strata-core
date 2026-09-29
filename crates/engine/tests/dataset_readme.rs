//! The dataset directory's advisory README (#3004), through the real open
//! path: written at creation, self-healed when missing on a later open, and
//! never overwriting a user-edited file.

mod common;

use strata_engine::{Database, DurableLocalOpenOptions};

fn readme_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("README.md")
}

#[test]
fn a_new_durable_database_writes_the_dataset_readme() {
    let root = tempfile::tempdir().expect("tmp");
    let db_dir = root.path().join("db");
    {
        let _db = Database::open_local(&db_dir, DurableLocalOpenOptions::new())
            .expect("durable create opens");
    }
    let content = std::fs::read_to_string(readme_path(&db_dir)).expect("readme written");
    assert!(
        content.contains("Do not read or modify"),
        "the warning is the payload"
    );
    assert!(content.contains("strata . describe"));
    assert!(content.contains("https://stratadb.org/docs"));
}

#[test]
fn a_missing_readme_is_healed_on_reopen_and_edits_survive() {
    let root = tempfile::tempdir().expect("tmp");
    let db_dir = root.path().join("db");
    {
        let _db = Database::open_local(&db_dir, DurableLocalOpenOptions::new())
            .expect("durable create opens");
    }

    // Each handle here is dropped, not closed, so every reopen waits out a
    // detached worker's writer lock (#2837, #3546).
    // A pre-existing dataset (or a deleted file) gains the breadcrumb on the
    // next open.
    std::fs::remove_file(readme_path(&db_dir)).expect("delete readme");
    {
        let _db =
            common::reopen_durable_database_after_drop(&db_dir).expect("reopen heals the readme");
    }
    assert!(
        readme_path(&db_dir).exists(),
        "a missing README is rewritten on open"
    );

    // Direction control: a user-edited file is theirs — reopen never stomps it.
    std::fs::write(readme_path(&db_dir), "my notes\n").expect("user edit");
    {
        let _db = common::reopen_durable_database_after_drop(&db_dir)
            .expect("reopen with an edited readme");
    }
    assert_eq!(
        std::fs::read_to_string(readme_path(&db_dir)).expect("readable"),
        "my notes\n",
        "an existing README is never overwritten"
    );
}
