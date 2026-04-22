//! Engine-level branch export/import API
//!
//! This module provides high-level functions for exporting and importing
//! branches as `.branchbundle.tar.zst` archives. It bridges the engine's
//! `Database`/`BranchIndex` types with the durability crate's BranchBundle format.
//!
//! ## Export
//!
//! Exports scan the KV store for all keys in a branch's namespace and
//! reconstruct `BranchlogPayload` records grouped by version.
//!
//! ## Import
//!
//! Imports replay each `BranchlogPayload` as a transaction, writing puts
//! and deletes into the target database.

use crate::branch_ops::branch_control_store::BranchControlStore;
use crate::database::Database;
use crate::primitives::branch::BranchIndex;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::collections::BTreeMap;

use strata_core::types::{BranchId, Key, TypeTag};
use strata_core::{StrataError, StrataResult};
use strata_durability::branch_bundle::{
    BranchBundleReader, BranchBundleWriter, BranchlogPayload, BundleBranchInfo, ExportOptions,
};
use tracing::info;

// =============================================================================
// Public result types
// =============================================================================

/// Information returned after exporting a branch
#[derive(Debug, Clone)]
pub struct ExportInfo {
    /// Branch ID of the exported branch
    pub branch_id: String,
    /// Path where the bundle was written
    pub path: PathBuf,
    /// Number of transaction payloads in the bundle
    pub entry_count: u64,
    /// Size of the bundle file in bytes
    pub bundle_size: u64,
}

/// Information returned after importing a branch
#[derive(Debug, Clone)]
pub struct ImportInfo {
    /// Branch ID of the imported branch
    pub branch_id: String,
    /// Number of transactions applied
    pub transactions_applied: u64,
    /// Total number of keys written
    pub keys_written: u64,
}

/// Information about a bundle (from validation)
#[derive(Debug, Clone)]
pub struct BundleInfo {
    /// Branch ID from the bundle
    pub branch_id: String,
    /// Format version of the bundle
    pub format_version: u32,
    /// Number of transaction payloads
    pub entry_count: u64,
    /// Whether all checksums are valid
    pub checksums_valid: bool,
}

// =============================================================================
// Export
// =============================================================================

/// Export a branch to a `.branchbundle.tar.zst` archive
///
/// The branch must exist in the database. All data in the branch's namespace
/// is scanned and grouped into per-version `BranchlogPayload` records.
///
/// # Errors
///
/// - Branch does not exist
/// - I/O errors writing the archive
pub fn export_branch(db: &Arc<Database>, branch_id: &str, path: &Path) -> StrataResult<ExportInfo> {
    export_branch_with_options(db, branch_id, path, &ExportOptions::default())
}

/// Export a branch with custom options (e.g., compression level)
pub fn export_branch_with_options(
    db: &Arc<Database>,
    branch_id: &str,
    path: &Path,
    options: &ExportOptions,
) -> StrataResult<ExportInfo> {
    let branch_index = BranchIndex::new(db.clone());

    // 1. Verify branch exists and get metadata
    let branch_meta = branch_index
        .get_branch(branch_id)?
        .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", branch_id)))?
        .value;

    // 2. Build BundleBranchInfo from metadata.
    //
    // B3.4 (AD7): record the source-DB lifecycle generation so an import
    // onto a fresh target can preserve it. Missing active control state
    // here is corruption once B3 is landed; bundle export must not invent
    // `gen 0` and silently mislabel the lifecycle instance.
    let generation = BranchControlStore::new(db.clone())
        .find_active_by_name(&branch_meta.name)?
        .map(|rec| rec.branch.generation)
        .ok_or_else(|| {
            StrataError::corruption(format!(
                "branch '{}' has legacy metadata but no active control record during export",
                branch_meta.name
            ))
        })?;
    let bundle_branch_info = BundleBranchInfo {
        branch_id: branch_meta.branch_id.clone(),
        name: branch_meta.name.clone(),
        state: branch_meta.status.as_str().to_lowercase(),
        created_at: format_micros(branch_meta.created_at.as_micros()),
        closed_at: format_micros(
            branch_meta
                .completed_at
                .unwrap_or(branch_meta.updated_at)
                .as_micros(),
        ),
        parent_branch_id: branch_meta.parent_branch.clone(),
        error: branch_meta.error.clone(),
        generation,
    };

    // 3. Scan storage for all branch data -> Vec<BranchlogPayload>
    let core_branch_id = crate::primitives::branch::resolve_branch_name(&branch_meta.name);

    let payloads = scan_branch_data(db, core_branch_id, branch_id)?;

    // 4. Write bundle
    let writer = BranchBundleWriter::new(options);
    let export_info = writer
        .write(&bundle_branch_info, &payloads, path)
        .map_err(|e| StrataError::storage(format!("Failed to write bundle: {}", e)))?;

    Ok(ExportInfo {
        branch_id: branch_id.to_string(),
        path: export_info.path,
        entry_count: export_info.wal_entry_count,
        bundle_size: export_info.bundle_size_bytes,
    })
}

/// Scan all data in a branch's namespace and group into BranchlogPayload records.
///
/// Uses the storage layer's version history to produce one payload per commit
/// version, preserving the full version chain for import.
fn scan_branch_data(
    db: &Arc<Database>,
    core_branch_id: BranchId,
    branch_id_str: &str,
) -> StrataResult<Vec<BranchlogPayload>> {
    let storage = db.storage();

    // Capture snapshot version for consistent reads (#1920)
    let snapshot_version = db.current_version();

    // Discover all current keys across all type tags
    let type_tags = [
        TypeTag::KV,
        TypeTag::Event,
        TypeTag::Json,
        TypeTag::Vector,
        TypeTag::Graph,
    ];

    let mut all_keys: Vec<Key> = Vec::new();
    for type_tag in type_tags {
        let entries = storage.list_by_type_at_version(&core_branch_id, type_tag, snapshot_version);
        all_keys.extend(
            entries
                .into_iter()
                .filter(|e| !e.is_tombstone)
                .map(|e| e.key),
        );
    }

    if all_keys.is_empty() {
        return Ok(vec![]);
    }

    // For each key, get the full version history and group entries by version.
    // BTreeMap keeps versions sorted ascending for deterministic replay order.
    let mut version_groups: BTreeMap<u64, Vec<(Key, strata_core::value::Value)>> = BTreeMap::new();

    for key in &all_keys {
        let history = db.get_history(key, None, None)?;
        for vv in history {
            let ver = match vv.version {
                strata_core::Version::Counter(v)
                | strata_core::Version::Txn(v)
                | strata_core::Version::Sequence(v) => v,
            };
            version_groups
                .entry(ver)
                .or_default()
                .push((key.clone(), vv.value));
        }
    }

    // Convert grouped entries into payloads sorted by version
    let payloads: Vec<BranchlogPayload> = version_groups
        .into_iter()
        .map(|(version, puts)| BranchlogPayload {
            branch_id: branch_id_str.to_string(),
            version,
            puts,
            deletes: vec![],
        })
        .collect();

    Ok(payloads)
}

// =============================================================================
// Import
// =============================================================================

/// Import a branch from a `.branchbundle.tar.zst` archive
///
/// Creates the branch in the database and replays all transaction payloads.
///
/// ## Generation handling (B3.4 / AD7)
///
/// - If the target DB has any record (active OR tombstoned) for the
///   bundle's branch name, the import allocates a fresh generation from
///   the target's `next_gen` counter and ignores the bundle's
///   `generation`. The imported branch becomes a new lifecycle instance
///   in the target.
/// - Otherwise the bundle's `generation` is preserved verbatim and the
///   target's `next_gen` is seeded to `generation + 1` so a later
///   recreate allocates a strictly-larger generation.
///
/// In both cases the import always materialises a brand-new control
/// record — source-DB identity is not reattached.
///
/// # Errors
///
/// - Bundle is invalid or corrupt
/// - Branch with same name is currently active in the target
/// - I/O errors reading the archive
pub fn import_branch(db: &Arc<Database>, path: &Path) -> StrataResult<ImportInfo> {
    // 1. Read and validate bundle
    let contents = BranchBundleReader::read_all(path)
        .map_err(|e| StrataError::storage(format!("Failed to read bundle: {}", e)))?;

    let branch_id_str = &contents.branch_info.name;
    // 2. Create the imported branch through the canonical branch-service
    //    mutation boundary. This keeps import on the same control-store,
    //    rollback, DAG, and observer path as ordinary branch creation while
    //    still honoring AD7's generation-allocation rules.
    let canonical_id = BranchId::from_user_name(branch_id_str);
    let bundle_generation = contents.branch_info.generation;
    let (_branch_meta, _branch_ref, collision) = db
        .branches()
        .create_imported_branch(branch_id_str, bundle_generation)?;
    if collision {
        info!(
            target: "strata::bundle",
            name = %branch_id_str,
            bundle_generation,
            "Bundle generation ignored; allocated fresh generation due to existing lineage in target"
        );
    }

    // 3. Resolve BranchId for namespace
    let core_branch_id = canonical_id;

    // 4. Replay each payload as a transaction
    let mut transactions_applied = 0u64;
    let mut keys_written = 0u64;

    for payload in &contents.payloads {
        let put_count = payload.puts.len() as u64;

        db.transaction(core_branch_id, |txn| {
            // Apply puts
            for (key, value) in &payload.puts {
                txn.put(key.clone(), value.clone())?;
            }

            // Apply deletes
            for key in &payload.deletes {
                txn.delete(key.clone())?;
            }

            Ok(())
        })?;

        transactions_applied += 1;
        keys_written += put_count;
    }

    // Note: the DAG create event already fired from inside
    // `BranchService::create_imported_branch(...)` above. We do NOT fire it
    // again here — doing so would upsert the branch node and clobber the
    // timestamps from the first fire. Branch import shows up in the lineage
    // graph as a freshly-created branch automatically.

    Ok(ImportInfo {
        branch_id: branch_id_str.to_string(),
        transactions_applied,
        keys_written,
    })
}

// =============================================================================
// Validate
// =============================================================================

/// Validate a bundle without importing it
///
/// Checks the archive structure, checksums, and format version.
pub fn validate_bundle(path: &Path) -> StrataResult<BundleInfo> {
    let verify = BranchBundleReader::validate(path)
        .map_err(|e| StrataError::storage(format!("Bundle validation failed: {}", e)))?;

    Ok(BundleInfo {
        branch_id: verify.branch_id,
        format_version: verify.format_version,
        entry_count: verify.wal_entry_count,
        checksums_valid: verify.checksums_valid,
    })
}

// =============================================================================
// Helpers
// =============================================================================

/// Format microsecond timestamp as ISO 8601 string
fn format_micros(micros: u64) -> String {
    use chrono::DateTime;
    DateTime::from_timestamp_micros(micros as i64)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| format!("{}µs", micros))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch_ops::branch_control_store::{active_ptr_key, control_record_key};
    use crate::database::dag_hook::{
        AncestryEntry, BranchDagError, BranchDagHook, DagEvent, DagEventKind, MergeBaseResult,
    };
    use std::sync::Arc;
    use strata_core::types::Namespace;
    use tempfile::TempDir;

    #[derive(Default)]
    struct RecordingDagHook {
        events: parking_lot::Mutex<Vec<DagEvent>>,
    }

    impl RecordingDagHook {
        fn events(&self) -> Vec<DagEvent> {
            self.events.lock().clone()
        }
    }

    impl BranchDagHook for RecordingDagHook {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn record_event(&self, event: &DagEvent) -> Result<(), BranchDagError> {
            self.events.lock().push(event.clone());
            Ok(())
        }

        fn find_merge_base(
            &self,
            _branch_a: &str,
            _branch_b: &str,
        ) -> Result<Option<MergeBaseResult>, BranchDagError> {
            Ok(None)
        }

        fn log(&self, _branch: &str, _limit: usize) -> Result<Vec<DagEvent>, BranchDagError> {
            Ok(self.events())
        }

        fn ancestors(&self, _branch: &str) -> Result<Vec<AncestryEntry>, BranchDagError> {
            Ok(Vec::new())
        }
    }

    fn setup() -> (TempDir, Arc<Database>) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        (temp_dir, db)
    }

    fn setup_with_branch(branch_name: &str) -> (TempDir, Arc<Database>) {
        let (temp_dir, db) = setup();
        db.branches().create(branch_name).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_export_branch_exists() {
        let (temp_dir, db) = setup_with_branch("test-branch");
        let path = temp_dir.path().join("test.branchbundle.tar.zst");

        let result = export_branch(&db, "test-branch", &path);
        assert!(result.is_ok());

        let info = result.unwrap();
        assert_eq!(info.branch_id, "test-branch");
        assert!(info.path.exists());
    }

    #[test]
    fn test_export_branch_not_found() {
        let (temp_dir, db) = setup();
        let path = temp_dir.path().join("test.branchbundle.tar.zst");

        let result = export_branch(&db, "nonexistent", &path);
        assert!(result.is_err());
    }

    #[test]
    fn test_export_with_data() {
        let (temp_dir, db) = setup_with_branch("data-branch");

        // Write some data to the branch
        let branch_index = BranchIndex::new(db.clone());
        let meta = branch_index
            .get_branch("data-branch")
            .unwrap()
            .unwrap()
            .value;
        let core_branch_id = crate::primitives::branch::resolve_branch_name(&meta.name);
        let ns = Arc::new(Namespace::for_branch(core_branch_id));

        db.transaction(core_branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, b"key1".to_vec()),
                strata_core::value::Value::String("value1".to_string()),
            )?;
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, b"key2".to_vec()),
                strata_core::value::Value::Int(42),
            )?;
            Ok(())
        })
        .unwrap();

        let path = temp_dir.path().join("data.branchbundle.tar.zst");
        let info = export_branch(&db, "data-branch", &path).unwrap();

        assert_eq!(info.branch_id, "data-branch");
        assert!(info.entry_count > 0);
        assert!(info.bundle_size > 0);
    }

    #[test]
    fn test_validate_bundle() {
        let (temp_dir, db) = setup_with_branch("validate-branch");
        let path = temp_dir.path().join("validate.branchbundle.tar.zst");

        export_branch(&db, "validate-branch", &path).unwrap();

        let info = validate_bundle(&path).unwrap();
        assert!(!info.branch_id.is_empty());
        assert!(info.checksums_valid);
    }

    #[test]
    fn test_validate_nonexistent_bundle() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nonexistent.branchbundle.tar.zst");

        let result = validate_bundle(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_branch() {
        let (temp_dir, db) = setup_with_branch("export-branch");

        // Write data
        let branch_index = BranchIndex::new(db.clone());
        let meta = branch_index
            .get_branch("export-branch")
            .unwrap()
            .unwrap()
            .value;
        let core_branch_id = crate::primitives::branch::resolve_branch_name(&meta.name);
        let ns = Arc::new(Namespace::for_branch(core_branch_id));

        db.transaction(core_branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, b"key1".to_vec()),
                strata_core::value::Value::String("hello".to_string()),
            )?;
            Ok(())
        })
        .unwrap();

        // Export
        let bundle_path = temp_dir.path().join("export.branchbundle.tar.zst");
        export_branch(&db, "export-branch", &bundle_path).unwrap();

        // Import into a fresh database
        let import_dir = TempDir::new().unwrap();
        let import_db = Database::open(import_dir.path()).unwrap();
        let hook = Arc::new(RecordingDagHook::default());
        import_db.dag_hook().install(hook.clone()).unwrap();

        let import_info = import_branch(&import_db, &bundle_path).unwrap();
        assert_eq!(import_info.branch_id, "export-branch");
        assert!(import_info.transactions_applied > 0);
        let control = import_db
            .branches()
            .control_record("export-branch")
            .unwrap()
            .expect("imported branch should have a control record");
        assert_eq!(control.branch.generation, 0);

        let events = hook.events();
        let create = events
            .iter()
            .find(|event| event.kind == DagEventKind::BranchCreate)
            .expect("imported branch should emit a canonical create event");
        assert_eq!(
            create.branch_ref.map(|branch| branch.generation),
            Some(0),
            "import must record the imported lifecycle generation through the canonical DAG path"
        );
    }

    #[test]
    fn test_imported_branch_recreate_bumps_generation() {
        let (temp_dir, db) = setup_with_branch("export-branch");
        let bundle_path = temp_dir.path().join("export.branchbundle.tar.zst");
        export_branch(&db, "export-branch", &bundle_path).unwrap();

        let import_dir = TempDir::new().unwrap();
        let import_db = Database::open(import_dir.path()).unwrap();
        import_branch(&import_db, &bundle_path).unwrap();

        import_db.branches().delete("export-branch").unwrap();
        import_db.branches().create("export-branch").unwrap();

        let control = import_db
            .branches()
            .control_record("export-branch")
            .unwrap()
            .expect("recreated imported branch should stay on the control-store path");
        assert_eq!(control.branch.generation, 1);
    }

    #[test]
    fn test_import_duplicate_branch_fails() {
        let (temp_dir, db) = setup_with_branch("dup-branch");

        let path = temp_dir.path().join("dup.branchbundle.tar.zst");
        export_branch(&db, "dup-branch", &path).unwrap();

        // Importing into same db should fail (branch already exists)
        let result = import_branch(&db, &path);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_rejects_active_control_record_without_metadata() {
        let (temp_dir, db) = setup_with_branch("split-brain");
        let path = temp_dir.path().join("split.branchbundle.tar.zst");
        export_branch(&db, "split-brain", &path).unwrap();

        let branch_ref = db
            .branches()
            .control_record("split-brain")
            .unwrap()
            .unwrap()
            .branch;
        let metadata_key = Key::new_branch_with_id(
            Arc::new(Namespace::for_branch(BranchId::from_bytes([0; 16]))),
            "split-brain",
        );
        db.transaction(BranchId::from_bytes([0; 16]), |txn| {
            txn.set_allow_cross_branch(true);
            txn.delete(metadata_key.clone())?;
            Ok(())
        })
        .unwrap();

        let err = import_branch(&db, &path).unwrap_err();
        assert!(
            err.to_string()
                .contains("active control record but no legacy metadata"),
            "unexpected error: {err}"
        );

        // The authoritative active lifecycle must remain untouched.
        let rec = db
            .branches()
            .control_record("split-brain")
            .unwrap()
            .unwrap();
        assert_eq!(rec.branch, branch_ref);
    }

    #[test]
    fn test_export_rejects_metadata_without_active_control_record() {
        let (temp_dir, db) = setup_with_branch("broken-export");
        let path = temp_dir.path().join("broken.branchbundle.tar.zst");

        let branch_ref = db
            .branches()
            .control_record("broken-export")
            .unwrap()
            .unwrap()
            .branch;
        db.transaction(BranchId::from_bytes([0; 16]), |txn| {
            txn.set_allow_cross_branch(true);
            txn.delete(control_record_key(branch_ref))?;
            txn.delete(active_ptr_key(branch_ref.id))?;
            Ok(())
        })
        .unwrap();

        let err = export_branch(&db, "broken-export", &path).unwrap_err();
        assert!(
            err.to_string()
                .contains("legacy metadata but no active control record during export"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_export_empty_branch() {
        let (temp_dir, db) = setup_with_branch("empty-branch");
        let path = temp_dir.path().join("empty.branchbundle.tar.zst");

        let info = export_branch(&db, "empty-branch", &path).unwrap();
        assert_eq!(info.entry_count, 0);
    }

    #[test]
    fn test_format_micros() {
        // Epoch should be 1970
        assert!(format_micros(0).starts_with("1970-"));

        // Some timestamp in 2025
        let ts = 1_706_000_000_000_000u64; // approx Jan 2024
        let formatted = format_micros(ts);
        assert!(!formatted.is_empty());
        assert!(formatted.ends_with('Z'));
    }

    #[test]
    fn test_issue_1920_export_branch_snapshot_consistency() {
        // Verify that export_branch uses snapshot-isolated reads so that
        // the exported bundle captures a consistent point-in-time view.
        let (temp_dir, db) = setup_with_branch("snap-export");

        // Write data to the branch
        let branch_index = BranchIndex::new(db.clone());
        let meta = branch_index
            .get_branch("snap-export")
            .unwrap()
            .unwrap()
            .value;
        let core_branch_id = crate::primitives::branch::resolve_branch_name(&meta.name);
        let ns = Arc::new(Namespace::for_branch(core_branch_id));

        db.transaction(core_branch_id, |txn| {
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, b"k1".to_vec()),
                strata_core::value::Value::String("v1".to_string()),
            )?;
            txn.put(
                Key::new(ns.clone(), TypeTag::KV, b"k2".to_vec()),
                strata_core::value::Value::Int(42),
            )?;
            Ok(())
        })
        .unwrap();

        // Export should succeed with snapshot-isolated reads
        let path = temp_dir.path().join("snap.branchbundle.tar.zst");
        let info = export_branch(&db, "snap-export", &path).unwrap();

        assert_eq!(info.branch_id, "snap-export");
        assert!(info.entry_count > 0);
        assert!(info.bundle_size > 0);

        // Validate the bundle is well-formed
        let bundle_info = validate_bundle(&path).unwrap();
        assert!(bundle_info.checksums_valid);
    }
}
