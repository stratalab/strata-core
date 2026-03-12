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

use crate::database::Database;
use crate::BranchIndex;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::collections::BTreeMap;

use strata_core::types::{BranchId, Key, TypeTag};
use strata_core::StrataError;
use strata_core::StrataResult;
use strata_durability::branch_bundle::{
    BranchBundleReader, BranchBundleWriter, BranchlogPayload, BundleBranchInfo, ExportOptions,
};

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

    // 2. Build BundleBranchInfo from metadata
    let bundle_branch_info = BundleBranchInfo {
        branch_id: branch_meta.branch_id.clone(),
        name: branch_meta.name.clone(),
        state: branch_meta.status.as_str().to_lowercase(),
        created_at: format_micros(branch_meta.created_at.as_micros()),
        closed_at: format_micros(branch_meta.completed_at.unwrap_or(branch_meta.updated_at).as_micros()),
        parent_branch_id: branch_meta.parent_branch.clone(),
        error: branch_meta.error.clone(),
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

    // Discover all current keys across all type tags
    let type_tags = [
        TypeTag::KV,
        TypeTag::Event,
        TypeTag::State,
        TypeTag::Json,
        TypeTag::Vector,
    ];

    let mut all_keys: Vec<Key> = Vec::new();
    for type_tag in type_tags {
        let entries = storage.list_by_type(&core_branch_id, type_tag);
        all_keys.extend(entries.into_iter().map(|(k, _)| k));
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
/// # Errors
///
/// - Bundle is invalid or corrupt
/// - Branch with same ID already exists
/// - I/O errors reading the archive
pub fn import_branch(db: &Arc<Database>, path: &Path) -> StrataResult<ImportInfo> {
    // 1. Read and validate bundle
    let contents = BranchBundleReader::read_all(path)
        .map_err(|e| StrataError::storage(format!("Failed to read bundle: {}", e)))?;

    let branch_id_str = &contents.branch_info.name;
    let branch_index = BranchIndex::new(db.clone());

    // 2. Check branch doesn't already exist
    if branch_index.exists(branch_id_str)? {
        return Err(StrataError::invalid_input(format!(
            "Branch '{}' already exists. Delete it first or use a different name.",
            branch_id_str
        )));
    }

    // 3. Create branch via BranchIndex
    branch_index.create_branch(branch_id_str)?;

    // 4. Resolve BranchId for namespace
    let branch_meta = branch_index
        .get_branch(branch_id_str)?
        .ok_or_else(|| {
            StrataError::internal(format!(
                "Branch '{}' was just created but cannot be found",
                branch_id_str
            ))
        })?
        .value;

    let core_branch_id = crate::primitives::branch::resolve_branch_name(&branch_meta.name);

    // 5. Replay each payload as a transaction
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
    let secs = micros / 1_000_000;
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    let years = 1970 + (days / 365);
    let day_of_year = days % 365;
    let month = (day_of_year / 30).min(11) + 1;
    let day = (day_of_year % 30) + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        years, month, day, hours, minutes, seconds
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::types::Namespace;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        (temp_dir, db)
    }

    fn setup_with_branch(branch_name: &str) -> (TempDir, Arc<Database>) {
        let (temp_dir, db) = setup();
        let branch_index = BranchIndex::new(db.clone());
        branch_index.create_branch(branch_name).unwrap();
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

        let import_info = import_branch(&import_db, &bundle_path).unwrap();
        assert_eq!(import_info.branch_id, "export-branch");
        assert!(import_info.transactions_applied > 0);
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
        let ts = 1706_000_000_000_000u64; // approx Jan 2024
        let formatted = format_micros(ts);
        assert!(!formatted.is_empty());
        assert!(formatted.ends_with('Z'));
    }
}
