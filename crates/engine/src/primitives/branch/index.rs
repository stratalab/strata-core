//! BranchIndex: Branch lifecycle management
//!
//! BranchIndex tracks which branches exist. Branches are created or deleted.
//! Export/import is handled by the bundle module.
//!
//! ## MVP Methods
//!
//! - `create_branch(name)` - Create a new branch
//! - `get_branch(name)` - Get branch metadata
//! - `exists(name)` - Check if branch exists
//! - `list_branches()` - List all branch names
//! - `delete_branch(name)` - Delete branch and ALL its data (cascading)
//!
//! ## Key Design
//!
//! - TypeTag: Run (0x05)
//! - Primary key format: `<global_namespace>:<TypeTag::Branch>:<branch_id>`
//! - BranchIndex uses a global namespace (not branch-scoped) since it manages branches themselves.

use crate::database::Database;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::StrataResult;
use tracing::info;
use uuid::Uuid;

/// Namespace UUID for generating deterministic branch IDs from names.
/// Must match the executor's BRANCH_NAMESPACE for consistency.
const BRANCH_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]);

/// Resolve a branch name to a core BranchId using the same logic as the executor.
///
/// - "default" → nil UUID (all zeros)
/// - Valid UUID string → parsed directly
/// - Any other string → deterministic UUID v5 from name
pub fn resolve_branch_name(name: &str) -> BranchId {
    if name == "default" {
        BranchId::from_bytes([0u8; 16])
    } else if let Ok(u) = Uuid::parse_str(name) {
        BranchId::from_bytes(*u.as_bytes())
    } else {
        let uuid = Uuid::new_v5(&BRANCH_NAMESPACE, name.as_bytes());
        BranchId::from_bytes(*uuid.as_bytes())
    }
}

// ========== Global Branch ID for BranchIndex Operations ==========

/// Get the global BranchId used for BranchIndex operations
///
/// BranchIndex is a global index (not scoped to any particular branch),
/// so we use a nil UUID as a sentinel value.
fn global_branch_id() -> BranchId {
    BranchId::from_bytes([0; 16])
}

/// Get the global namespace for BranchIndex operations
fn global_namespace() -> Arc<Namespace> {
    Arc::new(Namespace::for_branch(global_branch_id()))
}

// ========== BranchStatus Enum ==========

/// Branch lifecycle status.
///
/// All branches are Active. Additional statuses will be added when
/// lifecycle transitions are implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum BranchStatus {
    /// Branch is currently active
    #[default]
    Active,
}

impl BranchStatus {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            BranchStatus::Active => "Active",
        }
    }
}

// ========== BranchMetadata Struct ==========

/// Metadata about a branch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BranchMetadata {
    /// User-provided name/key for the branch (used for lookups)
    pub name: String,
    /// Unique branch identifier (UUID) for internal use and namespacing
    pub branch_id: String,
    /// Parent branch name if forked (post-MVP)
    pub parent_branch: Option<String>,

    /// Current status
    pub status: BranchStatus,
    /// Creation timestamp (microseconds since epoch)
    pub created_at: Timestamp,
    /// Last update timestamp (microseconds since epoch)
    pub updated_at: Timestamp,
    /// Completion timestamp (post-MVP)
    pub completed_at: Option<Timestamp>,
    /// Error message if failed (post-MVP)
    pub error: Option<String>,
    /// Internal version counter
    #[serde(default = "default_version")]
    pub version: u64,
}

fn default_version() -> u64 {
    1
}

impl BranchMetadata {
    /// Create new branch metadata with Active status
    pub fn new(name: &str) -> Self {
        let now = Self::now();
        let branch_id = BranchId::new();
        Self {
            name: name.to_string(),
            branch_id: branch_id.to_string(),
            parent_branch: None,
            status: BranchStatus::Active,
            created_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            version: 1,
        }
    }

    /// Wrap this metadata in a Versioned container
    pub fn into_versioned(self) -> Versioned<BranchMetadata> {
        let version = self.version;
        let timestamp = self.updated_at;
        Versioned::with_timestamp(self, Version::counter(version), timestamp)
    }

    /// Get current timestamp in microseconds
    fn now() -> Timestamp {
        Timestamp::now()
    }
}

// ========== Serialization Helpers ==========

/// Serialize a struct to Value::String for storage
fn to_stored_value<T: Serialize>(v: &T) -> StrataResult<Value> {
    serde_json::to_string(v)
        .map(Value::String)
        .map_err(|e| StrataError::serialization(e.to_string()))
}

/// Deserialize from Value::String storage
fn from_stored_value<T: for<'de> Deserialize<'de>>(
    v: &Value,
) -> std::result::Result<T, serde_json::Error> {
    match v {
        Value::String(s) => serde_json::from_str(s),
        _ => serde_json::from_str("null"),
    }
}

// ========== BranchIndex Core ==========

/// Branch lifecycle management primitive (MVP)
///
/// ## Design
///
/// BranchIndex provides branch lifecycle management. It is a stateless facade
/// over the Database engine, holding only an `Arc<Database>` reference.
///
/// ## MVP Methods
///
/// - `create_branch()` - Create a new branch
/// - `get_branch()` - Get branch metadata
/// - `exists()` - Check if branch exists
/// - `list_branches()` - List all branchs
/// - `delete_branch()` - Delete branch and all its data
///
/// ## Example
///
/// ```text
/// let ri = BranchIndex::new(db.clone());
///
/// // Create a branch
/// let meta = ri.create_branch("my-run")?;
/// assert_eq!(meta.value.status, BranchStatus::Active);
///
/// // Check existence
/// assert!(ri.exists("my-run")?);
///
/// // Delete it
/// ri.delete_branch("my-run")?;
/// ```
#[derive(Clone)]
pub struct BranchIndex {
    db: Arc<Database>,
}

impl BranchIndex {
    /// Create new BranchIndex instance
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Build key for branch metadata
    fn key_for(&self, branch_id: &str) -> Key {
        Key::new_branch_with_id(global_namespace(), branch_id)
    }

    // ========== MVP Methods ==========

    /// Create a new branch
    ///
    /// Creates a branch with Active status.
    ///
    /// ## Errors
    /// - `InvalidInput` if branch already exists
    pub fn create_branch(&self, branch_id: &str) -> StrataResult<Versioned<BranchMetadata>> {
        self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);

            // Check if branch already exists
            if txn.get(&key)?.is_some() {
                return Err(StrataError::invalid_input(format!(
                    "Branch '{}' already exists",
                    branch_id
                )));
            }

            let branch_meta = BranchMetadata::new(branch_id);
            txn.put(key, to_stored_value(&branch_meta)?)?;

            info!(target: "strata::branch", %branch_id, "Branch created");
            Ok(branch_meta.into_versioned())
        })
    }

    /// Get branch metadata
    ///
    /// ## Returns
    /// - `Some(Versioned<metadata>)` if branch exists
    /// - `None` if branch doesn't exist
    pub fn get_branch(&self, branch_id: &str) -> StrataResult<Option<Versioned<BranchMetadata>>> {
        self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);
            match txn.get(&key)? {
                Some(v) => {
                    let meta: BranchMetadata = from_stored_value(&v)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(meta.into_versioned()))
                }
                None => Ok(None),
            }
        })
    }

    /// Check if a branch exists
    pub fn exists(&self, branch_id: &str) -> StrataResult<bool> {
        self.db.transaction(global_branch_id(), |txn| {
            let key = self.key_for(branch_id);
            Ok(txn.get(&key)?.is_some())
        })
    }

    /// List all branch IDs
    pub fn list_branches(&self) -> StrataResult<Vec<String>> {
        self.db.transaction(global_branch_id(), |txn| {
            let prefix = Key::new_branch_with_id(global_namespace(), "");
            let results = txn.scan_prefix(&prefix)?;

            Ok(results
                .into_iter()
                .filter_map(|(k, _)| {
                    let key_str = String::from_utf8(k.user_key.to_vec()).ok()?;
                    // Filter out any index keys (legacy data)
                    if key_str.contains("__idx_") {
                        None
                    } else {
                        Some(key_str)
                    }
                })
                .collect())
        })
    }

    /// Delete a branch and ALL its data (cascading delete)
    ///
    /// This deletes:
    /// - The branch metadata
    /// - All branch-scoped data (KV, Events, States, JSON, Vectors)
    ///
    /// USE WITH CAUTION - this is irreversible!
    pub fn delete_branch(&self, branch_id: &str) -> StrataResult<()> {
        // First get the branch metadata (read-only, no WAL after #970)
        let branch_meta = self
            .get_branch(branch_id)?
            .ok_or_else(|| StrataError::invalid_input(format!("Branch '{}' not found", branch_id)))?
            .value;

        // Resolve the executor's deterministic BranchId for this name.
        let executor_branch_id = resolve_branch_name(branch_id);

        // Also get the metadata BranchId (random UUID from BranchMetadata::new).
        let metadata_branch_id = BranchId::from_string(&branch_meta.branch_id);

        let meta_key = self.key_for(branch_id);

        // Single atomic transaction for all delete operations (#974).
        // Deletes branch data from all namespaces + metadata entry.
        self.db.transaction(global_branch_id(), |txn| {
            // Delete data from the executor's namespace
            Self::delete_namespace_data(txn, executor_branch_id)?;

            // If the metadata BranchId differs, also delete from that namespace
            if let Some(meta_id) = metadata_branch_id {
                if meta_id != executor_branch_id {
                    Self::delete_namespace_data(txn, meta_id)?;
                }
            }

            // Delete the branch metadata entry
            txn.delete(meta_key.clone())?;

            info!(target: "strata::branch", %branch_id, "Branch deleted");
            Ok(())
        })
    }

    /// Delete all branch-scoped data within an existing transaction context.
    fn delete_namespace_data(
        txn: &mut strata_concurrency::TransactionContext,
        branch_id: BranchId,
    ) -> StrataResult<()> {
        let ns = Arc::new(Namespace::for_branch(branch_id));

        #[allow(deprecated)]
        for type_tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::State,
            TypeTag::Trace, // Deprecated but kept for backwards compatibility
            TypeTag::Json,
            TypeTag::Vector,
        ] {
            let prefix = Key::new(ns.clone(), type_tag, vec![]);
            let entries = txn.scan_prefix(&prefix)?;

            for (key, _) in entries {
                txn.delete(key)?;
            }
        }

        Ok(())
    }
}

// ========== Searchable Trait Implementation ==========
//
// Search is handled by the intelligence layer.
// This implementation returns empty results.

impl crate::search::Searchable for BranchIndex {
    fn search(
        &self,
        _req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        // Search moved to intelligence layer - return empty results
        Ok(crate::SearchResponse::empty())
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Branch
    }
}

// ========== Tests ==========

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, BranchIndex) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let ri = BranchIndex::new(db.clone());
        (temp_dir, db, ri)
    }

    #[test]
    fn test_create_branch() {
        let (_temp, _db, ri) = setup();

        let result = ri.create_branch("test-run").unwrap();
        assert_eq!(result.value.name, "test-run");
        assert_eq!(result.value.status, BranchStatus::Active);
    }

    #[test]
    fn test_create_branch_duplicate_fails() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();
        let result = ri.create_branch("test-run");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_branch() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();

        let result = ri.get_branch("test-run").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value.name, "test-run");
    }

    #[test]
    fn test_get_branch_not_found() {
        let (_temp, _db, ri) = setup();

        let result = ri.get_branch("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_exists() {
        let (_temp, _db, ri) = setup();

        assert!(!ri.exists("test-run").unwrap());

        ri.create_branch("test-run").unwrap();
        assert!(ri.exists("test-run").unwrap());
    }

    #[test]
    fn test_list_branches() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("run-a").unwrap();
        ri.create_branch("run-b").unwrap();
        ri.create_branch("run-c").unwrap();

        let branches = ri.list_branches().unwrap();
        // Filter out _system_ branch created by init_system_branch
        let user_branches: Vec<_> = branches
            .iter()
            .filter(|b| !b.starts_with("_system"))
            .collect();
        assert_eq!(user_branches.len(), 3);
        assert!(branches.contains(&"run-a".to_string()));
        assert!(branches.contains(&"run-b".to_string()));
        assert!(branches.contains(&"run-c".to_string()));
    }

    #[test]
    fn test_delete_branch() {
        let (_temp, _db, ri) = setup();

        ri.create_branch("test-run").unwrap();
        assert!(ri.exists("test-run").unwrap());

        ri.delete_branch("test-run").unwrap();
        assert!(!ri.exists("test-run").unwrap());
    }

    #[test]
    fn test_delete_branch_not_found() {
        let (_temp, _db, ri) = setup();

        let result = ri.delete_branch("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_branch_status_default() {
        assert_eq!(BranchStatus::default(), BranchStatus::Active);
    }

    #[test]
    fn test_branch_status_as_str() {
        assert_eq!(BranchStatus::Active.as_str(), "Active");
    }
}
