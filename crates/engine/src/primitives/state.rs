//! StateCell: CAS-based versioned cells for coordination
//!
//! ## Design Principles
//!
//! 1. **Versioned Updates**: Every update increments the version.
//! 2. **CAS Semantics**: Compare-and-swap ensures safe concurrent updates.
//!
//! ## API
//!
//! All operations go through `db.transaction()` for consistency:
//! - `init`, `read`, `set`, `cas`
//!
//! ## Key Design
//!
//! - TypeTag: State (0x03)
//! - Key format: `<namespace>:<TypeTag::State>:<cell_name>`

use crate::database::{Database, RetryConfig};
use crate::primitives::extensions::StateCellExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Version, Versioned};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::{StrataResult, VersionedHistory};

// Re-export State from core
pub use strata_core::primitives::State;

/// Serialize a struct to Value::String for storage
fn to_stored_value<T: Serialize>(v: &T) -> StrataResult<Value> {
    serde_json::to_string(v)
        .map(Value::String)
        .map_err(|e| strata_core::StrataError::serialization(e.to_string()))
}

/// Deserialize from Value::String storage
fn from_stored_value<T: for<'de> Deserialize<'de>>(
    v: &Value,
) -> std::result::Result<T, serde_json::Error> {
    match v {
        Value::String(s) => serde_json::from_str(s),
        _ => serde_json::from_str("null"), // Will fail with appropriate error
    }
}

/// CAS-based versioned cells for coordination
///
/// ## Design
///
/// Each cell has a value and monotonically increasing version.
/// Updates via CAS ensure safe concurrent access.
///
/// ## Example
///
/// ```text
/// use strata_primitives::StateCell;
/// use strata_core::value::Value;
///
/// let sc = StateCell::new(db.clone());
/// let branch_id = BranchId::new();
///
/// // Initialize a counter
/// sc.init(&branch_id, "counter", Value::Int(0))?;
///
/// // Read current state
/// let state = sc.get(&branch_id, "counter")?.unwrap();
/// assert_eq!(state.value.version, Version::counter(1));
///
/// // CAS update (only succeeds if version matches)
/// sc.cas(&branch_id, "counter", Version::counter(1), Value::Int(1))?;
///
/// // Unconditional set
/// sc.set(&branch_id, "counter", Value::Int(10))?;
/// ```
#[derive(Clone)]
pub struct StateCell {
    db: Arc<Database>,
}

impl StateCell {
    /// Create new StateCell instance
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Namespace {
        Namespace::for_branch_space(*branch_id, space)
    }

    /// Build key for state cell
    fn key_for(&self, branch_id: &BranchId, space: &str, name: &str) -> Key {
        Key::new_state(Arc::new(self.namespace_for(branch_id, space)), name)
    }

    // ========== Read/Init Operations ==========

    /// Initialize a cell with a value (only if it doesn't exist)
    ///
    /// Returns the `Version` assigned to the cell.
    /// The version uses `Version::Counter` type.
    pub fn init(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
        value: Value,
    ) -> StrataResult<Version> {
        self.db.transaction(*branch_id, |txn| {
            let key = self.key_for(branch_id, space, name);

            // Idempotent: if cell already exists, return existing version
            if let Some(existing) = txn.get(&key)? {
                let state: State = from_stored_value(&existing)?;
                return Ok(state.version);
            }

            // Create new state
            let state = State::new(value);
            txn.put(key, to_stored_value(&state)?)?;
            Ok(state.version)
        })
    }

    /// Read current state value.
    ///
    /// Returns the user value, or `None` if the cell doesn't exist.
    /// Use `getv()` to access version metadata and history.
    pub fn get(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
    ) -> StrataResult<Option<Value>> {
        let key = self.key_for(branch_id, space, name);

        self.db.transaction(*branch_id, |txn| match txn.get(&key)? {
            Some(v) => {
                let state: State = from_stored_value(&v)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                Ok(Some(state.value))
            }
            None => Ok(None),
        })
    }

    /// Read current state value with version metadata.
    ///
    /// Reads directly from the committed store (non-transactional) to
    /// retrieve the user value together with its counter version and timestamp.
    /// Returns `None` if the cell doesn't exist.
    pub fn get_versioned(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
    ) -> StrataResult<Option<Versioned<Value>>> {
        let key = self.key_for(branch_id, space, name);
        use strata_core::Storage;
        match self.db.storage().get(&key)? {
            Some(vv) => {
                let state: State = from_stored_value(&vv.value)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                Ok(Some(Versioned::with_timestamp(
                    state.value,
                    state.version,
                    vv.timestamp,
                )))
            }
            None => Ok(None),
        }
    }

    /// Get full version history for a state cell.
    ///
    /// Returns `None` if the cell doesn't exist. Index with `[0]` = latest,
    /// `[1]` = previous, etc. Reads directly from storage (non-transactional).
    ///
    /// Returns `VersionedHistory<Value>` — the internal `State` wrapper is
    /// unwrapped so callers see the user value with storage-layer version/timestamp.
    pub fn getv(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
    ) -> StrataResult<Option<VersionedHistory<Value>>> {
        let key = self.key_for(branch_id, space, name);
        let history = self.db.get_history(&key, None, None)?;
        let versions: Vec<Versioned<Value>> = history
            .iter()
            .filter_map(|vv| {
                let state: State = from_stored_value(&vv.value).ok()?;
                Some(Versioned::with_timestamp(
                    state.value,
                    state.version,
                    vv.timestamp,
                ))
            })
            .collect();
        Ok(VersionedHistory::new(versions))
    }

    // ========== CAS & Set Operations ==========

    /// Compare-and-swap: Update only if version matches
    ///
    /// Returns the new `Version` on success.
    /// Uses `Version::Counter` type.
    pub fn cas(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
        expected_version: Version,
        new_value: Value,
    ) -> StrataResult<Version> {
        let retry_config = RetryConfig::default()
            .with_max_retries(50)
            .with_base_delay_ms(1)
            .with_max_delay_ms(50);
        self.db
            .transaction_with_retry(*branch_id, retry_config, |txn| {
                let key = self.key_for(branch_id, space, name);

                let current: State = match txn.get(&key)? {
                    Some(v) => from_stored_value(&v)
                        .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?,
                    None => {
                        return Err(strata_core::StrataError::invalid_input(format!(
                            "StateCell '{}' not found",
                            name
                        )))
                    }
                };

                if current.version != expected_version {
                    return Err(strata_core::StrataError::conflict(format!(
                        "Version mismatch: expected {:?}, got {:?}",
                        expected_version, current.version
                    )));
                }

                let new_version = current.version.increment();
                let new_state = State {
                    value: new_value.clone(),
                    version: new_version,
                    updated_at: State::now(),
                };

                txn.put(key, to_stored_value(&new_state)?)?;
                Ok(new_state.version)
            })
    }

    /// Unconditional set (force write)
    ///
    /// Always succeeds, overwrites any existing value.
    /// Creates the cell if it doesn't exist.
    ///
    /// Returns the new `Version` assigned to the cell.
    pub fn set(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
        value: Value,
    ) -> StrataResult<Version> {
        let retry_config = RetryConfig::default()
            .with_max_retries(50)
            .with_base_delay_ms(1)
            .with_max_delay_ms(50);
        let value_for_index = value.clone();
        let result = self
            .db
            .transaction_with_retry(*branch_id, retry_config, |txn| {
                let key = self.key_for(branch_id, space, name);

                let new_version = match txn.get(&key)? {
                    Some(v) => {
                        let current: State = from_stored_value(&v)
                            .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                        current.version.increment()
                    }
                    None => Version::counter(1),
                };

                let new_state = State {
                    value: value.clone(),
                    version: new_version,
                    updated_at: State::now(),
                };

                txn.put(key, to_stored_value(&new_state)?)?;
                Ok(new_state.version)
            })?;

        // Update inverted index (zero overhead when disabled)
        let index = self.db.extension::<crate::search::InvertedIndex>()?;
        if index.is_enabled() {
            let text = format!(
                "{} {}",
                name,
                serde_json::to_string(&value_for_index).unwrap_or_default()
            );
            let entity_ref = crate::search::EntityRef::State {
                branch_id: *branch_id,
                name: name.to_string(),
            };
            index.index_document(&entity_ref, &text, None);
        }

        Ok(result)
    }

    // ========== Batch API ==========

    /// Set multiple state cells in a single transaction.
    ///
    /// Each cell is independently read-then-written: existing cells get their
    /// version incremented, new cells start at version 1. All writes share
    /// one lock acquisition, one WAL record, and one commit.
    pub fn batch_set(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: Vec<(String, Value)>,
    ) -> StrataResult<Vec<Result<Version, String>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let retry_config = RetryConfig::default()
            .with_max_retries(50)
            .with_base_delay_ms(1)
            .with_max_delay_ms(50);

        let values_for_index: Vec<(String, Value)> = entries
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();

        let versions = self
            .db
            .transaction_with_retry(*branch_id, retry_config, |txn| {
                let mut versions = Vec::with_capacity(entries.len());
                for (name, value) in &entries {
                    let key = self.key_for(branch_id, space, name);

                    let new_version = match txn.get(&key)? {
                        Some(v) => {
                            let current: State = from_stored_value(&v).map_err(|e| {
                                strata_core::StrataError::serialization(e.to_string())
                            })?;
                            current.version.increment()
                        }
                        None => Version::counter(1),
                    };

                    let new_state = State {
                        value: value.clone(),
                        version: new_version,
                        updated_at: State::now(),
                    };

                    txn.put(key, to_stored_value(&new_state)?)?;
                    versions.push(new_version);
                }
                Ok(versions)
            })?;

        // Post-commit: update inverted index
        let index = self.db.extension::<crate::search::InvertedIndex>()?;
        if index.is_enabled() {
            for (name, value) in &values_for_index {
                let text = format!(
                    "{} {}",
                    name,
                    serde_json::to_string(value).unwrap_or_default()
                );
                let entity_ref = crate::search::EntityRef::State {
                    branch_id: *branch_id,
                    name: name.clone(),
                };
                index.index_document(&entity_ref, &text, None);
            }
        }

        Ok(versions.into_iter().map(Ok).collect())
    }

    // ========== Delete & List Operations ==========

    /// Delete a state cell.
    ///
    /// Returns `true` if the cell existed and was deleted, `false` if it didn't exist.
    pub fn delete(&self, branch_id: &BranchId, space: &str, name: &str) -> StrataResult<bool> {
        let existed = self.db.transaction(*branch_id, |txn| {
            let key = self.key_for(branch_id, space, name);
            let exists = txn.get(&key)?.is_some();
            if exists {
                txn.delete(key)?;
            }
            Ok(exists)
        })?;

        // Remove from inverted index to prevent stale postings
        if existed {
            if let Ok(index) = self.db.extension::<crate::search::InvertedIndex>() {
                if index.is_enabled() {
                    let entity_ref = crate::search::EntityRef::State {
                        branch_id: *branch_id,
                        name: name.to_string(),
                    };
                    index.remove_document(&entity_ref);
                }
            }
        }

        Ok(existed)
    }

    /// List state cell names with optional prefix filter.
    ///
    /// Returns all cell names matching the prefix (or all cells if prefix is None).
    pub fn list(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
    ) -> StrataResult<Vec<String>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = Arc::new(self.namespace_for(branch_id, space));
            let scan_prefix = Key::new_state(ns, prefix.unwrap_or(""));

            let results = txn.scan_prefix(&scan_prefix)?;

            Ok(results
                .into_iter()
                .filter_map(|(key, _)| key.user_key_string())
                .collect())
        })
    }
    // ========== Time-Travel API ==========

    /// Get a state cell value as of a past timestamp (microseconds since epoch).
    ///
    /// Returns the value at the given timestamp, or None if the cell didn't exist then.
    /// This is a non-transactional read directly from the storage version chain.
    pub fn get_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        name: &str,
        as_of_ts: u64,
    ) -> StrataResult<Option<Value>> {
        let key = self.key_for(branch_id, space, name);
        let result = self.db.get_at_timestamp(&key, as_of_ts)?;
        match result {
            Some(vv) => {
                let state: State = from_stored_value(&vv.value)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                Ok(Some(state.value))
            }
            None => Ok(None),
        }
    }

    /// List state cell names as of a past timestamp.
    pub fn list_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        prefix: Option<&str>,
        as_of_ts: u64,
    ) -> StrataResult<Vec<String>> {
        let ns = Arc::new(self.namespace_for(branch_id, space));
        let scan_prefix = Key::new_state(ns, prefix.unwrap_or(""));
        let results = self.db.scan_prefix_at_timestamp(&scan_prefix, as_of_ts)?;
        Ok(results
            .into_iter()
            .filter_map(|(key, _)| key.user_key_string())
            .collect())
    }
}

// ========== Searchable Trait Implementation ==========

impl crate::search::Searchable for StateCell {
    fn search(
        &self,
        _req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        // StateCell does not support search in MVP
        Ok(crate::SearchResponse::empty())
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::State
    }
}

// ========== StateCellExt Implementation ==========

impl StateCellExt for TransactionContext {
    fn state_get(&mut self, name: &str) -> StrataResult<Option<Value>> {
        let ns = Arc::new(Namespace::for_branch(self.branch_id));
        let key = Key::new_state(ns, name);

        match self.get(&key)? {
            Some(v) => {
                let state: State = from_stored_value(&v)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                Ok(Some(state.value))
            }
            None => Ok(None),
        }
    }

    fn state_cas(
        &mut self,
        name: &str,
        expected_version: Version,
        new_value: Value,
    ) -> StrataResult<Version> {
        let ns = Arc::new(Namespace::for_branch(self.branch_id));
        let key = Key::new_state(ns, name);

        let current: State = match self.get(&key)? {
            Some(v) => from_stored_value(&v)
                .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?,
            None => {
                return Err(strata_core::StrataError::invalid_input(format!(
                    "StateCell '{}' not found",
                    name
                )))
            }
        };

        if current.version != expected_version {
            return Err(strata_core::StrataError::conflict(format!(
                "Version mismatch: expected {:?}, got {:?}",
                expected_version, current.version
            )));
        }

        let new_version = current.version.increment();
        let new_state = State {
            value: new_value,
            version: new_version,
            updated_at: State::now(),
        };

        self.put(key, to_stored_value(&new_state)?)?;
        Ok(new_version)
    }

    fn state_set(&mut self, name: &str, value: Value) -> StrataResult<Version> {
        let ns = Arc::new(Namespace::for_branch(self.branch_id));
        let key = Key::new_state(ns, name);

        let new_version = match self.get(&key)? {
            Some(v) => {
                let current: State = from_stored_value(&v)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                current.version.increment()
            }
            None => Version::counter(1),
        };

        let new_state = State {
            value,
            version: new_version,
            updated_at: State::now(),
        };

        self.put(key, to_stored_value(&new_state)?)?;
        Ok(new_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, StateCell) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let sc = StateCell::new(db.clone());
        (temp_dir, db, sc)
    }

    // ========== Core & State Structure Tests ==========

    #[test]
    fn test_state_creation() {
        let state = State::new(Value::Int(42));
        assert_eq!(state.version, Version::counter(1));
        assert!(state.updated_at > 0);
        assert_eq!(state.value, Value::Int(42));
    }

    #[test]
    fn test_state_serialization() {
        let state = State::new(Value::String("test".into()));
        let json = serde_json::to_string(&state).unwrap();
        let restored: State = serde_json::from_str(&json).unwrap();
        assert_eq!(state.value, restored.value);
        assert_eq!(state.version, restored.version);
    }

    #[test]
    fn test_statecell_is_clone() {
        let (_temp, _db, sc) = setup();
        let _sc2 = sc.clone();
    }

    #[test]
    fn test_statecell_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StateCell>();
    }

    // ========== Read/Init Tests ==========

    #[test]
    fn test_init_and_read() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let version = sc
            .init(&branch_id, "default", "counter", Value::Int(0))
            .unwrap();
        assert_eq!(version, Version::counter(1));
        assert!(version.is_counter());

        let value = sc.get(&branch_id, "default", "counter").unwrap().unwrap();
        assert_eq!(value, Value::Int(0));
    }

    #[test]
    fn test_init_is_idempotent() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let v1 = sc
            .init(&branch_id, "default", "cell", Value::Int(42))
            .unwrap();
        // Second init with different value should succeed but return existing version
        let v2 = sc
            .init(&branch_id, "default", "cell", Value::Int(99))
            .unwrap();
        assert_eq!(v1, v2, "Idempotent init should return same version");
    }

    #[test]
    fn test_read_nonexistent() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let result = sc.get(&branch_id, "default", "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_branch_isolation() {
        let (_temp, _db, sc) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        sc.init(&branch1, "default", "shared", Value::Int(1))
            .unwrap();
        sc.init(&branch2, "default", "shared", Value::Int(2))
            .unwrap();

        let value1 = sc.get(&branch1, "default", "shared").unwrap().unwrap();
        let value2 = sc.get(&branch2, "default", "shared").unwrap().unwrap();

        assert_eq!(value1, Value::Int(1));
        assert_eq!(value2, Value::Int(2));
    }

    // ========== CAS & Set Tests ==========

    #[test]
    fn test_cas_success() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "counter", Value::Int(0))
            .unwrap();

        // CAS with correct version
        let new_version = sc
            .cas(
                &branch_id,
                "default",
                "counter",
                Version::counter(1),
                Value::Int(1),
            )
            .unwrap();
        assert_eq!(new_version, Version::counter(2));
        assert!(new_version.is_counter());

        let value = sc.get(&branch_id, "default", "counter").unwrap().unwrap();
        assert_eq!(value, Value::Int(1));
    }

    #[test]
    fn test_cas_conflict() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "counter", Value::Int(0))
            .unwrap();

        // CAS with wrong version
        let result = sc.cas(
            &branch_id,
            "default",
            "counter",
            Version::counter(999),
            Value::Int(1),
        );
        assert!(matches!(
            result,
            Err(strata_core::StrataError::Conflict { .. })
        ));
    }

    #[test]
    fn test_cas_not_found() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let result = sc.cas(
            &branch_id,
            "default",
            "nonexistent",
            Version::counter(1),
            Value::Int(1),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_set_creates_if_not_exists() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let version = sc
            .set(&branch_id, "default", "new-cell", Value::Int(42))
            .unwrap();
        assert_eq!(version, Version::counter(1));

        let value = sc.get(&branch_id, "default", "new-cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(42));
    }

    #[test]
    fn test_set_overwrites() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(1))
            .unwrap();
        let version = sc
            .set(&branch_id, "default", "cell", Value::Int(100))
            .unwrap();
        assert_eq!(version, Version::counter(2));

        let value = sc.get(&branch_id, "default", "cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(100));
    }

    #[test]
    fn test_version_monotonicity() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(0))
            .unwrap();

        for i in 1..=10 {
            let v = sc
                .set(&branch_id, "default", "cell", Value::Int(i))
                .unwrap();
            assert_eq!(v, Version::counter((i + 1) as u64));
        }

        let value = sc.get(&branch_id, "default", "cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(10));
    }

    // ========== StateCellExt Tests ==========

    #[test]
    fn test_statecell_ext_read() {
        let (_temp, db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::String("hello".into()))
            .unwrap();

        let result = db
            .transaction(branch_id, |txn| {
                let value = txn.state_get("cell")?;
                Ok(value)
            })
            .unwrap();

        assert_eq!(result, Some(Value::String("hello".into())));
    }

    #[test]
    fn test_statecell_ext_read_not_found() {
        let (_temp, db, _sc) = setup();
        let branch_id = BranchId::new();

        let result = db
            .transaction(branch_id, |txn| {
                let value = txn.state_get("nonexistent")?;
                Ok(value)
            })
            .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_statecell_ext_cas() {
        let (_temp, db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(1))
            .unwrap();

        let new_version = db
            .transaction(branch_id, |txn| {
                txn.state_cas("cell", Version::counter(1), Value::Int(2))
            })
            .unwrap();

        assert_eq!(new_version, Version::counter(2));

        let value = sc.get(&branch_id, "default", "cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(2));
    }

    #[test]
    fn test_statecell_ext_set() {
        let (_temp, db, sc) = setup();
        let branch_id = BranchId::new();

        let version = db
            .transaction(branch_id, |txn| txn.state_set("new-cell", Value::Int(42)))
            .unwrap();

        assert_eq!(version, Version::counter(1));

        let value = sc.get(&branch_id, "default", "new-cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(42));
    }

    #[test]
    fn test_cross_primitive_transaction() {
        use crate::primitives::extensions::KVStoreExt;

        let (_temp, db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "counter", Value::Int(0))
            .unwrap();

        // Combine KV and StateCell in single transaction
        db.transaction(branch_id, |txn| {
            txn.kv_put("key", Value::String("value".into()))?;
            txn.state_set("counter", Value::Int(1))?;
            Ok(())
        })
        .unwrap();

        // Verify both were written
        let value = sc.get(&branch_id, "default", "counter").unwrap().unwrap();
        assert_eq!(value, Value::Int(1));
    }

    // ========== Read Tests ==========

    #[test]
    fn test_read_returns_correct_value() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(42))
            .unwrap();

        let value = sc.get(&branch_id, "default", "cell").unwrap().unwrap();
        assert_eq!(value, Value::Int(42));
    }

    #[test]
    fn test_read_returns_none_for_missing() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let value = sc.get(&branch_id, "default", "nonexistent").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn test_read_branch_isolation() {
        let (_temp, _db, sc) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        sc.init(&branch1, "default", "shared", Value::Int(1))
            .unwrap();
        sc.init(&branch2, "default", "shared", Value::Int(2))
            .unwrap();

        let value1 = sc.get(&branch1, "default", "shared").unwrap().unwrap();
        let value2 = sc.get(&branch2, "default", "shared").unwrap().unwrap();

        assert_eq!(value1, Value::Int(1));
        assert_eq!(value2, Value::Int(2));
    }

    // ========== Versioned Returns Tests ==========

    #[test]
    fn test_init_has_counter_version() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let version = sc
            .init(&branch_id, "default", "cell", Value::Int(0))
            .unwrap();
        assert_eq!(version, Version::counter(1));
        assert!(version.is_counter());
    }

    #[test]
    fn test_getv_has_counter_version() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(42))
            .unwrap();
        let history = sc.getv(&branch_id, "default", "cell").unwrap().unwrap();

        assert!(history.version().is_counter());
        assert_eq!(history.version(), Version::counter(1));
        assert!(history.timestamp().as_micros() > 0);
        assert_eq!(*history.value(), Value::Int(42));
    }

    #[test]
    fn test_cas_has_counter_version() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(0))
            .unwrap();
        let version = sc
            .cas(
                &branch_id,
                "default",
                "cell",
                Version::counter(1),
                Value::Int(1),
            )
            .unwrap();

        assert!(version.is_counter());
        assert_eq!(version, Version::counter(2));
    }

    // ========== Time-Travel Boundary Tests ==========

    #[test]
    fn test_get_at_exact_versioned_timestamp() {
        // Reproduces: get_versioned returns timestamp T1 (State.updated_at),
        // but get_at compares against storage-layer timestamp T2 (StoredValue).
        // Since T2 > T1, using T1 as as_of fails to find the value.
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.set(&branch_id, "default", "cell", Value::String("v1".into()))
            .unwrap();

        // Capture the timestamp returned by get_versioned
        let versioned = sc
            .get_versioned(&branch_id, "default", "cell")
            .unwrap()
            .unwrap();
        let ts = versioned.timestamp.as_micros();

        // Update to v2
        sc.set(&branch_id, "default", "cell", Value::String("v2".into()))
            .unwrap();

        // Reading at the exact timestamp of v1 should return v1, not None
        let result = sc.get_at(&branch_id, "default", "cell", ts).unwrap();
        assert!(
            result.is_some(),
            "get_at with exact versioned timestamp returned None — \
             timestamp mismatch between State.updated_at and StoredValue.timestamp"
        );
        assert_eq!(result.unwrap(), Value::String("v1".into()));
    }

    // ========== Batch API Tests ==========

    #[test]
    fn test_batch_set_basic() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let entries = vec![
            ("cell1".to_string(), Value::Int(1)),
            ("cell2".to_string(), Value::String("two".into())),
            ("cell3".to_string(), Value::Bool(true)),
        ];

        let results = sc.batch_set(&branch_id, "default", entries).unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
            assert_eq!(*r.as_ref().unwrap(), Version::counter(1));
        }

        // Verify all items persisted
        assert_eq!(
            sc.get(&branch_id, "default", "cell1").unwrap(),
            Some(Value::Int(1))
        );
        assert_eq!(
            sc.get(&branch_id, "default", "cell2").unwrap(),
            Some(Value::String("two".into()))
        );
    }

    #[test]
    fn test_batch_set_empty() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        let results = sc.batch_set(&branch_id, "default", vec![]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_set_increments_existing() {
        let (_temp, _db, sc) = setup();
        let branch_id = BranchId::new();

        sc.init(&branch_id, "default", "cell", Value::Int(0))
            .unwrap();

        let entries = vec![("cell".to_string(), Value::Int(42))];
        let results = sc.batch_set(&branch_id, "default", entries).unwrap();
        assert_eq!(*results[0].as_ref().unwrap(), Version::counter(2));

        assert_eq!(
            sc.get(&branch_id, "default", "cell").unwrap(),
            Some(Value::Int(42))
        );
    }

    #[test]
    fn test_state_delete_removes_from_index() {
        let (_temp, db, sc) = setup();
        let branch_id = BranchId::new();

        // Database::open enables the index
        let index = db.extension::<crate::search::InvertedIndex>().unwrap();
        assert!(index.is_enabled());

        // set() indexes "{name} {json_value}" into the inverted index.
        // Use stemming-stable words: "alpha", "beta", "gamma", "delta".
        sc.set(
            &branch_id,
            "default",
            "cell1",
            Value::String("alpha beta gamma".into()),
        )
        .unwrap();
        sc.set(
            &branch_id,
            "default",
            "cell2",
            Value::String("alpha delta epsilon".into()),
        )
        .unwrap();

        // Both should be indexed
        assert_eq!(index.total_docs(), 2);

        // Use tokenizer to get correct stemmed terms for score_top_k
        let alpha_terms = crate::search::tokenize("alpha");
        let results = index.score_top_k(&alpha_terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(results.len(), 2, "Both cells should match 'alpha'");

        // Delete cell1 — should remove from index
        let deleted = sc.delete(&branch_id, "default", "cell1").unwrap();
        assert!(deleted);

        // Only 1 doc should remain in the index
        assert_eq!(index.total_docs(), 1);

        // Search for "alpha" should only find cell2
        let entity_ref_cell2 = crate::search::EntityRef::State {
            branch_id,
            name: "cell2".to_string(),
        };
        let results = index.score_top_k(&alpha_terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(results.len(), 1);
        let resolved = index.resolve_doc_id(results[0].doc_id).unwrap();
        assert_eq!(resolved, entity_ref_cell2);

        // Term unique to deleted cell should yield no results
        let beta_terms = crate::search::tokenize("beta");
        let results = index.score_top_k(&beta_terms, &branch_id, 10, 0.9, 0.4);
        assert!(
            results.is_empty(),
            "Deleted cell's unique terms should not match"
        );
    }
}
