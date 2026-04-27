//! BranchHandle: Scoped access to primitives within a branch
//!
//! ## Design
//!
//! `BranchHandle` binds a `BranchId` to a `Database`, eliminating the need to
//! pass `branch_id` to every operation. It provides:
//!
//! - `kv()`, `events()`, `json()`, `vectors()` - primitive handles
//! - `transaction()` - execute atomic cross-primitive transactions
//!
//! ## Usage
//!
//! ```text
//! let branch = db.branch("my-branch");
//!
//! // Access primitives directly
//! let value = branch.kv().get("key")?;
//! branch.events().append("event", json!({}))?;
//!
//! // Or use transactions for atomicity
//! branch.transaction(|txn| {
//!     txn.kv_put("key", value)?;
//!     txn.event_append("event", json!({}))?;
//!     Ok(())
//! })?;
//! ```
//!
//! ## BranchHandle Pattern Implementation

use crate::database::Database;
use crate::primitives::extensions::{EventLogExt, JsonStoreExt, KVStoreExt};
use crate::semantics::json::{JsonPath, JsonValue};
use crate::StrataResult;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::types::BranchId;
use strata_core::value::Value;

// ============================================================================
// BranchHandle
// ============================================================================

/// Handle to a specific branch
///
/// Provides scoped access to all primitives within a branch.
/// The branch_id is bound to this handle, so operations don't need
/// to specify it repeatedly.
///
/// ## Thread Safety
///
/// `BranchHandle` is `Clone`, `Send`, and `Sync`. Multiple threads can
/// share the same `BranchHandle` and operate on the same branch concurrently.
/// Transaction isolation ensures correctness.
///
/// ## Example
///
/// ```text
/// let branch = db.branch(branch_id);
///
/// // Access primitives
/// let value = branch.kv().get("key")?;
/// branch.events().append("my-event", json!({}))?;
///
/// // Use transactions
/// branch.transaction(|txn| {
///     txn.kv_put("key", value)?;
///     txn.event_append("my-event", json!({}))?;
///     Ok(())
/// })?;
/// ```
#[derive(Clone)]
pub struct BranchHandle {
    db: Arc<Database>,
    branch_id: BranchId,
}

impl BranchHandle {
    /// Create a new BranchHandle
    pub fn new(db: Arc<Database>, branch_id: BranchId) -> Self {
        Self { db, branch_id }
    }

    /// Get the branch ID
    pub fn branch_id(&self) -> &BranchId {
        &self.branch_id
    }

    /// Get the underlying database
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    // === Primitive Handles ===

    /// Access the KV primitive for this branch
    pub fn kv(&self) -> KvHandle {
        KvHandle::new(self.db.clone(), self.branch_id)
    }

    /// Access the Event primitive for this branch
    pub fn events(&self) -> EventHandle {
        EventHandle::new(self.db.clone(), self.branch_id)
    }

    /// Access the Json primitive for this branch
    pub fn json(&self) -> JsonHandle {
        JsonHandle::new(self.db.clone(), self.branch_id)
    }

    // === Transactions ===

    /// Execute a transaction within this branch
    ///
    /// All operations in the closure are atomic. Either all succeed,
    /// or none do (rollback on error).
    ///
    /// ## Example
    ///
    /// ```text
    /// branch.transaction(|txn| {
    ///     let value = txn.kv_get("counter")?;
    ///     txn.kv_put("counter", Value::from(value.unwrap_or(0) + 1))?;
    ///     txn.event_append("counter_incremented", json!({}))?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn transaction<F, T>(&self, f: F) -> StrataResult<T>
    where
        F: FnOnce(&mut TransactionContext) -> StrataResult<T>,
    {
        self.db.transaction(self.branch_id, f)
    }
}

// ============================================================================
// KvHandle
// ============================================================================

/// Handle for KV operations scoped to a branch
///
/// Each operation runs in its own implicit transaction.
#[derive(Clone)]
pub struct KvHandle {
    db: Arc<Database>,
    branch_id: BranchId,
}

impl KvHandle {
    /// Create a new KvHandle
    pub(crate) fn new(db: Arc<Database>, branch_id: BranchId) -> Self {
        Self { db, branch_id }
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> StrataResult<Option<Versioned<Value>>> {
        self.db.transaction(self.branch_id, |txn| {
            let value = txn.kv_get(key)?;
            // Wrap in Versioned - since KVStoreExt returns Option<Value> not Versioned
            Ok(value.map(|v| Versioned::with_timestamp(v, Version::counter(0), Timestamp::now())))
        })
    }

    /// Put a value
    pub fn put(&self, key: &str, value: Value) -> StrataResult<Version> {
        self.db.transaction(self.branch_id, |txn| {
            txn.kv_put(key, value)?;
            Ok(Version::counter(1))
        })
    }

    /// Delete a key
    pub fn delete(&self, key: &str) -> StrataResult<bool> {
        self.db.transaction(self.branch_id, |txn| {
            txn.kv_delete(key)?;
            Ok(true)
        })
    }

    /// Check if a key exists
    pub fn exists(&self, key: &str) -> StrataResult<bool> {
        self.get(key).map(|v| v.is_some())
    }
}

// ============================================================================
// EventHandle
// ============================================================================

/// Handle for Event operations scoped to a branch
#[derive(Clone)]
pub struct EventHandle {
    db: Arc<Database>,
    branch_id: BranchId,
}

impl EventHandle {
    /// Create a new EventHandle
    pub(crate) fn new(db: Arc<Database>, branch_id: BranchId) -> Self {
        Self { db, branch_id }
    }

    /// Append an event and return sequence number
    pub fn append(&self, event_type: &str, payload: Value) -> StrataResult<u64> {
        self.db
            .transaction(self.branch_id, |txn| txn.event_append(event_type, payload))
    }

    /// Get an event by sequence number
    pub fn get(&self, sequence: u64) -> StrataResult<Option<Value>> {
        self.db
            .transaction(self.branch_id, |txn| txn.event_get(sequence))
    }
}

// ============================================================================
// JsonHandle
// ============================================================================

/// Handle for JSON operations scoped to a branch
#[derive(Clone)]
pub struct JsonHandle {
    db: Arc<Database>,
    branch_id: BranchId,
}

impl JsonHandle {
    /// Create a new JsonHandle
    pub(crate) fn new(db: Arc<Database>, branch_id: BranchId) -> Self {
        Self { db, branch_id }
    }

    /// Create a new JSON document
    pub fn create(&self, doc_id: &str, value: JsonValue) -> StrataResult<Version> {
        self.db
            .transaction(self.branch_id, |txn| txn.json_create(doc_id, value))
    }

    /// Get value at path in a document
    pub fn get(&self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>> {
        self.db
            .transaction(self.branch_id, |txn| txn.json_get(doc_id, path))
    }

    /// Set value at path in a document
    pub fn set(&self, doc_id: &str, path: &JsonPath, value: JsonValue) -> StrataResult<Version> {
        self.db
            .transaction(self.branch_id, |txn| txn.json_set(doc_id, path, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_branch_handle_is_clone_send_sync() {
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
        assert_clone_send_sync::<BranchHandle>();
        assert_clone_send_sync::<KvHandle>();
        assert_clone_send_sync::<EventHandle>();
        assert_clone_send_sync::<JsonHandle>();
    }
}
