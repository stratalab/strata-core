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
use crate::transaction_ops::TransactionOps;
use crate::StrataResult;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::BranchId;
use strata_core::Value;
use strata_storage::Namespace;

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

/// Branch-scoped transaction wrapper for public branch transactions.
///
/// This wrapper preserves the generic lower transaction context for raw KV/event
/// operations while routing JSON document semantics through the engine-owned
/// scoped transaction surface.
pub struct BranchTransaction<'a> {
    txn: &'a mut crate::transaction::Transaction,
}

impl<'a> BranchTransaction<'a> {
    fn new(txn: &'a mut crate::transaction::Transaction) -> Self {
        Self { txn }
    }
}

impl Deref for BranchTransaction<'_> {
    type Target = crate::transaction::Transaction;

    fn deref(&self) -> &Self::Target {
        self.txn
    }
}

impl DerefMut for BranchTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.txn
    }
}

impl JsonStoreExt for BranchTransaction<'_> {
    fn json_get_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Option<JsonValue>> {
        let namespace = Arc::new(Namespace::for_branch_space(self.txn.branch_id(), space));
        let mut scoped = self.txn.scoped(namespace);
        scoped.json_get_path(doc_id, path)
    }

    fn json_set_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        let namespace = Arc::new(Namespace::for_branch_space(self.txn.branch_id(), space));
        let mut scoped = self.txn.scoped(namespace);
        scoped.json_set(doc_id, path, value)
    }

    fn json_create_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        value: JsonValue,
    ) -> StrataResult<Version> {
        let namespace = Arc::new(Namespace::for_branch_space(self.txn.branch_id(), space));
        let mut scoped = self.txn.scoped(namespace);
        scoped.json_create(doc_id, value)
    }
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
        F: FnOnce(&mut BranchTransaction<'_>) -> StrataResult<T>,
    {
        let mut txn = self.db.begin_transaction(self.branch_id)?;
        let value = {
            let mut branch_txn = BranchTransaction::new(&mut txn);
            f(&mut branch_txn)?
        };
        txn.commit()?;
        Ok(value)
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
        let mut txn = self.db.begin_transaction(self.branch_id)?;
        let namespace = Arc::new(Namespace::for_branch_space(self.branch_id, "default"));
        let version = txn.scoped(namespace).json_create(doc_id, value)?;
        txn.commit()?;
        Ok(version)
    }

    /// Get value at path in a document
    pub fn get(&self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>> {
        let mut txn = self.db.begin_transaction(self.branch_id)?;
        let namespace = Arc::new(Namespace::for_branch_space(self.branch_id, "default"));
        let value = txn.scoped(namespace).json_get_path(doc_id, path)?;
        txn.commit()?;
        Ok(value)
    }

    /// Set value at path in a document
    pub fn set(&self, doc_id: &str, path: &JsonPath, value: JsonValue) -> StrataResult<Version> {
        let mut txn = self.db.begin_transaction(self.branch_id)?;
        let namespace = Arc::new(Namespace::for_branch_space(self.branch_id, "default"));
        let version = txn.scoped(namespace).json_set(doc_id, path, value)?;
        txn.commit()?;
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, TransactionOps};

    #[test]
    fn test_branch_handle_is_clone_send_sync() {
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
        assert_clone_send_sync::<BranchHandle>();
        assert_clone_send_sync::<KvHandle>();
        assert_clone_send_sync::<EventHandle>();
        assert_clone_send_sync::<JsonHandle>();
    }

    #[test]
    fn test_json_handle_set_creates_missing_document() {
        let db = Database::cache().unwrap();
        let branch_id = BranchId::new();
        let handle = BranchHandle::new(db.clone(), branch_id);

        handle
            .json()
            .set(
                "doc1",
                &"profile.name".parse().unwrap(),
                JsonValue::from("Ada"),
            )
            .unwrap();

        let doc = handle
            .json()
            .get("doc1", &JsonPath::root())
            .unwrap()
            .expect("document should exist");
        assert_eq!(
            doc,
            JsonValue::from_value(serde_json::json!({
                "profile": { "name": "Ada" }
            }))
        );
    }

    #[test]
    fn test_branch_transaction_json_uses_engine_scoped_state() {
        let db = Database::cache().unwrap();
        let branch_id = BranchId::new();
        let branch = BranchHandle::new(db, branch_id);

        branch
            .transaction(|txn| {
                txn.json_set(
                    "doc1",
                    &"profile.age".parse().unwrap(),
                    JsonValue::from(30i64),
                )?;
                let doc = txn
                    .json_get("doc1", &JsonPath::root())?
                    .expect("document should be visible inside branch transaction");
                assert_eq!(
                    doc,
                    JsonValue::from_value(serde_json::json!({
                        "profile": { "age": 30 }
                    }))
                );
                Ok(())
            })
            .unwrap();
    }
}
