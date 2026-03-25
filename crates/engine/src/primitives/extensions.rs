//! Transaction extension traits for cross-primitive operations
//!
//! ## Design Principle
//!
//! Extension traits allow multiple primitives to participate in a single
//! transaction. Each trait provides domain-specific methods that operate
//! on a `TransactionContext`.
//!
//! ## Usage
//!
//! ```text
//! use strata_primitives::extensions::*;
//!
//! db.transaction(branch_id, |txn| {
//!     // KV operation
//!     txn.kv_put("key", value)?;
//!
//!     // Event operation
//!     txn.event_append("type", payload)?;
//!
//!     Ok(())
//! })?;
//! ```
//!
//! ## Implementation Note
//!
//! Extension traits DELEGATE to primitive internals - they do NOT
//! reimplement logic. Each trait implementation calls the same
//! internal functions used by the standalone primitive API.
//!
//! This ensures:
//! - Single source of truth for each primitive's logic
//! - Consistent behavior between standalone and transaction APIs
//! - Easier maintenance and testing

use strata_core::contract::Version;
use strata_core::primitives::json::{JsonPath, JsonValue};
use strata_core::{StrataResult, Value};

// Forward declarations - traits are defined here, implementations
// are added in their respective primitive modules.

/// KV operations within a transaction
///
/// Implemented in `kv.rs`
pub trait KVStoreExt {
    /// Get a value by key
    fn kv_get(&mut self, key: &str) -> StrataResult<Option<Value>>;

    /// Put a value
    fn kv_put(&mut self, key: &str, value: Value) -> StrataResult<()>;

    /// Delete a key
    fn kv_delete(&mut self, key: &str) -> StrataResult<()>;
}

/// Event log operations within a transaction
///
/// Implemented in `event_log.rs`
pub trait EventLogExt {
    /// Append an event and return sequence number
    fn event_append(&mut self, event_type: &str, payload: Value) -> StrataResult<u64>;

    /// Read an event by sequence number
    fn event_get(&mut self, sequence: u64) -> StrataResult<Option<Value>>;
}

/// JSON store operations within a transaction
///
/// Implemented in `json_store.rs`
pub trait JsonStoreExt {
    /// Get value at path in a document
    fn json_get(&mut self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>>;

    /// Set value at path in a document, returns new version
    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version>;

    /// Create a new JSON document, returns version
    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> StrataResult<Version>;
}

/// Vector store operations within a transaction
///
/// Note: VectorStore operations in transactions are limited due to
/// the backend state management complexity. Only basic read/write ops
/// are supported.
///
/// Implemented in `vector/store.rs`
pub trait VectorStoreExt {
    /// Get a vector by key
    fn vector_get(&mut self, collection: &str, key: &str) -> StrataResult<Option<Vec<f32>>>;

    /// Insert/upsert a vector, returns version
    fn vector_insert(
        &mut self,
        collection: &str,
        key: &str,
        embedding: &[f32],
    ) -> StrataResult<Version>;
}

// VectorStoreExt stub impl for TransactionContext.
// Vector operations in transactions are not supported (require in-memory backends).
impl VectorStoreExt for strata_concurrency::TransactionContext {
    fn vector_get(&mut self, collection: &str, key: &str) -> StrataResult<Option<Vec<f32>>> {
        let _ = (collection, key);
        Err(strata_core::StrataError::invalid_input(
            "VectorStore get operations are not supported in cross-primitive transactions. \
             Embeddings are stored in in-memory backends not accessible from TransactionContext. \
             Use VectorStore::get() directly outside of transactions."
                .to_string(),
        ))
    }

    fn vector_insert(
        &mut self,
        collection: &str,
        key: &str,
        embedding: &[f32],
    ) -> StrataResult<Version> {
        let _ = (collection, key, embedding);
        Err(strata_core::StrataError::invalid_input(
            "VectorStore insert operations are not supported in cross-primitive transactions. \
             Vector operations require access to in-memory backends not accessible from \
             TransactionContext. Use VectorStore::insert() directly outside of transactions."
                .to_string(),
        ))
    }
}

// Note: BranchIndex does not have an extension trait because run operations
// are typically done outside of cross-primitive transactions. Run lifecycle
// operations (create, complete, fail) are usually standalone operations
// that bookend a series of primitive operations.

#[cfg(test)]
mod tests {
    use super::*;

    // These tests verify trait definitions compile correctly.
    // Implementation tests will be in their respective primitive stories.

    #[test]
    fn test_traits_are_object_safe() {
        // Verify traits can be used as trait objects if needed
        fn _accepts_kv_ext(_ext: &dyn KVStoreExt) {}
        fn _accepts_event_ext(_ext: &dyn EventLogExt) {}
    }
}
