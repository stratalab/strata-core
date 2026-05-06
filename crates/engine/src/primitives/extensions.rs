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

use crate::semantics::json::{JsonPath, JsonValue};
use crate::StrataResult;
use strata_core::contract::Version;
use strata_core::Value;

// Forward declarations - traits are defined here, implementations
// are added in their respective primitive modules.

/// KV operations within a transaction
///
/// Implemented in `kv.rs`.
///
/// The `_in_space` methods accept an explicit space parameter. The convenience
/// methods (`kv_get`, `kv_put`, `kv_delete`) delegate to them with `"default"`.
pub trait KVStoreExt {
    /// Get a value by key in the specified space
    fn kv_get_in_space(&mut self, space: &str, key: &str) -> StrataResult<Option<Value>>;

    /// Put a value in the specified space
    fn kv_put_in_space(&mut self, space: &str, key: &str, value: Value) -> StrataResult<()>;

    /// Delete a key in the specified space
    fn kv_delete_in_space(&mut self, space: &str, key: &str) -> StrataResult<()>;

    /// Get a value by key (default space)
    fn kv_get(&mut self, key: &str) -> StrataResult<Option<Value>> {
        self.kv_get_in_space("default", key)
    }

    /// Put a value (default space)
    fn kv_put(&mut self, key: &str, value: Value) -> StrataResult<()> {
        self.kv_put_in_space("default", key, value)
    }

    /// Delete a key (default space)
    fn kv_delete(&mut self, key: &str) -> StrataResult<()> {
        self.kv_delete_in_space("default", key)
    }
}

/// Event log operations within a transaction
///
/// Implemented in `event.rs`.
///
/// The `_in_space` methods accept an explicit space parameter. The convenience
/// methods (`event_append`, `event_get`) delegate to them with `"default"`.
pub trait EventLogExt {
    /// Append an event in the specified space and return sequence number
    fn event_append_in_space(
        &mut self,
        space: &str,
        event_type: &str,
        payload: Value,
    ) -> StrataResult<u64>;

    /// Read an event by sequence number in the specified space
    fn event_get_in_space(&mut self, space: &str, sequence: u64) -> StrataResult<Option<Value>>;

    /// Append an event (default space)
    fn event_append(&mut self, event_type: &str, payload: Value) -> StrataResult<u64> {
        self.event_append_in_space("default", event_type, payload)
    }

    /// Read an event by sequence number (default space)
    fn event_get(&mut self, sequence: u64) -> StrataResult<Option<Value>> {
        self.event_get_in_space("default", sequence)
    }
}

/// JSON store operations within a transaction
///
/// Implemented in `json.rs`.
///
/// The `_in_space` methods accept an explicit space parameter. The convenience
/// methods (`json_get`, `json_set`, `json_create`) delegate to them with `"default"`.
///
/// New JSON transaction logic should prefer the engine-owned scoped transaction
/// path exposed by `crate::transaction::Transaction::scoped()` and the public
/// branch transaction wrappers. The raw `TransactionContext` implementation of
/// this trait is retained as a compatibility seam for older closure-style call
/// sites and performs direct full-document read/modify/write operations instead
/// of buffering JSON path semantics through `JsonTxnState`.
pub trait JsonStoreExt {
    /// Get value at path in a document in the specified space
    fn json_get_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
    ) -> StrataResult<Option<JsonValue>>;

    /// Set value at path in a document in the specified space, returns new version
    fn json_set_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version>;

    /// Create a new JSON document in the specified space, returns version
    fn json_create_in_space(
        &mut self,
        space: &str,
        doc_id: &str,
        value: JsonValue,
    ) -> StrataResult<Version>;

    /// Get value at path in a document (default space)
    fn json_get(&mut self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>> {
        self.json_get_in_space("default", doc_id, path)
    }

    /// Set value at path in a document (default space), returns new version
    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> StrataResult<Version> {
        self.json_set_in_space("default", doc_id, path, value)
    }

    /// Create a new JSON document (default space), returns version
    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> StrataResult<Version> {
        self.json_create_in_space("default", doc_id, value)
    }
}

// Note: VectorStoreExt is defined in the engine-owned vector module because it
// needs access to vector-specific runtime types (VectorRecord, VectorId, etc.).

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
