//! TransactionOps trait - unified primitive operations
//!
//! This trait expresses Invariant 3: Everything is Transactional.
//! Every primitive's operations are accessible through this trait,
//! enabling cross-primitive atomic operations.
//!
//! ## Design Principles
//!
//! 1. **All methods are `&mut self`**: Reads need mutability for read-set tracking
//! 2. **Writes modify state**: Write operations require exclusive access
//! 3. **All operations return `Result<T, StrataError>`**: Consistent error handling
//! 4. **All reads return `Versioned<T>`**: Version information is never lost
//! 5. **All writes return `Version`**: Every mutation produces a version
//!
//! ## Usage
//!
//! ```text
//! db.transaction(&branch_id, |txn| {
//!     // Read from KV
//!     let config = txn.kv_get("config")?;
//!
//!     // Write to Event
//!     let event_version = txn.event_append("config_read", json!({}))?;
//!
//!     Ok(())
//! })?;
//! ```

use crate::{Event, JsonPath, JsonValue, StrataError};
use strata_core::{Value, Version, Versioned};

/// Operations available within a transaction
///
/// This trait expresses Invariant 3: Everything is Transactional.
/// Every primitive's operations are accessible through this trait,
/// enabling cross-primitive atomic operations.
///
/// ## Implemented Operations
///
/// - **KV**: key-value get, put, delete, exists, list
/// - **Event**: append-only event log with hash chaining
/// - **JSON**: document operations with path-based access
///
/// ## Not Supported in Transactions
///
/// - **Vector**: requires in-memory index backends; use `VectorStore` directly
/// - **Branch**: metadata/status operations; use `Database::branches()`
///
/// These return `Err(StrataError::InvalidInput)` with guidance on the
/// correct API to use.
pub trait TransactionOps {
    // =========================================================================
    // KV Operations (Phase 2)
    // =========================================================================

    /// Get a KV entry by key
    fn kv_get(&mut self, key: &str) -> Result<Option<Versioned<Value>>, StrataError>;

    /// Put a KV entry (upsert semantics)
    fn kv_put(&mut self, key: &str, value: Value) -> Result<Version, StrataError>;

    /// Delete a KV entry
    fn kv_delete(&mut self, key: &str) -> Result<bool, StrataError>;

    /// Check if a KV entry exists
    fn kv_exists(&mut self, key: &str) -> Result<bool, StrataError>;

    /// List keys matching a prefix
    fn kv_list(&mut self, prefix: Option<&str>) -> Result<Vec<String>, StrataError>;

    // =========================================================================
    // Event Operations (Phase 2)
    // =========================================================================

    /// Append an event to the log
    fn event_append(&mut self, event_type: &str, payload: Value) -> Result<Version, StrataError>;

    /// Read an event by sequence number
    fn event_get(&mut self, sequence: u64) -> Result<Option<Versioned<Event>>, StrataError>;

    /// Read a range of events [start, end)
    fn event_range(&mut self, start: u64, end: u64) -> Result<Vec<Versioned<Event>>, StrataError>;

    /// Get current event count (length of the log)
    fn event_len(&mut self) -> Result<u64, StrataError>;

    // =========================================================================
    // Json Operations (Phase 4)
    // =========================================================================

    /// Create a JSON document
    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> Result<Version, StrataError>;

    /// Get an entire JSON document
    fn json_get(&mut self, doc_id: &str) -> Result<Option<Versioned<JsonValue>>, StrataError>;

    /// Get a value at a path within a JSON document
    fn json_get_path(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
    ) -> Result<Option<JsonValue>, StrataError>;

    /// Set a value at a path within a JSON document
    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> Result<Version, StrataError>;

    /// Delete a JSON document
    fn json_delete(&mut self, doc_id: &str) -> Result<bool, StrataError>;

    /// Check if a JSON document exists
    fn json_exists(&mut self, doc_id: &str) -> Result<bool, StrataError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock implementation of TransactionOps for testing trait properties
    ///
    /// Implements KV, Event, and JSON operations.
    /// Vector and Branch return proper errors (matching real Transaction behavior).
    struct MockTransactionOps {
        kv_data: std::collections::HashMap<String, Value>,
        json_data: std::collections::HashMap<String, JsonValue>,
        event_count: u64,
    }

    impl MockTransactionOps {
        fn new() -> Self {
            Self {
                kv_data: std::collections::HashMap::new(),
                json_data: std::collections::HashMap::new(),
                event_count: 0,
            }
        }
    }

    impl TransactionOps for MockTransactionOps {
        fn kv_get(&mut self, key: &str) -> Result<Option<Versioned<Value>>, StrataError> {
            Ok(self
                .kv_data
                .get(key)
                .map(|v| Versioned::new(v.clone(), Version::txn(1))))
        }

        fn kv_put(&mut self, key: &str, value: Value) -> Result<Version, StrataError> {
            self.kv_data.insert(key.to_string(), value);
            Ok(Version::txn(1))
        }

        fn kv_delete(&mut self, key: &str) -> Result<bool, StrataError> {
            Ok(self.kv_data.remove(key).is_some())
        }

        fn kv_exists(&mut self, key: &str) -> Result<bool, StrataError> {
            Ok(self.kv_data.contains_key(key))
        }

        fn kv_list(&mut self, prefix: Option<&str>) -> Result<Vec<String>, StrataError> {
            let keys: Vec<_> = self
                .kv_data
                .keys()
                .filter(|k| prefix.is_none() || k.starts_with(prefix.unwrap()))
                .cloned()
                .collect();
            Ok(keys)
        }

        fn event_append(
            &mut self,
            _event_type: &str,
            _payload: Value,
        ) -> Result<Version, StrataError> {
            self.event_count += 1;
            Ok(Version::seq(self.event_count))
        }

        fn event_get(&mut self, sequence: u64) -> Result<Option<Versioned<Event>>, StrataError> {
            if sequence == 0 || sequence > self.event_count {
                return Ok(None);
            }
            Ok(None)
        }

        fn event_range(
            &mut self,
            _start: u64,
            _end: u64,
        ) -> Result<Vec<Versioned<Event>>, StrataError> {
            Ok(Vec::new())
        }

        fn event_len(&mut self) -> Result<u64, StrataError> {
            Ok(self.event_count)
        }

        fn json_create(&mut self, doc_id: &str, value: JsonValue) -> Result<Version, StrataError> {
            if self.json_data.contains_key(doc_id) {
                return Err(StrataError::invalid_input(format!(
                    "document '{}' already exists",
                    doc_id
                )));
            }
            self.json_data.insert(doc_id.to_string(), value);
            Ok(Version::txn(1))
        }

        fn json_get(&mut self, doc_id: &str) -> Result<Option<Versioned<JsonValue>>, StrataError> {
            Ok(self
                .json_data
                .get(doc_id)
                .map(|v| Versioned::new(v.clone(), Version::txn(1))))
        }

        fn json_get_path(
            &mut self,
            doc_id: &str,
            path: &JsonPath,
        ) -> Result<Option<JsonValue>, StrataError> {
            match self.json_data.get(doc_id) {
                Some(doc) => {
                    if path.is_root() {
                        Ok(Some(doc.clone()))
                    } else {
                        Ok(strata_core::get_at_path(doc, path).cloned())
                    }
                }
                None => Ok(None),
            }
        }

        fn json_set(
            &mut self,
            doc_id: &str,
            path: &JsonPath,
            value: JsonValue,
        ) -> Result<Version, StrataError> {
            if path.is_root() {
                self.json_data.insert(doc_id.to_string(), value);
            }
            Ok(Version::txn(1))
        }

        fn json_delete(&mut self, doc_id: &str) -> Result<bool, StrataError> {
            Ok(self.json_data.remove(doc_id).is_some())
        }

        fn json_exists(&mut self, doc_id: &str) -> Result<bool, StrataError> {
            Ok(self.json_data.contains_key(doc_id))
        }
    }

    // ========== Object Safety Tests ==========

    /// Verify trait is object-safe by creating a trait object
    fn accept_dyn_transaction_ops(_ops: &dyn TransactionOps) {}

    fn accept_mut_dyn_transaction_ops(_ops: &mut dyn TransactionOps) {}

    fn accept_boxed_dyn_transaction_ops(_ops: Box<dyn TransactionOps>) {}

    #[test]
    fn test_trait_is_object_safe_ref() {
        let ops = MockTransactionOps::new();
        accept_dyn_transaction_ops(&ops);
    }

    #[test]
    fn test_trait_is_object_safe_mut_ref() {
        let mut ops = MockTransactionOps::new();
        accept_mut_dyn_transaction_ops(&mut ops);
    }

    #[test]
    fn test_trait_is_object_safe_boxed() {
        let ops = MockTransactionOps::new();
        accept_boxed_dyn_transaction_ops(Box::new(ops));
    }

    // ========== Read Operations Through Trait Object ==========

    #[test]
    fn test_kv_operations_through_trait_object() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        // Put through trait object
        let version = ops.kv_put("key1", Value::Int(42)).unwrap();
        assert_eq!(version.as_u64(), 1);

        // Get through trait object
        let result = ops.kv_get("key1").unwrap();
        assert!(result.is_some());
        let versioned = result.unwrap();
        assert_eq!(versioned.value, Value::Int(42));

        // Exists through trait object
        assert!(ops.kv_exists("key1").unwrap());
        assert!(!ops.kv_exists("nonexistent").unwrap());

        // Delete through trait object
        let deleted = ops.kv_delete("key1").unwrap();
        assert!(deleted);
        assert!(!ops.kv_exists("key1").unwrap());
    }

    #[test]
    fn test_event_operations_through_trait_object() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        // Append events
        let v1 = ops
            .event_append("UserCreated", Value::String("alice".into()))
            .unwrap();
        let v2 = ops
            .event_append("UserUpdated", Value::String("bob".into()))
            .unwrap();

        // Check versions are sequential
        assert_eq!(v1.as_u64(), 1);
        assert_eq!(v2.as_u64(), 2);

        // Check length
        assert_eq!(ops.event_len().unwrap(), 2);

        // Non-existent event (beyond the event count)
        assert!(ops.event_get(999).unwrap().is_none());
    }

    #[test]
    fn test_kv_list_through_trait_object() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        ops.kv_put("user:1", Value::Int(1)).unwrap();
        ops.kv_put("user:2", Value::Int(2)).unwrap();
        ops.kv_put("config:a", Value::Int(3)).unwrap();

        // List all
        let all_keys = ops.kv_list(None).unwrap();
        assert_eq!(all_keys.len(), 3);

        // List with prefix
        let user_keys = ops.kv_list(Some("user:")).unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.iter().all(|k| k.starts_with("user:")));
    }

    // ========== JSON Operations Through Trait Object ==========

    #[test]
    fn test_json_create_and_get_through_trait_object() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        let doc: JsonValue = serde_json::json!({"name": "Alice"}).into();
        ops.json_create("user:1", doc.clone()).unwrap();

        let result = ops.json_get("user:1").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, doc);
    }

    #[test]
    fn test_json_delete_through_trait_object() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        let doc: JsonValue = serde_json::json!({"key": "value"}).into();
        ops.json_create("doc", doc).unwrap();
        assert!(ops.json_exists("doc").unwrap());

        let deleted = ops.json_delete("doc").unwrap();
        assert!(deleted);
        assert!(!ops.json_exists("doc").unwrap());
    }

    #[test]
    fn test_json_create_duplicate_returns_error() {
        let mut ops: Box<dyn TransactionOps> = Box::new(MockTransactionOps::new());

        let doc: JsonValue = serde_json::json!({}).into();
        ops.json_create("doc", doc.clone()).unwrap();
        let result = ops.json_create("doc", doc);
        assert!(result.is_err());
    }

    // ========== Trait Method Signatures ==========

    #[test]
    fn test_all_methods_take_mutable_ref() {
        // All TransactionOps methods now use &mut self (reads need
        // mutability for read-set tracking in snapshot isolation).
        let mut ops = MockTransactionOps::new();
        let ops_mut: &mut dyn TransactionOps = &mut ops;

        // Reads
        let _ = ops_mut.kv_get("key");
        let _ = ops_mut.kv_exists("key");
        let _ = ops_mut.kv_list(None);
        let _ = ops_mut.event_get(1);
        let _ = ops_mut.event_range(1, 10);
        let _ = ops_mut.event_len();

        // Writes
        let _ = ops_mut.kv_put("key", Value::Int(1));
        let _ = ops_mut.kv_delete("key");
        let _ = ops_mut.event_append("test", Value::Null);
    }
}
