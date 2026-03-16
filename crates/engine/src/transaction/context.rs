//! Transaction wrapper implementing TransactionOps
//!
//! This module provides the Transaction type that wraps TransactionContext
//! and implements the TransactionOps trait for unified primitive access.
//!
//! # Supported Operations
//! - KV: key-value operations (get, put, delete, exists, list)
//! - Event: append-only event log operations
//! - State: compare-and-swap state cells
//! - JSON: document operations with path-based access
//!
//! This implementation provides:
//! - Read-your-writes semantics (check write set first)
//! - Read set tracking for conflict detection
//! - Proper key construction with namespaces
//! - Event buffering with sequence allocation
//! - State cell CAS (compare-and-swap) support
//! - JSON document operations via TransactionContext

use crate::primitives::event::{EventLogMeta, HASH_VERSION_SHA256};
use crate::transaction_ops::TransactionOps;
use std::sync::Arc;
use strata_concurrency::{JsonStoreExt, TransactionContext};
use strata_core::types::{BranchId, Key, Namespace, TypeTag};
use strata_core::{
    EntityRef, Event, JsonPath, JsonValue, State, StrataError, Timestamp, Value, Version, Versioned,
};

/// Transaction wrapper that implements TransactionOps
///
/// Wraps a TransactionContext and provides the unified primitive API
/// defined by the TransactionOps trait.
///
/// # Usage
///
/// ```text
/// db.transaction(&branch_id, |txn| {
///     // KV operations
///     let value = txn.kv_get("key")?;
///     txn.kv_put("key", Value::from("value"))?;
///
///     // Event operations
///     txn.event_append("event_type", json!({}))?;
///
///     // State operations
///     txn.state_init("counter", Value::Int(0))?;
///     txn.state_cas("counter", 1, Value::Int(1))?;
///
///     Ok(())
/// })?;
/// ```
pub struct Transaction<'a> {
    /// The underlying transaction context
    ctx: &'a mut TransactionContext,
    /// Namespace for this transaction's run
    namespace: Arc<Namespace>,
    /// Pending events buffered in this transaction
    pending_events: Vec<Event>,
    /// Base sequence number from snapshot (events start at base_sequence)
    base_sequence: u64,
    /// Last hash for chaining (starts as zero hash or last event's hash)
    last_hash: [u8; 32],
}

impl<'a> Transaction<'a> {
    /// Create a new Transaction wrapper
    ///
    /// Reads event state (sequence count, last hash) from TransactionContext
    /// to maintain continuity across multiple Transaction instances within
    /// the same session transaction.
    pub fn new(ctx: &'a mut TransactionContext, namespace: Arc<Namespace>) -> Self {
        let base_sequence = ctx.event_sequence_count();
        let last_hash = ctx.event_last_hash();
        Self {
            ctx,
            namespace,
            pending_events: Vec::new(),
            base_sequence,
            last_hash,
        }
    }

    /// Create a new Transaction with explicit base sequence
    ///
    /// Use this when you know the current event count from snapshot.
    pub fn with_base_sequence(
        ctx: &'a mut TransactionContext,
        namespace: Arc<Namespace>,
        base_sequence: u64,
        last_hash: [u8; 32],
    ) -> Self {
        Self {
            ctx,
            namespace,
            pending_events: Vec::new(),
            base_sequence,
            last_hash,
        }
    }

    /// Get the run ID for this transaction
    pub fn branch_id(&self) -> BranchId {
        self.ctx.branch_id
    }

    /// Create a KV key for the given user key
    fn kv_key(&self, key: &str) -> Key {
        Key::new_kv(self.namespace.clone(), key)
    }

    /// Create an event key for the given sequence
    fn event_key(&self, sequence: u64) -> Key {
        Key::new_event(self.namespace.clone(), sequence)
    }

    /// Compute hash for an event using the canonical hash function.
    fn compute_event_hash(event: &Event) -> [u8; 32] {
        crate::primitives::event::compute_event_hash(
            event.sequence,
            &event.event_type,
            &event.payload,
            event.timestamp.as_micros(),
            &event.prev_hash,
        )
    }

    /// Get pending events (for commit)
    pub fn pending_events(&self) -> &[Event] {
        &self.pending_events
    }

    /// Get the next sequence number for a new event
    fn next_sequence(&self) -> u64 {
        self.base_sequence + self.pending_events.len() as u64
    }

    /// Create a state key for the given name
    fn state_key(&self, name: &str) -> Key {
        Key::new_state(self.namespace.clone(), name)
    }

    /// Create a JSON key for the given document ID
    fn json_key(&self, doc_id: &str) -> Key {
        Key::new_json(self.namespace.clone(), doc_id)
    }
}

impl<'a> TransactionOps for Transaction<'a> {
    // =========================================================================
    // KV Operations (Phase 2)
    // =========================================================================

    fn kv_get(&mut self, key: &str) -> Result<Option<Versioned<Value>>, StrataError> {
        let full_key = self.kv_key(key);

        // Delegate to ctx.get() which checks write_set → delete_set → snapshot
        let result = self.ctx.get(&full_key)?;
        Ok(result.map(|v| Versioned::new(v, Version::txn(self.ctx.txn_id))))
    }

    fn kv_put(&mut self, key: &str, value: Value) -> Result<Version, StrataError> {
        let full_key = self.kv_key(key);

        // Use the ctx.put() method which handles all the bookkeeping
        self.ctx.put(full_key, value)?;

        Ok(Version::txn(self.ctx.txn_id))
    }

    fn kv_delete(&mut self, key: &str) -> Result<bool, StrataError> {
        let full_key = self.kv_key(key);

        // Check if key exists (for return value)
        let existed = self.kv_exists(key)?;

        // Use the ctx.delete() method
        self.ctx.delete(full_key)?;

        Ok(existed)
    }

    fn kv_exists(&mut self, key: &str) -> Result<bool, StrataError> {
        let full_key = self.kv_key(key);

        // Delegate to ctx.get() which checks write_set → delete_set → snapshot
        Ok(self.ctx.get(&full_key)?.is_some())
    }

    fn kv_list(&mut self, prefix: Option<&str>) -> Result<Vec<String>, StrataError> {
        // Build prefix key for snapshot scan
        let prefix_key = match prefix {
            Some(p) => Key::new_kv(self.namespace.clone(), p),
            None => Key::new(self.namespace.clone(), TypeTag::KV, vec![]),
        };

        // scan_prefix merges write_set, excludes delete_set, and reads snapshot
        let entries = self.ctx.scan_prefix(&prefix_key)?;
        let mut keys: Vec<String> = entries
            .into_iter()
            .filter_map(|(k, _)| {
                if k.type_tag == TypeTag::KV && k.namespace == self.namespace {
                    k.user_key_string()
                } else {
                    None
                }
            })
            .collect();

        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    // =========================================================================
    // Event Operations (Phase 2)
    // =========================================================================

    fn event_append(&mut self, event_type: &str, payload: Value) -> Result<Version, StrataError> {
        let sequence = self.next_sequence();
        let timestamp = Timestamp::now();
        let prev_hash = self.last_hash;

        // Create the event
        let mut event = Event {
            sequence,
            event_type: event_type.to_string(),
            payload,
            timestamp,
            prev_hash,
            hash: [0u8; 32], // Will be computed
        };

        // Compute and set the hash
        event.hash = Self::compute_event_hash(&event);

        // Update last_hash for next event in chain
        self.last_hash = event.hash;

        // Write event to context as Value::String (matching EventLog primitive format)
        let event_key = self.event_key(sequence);
        let event_json = serde_json::to_string(&event).map_err(|e| StrataError::Serialization {
            message: e.to_string(),
        })?;
        self.ctx.put(event_key, Value::String(event_json))?;

        // Write EventLogMeta so EventLog::len() and other readers see the update after commit
        let meta_key = Key::new_event_meta(self.namespace.clone());
        let meta = EventLogMeta {
            next_sequence: sequence + 1,
            head_hash: event.hash,
            hash_version: HASH_VERSION_SHA256,
            streams: Default::default(),
        };
        let meta_json = serde_json::to_string(&meta).map_err(|e| StrataError::Serialization {
            message: e.to_string(),
        })?;
        self.ctx.put(meta_key, Value::String(meta_json))?;

        // Update TransactionContext event state for cross-Transaction continuity
        self.ctx.set_event_state(
            self.base_sequence + self.pending_events.len() as u64 + 1,
            event.hash,
        );

        // Buffer the event
        self.pending_events.push(event);

        Ok(Version::seq(sequence))
    }

    fn event_get(&mut self, sequence: u64) -> Result<Option<Versioned<Event>>, StrataError> {
        // Check pending events first (read-your-writes for this Transaction instance)
        if sequence >= self.base_sequence {
            let index = (sequence - self.base_sequence) as usize;
            if index < self.pending_events.len() {
                let event = &self.pending_events[index];
                return Ok(Some(Versioned::new(event.clone(), Version::seq(sequence))));
            }
        }

        // Delegate to ctx.get() which checks write_set → delete_set → snapshot
        let event_key = self.event_key(sequence);
        match self.ctx.get(&event_key)? {
            Some(Value::String(s)) => {
                let event: Event =
                    serde_json::from_str(&s).map_err(|e| StrataError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(Versioned::new(event, Version::seq(sequence))))
            }
            _ => Ok(None),
        }
    }

    fn event_range(&mut self, start: u64, end: u64) -> Result<Vec<Versioned<Event>>, StrataError> {
        let mut results = Vec::new();

        for seq in start..end {
            if let Some(versioned) = self.event_get(seq)? {
                results.push(versioned);
            }
        }

        Ok(results)
    }

    fn event_len(&mut self) -> Result<u64, StrataError> {
        // Base sequence from snapshot + pending events
        Ok(self.base_sequence + self.pending_events.len() as u64)
    }

    // =========================================================================
    // State Operations (Phase 3)
    // =========================================================================

    fn state_get(&mut self, name: &str) -> Result<Option<Versioned<State>>, StrataError> {
        let full_key = self.state_key(name);

        // Delegate to ctx.get() which checks write_set → delete_set → snapshot
        match self.ctx.get(&full_key)? {
            Some(Value::String(s)) => {
                let state: State =
                    serde_json::from_str(&s).map_err(|e| StrataError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(Versioned::new(state.clone(), state.version)))
            }
            _ => Ok(None),
        }
    }

    fn state_init(&mut self, name: &str, value: Value) -> Result<Version, StrataError> {
        let full_key = self.state_key(name);

        // Check if state already exists (in write_set, or snapshot)
        if self.ctx.get(&full_key)?.is_some() {
            return Err(StrataError::invalid_operation(
                EntityRef::state(self.branch_id(), name),
                "state already exists",
            ));
        }

        // Create new state with version 1
        let state = State::new(value);
        let version = state.version;

        // Serialize as Value::String (matching StateCell primitive format)
        let state_json = serde_json::to_string(&state).map_err(|e| StrataError::Serialization {
            message: e.to_string(),
        })?;

        self.ctx.put(full_key, Value::String(state_json))?;

        Ok(version)
    }

    fn state_cas(
        &mut self,
        name: &str,
        expected_version: Version,
        value: Value,
    ) -> Result<Version, StrataError> {
        let full_key = self.state_key(name);

        // Read current state (write_set → delete_set → snapshot)
        let current_state = match self.ctx.get(&full_key)? {
            Some(Value::String(s)) => {
                let state: State =
                    serde_json::from_str(&s).map_err(|e| StrataError::Serialization {
                        message: e.to_string(),
                    })?;
                Some(state)
            }
            _ => None,
        };

        // For CAS, state must exist
        let current = current_state
            .ok_or_else(|| StrataError::not_found(EntityRef::state(self.branch_id(), name)))?;

        // Check version matches
        if current.version != expected_version {
            return Err(StrataError::version_conflict(
                EntityRef::state(self.branch_id(), name),
                expected_version,
                current.version,
            ));
        }

        // Create new state with incremented version
        let new_version = expected_version.increment();
        let new_state = State::with_version(value, new_version);

        // Serialize as Value::String (matching StateCell primitive format)
        let state_json =
            serde_json::to_string(&new_state).map_err(|e| StrataError::Serialization {
                message: e.to_string(),
            })?;

        self.ctx.put(full_key, Value::String(state_json))?;

        Ok(new_version)
    }

    // =========================================================================
    // Json Operations
    // =========================================================================

    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> Result<Version, StrataError> {
        let full_key = self.json_key(doc_id);

        // Check if document already exists (in write buffer or snapshot)
        if self.ctx.json_exists(&full_key)? {
            return Err(StrataError::invalid_operation(
                EntityRef::json(self.branch_id(), doc_id),
                "document already exists",
            ));
        }

        // Create the document by setting at root path
        self.ctx.json_set(&full_key, &JsonPath::root(), value)?;

        Ok(Version::txn(self.ctx.txn_id))
    }

    fn json_get(&mut self, doc_id: &str) -> Result<Option<Versioned<JsonValue>>, StrataError> {
        let full_key = self.json_key(doc_id);

        // Delegate to ctx which checks json_writes buffer → snapshot
        match self.ctx.json_get_document(&full_key)? {
            Some(jv) => Ok(Some(Versioned::new(jv, Version::txn(self.ctx.txn_id)))),
            None => Ok(None),
        }
    }

    fn json_get_path(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
    ) -> Result<Option<JsonValue>, StrataError> {
        let full_key = self.json_key(doc_id);

        // Delegate to ctx which checks json_writes buffer → snapshot
        self.ctx.json_get(&full_key, path)
    }

    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> Result<Version, StrataError> {
        let full_key = self.json_key(doc_id);

        // Call ctx.json_set (same pattern as kv_put calling ctx.put)
        self.ctx.json_set(&full_key, path, value)?;

        Ok(Version::txn(self.ctx.txn_id))
    }

    fn json_delete(&mut self, doc_id: &str) -> Result<bool, StrataError> {
        let full_key = self.json_key(doc_id);

        // Check if document exists (for return value, same pattern as kv_delete)
        let existed = self.json_exists(doc_id)?;

        // Delete the entire document by deleting at root path
        self.ctx.json_delete(&full_key, &JsonPath::root())?;

        Ok(existed)
    }

    fn json_exists(&mut self, doc_id: &str) -> Result<bool, StrataError> {
        let full_key = self.json_key(doc_id);

        // Delegate to ctx which checks json_writes buffer → snapshot
        self.ctx.json_exists(&full_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_storage::SegmentedStore;

    fn create_test_namespace() -> Arc<Namespace> {
        let branch_id = BranchId::new();
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn create_test_context(ns: &Namespace) -> TransactionContext {
        let store = Arc::new(SegmentedStore::new());
        TransactionContext::with_store(1, ns.branch_id, store)
    }

    // =========================================================================
    // KV Tests
    // =========================================================================

    #[test]
    fn test_kv_put_and_get() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Put a value
        let version = txn
            .kv_put("test_key", Value::String("test_value".to_string()))
            .unwrap();
        assert!(version.as_u64() > 0);

        // Get the value back (read-your-writes)
        let result = txn.kv_get("test_key").unwrap();
        assert!(result.is_some());
        let versioned = result.unwrap();
        assert_eq!(versioned.value, Value::String("test_value".to_string()));
    }

    #[test]
    fn test_kv_delete() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Put then delete
        txn.kv_put("test_key", Value::String("value".to_string()))
            .unwrap();
        let existed = txn.kv_delete("test_key").unwrap();
        assert!(existed);

        // Get should return None
        let result = txn.kv_get("test_key").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_kv_exists() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Key doesn't exist initially
        assert!(!txn.kv_exists("missing").unwrap());

        // Put and check
        txn.kv_put("present", Value::String("value".to_string()))
            .unwrap();
        assert!(txn.kv_exists("present").unwrap());
    }

    #[test]
    fn test_kv_list() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Add some keys
        txn.kv_put("user:1", Value::String("alice".to_string()))
            .unwrap();
        txn.kv_put("user:2", Value::String("bob".to_string()))
            .unwrap();
        txn.kv_put("config:app", Value::String("settings".to_string()))
            .unwrap();

        // List all
        let all_keys = txn.kv_list(None).unwrap();
        assert_eq!(all_keys.len(), 3);

        // List with prefix
        let user_keys = txn.kv_list(Some("user:")).unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&"user:1".to_string()));
        assert!(user_keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_kv_list_with_delete() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Add keys then delete one
        txn.kv_put("key1", Value::String("v1".to_string())).unwrap();
        txn.kv_put("key2", Value::String("v2".to_string())).unwrap();
        txn.kv_delete("key1").unwrap();

        let keys = txn.kv_list(None).unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "key2");
    }

    // =========================================================================
    // Event Tests
    // =========================================================================

    #[test]
    fn test_event_append() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append an event
        let version = txn
            .event_append("user_created", Value::String("alice".to_string()))
            .unwrap();
        assert_eq!(version, Version::seq(0));

        // Check event count
        assert_eq!(txn.event_len().unwrap(), 1);
    }

    #[test]
    fn test_event_append_multiple() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append multiple events
        let v1 = txn.event_append("event1", Value::Int(1)).unwrap();
        let v2 = txn.event_append("event2", Value::Int(2)).unwrap();
        let v3 = txn.event_append("event3", Value::Int(3)).unwrap();

        assert_eq!(v1, Version::seq(0));
        assert_eq!(v2, Version::seq(1));
        assert_eq!(v3, Version::seq(2));
        assert_eq!(txn.event_len().unwrap(), 3);
    }

    #[test]
    fn test_event_get() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append an event
        txn.event_append("test_event", Value::String("payload".to_string()))
            .unwrap();

        // Read it back
        let result = txn.event_get(0).unwrap();
        assert!(result.is_some());

        let versioned = result.unwrap();
        assert_eq!(versioned.value.sequence, 0);
        assert_eq!(versioned.value.event_type, "test_event");
        assert_eq!(
            versioned.value.payload,
            Value::String("payload".to_string())
        );
    }

    #[test]
    fn test_event_get_your_writes() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append events and read them back immediately
        txn.event_append("first", Value::Int(100)).unwrap();
        txn.event_append("second", Value::Int(200)).unwrap();

        let first = txn.event_get(0).unwrap().unwrap();
        let second = txn.event_get(1).unwrap().unwrap();

        assert_eq!(first.value.event_type, "first");
        assert_eq!(first.value.payload, Value::Int(100));
        assert_eq!(second.value.event_type, "second");
        assert_eq!(second.value.payload, Value::Int(200));
    }

    #[test]
    fn test_event_get_not_found() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Reading non-existent event returns None
        let result = txn.event_get(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_event_range() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append several events
        for i in 0..5 {
            txn.event_append(&format!("event_{}", i), Value::Int(i))
                .unwrap();
        }

        // Read a range
        let events = txn.event_range(1, 4).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].value.event_type, "event_1");
        assert_eq!(events[1].value.event_type, "event_2");
        assert_eq!(events[2].value.event_type, "event_3");
    }

    #[test]
    fn test_event_hash_chaining() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Append events
        txn.event_append("first", Value::Int(1)).unwrap();
        txn.event_append("second", Value::Int(2)).unwrap();

        let first = txn.event_get(0).unwrap().unwrap();
        let second = txn.event_get(1).unwrap().unwrap();

        // First event's prev_hash should be zeros (genesis)
        assert_eq!(first.value.prev_hash, [0u8; 32]);

        // Second event's prev_hash should be first event's hash
        assert_eq!(second.value.prev_hash, first.value.hash);

        // Each event should have a non-zero hash
        assert_ne!(first.value.hash, [0u8; 32]);
        assert_ne!(second.value.hash, [0u8; 32]);
    }

    #[test]
    fn test_event_with_base_sequence() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);

        // Create transaction with existing events (simulating snapshot)
        let last_hash = [42u8; 32];
        let mut txn = Transaction::with_base_sequence(&mut ctx, ns.clone(), 100, last_hash);

        // New events should continue from base
        let v1 = txn.event_append("new_event", Value::Int(1)).unwrap();
        assert_eq!(v1, Version::seq(100));
        assert_eq!(txn.event_len().unwrap(), 101);

        // The event should chain from the provided last_hash
        let event = txn.event_get(100).unwrap().unwrap();
        assert_eq!(event.value.prev_hash, last_hash);
    }

    #[test]
    fn test_pending_events_accessor() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        txn.event_append("e1", Value::Int(1)).unwrap();
        txn.event_append("e2", Value::Int(2)).unwrap();

        let pending = txn.pending_events();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].event_type, "e1");
        assert_eq!(pending[1].event_type, "e2");
    }

    // =========================================================================
    // State Tests
    // =========================================================================

    #[test]
    fn test_state_init_and_read() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Initialize a state cell
        let version = txn.state_init("counter", Value::Int(0)).unwrap();
        assert_eq!(version, Version::counter(1)); // Version 1 for new state

        // Read it back (read-your-writes)
        let result = txn.state_get("counter").unwrap();
        assert!(result.is_some());
        let versioned = result.unwrap();
        assert_eq!(versioned.value.value, Value::Int(0));
        assert_eq!(versioned.value.version, Version::counter(1));
    }

    #[test]
    fn test_state_cas_success() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Initialize then CAS
        txn.state_init("counter", Value::Int(0)).unwrap();
        let new_version = txn
            .state_cas("counter", Version::counter(1), Value::Int(1))
            .unwrap();
        assert_eq!(new_version, Version::counter(2)); // Version incremented

        // Verify the value changed
        let result = txn.state_get("counter").unwrap().unwrap();
        assert_eq!(result.value.value, Value::Int(1));
        assert_eq!(result.value.version, Version::counter(2));
    }

    #[test]
    fn test_state_cas_version_mismatch() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Initialize then CAS with wrong version
        txn.state_init("counter", Value::Int(0)).unwrap();
        let result = txn.state_cas("counter", Version::counter(99), Value::Int(1)); // Wrong version

        assert!(result.is_err());
        match result.unwrap_err() {
            StrataError::VersionConflict {
                expected, actual, ..
            } => {
                assert_eq!(expected, Version::counter(99));
                assert_eq!(actual, Version::counter(1));
            }
            _ => panic!("Expected VersionConflict error"),
        }
    }

    #[test]
    fn test_state_init_duplicate_fails() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Initialize twice should fail
        txn.state_init("counter", Value::Int(0)).unwrap();
        let result = txn.state_init("counter", Value::Int(1));

        assert!(result.is_err());
        match result.unwrap_err() {
            StrataError::InvalidOperation { reason, .. } => {
                assert!(reason.contains("already exists"));
            }
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    // =========================================================================
    // JSON Tests
    // =========================================================================

    #[test]
    fn test_json_create_and_get() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create a JSON document
        let doc: JsonValue = serde_json::json!({"name": "Alice", "age": 30}).into();
        let version = txn.json_create("user:1", doc.clone()).unwrap();
        assert!(version.as_u64() > 0);

        // Get the document back (read-your-writes)
        let result = txn.json_get("user:1").unwrap();
        assert!(result.is_some());
        let versioned = result.unwrap();
        assert_eq!(versioned.value, doc);
    }

    #[test]
    fn test_json_create_duplicate_fails() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create twice should fail
        let doc: JsonValue = serde_json::json!({"key": "value"}).into();
        txn.json_create("doc1", doc).unwrap();
        let result = txn.json_create("doc1", serde_json::json!({"other": "data"}).into());

        assert!(result.is_err());
        match result.unwrap_err() {
            StrataError::InvalidOperation { reason, .. } => {
                assert!(reason.contains("already exists"));
            }
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[test]
    fn test_json_set_and_get_path() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create a document
        let doc: JsonValue = serde_json::json!({"user": {"name": "Bob"}}).into();
        txn.json_create("doc", doc).unwrap();

        // Set a nested value
        let path: JsonPath = "user.age".parse().unwrap();
        txn.json_set("doc", &path, serde_json::json!(25).into())
            .unwrap();

        // Get the path value back
        let result = txn.json_get_path("doc", &path).unwrap();
        let expected: JsonValue = serde_json::json!(25).into();
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_json_delete() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create then delete
        let doc: JsonValue = serde_json::json!({"key": "value"}).into();
        txn.json_create("doc", doc).unwrap();
        let existed = txn.json_delete("doc").unwrap();
        assert!(existed);

        // Get should return None
        let result = txn.json_get("doc").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_exists() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Document doesn't exist initially
        assert!(!txn.json_exists("missing").unwrap());

        // Create and check
        let doc: JsonValue = serde_json::json!({}).into();
        txn.json_create("doc", doc).unwrap();
        assert!(txn.json_exists("doc").unwrap());
    }

    #[test]
    fn test_json_get_nonexistent() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Getting non-existent document returns None
        let result = txn.json_get("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_get_path_root() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create a document
        let doc: JsonValue = serde_json::json!({"name": "Test"}).into();
        txn.json_create("doc", doc.clone()).unwrap();

        // Get at root path returns the whole document
        let result = txn.json_get_path("doc", &JsonPath::root()).unwrap();
        assert_eq!(result, Some(doc));
    }

    // =========================================================================
    // Snapshot Read Tests (#1414)
    //
    // These tests verify that Transaction read methods correctly fall through
    // to the snapshot when data is not in the write_set or delete_set.
    // =========================================================================

    /// Helper: pre-commit data into the store so a new TransactionContext can
    /// read it from the snapshot.
    fn commit_kv(store: &Arc<SegmentedStore>, ns: &Namespace, key: &str, value: Value) {
        use strata_core::traits::Storage;
        use strata_core::WriteMode;
        let k = Key::new_kv(Arc::new(ns.clone()), key);
        let version = store.next_version();
        store
            .put_with_version_mode(k, value, version, None, WriteMode::Append)
            .unwrap();
    }

    #[test]
    fn test_kv_get_reads_snapshot() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());

        // Pre-commit data into the store
        commit_kv(
            &store,
            &ns,
            "committed_key",
            Value::String("from_snapshot".into()),
        );

        // Create a new TransactionContext that sees the snapshot
        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Read should see the committed data
        let result = txn.kv_get("committed_key").unwrap();
        assert!(
            result.is_some(),
            "kv_get should see committed snapshot data"
        );
        assert_eq!(result.unwrap().value, Value::String("from_snapshot".into()));
    }

    #[test]
    fn test_kv_exists_sees_snapshot() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        commit_kv(&store, &ns, "exists_key", Value::Int(42));

        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        assert!(txn.kv_exists("exists_key").unwrap());
        assert!(!txn.kv_exists("missing_key").unwrap());
    }

    #[test]
    fn test_kv_list_includes_snapshot() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        commit_kv(&store, &ns, "user:1", Value::Int(1));
        commit_kv(&store, &ns, "user:2", Value::Int(2));

        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        let keys = txn.kv_list(Some("user:")).unwrap();
        assert_eq!(keys.len(), 2, "kv_list should include snapshot keys");
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_kv_list_merges_snapshot_and_writes() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        commit_kv(&store, &ns, "user:1", Value::Int(1));

        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Add a new key in the transaction
        txn.kv_put("user:2", Value::Int(2)).unwrap();

        let keys = txn.kv_list(Some("user:")).unwrap();
        assert_eq!(
            keys.len(),
            2,
            "should see both snapshot and uncommitted keys"
        );
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_kv_delete_hides_snapshot_data() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        commit_kv(&store, &ns, "to_delete", Value::Int(1));

        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Verify it exists in snapshot
        assert!(txn.kv_exists("to_delete").unwrap());

        // Delete it
        let existed = txn.kv_delete("to_delete").unwrap();
        assert!(existed, "should report that snapshot key existed");

        // Now it should be gone
        assert!(!txn.kv_exists("to_delete").unwrap());
        assert!(txn.kv_get("to_delete").unwrap().is_none());
    }

    #[test]
    fn test_write_overrides_snapshot() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        commit_kv(&store, &ns, "key", Value::String("old".into()));

        let mut ctx = TransactionContext::with_store(2, ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Override the snapshot value
        txn.kv_put("key", Value::String("new".into())).unwrap();

        // Should see the new value, not the snapshot
        let result = txn.kv_get("key").unwrap().unwrap();
        assert_eq!(result.value, Value::String("new".into()));
    }
}
