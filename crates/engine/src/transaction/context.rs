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
    BranchMetadata, BranchStatus, EntityRef, Event, JsonPatch, JsonPath, JsonValue, MetadataFilter,
    State, StrataError, Timestamp, Value, VectorEntry, VectorMatch, Version, Versioned,
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

    /// Extract user key from a full Key
    fn user_key(key: &Key) -> String {
        key.user_key_string().unwrap_or_default()
    }

    /// Compute hash for an event using the canonical hash function.
    fn compute_event_hash(event: &Event) -> [u8; 32] {
        crate::primitives::event::compute_event_hash(
            event.sequence,
            &event.event_type,
            &event.payload,
            event.timestamp,
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

    fn kv_get(&self, key: &str) -> Result<Option<Versioned<Value>>, StrataError> {
        let full_key = self.kv_key(key);

        // Check write set first (read-your-writes)
        if let Some(value) = self.ctx.write_set.get(&full_key) {
            return Ok(Some(Versioned::new(
                value.clone(),
                Version::txn(self.ctx.txn_id),
            )));
        }

        // Check delete set (uncommitted delete returns None)
        if self.ctx.delete_set.contains(&full_key) {
            return Ok(None);
        }

        // For reads from snapshot, we can only see uncommitted changes
        // The full implementation would need TransactionContext to expose
        // a snapshot read method.
        Ok(None)
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

    fn kv_exists(&self, key: &str) -> Result<bool, StrataError> {
        let full_key = self.kv_key(key);

        // Check write set first
        if self.ctx.write_set.contains_key(&full_key) {
            return Ok(true);
        }

        // Check delete set
        if self.ctx.delete_set.contains(&full_key) {
            return Ok(false);
        }

        // For keys not in write/delete set, we'd need snapshot access
        Ok(false)
    }

    fn kv_list(&self, prefix: Option<&str>) -> Result<Vec<String>, StrataError> {
        let mut keys: Vec<String> = Vec::new();

        // Collect keys from write set matching prefix
        for key in self.ctx.write_set.keys() {
            if key.type_tag == TypeTag::KV && key.namespace == self.namespace {
                let user_key = Self::user_key(key);
                if let Some(p) = prefix {
                    if user_key.starts_with(p) {
                        keys.push(user_key);
                    }
                } else {
                    keys.push(user_key);
                }
            }
        }

        // Remove deleted keys
        for key in &self.ctx.delete_set {
            if key.type_tag == TypeTag::KV && key.namespace == self.namespace {
                let user_key = Self::user_key(key);
                keys.retain(|k| k != &user_key);
            }
        }

        keys.sort();
        Ok(keys)
    }

    // =========================================================================
    // Event Operations (Phase 2)
    // =========================================================================

    fn event_append(&mut self, event_type: &str, payload: Value) -> Result<Version, StrataError> {
        let sequence = self.next_sequence();
        let timestamp = Timestamp::now().as_micros();
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

    fn event_get(&self, sequence: u64) -> Result<Option<Versioned<Event>>, StrataError> {
        // Check pending events first (read-your-writes)
        if sequence >= self.base_sequence {
            let index = (sequence - self.base_sequence) as usize;
            if index < self.pending_events.len() {
                let event = &self.pending_events[index];
                return Ok(Some(Versioned::new(event.clone(), Version::seq(sequence))));
            }
        }

        // Check if the event was written to ctx.write_set
        let event_key = self.event_key(sequence);
        if let Some(Value::String(s)) = self.ctx.write_set.get(&event_key) {
            let event: Event = serde_json::from_str(s).map_err(|e| StrataError::Serialization {
                message: e.to_string(),
            })?;
            return Ok(Some(Versioned::new(event, Version::seq(sequence))));
        }

        // For reads from snapshot, would need snapshot access
        // Return None for events not in pending or write set
        Ok(None)
    }

    fn event_range(&self, start: u64, end: u64) -> Result<Vec<Versioned<Event>>, StrataError> {
        let mut results = Vec::new();

        for seq in start..end {
            if let Some(versioned) = self.event_get(seq)? {
                results.push(versioned);
            }
        }

        Ok(results)
    }

    fn event_len(&self) -> Result<u64, StrataError> {
        // Base sequence from snapshot + pending events
        Ok(self.base_sequence + self.pending_events.len() as u64)
    }

    // =========================================================================
    // State Operations (Phase 3)
    // =========================================================================

    fn state_get(&self, name: &str) -> Result<Option<Versioned<State>>, StrataError> {
        let full_key = self.state_key(name);

        // Check write set first (read-your-writes)
        // Uses Value::String matching StateCell primitive format
        if let Some(Value::String(s)) = self.ctx.write_set.get(&full_key) {
            let state: State = serde_json::from_str(s).map_err(|e| StrataError::Serialization {
                message: e.to_string(),
            })?;
            return Ok(Some(Versioned::new(state.clone(), state.version)));
        }

        // Check delete set (uncommitted delete returns None)
        if self.ctx.delete_set.contains(&full_key) {
            return Ok(None);
        }

        // For reads from snapshot, would need snapshot access
        // Return None for state not in write set
        Ok(None)
    }

    fn state_init(&mut self, name: &str, value: Value) -> Result<Version, StrataError> {
        let full_key = self.state_key(name);

        // Check if state already exists (init should only work for new state)
        if self.ctx.write_set.contains_key(&full_key) {
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

        // Read current state to get version (Value::String matching StateCell format)
        let current_state = if let Some(Value::String(s)) = self.ctx.write_set.get(&full_key) {
            let state: State = serde_json::from_str(s).map_err(|e| StrataError::Serialization {
                message: e.to_string(),
            })?;
            Some(state)
        } else {
            None
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

        // Check if document already exists in this transaction's writes
        // (same pattern as state_init checking write_set)
        for entry in self.ctx.json_writes() {
            if entry.key == full_key {
                if let JsonPatch::Set { path, .. } = &entry.patch {
                    if path.is_root() {
                        return Err(StrataError::invalid_operation(
                            EntityRef::json(self.branch_id(), doc_id),
                            "document already exists",
                        ));
                    }
                }
            }
        }

        // Create the document by setting at root path
        self.ctx.json_set(&full_key, &JsonPath::root(), value)?;

        Ok(Version::txn(self.ctx.txn_id))
    }

    fn json_get(&self, doc_id: &str) -> Result<Option<Versioned<JsonValue>>, StrataError> {
        let full_key = self.json_key(doc_id);

        // Check json_writes for root Set on this key (read-your-writes)
        // Iterate in reverse to get the most recent write
        for entry in self.ctx.json_writes().iter().rev() {
            if entry.key == full_key {
                match &entry.patch {
                    JsonPatch::Set { path, value } if path.is_root() => {
                        return Ok(Some(Versioned::new(
                            value.clone(),
                            Version::txn(self.ctx.txn_id),
                        )));
                    }
                    JsonPatch::Delete { path } if path.is_root() => {
                        // Document was deleted in this transaction
                        return Ok(None);
                    }
                    _ => {}
                }
            }
        }

        // For documents not in json_writes, return None
        // (same pattern as kv_get returning None for snapshot reads)
        Ok(None)
    }

    fn json_get_path(
        &self,
        doc_id: &str,
        path: &JsonPath,
    ) -> Result<Option<JsonValue>, StrataError> {
        let full_key = self.json_key(doc_id);

        // Check json_writes for writes affecting this path (read-your-writes)
        for entry in self.ctx.json_writes().iter().rev() {
            if entry.key == full_key {
                match &entry.patch {
                    JsonPatch::Set {
                        path: set_path,
                        value,
                    } => {
                        // If the set path is an ancestor of or equal to our path
                        if set_path.is_ancestor_of(path) || set_path == path {
                            if set_path == path {
                                return Ok(Some(value.clone()));
                            }
                            // Navigate into the written value using relative path
                            let relative_segments: Vec<_> = path
                                .segments()
                                .iter()
                                .skip(set_path.len())
                                .cloned()
                                .collect();
                            let relative_path = JsonPath::from_segments(relative_segments);
                            return Ok(strata_core::get_at_path(value, &relative_path).cloned());
                        }
                    }
                    JsonPatch::Delete { path: del_path } => {
                        if del_path.is_ancestor_of(path) || del_path == path {
                            return Ok(None);
                        }
                    }
                }
            }
        }

        // For paths not in json_writes, return None
        Ok(None)
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

    fn json_exists(&self, doc_id: &str) -> Result<bool, StrataError> {
        let full_key = self.json_key(doc_id);

        // Check json_writes for root Set/Delete on this key
        // (same pattern as kv_exists checking write_set/delete_set)
        for entry in self.ctx.json_writes().iter().rev() {
            if entry.key == full_key {
                match &entry.patch {
                    JsonPatch::Set { path, .. } if path.is_root() => {
                        return Ok(true);
                    }
                    JsonPatch::Delete { path } if path.is_root() => {
                        return Ok(false);
                    }
                    _ => {}
                }
            }
        }

        // For documents not in json_writes, return false
        // (same pattern as kv_exists returning false for keys not in buffers)
        Ok(false)
    }

    fn json_destroy(&mut self, doc_id: &str) -> Result<bool, StrataError> {
        // json_destroy is the same as json_delete
        // (destroy entire document)
        self.json_delete(doc_id)
    }

    // =========================================================================
    // Vector Operations — not supported in transactions
    //
    // Vector operations require in-memory index backends that are not
    // accessible from TransactionContext. Use VectorStore methods directly.
    // =========================================================================

    fn vector_insert(
        &mut self,
        _collection: &str,
        _key: &str,
        _embedding: &[f32],
        _metadata: Option<Value>,
    ) -> Result<Version, StrataError> {
        Err(StrataError::invalid_input(
            "Vector insert is not supported inside transactions. \
             Use VectorStore::insert() directly."
                .to_string(),
        ))
    }

    fn vector_get(
        &self,
        _collection: &str,
        _key: &str,
    ) -> Result<Option<Versioned<VectorEntry>>, StrataError> {
        Err(StrataError::invalid_input(
            "Vector get is not supported inside transactions. \
             Use VectorStore::get() directly."
                .to_string(),
        ))
    }

    fn vector_delete(&mut self, _collection: &str, _key: &str) -> Result<bool, StrataError> {
        Err(StrataError::invalid_input(
            "Vector delete is not supported inside transactions. \
             Use VectorStore::delete() directly."
                .to_string(),
        ))
    }

    fn vector_search(
        &self,
        _collection: &str,
        _query: &[f32],
        _k: usize,
        _filter: Option<MetadataFilter>,
    ) -> Result<Vec<VectorMatch>, StrataError> {
        Err(StrataError::invalid_input(
            "Vector search is not supported inside transactions. \
             Use VectorStore::search() directly."
                .to_string(),
        ))
    }

    fn vector_exists(&self, _collection: &str, _key: &str) -> Result<bool, StrataError> {
        Err(StrataError::invalid_input(
            "Vector exists is not supported inside transactions. \
             Use VectorStore::exists() directly."
                .to_string(),
        ))
    }

    // =========================================================================
    // Branch Operations — not supported in transactions
    // =========================================================================

    fn branch_metadata(&self) -> Result<Option<Versioned<BranchMetadata>>, StrataError> {
        Err(StrataError::invalid_input(
            "Branch metadata is not supported inside transactions. \
             Use BranchIndex methods directly."
                .to_string(),
        ))
    }

    fn branch_update_status(&mut self, _status: BranchStatus) -> Result<Version, StrataError> {
        Err(StrataError::invalid_input(
            "Branch status update is not supported inside transactions. \
             Use BranchIndex methods directly."
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_concurrency::snapshot::ClonedSnapshotView;

    fn create_test_namespace() -> Arc<Namespace> {
        let branch_id = BranchId::new();
        Arc::new(Namespace::new(
            branch_id,
            "default".to_string(),
        ))
    }

    fn create_test_context(ns: &Namespace) -> TransactionContext {
        let snapshot = Box::new(ClonedSnapshotView::empty(100));
        TransactionContext::with_snapshot(1, ns.branch_id, snapshot)
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
        let txn = Transaction::new(&mut ctx, ns.clone());

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
    fn test_json_destroy() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Create then destroy
        let doc: JsonValue = serde_json::json!({"data": true}).into();
        txn.json_create("doc", doc).unwrap();
        let existed = txn.json_destroy("doc").unwrap();
        assert!(existed);

        // Should no longer exist
        assert!(!txn.json_exists("doc").unwrap());
    }

    #[test]
    fn test_json_get_nonexistent() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let txn = Transaction::new(&mut ctx, ns.clone());

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
}
