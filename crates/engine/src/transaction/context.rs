//! Transaction wrapper implementing TransactionOps
//!
//! This module provides the Transaction type that wraps TransactionContext
//! and implements the TransactionOps trait for unified primitive access.
//!
//! # Supported Operations
//! - KV: key-value operations (get, put, delete, exists, list)
//! - Event: append-only event log operations
//! - JSON: document operations with path-based access
//!
//! This implementation provides:
//! - Read-your-writes semantics (check write set first)
//! - Read set tracking for conflict detection
//! - Proper key construction with namespaces
//! - Event buffering with sequence allocation
//! - JSON document operations via TransactionContext

use crate::primitives::event::{EventLogMeta, HASH_VERSION_SHA256};
use crate::primitives::json::JsonStore;
use crate::transaction_ops::TransactionOps;
use crate::{Event, JsonPath, JsonValue};
use std::sync::Arc;
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;
use strata_core::{EntityRef, StrataError, Timestamp, Value, Version, Versioned};
use strata_storage::{Key, Namespace, SegmentedStore, TransactionContext, TypeTag};

use super::json_state::JsonTxnState;

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
    /// Whether the event chain state is already initialized for this wrapper.
    ///
    /// Wrappers created with `new()` defer initialization until the first
    /// event operation so wrapper construction does not create extra reads.
    /// Wrappers created with `with_base_sequence()` keep the caller-provided
    /// state authoritative.
    event_state_initialized: bool,
    /// Optional engine-owned JSON transaction semantics.
    ///
    /// Manual transaction handles can borrow shared state from an outer owned
    /// transaction. Generic scoped wrappers do not own JSON state because
    /// there is no matching commit hook for them.
    json_state: Option<&'a mut JsonTxnState>,
    /// Snapshot store for first-seen JSON snapshot version tracking.
    ///
    /// JSON document reads themselves should come from the transaction view so
    /// generic raw writes/deletes remain visible when JSON semantics are mixed
    /// with lower write buffers in the same transaction.
    json_snapshot_store: Option<Arc<SegmentedStore>>,
}

impl<'a> Transaction<'a> {
    /// Create a new Transaction wrapper
    ///
    /// Event continuity is loaded lazily from the transaction context's
    /// staged event metadata on first use. This keeps primitive semantics in
    /// the engine while relying on the lower transaction buffer as the single
    /// source of truth for intra-transaction continuity.
    pub fn new(ctx: &'a mut TransactionContext, namespace: Arc<Namespace>) -> Self {
        Self {
            ctx,
            namespace,
            pending_events: Vec::new(),
            base_sequence: 0,
            last_hash: [0u8; 32],
            event_state_initialized: false,
            json_state: None,
            json_snapshot_store: None,
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
            event_state_initialized: true,
            json_state: None,
            json_snapshot_store: None,
        }
    }

    /// Create a transaction wrapper that shares engine-owned JSON semantics.
    pub fn with_json_state(
        ctx: &'a mut TransactionContext,
        namespace: Arc<Namespace>,
        json_state: &'a mut JsonTxnState,
        json_snapshot_store: Arc<SegmentedStore>,
    ) -> Self {
        Self {
            ctx,
            namespace,
            pending_events: Vec::new(),
            base_sequence: 0,
            last_hash: [0u8; 32],
            event_state_initialized: false,
            json_state: Some(json_state),
            json_snapshot_store: Some(json_snapshot_store),
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
    fn compute_event_hash(event: &Event) -> crate::StrataResult<[u8; 32]> {
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

    /// Create a JSON key for the given document ID
    fn json_key(&self, doc_id: &str) -> Key {
        Key::new_json(self.namespace.clone(), doc_id)
    }

    fn json_state(&self) -> Option<&JsonTxnState> {
        self.json_state.as_deref()
    }

    fn json_state_mut(&mut self) -> Option<&mut JsonTxnState> {
        self.json_state.as_deref_mut()
    }

    fn has_engine_json_state(&self) -> bool {
        self.json_state().is_some()
    }

    fn json_state_required_error() -> StrataError {
        StrataError::invalid_input(
            "JSON operations require a scoped transaction created from an owned transaction handle"
                .to_string(),
        )
    }

    fn ensure_json_writable(&self) -> Result<(), StrataError> {
        if self.ctx.is_read_only_mode() {
            return Err(StrataError::invalid_input(
                "Cannot write JSON in a read-only transaction".to_string(),
            ));
        }
        Ok(())
    }

    fn load_json_snapshot_base(&mut self, key: &Key) -> Result<Option<Value>, StrataError> {
        let store = self.json_snapshot_store.as_ref().ok_or_else(|| {
            StrataError::internal("engine-owned JSON state requires a snapshot store")
        })?;
        let versioned = store.get_versioned(key, self.ctx.start_version)?;

        if let Some(state) = self.json_state_mut() {
            if !state.snapshot_versions().contains_key(key) {
                match versioned.as_ref() {
                    Some(vv) => state
                        .record_snapshot_version(key.clone(), CommitVersion(vv.version.as_u64())),
                    None => state.record_snapshot_version(key.clone(), CommitVersion::ZERO),
                }
            }
        }

        Ok(versioned.map(|vv| vv.value))
    }

    fn load_json_effective_base(&mut self, key: &Key) -> Result<Option<Value>, StrataError> {
        let _ = self.load_json_snapshot_base(key)?;
        Ok(self.ctx.get(key)?)
    }

    fn load_json_document(&mut self, doc_id: &str) -> Result<Option<JsonValue>, StrataError> {
        let full_key = self.json_key(doc_id);
        if self.has_engine_json_state() {
            let has_writes = self
                .json_state()
                .expect("json state should be present")
                .has_writes_for_key(&full_key);
            let base = self.load_json_effective_base(&full_key)?;
            if has_writes {
                return self
                    .json_state()
                    .expect("json state should be present")
                    .preview_document(&full_key, |_| Ok(base.clone()))
                    .map(|doc| doc.map(|doc| doc.value));
            }

            return match base {
                Some(stored) => Ok(Some(
                    JsonStore::deserialize_doc_with_fallback_id(&stored, doc_id)?.value,
                )),
                None => Ok(None),
            };
        }

        Err(Self::json_state_required_error())
    }

    /// List JSON document ids visible in the current transaction view.
    pub fn json_list_keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>, StrataError> {
        let prefix_key = match prefix {
            Some(prefix) if !prefix.is_empty() => Key::new_json(self.namespace.clone(), prefix),
            _ => Key::new(self.namespace.clone(), TypeTag::Json, vec![]),
        };

        let mut keys = self
            .ctx
            .scan_prefix(&prefix_key)?
            .into_iter()
            .filter_map(|(key, _)| key.user_key_string())
            .collect::<std::collections::BTreeSet<_>>();

        if let Some(state) = self.json_state() {
            let touched_doc_ids = state
                .writes()
                .iter()
                .filter(|write| write.key.namespace == self.namespace)
                .filter_map(|write| write.key.user_key_string())
                .filter(|doc_id| prefix.is_none_or(|prefix| doc_id.starts_with(prefix)))
                .collect::<std::collections::BTreeSet<_>>();

            for doc_id in touched_doc_ids {
                if self.load_json_document(&doc_id)?.is_some() {
                    keys.insert(doc_id);
                } else {
                    keys.remove(&doc_id);
                }
            }
        }

        Ok(keys.into_iter().collect())
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
        Ok(result.map(|v| Versioned::new(v, Version::txn(self.ctx.txn_id.as_u64()))))
    }

    fn kv_put(&mut self, key: &str, value: Value) -> Result<Version, StrataError> {
        let full_key = self.kv_key(key);

        // Phase 3 contract: every write that touches a non-default,
        // non-system space must atomically register that space's
        // metadata key inside the same transaction. Without this, a
        // session-driven KV put against `tenant_x` commits the user
        // value but leaves the space invisible to `space list` /
        // `space exists` until startup repair re-discovers it via
        // data scan. The helper is idempotent and skips `default` /
        // `_system_`.
        let branch_id = self.branch_id();
        let space = self.namespace.space.as_str();
        crate::primitives::space::ensure_space_registered_in_txn(self.ctx, &branch_id, space)?;

        // Use the ctx.put() method which handles all the bookkeeping
        self.ctx.put(full_key, value)?;

        Ok(Version::txn(self.ctx.txn_id.as_u64()))
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
        // Phase 3 contract: register the space atomically with the
        // first write of this txn. See `kv_put` for the rationale —
        // session-driven event appends to a non-default space must
        // not leave the space invisible to discovery.
        let branch_id = self.branch_id();
        let space = self.namespace.space.as_str();
        crate::primitives::space::ensure_space_registered_in_txn(self.ctx, &branch_id, space)?;

        // Read persisted meta BEFORE writing — adds meta_key to the read_set
        // so OCC detects concurrent appends (#1914).
        // ctx.get() checks write_set first (our prior appends), then snapshot.
        let meta_key = Key::new_event_meta(self.namespace.clone());
        let persisted_meta: EventLogMeta = match self.ctx.get(&meta_key)? {
            Some(Value::String(s)) => {
                serde_json::from_str(&s).unwrap_or_else(|_| EventLogMeta::default())
            }
            _ => EventLogMeta::default(),
        };

        // On first append in a lazily initialized wrapper, load continuity
        // from the transaction view before assigning a new sequence.
        if !self.event_state_initialized {
            self.base_sequence = persisted_meta.next_sequence;
            self.last_hash = persisted_meta.head_hash;
            self.event_state_initialized = true;
        }

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
        event.hash = Self::compute_event_hash(&event)?;

        // Update last_hash for next event in chain
        self.last_hash = event.hash;

        // Write event to context as Value::String (matching EventLog primitive format)
        let event_key = self.event_key(sequence);
        let event_json = serde_json::to_string(&event).map_err(|e| StrataError::Serialization {
            message: e.to_string(),
        })?;
        self.ctx.put(event_key, Value::String(event_json))?;

        // Write per-type index key for efficient get_by_type lookups (#972, #1972)
        let idx_key = Key::new_event_type_idx(self.namespace.clone(), event_type, sequence);
        self.ctx.put(idx_key, Value::Null)?;

        // Write EventLogMeta — preserve streams from persisted meta (#1914)
        let mut streams = persisted_meta.streams;
        let ts_micros = timestamp.as_micros();
        if let Some(sm) = streams.get_mut(event_type) {
            sm.count += 1;
            sm.last_sequence = sequence;
            sm.last_timestamp = ts_micros;
        } else {
            streams.insert(
                event_type.to_string(),
                crate::primitives::event::StreamMeta {
                    count: 1,
                    first_sequence: sequence,
                    last_sequence: sequence,
                    first_timestamp: ts_micros,
                    last_timestamp: ts_micros,
                },
            );
        }

        let meta = EventLogMeta {
            next_sequence: sequence + 1,
            head_hash: event.hash,
            hash_version: HASH_VERSION_SHA256,
            streams,
        };
        let meta_json = serde_json::to_string(&meta).map_err(|e| StrataError::Serialization {
            message: e.to_string(),
        })?;
        self.ctx.put(meta_key, Value::String(meta_json))?;

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
        // If this wrapper has not initialized event state yet, load the
        // current count from the transaction view instead of trusting the
        // default zero base.
        if !self.event_state_initialized && self.pending_events.is_empty() {
            let meta_key = Key::new_event_meta(self.namespace.clone());
            if let Some(Value::String(s)) = self.ctx.get(&meta_key)? {
                if let Ok(meta) = serde_json::from_str::<EventLogMeta>(&s) {
                    return Ok(meta.next_sequence);
                }
            }
        }
        Ok(self.base_sequence + self.pending_events.len() as u64)
    }

    // =========================================================================
    // Json Operations
    // =========================================================================

    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> Result<Version, StrataError> {
        if self.has_engine_json_state() {
            self.ensure_json_writable()?;
            let full_key = self.json_key(doc_id);

            if self.json_exists(doc_id)? {
                return Err(StrataError::invalid_operation(
                    EntityRef::json(self.branch_id(), self.namespace.space.as_str(), doc_id),
                    "document already exists",
                ));
            }

            let branch_id = self.branch_id();
            let space = self.namespace.space.as_str();
            crate::primitives::space::ensure_space_registered_in_txn(self.ctx, &branch_id, space)?;

            let _ = self.load_json_snapshot_base(&full_key)?;
            self.json_state_mut()
                .expect("json state should be present")
                .record_set(full_key, JsonPath::root(), value);

            return Ok(Version::txn(self.ctx.txn_id.as_u64()));
        }

        Err(Self::json_state_required_error())
    }

    fn json_get(&mut self, doc_id: &str) -> Result<Option<Versioned<JsonValue>>, StrataError> {
        if self.has_engine_json_state() {
            return Ok(self
                .load_json_document(doc_id)?
                .map(|value| Versioned::new(value, Version::txn(self.ctx.txn_id.as_u64()))));
        }

        Err(Self::json_state_required_error())
    }

    fn json_get_path(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
    ) -> Result<Option<JsonValue>, StrataError> {
        if self.has_engine_json_state() {
            return Ok(self
                .load_json_document(doc_id)?
                .and_then(|value| crate::get_at_path(&value, path).cloned()));
        }

        Err(Self::json_state_required_error())
    }

    fn json_set(
        &mut self,
        doc_id: &str,
        path: &JsonPath,
        value: JsonValue,
    ) -> Result<Version, StrataError> {
        if self.has_engine_json_state() {
            self.ensure_json_writable()?;
            let full_key = self.json_key(doc_id);

            let branch_id = self.branch_id();
            let space = self.namespace.space.as_str();
            crate::primitives::space::ensure_space_registered_in_txn(self.ctx, &branch_id, space)?;

            let _ = self.load_json_snapshot_base(&full_key)?;
            self.json_state_mut()
                .expect("json state should be present")
                .record_set(full_key, path.clone(), value);

            return Ok(Version::txn(self.ctx.txn_id.as_u64()));
        }

        Err(Self::json_state_required_error())
    }

    fn json_delete(&mut self, doc_id: &str) -> Result<bool, StrataError> {
        if self.has_engine_json_state() {
            self.ensure_json_writable()?;
            let full_key = self.json_key(doc_id);
            let existed = self.json_exists(doc_id)?;
            if existed {
                let _ = self.load_json_snapshot_base(&full_key)?;
                self.json_state_mut()
                    .expect("json state should be present")
                    .record_delete(full_key, JsonPath::root());
            }
            return Ok(existed);
        }

        Err(Self::json_state_required_error())
    }

    fn json_exists(&mut self, doc_id: &str) -> Result<bool, StrataError> {
        if self.has_engine_json_state() {
            return Ok(self.load_json_document(doc_id)?.is_some());
        }

        Err(Self::json_state_required_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::id::{CommitVersion, TxnId};
    use strata_storage::{SegmentedStore, WriteMode};

    fn create_test_namespace() -> Arc<Namespace> {
        let branch_id = BranchId::new();
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    fn create_test_context(ns: &Namespace) -> TransactionContext {
        let store = Arc::new(SegmentedStore::new());
        TransactionContext::with_store(TxnId(1), ns.branch_id, store)
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
        let store = Arc::new(SegmentedStore::new());
        let last_hash = [42u8; 32];
        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
        let mut txn = Transaction::with_base_sequence(&mut ctx, ns.clone(), 100, last_hash);

        // The explicit base is authoritative even when the store has no event
        // metadata to read from.
        assert_eq!(txn.event_len().unwrap(), 100);

        // New events should continue from the explicit base.
        let v1 = txn.event_append("new_event", Value::Int(1)).unwrap();
        assert_eq!(v1, Version::seq(100));
        assert_eq!(txn.event_len().unwrap(), 101);

        // The event should chain from the provided last_hash
        let event = txn.event_get(100).unwrap().unwrap();
        assert_eq!(event.value.prev_hash, last_hash);
    }

    #[test]
    fn test_event_continuity_across_scoped_wrappers() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);

        let first_hash = {
            let mut txn = Transaction::new(&mut ctx, ns.clone());
            let version = txn.event_append("first", Value::Int(1)).unwrap();
            assert_eq!(version, Version::seq(0));

            let first = txn.event_get(0).unwrap().unwrap();
            first.value.hash
        };

        {
            let mut txn = Transaction::new(&mut ctx, ns.clone());
            let version = txn.event_append("second", Value::Int(2)).unwrap();
            assert_eq!(version, Version::seq(1));

            let second = txn.event_get(1).unwrap().unwrap();
            assert_eq!(second.value.prev_hash, first_hash);
        }
    }

    #[test]
    fn test_event_continuity_comes_from_staged_meta() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        let last_hash = [9u8; 32];
        let meta = EventLogMeta {
            next_sequence: 7,
            head_hash: last_hash,
            hash_version: HASH_VERSION_SHA256,
            streams: Default::default(),
        };
        let meta_json = serde_json::to_string(&meta).unwrap();
        let meta_key = Key::new_event_meta(ns.clone());
        let version = CommitVersion(store.next_version());
        store
            .put_with_version_mode(
                meta_key,
                Value::String(meta_json),
                version,
                None,
                WriteMode::Append,
            )
            .unwrap();

        let mut ctx = TransactionContext::with_store(TxnId(3), ns.branch_id, store);

        {
            let mut txn = Transaction::new(&mut ctx, ns.clone());
            assert_eq!(txn.event_len().unwrap(), 7);
            let version = txn.event_append("next", Value::Int(1)).unwrap();
            assert_eq!(version, Version::seq(7));
            let event = txn.event_get(7).unwrap().unwrap();
            assert_eq!(event.value.prev_hash, last_hash);
        }

        {
            let mut txn = Transaction::new(&mut ctx, ns.clone());
            assert_eq!(txn.event_len().unwrap(), 8);
            let version = txn.event_append("again", Value::Int(2)).unwrap();
            assert_eq!(version, Version::seq(8));
            let event = txn.event_get(8).unwrap().unwrap();
            assert_ne!(event.value.prev_hash, [0u8; 32]);
        }
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
    // Regression: #1972 — event_append writes per-type index key
    // =========================================================================

    #[test]
    fn test_event_append_writes_type_index_key() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        txn.event_append("user_created", Value::Int(1)).unwrap();
        txn.event_append("order_placed", Value::Int(2)).unwrap();
        txn.event_append("user_created", Value::Int(3)).unwrap();

        // The type index keys should be in the write set (via ctx)
        let idx_key_0 = Key::new_event_type_idx(ns.clone(), "user_created", 0);
        let idx_key_1 = Key::new_event_type_idx(ns.clone(), "order_placed", 1);
        let idx_key_2 = Key::new_event_type_idx(ns.clone(), "user_created", 2);

        // All three index keys should be Null in the transaction context
        assert_eq!(txn.ctx.get(&idx_key_0).unwrap(), Some(Value::Null));
        assert_eq!(txn.ctx.get(&idx_key_1).unwrap(), Some(Value::Null));
        assert_eq!(txn.ctx.get(&idx_key_2).unwrap(), Some(Value::Null));

        // Non-existent type index should not be present
        let missing = Key::new_event_type_idx(ns.clone(), "nonexistent", 0);
        assert_eq!(txn.ctx.get(&missing).unwrap(), None);
    }

    // =========================================================================
    // Regression: #1973 — event_len reads persisted meta before first append
    // =========================================================================

    #[test]
    fn test_event_len_with_committed_events_no_appends() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());

        // Pre-populate 50 committed events in store metadata
        let meta = EventLogMeta {
            next_sequence: 50,
            head_hash: [7u8; 32],
            hash_version: HASH_VERSION_SHA256,
            streams: Default::default(),
        };
        let meta_json = serde_json::to_string(&meta).unwrap();
        let meta_key = Key::new_event_meta(ns.clone());
        let version = CommitVersion(store.next_version());
        store
            .put_with_version_mode(
                meta_key,
                Value::String(meta_json),
                version,
                None,
                WriteMode::Append,
            )
            .unwrap();

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Before any append, event_len should reflect committed count
        assert_eq!(txn.event_len().unwrap(), 50);
    }

    #[test]
    fn test_event_len_with_committed_events_after_append() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());

        // Pre-populate 10 committed events in store metadata
        let meta = EventLogMeta {
            next_sequence: 10,
            head_hash: [5u8; 32],
            hash_version: HASH_VERSION_SHA256,
            streams: Default::default(),
        };
        let meta_json = serde_json::to_string(&meta).unwrap();
        let meta_key = Key::new_event_meta(ns.clone());
        let version = CommitVersion(store.next_version());
        store
            .put_with_version_mode(
                meta_key,
                Value::String(meta_json),
                version,
                None,
                WriteMode::Append,
            )
            .unwrap();

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Before append: should read 10 from persisted meta
        assert_eq!(txn.event_len().unwrap(), 10);

        // After append: should be 11 (base_sequence gets initialized + 1 pending)
        txn.event_append("new_event", Value::Int(1)).unwrap();
        assert_eq!(txn.event_len().unwrap(), 11);
    }

    // =========================================================================
    // JSON Tests
    // =========================================================================

    #[test]
    fn test_json_create_and_get() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(4), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

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
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(5), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

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
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(6), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

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
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(7), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

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
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(8), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

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
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(9), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

        // Getting non-existent document returns None
        let result = txn.json_get("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_get_path_root() {
        let ns = create_test_namespace();
        let store = Arc::new(SegmentedStore::new());
        let mut ctx = TransactionContext::with_store(TxnId(10), ns.branch_id, store.clone());
        let mut json_state = JsonTxnState::new();
        let mut txn = Transaction::with_json_state(&mut ctx, ns.clone(), &mut json_state, store);

        // Create a document
        let doc: JsonValue = serde_json::json!({"name": "Test"}).into();
        txn.json_create("doc", doc.clone()).unwrap();

        // Get at root path returns the whole document
        let result = txn.json_get_path("doc", &JsonPath::root()).unwrap();
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn test_json_requires_owned_transaction_handle() {
        let ns = create_test_namespace();
        let mut ctx = create_test_context(&ns);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        let err = txn
            .json_get("doc")
            .expect_err("generic scoped wrappers should not own JSON semantics");
        assert!(matches!(err, StrataError::InvalidInput { .. }));
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
        let k = Key::new_kv(Arc::new(ns.clone()), key);
        let version = CommitVersion(store.next_version());
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
        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
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

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
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

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
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

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
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

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
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

        let mut ctx = TransactionContext::with_store(TxnId(2), ns.branch_id, store);
        let mut txn = Transaction::new(&mut ctx, ns.clone());

        // Override the snapshot value
        txn.kv_put("key", Value::String("new".into())).unwrap();

        // Should see the new value, not the snapshot
        let result = txn.kv_get("key").unwrap().unwrap();
        assert_eq!(result.value, Value::String("new".into()));
    }
}
