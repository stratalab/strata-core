//! EventLog: Immutable append-only event stream primitive
//!
//! ## Role: Determinism Boundary Recorder
//!
//! EventLog records nondeterministic external inputs that cross the determinism boundary.
//! Its purpose is to enable deterministic replay of agent branches by capturing exactly
//! the information needed to reproduce nondeterministic behavior.
//!
//! Key invariant: If an operation's result is NOT recorded in EventLog, that operation
//! MUST be deterministic given the current state.
//!
//! ## Design Principles
//!
//! 1. **Single-Writer-Ordered**: All appends serialize through CAS on metadata key.
//!    Parallel append is NOT supported - event ordering must be total within a branch.
//!
//! 2. **Causal Hash Chaining**: Each event includes SHA-256 hash of previous event.
//!    Provides tamper-evidence and deterministic verification.
//!
//! 3. **Append-Only**: No update or delete operations - events are immutable.
//!
//! 4. **Object-Only Payloads**: All payloads must be JSON objects (not primitives/arrays).
//!
//! 5. **Global Sequences**: Streams are filters over a single global sequence per branch.
//!
//! ## Hash Chain
//!
//! Uses SHA-256 for deterministic cross-platform hashing. Hash version 1 computes:
//! SHA256(sequence || event_type_len || event_type || timestamp || payload_len || payload || prev_hash)
//!
//! ## Key Design
//!
//! - TypeTag: Event (0x02)
//! - Event key: `<namespace>:<TypeTag::Event>:<sequence_be_bytes>`
//! - Metadata key: `<namespace>:<TypeTag::Event>:__meta__`

use crate::database::Database;
use crate::primitives::extensions::EventLogExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use strata_concurrency::TransactionContext;
use strata_core::contract::{Timestamp, Version, Versioned};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::StrataError;
use strata_core::StrataResult;

// Re-export Event from core
pub use strata_core::primitives::Event;

/// Hash version constants
pub(crate) const HASH_VERSION_SHA256: u8 = 1; // SHA-256

/// Per-stream metadata for O(1) access to stream statistics
///
/// Note: The `sequences` field was removed in #972 to fix O(N) metadata growth.
/// Per-type sequence lookups now use separate index keys (see `Key::new_event_type_idx`).
/// Old metadata with `sequences` will deserialize correctly (serde ignores unknown fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    /// Number of events in this stream
    pub count: u64,
    /// First sequence number in this stream (global sequence)
    pub first_sequence: u64,
    /// Last sequence number in this stream (global sequence)
    pub last_sequence: u64,
    /// Timestamp of first event in stream (microseconds since epoch)
    pub first_timestamp: u64,
    /// Timestamp of last event in stream (microseconds since epoch)
    pub last_timestamp: u64,
}

impl StreamMeta {
    fn new(sequence: u64, timestamp: u64) -> Self {
        Self {
            count: 1,
            first_sequence: sequence,
            last_sequence: sequence,
            first_timestamp: timestamp,
            last_timestamp: timestamp,
        }
    }

    fn update(&mut self, sequence: u64, timestamp: u64) {
        self.count += 1;
        self.last_sequence = sequence;
        self.last_timestamp = timestamp;
    }
}

/// EventLog metadata stored per branch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EventLogMeta {
    /// Next sequence number to assign
    pub next_sequence: u64,
    /// Hash of the last event (head of chain)
    pub head_hash: [u8; 32],
    /// Hash algorithm version (0 = legacy DefaultHasher, 1 = SHA-256)
    #[serde(default)]
    pub hash_version: u8,
    /// Per-stream metadata for O(1) stream queries
    #[serde(default)]
    pub streams: HashMap<String, StreamMeta>,
}

impl Default for EventLogMeta {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            head_hash: [0u8; 32],
            hash_version: HASH_VERSION_SHA256, // New logs use SHA-256
            streams: HashMap::new(),
        }
    }
}

/// Compute event hash using SHA-256
///
/// Deterministic across platforms and Rust versions.
/// Format: SHA256(sequence || event_type_len || event_type || timestamp || payload_len || payload || prev_hash)
///
/// This is the canonical hash function for event chain integrity.
/// All code paths that compute event hashes MUST use this function.
pub fn compute_event_hash(
    sequence: u64,
    event_type: &str,
    payload: &Value,
    timestamp: u64,
    prev_hash: &[u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();

    // Sequence (8 bytes, little-endian)
    hasher.update(sequence.to_le_bytes());

    // Event type with length prefix (4 bytes length + content)
    hasher.update((event_type.len() as u32).to_le_bytes());
    hasher.update(event_type.as_bytes());

    // Timestamp (8 bytes, little-endian)
    hasher.update(timestamp.to_le_bytes());

    // Payload as canonical JSON with length prefix
    let payload_bytes = serde_json::to_vec(payload).unwrap_or_default();
    hasher.update((payload_bytes.len() as u32).to_le_bytes());
    hasher.update(&payload_bytes);

    // Previous hash (32 bytes)
    hasher.update(prev_hash);

    hasher.finalize().into()
}

/// Validation error for EventLog operations
#[derive(Debug, Clone, PartialEq)]
pub enum EventLogValidationError {
    /// Payload must be an object, not a primitive or array
    PayloadNotObject,
    /// Payload contains NaN or Infinity which are not valid JSON
    PayloadContainsNonFiniteFloat,
    /// Event type cannot be empty
    EmptyEventType,
    /// Event type cannot exceed maximum length
    EventTypeTooLong(usize),
}

impl std::fmt::Display for EventLogValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadNotObject => write!(f, "payload must be a JSON object"),
            Self::PayloadContainsNonFiniteFloat => write!(f, "payload contains NaN or Infinity"),
            Self::EmptyEventType => write!(f, "event_type cannot be empty"),
            Self::EventTypeTooLong(len) => write!(f, "event_type exceeds maximum length ({})", len),
        }
    }
}

/// Maximum allowed event type length
const MAX_EVENT_TYPE_LENGTH: usize = 256;

/// Validate event type
fn validate_event_type(event_type: &str) -> std::result::Result<(), EventLogValidationError> {
    if event_type.is_empty() {
        return Err(EventLogValidationError::EmptyEventType);
    }
    if event_type.len() > MAX_EVENT_TYPE_LENGTH {
        return Err(EventLogValidationError::EventTypeTooLong(event_type.len()));
    }
    Ok(())
}

/// Validate payload is an object and contains no non-finite floats
fn validate_payload(payload: &Value) -> std::result::Result<(), EventLogValidationError> {
    // Payload must be an object
    if !matches!(payload, Value::Object(_)) {
        return Err(EventLogValidationError::PayloadNotObject);
    }

    // Check for non-finite floats recursively
    if contains_non_finite_float(payload) {
        return Err(EventLogValidationError::PayloadContainsNonFiniteFloat);
    }

    Ok(())
}

/// Check if a Value contains NaN or Infinity
fn contains_non_finite_float(value: &Value) -> bool {
    match value {
        Value::Float(f) => !f.is_finite(),
        Value::Object(map) => map.values().any(contains_non_finite_float),
        Value::Array(arr) => arr.iter().any(contains_non_finite_float),
        _ => false,
    }
}

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
        _ => serde_json::from_str("null"), // Will fail with appropriate error
    }
}

/// Immutable append-only event stream
///
/// DESIGN: Single-writer-ordered per branch.
/// All appends serialize through CAS on metadata key.
///
/// # Example
///
/// ```text
/// use strata_primitives::EventLog;
/// use strata_engine::Database;
/// use strata_core::types::BranchId;
/// use strata_core::value::Value;
///
/// let db = Database::open("/path/to/data")?;
/// let log = EventLog::new(db);
/// let branch_id = BranchId::new();
///
/// // Append events
/// let (seq, hash) = log.append(&branch_id, "tool_call", Value::String("search".into()))?;
///
/// // Read events
/// let event = log.get(&branch_id, seq)?;
///
/// // Verify chain
/// let verification = log.verify_chain(&branch_id)?;
/// assert!(verification.is_valid);
/// ```
#[derive(Clone)]
pub struct EventLog {
    db: Arc<Database>,
}

impl EventLog {
    /// Create new EventLog instance
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Get the underlying database reference
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Build namespace for branch+space-scoped operations
    fn namespace_for(&self, branch_id: &BranchId, space: &str) -> Arc<Namespace> {
        Arc::new(Namespace::for_branch_space(*branch_id, space))
    }

    // ========== Append Operation ==========

    /// Core event creation: hash, build, store event + type index, update metadata chain.
    ///
    /// Used by both `append()` and `batch_append()`. Caller is responsible
    /// for writing the updated `meta` back to storage after all events.
    fn append_event_in_txn(
        txn: &mut TransactionContext,
        ns: &Arc<Namespace>,
        meta: &mut EventLogMeta,
        event_type: &str,
        payload: &Value,
    ) -> StrataResult<u64> {
        let sequence = meta.next_sequence;
        let timestamp = Timestamp::now();

        let hash = compute_event_hash(
            sequence,
            event_type,
            payload,
            timestamp.as_micros(),
            &meta.head_hash,
        );

        let event = Event {
            sequence,
            event_type: event_type.to_string(),
            payload: payload.clone(),
            timestamp,
            prev_hash: meta.head_hash,
            hash,
        };

        // Write event
        let event_key = Key::new_event(ns.clone(), sequence);
        txn.put(event_key, to_stored_value(&event)?)?;

        // Write per-type index key for efficient get_by_type lookups (#972)
        let idx_key = Key::new_event_type_idx(ns.clone(), event_type, sequence);
        txn.put(idx_key, Value::Null)?;

        // Update stream metadata
        match meta.streams.get_mut(event_type) {
            Some(stream_meta) => stream_meta.update(sequence, timestamp.as_micros()),
            None => {
                meta.streams.insert(
                    event_type.to_string(),
                    StreamMeta::new(sequence, timestamp.as_micros()),
                );
            }
        }

        // Update chain
        meta.next_sequence = sequence + 1;
        meta.head_hash = hash;

        Ok(sequence)
    }

    /// Append a new event to the log.
    ///
    /// Returns the assigned sequence version.
    pub fn append(
        &self,
        branch_id: &BranchId,
        space: &str,
        event_type: &str,
        payload: Value,
    ) -> StrataResult<Version> {
        validate_event_type(event_type).map_err(|e| StrataError::invalid_input(e.to_string()))?;
        validate_payload(&payload).map_err(|e| StrataError::invalid_input(e.to_string()))?;

        let ns = self.namespace_for(branch_id, space);

        let result = self.db.transaction(*branch_id, |txn| {
            let meta_key = Key::new_event_meta(ns.clone());
            let mut meta: EventLogMeta = match txn.get(&meta_key)? {
                Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
                None => EventLogMeta::default(),
            };

            let sequence = Self::append_event_in_txn(txn, &ns, &mut meta, event_type, &payload)?;

            txn.put(meta_key, to_stored_value(&meta)?)?;
            Ok(Version::Sequence(sequence))
        })?;

        // Update inverted index (zero overhead when disabled)
        let idx = self.db.extension::<crate::search::InvertedIndex>()?;
        if idx.is_enabled() {
            let text = format!(
                "{} {}",
                event_type,
                serde_json::to_string(&payload).unwrap_or_default()
            );
            if let Version::Sequence(seq) = result {
                let entity_ref = crate::search::EntityRef::Event {
                    branch_id: *branch_id,
                    sequence: seq,
                };
                idx.index_document(&entity_ref, &text, None);
            }
        }

        Ok(result)
    }

    // ========== Batch API ==========

    /// Append multiple events in a single transaction.
    ///
    /// All events share one lock acquisition, one WAL record, and one commit.
    /// Sequence monotonicity and hash chaining are preserved within the batch.
    /// Each entry reports success/failure independently.
    pub fn batch_append(
        &self,
        branch_id: &BranchId,
        space: &str,
        entries: Vec<(String, Value)>,
    ) -> StrataResult<Vec<Result<strata_core::contract::Version, String>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-validate all entries outside transaction
        let mut validation_errors: Vec<Option<String>> = vec![None; entries.len()];
        for (i, (event_type, payload)) in entries.iter().enumerate() {
            if let Err(e) = validate_event_type(event_type) {
                validation_errors[i] = Some(e.to_string());
                continue;
            }
            if let Err(e) = validate_payload(payload) {
                validation_errors[i] = Some(e.to_string());
                continue;
            }
        }

        // Collect indices of valid entries
        let valid_indices: Vec<usize> = validation_errors
            .iter()
            .enumerate()
            .filter(|(_, e)| e.is_none())
            .map(|(i, _)| i)
            .collect();

        if valid_indices.is_empty() {
            return Ok(validation_errors
                .into_iter()
                .map(|e| Err(e.unwrap()))
                .collect());
        }

        let ns = self.namespace_for(branch_id, space);

        let sequences = self.db.transaction(*branch_id, |txn| {
            let meta_key = Key::new_event_meta(ns.clone());
            let mut meta: EventLogMeta = match txn.get(&meta_key)? {
                Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
                None => EventLogMeta::default(),
            };

            let mut sequences = Vec::with_capacity(valid_indices.len());
            for &i in &valid_indices {
                let (event_type, payload) = &entries[i];
                let sequence = Self::append_event_in_txn(txn, &ns, &mut meta, event_type, payload)?;
                sequences.push(sequence);
            }

            // Write updated metadata once
            txn.put(meta_key, to_stored_value(&meta)?)?;
            Ok(sequences)
        })?;

        // Post-commit: update inverted index
        let idx = self.db.extension::<crate::search::InvertedIndex>()?;
        let idx_enabled = idx.is_enabled();

        // Build final results
        let mut results: Vec<Result<Version, String>> = Vec::with_capacity(entries.len());
        let mut valid_iter = sequences.into_iter();

        for (i, (event_type, payload)) in entries.iter().enumerate() {
            if let Some(ref err) = validation_errors[i] {
                results.push(Err(err.clone()));
            } else {
                let seq = valid_iter.next().unwrap();
                // Index the event
                if idx_enabled {
                    let text = format!(
                        "{} {}",
                        event_type,
                        serde_json::to_string(payload).unwrap_or_default()
                    );
                    let entity_ref = crate::search::EntityRef::Event {
                        branch_id: *branch_id,
                        sequence: seq,
                    };
                    idx.index_document(&entity_ref, &text, None);
                }
                results.push(Ok(Version::Sequence(seq)));
            }
        }

        Ok(results)
    }

    // ========== Read Operations ==========

    /// Read a single event by sequence number.
    ///
    /// Returns Versioned<Event> if found.
    pub fn get(
        &self,
        branch_id: &BranchId,
        space: &str,
        sequence: u64,
    ) -> StrataResult<Option<Versioned<Event>>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let event_key = Key::new_event(ns, sequence);

            match txn.get(&event_key)? {
                Some(v) => {
                    let event: Event = from_stored_value(&v)
                        .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                    Ok(Some(Versioned::with_timestamp(
                        event.clone(),
                        Version::Sequence(sequence),
                        event.timestamp,
                    )))
                }
                None => Ok(None),
            }
        })
    }

    /// Get the current length of the log.
    pub fn len(&self, branch_id: &BranchId, space: &str) -> StrataResult<u64> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let meta_key = Key::new_event_meta(ns);

            let meta: EventLogMeta = match txn.get(&meta_key)? {
                Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
                None => EventLogMeta::default(),
            };

            Ok(meta.next_sequence)
        })
    }

    /// List all known event types in the stream.
    ///
    /// Returns the event type names from the stream metadata.
    /// Returns an empty Vec if no events have been appended.
    pub fn list_types(&self, branch_id: &BranchId, space: &str) -> StrataResult<Vec<String>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);
            let meta_key = Key::new_event_meta(ns);

            let meta: EventLogMeta = match txn.get(&meta_key)? {
                Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
                None => return Ok(Vec::new()),
            };

            Ok(meta.streams.keys().cloned().collect())
        })
    }

    // ========== Query by Type ==========

    /// Read events filtered by type
    ///
    /// Returns Vec<Versioned<Event>> for events matching the type.
    /// Uses per-type index keys for O(K) lookup where K = events of that type.
    pub fn get_by_type(
        &self,
        branch_id: &BranchId,
        space: &str,
        event_type: &str,
        after_sequence: Option<u64>,
        limit: Option<usize>,
    ) -> StrataResult<Vec<Versioned<Event>>> {
        self.db.transaction(*branch_id, |txn| {
            let ns = self.namespace_for(branch_id, space);

            // Use per-type index keys for efficient lookup (#972)
            let idx_prefix = Key::new_event_type_idx_prefix(ns.clone(), event_type);
            let idx_entries = txn.scan_prefix(&idx_prefix)?;

            if !idx_entries.is_empty() {
                let capacity = limit.unwrap_or(idx_entries.len()).min(idx_entries.len());
                let mut results = Vec::with_capacity(capacity);
                for (idx_key, _) in &idx_entries {
                    // Extract sequence from the last 8 bytes of the user_key
                    let user_key = &idx_key.user_key;
                    if user_key.len() >= 8 {
                        let seq_bytes: [u8; 8] = user_key[user_key.len() - 8..].try_into().unwrap();
                        let seq = u64::from_be_bytes(seq_bytes);

                        if after_sequence.is_some_and(|after| seq <= after) {
                            continue;
                        }

                        let event_key = Key::new_event(ns.clone(), seq);
                        if let Some(v) = txn.get(&event_key)? {
                            let event: Event = from_stored_value(&v).map_err(|e| {
                                strata_core::StrataError::serialization(e.to_string())
                            })?;
                            results.push(Versioned::with_timestamp(
                                event.clone(),
                                Version::Sequence(seq),
                                event.timestamp,
                            ));

                            if limit.is_some_and(|l| results.len() >= l) {
                                break;
                            }
                        }
                    }
                }
                return Ok(results);
            }

            // Fallback: O(N) scan for old data without type index keys
            let meta_key = Key::new_event_meta(ns.clone());
            let meta: EventLogMeta = match txn.get(&meta_key)? {
                Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
                None => return Ok(Vec::new()),
            };

            let mut filtered = Vec::new();
            for seq in 0..meta.next_sequence {
                if after_sequence.is_some_and(|after| seq <= after) {
                    continue;
                }

                let event_key = Key::new_event(ns.clone(), seq);
                if let Some(v) = txn.get(&event_key)? {
                    let event: Event = from_stored_value(&v)
                        .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                    if event.event_type == event_type {
                        filtered.push(Versioned::with_timestamp(
                            event.clone(),
                            Version::Sequence(seq),
                            event.timestamp,
                        ));

                        if limit.is_some_and(|l| filtered.len() >= l) {
                            break;
                        }
                    }
                }
            }

            Ok(filtered)
        })
    }
    // ========== Time-Travel API ==========

    /// List events up to a given timestamp.
    ///
    /// Returns all events whose timestamp <= as_of_ts.
    /// Optionally filtered by event_type.
    pub fn list_at(
        &self,
        branch_id: &BranchId,
        space: &str,
        event_type: Option<&str>,
        as_of_ts: u64,
    ) -> StrataResult<Vec<Event>> {
        // Read metadata to get the total event count
        use strata_core::Storage;
        let ns = self.namespace_for(branch_id, space);
        let meta_key = Key::new_event_meta(ns.clone());
        let meta: EventLogMeta = match self.db.storage().get_versioned(&meta_key, u64::MAX)? {
            Some(vv) => from_stored_value(&vv.value).unwrap_or_else(|_| EventLogMeta::default()),
            None => return Ok(Vec::new()),
        };

        let mut events = Vec::new();
        for seq in 0..meta.next_sequence {
            let event_key = Key::new_event(ns.clone(), seq);
            // Use get_at_timestamp to get the event as it existed at that time
            if let Some(vv) = self.db.get_at_timestamp(&event_key, as_of_ts)? {
                let event: Event = from_stored_value(&vv.value)
                    .map_err(|e| strata_core::StrataError::serialization(e.to_string()))?;
                // Filter by event's own timestamp (when the event was appended)
                if event.timestamp.as_micros() <= as_of_ts {
                    if let Some(et) = event_type {
                        if event.event_type == et {
                            events.push(event);
                        }
                    } else {
                        events.push(event);
                    }
                }
            }
        }
        Ok(events)
    }
}

// ========== Searchable Trait Implementation ==========

impl crate::search::Searchable for EventLog {
    fn search(
        &self,
        _req: &crate::SearchRequest,
    ) -> strata_core::StrataResult<crate::SearchResponse> {
        // Search is handled by the intelligence layer, not the primitive
        Ok(crate::SearchResponse::empty())
    }

    fn primitive_kind(&self) -> strata_core::PrimitiveType {
        strata_core::PrimitiveType::Event
    }
}

// ========== EventLogExt Implementation ==========

impl EventLogExt for TransactionContext {
    fn event_append(&mut self, event_type: &str, payload: Value) -> StrataResult<u64> {
        // Validate inputs
        validate_event_type(event_type).map_err(|e| StrataError::invalid_input(e.to_string()))?;
        validate_payload(&payload).map_err(|e| StrataError::invalid_input(e.to_string()))?;

        let ns = Arc::new(Namespace::for_branch(self.branch_id));

        // Read current metadata (or default)
        let meta_key = Key::new_event_meta(ns.clone());
        let mut meta: EventLogMeta = match self.get(&meta_key)? {
            Some(v) => from_stored_value(&v).unwrap_or_else(|_| EventLogMeta::default()),
            None => EventLogMeta::default(),
        };

        // Compute event hash using current hash version
        let sequence = meta.next_sequence;
        let timestamp = Timestamp::now();

        let hash = compute_event_hash(
            sequence,
            event_type,
            &payload,
            timestamp.as_micros(),
            &meta.head_hash,
        );

        // Build event
        let event = Event {
            sequence,
            event_type: event_type.to_string(),
            payload: payload.clone(),
            timestamp,
            prev_hash: meta.head_hash,
            hash,
        };

        // Write event
        let event_key = Key::new_event(ns.clone(), sequence);
        self.put(event_key, to_stored_value(&event)?)?;

        // Write per-type index key for efficient get_by_type lookups (#972)
        let idx_key = Key::new_event_type_idx(ns.clone(), event_type, sequence);
        self.put(idx_key, Value::Null)?;

        // Update stream metadata
        let event_type_owned = event_type.to_string();
        match meta.streams.get_mut(&event_type_owned) {
            Some(stream_meta) => stream_meta.update(sequence, timestamp.as_micros()),
            None => {
                meta.streams.insert(
                    event_type_owned,
                    StreamMeta::new(sequence, timestamp.as_micros()),
                );
            }
        }

        // Update metadata
        meta.next_sequence = sequence + 1;
        meta.head_hash = hash;
        self.put(meta_key, to_stored_value(&meta)?)?;

        Ok(sequence)
    }

    fn event_get(&mut self, sequence: u64) -> StrataResult<Option<Value>> {
        let ns = Arc::new(Namespace::for_branch(self.branch_id));
        let event_key = Key::new_event(ns, sequence);
        self.get(&event_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<Database>, EventLog) {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path()).unwrap();
        let log = EventLog::new(db.clone());
        (temp_dir, db, log)
    }

    /// Helper to create an empty object payload
    fn empty_payload() -> Value {
        Value::object(HashMap::new())
    }

    /// Helper to create an object payload with a single value
    fn payload_with(key: &str, value: Value) -> Value {
        Value::object(HashMap::from([(key.to_string(), value)]))
    }

    /// Helper to create an object payload with an integer
    fn int_payload(v: i64) -> Value {
        payload_with("value", Value::Int(v))
    }

    // ========== Core Structure Tests ==========

    #[test]
    fn test_event_serialization() {
        let event = Event {
            sequence: 42,
            event_type: "test".to_string(),
            payload: payload_with("data", Value::String("test".into())),
            timestamp: Timestamp::from(1234567890),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };

        let json = serde_json::to_string(&event).unwrap();
        let restored: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event, restored);
    }

    #[test]
    fn test_eventlog_meta_default() {
        let meta = EventLogMeta::default();
        assert_eq!(meta.next_sequence, 0);
        assert_eq!(meta.head_hash, [0u8; 32]);
        assert_eq!(meta.hash_version, HASH_VERSION_SHA256);
        assert!(meta.streams.is_empty());
    }

    #[test]
    fn test_eventlog_creation() {
        let (_temp, _db, log) = setup();
        assert!(Arc::strong_count(log.database()) >= 1);
    }

    #[test]
    fn test_eventlog_is_clone() {
        let (_temp, _db, log1) = setup();
        let log2 = log1.clone();
        assert!(Arc::ptr_eq(log1.database(), log2.database()));
    }

    #[test]
    fn test_eventlog_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventLog>();
    }

    // ========== Validation Tests ==========

    #[test]
    fn test_validation_rejects_null_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let result = log.append(&branch_id, "default", "test", Value::Null);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("object"));
    }

    #[test]
    fn test_validation_rejects_primitive_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        // Test various primitive types
        assert!(log
            .append(&branch_id, "default", "test", Value::Int(42))
            .is_err());
        assert!(log
            .append(&branch_id, "default", "test", Value::String("hello".into()))
            .is_err());
        assert!(log
            .append(&branch_id, "default", "test", Value::Bool(true))
            .is_err());
        assert!(log
            .append(&branch_id, "default", "test", Value::Float(2.78))
            .is_err());
    }

    #[test]
    fn test_validation_rejects_array_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let result = log.append(
            &branch_id,
            "default",
            "test",
            Value::array(vec![Value::Int(1)]),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_rejects_nan_in_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = payload_with("value", Value::Float(f64::NAN));
        let result = log.append(&branch_id, "default", "test", payload);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN"));
    }

    #[test]
    fn test_validation_rejects_infinity_in_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = payload_with("value", Value::Float(f64::INFINITY));
        let result = log.append(&branch_id, "default", "test", payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_rejects_empty_event_type() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let result = log.append(&branch_id, "default", "", empty_payload());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_validation_rejects_too_long_event_type() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let long_type = "x".repeat(MAX_EVENT_TYPE_LENGTH + 1);
        let result = log.append(&branch_id, "default", &long_type, empty_payload());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length"));
    }

    #[test]
    fn test_validation_accepts_valid_object_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = Value::object(HashMap::from([
            ("tool".to_string(), Value::String("search".into())),
            ("count".to_string(), Value::Int(42)),
        ]));

        let result = log.append(&branch_id, "default", "test", payload);
        assert!(result.is_ok());
    }

    // ========== Append Tests ==========

    #[test]
    fn test_append_first_event() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let version = log
            .append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        assert!(matches!(version, Version::Sequence(0)));
    }

    #[test]
    fn test_append_increments_sequence() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let v1 = log
            .append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        let v2 = log
            .append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        let v3 = log
            .append(&branch_id, "default", "test", empty_payload())
            .unwrap();

        assert!(matches!(v1, Version::Sequence(0)));
        assert!(matches!(v2, Version::Sequence(1)));
        assert!(matches!(v3, Version::Sequence(2)));
    }

    #[test]
    fn test_hash_chain_links() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        let event1 = log.get(&branch_id, "default", 0).unwrap().unwrap();
        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();

        // Verify chain through read
        let event2 = log.get(&branch_id, "default", 1).unwrap().unwrap();
        assert_eq!(event2.value.prev_hash, event1.value.hash);
    }

    #[test]
    fn test_append_with_payload() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = Value::object(HashMap::from([
            ("tool".to_string(), Value::String("search".into())),
            ("query".to_string(), Value::String("rust async".into())),
        ]));

        let version = log
            .append(&branch_id, "default", "tool_call", payload.clone())
            .unwrap();
        let seq = match version {
            Version::Sequence(s) => s,
            _ => panic!("Expected sequence"),
        };
        let event = log.get(&branch_id, "default", seq).unwrap().unwrap();

        assert_eq!(event.value.event_type, "tool_call");
        assert_eq!(event.value.payload, payload);
    }

    #[test]
    fn test_branch_isolation() {
        let (_temp, _db, log) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        log.append(&branch1, "default", "branch1_event", int_payload(1))
            .unwrap();
        log.append(&branch1, "default", "branch1_event", int_payload(2))
            .unwrap();
        log.append(&branch2, "default", "branch2_event", int_payload(100))
            .unwrap();

        assert_eq!(log.len(&branch1, "default").unwrap(), 2);
        assert_eq!(log.len(&branch2, "default").unwrap(), 1);

        // Check branch1 events
        let event0 = log.get(&branch1, "default", 0).unwrap().unwrap();
        let event1 = log.get(&branch1, "default", 1).unwrap().unwrap();
        assert_eq!(event0.value.event_type, "branch1_event");
        assert_eq!(event1.value.event_type, "branch1_event");

        // Check branch2 events
        let event2 = log.get(&branch2, "default", 0).unwrap().unwrap();
        assert_eq!(event2.value.event_type, "branch2_event");
    }

    // ========== Read Tests ==========

    #[test]
    fn test_read_single_event() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = payload_with("data", Value::String("test".into()));
        log.append(&branch_id, "default", "test", payload.clone())
            .unwrap();

        let versioned = log.get(&branch_id, "default", 0).unwrap().unwrap();
        assert_eq!(versioned.value.sequence, 0);
        assert_eq!(versioned.value.event_type, "test");
        assert_eq!(versioned.value.payload, payload);
        assert!(matches!(versioned.version, Version::Sequence(0)));
    }

    #[test]
    fn test_read_nonexistent() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let event = log.get(&branch_id, "default", 999).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn test_len() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        assert_eq!(log.len(&branch_id, "default").unwrap(), 0);

        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        assert_eq!(log.len(&branch_id, "default").unwrap(), 1);

        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        assert_eq!(log.len(&branch_id, "default").unwrap(), 3);
    }

    // ========== SHA-256 Hash Tests ==========

    #[test]
    fn test_sha256_hash_determinism() {
        // Same inputs should produce same hash
        let hash1 = compute_event_hash(42, "test_event", &int_payload(100), 1234567890, &[0u8; 32]);
        let hash2 = compute_event_hash(42, "test_event", &int_payload(100), 1234567890, &[0u8; 32]);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_sha256_hash_differs_for_different_inputs() {
        let base = compute_event_hash(42, "test", &empty_payload(), 1234567890, &[0u8; 32]);

        // Different sequence
        let diff_seq = compute_event_hash(43, "test", &empty_payload(), 1234567890, &[0u8; 32]);
        assert_ne!(base, diff_seq);

        // Different event type
        let diff_type = compute_event_hash(42, "other", &empty_payload(), 1234567890, &[0u8; 32]);
        assert_ne!(base, diff_type);

        // Different timestamp
        let diff_ts = compute_event_hash(42, "test", &empty_payload(), 1234567891, &[0u8; 32]);
        assert_ne!(base, diff_ts);

        // Different prev_hash
        let diff_prev = compute_event_hash(42, "test", &empty_payload(), 1234567890, &[1u8; 32]);
        assert_ne!(base, diff_prev);
    }

    #[test]
    fn test_sha256_uses_full_32_bytes() {
        let hash = compute_event_hash(42, "test", &empty_payload(), 1234567890, &[0u8; 32]);

        // SHA-256 should use all 32 bytes, not just the first 8 like DefaultHasher
        // Check that bytes beyond the first 8 are non-zero (statistically likely)
        let non_zero_after_8: usize = hash[8..].iter().filter(|&&b| b != 0).count();
        assert!(non_zero_after_8 > 0, "SHA-256 should use all 32 bytes");
    }

    // ========== Query by Type Tests ==========

    #[test]
    fn test_get_by_type() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        log.append(&branch_id, "default", "tool_call", int_payload(1))
            .unwrap();
        log.append(&branch_id, "default", "tool_result", int_payload(2))
            .unwrap();
        log.append(&branch_id, "default", "tool_call", int_payload(3))
            .unwrap();
        log.append(&branch_id, "default", "thought", int_payload(4))
            .unwrap();
        log.append(&branch_id, "default", "tool_call", int_payload(5))
            .unwrap();

        let tool_calls = log
            .get_by_type(&branch_id, "default", "tool_call", None, None)
            .unwrap();
        assert_eq!(tool_calls.len(), 3);
        assert_eq!(tool_calls[0].value.payload, int_payload(1));
        assert_eq!(tool_calls[1].value.payload, int_payload(3));
        assert_eq!(tool_calls[2].value.payload, int_payload(5));

        let thoughts = log
            .get_by_type(&branch_id, "default", "thought", None, None)
            .unwrap();
        assert_eq!(thoughts.len(), 1);

        let nonexistent = log
            .get_by_type(&branch_id, "default", "nonexistent", None, None)
            .unwrap();
        assert!(nonexistent.is_empty());
    }

    // ========== EventLogExt Tests ==========

    #[test]
    fn test_eventlog_ext_append() {
        use crate::primitives::extensions::EventLogExt;

        let (_temp, db, log) = setup();
        let branch_id = BranchId::new();

        // Append via extension trait
        db.transaction(branch_id, |txn| {
            let seq = txn.event_append(
                "ext_event",
                payload_with("data", Value::String("test".into())),
            )?;
            assert_eq!(seq, 0);
            Ok(())
        })
        .unwrap();

        // Verify via EventLog
        let versioned = log.get(&branch_id, "default", 0).unwrap().unwrap();
        assert_eq!(versioned.value.event_type, "ext_event");
    }

    #[test]
    fn test_eventlog_ext_read() {
        use crate::primitives::extensions::EventLogExt;

        let (_temp, db, log) = setup();
        let branch_id = BranchId::new();

        // Append via EventLog
        log.append(&branch_id, "default", "test", int_payload(42))
            .unwrap();

        // Read via extension trait
        db.transaction(branch_id, |txn| {
            let value = txn.event_get(0)?;
            assert!(value.is_some());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_eventlog_ext_validation() {
        use crate::primitives::extensions::EventLogExt;

        let (_temp, db, _log) = setup();
        let branch_id = BranchId::new();

        // EventLogExt should also validate payloads
        let result = db.transaction(branch_id, |txn| {
            txn.event_append("test", Value::Int(42)) // primitive not allowed
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_cross_primitive_transaction() {
        use crate::primitives::extensions::{EventLogExt, KVStoreExt};

        let (_temp, db, _log) = setup();
        let branch_id = BranchId::new();

        // Atomic: KV put + event append
        db.transaction(branch_id, |txn| {
            txn.kv_put("key", Value::String("value".into()))?;
            txn.event_append(
                "kv_updated",
                payload_with("key", Value::String("key".into())),
            )?;
            Ok(())
        })
        .unwrap();

        // Verify both operations committed
        db.transaction(branch_id, |txn| {
            let kv_val = txn.kv_get("key")?;
            assert_eq!(kv_val, Some(Value::String("value".into())));

            let event_val = txn.event_get(0)?;
            assert!(event_val.is_some());
            Ok(())
        })
        .unwrap();
    }

    // ========== Fast Path Tests ==========

    #[test]
    fn test_fast_read_returns_correct_value() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let payload = payload_with("data", Value::String("test".into()));
        log.append(&branch_id, "default", "test", payload.clone())
            .unwrap();

        let versioned = log.get(&branch_id, "default", 0).unwrap().unwrap();
        assert_eq!(versioned.value.event_type, "test");
        assert_eq!(versioned.value.payload, payload);
    }

    #[test]
    fn test_fast_read_returns_none_for_missing() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let event = log.get(&branch_id, "default", 999).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn test_fast_len_returns_correct_count() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        assert_eq!(log.len(&branch_id, "default").unwrap(), 0);

        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        assert_eq!(log.len(&branch_id, "default").unwrap(), 1);

        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        log.append(&branch_id, "default", "test", empty_payload())
            .unwrap();
        assert_eq!(log.len(&branch_id, "default").unwrap(), 3);
    }

    #[test]
    fn test_fast_read_branch_isolation() {
        let (_temp, _db, log) = setup();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        log.append(&branch1, "default", "branch1", int_payload(1))
            .unwrap();
        log.append(&branch2, "default", "branch2", int_payload(2))
            .unwrap();

        // Each branch sees only its own events
        let event1 = log.get(&branch1, "default", 0).unwrap().unwrap();
        let event2 = log.get(&branch2, "default", 0).unwrap().unwrap();

        assert_eq!(event1.value.event_type, "branch1");
        assert_eq!(event2.value.event_type, "branch2");

        // Cross-branch reads return None
        assert!(log.get(&branch1, "default", 1).unwrap().is_none());
    }

    // ========== Batch API Tests ==========

    #[test]
    fn test_batch_append_basic() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let entries = vec![
            ("type_a".to_string(), int_payload(1)),
            ("type_b".to_string(), int_payload(2)),
            ("type_a".to_string(), int_payload(3)),
        ];

        let results = log.batch_append(&branch_id, "default", entries).unwrap();
        assert_eq!(results.len(), 3);
        assert!(matches!(results[0], Ok(Version::Sequence(0))));
        assert!(matches!(results[1], Ok(Version::Sequence(1))));
        assert!(matches!(results[2], Ok(Version::Sequence(2))));

        // Verify total count
        assert_eq!(log.len(&branch_id, "default").unwrap(), 3);
    }

    #[test]
    fn test_batch_append_empty() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let results = log.batch_append(&branch_id, "default", vec![]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_append_hash_chain_integrity() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let entries = vec![
            ("test".to_string(), int_payload(1)),
            ("test".to_string(), int_payload(2)),
            ("test".to_string(), int_payload(3)),
        ];

        log.batch_append(&branch_id, "default", entries).unwrap();

        // Verify hash chain
        let e0 = log.get(&branch_id, "default", 0).unwrap().unwrap();
        let e1 = log.get(&branch_id, "default", 1).unwrap().unwrap();
        let e2 = log.get(&branch_id, "default", 2).unwrap().unwrap();

        assert_eq!(e0.value.prev_hash, [0u8; 32]); // First event links to zero
        assert_eq!(e1.value.prev_hash, e0.value.hash); // Chain links
        assert_eq!(e2.value.prev_hash, e1.value.hash);
    }

    #[test]
    fn test_batch_append_monotonic_sequences() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        // Pre-existing event
        log.append(&branch_id, "default", "existing", empty_payload())
            .unwrap();

        let entries = vec![
            ("batch".to_string(), int_payload(1)),
            ("batch".to_string(), int_payload(2)),
        ];

        let results = log.batch_append(&branch_id, "default", entries).unwrap();
        assert!(matches!(results[0], Ok(Version::Sequence(1))));
        assert!(matches!(results[1], Ok(Version::Sequence(2))));
    }

    #[test]
    fn test_batch_append_partial_validation_failure() {
        let (_temp, _db, log) = setup();
        let branch_id = BranchId::new();

        let entries = vec![
            ("valid".to_string(), int_payload(1)),      // Valid
            ("".to_string(), int_payload(2)),           // Invalid: empty event type
            ("also_valid".to_string(), int_payload(3)), // Valid
            ("valid".to_string(), Value::Int(42)),      // Invalid: not an object
        ];

        let results = log.batch_append(&branch_id, "default", entries).unwrap();
        assert_eq!(results.len(), 4);
        assert!(results[0].is_ok()); // Sequence 0
        assert!(results[1].is_err()); // Validation error
        assert!(results[2].is_ok()); // Sequence 1
        assert!(results[3].is_err()); // Validation error

        // Only 2 events should be persisted
        assert_eq!(log.len(&branch_id, "default").unwrap(), 2);
    }
}
