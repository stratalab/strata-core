//! Event storage envelopes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use strata_core::Timestamp;

use crate::diagnostics::EngineError;

use super::hash::hash_version;
use super::{compute_event_hash, EventHash, EventPayload, EventSequence, EventType};

/// The event record format this build writes (#3594): the version byte, the
/// two 32-byte hashes as raw bytes (`previous_hash` then `hash`), then the
/// JSON body without them. Version 1 carried both hashes inside the JSON as
/// arrays of 32 decimal numbers — more bytes than the average payload — and
/// stays readable, so a database written by an earlier 1.x build opens
/// unchanged.
const EVENT_RECORD_FORMAT_VERSION: u8 = 2;
/// The version-1 event record: the version byte, then the whole record as
/// JSON with both hashes inside it.
const EVENT_RECORD_FORMAT_VERSION_V1: u8 = 1;
/// Bytes of one event hash.
const EVENT_HASH_BYTES: usize = 32;
/// Bytes of the raw hash block ahead of a version-2 record's JSON body.
const EVENT_RECORD_V2_HASH_BYTES: usize = 64;
/// The event-log head this build writes (#3595): a fixed 51-byte binary
/// record — version, `next_sequence` (u64 LE), a last-timestamp presence byte
/// and value (u64 LE), `hash_version`, then the 32-byte head hash. It is
/// rewritten on every append commit, so it carries only what an append needs;
/// the log's event types are read from the type index, which every event
/// already writes.
/// Version 1 (a JSON map with one summary per type and the head hash as a
/// decimal array, ~800 bytes with four types) stays readable.
const EVENT_METADATA_FORMAT_VERSION: u8 = 2;
/// The version-1 head: the version byte, then JSON with per-type summaries.
const EVENT_METADATA_FORMAT_VERSION_V1: u8 = 1;
/// Bytes of a version-2 head.
const EVENT_METADATA_V2_LEN: usize = 51;

/// Stored event record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EventRecordEnvelope {
    sequence: EventSequence,
    event_type: EventType,
    payload: EventPayload,
    timestamp: Timestamp,
    previous_hash: EventHash,
    hash: EventHash,
}

impl EventRecordEnvelope {
    pub(crate) const fn new(
        sequence: EventSequence,
        event_type: EventType,
        payload: EventPayload,
        timestamp: Timestamp,
        previous_hash: EventHash,
        hash: EventHash,
    ) -> Self {
        Self {
            sequence,
            event_type,
            payload,
            timestamp,
            previous_hash,
            hash,
        }
    }

    pub(crate) const fn sequence(&self) -> EventSequence {
        self.sequence
    }

    pub(crate) const fn event_type(&self) -> &EventType {
        &self.event_type
    }

    pub(crate) const fn payload(&self) -> &EventPayload {
        &self.payload
    }

    pub(crate) const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub(crate) const fn previous_hash(&self) -> EventHash {
        self.previous_hash
    }

    pub(crate) const fn hash(&self) -> EventHash {
        self.hash
    }
}

/// Per-type event summary — the version-1 head's shape, read only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
pub(crate) struct EventTypeSummary {
    count: u64,
    first_sequence: u64,
    last_sequence: u64,
    first_timestamp: u64,
    last_timestamp: u64,
}

impl EventTypeSummary {
    fn validate(self) -> bool {
        self.count > 0
            && self.first_sequence <= self.last_sequence
            && self.first_timestamp <= self.last_timestamp
    }
}

/// Stored event log head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EventLogMetadata {
    next_sequence: u64,
    head_hash: EventHash,
    hash_version: u8,
    last_timestamp: Option<u64>,
}

impl Default for EventLogMetadata {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            head_hash: [0; 32],
            hash_version: hash_version(),
            last_timestamp: None,
        }
    }
}

impl EventLogMetadata {
    pub(crate) const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub(crate) const fn head_hash(&self) -> EventHash {
        self.head_hash
    }

    pub(crate) const fn last_timestamp_micros(&self) -> Option<u64> {
        self.last_timestamp
    }

    pub(crate) fn push(&mut self, record: &EventRecordEnvelope) {
        let sequence = record.sequence().as_u64();
        self.next_sequence = sequence.saturating_add(1);
        self.head_hash = record.hash();
        self.hash_version = hash_version();
        let timestamp = record.timestamp().as_micros();
        self.last_timestamp = Some(
            self.last_timestamp
                .map_or(timestamp, |last| last.max(timestamp)),
        );
    }

    fn validate(&self) -> bool {
        self.hash_version == hash_version()
            && (self.last_timestamp.is_none() || self.next_sequence > 0)
    }
}

#[derive(Deserialize)]
struct StoredEventRecordV1 {
    sequence: u64,
    event_type: String,
    payload: serde_json::Value,
    timestamp: u64,
    previous_hash: EventHash,
    hash: EventHash,
}

/// The version-2 JSON body; the hashes precede it as raw bytes.
#[derive(Serialize, Deserialize)]
struct StoredEventRecordBody {
    sequence: u64,
    event_type: String,
    payload: serde_json::Value,
    timestamp: u64,
}

/// The version-1 head (read only).
#[derive(Deserialize)]
struct StoredEventMetadataV1 {
    next_sequence: u64,
    head_hash: EventHash,
    hash_version: u8,
    summaries: BTreeMap<String, EventTypeSummary>,
}

pub(crate) fn encode_event_record(record: &EventRecordEnvelope) -> Result<Vec<u8>, EngineError> {
    let body = StoredEventRecordBody {
        sequence: record.sequence().as_u64(),
        event_type: record.event_type().as_str().to_owned(),
        payload: record.payload().clone_inner(),
        timestamp: record.timestamp().as_micros(),
    };
    let mut bytes = Vec::with_capacity(1 + EVENT_RECORD_V2_HASH_BYTES + 128);
    bytes.push(EVENT_RECORD_FORMAT_VERSION);
    bytes.extend_from_slice(&record.previous_hash());
    bytes.extend_from_slice(&record.hash());
    serde_json::to_writer(&mut bytes, &body).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.event_record",
            format!("event record cannot be encoded: {error}"),
        )
    })?;
    Ok(bytes)
}

fn event_record_corruption(message: impl Into<String>) -> EngineError {
    EngineError::corruption("data_loss.engine.event_record", message.into())
}

pub(crate) fn decode_event_record(
    expected_sequence: EventSequence,
    bytes: &[u8],
) -> Result<EventRecordEnvelope, EngineError> {
    let (body, previous_hash, hash) = match bytes.split_first() {
        Some((&EVENT_RECORD_FORMAT_VERSION, rest)) => decode_event_record_v2(rest)?,
        Some((&EVENT_RECORD_FORMAT_VERSION_V1, rest)) => decode_event_record_v1(rest)?,
        _ => {
            return Err(event_record_corruption(
                "stored event record has an unknown format version",
            ))
        }
    };
    validate_event_record(expected_sequence, body, previous_hash, hash)
}

fn decode_event_record_v2(
    bytes: &[u8],
) -> Result<(StoredEventRecordBody, EventHash, EventHash), EngineError> {
    let Some((hashes, json)) = bytes.split_at_checked(EVENT_RECORD_V2_HASH_BYTES) else {
        return Err(event_record_corruption(
            "stored event record is shorter than its hash block",
        ));
    };
    let (previous, current) = hashes.split_at(EVENT_HASH_BYTES);
    let previous_hash: EventHash = previous
        .try_into()
        .map_err(|_| event_record_corruption("stored event previous hash is malformed"))?;
    let hash: EventHash = current
        .try_into()
        .map_err(|_| event_record_corruption("stored event hash is malformed"))?;
    let body = serde_json::from_slice::<StoredEventRecordBody>(json).map_err(|error| {
        event_record_corruption(format!("stored event record cannot be decoded: {error}"))
    })?;
    Ok((body, previous_hash, hash))
}

fn decode_event_record_v1(
    bytes: &[u8],
) -> Result<(StoredEventRecordBody, EventHash, EventHash), EngineError> {
    let stored = serde_json::from_slice::<StoredEventRecordV1>(bytes).map_err(|error| {
        event_record_corruption(format!("stored event record cannot be decoded: {error}"))
    })?;
    Ok((
        StoredEventRecordBody {
            sequence: stored.sequence,
            event_type: stored.event_type,
            payload: stored.payload,
            timestamp: stored.timestamp,
        },
        stored.previous_hash,
        stored.hash,
    ))
}

/// The checks every record format shares: the sequence matches the row key,
/// the type and payload respect engine limits, and the stored hash is the
/// hash of the content.
fn validate_event_record(
    expected_sequence: EventSequence,
    body: StoredEventRecordBody,
    previous_hash: EventHash,
    hash: EventHash,
) -> Result<EventRecordEnvelope, EngineError> {
    if body.sequence != expected_sequence.as_u64() {
        return Err(event_record_corruption(
            "stored event sequence does not match its row key",
        ));
    }
    let event_type = EventType::new(body.event_type)
        .map_err(|_| event_record_corruption("stored event type violates engine limits"))?;
    let payload = EventPayload::from_stored(body.payload)
        .map_err(|_| event_record_corruption("stored event payload violates engine limits"))?;
    let timestamp = Timestamp::from_micros(body.timestamp);
    let computed = compute_event_hash(
        body.sequence,
        &event_type,
        &payload,
        body.timestamp,
        &previous_hash,
    )?;
    if computed != hash {
        return Err(event_record_corruption(
            "stored event hash does not match its content",
        ));
    }
    Ok(EventRecordEnvelope::new(
        expected_sequence,
        event_type,
        payload,
        timestamp,
        previous_hash,
        hash,
    ))
}

pub(crate) fn encode_event_metadata(metadata: &EventLogMetadata) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(EVENT_METADATA_V2_LEN);
    bytes.push(EVENT_METADATA_FORMAT_VERSION);
    bytes.extend_from_slice(&metadata.next_sequence.to_le_bytes());
    bytes.push(u8::from(metadata.last_timestamp.is_some()));
    bytes.extend_from_slice(&metadata.last_timestamp.unwrap_or(0).to_le_bytes());
    bytes.push(metadata.hash_version);
    bytes.extend_from_slice(&metadata.head_hash);
    bytes
}

fn event_metadata_corruption(message: impl Into<String>) -> EngineError {
    EngineError::corruption("data_loss.engine.event_metadata", message.into())
}

pub(crate) fn decode_event_metadata(bytes: &[u8]) -> Result<EventLogMetadata, EngineError> {
    let metadata = match bytes.split_first() {
        Some((&EVENT_METADATA_FORMAT_VERSION, _)) => decode_event_metadata_v2(bytes)?,
        Some((&EVENT_METADATA_FORMAT_VERSION_V1, rest)) => decode_event_metadata_v1(rest)?,
        _ => {
            return Err(event_metadata_corruption(
                "stored event metadata has an unknown format version",
            ))
        }
    };
    if !metadata.validate() {
        return Err(event_metadata_corruption(
            "stored event metadata violates engine invariants",
        ));
    }
    Ok(metadata)
}

fn decode_event_metadata_v2(bytes: &[u8]) -> Result<EventLogMetadata, EngineError> {
    let fixed: &[u8; EVENT_METADATA_V2_LEN] = bytes
        .try_into()
        .map_err(|_| event_metadata_corruption("stored event metadata has the wrong length"))?;
    let (_, rest) = fixed.split_at(1);
    let (next_sequence, rest) = rest.split_at(8);
    let (has_timestamp, rest) = rest.split_at(1);
    let (timestamp, rest) = rest.split_at(8);
    let (hash_version, head_hash) = rest.split_at(1);
    let last_timestamp = match has_timestamp[0] {
        0 => None,
        1 => Some(u64::from_le_bytes(
            timestamp.try_into().expect("an eight-byte slice"),
        )),
        _ => {
            return Err(event_metadata_corruption(
                "stored event metadata has an invalid timestamp flag",
            ))
        }
    };
    Ok(EventLogMetadata {
        next_sequence: u64::from_le_bytes(next_sequence.try_into().expect("an eight-byte slice")),
        head_hash: head_hash.try_into().expect("a 32-byte slice"),
        hash_version: hash_version[0],
        last_timestamp,
    })
}

fn decode_event_metadata_v1(bytes: &[u8]) -> Result<EventLogMetadata, EngineError> {
    let stored = serde_json::from_slice::<StoredEventMetadataV1>(bytes).map_err(|error| {
        event_metadata_corruption(format!("stored event metadata cannot be decoded: {error}"))
    })?;
    let summaries_valid = stored
        .summaries
        .values()
        .all(|summary| summary.validate() && summary.last_sequence < stored.next_sequence);
    if !summaries_valid {
        return Err(event_metadata_corruption(
            "stored event metadata violates engine invariants",
        ));
    }
    Ok(EventLogMetadata {
        next_sequence: stored.next_sequence,
        head_hash: stored.head_hash,
        hash_version: stored.hash_version,
        last_timestamp: stored
            .summaries
            .values()
            .map(|summary| summary.last_timestamp)
            .max(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use strata_core::Timestamp;

    use super::{
        decode_event_metadata, decode_event_record, encode_event_metadata, encode_event_record,
        EventLogMetadata, EventRecordEnvelope,
    };
    use crate::data::event::{compute_event_hash, EventPayload, EventSequence, EventType};
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn event_record_and_metadata_envelopes_round_trip() {
        let event_type = EventType::new("order.created").expect("valid type");
        let payload = EventPayload::new(json!({"id": 1})).expect("valid payload");
        let previous_hash = [0; 32];
        let timestamp = Timestamp::from_micros(42);
        let hash = compute_event_hash(
            0,
            &event_type,
            &payload,
            timestamp.as_micros(),
            &previous_hash,
        )
        .expect("hash");
        let record = EventRecordEnvelope::new(
            EventSequence::new(0),
            event_type,
            payload,
            timestamp,
            previous_hash,
            hash,
        );
        let encoded = encode_event_record(&record).expect("encoded record");
        assert_eq!(
            decode_event_record(EventSequence::new(0), &encoded).expect("decoded record"),
            record
        );

        let mut metadata = EventLogMetadata::default();
        metadata.push(&record);
        let encoded = encode_event_metadata(&metadata);
        assert_eq!(
            decode_event_metadata(&encoded).expect("decoded metadata"),
            metadata
        );
    }

    #[test]
    fn event_record_decode_rejects_mismatched_sequence() {
        let event_type = EventType::new("order.created").expect("valid type");
        let payload = EventPayload::new(json!({})).expect("valid payload");
        let hash = compute_event_hash(0, &event_type, &payload, 1, &[0; 32]).expect("hash");
        let record = EventRecordEnvelope::new(
            EventSequence::new(0),
            event_type,
            payload,
            Timestamp::from_micros(1),
            [0; 32],
            hash,
        );
        let encoded = encode_event_record(&record).expect("encoded record");
        let error = decode_event_record(EventSequence::new(1), &encoded)
            .expect_err("sequence mismatch rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_record");
    }

    #[test]
    fn event_record_decode_rejects_unknown_version_and_hash_mismatch() {
        let error =
            decode_event_record(EventSequence::new(0), &[3]).expect_err("unknown version rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_record");

        let stored = json!({
            "sequence": 0,
            "event_type": "order.created",
            "payload": {"id": 1},
            "timestamp": 42,
            "previous_hash": vec![0_u8; 32],
            "hash": vec![1_u8; 32],
        });
        let mut bytes = vec![1];
        bytes.extend(serde_json::to_vec(&stored).expect("stored record encodes"));
        let error =
            decode_event_record(EventSequence::new(0), &bytes).expect_err("hash mismatch rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_record");
    }

    #[test]
    fn event_metadata_decode_rejects_unknown_version_and_invalid_summaries() {
        let error = decode_event_metadata(&[3]).expect_err("unknown version rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_metadata");

        let stored = json!({
            "next_sequence": 0,
            "head_hash": vec![0_u8; 32],
            "hash_version": 1,
            "summaries": {
                "order.created": {
                    "count": 1,
                    "first_sequence": 0,
                    "last_sequence": 0,
                    "first_timestamp": 42,
                    "last_timestamp": 42
                }
            }
        });
        let mut bytes = vec![1];
        bytes.extend(serde_json::to_vec(&stored).expect("stored metadata encodes"));
        let error = decode_event_metadata(&bytes).expect_err("invalid summary rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_metadata");
    }

    fn sample_record() -> EventRecordEnvelope {
        let event_type = EventType::new("tool_call").expect("valid type");
        let payload = EventPayload::new(json!({"turn": 7, "tool": "search"})).expect("payload");
        let previous_hash = [0xab; 32];
        let timestamp = Timestamp::from_micros(1_700_000_000);
        let hash = compute_event_hash(
            7,
            &event_type,
            &payload,
            timestamp.as_micros(),
            &previous_hash,
        )
        .expect("hash");
        EventRecordEnvelope::new(
            EventSequence::new(7),
            event_type,
            payload,
            timestamp,
            previous_hash,
            hash,
        )
    }

    /// #3594: records a 1.2.x build wrote (version 1: both hashes as JSON
    /// number arrays inside the body) still decode, to the same envelope a
    /// version-2 record of the same event decodes to.
    #[test]
    fn a_version_one_record_still_decodes() {
        let record = sample_record();
        let stored = json!({
            "sequence": 7,
            "event_type": "tool_call",
            "payload": {"turn": 7, "tool": "search"},
            "timestamp": 1_700_000_000_u64,
            "previous_hash": record.previous_hash().to_vec(),
            "hash": record.hash().to_vec(),
        });
        let mut v1 = vec![1];
        v1.extend(serde_json::to_vec(&stored).expect("v1 encodes"));
        assert_eq!(
            decode_event_record(EventSequence::new(7), &v1).expect("v1 decodes"),
            record
        );
        let v2 = encode_event_record(&record).expect("v2 encodes");
        assert_eq!(v2[0], 2, "this build writes version 2");
        assert_eq!(&v2[1..33], &record.previous_hash()[..]);
        assert_eq!(&v2[33..65], &record.hash()[..]);
        assert_eq!(
            decode_event_record(EventSequence::new(7), &v2).expect("v2 decodes"),
            record
        );
        // The point of the format: the two hashes cost 64 bytes, not ~230.
        assert!(
            v1.len() - v2.len() > 100,
            "v2 is materially smaller: v1={} v2={}",
            v1.len(),
            v2.len()
        );
    }

    /// Version-2 records fail closed like version 1: a truncated hash block and
    /// a hash that does not match the content are corruption, never a record.
    #[test]
    fn a_version_two_record_rejects_truncation_and_a_tampered_hash() {
        let record = sample_record();
        let encoded = encode_event_record(&record).expect("encoded");
        for bytes in [&encoded[..1], &encoded[..40], &encoded[..65]] {
            let error = decode_event_record(EventSequence::new(7), bytes)
                .expect_err("truncated record rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.event_record");
        }
        for index in [1, 33] {
            let mut tampered = encoded.clone();
            tampered[index] ^= 0x01;
            let error = decode_event_record(EventSequence::new(7), &tampered)
                .expect_err("tampered hash rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.event_record");
        }
    }

    /// #3595: the head is a fixed 51-byte record that round-trips, far
    /// smaller than the version-1 JSON map it replaces.
    #[test]
    fn a_version_two_head_is_fixed_size_and_round_trips() {
        let empty = EventLogMetadata::default();
        let encoded = encode_event_metadata(&empty);
        assert_eq!(encoded.len(), 51);
        assert_eq!(encoded[0], 2);
        assert_eq!(decode_event_metadata(&encoded).expect("decodes"), empty);

        let mut metadata = EventLogMetadata::default();
        metadata.push(&sample_record());
        let encoded = encode_event_metadata(&metadata);
        assert_eq!(encoded.len(), 51);
        let decoded = decode_event_metadata(&encoded).expect("decodes");
        assert_eq!(decoded, metadata);
        assert_eq!(decoded.next_sequence(), 8);
        assert_eq!(decoded.last_timestamp_micros(), Some(1_700_000_000));
        assert_eq!(decoded.head_hash(), sample_record().hash());
    }

    /// A version-2 head fails closed on a wrong length, an invalid timestamp
    /// flag, and a timestamp on an empty log.
    #[test]
    fn a_version_two_head_rejects_malformed_bytes() {
        let mut metadata = EventLogMetadata::default();
        metadata.push(&sample_record());
        let encoded = encode_event_metadata(&metadata);
        let mut bad_flag = encoded.clone();
        bad_flag[9] = 2;
        let mut timestamp_on_empty = encode_event_metadata(&EventLogMetadata::default());
        timestamp_on_empty[9] = 1;
        for bytes in [
            &encoded[..50],
            &[encoded.as_slice(), &[0]].concat(),
            &bad_flag,
            &timestamp_on_empty,
        ] {
            let error = decode_event_metadata(bytes).expect_err("malformed head rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.event_metadata");
        }
    }

    /// #3595: a version-1 head (a 1.2.x log) still decodes; its last timestamp
    /// is the summaries' maximum (its types are read from the type index).
    #[test]
    fn a_version_one_head_still_decodes() {
        let stored = json!({
            "next_sequence": 3,
            "head_hash": vec![7_u8; 32],
            "hash_version": 1,
            "summaries": {
                "a": {"count": 2, "first_sequence": 0, "last_sequence": 2,
                      "first_timestamp": 10, "last_timestamp": 30},
                "b": {"count": 1, "first_sequence": 1, "last_sequence": 1,
                      "first_timestamp": 20, "last_timestamp": 20}
            }
        });
        let mut v1 = vec![1];
        v1.extend(serde_json::to_vec(&stored).expect("v1 encodes"));
        let metadata = decode_event_metadata(&v1).expect("v1 decodes");
        assert_eq!(metadata.next_sequence(), 3);
        assert_eq!(metadata.head_hash(), [7_u8; 32]);
        assert_eq!(metadata.last_timestamp_micros(), Some(30));
        // The next write replaces it with a version-2 head.
        let rewritten = decode_event_metadata(&encode_event_metadata(&metadata)).expect("v2");
        assert_eq!(rewritten, metadata);
        assert!(v1.len() > 3 * encode_event_metadata(&metadata).len());
    }

    /// A version-1 head is rejected when one summary alone is wrong, each
    /// check on its own: a summary ending at or past `next_sequence`, and a
    /// summary that is internally invalid while its sequence is in range.
    #[test]
    fn a_version_one_head_rejects_each_bad_summary_on_its_own() {
        let head = |summary: serde_json::Value| {
            let stored = json!({
                "next_sequence": 2,
                "head_hash": vec![0_u8; 32],
                "hash_version": 1,
                "summaries": {"a": summary}
            });
            let mut bytes = vec![1];
            bytes.extend(serde_json::to_vec(&stored).expect("v1 encodes"));
            bytes
        };
        let valid = head(json!({"count": 1, "first_sequence": 1, "last_sequence": 1,
                                "first_timestamp": 5, "last_timestamp": 5}));
        decode_event_metadata(&valid).expect("a valid v1 head decodes");
        for bad in [
            // Ends at next_sequence: an event the head says was never written.
            json!({"count": 1, "first_sequence": 2, "last_sequence": 2,
                   "first_timestamp": 5, "last_timestamp": 5}),
            // In range, but a summary of zero events.
            json!({"count": 0, "first_sequence": 0, "last_sequence": 0,
                   "first_timestamp": 5, "last_timestamp": 5}),
        ] {
            let error = decode_event_metadata(&head(bad)).expect_err("bad summary rejected");
            assert_eq!(error.class(), EngineErrorClass::Corruption);
            assert_eq!(error.code(), "data_loss.engine.event_metadata");
        }
    }
}
