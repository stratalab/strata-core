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
const EVENT_METADATA_FORMAT_VERSION: u8 = 1;

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

/// Per-type event summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct EventTypeSummary {
    count: u64,
    first_sequence: u64,
    last_sequence: u64,
    first_timestamp: u64,
    last_timestamp: u64,
}

impl EventTypeSummary {
    pub(crate) const fn new(sequence: u64, timestamp: Timestamp) -> Self {
        Self {
            count: 1,
            first_sequence: sequence,
            last_sequence: sequence,
            first_timestamp: timestamp.as_micros(),
            last_timestamp: timestamp.as_micros(),
        }
    }

    pub(crate) fn update(&mut self, sequence: u64, timestamp: Timestamp) {
        self.count = self.count.saturating_add(1);
        self.last_sequence = sequence;
        self.last_timestamp = timestamp.as_micros();
    }

    pub(crate) const fn last_timestamp(self) -> u64 {
        self.last_timestamp
    }

    fn validate(self) -> bool {
        self.count > 0
            && self.first_sequence <= self.last_sequence
            && self.first_timestamp <= self.last_timestamp
    }
}

/// Stored event log metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EventLogMetadata {
    next_sequence: u64,
    head_hash: EventHash,
    hash_version: u8,
    summaries: BTreeMap<String, EventTypeSummary>,
}

impl Default for EventLogMetadata {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            head_hash: [0; 32],
            hash_version: hash_version(),
            summaries: BTreeMap::new(),
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

    pub(crate) fn event_types(&self) -> impl Iterator<Item = &str> {
        self.summaries.keys().map(String::as_str)
    }

    pub(crate) fn last_timestamp_micros(&self) -> Option<u64> {
        self.summaries
            .values()
            .map(|summary| summary.last_timestamp())
            .max()
    }

    pub(crate) fn push(&mut self, record: &EventRecordEnvelope) {
        let sequence = record.sequence().as_u64();
        self.next_sequence = sequence.saturating_add(1);
        self.head_hash = record.hash();
        self.hash_version = hash_version();
        self.summaries
            .entry(record.event_type().as_str().to_owned())
            .and_modify(|summary| summary.update(sequence, record.timestamp()))
            .or_insert_with(|| EventTypeSummary::new(sequence, record.timestamp()));
    }

    fn validate(&self) -> bool {
        self.hash_version == hash_version()
            && self
                .summaries
                .values()
                .all(|summary| summary.validate() && summary.last_sequence < self.next_sequence)
    }
}

/// The version-1 record body (read only).
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

#[derive(Serialize, Deserialize)]
struct StoredEventMetadata {
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

pub(crate) fn encode_event_metadata(metadata: &EventLogMetadata) -> Result<Vec<u8>, EngineError> {
    let stored = StoredEventMetadata {
        next_sequence: metadata.next_sequence,
        head_hash: metadata.head_hash,
        hash_version: metadata.hash_version,
        summaries: metadata.summaries.clone(),
    };
    let mut bytes = vec![EVENT_METADATA_FORMAT_VERSION];
    bytes.extend(serde_json::to_vec(&stored).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.event_metadata",
            format!("event metadata cannot be encoded: {error}"),
        )
    })?);
    Ok(bytes)
}

pub(crate) fn decode_event_metadata(bytes: &[u8]) -> Result<EventLogMetadata, EngineError> {
    if bytes.first().copied() != Some(EVENT_METADATA_FORMAT_VERSION) {
        return Err(EngineError::corruption(
            "data_loss.engine.event_metadata",
            "stored event metadata has an unknown format version",
        ));
    }
    let stored = serde_json::from_slice::<StoredEventMetadata>(&bytes[1..]).map_err(|error| {
        EngineError::corruption(
            "data_loss.engine.event_metadata",
            format!("stored event metadata cannot be decoded: {error}"),
        )
    })?;
    let metadata = EventLogMetadata {
        next_sequence: stored.next_sequence,
        head_hash: stored.head_hash,
        hash_version: stored.hash_version,
        summaries: stored.summaries,
    };
    if !metadata.validate() {
        return Err(EngineError::corruption(
            "data_loss.engine.event_metadata",
            "stored event metadata violates engine invariants",
        ));
    }
    Ok(metadata)
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
        let encoded = encode_event_metadata(&metadata).expect("encoded metadata");
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
        let error = decode_event_metadata(&[2]).expect_err("unknown version rejected");
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
}
