//! Event hash helpers.

use sha2::{Digest, Sha256};

use crate::diagnostics::EngineError;

use super::{EventHash, EventPayload, EventType};

const HASH_VERSION_SHA256: u8 = 1;

pub(crate) const fn hash_version() -> u8 {
    HASH_VERSION_SHA256
}

pub(crate) fn compute_event_hash(
    sequence: u64,
    event_type: &EventType,
    payload: &EventPayload,
    timestamp_micros: u64,
    previous_hash: &EventHash,
) -> Result<EventHash, EngineError> {
    let mut hasher = Sha256::new();
    hasher.update(sequence.to_le_bytes());
    hasher.update(
        u32::try_from(event_type.as_str().len())
            .map_err(|_| {
                EngineError::invalid_input(
                    "invalid_argument.engine.event_type",
                    "event type is too long to hash",
                )
            })?
            .to_le_bytes(),
    );
    hasher.update(event_type.as_str().as_bytes());
    hasher.update(timestamp_micros.to_le_bytes());
    let payload_bytes = serde_json::to_vec(payload.as_inner()).map_err(|error| {
        EngineError::invalid_input(
            "invalid_argument.engine.event_payload",
            format!("event payload cannot be hashed: {error}"),
        )
    })?;
    hasher.update(
        u32::try_from(payload_bytes.len())
            .map_err(|_| {
                EngineError::invalid_input(
                    "invalid_argument.engine.event_payload",
                    "event payload is too large to hash",
                )
            })?
            .to_le_bytes(),
    );
    hasher.update(payload_bytes);
    hasher.update(previous_hash);
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use serde_json::json;

    use super::compute_event_hash;
    use crate::data::event::{EventPayload, EventType};

    #[test]
    fn event_hash_changes_with_each_input() {
        let event_type = EventType::new("test").expect("valid type");
        let other_type = EventType::new("other").expect("valid type");
        let payload = EventPayload::new(json!({"value": 1})).expect("valid payload");
        let other_payload = EventPayload::new(json!({"value": 2})).expect("valid payload");
        let previous = [0; 32];
        let other_previous = [1; 32];

        let base = compute_event_hash(0, &event_type, &payload, 10, &previous).expect("hash");
        assert_eq!(
            base,
            compute_event_hash(0, &event_type, &payload, 10, &previous).expect("hash")
        );
        assert_ne!(
            base,
            compute_event_hash(1, &event_type, &payload, 10, &previous).expect("hash")
        );
        assert_ne!(
            base,
            compute_event_hash(0, &other_type, &payload, 10, &previous).expect("hash")
        );
        assert_ne!(
            base,
            compute_event_hash(0, &event_type, &other_payload, 10, &previous).expect("hash")
        );
        assert_ne!(
            base,
            compute_event_hash(0, &event_type, &payload, 11, &previous).expect("hash")
        );
        assert_ne!(
            base,
            compute_event_hash(0, &event_type, &payload, 10, &other_previous).expect("hash")
        );
    }

    #[test]
    fn event_hash_fixtures_are_stable() {
        let first_type = EventType::new("order.created").expect("valid type");
        let first_payload = EventPayload::new(json!({"id": 1})).expect("valid payload");
        let first =
            compute_event_hash(0, &first_type, &first_payload, 42, &[0; 32]).expect("first hash");
        assert_eq!(
            hash_hex(first),
            "089ec9bd81c2df9327f297a4d78bbf5fbfbc850fc1ed1df57c43c202d61b97e6"
        );

        let second_type = EventType::new("order.updated").expect("valid type");
        let second_payload =
            EventPayload::new(json!({"id": 1, "status": "paid"})).expect("valid payload");
        let second =
            compute_event_hash(1, &second_type, &second_payload, 43, &first).expect("second hash");
        assert_eq!(
            hash_hex(second),
            "438ac24dbfab59c0fedd26c5fc991b667cb18c2d228b2be2a806defb01578f57"
        );
    }

    fn hash_hex(hash: [u8; 32]) -> String {
        hash.iter().fold(String::new(), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("write to string");
            output
        })
    }
}
