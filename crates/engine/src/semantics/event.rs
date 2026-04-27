//! Engine-owned event semantics surface.

use serde::{Deserialize, Serialize};
use strata_core::{Timestamp, Value};

/// An event in the append-only event log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    /// Sequence number (auto-assigned, monotonic per branch)
    pub sequence: u64,
    /// Event type (user-defined category)
    pub event_type: String,
    /// Event payload (arbitrary data)
    pub payload: Value,
    /// Timestamp when event was appended
    pub timestamp: Timestamp,
    /// Hash of previous event (for chaining)
    pub prev_hash: [u8; 32],
    /// Hash of this event
    pub hash: [u8; 32],
}

/// Result of verifying an event hash chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainVerification {
    /// Whether the chain is valid
    pub is_valid: bool,
    /// Total length of the chain
    pub length: u64,
    /// First invalid sequence number (if any)
    pub first_invalid: Option<u64>,
    /// Error description (if any)
    pub error: Option<String>,
}

impl ChainVerification {
    /// Create a valid verification result.
    pub fn valid(length: u64) -> Self {
        Self {
            is_valid: true,
            length,
            first_invalid: None,
            error: None,
        }
    }

    /// Create an invalid verification result.
    pub fn invalid(length: u64, first_invalid: u64, error: impl Into<String>) -> Self {
        Self {
            is_valid: false,
            length,
            first_invalid: Some(first_invalid),
            error: Some(error.into()),
        }
    }
}

impl From<strata_core::primitives::event::Event> for Event {
    fn from(value: strata_core::primitives::event::Event) -> Self {
        Self {
            sequence: value.sequence,
            event_type: value.event_type,
            payload: value.payload,
            timestamp: value.timestamp,
            prev_hash: value.prev_hash,
            hash: value.hash,
        }
    }
}

impl From<Event> for strata_core::primitives::event::Event {
    fn from(value: Event) -> Self {
        Self {
            sequence: value.sequence,
            event_type: value.event_type,
            payload: value.payload,
            timestamp: value.timestamp,
            prev_hash: value.prev_hash,
            hash: value.hash,
        }
    }
}

impl From<strata_core::primitives::event::ChainVerification> for ChainVerification {
    fn from(value: strata_core::primitives::event::ChainVerification) -> Self {
        Self {
            is_valid: value.is_valid,
            length: value.length,
            first_invalid: value.first_invalid,
            error: value.error,
        }
    }
}

impl From<ChainVerification> for strata_core::primitives::event::ChainVerification {
    fn from(value: ChainVerification) -> Self {
        if value.is_valid {
            Self::valid(value.length)
        } else {
            let first_invalid = value.first_invalid.unwrap_or(value.length);
            let error = value.error.unwrap_or_else(|| "invalid chain".to_string());
            Self::invalid(value.length, first_invalid, error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_roundtrips_through_serde() {
        let event = Event {
            sequence: 42,
            event_type: "order.placed".to_string(),
            payload: Value::String("order-123".to_string()),
            timestamp: Timestamp::from(1_700_000_000),
            prev_hash: [0xABu8; 32],
            hash: [0xCDu8; 32],
        };

        let json = serde_json::to_string(&event).unwrap();
        let restored: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event, restored);
    }

    #[test]
    fn chain_verification_helpers_work() {
        let valid = ChainVerification::valid(100);
        assert!(valid.is_valid);
        assert_eq!(valid.length, 100);
        assert_eq!(valid.first_invalid, None);
        assert_eq!(valid.error, None);

        let invalid = ChainVerification::invalid(100, 42, "hash mismatch at seq 42");
        assert!(!invalid.is_valid);
        assert_eq!(invalid.length, 100);
        assert_eq!(invalid.first_invalid, Some(42));
        assert_eq!(invalid.error.as_deref(), Some("hash mismatch at seq 42"));
    }

    #[test]
    fn legacy_event_conversions_preserve_data() {
        let legacy = strata_core::Event {
            sequence: 7,
            event_type: "user.created".to_string(),
            payload: Value::Int(42),
            timestamp: Timestamp::from(1_000_000),
            prev_hash: [1u8; 32],
            hash: [2u8; 32],
        };

        let engine: Event = legacy.clone().into();
        assert_eq!(engine.sequence, 7);
        assert_eq!(engine.event_type, "user.created");
        assert_eq!(engine.payload, Value::Int(42));
        assert_eq!(engine.timestamp, Timestamp::from(1_000_000));
        assert_eq!(engine.prev_hash, [1u8; 32]);
        assert_eq!(engine.hash, [2u8; 32]);

        let roundtrip: strata_core::Event = engine.into();
        assert_eq!(roundtrip, legacy);
    }

    #[test]
    fn legacy_chain_verification_conversions_preserve_data() {
        let legacy = strata_core::ChainVerification::invalid(10, 5, "broken chain");
        let engine: ChainVerification = legacy.into();
        assert!(!engine.is_valid);
        assert_eq!(engine.length, 10);
        assert_eq!(engine.first_invalid, Some(5));
        assert_eq!(engine.error.as_deref(), Some("broken chain"));
    }

    #[test]
    fn chain_verification_roundtrips_back_to_legacy_surface() {
        let engine = ChainVerification::invalid(12, 6, "hash mismatch");
        let legacy: strata_core::ChainVerification = engine.clone().into();
        assert!(!legacy.is_valid);
        assert_eq!(legacy.length, 12);
        assert_eq!(legacy.first_invalid, Some(6));
        assert_eq!(legacy.error.as_deref(), Some("hash mismatch"));

        let roundtrip: ChainVerification = legacy.into();
        assert_eq!(roundtrip, engine);
    }
}
