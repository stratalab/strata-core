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
    fn chain_verification_preserves_optional_fields() {
        let invalid = ChainVerification::invalid(12, 6, "hash mismatch");
        assert_eq!(invalid.first_invalid, Some(6));
        assert_eq!(invalid.error.as_deref(), Some("hash mismatch"));
    }
}
