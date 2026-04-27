//! Event types for the EventLog primitive
//!
//! These types define the structure of events in the append-only event log.

use crate::contract::Timestamp;
use crate::value::Value;
use serde::{Deserialize, Serialize};

/// An event in the log
///
/// Events are immutable records in an append-only log. Each event includes:
/// - A monotonically increasing sequence number
/// - A user-defined event type for categorization
/// - An arbitrary payload
/// - Timestamp and hash chain for integrity
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

/// Chain verification result
///
/// Returned by `verify_chain()` to report the integrity status of an event chain.
#[derive(Debug, Clone)]
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
    /// Create a valid verification result
    pub fn valid(length: u64) -> Self {
        Self {
            is_valid: true,
            length,
            first_invalid: None,
            error: None,
        }
    }

    /// Create an invalid verification result
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
    fn test_event_fields() {
        let event = Event {
            sequence: 1,
            event_type: "user.created".to_string(),
            payload: Value::Int(42),
            timestamp: Timestamp::from(1_000_000),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };

        assert_eq!(event.sequence, 1);
        assert_eq!(event.event_type, "user.created");
        assert_eq!(event.payload, Value::Int(42));
        assert_eq!(event.timestamp, Timestamp::from(1_000_000));
        assert_eq!(event.prev_hash, [0u8; 32]);
        assert_eq!(event.hash, [1u8; 32]);
    }

    #[test]
    fn test_event_equality() {
        let e1 = Event {
            sequence: 1,
            event_type: "test".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn test_event_inequality_different_sequence() {
        let e1 = Event {
            sequence: 1,
            event_type: "test".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let mut e2 = e1.clone();
        e2.sequence = 2;
        assert_ne!(e1, e2);
    }

    #[test]
    fn test_event_clone_is_independent() {
        let e1 = Event {
            sequence: 1,
            event_type: "test".to_string(),
            payload: Value::String("data".to_string()),
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let mut e2 = e1.clone();
        e2.event_type = "modified".to_string();
        assert_eq!(e1.event_type, "test");
    }

    #[test]
    fn test_event_serialization_roundtrip() {
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
    fn test_chain_verification_valid() {
        let cv = ChainVerification::valid(100);
        assert!(cv.is_valid);
        assert_eq!(cv.length, 100);
        assert!(cv.first_invalid.is_none());
        assert!(cv.error.is_none());
    }

    #[test]
    fn test_chain_verification_invalid() {
        let cv = ChainVerification::invalid(100, 42, "hash mismatch at seq 42");
        assert!(!cv.is_valid);
        assert_eq!(cv.length, 100);
        assert_eq!(cv.first_invalid, Some(42));
        assert_eq!(cv.error.as_deref(), Some("hash mismatch at seq 42"));
    }

    #[test]
    fn test_chain_verification_invalid_accepts_string() {
        let cv = ChainVerification::invalid(10, 5, String::from("broken chain"));
        assert!(!cv.is_valid);
        assert_eq!(cv.error.as_deref(), Some("broken chain"));
    }

    #[test]
    fn test_event_empty_event_type() {
        let event = Event {
            sequence: 0,
            event_type: "".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(0),
            prev_hash: [0u8; 32],
            hash: [0u8; 32],
        };
        assert_eq!(event.event_type, "");
    }

    #[test]
    fn test_event_null_payload() {
        let event = Event {
            sequence: 1,
            event_type: "test".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        assert_eq!(event.payload, Value::Null);
    }

    #[test]
    fn test_event_inequality_different_hash() {
        let e1 = Event {
            sequence: 1,
            event_type: "t".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let mut e2 = e1.clone();
        e2.hash = [2u8; 32];
        assert_ne!(e1, e2);
    }

    #[test]
    fn test_event_inequality_different_payload() {
        let e1 = Event {
            sequence: 1,
            event_type: "t".to_string(),
            payload: Value::Int(1),
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let mut e2 = e1.clone();
        e2.payload = Value::Int(2);
        assert_ne!(e1, e2);
    }

    #[test]
    fn test_event_inequality_different_timestamp() {
        let e1 = Event {
            sequence: 1,
            event_type: "t".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(100),
            prev_hash: [0u8; 32],
            hash: [1u8; 32],
        };
        let mut e2 = e1.clone();
        e2.timestamp = Timestamp::from(200);
        assert_ne!(e1, e2);
    }

    #[test]
    fn test_event_max_sequence() {
        let event = Event {
            sequence: u64::MAX,
            event_type: "overflow".to_string(),
            payload: Value::Null,
            timestamp: Timestamp::from(0),
            prev_hash: [0u8; 32],
            hash: [0xFFu8; 32],
        };
        assert_eq!(event.sequence, u64::MAX);
        // Verify it serializes
        let json = serde_json::to_string(&event).unwrap();
        let restored: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.sequence, u64::MAX);
    }

    #[test]
    fn test_chain_verification_valid_length_zero() {
        let cv = ChainVerification::valid(0);
        assert!(cv.is_valid);
        assert_eq!(cv.length, 0);
    }
}
