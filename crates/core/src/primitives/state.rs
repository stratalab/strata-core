//! State types for the StateCell primitive
//!
//! These types define the structure of versioned state cells.

use crate::contract::Timestamp;
use crate::contract::Version;
use crate::value::Value;
use serde::{Deserialize, Serialize};

/// Current state of a cell
///
/// Each state cell has:
/// - A value (arbitrary data)
/// - A version (Counter-based, monotonically increasing)
/// - A timestamp of last update
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    /// Current value
    pub value: Value,
    /// Version (Counter-based for CAS operations)
    pub version: Version,
    /// Last update timestamp (microseconds since epoch)
    pub updated_at: Timestamp,
}

impl State {
    /// Create a new state with version 1
    pub fn new(value: Value) -> Self {
        Self {
            value,
            version: Version::counter(1),
            updated_at: Self::now(),
        }
    }

    /// Create a new state with explicit version
    pub fn with_version(value: Value, version: Version) -> Self {
        Self {
            value,
            version,
            updated_at: Self::now(),
        }
    }

    /// Get current timestamp in microseconds
    pub fn now() -> Timestamp {
        Timestamp::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_new() {
        let state = State::new(Value::Int(42));
        assert_eq!(state.value, Value::Int(42));
        assert_eq!(state.version, Version::counter(1));
        assert!(state.updated_at > Timestamp::EPOCH);
    }

    #[test]
    fn test_state_with_version() {
        let state = State::with_version(Value::String("hello".to_string()), Version::counter(5));
        assert_eq!(state.value, Value::String("hello".to_string()));
        assert_eq!(state.version, Version::counter(5));
        assert!(state.updated_at > Timestamp::EPOCH);
    }

    #[test]
    fn test_state_equality() {
        // Two states with same fields are equal (timestamps will differ slightly)
        let s1 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        let s2 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_state_inequality_different_value() {
        let s1 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        let s2 = State {
            value: Value::Int(2),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_state_clone() {
        let s1 = State::new(Value::String("data".to_string()));
        let s2 = s1.clone();
        assert_eq!(s1.value, s2.value);
        assert_eq!(s1.version, s2.version);
    }

    #[test]
    fn test_state_serialization_roundtrip() {
        let state = State {
            value: Value::Int(99),
            version: Version::counter(3),
            updated_at: Timestamp::from(1_700_000_000),
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: State = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_state_version_starts_at_one() {
        let state = State::new(Value::Null);
        assert_eq!(state.version, Version::counter(1));
        assert!(state.version.is_counter());
    }

    #[test]
    fn test_state_with_non_counter_version() {
        // with_version accepts any Version, not just Counter
        let state = State::with_version(Value::Int(1), Version::txn(42));
        assert_eq!(state.version, Version::Txn(42));
        assert!(!state.version.is_counter());
    }

    #[test]
    fn test_state_inequality_different_version() {
        let s1 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        let s2 = State {
            value: Value::Int(1),
            version: Version::counter(2),
            updated_at: Timestamp::from(1000),
        };
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_state_inequality_different_timestamp() {
        let s1 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(1000),
        };
        let s2 = State {
            value: Value::Int(1),
            version: Version::counter(1),
            updated_at: Timestamp::from(2000),
        };
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_state_with_complex_value() {
        let complex = Value::object({
            let mut m = std::collections::HashMap::new();
            m.insert(
                "nested".to_string(),
                Value::array(vec![Value::Int(1), Value::Null]),
            );
            m
        });
        let state = State::new(complex.clone());
        assert_eq!(state.value, complex);

        // Verify roundtrip
        let json = serde_json::to_string(&state).unwrap();
        let restored: State = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_state_now_returns_reasonable_timestamp() {
        let before = Timestamp::now();
        let now = State::now();
        let after = Timestamp::now();
        assert!(now >= before);
        assert!(now <= after);
    }
}
