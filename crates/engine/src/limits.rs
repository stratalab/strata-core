//! Engine-owned limits and validation policy.
//!
//! These limits are enforced at engine and executor boundaries. They define
//! the operational validation policy for keys, values, and vectors rather than
//! foundational shared language.

use strata_core::Value;
use thiserror::Error;

/// Size limits for values and keys.
///
/// These limits are enforced by the engine and wire decoding.
/// Violations return `ConstraintViolation` with appropriate reason codes.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum key length in bytes (default: 1024).
    pub max_key_bytes: usize,

    /// Maximum string length in bytes (default: 16MB).
    pub max_string_bytes: usize,

    /// Maximum bytes length (default: 16MB).
    pub max_bytes_len: usize,

    /// Maximum encoded value size in bytes (default: 32MB).
    pub max_value_bytes_encoded: usize,

    /// Maximum array length (default: 1M elements).
    pub max_array_len: usize,

    /// Maximum object entries (default: 1M entries).
    pub max_object_entries: usize,

    /// Maximum nesting depth (default: 128).
    pub max_nesting_depth: usize,

    /// Maximum vector dimensions (default: 8192).
    pub max_vector_dim: usize,

    /// Maximum number of keys to scan for fuzzy-match suggestions (default: 100).
    pub max_fuzzy_candidates: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_string_bytes: 16 * 1024 * 1024,
            max_bytes_len: 16 * 1024 * 1024,
            max_value_bytes_encoded: 32 * 1024 * 1024,
            max_array_len: 1_000_000,
            max_object_entries: 1_000_000,
            max_nesting_depth: 128,
            max_vector_dim: 8192,
            max_fuzzy_candidates: 100,
        }
    }
}

impl Limits {
    /// Create limits with small values for testing.
    pub fn with_small_limits() -> Self {
        Self {
            max_key_bytes: 100,
            max_string_bytes: 1000,
            max_bytes_len: 1000,
            max_value_bytes_encoded: 2000,
            max_array_len: 100,
            max_object_entries: 100,
            max_nesting_depth: 10,
            max_vector_dim: 100,
            max_fuzzy_candidates: 10,
        }
    }

    /// Validate a key length.
    pub fn validate_key_length(&self, key: &str) -> Result<(), LimitError> {
        let len = key.len();
        if len > self.max_key_bytes {
            return Err(LimitError::KeyTooLong {
                actual: len,
                max: self.max_key_bytes,
            });
        }
        Ok(())
    }

    /// Validate a value against structural size limits.
    pub fn validate_value(&self, value: &Value) -> Result<(), LimitError> {
        self.validate_value_impl(value, 0)
    }

    fn validate_value_impl(&self, value: &Value, depth: usize) -> Result<(), LimitError> {
        if depth > self.max_nesting_depth {
            return Err(LimitError::NestingTooDeep {
                actual: depth,
                max: self.max_nesting_depth,
            });
        }

        match value {
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => Ok(()),
            Value::String(s) => {
                if s.len() > self.max_string_bytes {
                    return Err(LimitError::ValueTooLarge {
                        reason: "string_too_long".to_string(),
                        actual: s.len(),
                        max: self.max_string_bytes,
                    });
                }
                Ok(())
            }
            Value::Bytes(b) => {
                if b.len() > self.max_bytes_len {
                    return Err(LimitError::ValueTooLarge {
                        reason: "bytes_too_long".to_string(),
                        actual: b.len(),
                        max: self.max_bytes_len,
                    });
                }
                Ok(())
            }
            Value::Array(arr) => {
                if arr.len() > self.max_array_len {
                    return Err(LimitError::ValueTooLarge {
                        reason: "array_too_long".to_string(),
                        actual: arr.len(),
                        max: self.max_array_len,
                    });
                }
                for v in arr.iter() {
                    self.validate_value_impl(v, depth + 1)?;
                }
                Ok(())
            }
            Value::Object(obj) => {
                if obj.len() > self.max_object_entries {
                    return Err(LimitError::ValueTooLarge {
                        reason: "object_too_many_entries".to_string(),
                        actual: obj.len(),
                        max: self.max_object_entries,
                    });
                }
                for v in obj.values() {
                    self.validate_value_impl(v, depth + 1)?;
                }
                Ok(())
            }
        }
    }

    /// Recursively reject non-finite floats that cannot be represented in JSON.
    fn reject_non_finite_floats(value: &Value) -> Result<(), LimitError> {
        match value {
            Value::Float(f) if !f.is_finite() => Err(LimitError::ValueTooLarge {
                reason: "value_not_serializable".to_string(),
                actual: 0,
                max: 0,
            }),
            Value::Array(arr) => {
                for v in arr.iter() {
                    Self::reject_non_finite_floats(v)?;
                }
                Ok(())
            }
            Value::Object(obj) => {
                for v in obj.values() {
                    Self::reject_non_finite_floats(v)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Validate a value against all limits in a single pass.
    pub fn validate_value_full(&self, value: &Value) -> Result<(), LimitError> {
        self.validate_value(value)?;
        Self::reject_non_finite_floats(value)?;

        let encoded_size =
            serde_json::to_vec(value)
                .map(|v| v.len())
                .map_err(|_| LimitError::ValueTooLarge {
                    reason: "value_not_serializable".to_string(),
                    actual: 0,
                    max: self.max_value_bytes_encoded,
                })?;

        if encoded_size > self.max_value_bytes_encoded {
            return Err(LimitError::ValueTooLarge {
                reason: "encoded_value_too_large".to_string(),
                actual: encoded_size,
                max: self.max_value_bytes_encoded,
            });
        }

        Ok(())
    }

    /// Validate a vector against dimension limits.
    pub fn validate_vector(&self, vec: &[f32]) -> Result<(), LimitError> {
        if vec.len() > self.max_vector_dim {
            return Err(LimitError::VectorDimExceeded {
                actual: vec.len(),
                max: self.max_vector_dim,
            });
        }
        Ok(())
    }

    /// Validate that a vector dimension matches the existing dimension.
    pub fn validate_vector_dimension_match(
        &self,
        existing_dim: usize,
        new_dim: usize,
    ) -> Result<(), LimitError> {
        if existing_dim != new_dim {
            return Err(LimitError::VectorDimMismatch {
                expected: existing_dim,
                actual: new_dim,
            });
        }
        Ok(())
    }
}

impl From<&strata_core::Limits> for Limits {
    fn from(value: &strata_core::Limits) -> Self {
        Self {
            max_key_bytes: value.max_key_bytes,
            max_string_bytes: value.max_string_bytes,
            max_bytes_len: value.max_bytes_len,
            max_value_bytes_encoded: value.max_value_bytes_encoded,
            max_array_len: value.max_array_len,
            max_object_entries: value.max_object_entries,
            max_nesting_depth: value.max_nesting_depth,
            max_vector_dim: value.max_vector_dim,
            max_fuzzy_candidates: value.max_fuzzy_candidates,
        }
    }
}

impl From<strata_core::Limits> for Limits {
    fn from(value: strata_core::Limits) -> Self {
        Self::from(&value)
    }
}

/// Limit validation errors.
///
/// These errors map to `ConstraintViolation` error codes in the wire protocol.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum LimitError {
    /// Key exceeds maximum length.
    #[error("Key too long: {actual} bytes exceeds maximum {max}")]
    KeyTooLong {
        /// Actual key length in bytes.
        actual: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Value exceeds size limits.
    #[error("Value too large ({reason}): {actual} exceeds maximum {max}")]
    ValueTooLarge {
        /// Reason code for the violation.
        reason: String,
        /// Actual size.
        actual: usize,
        /// Maximum allowed size.
        max: usize,
    },

    /// Value nesting exceeds maximum depth.
    #[error("Nesting too deep: {actual} levels exceeds maximum {max}")]
    NestingTooDeep {
        /// Actual nesting depth.
        actual: usize,
        /// Maximum allowed depth.
        max: usize,
    },

    /// Vector dimension exceeds maximum.
    #[error("Vector dimension exceeded: {actual} exceeds maximum {max}")]
    VectorDimExceeded {
        /// Actual vector dimension.
        actual: usize,
        /// Maximum allowed dimension.
        max: usize,
    },

    /// Vector dimension mismatch with existing vector.
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    VectorDimMismatch {
        /// Expected dimension.
        expected: usize,
        /// Actual dimension.
        actual: usize,
    },
}

impl LimitError {
    /// Get the reason code for wire protocol.
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::KeyTooLong { .. } => "key_too_long",
            Self::ValueTooLarge { reason, .. } => match reason.as_str() {
                "string_too_long" => "value_too_large",
                "bytes_too_long" => "value_too_large",
                "array_too_long" => "value_too_large",
                "object_too_many_entries" => "value_too_large",
                _ => "value_too_large",
            },
            Self::NestingTooDeep { .. } => "nesting_too_deep",
            Self::VectorDimExceeded { .. } => "vector_dim_exceeded",
            Self::VectorDimMismatch { .. } => "vector_dim_mismatch",
        }
    }

    /// Get the actual value that exceeded the limit.
    pub fn actual(&self) -> usize {
        match self {
            Self::KeyTooLong { actual, .. }
            | Self::ValueTooLarge { actual, .. }
            | Self::NestingTooDeep { actual, .. }
            | Self::VectorDimExceeded { actual, .. } => *actual,
            Self::VectorDimMismatch { actual, .. } => *actual,
        }
    }

    /// Get the maximum allowed value.
    pub fn max(&self) -> usize {
        match self {
            Self::KeyTooLong { max, .. }
            | Self::ValueTooLarge { max, .. }
            | Self::NestingTooDeep { max, .. }
            | Self::VectorDimExceeded { max, .. } => *max,
            Self::VectorDimMismatch { expected, .. } => *expected,
        }
    }
}

impl From<strata_core::LimitError> for LimitError {
    fn from(value: strata_core::LimitError) -> Self {
        match value {
            strata_core::LimitError::KeyTooLong { actual, max } => Self::KeyTooLong { actual, max },
            strata_core::LimitError::ValueTooLarge {
                reason,
                actual,
                max,
            } => Self::ValueTooLarge {
                reason,
                actual,
                max,
            },
            strata_core::LimitError::NestingTooDeep { actual, max } => {
                Self::NestingTooDeep { actual, max }
            }
            strata_core::LimitError::VectorDimExceeded { actual, max } => {
                Self::VectorDimExceeded { actual, max }
            }
            strata_core::LimitError::VectorDimMismatch { expected, actual } => {
                Self::VectorDimMismatch { expected, actual }
            }
            _ => Self::ValueTooLarge {
                reason: "legacy_limit_error".to_string(),
                actual: 0,
                max: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use strata_core::{LimitError as LegacyLimitError, Limits as LegacyLimits};

    fn create_nested_array(depth: usize) -> Value {
        let mut value = Value::Null;
        for _ in 0..depth {
            value = Value::array(vec![value]);
        }
        value
    }

    #[test]
    fn key_length_limits_are_enforced() {
        let limits = Limits::default();
        let key = "x".repeat(limits.max_key_bytes);
        assert!(limits.validate_key_length(&key).is_ok());

        let key = "x".repeat(limits.max_key_bytes + 1);
        assert!(matches!(
            limits.validate_key_length(&key),
            Err(LimitError::KeyTooLong { .. })
        ));

        let very_large_key = "x".repeat(10 * 1024);
        assert!(matches!(
            limits.validate_key_length(&very_large_key),
            Err(LimitError::KeyTooLong { .. })
        ));
    }

    #[test]
    fn value_structure_limits_are_enforced() {
        let limits = Limits::with_small_limits();

        let exact_string = Value::String("x".repeat(limits.max_string_bytes));
        assert!(limits.validate_value(&exact_string).is_ok());

        let string = Value::String("x".repeat(limits.max_string_bytes + 1));
        assert!(matches!(
            limits.validate_value(&string),
            Err(LimitError::ValueTooLarge { .. })
        ));

        let exact_bytes = Value::Bytes(vec![0u8; limits.max_bytes_len]);
        assert!(limits.validate_value(&exact_bytes).is_ok());

        let bytes = Value::Bytes(vec![0u8; limits.max_bytes_len + 1]);
        assert!(matches!(
            limits.validate_value(&bytes),
            Err(LimitError::ValueTooLarge { .. })
        ));

        let exact_array = Value::array(vec![Value::Null; limits.max_array_len]);
        assert!(limits.validate_value(&exact_array).is_ok());

        let array = Value::array(vec![Value::Null; limits.max_array_len + 1]);
        assert!(matches!(
            limits.validate_value(&array),
            Err(LimitError::ValueTooLarge { .. })
        ));

        let mut exact_object = HashMap::new();
        for i in 0..limits.max_object_entries {
            exact_object.insert(format!("key{i}"), Value::Null);
        }
        assert!(limits.validate_value(&Value::object(exact_object)).is_ok());

        let mut object = HashMap::new();
        for i in 0..=limits.max_object_entries {
            object.insert(format!("key{i}"), Value::Null);
        }
        assert!(matches!(
            limits.validate_value(&Value::object(object)),
            Err(LimitError::ValueTooLarge { .. })
        ));
    }

    #[test]
    fn nesting_and_vector_limits_are_enforced() {
        let limits = Limits::with_small_limits();

        let nested_at_boundary = create_nested_array(limits.max_nesting_depth);
        assert!(limits.validate_value(&nested_at_boundary).is_ok());

        let nested = create_nested_array(limits.max_nesting_depth + 1);
        assert!(matches!(
            limits.validate_value(&nested),
            Err(LimitError::NestingTooDeep { .. })
        ));

        let exact_vector = vec![0.0f32; limits.max_vector_dim];
        assert!(limits.validate_vector(&exact_vector).is_ok());

        let vec = vec![0.0f32; limits.max_vector_dim + 1];
        assert!(matches!(
            limits.validate_vector(&vec),
            Err(LimitError::VectorDimExceeded { .. })
        ));

        assert!(limits.validate_vector_dimension_match(256, 256).is_ok());
        assert!(matches!(
            limits.validate_vector_dimension_match(256, 512),
            Err(LimitError::VectorDimMismatch { .. })
        ));
    }

    #[test]
    fn full_validation_rejects_non_finite_and_encoded_overflow() {
        let limits = Limits::default();

        assert!(limits.validate_value_full(&Value::Null).is_ok());
        assert!(limits.validate_value_full(&Value::Bool(true)).is_ok());
        assert!(limits.validate_value_full(&Value::Bool(false)).is_ok());
        assert!(limits.validate_value_full(&Value::Int(42)).is_ok());
        assert!(limits.validate_value_full(&Value::Int(i64::MIN)).is_ok());
        assert!(limits.validate_value_full(&Value::Int(i64::MAX)).is_ok());
        assert!(limits.validate_value_full(&Value::Float(2.78)).is_ok());
        assert!(limits.validate_value_full(&Value::Float(0.0)).is_ok());
        assert!(limits.validate_value_full(&Value::Float(-0.0)).is_ok());
        assert!(limits.validate_value_full(&Value::array(vec![])).is_ok());
        assert!(limits
            .validate_value_full(&Value::object(HashMap::new()))
            .is_ok());

        let valid = Limits::with_small_limits();
        assert!(valid
            .validate_value_full(&Value::String("hello".to_string()))
            .is_ok());

        let structural_violation =
            Value::String("x".repeat(valid.max_string_bytes.saturating_add(1)));
        assert!(matches!(
            valid.validate_value_full(&structural_violation),
            Err(LimitError::ValueTooLarge { .. })
        ));

        assert!(matches!(
            limits.validate_value_full(&Value::Float(f64::NAN)),
            Err(LimitError::ValueTooLarge { reason, .. }) if reason == "value_not_serializable"
        ));
        assert!(matches!(
            limits.validate_value_full(&Value::Float(f64::INFINITY)),
            Err(LimitError::ValueTooLarge { reason, .. }) if reason == "value_not_serializable"
        ));

        let tiny = Limits {
            max_value_bytes_encoded: 10,
            ..Limits::default()
        };
        let value = Value::String("this is a long string".to_string());
        assert!(matches!(
            tiny.validate_value_full(&value),
            Err(LimitError::ValueTooLarge { reason, .. }) if reason == "encoded_value_too_large"
        ));
    }

    #[test]
    fn reason_code_actual_and_max_remain_stable() {
        let err = LimitError::VectorDimMismatch {
            expected: 256,
            actual: 512,
        };
        assert_eq!(err.reason_code(), "vector_dim_mismatch");
        assert_eq!(err.actual(), 512);
        assert_eq!(err.max(), 256);

        let err = LimitError::NestingTooDeep {
            actual: 200,
            max: 128,
        };
        assert_eq!(err.reason_code(), "nesting_too_deep");
        assert_eq!(err.actual(), 200);
        assert_eq!(err.max(), 128);
    }

    #[test]
    fn default_limits_match_spec() {
        let limits = Limits::default();
        assert_eq!(limits.max_key_bytes, 1024);
        assert_eq!(limits.max_string_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_bytes_len, 16 * 1024 * 1024);
        assert_eq!(limits.max_value_bytes_encoded, 32 * 1024 * 1024);
        assert_eq!(limits.max_array_len, 1_000_000);
        assert_eq!(limits.max_object_entries, 1_000_000);
        assert_eq!(limits.max_nesting_depth, 128);
        assert_eq!(limits.max_vector_dim, 8192);
        assert_eq!(limits.max_fuzzy_candidates, 100);
    }

    #[test]
    fn exact_encoded_boundary_passes() {
        let value = Value::Int(42);
        let encoded_len = serde_json::to_vec(&value).unwrap().len();

        let exact = Limits {
            max_value_bytes_encoded: encoded_len,
            ..Limits::default()
        };
        assert!(exact.validate_value_full(&value).is_ok());

        let smaller = Limits {
            max_value_bytes_encoded: encoded_len - 1,
            ..Limits::default()
        };
        assert!(matches!(
            smaller.validate_value_full(&value),
            Err(LimitError::ValueTooLarge { reason, .. }) if reason == "encoded_value_too_large"
        ));
    }

    #[test]
    fn legacy_limits_convert_without_drift() {
        let legacy = LegacyLimits::with_small_limits();
        let engine = Limits::from(&legacy);

        assert_eq!(engine.max_key_bytes, legacy.max_key_bytes);
        assert_eq!(engine.max_string_bytes, legacy.max_string_bytes);
        assert_eq!(engine.max_bytes_len, legacy.max_bytes_len);
        assert_eq!(
            engine.max_value_bytes_encoded,
            legacy.max_value_bytes_encoded
        );
        assert_eq!(engine.max_array_len, legacy.max_array_len);
        assert_eq!(engine.max_object_entries, legacy.max_object_entries);
        assert_eq!(engine.max_nesting_depth, legacy.max_nesting_depth);
        assert_eq!(engine.max_vector_dim, legacy.max_vector_dim);
        assert_eq!(engine.max_fuzzy_candidates, legacy.max_fuzzy_candidates);

        let boundary_string = Value::String("x".repeat(legacy.max_string_bytes));
        assert_eq!(
            legacy.validate_value(&boundary_string).is_ok(),
            engine.validate_value(&boundary_string).is_ok()
        );
    }

    #[test]
    fn legacy_limit_errors_convert_without_drift() {
        let cases = vec![
            LegacyLimitError::KeyTooLong {
                actual: 2048,
                max: 1024,
            },
            LegacyLimitError::ValueTooLarge {
                reason: "encoded_value_too_large".to_string(),
                actual: 8193,
                max: 8192,
            },
            LegacyLimitError::NestingTooDeep {
                actual: 129,
                max: 128,
            },
            LegacyLimitError::VectorDimExceeded {
                actual: 4097,
                max: 4096,
            },
            LegacyLimitError::VectorDimMismatch {
                expected: 256,
                actual: 128,
            },
        ];

        for legacy in cases {
            let legacy_display = legacy.to_string();
            let legacy_reason = legacy.reason_code();
            let legacy_actual = legacy.actual();
            let legacy_max = legacy.max();

            let engine: LimitError = legacy.into();
            assert_eq!(engine.to_string(), legacy_display);
            assert_eq!(engine.reason_code(), legacy_reason);
            assert_eq!(engine.actual(), legacy_actual);
            assert_eq!(engine.max(), legacy_max);
        }
    }

    #[test]
    fn engine_and_legacy_limits_remain_behaviorally_aligned() {
        let engine = Limits::with_small_limits();
        let legacy = LegacyLimits::with_small_limits();

        let cases = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(0),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Float(0.0),
            Value::Float(2.78),
            Value::array(vec![]),
            Value::object(HashMap::new()),
        ];

        for case in cases {
            assert_eq!(
                legacy.validate_value(&case).is_ok(),
                engine.validate_value(&case).is_ok()
            );
            assert_eq!(
                legacy.validate_value_full(&case).is_ok(),
                engine.validate_value_full(&case).is_ok()
            );
        }

        let nan = Value::Float(f64::NAN);
        assert_eq!(
            legacy.validate_value(&nan).is_ok(),
            engine.validate_value(&nan).is_ok()
        );
        assert_eq!(
            legacy
                .validate_value_full(&nan)
                .err()
                .map(|e| e.reason_code().to_string()),
            engine
                .validate_value_full(&nan)
                .err()
                .map(|e| e.reason_code().to_string())
        );
    }
}
