//! Storage-layer value wrapper with TTL support
//!
//! The contract type `Versioned<T>` doesn't include TTL because TTL is
//! a storage concern, not a contract concern. This module provides
//! `StoredValue` which combines value, version, timestamp, and optional TTL
//! for the storage layer.
//!
//! # Memory Optimization
//!
//! TTL and tombstone flag are packed into a single `u64`:
//! - Bit 63: tombstone flag
//! - Bits 0-62: TTL in milliseconds (max ~292 million years)
//!
//! Version is stored as raw `u64` instead of `Version` enum, since all
//! storage-layer versions are `Version::Txn` variants.
//!
//! This reduces `StoredValue` from ~80 bytes to 56 bytes per entry
//! (~24 bytes saved → ~2.9 GB at 128M entries).

use std::time::Duration;

use strata_core::{Timestamp, Value, Version, VersionedValue};

const TOMBSTONE_BIT: u64 = 1 << 63;
const TTL_MASK: u64 = !TOMBSTONE_BIT; // 0x7FFF_FFFF_FFFF_FFFF

/// Pack TTL and tombstone flag into a single u64.
///
/// Bit 63 = tombstone flag, bits 0-62 = TTL in milliseconds.
/// Sub-millisecond TTLs are truncated to zero and become `None` on read.
#[inline]
fn pack_ttl_and_flags(ttl: Option<Duration>, is_tombstone: bool) -> u64 {
    let ttl_millis = ttl.map(|d| d.as_millis() as u64).unwrap_or(0) & TTL_MASK;
    if is_tombstone {
        ttl_millis | TOMBSTONE_BIT
    } else {
        ttl_millis
    }
}

/// A stored value with optional TTL — compact 56-byte layout
///
/// Combines value, version, timestamp, TTL, and tombstone flag.
/// TTL and tombstone are packed into a single `u64` to minimize per-entry
/// overhead. Version is stored as raw `u64` since all storage-layer
/// versions are `Version::Txn` variants (validated at construction time).
///
/// # TTL Precision
///
/// TTL is stored with millisecond precision (63 bits → max ~292 million years).
/// Sub-millisecond TTLs (< 1ms) are truncated to zero and read back as `None`.
/// `Some(Duration::ZERO)` is also stored as zero and reads back as `None`.
/// In practice, TTLs are seconds/minutes/hours — millisecond precision is
/// more than sufficient.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredValue {
    value: Value,
    version: u64,
    timestamp: Timestamp,
    /// Bit 63 = tombstone, bits 0-62 = TTL in milliseconds
    ttl_and_flags: u64,
}

// Value is 32 bytes (24-byte String/Vec data + 8-byte enum tag).
// New: Value(32) + u64(8) + Timestamp(8) + u64(8) = 56 bytes.
// Old: VersionedValue(56) + Option<Duration>(16) + bool(8) = 80 bytes.
// Savings: 24 bytes/entry → ~2.9 GB at 128M entries.
const _: () = assert!(std::mem::size_of::<StoredValue>() <= 56);

impl StoredValue {
    /// Create a new stored value with TTL
    pub fn new(value: Value, version: Version, ttl: Option<Duration>) -> Self {
        debug_assert!(
            version.is_txn(),
            "Storage layer requires Txn versions, got {:?}",
            version
        );
        StoredValue {
            value,
            version: version.as_u64(),
            timestamp: Timestamp::now(),
            ttl_and_flags: pack_ttl_and_flags(ttl, false),
        }
    }

    /// Create a stored value with explicit timestamp
    pub fn with_timestamp(
        value: Value,
        version: Version,
        timestamp: Timestamp,
        ttl: Option<Duration>,
    ) -> Self {
        debug_assert!(
            version.is_txn(),
            "Storage layer requires Txn versions, got {:?}",
            version
        );
        StoredValue {
            value,
            version: version.as_u64(),
            timestamp,
            ttl_and_flags: pack_ttl_and_flags(ttl, false),
        }
    }

    /// Create from a VersionedValue without TTL
    pub fn from_versioned(vv: VersionedValue) -> Self {
        debug_assert!(
            vv.version.is_txn(),
            "Storage layer requires Txn versions, got {:?}",
            vv.version
        );
        StoredValue {
            version: vv.version.as_u64(),
            timestamp: vv.timestamp,
            value: vv.value,
            ttl_and_flags: 0,
        }
    }

    /// Create from a VersionedValue with TTL
    pub fn from_versioned_with_ttl(vv: VersionedValue, ttl: Option<Duration>) -> Self {
        debug_assert!(
            vv.version.is_txn(),
            "Storage layer requires Txn versions, got {:?}",
            vv.version
        );
        StoredValue {
            version: vv.version.as_u64(),
            timestamp: vv.timestamp,
            value: vv.value,
            ttl_and_flags: pack_ttl_and_flags(ttl, false),
        }
    }

    /// Create a tombstone entry (explicit deletion marker)
    ///
    /// Tombstones mark a key as deleted at a specific version without
    /// conflating `Value::Null` with deletion.
    pub fn tombstone(version: Version) -> Self {
        debug_assert!(
            version.is_txn(),
            "Storage layer requires Txn versions, got {:?}",
            version
        );
        StoredValue {
            value: Value::Null,
            version: version.as_u64(),
            timestamp: Timestamp::now(),
            ttl_and_flags: TOMBSTONE_BIT,
        }
    }

    /// Check whether this entry is a tombstone (explicit deletion marker)
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        (self.ttl_and_flags & TOMBSTONE_BIT) != 0
    }

    /// Reconstruct the VersionedValue on demand
    ///
    /// Returns an owned `VersionedValue` since the inner fields are no longer
    /// stored as a `VersionedValue` struct. Every caller previously did
    /// `.versioned().clone()`, so returning owned is a zero-cost change.
    ///
    /// **Clone cost (B6):** This method clones the inner `Value` on every call.
    /// Wrapping `Value` in `Arc<Value>` would eliminate the clone but requires
    /// changes to the public SDK API (`VersionedValue.value` field type).
    /// Deferred until SDK breaking-change window.
    #[inline]
    pub fn versioned(&self) -> VersionedValue {
        VersionedValue {
            value: self.value.clone(),
            version: Version::txn(self.version),
            timestamp: self.timestamp,
        }
    }

    /// Consume and return the inner VersionedValue
    #[inline]
    pub fn into_versioned(self) -> VersionedValue {
        VersionedValue {
            value: self.value,
            version: Version::txn(self.version),
            timestamp: self.timestamp,
        }
    }

    /// Get the value
    #[inline]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Get the version (reconstructs `Version::Txn` enum)
    ///
    /// For hot-path u64 comparisons, prefer [`version_raw`](Self::version_raw)
    /// to avoid the enum reconstruction overhead.
    #[inline]
    pub fn version(&self) -> Version {
        Version::txn(self.version)
    }

    /// Get the raw version as u64 (no enum reconstruction)
    ///
    /// Use this on hot paths where only the numeric value is needed
    /// (e.g., version chain lookups, GC comparisons).
    /// All storage-layer versions are `Version::Txn` — this invariant
    /// is enforced at construction time via debug_assert.
    #[inline]
    pub fn version_raw(&self) -> u64 {
        self.version
    }

    /// Get the timestamp
    #[inline]
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Get the TTL
    ///
    /// Returns the TTL with millisecond precision. Sub-millisecond
    /// durations (< 1ms) and `Duration::ZERO` are stored as zero
    /// and read back as `None`.
    #[inline]
    pub fn ttl(&self) -> Option<Duration> {
        let millis = self.ttl_and_flags & TTL_MASK;
        if millis == 0 {
            None
        } else {
            Some(Duration::from_millis(millis))
        }
    }

    /// Check if this value has expired
    pub fn is_expired(&self) -> bool {
        let millis = self.ttl_and_flags & TTL_MASK;
        if millis != 0 {
            let now = Timestamp::now();
            if let Some(age) = now.duration_since(self.timestamp) {
                return age >= Duration::from_millis(millis);
            }
        }
        false
    }

    /// Calculate the expiry timestamp
    ///
    /// Returns `Some(timestamp)` when the value will expire, or `None` if no TTL.
    pub fn expiry_timestamp(&self) -> Option<Timestamp> {
        let millis = self.ttl_and_flags & TTL_MASK;
        if millis == 0 {
            None
        } else {
            Some(self.timestamp.saturating_add(Duration::from_millis(millis)))
        }
    }
}

impl From<StoredValue> for VersionedValue {
    fn from(sv: StoredValue) -> Self {
        sv.into_versioned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stored_value_size() {
        assert!(
            std::mem::size_of::<StoredValue>() <= 56,
            "StoredValue should be <= 56 bytes, got {}",
            std::mem::size_of::<StoredValue>()
        );
    }

    #[test]
    fn test_stored_value_new() {
        let sv = StoredValue::new(Value::Int(42), Version::txn(1), None);
        assert_eq!(*sv.value(), Value::Int(42));
        assert_eq!(sv.version(), Version::Txn(1));
        assert_eq!(sv.version_raw(), 1);
        assert!(sv.ttl().is_none());
        assert!(!sv.is_expired());
        assert!(!sv.is_tombstone());
        // Timestamp should be recent (within last second)
        let age = Timestamp::now().duration_since(sv.timestamp());
        assert!(age.is_some() && age.unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn test_stored_value_with_ttl() {
        let sv = StoredValue::new(
            Value::String("test".to_string()),
            Version::txn(1),
            Some(Duration::from_secs(60)),
        );
        assert!(sv.ttl().is_some());
        assert_eq!(sv.ttl().unwrap(), Duration::from_secs(60));
        assert!(!sv.is_expired());
        assert!(!sv.is_tombstone());
    }

    #[test]
    fn test_stored_value_with_timestamp() {
        let ts = Timestamp::from_micros(5_000_000);
        let sv = StoredValue::with_timestamp(
            Value::Float(3.14),
            Version::txn(10),
            ts,
            Some(Duration::from_secs(30)),
        );
        assert_eq!(sv.timestamp(), ts);
        assert_eq!(sv.version_raw(), 10);
        assert_eq!(sv.ttl().unwrap(), Duration::from_secs(30));
        assert!(!sv.is_tombstone());
    }

    #[test]
    fn test_stored_value_expired() {
        let old_ts = Timestamp::from_micros(0);
        let sv = StoredValue::with_timestamp(
            Value::Null,
            Version::txn(1),
            old_ts,
            Some(Duration::from_secs(1)),
        );
        // Timestamp is epoch, TTL is 1 second — definitely expired
        assert!(sv.is_expired());
    }

    #[test]
    fn test_stored_value_not_expired_without_ttl() {
        let old_ts = Timestamp::from_micros(0);
        let sv = StoredValue::with_timestamp(Value::Null, Version::txn(1), old_ts, None);
        // No TTL means never expires, even with ancient timestamp
        assert!(!sv.is_expired());
    }

    #[test]
    fn test_stored_value_not_expired_with_future_timestamp() {
        let future_ts = Timestamp::from_micros(u64::MAX / 2);
        let sv = StoredValue::with_timestamp(
            Value::Null,
            Version::txn(1),
            future_ts,
            Some(Duration::from_secs(1)),
        );
        // Future timestamp — duration_since returns None → not expired
        assert!(!sv.is_expired());
    }

    #[test]
    fn test_stored_value_expiry_timestamp() {
        let ts = Timestamp::from_micros(1_000_000); // 1 second
        let sv = StoredValue::with_timestamp(
            Value::Null,
            Version::txn(1),
            ts,
            Some(Duration::from_secs(60)),
        );

        let expiry = sv.expiry_timestamp().unwrap();
        // 1 second + 60 seconds = 61 seconds = 61_000_000 microseconds
        assert_eq!(expiry.as_micros(), 61_000_000);
    }

    #[test]
    fn test_stored_value_no_ttl_expiry() {
        let sv = StoredValue::new(Value::Null, Version::txn(1), None);
        assert!(sv.expiry_timestamp().is_none());
    }

    #[test]
    fn test_stored_value_into_versioned() {
        let sv = StoredValue::new(
            Value::Int(42),
            Version::txn(5),
            Some(Duration::from_secs(10)),
        );
        let ts = sv.timestamp();
        let vv = sv.into_versioned();
        assert_eq!(vv.value, Value::Int(42));
        assert_eq!(vv.version, Version::Txn(5));
        assert_eq!(vv.timestamp, ts);
    }

    #[test]
    fn test_stored_value_from_trait() {
        let sv = StoredValue::new(Value::Int(77), Version::txn(3), None);
        let ts = sv.timestamp();
        let vv: VersionedValue = sv.into();
        assert_eq!(vv.value, Value::Int(77));
        assert_eq!(vv.version, Version::Txn(3));
        assert_eq!(vv.timestamp, ts);
    }

    #[test]
    fn test_stored_value_from_versioned() {
        let vv = VersionedValue::new(Value::Bool(true), Version::txn(10));
        let ts = vv.timestamp;
        let sv = StoredValue::from_versioned(vv);
        assert_eq!(*sv.value(), Value::Bool(true));
        assert_eq!(sv.version(), Version::Txn(10));
        assert_eq!(sv.timestamp(), ts);
        assert!(sv.ttl().is_none());
        assert!(!sv.is_tombstone());
    }

    #[test]
    fn test_stored_value_from_versioned_with_ttl() {
        let vv = VersionedValue::new(Value::Int(1), Version::txn(2));
        let ts = vv.timestamp;
        let sv = StoredValue::from_versioned_with_ttl(vv, Some(Duration::from_secs(120)));
        assert_eq!(sv.version_raw(), 2);
        assert_eq!(sv.timestamp(), ts);
        assert_eq!(sv.ttl().unwrap(), Duration::from_secs(120));
        assert!(!sv.is_tombstone());
    }

    #[test]
    fn test_tombstone_packing_roundtrip() {
        let sv = StoredValue::tombstone(Version::txn(42));
        assert!(sv.is_tombstone());
        assert!(sv.ttl().is_none());
        assert_eq!(sv.version(), Version::Txn(42));
        assert_eq!(sv.version_raw(), 42);
        assert_eq!(*sv.value(), Value::Null);
    }

    #[test]
    fn test_non_tombstone_is_not_tombstone() {
        let sv = StoredValue::new(Value::Null, Version::txn(1), None);
        assert!(!sv.is_tombstone());
    }

    #[test]
    fn test_ttl_packing_roundtrip() {
        // Various durations survive at millisecond precision
        for secs in [1, 60, 3600, 86400, 604800] {
            let sv = StoredValue::new(
                Value::Null,
                Version::txn(1),
                Some(Duration::from_secs(secs)),
            );
            assert_eq!(sv.ttl().unwrap(), Duration::from_secs(secs));
        }

        // Millisecond precision
        let sv = StoredValue::new(
            Value::Null,
            Version::txn(1),
            Some(Duration::from_millis(1500)),
        );
        assert_eq!(sv.ttl().unwrap(), Duration::from_millis(1500));
    }

    #[test]
    fn test_sub_millisecond_ttl_becomes_none() {
        // Sub-millisecond TTL truncates to zero millis → reads back as None
        let sv = StoredValue::new(
            Value::Null,
            Version::txn(1),
            Some(Duration::from_micros(500)),
        );
        assert!(sv.ttl().is_none());
        assert!(sv.expiry_timestamp().is_none());
        assert!(!sv.is_expired());
    }

    #[test]
    fn test_duration_zero_ttl_becomes_none() {
        // Duration::ZERO stores as 0 millis → reads back as None
        let sv = StoredValue::new(Value::Null, Version::txn(1), Some(Duration::ZERO));
        assert!(sv.ttl().is_none());
        assert!(sv.expiry_timestamp().is_none());
        assert!(!sv.is_expired());
    }

    #[test]
    fn test_tombstone_with_ttl() {
        // Construct a tombstone-like entry with TTL via manual packing
        // In practice tombstones don't have TTL, but test the bit packing
        let packed = pack_ttl_and_flags(Some(Duration::from_secs(30)), true);
        let sv = StoredValue {
            value: Value::Null,
            version: 1,
            timestamp: Timestamp::now(),
            ttl_and_flags: packed,
        };
        assert!(sv.is_tombstone());
        assert_eq!(sv.ttl().unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_large_ttl_does_not_collide_with_tombstone_bit() {
        // Very large TTL (close to 63-bit max) should not set tombstone bit
        let large_millis = TTL_MASK; // max 63-bit value
        let sv = StoredValue {
            value: Value::Null,
            version: 1,
            timestamp: Timestamp::now(),
            ttl_and_flags: large_millis,
        };
        assert!(!sv.is_tombstone());
        assert_eq!(sv.ttl().unwrap().as_millis(), large_millis as u128);
    }

    #[test]
    fn test_versioned_reconstruction() {
        let ts = Timestamp::from_micros(12345678);
        let sv = StoredValue::with_timestamp(Value::Int(99), Version::txn(7), ts, None);
        let vv = sv.versioned();
        assert_eq!(vv.value, Value::Int(99));
        assert_eq!(vv.version, Version::Txn(7));
        assert_eq!(vv.timestamp, ts);
    }

    #[test]
    fn test_versioned_clones_value() {
        // versioned() clones the value; original StoredValue is unaffected
        let sv = StoredValue::new(Value::String("hello".to_string()), Version::txn(1), None);
        let vv = sv.versioned();
        assert_eq!(vv.value, Value::String("hello".to_string()));
        assert_eq!(*sv.value(), Value::String("hello".to_string()));
    }

    #[test]
    fn test_clone_produces_equal_value() {
        let ts = Timestamp::from_micros(999);
        let sv = StoredValue::with_timestamp(
            Value::String("test".to_string()),
            Version::txn(5),
            ts,
            Some(Duration::from_secs(60)),
        );
        let cloned = sv.clone();
        assert_eq!(sv, cloned);
        assert_eq!(cloned.version_raw(), 5);
        assert_eq!(cloned.timestamp(), ts);
        assert_eq!(cloned.ttl().unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_partial_eq_different_values() {
        let ts = Timestamp::from_micros(1000);
        let sv1 = StoredValue::with_timestamp(Value::Int(1), Version::txn(1), ts, None);
        let sv2 = StoredValue::with_timestamp(Value::Int(2), Version::txn(1), ts, None);
        assert_ne!(sv1, sv2);
    }

    #[test]
    fn test_partial_eq_different_versions() {
        let ts = Timestamp::from_micros(1000);
        let sv1 = StoredValue::with_timestamp(Value::Int(1), Version::txn(1), ts, None);
        let sv2 = StoredValue::with_timestamp(Value::Int(1), Version::txn(2), ts, None);
        assert_ne!(sv1, sv2);
    }

    #[test]
    fn test_version_zero() {
        // Version 0 is a valid Txn version
        let sv = StoredValue::new(Value::Null, Version::txn(0), None);
        assert_eq!(sv.version_raw(), 0);
        assert_eq!(sv.version(), Version::Txn(0));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Storage layer requires Txn versions")]
    fn test_debug_assert_non_txn_version_new() {
        StoredValue::new(Value::Null, Version::seq(1), None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Storage layer requires Txn versions")]
    fn test_debug_assert_non_txn_version_with_timestamp() {
        StoredValue::with_timestamp(
            Value::Null,
            Version::seq(1),
            Timestamp::from_micros(0),
            None,
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Storage layer requires Txn versions")]
    fn test_debug_assert_non_txn_version_from_versioned() {
        let vv = VersionedValue::new(Value::Null, Version::seq(1));
        StoredValue::from_versioned(vv);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Storage layer requires Txn versions")]
    fn test_debug_assert_non_txn_version_tombstone() {
        StoredValue::tombstone(Version::seq(1));
    }
}
