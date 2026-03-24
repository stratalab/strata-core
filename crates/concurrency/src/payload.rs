//! Transaction payload serialization for WAL records
//!
//! Each committed transaction is serialized into a single `TransactionPayload`
//! blob stored in a `WalRecord.writeset`. This replaces the legacy multi-entry
//! approach (BeginTxn → Write×N → Delete×N → CommitTxn) with a single record
//! per committed transaction.
//!
//! ## Format
//!
//! The payload is serialized using MessagePack (`rmp-serde`) for compact
//! binary encoding with schema evolution support.

use serde::{Deserialize, Serialize};
use strata_core::types::Key;
use strata_core::value::Value;

use crate::TransactionContext;

/// Serializable payload for a committed transaction.
///
/// Contains all the writes and deletes from a single transaction,
/// along with the commit version. One `TransactionPayload` maps to
/// one `WalRecord` in the segmented WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPayload {
    /// Commit version (same for all operations in this transaction)
    pub version: u64,
    /// Key-value pairs to write (from write_set + cas_set)
    pub puts: Vec<(Key, Value)>,
    /// Keys to delete (from delete_set)
    pub deletes: Vec<Key>,
    /// Per-put TTL in milliseconds, parallel to `puts` (0 = no TTL).
    /// Empty vec means all puts have no TTL (backward compat with old WAL records).
    #[serde(default)]
    pub put_ttls: Vec<u64>,
}

impl TransactionPayload {
    /// Serialize to MessagePack bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("TransactionPayload serialization should not fail")
    }

    /// Deserialize from MessagePack bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PayloadError> {
        rmp_serde::from_slice(bytes).map_err(|e| PayloadError::DeserializeFailed(e.to_string()))
    }

    /// Build a payload from a committed transaction's write/delete/cas sets.
    ///
    /// CAS operations are included as puts (they have already been validated
    /// at commit time, so recovery just replays the final value).
    pub fn from_transaction(txn: &TransactionContext, version: u64) -> Self {
        let mut puts: Vec<(Key, Value)> = Vec::new();
        let mut put_ttls: Vec<u64> = Vec::new();

        for (k, v) in txn.write_set.iter() {
            puts.push((k.clone(), v.clone()));
            put_ttls.push(txn.ttl_map.get(k).copied().unwrap_or(0));
        }

        // CAS operations become puts (the new_value was already validated)
        for cas_op in &txn.cas_set {
            puts.push((cas_op.key.clone(), cas_op.new_value.clone()));
            put_ttls.push(txn.ttl_map.get(&cas_op.key).copied().unwrap_or(0));
        }

        let deletes: Vec<Key> = txn.delete_set.iter().cloned().collect();

        TransactionPayload {
            version,
            puts,
            deletes,
            put_ttls,
        }
    }
}

/// Errors from payload serialization/deserialization.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PayloadError {
    /// Failed to deserialize payload bytes
    #[error("Failed to deserialize transaction payload: {0}")]
    DeserializeFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use strata_core::types::{BranchId, Key, Namespace};
    use strata_core::value::Value;

    fn test_ns() -> Arc<Namespace> {
        let branch_id = BranchId::new();
        Arc::new(Namespace::new(branch_id, "default".to_string()))
    }

    #[test]
    fn test_roundtrip_empty() {
        let payload = TransactionPayload {
            version: 42,
            puts: vec![],
            deletes: vec![],
            put_ttls: vec![],
        };
        let bytes = payload.to_bytes();
        let decoded = TransactionPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.version, 42);
        assert!(decoded.puts.is_empty());
        assert!(decoded.deletes.is_empty());
    }

    #[test]
    fn test_roundtrip_with_data() {
        let ns = test_ns();
        let key1 = Key::new_kv(ns.clone(), "key1");
        let key2 = Key::new_kv(ns.clone(), "key2");
        let key3 = Key::new_kv(ns, "key3");

        let payload = TransactionPayload {
            version: 100,
            puts: vec![
                (key1.clone(), Value::Int(42)),
                (key2.clone(), Value::String("hello".to_string())),
            ],
            deletes: vec![key3.clone()],
            put_ttls: vec![0, 0],
        };

        let bytes = payload.to_bytes();
        let decoded = TransactionPayload::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.version, 100);
        assert_eq!(decoded.puts.len(), 2);
        assert_eq!(decoded.deletes.len(), 1);
        assert_eq!(decoded.puts[0].0, key1);
        assert_eq!(decoded.puts[1].0, key2);
        assert_eq!(decoded.deletes[0], key3);
    }

    #[test]
    fn test_invalid_bytes() {
        let result = TransactionPayload::from_bytes(&[0xFF, 0x00, 0x01]);
        assert!(result.is_err());
    }

    /// Issue #1754: Value::Bytes must use msgpack bin encoding, not array-of-u8.
    ///
    /// Without `serde_bytes`, rmp-serde encodes `Vec<u8>` as a msgpack array where
    /// each byte > 127 becomes 2 bytes (type tag + value), and deserialization
    /// calls `deserialize_u8` per byte — 32-36% of read latency at 1KB values.
    ///
    /// With `serde_bytes`, encoding uses msgpack bin (header + raw bulk copy) and
    /// deserialization is a single `visit_bytes` + memcpy.
    #[test]
    fn test_issue_1754_value_bytes_uses_efficient_msgpack_encoding() {
        // 256 bytes all > 127: without serde_bytes, each byte encodes as 2 bytes
        // (0xcc prefix + value), roughly doubling the payload size.
        let data = vec![0xFFu8; 256];
        let value = Value::Bytes(data.clone());
        let encoded = rmp_serde::to_vec(&value).unwrap();

        // With serde_bytes (bin encoding): ~260 bytes (enum overhead + bin16 header + 256 raw)
        // Without serde_bytes (array encoding): ~515 bytes (enum overhead + array16 header + 256×2)
        assert!(
            encoded.len() < 300,
            "Value::Bytes encoded size {} indicates array-of-u8 encoding \
             (expected bin encoding < 300 bytes for 256 high bytes)",
            encoded.len()
        );

        // Verify round-trip correctness
        let decoded: Value = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, Value::Bytes(vec![0xFFu8; 256]));
    }

    /// Issue #1740: TransactionPayload must include per-key TTL so WAL
    /// replay preserves TTL-bearing entries instead of making them permanent.
    #[test]
    fn test_issue_1740_payload_ttl_roundtrip() {
        let ns = test_ns();
        let key1 = Key::new_kv(ns.clone(), "ttl_key");
        let key2 = Key::new_kv(ns, "no_ttl_key");

        let payload = TransactionPayload {
            version: 10,
            puts: vec![(key1.clone(), Value::Int(1)), (key2.clone(), Value::Int(2))],
            deletes: vec![],
            put_ttls: vec![60_000, 0], // first key has 60s TTL, second has none
        };

        let bytes = payload.to_bytes();
        let decoded = TransactionPayload::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.put_ttls.len(), 2);
        assert_eq!(decoded.put_ttls[0], 60_000);
        assert_eq!(decoded.put_ttls[1], 0);
    }

    /// Issue #1740: Old WAL records (without put_ttls) must deserialize
    /// with an empty put_ttls vec (backward compatibility).
    #[test]
    fn test_issue_1740_payload_backward_compat() {
        // Simulate an old payload without put_ttls by serializing the
        // old 3-field struct shape.
        #[derive(serde::Serialize)]
        struct OldPayload {
            version: u64,
            puts: Vec<(Key, Value)>,
            deletes: Vec<Key>,
        }

        let ns = test_ns();
        let old = OldPayload {
            version: 5,
            puts: vec![(Key::new_kv(ns, "k"), Value::Int(1))],
            deletes: vec![],
        };
        let bytes = rmp_serde::to_vec(&old).unwrap();

        // Deserialize as the new TransactionPayload — put_ttls should default
        let decoded = TransactionPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.version, 5);
        assert_eq!(decoded.puts.len(), 1);
        assert!(
            decoded.put_ttls.is_empty(),
            "Old payloads without put_ttls must deserialize with empty vec"
        );
    }
}
