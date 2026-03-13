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
        let mut puts: Vec<(Key, Value)> = txn
            .write_set
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // CAS operations become puts (the new_value was already validated)
        for cas_op in &txn.cas_set {
            puts.push((cas_op.key.clone(), cas_op.new_value.clone()));
        }

        let deletes: Vec<Key> = txn.delete_set.iter().cloned().collect();

        TransactionPayload {
            version,
            puts,
            deletes,
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
}
