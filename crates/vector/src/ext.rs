//! Extension trait for vector operations on TransactionContext.
//!
//! Follows the same pattern as `GraphStoreExt`, `KVStoreExt`, `EventLogExt`,
//! and `JsonStoreExt`. By implementing vector operations directly on
//! `TransactionContext`, both the standalone `VectorStore` methods and the
//! Session's `dispatch_in_txn` can share one implementation.
//!
//! HNSW backend updates cannot participate in OCC (they're in-memory and not
//! rollback-safe), so write methods return a `StagedVectorOp` that the caller
//! applies post-commit.

use std::sync::Arc;

use strata_concurrency::TransactionContext;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::{EntityRef, Version};

use crate::error::{VectorError, VectorResult};
use crate::store::VectorBackendState;
use crate::types::{InlineMeta, VectorRecord};
use crate::{CollectionId, VectorId};

// ============================================================================
// StagedVectorOp — deferred HNSW updates applied after commit
// ============================================================================

/// A vector backend operation deferred until after transaction commit.
///
/// HNSW index updates cannot participate in OCC (they're in-memory and
/// not rollback-safe). These ops are buffered during the transaction and
/// applied to the backend after successful commit.
#[derive(Debug, Clone)]
pub enum StagedVectorOp {
    Insert {
        collection_id: CollectionId,
        vector_id: VectorId,
        embedding: Vec<f32>,
        key: String,
        source_ref: Option<EntityRef>,
        created_at: u64,
    },
    Delete {
        collection_id: CollectionId,
        vector_id: VectorId,
        key: String,
    },
}

/// Apply a staged vector op to the HNSW backend (best-effort, non-fatal).
pub fn apply_staged_vector_op(state: &VectorBackendState, op: StagedVectorOp) {
    match op {
        StagedVectorOp::Insert {
            collection_id,
            vector_id,
            embedding,
            key,
            source_ref,
            created_at,
        } => {
            if let Some(mut backend) = state.backends.get_mut(&collection_id) {
                if let Err(e) = backend.insert_with_timestamp(vector_id, &embedding, created_at) {
                    tracing::warn!(
                        target: "strata::vector",
                        error = %e,
                        "Post-commit HNSW insert failed; vector is durable but not searchable until recovery"
                    );
                } else {
                    backend.set_inline_meta(vector_id, InlineMeta { key, source_ref });
                }
            }
        }
        StagedVectorOp::Delete {
            collection_id,
            vector_id,
            key: _,
        } => {
            if let Some(mut backend) = state.backends.get_mut(&collection_id) {
                match backend.delete_with_timestamp(vector_id, crate::types::now_micros()) {
                    Ok(_) => {
                        backend.remove_inline_meta(vector_id);
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::vector",
                            error = %e,
                            "Post-commit HNSW delete failed; search will filter via KV check"
                        );
                    }
                }
            }
        }
    }
}

// ============================================================================
// VectorStoreExt trait
// ============================================================================

/// Extension trait providing vector operations on `TransactionContext`.
///
/// Write methods return a `StagedVectorOp` for post-commit HNSW backend
/// updates. The caller is responsible for applying these ops after a
/// successful commit (and discarding them on rollback).
pub trait VectorStoreExt {
    /// Upsert a vector inside a transaction.
    ///
    /// The KV write participates in OCC. The HNSW backend update is returned
    /// as a `StagedVectorOp` for the caller to apply post-commit.
    ///
    /// `source_ref` is an optional back-reference to the source entity (e.g.,
    /// a JSON document) used by auto-embed infrastructure.
    #[allow(clippy::too_many_arguments)]
    fn vector_upsert(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<serde_json::Value>,
        source_ref: Option<EntityRef>,
        backend_state: &VectorBackendState,
    ) -> VectorResult<(Version, StagedVectorOp)>;

    /// Delete a vector inside a transaction.
    ///
    /// Returns `(existed, optional staged op)`. If the vector didn't exist,
    /// returns `(false, None)`.
    fn vector_delete(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        backend_state: &VectorBackendState,
    ) -> VectorResult<(bool, Option<StagedVectorOp>)>;

    /// Get a vector inside a transaction (read-your-writes).
    ///
    /// Reads from the TransactionContext, so uncommitted writes from the
    /// same transaction are visible.
    fn vector_get(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<VectorRecord>>;
}

// ============================================================================
// Implementation for TransactionContext
// ============================================================================

impl VectorStoreExt for TransactionContext {
    fn vector_upsert(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        embedding: &[f32],
        metadata: Option<serde_json::Value>,
        source_ref: Option<EntityRef>,
        backend_state: &VectorBackendState,
    ) -> VectorResult<(Version, StagedVectorOp)> {
        // Validate key
        crate::collection::validate_vector_key(key)?;

        // Validate embedding (NaN/Infinity)
        if embedding.iter().any(|v| v.is_nan() || v.is_infinite()) {
            return Err(VectorError::InvalidEmbedding {
                reason: "embedding contains NaN or Infinity values".to_string(),
            });
        }

        let collection_id = CollectionId::new(branch_id, collection);

        // Validate dimension from backend config
        let mut backend = backend_state
            .backends
            .get_mut(&collection_id)
            .ok_or_else(|| VectorError::CollectionNotFound {
                name: collection.to_string(),
            })?;

        let config = backend.config();
        if embedding.len() != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension,
                got: embedding.len(),
            });
        }

        // Build KV key
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
        let kv_key = Key::new_vector(ns, collection, key);

        // Check existence via TransactionContext (read-your-writes)
        let existing = match self
            .get(&kv_key)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        {
            Some(Value::Bytes(b)) => Some(VectorRecord::from_bytes(&b)?),
            _ => None,
        };

        let (vector_id, record) = if let Some(mut existing_record) = existing {
            // Update existing: keep the same VectorId
            match source_ref.clone() {
                Some(sr) => {
                    existing_record.update_with_source(embedding.to_vec(), metadata, Some(sr))
                }
                None => existing_record.update(embedding.to_vec(), metadata),
            }
            (VectorId(existing_record.vector_id), existing_record)
        } else {
            // New vector: allocate VectorId from backend counter
            let vid = backend.allocate_id();
            let r = match source_ref.clone() {
                Some(sr) => VectorRecord::new_with_source(vid, embedding.to_vec(), metadata, sr),
                None => VectorRecord::new(vid, embedding.to_vec(), metadata),
            };
            (vid, r)
        };

        // Release DashMap lock before KV write
        drop(backend);

        // Write to TransactionContext (participates in OCC)
        let record_version = record.version;
        let created_at = record.created_at;
        let record_bytes = record.to_bytes()?;
        self.put(kv_key, Value::Bytes(record_bytes))
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        Ok((
            Version::counter(record_version),
            StagedVectorOp::Insert {
                collection_id,
                vector_id,
                embedding: embedding.to_vec(),
                key: key.to_string(),
                source_ref,
                created_at,
            },
        ))
    }

    fn vector_delete(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
        backend_state: &VectorBackendState,
    ) -> VectorResult<(bool, Option<StagedVectorOp>)> {
        crate::collection::validate_vector_key(key)?;

        let collection_id = CollectionId::new(branch_id, collection);
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
        let kv_key = Key::new_vector(ns, collection, key);

        // Check existence via TransactionContext (read-your-writes)
        let record = match self
            .get(&kv_key)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        {
            Some(Value::Bytes(b)) => VectorRecord::from_bytes(&b)?,
            _ => return Ok((false, None)),
        };

        let vector_id = VectorId(record.vector_id);

        // Verify collection exists in backend
        if !backend_state.backends.contains_key(&collection_id) {
            return Err(VectorError::CollectionNotFound {
                name: collection.to_string(),
            });
        }

        // Delete from TransactionContext (participates in OCC)
        self.delete(kv_key)
            .map_err(|e| VectorError::Storage(e.to_string()))?;

        Ok((
            true,
            Some(StagedVectorOp::Delete {
                collection_id,
                vector_id,
                key: key.to_string(),
            }),
        ))
    }

    fn vector_get(
        &mut self,
        branch_id: BranchId,
        space: &str,
        collection: &str,
        key: &str,
    ) -> VectorResult<Option<VectorRecord>> {
        let ns = Arc::new(Namespace::for_branch_space(branch_id, space));
        let kv_key = Key::new_vector(ns, collection, key);

        match self
            .get(&kv_key)
            .map_err(|e| VectorError::Storage(e.to_string()))?
        {
            Some(Value::Bytes(bytes)) => {
                let record = VectorRecord::from_bytes(&bytes)?;
                Ok(Some(record))
            }
            _ => Ok(None),
        }
    }
}
