//! Vector store operations (7 MVP).
//!
//! MVP: upsert, get, delete, query, create_collection, delete_collection, list_collections

use super::Strata;
use crate::types::*;
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // Vector Operations (7 MVP)
    // =========================================================================

    /// Create a vector collection.
    pub fn vector_create_collection(
        &self,
        collection: &str,
        dimension: u64,
        metric: DistanceMetric,
    ) -> Result<u64> {
        match self.execute_cmd(Command::VectorCreateCollection {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            dimension,
            metric,
        })? {
            Output::Version(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorCreateCollection".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a collection.
    pub fn vector_delete_collection(&self, collection: &str) -> Result<bool> {
        match self.execute_cmd(Command::VectorDeleteCollection {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
        })? {
            Output::Bool(dropped) => Ok(dropped),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorDeleteCollection".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// List all collections.
    pub fn vector_list_collections(&self) -> Result<Vec<CollectionInfo>> {
        match self.execute_cmd(Command::VectorListCollections {
            branch: self.branch_id(),
            space: self.space_id(),
        })? {
            Output::VectorCollectionList(infos) => Ok(infos),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorListCollections".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Upsert a vector.
    pub fn vector_upsert(
        &self,
        collection: &str,
        key: &str,
        vector: Vec<f32>,
        metadata: Option<Value>,
    ) -> Result<u64> {
        match self.execute_cmd(Command::VectorUpsert {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            key: key.to_string(),
            vector,
            metadata,
        })? {
            Output::VectorWriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorUpsert".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get a vector by key.
    pub fn vector_get(&self, collection: &str, key: &str) -> Result<Option<VersionedVectorData>> {
        match self.execute_cmd(Command::VectorGet {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            key: key.to_string(),
            as_of: None,
        })? {
            Output::VectorData(data) => Ok(data),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get the full version history for a vector key.
    ///
    /// Returns `None` if the key has never existed. Index `[0]` is the latest
    /// version, `[1]` the previous, etc. Newest-first ordering. Reads directly
    /// from the storage version chain and is unaffected by collection load
    /// state.
    pub fn vector_getv(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<Option<Vec<VersionedVectorData>>> {
        match self.execute_cmd(Command::VectorGetv {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            key: key.to_string(),
        })? {
            Output::VectorVersionHistory(h) => Ok(h),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorGetv".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Delete a vector.
    pub fn vector_delete(&self, collection: &str, key: &str) -> Result<bool> {
        match self.execute_cmd(Command::VectorDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            key: key.to_string(),
        })? {
            Output::VectorDeleteResult { deleted, .. } => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get detailed statistics for a single collection.
    pub fn vector_collection_stats(&self, collection: &str) -> Result<CollectionInfo> {
        match self.execute_cmd(Command::VectorCollectionStats {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
        })? {
            Output::VectorCollectionList(mut infos) => infos.pop().ok_or(Error::Internal {
                reason: "Empty response for VectorCollectionStats".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorCollectionStats".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Batch upsert multiple vectors.
    pub fn vector_batch_upsert(
        &self,
        collection: &str,
        entries: Vec<crate::types::BatchVectorEntry>,
    ) -> Result<Vec<u64>> {
        match self.execute_cmd(Command::VectorBatchUpsert {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            entries,
        })? {
            Output::Versions(versions) => Ok(versions),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorBatchUpsert".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Query a vector collection for nearest neighbors.
    pub fn vector_query(
        &self,
        collection: &str,
        query: Vec<f32>,
        k: u64,
    ) -> Result<Vec<VectorMatch>> {
        match self.execute_cmd(Command::VectorQuery {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            query,
            k,
            filter: None,
            metric: None,
            as_of: None,
        })? {
            Output::VectorMatches(matches) => Ok(matches),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorQuery".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Query for nearest neighbors with optional filter, metric override, and time-travel.
    pub fn vector_query_with_filter(
        &self,
        collection: &str,
        query: Vec<f32>,
        k: u64,
        filter: Option<Vec<MetadataFilter>>,
        metric: Option<DistanceMetric>,
        as_of: Option<u64>,
    ) -> Result<Vec<VectorMatch>> {
        match self.execute_cmd(Command::VectorQuery {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            query,
            k,
            filter,
            metric,
            as_of,
        })? {
            Output::VectorMatches(matches) => Ok(matches),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorQuery".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Batch get multiple vectors by key.
    ///
    /// Returns per-item results positionally mapped to the input keys.
    /// Missing keys yield `None`.
    pub fn vector_batch_get(
        &self,
        collection: &str,
        keys: Vec<String>,
    ) -> Result<Vec<Option<VersionedVectorData>>> {
        match self.execute_cmd(Command::VectorBatchGet {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            keys,
        })? {
            Output::BatchVectorGetResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorBatchGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Batch delete multiple vectors by key.
    ///
    /// Returns per-item results positionally mapped to the input keys.
    pub fn vector_batch_delete(
        &self,
        collection: &str,
        keys: Vec<String>,
    ) -> Result<Vec<crate::types::BatchItemResult>> {
        match self.execute_cmd(Command::VectorBatchDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            keys,
        })? {
            Output::BatchResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorBatchDelete".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }

    /// Get a vector by key at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn vector_get_as_of(
        &self,
        collection: &str,
        key: &str,
        as_of: Option<u64>,
    ) -> Result<Option<VersionedVectorData>> {
        match self.execute_cmd(Command::VectorGet {
            branch: self.branch_id(),
            space: self.space_id(),
            collection: collection.to_string(),
            key: key.to_string(),
            as_of,
        })? {
            Output::VectorData(data) => Ok(data),
            _ => Err(Error::Internal {
                reason: "Unexpected output for VectorGet".into(),
                hint: Some("This is likely a bug. Please report it at https://github.com/stratalab/strata-core/issues".to_string()),
            }),
        }
    }
}
