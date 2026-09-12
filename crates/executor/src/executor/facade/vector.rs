use super::super::{
    BatchVectorEntry, Command, Executor, ExecutorError, Output, VectorDistanceMetric,
    VectorMetadataFilter,
};

impl Executor {
    /// Executes a default-branch vector collection-create command.
    pub fn vector_create_collection(
        &mut self,
        collection: impl Into<String>,
        dimension: u64,
        metric: VectorDistanceMetric,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorCreateCollection {
            branch: None,
            space: None,
            collection: collection.into(),
            dimension,
            metric,
            embedding_model: None,
        })
    }

    /// Executes a default-branch vector collection-delete command.
    pub fn vector_delete_collection(
        &mut self,
        collection: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorDeleteCollection {
            branch: None,
            space: None,
            collection: collection.into(),
        })
    }

    /// Executes a default-branch vector collection-list command.
    pub fn vector_list_collections(&mut self) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorListCollections {
            branch: None,
            space: None,
        })
    }

    /// Executes a default-branch vector collection-stats command.
    pub fn vector_collection_stats(
        &mut self,
        collection: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorCollectionStats {
            branch: None,
            space: None,
            collection: collection.into(),
        })
    }

    /// Executes a default-branch vector set-embedding-model command.
    pub fn vector_set_embedding_model(
        &mut self,
        collection: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorSetEmbeddingModel {
            branch: None,
            space: None,
            collection: collection.into(),
            model: model.into(),
        })
    }

    /// Executes a default-branch vector count command.
    pub fn vector_count(&mut self, collection: impl Into<String>) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorCount {
            branch: None,
            space: None,
            collection: collection.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch vector upsert command.
    pub fn vector_upsert(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
        vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorUpsert {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
            vector: vector.into_iter().map(f64::from).collect(),
            text: None,
            metadata,
        })
    }

    /// Executes a default-branch vector get command.
    pub fn vector_get(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorGet {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch vector history command.
    pub fn vector_history(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorHistory {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
        })
    }

    /// Executes a default-branch vector exists command.
    pub fn vector_exists(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorExists {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
        })
    }

    /// Executes a default-branch vector key-list command.
    pub fn vector_list_keys(
        &mut self,
        collection: impl Into<String>,
        prefix: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorListKeys {
            branch: None,
            space: None,
            collection: collection.into(),
            prefix,
            cursor,
            limit,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch vector metadata-update command.
    pub fn vector_update_metadata(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
        patch: serde_json::Value,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorUpdateMetadata {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
            patch,
        })
    }

    /// Executes a default-branch vector delete command.
    pub fn vector_delete(
        &mut self,
        collection: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorDelete {
            branch: None,
            space: None,
            collection: collection.into(),
            key: key.into(),
        })
    }

    /// Executes a default-branch vector filtered-delete command.
    pub fn vector_delete_by_filter(
        &mut self,
        collection: impl Into<String>,
        filter: VectorMetadataFilter,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorDeleteByFilter {
            branch: None,
            space: None,
            collection: collection.into(),
            filter,
        })
    }

    /// Executes a default-branch vector delete-all command.
    pub fn vector_delete_all(
        &mut self,
        collection: impl Into<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorDeleteAll {
            branch: None,
            space: None,
            collection: collection.into(),
        })
    }

    /// Executes a default-branch vector query command.
    pub fn vector_query(
        &mut self,
        collection: impl Into<String>,
        query: Vec<f32>,
        k: u64,
        filter: Option<VectorMetadataFilter>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorQuery {
            branch: None,
            space: None,
            collection: collection.into(),
            query: query.into_iter().map(f64::from).collect(),
            text: None,
            k,
            filter,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch vector index-query command.
    pub fn vector_index_query(
        &mut self,
        collection: impl Into<String>,
        query: Vec<f32>,
        k: u64,
        filter: Option<VectorMetadataFilter>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorIndexQuery {
            branch: None,
            space: None,
            collection: collection.into(),
            query: query.into_iter().map(f64::from).collect(),
            k,
            filter,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch vector batch-upsert command.
    pub fn vector_batch_upsert(
        &mut self,
        collection: impl Into<String>,
        entries: Vec<BatchVectorEntry>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorBatchUpsert {
            branch: None,
            space: None,
            collection: collection.into(),
            entries,
        })
    }

    /// Executes a default-branch vector batch-get command.
    pub fn vector_batch_get(
        &mut self,
        collection: impl Into<String>,
        keys: Vec<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorBatchGet {
            branch: None,
            space: None,
            collection: collection.into(),
            keys,
        })
    }

    /// Executes a default-branch vector batch-delete command.
    pub fn vector_batch_delete(
        &mut self,
        collection: impl Into<String>,
        keys: Vec<String>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::VectorBatchDelete {
            branch: None,
            space: None,
            collection: collection.into(),
            keys,
        })
    }
}
