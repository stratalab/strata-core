//! Vector outcome types.

use strata_core::{CommitVersion, Timestamp};

use crate::commit::CommitOutcome;

use super::{VectorCollectionName, VectorConfig, VectorEmbedding, VectorKey, VectorMetadata};

/// Vector collection metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorCollectionInfo {
    name: VectorCollectionName,
    config: VectorConfig,
    count: u64,
    created_version: CommitVersion,
    created_timestamp: Timestamp,
}

impl VectorCollectionInfo {
    pub(crate) const fn new(
        name: VectorCollectionName,
        config: VectorConfig,
        count: u64,
        created_version: CommitVersion,
        created_timestamp: Timestamp,
    ) -> Self {
        Self {
            name,
            config,
            count,
            created_version,
            created_timestamp,
        }
    }

    #[must_use]
    /// Returns the collection name.
    pub const fn name(&self) -> &VectorCollectionName {
        &self.name
    }

    #[must_use]
    /// Returns the immutable collection config.
    pub const fn config(&self) -> VectorConfig {
        self.config
    }

    #[must_use]
    /// Returns the visible vector count.
    pub const fn count(&self) -> u64 {
        self.count
    }

    #[must_use]
    /// Returns the commit version that created the visible config row.
    pub const fn created_version(&self) -> CommitVersion {
        self.created_version
    }

    #[must_use]
    /// Returns the commit timestamp that created the visible config row.
    pub const fn created_timestamp(&self) -> Timestamp {
        self.created_timestamp
    }
}

/// Latest vector entry.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorEntry {
    key: VectorKey,
    embedding: VectorEmbedding,
    metadata: Option<VectorMetadata>,
    vector_revision: u64,
}

impl VectorEntry {
    pub(crate) const fn new(
        key: VectorKey,
        embedding: VectorEmbedding,
        metadata: Option<VectorMetadata>,
        vector_revision: u64,
    ) -> Self {
        Self {
            key,
            embedding,
            metadata,
            vector_revision,
        }
    }

    #[must_use]
    /// Returns the vector key.
    pub const fn key(&self) -> &VectorKey {
        &self.key
    }

    #[must_use]
    /// Returns the embedding.
    pub const fn embedding(&self) -> &VectorEmbedding {
        &self.embedding
    }

    #[must_use]
    /// Returns optional metadata.
    pub const fn metadata(&self) -> Option<&VectorMetadata> {
        self.metadata.as_ref()
    }

    #[must_use]
    /// Returns the product vector revision.
    pub const fn vector_revision(&self) -> u64 {
        self.vector_revision
    }
}

/// Vector entry with commit metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorVersionedEntry {
    entry: VectorEntry,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl VectorVersionedEntry {
    pub(crate) const fn new(
        entry: VectorEntry,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            entry,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the vector entry.
    pub const fn entry(&self) -> &VectorEntry {
        &self.entry
    }

    #[must_use]
    /// Returns the vector key.
    pub const fn key(&self) -> &VectorKey {
        self.entry.key()
    }

    #[must_use]
    /// Returns the embedding.
    pub const fn embedding(&self) -> &VectorEmbedding {
        self.entry.embedding()
    }

    #[must_use]
    /// Returns optional metadata.
    pub const fn metadata(&self) -> Option<&VectorMetadata> {
        self.entry.metadata()
    }

    #[must_use]
    /// Returns the product vector revision.
    pub const fn vector_revision(&self) -> u64 {
        self.entry.vector_revision()
    }

    #[must_use]
    /// Returns the commit version that wrote this entry.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp that wrote this entry.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Vector history row.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorHistoryRow {
    entry: Option<VectorEntry>,
    tombstone: bool,
    version: CommitVersion,
    timestamp: Timestamp,
    vector_revision: Option<u64>,
    committed_at: Option<Timestamp>,
}

impl VectorHistoryRow {
    pub(crate) const fn new(
        entry: Option<VectorEntry>,
        tombstone: bool,
        version: CommitVersion,
        timestamp: Timestamp,
        vector_revision: Option<u64>,
    ) -> Self {
        Self {
            entry,
            tombstone,
            version,
            timestamp,
            vector_revision,
            committed_at: None,
        }
    }

    /// The commit's wall-clock instant (#3112 S4), or `None` when unknown —
    /// a commit predating `committed_at`, or one whose dates the branch
    /// cannot vouch for. Unknown is a first-class answer here: the history
    /// row itself is still exact, only its date is missing.
    #[must_use]
    pub const fn committed_at(&self) -> Option<Timestamp> {
        self.committed_at
    }

    /// Attaches the commit's wall-clock instant. A builder so `new`'s call
    /// sites stay put (#3112 S4).
    #[must_use]
    pub(crate) const fn with_committed_at(mut self, committed_at: Option<Timestamp>) -> Self {
        self.committed_at = committed_at;
        self
    }

    #[must_use]
    /// Returns the vector entry for value rows.
    pub const fn entry(&self) -> Option<&VectorEntry> {
        self.entry.as_ref()
    }

    #[must_use]
    /// Returns true when this row is a delete tombstone.
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    #[must_use]
    /// Returns the storage commit version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the storage commit timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    #[must_use]
    /// Returns the product vector revision when present.
    pub const fn vector_revision(&self) -> Option<u64> {
        self.vector_revision
    }
}

/// Full vector history for one key.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorHistory {
    rows: Vec<VectorHistoryRow>,
}

impl VectorHistory {
    pub(crate) const fn new(rows: Vec<VectorHistoryRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    /// Returns history rows newest-first.
    pub fn rows(&self) -> &[VectorHistoryRow] {
        &self.rows
    }
}

/// Vector search match.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchMatch {
    entry: VectorEntry,
    score: f32,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl VectorSearchMatch {
    pub(crate) const fn new(
        entry: VectorEntry,
        score: f32,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            entry,
            score,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the vector entry.
    pub const fn entry(&self) -> &VectorEntry {
        &self.entry
    }

    #[must_use]
    /// Returns the similarity score.
    pub const fn score(&self) -> f32 {
        self.score
    }

    #[must_use]
    /// Returns the commit version of the matched row.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp of the matched row.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Vector search result.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    matches: Vec<VectorSearchMatch>,
}

impl VectorSearchResult {
    pub(crate) const fn new(matches: Vec<VectorSearchMatch>) -> Self {
        Self { matches }
    }

    #[must_use]
    /// Returns ordered search matches.
    pub fn matches(&self) -> &[VectorSearchMatch] {
        &self.matches
    }
}

/// Paginated vector key result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorKeyPage {
    keys: Vec<VectorKey>,
    has_more: bool,
    cursor: Option<VectorKey>,
}

impl VectorKeyPage {
    pub(crate) const fn new(
        keys: Vec<VectorKey>,
        has_more: bool,
        cursor: Option<VectorKey>,
    ) -> Self {
        Self {
            keys,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns page keys.
    pub fn keys(&self) -> &[VectorKey] {
        &self.keys
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns the cursor for the next page.
    pub const fn cursor(&self) -> Option<&VectorKey> {
        self.cursor.as_ref()
    }
}

/// Vector write outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorWriteOutcome {
    commit: CommitOutcome,
    vector_revision: u64,
    created: bool,
}

impl VectorWriteOutcome {
    pub(crate) const fn new(commit: CommitOutcome, vector_revision: u64, created: bool) -> Self {
        Self {
            commit,
            vector_revision,
            created,
        }
    }

    #[must_use]
    /// Returns storage commit facts.
    pub const fn commit(self) -> CommitOutcome {
        self.commit
    }

    #[must_use]
    /// Returns the product vector revision after the write.
    pub const fn vector_revision(self) -> u64 {
        self.vector_revision
    }

    #[must_use]
    /// Returns true when the write created a visible vector.
    pub const fn created(self) -> bool {
        self.created
    }
}

/// Metadata update outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorMetadataUpdateOutcome {
    key: VectorKey,
    updated: bool,
    vector_revision: Option<u64>,
    commit: Option<CommitOutcome>,
}

impl VectorMetadataUpdateOutcome {
    pub(crate) const fn new(
        key: VectorKey,
        updated: bool,
        vector_revision: Option<u64>,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self {
            key,
            updated,
            vector_revision,
            commit,
        }
    }

    #[must_use]
    /// Returns the vector key.
    pub const fn key(&self) -> &VectorKey {
        &self.key
    }

    #[must_use]
    /// Returns true when a row was updated.
    pub const fn updated(&self) -> bool {
        self.updated
    }

    #[must_use]
    /// Returns the vector revision when updated.
    pub const fn vector_revision(&self) -> Option<u64> {
        self.vector_revision
    }

    #[must_use]
    /// Returns commit facts when a row was updated.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Vector delete outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorDeleteOutcome {
    key: VectorKey,
    deleted: bool,
    commit: Option<CommitOutcome>,
}

impl VectorDeleteOutcome {
    pub(crate) const fn new(key: VectorKey, deleted: bool, commit: Option<CommitOutcome>) -> Self {
        Self {
            key,
            deleted,
            commit,
        }
    }

    #[must_use]
    /// Returns the vector key.
    pub const fn key(&self) -> &VectorKey {
        &self.key
    }

    #[must_use]
    /// Returns true when a visible vector was deleted.
    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    #[must_use]
    /// Returns commit facts when a row was deleted.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Vector bulk delete outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorBulkDeleteOutcome {
    deleted_count: u64,
    commit: Option<CommitOutcome>,
}

impl VectorBulkDeleteOutcome {
    pub(crate) const fn new(deleted_count: u64, commit: Option<CommitOutcome>) -> Self {
        Self {
            deleted_count,
            commit,
        }
    }

    #[must_use]
    /// Returns the number of visible vectors deleted.
    pub const fn deleted_count(self) -> u64 {
        self.deleted_count
    }

    #[must_use]
    /// Returns commit facts when rows were deleted.
    pub const fn commit(self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Vector batch upsert outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorBatchUpsertOutcome {
    vector_revisions: Vec<u64>,
    created: Vec<bool>,
    commit: Option<CommitOutcome>,
}

impl VectorBatchUpsertOutcome {
    pub(crate) fn new(
        vector_revisions: Vec<u64>,
        created: Vec<bool>,
        commit: Option<CommitOutcome>,
    ) -> Self {
        assert_eq!(
            vector_revisions.len(),
            created.len(),
            "vector batch upsert revisions and create facts must stay positional"
        );
        Self {
            vector_revisions,
            created,
            commit,
        }
    }

    #[must_use]
    /// Returns positional vector revisions.
    pub fn vector_revisions(&self) -> &[u64] {
        &self.vector_revisions
    }

    #[must_use]
    /// Returns positional create/update facts.
    pub fn created(&self) -> &[bool] {
        &self.created
    }

    #[must_use]
    /// Returns commit facts when the batch wrote rows.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Vector batch get outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorBatchGetOutcome {
    entries: Vec<Option<VectorVersionedEntry>>,
}

impl VectorBatchGetOutcome {
    pub(crate) const fn new(entries: Vec<Option<VectorVersionedEntry>>) -> Self {
        Self { entries }
    }

    #[must_use]
    /// Returns positional get entries.
    pub fn entries(&self) -> &[Option<VectorVersionedEntry>] {
        &self.entries
    }
}

/// Vector batch delete outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorBatchDeleteOutcome {
    deleted: Vec<bool>,
    commit: Option<CommitOutcome>,
}

impl VectorBatchDeleteOutcome {
    pub(crate) const fn new(deleted: Vec<bool>, commit: Option<CommitOutcome>) -> Self {
        Self { deleted, commit }
    }

    #[must_use]
    /// Returns positional delete flags.
    pub fn deleted(&self) -> &[bool] {
        &self.deleted
    }

    #[must_use]
    /// Returns commit facts when rows were deleted.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use strata_core::{CommitVersion, Timestamp};

    use super::{
        VectorBatchDeleteOutcome, VectorBatchGetOutcome, VectorBatchUpsertOutcome,
        VectorBulkDeleteOutcome, VectorCollectionInfo, VectorDeleteOutcome, VectorEntry,
        VectorHistory, VectorHistoryRow, VectorKeyPage, VectorMetadataUpdateOutcome,
        VectorSearchMatch, VectorSearchResult, VectorVersionedEntry, VectorWriteOutcome,
    };
    use crate::commit::{CommitDurability, CommitOutcome};
    use crate::data::vector::{
        VectorCollectionName, VectorConfig, VectorDistanceMetric, VectorEmbedding, VectorKey,
        VectorMetadata,
    };

    #[test]
    fn vector_collection_and_entry_outcomes_expose_commit_facts() {
        let collection = collection("docs");
        let config = VectorConfig::new(2, VectorDistanceMetric::Cosine).expect("valid config");
        let info = VectorCollectionInfo::new(
            collection.clone(),
            config,
            3,
            CommitVersion::new(7),
            Timestamp::from_micros(70),
        );
        assert_eq!(info.name(), &collection);
        assert_eq!(info.config(), config);
        assert_eq!(info.count(), 3);
        assert_eq!(info.created_version(), CommitVersion::new(7));
        assert_eq!(info.created_timestamp(), Timestamp::from_micros(70));

        let entry = entry("a", [1.0, 0.0], Some(json!({"kind": "doc"})), 4);
        let versioned = VectorVersionedEntry::new(
            entry.clone(),
            CommitVersion::new(8),
            Timestamp::from_micros(80),
        );
        assert_eq!(versioned.entry(), &entry);
        assert_eq!(versioned.key(), entry.key());
        assert_eq!(versioned.embedding(), entry.embedding());
        assert_eq!(versioned.metadata(), entry.metadata());
        assert_eq!(versioned.vector_revision(), 4);
        assert_eq!(versioned.version(), CommitVersion::new(8));
        assert_eq!(versioned.timestamp(), Timestamp::from_micros(80));
    }

    #[test]
    fn vector_history_search_page_and_delete_outcomes_expose_facts() {
        let commit = commit(9, 90, 2, 1);
        let entry = entry("a", [1.0, 0.0], None, 2);
        let tombstone = VectorHistoryRow::new(
            None,
            true,
            CommitVersion::new(10),
            Timestamp::from_micros(100),
            None,
        );
        let value = VectorHistoryRow::new(
            Some(entry.clone()),
            false,
            CommitVersion::new(9),
            Timestamp::from_micros(90),
            Some(2),
        );
        let history = VectorHistory::new(vec![tombstone.clone(), value.clone()]);
        assert!(history.rows()[0].is_tombstone());
        assert_eq!(history.rows()[1].entry(), Some(&entry));
        assert_eq!(history.rows()[1].vector_revision(), Some(2));

        let search = VectorSearchResult::new(vec![VectorSearchMatch::new(
            entry.clone(),
            0.5,
            CommitVersion::new(9),
            Timestamp::from_micros(90),
        )]);
        assert_eq!(search.matches()[0].entry(), &entry);
        assert!((search.matches()[0].score() - 0.5).abs() < f32::EPSILON);
        assert_eq!(search.matches()[0].version(), CommitVersion::new(9));
        assert_eq!(search.matches()[0].timestamp(), Timestamp::from_micros(90));

        let page = VectorKeyPage::new(vec![key("a")], true, Some(key("a")));
        assert_eq!(page.keys(), &[key("a")]);
        assert!(page.has_more());
        assert_eq!(page.cursor(), Some(&key("a")));

        let deleted = VectorDeleteOutcome::new(key("a"), true, Some(commit));
        assert_eq!(deleted.key(), &key("a"));
        assert!(deleted.deleted());
        assert_eq!(deleted.commit(), Some(commit));

        let bulk = VectorBulkDeleteOutcome::new(2, Some(commit));
        assert_eq!(bulk.deleted_count(), 2);
        assert_eq!(bulk.commit(), Some(commit));
    }

    #[test]
    fn vector_write_metadata_and_batch_outcomes_are_positional() {
        let commit = commit(11, 110, 3, 0);
        let write = VectorWriteOutcome::new(commit, 6, true);
        assert_eq!(write.commit(), commit);
        assert_eq!(write.vector_revision(), 6);
        assert!(write.created());

        let update = VectorMetadataUpdateOutcome::new(key("a"), true, Some(7), Some(commit));
        assert_eq!(update.key(), &key("a"));
        assert!(update.updated());
        assert_eq!(update.vector_revision(), Some(7));
        assert_eq!(update.commit(), Some(commit));

        let upsert =
            VectorBatchUpsertOutcome::new(vec![1, 2, 3], vec![true, false, true], Some(commit));
        assert_eq!(upsert.vector_revisions(), &[1, 2, 3]);
        assert_eq!(upsert.created(), &[true, false, true]);
        assert_eq!(upsert.commit(), Some(commit));

        let get = VectorBatchGetOutcome::new(vec![
            Some(VectorVersionedEntry::new(
                entry("a", [1.0, 0.0], None, 1),
                CommitVersion::new(11),
                Timestamp::from_micros(110),
            )),
            None,
        ]);
        assert_eq!(get.entries().len(), 2);
        assert!(get.entries()[0].is_some());
        assert!(get.entries()[1].is_none());

        let delete = VectorBatchDeleteOutcome::new(vec![true, false, true], Some(commit));
        assert_eq!(delete.deleted(), &[true, false, true]);
        assert_eq!(delete.commit(), Some(commit));
    }

    fn commit(version: u64, timestamp: u64, puts: usize, deletes: usize) -> CommitOutcome {
        CommitOutcome::new(
            CommitVersion::new(version),
            Timestamp::from_micros(timestamp),
            puts,
            deletes,
            CommitDurability::Standard,
        )
    }

    fn entry<const N: usize>(
        key_text: &str,
        values: [f32; N],
        metadata: Option<serde_json::Value>,
        revision: u64,
    ) -> VectorEntry {
        VectorEntry::new(
            key(key_text),
            VectorEmbedding::new(values).expect("valid embedding"),
            metadata.map(|value| VectorMetadata::new(value).expect("valid metadata")),
            revision,
        )
    }

    fn collection(name: &str) -> VectorCollectionName {
        VectorCollectionName::new(name).expect("valid collection")
    }

    fn key(value: &str) -> VectorKey {
        VectorKey::new(value).expect("valid key")
    }
}
