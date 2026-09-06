//! JSON outcome types.

use strata_core::{CommitVersion, Timestamp};

use crate::commit::CommitOutcome;

use super::{JsonDocumentId, JsonValue};

/// JSON write outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonWriteOutcome {
    commit: CommitOutcome,
    document_version: u64,
    created: bool,
}

impl JsonWriteOutcome {
    pub(crate) const fn new(commit: CommitOutcome, document_version: u64, created: bool) -> Self {
        Self {
            commit,
            document_version,
            created,
        }
    }

    #[must_use]
    /// Returns the storage commit facts.
    pub const fn commit(self) -> CommitOutcome {
        self.commit
    }

    #[must_use]
    /// Returns the JSON document version after the write.
    pub const fn document_version(self) -> u64 {
        self.document_version
    }

    #[must_use]
    /// Returns true when the write created a new document (rather than mutating
    /// an existing one). Computed by the engine so executors need no pre-read.
    pub const fn created(self) -> bool {
        self.created
    }
}

/// Latest or historical JSON value with commit metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonVersionedValue {
    value: JsonValue,
    version: CommitVersion,
    timestamp: Timestamp,
    document_version: u64,
}

impl JsonVersionedValue {
    pub(crate) const fn new(
        value: JsonValue,
        version: CommitVersion,
        timestamp: Timestamp,
        document_version: u64,
    ) -> Self {
        Self {
            value,
            version,
            timestamp,
            document_version,
        }
    }

    #[must_use]
    /// Returns the selected JSON value.
    pub const fn value(&self) -> &JsonValue {
        &self.value
    }

    #[must_use]
    /// Returns the commit version that wrote this document version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp that wrote this document version.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    #[must_use]
    /// Returns the JSON document version.
    pub const fn document_version(&self) -> u64 {
        self.document_version
    }
}

/// JSON history row.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonHistoryRow {
    value: Option<JsonValue>,
    tombstone: bool,
    version: CommitVersion,
    timestamp: Timestamp,
    document_version: Option<u64>,
    committed_at: Option<Timestamp>,
}

impl JsonHistoryRow {
    pub(crate) const fn new(
        value: Option<JsonValue>,
        tombstone: bool,
        version: CommitVersion,
        timestamp: Timestamp,
        document_version: Option<u64>,
    ) -> Self {
        Self {
            value,
            tombstone,
            version,
            timestamp,
            document_version,
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
    /// Returns the full document value when this row is not a tombstone.
    pub const fn value(&self) -> Option<&JsonValue> {
        self.value.as_ref()
    }

    #[must_use]
    /// Returns true when this row represents a document delete.
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
    /// Returns the JSON document version when present.
    pub const fn document_version(&self) -> Option<u64> {
        self.document_version
    }
}

/// Full JSON document history.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonHistory {
    rows: Vec<JsonHistoryRow>,
}

impl JsonHistory {
    pub(crate) const fn new(rows: Vec<JsonHistoryRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    /// Returns history rows newest-first.
    pub fn rows(&self) -> &[JsonHistoryRow] {
        &self.rows
    }
}

/// Paginated JSON document id list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonListPage {
    document_ids: Vec<JsonDocumentId>,
    has_more: bool,
    cursor: Option<JsonDocumentId>,
}

impl JsonListPage {
    pub(crate) const fn new(
        document_ids: Vec<JsonDocumentId>,
        has_more: bool,
        cursor: Option<JsonDocumentId>,
    ) -> Self {
        Self {
            document_ids,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns the document ids in this page.
    pub fn document_ids(&self) -> &[JsonDocumentId] {
        &self.document_ids
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns the cursor for the next page.
    pub const fn cursor(&self) -> Option<&JsonDocumentId> {
        self.cursor.as_ref()
    }
}

/// Sampled JSON document row.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonSampleRow {
    document_id: JsonDocumentId,
    value: JsonValue,
    version: CommitVersion,
    timestamp: Timestamp,
    document_version: u64,
}

impl JsonSampleRow {
    pub(crate) const fn new(
        document_id: JsonDocumentId,
        value: JsonValue,
        version: CommitVersion,
        timestamp: Timestamp,
        document_version: u64,
    ) -> Self {
        Self {
            document_id,
            value,
            version,
            timestamp,
            document_version,
        }
    }

    #[must_use]
    /// Returns the document id.
    pub const fn document_id(&self) -> &JsonDocumentId {
        &self.document_id
    }

    #[must_use]
    /// Returns the full document value.
    pub const fn value(&self) -> &JsonValue {
        &self.value
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
    /// Returns the JSON document version.
    pub const fn document_version(&self) -> u64 {
        self.document_version
    }
}

/// JSON sample result.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonSample {
    total_count: u64,
    rows: Vec<JsonSampleRow>,
}

impl JsonSample {
    pub(crate) const fn new(total_count: u64, rows: Vec<JsonSampleRow>) -> Self {
        Self { total_count, rows }
    }

    #[must_use]
    /// Returns the total number of matching live documents.
    pub const fn total_count(&self) -> u64 {
        self.total_count
    }

    #[must_use]
    /// Returns sampled rows.
    pub fn rows(&self) -> &[JsonSampleRow] {
        &self.rows
    }
}

/// JSON delete outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonDeleteOutcome {
    deleted: bool,
    commit: Option<CommitOutcome>,
}

impl JsonDeleteOutcome {
    pub(crate) const fn new(deleted: bool, commit: Option<CommitOutcome>) -> Self {
        Self { deleted, commit }
    }

    #[must_use]
    /// Returns true when the selected document/path existed and was deleted.
    pub const fn deleted(self) -> bool {
        self.deleted
    }

    #[must_use]
    /// Returns the commit outcome when a mutation was committed.
    pub const fn commit(self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Per-entry batch set outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonBatchSetItemOutcome {
    document_version: u64,
    created: bool,
}

impl JsonBatchSetItemOutcome {
    pub(crate) const fn new(document_version: u64, created: bool) -> Self {
        Self {
            document_version,
            created,
        }
    }

    #[must_use]
    /// Returns the document version after this entry.
    pub const fn document_version(self) -> u64 {
        self.document_version
    }

    #[must_use]
    /// Returns true when this entry created a new document. A repeated document
    /// id within one batch is an update from its second occurrence on; the
    /// engine owns this classification so executors need no intra-batch tracking.
    pub const fn created(self) -> bool {
        self.created
    }
}

/// JSON batch set outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonBatchSetOutcome {
    results: Vec<JsonBatchSetItemOutcome>,
    commit: Option<CommitOutcome>,
}

impl JsonBatchSetOutcome {
    pub(crate) const fn new(
        results: Vec<JsonBatchSetItemOutcome>,
        commit: Option<CommitOutcome>,
    ) -> Self {
        Self { results, commit }
    }

    #[must_use]
    /// Returns positional batch item outcomes.
    pub fn results(&self) -> &[JsonBatchSetItemOutcome] {
        &self.results
    }

    #[must_use]
    /// Returns the shared storage commit outcome when one was needed.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// JSON batch delete outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonBatchDeleteOutcome {
    deleted: Vec<bool>,
    commit: Option<CommitOutcome>,
}

impl JsonBatchDeleteOutcome {
    pub(crate) const fn new(deleted: Vec<bool>, commit: Option<CommitOutcome>) -> Self {
        Self { deleted, commit }
    }

    #[must_use]
    /// Returns positional deleted flags.
    pub fn deleted(&self) -> &[bool] {
        &self.deleted
    }

    #[must_use]
    /// Returns the shared storage commit outcome when one was needed.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

#[cfg(test)]
mod tests {
    use strata_core::{CommitVersion, Timestamp};

    use super::{
        JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome, JsonDeleteOutcome,
        JsonHistory, JsonHistoryRow, JsonListPage, JsonSample, JsonSampleRow, JsonVersionedValue,
        JsonWriteOutcome,
    };
    use crate::commit::{CommitDurability, CommitOutcome};
    use crate::data::json::{JsonDocumentId, JsonValue};

    #[test]
    fn outcome_accessors_return_stored_facts() {
        let commit = commit(7);
        let value = value(serde_json::json!({"a": 1}));
        let id = doc_id("doc");

        let write = JsonWriteOutcome::new(commit, 3, true);
        assert_eq!(write.commit(), commit);
        assert_eq!(write.document_version(), 3);
        assert!(write.created());
        let overwrite = JsonWriteOutcome::new(commit, 4, false);
        assert!(!overwrite.created());

        let versioned =
            JsonVersionedValue::new(value.clone(), commit.version(), commit.timestamp(), 3);
        assert_eq!(versioned.value(), &value);
        assert_eq!(versioned.version(), commit.version());
        assert_eq!(versioned.timestamp(), commit.timestamp());
        assert_eq!(versioned.document_version(), 3);

        let history_value = JsonHistoryRow::new(
            Some(value.clone()),
            false,
            commit.version(),
            commit.timestamp(),
            Some(3),
        );
        assert_eq!(history_value.value(), Some(&value));
        assert!(!history_value.is_tombstone());
        assert_eq!(history_value.document_version(), Some(3));

        let history_tombstone =
            JsonHistoryRow::new(None, true, CommitVersion::new(8), commit.timestamp(), None);
        assert_eq!(history_tombstone.value(), None);
        assert!(history_tombstone.is_tombstone());

        let sample = JsonSampleRow::new(
            id.clone(),
            value.clone(),
            commit.version(),
            commit.timestamp(),
            3,
        );
        assert_eq!(sample.document_id(), &id);
        assert_eq!(sample.value(), &value);
        assert_eq!(sample.version(), commit.version());
        assert_eq!(sample.timestamp(), commit.timestamp());
        assert_eq!(sample.document_version(), 3);
    }

    #[test]
    fn collection_outcomes_borrow_internal_rows_immutably() {
        let first = doc_id("a");
        let second = doc_id("b");
        let page = JsonListPage::new(
            vec![first.clone(), second.clone()],
            true,
            Some(second.clone()),
        );
        assert_eq!(page.document_ids(), &[first.clone(), second.clone()]);
        assert!(page.has_more());
        assert_eq!(page.cursor(), Some(&second));

        let row = JsonSampleRow::new(
            first.clone(),
            value(serde_json::json!(1)),
            CommitVersion::new(1),
            Timestamp::from_micros(1),
            1,
        );
        let sample = JsonSample::new(2, vec![row.clone()]);
        assert_eq!(sample.total_count(), 2);
        assert_eq!(sample.rows(), &[row]);

        let history_row = JsonHistoryRow::new(
            Some(value(serde_json::json!(2))),
            false,
            CommitVersion::new(2),
            Timestamp::from_micros(2),
            Some(2),
        );
        let history = JsonHistory::new(vec![history_row.clone()]);
        assert_eq!(history.rows(), &[history_row]);
    }

    #[test]
    fn mutation_outcomes_represent_committed_and_noop_batches() {
        let committed = commit(3);
        let delete = JsonDeleteOutcome::new(true, Some(committed));
        assert!(delete.deleted());
        assert_eq!(delete.commit(), Some(committed));

        let missing_delete = JsonDeleteOutcome::new(false, None);
        assert!(!missing_delete.deleted());
        assert_eq!(missing_delete.commit(), None);

        let set =
            JsonBatchSetOutcome::new(vec![JsonBatchSetItemOutcome::new(1, true)], Some(committed));
        assert_eq!(set.results()[0].document_version(), 1);
        assert!(set.results()[0].created());
        assert_eq!(set.commit(), Some(committed));

        let empty_set = JsonBatchSetOutcome::new(Vec::new(), None);
        assert!(empty_set.results().is_empty());
        assert_eq!(empty_set.commit(), None);

        let batch_delete = JsonBatchDeleteOutcome::new(vec![true, false], Some(committed));
        assert_eq!(batch_delete.deleted(), &[true, false]);
        assert_eq!(batch_delete.commit(), Some(committed));
    }

    #[test]
    fn outcome_types_are_test_comparable_without_lower_layer_types() {
        let page = JsonListPage::new(vec![doc_id("a")], false, None);
        assert_eq!(page.clone(), page);

        let sample = JsonSample::new(
            1,
            vec![JsonSampleRow::new(
                doc_id("a"),
                value(serde_json::json!("value")),
                CommitVersion::new(1),
                Timestamp::from_micros(1),
                1,
            )],
        );
        assert_eq!(sample.clone(), sample);
    }

    fn commit(version: u64) -> CommitOutcome {
        CommitOutcome::new(
            CommitVersion::new(version),
            Timestamp::from_micros(version * 10),
            1,
            0,
            CommitDurability::Standard,
        )
    }

    fn doc_id(value: &str) -> JsonDocumentId {
        JsonDocumentId::new(value).expect("valid document id")
    }

    fn value(value: serde_json::Value) -> JsonValue {
        JsonValue::new(value).expect("valid JSON value")
    }
}
