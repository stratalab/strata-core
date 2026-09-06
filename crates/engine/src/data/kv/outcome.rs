//! KV read outcome types.

use strata_core::{CommitVersion, Timestamp};

use crate::commit::CommitOutcome;

use super::{KvKey, KvValue};

/// Latest or historical KV value with commit metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvVersionedValue {
    value: KvValue,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl KvVersionedValue {
    pub(crate) const fn new(value: KvValue, version: CommitVersion, timestamp: Timestamp) -> Self {
        Self {
            value,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the stored value bytes.
    pub const fn value(&self) -> &KvValue {
        &self.value
    }

    /// B4: consume the versioned wrapper, moving the value out (kept
    /// `pub(crate)`: only `KvService::get`/`get_at` need it — rule 8).
    pub(crate) fn into_value(self) -> KvValue {
        self.value
    }

    #[must_use]
    /// Returns the commit version that wrote this value.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp that wrote this value.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// KV scan row with decoded user key and commit metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvScanRow {
    key: KvKey,
    value: KvValue,
    version: CommitVersion,
    timestamp: Timestamp,
}

impl KvScanRow {
    pub(crate) const fn new(
        key: KvKey,
        value: KvValue,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            key,
            value,
            version,
            timestamp,
        }
    }

    #[must_use]
    /// Returns the decoded user key.
    pub const fn key(&self) -> &KvKey {
        &self.key
    }

    #[must_use]
    /// Returns the stored value bytes.
    pub const fn value(&self) -> &KvValue {
        &self.value
    }

    #[must_use]
    /// Returns the commit version that wrote this row.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the commit timestamp that wrote this row.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Paginated list result for decoded KV keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvListPage {
    keys: Vec<KvKey>,
    has_more: bool,
    cursor: Option<KvKey>,
}

impl KvListPage {
    pub(crate) const fn new(keys: Vec<KvKey>, has_more: bool, cursor: Option<KvKey>) -> Self {
        Self {
            keys,
            has_more,
            cursor,
        }
    }

    #[must_use]
    /// Returns the page keys.
    pub fn keys(&self) -> &[KvKey] {
        &self.keys
    }

    #[must_use]
    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    #[must_use]
    /// Returns the cursor for the next page.
    pub const fn cursor(&self) -> Option<&KvKey> {
        self.cursor.as_ref()
    }
}

/// Version-history row for one KV key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvHistoryRow {
    value: Option<KvValue>,
    tombstone: bool,
    version: CommitVersion,
    timestamp: Timestamp,
    committed_at: Option<Timestamp>,
}

impl KvHistoryRow {
    pub(crate) const fn new(
        value: Option<KvValue>,
        tombstone: bool,
        version: CommitVersion,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            value,
            tombstone,
            version,
            timestamp,
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
    /// Returns the row value, if this history row is not a tombstone.
    pub const fn value(&self) -> Option<&KvValue> {
        self.value.as_ref()
    }

    #[must_use]
    /// Returns true when this history row represents a delete.
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    #[must_use]
    /// Returns the row commit version.
    pub const fn version(&self) -> CommitVersion {
        self.version
    }

    #[must_use]
    /// Returns the row commit timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Full version history for one KV key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvHistory {
    rows: Vec<KvHistoryRow>,
}

impl KvHistory {
    pub(crate) const fn new(rows: Vec<KvHistoryRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    /// Returns history rows newest-first.
    pub fn rows(&self) -> &[KvHistoryRow] {
        &self.rows
    }
}

/// KV sample result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvSample {
    total_count: u64,
    rows: Vec<KvScanRow>,
}

impl KvSample {
    pub(crate) const fn new(total_count: u64, rows: Vec<KvScanRow>) -> Self {
        Self { total_count, rows }
    }

    #[must_use]
    /// Returns the total number of matching live rows.
    pub const fn total_count(&self) -> u64 {
        self.total_count
    }

    #[must_use]
    /// Returns sampled rows.
    pub fn rows(&self) -> &[KvScanRow] {
        &self.rows
    }
}

/// Write outcome for one KV key.
///
/// The engine owns the create-vs-update fact: `created` is true when the write
/// installed a value for a key that had no latest visible value, false when it
/// overwrote an existing one. Executors relay this rather than re-deriving it
/// with a separate existence read (rule 7).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvWriteOutcome {
    commit: CommitOutcome,
    created: bool,
}

impl KvWriteOutcome {
    pub(crate) const fn new(commit: CommitOutcome, created: bool) -> Self {
        Self { commit, created }
    }

    #[must_use]
    /// Returns the storage commit facts for the write.
    pub const fn commit(self) -> CommitOutcome {
        self.commit
    }

    #[must_use]
    /// Returns true when the write created a new visible value.
    pub const fn created(self) -> bool {
        self.created
    }
}

/// Write outcome for a positional KV batch put.
///
/// Carries the shared storage commit plus a per-input create-vs-update flag.
/// KV batch puts reject duplicate keys, so every flag reflects the key's latest
/// visible state before the batch committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvBatchPutOutcome {
    commit: CommitOutcome,
    created: Vec<bool>,
}

impl KvBatchPutOutcome {
    pub(crate) const fn new(commit: CommitOutcome, created: Vec<bool>) -> Self {
        Self { commit, created }
    }

    #[must_use]
    /// Returns the shared storage commit facts for the batch.
    pub const fn commit(&self) -> CommitOutcome {
        self.commit
    }

    #[must_use]
    /// Returns positional create/update facts, one per input entry.
    pub fn created(&self) -> &[bool] {
        &self.created
    }
}

/// Delete outcome for one KV key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvDeleteOutcome {
    deleted: bool,
    commit: Option<CommitOutcome>,
}

impl KvDeleteOutcome {
    pub(crate) const fn new(deleted: bool, commit: Option<CommitOutcome>) -> Self {
        Self { deleted, commit }
    }

    #[must_use]
    /// Returns true when the selected key existed and was deleted.
    pub const fn deleted(self) -> bool {
        self.deleted
    }

    #[must_use]
    /// Returns the commit outcome when a delete mutation was committed.
    pub const fn commit(self) -> Option<CommitOutcome> {
        self.commit
    }
}

/// Delete outcome for a positional KV batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvBatchDeleteOutcome {
    deleted: Vec<bool>,
    commit: Option<CommitOutcome>,
}

impl KvBatchDeleteOutcome {
    pub(crate) const fn new(deleted: Vec<bool>, commit: Option<CommitOutcome>) -> Self {
        Self { deleted, commit }
    }

    #[must_use]
    /// Returns per-input-key deleted flags.
    pub fn deleted(&self) -> &[bool] {
        &self.deleted
    }

    #[must_use]
    /// Returns the commit outcome when at least one delete mutation was committed.
    pub const fn commit(&self) -> Option<CommitOutcome> {
        self.commit
    }
}

#[cfg(test)]
mod tests {
    use strata_core::{CommitVersion, Timestamp};

    use super::{
        KvBatchPutOutcome, KvHistory, KvHistoryRow, KvListPage, KvSample, KvScanRow,
        KvVersionedValue, KvWriteOutcome,
    };
    use crate::commit::{CommitDurability, CommitOutcome};
    use crate::data::kv::{KvKey, KvValue};

    #[test]
    fn write_outcomes_expose_engine_created_facts() {
        let commit = CommitOutcome::new(
            CommitVersion::new(11),
            Timestamp::from_micros(110),
            2,
            0,
            CommitDurability::Standard,
        );

        let created = KvWriteOutcome::new(commit, true);
        assert_eq!(created.commit(), commit);
        assert!(created.created());
        let updated = KvWriteOutcome::new(commit, false);
        assert!(!updated.created());

        let batch = KvBatchPutOutcome::new(commit, vec![true, false, true]);
        assert_eq!(batch.commit(), commit);
        assert_eq!(batch.created(), &[true, false, true]);
    }

    #[test]
    fn outcome_accessors_return_stored_facts() {
        let key = key(b"alpha");
        let value = value(b"value");
        let version = CommitVersion::new(42);
        let timestamp = Timestamp::from_micros(1_700);

        let versioned = KvVersionedValue::new(value.clone(), version, timestamp);
        assert_eq!(versioned.value(), &value);
        assert_eq!(versioned.version(), version);
        assert_eq!(versioned.timestamp(), timestamp);

        let scan = KvScanRow::new(key.clone(), value.clone(), version, timestamp);
        assert_eq!(scan.key(), &key);
        assert_eq!(scan.value(), &value);
        assert_eq!(scan.version(), version);
        assert_eq!(scan.timestamp(), timestamp);

        let history_value = KvHistoryRow::new(Some(value.clone()), false, version, timestamp);
        assert_eq!(history_value.value(), Some(&value));
        assert!(!history_value.is_tombstone());
        assert_eq!(history_value.version(), version);
        assert_eq!(history_value.timestamp(), timestamp);

        let history_tombstone = KvHistoryRow::new(None, true, CommitVersion::new(43), timestamp);
        assert_eq!(history_tombstone.value(), None);
        assert!(history_tombstone.is_tombstone());
    }

    #[test]
    fn collection_outcomes_borrow_internal_rows_immutably() {
        let first = key(b"a");
        let second = key(b"b");
        let page = KvListPage::new(
            vec![first.clone(), second.clone()],
            true,
            Some(second.clone()),
        );
        let page_keys: &[KvKey] = page.keys();
        assert_eq!(page_keys, &[first.clone(), second.clone()]);
        assert!(page.has_more());
        assert_eq!(page.cursor(), Some(&second));

        let value = value(b"value");
        let row = KvScanRow::new(
            first.clone(),
            value.clone(),
            CommitVersion::new(7),
            Timestamp::from_micros(70),
        );
        let sample = KvSample::new(3, vec![row.clone()]);
        let sample_rows: &[KvScanRow] = sample.rows();
        assert_eq!(sample.total_count(), 3);
        assert_eq!(sample_rows, &[row]);

        let history_row = KvHistoryRow::new(
            Some(value),
            false,
            CommitVersion::new(8),
            Timestamp::from_micros(80),
        );
        let history = KvHistory::new(vec![history_row.clone()]);
        let history_rows: &[KvHistoryRow] = history.rows();
        assert_eq!(history_rows, &[history_row]);
    }

    #[test]
    fn outcome_types_are_test_comparable_without_lower_layer_types() {
        let page = KvListPage::new(vec![key(b"a")], false, None);
        assert_eq!(page.clone(), page);

        let sample = KvSample::new(
            1,
            vec![KvScanRow::new(
                key(b"a"),
                value(b"value"),
                CommitVersion::new(1),
                Timestamp::from_micros(1),
            )],
        );
        assert_eq!(sample.clone(), sample);
    }

    fn key(bytes: &[u8]) -> KvKey {
        KvKey::new(bytes).expect("valid key")
    }

    fn value(bytes: &[u8]) -> KvValue {
        KvValue::new(bytes)
    }
}
