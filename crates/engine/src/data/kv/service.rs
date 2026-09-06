//! Byte-oriented KV service.

use std::collections::BTreeSet;

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::ControlPlane;
use crate::diagnostics::{EngineError, EngineResult};
use crate::persistence::{
    decode_kv_key, encode_kv_key, encode_kv_key_bytes, encode_kv_space_prefix, CommitPlan,
    PersistenceReadRow, ReadSelector, RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::{
    KvBatchDeleteOutcome, KvBatchPutOutcome, KvDeleteOutcome, KvHistory, KvHistoryRow, KvKey,
    KvListPage, KvSample, KvScanRow, KvValue, KvVersionedValue, KvWriteOutcome, ProductSpace,
};

const KV_SCAN_RAW_PAGE_MIN: usize = 64;
const KV_SCAN_RAW_PAGE_MAX: usize = 4096;

/// Service for byte-oriented KV operations.
pub struct KvService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
    branch: BranchName,
    space: ProductSpace,
}

impl<'a> KvService<'a> {
    pub(crate) const fn new(
        persistence: &'a mut StoragePersistence,
        control: &'a mut ControlPlane,
        branch: BranchName,
        space: ProductSpace,
    ) -> Self {
        Self {
            persistence,
            control,
            branch,
            space,
        }
    }

    /// Writes a KV value.
    ///
    /// The returned outcome carries the create-vs-update fact computed by the
    /// engine, so callers never need a separate existence probe (rule 7).
    pub fn put(&mut self, key: KvKey, value: KvValue) -> EngineResult<KvWriteOutcome> {
        let outcome = self.put_batch([(key, value)])?;
        let created = outcome.created().first().copied().unwrap_or(false);
        Ok(KvWriteOutcome::new(outcome.commit(), created))
    }

    /// Writes multiple KV values in one commit.
    ///
    /// The engine determines whether each key was a create or an update by
    /// reading its latest visible state before the batch commits; duplicate keys
    /// are rejected, so the per-input flags stay positional with the input.
    pub fn put_batch<I>(&mut self, entries: I) -> EngineResult<KvBatchPutOutcome>
    where
        I: IntoIterator<Item = (KvKey, KvValue)>,
    {
        let record = self.branch_record()?;
        let mut seen = BTreeSet::new();
        let iterator = entries.into_iter();
        let mut mutations = Vec::with_capacity(iterator.size_hint().0);
        let mut created = Vec::with_capacity(iterator.size_hint().0);
        for (key, value) in iterator {
            let encoded_key = self.encode_batch_key(key, &mut seen)?;
            let address = RowAddress::new(record.storage_branch_id(), RowClass::Kv, encoded_key);
            let existed = self
                .persistence
                .read_row(address.clone(), ReadSelector::Latest)?
                .is_some_and(|row| !row.is_tombstone());
            created.push(!existed);
            mutations.push(RowMutation::put(address, value.into_bytes()));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(KvBatchPutOutcome::new(commit, created))
    }

    /// Reads the latest visible KV value.
    pub fn get(&mut self, key: &KvKey) -> EngineResult<Option<KvValue>> {
        Ok(self.get_versioned(key)?.map(KvVersionedValue::into_value))
    }

    /// Reads the latest visible KV value with commit metadata.
    pub fn get_versioned(&mut self, key: &KvKey) -> EngineResult<Option<KvVersionedValue>> {
        let branch_id = self.read_branch_id()?;
        let address = self.row_address_for_branch(branch_id, key);
        let Some(row) = self.persistence.read_row(address, ReadSelector::Latest)? else {
            return Ok(None);
        };
        versioned_value_from_row(&row)
    }

    /// Reads a KV value at a commit version.
    pub fn get_at_version(
        &mut self,
        key: &KvKey,
        version: CommitVersion,
    ) -> EngineResult<Option<KvValue>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, key);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtVersion(version))?
        else {
            return Ok(None);
        };
        value_from_row(&row)
    }

    /// Reads a KV value at a commit timestamp.
    pub fn get_at(&mut self, key: &KvKey, timestamp: Timestamp) -> EngineResult<Option<KvValue>> {
        Ok(self
            .get_versioned_at(key, timestamp)?
            .map(KvVersionedValue::into_value))
    }

    /// Reads a KV value at a commit timestamp with commit metadata.
    pub fn get_versioned_at(
        &mut self,
        key: &KvKey,
        timestamp: Timestamp,
    ) -> EngineResult<Option<KvVersionedValue>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, key);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtTimestamp(timestamp))?
        else {
            return Ok(None);
        };
        versioned_value_from_row(&row)
    }

    /// #3112 S4: joins each history row to its commit's wall-clock instant.
    ///
    /// One batched lookup for the whole history rather than one per row: the
    /// question is inherently plural, and the index answers it under a single
    /// lock. Rows keep their order, and a row whose instant is unknown keeps
    /// `None` — history stays exact even when its dates are not available.
    fn attach_committed_at(
        &mut self,
        record: &BranchCatalogRecord,
        rows: Vec<KvHistoryRow>,
    ) -> EngineResult<Vec<KvHistoryRow>> {
        let versions: Vec<_> = rows.iter().map(KvHistoryRow::version).collect();
        let instants = self
            .persistence
            .committed_at_for_versions(record.storage_branch_id(), &versions)?;
        Ok(rows
            .into_iter()
            .zip(instants)
            .map(|(row, instant)| row.with_committed_at(instant))
            .collect())
    }

    /// Reads full version history for a KV key.
    pub fn get_versions(&mut self, key: &KvKey) -> EngineResult<Option<KvHistory>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, key);
        let rows = self
            .persistence
            .read_history(&address, true)?
            .into_iter()
            .map(|row| history_row_from_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        // #3112 S4: instants are commit-scoped, so they cannot ride on the
        // rows — join them by commit version after the read.
        let rows = self.attach_committed_at(&record, rows)?;
        Ok((!rows.is_empty()).then(|| KvHistory::new(rows)))
    }

    /// Reads multiple latest visible KV values with commit metadata.
    pub fn batch_get(&mut self, keys: &[KvKey]) -> EngineResult<Vec<Option<KvVersionedValue>>> {
        let record = self.branch_record()?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let address = self.row_address(&record, key);
            let result = match self.persistence.read_row(address, ReadSelector::Latest)? {
                Some(row) => versioned_value_from_row(&row)?,
                None => None,
            };
            results.push(result);
        }
        Ok(results)
    }

    /// Returns true when the key has a latest visible value.
    pub fn exists(&mut self, key: &KvKey) -> EngineResult<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Checks multiple keys for latest visible values.
    pub fn batch_exists(&mut self, keys: &[KvKey]) -> EngineResult<Vec<bool>> {
        let record = self.branch_record()?;
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let address = self.row_address(&record, key);
            let exists = self
                .persistence
                .read_row(address, ReadSelector::Latest)?
                .is_some_and(|row| !row.is_tombstone());
            results.push(exists);
        }
        Ok(results)
    }

    /// Lists latest visible keys with an optional user-key prefix.
    pub fn list(&mut self, prefix: Option<&KvKey>) -> EngineResult<Vec<KvKey>> {
        let record = self.branch_record()?;
        let scan_prefix = self.scan_prefix(prefix);
        self.persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Kv,
                scan_prefix,
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| decode_kv_key(&self.space, row.key()))
            .collect()
    }

    /// Lists latest visible keys with cursor pagination.
    pub fn list_page(
        &mut self,
        prefix: Option<&KvKey>,
        cursor: Option<&KvKey>,
        limit: usize,
    ) -> EngineResult<KvListPage> {
        if limit == 0 {
            return Ok(KvListPage::new(Vec::new(), false, None));
        }
        let mut keys = self.scan_keys_after_cursor(
            prefix,
            cursor,
            limit.saturating_add(1),
            ReadSelector::Latest,
        )?;
        let has_more = keys.len() > limit;
        if has_more {
            keys.truncate(limit);
        }
        let cursor = has_more.then(|| keys.last().expect("non-empty page").clone());
        Ok(KvListPage::new(keys, has_more, cursor))
    }

    /// Lists a page of keys visible at a commit timestamp. Pagination
    /// (cursor + limit) is applied in the engine — mirrors `list_page`, so
    /// the caller never materializes the whole keyset for a time-travel list.
    pub fn list_at_page(
        &mut self,
        prefix: Option<&KvKey>,
        cursor: Option<&KvKey>,
        limit: usize,
        timestamp: Timestamp,
    ) -> EngineResult<KvListPage> {
        if limit == 0 {
            return Ok(KvListPage::new(Vec::new(), false, None));
        }
        let mut keys = self.scan_keys_after_cursor(
            prefix,
            cursor,
            limit.saturating_add(1),
            ReadSelector::AtTimestamp(timestamp),
        )?;
        let has_more = keys.len() > limit;
        if has_more {
            keys.truncate(limit);
        }
        let cursor = has_more.then(|| keys.last().expect("non-empty page").clone());
        Ok(KvListPage::new(keys, has_more, cursor))
    }

    /// Lists keys visible at a commit timestamp.
    pub fn list_at(
        &mut self,
        prefix: Option<&KvKey>,
        timestamp: Timestamp,
    ) -> EngineResult<Vec<KvKey>> {
        let record = self.branch_record()?;
        self.persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Kv,
                self.scan_prefix(prefix),
                ReadSelector::AtTimestamp(timestamp),
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| decode_kv_key(&self.space, row.key()))
            .collect()
    }

    /// Scans latest visible rows from an inclusive start key.
    pub fn scan(
        &mut self,
        start: Option<&KvKey>,
        limit: Option<usize>,
    ) -> EngineResult<Vec<KvScanRow>> {
        self.scan_range(start, None, limit)
    }

    /// Scans latest visible rows in an inclusive-lower, exclusive-upper range.
    pub fn scan_range(
        &mut self,
        start: Option<&KvKey>,
        end: Option<&KvKey>,
        limit: Option<usize>,
    ) -> EngineResult<Vec<KvScanRow>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let record = self.branch_record()?;
        let start = start.map_or_else(
            || encode_kv_space_prefix(&self.space),
            |key| encode_kv_key(&self.space, key),
        );
        let end = end.map_or_else(
            || next_prefix(&encode_kv_space_prefix(&self.space)),
            |key| encode_kv_key(&self.space, key),
        );
        if start >= end {
            return Ok(Vec::new());
        }
        match limit {
            Some(limit) => self.scan_range_limited(&record, start, &end, limit),
            None => self
                .persistence
                .scan_range(
                    record.storage_branch_id(),
                    RowClass::Kv,
                    Some(start),
                    Some(end),
                    ReadSelector::Latest,
                    None,
                )?
                .into_iter()
                .filter(|row| !row.is_tombstone())
                .map(|row| scan_row_from_row(&self.space, &row))
                .collect(),
        }
    }

    /// Counts latest visible keys with an optional user-key prefix.
    pub fn count(&mut self, prefix: Option<&KvKey>) -> EngineResult<u64> {
        self.count_with_selector(prefix, ReadSelector::Latest)
    }

    /// Counts keys visible at a commit timestamp with an optional user-key prefix.
    pub fn count_at(&mut self, prefix: Option<&KvKey>, timestamp: Timestamp) -> EngineResult<u64> {
        self.count_with_selector(prefix, ReadSelector::AtTimestamp(timestamp))
    }

    fn count_with_selector(
        &mut self,
        prefix: Option<&KvKey>,
        selector: ReadSelector,
    ) -> EngineResult<u64> {
        let record = self.branch_record()?;
        let count = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Kv,
                self.scan_prefix(prefix),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    /// Samples latest visible rows from one prefix scan.
    pub fn sample(&mut self, prefix: Option<&KvKey>, count: usize) -> EngineResult<KvSample> {
        let record = self.branch_record()?;
        let rows = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Kv,
                self.scan_prefix(prefix),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| scan_row_from_row(&self.space, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        let total_count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        if count == 0 || rows.is_empty() {
            return Ok(KvSample::new(total_count, Vec::new()));
        }
        if count >= rows.len() {
            return Ok(KvSample::new(total_count, rows));
        }
        let row_count = rows.len();
        let sampled = (0..count)
            .map(|index| rows[(index * row_count) / count].clone())
            .collect();
        Ok(KvSample::new(total_count, sampled))
    }

    /// Deletes a KV value if present.
    pub fn delete(&mut self, key: KvKey) -> EngineResult<KvDeleteOutcome> {
        let outcome = self.delete_batch([key])?;
        let deleted = outcome.deleted().first().copied().unwrap_or(false);
        Ok(KvDeleteOutcome::new(deleted, outcome.commit()))
    }

    /// Deletes multiple KV values in one commit.
    pub fn delete_batch<I>(&mut self, keys: I) -> EngineResult<KvBatchDeleteOutcome>
    where
        I: IntoIterator<Item = KvKey>,
    {
        let record = self.branch_record()?;
        let mut seen = BTreeSet::new();
        let iterator = keys.into_iter();
        let mut mutations = Vec::with_capacity(iterator.size_hint().0);
        let mut deleted = Vec::with_capacity(iterator.size_hint().0);
        for key in iterator {
            let encoded_key = self.encode_batch_key(key, &mut seen)?;
            let address = RowAddress::new(record.storage_branch_id(), RowClass::Kv, encoded_key);
            let exists = self
                .persistence
                .read_row(address.clone(), ReadSelector::Latest)?
                .is_some_and(|row| !row.is_tombstone());
            if exists {
                mutations.push(RowMutation::delete(address));
            }
            deleted.push(exists);
        }
        if deleted.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.kv_batch",
                "KV batch must contain at least one mutation",
            ));
        }
        if mutations.is_empty() {
            return Ok(KvBatchDeleteOutcome::new(deleted, None));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(KvBatchDeleteOutcome::new(deleted, Some(commit)))
    }

    /// B4: the read paths need only the storage branch id — a `Copy` — so
    /// they skip the per-read record clone (a `BranchName` String heap
    /// alloc). The catalog lookup itself stays: it is the staleness check
    /// (no control-plane epoch exists). Write paths keep [`branch_record`]
    /// (commits need the generation and full record).
    fn read_branch_id(&self) -> EngineResult<strata_core::BranchId> {
        self.control.require_healthy()?;
        self.control
            .lookup_branch(&self.branch)
            .map(BranchCatalogRecord::storage_branch_id)
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{}` does not exist", self.branch),
                )
            })
    }

    fn row_address_for_branch(&self, branch_id: strata_core::BranchId, key: &KvKey) -> RowAddress {
        RowAddress::new(branch_id, RowClass::Kv, encode_kv_key(&self.space, key))
    }

    fn branch_record(&self) -> EngineResult<BranchCatalogRecord> {
        self.control.require_healthy()?;
        self.control
            .lookup_branch(&self.branch)
            .cloned()
            .ok_or_else(|| {
                EngineError::not_found(
                    "not_found.engine.branch",
                    format!("branch `{}` does not exist", self.branch),
                )
            })
    }

    fn row_address(&self, record: &BranchCatalogRecord, key: &KvKey) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::Kv,
            encode_kv_key(&self.space, key),
        )
    }

    fn scan_prefix(&self, prefix: Option<&KvKey>) -> Vec<u8> {
        prefix.map_or_else(
            || encode_kv_space_prefix(&self.space),
            |key| encode_kv_key(&self.space, key),
        )
    }

    fn scan_keys_after_cursor(
        &mut self,
        prefix: Option<&KvKey>,
        cursor: Option<&KvKey>,
        limit: usize,
        selector: ReadSelector,
    ) -> EngineResult<Vec<KvKey>> {
        let record = self.branch_record()?;
        let prefix_start = self.scan_prefix(prefix);
        let prefix_end = next_prefix(&prefix_start);
        let mut start = cursor.map_or_else(
            || prefix_start.clone(),
            |key| encode_kv_key(&self.space, key),
        );
        if start < prefix_start {
            start = prefix_start;
        }
        if start >= prefix_end {
            return Ok(Vec::new());
        }
        let mut keys = Vec::with_capacity(limit);
        while keys.len() < limit && start < prefix_end {
            let raw_limit = limit.saturating_sub(keys.len()).saturating_add(1);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Kv,
                Some(start.clone()),
                Some(prefix_end.clone()),
                selector,
                Some(raw_limit),
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if row.is_tombstone() {
                    continue;
                }
                let key = decode_kv_key(&self.space, row.key())?;
                if cursor.is_some_and(|cursor| key.as_bytes() <= cursor.as_bytes()) {
                    continue;
                }
                if let Some(prefix) = prefix {
                    if !key.as_bytes().starts_with(prefix.as_bytes()) {
                        continue;
                    }
                }
                keys.push(key);
                if keys.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        Ok(keys)
    }

    fn scan_range_limited(
        &mut self,
        record: &BranchCatalogRecord,
        mut start: Vec<u8>,
        end: &[u8],
        limit: usize,
    ) -> EngineResult<Vec<KvScanRow>> {
        let mut visible = Vec::with_capacity(limit.min(KV_SCAN_RAW_PAGE_MIN));
        while visible.len() < limit && start.as_slice() < end {
            let remaining = limit.saturating_sub(visible.len());
            let raw_limit = remaining.clamp(KV_SCAN_RAW_PAGE_MIN, KV_SCAN_RAW_PAGE_MAX);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Kv,
                Some(start.clone()),
                Some(end.to_owned()),
                ReadSelector::Latest,
                Some(raw_limit),
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                if row.is_tombstone() {
                    continue;
                }
                visible.push(scan_row_from_row(&self.space, row)?);
                if visible.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        Ok(visible)
    }

    fn encode_batch_key(&self, key: KvKey, seen: &mut BTreeSet<Vec<u8>>) -> EngineResult<Vec<u8>> {
        let key_bytes = key.into_bytes();
        let encoded_key = encode_kv_key_bytes(&self.space, &key_bytes);
        if !seen.insert(encoded_key.clone()) {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.kv_batch_duplicate_key",
                "KV batch contains duplicate keys",
            ));
        }
        Ok(encoded_key)
    }

    fn commit_batch(
        &mut self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> EngineResult<CommitOutcome> {
        let mut mutations = mutations;
        if mutations.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.kv_batch",
                "KV batch must contain at least one mutation",
            ));
        }
        let user_put_count = mutations
            .iter()
            .filter(|mutation| mutation.is_put())
            .count();
        let user_delete_count = mutations
            .iter()
            .filter(|mutation| mutation.is_delete())
            .count();
        let mut space_mutations =
            ControlPlane::space_registration_mutations(self.persistence, record, &self.space)?;
        if !space_mutations.is_empty() {
            space_mutations.extend(mutations);
            mutations = space_mutations;
        }
        let plan = CommitPlan::new(
            record.storage_branch_id(),
            mutations,
            Some(record.generation()),
        );
        Ok(self
            .persistence
            .commit(&plan)?
            .with_counts(user_put_count, user_delete_count))
    }
}

fn versioned_value_from_row(row: &PersistenceReadRow) -> EngineResult<Option<KvVersionedValue>> {
    if row.is_tombstone() {
        return Ok(None);
    }
    let value = row_value(row)?;
    Ok(Some(KvVersionedValue::new(
        value,
        row.commit_version(),
        row.commit_timestamp(),
    )))
}

fn value_from_row(row: &PersistenceReadRow) -> EngineResult<Option<KvValue>> {
    if row.is_tombstone() {
        return Ok(None);
    }
    row_value(row).map(Some)
}

fn history_row_from_row(row: &PersistenceReadRow) -> EngineResult<KvHistoryRow> {
    let value = if row.is_tombstone() {
        None
    } else {
        Some(row_value(row)?)
    };
    Ok(KvHistoryRow::new(
        value,
        row.is_tombstone(),
        row.commit_version(),
        row.commit_timestamp(),
    ))
}

fn scan_row_from_row(space: &ProductSpace, row: &PersistenceReadRow) -> EngineResult<KvScanRow> {
    let key = decode_kv_key(space, row.key())?;
    let value = row_value(row)?;
    Ok(KvScanRow::new(
        key,
        value,
        row.commit_version(),
        row.commit_timestamp(),
    ))
}

fn row_value(row: &PersistenceReadRow) -> EngineResult<KvValue> {
    row.value().map(KvValue::new).ok_or_else(|| {
        EngineError::corruption(
            "data_loss.engine.kv_value",
            "stored KV row is missing a value",
        )
    })
}

fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return upper;
        }
    }
    vec![u8::MAX]
}

fn exclusive_after_key(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
