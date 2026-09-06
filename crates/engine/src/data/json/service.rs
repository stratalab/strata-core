//! JSON document service.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use strata_core::{CommitVersion, Timestamp};

use crate::branch::catalog::BranchCatalogRecord;
use crate::branch::BranchName;
use crate::commit::CommitOutcome;
use crate::control::ControlPlane;
use crate::data::kv::ProductSpace;
use crate::diagnostics::{EngineError, EngineResult};
use crate::persistence::{
    decode_json_document_id, decode_json_index_name, encode_json_index_entry_key,
    encode_json_index_entry_prefix, encode_json_index_meta_key, encode_json_index_meta_prefix,
    encode_json_key, encode_json_space_prefix, CommitPlan, PersistenceReadRow, ReadSelector,
    RowAddress, RowClass, RowMutation, StoragePersistence,
};

use super::{
    decode_index_definition, decode_stored_document, delete_at_path, encode_index_definition,
    encode_stored_document, extract_index_value, get_at_path, index_entry_value_bytes, set_at_path,
    JsonBatchDeleteOutcome, JsonBatchSetItemOutcome, JsonBatchSetOutcome, JsonDeleteOutcome,
    JsonDocument, JsonDocumentId, JsonGetEntry, JsonHistory, JsonHistoryRow, JsonIndexDefinition,
    JsonIndexName, JsonIndexType, JsonListPage, JsonSample, JsonSampleRow, JsonSetEntry, JsonValue,
    JsonVersionedValue, JsonWriteOutcome,
};

const JSON_LIST_RAW_PAGE_MIN: usize = 64;
const JSON_LIST_RAW_PAGE_MAX: usize = 1024;

/// Service for JSON document operations.
pub struct JsonService<'a> {
    persistence: &'a mut StoragePersistence,
    control: &'a mut ControlPlane,
    branch: BranchName,
    space: ProductSpace,
}

struct BatchDeleteState {
    original_value: Option<JsonValue>,
    current: Option<JsonDocument>,
    changed: bool,
}

impl BatchDeleteState {
    fn existing(document: JsonDocument) -> Self {
        Self {
            original_value: Some(document.value().clone()),
            current: Some(document),
            changed: false,
        }
    }

    const fn missing() -> Self {
        Self {
            original_value: None,
            current: None,
            changed: false,
        }
    }

    fn delete(&mut self, path: &super::JsonPath) -> EngineResult<bool> {
        if self.current.is_none() {
            return Ok(false);
        }
        if path.is_root() {
            self.current = None;
            self.changed = true;
            return Ok(true);
        }
        let document = self.current.as_mut().expect("current document exists");
        if !delete_at_path(document.value_mut(), path)? {
            return Ok(false);
        }
        document.touch();
        self.changed = true;
        Ok(true)
    }
}

impl<'a> JsonService<'a> {
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

    /// Creates a new document at the root path.
    pub fn create(
        &mut self,
        id: JsonDocumentId,
        value: JsonValue,
    ) -> EngineResult<JsonWriteOutcome> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, &id);
        if self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone())
        {
            return Err(EngineError::conflict(
                "already_exists.engine.json_document",
                "JSON document already exists",
            ));
        }
        let document = JsonDocument::new(id, value);
        let indexes = self.load_indexes(&record)?;
        let mut mutations = Vec::new();
        mutations.push(RowMutation::put(
            address,
            encode_stored_document(&document)?,
        ));
        mutations.extend(self.index_mutations_for_change(
            &record,
            document.id(),
            None,
            Some(document.value()),
            &indexes,
        ));
        let commit = self.commit_batch(&record, mutations)?;
        // `create` refuses to overwrite an existing document, so a successful
        // commit here always installed a brand-new document.
        Ok(JsonWriteOutcome::new(
            commit,
            document.document_version(),
            true,
        ))
    }

    /// Sets a root value or path, creating the document when missing.
    pub fn set_or_create(
        &mut self,
        id: JsonDocumentId,
        path: &super::JsonPath,
        value: JsonValue,
    ) -> EngineResult<JsonWriteOutcome> {
        self.set_inner(id, path, value, true)
    }

    /// Sets a root value or path on an existing document.
    pub fn set(
        &mut self,
        id: JsonDocumentId,
        path: &super::JsonPath,
        value: JsonValue,
    ) -> EngineResult<JsonWriteOutcome> {
        self.set_inner(id, path, value, false)
    }

    /// Reads the latest visible value at a path.
    pub fn get(
        &mut self,
        id: &JsonDocumentId,
        path: &super::JsonPath,
    ) -> EngineResult<Option<JsonValue>> {
        Ok(self
            .get_versioned(id, path)?
            .map(|value| value.value().clone()))
    }

    /// Reads the latest visible value at a path with metadata.
    pub fn get_versioned(
        &mut self,
        id: &JsonDocumentId,
        path: &super::JsonPath,
    ) -> EngineResult<Option<JsonVersionedValue>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, id);
        let Some(row) = self.persistence.read_row(address, ReadSelector::Latest)? else {
            return Ok(None);
        };
        Self::versioned_value_from_row(id, path, &row)
    }

    /// Reads a value at a commit version.
    pub fn get_at_version(
        &mut self,
        id: &JsonDocumentId,
        path: &super::JsonPath,
        version: CommitVersion,
    ) -> EngineResult<Option<JsonValue>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, id);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtVersion(version))?
        else {
            return Ok(None);
        };
        Self::value_from_row(id, path, &row)
    }

    /// Reads a value at a timestamp.
    pub fn get_at(
        &mut self,
        id: &JsonDocumentId,
        path: &super::JsonPath,
        timestamp: Timestamp,
    ) -> EngineResult<Option<JsonValue>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, id);
        let Some(row) = self
            .persistence
            .read_row(address, ReadSelector::AtTimestamp(timestamp))?
        else {
            return Ok(None);
        };
        Self::value_from_row(id, path, &row)
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
        rows: Vec<JsonHistoryRow>,
    ) -> EngineResult<Vec<JsonHistoryRow>> {
        let versions: Vec<_> = rows.iter().map(JsonHistoryRow::version).collect();
        let instants = self
            .persistence
            .committed_at_for_versions(record.storage_branch_id(), &versions)?;
        Ok(rows
            .into_iter()
            .zip(instants)
            .map(|(row, instant)| row.with_committed_at(instant))
            .collect())
    }

    /// Reads full document history newest-first.
    pub fn get_versions(&mut self, id: &JsonDocumentId) -> EngineResult<Option<JsonHistory>> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, id);
        let rows = self
            .persistence
            .read_history(&address, true)?
            .into_iter()
            .map(|row| Self::history_row_from_row(id, &row))
            .collect::<EngineResult<Vec<_>>>()?;
        // #3112 S4: instants are commit-scoped, so they cannot ride on the
        // rows — join them by commit version after the read.
        let rows = self.attach_committed_at(&record, rows)?;
        Ok((!rows.is_empty()).then(|| JsonHistory::new(rows)))
    }

    /// Reads multiple latest visible values with metadata.
    pub fn batch_get(
        &mut self,
        entries: &[JsonGetEntry],
    ) -> EngineResult<Vec<Option<JsonVersionedValue>>> {
        let record = self.branch_record()?;
        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            let address = self.row_address(&record, entry.id());
            let result = match self.persistence.read_row(address, ReadSelector::Latest)? {
                Some(row) => Self::versioned_value_from_row(entry.id(), entry.path(), &row)?,
                None => None,
            };
            results.push(result);
        }
        Ok(results)
    }

    /// Returns true when the document exists.
    pub fn exists(&mut self, id: &JsonDocumentId) -> EngineResult<bool> {
        let record = self.branch_record()?;
        let address = self.row_address(&record, id);
        Ok(self
            .persistence
            .read_row(address, ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone()))
    }

    /// Checks multiple documents for latest visible values.
    pub fn batch_exists(&mut self, ids: &[JsonDocumentId]) -> EngineResult<Vec<bool>> {
        let record = self.branch_record()?;
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let address = self.row_address(&record, id);
            let exists = self
                .persistence
                .read_row(address, ReadSelector::Latest)?
                .is_some_and(|row| !row.is_tombstone());
            results.push(exists);
        }
        Ok(results)
    }

    /// Deletes a root document or one path.
    pub fn delete(
        &mut self,
        id: JsonDocumentId,
        path: &super::JsonPath,
    ) -> EngineResult<JsonDeleteOutcome> {
        if path.is_root() {
            return self.delete_document(id);
        }
        let record = self.branch_record()?;
        let address = self.row_address(&record, &id);
        let Some(row) = self
            .persistence
            .read_row(address.clone(), ReadSelector::Latest)?
        else {
            return Ok(JsonDeleteOutcome::new(false, None));
        };
        if row.is_tombstone() {
            return Ok(JsonDeleteOutcome::new(false, None));
        }
        let mut document = Self::document_from_row(&id, &row)?;
        let old_value = document.value().clone();
        let deleted = delete_at_path(document.value_mut(), path)?;
        if !deleted {
            return Ok(JsonDeleteOutcome::new(false, None));
        }
        document.touch();
        let indexes = self.load_indexes(&record)?;
        let mut mutations = vec![RowMutation::put(
            address,
            encode_stored_document(&document)?,
        )];
        mutations.extend(self.index_mutations_for_change(
            &record,
            &id,
            Some(&old_value),
            Some(document.value()),
            &indexes,
        ));
        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonDeleteOutcome::new(true, Some(commit)))
    }

    /// Deletes a whole document.
    pub fn delete_document(&mut self, id: JsonDocumentId) -> EngineResult<JsonDeleteOutcome> {
        let outcome = self.batch_delete([id])?;
        let deleted = outcome.deleted().first().copied().unwrap_or(false);
        Ok(JsonDeleteOutcome::new(deleted, outcome.commit()))
    }

    /// Sets multiple JSON entries in one commit.
    pub fn batch_set_or_create<I>(&mut self, entries: I) -> EngineResult<JsonBatchSetOutcome>
    where
        I: IntoIterator<Item = JsonSetEntry>,
    {
        let record = self.branch_record()?;
        let indexes = self.load_indexes(&record)?;
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut documents: BTreeMap<JsonDocumentId, (JsonDocument, Option<JsonValue>)> =
            BTreeMap::new();
        let mut results = Vec::with_capacity(entries.len());
        if entries.is_empty() {
            return Ok(JsonBatchSetOutcome::new(Vec::new(), None));
        }

        for entry in &entries {
            if let Some((document, _old_value)) = documents.get_mut(entry.id()) {
                // A document id already touched earlier in this batch is an
                // update from its second occurrence on, regardless of whether it
                // existed before the batch — the engine owns this classification.
                set_at_path(
                    document.value_mut(),
                    entry.path(),
                    entry.value().clone(),
                    true,
                )?;
                document.touch();
                results.push(JsonBatchSetItemOutcome::new(
                    document.document_version(),
                    false,
                ));
                continue;
            }
            let (document, old_value) = self.apply_set(
                &record,
                entry.id().clone(),
                entry.path(),
                entry.value().clone(),
                true,
            )?;
            // First occurrence in this batch: created only when no prior visible
            // document existed (`apply_set` returns `None` for the old value).
            let created = old_value.is_none();
            results.push(JsonBatchSetItemOutcome::new(
                document.document_version(),
                created,
            ));
            documents.insert(entry.id().clone(), (document, old_value));
        }

        let mut mutations = Vec::with_capacity(documents.len());
        for (id, (document, old_value)) in documents {
            mutations.push(RowMutation::put(
                self.row_address(&record, &id),
                encode_stored_document(&document)?,
            ));
            mutations.extend(self.index_mutations_for_change(
                &record,
                &id,
                old_value.as_ref(),
                Some(document.value()),
                &indexes,
            ));
        }

        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonBatchSetOutcome::new(results, Some(commit)))
    }

    /// Deletes multiple whole documents in one commit.
    pub fn batch_delete<I>(&mut self, ids: I) -> EngineResult<JsonBatchDeleteOutcome>
    where
        I: IntoIterator<Item = JsonDocumentId>,
    {
        let record = self.branch_record()?;
        let indexes = self.load_indexes(&record)?;
        let iterator = ids.into_iter();
        let mut seen = BTreeSet::new();
        let mut mutations = Vec::with_capacity(iterator.size_hint().0);
        let mut deleted = Vec::with_capacity(iterator.size_hint().0);
        for id in iterator {
            if !seen.insert(id.clone()) {
                return Err(EngineError::invalid_input(
                    "invalid_argument.engine.json_batch_duplicate_document",
                    "JSON batch contains duplicate document ids",
                ));
            }
            let address = self.row_address(&record, &id);
            let maybe_row = self
                .persistence
                .read_row(address.clone(), ReadSelector::Latest)?;
            let exists = maybe_row.as_ref().is_some_and(|row| !row.is_tombstone());
            if let Some(row) = maybe_row.filter(|row| !row.is_tombstone()) {
                let document = Self::document_from_row(&id, &row)?;
                mutations.push(RowMutation::delete(address));
                mutations.extend(self.index_mutations_for_change(
                    &record,
                    &id,
                    Some(document.value()),
                    None,
                    &indexes,
                ));
            }
            deleted.push(exists);
        }
        if deleted.is_empty() {
            return Ok(JsonBatchDeleteOutcome::new(Vec::new(), None));
        }
        if mutations.is_empty() {
            return Ok(JsonBatchDeleteOutcome::new(deleted, None));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonBatchDeleteOutcome::new(deleted, Some(commit)))
    }

    /// Deletes multiple JSON documents or paths in one commit.
    pub fn batch_delete_entries<I>(&mut self, entries: I) -> EngineResult<JsonBatchDeleteOutcome>
    where
        I: IntoIterator<Item = JsonGetEntry>,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(JsonBatchDeleteOutcome::new(Vec::new(), None));
        }

        let record = self.branch_record()?;
        let indexes = self.load_indexes(&record)?;
        let mut states: BTreeMap<JsonDocumentId, BatchDeleteState> = BTreeMap::new();
        let mut deleted = Vec::with_capacity(entries.len());

        for entry in &entries {
            if !states.contains_key(entry.id()) {
                let address = self.row_address(&record, entry.id());
                let state = match self.persistence.read_row(address, ReadSelector::Latest)? {
                    Some(row) if !row.is_tombstone() => {
                        let document = Self::document_from_row(entry.id(), &row)?;
                        BatchDeleteState::existing(document)
                    }
                    _ => BatchDeleteState::missing(),
                };
                states.insert(entry.id().clone(), state);
            }

            let state = states
                .get_mut(entry.id())
                .expect("state inserted before deletion");
            let item_deleted = state.delete(entry.path())?;
            deleted.push(item_deleted);
        }

        let mut mutations = Vec::with_capacity(states.len());
        for (id, state) in states {
            if !state.changed {
                continue;
            }
            let address = self.row_address(&record, &id);
            match &state.current {
                Some(document) => {
                    mutations.push(RowMutation::put(address, encode_stored_document(document)?));
                }
                None => mutations.push(RowMutation::delete(address)),
            }
            mutations.extend(self.index_mutations_for_change(
                &record,
                &id,
                state.original_value.as_ref(),
                state.current.as_ref().map(JsonDocument::value),
                &indexes,
            ));
        }

        if mutations.is_empty() {
            return Ok(JsonBatchDeleteOutcome::new(deleted, None));
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonBatchDeleteOutcome::new(deleted, Some(commit)))
    }

    /// Lists latest visible document ids by prefix.
    pub fn list(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        cursor: Option<&JsonDocumentId>,
        limit: usize,
    ) -> EngineResult<JsonListPage> {
        self.list_with_selector(prefix, cursor, limit, ReadSelector::Latest)
    }

    /// Lists document ids visible at a timestamp.
    pub fn list_at(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        cursor: Option<&JsonDocumentId>,
        limit: usize,
        timestamp: Timestamp,
    ) -> EngineResult<JsonListPage> {
        self.list_with_selector(prefix, cursor, limit, ReadSelector::AtTimestamp(timestamp))
    }

    /// Counts latest visible documents by prefix.
    pub fn count(&mut self, prefix: Option<&JsonDocumentId>) -> EngineResult<u64> {
        self.count_with_selector(prefix, ReadSelector::Latest)
    }

    /// Counts documents visible at a commit timestamp by prefix.
    pub fn count_at(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        timestamp: Timestamp,
    ) -> EngineResult<u64> {
        self.count_with_selector(prefix, ReadSelector::AtTimestamp(timestamp))
    }

    fn count_with_selector(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        selector: ReadSelector,
    ) -> EngineResult<u64> {
        let record = self.branch_record()?;
        let count = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Json,
                self.scan_prefix(prefix),
                selector,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    /// Samples latest visible documents by prefix.
    pub fn sample(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        count: usize,
    ) -> EngineResult<JsonSample> {
        let record = self.branch_record()?;
        let rows = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::Json,
                self.scan_prefix(prefix),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| self.sample_row_from_row(&row))
            .collect::<EngineResult<Vec<_>>>()?;
        let total_count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        if count == 0 || rows.is_empty() {
            return Ok(JsonSample::new(total_count, Vec::new()));
        }
        if count >= rows.len() {
            return Ok(JsonSample::new(total_count, rows));
        }
        let row_count = rows.len();
        let sampled = (0..count)
            .map(|index| rows[(index * row_count) / count].clone())
            .collect();
        Ok(JsonSample::new(total_count, sampled))
    }

    /// Scans latest visible documents from an inclusive start id.
    ///
    /// Mirrors the KV scan: the range is bounded to the space prefix, tombstones
    /// are skipped, and a `Some(limit)` request refills across tombstones so a
    /// caller that reads `limit + 1` rows can page honestly.
    pub fn scan(
        &mut self,
        start: Option<&JsonDocumentId>,
        limit: Option<usize>,
    ) -> EngineResult<Vec<JsonSampleRow>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let record = self.branch_record()?;
        let prefix = encode_json_space_prefix(&self.space);
        let start = start.map_or_else(|| prefix.clone(), |id| encode_json_key(&self.space, id));
        let end = next_prefix(&prefix);
        if start >= end {
            return Ok(Vec::new());
        }
        match limit {
            Some(limit) => self.scan_range_limited(&record, start, &end, limit),
            None => self
                .persistence
                .scan_range(
                    record.storage_branch_id(),
                    RowClass::Json,
                    Some(start),
                    Some(end),
                    ReadSelector::Latest,
                    None,
                )?
                .into_iter()
                .filter(|row| !row.is_tombstone())
                .map(|row| self.sample_row_from_row(&row))
                .collect(),
        }
    }

    /// Creates a JSON secondary index and backfills current documents.
    pub fn create_index(
        &mut self,
        name: JsonIndexName,
        field_path: super::JsonPath,
        index_type: JsonIndexType,
    ) -> EngineResult<JsonIndexDefinition> {
        let record = self.branch_record()?;
        let meta_address = self.index_meta_address(&record, &name);
        if self
            .persistence
            .read_row(meta_address.clone(), ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone())
        {
            return Err(EngineError::conflict(
                "already_exists.engine.json_index",
                "JSON index already exists",
            ));
        }
        let definition = JsonIndexDefinition::new(
            name.clone(),
            self.space.clone(),
            field_path,
            index_type,
            0,
            0,
        );
        let mut mutations = vec![RowMutation::put(
            meta_address,
            encode_index_definition(&definition)?,
        )];
        for row in self.document_rows(&record, ReadSelector::Latest)? {
            if row.is_tombstone() {
                continue;
            }
            let id = decode_json_document_id(&self.space, row.key())?;
            let document = Self::document_from_row(&id, &row)?;
            if let Some(index_value) = extract_index_value(document.value(), &definition) {
                mutations.push(RowMutation::put(
                    self.index_entry_address(&record, &name, &index_value, &id),
                    index_entry_value_bytes(),
                ));
            }
        }
        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonIndexDefinition::new(
            name,
            self.space.clone(),
            definition.field_path().clone(),
            index_type,
            commit.version().as_u64(),
            commit.timestamp().as_micros(),
        ))
    }

    /// Drops a JSON secondary index and its entries.
    pub fn drop_index(&mut self, name: &JsonIndexName) -> EngineResult<bool> {
        let record = self.branch_record()?;
        let meta_address = self.index_meta_address(&record, name);
        let exists = self
            .persistence
            .read_row(meta_address.clone(), ReadSelector::Latest)?
            .is_some_and(|row| !row.is_tombstone());
        if !exists {
            return Ok(false);
        }
        let mut mutations = vec![RowMutation::delete(meta_address)];
        for row in self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::JsonIndex,
                encode_json_index_entry_prefix(&self.space, name),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
        {
            mutations.push(RowMutation::delete(RowAddress::new(
                record.storage_branch_id(),
                RowClass::JsonIndex,
                row.key().to_vec(),
            )));
        }
        self.commit_batch(&record, mutations)?;
        Ok(true)
    }

    /// Lists JSON secondary index definitions.
    pub fn list_indexes(&mut self) -> EngineResult<Vec<JsonIndexDefinition>> {
        let record = self.branch_record()?;
        self.load_indexes(&record)
    }

    fn set_inner(
        &mut self,
        id: JsonDocumentId,
        path: &super::JsonPath,
        value: JsonValue,
        create_if_missing: bool,
    ) -> EngineResult<JsonWriteOutcome> {
        let record = self.branch_record()?;
        let indexes = self.load_indexes(&record)?;
        let (document, old_value) = self.apply_set(&record, id, path, value, create_if_missing)?;
        // `apply_set` returns `None` for the prior value only when it created the
        // document fresh; a present prior value means this write updated it.
        let created = old_value.is_none();
        let mut mutations = vec![RowMutation::put(
            self.row_address(&record, document.id()),
            encode_stored_document(&document)?,
        )];
        mutations.extend(self.index_mutations_for_change(
            &record,
            document.id(),
            old_value.as_ref(),
            Some(document.value()),
            &indexes,
        ));
        let commit = self.commit_batch(&record, mutations)?;
        Ok(JsonWriteOutcome::new(
            commit,
            document.document_version(),
            created,
        ))
    }

    fn apply_set(
        &mut self,
        record: &BranchCatalogRecord,
        id: JsonDocumentId,
        path: &super::JsonPath,
        value: JsonValue,
        create_if_missing: bool,
    ) -> EngineResult<(JsonDocument, Option<JsonValue>)> {
        let address = self.row_address(record, &id);
        match self.persistence.read_row(address, ReadSelector::Latest)? {
            Some(row) if !row.is_tombstone() => {
                let mut document = Self::document_from_row(&id, &row)?;
                let old_value = document.value().clone();
                set_at_path(document.value_mut(), path, value, true)?;
                document.touch();
                Ok((document, Some(old_value)))
            }
            _ if create_if_missing => {
                let mut document = JsonDocument::new(id, JsonValue::object());
                set_at_path(document.value_mut(), path, value, true)?;
                Ok((document, None))
            }
            _ => Err(EngineError::not_found(
                "not_found.engine.json_document",
                "JSON document does not exist",
            )),
        }
    }

    fn list_with_selector(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        cursor: Option<&JsonDocumentId>,
        limit: usize,
        selector: ReadSelector,
    ) -> EngineResult<JsonListPage> {
        if limit == 0 {
            return Ok(JsonListPage::new(Vec::new(), false, None));
        }
        let mut ids =
            self.scan_ids_after_cursor(prefix, cursor, limit.saturating_add(1), selector)?;
        let has_more = ids.len() > limit;
        if has_more {
            ids.truncate(limit);
        }
        let cursor = has_more.then(|| ids.last().expect("non-empty page").clone());
        Ok(JsonListPage::new(ids, has_more, cursor))
    }

    fn scan_ids_after_cursor(
        &mut self,
        prefix: Option<&JsonDocumentId>,
        cursor: Option<&JsonDocumentId>,
        limit: usize,
        selector: ReadSelector,
    ) -> EngineResult<Vec<JsonDocumentId>> {
        let record = self.branch_record()?;
        let prefix_start = self.scan_prefix(prefix);
        let prefix_end = next_prefix(&prefix_start);
        let mut start = cursor.map_or_else(
            || prefix_start.clone(),
            |cursor| encode_json_key(&self.space, cursor),
        );
        if start < prefix_start {
            start = prefix_start;
        }
        if start >= prefix_end {
            return Ok(Vec::new());
        }
        let mut ids = Vec::with_capacity(limit);
        while ids.len() < limit && start < prefix_end {
            let remaining = limit.saturating_sub(ids.len());
            let raw_limit = remaining
                .saturating_add(1)
                .clamp(JSON_LIST_RAW_PAGE_MIN, JSON_LIST_RAW_PAGE_MAX);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Json,
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
                let id = decode_json_document_id(&self.space, row.key())?;
                if cursor.is_some_and(|cursor| id.as_str() <= cursor.as_str()) {
                    continue;
                }
                if let Some(prefix) = prefix {
                    if !id.as_str().starts_with(prefix.as_str()) {
                        continue;
                    }
                }
                ids.push(id);
                if ids.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        Ok(ids)
    }

    fn scan_range_limited(
        &mut self,
        record: &BranchCatalogRecord,
        mut start: Vec<u8>,
        end: &[u8],
        limit: usize,
    ) -> EngineResult<Vec<JsonSampleRow>> {
        let mut visible = Vec::with_capacity(limit.min(JSON_LIST_RAW_PAGE_MIN));
        while visible.len() < limit && start.as_slice() < end {
            let remaining = limit.saturating_sub(visible.len());
            let raw_limit = remaining.clamp(JSON_LIST_RAW_PAGE_MIN, JSON_LIST_RAW_PAGE_MAX);
            let rows = self.persistence.scan_range(
                record.storage_branch_id(),
                RowClass::Json,
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
                visible.push(self.sample_row_from_row(row)?);
                if visible.len() >= limit {
                    break;
                }
            }
            let last_raw_key = rows.last().expect("non-empty raw page").key();
            start = exclusive_after_key(last_raw_key);
        }
        Ok(visible)
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

    fn row_address(&self, record: &BranchCatalogRecord, id: &JsonDocumentId) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::Json,
            encode_json_key(&self.space, id),
        )
    }

    fn index_meta_address(&self, record: &BranchCatalogRecord, name: &JsonIndexName) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::JsonIndex,
            encode_json_index_meta_key(&self.space, name),
        )
    }

    fn index_entry_address(
        &self,
        record: &BranchCatalogRecord,
        name: &JsonIndexName,
        encoded_value: &[u8],
        id: &JsonDocumentId,
    ) -> RowAddress {
        RowAddress::new(
            record.storage_branch_id(),
            RowClass::JsonIndex,
            encode_json_index_entry_key(&self.space, name, encoded_value, id),
        )
    }

    fn scan_prefix(&self, prefix: Option<&JsonDocumentId>) -> Vec<u8> {
        prefix.map_or_else(
            || encode_json_space_prefix(&self.space),
            |id| encode_json_key(&self.space, id),
        )
    }

    fn document_rows(
        &mut self,
        record: &BranchCatalogRecord,
        selector: ReadSelector,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.persistence.scan_prefix(
            record.storage_branch_id(),
            RowClass::Json,
            encode_json_space_prefix(&self.space),
            selector,
            None,
        )
    }

    fn load_indexes(
        &mut self,
        record: &BranchCatalogRecord,
    ) -> EngineResult<Vec<JsonIndexDefinition>> {
        self.persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::JsonIndex,
                encode_json_index_meta_prefix(&self.space),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .map(|row| {
                let _name = decode_json_index_name(&self.space, row.key())?;
                let value = row.value().ok_or_else(|| {
                    EngineError::corruption(
                        "data_loss.engine.json_index",
                        "stored JSON index metadata row is missing a value",
                    )
                })?;
                let definition = decode_index_definition(value)?;
                Ok(JsonIndexDefinition::new(
                    definition.name().clone(),
                    definition.space().clone(),
                    definition.field_path().clone(),
                    definition.index_type(),
                    row.commit_version().as_u64(),
                    row.commit_timestamp().as_micros(),
                ))
            })
            .collect()
    }

    fn index_mutations_for_change(
        &self,
        record: &BranchCatalogRecord,
        id: &JsonDocumentId,
        old: Option<&JsonValue>,
        new: Option<&JsonValue>,
        indexes: &[JsonIndexDefinition],
    ) -> Vec<RowMutation> {
        let mut mutations = Vec::new();
        for index in indexes {
            let old_value = old.and_then(|value| extract_index_value(value, index));
            let new_value = new.and_then(|value| extract_index_value(value, index));
            if old_value == new_value {
                continue;
            }
            if let Some(value) = old_value {
                mutations.push(RowMutation::delete(self.index_entry_address(
                    record,
                    index.name(),
                    &value,
                    id,
                )));
            }
            if let Some(value) = new_value {
                mutations.push(RowMutation::put(
                    self.index_entry_address(record, index.name(), &value, id),
                    index_entry_value_bytes(),
                ));
            }
        }
        mutations
    }

    fn document_from_row(
        id: &JsonDocumentId,
        row: &PersistenceReadRow,
    ) -> EngineResult<JsonDocument> {
        let value = row.value().ok_or_else(|| {
            EngineError::corruption(
                "data_loss.engine.json_document",
                "stored JSON document row is missing a value",
            )
        })?;
        decode_stored_document(id, value)
    }

    fn value_from_row(
        id: &JsonDocumentId,
        path: &super::JsonPath,
        row: &PersistenceReadRow,
    ) -> EngineResult<Option<JsonValue>> {
        if row.is_tombstone() {
            return Ok(None);
        }
        let document = Self::document_from_row(id, row)?;
        Ok(get_at_path(document.value(), path))
    }

    fn versioned_value_from_row(
        id: &JsonDocumentId,
        path: &super::JsonPath,
        row: &PersistenceReadRow,
    ) -> EngineResult<Option<JsonVersionedValue>> {
        if row.is_tombstone() {
            return Ok(None);
        }
        let document = Self::document_from_row(id, row)?;
        Ok(get_at_path(document.value(), path).map(|value| {
            JsonVersionedValue::new(
                value,
                row.commit_version(),
                row.commit_timestamp(),
                document.document_version(),
            )
        }))
    }

    fn history_row_from_row(
        id: &JsonDocumentId,
        row: &PersistenceReadRow,
    ) -> EngineResult<JsonHistoryRow> {
        if row.is_tombstone() {
            return Ok(JsonHistoryRow::new(
                None,
                true,
                row.commit_version(),
                row.commit_timestamp(),
                None,
            ));
        }
        let document = Self::document_from_row(id, row)?;
        Ok(JsonHistoryRow::new(
            Some(document.value().clone()),
            false,
            row.commit_version(),
            row.commit_timestamp(),
            Some(document.document_version()),
        ))
    }

    fn sample_row_from_row(&self, row: &PersistenceReadRow) -> EngineResult<JsonSampleRow> {
        let id = decode_json_document_id(&self.space, row.key())?;
        let document = Self::document_from_row(&id, row)?;
        Ok(JsonSampleRow::new(
            id,
            document.value().clone(),
            row.commit_version(),
            row.commit_timestamp(),
            document.document_version(),
        ))
    }

    fn commit_batch(
        &mut self,
        record: &BranchCatalogRecord,
        mutations: Vec<RowMutation>,
    ) -> EngineResult<CommitOutcome> {
        let mut mutations = mutations;
        if mutations.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.json_batch",
                "JSON batch must contain at least one mutation",
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
