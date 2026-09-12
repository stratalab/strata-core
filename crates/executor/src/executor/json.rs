use super::{
    delete_effect, empty_json_batch_get_results, empty_json_batch_results,
    empty_presence_exists_results, engine_json_index_type, finish_json_batch_get_results,
    finish_json_batch_results, finish_presence_exists_results, json_batch_get_batch_result,
    json_batch_get_failed, json_batch_get_result, json_batch_item_failed, json_batch_item_result,
    json_batch_result, json_delete_output, json_document_id, json_get_entry, json_history_items,
    json_index_definition, json_index_name, json_list_output, json_path, json_sample_item,
    json_sample_output, json_value, json_versioned_value, json_write_output,
    optional_json_document_id, optional_json_prefix, optional_limit, presence_exists_failed,
    presence_exists_item, presence_exists_result, reject_duplicate_json_targets, upsert_effect,
    usize_to_u64, BatchJsonDeleteEntry, BatchJsonEntry, BatchJsonGetEntry, Executor, ExecutorError,
    ExecutorResult, JsonIndexType, JsonSetEntry, MaybeJsonVersionedValue, Output, PageInfo,
    DEFAULT_JSON_LIST_LIMIT,
};

impl Executor {
    pub(super) fn execute_json_set(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: &str,
        path: &str,
        value: serde_json::Value,
    ) -> Result<Output, ExecutorError> {
        let id = json_document_id(key)?;
        let path = json_path(path)?;
        let value = json_value(value)?;
        let mut service = self.json_service(branch, space)?;
        let outcome = service.set_or_create(id, &path, value)?;
        Ok(json_write_output(
            key,
            upsert_effect(!outcome.created()),
            outcome.commit(),
        ))
    }

    pub(super) fn execute_json_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: &str,
        path: &str,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let id = json_document_id(key)?;
        let path = json_path(path)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.json_service(branch, space)?;
        // #3334: an as-of read answers with the same versioned record a live
        // read does, and says which commit answered it — one wire shape per
        // command, as every other point read already has.
        let value = match as_of {
            Some(as_of) => service.get_versioned_at(&id, &path, as_of)?,
            None => service.get_versioned(&id, &path)?,
        };
        Ok(Output::JsonVersionedValue(
            MaybeJsonVersionedValue::from_option(value.as_ref().map(json_versioned_value)),
        ))
    }

    pub(super) fn execute_json_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: &str,
        path: &str,
    ) -> Result<Output, ExecutorError> {
        let id = json_document_id(key)?;
        let path = json_path(path)?;
        let mut service = self.json_service(branch, space)?;
        let outcome = service.delete(id, &path)?;
        Ok(json_delete_output(key, outcome.deleted(), outcome.commit()))
    }

    pub(super) fn execute_json_history(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: String,
    ) -> Result<Output, ExecutorError> {
        let id = json_document_id(key)?;
        let mut service = self.json_service(branch, space)?;
        Ok(Output::JsonVersionHistory(
            service.get_versions(&id)?.as_ref().map(json_history_items),
        ))
    }

    pub(super) fn execute_json_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: String,
    ) -> Result<Output, ExecutorError> {
        let id = json_document_id(key)?;
        let mut service = self.json_service(branch, space)?;
        Ok(Output::Bool(service.exists(&id)?))
    }

    pub(super) fn execute_json_batch_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        keys: Vec<String>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.json_service(branch, space)?;
        if keys.is_empty() {
            return Ok(Output::JsonBatchExistsResults(presence_exists_result(
                Vec::new(),
            )));
        }
        let mut results = empty_presence_exists_results(keys.len());
        let mut valid_ids = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            match json_document_id(key) {
                Ok(id) => valid_ids.push((index, id)),
                Err(error) => {
                    results[index] = Some(presence_exists_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_ids.is_empty() {
            return Ok(Output::JsonBatchExistsResults(
                finish_presence_exists_results(results),
            ));
        }
        let ids = valid_ids
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>();
        let exists = service.batch_exists(&ids)?;
        for ((index, _), exists) in valid_ids.into_iter().zip(exists) {
            results[index] = Some(presence_exists_item(usize_to_u64(index), exists));
        }
        Ok(Output::JsonBatchExistsResults(
            finish_presence_exists_results(results),
        ))
    }

    pub(super) fn execute_json_batch_set(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        entries: Vec<BatchJsonEntry>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.json_service(branch, space)?;
        if entries.is_empty() {
            return Ok(Output::JsonBatchResults(json_batch_result(Vec::new())));
        }
        let mut results = empty_json_batch_results(entries.len());
        let mut valid_entries = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let (key, path, value) = entry.into_parts();
            let target = (key.clone(), path.clone());
            let validation: ExecutorResult<JsonSetEntry> = (|| {
                let id = json_document_id(key)?;
                let path = json_path(&path)?;
                let value = json_value(value)?;
                Ok(JsonSetEntry::new(id, path, value))
            })();
            match validation {
                Ok(entry) => valid_entries.push((index, entry, target)),
                Err(error) => {
                    results[index] = Some(json_batch_item_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_entries.is_empty() {
            return Ok(Output::JsonBatchResults(finish_json_batch_results(results)));
        }
        reject_duplicate_json_targets(
            valid_entries
                .iter()
                .map(|(_, _, (key, path))| (key.as_str(), path.as_str())),
        )?;
        let engine_entries = valid_entries
            .iter()
            .map(|(_, entry, _)| entry.clone())
            .collect::<Vec<_>>();
        // The engine owns create-vs-update per item, so the executor relays
        // `created` with no pre-read. Two items targeting the same document and
        // path are rejected above, matching KV's whole-batch duplicate rule.
        let outcome = service.batch_set_or_create(engine_entries)?;
        for ((index, _, _), item) in valid_entries.into_iter().zip(outcome.results()) {
            results[index] = Some(json_batch_item_result(
                usize_to_u64(index),
                upsert_effect(!item.created()),
                outcome.commit(),
                Some(item.document_version()),
            ));
        }
        Ok(Output::JsonBatchResults(finish_json_batch_results(results)))
    }

    pub(super) fn execute_json_batch_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        entries: Vec<BatchJsonGetEntry>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.json_service(branch, space)?;
        if entries.is_empty() {
            return Ok(Output::JsonBatchGetResults(json_batch_get_batch_result(
                Vec::new(),
            )));
        }
        let mut results = empty_json_batch_get_results(entries.len());
        let mut valid_entries = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let (key, path) = entry.into_parts();
            match json_get_entry(key, &path) {
                Ok(entry) => valid_entries.push((index, entry)),
                Err(error) => {
                    results[index] = Some(json_batch_get_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_entries.is_empty() {
            return Ok(Output::JsonBatchGetResults(finish_json_batch_get_results(
                results,
            )));
        }
        let engine_entries = valid_entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let values = service.batch_get(&engine_entries)?;
        for ((index, _), value) in valid_entries.into_iter().zip(values) {
            results[index] = Some(json_batch_get_result(usize_to_u64(index), value));
        }
        Ok(Output::JsonBatchGetResults(finish_json_batch_get_results(
            results,
        )))
    }

    pub(super) fn execute_json_batch_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        entries: Vec<BatchJsonDeleteEntry>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.json_service(branch, space)?;
        if entries.is_empty() {
            return Ok(Output::JsonBatchResults(json_batch_result(Vec::new())));
        }
        let mut results = empty_json_batch_results(entries.len());
        let mut valid_entries = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let (key, path) = entry.into_parts();
            let target = (key.clone(), path.clone());
            match json_get_entry(key, &path) {
                Ok(entry) => valid_entries.push((index, entry, target)),
                Err(error) => {
                    results[index] = Some(json_batch_item_failed(usize_to_u64(index), error));
                }
            }
        }
        if valid_entries.is_empty() {
            return Ok(Output::JsonBatchResults(finish_json_batch_results(results)));
        }
        reject_duplicate_json_targets(
            valid_entries
                .iter()
                .map(|(_, _, (key, path))| (key.as_str(), path.as_str())),
        )?;
        let engine_entries = valid_entries
            .iter()
            .map(|(_, entry, _)| entry.clone())
            .collect::<Vec<_>>();
        let outcome = service.batch_delete_entries(engine_entries)?;
        for ((index, _, _), deleted) in valid_entries
            .into_iter()
            .zip(outcome.deleted().iter().copied())
        {
            results[index] = Some(json_batch_item_result(
                usize_to_u64(index),
                delete_effect(deleted),
                deleted.then(|| outcome.commit()).flatten(),
                None,
            ));
        }
        Ok(Output::JsonBatchResults(finish_json_batch_results(results)))
    }

    pub(super) fn execute_json_list(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<String>,
        cursor: Option<String>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_json_prefix(prefix)?;
        let cursor = optional_json_document_id(cursor)?;
        let limit = optional_limit(limit)?.unwrap_or(DEFAULT_JSON_LIST_LIMIT);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.json_service(branch, space)?;
        let page = if let Some(as_of) = as_of {
            service.list_at(prefix.as_ref(), cursor.as_ref(), limit, as_of)?
        } else {
            service.list(prefix.as_ref(), cursor.as_ref(), limit)?
        };
        Ok(json_list_output(&page))
    }

    pub(super) fn execute_json_scan(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        start: Option<String>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let start = optional_json_document_id(start)?;
        let limit = optional_limit(limit)?;
        let mut service = self.json_service(branch, space)?;
        // Fetch one extra row to detect truncation and report has_more/cursor
        // honestly, like KvScan. The continuation cursor is the first unreturned
        // key; the inclusive start resumes with neither a gap nor an overlap.
        let mut rows = service.scan(start.as_ref(), limit.map(|limit| limit.saturating_add(1)))?;
        let page = match limit {
            Some(limit) if rows.len() > limit => {
                let cursor = rows[limit].document_id().as_str().to_owned();
                rows.truncate(limit);
                PageInfo::new(true, Some(cursor))
            }
            _ => PageInfo::terminal(),
        };
        Ok(Output::JsonScanResult {
            items: rows.iter().map(json_sample_item).collect(),
            page,
        })
    }

    pub(super) fn execute_json_count(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<String>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_json_prefix(prefix)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.json_service(branch, space)?;
        let count = if let Some(as_of) = as_of {
            service.count_at(prefix.as_ref(), as_of)?
        } else {
            service.count(prefix.as_ref())?
        };
        Ok(Output::Uint(count))
    }

    pub(super) fn execute_json_sample(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<String>,
        count: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_json_prefix(prefix)?;
        let count = optional_limit(count)?.unwrap_or(10);
        let mut service = self.json_service(branch, space)?;
        Ok(json_sample_output(&service.sample(prefix.as_ref(), count)?))
    }

    pub(super) fn execute_json_create_index(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        name: String,
        field_path: &str,
        index_type: JsonIndexType,
    ) -> Result<Output, ExecutorError> {
        let name = json_index_name(name)?;
        let field_path = json_path(field_path)?;
        let index_type = engine_json_index_type(index_type);
        let mut service = self.json_service(branch, space)?;
        let definition = service.create_index(name, field_path, index_type)?;
        Ok(Output::JsonIndexDefinition(json_index_definition(
            &definition,
        )))
    }

    pub(super) fn execute_json_drop_index(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        name: String,
    ) -> Result<Output, ExecutorError> {
        let name = json_index_name(name)?;
        let mut service = self.json_service(branch, space)?;
        Ok(Output::Bool(service.drop_index(&name)?))
    }

    pub(super) fn execute_json_list_indexes(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.json_service(branch, space)?;
        Ok(Output::JsonIndexList {
            items: service
                .list_indexes()?
                .iter()
                .map(json_index_definition)
                .collect(),
            page: PageInfo::terminal(),
        })
    }
}
