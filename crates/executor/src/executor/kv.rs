use super::{
    batch_exists_failed, batch_exists_item, batch_get_failed, batch_get_result, batch_item_failed,
    batch_item_result, bytes_from_key, delete_effect, delete_output, empty_batch_exists_results,
    empty_batch_get_results, empty_batch_results, finish_batch_exists_results,
    finish_batch_get_results, finish_batch_results, history_result, kv_batch_exists_result,
    kv_batch_get_result, kv_batch_result, kv_key, kv_value, optional_key, optional_limit,
    reject_duplicate_valid_keys, sample_output, scan_item, upsert_effect, usize_to_u64,
    versioned_value, write_output, BatchKvEntry, Bytes, Executor, ExecutorError, Maybe, Output,
    PageInfo,
};

impl Executor {
    pub(super) fn execute_kv_put(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: Bytes,
        value: Bytes,
    ) -> Result<Output, ExecutorError> {
        let output_key = key.clone();
        let key = kv_key(key)?;
        let mut service = self.kv_service(branch, space)?;
        let outcome = service.put(key, kv_value(value))?;
        Ok(write_output(
            output_key,
            upsert_effect(!outcome.created()),
            outcome.commit(),
        ))
    }

    pub(super) fn execute_kv_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: Bytes,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let key = kv_key(key)?;
        // #3112 S3a: a wall-clock instant is resolved to a logical timestamp
        // BEFORE the read, so both forms run the identical as-of path below.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.kv_service(branch, space)?;
        if let Some(as_of) = as_of {
            return Ok(Output::KvVersionedValue(Maybe::from_option(
                service
                    .get_versioned_at(&key, as_of)?
                    .as_ref()
                    .map(versioned_value),
            )));
        }
        Ok(Output::KvVersionedValue(Maybe::from_option(
            service.get_versioned(&key)?.as_ref().map(versioned_value),
        )))
    }

    pub(super) fn execute_kv_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: Bytes,
    ) -> Result<Output, ExecutorError> {
        let output_key = key.clone();
        let key = kv_key(key)?;
        let mut service = self.kv_service(branch, space)?;
        let outcome = service.delete(key)?;
        Ok(delete_output(
            output_key,
            outcome.deleted(),
            outcome.commit(),
        ))
    }

    pub(super) fn execute_kv_list(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<Bytes>,
        cursor: Option<Bytes>,
        limit: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_key(prefix)?;
        let cursor = optional_key(cursor)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.kv_service(branch, space)?;
        // A bare list (no cursor, no limit) returns every key; any pagination
        // request routes through the engine's page methods so the executor
        // never materializes the whole keyset or does cursor math itself.
        if cursor.is_none() && limit.is_none() {
            let keys = match as_of {
                Some(as_of) => service.list_at(prefix.as_ref(), as_of)?,
                None => service.list(prefix.as_ref())?,
            };
            return Ok(Output::KeysPage {
                items: keys.iter().map(bytes_from_key).collect(),
                page: PageInfo::terminal(),
            });
        }
        let limit = optional_limit(limit)?.unwrap_or(usize::MAX);
        let page = match as_of {
            Some(as_of) => service.list_at_page(prefix.as_ref(), cursor.as_ref(), limit, as_of)?,
            None => service.list_page(prefix.as_ref(), cursor.as_ref(), limit)?,
        };
        Ok(Output::KeysPage {
            items: page.keys().iter().map(bytes_from_key).collect(),
            page: PageInfo::new(page.has_more(), page.cursor().map(bytes_from_key)),
        })
    }

    pub(super) fn execute_kv_scan(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        start: Option<Bytes>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let start = optional_key(start)?;
        let limit = optional_limit(limit)?;
        let mut service = self.kv_service(branch, space)?;
        // Fetch one extra row to detect truncation and report has_more/cursor
        // honestly, like KvList (DSGN-2). The continuation cursor is the first
        // unreturned key; KvScan's start is inclusive, so re-scanning from it
        // resumes with neither a gap nor an overlap.
        let mut rows = service.scan(start.as_ref(), limit.map(|limit| limit.saturating_add(1)))?;
        let page = match limit {
            Some(limit) if rows.len() > limit => {
                let cursor = bytes_from_key(rows[limit].key());
                rows.truncate(limit);
                PageInfo::new(true, Some(cursor))
            }
            _ => PageInfo::terminal(),
        };
        Ok(Output::KvScanResult {
            items: rows.iter().map(scan_item).collect(),
            page,
        })
    }

    pub(super) fn execute_kv_batch_put(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        entries: Vec<BatchKvEntry>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.kv_service(branch, space)?;
        if entries.is_empty() {
            return Ok(Output::BatchResults(kv_batch_result(Vec::new())));
        }
        let mut results = empty_batch_results(entries.len());
        let mut valid_entries = Vec::with_capacity(entries.len());
        for (index, entry) in entries.into_iter().enumerate() {
            let output_key = entry.key().clone();
            let (key, value) = entry.into_parts();
            match kv_key(key) {
                Ok(key) => valid_entries.push((index, output_key, key, kv_value(value))),
                Err(error) => {
                    results[index] =
                        Some(batch_item_failed(usize_to_u64(index), output_key, error));
                }
            }
        }
        reject_duplicate_valid_keys(valid_entries.iter().map(|(_, key, ..)| key))?;
        if valid_entries.is_empty() {
            return Ok(Output::BatchResults(finish_batch_results(results)));
        }
        let engine_entries = valid_entries
            .iter()
            .map(|(_, _, key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let outcome = service.put_batch(engine_entries)?;
        for ((index, output_key, ..), created) in valid_entries
            .into_iter()
            .zip(outcome.created().iter().copied())
        {
            results[index] = Some(batch_item_result(
                usize_to_u64(index),
                output_key,
                upsert_effect(!created),
                Some(outcome.commit()),
            ));
        }
        Ok(Output::BatchResults(finish_batch_results(results)))
    }

    pub(super) fn execute_kv_batch_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        keys: Vec<Bytes>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.kv_service(branch, space)?;
        if keys.is_empty() {
            return Ok(Output::BatchGetResults(kv_batch_get_result(Vec::new())));
        }
        let mut results = empty_batch_get_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            let output_key = key.clone();
            match kv_key(key) {
                Ok(key) => valid_keys.push((index, output_key, key)),
                Err(error) => {
                    results[index] = Some(batch_get_failed(usize_to_u64(index), output_key, error));
                }
            }
        }
        if valid_keys.is_empty() {
            return Ok(Output::BatchGetResults(finish_batch_get_results(results)));
        }
        let keys = valid_keys
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect::<Vec<_>>();
        let values = service.batch_get(&keys)?;
        for ((index, output_key, _), value) in valid_keys.into_iter().zip(values) {
            results[index] = Some(batch_get_result(usize_to_u64(index), output_key, value));
        }
        Ok(Output::BatchGetResults(finish_batch_get_results(results)))
    }

    pub(super) fn execute_kv_batch_delete(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        keys: Vec<Bytes>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.kv_service(branch, space)?;
        if keys.is_empty() {
            return Ok(Output::BatchResults(kv_batch_result(Vec::new())));
        }
        let mut results = empty_batch_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            let output_key = key.clone();
            match kv_key(key) {
                Ok(key) => valid_keys.push((index, output_key, key)),
                Err(error) => {
                    results[index] =
                        Some(batch_item_failed(usize_to_u64(index), output_key, error));
                }
            }
        }
        reject_duplicate_valid_keys(valid_keys.iter().map(|(_, key, _)| key))?;
        if valid_keys.is_empty() {
            return Ok(Output::BatchResults(finish_batch_results(results)));
        }
        let engine_keys = valid_keys
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect::<Vec<_>>();
        let outcome = service.delete_batch(engine_keys)?;
        for ((index, output_key, _), deleted) in valid_keys
            .into_iter()
            .zip(outcome.deleted().iter().copied())
        {
            let commit = deleted.then(|| outcome.commit()).flatten();
            results[index] = Some(batch_item_result(
                usize_to_u64(index),
                output_key,
                delete_effect(deleted),
                commit,
            ));
        }
        Ok(Output::BatchResults(finish_batch_results(results)))
    }

    pub(super) fn execute_kv_batch_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        keys: Vec<Bytes>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.kv_service(branch, space)?;
        if keys.is_empty() {
            return Ok(Output::BatchExistsResults(kv_batch_exists_result(
                Vec::new(),
            )));
        }
        let mut results = empty_batch_exists_results(keys.len());
        let mut valid_keys = Vec::with_capacity(keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            let output_key = key.clone();
            match kv_key(key) {
                Ok(key) => valid_keys.push((index, output_key, key)),
                Err(error) => {
                    results[index] =
                        Some(batch_exists_failed(usize_to_u64(index), output_key, error));
                }
            }
        }
        if valid_keys.is_empty() {
            return Ok(Output::BatchExistsResults(finish_batch_exists_results(
                results,
            )));
        }
        let keys = valid_keys
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect::<Vec<_>>();
        let exists = service.batch_exists(&keys)?;
        for ((index, output_key, _), exists) in valid_keys.into_iter().zip(exists) {
            results[index] = Some(batch_exists_item(usize_to_u64(index), output_key, exists));
        }
        Ok(Output::BatchExistsResults(finish_batch_exists_results(
            results,
        )))
    }

    pub(super) fn execute_kv_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: Bytes,
    ) -> Result<Output, ExecutorError> {
        let key = kv_key(key)?;
        let mut service = self.kv_service(branch, space)?;
        Ok(Output::Bool(service.exists(&key)?))
    }

    pub(super) fn execute_kv_history(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        key: Bytes,
    ) -> Result<Output, ExecutorError> {
        let key = kv_key(key)?;
        let mut service = self.kv_service(branch, space)?;
        Ok(Output::VersionHistory(
            service.get_versions(&key)?.as_ref().map(history_result),
        ))
    }

    pub(super) fn execute_kv_count(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<Bytes>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_key(prefix)?;
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.kv_service(branch, space)?;
        let count = if let Some(as_of) = as_of {
            service.count_at(prefix.as_ref(), as_of)?
        } else {
            service.count(prefix.as_ref())?
        };
        Ok(Output::Uint(count))
    }

    pub(super) fn execute_kv_sample(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        prefix: Option<Bytes>,
        count: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let prefix = optional_key(prefix)?;
        let count = optional_limit(count)?.unwrap_or(10);
        let mut service = self.kv_service(branch, space)?;
        Ok(sample_output(&service.sample(prefix.as_ref(), count)?))
    }
}
