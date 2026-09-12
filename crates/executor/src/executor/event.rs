use super::{
    engine_event_direction, engine_event_type, event_append_output, event_batch_append_item_result,
    event_batch_entry, event_batch_result, event_chain_verification, event_payload,
    event_range_output, event_records, event_sequence, event_versioned_data,
    optional_engine_event_type, optional_limit, usize_to_u64, BatchEventEntry, EventRangeDirection,
    Executor, ExecutorError, Maybe, Output, PageInfo, Timestamp,
};

impl Executor {
    pub(super) fn execute_event_batch_append(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        entries: Vec<BatchEventEntry>,
    ) -> Result<Output, ExecutorError> {
        let entries = entries
            .into_iter()
            .map(event_batch_entry)
            .collect::<Vec<_>>();
        let mut service = self.event_service(branch, space)?;
        let outcome = service.batch_append(entries)?;
        let commit = outcome.commit();
        Ok(Output::EventBatchAppendResults(event_batch_result(
            outcome
                .items()
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    event_batch_append_item_result(usize_to_u64(index), item, commit)
                })
                .collect(),
        )))
    }

    pub(super) fn execute_event_append(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        event_type: String,
        payload: serde_json::Value,
    ) -> Result<Output, ExecutorError> {
        let event_type = engine_event_type(event_type)?;
        let payload = event_payload(payload)?;
        let mut service = self.event_service(branch, space)?;
        Ok(event_append_output(&service.append(event_type, payload)?))
    }

    pub(super) fn execute_event_get(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        sequence: u64,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let sequence = event_sequence(sequence);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.event_service(branch, space)?;
        let record = if let Some(as_of) = as_of {
            service.get_at(sequence, as_of)?
        } else {
            service.get(sequence)?
        };
        Ok(Output::EventRecord(Maybe::from_option(
            record.as_ref().map(event_versioned_data),
        )))
    }

    pub(super) fn execute_event_exists(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        sequence: u64,
    ) -> Result<Output, ExecutorError> {
        let sequence = event_sequence(sequence);
        let mut service = self.event_service(branch, space)?;
        Ok(Output::Bool(service.exists(sequence)?))
    }

    pub(super) fn execute_event_count(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.event_service(branch, space)?;
        let count = if let Some(as_of) = as_of {
            service.len_at(as_of)?.count()
        } else {
            service.len()?.count()
        };
        Ok(Output::EventCount { count })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_event_range(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        start_seq: u64,
        end_seq: Option<u64>,
        limit: Option<u64>,
        direction: EventRangeDirection,
        event_type: Option<String>,
    ) -> Result<Output, ExecutorError> {
        let end_seq = end_seq.map(event_sequence);
        let limit = optional_limit(limit)?;
        let direction = engine_event_direction(direction);
        let event_type = optional_engine_event_type(event_type)?;
        let mut service = self.event_service(branch, space)?;
        let page = service.range(
            event_sequence(start_seq),
            end_seq,
            limit,
            direction,
            event_type.as_ref(),
        )?;
        Ok(event_range_output(&page))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_event_range_by_time(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        start_ts: u64,
        end_ts: Option<u64>,
        limit: Option<u64>,
        direction: EventRangeDirection,
        event_type: Option<String>,
    ) -> Result<Output, ExecutorError> {
        let end_ts = end_ts.map(Timestamp::from_micros);
        let limit = optional_limit(limit)?;
        let direction = engine_event_direction(direction);
        let event_type = optional_engine_event_type(event_type)?;
        let mut service = self.event_service(branch, space)?;
        let page = service.range_by_time(
            Timestamp::from_micros(start_ts),
            end_ts,
            limit,
            direction,
            event_type.as_ref(),
        )?;
        Ok(event_range_output(&page))
    }

    pub(super) fn execute_event_list_types(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.event_service(branch, space)?;
        let types = if let Some(as_of) = as_of {
            service.list_types_at(as_of)?
        } else {
            service.list_types()?
        };
        Ok(Output::EventTypeList {
            items: types
                .event_types()
                .iter()
                .map(|event_type| event_type.as_str().to_owned())
                .collect(),
            page: PageInfo::terminal(),
        })
    }

    pub(super) fn execute_event_list(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        event_type: Option<String>,
        limit: Option<u64>,
        after_sequence: Option<u64>,
        as_of: Option<u64>,
        as_of_time: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        let event_type = optional_engine_event_type(event_type)?;
        let limit = optional_limit(limit)?;
        let after_sequence = after_sequence.map(event_sequence);
        // #3112 S3b: resolve any wall-clock instant to a logical timestamp
        // BEFORE the service borrow, so both forms run the identical as-of path.
        let as_of = self.resolve_as_of(branch, as_of, as_of_time)?;
        let mut service = self.event_service(branch, space)?;
        let page = service.list_page(event_type.as_ref(), after_sequence, limit, as_of)?;
        Ok(Output::EventRecords {
            items: event_records(page.events()),
            page: PageInfo::new(
                page.has_more(),
                page.cursor().map(strata_engine::EventSequence::as_u64),
            ),
        })
    }

    pub(super) fn execute_event_verify_chain(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
    ) -> Result<Output, ExecutorError> {
        let mut service = self.event_service(branch, space)?;
        Ok(Output::EventChainVerification(event_chain_verification(
            &service.verify_chain()?,
        )))
    }
}
