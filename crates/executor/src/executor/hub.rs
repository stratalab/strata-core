//! `hub.*` command handlers: thin dispatch over the `strata-hub`
//! orchestration layer (the executor stays transport-only; resolution,
//! download, verification, reconstitution, and origin recording live in
//! `strata_hub`).

#[cfg(feature = "hub")]
use std::path::PathBuf;

#[cfg(feature = "hub")]
use strata_hub::stratahub_protocol::{BranchName, DatasetName, ProblemDetails};
#[cfg(feature = "hub")]
use strata_hub::{
    clone_dataset, resolve_hub_url, ClientError, ClientTransport, CloneError, CloneProgress,
    CloneRequest, DatasetFilter, HubUrlError, HubUrlInputs, ListPageReq,
};
#[cfg(feature = "hub")]
use time::format_description::well_known::Rfc3339;
#[cfg(feature = "hub")]
use time::OffsetDateTime;

use super::{Executor, ExecutorResult, Output};
#[cfg(feature = "hub")]
use crate::types::{HubCloneProgress, HubCloneProgressStage, HubDatasetSort};
use crate::ExecutorError;

#[cfg(feature = "hub")]
impl Executor {
    // Clone never touches the session database, but the verb dispatches
    // through the executor like every hub.* command so all frontends share
    // one path (coordination §3.6).
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_clone(
        &mut self,
        dataset: &str,
        branch: Option<&str>,
        dest: &str,
        hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        self.execute_hub_clone_inner(dataset, branch, dest, hub_url, &mut |_progress| {})
    }

    /// Clones a hub dataset and reports machine-readable progress events.
    #[allow(clippy::unused_self)]
    pub fn execute_hub_clone_with_progress(
        &mut self,
        dataset: &str,
        branch: Option<&str>,
        dest: &str,
        hub_url: Option<String>,
        progress: &mut dyn FnMut(Output),
    ) -> ExecutorResult<Output> {
        self.execute_hub_clone_inner(dataset, branch, dest, hub_url, progress)
    }

    #[allow(clippy::unused_self)]
    fn execute_hub_clone_inner(
        &mut self,
        dataset: &str,
        branch: Option<&str>,
        dest: &str,
        hub_url: Option<String>,
        progress: &mut dyn FnMut(Output),
    ) -> ExecutorResult<Output> {
        let transport = hub_transport(hub_url)?;
        let dataset = DatasetName::parse(dataset).map_err(|error| {
            ExecutorError::new(
                "invalid_argument.executor.hub_dataset",
                format!("dataset name is invalid: {error}"),
            )
        })?;
        let branch = branch.map(BranchName::parse).transpose().map_err(|error| {
            ExecutorError::new(
                "invalid_argument.executor.hub_branch",
                format!("branch name is invalid: {error}"),
            )
        })?;
        let dataset_text = dataset.as_str().to_owned();

        let outcome = clone_dataset(
            &transport,
            &CloneRequest {
                dataset: dataset.clone(),
                branch,
                dest: PathBuf::from(dest),
            },
            &mut |event| progress(clone_progress_output(&dataset_text, event)),
        )
        .map_err(|error| clone_error(&error))?;

        Ok(Output::HubCloneResult(crate::types::HubCloneResult {
            dataset: dataset.as_str().to_owned(),
            branch: outcome.branch.as_str().to_owned(),
            dest: dest.to_owned(),
            manifest_hash: outcome.manifest_hash.as_str().to_owned(),
            object_count: outcome.object_count,
            total_bytes: outcome.total_bytes,
        }))
    }

    /// Reads the hub capability advertisement.
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_info(&mut self, hub_url: Option<String>) -> ExecutorResult<Output> {
        let transport = hub_transport(hub_url)?;
        transport
            .info()
            .map_err(|error| hub_client_error(&error, "invalid_argument.executor.hub_filter", None))
            .and_then(TryInto::try_into)
            .map(Output::HubInfo)
    }

    /// Lists datasets from the resolved hub.
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_list_datasets(
        &mut self,
        hub_url: Option<String>,
        tasks: Vec<String>,
        tags: Vec<String>,
        primitives: Vec<String>,
        license: Option<String>,
        size_min_bytes: Option<u64>,
        size_max_bytes: Option<u64>,
        sort: Option<HubDatasetSort>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> ExecutorResult<Output> {
        validate_dataset_list_query(size_min_bytes, size_max_bytes, limit)?;
        let transport = hub_transport(hub_url)?;
        let filter = DatasetFilter {
            tasks,
            tags,
            primitives,
            license,
            size_min_bytes,
            size_max_bytes,
            sort: sort.map(Into::into),
        };
        let page = ListPageReq { limit, offset };
        transport
            .list_datasets(&filter, page)
            .map_err(|error| hub_client_error(&error, "invalid_argument.executor.hub_filter", None))
            .and_then(TryInto::try_into)
            .map(Output::HubDatasets)
    }

    /// Reads one full dataset card from the resolved hub.
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_get_dataset(
        &mut self,
        name: &str,
        hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        let dataset = parse_dataset(name)?;
        let transport = hub_transport(hub_url)?;
        transport
            .get_dataset(&dataset)
            .map_err(|error| {
                hub_client_error(
                    &error,
                    "invalid_argument.executor.hub_filter",
                    Some("not_found.executor.hub_dataset"),
                )
            })
            .and_then(TryInto::try_into)
            .map(Output::HubDataset)
    }

    /// Lists live refs for one hub dataset.
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_list_refs(
        &mut self,
        dataset: &str,
        hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        let dataset = parse_dataset(dataset)?;
        let transport = hub_transport(hub_url)?;
        transport
            .list_refs(&dataset)
            .map_err(|error| {
                hub_client_error(
                    &error,
                    "invalid_argument.executor.hub_filter",
                    Some("not_found.executor.hub_dataset"),
                )
            })
            .and_then(TryInto::try_into)
            .map(Output::HubRefs)
    }

    /// Lists yanked refs from the resolved hub.
    #[allow(clippy::unused_self)]
    pub(super) fn execute_hub_list_yanked(
        &mut self,
        since: Option<&str>,
        hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        let since = since.map(parse_since).transpose()?;
        let transport = hub_transport(hub_url)?;
        transport
            .yanked(since)
            .map_err(|error| hub_client_error(&error, "invalid_argument.executor.hub_since", None))
            .and_then(TryInto::try_into)
            .map(Output::HubYanked)
    }
}

#[cfg(feature = "hub")]
fn hub_transport(hub_url: Option<String>) -> ExecutorResult<ClientTransport> {
    let resolved = resolve_hub_url(&HubUrlInputs::from_process(hub_url))
        .map_err(|error| hub_url_error(&error))?;
    ClientTransport::new(resolved.url).map_err(|error| clone_error(&error))
}

#[cfg(feature = "hub")]
fn parse_dataset(dataset: &str) -> ExecutorResult<DatasetName> {
    DatasetName::parse(dataset).map_err(|error| {
        ExecutorError::new(
            "invalid_argument.executor.hub_dataset",
            format!("dataset name is invalid: {error}"),
        )
    })
}

#[cfg(feature = "hub")]
fn parse_since(since: &str) -> ExecutorResult<OffsetDateTime> {
    OffsetDateTime::parse(since, &Rfc3339).map_err(|error| {
        ExecutorError::new(
            "invalid_argument.executor.hub_since",
            format!("since timestamp must be RFC 3339: {error}"),
        )
    })
}

#[cfg(feature = "hub")]
fn validate_dataset_list_query(
    size_min_bytes: Option<u64>,
    size_max_bytes: Option<u64>,
    limit: Option<u32>,
) -> ExecutorResult<()> {
    if matches!(limit, Some(0 | 201..)) {
        return Err(ExecutorError::new(
            "invalid_argument.executor.hub_filter",
            "hub dataset list limit must be in the range 1..=200",
        ));
    }
    if let (Some(min), Some(max)) = (size_min_bytes, size_max_bytes) {
        if min > max {
            return Err(ExecutorError::new(
                "invalid_argument.executor.hub_filter",
                "hub dataset size_min_bytes must be less than or equal to size_max_bytes",
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "hub")]
fn clone_progress_output(dataset: &str, progress: CloneProgress) -> Output {
    let mut event = HubCloneProgress {
        stage: HubCloneProgressStage::Done,
        dataset: dataset.to_owned(),
        branch: None,
        manifest_hash: None,
        object_count: None,
        total_bytes: None,
        index: None,
        bytes: None,
    };
    match progress {
        CloneProgress::Resolved {
            branch,
            manifest_hash,
        } => {
            event.stage = HubCloneProgressStage::Resolved;
            event.branch = Some(branch);
            event.manifest_hash = Some(manifest_hash);
        }
        CloneProgress::ManifestFetched {
            object_count,
            total_bytes,
        } => {
            event.stage = HubCloneProgressStage::ManifestFetched;
            event.object_count = Some(object_count);
            event.total_bytes = Some(total_bytes);
        }
        CloneProgress::ObjectFetched {
            index,
            object_count,
            bytes,
        } => {
            event.stage = HubCloneProgressStage::ObjectFetched;
            event.index = Some(index);
            event.object_count = Some(object_count);
            event.bytes = Some(bytes);
        }
        CloneProgress::Importing => {
            event.stage = HubCloneProgressStage::Importing;
        }
        CloneProgress::Done => {}
        _ => {
            event.stage = HubCloneProgressStage::Unknown;
        }
    }
    Output::HubCloneProgress(event)
}

#[cfg(feature = "hub")]
fn hub_url_error(error: &HubUrlError) -> ExecutorError {
    // The only reachable HubUrlError is a malformed URL string — caller
    // input, not a precondition on well-formed input, so invalid_argument.
    ExecutorError::new("invalid_argument.executor.hub_url", error.to_string())
}

#[cfg(feature = "hub")]
fn clone_error(error: &CloneError) -> ExecutorError {
    match error {
        CloneError::Transport { .. } => {
            ExecutorError::new("unavailable.executor.hub_transport", error.to_string())
        }
        _ => ExecutorError::new("failed_precondition.executor.hub_clone", error.to_string()),
    }
}

#[cfg(feature = "hub")]
fn hub_client_error(
    error: &ClientError,
    bad_request_code: &'static str,
    not_found_code: Option<&'static str>,
) -> ExecutorError {
    match error {
        ClientError::BadRequest { problem } => {
            ExecutorError::new(bad_request_code, hub_problem_message(problem))
        }
        ClientError::NotFound { problem } => ExecutorError::new(
            not_found_code.unwrap_or("not_found.executor.hub_resource"),
            hub_problem_message(problem),
        ),
        _ => ExecutorError::new("unavailable.executor.hub_transport", error.to_string()),
    }
}

#[cfg(feature = "hub")]
fn hub_problem_message(problem: &ProblemDetails) -> String {
    let detail = problem.detail.as_deref().unwrap_or(problem.title.as_str());
    format!("hub returned {}: {detail}", problem.error_code.as_str())
}
