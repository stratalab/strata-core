//! `hub.*` stubs for builds without the `hub` feature (the wasm consumer):
//! every command returns one stable, typed feature-disabled refusal.
//!
//! Kept in a file of its own so the mutation gate can exclude it by PATH:
//! the default-feature lane never compiles this cfg, so its mutants would
//! survive vacuously (their strength mechanism is `hub_disabled_behavior`
//! in the no-default-features lane).

use super::{Executor, ExecutorResult, Output};
use crate::ExecutorError;

impl Executor {
    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the hub-enabled signature at the dispatch site"
    )]
    pub(super) fn execute_hub_clone(
        &mut self,
        _dataset: &str,
        _branch: Option<&str>,
        _dest: &str,
        _hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the hub-enabled signature for external callers"
    )]
    /// Clones a hub dataset and reports machine-readable progress events.
    pub fn execute_hub_clone_with_progress(
        &mut self,
        _dataset: &str,
        _branch: Option<&str>,
        _dest: &str,
        _hub_url: Option<String>,
        _progress: &mut dyn FnMut(Output),
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub(super) fn execute_hub_info(&mut self, _hub_url: Option<String>) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub(super) fn execute_hub_list_datasets(
        &mut self,
        _hub_url: Option<String>,
        _tasks: Vec<String>,
        _tags: Vec<String>,
        _primitives: Vec<String>,
        _license: Option<String>,
        _size_min_bytes: Option<u64>,
        _size_max_bytes: Option<u64>,
        _sort: Option<crate::types::HubDatasetSort>,
        _limit: Option<u32>,
        _offset: Option<u32>,
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub(super) fn execute_hub_get_dataset(
        &mut self,
        _name: &str,
        _hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub(super) fn execute_hub_list_refs(
        &mut self,
        _dataset: &str,
        _hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }

    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    pub(super) fn execute_hub_list_yanked(
        &mut self,
        _since: Option<&str>,
        _hub_url: Option<String>,
    ) -> ExecutorResult<Output> {
        Err(hub_feature_disabled())
    }
}

pub(super) fn hub_feature_disabled() -> ExecutorError {
    ExecutorError::new(
        "unsupported.executor.hub_feature_disabled",
        "hub commands require the executor hub feature",
    )
}
