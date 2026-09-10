//! `arrow.*` stubs for builds without the `arrow` feature (the wasm
//! consumer): import still refuses a missing input file with the same typed
//! error as the real path, then every command returns one stable, typed
//! feature-disabled refusal.
//!
//! Kept in a file of its own so the mutation gate can exclude it by PATH:
//! the default-feature lane never compiles this cfg, so its mutants would
//! survive vacuously (their strength mechanism is `arrow_disabled_behavior`
//! in the no-arrow feature-matrix leg).

use super::admin_convert::arrow_feature_disabled;
use super::{
    ArrowExportPrimitive, ArrowFileFormat, ArrowImportTarget, Executor, ExecutorResult, Output,
};
use crate::ExecutorError;

impl Executor {
    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the arrow-enabled signature at the dispatch site"
    )]
    pub(super) fn execute_arrow_import(
        &mut self,
        _branch: Option<&str>,
        _space: Option<&str>,
        file_path: String,
        _format: Option<ArrowFileFormat>,
        _target: ArrowImportTarget,
        _key_column: Option<&str>,
        _value_column: Option<&str>,
        _collection: Option<&str>,
        _graph: Option<&str>,
    ) -> ExecutorResult<Output> {
        if !std::path::Path::new(&file_path).exists() {
            return Err(ExecutorError::new(
                "invalid_argument.executor.arrow_input_missing",
                format!("file not found: '{file_path}'"),
            ));
        }
        Err(arrow_feature_disabled())
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the arrow-enabled signature at the dispatch site"
    )]
    pub(super) fn execute_arrow_export(
        &mut self,
        _branch: Option<&str>,
        _space: Option<&str>,
        _primitive: ArrowExportPrimitive,
        _format: ArrowFileFormat,
        _path: String,
        _prefix: Option<&str>,
        _limit: Option<u64>,
        _collection: Option<String>,
        _graph: Option<String>,
        _event_type: Option<String>,
    ) -> ExecutorResult<Output> {
        Err(arrow_feature_disabled())
    }
}
