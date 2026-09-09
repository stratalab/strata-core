use super::{
    ArrowExportPrimitive, ArrowFileFormat, ArrowImportTarget, Executor, ExecutorResult, Output,
};

#[cfg(not(feature = "arrow"))]
use super::{admin_convert::arrow_feature_disabled, ExecutorError};

impl Executor {
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "arrow")]
    pub(super) fn execute_arrow_import(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        file_path: String,
        format: Option<ArrowFileFormat>,
        target: ArrowImportTarget,
        key_column: Option<&str>,
        value_column: Option<&str>,
        collection: Option<&str>,
        graph: Option<&str>,
    ) -> ExecutorResult<Output> {
        crate::arrow::import::import_file(
            self,
            branch,
            space,
            file_path,
            format,
            target,
            key_column,
            value_column,
            collection,
            graph,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the arrow-enabled signature at the dispatch site"
    )]
    #[cfg(not(feature = "arrow"))]
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
    #[cfg(feature = "arrow")]
    pub(super) fn execute_arrow_export(
        &mut self,
        branch: Option<&str>,
        space: Option<&str>,
        primitive: ArrowExportPrimitive,
        format: ArrowFileFormat,
        path: String,
        prefix: Option<&str>,
        limit: Option<u64>,
        collection: Option<String>,
        graph: Option<String>,
        event_type: Option<String>,
    ) -> ExecutorResult<Output> {
        crate::arrow::export::export_file(
            self, branch, space, primitive, format, path, prefix, limit, collection, graph,
            event_type,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::unused_self,
        clippy::needless_pass_by_value,
        reason = "stub mirrors the arrow-enabled signature at the dispatch site"
    )]
    #[cfg(not(feature = "arrow"))]
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
