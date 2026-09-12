//! `arrow.*` handlers for builds with the `arrow` feature; the cfg-disabled
//! twins live in `arrow_disabled.rs`.

use super::{
    ArrowExportPrimitive, ArrowFileFormat, ArrowImportTarget, Executor, ExecutorError, Output,
};

impl Executor {
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Output, ExecutorError> {
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
    ) -> Result<Output, ExecutorError> {
        crate::arrow::export::export_file(
            self, branch, space, primitive, format, path, prefix, limit, collection, graph,
            event_type,
        )
    }
}
