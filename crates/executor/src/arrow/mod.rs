//! Arrow import/export helpers for the executor command boundary.

pub(crate) mod export;
pub(crate) mod format;
pub(crate) mod import;
pub(crate) mod reader;
pub(crate) mod schema;
pub(crate) mod writer;

use crate::error::ExecutorError;

fn invalid_input(code: &'static str, message: impl Into<String>) -> ExecutorError {
    ExecutorError::new(code, message)
}

fn not_found(code: &'static str, message: impl Into<String>) -> ExecutorError {
    ExecutorError::new(code, message)
}

fn io_error(message: impl Into<String>) -> ExecutorError {
    ExecutorError::new("unavailable.executor.arrow_io", message)
}

fn internal_error(message: impl Into<String>) -> ExecutorError {
    ExecutorError::new("internal.executor.arrow", message)
}

fn unexpected_output(command: &'static str) -> ExecutorError {
    internal_error(format!("unexpected output for {command}"))
}

fn required_option<T>(
    value: Option<T>,
    code: &'static str,
    message: &'static str,
) -> Result<T, ExecutorError> {
    value.ok_or_else(|| invalid_input(code, message))
}
