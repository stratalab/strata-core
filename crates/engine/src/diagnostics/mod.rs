//! Engine diagnostics and error vocabulary.

mod error;
mod registry;

pub(crate) use error::{size_limit_details, with_size_suffix};
pub use error::{
    CommitOutcomeStatus, EngineError, EngineErrorClass, EngineErrorStatus, EngineResult,
    ErrorClass, ErrorDetail, RetryPolicy,
};
#[cfg(test)]
pub(crate) use error::{ACTUAL_BYTES_DETAIL, LIMIT_BYTES_DETAIL};
pub use registry::{
    error_code_registry_entries, error_code_registry_entry, ErrorCodeRegistryEntry,
};
