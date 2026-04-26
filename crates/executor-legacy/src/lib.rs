//! Transitional bootstrap shell retained while the new executor still borrows
//! the legacy open policy.

mod bootstrap;
mod error;

pub use bootstrap::Strata;
pub use error::Error;

pub use strata_security::{AccessMode, OpenOptions};

/// Transitional result type for the bootstrap-only legacy shell.
pub type Result<T> = std::result::Result<T, Error>;
