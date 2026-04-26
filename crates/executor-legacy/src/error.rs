/// Transitional error type for the bootstrap-only legacy shell.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// I/O-adjacent bootstrap failure.
    #[error("i/o error: {reason}")]
    Io {
        /// Human-readable error reason.
        reason: String,
    },
    /// Non-I/O bootstrap failure.
    #[error("bootstrap error: {reason}")]
    Internal {
        /// Human-readable error reason.
        reason: String,
    },
}
