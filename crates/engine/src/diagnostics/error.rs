//! Stable executor-facing engine errors.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// V1 public error class.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// A requested object does not exist.
    NotFound,
    /// A create operation targeted an object that already exists.
    AlreadyExists,
    /// Caller supplied malformed input or an invalid option.
    InvalidArgument,
    /// Current database state or mode does not allow the operation.
    FailedPrecondition,
    /// Caller or backend lacks permission for the operation.
    AccessDenied,
    /// Request conflicts with current state.
    Conflict,
    /// The system cannot prove whether a write committed.
    AmbiguousCommit,
    /// Requested history is outside the retained window.
    HistoryUnavailable,
    /// Feature, backend, mode, format, provider, or capability is unsupported.
    Unsupported,
    /// A capacity, quota, memory, disk, or configured limit was exceeded.
    ResourceExhausted,
    /// Required service, backend, provider, lock, endpoint, or model is unavailable.
    Unavailable,
    /// Storage or filesystem IO failed without stronger classification.
    Io,
    /// Durable state or provider output violates integrity expectations.
    Corruption,
    /// Durable engine state that should exist cannot be reconstructed — a
    /// stored record failed to decode or a required artifact is gone. Distinct
    /// from `Corruption` (integrity violation detected) in that the data is
    /// unrecoverable, not merely inconsistent.
    DataLoss,
    /// Encoding, decoding, schema, format, or protocol conversion failed.
    Serialization,
    /// Strata hit an invariant failure.
    Internal,
}

/// V1 retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum RetryPolicy {
    /// Retrying the same request without changing input or state should not help.
    Never,
    /// Retry may work after configuration, branch, backend, model, or permission changes.
    AfterStateChange,
    /// Retrying the exact same request is safe and may succeed.
    SameRequest,
    /// Retry is safe only for proven-idempotent operations.
    IdempotentOnly,
    /// Strata cannot safely classify retryability.
    Unknown,
}

impl RetryPolicy {
    const fn retryable(self) -> bool {
        matches!(
            self,
            Self::AfterStateChange | Self::SameRequest | Self::IdempotentOnly
        )
    }
}

/// V1 commit outcome status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum CommitOutcomeStatus {
    /// The operation did not attempt a commit.
    NotApplicable,
    /// Validation failed before commit machinery began.
    NotStarted,
    /// Commit machinery began, but no commit became visible or durable.
    DefinitelyNotCommitted,
    /// Strata cannot prove whether the commit became visible or durable.
    MaybeCommitted,
    /// The commit succeeded, but a post-commit action failed.
    CommittedPostCommitFailed,
}

/// Redacted structured error detail.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wire-schemas", derive(schemars::JsonSchema))]
pub struct ErrorDetail {
    key: String,
    value: String,
}

impl ErrorDetail {
    /// Creates a redacted structured detail.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the detail key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the redacted detail value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Engine-owned status facts before executor boundary rendering.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EngineErrorStatus {
    class: ErrorClass,
    code: String,
    retry_policy: RetryPolicy,
    commit_outcome: CommitOutcomeStatus,
    message: String,
    suggested_fix: String,
    details: Vec<ErrorDetail>,
    hints: Vec<String>,
}

impl EngineErrorStatus {
    /// Creates engine status facts. Crate-private: the row fields come from
    /// the registry through `EngineError`'s constructors, never from a caller
    /// (#3280).
    #[must_use]
    pub(crate) fn new(
        class: ErrorClass,
        code: impl Into<String>,
        retry_policy: RetryPolicy,
        commit_outcome: CommitOutcomeStatus,
        message: impl Into<String>,
        suggested_fix: impl Into<String>,
        details: Vec<ErrorDetail>,
        hints: Vec<String>,
    ) -> Self {
        Self {
            class,
            code: code.into(),
            retry_policy,
            commit_outcome,
            message: message.into(),
            suggested_fix: suggested_fix.into(),
            details,
            hints,
        }
    }

    /// Returns the public class.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        self.class
    }

    /// Returns the stable code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the retry policy.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// Returns the commit outcome.
    #[must_use]
    pub const fn commit_outcome(&self) -> CommitOutcomeStatus {
        self.commit_outcome
    }

    /// Returns the message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the suggested fix.
    #[must_use]
    pub fn suggested_fix(&self) -> &str {
        &self.suggested_fix
    }

    /// Returns structured details.
    #[must_use]
    pub fn details(&self) -> &[ErrorDetail] {
        &self.details
    }

    /// Returns user-facing hints.
    #[must_use]
    pub fn hints(&self) -> &[String] {
        &self.hints
    }
}

/// Stable engine error class.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineErrorClass {
    /// Caller supplied invalid input.
    InvalidInput,
    /// Requested engine object was not found.
    NotFound,
    /// Request conflicted with current state.
    Conflict,
    /// Required persistence capability or state is unavailable.
    Unavailable,
    /// Persistence could not prove whether a commit succeeded.
    AmbiguousCommit,
    /// Stored engine layout is incompatible with this binary.
    IncompatibleLayout,
    /// Stored engine control data is corrupt.
    Corruption,
    /// Database handle is closed.
    ClosedRuntime,
    /// Internal engine failure.
    Internal,
}

/// Engine result alias.
pub type EngineResult<T> = Result<T, EngineError>;

/// Stable executor-facing engine error.
#[derive(Clone, Debug)]
pub struct EngineError {
    class: EngineErrorClass,
    status: EngineErrorStatus,
    source: Option<Arc<dyn Error + Send + Sync + 'static>>,
}

impl EngineError {
    /// Creates an engine error for a registered `code`.
    ///
    /// The registry row supplies the class, retry policy, commit outcome and
    /// suggested fix; a construction site owns only the code and the message
    /// (#3280). Sites with structured facts use [`Self::with_context`].
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self::build(code, message.into(), Vec::new(), Vec::new(), None)
    }

    /// [`Self::new`] retaining the underlying error as the source.
    pub(crate) fn with_source(
        code: &'static str,
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            code,
            message.into(),
            Vec::new(),
            Vec::new(),
            Some(Arc::new(source)),
        )
    }

    /// Creates an engine error from everything a construction site owns: the
    /// code, the message, structured `details`, and `hints` — the site-level
    /// remediation the registry row cannot express. The row itself is not a
    /// parameter: there is no channel to override it (#3244, #3280).
    pub(crate) fn with_context(
        code: &'static str,
        message: impl Into<String>,
        details: Vec<ErrorDetail>,
        hints: Vec<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::build(code, message.into(), details, hints, Some(Arc::new(source)))
    }

    fn build(
        code: &'static str,
        message: String,
        details: Vec<ErrorDetail>,
        hints: Vec<String>,
        source: Option<Arc<dyn Error + Send + Sync + 'static>>,
    ) -> Self {
        let (class, row) = super::registry::registry_row(code);
        Self {
            class,
            status: EngineErrorStatus::new(
                row.class,
                code,
                row.retry_policy,
                row.commit_outcome,
                message,
                row.suggested_fix,
                details,
                hints,
            ),
            source,
        }
    }

    /// Reconstructs an engine error from a preserved V1 status.
    ///
    /// Used when a captured per-item status (e.g. a failed batch entry) must be
    /// re-raised as a top-level error without substituting a coarser code. The
    /// legacy class is recovered from the registered code so `class()` stays
    /// consistent with the status.
    pub(crate) fn from_status(status: EngineErrorStatus) -> Self {
        // Rationale: a preserved status only ever carries a code the registry
        // produced, so the fallback is unreachable; it is kept as bad input
        // (the status came from outside this constructor) rather than the
        // constructors' `Internal` fallback, which covers a code typed into
        // the tree (`registry_row`).
        let legacy_class = super::registry::class_for_code(status.code())
            .unwrap_or(EngineErrorClass::InvalidInput);
        Self {
            class: legacy_class,
            status,
            source: None,
        }
    }

    /// The named constructors below are a readability convenience: the class
    /// is the registry's, and the name is checked against it in debug builds
    /// so a code typed into the wrong constructor fails at the site.
    fn of_class(class: EngineErrorClass, code: &'static str, message: impl Into<String>) -> Self {
        let error = Self::new(code, message);
        debug_assert!(
            error.class == class,
            "engine error code `{code}` is registered under {:?}, not {class:?}",
            error.class
        );
        error
    }

    #[must_use]
    /// Creates an invalid-input error.
    pub fn invalid_input(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::InvalidInput, code, message)
    }

    #[must_use]
    /// Creates a not-found error.
    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::NotFound, code, message)
    }

    #[must_use]
    /// Creates a conflict error.
    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::Conflict, code, message)
    }

    #[must_use]
    /// Creates a corruption error.
    pub fn corruption(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::Corruption, code, message)
    }

    #[must_use]
    /// Creates an incompatible-layout error.
    pub fn incompatible_layout(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::IncompatibleLayout, code, message)
    }

    #[must_use]
    /// Creates an unsupported-operation error.
    ///
    /// The legacy class is `Unavailable` (the coarse pre-V1 vocabulary has no
    /// dedicated unsupported variant); the public V1 class is derived from the
    /// `unsupported.` code prefix and resolves to `ErrorClass::Unsupported`.
    pub(crate) fn unsupported(code: &'static str, message: impl Into<String>) -> Self {
        Self::of_class(EngineErrorClass::Unavailable, code, message)
    }

    #[must_use]
    /// Creates a control-plane-unavailable error.
    pub(crate) fn control_plane_unavailable(message: impl Into<String>) -> Self {
        Self::new("unavailable.engine.control_plane", message)
    }

    #[must_use]
    /// Creates a closed-runtime error.
    pub fn closed_runtime(message: impl Into<String>) -> Self {
        Self::new("failed_precondition.engine.runtime_closed", message)
    }

    #[must_use]
    /// Returns the stable error class.
    pub const fn class(&self) -> EngineErrorClass {
        self.class
    }

    #[must_use]
    /// Returns the stable error code.
    pub fn code(&self) -> &str {
        self.status.code()
    }

    #[must_use]
    /// Returns whether this error has a retry-permitting policy.
    ///
    /// Prefer [`Self::retry_policy`] when deciding whether the caller can retry
    /// the same request or must first change state, configuration, or input.
    pub const fn retryable(&self) -> bool {
        self.status.retry_policy().retryable()
    }

    #[must_use]
    /// Returns the executor-facing message.
    pub fn message(&self) -> &str {
        self.status.message()
    }

    /// Returns the V1 public class.
    #[must_use]
    pub const fn public_class(&self) -> ErrorClass {
        self.status.class()
    }

    /// Returns the V1 retry policy.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.status.retry_policy()
    }

    /// Returns the V1 commit outcome.
    #[must_use]
    pub const fn commit_outcome(&self) -> CommitOutcomeStatus {
        self.status.commit_outcome()
    }

    /// Returns the suggested fix.
    #[must_use]
    pub fn suggested_fix(&self) -> &str {
        self.status.suggested_fix()
    }

    /// Returns structured details.
    #[must_use]
    pub fn details(&self) -> &[ErrorDetail] {
        self.status.details()
    }

    /// Returns user-facing hints.
    #[must_use]
    pub fn hints(&self) -> &[String] {
        self.status.hints()
    }

    /// Returns engine status facts.
    #[must_use]
    pub const fn status(&self) -> &EngineErrorStatus {
        &self.status
    }

    #[must_use]
    /// Returns the retained source error.
    pub fn source_arc(&self) -> Option<&Arc<dyn Error + Send + Sync + 'static>> {
        self.source.as_ref()
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message())
    }
}

impl Error for EngineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::{EngineError, EngineErrorClass, ErrorDetail};
    use crate::diagnostics::registry::{
        class_for_code, error_code_registry_entries, error_code_registry_entry,
    };

    fn row_mismatches(error: &EngineError, code: &str) -> Vec<String> {
        let row = error_code_registry_entry(code).expect("registered");
        let legacy = class_for_code(code).expect("registered");
        let mut mismatches = Vec::new();
        if error.class() != legacy {
            mismatches.push(format!("class {:?} != {legacy:?}", error.class()));
        }
        if error.public_class() != row.class {
            mismatches.push(format!(
                "public class {:?} != {:?}",
                error.public_class(),
                row.class
            ));
        }
        if error.retry_policy() != row.retry_policy {
            mismatches.push(format!(
                "retry {:?} != {:?}",
                error.retry_policy(),
                row.retry_policy
            ));
        }
        if error.commit_outcome() != row.commit_outcome {
            mismatches.push(format!(
                "commit {:?} != {:?}",
                error.commit_outcome(),
                row.commit_outcome
            ));
        }
        if error.suggested_fix() != row.suggested_fix {
            mismatches.push(format!(
                "suggested_fix `{}` != `{}`",
                error.suggested_fix(),
                row.suggested_fix
            ));
        }
        mismatches
    }

    /// The registry is the single authority for a code's row: the class,
    /// retry policy, commit outcome and suggested fix `strata agents errors`
    /// documents are what a live error carries. Every constructor is swept
    /// over every registered code so a private table shadowing the row fails
    /// here instead of shipping (#3237: 145 of 161 codes did; #3280).
    #[test]
    fn test_constructed_errors_carry_the_registry_row() {
        let mut violations = Vec::new();
        for entry in error_code_registry_entries() {
            let constructed = [
                ("new", EngineError::new(entry.code, "probe")),
                (
                    "with_source",
                    EngineError::with_source(
                        entry.code,
                        "probe",
                        std::io::Error::other("probe source"),
                    ),
                ),
                (
                    "with_context",
                    EngineError::with_context(
                        entry.code,
                        "probe",
                        vec![ErrorDetail::new("probe", "detail")],
                        vec!["probe hint".to_owned()],
                        std::io::Error::other("probe source"),
                    ),
                ),
            ];
            for (constructor, error) in constructed {
                let mismatches = row_mismatches(&error, entry.code);
                if !mismatches.is_empty() {
                    violations.push(format!(
                        "{} ({constructor}): {}",
                        entry.code,
                        mismatches.join("; ")
                    ));
                }
                if error.message() != "probe" {
                    violations.push(format!("{} ({constructor}): message lost", entry.code));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "runtime status diverges from the registry row:\n  {}",
            violations.join("\n  ")
        );
    }

    /// A construction site owns its details and hints and nothing else: the
    /// row is not a parameter, so site text lands in `hints`, never in
    /// `suggested_fix` (the negation of the pre-#3280 `with_status` contract).
    #[test]
    fn test_with_context_keeps_site_facts_and_only_site_facts() {
        let code = "unavailable.engine.persistence";
        let error = EngineError::with_context(
            code,
            "probe",
            vec![ErrorDetail::new("layer", "Service")],
            vec!["Site-specific remediation.".to_owned()],
            std::io::Error::other("probe"),
        );
        assert_eq!(error.details(), [ErrorDetail::new("layer", "Service")]);
        assert_eq!(error.hints(), ["Site-specific remediation.".to_owned()]);
        assert!(error.source_arc().is_some());
        assert_eq!(row_mismatches(&error, code), Vec::<String>::new());
        assert_ne!(error.suggested_fix(), "Site-specific remediation.");
    }

    /// Direction control for the row lookup: a persistence code whose row
    /// differs from its class fallback (per-code retry and commit arms) gets
    /// the per-code row through the plain constructor too, not only through
    /// the adapter — the constructor has no class table of its own.
    #[test]
    fn test_plain_constructor_uses_per_code_rows_not_class_defaults() {
        let error = EngineError::new("conflict.engine.branch_generation", "probe");
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(
            error.retry_policy(),
            crate::diagnostics::RetryPolicy::AfterStateChange
        );
        assert_eq!(
            error.commit_outcome(),
            crate::diagnostics::CommitOutcomeStatus::DefinitelyNotCommitted
        );
        let sibling = EngineError::conflict("conflict.engine.branch_generation", "probe");
        assert_eq!(sibling.status(), error.status());
    }

    /// The named constructors are aliases of the row, not a second class
    /// table: they agree with `new` on every field.
    #[test]
    fn test_named_constructors_match_new() {
        let pairs = [
            (
                EngineError::invalid_input("invalid_argument.engine.persistence", "probe"),
                EngineError::new("invalid_argument.engine.persistence", "probe"),
            ),
            (
                EngineError::not_found("not_found.engine.persistence", "probe"),
                EngineError::new("not_found.engine.persistence", "probe"),
            ),
            (
                EngineError::conflict("conflict.engine.persistence", "probe"),
                EngineError::new("conflict.engine.persistence", "probe"),
            ),
            (
                EngineError::corruption("corruption.engine.persistence_recovery", "probe"),
                EngineError::new("corruption.engine.persistence_recovery", "probe"),
            ),
            (
                EngineError::incompatible_layout(
                    "failed_precondition.engine.layout_version",
                    "probe",
                ),
                EngineError::new("failed_precondition.engine.layout_version", "probe"),
            ),
            (
                EngineError::unsupported("unsupported.engine.persistence_capability", "probe"),
                EngineError::new("unsupported.engine.persistence_capability", "probe"),
            ),
            (
                EngineError::control_plane_unavailable("probe"),
                EngineError::new("unavailable.engine.control_plane", "probe"),
            ),
            (
                EngineError::closed_runtime("probe"),
                EngineError::new("failed_precondition.engine.runtime_closed", "probe"),
            ),
        ];
        for (named, plain) in pairs {
            assert_eq!(named.class(), plain.class(), "{}", plain.code());
            assert_eq!(named.status(), plain.status(), "{}", plain.code());
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "is registered under")]
    fn test_named_constructor_rejects_a_code_of_another_class() {
        // Direction control for the debug class check: a not-found code typed
        // into the conflict constructor is caught at the site.
        drop(EngineError::conflict(
            "not_found.engine.persistence",
            "probe",
        ));
    }
}
