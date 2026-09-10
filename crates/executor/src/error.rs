//! Executor error boundary.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Deserializer, Serialize};
use strata_engine::{
    CommitOutcomeStatus, EngineError, EngineErrorStatus, ErrorClass, ErrorCodeRegistryEntry,
    ErrorDetail, RetryPolicy,
};

use crate::error_registry::{
    public_error_code_entry, unregistered_code_entry, ERROR_REGISTRY_DOC_PAGE,
};

const DEFAULT_DOCS_BASE_URL: &str = "https://stratadb.org";

/// Source of user-visible error reference ids.
pub trait ErrorReferenceIdSource: Send + Sync {
    /// Returns the next reference id to attach to a rendered public error.
    fn next_reference_id(&self) -> String;
}

/// Sequential local reference id source used by the embedded default boundary.
#[derive(Debug)]
pub struct SequentialErrorReferenceIdSource {
    prefix: String,
    counter: AtomicU64,
}

impl SequentialErrorReferenceIdSource {
    /// Creates a sequential source using `prefix` and one-based numeric ids.
    #[must_use]
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            counter: AtomicU64::new(1),
        }
    }
}

impl ErrorReferenceIdSource for SequentialErrorReferenceIdSource {
    fn next_reference_id(&self) -> String {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}{id:06}", self.prefix)
    }
}

/// Boundary-specific public error rendering configuration.
#[derive(Clone)]
pub struct ErrorRenderConfig {
    docs_base_url: String,
    reference_id_source: Arc<dyn ErrorReferenceIdSource>,
}

impl fmt::Debug for ErrorRenderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ErrorRenderConfig")
            .field("docs_base_url", &self.docs_base_url)
            .finish_non_exhaustive()
    }
}

impl ErrorRenderConfig {
    /// Creates a renderer with an explicit docs base URL and reference id source.
    #[must_use]
    pub fn new(
        docs_base_url: impl Into<String>,
        reference_id_source: Arc<dyn ErrorReferenceIdSource>,
    ) -> Self {
        Self {
            docs_base_url: docs_base_url.into(),
            reference_id_source,
        }
    }

    fn docs_url_for(&self, docs_slug: &str) -> String {
        // Stable short per-code slug: https://stratadb.org/e/<code> — the code
        // is a path segment, not a fragment, so every code is its own page.
        format!(
            "{}/{ERROR_REGISTRY_DOC_PAGE}/{docs_slug}",
            self.docs_base_url.trim_end_matches('/')
        )
    }

    fn next_reference_id(&self) -> String {
        self.reference_id_source.next_reference_id()
    }
}

impl Default for ErrorRenderConfig {
    fn default() -> Self {
        Self::new(DEFAULT_DOCS_BASE_URL, default_reference_id_source())
    }
}

/// Runs `operation` with a boundary-specific error renderer.
pub fn with_error_render_config<T>(config: ErrorRenderConfig, operation: impl FnOnce() -> T) -> T {
    let previous = ERROR_RENDER_CONFIG.with(|current| current.replace(config));
    let _reset = ErrorRenderConfigReset {
        previous: Some(previous),
    };
    operation()
}

struct ErrorRenderConfigReset {
    previous: Option<ErrorRenderConfig>,
}

impl Drop for ErrorRenderConfigReset {
    fn drop(&mut self) {
        let previous = self
            .previous
            .take()
            .expect("render config reset should have previous config");
        ERROR_RENDER_CONFIG.with(|current| {
            current.replace(previous);
        });
    }
}

thread_local! {
    static ERROR_RENDER_CONFIG: std::cell::RefCell<ErrorRenderConfig> =
        std::cell::RefCell::new(ErrorRenderConfig::default());
}

/// Stable executor compatibility error class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ExecutorErrorClass {
    /// Caller supplied invalid input.
    InvalidInput,
    /// Requested object was not found.
    NotFound,
    /// Request conflicted with current state.
    Conflict,
    /// Required state is unavailable.
    Unavailable,
    /// Commit result could not be proven.
    AmbiguousCommit,
    /// Stored layout is incompatible.
    IncompatibleLayout,
    /// Stored data is corrupt.
    Corruption,
    /// Executor handle is closed.
    ClosedHandle,
    /// Internal failure.
    Internal,
}

/// Public V1 executor error status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct ErrorStatus {
    class: ErrorClass,
    code: String,
    retry_policy: RetryPolicy,
    retryable: bool,
    commit_outcome: CommitOutcomeStatus,
    message: String,
    suggested_fix: String,
    docs_url: String,
    reference_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    details: Vec<ErrorDetail>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    hints: Vec<String>,
}

impl ErrorStatus {
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

    /// Returns whether the retry policy permits a retry.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    /// Returns the commit outcome.
    #[must_use]
    pub const fn commit_outcome(&self) -> CommitOutcomeStatus {
        self.commit_outcome
    }

    /// Returns the public message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the suggested fix.
    #[must_use]
    pub fn suggested_fix(&self) -> &str {
        &self.suggested_fix
    }

    /// Returns the docs URL.
    #[must_use]
    pub fn docs_url(&self) -> &str {
        &self.docs_url
    }

    /// Returns the reference id.
    #[must_use]
    pub fn reference_id(&self) -> &str {
        &self.reference_id
    }

    /// Returns the optional trace id.
    #[must_use]
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
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

#[derive(Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
struct RawErrorStatus {
    class: ErrorClass,
    code: String,
    retry_policy: RetryPolicy,
    #[serde(default, rename = "retryable")]
    _retryable: Option<bool>,
    commit_outcome: CommitOutcomeStatus,
    message: String,
    suggested_fix: String,
    docs_url: String,
    reference_id: String,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    details: Vec<ErrorDetail>,
    #[serde(default)]
    hints: Vec<String>,
}

impl<'de> Deserialize<'de> for ErrorStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawErrorStatus::deserialize(deserializer)?;
        Ok(Self {
            class: raw.class,
            code: raw.code,
            retry_policy: raw.retry_policy,
            retryable: retry_policy_allows_retry(raw.retry_policy),
            commit_outcome: raw.commit_outcome,
            message: raw.message,
            suggested_fix: raw.suggested_fix,
            docs_url: raw.docs_url,
            reference_id: raw.reference_id,
            trace_id: raw.trace_id,
            details: raw.details,
            hints: raw.hints,
        })
    }
}

/// Executor result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Public executor error.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct ExecutorError {
    status: ErrorStatus,
}

impl ExecutorError {
    /// Creates an executor error for a registered code.
    ///
    /// The registry row for `code` supplies the class, retry policy, commit
    /// outcome and suggested fix; a construction site owns only the public
    /// message (#3244 — the old `(class, retryable)` arguments were per-site
    /// copies of the row, and sites drifted from it). An unregistered code
    /// renders as `internal.executor.unregistered_code` with the requested
    /// code kept as a detail.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: render_status(code, message, None, Vec::new(), Vec::new()),
        }
    }

    /// Creates an executor error from a wire status.
    ///
    /// The status is a transport DTO: its class, retry policy, commit outcome
    /// and suggested fix are re-derived from this process's registry row for
    /// the code, so every `ExecutorError` in-process carries the row whatever
    /// the sender rendered. The message, reference id, trace id, details and
    /// hints are kept; the docs URL is re-anchored to the code's page.
    #[must_use]
    pub fn from_status(status: ErrorStatus) -> Self {
        Self {
            status: normalize_status(status),
        }
    }

    /// Returns the public status.
    #[must_use]
    pub const fn status(&self) -> &ErrorStatus {
        &self.status
    }

    /// Consumes this error and returns the public status.
    pub(crate) fn into_status(self) -> ErrorStatus {
        self.status
    }

    /// Returns the compatibility class.
    #[must_use]
    pub fn class(&self) -> ExecutorErrorClass {
        executor_class_for_status(&self.status)
    }

    /// Returns the public class.
    #[must_use]
    pub const fn public_class(&self) -> ErrorClass {
        self.status.class()
    }

    /// Returns the stable code.
    #[must_use]
    pub fn code(&self) -> &str {
        self.status.code()
    }

    /// Returns whether this error has a retry-permitting policy.
    ///
    /// Prefer [`Self::retry_policy`] when deciding whether the caller can retry
    /// the same request or must first change state, configuration, or input.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.status.retryable()
    }

    /// Returns the retry policy.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.status.retry_policy()
    }

    /// Returns the commit outcome.
    #[must_use]
    pub const fn commit_outcome(&self) -> CommitOutcomeStatus {
        self.status.commit_outcome()
    }

    /// Returns the public message.
    #[must_use]
    pub fn message(&self) -> &str {
        self.status.message()
    }

    /// Returns the suggested fix.
    #[must_use]
    pub fn suggested_fix(&self) -> &str {
        self.status.suggested_fix()
    }

    /// Returns the docs URL.
    #[must_use]
    pub fn docs_url(&self) -> &str {
        self.status.docs_url()
    }

    /// Returns the reference id.
    #[must_use]
    pub fn reference_id(&self) -> &str {
        self.status.reference_id()
    }
}

impl From<EngineError> for ExecutorError {
    fn from(value: EngineError) -> Self {
        // The public status is intentionally pure (no source wording), so the
        // engine source chain would be lost at the boundary. Emit one structured
        // log line correlating the reference id shown to users with the code and
        // the full underlying cause (ERR-2). Capturing is the consumer's choice.
        let source_chain = source_chain_display(&value);
        let error = Self::from_status(engine_error_status(value.status()));
        if let Some(chain) = source_chain {
            tracing::error!(
                reference_id = error.reference_id(),
                code = error.code(),
                source = chain.as_str(),
                "engine error crossed the executor boundary"
            );
        }
        error
    }
}

/// Renders an error's full `Error::source()` chain into one line, or `None` when
/// the error has no source.
fn source_chain_display(error: &dyn Error) -> Option<String> {
    let mut source = error.source()?;
    let mut chain = source.to_string();
    while let Some(next) = source.source() {
        chain.push_str("; caused by: ");
        chain.push_str(&next.to_string());
        source = next;
    }
    Some(chain)
}

#[cfg(feature = "inference")]
impl From<strata_inference::InferenceError> for ExecutorError {
    fn from(value: strata_inference::InferenceError) -> Self {
        // The registry row is the single authority for a registered code's
        // class, retry policy, commit outcome and suggested fix; private
        // per-code tables here drifted from it (#3243). A code missing from
        // the registry renders as `internal.executor.unregistered_code`.
        Self::new(value.code(), value.public_message())
    }
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message())
    }
}

impl Error for ExecutorError {}

fn default_reference_id_source() -> Arc<dyn ErrorReferenceIdSource> {
    static SOURCE: OnceLock<Arc<SequentialErrorReferenceIdSource>> = OnceLock::new();
    SOURCE
        .get_or_init(|| {
            Arc::new(SequentialErrorReferenceIdSource::new(
                process_reference_prefix(),
            ))
        })
        .clone()
}

/// A per-process reference-id prefix so ids do not collide across process runs
/// (the sequential counter alone restarts at 1 each run, so `err_local_000001`
/// would recur every process). Still begins with `err_local_`.
fn process_reference_prefix() -> String {
    let token = crate::time_compat::SystemTime::now()
        .duration_since(crate::time_compat::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!("err_local_{token:08x}_")
}

fn current_error_render_config() -> ErrorRenderConfig {
    ERROR_RENDER_CONFIG.with(|current| current.borrow().clone())
}

/// What a construction site owns on a status: everything the registry row
/// does not.
struct SiteFields {
    message: String,
    trace_id: Option<String>,
    details: Vec<ErrorDetail>,
    hints: Vec<String>,
}

/// Resolves the registry row a status renders with: the row for
/// `requested_code` when it is registered, otherwise the
/// `internal.executor.unregistered_code` fallback with the requested code
/// recorded as a detail. Returns the row and the code the status carries.
fn resolve_row(
    requested_code: String,
    details: &mut Vec<ErrorDetail>,
) -> (ErrorCodeRegistryEntry, String) {
    if let Some(entry) = public_error_code_entry(&requested_code) {
        return (entry, requested_code);
    }
    let entry = unregistered_code_entry();
    details.push(ErrorDetail::new("unregistered_code", requested_code));
    (entry, entry.code.to_owned())
}

/// The one place a registry row becomes a status: class, retry policy,
/// commit outcome and suggested fix are the row's, unconditionally.
fn status_from_row(
    row: &ErrorCodeRegistryEntry,
    code: String,
    site: SiteFields,
    docs_url: String,
    reference_id: String,
) -> ErrorStatus {
    ErrorStatus {
        class: row.class,
        code,
        retry_policy: row.retry_policy,
        retryable: retry_policy_allows_retry(row.retry_policy),
        commit_outcome: row.commit_outcome,
        message: site.message,
        suggested_fix: row.suggested_fix.to_owned(),
        docs_url,
        reference_id,
        trace_id: site.trace_id,
        details: site.details,
        hints: site.hints,
    }
}

/// Renders a fresh status for `code` under the current boundary config, which
/// supplies the docs URL and the reference id.
fn render_status(
    code: impl Into<String>,
    message: impl Into<String>,
    trace_id: Option<String>,
    details: Vec<ErrorDetail>,
    hints: Vec<String>,
) -> ErrorStatus {
    let config = current_error_render_config();
    let mut site = SiteFields {
        message: message.into(),
        trace_id,
        details,
        hints,
    };
    let (row, code) = resolve_row(code.into(), &mut site.details);
    let docs_url = config.docs_url_for(row.docs_slug);
    let reference_id = config.next_reference_id();
    status_from_row(&row, code, site, docs_url, reference_id)
}

/// Re-derives the row-owned fields of a wire status from this process's
/// registry, keeping what the sender owns (see [`ExecutorError::from_status`]).
fn normalize_status(status: ErrorStatus) -> ErrorStatus {
    let mut site = SiteFields {
        message: status.message,
        trace_id: status.trace_id,
        details: status.details,
        hints: status.hints,
    };
    let (row, code) = resolve_row(status.code, &mut site.details);
    let docs_url = normalize_docs_url(&status.docs_url, row.docs_slug);
    status_from_row(&row, code, site, docs_url, status.reference_id)
}

fn normalize_docs_url(docs_url: &str, docs_slug: &str) -> String {
    // Ignore any legacy fragment; the canonical form is a path slug.
    let base = docs_url
        .split_once('#')
        .map_or(docs_url, |(base, _anchor)| base);
    let base = base.trim_end_matches('/');
    let mut segments = base.rsplit('/');
    let last = segments.next();
    let second_last = segments.next();
    // Already the canonical per-code page: …/e/<code>.
    if last == Some(docs_slug) && second_last == Some(ERROR_REGISTRY_DOC_PAGE) {
        return base.to_owned();
    }
    // A docs base ending at the error segment: …/e — append the code.
    if last == Some(ERROR_REGISTRY_DOC_PAGE) {
        return format!("{base}/{docs_slug}");
    }
    current_error_render_config().docs_url_for(docs_slug)
}

const fn retry_policy_allows_retry(policy: RetryPolicy) -> bool {
    matches!(
        policy,
        RetryPolicy::AfterStateChange | RetryPolicy::SameRequest | RetryPolicy::IdempotentOnly
    )
}

/// Renders an engine status at the boundary. The engine's own class, retry
/// policy, commit outcome and suggested fix are not consulted: the registry
/// row for the code is the authority on this side too, and a site-specific
/// remedy travels in `hints`.
pub(crate) fn engine_error_status(status: &EngineErrorStatus) -> ErrorStatus {
    render_status(
        status.code().to_owned(),
        status.message().to_owned(),
        None,
        status.details().to_vec(),
        status.hints().to_vec(),
    )
}

fn executor_class_for_status(status: &ErrorStatus) -> ExecutorErrorClass {
    match status.code() {
        "failed_precondition.engine.runtime_closed" => {
            return ExecutorErrorClass::ClosedHandle;
        }
        "failed_precondition.engine.space_not_empty" => return ExecutorErrorClass::Conflict,
        _ => {}
    }
    match status.class() {
        ErrorClass::InvalidArgument | ErrorClass::Serialization => ExecutorErrorClass::InvalidInput,
        ErrorClass::NotFound | ErrorClass::HistoryUnavailable => ExecutorErrorClass::NotFound,
        ErrorClass::AlreadyExists | ErrorClass::Conflict => ExecutorErrorClass::Conflict,
        ErrorClass::Unavailable
        | ErrorClass::Unsupported
        | ErrorClass::ResourceExhausted
        | ErrorClass::Io
        | ErrorClass::AccessDenied
        | ErrorClass::FailedPrecondition => ExecutorErrorClass::Unavailable,
        ErrorClass::AmbiguousCommit => ExecutorErrorClass::AmbiguousCommit,
        // `data_loss` shares the internal corruption class: same non-retryable
        // "stop and inspect" posture, only the public class segment differs.
        ErrorClass::Corruption | ErrorClass::DataLoss => ExecutorErrorClass::Corruption,
        _ => ExecutorErrorClass::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::{process_reference_prefix, source_chain_display};
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct Layer {
        message: &'static str,
        cause: Option<Box<Layer>>,
    }

    impl fmt::Display for Layer {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message)
        }
    }

    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.cause.as_deref().map(|cause| cause as &dyn Error)
        }
    }

    #[test]
    fn source_chain_display_walks_every_cause() {
        let error = Layer {
            message: "top",
            cause: Some(Box::new(Layer {
                message: "middle",
                cause: Some(Box::new(Layer {
                    message: "root",
                    cause: None,
                })),
            })),
        };
        assert_eq!(
            source_chain_display(&error).as_deref(),
            Some("middle; caused by: root")
        );
    }

    #[test]
    fn source_chain_display_is_none_without_a_source() {
        let leaf = Layer {
            message: "only",
            cause: None,
        };
        assert!(source_chain_display(&leaf).is_none());
    }

    #[test]
    fn process_reference_prefix_is_namespaced_and_process_unique() {
        let prefix = process_reference_prefix();
        assert!(prefix.starts_with("err_local_"));
        // Carries a per-process token beyond the bare namespace, so ids do not
        // collide across runs.
        assert!(prefix.len() > "err_local_".len() + 1);
    }

    #[test]
    fn normalize_docs_url_keeps_only_the_canonical_per_code_page() {
        use super::{normalize_docs_url, ERROR_REGISTRY_DOC_PAGE};

        let slug = "not_found.engine.persistence";
        let canonical = format!("https://docs.example/{ERROR_REGISTRY_DOC_PAGE}/{slug}");
        let local = format!("/{ERROR_REGISTRY_DOC_PAGE}/{slug}");

        // A foreign canonical page survives as sent, fragment stripped.
        assert_eq!(normalize_docs_url(&canonical, slug), canonical);
        assert_eq!(
            normalize_docs_url(&format!("{canonical}#legacy"), slug),
            canonical
        );
        // A base ending at the error segment gets the code appended.
        assert_eq!(
            normalize_docs_url(
                &format!("https://docs.example/{ERROR_REGISTRY_DOC_PAGE}/"),
                slug
            ),
            canonical
        );
        // Direction controls: both halves of the canonical test are
        // load-bearing. Another code's page is not this code's page, and a
        // path that merely ends in the code is not the error page — each is
        // re-derived from this process's docs base instead.
        for foreign in [
            format!("https://docs.example/{ERROR_REGISTRY_DOC_PAGE}/some.other.code"),
            format!("https://docs.example/guide/{slug}"),
        ] {
            let normalized = normalize_docs_url(&foreign, slug);
            assert_ne!(normalized, foreign);
            assert!(normalized.ends_with(&local), "{normalized}");
        }
    }

    #[test]
    fn data_loss_error_surfaces_data_loss_public_class_but_corruption_compat_class() {
        use super::{ErrorClass, ExecutorError, ExecutorErrorClass};

        // #2749: a registered `data_loss.*` code surfaces its own public wire
        // class, driven by the registry entry, not folded onto `corruption`.
        let error = ExecutorError::new(
            "data_loss.engine.kv_value",
            "stored KV row is missing a value",
        );
        assert_eq!(error.public_class(), ErrorClass::DataLoss);
        // The compatibility class stays `Corruption`: `data_loss` shares the
        // non-retryable "stop and inspect" posture, and `.class()` must not
        // regress to `Internal` now that the public class is its own variant.
        assert_eq!(error.class(), ExecutorErrorClass::Corruption);
        assert!(!error.retryable());
    }

    #[test]
    fn compat_class_maps_the_arm_adjacent_to_the_data_loss_change() {
        use super::{ErrorClass, ExecutorError, ExecutorErrorClass};

        // `executor_class_for_status`'s `AmbiguousCommit` arm sits directly
        // above the `Corruption | DataLoss` arm this fix adds, so it rides into
        // the same diff hunk; pin it so its deletion is caught rather than
        // folding an ambiguous-commit error onto `Internal`.
        let error = ExecutorError::new(
            "ambiguous_commit.engine.persistence",
            "commit outcome could not be proven",
        );
        assert_eq!(error.public_class(), ErrorClass::AmbiguousCommit);
        assert_eq!(error.class(), ExecutorErrorClass::AmbiguousCommit);
    }
}
