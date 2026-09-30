//! Backend contract and capability facts.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the contract is consumed by concrete backends added in later work"
    )
)]

use crate::object::{ObjectName, ObjectPrefix};
use std::fmt;
#[cfg(feature = "localfs")]
use std::sync::Arc;

#[cfg(test)]
mod conformance;

#[cfg(feature = "localfs")]
pub(crate) mod local_fs;

mod delete;
pub(crate) mod memory;
mod publish;

#[cfg(all(debug_assertions, feature = "localfs"))]
pub(crate) use local_fs::with_authorized_wal_repair_mutation;
#[cfg(all(debug_assertions, feature = "localfs"))]
pub(crate) use local_fs::with_authorized_wal_retention_mutation;

#[cfg(not(all(debug_assertions, feature = "localfs")))]
pub(crate) fn with_authorized_wal_retention_mutation<R>(operation: impl FnOnce() -> R) -> R {
    operation()
}

#[cfg(not(all(debug_assertions, feature = "localfs")))]
pub(crate) fn with_authorized_wal_repair_mutation<R>(operation: impl FnOnce() -> R) -> R {
    operation()
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) use delete::{durable_delete_result, failed_delete_result};
#[cfg(test)]
pub(crate) use delete::{unsupported_delete, DeleteFailureKind};
pub(crate) use delete::{DeleteDurability, DeleteError, DeleteOutcome, DeleteResult, DeleteStatus};
pub(crate) use publish::{
    PublishDurability, PublishError, PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
};

pub(crate) type BackendResult<T> = Result<T, BackendError>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub(crate) enum BackendCapability {
    ReadObject = 0,
    ReadRange = 1,
    WriteObject = 2,
    DeleteObject = 3,
    ListPrefix = 4,
    ObjectMetadata = 5,
    AppendObject = 6,
    ConditionalCreate = 7,
    ConditionalUpdate = 8,
    DurablePublish = 9,
    ConditionalPublish = 10,
    DurableSync = 11,
    SingleWriterLock = 12,
    Lease = 13,
    ConsistentList = 14,
    MonotonicMetadata = 15,
    /// #3721: `link_object` gives an existing object a second, durable,
    /// no-clobber name without rewriting its bytes (a local-fs hard link).
    DurableLink = 16,
}

impl BackendCapability {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::ReadObject => "read_object",
            Self::ReadRange => "read_range",
            Self::WriteObject => "write_object",
            Self::DeleteObject => "delete_object",
            Self::ListPrefix => "list_prefix",
            Self::ObjectMetadata => "object_metadata",
            Self::AppendObject => "append_object",
            Self::ConditionalCreate => "conditional_create",
            Self::ConditionalUpdate => "conditional_update",
            Self::DurablePublish => "durable_publish",
            Self::ConditionalPublish => "conditional_publish",
            Self::DurableSync => "durable_sync",
            Self::SingleWriterLock => "single_writer_lock",
            Self::Lease => "lease",
            Self::ConsistentList => "consistent_list",
            Self::MonotonicMetadata => "monotonic_metadata",
            Self::DurableLink => "durable_link",
        }
    }

    const fn bit(self) -> u32 {
        1_u32 << (self as u8)
    }
}

impl fmt::Display for BackendCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BackendCapabilities {
    bits: u32,
}

impl BackendCapabilities {
    pub(crate) const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub(crate) fn from_slice(capabilities: &[BackendCapability]) -> Self {
        let mut result = Self::empty();
        for capability in capabilities {
            result.insert(*capability);
        }
        result
    }

    pub(crate) const fn contains(self, capability: BackendCapability) -> bool {
        self.bits & capability.bit() != 0
    }

    pub(crate) fn insert(&mut self, capability: BackendCapability) {
        self.bits |= capability.bit();
    }

    pub(crate) fn supports(self, required: &[BackendCapability]) -> bool {
        required.iter().all(|capability| self.contains(*capability))
    }

    pub(crate) fn missing(self, required: &[BackendCapability]) -> Vec<BackendCapability> {
        required
            .iter()
            .copied()
            .filter(|capability| !self.contains(*capability))
            .collect()
    }
}

pub(crate) const CACHE_MODE_REQUIREMENTS: &[BackendCapability] = &[
    BackendCapability::ReadObject,
    BackendCapability::ReadRange,
    BackendCapability::WriteObject,
    BackendCapability::DeleteObject,
    BackendCapability::ListPrefix,
];

// Cache mode can run on a weaker browser-like object store than the full basic
// backend conformance contract. Metadata is useful for local validation, but it
// is not required to claim cache-mode behavior.
pub(crate) const BASIC_OBJECT_BACKEND_CAPABILITIES: &[BackendCapability] = &[
    BackendCapability::ReadObject,
    BackendCapability::ReadRange,
    BackendCapability::WriteObject,
    BackendCapability::DeleteObject,
    BackendCapability::ListPrefix,
    BackendCapability::ObjectMetadata,
];

// Durable local storage needs append, publish, sync, and a single-writer guard
// before upper layers can make commit-ordering claims.
pub(crate) const DURABLE_LOCAL_MODE_REQUIREMENTS: &[BackendCapability] = &[
    BackendCapability::ReadObject,
    BackendCapability::ReadRange,
    BackendCapability::WriteObject,
    BackendCapability::DeleteObject,
    BackendCapability::ListPrefix,
    BackendCapability::ObjectMetadata,
    BackendCapability::AppendObject,
    BackendCapability::DurablePublish,
    BackendCapability::DurableSync,
    BackendCapability::SingleWriterLock,
];

// Object-durable backends can later satisfy durability through conditional
// publication or create/update fences, but they still need consistent listing
// and monotonic metadata for recovery decisions.
pub(crate) const OBJECT_DURABLE_CANDIDATE_BASE_REQUIREMENTS: &[BackendCapability] = &[
    BackendCapability::ReadObject,
    BackendCapability::ReadRange,
    BackendCapability::WriteObject,
    BackendCapability::DeleteObject,
    BackendCapability::ListPrefix,
    BackendCapability::ObjectMetadata,
    BackendCapability::ConsistentList,
    BackendCapability::MonotonicMetadata,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendFence(Vec<u8>);

impl BackendFence {
    pub(crate) fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendMetadata {
    size_bytes: u64,
    fence: Option<BackendFence>,
}

impl BackendMetadata {
    pub(crate) const fn new(size_bytes: u64, fence: Option<BackendFence>) -> Self {
        Self { size_bytes, fence }
    }

    pub(crate) const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) const fn fence(&self) -> Option<&BackendFence> {
        self.fence.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendAppend {
    start_offset: u64,
    bytes_written: u64,
    metadata: BackendMetadata,
}

impl BackendAppend {
    pub(crate) const fn new(
        start_offset: u64,
        bytes_written: u64,
        metadata: BackendMetadata,
    ) -> Self {
        // Callers must verify all three facts. Some backends can expose bytes
        // even when the reported append facts are wrong.
        Self {
            start_offset,
            bytes_written,
            metadata,
        }
    }

    pub(crate) const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    pub(crate) const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) const fn metadata(&self) -> &BackendMetadata {
        &self.metadata
    }
}

/// A persistent append handle over a single durable object, held across many
/// appends by a single writer. `append` writes through the held descriptor and
/// returns the same `BackendAppend` facts as `Backend::append_object` (the caller
/// still verifies them); `sync` fsyncs the held descriptor. The handle tracks the
/// object size in memory, so steady-state appends do no `stat`/`open`/`close`.
///
/// `Send` is load-bearing: the owning `WalService` is moved across threads for
/// background retention (it never carries the handle into that clone, but the type
/// must stay `Send`). It need not be `Sync` — it lives behind `&mut self`.
pub(crate) trait BackendAppendHandle: Send {
    fn append(&mut self, bytes: &[u8]) -> BackendResult<BackendAppend>;

    fn sync(&mut self) -> BackendResult<()>;
}

trait BackendWriterGuardInner: Send + Sync {}

impl<T> BackendWriterGuardInner for T where T: Send + Sync {}

pub(crate) struct BackendWriterGuard {
    object: ObjectName,
    // The concrete guard owns the backend-specific lock handle. Releasing this
    // wrapper drops that handle and releases the writer lock by RAII.
    _inner: Box<dyn BackendWriterGuardInner>,
}

impl BackendWriterGuard {
    #[cfg_attr(
        all(not(feature = "localfs"), not(feature = "testkit"), not(test)),
        expect(
            dead_code,
            reason = "writer guards are only constructed by backends with single-writer support"
        )
    )]
    pub(crate) fn new(object: ObjectName, inner: impl Send + Sync + 'static) -> Self {
        Self {
            object,
            _inner: Box::new(inner),
        }
    }

    #[cfg_attr(
        any(test, feature = "testkit"),
        allow(
            dead_code,
            reason = "test and testkit builds construct guard wrappers without always reading the lock object"
        )
    )]
    #[cfg_attr(
        all(not(feature = "localfs"), not(feature = "testkit"), not(test)),
        expect(
            dead_code,
            reason = "no-default builds only exercise the unsupported guard path"
        )
    )]
    pub(crate) fn object(&self) -> &ObjectName {
        &self.object
    }
}

impl fmt::Debug for BackendWriterGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendWriterGuard")
            .field("object", &self.object)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendRange {
    offset: u64,
    length: u64,
}

impl BackendRange {
    pub(crate) const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn end_offset(self) -> Option<u64> {
        self.offset.checked_add(self.length)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendErrorKind {
    NotFound,
    AlreadyExists,
    PreconditionFailed,
    PermissionDenied,
    InvalidObjectName,
    InvalidRange,
    UnsupportedOperation,
    CapabilityMismatch,
    Unavailable,
    Interrupted,
    MetadataMismatch,
    Corruption,
    NoSpace,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendError {
    kind: BackendErrorKind,
    message: String,
}

impl BackendError {
    pub(crate) fn new(kind: BackendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn unsupported(capability: BackendCapability) -> Self {
        Self::new(
            BackendErrorKind::UnsupportedOperation,
            format!("backend does not support {capability}"),
        )
    }

    pub(crate) const fn kind(&self) -> BackendErrorKind {
        self.kind
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BackendError {}

pub(crate) trait Backend: Send + Sync {
    fn capabilities(&self) -> BackendCapabilities;

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>>;

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>>;

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata>;

    fn delete_object(&self, name: &ObjectName) -> DeleteResult;

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>>;

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata>;

    // #3692: remove every empty directory below the family root named by
    // `prefix` (a directory prefix such as `timeline/`), returning how many
    // went. Garbage removal for backends whose object names nest into real
    // directories; object-keyed backends have no directories, so the default
    // is a no-op. Never removes the family root or anything holding an object.
    fn remove_empty_dirs_under(&self, _prefix: &ObjectPrefix) -> Result<u64, BackendError> {
        Ok(0)
    }

    // The returned guard is the lock lifetime. Dropping it releases the
    // backend-specific single-writer primitive.
    fn acquire_writer_lock(&self, _name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        Err(BackendError::unsupported(
            BackendCapability::SingleWriterLock,
        ))
    }

    fn append_object(&self, _name: &ObjectName, _bytes: &[u8]) -> BackendResult<BackendAppend> {
        Err(BackendError::unsupported(BackendCapability::AppendObject))
    }

    fn sync_object(&self, _name: &ObjectName) -> BackendResult<()> {
        Err(BackendError::unsupported(BackendCapability::DurableSync))
    }

    // Open a persistent append handle for `name`, letting a single-writer caller
    // append and fsync through one held descriptor instead of reopening the
    // object per call. `expected_size` is the caller's authoritative current size;
    // implementations verify it against the object as a once-per-open boundary
    // check. Returns `Ok(None)` by default: callers must fall back to
    // `append_object`/`sync_object`. Only durable backends override this.
    fn open_append_handle(
        &self,
        _name: &ObjectName,
        _expected_size: u64,
    ) -> BackendResult<Option<Box<dyn BackendAppendHandle>>> {
        Ok(None)
    }

    fn conditional_create(
        &self,
        _name: &ObjectName,
        _bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        Err(BackendError::unsupported(
            BackendCapability::ConditionalCreate,
        ))
    }

    fn conditional_update(
        &self,
        _name: &ObjectName,
        _expected: &BackendFence,
        _bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        Err(BackendError::unsupported(
            BackendCapability::ConditionalUpdate,
        ))
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        _bytes: &[u8],
        _mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        Err(PublishError::unsupported(
            name,
            BackendError::unsupported(BackendCapability::DurablePublish),
        ))
    }

    // #3721: make `to` a second name for `from`'s bytes without rewriting
    // them, with create-mode publish semantics on `to`: no-clobber (an
    // existing `to` is `PreconditionFailed`), failure classified by
    // visibility, and success only once `to`'s entry is durable. `from` is
    // left in place; removing it is the caller's separate `delete_object`.
    // Backends advertise it as `DurableLink`; the default refuses before any
    // mutation (`Unsupported`), so a caller falls back to a byte copy.
    fn link_object(
        &self,
        _from: &ObjectName,
        to: &ObjectName,
    ) -> Result<PublishOutcome, PublishError> {
        Err(PublishError::unsupported(
            to,
            BackendError::unsupported(BackendCapability::DurableLink),
        ))
    }
}

#[derive(Clone)]
pub(crate) struct BackendHandle<'a> {
    inner: BackendHandleInner<'a>,
}

#[derive(Clone)]
enum BackendHandleInner<'a> {
    Borrowed(&'a dyn Backend),
    #[cfg(feature = "localfs")]
    Owned(Arc<dyn Backend>),
}

impl<'a> BackendHandle<'a> {
    pub(crate) const fn borrowed(backend: &'a dyn Backend) -> Self {
        Self {
            inner: BackendHandleInner::Borrowed(backend),
        }
    }

    pub(crate) fn as_backend(&self) -> &dyn Backend {
        match &self.inner {
            BackendHandleInner::Borrowed(backend) => *backend,
            #[cfg(feature = "localfs")]
            BackendHandleInner::Owned(backend) => backend.as_ref(),
        }
    }
}

#[cfg(feature = "localfs")]
impl BackendHandle<'static> {
    pub(crate) fn owned(backend: impl Backend + 'static) -> Self {
        Self {
            inner: BackendHandleInner::Owned(Arc::new(backend)),
        }
    }
}

impl<'a> From<&'a dyn Backend> for BackendHandle<'a> {
    fn from(backend: &'a dyn Backend) -> Self {
        Self::borrowed(backend)
    }
}

impl<'a, T> From<&'a T> for BackendHandle<'a>
where
    T: Backend + Sized + 'a,
{
    fn from(backend: &'a T) -> Self {
        Self::borrowed(backend)
    }
}

impl fmt::Debug for BackendHandle<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendHandle")
            .finish_non_exhaustive()
    }
}

impl Backend for BackendHandle<'_> {
    fn capabilities(&self) -> BackendCapabilities {
        self.as_backend().capabilities()
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.as_backend().read_object(name)
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        self.as_backend().read_range(name, range)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.as_backend().write_object(name, bytes)
    }

    fn delete_object(&self, name: &ObjectName) -> DeleteResult {
        self.as_backend().delete_object(name)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.as_backend().list_prefix(prefix)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.as_backend().object_metadata(name)
    }

    fn remove_empty_dirs_under(&self, prefix: &ObjectPrefix) -> Result<u64, BackendError> {
        self.as_backend().remove_empty_dirs_under(prefix)
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        self.as_backend().acquire_writer_lock(name)
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        self.as_backend().append_object(name, bytes)
    }

    fn sync_object(&self, name: &ObjectName) -> BackendResult<()> {
        self.as_backend().sync_object(name)
    }

    fn open_append_handle(
        &self,
        name: &ObjectName,
        expected_size: u64,
    ) -> BackendResult<Option<Box<dyn BackendAppendHandle>>> {
        self.as_backend().open_append_handle(name, expected_size)
    }

    fn conditional_create(
        &self,
        name: &ObjectName,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        self.as_backend().conditional_create(name, bytes)
    }

    fn conditional_update(
        &self,
        name: &ObjectName,
        expected: &BackendFence,
        bytes: &[u8],
    ) -> BackendResult<BackendMetadata> {
        self.as_backend().conditional_update(name, expected, bytes)
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        self.as_backend().publish_object(name, bytes, mode)
    }

    fn link_object(
        &self,
        from: &ObjectName,
        to: &ObjectName,
    ) -> Result<PublishOutcome, PublishError> {
        self.as_backend().link_object(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Backend, BackendAppend, BackendCapabilities, BackendCapability, BackendError,
        BackendErrorKind, BackendFence, BackendMetadata, BackendRange, DeleteResult,
        BASIC_OBJECT_BACKEND_CAPABILITIES, CACHE_MODE_REQUIREMENTS,
        DURABLE_LOCAL_MODE_REQUIREMENTS, OBJECT_DURABLE_CANDIDATE_BASE_REQUIREMENTS,
    };
    use crate::object::{ObjectName, ObjectPrefix};

    struct EmptyBackend;

    impl Backend for EmptyBackend {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities::empty()
        }

        fn read_object(&self, _name: &ObjectName) -> Result<Vec<u8>, BackendError> {
            Err(BackendError::new(
                BackendErrorKind::NotFound,
                "not implemented",
            ))
        }

        fn read_range(
            &self,
            _name: &ObjectName,
            _range: BackendRange,
        ) -> Result<Vec<u8>, BackendError> {
            Err(BackendError::new(
                BackendErrorKind::UnsupportedOperation,
                "not implemented",
            ))
        }

        fn write_object(
            &self,
            _name: &ObjectName,
            _bytes: &[u8],
        ) -> Result<BackendMetadata, BackendError> {
            Err(BackendError::new(
                BackendErrorKind::UnsupportedOperation,
                "not implemented",
            ))
        }

        fn delete_object(&self, name: &ObjectName) -> DeleteResult {
            Err(super::unsupported_delete(name))
        }

        fn list_prefix(&self, _prefix: &ObjectPrefix) -> Result<Vec<ObjectName>, BackendError> {
            Ok(Vec::new())
        }

        fn object_metadata(&self, _name: &ObjectName) -> Result<BackendMetadata, BackendError> {
            Err(BackendError::new(
                BackendErrorKind::NotFound,
                "not implemented",
            ))
        }
    }

    #[test]
    fn capability_names_match_backend_vocabulary() {
        let capabilities = [
            (BackendCapability::ReadObject, "read_object"),
            (BackendCapability::ReadRange, "read_range"),
            (BackendCapability::WriteObject, "write_object"),
            (BackendCapability::DeleteObject, "delete_object"),
            (BackendCapability::ListPrefix, "list_prefix"),
            (BackendCapability::ObjectMetadata, "object_metadata"),
            (BackendCapability::AppendObject, "append_object"),
            (BackendCapability::ConditionalCreate, "conditional_create"),
            (BackendCapability::ConditionalUpdate, "conditional_update"),
            (BackendCapability::DurablePublish, "durable_publish"),
            (BackendCapability::ConditionalPublish, "conditional_publish"),
            (BackendCapability::DurableSync, "durable_sync"),
            (BackendCapability::SingleWriterLock, "single_writer_lock"),
            (BackendCapability::Lease, "lease"),
            (BackendCapability::ConsistentList, "consistent_list"),
            (BackendCapability::MonotonicMetadata, "monotonic_metadata"),
            (BackendCapability::DurableLink, "durable_link"),
        ];

        for (capability, name) in capabilities {
            assert_eq!(capability.name(), name);
            assert_eq!(capability.to_string(), name);
        }
    }

    #[test]
    fn cache_mode_requirement_matches_basic_object_backend_capabilities() {
        let memory = BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES);
        let cache_mode = BackendCapabilities::from_slice(CACHE_MODE_REQUIREMENTS);

        assert!(memory.supports(CACHE_MODE_REQUIREMENTS));
        assert!(memory.supports(BASIC_OBJECT_BACKEND_CAPABILITIES));
        assert!(
            !cache_mode.contains(BackendCapability::ObjectMetadata),
            "cache mode must not require metadata support"
        );
        assert_eq!(
            memory.missing(DURABLE_LOCAL_MODE_REQUIREMENTS),
            vec![
                BackendCapability::AppendObject,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
                BackendCapability::SingleWriterLock,
            ]
        );
    }

    #[test]
    fn delete_object_is_the_only_delete_capability_requirement() {
        let delete_capabilities = |capabilities: &[BackendCapability]| {
            capabilities
                .iter()
                .copied()
                .filter(|capability| capability.name().contains("delete"))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            delete_capabilities(CACHE_MODE_REQUIREMENTS),
            vec![BackendCapability::DeleteObject]
        );
        assert_eq!(
            delete_capabilities(DURABLE_LOCAL_MODE_REQUIREMENTS),
            vec![BackendCapability::DeleteObject]
        );
        assert_eq!(
            delete_capabilities(OBJECT_DURABLE_CANDIDATE_BASE_REQUIREMENTS),
            vec![BackendCapability::DeleteObject]
        );
    }

    #[test]
    fn object_durable_candidate_base_requires_consistent_metadata() {
        let candidate = BackendCapabilities::from_slice(&[
            BackendCapability::ReadObject,
            BackendCapability::ReadRange,
            BackendCapability::WriteObject,
            BackendCapability::DeleteObject,
            BackendCapability::ListPrefix,
            BackendCapability::ObjectMetadata,
            BackendCapability::ConditionalCreate,
            BackendCapability::ConditionalUpdate,
            BackendCapability::ConsistentList,
        ]);

        assert_eq!(
            candidate.missing(OBJECT_DURABLE_CANDIDATE_BASE_REQUIREMENTS),
            vec![BackendCapability::MonotonicMetadata]
        );
    }

    #[test]
    fn backend_range_reports_checked_end_offset() {
        let range = BackendRange::new(10, 5);
        let overflow = BackendRange::new(u64::MAX, 1);

        assert_eq!(range.offset(), 10);
        assert_eq!(range.length(), 5);
        assert_eq!(range.end_offset(), Some(15));
        assert_eq!(overflow.end_offset(), None);
    }

    #[test]
    fn backend_metadata_carries_size_and_opaque_fence() {
        let fence = BackendFence::new([1, 2, 3]);
        let metadata = BackendMetadata::new(42, Some(fence));

        assert_eq!(metadata.size_bytes(), 42);
        assert_eq!(
            metadata.fence().map(BackendFence::as_bytes),
            Some(&[1, 2, 3][..])
        );
    }

    #[test]
    fn backend_append_reports_offsets_and_metadata() {
        let metadata = BackendMetadata::new(12, None);
        let append = BackendAppend::new(4, 8, metadata.clone());

        assert_eq!(append.start_offset(), 4);
        assert_eq!(append.bytes_written(), 8);
        assert_eq!(append.metadata(), &metadata);
    }

    #[test]
    fn conditional_operations_default_to_unsupported() {
        let backend = EmptyBackend;
        let name = ObjectName::new("manifest/current").expect("valid object name");
        let fence = BackendFence::new([7]);

        let create_error = backend
            .conditional_create(&name, b"payload")
            .expect_err("conditional create should be unsupported");
        let update_error = backend
            .conditional_update(&name, &fence, b"payload")
            .expect_err("conditional update should be unsupported");
        let delete_error = backend
            .delete_object(&name)
            .expect_err("delete should be unsupported");
        let publish_error = backend
            .publish_object(&name, b"payload", super::PublishMode::Replace)
            .expect_err("publish should be unsupported");
        let append_error = backend
            .append_object(&name, b"payload")
            .expect_err("append should be unsupported");
        let sync_error = backend
            .sync_object(&name)
            .expect_err("sync should be unsupported");
        let writer_lock_error = backend
            .acquire_writer_lock(&name)
            .expect_err("writer lock should be unsupported");

        assert_eq!(backend.capabilities(), BackendCapabilities::empty());
        assert_eq!(create_error.kind(), BackendErrorKind::UnsupportedOperation);
        assert_eq!(update_error.kind(), BackendErrorKind::UnsupportedOperation);
        assert_eq!(append_error.kind(), BackendErrorKind::UnsupportedOperation);
        assert_eq!(sync_error.kind(), BackendErrorKind::UnsupportedOperation);
        assert_eq!(
            delete_error.source_error().kind(),
            BackendErrorKind::UnsupportedOperation
        );
        assert_eq!(
            writer_lock_error.kind(),
            BackendErrorKind::UnsupportedOperation
        );
        assert_eq!(publish_error.kind(), super::PublishFailureKind::Unsupported);
        assert_eq!(
            publish_error.source_error().kind(),
            BackendErrorKind::UnsupportedOperation
        );
    }

    #[test]
    fn required_backend_methods_are_callable_through_trait() {
        let backend = EmptyBackend;
        let name = ObjectName::new("manifest/current").expect("valid object name");
        let prefix = ObjectPrefix::new("manifest/").expect("valid object prefix");
        let range = BackendRange::new(0, 4);

        assert_eq!(
            backend
                .read_object(&name)
                .expect_err("empty backend read")
                .kind(),
            BackendErrorKind::NotFound
        );
        assert_eq!(
            backend
                .read_range(&name, range)
                .expect_err("empty backend range read")
                .kind(),
            BackendErrorKind::UnsupportedOperation
        );
        assert_eq!(
            backend
                .write_object(&name, b"bytes")
                .expect_err("empty backend write")
                .kind(),
            BackendErrorKind::UnsupportedOperation
        );
        assert_eq!(
            backend
                .delete_object(&name)
                .expect_err("empty backend delete")
                .source_error()
                .kind(),
            BackendErrorKind::UnsupportedOperation
        );
        assert_eq!(
            backend.list_prefix(&prefix).expect("empty list"),
            Vec::new()
        );
        assert_eq!(
            backend
                .object_metadata(&name)
                .expect_err("empty backend metadata")
                .kind(),
            BackendErrorKind::NotFound
        );
    }

    #[test]
    fn all_backend_error_kinds_are_constructible() {
        let kinds = [
            BackendErrorKind::NotFound,
            BackendErrorKind::AlreadyExists,
            BackendErrorKind::PreconditionFailed,
            BackendErrorKind::PermissionDenied,
            BackendErrorKind::InvalidObjectName,
            BackendErrorKind::InvalidRange,
            BackendErrorKind::UnsupportedOperation,
            BackendErrorKind::CapabilityMismatch,
            BackendErrorKind::Unavailable,
            BackendErrorKind::Interrupted,
            BackendErrorKind::MetadataMismatch,
            BackendErrorKind::Corruption,
            BackendErrorKind::Unknown,
        ];

        for kind in kinds {
            let error = BackendError::new(kind, format!("{kind:?}"));
            assert_eq!(error.kind(), kind);
            assert!(!error.to_string().is_empty());
        }
    }
}
