//! Local filesystem backend shell.
//!
//! # Concurrent-mutator contract (TCP4.15)
//!
//! Every lister and statter here races every writer BY DESIGN: background
//! sweeps, retention deletes, and rewrite publications run concurrently with
//! commits and each other (#2524). Helpers are therefore written against a
//! *mutating* filesystem, never a quiescent one — the audit below is the
//! contract each operation upholds, and new helpers must pick their row
//! before they land. Four TOCTOU bugs came from helpers written against the
//! quiescent model (#2776, #2778, #2781, #2799); the conventions are now:
//! **absence mid-operation is an outcome, not an error** (skip-NotFound,
//! already-missing, tolerate-EEXIST), while non-absence errors stay fatal.
//!
//! | operation | behavior under a concurrent mutator |
//! |---|---|
//! | `read_object` / `read_range` / `object_metadata` | absence-propagating: a racer's delete surfaces as `NotFound`, a replace serves the new bytes; the symlink/dir prechecks are best-effort classifiers, not guards |
//! | `write_object` | last-writer-wins: a raced delete is recreated; a raced parent-directory delete fails `NotFound` (the owner is gone) |
//! | `delete_object` | idempotent: losing any window (parent walk, stat, or the stat→unlink gap) reports `already_missing`; the winner's parent fsync carries removal durability |
//! | `list_prefix` / `collect_files` | fuzzy snapshot: concurrently created/deleted entries may or may not appear, a vanished entry or directory is skipped, and the walk itself never fails on absence |
//! | `publish_object` | atomic install: parent-dir creation tolerates a racer's `EEXIST` with post-verify (#2799), temp files retry collisions, create-mode no-clobber maps a raced final link to `PreconditionFailed`, and partial failures classify by visibility |
//! | `acquire_writer_lock` | fail-fast BY CONTRACT: contention is the "another live opener" signal and must never be retried here (harness-side retry policy lives in `testkit::reopen_retry`) |
//! | `append_object` / `open_append_handle` / `sync_object` | single-writer by the lifecycle writer-lock contract; a raced delete of the target fails `NotFound` deliberately (#2766: name-based loss detection) |
//! | `sync_publish_parent` / `sync_delete_parent` | fail-safe ambiguity: a vanished parent reports durability-unconfirmed — visibility is known, durability of the rename/unlink genuinely is not |

use super::{
    Backend, BackendAppend, BackendAppendHandle, BackendCapabilities, BackendError,
    BackendErrorKind, BackendMetadata, BackendRange, BackendResult, BackendWriterGuard,
    DeleteDurability, DeleteError, DeleteOutcome, DeleteResult, PublishDurability, PublishError,
    PublishFailureKind, PublishMode, PublishOutcome, PublishResult,
    BASIC_OBJECT_BACKEND_CAPABILITIES,
};
use crate::layout::ObjectLayout;
use crate::object::{ObjectName, ObjectPrefix};
use fs2::FileExt as _;
#[cfg(debug_assertions)]
use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(all(test, unix))]
use std::sync::{Arc, Mutex};

const OBJECT_FILE_SUFFIX: &str = ".object@";
static TEMP_OBJECT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(debug_assertions)]
thread_local! {
    static WAL_RETENTION_MUTATION_AUTHORIZATION_DEPTH: Cell<u32> = const { Cell::new(0) };
    static WAL_REPAIR_MUTATION_AUTHORIZATION_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(debug_assertions)]
pub(crate) fn with_authorized_wal_retention_mutation<R>(operation: impl FnOnce() -> R) -> R {
    let previous_depth = WAL_RETENTION_MUTATION_AUTHORIZATION_DEPTH.with(|depth| {
        let previous = depth.get();
        depth.set(previous.saturating_add(1));
        previous
    });
    let _guard = WalRetentionMutationAuthorizationGuard { previous_depth };
    operation()
}

#[cfg(debug_assertions)]
pub(crate) fn with_authorized_wal_repair_mutation<R>(operation: impl FnOnce() -> R) -> R {
    let previous_depth = WAL_REPAIR_MUTATION_AUTHORIZATION_DEPTH.with(|depth| {
        let previous = depth.get();
        depth.set(previous.saturating_add(1));
        previous
    });
    let _guard = WalRepairMutationAuthorizationGuard { previous_depth };
    operation()
}

#[cfg(debug_assertions)]
struct WalRetentionMutationAuthorizationGuard {
    previous_depth: u32,
}

#[cfg(debug_assertions)]
struct WalRepairMutationAuthorizationGuard {
    previous_depth: u32,
}

#[cfg(debug_assertions)]
impl Drop for WalRetentionMutationAuthorizationGuard {
    fn drop(&mut self) {
        WAL_RETENTION_MUTATION_AUTHORIZATION_DEPTH.with(|depth| depth.set(self.previous_depth));
    }
}

#[cfg(debug_assertions)]
impl Drop for WalRepairMutationAuthorizationGuard {
    fn drop(&mut self) {
        WAL_REPAIR_MUTATION_AUTHORIZATION_DEPTH.with(|depth| depth.set(self.previous_depth));
    }
}

#[cfg(debug_assertions)]
fn is_wal_retention_object(name: &ObjectName) -> bool {
    ObjectLayout::has_wal_segment_prefix(name)
        || ObjectLayout::has_wal_segment_metadata_prefix(name)
}

#[cfg(debug_assertions)]
fn is_wal_segment_object(name: &ObjectName) -> bool {
    ObjectLayout::has_wal_segment_prefix(name)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalFsPublishStep {
    TemporaryCreate,
    TemporaryWrite,
    TemporarySync,
    FinalPublish,
    ParentSync,
}

impl LocalFsPublishStep {
    const fn name(self) -> &'static str {
        match self {
            Self::TemporaryCreate => "temporary_create",
            Self::TemporaryWrite => "temporary_write",
            Self::TemporarySync => "temporary_sync",
            Self::FinalPublish => "final_publish",
            Self::ParentSync => "parent_sync",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalFsDeleteStep {
    BeforeRemoval,
    Removal,
    ParentSync,
}

impl LocalFsDeleteStep {
    const fn name(self) -> &'static str {
        match self {
            Self::BeforeRemoval => "before_removal",
            Self::Removal => "removal",
            Self::ParentSync => "parent_sync",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LocalFsBackend {
    root: PathBuf,
    /// Armed publish fault `(step, target_object)`: a `None` target faults the step for any object,
    /// `Some(name)` faults only that object — so a durability test can fail one specific publish
    /// (e.g. a branch table manifest). Inlined tuple (not a named type) so this test-only hook adds
    /// nothing to the production type inventory.
    #[cfg(all(test, unix))]
    #[allow(
        clippy::type_complexity,
        reason = "inlined tuple keeps this test-only fault hook out of the production type inventory"
    )]
    publish_fault: Arc<Mutex<Option<(LocalFsPublishStep, Option<String>)>>>,
    #[cfg(all(test, unix))]
    delete_fault: Arc<Mutex<Option<LocalFsDeleteStep>>>,
}

impl LocalFsBackend {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            #[cfg(all(test, unix))]
            publish_fault: Arc::new(Mutex::new(None)),
            #[cfg(all(test, unix))]
            delete_fault: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn writer_lock_object() -> BackendResult<ObjectName> {
        // Backend bootstrap exception: the writer lock is the one reserved
        // object-layout name the backend must recognize before services can
        // safely open a durable local runtime.
        ObjectLayout::writer_lock().map_err(|error| {
            BackendError::new(
                BackendErrorKind::InvalidObjectName,
                format!("writer lock layout is invalid: {error}"),
            )
        })
    }

    fn is_writer_lock_object(name: &ObjectName) -> BackendResult<bool> {
        Ok(Self::writer_lock_object()? == *name)
    }

    fn require_writer_lock_object(name: &ObjectName) -> BackendResult<()> {
        let expected = Self::writer_lock_object()?;
        if &expected == name {
            Ok(())
        } else {
            Err(BackendError::new(
                BackendErrorKind::InvalidObjectName,
                format!("single-writer lock must use object {expected}, got {name}"),
            ))
        }
    }

    fn reject_writer_lock_object_mutation(
        name: &ObjectName,
        operation: &'static str,
    ) -> BackendResult<()> {
        if Self::is_writer_lock_object(name)? {
            Err(BackendError::new(
                BackendErrorKind::PermissionDenied,
                format!("{operation} cannot mutate reserved writer lock object {name}"),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(debug_assertions)]
    fn assert_wal_retention_mutation_authorized(
        name: &ObjectName,
        operation: &'static str,
        caller: &'static str,
    ) {
        let guarded = match operation {
            "delete" => is_wal_retention_object(name),
            "rename" => is_wal_segment_object(name),
            _ => false,
        };
        if !guarded {
            return;
        }
        let authorized = match operation {
            "delete" => WAL_RETENTION_MUTATION_AUTHORIZATION_DEPTH.with(|depth| depth.get() > 0),
            "rename" => WAL_REPAIR_MUTATION_AUTHORIZATION_DEPTH.with(|depth| depth.get() > 0),
            _ => false,
        };
        assert!(
            authorized,
            "unexpected WAL retention mutation: operation={operation} object={name} caller={caller} outside authorized WAL retention/repair path"
        );
    }

    #[cfg(not(debug_assertions))]
    fn assert_wal_retention_mutation_authorized(
        _name: &ObjectName,
        _operation: &'static str,
        _caller: &'static str,
    ) {
    }

    #[cfg(all(test, unix))]
    fn arm_publish_fault(&self, step: LocalFsPublishStep) -> BackendResult<()> {
        let mut fault = self.publish_fault.lock().map_err(|_| {
            BackendError::new(
                BackendErrorKind::Unknown,
                "local filesystem publish fault state is poisoned",
            )
        })?;
        *fault = Some((step, None));
        Ok(())
    }

    #[cfg(all(test, unix))]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    fn arm_targeted_publish_fault(
        &self,
        step: LocalFsPublishStep,
        target_object: String,
    ) -> BackendResult<()> {
        let mut fault = self.publish_fault.lock().map_err(|_| {
            BackendError::new(
                BackendErrorKind::Unknown,
                "local filesystem publish fault state is poisoned",
            )
        })?;
        *fault = Some((step, Some(target_object)));
        Ok(())
    }

    /// Arm a targeted publish fault at the temp-file fsync (before the object becomes visible) for
    /// `target_object`. Generic over any durable object — table manifest, checkpoint snapshot, etc.
    #[cfg(all(test, unix))]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn inject_targeted_publish_fault_before_visibility(
        &self,
        target_object: String,
    ) -> BackendResult<()> {
        self.arm_targeted_publish_fault(LocalFsPublishStep::TemporarySync, target_object)
    }

    /// Arm a targeted publish fault at the parent-directory fsync (after the object is visible but
    /// before its durability is confirmed) for `target_object`.
    #[cfg(all(test, unix))]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn inject_targeted_publish_fault_visible_unconfirmed(
        &self,
        target_object: String,
    ) -> BackendResult<()> {
        self.arm_targeted_publish_fault(LocalFsPublishStep::ParentSync, target_object)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn inject_temporary_write_publish_fault(&self) -> BackendResult<()> {
        self.arm_publish_fault(LocalFsPublishStep::TemporaryWrite)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn inject_temporary_sync_publish_fault(&self) -> BackendResult<()> {
        self.arm_publish_fault(LocalFsPublishStep::TemporarySync)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn inject_final_publish_fault(&self) -> BackendResult<()> {
        self.arm_publish_fault(LocalFsPublishStep::FinalPublish)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn inject_parent_sync_publish_fault(&self) -> BackendResult<()> {
        self.arm_publish_fault(LocalFsPublishStep::ParentSync)
    }

    #[cfg(all(test, unix))]
    fn injected_publish_fault(
        &self,
        step: LocalFsPublishStep,
        name: &ObjectName,
    ) -> Option<BackendError> {
        let Ok(mut fault) = self.publish_fault.lock() else {
            return Some(BackendError::new(
                BackendErrorKind::Unknown,
                "local filesystem publish fault state is poisoned",
            ));
        };

        let fires = fault.as_ref().is_some_and(|(armed_step, target_object)| {
            *armed_step == step
                && target_object
                    .as_deref()
                    .is_none_or(|target| target == name.as_str())
        });
        if fires {
            *fault = None;
            Some(BackendError::new(
                BackendErrorKind::Interrupted,
                format!("test fault injected at {}", step.name()),
            ))
        } else {
            None
        }
    }

    #[cfg(not(all(test, unix)))]
    fn injected_publish_fault(
        &self,
        _step: LocalFsPublishStep,
        _name: &ObjectName,
    ) -> Option<BackendError> {
        let _ = &self.root;
        None
    }

    #[cfg(all(test, unix))]
    fn arm_delete_fault(&self, step: LocalFsDeleteStep) -> BackendResult<()> {
        let mut fault = self.delete_fault.lock().map_err(|_| {
            BackendError::new(
                BackendErrorKind::Unknown,
                "local filesystem delete fault state is poisoned",
            )
        })?;
        *fault = Some(step);
        Ok(())
    }

    #[cfg(all(test, unix))]
    fn injected_delete_fault(&self, step: LocalFsDeleteStep) -> Option<BackendError> {
        let Ok(mut fault) = self.delete_fault.lock() else {
            return Some(BackendError::new(
                BackendErrorKind::Unknown,
                "local filesystem delete fault state is poisoned",
            ));
        };

        if *fault == Some(step) {
            *fault = None;
            Some(BackendError::new(
                BackendErrorKind::Interrupted,
                format!("test fault injected at {}", step.name()),
            ))
        } else {
            None
        }
    }

    #[cfg(not(all(test, unix)))]
    fn injected_delete_fault(&self, _step: LocalFsDeleteStep) -> Option<BackendError> {
        let _ = &self.root;
        None
    }

    fn path_for(&self, name: &ObjectName) -> PathBuf {
        // Object bytes live in a suffixed file so `tables/a` and
        // `tables/a/child` can coexist on filesystems where a path cannot be
        // both file and directory.
        let mut path = self.root.clone();
        let mut components = name.as_str().split('/').peekable();
        while let Some(component) = components.next() {
            if components.peek().is_some() {
                path.push(component);
            } else {
                path.push(format!("{component}{OBJECT_FILE_SUFFIX}"));
            }
        }
        path
    }

    fn temporary_path_for(path: &Path, sequence: u64) -> BackendResult<PathBuf> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                BackendError::new(
                    BackendErrorKind::InvalidObjectName,
                    format!("object path {} has no file name", path.display()),
                )
            })?;

        Ok(path.with_file_name(format!(
            "{file_name}.tmp.{:x}.{sequence:016x}",
            std::process::id()
        )))
    }

    fn create_temporary_file(path: &Path) -> BackendResult<(PathBuf, File)> {
        // Temporary names include process id and a local sequence. Retrying a
        // bounded number of create_new attempts preserves no-clobber behavior
        // without assuming the directory is free of stale temp files.
        for _ in 0..16 {
            let sequence = TEMP_OBJECT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let temp_path = Self::temporary_path_for(path, sequence)?;
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
            {
                Ok(file) => return Ok((temp_path, file)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(map_io_error(&error)),
            }
        }

        // Retry exhaustion is treated as a collision error instead of deleting
        // matching temp files here. A matching temp path can belong to an
        // in-flight publish in the same process; cleanup belongs to a future
        // operator-visible maintenance pass that can prove staleness.
        Err(BackendError::new(
            BackendErrorKind::AlreadyExists,
            format!(
                "could not allocate a temporary object path for {}",
                path.display()
            ),
        ))
    }

    fn ensure_parent_dirs(&self, parent: &Path, create_missing: bool) -> BackendResult<()> {
        self.ensure_root_dir(create_missing)?;
        let relative = parent.strip_prefix(&self.root).map_err(|_| {
            BackendError::new(
                BackendErrorKind::Corruption,
                format!("path {} escaped backend root", parent.display()),
            )
        })?;

        let mut current = self.root.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("path {} contains a non-normal component", parent.display()),
                ));
            };
            current.push(part);
            Self::ensure_dir(&current, create_missing)?;
        }

        Ok(())
    }

    fn ensure_root_dir(&self, create_missing: bool) -> BackendResult<()> {
        Self::ensure_dir(&self.root, create_missing)
    }

    fn ensure_dir(path: &Path, create_missing: bool) -> BackendResult<()> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("directory path {} is a symlink", path.display()),
            )),
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("directory path {} is not a directory", path.display()),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create_missing => {
                // #2799: a concurrent publish into the same fresh directory
                // can win the stat->create_dir window; the directory existing
                // is this call's goal, so EEXIST is success, not failure. The
                // re-verify below still rejects a file or symlink that
                // appeared instead.
                match fs::create_dir(path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(err) => return Err(map_io_error(&err)),
                }
                Self::ensure_dir(path, false)
            }
            Err(error) => Err(map_io_error(&error)),
        }
    }

    fn metadata_for_object_path(&self, path: &Path) -> BackendResult<BackendMetadata> {
        if let Some(parent) = path.parent() {
            self.ensure_parent_dirs(parent, false)?;
        }

        let metadata = fs::symlink_metadata(path).map_err(|err| map_io_error(&err))?;
        if metadata.file_type().is_symlink() {
            return Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("object path {} is a symlink", path.display()),
            ));
        }
        if !metadata.is_file() {
            return Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("object path {} is not a file", path.display()),
            ));
        }
        Ok(BackendMetadata::new(metadata.len(), None))
    }

    fn validate_optional_file_path(path: &Path) -> BackendResult<Option<BackendMetadata>> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("object path {} is a symlink", path.display()),
            )),
            Ok(metadata) if !metadata.is_file() => Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("object path {} is not a file", path.display()),
            )),
            Ok(metadata) => Ok(Some(BackendMetadata::new(metadata.len(), None))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(map_io_error(&error)),
        }
    }

    fn publish_error(
        name: &ObjectName,
        kind: PublishFailureKind,
        error: BackendError,
    ) -> PublishError {
        PublishError::new(name.clone(), kind, error)
    }

    fn cleanup_temporary_path(path: &Path) {
        // Best-effort temp removal must not mask the primary publish failure.
        let _ = fs::remove_file(path);
    }

    fn prepare_publish_target(
        &self,
        name: &ObjectName,
        mode: PublishMode,
    ) -> PublishResult<PathBuf> {
        let final_path = self.path_for(name);
        if let Some(parent) = final_path.parent() {
            self.ensure_parent_dirs(parent, true).map_err(|error| {
                Self::publish_error(name, PublishFailureKind::FailedBeforeVisibility, error)
            })?;
        }

        let target_metadata = Self::validate_optional_file_path(&final_path).map_err(|error| {
            Self::publish_error(name, PublishFailureKind::FailedBeforeVisibility, error)
        })?;
        // Create mode must fail before visibility if the target already
        // exists; replace mode is allowed to publish over an existing object.
        if mode == PublishMode::Create && target_metadata.is_some() {
            return Err(PublishError::precondition_failed(
                name,
                format!("object {name} already exists"),
            ));
        }

        Ok(final_path)
    }

    fn write_publish_temporary_object(
        &self,
        name: &ObjectName,
        final_path: &Path,
        bytes: &[u8],
    ) -> PublishResult<PathBuf> {
        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::TemporaryCreate, name)
        {
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }

        let (temp_path, mut file) = Self::create_temporary_file(final_path).map_err(|error| {
            Self::publish_error(name, PublishFailureKind::FailedBeforeVisibility, error)
        })?;

        // The temporary object is not reachable by object name. Failures before
        // the final install are therefore classified as before-visibility.
        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::TemporaryWrite, name) {
            Self::cleanup_temporary_path(&temp_path);
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }

        if let Err(error) = file.write_all(bytes).map_err(|err| map_io_error(&err)) {
            Self::cleanup_temporary_path(&temp_path);
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }

        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::TemporarySync, name) {
            Self::cleanup_temporary_path(&temp_path);
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }

        // fsync the temporary file before publishing it. After this point the
        // remaining uncertainty is about visibility or parent-directory
        // durability, not about whether the temp file bytes reached storage.
        if let Err(error) = file.sync_all().map_err(|err| map_io_error(&err)) {
            Self::cleanup_temporary_path(&temp_path);
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }

        drop(file);
        Ok(temp_path)
    }

    fn install_publish_temporary_object(
        &self,
        name: &ObjectName,
        mode: PublishMode,
        temp_path: &Path,
        final_path: &Path,
    ) -> PublishResult<()> {
        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::FinalPublish, name) {
            Self::cleanup_temporary_path(temp_path);
            let kind = if mode == PublishMode::Replace {
                PublishFailureKind::VisibilityUnknown
            } else {
                PublishFailureKind::FailedBeforeVisibility
            };
            return Err(Self::publish_error(name, kind, error));
        }

        if mode == PublishMode::Create {
            // A hard link gives create-only no-clobber semantics: success makes
            // the temp bytes visible at the final path; AlreadyExists remains a
            // precondition failure.
            if let Err(error) = fs::hard_link(temp_path, final_path) {
                Self::cleanup_temporary_path(temp_path);
                let error = map_io_error(&error);
                if error.kind() == BackendErrorKind::AlreadyExists {
                    return Err(PublishError::precondition_failed(
                        name,
                        format!("object {name} already exists"),
                    ));
                }
                return Err(Self::publish_error(
                    name,
                    PublishFailureKind::FailedBeforeVisibility,
                    error,
                ));
            }
            Self::cleanup_temporary_path(temp_path);
            Ok(())
        } else {
            Self::assert_wal_retention_mutation_authorized(
                name,
                "rename",
                "LocalFsBackend::install_publish_temporary_object",
            );
            if let Err(error) = fs::rename(temp_path, final_path) {
                // rename failure for replace is ambiguous across platforms and
                // filesystems: the caller must not assume either old or new bytes.
                Self::cleanup_temporary_path(temp_path);
                Err(Self::publish_error(
                    name,
                    PublishFailureKind::VisibilityUnknown,
                    map_io_error(&error),
                ))
            } else {
                Ok(())
            }
        }
    }

    fn sync_publish_parent(&self, name: &ObjectName, final_path: &Path) -> PublishResult<()> {
        let Some(parent) = final_path.parent() else {
            return Ok(());
        };

        // The object is visible before the parent directory is synced. A
        // failure here leaves visibility known but durability unconfirmed.
        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::ParentSync, name) {
            return Err(Self::publish_error(
                name,
                PublishFailureKind::VisibleDurabilityUnconfirmed,
                error,
            ));
        }

        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| {
                Self::publish_error(
                    name,
                    PublishFailureKind::VisibleDurabilityUnconfirmed,
                    map_io_error(&err),
                )
            })
    }

    fn sync_delete_parent(&self, name: &ObjectName, path: &Path) -> DeleteResult {
        if !cfg!(unix) {
            return Ok(DeleteOutcome::deleted(
                name.clone(),
                DeleteDurability::NonDurable,
            ));
        }

        let Some(parent) = path.parent() else {
            return Ok(DeleteOutcome::deleted(
                name.clone(),
                DeleteDurability::Durable,
            ));
        };

        if let Some(error) = self.injected_delete_fault(LocalFsDeleteStep::ParentSync) {
            return Err(DeleteError::removed_durability_unconfirmed(name, error));
        }

        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| DeleteError::removed_durability_unconfirmed(name, map_io_error(&err)))?;

        Ok(DeleteOutcome::deleted(
            name.clone(),
            DeleteDurability::Durable,
        ))
    }

    fn name_from_path(&self, path: &Path) -> BackendResult<ObjectName> {
        let relative = path.strip_prefix(&self.root).map_err(|_| {
            BackendError::new(
                BackendErrorKind::Corruption,
                format!("path {} escaped backend root", path.display()),
            )
        })?;

        let mut parts = Vec::new();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("path {} contains a non-normal component", path.display()),
                ));
            };
            let Some(part) = part.to_str() else {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("path {} contains non-UTF-8 data", path.display()),
                ));
            };
            parts.push(part.to_owned());
        }

        let Some(last) = parts.last_mut() else {
            return Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!("path {} does not name an object file", path.display()),
            ));
        };
        // Only files with the object suffix map back to ObjectName values; this
        // prevents directory-only prefixes and temporary files from leaking
        // into backend listings.
        let Some(stem) = last.strip_suffix(OBJECT_FILE_SUFFIX) else {
            return Err(BackendError::new(
                BackendErrorKind::Corruption,
                format!(
                    "path {} does not use the object-file suffix",
                    path.display()
                ),
            ));
        };
        *last = stem.to_owned();

        ObjectName::new(parts.join("/")).map_err(|err| {
            BackendError::new(
                BackendErrorKind::Corruption,
                format!("path {} is not a valid object name: {err}", path.display()),
            )
        })
    }

    fn collect_files(&self, dir: &Path, files: &mut Vec<ObjectName>) -> BackendResult<()> {
        match fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|err| map_io_error(&err))?;
                    let path = entry.path();
                    // TCP4.15: an entry can vanish between the readdir yield
                    // and this type probe (ext4's d_type masks the race; a
                    // non-d_type filesystem stats here). A vanished entry is
                    // an absence, not a listing failure — the snapshot is
                    // documented as fuzzy against concurrent mutators.
                    let Some(file_type) = classify_entry_type(entry.file_type())? else {
                        continue;
                    };
                    if file_type.is_symlink() {
                        return Err(BackendError::new(
                            BackendErrorKind::Corruption,
                            format!("path {} is a symlink", path.display()),
                        ));
                    } else if file_type.is_dir() {
                        self.collect_files(&path, files)?;
                    } else if file_type.is_file() && is_object_file_path(&path) {
                        // Unknown files are ignored; malformed object-suffixed
                        // files are corruption because they could shadow a
                        // backend object.
                        files.push(self.name_from_path(&path)?);
                    }
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(map_io_error(&err)),
        }
    }
}

/// Persistent append handle over a single WAL segment. Holds the `O_APPEND`
/// descriptor open and tracks the on-disk size in memory so each append is a
/// single `write` (no per-call stat/open/close). Single-writer exclusion is
/// provided by the durable lifecycle's writer lock; `write_all` guarantees the
/// full frame is written, so `before + len` is the authoritative post-append size.
struct LocalFsAppendHandle {
    file: File,
    size: u64,
}

impl BackendAppendHandle for LocalFsAppendHandle {
    fn append(&mut self, bytes: &[u8]) -> BackendResult<BackendAppend> {
        let before = self.size;
        self.file
            .write_all(bytes)
            .map_err(|err| map_io_error(&err))?;
        self.size = self.size.saturating_add(bytes.len() as u64);
        Ok(BackendAppend::new(
            before,
            bytes.len() as u64,
            BackendMetadata::new(self.size, None),
        ))
    }

    fn sync(&mut self) -> BackendResult<()> {
        self.file.sync_all().map_err(|err| map_io_error(&err))
    }
}

impl Backend for LocalFsBackend {
    fn capabilities(&self) -> BackendCapabilities {
        let mut capabilities = BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES);
        capabilities.insert(super::BackendCapability::AppendObject);
        capabilities.insert(super::BackendCapability::SingleWriterLock);
        if cfg!(unix) {
            capabilities.insert(super::BackendCapability::DurablePublish);
            capabilities.insert(super::BackendCapability::DurableSync);
        }
        capabilities
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        let path = self.path_for(name);
        self.metadata_for_object_path(&path)?;
        fs::read(path).map_err(|err| map_io_error(&err))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let Some(end_offset) = range.end_offset() else {
            return Err(BackendError::new(
                BackendErrorKind::InvalidRange,
                format!("range {}.. overflows for object {name}", range.offset()),
            ));
        };

        let path = self.path_for(name);
        self.metadata_for_object_path(&path)?;
        let mut file = File::open(path).map_err(|err| map_io_error(&err))?;
        file.seek(SeekFrom::Start(range.offset()))
            .map_err(|err| map_io_error(&err))?;
        let mut bytes = Vec::new();
        file.take(end_offset.saturating_sub(range.offset()))
            .read_to_end(&mut bytes)
            .map_err(|err| map_io_error(&err))?;
        Ok(bytes)
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        Self::reject_writer_lock_object_mutation(name, "write")?;
        let path = self.path_for(name);
        if let Some(parent) = path.parent() {
            self.ensure_parent_dirs(parent, true)?;
        }
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("object path {} is a symlink", path.display()),
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("object path {} is not a file", path.display()),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(map_io_error(&error)),
        }
        fs::write(&path, bytes).map_err(|err| map_io_error(&err))?;
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> DeleteResult {
        Self::reject_writer_lock_object_mutation(name, "delete")
            .map_err(|error| DeleteError::failed_before_removal(name, error))?;
        Self::assert_wal_retention_mutation_authorized(
            name,
            "delete",
            "LocalFsBackend::delete_object",
        );
        let path = self.path_for(name);
        if let Some(parent) = path.parent() {
            match self.ensure_parent_dirs(parent, false) {
                Ok(()) => {}
                Err(error) if error.kind() == BackendErrorKind::NotFound => {
                    return Ok(DeleteOutcome::already_missing(
                        name.clone(),
                        DeleteDurability::NonDurable,
                    ));
                }
                Err(error) => return Err(DeleteError::failed_before_removal(name, error)),
            }
        }

        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(DeleteError::failed_before_removal(
                    name,
                    BackendError::new(
                        BackendErrorKind::Corruption,
                        format!("object path {} is a symlink", path.display()),
                    ),
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(DeleteError::failed_before_removal(
                    name,
                    BackendError::new(
                        BackendErrorKind::Corruption,
                        format!("object path {} is not a file", path.display()),
                    ),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DeleteOutcome::already_missing(
                    name.clone(),
                    DeleteDurability::NonDurable,
                ));
            }
            Err(error) => {
                return Err(DeleteError::failed_before_removal(
                    name,
                    map_io_error(&error),
                ))
            }
        }

        if let Some(error) = self.injected_delete_fault(LocalFsDeleteStep::BeforeRemoval) {
            return Err(DeleteError::failed_before_removal(name, error));
        }
        if let Some(error) = self.injected_delete_fault(LocalFsDeleteStep::Removal) {
            return Err(DeleteError::removal_unknown(name, error));
        }

        match fs::remove_file(&path) {
            Ok(()) => {}
            // TCP4.15: a concurrent deleter can win between the stat above and
            // this unlink; absence is this call's goal, exactly like the two
            // already-missing arms above (the winner's parent sync carries the
            // durability of the removal).
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DeleteOutcome::already_missing(
                    name.clone(),
                    DeleteDurability::NonDurable,
                ));
            }
            Err(err) => return Err(DeleteError::removal_unknown(name, map_io_error(&err))),
        }
        self.sync_delete_parent(name, &path)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        match fs::symlink_metadata(&self.root) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("directory path {} is a symlink", self.root.display()),
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("directory path {} is not a directory", self.root.display()),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(map_io_error(&error)),
        }

        let mut names = Vec::new();
        self.collect_files(&self.root, &mut names)?;
        names.retain(|name| name.as_str().starts_with(prefix.as_str()));
        names.sort();
        Ok(names)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.metadata_for_object_path(&self.path_for(name))
    }

    fn acquire_writer_lock(&self, name: &ObjectName) -> BackendResult<BackendWriterGuard> {
        Self::require_writer_lock_object(name)?;
        let path = self.path_for(name);
        if let Some(parent) = path.parent() {
            self.ensure_parent_dirs(parent, true)?;
        }
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("writer lock path {} is a symlink", path.display()),
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(BackendError::new(
                    BackendErrorKind::Corruption,
                    format!("writer lock path {} is not a file", path.display()),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(map_io_error(&error)),
        }

        // The lock file is a normal backend object so object-family scans can
        // see `locks/writer`, while the held file descriptor carries the actual
        // OS advisory exclusion.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|err| map_io_error(&err))?;
        file.try_lock_exclusive().map_err(|err| {
            // Contention is NOT a backend outage. `map_io_error` folds
            // `WouldBlock` into `Unavailable`, which upstream reads as a
            // transient failure worth retrying with the same request — but the
            // lock is released by another opener, never by retrying. Classify
            // it here rather than in `map_io_error`, whose `WouldBlock` mapping
            // is correct for every other call site (#3005, #3167).
            if err.kind() == std::io::ErrorKind::WouldBlock {
                BackendError::new(
                    BackendErrorKind::AlreadyExists,
                    "writer lock is held by another opener",
                )
            } else {
                map_io_error(&err)
            }
        })?;
        Ok(BackendWriterGuard::new(name.clone(), file))
    }

    fn append_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendAppend> {
        Self::reject_writer_lock_object_mutation(name, "append")?;
        let path = self.path_for(name);
        let before = self.metadata_for_object_path(&path)?.size_bytes();
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|err| map_io_error(&err))?;
        file.write_all(bytes).map_err(|err| map_io_error(&err))?;
        let after = self.metadata_for_object_path(&path)?;
        Ok(BackendAppend::new(before, bytes.len() as u64, after))
    }

    fn sync_object(&self, name: &ObjectName) -> BackendResult<()> {
        let path = self.path_for(name);
        self.metadata_for_object_path(&path)?;
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|err| map_io_error(&err))
    }

    fn open_append_handle(
        &self,
        name: &ObjectName,
        expected_size: u64,
    ) -> BackendResult<Option<Box<dyn BackendAppendHandle>>> {
        Self::reject_writer_lock_object_mutation(name, "append")?;
        let path = self.path_for(name);
        // The caller (WalService) reconciles `expected_size` against the backend
        // via its own boundary stat right after opening, so the handle trusts the
        // passed size and starts tracking from there — no stat here.
        let file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|err| map_io_error(&err))?;
        Ok(Some(Box::new(LocalFsAppendHandle {
            file,
            size: expected_size,
        })))
    }

    fn publish_object(
        &self,
        name: &ObjectName,
        bytes: &[u8],
        mode: PublishMode,
    ) -> PublishResult<PublishOutcome> {
        if let Err(error) = Self::reject_writer_lock_object_mutation(name, "publish") {
            return Err(Self::publish_error(
                name,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }
        if mode == PublishMode::NonDurableReplace {
            return Err(PublishError::unsupported(
                name,
                BackendError::unsupported(super::BackendCapability::DurablePublish),
            ));
        }
        if !cfg!(unix) {
            return Err(PublishError::unsupported(
                name,
                BackendError::unsupported(super::BackendCapability::DurablePublish),
            ));
        }

        let final_path = self.prepare_publish_target(name, mode)?;
        let temp_path = self.write_publish_temporary_object(name, &final_path, bytes)?;
        self.install_publish_temporary_object(name, mode, &temp_path, &final_path)?;
        self.sync_publish_parent(name, &final_path)?;

        Ok(PublishOutcome::new(
            name.clone(),
            BackendMetadata::new(bytes.len() as u64, None),
            PublishDurability::Durable,
        ))
    }
}

/// The fuzzy-snapshot rule for a directory entry's type probe: a vanished
/// entry (`NotFound`) is `Ok(None)` — skip it — while every other probe
/// failure is a real listing error. Pure so the boundary is truth-tabled
/// (the `NotFound` arm is dead code on `d_type` filesystems and only a unit
/// test can prove it).
fn classify_entry_type(
    probe: std::io::Result<std::fs::FileType>,
) -> BackendResult<Option<std::fs::FileType>> {
    match probe {
        Ok(file_type) => Ok(Some(file_type)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(map_io_error(&err)),
    }
}

fn is_object_file_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(OBJECT_FILE_SUFFIX))
}

fn map_io_error(error: &std::io::Error) -> BackendError {
    let kind = match error.kind() {
        std::io::ErrorKind::NotFound => BackendErrorKind::NotFound,
        std::io::ErrorKind::AlreadyExists => BackendErrorKind::AlreadyExists,
        std::io::ErrorKind::PermissionDenied => BackendErrorKind::PermissionDenied,
        std::io::ErrorKind::Interrupted => BackendErrorKind::Interrupted,
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
            BackendErrorKind::Unavailable
        }
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof => {
            BackendErrorKind::Corruption
        }
        std::io::ErrorKind::InvalidInput => BackendErrorKind::InvalidObjectName,
        std::io::ErrorKind::StorageFull => BackendErrorKind::NoSpace,
        _ => BackendErrorKind::Unknown,
    };
    BackendError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::LocalFsBackend;
    #[cfg(unix)]
    use super::LocalFsDeleteStep;
    #[cfg(unix)]
    use super::LocalFsPublishStep;
    #[cfg(unix)]
    use crate::backend::PublishDurability;
    use crate::backend::{
        Backend, BackendCapability, BackendErrorKind, BackendRange, DeleteDurability,
        DeleteFailureKind, DeleteStatus, PublishFailureKind, PublishMode,
        BASIC_OBJECT_BACKEND_CAPABILITIES, CACHE_MODE_REQUIREMENTS,
    };
    use crate::config::mode::{DurabilityPolicy, StorageModeRequest};
    use crate::layout::ObjectLayout;
    use crate::object::{ObjectName, ObjectPrefix};
    #[cfg(unix)]
    use std::path::PathBuf;

    #[cfg(unix)]
    fn temporary_entries(backend: &LocalFsBackend, name: &ObjectName) -> Vec<PathBuf> {
        let final_path = backend.path_for(name);
        let parent = final_path.parent().expect("object parent");
        match std::fs::read_dir(parent) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.contains(".tmp."))
                })
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("could not read object parent {}: {error}", parent.display()),
        }
    }

    #[cfg(unix)]
    fn assert_no_temporary_entries(backend: &LocalFsBackend, name: &ObjectName) {
        let entries = temporary_entries(backend, name);
        assert!(
            entries.is_empty(),
            "publish left temporary entries: {entries:?}"
        );
    }

    #[test]
    fn localfs_backend_reports_object_and_durable_unix_capabilities() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let capabilities = backend.capabilities();

        assert_eq!(backend.root(), dir.path());
        assert!(capabilities.supports(CACHE_MODE_REQUIREMENTS));
        assert!(capabilities.supports(BASIC_OBJECT_BACKEND_CAPABILITIES));
        assert_eq!(
            capabilities.contains(BackendCapability::DurablePublish),
            cfg!(unix)
        );
        assert!(capabilities.contains(BackendCapability::AppendObject));
        assert_eq!(
            capabilities.contains(BackendCapability::DurableSync),
            cfg!(unix)
        );
        assert!(capabilities.contains(BackendCapability::SingleWriterLock));

        if cfg!(unix) {
            StorageModeRequest::durable_local(DurabilityPolicy::Standard)
                .validate_backend(capabilities)
                .expect("unix localfs backend should satisfy durable local mode");
            StorageModeRequest::durable_local(DurabilityPolicy::Always)
                .validate_backend(capabilities)
                .expect("unix localfs backend should satisfy durable local mode");
        }
    }

    #[test]
    fn localfs_backend_writer_lock_excludes_second_holder_until_released() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first_backend = LocalFsBackend::new(dir.path());
        let second_backend = LocalFsBackend::new(dir.path());
        let lock_name = ObjectLayout::writer_lock().expect("writer lock name");

        let first_guard = first_backend
            .acquire_writer_lock(&lock_name)
            .expect("first writer lock");
        let error = second_backend
            .acquire_writer_lock(&lock_name)
            .expect_err("second writer lock should be unavailable");

        assert_eq!(first_guard.object(), &lock_name);
        // NOT `Unavailable`: this module's own header states the contract —
        // contention is the "another live opener" signal and must never be
        // retried here. `Unavailable` is read upstream as a transient outage
        // worth retrying with the same request, which cannot release a lock
        // another opener holds (#3005, #3167).
        assert_eq!(error.kind(), BackendErrorKind::AlreadyExists);
        assert_eq!(
            first_backend
                .object_metadata(&lock_name)
                .expect("lock file metadata")
                .size_bytes(),
            0
        );

        drop(first_guard);
        let second_guard = second_backend
            .acquire_writer_lock(&lock_name)
            .expect("released writer lock can be reacquired");
        assert_eq!(second_guard.object(), &lock_name);
    }

    #[test]
    fn localfs_backend_writer_lock_requires_reserved_layout_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let wrong_name = ObjectName::new("locks/not-writer").expect("wrong lock name");

        let error = backend
            .acquire_writer_lock(&wrong_name)
            .expect_err("writer lock should reject alternate lock object names");

        assert_eq!(error.kind(), BackendErrorKind::InvalidObjectName);
    }

    #[test]
    fn localfs_backend_writer_lock_object_rejects_generic_mutations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first_backend = LocalFsBackend::new(dir.path());
        let second_backend = LocalFsBackend::new(dir.path());
        let lock_name = ObjectLayout::writer_lock().expect("writer lock name");
        let guard = first_backend
            .acquire_writer_lock(&lock_name)
            .expect("first writer lock");

        let write_error = first_backend
            .write_object(&lock_name, b"tamper")
            .expect_err("writer lock object should reject writes");
        let append_error = first_backend
            .append_object(&lock_name, b"tamper")
            .expect_err("writer lock object should reject appends");
        let delete_error = first_backend
            .delete_object(&lock_name)
            .expect_err("writer lock object should reject deletes");
        let publish_error = first_backend
            .publish_object(&lock_name, b"tamper", PublishMode::Replace)
            .expect_err("writer lock object should reject publish replacement");

        assert_eq!(write_error.kind(), BackendErrorKind::PermissionDenied);
        assert_eq!(append_error.kind(), BackendErrorKind::PermissionDenied);
        assert_eq!(delete_error.kind(), DeleteFailureKind::FailedBeforeRemoval);
        assert_eq!(
            delete_error.source_error().kind(),
            BackendErrorKind::PermissionDenied
        );
        assert_eq!(
            publish_error.kind(),
            PublishFailureKind::FailedBeforeVisibility
        );
        assert_eq!(
            publish_error.source_error().kind(),
            BackendErrorKind::PermissionDenied
        );

        let contention = second_backend
            .acquire_writer_lock(&lock_name)
            .expect_err("generic mutation attempts must not bypass held lock");
        assert_eq!(contention.kind(), BackendErrorKind::AlreadyExists);

        drop(guard);
        let second_guard = second_backend
            .acquire_writer_lock(&lock_name)
            .expect("released writer lock can still be acquired");
        assert_eq!(second_guard.object(), &lock_name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_writer_lock_rejects_symlink_lock_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let lock_name = ObjectLayout::writer_lock().expect("writer lock name");
        let lock_path = backend.path_for(&lock_name);
        std::fs::create_dir_all(lock_path.parent().expect("lock parent")).expect("lock parent");
        std::os::unix::fs::symlink(dir.path().join("target"), &lock_path).expect("symlink");

        let error = backend
            .acquire_writer_lock(&lock_name)
            .expect_err("symlink lock file should be rejected");

        assert_eq!(error.kind(), BackendErrorKind::Corruption);
    }

    #[test]
    fn metadata_probe_does_not_create_missing_parent_directories() {
        // A read-only probe walks parents with create_missing=false: it must
        // report absence, never manufacture the directory chain.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("ghost/sub/object").expect("name");
        let error = backend.object_metadata(&name).expect_err("missing object");
        assert_eq!(error.kind(), BackendErrorKind::NotFound);
        assert!(
            !dir.path().join("ghost").exists(),
            "a metadata probe created parent directories"
        );
    }

    #[cfg(unix)]
    #[test]
    fn publish_surfaces_the_real_directory_creation_error() {
        use std::os::unix::fs::PermissionsExt;
        // When create_dir fails for a real reason, that error must surface —
        // not a follow-on NotFound from treating the failure as success.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("locked dir");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555))
            .expect("read-only");
        let name = ObjectName::new("locked/child/object").expect("name");
        let error = backend
            .publish_object(&name, b"bytes", PublishMode::Create)
            .expect_err("publish into a read-only parent must fail");
        assert_eq!(
            error.source_error().kind(),
            BackendErrorKind::PermissionDenied,
            "{error:?}"
        );
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
            .expect("restore permissions");
    }

    #[test]
    fn entry_type_probe_truth_table() {
        // The NotFound arm is dead code on d_type filesystems (ext4 answers
        // from the dirent), so only this table proves the boundary.
        let vanished = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert!(matches!(
            super::classify_entry_type(Err(vanished)),
            Ok(None)
        ));
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert!(super::classify_entry_type(Err(denied)).is_err());
        let file_type = std::fs::metadata(std::env::temp_dir())
            .expect("temp dir metadata")
            .file_type();
        assert!(matches!(
            super::classify_entry_type(Ok(file_type)),
            Ok(Some(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn delete_surfaces_the_real_unlink_error() {
        use std::os::unix::fs::PermissionsExt;
        // A real unlink failure (EACCES on a read-only parent) must stay an
        // ambiguous removal error — never be misread as absence.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("sealed/victim").expect("name");
        backend
            .publish_object(&name, b"victim", PublishMode::Create)
            .expect("stage victim");
        let sealed = dir.path().join("sealed");
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o555))
            .expect("read-only parent");
        let error = backend
            .delete_object(&name)
            .expect_err("unlink in a read-only parent must fail");
        assert_eq!(error.kind(), DeleteFailureKind::RemovalUnknown, "{error:?}");
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o755))
            .expect("restore permissions");
    }

    #[test]
    fn concurrent_deletes_of_the_same_object_are_idempotent() {
        // Two racers deleting one object must BOTH succeed — one as the
        // deleter, the loser as already-missing. Losing the stat->unlink
        // window is an absence, never an ambiguous removal (TCP4.15).
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        for round in 0..200 {
            let name = ObjectName::new(format!("race/del{round}/victim")).expect("name");
            backend
                .publish_object(&name, b"victim", PublishMode::Create)
                .expect("stage victim");
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let backend = &backend;
                let barrier = &barrier;
                let name = &name;
                let first = scope.spawn(move || {
                    barrier.wait();
                    backend.delete_object(name)
                });
                let second = scope.spawn(move || {
                    barrier.wait();
                    backend.delete_object(name)
                });
                let outcomes = [
                    first.join().expect("first deleter"),
                    second.join().expect("second deleter"),
                ];
                for outcome in outcomes {
                    outcome.unwrap_or_else(|error| {
                        panic!("round {round}: concurrent delete not idempotent: {error:?}")
                    });
                }
            });
        }
    }

    #[test]
    fn listing_stays_clean_while_objects_churn() {
        // The listing snapshot is documented as fuzzy against concurrent
        // mutators: entries may or may not appear, but a vanished entry is
        // never a listing FAILURE (TCP4.15 contract pin; the d_type-less
        // stat path is the latent hazard this protects).
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let prefix = ObjectPrefix::new("churn/".to_owned()).expect("prefix");
        let stop = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            let backend = &backend;
            let stop = &stop;
            let churn = scope.spawn(move || {
                for round in 0..400u32 {
                    let name = ObjectName::new(format!("churn/c{}/obj", round % 8)).expect("name");
                    let _ = backend.publish_object(&name, b"x", PublishMode::Replace);
                    let _ = backend.delete_object(&name);
                }
                stop.store(true, std::sync::atomic::Ordering::Release);
            });
            let mut listings = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                backend
                    .list_prefix(&prefix)
                    .unwrap_or_else(|error| panic!("listing failed under churn: {error:?}"));
                listings += 1;
            }
            churn.join().expect("churn thread");
            assert!(listings > 0, "vacuous: no listing raced the churn");
        });
    }

    #[test]
    fn concurrent_publishes_into_a_fresh_directory_all_succeed() {
        // Two workers publishing the first objects into a directory that does
        // not exist yet must both succeed: losing the parent-directory
        // creation race is not a publish failure. Barrier-synced rounds put
        // both threads inside the stat->create_dir window reliably.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        for round in 0..200 {
            let first = ObjectName::new(format!("race/r{round}/first")).expect("first name");
            let second = ObjectName::new(format!("race/r{round}/second")).expect("second name");
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let backend = &backend;
                let barrier = &barrier;
                let publish_first = scope.spawn(move || {
                    barrier.wait();
                    backend.publish_object(&first, b"first", PublishMode::Create)
                });
                let publish_second = scope.spawn(move || {
                    barrier.wait();
                    backend.publish_object(&second, b"second", PublishMode::Create)
                });
                publish_first
                    .join()
                    .expect("first publish thread")
                    .unwrap_or_else(|error| panic!("round {round} first publish: {error:?}"));
                publish_second
                    .join()
                    .expect("second publish thread")
                    .unwrap_or_else(|error| panic!("round {round} second publish: {error:?}"));
            });
        }
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publishes_object_durably() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        let outcome = backend
            .publish_object(&name, b"manifest", PublishMode::Replace)
            .expect("publish");

        assert_eq!(outcome.object(), &name);
        assert_eq!(outcome.metadata().size_bytes(), 8);
        assert_eq!(outcome.durability(), PublishDurability::Durable);
        assert_eq!(backend.read_object(&name).expect("read"), b"manifest");
        let final_path = backend.path_for(&name);
        let parent = final_path.parent().expect("parent");
        let temp_entries: Vec<_> = std::fs::read_dir(parent)
            .expect("read parent")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            temp_entries.is_empty(),
            "publish should not leave temporary files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_create_publish_preserves_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        backend.write_object(&name, b"old").expect("seed");
        let error = backend
            .publish_object(&name, b"new", PublishMode::Create)
            .expect_err("create should reject existing object");

        assert_eq!(error.kind(), PublishFailureKind::PreconditionFailed);
        assert_eq!(error.source_error().kind(), BackendErrorKind::AlreadyExists);
        assert_eq!(backend.read_object(&name).expect("old preserved"), b"old");
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_replace_publish_overwrites_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        backend.write_object(&name, b"old").expect("seed");
        let outcome = backend
            .publish_object(&name, b"new manifest", PublishMode::Replace)
            .expect("replace publish");

        assert_eq!(outcome.object(), &name);
        assert_eq!(outcome.metadata().size_bytes(), 12);
        assert_eq!(backend.read_object(&name).expect("read"), b"new manifest");
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_temp_create_fault_leaves_no_visible_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend
            .arm_publish_fault(LocalFsPublishStep::TemporaryCreate)
            .expect("arm fault");

        let error = backend
            .publish_object(&name, b"manifest", PublishMode::Replace)
            .expect_err("publish fault");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(
            backend
                .read_object(&name)
                .expect_err("final object absent")
                .kind(),
            BackendErrorKind::NotFound
        );
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_temp_write_fault_cleans_temp_and_preserves_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"old").expect("seed");
        backend
            .arm_publish_fault(LocalFsPublishStep::TemporaryWrite)
            .expect("arm fault");

        let error = backend
            .publish_object(&name, b"new", PublishMode::Replace)
            .expect_err("publish fault");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(backend.read_object(&name).expect("old preserved"), b"old");
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_temp_sync_fault_cleans_temp_and_preserves_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"old").expect("seed");
        backend
            .arm_publish_fault(LocalFsPublishStep::TemporarySync)
            .expect("arm fault");

        let error = backend
            .publish_object(&name, b"new", PublishMode::Replace)
            .expect_err("publish fault");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(backend.read_object(&name).expect("old preserved"), b"old");
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_final_publish_fault_cleans_temp_and_preserves_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"old").expect("seed");
        backend.inject_final_publish_fault().expect("arm fault");

        let error = backend
            .publish_object(&name, b"new", PublishMode::Replace)
            .expect_err("publish fault");

        assert_eq!(error.kind(), PublishFailureKind::VisibilityUnknown);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(backend.read_object(&name).expect("old preserved"), b"old");
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_create_final_publish_fault_leaves_no_visible_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.inject_final_publish_fault().expect("arm fault");

        let error = backend
            .publish_object(&name, b"new", PublishMode::Create)
            .expect_err("publish fault");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(
            backend
                .read_object(&name)
                .expect_err("final object absent")
                .kind(),
            BackendErrorKind::NotFound
        );
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_parent_sync_fault_leaves_new_object_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"old").expect("seed");
        backend
            .arm_publish_fault(LocalFsPublishStep::ParentSync)
            .expect("arm fault");

        let error = backend
            .publish_object(&name, b"new", PublishMode::Replace)
            .expect_err("publish fault");

        assert_eq!(
            error.kind(),
            PublishFailureKind::VisibleDurabilityUnconfirmed
        );
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(backend.read_object(&name).expect("new visible"), b"new");
        assert_no_temporary_entries(&backend, &name);
    }

    #[cfg(not(unix))]
    #[test]
    fn localfs_backend_rejects_durable_publish_without_unix_directory_sync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        for mode in [PublishMode::Create, PublishMode::Replace] {
            let error = backend
                .publish_object(&name, b"bytes", mode)
                .expect_err("durable publish should require unix directory sync");

            assert_eq!(error.kind(), PublishFailureKind::Unsupported);
            assert_eq!(
                error.source_error().kind(),
                BackendErrorKind::UnsupportedOperation
            );
        }
    }

    #[test]
    fn localfs_backend_rejects_non_durable_publish_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        let error = backend
            .publish_object(&name, b"bytes", PublishMode::NonDurableReplace)
            .expect_err("local durable backend should not claim cache publishing");

        assert_eq!(error.kind(), PublishFailureKind::Unsupported);
        assert_eq!(
            error.source_error().kind(),
            BackendErrorKind::UnsupportedOperation
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_rejects_symlink_object_paths() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        std::fs::write(outside.path(), b"outside").expect("outside write");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("escape").expect("name");

        symlink(outside.path(), backend.path_for(&name)).expect("symlink");
        let error = backend
            .publish_object(&name, b"should not escape", PublishMode::Replace)
            .expect_err("publish symlink");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Corruption);
        assert_eq!(
            std::fs::read(outside.path()).expect("outside read"),
            b"outside"
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_rejects_symlink_parent_paths() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside dir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("tables/object").expect("name");
        symlink(outside.path(), dir.path().join("tables")).expect("symlink dir");

        let error = backend
            .publish_object(&name, b"bytes", PublishMode::Replace)
            .expect_err("publish through symlink parent");

        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Corruption);
        assert!(std::fs::read_dir(outside.path())
            .expect("outside dir")
            .next()
            .is_none());
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_publish_ignores_stale_temporary_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        let final_path = backend.path_for(&name);
        let parent = final_path.parent().expect("parent");
        std::fs::create_dir_all(parent).expect("parent dirs");
        let final_file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("file name");
        let stale_temp = parent.join(format!("{final_file_name}.tmp.stale"));
        std::fs::write(&stale_temp, b"stale").expect("stale temp");

        backend
            .publish_object(&name, b"manifest", PublishMode::Replace)
            .expect("publish");

        assert_eq!(backend.read_object(&name).expect("read"), b"manifest");
        assert_eq!(
            std::fs::read(&stale_temp).expect("stale temp preserved"),
            b"stale"
        );
        let listed = backend
            .list_prefix(&ObjectPrefix::new("manifest/").expect("prefix"))
            .expect("list");
        assert_eq!(listed, vec![name]);
    }

    #[test]
    fn localfs_backend_round_trips_object_bytes_and_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("tables/main/object").expect("valid object name");

        let metadata = backend
            .write_object(&name, b"abcdef")
            .expect("write should succeed");

        assert_eq!(metadata.size_bytes(), 6);
        assert_eq!(backend.read_object(&name).expect("read object"), b"abcdef");
        assert_eq!(
            backend
                .read_range(&name, BackendRange::new(2, 3))
                .expect("read range"),
            b"cde"
        );
        assert_eq!(
            backend
                .object_metadata(&name)
                .expect("metadata")
                .size_bytes(),
            6
        );
    }

    #[test]
    fn localfs_backend_appends_to_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("wal/0000000000000001").expect("valid object name");
        backend.write_object(&name, b"abc").expect("seed");

        let append = backend.append_object(&name, b"def").expect("append");

        assert_eq!(append.start_offset(), 3);
        assert_eq!(append.bytes_written(), 3);
        assert_eq!(append.metadata().size_bytes(), 6);
        assert_eq!(backend.read_object(&name).expect("read"), b"abcdef");
    }

    #[test]
    fn localfs_backend_append_requires_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("wal/0000000000000001").expect("valid object name");

        assert_eq!(
            backend
                .append_object(&name, b"def")
                .expect_err("missing object")
                .kind(),
            BackendErrorKind::NotFound
        );
    }

    #[test]
    fn localfs_backend_sync_requires_existing_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("wal/0000000000000001").expect("valid object name");

        assert_eq!(
            backend
                .sync_object(&name)
                .expect_err("missing object")
                .kind(),
            BackendErrorKind::NotFound
        );
        backend.write_object(&name, b"abc").expect("write");
        backend.sync_object(&name).expect("sync existing object");
    }

    #[test]
    fn localfs_backend_range_reads_are_bounded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("tables/main/object").expect("valid object name");
        backend.write_object(&name, b"abc").expect("write");

        assert_eq!(
            backend
                .read_range(&name, BackendRange::new(2, 20))
                .expect("range truncates"),
            b"c"
        );
        assert_eq!(
            backend
                .read_range(&name, BackendRange::new(3, 1))
                .expect("range at end"),
            b""
        );
        assert_eq!(
            backend
                .read_range(&name, BackendRange::new(u64::MAX, 1))
                .expect_err("overflow rejected")
                .kind(),
            BackendErrorKind::InvalidRange
        );
    }

    #[test]
    fn localfs_backend_lists_prefixes_in_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let names = [
            ObjectName::new("tables/a/002").expect("name"),
            ObjectName::new("tables/a/001").expect("name"),
            ObjectName::new("tables/b/001").expect("name"),
        ];
        for name in &names {
            backend
                .write_object(name, name.as_str().as_bytes())
                .expect("write");
        }

        let prefix = ObjectPrefix::new("tables/a/").expect("prefix");
        let listed = backend.list_prefix(&prefix).expect("list prefix");
        let listed: Vec<_> = listed.iter().map(ObjectName::as_str).collect();

        assert_eq!(listed, vec!["tables/a/001", "tables/a/002"]);
    }

    #[test]
    fn localfs_backend_can_store_object_and_child_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let parent = ObjectName::new("tables/a").expect("parent name");
        let child = ObjectName::new("tables/a/child").expect("child name");

        backend
            .write_object(&parent, b"parent")
            .expect("parent write");
        backend.write_object(&child, b"child").expect("child write");

        assert_eq!(
            backend.read_object(&parent).expect("parent read"),
            b"parent"
        );
        assert_eq!(backend.read_object(&child).expect("child read"), b"child");

        let all = backend
            .list_prefix(&ObjectPrefix::new("").expect("all prefix"))
            .expect("list all");
        let all: Vec<_> = all.iter().map(ObjectName::as_str).collect();

        assert_eq!(all, vec!["tables/a", "tables/a/child"]);
    }

    #[test]
    fn localfs_backend_missing_paths_are_classified() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        assert_eq!(
            backend.read_object(&name).expect_err("missing read").kind(),
            BackendErrorKind::NotFound
        );
        let missing_delete = backend.delete_object(&name).expect("missing delete");
        assert_eq!(missing_delete.object(), &name);
        assert_eq!(missing_delete.status(), DeleteStatus::AlreadyMissing);
        assert_eq!(missing_delete.durability(), DeleteDurability::NonDurable);
        assert!(backend
            .list_prefix(&ObjectPrefix::new("manifest/").expect("prefix"))
            .expect("list")
            .is_empty());
    }

    #[test]
    fn localfs_backend_delete_removes_object_and_reports_mode_durability() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");

        backend.write_object(&name, b"manifest").expect("write");
        let outcome = backend.delete_object(&name).expect("delete");

        assert_eq!(outcome.object(), &name);
        assert_eq!(outcome.status(), DeleteStatus::Deleted);
        assert_eq!(
            outcome.durability(),
            if cfg!(unix) {
                DeleteDurability::Durable
            } else {
                DeleteDurability::NonDurable
            }
        );
        assert_eq!(
            backend.read_object(&name).expect_err("deleted").kind(),
            BackendErrorKind::NotFound
        );

        let reopened = LocalFsBackend::new(dir.path());
        assert_eq!(
            reopened
                .read_object(&name)
                .expect_err("delete survives reopen")
                .kind(),
            BackendErrorKind::NotFound
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "unexpected WAL retention mutation")]
    fn localfs_backend_debug_tripwire_rejects_unscoped_wal_segment_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectLayout::wal_segment(1).expect("wal segment");
        backend
            .write_object(&name, b"segment")
            .expect("write segment");

        let _ = backend.delete_object(&name);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "unexpected WAL retention mutation")]
    fn localfs_backend_debug_tripwire_rejects_unscoped_wal_sidecar_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectLayout::wal_segment_metadata(1).expect("wal sidecar");
        backend
            .write_object(&name, b"sidecar")
            .expect("write sidecar");

        let _ = backend.delete_object(&name);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn localfs_backend_debug_tripwire_allows_authorized_wal_retention_delete() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectLayout::wal_segment(1).expect("wal segment");
        backend
            .write_object(&name, b"segment")
            .expect("write segment");

        let outcome =
            super::with_authorized_wal_retention_mutation(|| backend.delete_object(&name))
                .expect("authorized delete");

        assert_eq!(outcome.status(), DeleteStatus::Deleted);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "unexpected WAL retention mutation")]
    fn localfs_backend_debug_tripwire_rejects_unscoped_wal_segment_replace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectLayout::wal_segment(1).expect("wal segment");
        backend
            .publish_object(&name, b"old", PublishMode::Create)
            .expect("create segment");

        let _ = backend.publish_object(&name, b"new", PublishMode::Replace);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn localfs_backend_debug_tripwire_allows_authorized_wal_repair_replace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectLayout::wal_segment(1).expect("wal segment");
        backend
            .publish_object(&name, b"old", PublishMode::Create)
            .expect("create segment");

        let outcome = super::with_authorized_wal_repair_mutation(|| {
            backend.publish_object(&name, b"new", PublishMode::Replace)
        })
        .expect("authorized replace");

        assert_eq!(outcome.metadata().size_bytes(), 3);
        assert_eq!(
            backend.read_object(&name).expect("read replaced segment"),
            b"new"
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_delete_before_removal_fault_leaves_object_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"manifest").expect("write");
        backend
            .arm_delete_fault(LocalFsDeleteStep::BeforeRemoval)
            .expect("arm fault");

        let error = backend.delete_object(&name).expect_err("delete fault");

        assert_eq!(error.kind(), DeleteFailureKind::FailedBeforeRemoval);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(
            backend.read_object(&name).expect("still visible"),
            b"manifest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_delete_removal_fault_reports_unknown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"manifest").expect("write");
        backend
            .arm_delete_fault(LocalFsDeleteStep::Removal)
            .expect("arm fault");

        let error = backend.delete_object(&name).expect_err("delete fault");

        assert_eq!(error.kind(), DeleteFailureKind::RemovalUnknown);
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(
            backend.read_object(&name).expect("still visible"),
            b"manifest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_delete_parent_sync_fault_reports_unconfirmed_and_removes_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        backend.write_object(&name, b"manifest").expect("write");
        backend
            .arm_delete_fault(LocalFsDeleteStep::ParentSync)
            .expect("arm fault");

        let error = backend.delete_object(&name).expect_err("delete fault");

        assert_eq!(
            error.kind(),
            DeleteFailureKind::RemovedDurabilityUnconfirmed
        );
        assert_eq!(error.source_error().kind(), BackendErrorKind::Interrupted);
        assert_eq!(
            backend
                .read_object(&name)
                .expect_err("object no longer visible")
                .kind(),
            BackendErrorKind::NotFound
        );
        let reopened = LocalFsBackend::new(dir.path());
        assert_eq!(
            reopened
                .read_object(&name)
                .expect_err("object remains absent after reopen")
                .kind(),
            BackendErrorKind::NotFound
        );
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_delete_ignores_stale_temporary_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        let final_path = backend.path_for(&name);
        let parent = final_path.parent().expect("parent");
        std::fs::create_dir_all(parent).expect("parent dirs");
        let final_file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("file name");
        let stale_temp = parent.join(format!("{final_file_name}.tmp.stale"));
        std::fs::write(&stale_temp, b"stale").expect("stale temp");
        backend.write_object(&name, b"manifest").expect("write");

        let outcome = backend.delete_object(&name).expect("delete");

        assert_eq!(outcome.status(), DeleteStatus::Deleted);
        assert_eq!(outcome.durability(), DeleteDurability::Durable);
        assert_eq!(
            backend.read_object(&name).expect_err("deleted").kind(),
            BackendErrorKind::NotFound
        );
        assert_eq!(
            std::fs::read(&stale_temp).expect("stale temp preserved"),
            b"stale"
        );
        assert!(backend
            .list_prefix(&ObjectPrefix::new("manifest/").expect("prefix"))
            .expect("list")
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_rejects_symlink_object_paths() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::NamedTempFile::new().expect("outside file");
        std::fs::write(outside.path(), b"outside").expect("outside write");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("escape").expect("name");

        symlink(outside.path(), backend.path_for(&name)).expect("symlink");

        assert_eq!(
            backend.read_object(&name).expect_err("read symlink").kind(),
            BackendErrorKind::Corruption
        );
        assert_eq!(
            backend
                .write_object(&name, b"should not escape")
                .expect_err("write symlink")
                .kind(),
            BackendErrorKind::Corruption
        );
        let delete_error = backend
            .delete_object(&name)
            .expect_err("delete symlink should fail before removal");
        assert_eq!(delete_error.kind(), DeleteFailureKind::FailedBeforeRemoval);
        assert_eq!(
            delete_error.source_error().kind(),
            BackendErrorKind::Corruption
        );
        assert_eq!(
            std::fs::read(outside.path()).expect("outside read"),
            b"outside"
        );
    }

    #[test]
    fn localfs_backend_delete_rejects_non_file_object_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("manifest/current").expect("name");
        let path = backend.path_for(&name);
        std::fs::create_dir_all(&path).expect("directory at object path");

        let delete_error = backend
            .delete_object(&name)
            .expect_err("delete directory object path should fail before removal");

        assert_eq!(delete_error.kind(), DeleteFailureKind::FailedBeforeRemoval);
        assert_eq!(
            delete_error.source_error().kind(),
            BackendErrorKind::Corruption
        );
        assert!(path.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn localfs_backend_rejects_symlink_parent_paths() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside dir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("tables/object").expect("name");
        symlink(outside.path(), dir.path().join("tables")).expect("symlink dir");

        assert_eq!(
            backend
                .write_object(&name, b"bytes")
                .expect_err("write")
                .kind(),
            BackendErrorKind::Corruption
        );
        assert_eq!(
            backend.read_object(&name).expect_err("read").kind(),
            BackendErrorKind::Corruption
        );
        assert_eq!(
            backend
                .list_prefix(&ObjectPrefix::new("tables/").expect("prefix"))
                .expect_err("list")
                .kind(),
            BackendErrorKind::Corruption
        );
    }
}
