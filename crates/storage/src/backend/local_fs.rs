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
//! | `write_object` | last-writer-wins: a raced delete is recreated; a parent directory an emptied-directory prune removes between parent creation and file creation is re-created and the write retried (bounded, #3692) |
//! | `delete_object` | idempotent: losing any window (parent walk, stat, or the stat→unlink gap) reports `already_missing`; the winner's parent fsync carries removal durability; the winner then `rmdir`s the directories it emptied below the family root — a non-empty or vanished directory stops or skips the climb, never fails the delete (#3692) |
//! | `remove_empty_dirs_under` | fuzzy sweep: `rmdir` refuses a directory a racer refilled, a vanished directory is skipped; never removes a family root or the database root (#3692) |
//! | `list_prefix` / `collect_files` | fuzzy snapshot: concurrently created/deleted entries may or may not appear, a vanished entry or directory is skipped, and the walk itself never fails on absence |
//! | `publish_object` | atomic install: parent-dir creation tolerates a racer's `EEXIST` with post-verify (#2799), a parent removed before the temp file lands is re-created and the create retried (#3692), temp files retry collisions, create-mode no-clobber maps a raced final link to `PreconditionFailed`, and partial failures classify by visibility |
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
    /// #3692 race seam: how many upcoming object-creation attempts first have
    /// their (empty) parent directory removed, exactly as a concurrent delete's
    /// emptied-directory pruning would between parent creation and file
    /// creation.
    #[cfg(all(test, unix))]
    parent_race: Arc<Mutex<(u32, LocalFsParentRace)>>,
}

/// #3692 race seam: what a fired race does to the creation's parent directory.
#[cfg(all(test, unix))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalFsParentRace {
    /// Remove the empty parent — a concurrent emptied-directory prune.
    Prune,
    /// Prune, then make the family root read-only, so re-creating the parent
    /// fails for a real reason (`PermissionDenied`). The test restores it.
    PruneAndLockFamily,
    /// Replace the parent with a file, so the creation itself fails for a
    /// reason other than absence.
    ReplaceWithFile,
}

impl LocalFsBackend {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            #[cfg(all(test, unix))]
            publish_fault: Arc::new(Mutex::new(None)),
            #[cfg(all(test, unix))]
            delete_fault: Arc::new(Mutex::new(None)),
            #[cfg(all(test, unix))]
            parent_race: Arc::new(Mutex::new((0, LocalFsParentRace::Prune))),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// #3008: resolve a database root whose FINAL component is a symlink to
    /// the path it names. The caller chose that link (`~/data -> /mnt/…`, a
    /// shortened deep path), exactly as every other tool follows it; the
    /// backend refuses a symlinked root only because it refuses symlinks
    /// *inside* the tree, so the durable open resolves the root once, here,
    /// before the backend is built. Everything below (writer lock, layout,
    /// WAL) then operates on the real directory, and the writer lock — an OS
    /// advisory lock on the lock file's inode — is the same lock through
    /// either spelling.
    ///
    /// Any other root keeps the caller's spelling (a symlinked PARENT is
    /// already followed by the OS). A link that does not resolve (dangling, a
    /// loop, permission denied) is also left as given, so the backend's
    /// path-shape check refuses it with its existing classification; a link
    /// to a file resolves to the file and is refused as not-a-directory.
    pub(crate) fn resolve_root_symlink(root: PathBuf) -> PathBuf {
        match fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                // Unresolvable: keep the original spelling (see above).
                fs::canonicalize(&root).unwrap_or(root)
            }
            // Not a symlink, or missing/unreadable: the backend's own stat
            // reports it with its typed classification.
            _ => root,
        }
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

    /// #3721: arm a delete fault before the unlink (the object stays visible).
    #[cfg(all(test, unix))]
    pub(crate) fn inject_before_removal_delete_fault(&self) -> BackendResult<()> {
        self.arm_delete_fault(LocalFsDeleteStep::BeforeRemoval)
    }

    /// #3721: arm a targeted fault at the final install step of `target_object`
    /// (for a link, the `link(2)` itself — nothing becomes visible).
    #[cfg(all(test, unix))]
    pub(crate) fn inject_targeted_final_publish_fault(
        &self,
        target_object: String,
    ) -> BackendResult<()> {
        self.arm_targeted_publish_fault(LocalFsPublishStep::FinalPublish, target_object)
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

    /// #3692: arm the race seam for the next `attempts` object creations.
    #[cfg(all(test, unix))]
    fn arm_parent_race(&self, attempts: u32) {
        self.arm_parent_race_with(attempts, LocalFsParentRace::Prune);
    }

    /// #3692: arm the race seam with a specific race.
    #[cfg(all(test, unix))]
    fn arm_parent_race_with(&self, attempts: u32, race: LocalFsParentRace) {
        *self.parent_race.lock().expect("parent race seam lock") = (attempts, race);
    }

    /// #3692: the armed race seam's remaining count (a test reads it to prove
    /// the seam fired on exactly the attempts the retry made).
    #[cfg(all(test, unix))]
    fn parent_race_remaining(&self) -> u32 {
        self.parent_race.lock().expect("parent race seam lock").0
    }

    /// #3692: fire the race seam — remove the (empty) parent directory the
    /// creation is about to use, as a concurrent emptied-directory prune can.
    /// Written without a comparison so no mutant of it can fire unarmed: an
    /// unarmed seam returns before touching the filesystem.
    #[cfg(all(test, unix))]
    fn injected_parent_race(&self, parent: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut armed = self.parent_race.lock().expect("parent race seam lock");
        let (attempts, race) = *armed;
        let Some(remaining) = attempts.checked_sub(1) else {
            return;
        };
        armed.0 = remaining;
        fs::remove_dir(parent).expect("race seam prunes the empty parent");
        match race {
            LocalFsParentRace::Prune => {}
            LocalFsParentRace::PruneAndLockFamily => {
                let family = parent.parent().expect("family root");
                fs::set_permissions(family, fs::Permissions::from_mode(0o555))
                    .expect("race seam locks the family root");
            }
            LocalFsParentRace::ReplaceWithFile => {
                fs::write(parent, b"").expect("race seam plants a file");
            }
        }
    }

    /// #3692: one creation attempt — the race seam (tests only), then `create`.
    fn attempt_object_creation<T>(
        &self,
        parent: &Path,
        create: &mut impl FnMut() -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        #[cfg(all(test, unix))]
        self.injected_parent_race(parent);
        // Outside tests the seam compiles away, and `self` and `parent` have
        // no other use (the method stays one, so the seam can read its state).
        #[cfg(not(all(test, unix)))]
        let _ = (&self.root, parent);
        create()
    }

    /// #3692: run `create` — a file creation inside `parent`, which the caller
    /// has already created — and, when it fails `NotFound`, re-create the
    /// parent chain and try again, at most [`OBJECT_CREATION_ATTEMPTS`]
    /// attempts in all. A delete that empties a directory removes it
    /// (`prune_emptied_ancestors`), so a concurrent delete can win the window
    /// between this call's parent creation and its file creation; the
    /// directory's absence then is a lost race, not the owner being gone. A
    /// re-creation that itself fails `NotFound` (an intermediate directory
    /// pruned mid-walk) is the same race and spends an attempt; any other
    /// failure is final. The bound is the loop's own range, so no retry
    /// decision can make it spin. Directory creation keeps its existing
    /// semantics (`ensure_parent_dirs`), so a re-created directory is exactly
    /// as durable as a first-time one: the object's own parent fsync
    /// (publish) covers its entry.
    fn create_with_parent_retry<T>(
        &self,
        parent: &Path,
        mut create: impl FnMut() -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        let mut outcome = self.attempt_object_creation(parent, &mut create);
        for _ in 1..OBJECT_CREATION_ATTEMPTS {
            match &outcome {
                Err(error) if should_retry_object_creation(error.kind()) => {}
                _ => return outcome,
            }
            outcome = self
                .ensure_parent_dirs(parent, true)
                .and_then(|()| self.attempt_object_creation(parent, &mut create));
        }
        outcome
    }

    /// #3692: after an object's file is unlinked, remove the directories it
    /// leaves empty, deepest first, stopping at the first directory that is
    /// kept. Only directories strictly below the object's top-level family
    /// directory are candidates ([`prunable_ancestors`]): the database root and
    /// the family roots (`timeline/`, `tables/`, ...) are never removed.
    ///
    /// `rmdir` semantics: `fs::remove_dir` refuses a non-empty directory, so a
    /// sibling object, a racing publish's temporary file, or a racing creation
    /// always keeps its directory. Every removal failure is ignored by design
    /// (see [`ancestor_removal_step`]): the object is already deleted, and an
    /// empty directory left behind is garbage the open-time reconcile sweeps
    /// (`remove_empty_dirs_under`), never a correctness fact.
    ///
    /// No parent-directory fsync follows a removal. This is garbage removal,
    /// not a durability commitment: the object's unlink was already made
    /// durable by `sync_delete_parent`, and a crash that forgets the `rmdir`
    /// resurrects only an empty directory — no object, no listing entry.
    fn prune_emptied_ancestors(&self, object_path: &Path) {
        let Some(parent) = object_path.parent() else {
            return;
        };
        // A parent outside the root is impossible for a `path_for` path; were it
        // ever to happen, removing nothing is the only safe answer.
        let Ok(relative) = parent.strip_prefix(&self.root) else {
            return;
        };
        for dir in prunable_ancestors(relative) {
            let removal = fs::remove_dir(self.root.join(dir));
            if ancestor_removal_step(&removal) == AncestorRemovalStep::Stop {
                break;
            }
        }
    }

    /// #3692: walk `dir` depth-first and remove every empty directory below
    /// the family root (post-order, so a chain of empty directories goes in
    /// one pass). Returns how many were removed. Symlinks are never followed.
    /// A directory that vanishes or is (re)filled concurrently is skipped;
    /// only a failure to read a directory that exists is an error.
    fn remove_empty_dirs_in(&self, dir: &Path) -> Result<u64, BackendError> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(map_io_error(&error)),
        };
        let mut removed = 0_u64;
        for entry in entries {
            let entry = entry.map_err(|err| map_io_error(&err))?;
            let Some(file_type) = classify_entry_type(entry.file_type())? else {
                continue;
            };
            if file_type.is_dir() {
                removed = removed.saturating_add(self.remove_empty_dirs_in(&entry.path())?);
            }
        }
        let prunable = dir.strip_prefix(&self.root).is_ok_and(dir_is_prunable);
        // Non-empty, vanished or otherwise refused: kept, and not an error —
        // the sweep is best-effort garbage removal (see `prune_emptied_ancestors`).
        if prunable && fs::remove_dir(dir).is_ok() {
            removed = removed.saturating_add(1);
        }
        Ok(removed)
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

        let created = match final_path.parent() {
            Some(parent) => {
                self.create_with_parent_retry(parent, || Self::create_temporary_file(final_path))
            }
            None => Self::create_temporary_file(final_path),
        };
        let (temp_path, mut file) = created.map_err(|error| {
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
            capabilities.insert(super::BackendCapability::DurableLink);
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
        match path.parent() {
            Some(parent) => self.create_with_parent_retry(parent, || {
                fs::write(&path, bytes).map_err(|err| map_io_error(&err))
            })?,
            None => fs::write(&path, bytes).map_err(|err| map_io_error(&err))?,
        }
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
        // The unlink's durability is settled (or reported unconfirmed) BEFORE
        // any emptied directory goes, so the parent fsync never races its own
        // directory's removal; the object is gone either way, so the emptied
        // directories are pruned whatever the sync reported (#3692).
        let outcome = self.sync_delete_parent(name, &path);
        self.prune_emptied_ancestors(&path);
        outcome
    }

    fn remove_empty_dirs_under(&self, prefix: &ObjectPrefix) -> Result<u64, BackendError> {
        let mut dir = self.root.clone();
        for component in prefix.as_str().split('/').filter(|part| !part.is_empty()) {
            dir.push(component);
        }
        match fs::symlink_metadata(&dir) {
            Ok(metadata) if metadata.is_dir() => self.remove_empty_dirs_in(&dir),
            // Absent (nothing ever written under it), or not a real directory
            // (a symlink is never followed): nothing to sweep.
            Ok(_) => Ok(0),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(map_io_error(&error)),
        }
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
        let lock = LocalFsWriterLock::acquire(file)?;
        Ok(BackendWriterGuard::new(name.clone(), lock))
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

    /// #3721: a hard link — `to` becomes a second directory entry for `from`'s
    /// inode, so no payload byte is written. It is a create-mode publish in
    /// every other respect: the target's parents are prepared the same way,
    /// `link(2)` is atomic and no-clobber (an existing `to` is
    /// `PreconditionFailed`), a failed link leaves nothing visible, and the
    /// target's parent fsync is the durability point (a failure there is
    /// `VisibleDurabilityUnconfirmed`). The creation rides the #3692 parent
    /// retry, because a concurrent delete that empties `to`'s directory prunes
    /// it. `from` is untouched; the caller removes it with `delete_object`,
    /// whose unlink, parent fsync and emptied-directory pruning are unchanged.
    fn link_object(
        &self,
        from: &ObjectName,
        to: &ObjectName,
    ) -> Result<PublishOutcome, PublishError> {
        for name in [from, to] {
            if let Err(error) = Self::reject_writer_lock_object_mutation(name, "link") {
                return Err(Self::publish_error(
                    to,
                    PublishFailureKind::FailedBeforeVisibility,
                    error,
                ));
            }
        }
        if !cfg!(unix) {
            return Err(PublishError::unsupported(
                to,
                BackendError::unsupported(super::BackendCapability::DurableLink),
            ));
        }

        let from_path = self.path_for(from);
        let source = self.metadata_for_object_path(&from_path).map_err(|error| {
            Self::publish_error(to, PublishFailureKind::FailedBeforeVisibility, error)
        })?;
        let final_path = self.prepare_publish_target(to, PublishMode::Create)?;
        if let Some(error) = self.injected_publish_fault(LocalFsPublishStep::FinalPublish, to) {
            return Err(Self::publish_error(
                to,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }
        let link = || fs::hard_link(&from_path, &final_path).map_err(|err| map_link_error(&err));
        let linked = match final_path.parent() {
            Some(parent) => self.create_with_parent_retry(parent, link),
            None => link(),
        };
        if let Err(error) = linked {
            if error.kind() == BackendErrorKind::AlreadyExists {
                return Err(PublishError::precondition_failed(
                    to,
                    format!("object {to} already exists"),
                ));
            }
            return Err(Self::publish_error(
                to,
                PublishFailureKind::FailedBeforeVisibility,
                error,
            ));
        }
        self.sync_publish_parent(to, &final_path)?;

        Ok(PublishOutcome::new(
            to.clone(),
            BackendMetadata::new(source.size_bytes(), None),
            PublishDurability::Durable,
        ))
    }
}

/// #3692: whether a directory (relative to the backend root) may be removed
/// when empty. The root (no components) and the top-level family directories
/// (`timeline`, `tables`, `wal`, ... — one component) are permanent; every
/// directory below a family root exists only to hold objects.
fn dir_is_prunable(relative: &Path) -> bool {
    relative.components().count() >= 2
}

/// #3692: the ancestors of an object whose parent directory is `relative_parent`
/// (relative to the backend root) that a delete may try to remove, deepest
/// first. Stops at — and excludes — the family root.
fn prunable_ancestors(relative_parent: &Path) -> Vec<&Path> {
    relative_parent
        .ancestors()
        .take_while(|dir| dir_is_prunable(dir))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AncestorRemovalStep {
    /// The directory is gone (removed now, or by a racer): try its parent.
    Climb,
    /// The directory is kept (non-empty, or any other refusal): stop.
    Stop,
}

/// #3692: what one ancestor `rmdir` result means for the climb. `NotFound` is
/// a racer having removed it first — still climb, since the racer may have
/// stopped on a directory this delete just emptied. Every other failure
/// (non-empty, permission, I/O) keeps the directory and is not an error: the
/// object is already deleted.
fn ancestor_removal_step(result: &std::io::Result<()>) -> AncestorRemovalStep {
    match result {
        Ok(()) => AncestorRemovalStep::Climb,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => AncestorRemovalStep::Climb,
        Err(_) => AncestorRemovalStep::Stop,
    }
}

/// #3692: how many times an object creation is attempted before its parent's
/// absence is surfaced. Each retry answers one concurrent emptied-directory
/// prune; a directory removed this many times in a row is not a race to ride.
const OBJECT_CREATION_ATTEMPTS: u32 = 4;

/// #3692: whether a failed object creation re-creates its parent directories
/// and tries again (within [`OBJECT_CREATION_ATTEMPTS`]). Only `NotFound` — the
/// parent vanished — is the race; every other failure is final.
fn should_retry_object_creation(kind: BackendErrorKind) -> bool {
    kind == BackendErrorKind::NotFound
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

/// The OS advisory writer lock a local-filesystem writer guard holds.
///
/// Releasing it is explicit, not left to the descriptor closing (#3609). A
/// `flock` belongs to the *open file description*, and a child that any
/// thread of this process forks shares that description until it execs.
/// Closing only this descriptor therefore leaves the lock held for as long as
/// a concurrently forked child is still between `fork` and `exec` — an
/// immediate reopen of the same database in this process, even after a clean
/// drop or close, fails as writer-lock contention. `LOCK_UN` releases the
/// lock for every descriptor sharing the description, inherited ones
/// included, so the release happens when the guard drops.
struct LocalFsWriterLock {
    file: File,
}

impl LocalFsWriterLock {
    /// Takes the exclusive lock on `file`, fail-fast by contract.
    fn acquire(file: File) -> Result<Self, BackendError> {
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
        Ok(Self { file })
    }
}

impl Drop for LocalFsWriterLock {
    fn drop(&mut self) {
        // Rationale: a drop has no caller to report to, and a failed unlock
        // degrades to the descriptor close that follows, which still releases
        // the lock once no inherited descriptor shares it (the pre-#3609
        // behavior).
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

/// #3721: how a failed `link(2)` is reported. A filesystem that cannot
/// hard-link these two paths — `EXDEV` (the quarantine directory is another
/// mount) or `ENOTSUP` — lacks the capability rather than failing I/O, so it
/// surfaces as `UnsupportedOperation` and the quarantine stage falls back to
/// its byte copy. Every other failure maps like any other I/O error.
fn map_link_error(error: &std::io::Error) -> BackendError {
    match error.kind() {
        std::io::ErrorKind::CrossesDevices | std::io::ErrorKind::Unsupported => {
            BackendError::new(BackendErrorKind::UnsupportedOperation, error.to_string())
        }
        _ => map_io_error(error),
    }
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

    /// #3008: a root whose final component is a symlink resolves to the
    /// directory it names.
    #[cfg(unix)]
    #[test]
    fn resolve_root_symlink_follows_a_final_component_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real");
        std::fs::create_dir(&real).expect("real dir");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");

        assert_eq!(
            LocalFsBackend::resolve_root_symlink(link),
            std::fs::canonicalize(&real).expect("canonical real dir")
        );
    }

    /// #3008: only a final-component link is resolved. A root that is not
    /// itself a link keeps the caller's spelling, even when a PARENT is a
    /// symlink (the OS already follows it) — canonicalizing every root would
    /// rewrite paths the caller never asked to have rewritten.
    #[cfg(unix)]
    #[test]
    fn resolve_root_symlink_keeps_the_spelling_of_a_root_that_is_not_a_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_parent = dir.path().join("real-parent");
        std::fs::create_dir_all(real_parent.join("db")).expect("db dir");
        let linked_parent = dir.path().join("linked-parent");
        std::os::unix::fs::symlink(&real_parent, &linked_parent).expect("parent symlink");

        let through_linked_parent = linked_parent.join("db");
        assert_eq!(
            LocalFsBackend::resolve_root_symlink(through_linked_parent.clone()),
            through_linked_parent
        );
        let missing = dir.path().join("not-created-yet");
        assert_eq!(
            LocalFsBackend::resolve_root_symlink(missing.clone()),
            missing
        );
    }

    /// #3008: a dangling link is left as given, so the backend's path-shape
    /// check refuses it with its existing classification.
    #[cfg(unix)]
    #[test]
    fn resolve_root_symlink_leaves_a_dangling_link_as_given() {
        let dir = tempfile::tempdir().expect("tempdir");
        let link = dir.path().join("dangling");
        std::os::unix::fs::symlink(dir.path().join("absent"), &link).expect("symlink");

        assert_eq!(LocalFsBackend::resolve_root_symlink(link.clone()), link);
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
        assert_eq!(
            capabilities.contains(BackendCapability::DurableLink),
            cfg!(unix)
        );

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
    fn writer_lock_release_does_not_wait_for_an_inherited_descriptor() {
        use fs2::FileExt as _;

        // #3609: a child forked by any thread of this process shares the lock
        // file's open file description until it execs. A `try_clone` shares
        // the description the same way, so it stands in for that child
        // deterministically: dropping the guard must release the lock while
        // the inherited descriptor is still open.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let lock_name = ObjectLayout::writer_lock().expect("writer lock name");
        drop(
            backend
                .acquire_writer_lock(&lock_name)
                .expect("stage the lock object"),
        );
        let lock_path = backend.path_for(&lock_name);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open lock object");
        let inherited = file.try_clone().expect("share the open file description");
        let guard = crate::backend::BackendWriterGuard::new(
            lock_name.clone(),
            super::LocalFsWriterLock::acquire(file).expect("take the writer lock"),
        );
        let contender = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open contender");
        assert_eq!(
            contender
                .try_lock_exclusive()
                .expect_err("the held lock excludes a second description")
                .kind(),
            std::io::ErrorKind::WouldBlock
        );

        drop(guard);

        contender
            .try_lock_exclusive()
            .expect("a dropped guard releases the lock despite an inherited descriptor");
        // The inherited descriptor is still open: the release did not come
        // from the last reference to the description closing.
        inherited
            .metadata()
            .expect("inherited descriptor still open");
        drop(contender);
        backend
            .acquire_writer_lock(&lock_name)
            .expect("the production acquire path reacquires the released lock");
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

    // ---- #3692: emptied directories -------------------------------------

    /// Every directory under `root` (excluding `root` itself) that holds no
    /// entry at all, relative to `root`.
    fn empty_dirs_under(root: &std::path::Path) -> Vec<String> {
        fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
            let entries: Vec<_> = std::fs::read_dir(dir)
                .expect("read dir")
                .map(|entry| entry.expect("entry"))
                .collect();
            if entries.is_empty() && dir != root {
                out.push(
                    dir.strip_prefix(root)
                        .expect("under root")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            for entry in entries {
                if entry.file_type().expect("file type").is_dir() {
                    walk(root, &entry.path(), out);
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out.sort();
        out
    }

    #[test]
    fn dir_is_prunable_truth_table() {
        use super::dir_is_prunable;
        use std::path::Path;
        // (relative dir, prunable): the root and a family root are permanent.
        let cases = [
            ("", false),
            ("timeline", false),
            ("tables", false),
            ("timeline/0000000000000001", true),
            ("tables/branch", true),
            ("tables/branch/l0001", true),
        ];
        for (dir, prunable) in cases {
            assert_eq!(dir_is_prunable(Path::new(dir)), prunable, "{dir:?}");
        }
    }

    #[test]
    fn prunable_ancestors_truth_table() {
        use super::prunable_ancestors;
        use std::path::Path;
        // (object's parent dir, candidates deepest-first): never the family
        // root, never the database root.
        let cases: [(&str, &[&str]); 5] = [
            ("", &[]),
            ("wal", &[]),
            ("timeline/0000000000000001", &["timeline/0000000000000001"]),
            (
                "tables/branch/l0001",
                &["tables/branch/l0001", "tables/branch"],
            ),
            ("a/b/c/d", &["a/b/c/d", "a/b/c", "a/b"]),
        ];
        for (parent, expected) in cases {
            let got = prunable_ancestors(Path::new(parent));
            let expected: Vec<&Path> = expected.iter().map(Path::new).collect();
            assert_eq!(got, expected, "{parent:?}");
        }
    }

    #[test]
    fn ancestor_removal_step_truth_table() {
        use super::{ancestor_removal_step, AncestorRemovalStep};
        use std::io::{Error, ErrorKind};
        let cases = [
            (Ok(()), AncestorRemovalStep::Climb),
            (
                Err(Error::from(ErrorKind::NotFound)),
                AncestorRemovalStep::Climb,
            ),
            (
                Err(Error::from(ErrorKind::DirectoryNotEmpty)),
                AncestorRemovalStep::Stop,
            ),
            // POSIX lets rmdir report a non-empty directory as EEXIST.
            (
                Err(Error::from(ErrorKind::AlreadyExists)),
                AncestorRemovalStep::Stop,
            ),
            (
                Err(Error::from(ErrorKind::PermissionDenied)),
                AncestorRemovalStep::Stop,
            ),
            (
                Err(Error::from(ErrorKind::NotADirectory)),
                AncestorRemovalStep::Stop,
            ),
        ];
        for (result, expected) in cases {
            let label = format!("{result:?}");
            assert_eq!(ancestor_removal_step(&result), expected, "{label}");
        }
    }

    #[test]
    fn should_retry_object_creation_truth_table() {
        use super::{should_retry_object_creation, OBJECT_CREATION_ATTEMPTS};
        assert_eq!(OBJECT_CREATION_ATTEMPTS, 4);
        let cases = [
            (BackendErrorKind::NotFound, true),
            (BackendErrorKind::AlreadyExists, false),
            (BackendErrorKind::Corruption, false),
            (BackendErrorKind::PermissionDenied, false),
            (BackendErrorKind::Unknown, false),
        ];
        for (kind, retry) in cases {
            assert_eq!(should_retry_object_creation(kind), retry, "{kind:?}");
        }
    }

    /// (d) A delete removes the directory it empties, and only that: a
    /// directory still holding a sibling object (or any other entry) is kept,
    /// and the family root and the database root are never removed.
    #[test]
    fn delete_removes_emptied_directories_below_the_family_root_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let backend = LocalFsBackend::new(root);
        let first = ObjectName::new("timeline/0000000000000001/0000000000000000").expect("name");
        let second = ObjectName::new("timeline/0000000000000001/0000000000000001").expect("name");
        backend.write_object(&first, b"a").expect("write");
        backend.write_object(&second, b"b").expect("write");

        backend.delete_object(&first).expect("delete first");
        assert!(
            root.join("timeline/0000000000000001").is_dir(),
            "a directory still holding a sibling object is kept"
        );
        assert_eq!(
            backend.read_object(&second).expect("sibling survives"),
            b"b"
        );

        backend.delete_object(&second).expect("delete second");
        assert!(
            !root.join("timeline/0000000000000001").exists(),
            "the emptied segment directory goes"
        );
        assert!(root.join("timeline").is_dir(), "the family root stays");
        assert!(root.is_dir(), "the database root stays");

        // A multi-level chain goes in one delete, up to (not including) the
        // family root.
        let nested = ObjectName::new("tables/branch/l0001/table").expect("name");
        backend.write_object(&nested, b"t").expect("write");
        backend.delete_object(&nested).expect("delete nested");
        assert!(!root.join("tables/branch").exists(), "the chain goes");
        assert!(root.join("tables").is_dir(), "the family root stays");

        // A top-level object's only parent is its family root: kept.
        let top = ObjectName::new("meta/database").expect("name");
        backend.write_object(&top, b"m").expect("write");
        backend.delete_object(&top).expect("delete top-level");
        assert!(root.join("meta").is_dir(), "the family root stays");

        // A non-object entry (a stale publish temp file) keeps its directory.
        let kept = ObjectName::new("timeline/0000000000000002/0000000000000000").expect("name");
        backend.write_object(&kept, b"k").expect("write");
        std::fs::write(
            root.join("timeline/0000000000000002/0000000000000000.object@.tmp.1.0"),
            b"stale",
        )
        .expect("stale temp");
        backend.delete_object(&kept).expect("delete");
        assert!(
            root.join("timeline/0000000000000002").is_dir(),
            "rmdir refuses a directory holding any entry"
        );

        assert_eq!(
            empty_dirs_under(root),
            vec!["meta".to_owned(), "tables".to_owned()],
            "only family roots may be left empty"
        );
    }

    /// (d) A delete whose parent fsync is faulted still reports the fault and
    /// still prunes the emptied directory: the object is gone either way.
    #[cfg(unix)]
    #[test]
    fn delete_prunes_the_emptied_directory_even_when_the_parent_sync_faults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let name = ObjectName::new("timeline/0000000000000003/0000000000000000").expect("name");
        backend.write_object(&name, b"x").expect("write");
        backend
            .arm_delete_fault(LocalFsDeleteStep::ParentSync)
            .expect("arm");
        let error = backend
            .delete_object(&name)
            .expect_err("parent sync faults");
        assert_eq!(
            error.kind(),
            DeleteFailureKind::RemovedDurabilityUnconfirmed
        );
        assert!(!dir.path().join("timeline/0000000000000003").exists());
        assert!(dir.path().join("timeline").is_dir());
    }

    /// (c) The race: a concurrent delete's emptied-directory prune removes the
    /// parent between this write's parent creation and its file creation. The
    /// write re-creates the parent and lands; only a parent removed on every
    /// bounded attempt surfaces `NotFound`.
    #[cfg(unix)]
    #[test]
    fn write_survives_its_parent_being_pruned_before_the_file_lands() {
        use super::OBJECT_CREATION_ATTEMPTS;
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());

        // Every attempt short of the bound rides the race; the seam proves it
        // fired on exactly that many attempts (the count drains to zero).
        for (ordinal, raced) in [(0x40_u64, 1), (0x41, OBJECT_CREATION_ATTEMPTS - 1)] {
            let name =
                ObjectName::new(format!("timeline/{ordinal:016x}/0000000000000000")).expect("name");
            backend.arm_parent_race(raced);
            backend
                .write_object(&name, b"raced")
                .expect("write rides the race");
            assert_eq!(backend.parent_race_remaining(), 0, "{raced} races fired");
            assert_eq!(backend.read_object(&name).expect("read"), b"raced");
        }

        // A parent removed on every attempt surfaces NotFound after exactly
        // OBJECT_CREATION_ATTEMPTS attempts — one armed race is left unspent.
        let exhausted =
            ObjectName::new("timeline/0000000000000005/0000000000000000").expect("name");
        backend.arm_parent_race(OBJECT_CREATION_ATTEMPTS + 1);
        assert_eq!(
            backend
                .write_object(&exhausted, b"x")
                .expect_err("bounded retry")
                .kind(),
            BackendErrorKind::NotFound
        );
        assert_eq!(backend.parent_race_remaining(), 1, "exactly the bound");
        backend.arm_parent_race(0);

        // Unarmed, the seam never races: a write into an existing directory
        // leaves the count at zero and the directory in place.
        let plain = ObjectName::new("timeline/0000000000000041/0000000000000001").expect("name");
        backend.write_object(&plain, b"p").expect("plain write");
        assert_eq!(backend.parent_race_remaining(), 0);
    }

    /// #3692: the retry answers only `NotFound`. A creation that fails for a
    /// real reason is final and surfaces as itself; so is a re-creation of the
    /// parent that fails for a real reason.
    #[cfg(unix)]
    #[test]
    fn write_retry_surfaces_real_errors_as_themselves() {
        use super::LocalFsParentRace;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());

        // The parent became a file: the creation fails not-a-directory
        // (`Unknown`), which is no race — retrying would instead report the
        // re-creation's `Corruption`.
        let blocked = ObjectName::new("timeline/0000000000000042/0000000000000000").expect("name");
        backend.arm_parent_race_with(1, LocalFsParentRace::ReplaceWithFile);
        assert_eq!(
            backend
                .write_object(&blocked, b"x")
                .expect_err("blocked")
                .kind(),
            BackendErrorKind::Unknown
        );
        assert_eq!(backend.parent_race_remaining(), 0);

        // The parent was pruned (a race: retried) and re-creating it fails
        // PermissionDenied — final, surfaced as itself, not the NotFound that
        // started the retry.
        let locked = ObjectName::new("timeline/0000000000000043/0000000000000000").expect("name");
        backend.arm_parent_race_with(1, LocalFsParentRace::PruneAndLockFamily);
        let result = backend.write_object(&locked, b"x");
        std::fs::set_permissions(
            dir.path().join("timeline"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("restore permissions");
        assert_eq!(
            result.expect_err("locked").kind(),
            BackendErrorKind::PermissionDenied
        );
        assert_eq!(backend.parent_race_remaining(), 0);
    }

    /// (c) The same race on the durable publish path, in both modes: the temp
    /// file's creation is the step that needs the parent.
    #[cfg(unix)]
    #[test]
    fn publish_survives_its_parent_being_pruned_before_the_temp_file_lands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        for (ordinal, mode) in [(6_u64, PublishMode::Replace), (7, PublishMode::Create)] {
            let name =
                ObjectName::new(format!("timeline/{ordinal:016x}/0000000000000000")).expect("name");
            backend.arm_parent_race(1);
            let outcome = backend
                .publish_object(&name, b"published", mode)
                .expect("publish rides the race");
            assert_eq!(outcome.durability(), PublishDurability::Durable);
            assert_eq!(backend.read_object(&name).expect("read"), b"published");
            assert_no_temporary_entries(&backend, &name);
        }

        let exhausted =
            ObjectName::new("timeline/0000000000000008/0000000000000000").expect("name");
        backend.arm_parent_race(super::OBJECT_CREATION_ATTEMPTS + 1);
        let error = backend
            .publish_object(&exhausted, b"x", PublishMode::Replace)
            .expect_err("bounded retry");
        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::NotFound);
        assert_eq!(backend.parent_race_remaining(), 1, "exactly the bound");
        backend.arm_parent_race(0);
    }

    /// (b, backend half) The reconcile sweep removes every empty directory
    /// below the family root — including a chain — and nothing else.
    #[test]
    fn remove_empty_dirs_under_sweeps_only_empty_dirs_below_the_family_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let backend = LocalFsBackend::new(root);
        let prefix = ObjectLayout::timeline_prefix().expect("prefix");

        assert_eq!(
            backend.remove_empty_dirs_under(&prefix).expect("absent"),
            0,
            "a family never written is nothing to sweep"
        );

        let live = ObjectName::new("timeline/00000000000000c0/0000000000000000").expect("name");
        backend.write_object(&live, b"live").expect("write");
        for id in 1..=5_u64 {
            std::fs::create_dir_all(root.join(format!("timeline/{id:016x}"))).expect("leftover");
        }
        std::fs::create_dir_all(root.join("timeline/0000000000000009/deeper/still"))
            .expect("chain");
        std::fs::create_dir_all(root.join("tables/branch/l0001")).expect("other family");

        assert_eq!(
            backend.remove_empty_dirs_under(&prefix).expect("sweep"),
            8,
            "five leftovers plus a three-directory chain"
        );
        assert!(root.join("timeline").is_dir(), "the family root stays");
        assert_eq!(backend.read_object(&live).expect("live"), b"live");
        assert!(
            root.join("tables/branch/l0001").is_dir(),
            "another family is out of the sweep's scope"
        );
        assert_eq!(
            backend.remove_empty_dirs_under(&prefix).expect("again"),
            0,
            "nothing left to sweep"
        );
    }

    /// The sweep never follows a symlink out of the tree.
    #[cfg(unix)]
    #[test]
    fn remove_empty_dirs_under_never_follows_a_symlink() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir(outside.path().join("empty")).expect("outside empty dir");
        std::fs::create_dir_all(dir.path().join("timeline")).expect("family");
        symlink(outside.path(), dir.path().join("timeline/link")).expect("symlink");
        let backend = LocalFsBackend::new(dir.path());
        let prefix = ObjectLayout::timeline_prefix().expect("prefix");

        assert_eq!(backend.remove_empty_dirs_under(&prefix).expect("sweep"), 0);
        assert!(outside.path().join("empty").is_dir());
    }

    /// #3692: the sweep's own guards. A directory that vanished before the
    /// walk reached it counts nothing (not an error); one that cannot be read
    /// is a real error.
    #[cfg(unix)]
    #[test]
    fn remove_empty_dirs_in_separates_a_vanished_dir_from_an_unreadable_one() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        assert_eq!(
            backend
                .remove_empty_dirs_in(&dir.path().join("timeline/0000000000000001"))
                .expect("vanished"),
            0
        );

        let locked = dir.path().join("timeline/0000000000000002");
        std::fs::create_dir_all(locked.join("child")).expect("dirs");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))
            .expect("unreadable");
        let result = backend.remove_empty_dirs_under(&ObjectLayout::timeline_prefix().expect("p"));
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
            .expect("restore permissions");
        assert_eq!(
            result.expect_err("unreadable").kind(),
            BackendErrorKind::PermissionDenied
        );
    }

    /// #3692: the sweep's entry guards. A family path that is a file or a
    /// symlink is nothing to sweep (never followed); a family path that cannot
    /// be probed is a real error.
    #[cfg(unix)]
    #[test]
    fn remove_empty_dirs_under_guards_the_family_path() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let prefix = ObjectLayout::timeline_prefix().expect("prefix");

        let file_root = tempfile::tempdir().expect("tempdir");
        std::fs::write(file_root.path().join("timeline"), b"not a dir").expect("file");
        assert_eq!(
            LocalFsBackend::new(file_root.path())
                .remove_empty_dirs_under(&prefix)
                .expect("a file is nothing to sweep"),
            0
        );

        let link_root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::create_dir_all(outside.path().join("a/b")).expect("outside empties");
        symlink(outside.path(), link_root.path().join("timeline")).expect("symlink");
        assert_eq!(
            LocalFsBackend::new(link_root.path())
                .remove_empty_dirs_under(&prefix)
                .expect("a symlinked family is never followed"),
            0
        );
        assert!(outside.path().join("a/b").is_dir());

        let locked = tempfile::tempdir().expect("tempdir");
        let db = locked.path().join("db");
        std::fs::create_dir_all(db.join("timeline/0000000000000001")).expect("dirs");
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600))
            .expect("unsearchable root");
        let result = LocalFsBackend::new(&db).remove_empty_dirs_under(&prefix);
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o755))
            .expect("restore permissions");
        assert_eq!(
            result.expect_err("unprobeable").kind(),
            BackendErrorKind::PermissionDenied
        );
    }

    /// #3692: every wrapper forwards the sweep and its exact count, and a
    /// backend without directories keeps the trait default (nothing swept).
    #[test]
    fn remove_empty_dirs_under_count_is_forwarded_by_every_wrapper() {
        use crate::backend::memory::MemoryBackend;
        use crate::backend::BackendHandle;
        use crate::testkit::{FaultScript, FaultingBackend, WriteOrderingWatchdog};

        fn plant(root: &std::path::Path) {
            for id in 1..=3_u64 {
                std::fs::create_dir_all(root.join(format!("timeline/{id:016x}")))
                    .expect("leftover");
            }
        }
        let prefix = ObjectLayout::timeline_prefix().expect("prefix");

        let dir = tempfile::tempdir().expect("tempdir");
        plant(dir.path());
        let local = LocalFsBackend::new(dir.path());
        assert_eq!(
            BackendHandle::borrowed(&local)
                .remove_empty_dirs_under(&prefix)
                .expect("handle"),
            3
        );

        plant(dir.path());
        let faulting = FaultingBackend::new(LocalFsBackend::new(dir.path()), FaultScript::empty());
        assert_eq!(
            faulting.remove_empty_dirs_under(&prefix).expect("faulting"),
            3
        );

        plant(dir.path());
        let watchdog = WriteOrderingWatchdog::new(LocalFsBackend::new(dir.path()));
        assert_eq!(
            watchdog.remove_empty_dirs_under(&prefix).expect("watchdog"),
            3
        );

        assert_eq!(
            MemoryBackend::new()
                .remove_empty_dirs_under(&prefix)
                .expect("default"),
            0
        );
    }

    #[cfg(unix)]
    fn inode(path: &std::path::Path) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(path).expect("stat");
        (metadata.ino(), metadata.nlink())
    }

    #[test]
    fn map_link_error_truth_table() {
        use super::map_link_error;
        use std::io::{Error, ErrorKind};
        let cases = [
            // No hard link between these paths: a missing capability.
            (
                ErrorKind::CrossesDevices,
                BackendErrorKind::UnsupportedOperation,
            ),
            (
                ErrorKind::Unsupported,
                BackendErrorKind::UnsupportedOperation,
            ),
            // Everything else maps like any other I/O error.
            (ErrorKind::AlreadyExists, BackendErrorKind::AlreadyExists),
            (ErrorKind::NotFound, BackendErrorKind::NotFound),
            (
                ErrorKind::PermissionDenied,
                BackendErrorKind::PermissionDenied,
            ),
            (ErrorKind::StorageFull, BackendErrorKind::NoSpace),
            (ErrorKind::Interrupted, BackendErrorKind::Interrupted),
            (ErrorKind::Other, BackendErrorKind::Unknown),
        ];
        for (io_kind, expected) in cases {
            assert_eq!(
                map_link_error(&Error::from(io_kind)).kind(),
                expected,
                "{io_kind:?}"
            );
        }
    }

    /// #3721: a link is a second directory entry for the same inode — no
    /// payload byte is written — and it leaves the source in place.
    #[cfg(unix)]
    #[test]
    fn link_object_shares_the_source_inode_and_leaves_the_source() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");
        backend
            .publish_object(&source, b"table-bytes", PublishMode::Create)
            .expect("seed");
        let (source_inode, _) = inode(&backend.path_for(&source));

        let outcome = backend.link_object(&source, &target).expect("link");

        assert_eq!(outcome.object(), &target);
        assert_eq!(outcome.metadata().size_bytes(), 11);
        assert_eq!(outcome.durability(), PublishDurability::Durable);
        assert_eq!(
            inode(&backend.path_for(&target)),
            (source_inode, 2),
            "the target is the source's inode, now with two names"
        );
        assert_eq!(
            backend.read_object(&source).expect("source"),
            b"table-bytes"
        );
        assert_eq!(
            backend.read_object(&target).expect("target"),
            b"table-bytes"
        );
        assert_eq!(
            backend
                .list_prefix(&ObjectPrefix::new("quarantine/").expect("prefix"))
                .expect("list"),
            vec![target.clone()]
        );

        // Deleting the source leaves the target holding the bytes; its
        // emptied directory is pruned as by any delete (#3692).
        backend.delete_object(&source).expect("delete source");
        assert_eq!(inode(&backend.path_for(&target)), (source_inode, 1));
        assert_eq!(
            backend.read_object(&target).expect("target"),
            b"table-bytes"
        );
        assert!(!dir.path().join("tables/main").exists());
        assert!(dir.path().join("tables").is_dir());
    }

    /// #3721: create-mode no-clobber — an existing target is refused and kept.
    #[cfg(unix)]
    #[test]
    fn link_object_refuses_an_existing_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");
        backend.write_object(&source, b"source").expect("seed");
        backend.write_object(&target, b"other").expect("seed");

        let error = backend
            .link_object(&source, &target)
            .expect_err("no clobber");

        assert_eq!(error.kind(), PublishFailureKind::PreconditionFailed);
        assert_eq!(backend.read_object(&target).expect("kept"), b"other");
        assert_eq!(backend.read_object(&source).expect("kept"), b"source");
    }

    /// #3721: a missing source, and a fault at the link itself, fail before
    /// anything becomes visible.
    #[cfg(unix)]
    #[test]
    fn link_object_failures_before_the_link_leave_nothing_visible() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");

        let missing = backend.link_object(&source, &target).expect_err("missing");
        assert_eq!(missing.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(missing.source_error().kind(), BackendErrorKind::NotFound);
        assert!(!backend.path_for(&target).exists());

        backend.write_object(&source, b"source").expect("seed");
        backend
            .inject_targeted_final_publish_fault(target.as_str().to_owned())
            .expect("arm");
        let faulted = backend.link_object(&source, &target).expect_err("fault");
        assert_eq!(faulted.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(faulted.source_error().kind(), BackendErrorKind::Interrupted);
        assert!(!backend.path_for(&target).exists());
        assert_eq!(inode(&backend.path_for(&source)).1, 1);
    }

    /// #3721: a parent-sync failure after the link is visible-but-unconfirmed.
    #[cfg(unix)]
    #[test]
    fn link_object_parent_sync_fault_is_visible_unconfirmed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");
        backend.write_object(&source, b"source").expect("seed");
        backend
            .inject_targeted_publish_fault_visible_unconfirmed(target.as_str().to_owned())
            .expect("arm");

        let error = backend.link_object(&source, &target).expect_err("fault");

        assert_eq!(
            error.kind(),
            PublishFailureKind::VisibleDurabilityUnconfirmed
        );
        assert_eq!(backend.read_object(&target).expect("visible"), b"source");
        assert_eq!(backend.read_object(&source).expect("kept"), b"source");
    }

    /// #3721 x #3692: a concurrent delete that empties the target's directory
    /// prunes it; the link re-creates the parent and lands.
    #[cfg(unix)]
    #[test]
    fn link_object_survives_its_parent_being_pruned_before_the_link_lands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");
        backend.write_object(&source, b"source").expect("seed");

        backend.arm_parent_race(1);
        backend
            .link_object(&source, &target)
            .expect("link rides the race");

        assert_eq!(backend.parent_race_remaining(), 0, "the race fired");
        assert_eq!(backend.read_object(&target).expect("linked"), b"source");
    }

    /// #3721: a `link(2)` that fails for any reason other than an existing
    /// target is a before-visibility failure carrying its own kind — never
    /// the no-clobber `PreconditionFailed`, which is reserved for
    /// `AlreadyExists`. Two real failures: the parent pruned on every bounded
    /// attempt (`NotFound`), and the parent replaced by a file (not a
    /// directory).
    #[cfg(unix)]
    #[test]
    fn link_object_failures_other_than_an_existing_target_are_not_preconditions() {
        use super::{LocalFsParentRace, OBJECT_CREATION_ATTEMPTS};
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LocalFsBackend::new(dir.path());
        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        backend.write_object(&source, b"source").expect("seed");

        let exhausted = ObjectName::new("quarantine/branch-a/table0001").expect("name");
        backend.arm_parent_race(OBJECT_CREATION_ATTEMPTS);
        let error = backend
            .link_object(&source, &exhausted)
            .expect_err("parent pruned on every attempt");
        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_eq!(error.source_error().kind(), BackendErrorKind::NotFound);
        assert_eq!(backend.parent_race_remaining(), 0);

        let blocked = ObjectName::new("quarantine/branch-b/table0001").expect("name");
        backend.arm_parent_race_with(1, LocalFsParentRace::ReplaceWithFile);
        let error = backend
            .link_object(&source, &blocked)
            .expect_err("parent is a file");
        assert_eq!(error.kind(), PublishFailureKind::FailedBeforeVisibility);
        assert_ne!(error.source_error().kind(), BackendErrorKind::AlreadyExists);
        assert_eq!(backend.read_object(&source).expect("kept"), b"source");
    }

    /// #3721: every production and harness wrapper forwards the link except the
    /// fault injector, which keeps the trait default so the quarantine stage
    /// falls back to the byte copy its fault scripts target; a backend without
    /// hard links refuses before any mutation.
    #[cfg(unix)]
    #[test]
    fn link_object_is_forwarded_by_every_wrapper_but_the_fault_injector() {
        use crate::backend::memory::MemoryBackend;
        use crate::backend::BackendHandle;
        use crate::testkit::{
            FaultScript, FaultingBackend, ReorderingBackend, WriteOrderingWatchdog,
        };

        let source = ObjectName::new("tables/main/l0000/table0001").expect("name");
        let target = ObjectName::new("quarantine/branch/table0001").expect("name");
        let linked = |backend: &dyn Backend, root: &std::path::Path| {
            let local = LocalFsBackend::new(root);
            local.write_object(&source, b"source").expect("seed");
            backend
                .link_object(&source, &target)
                .expect("forwarded link");
            let shared = inode(&local.path_for(&target));
            assert_eq!(shared, inode(&local.path_for(&source)));
            assert_eq!(shared.1, 2);
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let local = LocalFsBackend::new(dir.path());
        linked(&BackendHandle::borrowed(&local), dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        linked(
            &WriteOrderingWatchdog::new(LocalFsBackend::new(dir.path())),
            dir.path(),
        );

        let dir = tempfile::tempdir().expect("tempdir");
        linked(&ReorderingBackend::local_fs(dir.path()), dir.path());

        let dir = tempfile::tempdir().expect("tempdir");
        let faulting = FaultingBackend::new(LocalFsBackend::new(dir.path()), FaultScript::empty());
        faulting.write_object(&source, b"source").expect("seed");
        let refused = faulting.link_object(&source, &target).expect_err("default");
        assert_eq!(refused.kind(), PublishFailureKind::Unsupported);
        assert!(!LocalFsBackend::new(dir.path()).path_for(&target).exists());

        let memory = MemoryBackend::new();
        memory.write_object(&source, b"source").expect("seed");
        assert!(!memory
            .capabilities()
            .contains(BackendCapability::DurableLink));
        assert_eq!(
            memory
                .link_object(&source, &target)
                .expect_err("default")
                .kind(),
            PublishFailureKind::Unsupported
        );
        assert!(memory.read_object(&target).is_err());
    }
}
