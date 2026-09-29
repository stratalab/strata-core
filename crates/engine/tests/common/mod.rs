#![allow(clippy::result_large_err, dead_code)]

use strata_engine::{
    BranchName, CacheOpenOptions, Database, DatabaseOpenOutcome, DurableLocalOpenOptions,
    EngineError, EngineErrorClass, EngineResult, KvKey, KvValue, ProductSpace,
};

pub(crate) fn open_cache_database() -> EngineResult<Database> {
    Database::open_cache(CacheOpenOptions::new()).map(DatabaseOpenOutcome::into_database)
}

pub(crate) fn open_durable_database(path: &std::path::Path) -> EngineResult<Database> {
    Database::open_local(path, DurableLocalOpenOptions::new())
        .map(DatabaseOpenOutcome::into_database)
}

/// The writer-lock code a same-process reopen sees while the previous
/// runtime still holds the lock.
const WRITER_LOCK_CODE: &str = "failed_precondition.engine.writer_lock";

/// Budget for [`reopen_after_drop`]: generous, because a detached worker's
/// task is not bounded by the 250 ms shutdown window, and a loaded CI runner
/// (thread sanitizer, the parallel mutation baseline) stretches it further.
const REOPEN_AFTER_DROP_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Reopens a durable database whose previous handle in this process was
/// DROPPED, not closed (#2837, #3546).
///
/// `Database::close` releases the writer lock before it returns. A drop does
/// not promise that: it bounds background shutdown and detaches a worker that
/// misses the window, and the detached worker keeps the runtime — and its
/// writer lock — until its task finishes (pinned in storage by
/// `a_detached_worker_holds_the_writer_lock_past_drop_but_not_past_close`).
/// A test that drops and reopens the same path must either close first or
/// reopen through here. Only `failed_precondition.engine.writer_lock` is
/// retried; any other error, and the lock still held when the budget runs
/// out, is returned unchanged so a real leak fails loud.
pub(crate) fn reopen_after_drop<T>(mut open: impl FnMut() -> EngineResult<T>) -> EngineResult<T> {
    let deadline = std::time::Instant::now() + REOPEN_AFTER_DROP_BUDGET;
    let mut backoff = std::time::Duration::from_millis(2);
    loop {
        match open() {
            Err(error)
                if error.code() == WRITER_LOCK_CODE && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
            }
            outcome => return outcome,
        }
    }
}

/// [`open_durable_database`] for a path whose previous handle was dropped:
/// see [`reopen_after_drop`].
pub(crate) fn reopen_durable_database_after_drop(path: &std::path::Path) -> EngineResult<Database> {
    reopen_local_after_drop(path, &DurableLocalOpenOptions::new())
}

/// `Database::open_local` with `options` for a path whose previous handle was
/// dropped: see [`reopen_after_drop`].
pub(crate) fn reopen_local_after_drop(
    path: &std::path::Path,
    options: &DurableLocalOpenOptions,
) -> EngineResult<Database> {
    reopen_after_drop(|| {
        Database::open_local(path, options.clone()).map(DatabaseOpenOutcome::into_database)
    })
}

pub(crate) fn branch(name: &str) -> BranchName {
    BranchName::new(name).expect("valid branch name")
}

pub(crate) fn space(name: &str) -> ProductSpace {
    ProductSpace::new(name).expect("valid product space")
}

pub(crate) fn key(bytes: &[u8]) -> KvKey {
    KvKey::new(bytes).expect("valid key")
}

pub(crate) fn value(bytes: &[u8]) -> KvValue {
    KvValue::new(bytes)
}

pub(crate) fn assert_branch_value(
    database: &mut Database,
    branch_name: &str,
    space_name: &str,
    key_bytes: &[u8],
    expected: &[u8],
) {
    let mut kv = database
        .kv(branch(branch_name), space(space_name))
        .expect("KV service opens");
    let value = kv
        .get(&key(key_bytes))
        .expect("KV read succeeds")
        .expect("value exists");
    assert_eq!(value.as_bytes(), expected);
}

pub(crate) fn assert_default_branch_exists(database: &mut Database) {
    let summary = database
        .branches()
        .expect("branch service opens")
        .get(&branch("default"))
        .expect("default branch exists");
    assert_eq!(summary.name().as_str(), "default");
    assert_eq!(summary.generation(), 1);
}

pub(crate) fn assert_no_storage_type_in_engine_error(error: &strata_engine::EngineError) {
    let text = error.to_string();
    for forbidden in [
        "StorageRuntime",
        "CommitBatch",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "BranchRequest",
        "storage_api",
    ] {
        assert!(
            !text.contains(forbidden),
            "engine error exposed storage detail: {text}"
        );
    }
}

/// Asserts an engine error's stable status fields in one place.
pub(crate) fn assert_status(
    error: &EngineError,
    class: EngineErrorClass,
    code: &str,
    retryable: bool,
) {
    assert_eq!(error.class(), class, "unexpected error class for `{code}`");
    assert_eq!(error.code(), code, "unexpected error code");
    assert_eq!(
        error.retryable(),
        retryable,
        "unexpected retryable flag for `{code}`"
    );
}

/// Renders an error plus its full source chain into one string.
fn full_error_text(error: &EngineError) -> String {
    let mut text = error.to_string();
    let mut current = std::error::Error::source(error);
    while let Some(source) = current {
        text.push('\n');
        text.push_str(&source.to_string());
        current = source.source();
    }
    text
}

/// Asserts no storage implementation detail leaks through the error or any of
/// its sources. Unlike [`assert_no_storage_type_in_engine_error`], this walks
/// the whole source chain, where the raw storage error would otherwise surface.
pub(crate) fn assert_no_storage_leak(error: &EngineError) {
    let text = full_error_text(error);
    for forbidden in [
        "StorageRuntime",
        "CommitBatch",
        "StorageSpaceId",
        "StorageKey",
        "StorageValue",
        "BranchRequest",
        "StorageApiError",
        "WalService",
        "ManifestService",
        "TableRuntime",
        "storage_api",
    ] {
        assert!(
            !text.contains(forbidden),
            "engine error leaked storage detail `{forbidden}`: {text}"
        );
    }
}

/// Asserts no credential, token, or signed-URL material leaks through the error
/// or its sources. Forward-looking guard for clone and provider work.
pub(crate) fn assert_no_secret_leak(error: &EngineError) {
    let text = full_error_text(error).to_lowercase();
    for forbidden in [
        "authorization",
        "bearer ",
        "api_key",
        "apikey",
        "access_key",
        "signature=",
        "x-amz-",
    ] {
        assert!(
            !text.contains(forbidden),
            "engine error may have leaked a secret (`{forbidden}`)"
        );
    }
}
