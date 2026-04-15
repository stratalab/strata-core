use std::io;
use std::sync::{Mutex, OnceLock};

fn sync_failure_slot() -> &'static Mutex<Option<io::ErrorKind>> {
    static SYNC_FAILURE: OnceLock<Mutex<Option<io::ErrorKind>>> = OnceLock::new();
    SYNC_FAILURE.get_or_init(|| Mutex::new(None))
}

fn flush_thread_spawn_failure_slot() -> &'static Mutex<bool> {
    static FLUSH_THREAD_SPAWN_FAILURE: OnceLock<Mutex<bool>> = OnceLock::new();
    FLUSH_THREAD_SPAWN_FAILURE.get_or_init(|| Mutex::new(false))
}

pub(super) fn inject_sync_failure(kind: io::ErrorKind) {
    *sync_failure_slot().lock().unwrap() = Some(kind);
}

pub(super) fn clear_sync_failure() {
    *sync_failure_slot().lock().unwrap() = None;
}

pub(super) fn inject_flush_thread_spawn_failure() {
    *flush_thread_spawn_failure_slot().lock().unwrap() = true;
}

pub(super) fn clear_flush_thread_spawn_failure() {
    *flush_thread_spawn_failure_slot().lock().unwrap() = false;
}

pub(super) fn maybe_inject_sync_failure() -> Option<io::Error> {
    sync_failure_slot()
        .lock()
        .unwrap()
        .take()
        .map(|kind| io::Error::new(kind, "injected sync failure"))
}

pub(super) fn take_flush_thread_spawn_failure() -> bool {
    let mut slot = flush_thread_spawn_failure_slot().lock().unwrap();
    let should_fail = *slot;
    *slot = false;
    should_fail
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_failure_hook_consumes_one_failure() {
        clear_sync_failure();
        assert!(maybe_inject_sync_failure().is_none());

        inject_sync_failure(io::ErrorKind::Other);
        let err = maybe_inject_sync_failure().expect("expected injected failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_sync_failure().is_none());
    }

    #[test]
    fn test_flush_thread_spawn_hook_consumes_one_failure() {
        clear_flush_thread_spawn_failure();
        assert!(!take_flush_thread_spawn_failure());

        inject_flush_thread_spawn_failure();
        assert!(take_flush_thread_spawn_failure());
        assert!(!take_flush_thread_spawn_failure());
    }
}
