use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

fn sync_failure_slot() -> &'static Mutex<HashMap<PathBuf, io::ErrorKind>> {
    static SYNC_FAILURE: OnceLock<Mutex<HashMap<PathBuf, io::ErrorKind>>> = OnceLock::new();
    SYNC_FAILURE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn flush_thread_spawn_failure_slot() -> &'static Mutex<HashSet<PathBuf>> {
    static FLUSH_THREAD_SPAWN_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    FLUSH_THREAD_SPAWN_FAILURE.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(super) fn inject_sync_failure(path: &Path, kind: io::ErrorKind) {
    sync_failure_slot()
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), kind);
}

pub(super) fn clear_sync_failure(path: &Path) {
    sync_failure_slot().lock().unwrap().remove(path);
}

pub(super) fn inject_flush_thread_spawn_failure(path: &Path) {
    flush_thread_spawn_failure_slot()
        .lock()
        .unwrap()
        .insert(path.to_path_buf());
}

pub(super) fn clear_flush_thread_spawn_failure(path: &Path) {
    flush_thread_spawn_failure_slot()
        .lock()
        .unwrap()
        .remove(path);
}

pub(super) fn maybe_inject_sync_failure(path: &Path) -> Option<io::Error> {
    sync_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
        .map(|kind| io::Error::new(kind, "injected sync failure"))
}

pub(super) fn take_flush_thread_spawn_failure(path: &Path) -> bool {
    flush_thread_spawn_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_failure_hook_consumes_one_failure() {
        let path = Path::new("/tmp/test-sync-failure");
        let other_path = Path::new("/tmp/test-sync-failure-other");
        clear_sync_failure(path);
        clear_sync_failure(other_path);
        assert!(maybe_inject_sync_failure(path).is_none());
        assert!(maybe_inject_sync_failure(other_path).is_none());

        inject_sync_failure(path, io::ErrorKind::Other);
        assert!(
            maybe_inject_sync_failure(other_path).is_none(),
            "hook must be scoped by path"
        );
        let err = maybe_inject_sync_failure(path).expect("expected injected failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_sync_failure(path).is_none());
    }

    #[test]
    fn test_flush_thread_spawn_hook_consumes_one_failure() {
        let path = Path::new("/tmp/test-spawn-failure");
        let other_path = Path::new("/tmp/test-spawn-failure-other");
        clear_flush_thread_spawn_failure(path);
        clear_flush_thread_spawn_failure(other_path);
        assert!(!take_flush_thread_spawn_failure(path));
        assert!(!take_flush_thread_spawn_failure(other_path));

        inject_flush_thread_spawn_failure(path);
        assert!(
            !take_flush_thread_spawn_failure(other_path),
            "hook must be scoped by path"
        );
        assert!(take_flush_thread_spawn_failure(path));
        assert!(!take_flush_thread_spawn_failure(path));
    }
}
