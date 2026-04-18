use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

fn sync_failure_slot() -> &'static Mutex<HashMap<PathBuf, io::ErrorKind>> {
    static SYNC_FAILURE: OnceLock<Mutex<HashMap<PathBuf, io::ErrorKind>>> = OnceLock::new();
    SYNC_FAILURE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn begin_sync_failure_slot() -> &'static Mutex<HashMap<PathBuf, io::ErrorKind>> {
    static BEGIN_SYNC_FAILURE: OnceLock<Mutex<HashMap<PathBuf, io::ErrorKind>>> = OnceLock::new();
    BEGIN_SYNC_FAILURE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn commit_sync_failure_slot() -> &'static Mutex<HashMap<PathBuf, io::ErrorKind>> {
    static COMMIT_SYNC_FAILURE: OnceLock<Mutex<HashMap<PathBuf, io::ErrorKind>>> = OnceLock::new();
    COMMIT_SYNC_FAILURE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn flush_thread_spawn_failure_slot() -> &'static Mutex<HashSet<PathBuf>> {
    static FLUSH_THREAD_SPAWN_FAILURE: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    FLUSH_THREAD_SPAWN_FAILURE.get_or_init(|| Mutex::new(HashSet::new()))
}

#[derive(Default)]
struct HaltPublishPauseState {
    reached: bool,
    released: bool,
}

type HaltPublishPause = Arc<(Mutex<HaltPublishPauseState>, Condvar)>;

fn halt_publish_pause_slot() -> &'static Mutex<HashMap<PathBuf, HaltPublishPause>> {
    static HALT_PUBLISH_PAUSE: OnceLock<Mutex<HashMap<PathBuf, HaltPublishPause>>> =
        OnceLock::new();
    HALT_PUBLISH_PAUSE.get_or_init(|| Mutex::new(HashMap::new()))
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

pub(super) fn inject_begin_sync_failure(path: &Path, kind: io::ErrorKind) {
    begin_sync_failure_slot()
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), kind);
}

pub(super) fn clear_begin_sync_failure(path: &Path) {
    begin_sync_failure_slot().lock().unwrap().remove(path);
}

pub(super) fn inject_commit_sync_failure(path: &Path, kind: io::ErrorKind) {
    commit_sync_failure_slot()
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), kind);
}

pub(super) fn clear_commit_sync_failure(path: &Path) {
    commit_sync_failure_slot().lock().unwrap().remove(path);
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

pub(super) fn install_halt_publish_pause(path: &Path) {
    halt_publish_pause_slot().lock().unwrap().insert(
        path.to_path_buf(),
        Arc::new((Mutex::new(HaltPublishPauseState::default()), Condvar::new())),
    );
}

pub(super) fn clear_halt_publish_pause(path: &Path) {
    let pause = halt_publish_pause_slot().lock().unwrap().remove(path);
    if let Some(pause) = pause {
        let (state, cv) = &*pause;
        let mut state = state.lock().unwrap();
        state.released = true;
        cv.notify_all();
    }
}

pub(super) fn maybe_inject_sync_failure(path: &Path) -> Option<io::Error> {
    sync_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
        .map(|kind| io::Error::new(kind, "injected sync failure"))
}

pub(super) fn maybe_inject_begin_sync_failure(path: &Path) -> Option<io::Error> {
    begin_sync_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
        .map(|kind| io::Error::new(kind, "injected begin_background_sync failure"))
}

pub(super) fn maybe_inject_commit_sync_failure(path: &Path) -> Option<io::Error> {
    commit_sync_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
        .map(|kind| io::Error::new(kind, "injected commit_background_sync failure"))
}

pub(super) fn take_flush_thread_spawn_failure(path: &Path) -> bool {
    flush_thread_spawn_failure_slot()
        .lock()
        .unwrap()
        .remove(path)
}

pub(super) fn maybe_pause_after_halt_health_publish(path: &Path) {
    let pause = halt_publish_pause_slot().lock().unwrap().get(path).cloned();
    if let Some(pause) = pause {
        let (state, cv) = &*pause;
        let mut state = state.lock().unwrap();
        state.reached = true;
        cv.notify_all();
        while !state.released {
            state = cv.wait(state).unwrap();
        }
    }
}

pub(super) fn wait_for_halt_publish_pause(path: &Path, timeout: Duration) -> bool {
    let pause = halt_publish_pause_slot().lock().unwrap().get(path).cloned();
    let Some(pause) = pause else {
        return false;
    };

    let start = Instant::now();
    let (state, cv) = &*pause;
    let mut state = state.lock().unwrap();
    while !state.reached {
        let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
            return false;
        };
        let (next_state, wait_result) = cv.wait_timeout(state, remaining).unwrap();
        state = next_state;
        if wait_result.timed_out() && !state.reached {
            return false;
        }
    }
    true
}

pub(super) fn release_halt_publish_pause(path: &Path) {
    let pause = halt_publish_pause_slot().lock().unwrap().get(path).cloned();
    if let Some(pause) = pause {
        let (state, cv) = &*pause;
        let mut state = state.lock().unwrap();
        state.released = true;
        cv.notify_all();
    }
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

    #[test]
    fn test_begin_sync_failure_hook_consumes_one_failure() {
        let path = Path::new("/tmp/test-begin-sync-failure");
        let other_path = Path::new("/tmp/test-begin-sync-failure-other");
        clear_begin_sync_failure(path);
        clear_begin_sync_failure(other_path);
        assert!(maybe_inject_begin_sync_failure(path).is_none());
        assert!(maybe_inject_begin_sync_failure(other_path).is_none());

        inject_begin_sync_failure(path, io::ErrorKind::Other);
        assert!(
            maybe_inject_begin_sync_failure(other_path).is_none(),
            "hook must be scoped by path"
        );
        let err = maybe_inject_begin_sync_failure(path).expect("expected injected failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_begin_sync_failure(path).is_none());
    }

    #[test]
    fn test_commit_sync_failure_hook_consumes_one_failure() {
        let path = Path::new("/tmp/test-commit-sync-failure");
        let other_path = Path::new("/tmp/test-commit-sync-failure-other");
        clear_commit_sync_failure(path);
        clear_commit_sync_failure(other_path);
        assert!(maybe_inject_commit_sync_failure(path).is_none());
        assert!(maybe_inject_commit_sync_failure(other_path).is_none());

        inject_commit_sync_failure(path, io::ErrorKind::Other);
        assert!(
            maybe_inject_commit_sync_failure(other_path).is_none(),
            "hook must be scoped by path"
        );
        let err = maybe_inject_commit_sync_failure(path).expect("expected injected failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_commit_sync_failure(path).is_none());
    }

    #[test]
    fn test_halt_publish_pause_hook_reaches_and_releases() {
        let path = Path::new("/tmp/test-halt-publish-pause");
        clear_halt_publish_pause(path);
        install_halt_publish_pause(path);

        let thread_path = path.to_path_buf();
        let handle = std::thread::spawn(move || {
            maybe_pause_after_halt_health_publish(&thread_path);
        });

        assert!(
            wait_for_halt_publish_pause(path, Duration::from_secs(1)),
            "pause hook should report when it is reached"
        );
        release_halt_publish_pause(path);
        handle.join().unwrap();
        clear_halt_publish_pause(path);
    }
}
