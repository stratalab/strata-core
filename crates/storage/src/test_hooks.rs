#[cfg(test)]
use std::cell::Cell;
use std::io;

#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use std::sync::Mutex;

#[cfg(test)]
thread_local! {
    static MANIFEST_PUBLISH_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
    static MANIFEST_DIR_FSYNC_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
}

#[cfg(not(test))]
fn manifest_publish_failure_slot() -> &'static Mutex<Option<io::ErrorKind>> {
    static MANIFEST_PUBLISH_FAILURE: OnceLock<Mutex<Option<io::ErrorKind>>> = OnceLock::new();
    MANIFEST_PUBLISH_FAILURE.get_or_init(|| Mutex::new(None))
}

#[cfg(not(test))]
fn manifest_dir_fsync_failure_slot() -> &'static Mutex<Option<io::ErrorKind>> {
    static MANIFEST_DIR_FSYNC_FAILURE: OnceLock<Mutex<Option<io::ErrorKind>>> = OnceLock::new();
    MANIFEST_DIR_FSYNC_FAILURE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn inject_manifest_publish_failure(kind: io::ErrorKind) {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(Some(kind)));
}

#[cfg(test)]
pub(crate) fn clear_manifest_publish_failure() {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(None));
}

pub(crate) fn maybe_inject_manifest_publish_failure() -> Option<io::Error> {
    #[cfg(test)]
    let kind = MANIFEST_PUBLISH_FAILURE.with(|slot| slot.take());
    #[cfg(not(test))]
    let kind = manifest_publish_failure_slot().lock().unwrap().take();

    kind.map(|kind| io::Error::new(kind, "injected manifest publish failure"))
}

#[cfg(test)]
pub(crate) fn inject_manifest_dir_fsync_failure(kind: io::ErrorKind) {
    MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.set(Some(kind)));
}

#[cfg(test)]
pub(crate) fn clear_manifest_dir_fsync_failure() {
    MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.set(None));
}

pub(crate) fn maybe_inject_manifest_dir_fsync_failure() -> Option<io::Error> {
    #[cfg(test)]
    let kind = MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.take());
    #[cfg(not(test))]
    let kind = manifest_dir_fsync_failure_slot().lock().unwrap().take();

    kind.map(|kind| io::Error::new(kind, "injected manifest dir fsync failure"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_publish_failure_hook_consumes_one_failure() {
        clear_manifest_publish_failure();
        assert!(maybe_inject_manifest_publish_failure().is_none());

        inject_manifest_publish_failure(io::ErrorKind::Other);
        let err =
            maybe_inject_manifest_publish_failure().expect("expected injected publish failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_manifest_publish_failure().is_none());
    }

    #[test]
    fn test_manifest_dir_fsync_failure_hook_consumes_one_failure() {
        clear_manifest_dir_fsync_failure();
        assert!(maybe_inject_manifest_dir_fsync_failure().is_none());

        inject_manifest_dir_fsync_failure(io::ErrorKind::Other);
        let err =
            maybe_inject_manifest_dir_fsync_failure().expect("expected injected fsync failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_manifest_dir_fsync_failure().is_none());
    }
}
