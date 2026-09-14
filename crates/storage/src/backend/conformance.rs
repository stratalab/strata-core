use super::{
    Backend, BackendErrorKind, DeleteDurability, DeleteStatus, PublishDurability, PublishMode,
    BASIC_OBJECT_BACKEND_CAPABILITIES, CACHE_MODE_REQUIREMENTS, DURABLE_LOCAL_MODE_REQUIREMENTS,
};
use crate::config::mode::{DurabilityPolicy, StorageModeRequest};
use crate::layout::ObjectLayout;
use crate::test_support::{
    assert_backend_error_kind, assert_backend_list, object_name as name, range,
};

fn assert_basic_object_conformance(backend: &dyn Backend) {
    assert_basic_capabilities(backend);
    assert_storage_mode_validation(backend);
    assert_missing_object_behavior(backend);
    assert_write_read_metadata_and_ranges(backend);
    assert_prefix_listing_and_delete(backend);
    assert_conditional_operations_are_unsupported(backend);
}

fn assert_basic_capabilities(backend: &dyn Backend) {
    let capabilities = backend.capabilities();

    assert!(capabilities.supports(CACHE_MODE_REQUIREMENTS));
    assert!(capabilities.supports(BASIC_OBJECT_BACKEND_CAPABILITIES));
}

fn assert_storage_mode_validation(backend: &dyn Backend) {
    let capabilities = backend.capabilities();

    StorageModeRequest::cache()
        .validate_backend(capabilities)
        .expect("basic object backend should satisfy cache mode");

    for request in [
        StorageModeRequest::durable_local(DurabilityPolicy::Standard),
        StorageModeRequest::durable_local(DurabilityPolicy::Always),
    ] {
        if capabilities.supports(DURABLE_LOCAL_MODE_REQUIREMENTS) {
            request
                .validate_backend(capabilities)
                .expect("backend with durable-local capabilities should satisfy durable mode");
        } else {
            let error = request.validate_backend(capabilities).expect_err(
                "backend without durable-local capabilities should reject durable mode",
            );

            assert_eq!(error.kind(), BackendErrorKind::CapabilityMismatch);
            assert!(
                !request.missing_capabilities(capabilities).is_empty(),
                "capability mismatch should report at least one missing capability"
            );
        }
    }

    let object_request = StorageModeRequest::object_durable_candidate();
    let error = object_request
        .validate_backend(capabilities)
        .expect_err("basic local backends should not satisfy object-durable mode");

    assert_eq!(error.kind(), BackendErrorKind::CapabilityMismatch);
    assert!(
        !object_request.missing_capabilities(capabilities).is_empty(),
        "capability mismatch should report at least one missing capability"
    );
}

fn assert_missing_object_behavior(backend: &dyn Backend) {
    let missing = name("objects/missing");

    assert_backend_error_kind(backend.read_object(&missing), BackendErrorKind::NotFound);
    assert_backend_error_kind(
        backend.read_range(&missing, range(0, 1)),
        BackendErrorKind::NotFound,
    );
    assert_backend_error_kind(
        backend.object_metadata(&missing),
        BackendErrorKind::NotFound,
    );
    let outcome = backend
        .delete_object(&missing)
        .expect("missing delete should be idempotent");
    assert_eq!(outcome.object(), &missing);
    assert_eq!(outcome.status(), DeleteStatus::AlreadyMissing);
}

fn assert_write_read_metadata_and_ranges(backend: &dyn Backend) {
    let item = name("range/item");

    let metadata = backend
        .write_object(&item, b"abcdef")
        .expect("write should succeed");
    assert_eq!(metadata.size_bytes(), 6);
    assert_eq!(metadata.fence(), None);

    assert_eq!(
        backend.read_object(&item).expect("read should succeed"),
        b"abcdef"
    );
    assert_eq!(
        backend
            .object_metadata(&item)
            .expect("metadata should exist")
            .size_bytes(),
        6
    );
    assert_eq!(
        backend
            .read_range(&item, range(2, 3))
            .expect("range should succeed"),
        b"cde"
    );
    assert_eq!(
        backend
            .read_range(&item, range(2, 20))
            .expect("range should truncate at object length"),
        b"cdef"
    );
    assert_eq!(
        backend
            .read_range(&item, range(3, 0))
            .expect("zero-length range should succeed"),
        b""
    );
    assert_eq!(
        backend
            .read_range(&item, range(99, 1))
            .expect("range past end should return empty bytes"),
        b""
    );
    assert_backend_error_kind(
        backend.read_range(&item, range(u64::MAX, 1)),
        BackendErrorKind::InvalidRange,
    );

    let replacement = backend
        .write_object(&item, b"xy")
        .expect("overwrite should succeed");
    assert_eq!(replacement.size_bytes(), 2);
    assert_eq!(backend.read_object(&item).expect("read replacement"), b"xy");
}

fn assert_prefix_listing_and_delete(backend: &dyn Backend) {
    let parent = name("listing/tree");
    let child = name("listing/tree/child");
    let sibling = name("listing/tree/sibling");
    let other = name("listing/other");

    backend
        .write_object(&parent, b"parent")
        .expect("parent write");
    backend.write_object(&child, b"child").expect("child write");
    backend
        .write_object(&sibling, b"sibling")
        .expect("sibling write");
    backend.write_object(&other, b"other").expect("other write");

    assert_backend_list(
        backend,
        "listing/",
        &[
            "listing/other",
            "listing/tree",
            "listing/tree/child",
            "listing/tree/sibling",
        ],
    );
    assert_backend_list(
        backend,
        "listing/tree/",
        &["listing/tree/child", "listing/tree/sibling"],
    );

    let outcome = backend.delete_object(&child).expect("delete child");
    assert_eq!(outcome.object(), &child);
    assert_eq!(outcome.status(), DeleteStatus::Deleted);
    assert_backend_error_kind(backend.read_object(&child), BackendErrorKind::NotFound);
    assert_backend_list(backend, "listing/tree/", &["listing/tree/sibling"]);
}

fn assert_conditional_operations_are_unsupported(backend: &dyn Backend) {
    let name = name("objects/conditional");
    let fence = super::BackendFence::new([1, 2, 3]);

    assert_backend_error_kind(
        backend.conditional_create(&name, b"bytes"),
        BackendErrorKind::UnsupportedOperation,
    );
    assert_backend_error_kind(
        backend.conditional_update(&name, &fence, b"bytes"),
        BackendErrorKind::UnsupportedOperation,
    );
}

fn assert_cache_mode_conformance(backend: &dyn Backend) {
    let capabilities = backend.capabilities();

    assert!(capabilities.supports(CACHE_MODE_REQUIREMENTS));
    StorageModeRequest::cache()
        .validate_backend(capabilities)
        .expect("backend should satisfy cache mode");
}

fn assert_durable_local_capability_mismatch(backend: &dyn Backend) {
    let capabilities = backend.capabilities();
    let request = StorageModeRequest::durable_local(DurabilityPolicy::Standard);
    let error = request
        .validate_backend(capabilities)
        .expect_err("backend should not satisfy durable local mode");

    assert_eq!(error.kind(), BackendErrorKind::CapabilityMismatch);
    assert!(
        !request.missing_capabilities(capabilities).is_empty(),
        "durable local mismatch should report missing capabilities"
    );
}

fn assert_durable_local_conformance(backend: &dyn Backend) {
    let capabilities = backend.capabilities();

    assert!(capabilities.supports(DURABLE_LOCAL_MODE_REQUIREMENTS));
    StorageModeRequest::durable_local(DurabilityPolicy::Standard)
        .validate_backend(capabilities)
        .expect("backend should satisfy standard durable local mode");
    StorageModeRequest::durable_local(DurabilityPolicy::Always)
        .validate_backend(capabilities)
        .expect("backend should satisfy always-durable local mode");

    let lock_name = ObjectLayout::writer_lock().expect("writer lock name");
    let guard = backend
        .acquire_writer_lock(&lock_name)
        .expect("acquire writer lock");
    assert_eq!(guard.object(), &lock_name);
    drop(guard);

    let object = name("durable-conformance/object");
    let publish = backend
        .publish_object(&object, b"abc", PublishMode::Replace)
        .expect("durable publish");
    assert_eq!(publish.object(), &object);
    assert_eq!(publish.metadata().size_bytes(), 3);
    assert_eq!(publish.durability(), PublishDurability::Durable);
    assert_eq!(
        backend.read_object(&object).expect("published bytes"),
        b"abc"
    );

    let append = backend.append_object(&object, b"def").expect("append");
    assert_eq!(append.start_offset(), 3);
    assert_eq!(append.bytes_written(), 3);
    assert_eq!(append.metadata().size_bytes(), 6);
    assert_eq!(
        backend.read_object(&object).expect("appended bytes"),
        b"abcdef"
    );

    backend.sync_object(&object).expect("sync object");

    let delete = backend.delete_object(&object).expect("delete object");
    assert_eq!(delete.object(), &object);
    assert_eq!(delete.status(), DeleteStatus::Deleted);
    assert_eq!(delete.durability(), DeleteDurability::Durable);
    assert_backend_error_kind(backend.read_object(&object), BackendErrorKind::NotFound);

    let missing_delete = backend.delete_object(&object).expect("missing delete");
    assert_eq!(missing_delete.object(), &object);
    assert_eq!(missing_delete.status(), DeleteStatus::AlreadyMissing);
}

fn assert_durable_local_writer_lock_exclusion(first: &dyn Backend, second: &dyn Backend) {
    let lock_name = ObjectLayout::writer_lock().expect("writer lock name");
    let first_guard = first
        .acquire_writer_lock(&lock_name)
        .expect("first writer lock");

    assert_eq!(first_guard.object(), &lock_name);
    assert_eq!(
        second
            .acquire_writer_lock(&lock_name)
            .expect_err("second writer lock is held by the first opener")
            .kind(),
        // Contention is its own kind, not a transient outage (#3005, #3167).
        BackendErrorKind::AlreadyExists
    );

    drop(first_guard);
    let second_guard = second
        .acquire_writer_lock(&lock_name)
        .expect("released writer lock can be reacquired");
    assert_eq!(second_guard.object(), &lock_name);
}

#[cfg(test)]
mod tests {
    use super::{
        assert_basic_object_conformance, assert_cache_mode_conformance,
        assert_durable_local_capability_mismatch, assert_durable_local_conformance,
        assert_durable_local_writer_lock_exclusion,
    };
    use crate::backend::{memory::MemoryBackend, Backend, DeleteDurability};

    #[test]
    fn memory_backend_satisfies_basic_object_conformance() {
        let backend = MemoryBackend::new();

        assert_basic_object_conformance(&backend);
    }

    #[test]
    fn memory_backend_satisfies_cache_mode_conformance() {
        let backend = MemoryBackend::new();

        assert_cache_mode_conformance(&backend);
    }

    #[test]
    fn memory_backend_rejects_durable_local_conformance() {
        let backend = MemoryBackend::new();

        assert_durable_local_capability_mismatch(&backend);
        let name = crate::test_support::object_name("cache/delete-durability");
        backend.write_object(&name, b"bytes").expect("seed");
        let outcome = backend.delete_object(&name).expect("cache delete");
        assert_eq!(outcome.durability(), DeleteDurability::NonDurable);
    }

    #[cfg(feature = "localfs")]
    #[test]
    fn localfs_backend_satisfies_basic_object_conformance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());

        assert_basic_object_conformance(&backend);
    }

    #[cfg(feature = "localfs")]
    #[test]
    fn localfs_backend_satisfies_cache_mode_conformance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());

        assert_cache_mode_conformance(&backend);
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn localfs_backend_satisfies_durable_local_conformance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());

        assert_durable_local_conformance(&backend);
    }

    #[cfg(all(feature = "localfs", unix))]
    #[test]
    fn localfs_backend_satisfies_durable_local_writer_lock_exclusion() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = crate::backend::local_fs::LocalFsBackend::new(dir.path());
        let second = crate::backend::local_fs::LocalFsBackend::new(dir.path());

        assert_durable_local_writer_lock_exclusion(&first, &second);
    }

    #[cfg(all(feature = "localfs", not(unix)))]
    #[test]
    fn localfs_backend_rejects_durable_local_conformance_without_directory_sync() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = crate::backend::local_fs::LocalFsBackend::new(dir.path());

        assert_durable_local_capability_mismatch(&backend);
    }
}
