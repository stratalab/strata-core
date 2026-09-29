//! W3.3a differential oracle: a coalescing (buffered) WAL service must produce
//! byte-identical durable segment content and identical growth facts to the
//! direct (unbuffered) service at every durability boundary — the buffer may
//! only change WHEN bytes reach the backend, never WHAT they are.

use super::support::{database_id, record, StoredWalBackend};
use super::{WalService, WalServiceConfig};
use crate::backend::Backend;
use crate::config::mode::DurabilityPolicy;
use crate::layout::ObjectLayout;
use crate::service::wal::WalSegmentCoverage;

const SEGMENT_SIZE: u64 = 16 * 1024;

fn open_service(
    backend: &StoredWalBackend,
    policy: DurabilityPolicy,
    append_buffer_bytes: u64,
    segment_size: u64,
) -> WalService<'_> {
    WalService::open(
        backend,
        database_id(),
        1,
        policy,
        WalServiceConfig::new(segment_size).with_append_buffer_bytes(append_buffer_bytes),
    )
    .expect("open WAL service")
}

/// Every WAL segment object (1..=active) must hold identical bytes in both
/// backends, and the services must agree on every growth-visible fact.
fn assert_converged(
    direct_backend: &StoredWalBackend,
    buffered_backend: &StoredWalBackend,
    direct: &WalService<'_>,
    buffered: &WalService<'_>,
) {
    assert_eq!(direct.active_segment_id(), buffered.active_segment_id());
    assert_eq!(direct.dirty_bytes(), buffered.dirty_bytes());
    assert_eq!(direct.dirty_records(), buffered.dirty_records());
    for segment_id in 1..=direct.active_segment_id() {
        let object = ObjectLayout::wal_segment(segment_id).expect("segment object");
        let direct_bytes = direct_backend.read_object(&object).expect("direct bytes");
        let buffered_bytes = buffered_backend
            .read_object(&object)
            .expect("buffered bytes");
        assert_eq!(
            direct_bytes, buffered_bytes,
            "segment {segment_id} bytes diverged"
        );
    }
}

/// Deterministic pseudo-random sequence (no external entropy in tests).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
}

#[test]
fn buffered_standard_appends_converge_at_durability_barrier() {
    let direct_backend = StoredWalBackend::default();
    let buffered_backend = StoredWalBackend::default();
    let mut direct = open_service(&direct_backend, DurabilityPolicy::Standard, 0, SEGMENT_SIZE);
    let mut buffered = open_service(
        &buffered_backend,
        DurabilityPolicy::Standard,
        u64::from(u16::MAX),
        SEGMENT_SIZE,
    );

    for version in 1..=8u64 {
        let entry = record(version, format!("standard payload {version}").into_bytes());
        let direct_append = direct.append(&entry).expect("direct append");
        let buffered_append = buffered.append(&entry).expect("buffered append");
        // The logical facts must agree even while the bytes are staged.
        assert_eq!(direct_append.start_offset(), buffered_append.start_offset());
        assert_eq!(
            direct_append.bytes_written(),
            buffered_append.bytes_written()
        );
        assert_eq!(direct.dirty_bytes(), buffered.dirty_bytes());
    }
    // Below the threshold nothing has reached the backend yet — that is the
    // point of coalescing.
    let object = ObjectLayout::wal_segment(1).expect("segment object");
    let direct_len = direct_backend.read_object(&object).expect("direct").len();
    let buffered_len = buffered_backend
        .read_object(&object)
        .expect("buffered")
        .len();
    assert!(buffered_len < direct_len, "appends were not coalesced");

    direct.force_durable().expect("direct barrier");
    buffered.force_durable().expect("buffered barrier");
    assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
}

#[test]
fn buffered_threshold_flush_matches_direct_bytes() {
    let direct_backend = StoredWalBackend::default();
    let buffered_backend = StoredWalBackend::default();
    let mut direct = open_service(&direct_backend, DurabilityPolicy::Standard, 0, SEGMENT_SIZE);
    // A tiny threshold forces mid-stream flushes, including a frame larger
    // than the buffer itself (staged then flushed immediately).
    let mut buffered = open_service(
        &buffered_backend,
        DurabilityPolicy::Standard,
        256,
        SEGMENT_SIZE,
    );

    for version in 1..=12u64 {
        let payload_len = usize::try_from(96 * version % 700 + 16).expect("payload length");
        let payload = vec![u8::try_from(version).expect("small version"); payload_len];
        let entry = record(version, payload);
        direct.append(&entry).expect("direct append");
        buffered.append(&entry).expect("buffered append");
    }
    direct.force_durable().expect("direct barrier");
    buffered.force_durable().expect("buffered barrier");
    assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
}

#[test]
fn buffered_always_policy_is_write_identical_per_append() {
    let direct_backend = StoredWalBackend::default();
    let buffered_backend = StoredWalBackend::default();
    let mut direct = open_service(&direct_backend, DurabilityPolicy::Always, 0, SEGMENT_SIZE);
    let mut buffered = open_service(
        &buffered_backend,
        DurabilityPolicy::Always,
        u64::from(u16::MAX),
        SEGMENT_SIZE,
    );

    for version in 1..=4u64 {
        let entry = record(version, format!("always payload {version}").into_bytes());
        let direct_append = direct.append(&entry).expect("direct append");
        let buffered_append = buffered.append(&entry).expect("buffered append");
        assert!(direct_append.forced_durable());
        assert!(buffered_append.forced_durable());
        // Always syncs inline — the buffer drains on every append, so the
        // backends must agree after each one.
        assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
    }
}

#[test]
fn group_sync_capture_flushes_buffered_appends() {
    let direct_backend = StoredWalBackend::default();
    let buffered_backend = StoredWalBackend::default();
    let mut direct = open_service(&direct_backend, DurabilityPolicy::Standard, 0, SEGMENT_SIZE);
    let mut buffered = open_service(
        &buffered_backend,
        DurabilityPolicy::Standard,
        u64::from(u16::MAX),
        SEGMENT_SIZE,
    );

    for version in 1..=5u64 {
        let entry = record(version, format!("group member {version}").into_bytes());
        direct.append(&entry).expect("direct append");
        buffered.append(&entry).expect("buffered append");
    }
    // Capture must make every staged byte backend-visible — the ticket's
    // off-lock fsync covers only what the backend has.
    let direct_ticket = direct.begin_group_sync().expect("direct capture");
    let buffered_ticket = buffered.begin_group_sync().expect("buffered capture");
    assert_eq!(direct_ticket.captured_seq(), buffered_ticket.captured_seq());
    let object = ObjectLayout::wal_segment(1).expect("segment object");
    assert_eq!(
        direct_backend.read_object(&object).expect("direct"),
        buffered_backend.read_object(&object).expect("buffered"),
        "capture left staged bytes behind"
    );
    direct_ticket.sync().expect("direct covering fsync");
    buffered_ticket.sync().expect("buffered covering fsync");
    direct.complete_group_sync(&direct_ticket);
    buffered.complete_group_sync(&buffered_ticket);
    assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
}

#[test]
fn randomized_sequences_converge_across_rotations_and_barriers() {
    for seed in [7u64, 1_337, 99_991] {
        let mut lcg = Lcg(seed);
        let direct_backend = StoredWalBackend::default();
        let buffered_backend = StoredWalBackend::default();
        let mut direct = open_service(&direct_backend, DurabilityPolicy::Standard, 0, SEGMENT_SIZE);
        let mut buffered = open_service(
            &buffered_backend,
            DurabilityPolicy::Standard,
            1024,
            SEGMENT_SIZE,
        );

        for version in 1..=200u64 {
            let payload_len = usize::try_from(lcg.next() % 1_800 + 16).expect("payload length");
            let fill = u8::try_from(version % 251).expect("byte fill");
            let entry = record(version, vec![fill; payload_len]);
            direct.append(&entry).expect("direct append");
            buffered.append(&entry).expect("buffered append");
            // Rotation decisions are made on logical sizes, so both services
            // must rotate on exactly the same append.
            assert_eq!(
                direct.active_segment_id(),
                buffered.active_segment_id(),
                "rotation diverged at version {version} (seed {seed})"
            );
            match lcg.next() % 10 {
                0 => {
                    direct.force_durable().expect("direct barrier");
                    buffered.force_durable().expect("buffered barrier");
                    assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
                }
                1 => {
                    let direct_ticket = direct.begin_group_sync().expect("direct capture");
                    let buffered_ticket = buffered.begin_group_sync().expect("buffered capture");
                    direct_ticket.sync().expect("direct fsync");
                    buffered_ticket.sync().expect("buffered fsync");
                    direct.complete_group_sync(&direct_ticket);
                    buffered.complete_group_sync(&buffered_ticket);
                    assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
                }
                _ => {}
            }
        }
        direct.close().expect("direct close");
        buffered.close().expect("buffered close");
        assert_converged(&direct_backend, &buffered_backend, &direct, &buffered);
    }
}

/// W3.3b: a sub-threshold buffer older than the flush window drains on the
/// NEXT append — steady slow traffic cannot hold staged bytes indefinitely.
#[test]
fn trickle_flushes_stale_subthreshold_buffer_on_next_append() {
    let backend = StoredWalBackend::default();
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(SEGMENT_SIZE)
            .with_append_buffer_bytes(u64::from(u16::MAX))
            .with_append_buffer_flush_window(std::time::Duration::from_millis(1)),
    )
    .expect("open WAL service");
    let object = ObjectLayout::wal_segment(1).expect("segment object");

    service
        .append(&record(1, b"staged and aging".to_vec()))
        .expect("first append");
    let staged_only = backend.read_object(&object).expect("bytes").len();
    std::thread::sleep(std::time::Duration::from_millis(5));
    service
        .append(&record(2, b"triggers the trickle".to_vec()))
        .expect("second append");

    let after_trickle = backend.read_object(&object).expect("bytes").len();
    assert!(
        after_trickle > staged_only,
        "stale buffer did not trickle on the next append"
    );
    let read = service.read_all().expect("read");
    assert_eq!(
        read.records().len(),
        2,
        "both records must be backend-visible"
    );
}

/// W3.3b: the background entry point drains only when the window has elapsed.
#[test]
fn flush_pending_if_stale_respects_the_window() {
    let backend = StoredWalBackend::default();
    let mut service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(SEGMENT_SIZE)
            .with_append_buffer_bytes(u64::from(u16::MAX))
            .with_append_buffer_flush_window(std::time::Duration::from_millis(2)),
    )
    .expect("open WAL service");

    service
        .append(&record(1, b"fresh bytes stay staged".to_vec()))
        .expect("append");
    assert!(
        !service.flush_pending_if_stale().expect("fresh check"),
        "a fresh buffer must not trickle"
    );
    std::thread::sleep(std::time::Duration::from_millis(6));
    assert!(
        service.flush_pending_if_stale().expect("stale check"),
        "an aged buffer must trickle"
    );
    assert!(
        !service.flush_pending_if_stale().expect("drained check"),
        "a drained buffer has nothing to trickle"
    );
    let read = service.read_all().expect("read");
    assert_eq!(read.records().len(), 1);
}

/// W3.3b degeneracy oracle: window ZERO makes every append trickle
/// immediately — the buffered service becomes byte-identical to the direct
/// service after EVERY append, no barrier needed.
#[test]
fn zero_window_degenerates_to_per_append_writes() {
    let direct_backend = StoredWalBackend::default();
    let buffered_backend = StoredWalBackend::default();
    let mut direct = open_service(&direct_backend, DurabilityPolicy::Standard, 0, SEGMENT_SIZE);
    let mut buffered = WalService::open(
        &buffered_backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::new(SEGMENT_SIZE)
            .with_append_buffer_bytes(u64::from(u16::MAX))
            .with_append_buffer_flush_window(std::time::Duration::ZERO),
    )
    .expect("open buffered service");

    let object = ObjectLayout::wal_segment(1).expect("segment object");
    for version in 1..=6u64 {
        let entry = record(version, format!("zero window {version}").into_bytes());
        direct.append(&entry).expect("direct append");
        buffered.append(&entry).expect("buffered append");
        assert_eq!(
            direct_backend.read_object(&object).expect("direct"),
            buffered_backend.read_object(&object).expect("buffered"),
            "zero-window buffering diverged at version {version}"
        );
    }
}

/// #3666: `segment_coverage` counts the active segment at the writer's
/// logical length, so its segments sum to `growth_facts().retained_bytes()`
/// even while the append buffer holds bytes the backend has not seen — each
/// segment exactly once, sealed ones at their on-disk size.
#[test]
fn segment_coverage_sums_to_retained_bytes_while_the_buffer_holds_bytes() {
    let backend = StoredWalBackend::default();
    let mut service = open_service(
        &backend,
        DurabilityPolicy::Standard,
        u64::from(u16::MAX),
        SEGMENT_SIZE,
    );
    for version in 1..=12u64 {
        service
            .append(&record(version, vec![0xa5; 3000]))
            .expect("append");
    }
    service
        .append(&record(13, b"staged in the buffer".to_vec()))
        .expect("staged append");
    assert!(service.active_segment_id() > 1, "the appends rotated");
    let growth = service.growth_facts().expect("growth facts");
    let active = ObjectLayout::wal_segment(service.active_segment_id()).expect("active object");
    let on_disk = backend.read_object(&active).expect("active bytes").len() as u64;
    assert!(
        on_disk < growth.active_segment_size(),
        "precondition: the buffer holds bytes ({on_disk} on disk, {} logical)",
        growth.active_segment_size()
    );

    let coverage = service.segment_coverage().expect("coverage");

    assert_eq!(coverage.len(), growth.retained_segments());
    assert_eq!(
        coverage.iter().map(WalSegmentCoverage::bytes).sum::<u64>(),
        growth.retained_bytes()
    );
    let unsealed: Vec<u64> = coverage
        .iter()
        .filter(|segment| segment.sealed_max_commit().is_none())
        .map(WalSegmentCoverage::bytes)
        .collect();
    assert_eq!(unsealed, vec![growth.active_segment_size()]);
    let sealed_on_disk: u64 = (1..service.active_segment_id())
        .map(|segment_id| {
            let object = ObjectLayout::wal_segment(segment_id).expect("sealed object");
            backend.read_object(&object).expect("sealed bytes").len() as u64
        })
        .sum();
    assert_eq!(
        coverage
            .iter()
            .filter(|segment| segment.sealed_max_commit().is_some())
            .map(WalSegmentCoverage::bytes)
            .sum::<u64>(),
        sealed_on_disk,
        "sealed segments are counted at their on-disk size"
    );
}
