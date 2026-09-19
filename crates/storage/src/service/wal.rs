//! WAL service mechanics over storage backend objects.

#![cfg_attr(
    any(not(test), all(test, not(feature = "localfs"))),
    expect(
        dead_code,
        reason = "WAL service is consumed by later commit and lifecycle layers; no-localfs test builds only exercise unsupported durable construction"
    )
)]

use crate::backend::{
    with_authorized_wal_repair_mutation, with_authorized_wal_retention_mutation, Backend,
    BackendAppend, BackendAppendHandle, BackendCapability, BackendError, BackendErrorKind,
    BackendHandle, BackendRange, DeleteDurability, DeleteError, DeleteOutcome, PublishError,
};
use crate::config::mode::DurabilityPolicy;
use crate::format::{
    decode_wal_record, decode_wal_record_envelope, decode_wal_segment_header, decode_wal_watermark,
    encode_wal_record_envelope_bytes_into, encode_wal_record_into_reusing,
    encode_wal_segment_header, encode_wal_watermark, FormatError, SegmentMetadata, WalRecord,
    WalSegmentHeader, WAL_SEGMENT_HEADER_SIZE,
};
use crate::layout::{LayoutError, ObjectLayout, WalObjectClassification};
use crate::object::ObjectName;
use crate::observability::perf_trace;
use crate::service::{
    durable_cleanup_failure, durable_cleanup_succeeded, validate_publish_outcome, ObjectPublisher,
};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use strata_core::CommitVersion;

const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const MIN_SEGMENT_SIZE: u64 = 1024;
const IDENTITY_CODEC_ID: &str = "identity";
const WAL_COMMIT_PAYLOAD_FIXED_BYTES: usize = 12;

pub(crate) type WalServiceResult<T> = Result<T, WalServiceError>;

thread_local! {
    static WAL_ENCODE_BUFFERS: RefCell<WalEncodeBuffers> =
        RefCell::new(WalEncodeBuffers::with_initial_capacity(4096));
}

#[derive(Debug)]
struct WalEncodeBuffers {
    frame: Vec<u8>,
    record: Vec<u8>,
    payload: Vec<u8>,
    row: Vec<u8>,
}

impl WalEncodeBuffers {
    fn with_initial_capacity(capacity: usize) -> Self {
        Self {
            frame: Vec::with_capacity(capacity),
            record: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity),
            row: Vec::with_capacity(capacity),
        }
    }

    fn capacities(&self) -> WalEncodeCapacities {
        WalEncodeCapacities {
            frame: self.frame.capacity(),
            record: self.record.capacity(),
            payload: self.payload.capacity(),
            row: self.row.capacity(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WalEncodeCapacities {
    frame: usize,
    record: usize,
    payload: usize,
    row: usize,
}

impl WalEncodeCapacities {
    fn growth_from(self, before: Self) -> WalEncodeBufferReuse {
        let before = [before.frame, before.record, before.payload, before.row];
        let after = [self.frame, self.record, self.payload, self.row];
        let mut allocations = 0usize;
        let mut reuses = 0usize;
        for (before_capacity, after_capacity) in before.into_iter().zip(after) {
            if after_capacity > before_capacity {
                allocations = allocations.saturating_add(1);
            } else {
                reuses = reuses.saturating_add(1);
            }
        }
        WalEncodeBufferReuse {
            allocations,
            reuses,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WalEncodeBufferReuse {
    allocations: usize,
    reuses: usize,
}

/// W3.3a: default user-space append-coalescing buffer. One backend write per
/// buffer-full instead of one per commit; 0 disables buffering (direct writes).
pub(crate) const DEFAULT_WAL_APPEND_BUFFER_BYTES: u64 = 128 * 1024;

/// W3.3b: how long a sub-threshold append buffer may hold staged bytes before
/// the next append or background drain round trickle-flushes it (write, no
/// fsync — bounds abrupt-kill exposure to roughly this window under any
/// activity; pure idleness is covered by flush-on-drop/close for orderly ends).
pub(crate) const DEFAULT_WAL_APPEND_BUFFER_FLUSH_WINDOW: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalServiceConfig {
    segment_size: u64,
    codec_id: &'static str,
    append_buffer_bytes: u64,
    append_buffer_flush_window: Duration,
}

impl WalServiceConfig {
    /// Bare construction is DIRECT (no coalescing) — service-level byte
    /// contracts are pinned against it. Production opens opt into the
    /// coalescing buffer via [`with_append_buffer_bytes`]
    /// (`DEFAULT_WAL_APPEND_BUFFER_BYTES`).
    pub(crate) const fn new(segment_size: u64) -> Self {
        Self {
            segment_size,
            codec_id: IDENTITY_CODEC_ID,
            append_buffer_bytes: 0,
            append_buffer_flush_window: DEFAULT_WAL_APPEND_BUFFER_FLUSH_WINDOW,
        }
    }

    pub(crate) const fn with_codec(segment_size: u64, codec_id: &'static str) -> Self {
        Self {
            segment_size,
            codec_id,
            append_buffer_bytes: 0,
            append_buffer_flush_window: DEFAULT_WAL_APPEND_BUFFER_FLUSH_WINDOW,
        }
    }

    /// W3.3a: override the append-coalescing buffer size; 0 = direct writes
    /// (the pre-coalescing behavior, kept for the differential oracle).
    pub(crate) const fn with_append_buffer_bytes(mut self, append_buffer_bytes: u64) -> Self {
        self.append_buffer_bytes = append_buffer_bytes;
        self
    }

    pub(crate) const fn append_buffer_bytes(self) -> u64 {
        self.append_buffer_bytes
    }

    /// W3.3b: override the trickle-flush staleness window (tests: `ZERO`
    /// degenerates to flush-per-append; large values disable trickling).
    pub(crate) const fn with_append_buffer_flush_window(mut self, window: Duration) -> Self {
        self.append_buffer_flush_window = window;
        self
    }

    pub(crate) const fn append_buffer_flush_window(self) -> Duration {
        self.append_buffer_flush_window
    }

    pub(crate) const fn segment_size(self) -> u64 {
        self.segment_size
    }

    pub(crate) const fn codec_id(self) -> &'static str {
        self.codec_id
    }

    pub(crate) fn validate(self) -> WalServiceResult<()> {
        if self.segment_size < MIN_SEGMENT_SIZE {
            Err(WalServiceError::InvalidConfig {
                field: "segment_size",
            })
        } else if self.codec_id != IDENTITY_CODEC_ID {
            Err(WalServiceError::InvalidConfig { field: "codec_id" })
        } else {
            Ok(())
        }
    }
}

impl Default for WalServiceConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            codec_id: IDENTITY_CODEC_ID,
            append_buffer_bytes: 0,
            append_buffer_flush_window: DEFAULT_WAL_APPEND_BUFFER_FLUSH_WINDOW,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalRetentionProof {
    covered_through: CommitVersion,
    source: WalRetentionProofSource,
}

impl WalRetentionProof {
    pub(crate) const fn snapshot_watermark(covered_through: CommitVersion) -> Self {
        Self {
            covered_through,
            source: WalRetentionProofSource::SnapshotWatermark,
        }
    }

    pub(crate) const fn flush_watermark(covered_through: CommitVersion) -> Self {
        Self {
            covered_through,
            source: WalRetentionProofSource::FlushWatermark,
        }
    }

    pub(crate) const fn covered_through(self) -> CommitVersion {
        self.covered_through
    }

    pub(crate) const fn source(self) -> WalRetentionProofSource {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalRetentionProofSource {
    SnapshotWatermark,
    FlushWatermark,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalOperation {
    Open,
    CreateSegment,
    Append,
    Sync,
    Repair,
    List,
    Read,
    /// W3.3a: draining the append-coalescing buffer to the backend.
    Flush,
}

impl WalOperation {
    const fn name(self) -> &'static str {
        match self {
            Self::Open => "open WAL",
            Self::CreateSegment => "create WAL segment",
            Self::Append => "append WAL record",
            Self::Sync => "sync WAL segment",
            Self::Repair => "repair WAL segment",
            Self::List => "list WAL segments",
            Self::Read => "read WAL segment",
            Self::Flush => "flush WAL append buffer",
        }
    }
}

impl fmt::Display for WalOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalServiceError {
    InvalidConfig {
        field: &'static str,
    },
    UnsupportedCapability {
        capability: BackendCapability,
    },
    InvalidSegmentId {
        segment_id: u64,
    },
    Layout {
        source: LayoutError,
    },
    Backend {
        operation: WalOperation,
        object: ObjectName,
        source: BackendError,
    },
    List {
        source: BackendError,
    },
    Publish {
        operation: WalOperation,
        source: PublishError,
    },
    Format {
        operation: WalOperation,
        object: ObjectName,
        source: FormatError,
    },
    DatabaseMismatch {
        object: ObjectName,
        segment_id: u64,
    },
    RecordTooLarge {
        bytes: u64,
        segment_size: u64,
    },
    SegmentIdOverflow {
        segment_id: u64,
    },
    TruncationSegmentMismatch {
        segment_id: u64,
        active_segment_id: u64,
    },
    InvalidTruncation {
        segment_id: u64,
        valid_end_offset: u64,
        object_size: u64,
    },
    RepairUncertain {
        segment_id: u64,
    },
    UnexpectedAppendOffset {
        object: ObjectName,
        expected: u64,
        actual: u64,
    },
    UnexpectedAppendLength {
        object: ObjectName,
        expected: u64,
        actual: u64,
    },
    UnexpectedObjectSize {
        object: ObjectName,
        expected: u64,
        actual: u64,
    },
    /// The on-disk WAL segments have a gap between the lowest and highest
    /// present id (retention only trims a contiguous prefix, so an interior
    /// hole means a segment — and its committed records — was removed).
    SegmentInventoryGap {
        missing_segment: u64,
    },
    /// The active WAL object vanished under a live writer (#2766: unlinked
    /// out of band). No future sync point can cover the log, so the writer
    /// is halted: reopen is the only remedy, and reopen classifies the loss.
    ActiveObjectLost {
        segment_id: u64,
    },
}

impl fmt::Display for WalServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field } => write!(formatter, "invalid WAL config field {field}"),
            Self::UnsupportedCapability { capability } => {
                write!(
                    formatter,
                    "backend does not support WAL capability {capability}"
                )
            }
            Self::InvalidSegmentId { segment_id } => {
                write!(formatter, "WAL segment id {segment_id} is invalid")
            }
            Self::Layout { source } => {
                write!(formatter, "failed to build WAL object name: {source}")
            }
            Self::Backend {
                operation,
                object,
                source,
            } => write!(formatter, "failed to {operation} at {object}: {source}"),
            Self::List { source } => write!(formatter, "failed to list WAL segments: {source}"),
            Self::Publish { operation, source } => {
                write!(formatter, "failed to {operation}: {source}")
            }
            Self::Format {
                operation,
                object,
                source,
            } => write!(
                formatter,
                "failed to {operation} bytes at {object}: {source}"
            ),
            Self::DatabaseMismatch { object, segment_id } => write!(
                formatter,
                "WAL segment {segment_id} at {object} belongs to a different database"
            ),
            Self::RecordTooLarge {
                bytes,
                segment_size,
            } => write!(
                formatter,
                "WAL record frame of {bytes} bytes exceeds segment size {segment_size}"
            ),
            Self::SegmentIdOverflow { segment_id } => {
                write!(formatter, "WAL segment id overflow after {segment_id}")
            }
            Self::TruncationSegmentMismatch {
                segment_id,
                active_segment_id,
            } => write!(
                formatter,
                "WAL truncation fact for segment {segment_id} does not match active segment {active_segment_id}"
            ),
            Self::InvalidTruncation {
                segment_id,
                valid_end_offset,
                object_size,
            } => write!(
                formatter,
                "invalid WAL truncation fact for segment {segment_id}: valid_end_offset {valid_end_offset} must be strictly less than object_size {object_size}"
            ),
            Self::RepairUncertain { segment_id } => write!(
                formatter,
                "WAL segment {segment_id} repair durability is uncertain; reopen before appending"
            ),
            Self::UnexpectedAppendOffset {
                object,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL append at {object} started at offset {actual}, expected {expected}"
            ),
            Self::UnexpectedAppendLength {
                object,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL append at {object} wrote {actual} bytes, expected {expected}"
            ),
            Self::UnexpectedObjectSize {
                object,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL object {object} has size {actual}, expected {expected}"
            ),
            Self::SegmentInventoryGap { missing_segment } => write!(
                formatter,
                "WAL segment {missing_segment} is missing from an otherwise contiguous log"
            ),
            Self::ActiveObjectLost { segment_id } => write!(
                formatter,
                "active WAL segment {segment_id} vanished under a live writer; the log cannot be synced again"
            ),
        }
    }
}

impl std::error::Error for WalServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Layout { source } => Some(source),
            Self::Backend { source, .. } | Self::List { source } => Some(source),
            Self::Publish { source, .. } => Some(source),
            Self::Format { source, .. } => Some(source),
            Self::InvalidConfig { .. }
            | Self::UnsupportedCapability { .. }
            | Self::InvalidSegmentId { .. }
            | Self::DatabaseMismatch { .. }
            | Self::RecordTooLarge { .. }
            | Self::SegmentIdOverflow { .. }
            | Self::TruncationSegmentMismatch { .. }
            | Self::InvalidTruncation { .. }
            | Self::RepairUncertain { .. }
            | Self::UnexpectedAppendOffset { .. }
            | Self::UnexpectedAppendLength { .. }
            | Self::UnexpectedObjectSize { .. }
            | Self::SegmentInventoryGap { .. }
            | Self::ActiveObjectLost { .. } => None,
        }
    }
}

impl WalServiceError {
    pub(crate) const fn is_writer_halted_append_failure(&self) -> bool {
        matches!(
            self,
            Self::RepairUncertain { .. } | Self::ActiveObjectLost { .. }
        )
    }

    /// #2766: a sync/flush that failed because the active object no longer
    /// exists — the ticket path constructs a plain [`Self::Backend`] error
    /// off-lock, so the redeeming caller uses this probe to latch the writer.
    pub(crate) fn is_active_object_missing(&self) -> bool {
        match self {
            Self::ActiveObjectLost { .. } => true,
            Self::Backend {
                operation: WalOperation::Append | WalOperation::Sync | WalOperation::Flush,
                source,
                ..
            } => source.kind() == BackendErrorKind::NotFound,
            _ => false,
        }
    }

    pub(crate) const fn is_durability_uncertain_append_failure(&self) -> bool {
        matches!(
            self,
            Self::Backend {
                operation: WalOperation::Sync | WalOperation::Flush,
                ..
            } | Self::UnexpectedAppendOffset { .. }
                | Self::UnexpectedAppendLength { .. }
                | Self::UnexpectedObjectSize { .. }
        )
    }

    /// Whether this failure means the durable WAL is malformed, incomplete, or
    /// belongs to a different database, rather than the backend being
    /// transiently unavailable. A decode failure (`Format` — checksum/magic/
    /// version/length mismatch), a segment from a different database
    /// (`DatabaseMismatch` — tamper/corruption), or a removed/gapped segment
    /// (`SegmentInventoryGap`) is permanent data loss:
    /// retrying the read cannot recover it. Backend IO, listing, and publish
    /// failures may be transient.
    pub(crate) const fn is_durable_corruption(&self) -> bool {
        matches!(
            self,
            Self::Format { .. }
                | Self::DatabaseMismatch { .. }
                | Self::SegmentInventoryGap { .. }
                | Self::ActiveObjectLost { .. }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalAppend {
    segment_id: u64,
    start_offset: u64,
    bytes_written: u64,
    dirty_bytes: u64,
    forced_durable: bool,
}

/// W3.3a: why the append-coalescing buffer is being drained (perf attribution).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalBufferFlushTrigger {
    /// The buffer crossed its configured size.
    Threshold,
    /// A group-sync ticket capture needs the bytes backend-visible.
    Capture,
    /// A durability barrier (`force_durable`: policy sync, rotation, close).
    Durability,
    /// W3.3b: the staleness window elapsed on a sub-threshold buffer.
    Trickle,
}

/// Who owns the fsync for an append (BS5.1): the service's configured policy (today's per-append
/// behavior), or a write-group caller that runs one covering `force_durable` after N appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalAppendDurability {
    PolicyDriven,
    DeferredToGroup,
}

/// One off-lock covering fsync (BS5.2): captured under the runtime lock after a group's
/// appends ([`WalService::begin_group_sync`]), synced WITHOUT the lock ([`sync`]), redeemed
/// back under it ([`WalService::complete_group_sync`]).
///
/// Sound because the backend fsyncs the FILE, not a writing descriptor: `LocalFs`
/// `sync_object` opens a fresh descriptor and `sync_all`s it, and the persistent append
/// handle is an unbuffered `File`, so a completed ticket covers every append that preceded
/// its capture — including appends made by OTHER groups before this ticket's capture, and
/// regardless of interleaved appends after it.
pub(crate) struct WalGroupSyncTicket<'a> {
    backend: BackendHandle<'a>,
    object: ObjectName,
    segment_id: u64,
    dirty_bytes: u64,
    dirty_records: u64,
    captured_seq: u64,
}

impl WalGroupSyncTicket<'_> {
    /// The covering fsync — call WITHOUT the runtime lock.
    pub(crate) fn sync(&self) -> WalServiceResult<()> {
        self.backend
            .sync_object(&self.object)
            .map_err(|source| WalServiceError::Backend {
                operation: WalOperation::Sync,
                object: self.object.clone(),
                source,
            })
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// The append sequence this ticket's completed sync proves durable.
    pub(crate) const fn captured_seq(&self) -> u64 {
        self.captured_seq
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalGrowthFacts {
    retained_segments: usize,
    retained_bytes: u64,
    active_segment_id: u64,
    active_segment_size: u64,
    dirty_bytes: u64,
    dirty_records: u64,
}

impl WalAppend {
    const fn new(
        segment_id: u64,
        start_offset: u64,
        bytes_written: u64,
        dirty_bytes: u64,
        forced_durable: bool,
    ) -> Self {
        Self {
            segment_id,
            start_offset,
            bytes_written,
            dirty_bytes,
            forced_durable,
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    pub(crate) const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) const fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    pub(crate) const fn forced_durable(&self) -> bool {
        self.forced_durable
    }
}

impl WalGrowthFacts {
    const fn new(
        retained_segments: usize,
        retained_bytes: u64,
        active_segment_id: u64,
        active_segment_size: u64,
        dirty_bytes: u64,
        dirty_records: u64,
    ) -> Self {
        Self {
            retained_segments,
            retained_bytes,
            active_segment_id,
            active_segment_size,
            dirty_bytes,
            dirty_records,
        }
    }

    pub(crate) const fn new_for_policy(
        retained_segments: usize,
        retained_bytes: u64,
        active_segment_id: u64,
        active_segment_size: u64,
        dirty_bytes: u64,
        dirty_records: u64,
    ) -> Self {
        Self::new(
            retained_segments,
            retained_bytes,
            active_segment_id,
            active_segment_size,
            dirty_bytes,
            dirty_records,
        )
    }

    pub(crate) const fn empty() -> Self {
        Self::new(0, 0, 0, 0, 0, 0)
    }

    pub(crate) const fn retained_segments(self) -> usize {
        self.retained_segments
    }

    pub(crate) const fn retained_bytes(self) -> u64 {
        self.retained_bytes
    }

    pub(crate) const fn active_segment_id(self) -> u64 {
        self.active_segment_id
    }

    pub(crate) const fn active_segment_size(self) -> u64 {
        self.active_segment_size
    }

    pub(crate) const fn dirty_bytes(self) -> u64 {
        self.dirty_bytes
    }

    pub(crate) const fn dirty_records(self) -> u64 {
        self.dirty_records
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalTailBoundary<const REPAIR: bool> {
    segment_id: u64,
    valid_end_offset: u64,
    extent_bytes: u64,
}

pub(crate) type WalTruncation = WalTailBoundary<false>;
pub(crate) type WalRepair = WalTailBoundary<true>;

impl<const REPAIR: bool> WalTailBoundary<REPAIR> {
    const fn from_extent(segment_id: u64, valid_end_offset: u64, extent_bytes: u64) -> Self {
        Self {
            segment_id,
            valid_end_offset,
            extent_bytes,
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn valid_end_offset(&self) -> u64 {
        self.valid_end_offset
    }
}

impl WalTailBoundary<false> {
    const fn new(segment_id: u64, valid_end_offset: u64, object_size: u64) -> Self {
        Self::from_extent(segment_id, valid_end_offset, object_size)
    }

    pub(crate) const fn object_size(&self) -> u64 {
        self.extent_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalRead {
    records: Vec<WalRecord>,
    truncation: Option<WalTruncation>,
}

impl WalRead {
    fn new(records: Vec<WalRecord>, truncation: Option<WalTruncation>) -> Self {
        Self {
            records,
            truncation,
        }
    }

    pub(crate) fn records(&self) -> &[WalRecord] {
        &self.records
    }

    pub(crate) const fn truncation(&self) -> Option<&WalTruncation> {
        self.truncation.as_ref()
    }
}

impl WalTailBoundary<true> {
    const fn new(segment_id: u64, valid_end_offset: u64, removed_bytes: u64) -> Self {
        Self::from_extent(segment_id, valid_end_offset, removed_bytes)
    }

    pub(crate) const fn removed_bytes(&self) -> u64 {
        self.extent_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalDeleteReport {
    deleted: Vec<u64>,
    delete_outcomes: Vec<WalSegmentDeleteOutcome>,
    protected: Vec<u64>,
    failed: Vec<u64>,
    delete_failures: Vec<WalSegmentDeleteFailure>,
    sidecar_deletes: Vec<WalSidecarDeleteOutcome>,
}

impl WalDeleteReport {
    fn new() -> Self {
        Self {
            deleted: Vec::new(),
            delete_outcomes: Vec::new(),
            protected: Vec::new(),
            failed: Vec::new(),
            delete_failures: Vec::new(),
            sidecar_deletes: Vec::new(),
        }
    }

    fn record_deleted(&mut self, segment_id: u64, outcome: DeleteOutcome) {
        self.deleted.push(segment_id);
        self.delete_outcomes
            .push(WalSegmentDeleteOutcome::new(segment_id, outcome));
    }

    fn record_failed(&mut self, segment_id: u64, failure: DeleteError) {
        self.failed.push(segment_id);
        self.delete_failures
            .push(WalSegmentDeleteFailure::new(segment_id, failure));
    }

    fn record_sidecar_delete(&mut self, outcome: WalSidecarDeleteOutcome) {
        self.sidecar_deletes.push(outcome);
    }

    pub(crate) fn deleted_segments(&self) -> &[u64] {
        &self.deleted
    }

    pub(crate) fn delete_outcomes(&self) -> &[WalSegmentDeleteOutcome] {
        &self.delete_outcomes
    }

    pub(crate) fn protected_segments(&self) -> &[u64] {
        &self.protected
    }

    pub(crate) fn failed_segments(&self) -> &[u64] {
        &self.failed
    }

    pub(crate) fn delete_failures(&self) -> &[WalSegmentDeleteFailure] {
        &self.delete_failures
    }

    pub(crate) fn sidecar_deletes(&self) -> &[WalSidecarDeleteOutcome] {
        &self.sidecar_deletes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalSegmentDeleteOutcome {
    segment_id: u64,
    outcome: DeleteOutcome,
}

impl WalSegmentDeleteOutcome {
    const fn new(segment_id: u64, outcome: DeleteOutcome) -> Self {
        Self {
            segment_id,
            outcome,
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        self.outcome.object()
    }

    pub(crate) const fn outcome(&self) -> &DeleteOutcome {
        &self.outcome
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalSegmentDeleteFailure {
    segment_id: u64,
    failure: DeleteError,
}

impl WalSegmentDeleteFailure {
    const fn new(segment_id: u64, failure: DeleteError) -> Self {
        Self {
            segment_id,
            failure,
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        self.failure.object()
    }

    pub(crate) const fn failure(&self) -> &DeleteError {
        &self.failure
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalSidecarDeleteOutcome {
    segment_id: u64,
    object: ObjectName,
    outcome: Option<DeleteOutcome>,
    failure: Option<DeleteError>,
}

impl WalSidecarDeleteOutcome {
    fn succeeded(segment_id: u64, outcome: DeleteOutcome) -> Self {
        Self {
            segment_id,
            object: outcome.object().clone(),
            outcome: Some(outcome),
            failure: None,
        }
    }

    fn failed(segment_id: u64, object: ObjectName, failure: DeleteError) -> Self {
        Self {
            segment_id,
            object,
            outcome: None,
            failure: Some(failure),
        }
    }

    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        &self.object
    }

    pub(crate) const fn outcome(&self) -> Option<&DeleteOutcome> {
        self.outcome.as_ref()
    }

    pub(crate) const fn failure(&self) -> Option<&DeleteError> {
        self.failure.as_ref()
    }
}

/// Incrementally-maintained retention totals for the *sealed* (non-active) WAL
/// segments. Combined with the live `active_segment_size`, this yields the total
/// retained WAL bytes/segments the growth policy needs without a per-commit
/// directory scan plus per-segment stat.
#[derive(Clone, Copy, Debug)]
struct SealedRetention {
    segments: usize,
    bytes: u64,
}

pub(crate) struct WalService<'a> {
    backend: BackendHandle<'a>,
    database_id: [u8; 16],
    active_segment_id: u64,
    active_object: ObjectName,
    active_segment_size: u64,
    segment_size: u64,
    codec_id: &'static str,
    durability_policy: DurabilityPolicy,
    dirty_bytes: u64,
    dirty_records: u64,
    active_metadata: SegmentMetadata,
    // #2690: highest commit version whose WAL record has been sealed durable,
    // as last read from / published to the durable watermark object. `None`
    // until acknowledged data reaches a seal point (rotation or close).
    durable_commit_watermark: Option<u64>,
    // #2766: latched when a sync point observed the active object missing.
    // A halted writer refuses every subsequent append until reopen.
    active_object_lost: bool,
    repair_uncertain: bool,
    // Cached retention totals for sealed (non-active) segments. `None` means the
    // totals must be refreshed from a backend scan on the next read; the scan is
    // memoized here so steady-state commits compute growth facts entirely from
    // memory (sealed totals + the live active segment size). Interior mutability
    // keeps `growth_facts` a `&self` read for the diagnostic/backpressure callers
    // while still memoizing the first scan.
    sealed_retention: Cell<Option<SealedRetention>>,
    // Persistent append descriptor for the active segment, lazily opened on the
    // first append after open/rotation/repair and held across subsequent appends
    // so steady-state commits do one `write` instead of stat+open+write+stat+close.
    // `None` means: not yet opened, or the backend does not support persistent
    // appends (then `append`/`force_durable` fall back to the per-call backend
    // path). Never carried into `clone_for_background_retention` — the clone only
    // deletes sealed segments and must not hold the writer's descriptor.
    active_append: Option<Box<dyn BackendAppendHandle>>,
    // BS5.2 group-flush bookkeeping: `append_seq` counts successful appends;
    // `durable_seq` mirrors the highest append sequence proven durable (a
    // completed sync covers every append that preceded its ticket's capture).
    // The mirror is an atomic so the pipelined commit path can check coverage
    // WITHOUT the runtime lock; it is only ever advanced under the lock.
    append_seq: u64,
    durable_seq: Arc<AtomicU64>,
    // W3.3a append coalescing: encoded frames accepted but not yet written to
    // the backend. `active_segment_size` is the LOGICAL size (physical bytes +
    // `pending.len()`); every backend-facing size check uses
    // `active_physical_size`. `pending` is never discarded on a flush failure —
    // the bytes belong to accepted commits; a stuck buffer surfaces as a
    // durability-uncertain flush error and the writer halt machinery takes over.
    pending: Vec<u8>,
    // Coalescing threshold from config; 0 = direct writes (pre-W3.3 behavior).
    append_buffer_bytes: u64,
    // W3.3b: when the buffer became non-empty (oldest staged byte), for the
    // trickle-flush staleness check. `None` while empty.
    pending_since: Option<Instant>,
    append_buffer_flush_window: Duration,
}

/// W3.3a: an orderly drop (runtime teardown without an explicit close) must
/// not lose staged bytes while the process is alive — pre-coalescing, every
/// append already sat in the OS page cache at drop. Best-effort: errors are
/// swallowed (nothing to propagate from drop); a true process kill still
/// loses the buffer, which is the accepted, bounded Standard exposure. No
/// fsync here — `close()` remains the durability barrier.
impl Drop for WalService<'_> {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            // Rationale: best-effort page-cache parity on abandon; a flush
            // failure here has no caller to inform and recovery treats the
            // missing tail as the ordinary unsynced-loss window.
            let _ = self.flush_pending(WalBufferFlushTrigger::Durability);
        }
    }
}

// `WalService` is moved across threads for background retention (the clone never
// carries `active_append`, but the type must stay `Send`). Guard it so a future
// non-`Send` append handle fails here, not as an opaque closure bound elsewhere.
const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<WalService<'static>>;
};

impl<'a> WalService<'a> {
    pub(crate) fn open(
        backend: impl Into<BackendHandle<'a>>,
        database_id: [u8; 16],
        active_segment_id: u64,
        durability_policy: DurabilityPolicy,
        config: WalServiceConfig,
    ) -> WalServiceResult<Self> {
        config.validate()?;
        validate_segment_id(active_segment_id)?;
        let backend = backend.into();
        require_capabilities(&backend, WAL_REQUIRED_CAPABILITIES)?;
        // The caller's seed is the manifest `active_wal_segment`, persisted only
        // when a checkpoint publishes — it lags behind every rotation since. The
        // directory is ground truth for which segments exist: resuming below its
        // max would append into a sealed segment and collide on the next roll
        // (#2555), and would hold the retention boundary below the true tail.
        let active_segment_id = resolve_resume_segment(&backend, active_segment_id)?;
        let (active_object, active_segment_size, active_metadata) =
            open_or_create_segment(&backend, database_id, active_segment_id, config.codec_id())?;
        let durable_commit_watermark = read_wal_watermark(backend.as_backend())?;

        Ok(Self {
            backend,
            database_id,
            active_segment_id,
            active_object,
            active_segment_size,
            segment_size: config.segment_size(),
            codec_id: config.codec_id(),
            durability_policy,
            dirty_bytes: 0,
            dirty_records: 0,
            active_metadata,
            durable_commit_watermark,
            active_object_lost: false,
            repair_uncertain: false,
            sealed_retention: Cell::new(None),
            active_append: None,
            append_seq: 0,
            durable_seq: Arc::new(AtomicU64::new(0)),
            pending: Vec::new(),
            append_buffer_bytes: config.append_buffer_bytes(),
            pending_since: None,
            append_buffer_flush_window: config.append_buffer_flush_window(),
        })
    }

    pub(crate) const fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    pub(crate) const fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    pub(crate) const fn dirty_records(&self) -> u64 {
        self.dirty_records
    }

    pub(crate) const fn active_metadata(&self) -> &SegmentMetadata {
        &self.active_metadata
    }

    pub(crate) const fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    pub(crate) fn clone_for_background_retention(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            database_id: self.database_id,
            active_segment_id: self.active_segment_id,
            active_object: self.active_object.clone(),
            active_segment_size: self.active_segment_size,
            segment_size: self.segment_size,
            codec_id: self.codec_id,
            durability_policy: self.durability_policy,
            dirty_bytes: 0,
            dirty_records: 0,
            active_metadata: self.active_metadata.clone(),
            durable_commit_watermark: self.durable_commit_watermark,
            active_object_lost: self.active_object_lost,
            repair_uncertain: self.repair_uncertain,
            // The background retention clone serves no growth-facts reads; it
            // refreshes lazily if ever asked. Its deletions invalidate the
            // primary's cache at the truncation publish step.
            sealed_retention: Cell::new(None),
            // Never duplicate the writer's append descriptor into the clone: it
            // only deletes sealed segments and must never append.
            active_append: None,
            // The retention clone never appends or syncs; give it inert
            // group-flush bookkeeping rather than sharing the writer's mirror.
            append_seq: 0,
            durable_seq: Arc::new(AtomicU64::new(0)),
            // The retention clone never appends; keep it bufferless so a
            // misuse would take the direct (validated) path, not stage bytes.
            pending: Vec::new(),
            append_buffer_bytes: 0,
            pending_since: None,
            append_buffer_flush_window: DEFAULT_WAL_APPEND_BUFFER_FLUSH_WINDOW,
        }
    }

    /// Backend-visible bytes of the active segment: the logical size minus
    /// what is still staged in the coalescing buffer.
    fn active_physical_size(&self) -> u64 {
        self.active_segment_size
            .saturating_sub(self.pending.len() as u64)
    }

    pub(crate) fn append(&mut self, record: &WalRecord) -> WalServiceResult<WalAppend> {
        self.append_with_durability(record, WalAppendDurability::PolicyDriven)
            .map_err(|error| self.observe_active_object_loss(error))
    }

    /// Append without the `Always` policy's inline fsync (BS5.1 write groups): the record
    /// accumulates dirty bytes exactly like `Standard`, and the CALLER owns durability — it must
    /// run one [`force_durable`](Self::force_durable) covering every deferred append before any
    /// covered commit is acked or made visible (WAL-before-visible). A group of one produces the
    /// identical syscall sequence to [`append`](Self::append) in `Always` mode: one append, one
    /// fsync — just sequenced by the caller.
    pub(crate) fn append_deferring_durability(
        &mut self,
        record: &WalRecord,
    ) -> WalServiceResult<WalAppend> {
        self.append_with_durability(record, WalAppendDurability::DeferredToGroup)
            .map_err(|error| self.observe_active_object_loss(error))
    }

    fn append_with_durability(
        &mut self,
        record: &WalRecord,
        durability: WalAppendDurability,
    ) -> WalServiceResult<WalAppend> {
        if self.active_object_lost {
            return Err(WalServiceError::ActiveObjectLost {
                segment_id: self.active_segment_id,
            });
        }
        if self.repair_uncertain {
            return Err(WalServiceError::RepairUncertain {
                segment_id: self.active_segment_id,
            });
        }

        WAL_ENCODE_BUFFERS.with(|buffer_cell| {
            let mut buffers = buffer_cell.borrow_mut();
            let encode =
                encode_record_frame(record, &self.active_object, self.codec_id, &mut buffers)?;
            perf_trace::record_commit_wal_encode_buffers(
                encode.record_bytes,
                encode.payload_bytes,
                encode.row_encode_bytes,
                encode.buffer_reuse.allocations,
                encode.buffer_reuse.reuses,
            );

            let frame_len = buffers.frame.len() as u64;
            // The segment header consumes part of the configured segment budget, so
            // a single record frame must fit in the remaining capacity before any
            // backend append is attempted.
            let max_record_bytes = self
                .segment_size
                .checked_sub(WAL_SEGMENT_HEADER_SIZE as u64)
                .ok_or(WalServiceError::InvalidConfig {
                    field: "segment_size",
                })?;
            if frame_len > max_record_bytes {
                return Err(WalServiceError::RecordTooLarge {
                    bytes: frame_len,
                    segment_size: self.segment_size,
                });
            }

            // Direct mode opens the persistent append descriptor up front (the
            // open-time stat is the boundary check the fast path then trusts).
            // Buffered mode defers the handle to flush time — appends touch
            // only the in-memory buffer.
            if self.append_buffer_bytes == 0 {
                self.ensure_active_append_handle()?;
                if self.active_append.is_none() {
                    self.validate_active_object_size(WalOperation::Append)?;
                }
            }

            // Rotation is decided against the LOGICAL size (authoritative under
            // the single-writer lock; buffered bytes count — they belong to the
            // active segment and flush before it seals).
            let projected_size = self.active_segment_size.checked_add(frame_len).ok_or(
                WalServiceError::RecordTooLarge {
                    bytes: frame_len,
                    segment_size: self.segment_size,
                },
            )?;
            if projected_size > self.segment_size {
                self.rotate_segment()?;
                if self.append_buffer_bytes == 0 {
                    self.ensure_active_append_handle()?;
                    if self.active_append.is_none() {
                        self.validate_active_object_size(WalOperation::Append)?;
                    }
                }
            }

            let expected_offset = self.active_segment_size;
            let expected_size =
                expected_offset
                    .checked_add(frame_len)
                    .ok_or(WalServiceError::RecordTooLarge {
                        bytes: frame_len,
                        segment_size: self.segment_size,
                    })?;
            if self.append_buffer_bytes > 0 {
                // W3.3a coalescing: stage the frame; the backend write (and its
                // offset/length cross-validation against real backend facts)
                // happens at flush. Threshold flushes run below, AFTER the
                // append bookkeeping, so a flush failure surfaces as a
                // durability-uncertain error with the record accepted — never
                // a half-staged record.
                if self.pending.is_empty() {
                    self.pending_since = Some(Instant::now());
                }
                self.pending.extend_from_slice(&buffers.frame);
                perf_trace::record_commit_wal_buffered_append();
            } else {
                self.append_frame_direct_validated(&buffers.frame, expected_offset)?;
            }

            self.active_segment_size = expected_size;
            self.dirty_bytes = self.dirty_bytes.saturating_add(frame_len);
            self.dirty_records = self.dirty_records.saturating_add(1);
            self.append_seq = self.append_seq.saturating_add(1);
            self.active_metadata
                .track_record(record.commit_version(), record.commit_timestamp());

            // W3.3a threshold flush: one coalesced backend write per buffer-full.
            // Runs after the append bookkeeping so a failure is a
            // durability-uncertain flush error on an ACCEPTED record (`pending`
            // is kept; the writer halt machinery owns what happens next).
            if self.append_buffer_bytes > 0 && self.pending.len() as u64 >= self.append_buffer_bytes
            {
                self.flush_pending(WalBufferFlushTrigger::Threshold)?;
            } else if self.pending_is_stale() {
                // W3.3b: steady sub-threshold traffic must not hold staged
                // bytes past the window — the oldest staged byte's age, not
                // the newest, drives the check.
                self.flush_pending(WalBufferFlushTrigger::Trickle)?;
            }

            // In always mode the append is already visible when sync runs. If sync
            // fails, dirty facts intentionally remain advanced so lifecycle can
            // classify the durability-uncertain window. A group append defers the
            // sync to the caller's single covering force_durable.
            let forced_durable = if durability == WalAppendDurability::PolicyDriven
                && self.durability_policy == DurabilityPolicy::Always
            {
                self.force_durable()?;
                true
            } else {
                false
            };

            // Offset/length facts come from the logical model; the direct
            // path validated them against the backend above, the buffered
            // path validates at flush.
            Ok(WalAppend::new(
                self.active_segment_id,
                expected_offset,
                frame_len,
                self.dirty_bytes,
                forced_durable,
            ))
        })
    }

    /// Direct-path append: one backend write per frame, cross-validated
    /// against the backend's returned facts (offset, length, resulting size).
    fn append_frame_direct_validated(
        &mut self,
        frame: &[u8],
        expected_offset: u64,
    ) -> WalServiceResult<()> {
        let frame_len = frame.len() as u64;
        let expected_size = expected_offset.saturating_add(frame_len);
        let append = self.append_frame(frame)?;
        if append.start_offset() != expected_offset {
            return Err(WalServiceError::UnexpectedAppendOffset {
                object: self.active_object.clone(),
                expected: expected_offset,
                actual: append.start_offset(),
            });
        }
        if append.bytes_written() != frame_len {
            return Err(WalServiceError::UnexpectedAppendLength {
                object: self.active_object.clone(),
                expected: frame_len,
                actual: append.bytes_written(),
            });
        }
        if append.metadata().size_bytes() != expected_size {
            return Err(WalServiceError::UnexpectedObjectSize {
                object: self.active_object.clone(),
                expected: expected_size,
                actual: append.metadata().size_bytes(),
            });
        }
        Ok(())
    }

    /// Lazily open a persistent append descriptor for the active segment when the
    /// backend supports one. No-op when already held or unsupported (then the
    /// per-call `append_object`/`sync_object` fallback is used). The open-time stat
    /// reconciles the in-memory size with the backend once per segment.
    fn ensure_active_append_handle(&mut self) -> WalServiceResult<()> {
        if self.active_append.is_some() {
            return Ok(());
        }
        let handle = self
            .backend
            .open_append_handle(&self.active_object, self.active_physical_size())
            .map_err(|source| WalServiceError::Backend {
                operation: WalOperation::Append,
                object: self.active_object.clone(),
                source,
            })?;
        if let Some(handle) = handle {
            // Reconcile the in-memory size with the backend once at handle open.
            // This is the boundary check the held descriptor then lets every later
            // append skip; it rejects an unrepaired partial tail (or any external
            // mutation) with the same `UnexpectedAppendOffset` the per-append stat
            // produced. On failure the freshly opened descriptor is dropped here.
            self.validate_active_object_size(WalOperation::Append)?;
            self.active_append = Some(handle);
        }
        Ok(())
    }

    /// Append a frame through the held descriptor when present, else the per-call
    /// backend path. The returned facts are validated by the caller either way.
    fn append_frame(&mut self, frame: &[u8]) -> WalServiceResult<BackendAppend> {
        let result = match self.active_append.as_mut() {
            Some(handle) => handle.append(frame),
            None => self.backend.append_object(&self.active_object, frame),
        };
        result.map_err(|source| WalServiceError::Backend {
            operation: WalOperation::Append,
            object: self.active_object.clone(),
            source,
        })
    }

    /// W3.3b: true when staged bytes have waited past the configured window.
    fn pending_is_stale(&self) -> bool {
        self.pending_since
            .is_some_and(|since| since.elapsed() >= self.append_buffer_flush_window)
    }

    /// W3.3b: background trickle entry — drain the buffer iff its oldest
    /// staged byte is older than the flush window. Returns whether a flush
    /// ran. Called from maintenance drains; a failure is returned for the
    /// caller to swallow (later triggers retry, and the commit path's
    /// durability barriers own the error surface).
    pub(crate) fn flush_pending_if_stale(&mut self) -> WalServiceResult<bool> {
        if self.pending.is_empty() || !self.pending_is_stale() {
            return Ok(false);
        }
        self.flush_pending(WalBufferFlushTrigger::Trickle)?;
        Ok(true)
    }

    /// W3.3a: drain the append-coalescing buffer with ONE backend write. The
    /// backend's returned facts replace the per-append cross-validation the
    /// direct path performs: offset must equal the physical size, length the
    /// buffer, and the resulting object size their sum. On a backend write
    /// failure the buffer is kept intact (the bytes belong to accepted
    /// commits) and the held descriptor is dropped so the retry re-validates
    /// the tail — a partial write then surfaces as `UnexpectedAppendOffset`.
    /// #2766: `flush_pending` with active-object-loss observation, for
    /// callers outside `force_durable`'s wrap (the group-sync capture).
    fn flush_pending_observing_loss(
        &mut self,
        trigger: WalBufferFlushTrigger,
    ) -> WalServiceResult<()> {
        self.flush_pending(trigger)
            .map_err(|error| self.observe_active_object_loss(error))
    }

    fn flush_pending(&mut self, trigger: WalBufferFlushTrigger) -> WalServiceResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.ensure_active_append_handle()?;
        if self.active_append.is_none() {
            self.validate_active_object_size(WalOperation::Flush)?;
        }
        let expected_offset = self.active_physical_size();
        let flush_len = self.pending.len() as u64;
        let pending = std::mem::take(&mut self.pending);
        let append = match self.append_frame(&pending) {
            Ok(append) => append,
            Err(WalServiceError::Backend { source, .. }) => {
                self.pending = pending;
                self.active_append = None;
                return Err(self.classify_sync_failure(WalOperation::Flush, source));
            }
            Err(error) => {
                self.pending = pending;
                self.active_append = None;
                return Err(error);
            }
        };
        // Reuse the buffer's capacity for the next batch of frames.
        self.pending = pending;
        self.pending.clear();
        self.pending_since = None;
        if append.start_offset() != expected_offset {
            return Err(WalServiceError::UnexpectedAppendOffset {
                object: self.active_object.clone(),
                expected: expected_offset,
                actual: append.start_offset(),
            });
        }
        if append.bytes_written() != flush_len {
            return Err(WalServiceError::UnexpectedAppendLength {
                object: self.active_object.clone(),
                expected: flush_len,
                actual: append.bytes_written(),
            });
        }
        let expected_size = expected_offset.saturating_add(flush_len);
        if append.metadata().size_bytes() != expected_size {
            return Err(WalServiceError::UnexpectedObjectSize {
                object: self.active_object.clone(),
                expected: expected_size,
                actual: append.metadata().size_bytes(),
            });
        }
        match trigger {
            WalBufferFlushTrigger::Threshold => {
                perf_trace::record_commit_wal_buffer_flush_threshold(flush_len);
            }
            WalBufferFlushTrigger::Capture => {
                perf_trace::record_commit_wal_buffer_flush_capture(flush_len);
            }
            WalBufferFlushTrigger::Durability => {
                perf_trace::record_commit_wal_buffer_flush_durability(flush_len);
            }
            WalBufferFlushTrigger::Trickle => {
                perf_trace::record_commit_wal_buffer_flush_trickle(flush_len);
            }
        }
        Ok(())
    }

    /// #2766: classifies a failed sync-point backend error. `NotFound` means
    /// the active object vanished under the live writer (we hold the writer
    /// lock, so nothing legitimate removes it): latch the writer halted and
    /// surface the permanent loss. Everything else stays a plain backend
    /// failure (possibly transient).
    fn classify_sync_failure(
        &mut self,
        operation: WalOperation,
        source: BackendError,
    ) -> WalServiceError {
        if source.kind() == BackendErrorKind::NotFound {
            self.active_object_lost = true;
            return WalServiceError::ActiveObjectLost {
                segment_id: self.active_segment_id,
            };
        }
        WalServiceError::Backend {
            operation,
            object: self.active_object.clone(),
            source,
        }
    }

    /// #2766: canonicalizes any active-object-missing failure observed at a
    /// write/sync boundary — latches the writer halted and rewrites the error
    /// to [`WalServiceError::ActiveObjectLost`] so every consumer classifies
    /// the loss as permanent. Non-loss errors pass through untouched.
    fn observe_active_object_loss(&mut self, error: WalServiceError) -> WalServiceError {
        if error.is_active_object_missing() {
            self.active_object_lost = true;
            return WalServiceError::ActiveObjectLost {
                segment_id: self.active_segment_id,
            };
        }
        error
    }

    /// #2766: latches the writer halted after an off-lock sync (the group
    /// ticket path) observed the active object missing. Called by the group
    /// finish path when it redeems such a failure under the runtime lock.
    pub(crate) fn record_active_object_lost(&mut self) {
        self.active_object_lost = true;
    }

    pub(crate) fn force_durable(&mut self) -> WalServiceResult<()> {
        // Buffered frames must reach the backend before the barrier can cover
        // them; a flush failure here is durability-uncertain, same class as a
        // failed fsync.
        self.flush_pending(WalBufferFlushTrigger::Durability)
            .map_err(|error| self.observe_active_object_loss(error))?;
        let result = match self.active_append.as_mut() {
            Some(handle) => handle.sync(),
            None => self.backend.sync_object(&self.active_object),
        };
        if let Err(source) = result {
            return Err(self.classify_sync_failure(WalOperation::Sync, source));
        }
        self.dirty_bytes = 0;
        self.dirty_records = 0;
        // Everything appended so far is now durable.
        self.durable_seq
            .fetch_max(self.append_seq, Ordering::AcqRel);
        Ok(())
    }

    /// Off-lock handle to the durable-append watermark (BS5.2): the pipelined
    /// commit path polls coverage through this atomic WITHOUT the runtime
    /// lock; it is only advanced under the lock (`force_durable`,
    /// `complete_group_sync`).
    pub(crate) fn durable_seq_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_seq)
    }

    /// Capture a group-sync ticket (BS5.2): called under the runtime lock after a
    /// group's appends. The ticket's [`WalGroupSyncTicket::sync`] then runs the
    /// covering fsync WITHOUT the lock, and [`complete_group_sync`] redeems the
    /// outcome back under it. Pure capture — no service mutation. The captured
    /// sequence makes the completed sync cover every append before the capture,
    /// so later captures cover earlier groups' appends too.
    pub(crate) fn begin_group_sync(&mut self) -> WalServiceResult<WalGroupSyncTicket<'a>> {
        // W3.3a: the ticket's off-lock fsync syncs by object name — it can
        // only cover bytes the backend has. Flushing here is the group-flush
        // coalescing: an Always group's N member appends become one write. A
        // flush failure means the capture's appends cannot be covered; the
        // caller maps it to its sync-failure handling.
        self.flush_pending_observing_loss(WalBufferFlushTrigger::Capture)?;
        Ok(WalGroupSyncTicket {
            backend: self.backend.clone(),
            object: self.active_object.clone(),
            segment_id: self.active_segment_id,
            dirty_bytes: self.dirty_bytes,
            dirty_records: self.dirty_records,
            captured_seq: self.append_seq,
        })
    }

    /// Redeem a successfully synced group ticket (BS5.2): advance the durable
    /// watermark to the ticket's capture point and retire exactly the dirty
    /// amounts the ticket covered — appends made while the fsync was in
    /// flight stay dirty. Dirty retirement is skipped after a rotation
    /// (rotation force-syncs the old segment and resets the counters itself);
    /// the watermark still advances (the rotation sync covered the capture).
    /// The caller maps a failed sync to its own durability handling; failure
    /// leaves the dirty facts advanced, same as a failed `force_durable`.
    pub(crate) fn complete_group_sync(&mut self, ticket: &WalGroupSyncTicket<'_>) {
        if self.active_segment_id == ticket.segment_id {
            self.dirty_bytes = self.dirty_bytes.saturating_sub(ticket.dirty_bytes);
            self.dirty_records = self.dirty_records.saturating_sub(ticket.dirty_records);
        }
        self.durable_seq
            .fetch_max(ticket.captured_seq, Ordering::AcqRel);
    }

    pub(crate) fn close(&mut self) -> WalServiceResult<()> {
        // Always issue at least one SyncObject through the backend on close,
        // regardless of `dirty_bytes`. Two reasons:
        //   1. The byte-level state may already be durable because an
        //      earlier `force_durable` succeeded, but a *partial-close
        //      retry* (e.g. the first close cleared `dirty_bytes` then
        //      failed downstream before completing the lifecycle close)
        //      must still confirm a fresh sync on the retry attempt.
        //      Otherwise the second close reports Complete with
        //      `durable_synced=true` without any operation log evidence.
        //   2. `sync_object` on an already-clean active segment is an
        //      observable fsync no-op at the backend; the cost is one
        //      syscall on close, paid once.
        self.force_durable()?;
        // Release the persistent append descriptor after the close sync. Any
        // later append (e.g. a close retry path) re-opens it lazily.
        self.active_append = None;
        // #2690: the close sync made every appended record durable; attest the
        // active segment's highest commit version so a post-close segment
        // removal is detectable at the next open.
        self.publish_commit_watermark()?;
        Ok(())
    }

    pub(crate) fn read_all(&self) -> WalServiceResult<WalRead> {
        let segments = list_segments(&self.backend)?;
        let latest_segment_id = segments.last().map(|(segment_id, _)| *segment_id);
        let mut records = Vec::new();
        let mut truncation = None;

        for (segment_id, object) in segments {
            let is_latest = latest_segment_id == Some(segment_id);
            let read = read_segment(
                &self.backend,
                self.database_id,
                segment_id,
                &object,
                is_latest,
                self.codec_id,
            )?;
            records.extend(read.records);
            if read.truncation.is_some() {
                truncation = read.truncation;
                break;
            }
        }

        Ok(WalRead::new(records, truncation))
    }

    pub(crate) fn read_after_commit_version(
        &self,
        watermark: CommitVersion,
    ) -> WalServiceResult<WalRead> {
        let read = self.read_all()?;
        let records = read
            .records
            .into_iter()
            .filter(|record| record.commit_version() > watermark)
            .collect();
        Ok(WalRead::new(records, read.truncation))
    }

    /// #2567 S3a (#3319): streaming read of every record above `watermark`,
    /// in segment-id then append order, WITHOUT materializing the tail:
    /// segments are read in fixed-size ranged chunks and each record is
    /// decoded, visited, and dropped. The buffer grows past one chunk only
    /// when a single envelope straddles chunks — bounded by the largest
    /// record, never the tail. Truncation semantics are identical to
    /// `read_all`: a short final envelope on the LATEST segment stops the
    /// visit and reports the repairable tail; the same shape anywhere else
    /// is hard corruption. Recovery is the intended caller — its peak was
    /// O(unreclaimed WAL) through `read_after_commit_version` and the kernel
    /// OOM killer was the backstop (#3319's 18 GiB field case).
    /// The visitor returns `ControlFlow::Break` to stop early (the caller
    /// owns whatever failure it captured); truncation detection past the
    /// break point is skipped, which only an aborting caller can reach.
    pub(crate) fn visit_records_after(
        &self,
        watermark: CommitVersion,
        visit: &mut dyn FnMut(&WalRecord) -> std::ops::ControlFlow<()>,
    ) -> WalServiceResult<Option<WalTruncation>> {
        let segments = list_segments(&self.backend)?;
        let latest_segment_id = segments.last().map(|(segment_id, _)| *segment_id);
        let mut filtered = |record: &WalRecord| {
            if record.commit_version() > watermark {
                visit(record)
            } else {
                std::ops::ControlFlow::Continue(())
            }
        };
        for (segment_id, object) in segments {
            let is_latest = latest_segment_id == Some(segment_id);
            let truncation = visit_segment_records(
                &self.backend,
                self.database_id,
                segment_id,
                &object,
                is_latest,
                self.codec_id,
                &mut filtered,
            )?;
            if truncation.is_some() {
                return Ok(truncation);
            }
        }
        Ok(None)
    }

    pub(crate) fn growth_facts(&self) -> WalServiceResult<WalGrowthFacts> {
        let sealed = self.sealed_retention_facts()?;
        let retained_segments = sealed.segments.saturating_add(1);
        let retained_bytes = sealed.bytes.checked_add(self.active_segment_size).ok_or(
            WalServiceError::UnexpectedObjectSize {
                object: self.active_object.clone(),
                expected: sealed.bytes,
                actual: self.active_segment_size,
            },
        )?;
        Ok(WalGrowthFacts::new(
            retained_segments,
            retained_bytes,
            self.active_segment_id,
            self.active_segment_size,
            self.dirty_bytes,
            self.dirty_records,
        ))
    }

    /// Returns the cached sealed-segment retention totals, refreshing them from a
    /// backend scan when the cache has been invalidated (open, repair, or a
    /// retention deletion). The active segment is excluded here — its live size
    /// is added by `growth_facts` — so steady-state appends never invalidate it.
    fn sealed_retention_facts(&self) -> WalServiceResult<SealedRetention> {
        if let Some(sealed) = self.sealed_retention.get() {
            return Ok(sealed);
        }
        let sealed = self.scan_sealed_retention()?;
        self.sealed_retention.set(Some(sealed));
        Ok(sealed)
    }

    fn scan_sealed_retention(&self) -> WalServiceResult<SealedRetention> {
        let segments = list_segments(&self.backend)?;
        let mut sealed = SealedRetention {
            segments: 0,
            bytes: 0,
        };
        for (segment_id, object) in &segments {
            if *segment_id == self.active_segment_id {
                continue;
            }
            let metadata = match self.backend.object_metadata(object) {
                Ok(metadata) => metadata,
                // Background truncation deletes covered segments on an
                // off-lock retention clone; a segment listed at the start of
                // this scan can legitimately be gone by the stat. Its bytes
                // are already reclaimed — skipping it makes the sample MORE
                // accurate, and the truncation publish step invalidates the
                // cache so the next sample sees the settled state. Recovery's
                // strict inventory checks are a separate path and keep their
                // contiguity semantics.
                Err(source) if source.kind() == BackendErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(WalServiceError::Backend {
                        operation: WalOperation::List,
                        object: object.clone(),
                        source,
                    });
                }
            };
            sealed.segments = sealed.segments.saturating_add(1);
            sealed.bytes = sealed.bytes.checked_add(metadata.size_bytes()).ok_or(
                WalServiceError::UnexpectedObjectSize {
                    object: object.clone(),
                    expected: sealed.bytes,
                    actual: metadata.size_bytes(),
                },
            )?;
        }
        Ok(sealed)
    }

    /// Invalidates the cached sealed-segment retention totals so the next
    /// `growth_facts` re-derives them from the backend. Called after retention
    /// deletes sealed segments (directly in `delete_covered_segments`, and on the
    /// primary service at the background truncation publish step).
    pub(crate) fn invalidate_sealed_retention(&self) {
        self.sealed_retention.set(None);
    }

    pub(crate) fn repair_latest_tail(
        &mut self,
        truncation: &WalTruncation,
    ) -> WalServiceResult<WalRepair> {
        // Repair rewrites the active object via publish_durable_replace (a
        // temp-file + rename on localfs), so any held append descriptor now points
        // at the old, unlinked inode. Drop it; the next append re-opens lazily.
        self.active_append = None;
        if truncation.segment_id() != self.active_segment_id {
            return Err(WalServiceError::TruncationSegmentMismatch {
                segment_id: truncation.segment_id(),
                active_segment_id: self.active_segment_id,
            });
        }
        if truncation.valid_end_offset() >= truncation.object_size() {
            return Err(WalServiceError::InvalidTruncation {
                segment_id: truncation.segment_id(),
                valid_end_offset: truncation.valid_end_offset(),
                object_size: truncation.object_size(),
            });
        }

        self.validate_active_object_size_against(WalOperation::Repair, truncation.object_size())?;
        let prefix = self
            .backend
            .read_range(
                &self.active_object,
                BackendRange::new(0, truncation.valid_end_offset()),
            )
            .map_err(|source| WalServiceError::Backend {
                operation: WalOperation::Repair,
                object: self.active_object.clone(),
                source,
            })?;
        let prefix_len = prefix.len() as u64;
        if prefix_len != truncation.valid_end_offset() {
            return Err(WalServiceError::UnexpectedObjectSize {
                object: self.active_object.clone(),
                expected: truncation.valid_end_offset(),
                actual: prefix_len,
            });
        }

        let repaired_read = decode_segment_bytes(
            self.database_id,
            self.active_segment_id,
            &self.active_object,
            &prefix,
            false,
            self.codec_id,
            WalOperation::Repair,
        )?;
        let outcome = match with_authorized_wal_repair_mutation(|| {
            ObjectPublisher::new(&self.backend)
                .publish_durable_replace(&self.active_object, &prefix)
        }) {
            Ok(outcome) => outcome,
            Err(source) => {
                self.repair_uncertain = true;
                return Err(WalServiceError::Publish {
                    operation: WalOperation::Repair,
                    source,
                });
            }
        };
        if let Err(error) = validate_wal_publish_outcome(
            WalOperation::Repair,
            &self.active_object,
            truncation.valid_end_offset(),
            &outcome,
        ) {
            self.repair_uncertain = true;
            return Err(error);
        }

        self.active_segment_size = truncation.valid_end_offset();
        self.active_metadata =
            segment_metadata_from_records(self.active_segment_id, repaired_read.records());
        self.dirty_bytes = 0;
        self.dirty_records = 0;
        self.repair_uncertain = false;
        Ok(WalRepair::new(
            truncation.segment_id(),
            truncation.valid_end_offset(),
            truncation.object_size() - truncation.valid_end_offset(),
        ))
    }

    pub(crate) fn delete_covered_segments(
        &self,
        retention_proof: WalRetentionProof,
    ) -> WalServiceResult<WalDeleteReport> {
        require_capability(&self.backend, BackendCapability::DeleteObject)?;
        let mut report = WalDeleteReport::new();
        let covered_through = retention_proof.covered_through();

        for (segment_id, object) in list_segments(&self.backend)? {
            if segment_id >= self.active_segment_id {
                report.protected.push(segment_id);
                continue;
            }

            let read = read_segment(
                &self.backend,
                self.database_id,
                segment_id,
                &object,
                false,
                self.codec_id,
            )?;
            if read
                .records
                .iter()
                .all(|record| record.commit_version() <= covered_through)
            {
                match with_authorized_wal_retention_mutation(|| self.backend.delete_object(&object))
                {
                    Ok(outcome) if durable_cleanup_succeeded(&outcome) => {
                        report.record_deleted(segment_id, outcome);
                        if let Some(sidecar) = self.delete_segment_sidecar_best_effort(segment_id) {
                            report.record_sidecar_delete(sidecar);
                        }
                    }
                    Ok(outcome) => {
                        report.record_failed(segment_id, durable_cleanup_failure(&outcome));
                    }
                    Err(error) if error.source_error().kind() == BackendErrorKind::NotFound => {
                        report.record_deleted(
                            segment_id,
                            DeleteOutcome::already_missing(
                                object.clone(),
                                DeleteDurability::NonDurable,
                            ),
                        );
                        if let Some(sidecar) = self.delete_segment_sidecar_best_effort(segment_id) {
                            report.record_sidecar_delete(sidecar);
                        }
                    }
                    Err(error) => report.record_failed(segment_id, error),
                }
            } else {
                report.protected.push(segment_id);
            }
        }

        // Deleting sealed segments invalidates this service's cached retention
        // totals. The primary service's cache is refreshed separately when a
        // background retention clone performs the deletion.
        if !report.deleted_segments().is_empty() {
            self.invalidate_sealed_retention();
        }

        Ok(report)
    }

    fn delete_segment_sidecar_best_effort(
        &self,
        segment_id: u64,
    ) -> Option<WalSidecarDeleteOutcome> {
        // Segment metadata sidecars are optional accelerators. Once a WAL
        // segment is pruned, its sidecar is unreachable recovery state and can
        // be removed, but sidecar deletion must not turn authoritative WAL
        // retention into a failure.
        let Ok(sidecar) = ObjectLayout::wal_segment_metadata(segment_id) else {
            return None;
        };
        Some(
            match with_authorized_wal_retention_mutation(|| self.backend.delete_object(&sidecar)) {
                Ok(outcome) => WalSidecarDeleteOutcome::succeeded(segment_id, outcome),
                Err(error) if error.source_error().kind() == BackendErrorKind::NotFound => {
                    WalSidecarDeleteOutcome::succeeded(
                        segment_id,
                        DeleteOutcome::already_missing(sidecar, DeleteDurability::NonDurable),
                    )
                }
                Err(error) => WalSidecarDeleteOutcome::failed(segment_id, sidecar, error),
            },
        )
    }

    fn rotate_segment(&mut self) -> WalServiceResult<()> {
        // Old segment bytes must be durable before the active pointer advances
        // to a freshly created segment. Recovery can then replay all complete
        // segments up to the active segment without losing the rotation record.
        if self.dirty_bytes > 0 || !self.pending.is_empty() {
            self.force_durable()?;
        }
        // The old segment is now sealed and durable; release its append descriptor
        // before advancing. The next append to the new segment re-opens lazily.
        self.active_append = None;
        // #2690: the sealed segment's records are durable, so its highest
        // commit version is now a safe lower bound on what recovery must be
        // able to reproduce. Publish it before the active pointer advances.
        self.publish_commit_watermark()?;

        let next_segment_id =
            self.active_segment_id
                .checked_add(1)
                .ok_or(WalServiceError::SegmentIdOverflow {
                    segment_id: self.active_segment_id,
                })?;
        let (next_object, next_size, next_metadata) =
            create_segment(&self.backend, self.database_id, next_segment_id)?;

        // The segment being rotated away is now sealed at its final size; fold it
        // into the cached sealed totals so growth facts stay correct without a
        // rescan. A cold cache will count it on its next scan, so only update a
        // populated cache. `active_segment_size` is still the old segment's size
        // until the assignment below.
        if let Some(mut sealed) = self.sealed_retention.get() {
            sealed.segments = sealed.segments.saturating_add(1);
            sealed.bytes = sealed.bytes.saturating_add(self.active_segment_size);
            self.sealed_retention.set(Some(sealed));
        }

        self.active_segment_id = next_segment_id;
        self.active_object = next_object;
        self.active_segment_size = next_size;
        self.active_metadata = next_metadata;
        Ok(())
    }

    /// #2690: publishes the durable commit watermark when the active segment's
    /// sealed-durable records advance it. Called strictly AFTER the sync that
    /// made those records durable (rotation seal, close sync), so the marker
    /// is always a safe lower bound: a crash that loses the marker update only
    /// understates, which can never refuse a recoverable database. Monotonic —
    /// a marker never moves backward.
    fn publish_commit_watermark(&mut self) -> WalServiceResult<()> {
        if self.active_metadata.record_count() == 0 {
            return Ok(());
        }
        let sealed_max = self.active_metadata.max_commit_version().as_u64();
        if sealed_max == 0
            || self
                .durable_commit_watermark
                .is_some_and(|current| current >= sealed_max)
        {
            return Ok(());
        }
        update_wal_watermark(self.backend.as_backend(), sealed_max)?;
        self.durable_commit_watermark = Some(sealed_max);
        Ok(())
    }

    /// #2690: the durable commit watermark as read at open — the highest commit
    /// version whose WAL record was attested sealed-durable. Recovery compares
    /// it against every recoverable source and refuses when the log cannot
    /// reproduce it.
    pub(crate) const fn durable_commit_watermark(&self) -> Option<u64> {
        self.durable_commit_watermark
    }

    fn validate_active_object_size(&self, operation: WalOperation) -> WalServiceResult<()> {
        // Backend stats see only flushed bytes — compare physical, not logical.
        self.validate_active_object_size_against(operation, self.active_physical_size())
    }

    fn validate_active_object_size_against(
        &self,
        operation: WalOperation,
        expected_size: u64,
    ) -> WalServiceResult<()> {
        let actual_size = self
            .backend
            .object_metadata(&self.active_object)
            .map_err(|source| WalServiceError::Backend {
                operation,
                object: self.active_object.clone(),
                source,
            })?
            .size_bytes();
        if actual_size == expected_size {
            Ok(())
        } else {
            Err(WalServiceError::UnexpectedAppendOffset {
                object: self.active_object.clone(),
                expected: expected_size,
                actual: actual_size,
            })
        }
    }
}

const WAL_REQUIRED_CAPABILITIES: &[BackendCapability] = &[
    BackendCapability::ReadObject,
    BackendCapability::ReadRange,
    BackendCapability::ListPrefix,
    BackendCapability::ObjectMetadata,
    BackendCapability::AppendObject,
    BackendCapability::DurablePublish,
    BackendCapability::DurableSync,
];

fn validate_segment_id(segment_id: u64) -> WalServiceResult<()> {
    if segment_id == 0 {
        Err(WalServiceError::InvalidSegmentId { segment_id })
    } else {
        Ok(())
    }
}

fn require_capabilities(
    backend: &dyn Backend,
    capabilities: &[BackendCapability],
) -> WalServiceResult<()> {
    for capability in capabilities {
        require_capability(backend, *capability)?;
    }
    Ok(())
}

fn require_capability(
    backend: &dyn Backend,
    capability: BackendCapability,
) -> WalServiceResult<()> {
    if backend.capabilities().contains(capability) {
        Ok(())
    } else {
        Err(WalServiceError::UnsupportedCapability { capability })
    }
}

fn segment_object(segment_id: u64) -> WalServiceResult<ObjectName> {
    ObjectLayout::wal_segment(segment_id).map_err(|source| WalServiceError::Layout { source })
}

fn open_or_create_segment(
    backend: &dyn Backend,
    database_id: [u8; 16],
    segment_id: u64,
    codec_id: &str,
) -> WalServiceResult<(ObjectName, u64, SegmentMetadata)> {
    let object = segment_object(segment_id)?;
    match backend.object_metadata(&object) {
        Ok(metadata) => {
            // #2567 S3a (#3319): the resume scan needs only the valid end
            // offset and the per-record metadata facts — stream them instead
            // of materializing the active segment's bytes and every decoded
            // record (on a store whose tail is one segment, that read was a
            // second whole-tail materialization on the open path).
            let mut active_metadata = SegmentMetadata::empty(segment_id);
            let truncation = visit_segment_records(
                backend,
                database_id,
                segment_id,
                &object,
                true,
                codec_id,
                &mut |record| {
                    active_metadata
                        .track_record(record.commit_version(), record.commit_timestamp());
                    std::ops::ControlFlow::Continue(())
                },
            )?;
            let segment_size = truncation
                .as_ref()
                .map_or(metadata.size_bytes(), WalTruncation::valid_end_offset);
            Ok((object, segment_size, active_metadata))
        }
        Err(source) if source.kind() == BackendErrorKind::NotFound => {
            create_segment(backend, database_id, segment_id)
        }
        Err(source) => Err(WalServiceError::Backend {
            operation: WalOperation::Open,
            object,
            source,
        }),
    }
}

fn create_segment(
    backend: &dyn Backend,
    database_id: [u8; 16],
    segment_id: u64,
) -> WalServiceResult<(ObjectName, u64, SegmentMetadata)> {
    let object = segment_object(segment_id)?;
    let header = WalSegmentHeader::new(segment_id, database_id);
    let bytes = encode_wal_segment_header(&header);
    let outcome = ObjectPublisher::new(backend)
        .publish_durable_create(&object, &bytes)
        .map_err(|source| WalServiceError::Publish {
            operation: WalOperation::CreateSegment,
            source,
        })?;
    validate_wal_publish_outcome(
        WalOperation::CreateSegment,
        &object,
        bytes.len() as u64,
        &outcome,
    )?;
    Ok((
        object,
        outcome.metadata().size_bytes(),
        SegmentMetadata::empty(segment_id),
    ))
}

fn validate_wal_publish_outcome(
    operation: WalOperation,
    object: &ObjectName,
    byte_count: u64,
    outcome: &crate::backend::PublishOutcome,
) -> WalServiceResult<()> {
    validate_publish_outcome(object, byte_count, outcome).map_err(|mismatch| {
        WalServiceError::Backend {
            operation,
            object: mismatch.object().clone(),
            source: BackendError::new(
                BackendErrorKind::MetadataMismatch,
                format!("WAL publish returned invalid {} metadata", mismatch.field()),
            ),
        }
    })
}

struct WalEncodeFacts {
    record_bytes: usize,
    payload_bytes: usize,
    row_encode_bytes: usize,
    buffer_reuse: WalEncodeBufferReuse,
}

fn encode_record_frame(
    record: &WalRecord,
    object: &ObjectName,
    codec_id: &str,
    buffers: &mut WalEncodeBuffers,
) -> WalServiceResult<WalEncodeFacts> {
    let before_capacities = buffers.capacities();
    encode_wal_record_into_reusing(
        record,
        &mut buffers.record,
        &mut buffers.payload,
        &mut buffers.row,
    )
    .map_err(|source| WalServiceError::Format {
        operation: WalOperation::Append,
        object: object.clone(),
        source,
    })?;
    validate_wal_codec_id(codec_id)?;
    encode_wal_record_envelope_bytes_into(&buffers.record, &mut buffers.frame).map_err(
        |source| WalServiceError::Format {
            operation: WalOperation::Append,
            object: object.clone(),
            source,
        },
    )?;
    let row_count = record.commit_payload().rows().len();
    let row_encode_bytes = buffers
        .payload
        .len()
        .saturating_sub(WAL_COMMIT_PAYLOAD_FIXED_BYTES)
        .saturating_sub(row_count.saturating_mul(4));
    Ok(WalEncodeFacts {
        record_bytes: buffers.record.len(),
        payload_bytes: buffers.payload.len(),
        row_encode_bytes,
        buffer_reuse: buffers.capacities().growth_from(before_capacities),
    })
}

fn validate_wal_codec_id(codec_id: &str) -> WalServiceResult<()> {
    match codec_id {
        IDENTITY_CODEC_ID => Ok(()),
        _ => Err(WalServiceError::InvalidConfig { field: "codec_id" }),
    }
}

fn decode_wal_codec_bytes<'a>(codec_id: &str, bytes: &'a [u8]) -> WalServiceResult<Cow<'a, [u8]>> {
    match codec_id {
        IDENTITY_CODEC_ID => Ok(Cow::Borrowed(bytes)),
        _ => Err(WalServiceError::InvalidConfig { field: "codec_id" }),
    }
}

type WalSegmentObject = (u64, ObjectName);

/// Resolves the segment the writer resumes in: the requested (manifest) seed
/// or the highest segment on disk, whichever is greater. An empty directory
/// (fresh store) resumes at the seed; a seed above the directory max (external
/// restore/tamper) is honored and created, matching fresh-store semantics.
/// Reports whether any WAL segment object exists on disk. Recovery uses this
/// to distinguish a fresh database from a gutted one: `open_or_create_segment`
/// recreates a missing active segment, so without this check an existing
/// database whose segments were all removed would silently reopen empty
/// (#2765).
pub(crate) fn wal_segments_present(backend: &dyn Backend) -> WalServiceResult<bool> {
    Ok(!list_segments(backend)?.is_empty())
}

pub(crate) fn resolve_resume_segment(
    backend: &dyn Backend,
    requested: u64,
) -> WalServiceResult<u64> {
    let on_disk_max = list_segments(backend)?
        .last()
        .map(|(segment_id, _)| *segment_id);
    let resolved = on_disk_max.map_or(requested, |max| max.max(requested));
    if resolved > requested {
        perf_trace::record_wal_open_segment_reconciliation();
    }
    Ok(resolved)
}

/// Verify the on-disk WAL segment id inventory is contiguous before opening.
/// Retention only ever trims a contiguous prefix and rotation allocates ids
/// sequentially, so a legitimate database NEVER has an interior id hole — a
/// gap means a segment was removed out of band, and recovery would silently
/// skip every record it held (#2690). Checkpoint state is irrelevant to this
/// check: interior segments are never individually retirable.
///
/// Loss at the inventory's *edges* is the durable commit watermark's job
/// (see [`WalService::durable_commit_watermark`]), judged at recovery time
/// where the checkpoint and replayed-record facts exist for comparison.
pub(crate) fn verify_wal_segment_inventory(backend: &dyn Backend) -> WalServiceResult<()> {
    let segments = list_segments(backend)?;
    for window in segments.windows(2) {
        let (lower, _) = &window[0];
        let (upper, _) = &window[1];
        if *upper != lower.saturating_add(1) {
            return Err(WalServiceError::SegmentInventoryGap {
                missing_segment: lower.saturating_add(1),
            });
        }
    }
    Ok(())
}

/// Reads the durable WAL watermark. A missing object (fresh database or a crash
/// before the first watermark sync) and an unreadable object (torn/corrupt
/// write) both return `None`: a corrupt marker degrades detection to a warning
/// rather than refusing a database that may be perfectly recoverable. Only a
/// genuine backend IO failure propagates.
fn read_wal_watermark(backend: &dyn Backend) -> WalServiceResult<Option<u64>> {
    let object =
        ObjectLayout::wal_watermark().map_err(|source| WalServiceError::Layout { source })?;
    match backend.read_object(&object) {
        Ok(bytes) => match decode_wal_watermark(&bytes) {
            Ok(watermark) => Ok(Some(watermark)),
            Err(source) => {
                tracing::warn!(
                    %object,
                    ?source,
                    "WAL segment-loss watermark is unreadable; deletion detection degraded to non-detection for this open"
                );
                Ok(None)
            }
        },
        Err(error) if error.kind() == BackendErrorKind::NotFound => Ok(None),
        Err(source) => Err(WalServiceError::Backend {
            operation: WalOperation::Open,
            object,
            source,
        }),
    }
}

/// Records the durable watermark after a segment is created and synced.
/// [`create_segment`] only ever creates a strictly higher segment, so the value
/// is monotonic and an unconditional durable overwrite is correct — it also
/// repairs any previously torn watermark. Writing this *after* the segment (and
/// never before) keeps the marker a safe lower bound: a crash before this leaves
/// it below the true highest, and recovery resumes at the higher on-disk segment
/// — the watermark never over-claims a segment that was never created.
fn update_wal_watermark(
    backend: &dyn Backend,
    highest_commit_version: u64,
) -> WalServiceResult<()> {
    let object =
        ObjectLayout::wal_watermark().map_err(|source| WalServiceError::Layout { source })?;
    let bytes =
        encode_wal_watermark(highest_commit_version).map_err(|source| WalServiceError::Format {
            operation: WalOperation::Sync,
            object: object.clone(),
            source,
        })?;
    let outcome = ObjectPublisher::new(backend)
        .publish_durable_replace(&object, &bytes)
        .map_err(|source| WalServiceError::Publish {
            operation: WalOperation::Sync,
            source,
        })?;
    validate_wal_publish_outcome(WalOperation::Sync, &object, bytes.len() as u64, &outcome)
}

fn list_segments(backend: &dyn Backend) -> WalServiceResult<Vec<WalSegmentObject>> {
    let prefix = ObjectLayout::wal_prefix().map_err(|source| WalServiceError::Layout { source })?;
    let mut segments = backend
        .list_prefix(&prefix)
        .map_err(|source| WalServiceError::List { source })?
        .into_iter()
        .map(parse_segment_object)
        .collect::<WalServiceResult<Vec<_>>>()?;
    // Valid segment names are fixed-width hex, but sorting by parsed id keeps
    // ordering correct even if a backend returns objects in arbitrary order.
    segments.sort_by_key(|(segment_id, _)| *segment_id);
    Ok(segments)
}

fn parse_segment_object(object: ObjectName) -> WalServiceResult<WalSegmentObject> {
    let segment_id = match ObjectLayout::classify_wal_object(&object) {
        Ok(Some(WalObjectClassification::Segment { segment_id })) => segment_id,
        Ok(None) => {
            return Err(WalServiceError::Backend {
                operation: WalOperation::List,
                object,
                source: BackendError::new(BackendErrorKind::InvalidObjectName, "not a WAL object"),
            });
        }
        Err(_) => {
            return Err(WalServiceError::Backend {
                operation: WalOperation::List,
                object,
                source: BackendError::new(
                    BackendErrorKind::InvalidObjectName,
                    "WAL segment object has invalid component",
                ),
            });
        }
    };
    if segment_id == 0 {
        return Err(WalServiceError::Backend {
            operation: WalOperation::List,
            object,
            source: BackendError::new(
                BackendErrorKind::InvalidObjectName,
                "WAL segment object id must be nonzero",
            ),
        });
    }
    Ok((segment_id, object))
}

/// One ranged read per refill; the transient the recovery path holds is one
/// chunk (plus at most one straddling envelope), never the tail.
const RECOVERY_READ_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// A WAL segment object read in ranged chunks: `bytes()` is the undecoded
/// window, `advance` consumes decoded envelopes, `refill` pulls the next
/// chunk (compacting the consumed prefix first). `at_object_end` is what
/// distinguishes "short envelope: fetch more" from "short envelope: torn
/// tail / corruption".
struct ChunkedSegment<'a> {
    backend: &'a dyn Backend,
    object: &'a ObjectName,
    object_len: u64,
    buffer: Vec<u8>,
    /// Absolute object offset of `buffer[0]`.
    buffer_start: u64,
    /// Bytes of `buffer` already decoded.
    consumed: usize,
}

impl<'a> ChunkedSegment<'a> {
    fn open(backend: &'a dyn Backend, object: &'a ObjectName) -> WalServiceResult<Self> {
        let object_len = backend
            .object_metadata(object)
            .map_err(|source| WalServiceError::Backend {
                operation: WalOperation::Read,
                object: object.clone(),
                source,
            })?
            .size_bytes();
        Ok(Self {
            backend,
            object,
            object_len,
            buffer: Vec::new(),
            buffer_start: 0,
            consumed: 0,
        })
    }

    fn bytes(&self) -> &[u8] {
        &self.buffer[self.consumed..]
    }

    /// Absolute object offset of the next undecoded byte.
    fn absolute_offset(&self) -> u64 {
        self.buffer_start + self.consumed as u64
    }

    fn advance(&mut self, decoded: usize) {
        self.consumed += decoded;
    }

    /// Whether the buffer already ends at the object's last byte — a decode
    /// that still wants more bytes here is a tail fact, not a short chunk.
    fn at_object_end(&self) -> bool {
        self.buffer_start + self.buffer.len() as u64 >= self.object_len
    }

    /// Pulls the next chunk. Returns false when the object is exhausted.
    fn refill(&mut self) -> WalServiceResult<bool> {
        let fetched_to = self.buffer_start + self.buffer.len() as u64;
        if fetched_to >= self.object_len {
            return Ok(false);
        }
        self.buffer.drain(..self.consumed);
        self.buffer_start += self.consumed as u64;
        self.consumed = 0;
        let take = RECOVERY_READ_CHUNK_BYTES.min(self.object_len - fetched_to);
        let chunk = self
            .backend
            .read_range(self.object, BackendRange::new(fetched_to, take))
            .map_err(|source| WalServiceError::Backend {
                operation: WalOperation::Read,
                object: self.object.clone(),
                source,
            })?;
        if chunk.len() as u64 != take {
            return Err(WalServiceError::UnexpectedObjectSize {
                object: self.object.clone(),
                expected: self.object_len,
                actual: fetched_to + chunk.len() as u64,
            });
        }
        self.buffer.extend_from_slice(&chunk);
        Ok(true)
    }
}

/// The streaming twin of `read_segment` + `decode_segment_bytes`: identical
/// decode steps, error mapping, and truncation semantics, but each record —
/// EVERY record, unfiltered, exactly like `read_segment` — is handed to
/// `visit` and dropped instead of accumulated.
#[allow(clippy::too_many_arguments, reason = "mirrors read_segment's inputs")]
fn visit_segment_records(
    backend: &dyn Backend,
    database_id: [u8; 16],
    segment_id: u64,
    object: &ObjectName,
    is_latest: bool,
    codec_id: &str,
    visit: &mut dyn FnMut(&WalRecord) -> std::ops::ControlFlow<()>,
) -> WalServiceResult<Option<WalTruncation>> {
    let mut segment = ChunkedSegment::open(backend, object)?;
    let header = loop {
        match decode_wal_segment_header(segment.bytes(), Some(segment_id)) {
            Ok((header, header_len)) => {
                segment.advance(header_len);
                break header;
            }
            Err(FormatError::InsufficientBytes { .. }) if !segment.at_object_end() => {
                segment.refill()?;
            }
            Err(source) => {
                return Err(WalServiceError::Format {
                    operation: WalOperation::Read,
                    object: object.clone(),
                    source,
                });
            }
        }
    };
    if header.database_id() != &database_id {
        return Err(WalServiceError::DatabaseMismatch {
            object: object.clone(),
            segment_id,
        });
    }

    while segment.absolute_offset() < segment.object_len {
        let (envelope, envelope_len) = match decode_wal_record_envelope(segment.bytes()) {
            Ok(decoded) => decoded,
            Err(FormatError::InsufficientBytes { .. }) if !segment.at_object_end() => {
                segment.refill()?;
                continue;
            }
            Err(FormatError::InsufficientBytes { .. }) if is_latest => {
                // A short FINAL envelope on the latest segment is the
                // repairable mid-append tail — decode_segment_bytes's exact
                // contract, offsets included.
                return Ok(Some(WalTruncation::new(
                    segment_id,
                    segment.absolute_offset(),
                    segment.object_len,
                )));
            }
            Err(source) => {
                return Err(WalServiceError::Format {
                    operation: WalOperation::Read,
                    object: object.clone(),
                    source,
                });
            }
        };
        let decoded_record = decode_wal_codec_bytes(codec_id, envelope.encoded_record())?;
        let (record, consumed) = decode_wal_record(decoded_record.as_ref()).map_err(|source| {
            WalServiceError::Format {
                operation: WalOperation::Read,
                object: object.clone(),
                source,
            }
        })?;
        if consumed != decoded_record.len() {
            return Err(WalServiceError::Format {
                operation: WalOperation::Read,
                object: object.clone(),
                source: FormatError::TrailingData {
                    format: "wal_record",
                    remaining: decoded_record.len() - consumed,
                },
            });
        }
        if let std::ops::ControlFlow::Break(()) = visit(&record) {
            return Ok(None);
        }
        segment.advance(envelope_len);
    }
    Ok(None)
}

fn read_segment(
    backend: &dyn Backend,
    database_id: [u8; 16],
    segment_id: u64,
    object: &ObjectName,
    is_latest: bool,
    codec_id: &str,
) -> WalServiceResult<WalRead> {
    let bytes = backend
        .read_object(object)
        .map_err(|source| WalServiceError::Backend {
            operation: WalOperation::Read,
            object: object.clone(),
            source,
        })?;
    decode_segment_bytes(
        database_id,
        segment_id,
        object,
        &bytes,
        is_latest,
        codec_id,
        WalOperation::Read,
    )
}

fn decode_segment_bytes(
    database_id: [u8; 16],
    segment_id: u64,
    object: &ObjectName,
    bytes: &[u8],
    is_latest: bool,
    codec_id: &str,
    operation: WalOperation,
) -> WalServiceResult<WalRead> {
    let (header, mut offset) =
        decode_wal_segment_header(bytes, Some(segment_id)).map_err(|source| {
            WalServiceError::Format {
                operation,
                object: object.clone(),
                source,
            }
        })?;
    if header.database_id() != &database_id {
        return Err(WalServiceError::DatabaseMismatch {
            object: object.clone(),
            segment_id,
        });
    }

    let mut records = Vec::new();
    while offset < bytes.len() {
        let (envelope, envelope_len) = match decode_wal_record_envelope(&bytes[offset..]) {
            Ok(decoded) => decoded,
            Err(FormatError::InsufficientBytes { .. }) if is_latest => {
                // A short final envelope on the latest segment is a repairable
                // tail fact. The same byte shape in an older segment is hard
                // corruption because later durable records depend on it.
                return Ok(WalRead::new(
                    records,
                    Some(WalTruncation::new(
                        segment_id,
                        offset as u64,
                        bytes.len() as u64,
                    )),
                ));
            }
            Err(source) => {
                return Err(WalServiceError::Format {
                    operation,
                    object: object.clone(),
                    source,
                });
            }
        };
        let decoded_record = decode_wal_codec_bytes(codec_id, envelope.encoded_record())?;
        let (record, consumed) = decode_wal_record(decoded_record.as_ref()).map_err(|source| {
            WalServiceError::Format {
                operation,
                object: object.clone(),
                source,
            }
        })?;
        if consumed != decoded_record.len() {
            return Err(WalServiceError::Format {
                operation,
                object: object.clone(),
                source: FormatError::TrailingData {
                    format: "wal_record",
                    remaining: decoded_record.len() - consumed,
                },
            });
        }
        records.push(record);
        offset += envelope_len;
    }

    Ok(WalRead::new(records, None))
}

fn segment_metadata_from_records(segment_id: u64, records: &[WalRecord]) -> SegmentMetadata {
    let mut metadata = SegmentMetadata::empty(segment_id);
    for record in records {
        metadata.track_record(record.commit_version(), record.commit_timestamp());
    }
    metadata
}

#[cfg(test)]
mod tests;
