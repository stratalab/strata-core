//! WAL segment file and record format.
//!
//! WAL segments are named `wal-NNNNNN.seg` where `NNNNNN` is a zero-padded segment number.
//!
//! # Segment Layout
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ Segment Header (32 bytes)          │
//! ├────────────────────────────────────┤
//! │ Record 1                           │
//! ├────────────────────────────────────┤
//! │ Record 2                           │
//! ├────────────────────────────────────┤
//! │ ...                                │
//! └────────────────────────────────────┘
//! ```
//!
//! # Record Layout (v2, issue #1577)
//!
//! ```text
//! ┌──────────┬──────────────┬──────────────┬──────────────────────┬──────────┐
//! │ Len (4B) │ FmtVer=2 (1) │ LenCRC (4B) │ Payload (variable)  │ CRC32 (4)│
//! └──────────┴──────────────┴──────────────┴──────────────────────┴──────────┘
//!
//! Payload:
//! ┌──────────────┬──────────────┬──────────────┬─────────────────────────────┐
//! │ TxnId (8)    │ BranchId (16)   │ Timestamp (8)│ Writeset (variable)         │
//! └──────────────┴──────────────┴──────────────┴─────────────────────────────┘
//! ```
//!
//! v1 records (FmtVer=1) lack the LenCRC field and are still readable.
//!
//! # Segment format versions
//!
//! * v1: original 32-byte header, no CRC. Read-only compatibility path
//!   (pre-March 2026).
//! * v2: 36-byte header with CRC32 over the first 32 bytes (March 2026,
//!   commit 77e9f258 / issue #1577). Records within the segment
//!   carry their own `LenCRC` for torn-write detection.
//! * v3: adds a per-record outer envelope `[u32 outer_len][u32 outer_len_crc]`
//!   wrapping the codec-encoded inner record (T3-E12 Phase 2, this
//!   build). The envelope is required so codec-encoded payloads
//!   (AES-GCM, etc.) have discoverable record boundaries — without
//!   it the reader cannot find the start of the next record on an
//!   encrypted WAL. Pre-v3 segments are rejected with
//!   [`SegmentHeaderError::LegacyFormat`]; clean break, no dual-path
//!   parsing.

use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use strata_core::id::TxnId;

/// Magic bytes identifying a WAL segment file: "STRA"
pub const SEGMENT_MAGIC: [u8; 4] = *b"STRA";

/// Current segment format version.
///
/// * v2 added the header CRC.
/// * v3 adds the per-record outer envelope for codec-aware reads.
pub const SEGMENT_FORMAT_VERSION: u32 = 3;

/// Oldest segment format version this build can read. Segments with a
/// `format_version` below this value are rejected with
/// [`SegmentHeaderError::LegacyFormat`] — operators must wipe `wal/`
/// and reopen.
pub const MIN_SUPPORTED_SEGMENT_FORMAT_VERSION: u32 = 3;

/// Size of v1 segment header in bytes (without CRC)
pub const SEGMENT_HEADER_SIZE: usize = 32;

/// Size of v2 segment header in bytes (with CRC32)
pub const SEGMENT_HEADER_SIZE_V2: usize = 36;

/// Current WAL record format version (v2 adds length CRC — see issue #1577)
pub const WAL_RECORD_FORMAT_VERSION: u8 = 2;

/// Typed errors from [`SegmentHeader::from_bytes_slice`].
///
/// Segment-header parsing folds minimum-size, magic, version, and CRC
/// checks into a single call so callers see a typed variant rather than
/// the `io::Error` / `Option<Self>` collapse the pre-T3-E12 code used.
/// `LegacyFormat` in particular is the load-bearing diagnostic the
/// engine's open path routes to [`strata_engine::StrataError::LegacyFormat`]
/// — see the T3-E12 tracking doc §D6 / §D8.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SegmentHeaderError {
    /// Fewer than `SEGMENT_HEADER_SIZE` bytes available. Typical cause:
    /// truncated segment file (disk full mid-create).
    #[error(
        "segment file too small for header: have {got} bytes, need at least {minimum_required}"
    )]
    InsufficientData {
        /// Bytes actually available.
        got: usize,
        /// Minimum header size required (`SEGMENT_HEADER_SIZE`).
        minimum_required: usize,
    },

    /// Magic bytes do not match [`SEGMENT_MAGIC`]. Typical cause: file
    /// is not a Strata WAL segment, or the leading bytes were corrupted.
    #[error("invalid segment magic bytes")]
    InvalidMagic,

    /// Segment was written by an older build whose on-disk format this
    /// build does not support. Operator action is required — the hint
    /// names the remediation (filesystem only; no CLI tool promised).
    #[error("legacy segment format: found version {found_version}. {hint}")]
    LegacyFormat {
        /// Format version read from disk.
        found_version: u32,
        /// Operator remediation hint.
        hint: String,
    },

    /// Segment was written by a newer build; this build refuses to
    /// guess at unfamiliar layouts.
    #[error("segment format version {found_version} is newer than this build supports (max {max_supported}). Upgrade Strata to read this database.")]
    FutureFormat {
        /// Format version read from disk.
        found_version: u32,
        /// Highest format version this build understands.
        max_supported: u32,
    },

    /// v2+ header CRC mismatch. Header bytes were corrupted in place
    /// (bit flip, partial overwrite).
    #[error("segment header CRC mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch {
        /// CRC read from disk.
        expected: u32,
        /// CRC recomputed over the header bytes.
        computed: u32,
    },

    /// Segment header's `segment_number` disagrees with the file name
    /// (`wal-NNNNNN.seg`). File was renamed or the header was swapped in.
    #[error("segment number mismatch: expected {expected}, header reports {got}")]
    SegmentNumberMismatch {
        /// Segment number derived from the file name.
        expected: u64,
        /// Segment number recorded in the header body.
        got: u64,
    },
}

/// Typed errors from [`WalSegment::open_read`] / [`WalSegment::open_append`].
///
/// Separates genuine I/O failures (disk full, permission denied) from
/// header-level failures (`Header(SegmentHeaderError::LegacyFormat)`,
/// etc.) so the reader layer can propagate typed `WalReaderError`
/// variants — T3-E12 §D8.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum WalSegmentError {
    /// Genuine filesystem-layer error (open failed, read failed, etc.).
    #[error("WAL segment I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Header-level failure surfaced from [`SegmentHeader::from_bytes_slice`].
    #[error("WAL segment header error: {0}")]
    Header(#[from] SegmentHeaderError),
}

/// WAL segment header (32 bytes for v1, 36 bytes for v2).
///
/// The header is written at the beginning of each segment file and contains
/// metadata for validation and compatibility checking.
///
/// v2 (format_version=2) adds a CRC32 checksum of the first 32 bytes,
/// appended as 4 bytes after the base header. v1 headers are still readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SegmentHeader {
    /// Magic bytes: "STRA" (0x53545241)
    pub magic: [u8; 4],

    /// Format version for forward compatibility
    pub format_version: u32,

    /// Segment number (monotonically increasing)
    pub segment_number: u64,

    /// Database UUID (for integrity checking across segments)
    pub database_uuid: [u8; 16],

    /// CRC32 of the first 32 header bytes (v2+ only, 0 for v1)
    pub header_crc: u32,
}

impl SegmentHeader {
    /// Create a new segment header (always creates v2 with CRC).
    pub fn new(segment_number: u64, database_uuid: [u8; 16]) -> Self {
        let mut header = SegmentHeader {
            magic: SEGMENT_MAGIC,
            format_version: SEGMENT_FORMAT_VERSION,
            segment_number,
            database_uuid,
            header_crc: 0,
        };
        // Compute CRC of the base 32 bytes
        header.header_crc = header.compute_crc();
        header
    }

    /// Compute CRC32 of the first 32 header bytes.
    fn compute_crc(&self) -> u32 {
        let mut base = [0u8; SEGMENT_HEADER_SIZE];
        base[0..4].copy_from_slice(&self.magic);
        base[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        base[8..16].copy_from_slice(&self.segment_number.to_le_bytes());
        base[16..32].copy_from_slice(&self.database_uuid);
        let mut hasher = Hasher::new();
        hasher.update(&base);
        hasher.finalize()
    }

    /// Serialize header to bytes (v2: 36 bytes).
    pub fn to_bytes(&self) -> [u8; SEGMENT_HEADER_SIZE_V2] {
        let mut bytes = [0u8; SEGMENT_HEADER_SIZE_V2];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.segment_number.to_le_bytes());
        bytes[16..32].copy_from_slice(&self.database_uuid);
        bytes[32..36].copy_from_slice(&self.header_crc.to_le_bytes());
        bytes
    }

    /// Deserialize and validate a segment header from a byte slice.
    ///
    /// This method is the single validation point for header-level
    /// failures: it folds the minimum-size, magic, version, CRC, and
    /// segment-number-mismatch checks that pre-T3-E12 callers performed
    /// ad-hoc with `io::Error` construction. Callers propagate the typed
    /// [`SegmentHeaderError`] via `?` and never fall back to generic
    /// `InvalidData` for header reasons (T3-E12 §D8).
    ///
    /// Segments with `format_version < MIN_SUPPORTED_SEGMENT_FORMAT_VERSION`
    /// are rejected with [`SegmentHeaderError::LegacyFormat`] — the
    /// hint names the manual filesystem remediation.
    ///
    /// `expected_segment_number`, when provided, enforces that the
    /// header's recorded segment number matches the file name; pass
    /// `None` when parsing a header slice without a corresponding file.
    pub fn from_bytes_slice(
        bytes: &[u8],
        expected_segment_number: Option<u64>,
    ) -> Result<Self, SegmentHeaderError> {
        if bytes.len() < SEGMENT_HEADER_SIZE {
            return Err(SegmentHeaderError::InsufficientData {
                got: bytes.len(),
                minimum_required: SEGMENT_HEADER_SIZE,
            });
        }

        let magic: [u8; 4] = bytes[0..4].try_into().expect("4-byte slice fits [u8;4]");
        if magic != SEGMENT_MAGIC {
            return Err(SegmentHeaderError::InvalidMagic);
        }

        let format_version =
            u32::from_le_bytes(bytes[4..8].try_into().expect("4-byte slice fits [u8;4]"));
        if format_version > SEGMENT_FORMAT_VERSION {
            return Err(SegmentHeaderError::FutureFormat {
                found_version: format_version,
                max_supported: SEGMENT_FORMAT_VERSION,
            });
        }
        if format_version < MIN_SUPPORTED_SEGMENT_FORMAT_VERSION {
            return Err(SegmentHeaderError::LegacyFormat {
                found_version: format_version,
                hint: format!(
                    "this build requires segment format version {required} \
                     (T3-E12 added a per-record outer envelope for codec-aware reads). \
                     Delete the `wal/` subdirectory and reopen with a fresh state.",
                    required = MIN_SUPPORTED_SEGMENT_FORMAT_VERSION,
                ),
            });
        }

        let segment_number =
            u64::from_le_bytes(bytes[8..16].try_into().expect("8-byte slice fits [u8;8]"));
        if let Some(expected) = expected_segment_number {
            if segment_number != expected {
                return Err(SegmentHeaderError::SegmentNumberMismatch {
                    expected,
                    got: segment_number,
                });
            }
        }
        let database_uuid: [u8; 16] = bytes[16..32]
            .try_into()
            .expect("16-byte slice fits [u8;16]");

        // v3+ segments always carry the CRC (v3 inherited the v2 CRC
        // layout). Short reads would have been caught by the initial
        // minimum-size check for a v2/v3 header length.
        if bytes.len() < SEGMENT_HEADER_SIZE_V2 {
            return Err(SegmentHeaderError::InsufficientData {
                got: bytes.len(),
                minimum_required: SEGMENT_HEADER_SIZE_V2,
            });
        }
        let stored_crc =
            u32::from_le_bytes(bytes[32..36].try_into().expect("4-byte slice fits [u8;4]"));
        let mut hasher = Hasher::new();
        hasher.update(&bytes[0..SEGMENT_HEADER_SIZE]);
        let computed_crc = hasher.finalize();
        if stored_crc != computed_crc {
            return Err(SegmentHeaderError::CrcMismatch {
                expected: stored_crc,
                computed: computed_crc,
            });
        }

        Ok(SegmentHeader {
            magic,
            format_version,
            segment_number,
            database_uuid,
            header_crc: stored_crc,
        })
    }

    /// Validate the header has correct magic bytes.
    pub fn is_valid(&self) -> bool {
        self.magic == SEGMENT_MAGIC
    }
}

/// WAL segment buffer size (8 KB). Batches multiple small WAL records
/// into a single `write` syscall, reducing per-record kernel overhead.
const WAL_BUF_SIZE: usize = 8192;

/// WAL segment file handle.
///
/// A segment is a single WAL file containing multiple records.
/// Only the active segment is writable; closed segments are immutable.
pub struct WalSegment {
    /// Buffered file handle — reduces syscalls for small writes.
    /// `flush()` is called before `sync_all()` to ensure all buffered
    /// data reaches the kernel before fsync (ACID-006).
    file: BufWriter<File>,

    /// Segment number
    segment_number: u64,

    /// Current write position (bytes from start)
    write_position: u64,

    /// Path to segment file
    path: PathBuf,

    /// Whether this segment is closed (immutable)
    closed: bool,

    /// Database UUID for this segment
    database_uuid: [u8; 16],

    /// Actual header size in bytes (32 for v1, 36 for v2)
    header_size: usize,
}

impl WalSegment {
    /// Create a new WAL segment.
    ///
    /// Creates a new segment file and writes the v2 header (36 bytes with CRC).
    pub fn create(
        dir: &Path,
        segment_number: u64,
        database_uuid: [u8; 16],
    ) -> std::io::Result<Self> {
        let path = Self::segment_path(dir, segment_number);

        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        // Restrict to owner-only (defense in depth for data files)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }

        // Write v2 header (36 bytes) and flush to OS so the segment is
        // immediately visible on disk (wal_disk_usage reads file metadata).
        let header = SegmentHeader::new(segment_number, database_uuid);
        let mut file = BufWriter::with_capacity(WAL_BUF_SIZE, file);
        file.write_all(&header.to_bytes())?;
        file.flush()?;

        // Fsync the parent directory so the new file's directory entry is
        // durable. Without this, a power loss can silently lose the entire
        // segment even though the file contents were fsynced (issue #1711).
        Self::fsync_parent_directory(dir)?;

        Ok(WalSegment {
            file,
            segment_number,
            write_position: SEGMENT_HEADER_SIZE_V2 as u64,
            path,
            closed: false,
            database_uuid,
            header_size: SEGMENT_HEADER_SIZE_V2,
        })
    }

    /// Open an existing WAL segment for reading.
    ///
    /// Validates the header through the typed [`SegmentHeader::from_bytes_slice`]
    /// path — magic, version, CRC, and segment-number-matches-filename
    /// checks all surface as [`SegmentHeaderError`] variants rather than
    /// generic `io::Error::InvalidData` strings (T3-E12 §D8).
    pub fn open_read(dir: &Path, segment_number: u64) -> Result<Self, WalSegmentError> {
        let path = Self::segment_path(dir, segment_number);
        let mut file = OpenOptions::new().read(true).open(&path)?;

        // Read the full v2/v3-size header. `from_bytes_slice` treats a
        // short read as `SegmentHeaderError::InsufficientData`.
        let (header_buf, bytes_read) = Self::read_header_bytes(&mut file)?;
        let header =
            SegmentHeader::from_bytes_slice(&header_buf[..bytes_read], Some(segment_number))?;

        let write_position = file.seek(SeekFrom::End(0))?;

        Ok(WalSegment {
            file: BufWriter::with_capacity(WAL_BUF_SIZE, file),
            segment_number: header.segment_number,
            write_position,
            path,
            closed: true, // Opened for reading = treat as closed
            database_uuid: header.database_uuid,
            header_size: SEGMENT_HEADER_SIZE_V2,
        })
    }

    /// Read exactly `SEGMENT_HEADER_SIZE_V2` bytes (or fewer if EOF) into
    /// a fresh buffer. Retries on `Interrupted`. Shared between
    /// `open_read` and `open_append`.
    fn read_header_bytes(
        file: &mut File,
    ) -> std::io::Result<([u8; SEGMENT_HEADER_SIZE_V2], usize)> {
        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE_V2];
        let mut total = 0;
        loop {
            match file.read(&mut header_buf[total..]) {
                Ok(0) => break,
                Ok(n) => {
                    total += n;
                    if total >= SEGMENT_HEADER_SIZE_V2 {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok((header_buf, total))
    }

    /// Open an existing WAL segment for appending.
    ///
    /// Used when resuming writes to an existing active segment.
    /// Shares the typed [`SegmentHeader::from_bytes_slice`] validation
    /// path with [`WalSegment::open_read`].
    pub fn open_append(dir: &Path, segment_number: u64) -> Result<Self, WalSegmentError> {
        let path = Self::segment_path(dir, segment_number);
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        let (header_buf, bytes_read) = Self::read_header_bytes(&mut file)?;
        let header =
            SegmentHeader::from_bytes_slice(&header_buf[..bytes_read], Some(segment_number))?;

        // Seek to end for appending
        let write_position = file.seek(SeekFrom::End(0))?;

        Ok(WalSegment {
            file: BufWriter::with_capacity(WAL_BUF_SIZE, file),
            segment_number: header.segment_number,
            write_position,
            path,
            closed: false,
            database_uuid: header.database_uuid,
            header_size: SEGMENT_HEADER_SIZE_V2,
        })
    }

    /// Fsync a parent directory to make new directory entries durable.
    /// Retries once on transient failure (matching disk_snapshot::fsync_directory).
    fn fsync_parent_directory(dir: &Path) -> std::io::Result<()> {
        let do_sync = || -> std::io::Result<()> {
            let dir_file = File::open(dir)?;
            dir_file.sync_all()
        };
        match do_sync() {
            Ok(()) => Ok(()),
            Err(first_err) => {
                tracing::warn!(target: "strata::wal",
                    error = %first_err, path = %dir.display(),
                    "WAL directory fsync failed, retrying once");
                do_sync()
            }
        }
    }

    /// Generate segment file path.
    ///
    /// Format: `wal-NNNNNN.seg` where NNNNNN is zero-padded segment number.
    pub fn segment_path(dir: &Path, segment_number: u64) -> PathBuf {
        dir.join(format!("wal-{:06}.seg", segment_number))
    }

    /// Get segment number.
    pub fn segment_number(&self) -> u64 {
        self.segment_number
    }

    /// Get current segment size in bytes.
    pub fn size(&self) -> u64 {
        self.write_position
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get database UUID.
    pub fn database_uuid(&self) -> [u8; 16] {
        self.database_uuid
    }

    /// Get the actual header size in bytes (32 for v1, 36 for v2).
    pub fn header_size(&self) -> usize {
        self.header_size
    }

    /// Write bytes to segment and update write position.
    ///
    /// Returns an error if the segment is closed.
    pub fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        if self.closed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Cannot write to closed segment",
            ));
        }

        self.file.write_all(data)?;
        self.write_position += data.len() as u64;
        Ok(())
    }

    /// Sync segment data to disk.
    ///
    /// Flushes the BufWriter to the OS page cache, then fsyncs the
    /// underlying file to stable storage. Both steps are required for
    /// ACID-006 (Always mode durability guarantee).
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }

    /// Flush the BufWriter to the OS page cache without fsync.
    ///
    /// Used by the background sync path: flush under the WAL lock,
    /// then fsync outside the lock via a cloned file descriptor.
    pub fn flush_to_os(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    /// Clone the underlying file descriptor for out-of-lock fsync.
    ///
    /// Returns a `File` handle that shares the same OS file descriptor.
    /// Calling `sync_all()` on the clone fsyncs the same file without
    /// needing to hold the WAL mutex.
    pub fn try_clone_fd(&self) -> std::io::Result<File> {
        self.file.get_ref().try_clone()
    }

    /// Mark segment as closed (immutable).
    ///
    /// Syncs data to disk before closing.
    pub fn close(&mut self) -> std::io::Result<()> {
        if !self.closed {
            self.file.flush()?;
            self.file.get_ref().sync_all()?;
            self.closed = true;
        }
        Ok(())
    }

    /// Check if segment is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get mutable reference to underlying file (for reading).
    pub fn file_mut(&mut self) -> &mut File {
        self.file.get_mut()
    }

    /// Seek to a specific position for reading.
    pub fn seek_to(&mut self, position: u64) -> std::io::Result<u64> {
        self.file.seek(SeekFrom::Start(position))
    }

    /// Flush any buffered data and seek to the end of the file.
    ///
    /// Used in multi-process mode: after another process has appended to the
    /// same segment, this re-positions the write cursor at the true end of file.
    pub fn seek_to_end(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        let end = self.file.seek(SeekFrom::End(0))?;
        self.write_position = end;
        Ok(())
    }

    /// Truncate segment at the given position.
    ///
    /// Used during recovery to remove partial records.
    pub fn truncate(&mut self, position: u64) -> std::io::Result<()> {
        if self.closed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Cannot truncate closed segment",
            ));
        }

        self.file.flush()?;
        self.file.get_mut().set_len(position)?;
        self.write_position = position;
        self.file.seek(SeekFrom::Start(position))?;
        Ok(())
    }
}

/// WAL record for a committed transaction.
///
/// Each record is self-delimiting with a length prefix and CRC32 checksum.
/// Records are immutable once written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Transaction ID (assigned by engine, NOT by storage)
    pub txn_id: TxnId,

    /// Run this transaction belongs to (UUID bytes)
    pub branch_id: [u8; 16],

    /// Commit timestamp (microseconds since epoch)
    pub timestamp: u64,

    /// Serialized writeset (codec-encoded)
    pub writeset: Vec<u8>,
}

impl WalRecord {
    /// Create a new WAL record.
    pub fn new(txn_id: TxnId, branch_id: [u8; 16], timestamp: u64, writeset: Vec<u8>) -> Self {
        WalRecord {
            txn_id,
            branch_id,
            timestamp,
            writeset,
        }
    }

    /// Serialize record to bytes (for writing to WAL).
    ///
    /// v2 format: length (4) + format_version=2 (1) + length_crc (4) + payload + crc32 (4)
    ///
    /// The length field contains the size of everything after it.
    /// The length_crc protects the length field against torn writes (issue #1577).
    /// The main CRC covers format_version + length_crc + txn_id + branch_id + timestamp + writeset.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Build inner payload: format_version + [placeholder for length_crc] + fields
        let mut payload = Vec::with_capacity(37 + self.writeset.len());
        payload.push(WAL_RECORD_FORMAT_VERSION); // 1 byte: version = 2
        payload.extend_from_slice(&[0u8; 4]); // 4 bytes: placeholder for length_crc
        payload.extend_from_slice(&self.txn_id.as_u64().to_le_bytes());
        payload.extend_from_slice(&self.branch_id);
        payload.extend_from_slice(&self.timestamp.to_le_bytes());
        payload.extend_from_slice(&self.writeset);

        // Total length = payload + main CRC
        let total_len = payload.len() + 4;
        let length_bytes = (total_len as u32).to_le_bytes();

        // Fill in the length CRC (CRC32 of the 4-byte length field)
        let length_crc = Self::compute_crc(&length_bytes);
        payload[1..5].copy_from_slice(&length_crc.to_le_bytes());

        // Main CRC covers the full payload (version + length_crc + fields)
        let crc = Self::compute_crc(&payload);

        // Build final record: length + payload + crc
        let mut record = Vec::with_capacity(4 + total_len);
        record.extend_from_slice(&length_bytes);
        record.extend_from_slice(&payload);
        record.extend_from_slice(&crc.to_le_bytes());

        record
    }

    /// Build WAL record bytes from a borrowed writeset, writing into a
    /// caller-provided buffer. Produces identical bytes to
    /// `WalRecord::new(txn_id, branch_id, timestamp, writeset.to_vec()).to_bytes()`.
    ///
    /// Avoids constructing the intermediate `WalRecord` struct with its owned
    /// `writeset: Vec<u8>`, and avoids the double-Vec allocation in `to_bytes()`.
    pub fn build_bytes_from_writeset_into(
        buf: &mut Vec<u8>,
        txn_id: TxnId,
        branch_id: [u8; 16],
        timestamp: u64,
        writeset: &[u8],
    ) {
        buf.clear();

        // Payload: format_version(1) + length_crc(4) + txn_id(8) + branch_id(16) + timestamp(8) + writeset
        let payload_len = 1 + 4 + 8 + 16 + 8 + writeset.len();
        let total_len = payload_len + 4; // + main CRC
        buf.reserve(4 + total_len);

        // Length prefix
        let length_bytes = (total_len as u32).to_le_bytes();
        buf.extend_from_slice(&length_bytes);

        // Payload
        let payload_start = buf.len();
        buf.push(WAL_RECORD_FORMAT_VERSION);
        buf.extend_from_slice(&[0u8; 4]); // length_crc placeholder
        buf.extend_from_slice(&txn_id.as_u64().to_le_bytes());
        buf.extend_from_slice(&branch_id);
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(writeset);

        // Fill in length CRC
        let length_crc = Self::compute_crc(&length_bytes);
        buf[payload_start + 1..payload_start + 5].copy_from_slice(&length_crc.to_le_bytes());

        // Main CRC over payload
        let crc = Self::compute_crc(&buf[payload_start..]);
        buf.extend_from_slice(&crc.to_le_bytes());
    }

    /// Deserialize record from bytes.
    ///
    /// Handles both v1 (no length CRC) and v2 (with length CRC) formats.
    /// Returns (record, bytes_consumed) on success.
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), WalRecordError> {
        // Need at least 5 bytes: 4 (length) + 1 (format_version)
        if bytes.len() < 5 {
            return Err(WalRecordError::InsufficientData);
        }

        // Read length
        let length = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;

        if length == 0 {
            return Err(WalRecordError::InvalidFormat);
        }

        // Peek at format version byte (always at offset 4, both v1 and v2)
        let format_version = bytes[4];

        if format_version == 2 {
            // v2: verify length CRC BEFORE trusting the length field
            if bytes.len() < 9 {
                return Err(WalRecordError::InsufficientData);
            }
            let stored_length_crc = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
            let computed_length_crc = Self::compute_crc(&bytes[0..4]);
            if stored_length_crc != computed_length_crc {
                return Err(WalRecordError::LengthChecksumMismatch);
            }
        }

        // Now trust the length (saturating add guards against usize overflow on 32-bit)
        let total = 4usize.saturating_add(length);
        if bytes.len() < total {
            return Err(WalRecordError::InsufficientData);
        }

        let payload_with_crc = &bytes[4..total];

        if format_version == 2 {
            // v2 minimum: 1 (version) + 4 (length_crc) + 4 (main CRC) = 9
            if length < 9 {
                return Err(WalRecordError::InvalidFormat);
            }

            // Split payload and CRC
            let payload = &payload_with_crc[..length - 4];
            let stored_crc = u32::from_le_bytes(payload_with_crc[length - 4..].try_into().unwrap());

            // Verify main CRC (covers version + length_crc + fields)
            let computed_crc = Self::compute_crc(payload);
            if computed_crc != stored_crc {
                return Err(WalRecordError::ChecksumMismatch {
                    expected: stored_crc,
                    computed: computed_crc,
                });
            }

            // Parse v2 payload: skip version (1) + length_crc (4) = 5 bytes
            // Minimum: 5 + 8 (txn_id) + 16 (branch_id) + 8 (timestamp) = 37
            if payload.len() < 37 {
                return Err(WalRecordError::InvalidFormat);
            }

            let txn_id = TxnId(u64::from_le_bytes(payload[5..13].try_into().unwrap()));
            let branch_id: [u8; 16] = payload[13..29].try_into().unwrap();
            let timestamp = u64::from_le_bytes(payload[29..37].try_into().unwrap());
            let writeset = payload[37..].to_vec();

            Ok((
                WalRecord {
                    txn_id,
                    branch_id,
                    timestamp,
                    writeset,
                },
                4 + length,
            ))
        } else if format_version == 1 {
            // v1: original format without length CRC
            if length < 5 {
                return Err(WalRecordError::InvalidFormat);
            }

            let payload = &payload_with_crc[..length - 4];
            let stored_crc = u32::from_le_bytes(payload_with_crc[length - 4..].try_into().unwrap());

            let computed_crc = Self::compute_crc(payload);
            if computed_crc != stored_crc {
                return Err(WalRecordError::ChecksumMismatch {
                    expected: stored_crc,
                    computed: computed_crc,
                });
            }

            if payload.len() < 33 {
                return Err(WalRecordError::InvalidFormat);
            }

            let txn_id = TxnId(u64::from_le_bytes(payload[1..9].try_into().unwrap()));
            let branch_id: [u8; 16] = payload[9..25].try_into().unwrap();
            let timestamp = u64::from_le_bytes(payload[25..33].try_into().unwrap());
            let writeset = payload[33..].to_vec();

            Ok((
                WalRecord {
                    txn_id,
                    branch_id,
                    timestamp,
                    writeset,
                },
                4 + length,
            ))
        } else {
            Err(WalRecordError::UnsupportedVersion(format_version))
        }
    }

    /// Compute CRC32 checksum of data.
    fn compute_crc(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Verify the checksum of serialized record bytes (delegates to from_bytes).
    pub fn verify_checksum(bytes: &[u8]) -> Result<(), WalRecordError> {
        Self::from_bytes(bytes).map(|_| ())
    }
}

/// WAL record parsing errors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WalRecordError {
    /// Not enough data to parse record
    #[error("Insufficient data to parse record")]
    InsufficientData,

    /// Record format is invalid
    #[error("Invalid record format")]
    InvalidFormat,

    /// Checksum verification failed
    #[error("Checksum mismatch: expected {expected:08x}, computed {computed:08x}")]
    ChecksumMismatch {
        /// Expected checksum from record
        expected: u32,
        /// Computed checksum
        computed: u32,
    },

    /// Unsupported format version
    #[error("Unsupported format version: {0}")]
    UnsupportedVersion(u8),

    /// Length field checksum mismatch (torn write to length prefix — issue #1577)
    #[error("Length field checksum mismatch (possible torn write)")]
    LengthChecksumMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::id::TxnId;
    use tempfile::tempdir;

    #[test]
    fn test_segment_header_roundtrip() {
        let header = SegmentHeader::new(12345, [0xAB; 16]);

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE_V2);
        let parsed = SegmentHeader::from_bytes_slice(&bytes, Some(12345)).unwrap();

        assert_eq!(parsed.magic, SEGMENT_MAGIC);
        assert_eq!(parsed.format_version, SEGMENT_FORMAT_VERSION);
        assert_eq!(parsed.segment_number, 12345);
        assert_eq!(parsed.database_uuid, [0xAB; 16]);
        assert!(parsed.is_valid());
        assert_ne!(parsed.header_crc, 0);
    }

    #[test]
    fn test_segment_header_rejects_pre_v3_as_legacy_format() {
        // T3-E12 Phase 2: v2 (and older) segment headers must surface
        // a typed `LegacyFormat` error, not silently parse. This is the
        // discriminator the engine's open path uses to produce
        // `StrataError::LegacyFormat` and skip the T3-E10 wipe.
        let mut bytes = [0u8; SEGMENT_HEADER_SIZE_V2];
        bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
        bytes[4..8].copy_from_slice(&2u32.to_le_bytes()); // v2
        bytes[8..16].copy_from_slice(&1u64.to_le_bytes());
        bytes[16..32].copy_from_slice(&[0xAB; 16]);
        let crc = {
            let mut h = Hasher::new();
            h.update(&bytes[0..SEGMENT_HEADER_SIZE]);
            h.finalize()
        };
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());

        match SegmentHeader::from_bytes_slice(&bytes, Some(1)) {
            Err(SegmentHeaderError::LegacyFormat {
                found_version,
                hint,
            }) => {
                assert_eq!(found_version, 2);
                assert!(
                    hint.contains(&format!(
                        "requires segment format version {}",
                        MIN_SUPPORTED_SEGMENT_FORMAT_VERSION
                    )),
                    "hint should name the required version, got: {hint}"
                );
                assert!(
                    hint.contains("wal/"),
                    "hint should name the wal/ directory for remediation, got: {hint}"
                );
            }
            other => panic!("expected LegacyFormat, got {other:?}"),
        }
    }

    #[test]
    fn test_segment_header_invalid_magic() {
        let mut header = SegmentHeader::new(1, [0; 16]);
        header.magic = *b"XXXX";
        assert!(!header.is_valid());
    }

    #[test]
    fn test_segment_path_format() {
        let dir = Path::new("/tmp/wal");
        assert_eq!(
            WalSegment::segment_path(dir, 1),
            PathBuf::from("/tmp/wal/wal-000001.seg")
        );
        assert_eq!(
            WalSegment::segment_path(dir, 999999),
            PathBuf::from("/tmp/wal/wal-999999.seg")
        );
    }

    #[test]
    fn test_segment_create_and_open() {
        let dir = tempdir().unwrap();
        let uuid = [1u8; 16];

        // Create segment (v2 with 36-byte header)
        let segment = WalSegment::create(dir.path(), 1, uuid).unwrap();
        assert_eq!(segment.segment_number(), 1);
        assert_eq!(segment.size(), SEGMENT_HEADER_SIZE_V2 as u64);
        assert!(!segment.is_closed());
        assert_eq!(segment.database_uuid(), uuid);
        drop(segment);

        // Open for reading
        let segment = WalSegment::open_read(dir.path(), 1).unwrap();
        assert_eq!(segment.segment_number(), 1);
        assert!(segment.is_closed());
    }

    #[test]
    fn test_segment_write_and_close() {
        let dir = tempdir().unwrap();
        let uuid = [2u8; 16];

        let mut segment = WalSegment::create(dir.path(), 1, uuid).unwrap();
        let initial_size = segment.size();

        segment.write(b"test data").unwrap();
        assert_eq!(segment.size(), initial_size + 9);

        segment.close().unwrap();
        assert!(segment.is_closed());

        // Cannot write to closed segment
        let result = segment.write(b"more data");
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_record_roundtrip() {
        let record = WalRecord::new(TxnId(42), [1u8; 16], 1234567890, vec![1, 2, 3, 4, 5]);

        let bytes = record.to_bytes();
        let (parsed, consumed) = WalRecord::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.txn_id, TxnId(42));
        assert_eq!(parsed.branch_id, [1u8; 16]);
        assert_eq!(parsed.timestamp, 1234567890);
        assert_eq!(parsed.writeset, vec![1, 2, 3, 4, 5]);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_wal_record_empty_writeset() {
        let record = WalRecord::new(TxnId(1), [0u8; 16], 0, Vec::new());

        let bytes = record.to_bytes();
        let (parsed, _) = WalRecord::from_bytes(&bytes).unwrap();

        assert!(parsed.writeset.is_empty());
    }

    #[test]
    fn test_wal_record_checksum_failure() {
        let record = WalRecord::new(TxnId(42), [1u8; 16], 1234567890, vec![1, 2, 3]);

        let mut bytes = record.to_bytes();

        // Corrupt a byte in the payload
        bytes[10] ^= 0xFF;

        let result = WalRecord::from_bytes(&bytes);
        assert!(matches!(
            result,
            Err(WalRecordError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_wal_record_insufficient_data() {
        // Too short for length field
        let result = WalRecord::from_bytes(&[1, 2, 3]);
        assert!(matches!(result, Err(WalRecordError::InsufficientData)));

        // Length says more data than available
        let result = WalRecord::from_bytes(&[100, 0, 0, 0, 1, 2, 3]);
        assert!(matches!(result, Err(WalRecordError::InsufficientData)));
    }

    #[test]
    fn test_wal_record_verify_checksum() {
        let record = WalRecord::new(TxnId(1), [0u8; 16], 123, vec![1, 2, 3]);
        let bytes = record.to_bytes();

        assert!(WalRecord::verify_checksum(&bytes).is_ok());

        let mut corrupted = bytes.clone();
        corrupted[10] ^= 0xFF;
        assert!(WalRecord::verify_checksum(&corrupted).is_err());
    }

    /// Issue #1577: v1 records (written before the length CRC change) must still
    /// be readable by the new from_bytes so recovery of existing databases works.
    #[test]
    fn test_v1_wal_record_still_readable() {
        // Manually construct a v1 record (format_version=1, no length_crc)
        let txn_id: u64 = 42;
        let branch_id = [0xAB; 16];
        let timestamp: u64 = 9999;
        let writeset = vec![1, 2, 3, 4, 5];

        let mut payload = Vec::new();
        payload.push(1u8); // format_version = 1
        payload.extend_from_slice(&txn_id.to_le_bytes());
        payload.extend_from_slice(&branch_id);
        payload.extend_from_slice(&timestamp.to_le_bytes());
        payload.extend_from_slice(&writeset);

        let crc = {
            let mut h = Hasher::new();
            h.update(&payload);
            h.finalize()
        };
        let total_len = (payload.len() + 4) as u32;

        let mut record_bytes = Vec::new();
        record_bytes.extend_from_slice(&total_len.to_le_bytes());
        record_bytes.extend_from_slice(&payload);
        record_bytes.extend_from_slice(&crc.to_le_bytes());

        let (parsed, consumed) = WalRecord::from_bytes(&record_bytes).unwrap();
        assert_eq!(parsed.txn_id, TxnId(txn_id));
        assert_eq!(parsed.branch_id, branch_id);
        assert_eq!(parsed.timestamp, timestamp);
        assert_eq!(parsed.writeset, writeset);
        assert_eq!(consumed, record_bytes.len());
    }

    #[test]
    fn test_multiple_records_in_sequence() {
        let records = vec![
            WalRecord::new(TxnId(1), [1u8; 16], 100, vec![1, 2, 3]),
            WalRecord::new(TxnId(2), [2u8; 16], 200, vec![4, 5, 6, 7]),
            WalRecord::new(TxnId(3), [3u8; 16], 300, vec![]),
        ];

        // Serialize all records
        let mut all_bytes = Vec::new();
        for record in &records {
            all_bytes.extend_from_slice(&record.to_bytes());
        }

        // Parse them back
        let mut offset = 0;
        for expected in &records {
            let (parsed, consumed) = WalRecord::from_bytes(&all_bytes[offset..]).unwrap();
            assert_eq!(parsed.txn_id, expected.txn_id);
            assert_eq!(parsed.branch_id, expected.branch_id);
            assert_eq!(parsed.timestamp, expected.timestamp);
            assert_eq!(parsed.writeset, expected.writeset);
            offset += consumed;
        }

        assert_eq!(offset, all_bytes.len());
    }

    // ========================================================================
    // D-10: V1/V2 segment header tests
    // ========================================================================

    // T3-E12 Phase 2: v1 and v2 segment headers are no longer readable.
    // The `test_v1_header_still_readable` test was intentionally removed
    // with the `SEGMENT_FORMAT_VERSION` 2→3 bump; its replacement
    // (`test_segment_header_rejects_pre_v3_as_legacy_format` above)
    // covers the new typed-rejection contract.

    #[test]
    fn test_segment_header_rejects_bad_crc() {
        // Create a valid (v3) header, then corrupt the CRC.
        let header = SegmentHeader::new(1, [0; 16]);
        let mut bytes = header.to_bytes();
        bytes[32] ^= 0xFF;

        assert!(matches!(
            SegmentHeader::from_bytes_slice(&bytes, Some(1)),
            Err(SegmentHeaderError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn test_segment_header_rejects_corrupted_payload() {
        // Create a valid (v3) header, then corrupt a byte inside the
        // `database_uuid` field (bytes 16..32). CRC recomputation
        // mismatches the stored CRC. We deliberately avoid the
        // `segment_number` bytes (8..16) so the earlier
        // `SegmentNumberMismatch` check does not fire first.
        let header = SegmentHeader::new(42, [0xCC; 16]);
        let mut bytes = header.to_bytes();
        bytes[20] ^= 0xFF;

        assert!(matches!(
            SegmentHeader::from_bytes_slice(&bytes, Some(42)),
            Err(SegmentHeaderError::CrcMismatch { .. })
        ));
    }

    /// Issue #1711: WalSegment::create() must fsync the parent directory so the
    /// new segment's directory entry survives power loss.
    ///
    /// We cannot simulate power loss in a unit test, but we verify that:
    /// 1. create() succeeds without error (including the directory fsync path)
    /// 2. The segment file is immediately visible via directory listing
    /// 3. Multiple segments in the same directory all remain visible after creation
    #[test]
    fn test_issue_1711_create_fsyncs_parent_directory() {
        let dir = tempdir().unwrap();
        let uuid = [0xAA; 16];

        // Create several segments in the same directory — each must fsync the
        // parent directory so its directory entry is durable.
        for i in 1..=5 {
            let segment = WalSegment::create(dir.path(), i, uuid).unwrap();
            assert_eq!(segment.segment_number(), i);
            drop(segment);

            // Verify segment file is visible in the directory listing
            let expected_path = WalSegment::segment_path(dir.path(), i);
            assert!(
                expected_path.exists(),
                "Segment {} should exist after create()",
                i
            );
        }

        // Verify all segments are visible by listing the directory
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
            .collect();
        assert_eq!(entries.len(), 5, "All 5 segments should be visible");

        // Verify each segment can be reopened (directory entry intact)
        for i in 1..=5 {
            let segment = WalSegment::open_read(dir.path(), i).unwrap();
            assert_eq!(segment.segment_number(), i);
        }

        // Verify the parent directory can be opened and synced (mechanism test)
        let dir_fd = File::open(dir.path()).unwrap();
        dir_fd.sync_all().unwrap();
    }

    // T3-E12 Phase 2: the v1-trailing-data test was deleted with the
    // 2→3 bump — v1/v2 headers now surface `LegacyFormat`, not a
    // successful parse.

    #[test]
    fn test_segment_header_rejects_future_version() {
        // Build a header with format_version = 99 (far beyond current).
        let mut bytes = [0u8; SEGMENT_HEADER_SIZE_V2];
        bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
        bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
        bytes[8..16].copy_from_slice(&1u64.to_le_bytes());
        bytes[16..32].copy_from_slice(&[0xAA; 16]);
        let crc = {
            let mut hasher = Hasher::new();
            hasher.update(&bytes[0..SEGMENT_HEADER_SIZE]);
            hasher.finalize()
        };
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());

        match SegmentHeader::from_bytes_slice(&bytes, Some(1)) {
            Err(SegmentHeaderError::FutureFormat {
                found_version,
                max_supported,
            }) => {
                assert_eq!(found_version, 99);
                assert_eq!(max_supported, SEGMENT_FORMAT_VERSION);
            }
            other => panic!("expected FutureFormat, got {other:?}"),
        }
    }
}
