//! WAL reader for recovery and replay.
//!
//! The reader handles reading WAL records from segments for recovery.

use crate::durability::codec::{clone_codec, StorageCodec};
use crate::durability::format::segment_meta::SegmentMeta;
use crate::durability::format::{WalRecord, WalRecordError, WalSegment};
use crate::durability::wal::writer::WAL_RECORD_ENVELOPE_OVERHEAD;
use crc32fast::Hasher;
use std::borrow::Cow;
use std::io::Read;
use std::path::{Path, PathBuf};
use strata_core::id::TxnId;
use tracing::warn;

/// Maximum number of bytes to scan forward when searching for the next
/// valid record after encountering corruption during WAL recovery.
const MAX_RECOVERY_SCAN_WINDOW: usize = 8 * 1_024 * 1_024; // 8 MB

/// Parse the per-record outer envelope (T3-E12 v3):
/// `[u32 outer_len][u32 outer_len_crc]`. Returns `Ok(Some((outer_len,
/// bytes_consumed)))` if the envelope header parsed and the length CRC
/// matched, `Ok(None)` if there are too few bytes to read the header
/// (caller treats as `PartialRecord` tail), and `Err(stored_crc,
/// computed_crc)` on CRC mismatch (caller decides strict/lossy).
fn read_outer_envelope(buf: &[u8]) -> Result<Option<u32>, (u32, u32)> {
    if buf.len() < WAL_RECORD_ENVELOPE_OVERHEAD {
        return Ok(None);
    }
    let outer_len_bytes: [u8; 4] = buf[0..4].try_into().expect("4-byte slice");
    let outer_len = u32::from_le_bytes(outer_len_bytes);
    let stored_crc = u32::from_le_bytes(buf[4..8].try_into().expect("4-byte slice"));
    let mut h = Hasher::new();
    h.update(&outer_len_bytes);
    let computed_crc = h.finalize();
    if stored_crc != computed_crc {
        return Err((stored_crc, computed_crc));
    }
    Ok(Some(outer_len))
}

/// WAL reader for iterating over records in segments.
///
/// The reader can read individual segments or scan all segments in order.
///
/// By default, mid-segment corruption (checksum mismatch with valid records
/// after the corrupted region) is fatal — `read_segment` returns an error.
/// Use [`with_lossy_recovery`](WalReader::with_lossy_recovery) to opt in to
/// the scan-ahead behavior that skips corrupted regions.
///
/// A codec may be installed via [`with_codec`](WalReader::with_codec).
/// With no codec, each record's payload is passed through to
/// `WalRecord::from_bytes` as-is (identity behavior). With a codec,
/// the payload is decoded first; decode failures surface as
/// `WalReaderError::CodecDecode` and are NEVER demoted to
/// `PartialRecord` — the engine's lossy branch catches the typed
/// error and routes to T3-E10 whole-DB wipe-and-reopen (§D5).
#[derive(Default)]
pub struct WalReader {
    pub(crate) allow_lossy_recovery: bool,
    codec: Option<Box<dyn StorageCodec>>,
}

impl WalReader {
    /// Create a new WAL reader with strict (default) corruption handling.
    pub fn new() -> Self {
        WalReader {
            allow_lossy_recovery: false,
            codec: None,
        }
    }

    /// Install a storage codec. Records read from segments will be
    /// decoded via this codec before inner-record parsing. If no codec
    /// is installed (`None`), payloads pass through as-is — equivalent
    /// to the `IdentityCodec`.
    ///
    /// Codec decode failures surface as
    /// [`WalReaderError::CodecDecode`] and NEVER demote to
    /// `ReadStopReason::PartialRecord`. Lossy mode does not scan
    /// forward past a codec decode failure (T3-E12 §D5) — the
    /// operator's escape hatch is `allow_lossy_recovery=true` at open
    /// time, which triggers the whole-DB wipe-and-reopen via T3-E10.
    pub fn with_codec(mut self, codec: Box<dyn StorageCodec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Decode one record payload through the installed codec, or pass
    /// through unchanged if no codec is installed. Returns
    /// `Err(WalReaderError::CodecDecode)` on decode failure, with the
    /// caller's offset encoded in the error so operators can locate
    /// the bad record.
    fn decode_payload<'a>(
        &self,
        payload: &'a [u8],
        offset_hint: u64,
    ) -> Result<Cow<'a, [u8]>, WalReaderError> {
        match self.codec.as_deref() {
            None => Ok(Cow::Borrowed(payload)),
            Some(codec) => {
                codec
                    .decode(payload)
                    .map(Cow::Owned)
                    .map_err(|err| WalReaderError::CodecDecode {
                        offset: offset_hint,
                        detail: err.to_string(),
                    })
            }
        }
    }

    /// Enable lossy recovery mode.
    ///
    /// When enabled, the reader scans forward past corrupted regions to find
    /// the next valid record instead of returning an error. This may silently
    /// lose committed records in the corrupted region.
    pub fn with_lossy_recovery(mut self) -> Self {
        self.allow_lossy_recovery = true;
        self
    }

    /// Read all records from a single segment.
    ///
    /// Returns records in order, stopping at the first invalid/incomplete record.
    /// The returned position indicates where valid records end (for truncation).
    pub fn read_segment(
        &self,
        wal_dir: &Path,
        segment_number: u64,
    ) -> Result<(Vec<WalRecord>, u64, ReadStopReason, usize), WalReaderError> {
        let mut segment = WalSegment::open_read(wal_dir, segment_number)?;

        self.read_segment_from(&mut segment)
    }

    /// Read records from an already-opened segment.
    ///
    /// # v3 parse taxonomy (T3-E12 §D4)
    ///
    /// Each record on disk is wrapped in `[u32 outer_len][u32 outer_len_crc][encoded payload]`.
    /// The parse loop handles the envelope + payload in two steps:
    ///
    /// 1. Read 8-byte outer envelope. Short read → `PartialRecord`
    ///    (genuine crash tail, truncate). CRC mismatch on the length
    ///    field → `CorruptedSegment` (strict) or scan-forward (lossy).
    /// 2. Read `outer_len` payload bytes. Short read → `PartialRecord`.
    ///    Phase 3 will decode via the installed `StorageCodec` here
    ///    (codec-decode failures return `Err(CodecDecode)`, NEVER
    ///    demote to `PartialRecord`). For the identity-codec slot
    ///    this is a pass-through.
    /// 3. Feed the decoded bytes to `WalRecord::from_bytes` — existing
    ///    inner-record parsing (LenCRC + payload CRC) runs unchanged.
    pub fn read_segment_from(
        &self,
        segment: &mut WalSegment,
    ) -> Result<(Vec<WalRecord>, u64, ReadStopReason, usize), WalReaderError> {
        let mut records = Vec::new();
        let mut buffer = Vec::new();
        let hdr_size = segment.header_size() as u64;
        let mut valid_end = hdr_size;

        // Seek to start of records (past header)
        segment
            .seek_to(hdr_size)
            .map_err(|e: std::io::Error| WalReaderError::IoError(e.to_string()))?;

        // Read entire segment content after header
        segment
            .file_mut()
            .read_to_end(&mut buffer)
            .map_err(|e: std::io::Error| WalReaderError::IoError(e.to_string()))?;

        let mut offset = 0;
        let mut stop_reason = ReadStopReason::EndOfData;
        let mut skipped_corrupted = 0usize;

        while offset < buffer.len() {
            // Step 1: parse the outer envelope header.
            let outer_len = match read_outer_envelope(&buffer[offset..]) {
                Ok(Some(len)) => len,
                Ok(None) => {
                    // Short read of envelope header → genuine crash tail.
                    stop_reason = ReadStopReason::PartialRecord;
                    break;
                }
                Err(_crc_mismatch) => {
                    // outer_len CRC mismatch is the same class as the
                    // inner `LengthChecksumMismatch`: strict fails,
                    // lossy scans forward byte-by-byte.
                    if !self.allow_lossy_recovery {
                        return Err(WalReaderError::CorruptedSegment {
                            offset,
                            records_before: records.len(),
                        });
                    }
                    match self.scan_forward_for_envelope(&buffer, offset) {
                        Some(new_offset) => {
                            tracing::warn!(
                                target: "strata::recovery",
                                corrupted_offset = offset,
                                resumed_offset = new_offset,
                                skipped_bytes = new_offset - offset,
                                "Skipped corrupted WAL region, found next valid envelope"
                            );
                            offset = new_offset;
                            skipped_corrupted += 1;
                            continue;
                        }
                        None => {
                            stop_reason = ReadStopReason::ChecksumMismatch { offset };
                            break;
                        }
                    }
                }
            };

            // Step 2: read the outer_len payload bytes.
            let payload_start = offset + WAL_RECORD_ENVELOPE_OVERHEAD;
            let payload_end = match payload_start.checked_add(outer_len as usize) {
                Some(end) if end <= buffer.len() => end,
                _ => {
                    // Short read on payload → crash tail.
                    stop_reason = ReadStopReason::PartialRecord;
                    break;
                }
            };
            let payload = &buffer[payload_start..payload_end];

            // Step 3: decode via the installed codec (or pass through
            // for identity). Codec errors surface as
            // `WalReaderError::CodecDecode` with NO demotion to
            // `PartialRecord` — even at the segment tail (T3-E12 §D5).
            let decoded = self.decode_payload(payload, offset as u64)?;
            let raw_record_bytes: &[u8] = &decoded;

            match WalRecord::from_bytes(raw_record_bytes) {
                Ok((record, _consumed)) => {
                    records.push(record);
                    offset = payload_end;
                    valid_end = hdr_size + offset as u64;
                }
                Err(WalRecordError::InsufficientData) => {
                    // Inner-record short read. This is anomalous — the
                    // outer_len said N bytes follow, so if we couldn't
                    // parse a full record in N bytes either the writer
                    // wrote a malformed record or the payload is
                    // corrupted. Treat as corruption, not partial tail,
                    // so mid-segment writes don't get silently dropped.
                    if !self.allow_lossy_recovery {
                        return Err(WalReaderError::CorruptedSegment {
                            offset,
                            records_before: records.len(),
                        });
                    }
                    match self.scan_forward_for_envelope(&buffer, offset + 1) {
                        Some(new_offset) => {
                            offset = new_offset;
                            skipped_corrupted += 1;
                            continue;
                        }
                        None => {
                            stop_reason = ReadStopReason::ChecksumMismatch { offset };
                            break;
                        }
                    }
                }
                Err(
                    WalRecordError::ChecksumMismatch { .. }
                    | WalRecordError::LengthChecksumMismatch
                    | WalRecordError::UnsupportedVersion(_),
                ) => {
                    if !self.allow_lossy_recovery {
                        return Err(WalReaderError::CorruptedSegment {
                            offset,
                            records_before: records.len(),
                        });
                    }
                    match self.scan_forward_for_envelope(&buffer, offset + 1) {
                        Some(new_offset) => {
                            tracing::warn!(
                                target: "strata::recovery",
                                corrupted_offset = offset,
                                resumed_offset = new_offset,
                                skipped_bytes = new_offset - offset,
                                "Skipped corrupted WAL record, found next valid envelope"
                            );
                            offset = new_offset;
                            skipped_corrupted += 1;
                            continue;
                        }
                        None => {
                            stop_reason = ReadStopReason::ChecksumMismatch { offset };
                            break;
                        }
                    }
                }
                Err(e) => {
                    // CRC passed (outer + inner length) but payload didn't
                    // parse — codec/version mismatch or bug.
                    stop_reason = ReadStopReason::ParseError {
                        offset,
                        detail: e.to_string(),
                    };
                    break;
                }
            }
        }

        Ok((records, valid_end, stop_reason, skipped_corrupted))
    }

    /// Scan forward within `buffer` looking for the next offset at which
    /// a valid outer envelope header parses AND whose codec-decoded
    /// payload parses as a valid `WalRecord`. Used by lossy recovery to
    /// resume reading past a corrupted region. Returns the byte offset
    /// of the next readable envelope, or `None` if none is found within
    /// the scan window.
    ///
    /// Codec-awareness matters: on encrypted WAL (`aes-gcm-256`) the
    /// payload bytes between the envelope header and the next envelope
    /// are ciphertext. Without routing candidate payloads through
    /// `decode_payload` first, the inner-record sanity check would
    /// always fail on encrypted databases and lossy scan-forward would
    /// never resume past corruption.
    fn scan_forward_for_envelope(&self, buffer: &[u8], scan_start: usize) -> Option<usize> {
        let scan_end = scan_start
            .saturating_add(MAX_RECOVERY_SCAN_WINDOW)
            .min(buffer.len());
        for candidate in scan_start..scan_end.saturating_sub(WAL_RECORD_ENVELOPE_OVERHEAD - 1) {
            if let Ok(Some(outer_len)) = read_outer_envelope(&buffer[candidate..]) {
                // Additionally require that the claimed payload actually
                // fits — rejects lucky CRC matches on garbage that would
                // have been truncated.
                let payload_end = candidate
                    .checked_add(WAL_RECORD_ENVELOPE_OVERHEAD)
                    .and_then(|p| p.checked_add(outer_len as usize));
                match payload_end {
                    Some(end) if end <= buffer.len() => {
                        let payload = &buffer[candidate + WAL_RECORD_ENVELOPE_OVERHEAD..end];
                        // Decode through the installed codec first
                        // (identity = pass-through). If decode fails,
                        // this candidate cannot be the start of a valid
                        // record — keep scanning.
                        if let Ok(decoded) = self.decode_payload(payload, candidate as u64) {
                            // Sanity-check: the decoded payload must
                            // parse as a valid WalRecord. Without this,
                            // a random 4-byte value could match its own
                            // CRC by luck once in ~4 billion attempts
                            // and we would resume at garbage.
                            if WalRecord::from_bytes(&decoded).is_ok() {
                                return Some(candidate);
                            }
                        }
                    }
                    _ => continue,
                }
            }
        }
        None
    }

    /// Read all records from all segments in a WAL directory.
    ///
    /// Segments are read in order. Returns all valid records and information
    /// about any truncation needed.
    pub fn read_all(&self, wal_dir: &Path) -> Result<WalReadResult, WalReaderError> {
        let mut segments = self.list_segments(wal_dir)?;
        segments.sort();

        let mut all_records = Vec::new();
        let mut truncate_info = None;
        let mut last_stop_reason = ReadStopReason::EndOfData;
        let mut total_skipped_corrupted = 0usize;

        for (idx, segment_num) in segments.iter().enumerate() {
            let (records, valid_end, stop_reason, skipped) =
                self.read_segment(wal_dir, *segment_num)?;
            all_records.extend(records);
            last_stop_reason = stop_reason;
            total_skipped_corrupted += skipped;

            // Check if this segment needs truncation (only the last one can)
            if idx == segments.len() - 1 {
                let segment = WalSegment::open_read(wal_dir, *segment_num)?;

                if valid_end < segment.size() {
                    truncate_info = Some(TruncateInfo {
                        segment_number: *segment_num,
                        valid_end,
                        original_size: segment.size(),
                    });
                }
            }
        }

        Ok(WalReadResult {
            records: all_records,
            truncate_info,
            stop_reason: last_stop_reason,
            skipped_corrupted: total_skipped_corrupted,
        })
    }

    /// Read records from a segment, filtering by transaction ID.
    ///
    /// Only returns records with txn_id > watermark (for recovery after snapshot).
    #[cfg(test)]
    pub fn read_segment_after_watermark(
        &self,
        wal_dir: &Path,
        segment_number: u64,
        watermark: u64,
    ) -> Result<Vec<WalRecord>, WalReaderError> {
        let (records, _, _, _) = self.read_segment(wal_dir, segment_number)?;

        Ok(records
            .into_iter()
            .filter(|r| r.txn_id.as_u64() > watermark)
            .collect())
    }

    /// Read all records after a watermark from all segments.
    ///
    /// Uses segment metadata (`.meta` sidecars) to skip entire closed segments
    /// whose `max_txn_id <= watermark`. The active (latest) segment is always
    /// read since it has no `.meta` file. If a `.meta` file is missing for a
    /// closed segment, that segment is read in full as a fallback.
    pub fn read_all_after_watermark(
        &self,
        wal_dir: &Path,
        watermark: u64,
    ) -> Result<Vec<WalRecord>, WalReaderError> {
        let mut segments = self.list_segments(wal_dir)?;
        segments.sort();

        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let latest_segment = *segments.last().unwrap();
        let mut result = Vec::new();

        for &segment_number in &segments {
            if segment_number != latest_segment {
                // Closed segment — check .meta to see if we can skip it
                match SegmentMeta::read_from_file(wal_dir, segment_number) {
                    Ok(Some(meta))
                        if meta.segment_number == segment_number
                            && meta.max_txn_id.as_u64() <= watermark =>
                    {
                        // All records in this segment are at or below the watermark — skip
                        continue;
                    }
                    Ok(Some(meta)) if meta.segment_number != segment_number => {
                        warn!(
                            target: "strata::wal",
                            segment = segment_number,
                            meta_segment = meta.segment_number,
                            "Segment meta has mismatched segment number, reading full segment"
                        );
                        // Fall through to read the segment
                    }
                    Err(e) => {
                        warn!(
                            target: "strata::wal",
                            segment = segment_number,
                            error = %e,
                            "Could not read .meta sidecar, reading full segment"
                        );
                        // Fall through to read the segment
                    }
                    _ => {
                        // meta.max_txn_id > watermark, or meta missing — read segment
                    }
                }
            }
            // Active segment (no .meta) or closed segment that may have records above watermark
            let (records, _, _, _) = self.read_segment(wal_dir, segment_number)?;
            result.extend(
                records
                    .into_iter()
                    .filter(|r| r.txn_id.as_u64() > watermark),
            );
        }

        Ok(result)
    }

    /// Read the longest contiguous WAL prefix after `watermark`.
    ///
    /// Unlike [`read_all_after_watermark`], this preserves valid records before the
    /// first unreadable or missing transaction and reports the first blocked txn id.
    pub fn read_all_after_watermark_contiguous(
        &self,
        wal_dir: &Path,
        watermark: u64,
    ) -> Result<WatermarkReadResult, WalReaderError> {
        let mut segments = self.list_segments(wal_dir)?;
        segments.sort();

        if segments.is_empty() {
            return Ok(WatermarkReadResult {
                records: Vec::new(),
                blocked: None,
            });
        }

        let latest_segment = *segments.last().unwrap();
        let mut records = Vec::new();
        let mut next_expected = watermark.saturating_add(1);

        for &segment_number in &segments {
            if segment_number != latest_segment {
                match SegmentMeta::read_from_file(wal_dir, segment_number) {
                    Ok(Some(meta))
                        if meta.segment_number == segment_number
                            && meta.max_txn_id.as_u64() < next_expected =>
                    {
                        continue;
                    }
                    Ok(Some(meta)) if meta.segment_number != segment_number => {
                        warn!(
                            target: "strata::wal",
                            segment = segment_number,
                            meta_segment = meta.segment_number,
                            "Segment meta has mismatched segment number, reading full segment"
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "strata::wal",
                            segment = segment_number,
                            error = %e,
                            "Could not read .meta sidecar, reading full segment"
                        );
                    }
                    _ => {}
                }
            }

            let segment_result = self.read_segment_after_watermark_contiguous(
                wal_dir,
                segment_number,
                next_expected,
            )?;

            if let Some(last) = segment_result.records.last() {
                next_expected = last.txn_id.as_u64().saturating_add(1);
            }
            records.extend(segment_result.records);

            if let Some(blocked) = segment_result.blocked {
                return Ok(WatermarkReadResult {
                    records,
                    blocked: Some(blocked),
                });
            }
        }

        Ok(WatermarkReadResult {
            records,
            blocked: None,
        })
    }

    /// List all segment numbers in the WAL directory.
    pub fn list_segments(&self, wal_dir: &Path) -> Result<Vec<u64>, WalReaderError> {
        let mut segments = Vec::new();

        let entries =
            std::fs::read_dir(wal_dir).map_err(|e| WalReaderError::IoError(e.to_string()))?;

        for entry in entries {
            let entry = entry.map_err(|e| WalReaderError::IoError(e.to_string()))?;
            let name = entry.file_name().to_string_lossy().to_string();

            if let Some(num) = super::parse_segment_number(&name) {
                segments.push(num);
            }
        }

        segments.sort();
        Ok(segments)
    }

    fn read_segment_after_watermark_contiguous(
        &self,
        wal_dir: &Path,
        segment_number: u64,
        next_expected: u64,
    ) -> Result<WatermarkReadResult, WalReaderError> {
        let mut segment = WalSegment::open_read(wal_dir, segment_number)?;
        let mut buffer = Vec::new();
        let hdr_size = segment.header_size() as u64;

        segment
            .seek_to(hdr_size)
            .map_err(|e: std::io::Error| WalReaderError::IoError(e.to_string()))?;
        segment
            .file_mut()
            .read_to_end(&mut buffer)
            .map_err(|e: std::io::Error| WalReaderError::IoError(e.to_string()))?;

        let mut records = Vec::new();
        let mut offset = 0usize;
        let mut next_expected = next_expected;

        while offset < buffer.len() {
            // Parse the per-record outer envelope (T3-E12 v3) before
            // delegating to the inner `WalRecord::from_bytes`.
            let outer_len = match read_outer_envelope(&buffer[offset..]) {
                Ok(Some(len)) => len,
                Ok(None) => break, // envelope header short-read = crash tail
                Err(_crc_mismatch) => {
                    if let Some((scan_offset, next_record)) =
                        self.scan_forward_to_valid_envelope(&buffer, offset + 1)
                    {
                        let candidate_txn = next_record.txn_id.as_u64();
                        if candidate_txn <= next_expected {
                            offset = scan_offset;
                            continue;
                        }
                        return Ok(WatermarkReadResult {
                            records,
                            blocked: Some(WatermarkBlockedRecord {
                                txn_id: TxnId(next_expected),
                                detail: format!(
                                    "outer envelope CRC mismatch at offset {offset}; next readable txn is {next}",
                                    next = next_record.txn_id,
                                ),
                                skip_allowed: true,
                                stop_reason: ReadStopReason::ChecksumMismatch { offset },
                            }),
                        });
                    }
                    return Ok(WatermarkReadResult {
                        records,
                        blocked: Some(WatermarkBlockedRecord {
                            txn_id: TxnId(next_expected),
                            detail: format!("outer envelope CRC mismatch at offset {offset}"),
                            skip_allowed: false,
                            stop_reason: ReadStopReason::ChecksumMismatch { offset },
                        }),
                    });
                }
            };

            let payload_start = offset + WAL_RECORD_ENVELOPE_OVERHEAD;
            let payload_end = match payload_start.checked_add(outer_len as usize) {
                Some(end) if end <= buffer.len() => end,
                _ => break, // payload short-read = crash tail
            };
            let payload = &buffer[payload_start..payload_end];

            // Decode via installed codec or pass through for identity.
            // Preserve the already-read prefix on codec-decode failure
            // (D2 / DG-002). If scan-forward finds only stale records at or
            // below `next_expected`, skip past them just like the
            // checksum/parse paths; otherwise surface the first missing txn as
            // blocked and include the next readable anchor when one exists.
            let decoded = match self.decode_payload(payload, offset as u64) {
                Ok(d) => d,
                Err(WalReaderError::CodecDecode {
                    offset: fail_offset,
                    detail,
                }) => {
                    if let Some((scan_offset, next_record)) =
                        self.scan_forward_to_valid_envelope(&buffer, offset + 1)
                    {
                        let candidate_txn = next_record.txn_id.as_u64();
                        if candidate_txn <= next_expected {
                            offset = scan_offset;
                            continue;
                        }
                        return Ok(WatermarkReadResult {
                            records,
                            blocked: Some(WatermarkBlockedRecord {
                                txn_id: TxnId(next_expected),
                                detail: format!(
                                    "codec decode failed: {detail}; next readable txn is {next}",
                                    next = next_record.txn_id,
                                ),
                                skip_allowed: true,
                                stop_reason: ReadStopReason::CodecDecode {
                                    offset: fail_offset as usize,
                                    detail,
                                },
                            }),
                        });
                    }
                    return Ok(WatermarkReadResult {
                        records,
                        blocked: Some(WatermarkBlockedRecord {
                            txn_id: TxnId(next_expected),
                            detail: format!("codec decode failed: {detail}"),
                            skip_allowed: false,
                            stop_reason: ReadStopReason::CodecDecode {
                                offset: fail_offset as usize,
                                detail,
                            },
                        }),
                    });
                }
                Err(e) => return Err(e),
            };

            match WalRecord::from_bytes(&decoded) {
                Ok((record, _consumed)) => {
                    offset = payload_end;
                    let txn_id = record.txn_id.as_u64();
                    if txn_id < next_expected {
                        continue;
                    }
                    if txn_id == next_expected {
                        next_expected = next_expected.saturating_add(1);
                        records.push(record);
                        continue;
                    }

                    return Ok(WatermarkReadResult {
                        records,
                        blocked: Some(WatermarkBlockedRecord {
                            txn_id: TxnId(next_expected),
                            detail: format!(
                                "non-contiguous WAL: expected txn {}, found txn {}",
                                next_expected, txn_id
                            ),
                            skip_allowed: true,
                            stop_reason: ReadStopReason::Gap {
                                expected_txn_id: TxnId(next_expected),
                                observed_txn_id: record.txn_id,
                            },
                        }),
                    });
                }
                Err(err) => {
                    if let Some((scan_offset, next_record)) =
                        self.scan_forward_to_valid_envelope(&buffer, offset + 1)
                    {
                        let candidate_txn = next_record.txn_id.as_u64();
                        if candidate_txn <= next_expected {
                            offset = scan_offset;
                            continue;
                        }

                        return Ok(WatermarkReadResult {
                            records,
                            blocked: Some(WatermarkBlockedRecord {
                                txn_id: TxnId(next_expected),
                                detail: format!(
                                    "{err}; next readable txn is {next}",
                                    next = next_record.txn_id
                                ),
                                skip_allowed: true,
                                stop_reason: Self::stop_reason_for_record_error(offset, &err),
                            }),
                        });
                    }

                    return Ok(WatermarkReadResult {
                        records,
                        blocked: Some(WatermarkBlockedRecord {
                            txn_id: TxnId(next_expected),
                            detail: err.to_string(),
                            skip_allowed: false,
                            stop_reason: Self::stop_reason_for_record_error(offset, &err),
                        }),
                    });
                }
            }
        }

        Ok(WatermarkReadResult {
            records,
            blocked: None,
        })
    }

    /// Scan forward for the next byte offset at which a valid v3 outer
    /// envelope parses AND whose codec-decoded payload parses as a
    /// valid `WalRecord`. Returns the scan offset and the decoded
    /// record. Used by follower-refresh lossy-scan paths.
    ///
    /// Codec-awareness matters for the same reason as
    /// [`scan_forward_for_envelope`]: encrypted WAL payloads must be
    /// decoded before inner-record validation, otherwise follower
    /// exact-skip resume would fail every lookup on encrypted DBs.
    fn scan_forward_to_valid_envelope(
        &self,
        buffer: &[u8],
        scan_start: usize,
    ) -> Option<(usize, WalRecord)> {
        let scan_end = scan_start
            .saturating_add(MAX_RECOVERY_SCAN_WINDOW)
            .min(buffer.len());
        for candidate in scan_start..scan_end.saturating_sub(WAL_RECORD_ENVELOPE_OVERHEAD - 1) {
            if let Ok(Some(outer_len)) = read_outer_envelope(&buffer[candidate..]) {
                let payload_end = candidate
                    .checked_add(WAL_RECORD_ENVELOPE_OVERHEAD)
                    .and_then(|p| p.checked_add(outer_len as usize));
                if let Some(end) = payload_end {
                    if end <= buffer.len() {
                        let payload = &buffer[candidate + WAL_RECORD_ENVELOPE_OVERHEAD..end];
                        if let Ok(decoded) = self.decode_payload(payload, candidate as u64) {
                            if let Ok((record, _)) = WalRecord::from_bytes(&decoded) {
                                return Some((candidate, record));
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn stop_reason_for_record_error(offset: usize, err: &WalRecordError) -> ReadStopReason {
        match err {
            WalRecordError::ChecksumMismatch { .. } | WalRecordError::LengthChecksumMismatch => {
                ReadStopReason::ChecksumMismatch { offset }
            }
            other => ReadStopReason::ParseError {
                offset,
                detail: other.to_string(),
            },
        }
    }

    /// Get the highest transaction ID in a segment.
    #[cfg(test)]
    pub fn max_txn_id_in_segment(
        &self,
        wal_dir: &Path,
        segment_number: u64,
    ) -> Result<Option<u64>, WalReaderError> {
        let (records, _, _, _) = self.read_segment(wal_dir, segment_number)?;
        Ok(records.iter().map(|r| r.txn_id.as_u64()).max())
    }

    /// Get the highest transaction ID across all segments.
    pub fn max_txn_id(&self, wal_dir: &Path) -> Result<Option<u64>, WalReaderError> {
        let result = self.read_all(wal_dir)?;
        Ok(result.records.iter().map(|r| r.txn_id.as_u64()).max())
    }

    /// List all segments with their metadata sidecars.
    ///
    /// For each segment, attempts to load its `.meta` file. Missing or corrupted
    /// metadata is returned as `None` (with a warning logged).
    #[cfg(test)]
    fn list_segments_with_metadata(
        &self,
        wal_dir: &Path,
    ) -> Result<Vec<(u64, Option<SegmentMeta>)>, WalReaderError> {
        let segments = self.list_segments(wal_dir)?;
        let mut result = Vec::with_capacity(segments.len());

        for seg_num in segments {
            match SegmentMeta::read_from_file(wal_dir, seg_num) {
                Ok(Some(meta)) => {
                    if meta.segment_number != seg_num {
                        warn!(
                            target: "strata::wal",
                            segment = seg_num,
                            meta_segment = meta.segment_number,
                            "Segment meta has mismatched segment number, ignoring"
                        );
                        result.push((seg_num, None));
                    } else {
                        result.push((seg_num, Some(meta)));
                    }
                }
                Ok(None) => {
                    result.push((seg_num, None));
                }
                Err(e) => {
                    warn!(
                        target: "strata::wal",
                        segment = seg_num,
                        error = %e,
                        "Corrupted .meta sidecar, ignoring"
                    );
                    result.push((seg_num, None));
                }
            }
        }

        Ok(result)
    }

    /// Find segments that may contain a record with the given timestamp.
    ///
    /// A segment is included if `min_ts <= target_ts <= max_ts`.
    /// Segments without metadata are conservatively included.
    #[cfg(test)]
    pub fn find_segments_for_timestamp(
        &self,
        wal_dir: &Path,
        target_ts: u64,
    ) -> Result<Vec<u64>, WalReaderError> {
        let segments = self.list_segments_with_metadata(wal_dir)?;
        Ok(segments
            .into_iter()
            .filter(|(_, meta)| match meta {
                Some(m) => m.min_timestamp <= target_ts && target_ts <= m.max_timestamp,
                None => true, // Conservative: include segments without metadata
            })
            .map(|(num, _)| num)
            .collect())
    }

    /// Find segments that overlap with a timestamp range `[min_ts, max_ts]`.
    ///
    /// A segment is included if its range overlaps: `seg.min_ts <= max_ts && seg.max_ts >= min_ts`.
    /// Segments without metadata are conservatively included.
    #[cfg(test)]
    pub fn find_segments_for_timestamp_range(
        &self,
        wal_dir: &Path,
        min_ts: u64,
        max_ts: u64,
    ) -> Result<Vec<u64>, WalReaderError> {
        let segments = self.list_segments_with_metadata(wal_dir)?;
        Ok(segments
            .into_iter()
            .filter(|(_, meta)| match meta {
                Some(m) => m.min_timestamp <= max_ts && m.max_timestamp >= min_ts,
                None => true,
            })
            .map(|(num, _)| num)
            .collect())
    }

    /// Find segments that may contain records at or before `target_ts`.
    ///
    /// A segment is included if `min_ts <= target_ts` (the segment contains at
    /// least one record at or before the target). This is the key API for
    /// time-travel state reconstruction.
    /// Segments without metadata are conservatively included.
    #[cfg(test)]
    pub fn find_segments_before_timestamp(
        &self,
        wal_dir: &Path,
        target_ts: u64,
    ) -> Result<Vec<u64>, WalReaderError> {
        let segments = self.list_segments_with_metadata(wal_dir)?;
        Ok(segments
            .into_iter()
            .filter(|(_, meta)| match meta {
                Some(m) => m.min_timestamp <= target_ts,
                None => true,
            })
            .map(|(num, _)| num)
            .collect())
    }

    /// Lazy iterator over all WAL records across all segments.
    ///
    /// Unlike `read_all()`, this processes one segment at a time, keeping
    /// only the current segment's records in memory. Total memory usage is
    /// O(largest_segment) instead of O(total_wal_size).
    pub fn iter_all(&self, wal_dir: &Path) -> Result<WalRecordIterator, WalReaderError> {
        let mut segments = self.list_segments(wal_dir)?;
        segments.sort();
        Ok(WalRecordIterator {
            wal_dir: wal_dir.to_path_buf(),
            segments,
            current_segment_idx: 0,
            current_records: Vec::new(),
            current_record_idx: 0,
            allow_lossy_recovery: self.allow_lossy_recovery,
            codec: self.codec.as_deref().map(clone_codec),
        })
    }
}

/// Iterator that yields WalRecords one at a time across all WAL segments.
///
/// Loads one segment at a time into memory, yielding records from it before
/// moving to the next segment. This bounds memory to O(largest_segment)
/// instead of O(total_wal_size), preventing OOM on large databases.
pub struct WalRecordIterator {
    wal_dir: PathBuf,
    segments: Vec<u64>,
    current_segment_idx: usize,
    current_records: Vec<WalRecord>,
    current_record_idx: usize,
    allow_lossy_recovery: bool,
    /// Codec installed on the parent `WalReader`; threaded into each
    /// per-segment reader so codec-encoded payloads (AES-GCM, etc.)
    /// decode correctly (T3-E12 §D3 Site 3).
    codec: Option<Box<dyn StorageCodec>>,
}

impl Iterator for WalRecordIterator {
    type Item = Result<WalRecord, WalReaderError>;

    fn next(&mut self) -> Option<Result<WalRecord, WalReaderError>> {
        loop {
            // Yield from current segment's records
            if self.current_record_idx < self.current_records.len() {
                let idx = self.current_record_idx;
                self.current_record_idx += 1;
                return Some(Ok(self.current_records[idx].clone()));
            }

            // Free the previous segment's records before loading the next
            if !self.current_records.is_empty() {
                self.current_records = Vec::new();
            }

            // Move to next segment
            if self.current_segment_idx >= self.segments.len() {
                return None;
            }

            let seg_num = self.segments[self.current_segment_idx];
            self.current_segment_idx += 1;

            let mut reader = WalReader::new();
            reader.allow_lossy_recovery = self.allow_lossy_recovery;
            if let Some(c) = self.codec.as_deref() {
                reader = reader.with_codec(clone_codec(c));
            }
            match reader.read_segment(&self.wal_dir, seg_num) {
                Ok((records, _, _, _)) => {
                    self.current_records = records;
                    self.current_record_idx = 0;
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Reason why record reading stopped before reaching end of segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadStopReason {
    /// Successfully read all records to end of data
    EndOfData,
    /// Partial record at end of segment (expected after crash)
    PartialRecord,
    /// CRC checksum mismatch - data is corrupted
    ChecksumMismatch {
        /// Byte offset within the segment where the mismatch was detected
        offset: usize,
    },
    /// CRC was valid but payload could not be parsed.
    /// This indicates a codec mismatch, unsupported format version,
    /// or a bug in the record format (not data corruption).
    ParseError {
        /// Byte offset within the segment where parsing failed
        offset: usize,
        /// Human-readable error description
        detail: String,
    },
    /// Codec decode failed for a record. Distinct from
    /// [`ReadStopReason::ChecksumMismatch`] because the outer envelope
    /// parsed and its CRC matched — it was the installed
    /// `StorageCodec`'s `decode()` that rejected the payload. Typical
    /// cause: wrong encryption key or AES-GCM auth-tag mismatch on an
    /// encrypted WAL.
    ///
    /// **Staging note (T3-E12 Phase 1):** variant declared here for
    /// follower-refresh blocked-state classification. Phase 2's
    /// codec-aware reader is the first producer; Phase 1 only stages
    /// the enum arm and the `lifecycle.rs` follower-refresh match.
    CodecDecode {
        /// Byte offset within the segment where decode failed.
        offset: usize,
        /// Human-readable decode failure description (typically the
        /// `Display` of `CodecError`).
        detail: String,
    },
    /// A later valid record was found after one or more missing transaction IDs.
    Gap {
        /// First missing transaction ID in the contiguous sequence.
        expected_txn_id: TxnId,
        /// First later transaction ID that was still readable.
        observed_txn_id: TxnId,
    },
}

/// Result of reading all WAL segments.
#[derive(Debug)]
pub struct WalReadResult {
    /// All valid records in order
    pub records: Vec<WalRecord>,

    /// Information about truncation needed (if any)
    pub truncate_info: Option<TruncateInfo>,

    /// Why reading stopped (for diagnostics)
    pub stop_reason: ReadStopReason,

    /// Number of corrupted records that were skipped during reading
    pub skipped_corrupted: usize,
}

/// Information about a segment that needs truncation.
#[derive(Debug, Clone)]
pub struct TruncateInfo {
    /// Segment number
    pub segment_number: u64,

    /// Position where valid data ends
    pub valid_end: u64,

    /// Original file size
    pub original_size: u64,
}

impl TruncateInfo {
    /// Get the number of bytes that need to be truncated.
    pub fn bytes_to_truncate(&self) -> u64 {
        self.original_size - self.valid_end
    }
}

/// Block encountered while reading a contiguous replay prefix.
#[derive(Debug, Clone)]
pub struct WatermarkBlockedRecord {
    /// First transaction ID in the contiguous sequence that could not be proven readable.
    pub txn_id: TxnId,
    /// Diagnostic detail describing the read failure or gap.
    pub detail: String,
    /// Whether an exact operator skip can safely advance past this transaction ID.
    pub skip_allowed: bool,
    /// Why segment reading stopped.
    pub stop_reason: ReadStopReason,
}

/// Records read after a watermark until the first contiguous block, if any.
#[derive(Debug, Clone)]
pub struct WatermarkReadResult {
    /// Contiguous readable records after the requested watermark.
    pub records: Vec<WalRecord>,
    /// First blocked txn, if contiguous replay could not continue.
    pub blocked: Option<WatermarkBlockedRecord>,
}

/// WAL reader errors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WalReaderError {
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(String),

    /// Segment not found
    #[error("Segment not found: {0}")]
    SegmentNotFound(u64),

    /// Record parsing error
    #[error("Record parsing error: {0}")]
    ParseError(String),

    /// Mid-segment corruption detected (checksum mismatch).
    ///
    /// Returned in strict mode (default) when a corrupted record is found.
    /// Use `WalReader::with_lossy_recovery()` to skip past corruption instead.
    #[error("Corrupted WAL segment at byte offset {offset} ({records_before} valid records before corruption)")]
    CorruptedSegment {
        /// Byte offset within the record data where corruption was detected
        offset: usize,
        /// Number of valid records read before the corruption
        records_before: usize,
    },

    /// Codec decode failure at a specific record offset.
    ///
    /// **Staging note (T3-E12 Phase 1):** this variant is declared
    /// for Phase 1's cross-crate scaffolding. No production code
    /// path in `WalReader` constructs it yet. Phase 2 will wire the
    /// codec-aware reader; at that point strict callers will see
    /// this error directly, and the engine's lossy open branch will
    /// catch it and route to the whole-DB wipe-and-reopen path
    /// (T3-E10) with typed `LossyErrorKind::CodecDecode`. Lossy
    /// mode will NOT scan forward past a codec-decode failure —
    /// byte-aligned codec scan is unreliable for encrypted payloads
    /// (see T3-E12 tracking doc §D5 for rationale).
    #[error("Codec decode failure at byte offset {offset}: {detail}")]
    CodecDecode {
        /// Byte offset within the segment where decode failed.
        offset: u64,
        /// Decode error detail (typically the `Display` of `CodecError`).
        detail: String,
    },

    /// Legacy WAL segment format — rejected hard, even under lossy
    /// recovery.
    ///
    /// Produced when a segment's `SEGMENT_FORMAT_VERSION` is older than
    /// this build supports. Surfaces unconditionally (strict and lossy
    /// alike); the engine's open path re-raises as
    /// [`strata_core::StrataError::LegacyFormat`] and the operator
    /// must delete the `wal/` subdirectory manually before reopening.
    /// Lossy recovery does not bypass format incompatibility (T3-E12 §D6).
    ///
    /// The `hint` carries the full operator-facing message including
    /// the required version number, so the rendered diagnostic stays
    /// stable across future `SEGMENT_FORMAT_VERSION` bumps without a
    /// separate `required_version` struct field.
    #[error("Legacy WAL segment format: found version {found_version}. {hint}")]
    LegacyFormat {
        /// Segment format version read from disk.
        found_version: u32,
        /// Operator remediation hint — filesystem action only. The
        /// hint is expected to include the required version number.
        hint: String,
    },
}

impl From<crate::durability::format::WalSegmentError> for WalReaderError {
    /// Preserve the typed `LegacyFormat` diagnostic; render other
    /// header-level errors as `IoError`-wrapped strings. Genuine I/O
    /// errors (disk full, permission denied) go through `IoError`
    /// unchanged. T3-E12 §D8.
    fn from(err: crate::durability::format::WalSegmentError) -> Self {
        use crate::durability::format::{SegmentHeaderError, WalSegmentError};
        match err {
            WalSegmentError::Io(io) => WalReaderError::IoError(io.to_string()),
            WalSegmentError::Header(SegmentHeaderError::LegacyFormat {
                found_version,
                hint,
            }) => WalReaderError::LegacyFormat {
                found_version,
                hint,
            },
            WalSegmentError::Header(other) => WalReaderError::IoError(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::{IdentityCodec, StorageCodec};
    use crate::durability::wal::config::WalConfig;
    use crate::durability::wal::writer::WalWriter;
    use crate::durability::wal::DurabilityMode;
    use strata_core::id::TxnId;
    use tempfile::tempdir;

    fn make_codec() -> Box<dyn StorageCodec> {
        Box::new(IdentityCodec)
    }

    fn write_records(wal_dir: &Path, records: &[WalRecord]) {
        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            make_codec(),
        )
        .unwrap();

        for record in records {
            writer.append(record).unwrap();
        }

        writer.flush().unwrap();
    }

    #[test]
    fn test_read_empty_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Create empty segment
        std::fs::create_dir_all(&wal_dir).unwrap();
        WalSegment::create(&wal_dir, 1, [1u8; 16]).unwrap();

        let reader = WalReader::new();
        let (records, _, _, _) = reader.read_segment(&wal_dir, 1).unwrap();

        assert!(records.is_empty());
    }

    #[test]
    fn test_read_single_record() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let record = WalRecord::new(TxnId(1), [1u8; 16], 12345, vec![1, 2, 3]);
        write_records(&wal_dir, std::slice::from_ref(&record));

        let reader = WalReader::new();
        let (records, _, _, _) = reader.read_segment(&wal_dir, 1).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].txn_id, TxnId(1));
        assert_eq!(records[0].writeset, vec![1, 2, 3]);
    }

    #[test]
    fn test_read_multiple_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 10]))
            .collect();

        write_records(&wal_dir, &records);

        let reader = WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();

        assert_eq!(result.records.len(), 5);
        for (i, record) in result.records.iter().enumerate() {
            assert_eq!(record.txn_id, TxnId((i + 1) as u64));
        }
    }

    #[test]
    fn test_read_after_watermark() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let reader = WalReader::new();
        let filtered = reader.read_all_after_watermark(&wal_dir, 5).unwrap();

        assert_eq!(filtered.len(), 5);
        assert!(filtered.iter().all(|r| r.txn_id.as_u64() > 5));
    }

    #[test]
    fn test_list_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create multiple segments
        WalSegment::create(&wal_dir, 1, [1u8; 16]).unwrap();
        WalSegment::create(&wal_dir, 2, [1u8; 16]).unwrap();
        WalSegment::create(&wal_dir, 3, [1u8; 16]).unwrap();

        let reader = WalReader::new();
        let segments = reader.list_segments(&wal_dir).unwrap();

        assert_eq!(segments, vec![1, 2, 3]);
    }

    #[test]
    fn test_max_txn_id() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], 0, vec![]))
            .collect();

        write_records(&wal_dir, &records);

        let reader = WalReader::new();
        let max = reader.max_txn_id(&wal_dir).unwrap();

        assert_eq!(max, Some(10));
    }

    #[test]
    fn test_partial_record_detection() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write valid records
        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], 0, vec![i as u8]))
            .collect();
        write_records(&wal_dir, &records);

        // Append garbage to simulate crash mid-write
        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 10]).unwrap(); // Garbage bytes

        let reader = WalReader::new();
        let result = reader.read_all(&wal_dir).unwrap();

        // Should still read valid records
        assert_eq!(result.records.len(), 3);

        // Should report truncation needed
        assert!(result.truncate_info.is_some());
        let truncate = result.truncate_info.unwrap();
        assert_eq!(truncate.bytes_to_truncate(), 10);
    }

    // ---- Metadata query tests ----

    /// Helper: write records using WalWriter (which emits .meta on close)
    fn write_records_with_close(wal_dir: &Path, records: &[WalRecord]) {
        let writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            WalConfig::for_testing(),
            make_codec(),
        )
        .unwrap();

        let mut writer = writer;
        for record in records {
            writer.append(record).unwrap();
        }

        writer.close().unwrap();
    }

    /// Helper: create 3 segments directly with known timestamp ranges and .meta files.
    ///
    /// Segment 1: txn_ids 1-2, timestamps 1000-2000
    /// Segment 2: txn_ids 3-4, timestamps 3000-4000
    /// Segment 3: txn_ids 5-6, timestamps 5000-6000
    fn create_segments_with_meta(wal_dir: &Path) {
        std::fs::create_dir_all(wal_dir).unwrap();

        for (seg_num, txn_ids, timestamps) in [
            (1u64, [1u64, 2], [1000u64, 2000]),
            (2, [3, 4], [3000, 4000]),
            (3, [5, 6], [5000, 6000]),
        ] {
            let mut segment = WalSegment::create(wal_dir, seg_num, [1u8; 16]).unwrap();
            let mut meta = SegmentMeta::new_empty(seg_num);
            for (&txn_id, &ts) in txn_ids.iter().zip(timestamps.iter()) {
                let record = WalRecord::new(TxnId(txn_id), [1u8; 16], ts, vec![txn_id as u8; 10]);
                write_enveloped_record(&mut segment, &record.to_bytes());
                meta.track_record(TxnId(txn_id), ts);
            }
            segment.close().unwrap();
            meta.write_to_file(wal_dir).unwrap();
        }
    }

    #[test]
    fn test_meta_written_on_close() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8]))
            .collect();
        write_records_with_close(&wal_dir, &records);

        // .meta should exist for segment 1
        let meta = SegmentMeta::read_from_file(&wal_dir, 1)
            .unwrap()
            .expect(".meta should exist");
        assert_eq!(meta.segment_number, 1);
        assert_eq!(meta.min_txn_id, TxnId(1));
        assert_eq!(meta.max_txn_id, TxnId(3));
        assert_eq!(meta.min_timestamp, 1000);
        assert_eq!(meta.max_timestamp, 3000);
        assert_eq!(meta.record_count, 3);
    }

    #[test]
    fn test_meta_written_on_rotation() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use tiny segment size to force rotation
        let config = crate::durability::wal::config::WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            make_codec(),
        )
        .unwrap();

        // Write enough records to force at least one rotation
        writer
            .append(&WalRecord::new(TxnId(1), [1u8; 16], 1000, vec![0; 50]))
            .unwrap();
        writer
            .append(&WalRecord::new(TxnId(2), [1u8; 16], 2000, vec![0; 50]))
            .unwrap();
        writer
            .append(&WalRecord::new(TxnId(3), [1u8; 16], 3000, vec![0; 50]))
            .unwrap();

        writer.close().unwrap();

        // There should be multiple segments with .meta files
        let reader = WalReader::new();
        let segments_with_meta = reader.list_segments_with_metadata(&wal_dir).unwrap();

        // Multiple segments should exist (rotation happened)
        assert!(segments_with_meta.len() > 1, "Should have rotated");

        // At least some segments should have metadata
        let with_meta_count = segments_with_meta
            .iter()
            .filter(|(_, m)| m.is_some())
            .count();
        assert!(
            with_meta_count > 0,
            "At least one segment should have .meta"
        );
    }

    #[test]
    fn test_list_segments_with_metadata_no_meta() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create segments without .meta files (legacy scenario)
        WalSegment::create(&wal_dir, 1, [1u8; 16]).unwrap();
        WalSegment::create(&wal_dir, 2, [1u8; 16]).unwrap();

        let reader = WalReader::new();
        let result = reader.list_segments_with_metadata(&wal_dir).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0].1.is_none());
        assert!(result[1].1.is_none());
    }

    #[test]
    fn test_find_segments_for_timestamp() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Create 3 segments with known ranges:
        //   seg 1: ts 1000-2000, seg 2: ts 3000-4000, seg 3: ts 5000-6000
        create_segments_with_meta(&wal_dir);

        let reader = WalReader::new();

        // Timestamp 1500 should match segment 1 (1000 <= 1500 <= 2000)
        let result = reader.find_segments_for_timestamp(&wal_dir, 1500).unwrap();
        assert_eq!(result, vec![1]);

        // Exact boundary: timestamp 2000 matches segment 1
        let result = reader.find_segments_for_timestamp(&wal_dir, 2000).unwrap();
        assert_eq!(result, vec![1]);

        // Exact boundary: timestamp 3000 matches segment 2
        let result = reader.find_segments_for_timestamp(&wal_dir, 3000).unwrap();
        assert_eq!(result, vec![2]);

        // Between segments: timestamp 2500 matches nothing
        let result = reader.find_segments_for_timestamp(&wal_dir, 2500).unwrap();
        assert!(result.is_empty());

        // Far future: matches nothing
        let result = reader
            .find_segments_for_timestamp(&wal_dir, 999_999)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_find_segments_for_timestamp_range() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_meta(&wal_dir);

        let reader = WalReader::new();

        // Range covering all data
        let result = reader
            .find_segments_for_timestamp_range(&wal_dir, 0, 10000)
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);

        // Range covering first two segments
        let result = reader
            .find_segments_for_timestamp_range(&wal_dir, 1500, 3500)
            .unwrap();
        assert_eq!(result, vec![1, 2]);

        // Range between segments: 2500-2900 matches nothing
        let result = reader
            .find_segments_for_timestamp_range(&wal_dir, 2500, 2900)
            .unwrap();
        assert!(result.is_empty());

        // Range touching segment boundary: 2000-3000 matches segments 1 and 2
        let result = reader
            .find_segments_for_timestamp_range(&wal_dir, 2000, 3000)
            .unwrap();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_find_segments_before_timestamp() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_meta(&wal_dir);

        let reader = WalReader::new();

        // Before any timestamp: nothing
        let result = reader.find_segments_before_timestamp(&wal_dir, 0).unwrap();
        assert!(result.is_empty());

        // At first segment's min_ts: includes segment 1
        let result = reader
            .find_segments_before_timestamp(&wal_dir, 1000)
            .unwrap();
        assert_eq!(result, vec![1]);

        // At second segment's min_ts: includes segments 1 and 2
        let result = reader
            .find_segments_before_timestamp(&wal_dir, 3000)
            .unwrap();
        assert_eq!(result, vec![1, 2]);

        // After all timestamps: all segments
        let result = reader
            .find_segments_before_timestamp(&wal_dir, 999_999)
            .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_find_segments_conservative_without_meta() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_meta(&wal_dir);

        // Delete .meta for segment 2 to simulate legacy/corrupted state
        let meta_path = SegmentMeta::meta_path(&wal_dir, 2);
        std::fs::remove_file(&meta_path).unwrap();

        let reader = WalReader::new();

        // Between segments: segment 2 (no meta) should be conservatively included
        let result = reader.find_segments_for_timestamp(&wal_dir, 2500).unwrap();
        assert_eq!(
            result,
            vec![2],
            "Segment without .meta should be conservatively included"
        );
    }

    #[test]
    fn test_meta_written_for_reopened_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write a record and flush (but don't close — simulates crash)
        {
            let mut writer = WalWriter::new(
                wal_dir.to_path_buf(),
                [1u8; 16],
                DurabilityMode::Always,
                WalConfig::for_testing(),
                make_codec(),
            )
            .unwrap();
            writer
                .append(&WalRecord::new(TxnId(1), [1u8; 16], 1000, vec![1]))
                .unwrap();
            writer.flush().unwrap();
            // Drop without close — no .meta written
        }

        // Reopen (segment is reopened) — writer should rebuild metadata from records
        {
            let mut writer = WalWriter::new(
                wal_dir.to_path_buf(),
                [1u8; 16],
                DurabilityMode::Always,
                WalConfig::for_testing(),
                make_codec(),
            )
            .unwrap();

            // In-memory metadata should have been rebuilt from existing records
            let meta = writer.current_segment_meta().expect("should have metadata");
            assert_eq!(meta.min_txn_id, TxnId(1));
            assert_eq!(meta.max_txn_id, TxnId(1));
            assert_eq!(meta.min_timestamp, 1000);
            assert_eq!(meta.max_timestamp, 1000);
            assert_eq!(meta.record_count, 1);

            // Append another record
            writer
                .append(&WalRecord::new(TxnId(2), [1u8; 16], 2000, vec![2]))
                .unwrap();

            // Metadata should track both records
            let meta = writer.current_segment_meta().unwrap();
            assert_eq!(meta.min_txn_id, TxnId(1));
            assert_eq!(meta.max_txn_id, TxnId(2));
            assert_eq!(meta.min_timestamp, 1000);
            assert_eq!(meta.max_timestamp, 2000);
            assert_eq!(meta.record_count, 2);

            writer.close().unwrap();
        }

        // .meta should exist with correct values
        let seg_num = {
            let reader = WalReader::new();
            let segments = reader.list_segments(&wal_dir).unwrap();
            *segments.last().unwrap()
        };
        let meta = SegmentMeta::read_from_file(&wal_dir, seg_num)
            .unwrap()
            .expect(".meta should exist after close");
        assert_eq!(meta.min_txn_id, TxnId(1));
        assert_eq!(meta.max_txn_id, TxnId(2));
        assert_eq!(meta.record_count, 2);
    }

    // ---- Incremental watermark refresh (segment-skipping) tests ----

    /// Helper: create 2 closed segments with .meta + 1 active segment without .meta.
    ///
    /// Segment 1 (closed): txn_ids 1-2, timestamps 1000-2000
    /// Segment 2 (closed): txn_ids 3-4, timestamps 3000-4000
    /// Segment 3 (active): txn_ids 5-6, timestamps 5000-6000, NO .meta
    /// Manually emit the T3-E12 v3 outer envelope followed by a raw
    /// record payload. Used by tests that bypass `WalWriter::append`
    /// to construct specific on-disk layouts (corruption fixtures,
    /// watermark scenarios). Mirrors the envelope logic in
    /// `WalWriter::append_inner`.
    fn write_enveloped_record(segment: &mut WalSegment, record_bytes: &[u8]) {
        let outer_len = u32::try_from(record_bytes.len()).unwrap();
        let outer_len_bytes = outer_len.to_le_bytes();
        let outer_len_crc = {
            let mut h = Hasher::new();
            h.update(&outer_len_bytes);
            h.finalize()
        };
        segment.write(&outer_len_bytes).unwrap();
        segment.write(&outer_len_crc.to_le_bytes()).unwrap();
        segment.write(record_bytes).unwrap();
    }

    fn create_segments_with_active(wal_dir: &Path) {
        std::fs::create_dir_all(wal_dir).unwrap();

        // Closed segments with .meta
        for (seg_num, txn_ids, timestamps) in [
            (1u64, [1u64, 2], [1000u64, 2000]),
            (2, [3, 4], [3000, 4000]),
        ] {
            let mut segment = WalSegment::create(wal_dir, seg_num, [1u8; 16]).unwrap();
            let mut meta = SegmentMeta::new_empty(seg_num);
            for (&txn_id, &ts) in txn_ids.iter().zip(timestamps.iter()) {
                let record = WalRecord::new(TxnId(txn_id), [1u8; 16], ts, vec![txn_id as u8; 10]);
                write_enveloped_record(&mut segment, &record.to_bytes());
                meta.track_record(TxnId(txn_id), ts);
            }
            segment.close().unwrap();
            meta.write_to_file(wal_dir).unwrap();
        }

        // Active segment — no .meta
        let mut segment = WalSegment::create(wal_dir, 3, [1u8; 16]).unwrap();
        for (&txn_id, &ts) in [5u64, 6].iter().zip([5000u64, 6000].iter()) {
            let record = WalRecord::new(TxnId(txn_id), [1u8; 16], ts, vec![txn_id as u8; 10]);
            write_enveloped_record(&mut segment, &record.to_bytes());
        }
        // Deliberately NOT closing or writing .meta — this is the active segment
    }

    #[test]
    fn test_read_after_watermark_skips_old_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // 2 closed segments with .meta + 1 active without .meta
        create_segments_with_active(&wal_dir);

        let reader = WalReader::new();

        // Verify precondition: segments 1,2 have .meta; segment 3 does not
        assert!(SegmentMeta::read_from_file(&wal_dir, 1).unwrap().is_some());
        assert!(SegmentMeta::read_from_file(&wal_dir, 2).unwrap().is_some());
        assert!(SegmentMeta::read_from_file(&wal_dir, 3).unwrap().is_none());

        // Watermark=4: segments 1 (max_txn_id=2) and 2 (max_txn_id=4) should be
        // skipped via .meta check. Segment 3 (active, no .meta) is always read.
        let filtered = reader.read_all_after_watermark(&wal_dir, 4).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![5, 6]);

        // Watermark=0: nothing skippable, all 6 records returned
        let filtered = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert_eq!(filtered.len(), 6);

        // Watermark=2: segment 1 (max_txn_id=2 <= 2) skipped.
        // Segment 2 (max_txn_id=4 > 2) read. Segment 3 (active) read.
        let filtered = reader.read_all_after_watermark(&wal_dir, 2).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![3, 4, 5, 6]);
    }

    #[test]
    fn test_read_after_watermark_proves_segment_skipped() {
        // Proves the optimization actually skips segment I/O: corrupt a closed
        // segment's .seg file but leave its .meta intact. If the segment were
        // read, it would error. If correctly skipped, the call succeeds.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_active(&wal_dir);

        // Corrupt segment 1's .seg file (overwrite contents after header with garbage)
        let seg1_path = WalSegment::segment_path(&wal_dir, 1);
        let mut seg1_data = std::fs::read(&seg1_path).unwrap();
        // Overwrite record data (after the 36-byte v2 segment header) with garbage
        for byte in seg1_data[36..].iter_mut() {
            *byte = 0xFF;
        }
        std::fs::write(&seg1_path, &seg1_data).unwrap();

        // .meta for segment 1 still says max_txn_id=2
        let meta = SegmentMeta::read_from_file(&wal_dir, 1).unwrap().unwrap();
        assert_eq!(meta.max_txn_id, TxnId(2));

        let reader = WalReader::new();

        // Watermark=2: segment 1 (max_txn_id=2 <= 2) should be SKIPPED via .meta.
        // If it were read, the corrupted data would cause an error or wrong results.
        let filtered = reader.read_all_after_watermark(&wal_dir, 2).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![3, 4, 5, 6]);

        // Watermark=0: segment 1 MUST be read — and it's corrupted.
        // Verify we get fewer records (corruption causes early stop in segment 1).
        let filtered = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        // Segment 1 is corrupted so its records may be missing or partial.
        // The important thing: txn_ids 3-6 from segments 2+3 are still present.
        assert!(
            filtered.len() < 6,
            "Should get fewer than 6 records due to corruption in segment 1, got {}",
            filtered.len()
        );
        let has_seg2_and_3: Vec<u64> = filtered
            .iter()
            .filter(|r| r.txn_id.as_u64() >= 3)
            .map(|r| r.txn_id.as_u64())
            .collect();
        assert_eq!(has_seg2_and_3, vec![3, 4, 5, 6]);
    }

    #[test]
    fn test_read_after_watermark_boundary_max_txn_equals_watermark() {
        // Explicitly tests the <= boundary: segment 2 has max_txn_id=4,
        // watermark=4 means 4 <= 4 is true, so segment 2 must be skipped.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_active(&wal_dir);

        let reader = WalReader::new();

        // Watermark=3: segment 2 max_txn_id=4 > 3, so segment 2 is NOT skipped
        let filtered = reader.read_all_after_watermark(&wal_dir, 3).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![4, 5, 6], "txn_id=4 should be included");

        // Watermark=4: segment 2 max_txn_id=4 <= 4, so segment 2 IS skipped
        let filtered = reader.read_all_after_watermark(&wal_dir, 4).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![5, 6], "txn_ids 3,4 should be excluded");
    }

    #[test]
    fn test_read_after_watermark_reads_active_segment_without_meta() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write records to a single segment.
        // D-7: flush() now writes .meta for the active segment, so delete it
        // to simulate the pre-D-7 scenario where active segments lack .meta.
        let records: Vec<_> = (1..=5)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8]))
            .collect();
        write_records(&wal_dir, &records);

        // Remove .meta to simulate pre-D-7 / crash-before-flush scenario
        let reader = WalReader::new();
        let segments = reader.list_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 1);
        let meta_path = SegmentMeta::meta_path(&wal_dir, segments[0]);
        if meta_path.exists() {
            std::fs::remove_file(&meta_path).unwrap();
        }

        // Watermark=3: only txn_ids 4,5 returned
        let filtered = reader.read_all_after_watermark(&wal_dir, 3).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![4, 5]);

        // Watermark=0: all records
        let filtered = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert_eq!(filtered.len(), 5);

        // Watermark=5: nothing above watermark
        let filtered = reader.read_all_after_watermark(&wal_dir, 5).unwrap();
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_read_after_watermark_handles_missing_meta_gracefully() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Create 3 closed segments with .meta
        create_segments_with_meta(&wal_dir);

        // Delete .meta for segment 1 to simulate legacy/corrupted state
        let meta_path = SegmentMeta::meta_path(&wal_dir, 1);
        std::fs::remove_file(&meta_path).unwrap();

        let reader = WalReader::new();

        // Watermark=0: all records returned despite missing .meta
        let filtered = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert_eq!(filtered.len(), 6);

        // Watermark=4: segment 1 (no meta) falls through to full read,
        // its records (txn_ids 1,2) get filtered out at record level.
        // Segment 2 (meta max_txn_id=4 <= 4) is skipped.
        // Segment 3 (latest) is always read.
        let filtered = reader.read_all_after_watermark(&wal_dir, 4).unwrap();
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![5, 6]);
    }

    #[test]
    fn test_read_after_watermark_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let reader = WalReader::new();
        let filtered = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_read_after_watermark_above_all_records() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        create_segments_with_active(&wal_dir);

        let reader = WalReader::new();

        // Watermark=100: far above all records (max txn_id=6).
        // Closed segments skipped via meta, active segment read but all filtered.
        let filtered = reader.read_all_after_watermark(&wal_dir, 100).unwrap();
        assert!(filtered.is_empty());

        // Watermark=u64::MAX: extreme case
        let filtered = reader.read_all_after_watermark(&wal_dir, u64::MAX).unwrap();
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_read_after_watermark_with_writer_rotation() {
        // End-to-end test using WalWriter with forced rotation (tiny segment size).
        // Verifies the optimization works correctly with real WalWriter-produced
        // segments and .meta files.
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            make_codec(),
        )
        .unwrap();

        // Write enough records to force multiple rotations
        for i in 1..=6 {
            writer
                .append(&WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![0; 50]))
                .unwrap();
        }
        // close() writes .meta for the final segment too
        writer.close().unwrap();

        let reader = WalReader::new();
        let segments = reader.list_segments(&wal_dir).unwrap();
        assert!(
            segments.len() > 1,
            "Should have rotated, got {} segments",
            segments.len()
        );

        // Verify watermark filtering returns correct records across rotated segments
        let all = reader.read_all_after_watermark(&wal_dir, 0).unwrap();
        assert_eq!(all.len(), 6);

        let filtered = reader.read_all_after_watermark(&wal_dir, 3).unwrap();
        assert!(filtered.iter().all(|r| r.txn_id.as_u64() > 3));
        let txn_ids: Vec<u64> = filtered.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![4, 5, 6]);
    }

    // ---- Streaming iterator (iter_all) tests ----

    #[test]
    fn test_iter_all_matches_read_all() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=10)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 20]))
            .collect();

        write_records(&wal_dir, &records);

        let reader = WalReader::new();

        // Collect via read_all
        let read_all_result = reader.read_all(&wal_dir).unwrap();
        let read_all_records = read_all_result.records;

        // Collect via iter_all
        let iter_all_records: Vec<WalRecord> = reader
            .iter_all(&wal_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Must produce identical records in the same order
        assert_eq!(read_all_records.len(), iter_all_records.len());
        for (a, b) in read_all_records.iter().zip(iter_all_records.iter()) {
            assert_eq!(a.txn_id, b.txn_id);
            assert_eq!(a.branch_id, b.branch_id);
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.writeset, b.writeset);
        }
    }

    #[test]
    fn test_iter_all_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create empty segment
        WalSegment::create(&wal_dir, 1, [1u8; 16]).unwrap();

        let reader = WalReader::new();
        let iter_records: Vec<WalRecord> = reader
            .iter_all(&wal_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(iter_records.is_empty());
    }

    #[test]
    fn test_iter_all_multiple_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use tiny segment size to force rotation across multiple segments
        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            make_codec(),
        )
        .unwrap();

        for i in 1..=6u64 {
            writer
                .append(&WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![0; 50]))
                .unwrap();
        }
        writer.close().unwrap();

        let reader = WalReader::new();
        let segments = reader.list_segments(&wal_dir).unwrap();
        assert!(segments.len() > 1, "Should have rotated");

        // Verify iter_all matches read_all across multiple segments
        let read_all_records = reader.read_all(&wal_dir).unwrap().records;
        let iter_all_records: Vec<WalRecord> = reader
            .iter_all(&wal_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(read_all_records.len(), iter_all_records.len());
        for (a, b) in read_all_records.iter().zip(iter_all_records.iter()) {
            assert_eq!(a.txn_id, b.txn_id);
            assert_eq!(a.writeset, b.writeset);
        }
    }

    #[test]
    fn test_iter_all_with_partial_record() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], 0, vec![i as u8]))
            .collect();
        write_records(&wal_dir, &records);

        // Append garbage to simulate crash mid-write
        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 10]).unwrap();

        let reader = WalReader::new();

        // iter_all should still yield the 3 valid records (partial record
        // at end of a valid segment is not an error — read_segment stops
        // at the first invalid record and returns what it could parse).
        let iter_records: Vec<WalRecord> = reader
            .iter_all(&wal_dir)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(iter_records.len(), 3);
        for (i, record) in iter_records.iter().enumerate() {
            assert_eq!(record.txn_id, TxnId((i + 1) as u64));
        }
    }

    /// Issue #1712: Mid-segment corruption should be fatal by default.
    ///
    /// Write 6 records, corrupt the bytes of record 3 (mid-segment), verify
    /// that the default (strict) reader returns an error instead of silently
    /// skipping the corrupted region and returning records 4-6.
    #[test]
    fn test_issue_1712_mid_segment_corruption_fatal_by_default() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write 6 valid records
        let records: Vec<_> = (1..=6)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 20]))
            .collect();
        write_records(&wal_dir, &records);

        // Serialize records to find byte offsets for corruption. On-disk
        // layout (v3+): each record is preceded by an 8-byte outer
        // envelope `[u32 outer_len][u32 outer_len_crc]`, so record N
        // starts at `sum(record_bytes[0..N].len()) + N * ENVELOPE`.
        let record_bytes: Vec<Vec<u8>> = records.iter().map(|r| r.to_bytes()).collect();
        let offset_of_record_3: usize =
            record_bytes[0].len() + record_bytes[1].len() + 2 * WAL_RECORD_ENVELOPE_OVERHEAD; // two envelopes before record 3

        // Read the segment file, corrupt record 3's inner payload so the
        // outer envelope still parses (valid outer_len + outer_len_crc)
        // but the inner CRC fails. That tests the mid-segment corruption
        // path after the envelope has been validated.
        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        let mut seg_data = std::fs::read(&segment_path).unwrap();
        let hdr_size = 36; // v2/v3 segment header size (unchanged in v3).
        let envelope_start = hdr_size + offset_of_record_3;
        let inner_record_start = envelope_start + WAL_RECORD_ENVELOPE_OVERHEAD;
        // Corrupt payload bytes (skip 4-byte inner length prefix, flip
        // payload bytes). Inner-record CRC fails, outer envelope is fine.
        let payload_start = inner_record_start + 4;
        let payload_end = inner_record_start + record_bytes[2].len() - 4; // before inner CRC
        for byte in seg_data[payload_start..payload_end].iter_mut() {
            *byte ^= 0xFF;
        }
        std::fs::write(&segment_path, &seg_data).unwrap();

        // Default (strict) reader should return an error on mid-segment corruption
        let reader = WalReader::new();
        let result = reader.read_segment(&wal_dir, 1);
        assert!(
            result.is_err(),
            "Default reader should error on mid-segment corruption, got: {:?}",
            result.unwrap(),
        );
        match result.unwrap_err() {
            WalReaderError::CorruptedSegment { offset, .. } => {
                // Corruption detected at the right place
                assert_eq!(offset, offset_of_record_3);
            }
            other => panic!("Expected CorruptedSegment error, got: {:?}", other),
        }

        // Lossy reader should skip the corrupted region and return records on both sides
        let lossy_reader = WalReader::new().with_lossy_recovery();
        let (lossy_records, _, _, skipped) = lossy_reader.read_segment(&wal_dir, 1).unwrap();
        assert!(skipped > 0, "Lossy reader should report skipped corruption");
        // Records 1-2 before corruption and 4-6 after should be present
        let txn_ids: Vec<u64> = lossy_records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(txn_ids, vec![1, 2, 4, 5, 6]);
    }

    /// Issue #1556: Multi-segment WAL corruption recovery.
    ///
    /// Write records across multiple segments (via tiny segment size), corrupt
    /// a record payload in the middle segment (preserving the length field so
    /// it triggers CRC mismatch, not InsufficientData), and verify:
    /// - Strict reader returns an error
    /// - Lossy reader recovers records from all segments, skipping corrupted ones
    #[test]
    fn test_issue_1556_multi_segment_corruption_recovery() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use tiny segments to force rotation
        let config = WalConfig::new()
            .with_segment_size(100)
            .with_buffered_sync_bytes(50);

        let mut writer = WalWriter::new(
            wal_dir.to_path_buf(),
            [1u8; 16],
            DurabilityMode::Always,
            config,
            make_codec(),
        )
        .unwrap();

        // Write 10 records with large enough payloads to spread across segments
        for i in 1..=10u64 {
            writer
                .append(&WalRecord::new(
                    TxnId(i),
                    [1u8; 16],
                    i * 1000,
                    vec![0xAA; 40],
                ))
                .unwrap();
        }
        writer.close().unwrap();

        let reader = WalReader::new();
        let segments = reader.list_segments(&wal_dir).unwrap();
        assert!(
            segments.len() >= 3,
            "Expected at least 3 segments, got {}",
            segments.len()
        );

        // Read all records before corruption to get baseline
        let baseline = reader.read_all(&wal_dir).unwrap();
        assert_eq!(baseline.records.len(), 10);

        // Count records in each segment before corruption
        let seg1_count_before = reader.read_segment(&wal_dir, segments[0]).unwrap().0.len();
        let seg_last_count_before = reader
            .read_segment(&wal_dir, *segments.last().unwrap())
            .unwrap()
            .0
            .len();

        // Corrupt the second segment's record payload (not the length field).
        // The v2 header is 36 bytes. The first record starts at offset 36.
        // Record format: [length: 4 bytes] [payload] [crc: 4 bytes]
        // Corrupt a byte inside the payload (offset 36 + 4 + 5 = 45) so
        // the length is valid but CRC mismatches.
        let seg2_path = WalSegment::segment_path(&wal_dir, segments[1]);
        {
            let mut data = std::fs::read(&seg2_path).unwrap();
            // XOR-flip a byte in the payload area of the first record
            let corrupt_offset = 36 + 4 + 5; // past header + length + 5 bytes into payload
            if corrupt_offset < data.len() {
                data[corrupt_offset] ^= 0xFF;
            }
            std::fs::write(&seg2_path, &data).unwrap();
        }

        // Strict reader should fail on the corrupted segment
        let strict_result = reader.read_all(&wal_dir);
        assert!(
            strict_result.is_err(),
            "Strict reader should fail on corrupted segment"
        );

        // Lossy reader should recover records from all segments,
        // skipping only the corrupted records in segment 2
        let lossy_reader = WalReader::new().with_lossy_recovery();
        let lossy_result = lossy_reader.read_all(&wal_dir).unwrap();

        // We should get records from the uncorrupted segments
        assert!(
            !lossy_result.records.is_empty(),
            "Lossy reader should recover some records"
        );

        // With tiny segments (1 record each), corrupting the only record
        // in a segment means there's no next record to scan to, so
        // skipped_corrupted may be 0. The key guarantee is that records
        // from OTHER segments are still recovered.

        // Segment 1 and last segment should be fully readable
        let seg1_count_after = lossy_reader
            .read_segment(&wal_dir, segments[0])
            .unwrap()
            .0
            .len();
        assert_eq!(
            seg1_count_after, seg1_count_before,
            "First segment should be fully intact"
        );

        let seg_last_count_after = lossy_reader
            .read_segment(&wal_dir, *segments.last().unwrap())
            .unwrap()
            .0
            .len();
        assert_eq!(
            seg_last_count_after, seg_last_count_before,
            "Last segment should be fully intact"
        );

        // Total recovered should be fewer than the baseline (some records lost
        // from the corrupted segment) but non-zero
        assert!(
            lossy_result.records.len() < baseline.records.len(),
            "Should recover fewer records than baseline ({} vs {})",
            lossy_result.records.len(),
            baseline.records.len(),
        );
        assert!(
            lossy_result.records.len() >= seg1_count_before + seg_last_count_before,
            "Should recover at least all records from uncorrupted segments"
        );
    }

    /// Issue #1577: A torn write to the 4-byte length prefix produces a garbage
    /// length. The reader trusts this length, skips forward past all remaining
    /// valid records, hits InsufficientData, and stops — losing every record
    /// after the corruption point.
    ///
    /// The fix adds a per-record length CRC (v2 format) so the reader detects
    /// the bad length immediately and scans forward to find subsequent valid
    /// records.
    #[test]
    fn test_issue_1577_torn_length_field_recovery() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write 6 valid records into a single segment
        let records: Vec<_> = (1..=6)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 20]))
            .collect();
        write_records(&wal_dir, &records);

        // Compute byte offsets to find record 3's inner length field.
        // On-disk layout (v3+): each record has an 8-byte outer
        // envelope in front. Torn-write test target is the INNER
        // record's length field (4 bytes into the inner record), which
        // lives 2*ENVELOPE + sum(record_bytes[0..2]) + ENVELOPE bytes
        // past the segment header.
        let record_bytes: Vec<Vec<u8>> = records.iter().map(|r| r.to_bytes()).collect();
        let offset_of_record_3_envelope: usize =
            record_bytes[0].len() + record_bytes[1].len() + 2 * WAL_RECORD_ENVELOPE_OVERHEAD;

        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        let mut seg_data = std::fs::read(&segment_path).unwrap();
        let hdr_size = 36; // v2/v3 segment header size.
        let inner_length_offset =
            hdr_size + offset_of_record_3_envelope + WAL_RECORD_ENVELOPE_OVERHEAD;

        // Overwrite the 4-byte INNER length prefix with 0x7FFFFFFF
        // (2GB — way past EOF). Outer envelope still parses cleanly,
        // so the reader enters inner-record parsing and hits the torn
        // length. With v2 LenCRC, the reader detects the lie and the
        // lossy scan-forward recovers records 4-6.
        seg_data[inner_length_offset..inner_length_offset + 4]
            .copy_from_slice(&0x7FFF_FFFFu32.to_le_bytes());
        std::fs::write(&segment_path, &seg_data).unwrap();

        // Lossy reader must detect the bad length and scan forward to records 4-6.
        // Before the fix: reader hits InsufficientData at the garbage length and
        // stops, returning only records 1-2.
        let lossy_reader = WalReader::new().with_lossy_recovery();
        let (lossy_records, _, _, skipped) = lossy_reader.read_segment(&wal_dir, 1).unwrap();
        assert!(skipped > 0, "Lossy reader should report skipped corruption");
        let txn_ids: Vec<u64> = lossy_records.iter().map(|r| r.txn_id.as_u64()).collect();
        assert_eq!(
            txn_ids,
            vec![1, 2, 4, 5, 6],
            "Records after the torn length must be recovered"
        );
    }

    /// Issue #1577: The length CRC in v2 records catches torn lengths that happen
    /// to be "reasonable" (within segment bounds but pointing to the wrong offset).
    #[test]
    fn test_issue_1577_length_crc_catches_plausible_torn_length() {
        let record = WalRecord::new(TxnId(42), [1u8; 16], 99999, vec![1, 2, 3, 4, 5]);
        let bytes = record.to_bytes();

        // Corrupt only the length field to a plausible but wrong value
        let mut corrupted = bytes.clone();
        let original_length = u32::from_le_bytes(corrupted[0..4].try_into().unwrap());
        let bad_length = original_length + 1; // off by one — plausible but wrong
        corrupted[0..4].copy_from_slice(&bad_length.to_le_bytes());

        // from_bytes must detect this via LengthChecksumMismatch (not just InsufficientData)
        let result = WalRecord::from_bytes(&corrupted);
        assert!(
            matches!(result, Err(WalRecordError::LengthChecksumMismatch)),
            "Expected LengthChecksumMismatch for corrupted length, got: {:?}",
            result
        );
    }

    #[test]
    fn test_read_all_after_watermark_contiguous_preserves_prefix_and_exact_skip_resume() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [7u8; 16], i * 1000, vec![i as u8; 24]))
            .collect();
        write_records(&wal_dir, &records);

        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        let segment = WalSegment::open_read(&wal_dir, 1).unwrap();
        let hdr_size = segment.header_size() as usize;
        let mut bytes = std::fs::read(&segment_path).unwrap();
        let record_1_len = records[0].to_bytes().len();
        let record_2_len = records[1].to_bytes().len();
        let corrupt_offset = hdr_size + record_1_len + (record_2_len / 2);
        bytes[corrupt_offset] ^= 0x5A;
        std::fs::write(&segment_path, bytes).unwrap();

        let reader = WalReader::new();
        let first = reader
            .read_all_after_watermark_contiguous(&wal_dir, 0)
            .unwrap();
        assert_eq!(
            first
                .records
                .iter()
                .map(|r| r.txn_id.as_u64())
                .collect::<Vec<_>>(),
            vec![1],
            "reader must preserve the valid prefix before the corrupted record"
        );
        let blocked = first.blocked.expect("expected a blocked txn");
        assert_eq!(blocked.txn_id, TxnId(2));
        assert!(
            blocked.skip_allowed,
            "a later valid record should make an exact skip resumable"
        );

        let resumed = reader
            .read_all_after_watermark_contiguous(&wal_dir, 2)
            .unwrap();
        assert_eq!(
            resumed
                .records
                .iter()
                .map(|r| r.txn_id.as_u64())
                .collect::<Vec<_>>(),
            vec![3],
            "after an exact skip, reader should resume at the next readable txn"
        );
        assert!(
            resumed.blocked.is_none(),
            "skipping the blocked txn should clear the gap when a later anchor exists"
        );
    }

    #[test]
    fn test_read_all_after_watermark_contiguous_marks_unanchored_tail_corruption_not_skippable() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let records: Vec<_> = (1..=2)
            .map(|i| WalRecord::new(TxnId(i), [9u8; 16], i * 1000, vec![i as u8; 24]))
            .collect();
        write_records(&wal_dir, &records);

        let segment_path = WalSegment::segment_path(&wal_dir, 1);
        let segment = WalSegment::open_read(&wal_dir, 1).unwrap();
        let hdr_size = segment.header_size() as usize;
        let mut bytes = std::fs::read(&segment_path).unwrap();
        let record_1_len = records[0].to_bytes().len();
        let record_2_len = records[1].to_bytes().len();
        let corrupt_offset = hdr_size + record_1_len + (record_2_len / 2);
        bytes[corrupt_offset] ^= 0x33;
        std::fs::write(&segment_path, bytes).unwrap();

        let reader = WalReader::new();
        let result = reader
            .read_all_after_watermark_contiguous(&wal_dir, 0)
            .unwrap();
        assert_eq!(
            result
                .records
                .iter()
                .map(|r| r.txn_id.as_u64())
                .collect::<Vec<_>>(),
            vec![1]
        );
        let blocked = result.blocked.expect("expected tail corruption to block");
        assert_eq!(blocked.txn_id, TxnId(2));
        assert!(
            !blocked.skip_allowed,
            "without a later valid anchor, exact skip must fail closed"
        );
    }

    #[test]
    fn test_read_all_after_watermark_contiguous_skips_stale_codec_failure_before_watermark() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [5u8; 16], i * 1000, vec![i as u8; 16]))
            .collect();
        write_records(&wal_dir, &records);

        let reader =
            WalReader::new().with_codec(Box::new(RejectTxnDecodeCodec { reject_txn_id: 1 }));
        let resumed = reader
            .read_all_after_watermark_contiguous(&wal_dir, 2)
            .unwrap();

        assert_eq!(
            resumed
                .records
                .iter()
                .map(|r| r.txn_id.as_u64())
                .collect::<Vec<_>>(),
            vec![3],
            "codec-decode failure on an already-applied txn must not block later contiguous records"
        );
        assert!(
            resumed.blocked.is_none(),
            "stale codec-decode failures below the watermark should be skipped when the next expected txn is readable"
        );
    }

    // ========================================================================
    // T3-E12 Phase 2 — envelope-parse + codec-error taxonomy
    // ========================================================================

    /// Test-only codec that always fails on `decode` while letting
    /// `encode` pass through. Used to exercise the codec-decode-error
    /// branch of the reader without pulling in `AesGcmCodec` + env
    /// vars. `codec_id()` returns a unique name so MANIFEST checks
    /// don't confuse it with identity.
    struct AlwaysFailDecodeCodec;

    impl StorageCodec for AlwaysFailDecodeCodec {
        fn encode(&self, data: &[u8]) -> Vec<u8> {
            data.to_vec()
        }

        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, crate::durability::codec::CodecError> {
            Err(crate::durability::codec::CodecError::decode(
                "test codec forcibly fails decode",
                "always-fail-decode",
                data.len(),
            ))
        }

        fn codec_id(&self) -> &str {
            "always-fail-decode"
        }

        fn clone_box(&self) -> Box<dyn StorageCodec> {
            Box::new(AlwaysFailDecodeCodec)
        }
    }

    /// Test-only codec that rejects a specific txn id while passing all other
    /// payloads through unchanged. The contiguous reader uses this to prove
    /// codec-decode failures below the watermark can be scanned past without
    /// blocking later records.
    struct RejectTxnDecodeCodec {
        reject_txn_id: u64,
    }

    impl StorageCodec for RejectTxnDecodeCodec {
        fn encode(&self, data: &[u8]) -> Vec<u8> {
            data.to_vec()
        }

        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, crate::durability::codec::CodecError> {
            let txn_id = data
                .get(9..17)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_le_bytes);
            if txn_id == Some(self.reject_txn_id) {
                return Err(crate::durability::codec::CodecError::decode(
                    format!("test codec rejects txn {}", self.reject_txn_id),
                    "reject-txn-decode",
                    data.len(),
                ));
            }
            Ok(data.to_vec())
        }

        fn codec_id(&self) -> &str {
            "reject-txn-decode"
        }

        fn clone_box(&self) -> Box<dyn StorageCodec> {
            Box::new(Self {
                reject_txn_id: self.reject_txn_id,
            })
        }
    }

    /// Read the outer envelope + inner record offsets for a segment
    /// populated by `write_records`. The first record starts
    /// immediately after the 36-byte v2/v3 segment header.
    const V3_HEADER_SIZE: usize = 36;

    /// T3-E12 §D4: an outer_len_crc mismatch is corruption, NOT a
    /// partial-record tail. Strict mode must error immediately; lossy
    /// mode can scan forward but must NOT demote the failure to
    /// `Ok(stop_reason = PartialRecord)`.
    #[test]
    fn test_outer_len_crc_mismatch_is_corruption_not_partial_tail() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 12]))
            .collect();
        write_records(&wal_dir, &records);

        // Flip a byte inside the outer_len_crc field of the first
        // record. The envelope layout at [segment_header][record_1]
        // is: 36 bytes segment header, then 4 bytes outer_len, then
        // 4 bytes outer_len_crc. Byte 36+4+1 = 41 lands in outer_len_crc.
        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        {
            let mut data = std::fs::read(&seg_path).unwrap();
            data[V3_HEADER_SIZE + 4 + 1] ^= 0xFF;
            std::fs::write(&seg_path, &data).unwrap();
        }

        // Strict: expect CorruptedSegment error (NOT a PartialRecord
        // stop reason buried in an Ok result).
        let strict = WalReader::new();
        match strict.read_segment(&wal_dir, 1) {
            Err(WalReaderError::CorruptedSegment { records_before, .. }) => {
                assert_eq!(
                    records_before, 0,
                    "first-record envelope CRC mismatch must fail at offset 0"
                );
            }
            other => panic!(
                "outer_len_crc mismatch in strict mode must return CorruptedSegment; got {:?}",
                other.map(|t| format!("Ok({} records, stop_reason={:?})", t.0.len(), t.2,)),
            ),
        }
    }

    /// T3-E12 §D4: in lossy mode, outer_len_crc mismatch on a
    /// middle record triggers scan-forward to the next valid envelope.
    /// Records before the corruption are preserved; records after
    /// the next valid envelope are recovered.
    #[test]
    fn test_outer_len_crc_mismatch_lossy_scans_forward() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let records: Vec<_> = (1..=4)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 12]))
            .collect();
        write_records(&wal_dir, &records);

        // Compute byte offset of record 2's envelope (first byte of
        // its outer_len_crc) on disk. Each on-disk record is
        // [8-byte envelope] + record_bytes. Record 2 starts at
        // hdr + envelope_of_record_1 + record_bytes[0].
        let record_bytes: Vec<Vec<u8>> = records.iter().map(|r| r.to_bytes()).collect();
        let record_2_envelope_start =
            V3_HEADER_SIZE + super::WAL_RECORD_ENVELOPE_OVERHEAD + record_bytes[0].len();
        let crc_byte_offset = record_2_envelope_start + 4 + 1; // first byte of outer_len_crc

        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        {
            let mut data = std::fs::read(&seg_path).unwrap();
            data[crc_byte_offset] ^= 0xFF;
            std::fs::write(&seg_path, &data).unwrap();
        }

        let lossy = WalReader::new().with_lossy_recovery();
        let (records_recovered, _, stop_reason, skipped) = lossy.read_segment(&wal_dir, 1).unwrap();
        assert!(
            skipped > 0,
            "lossy scan-forward must count the skipped corrupt envelope"
        );
        let txn_ids: Vec<u64> = records_recovered
            .iter()
            .map(|r| r.txn_id.as_u64())
            .collect();
        assert!(
            txn_ids.contains(&1),
            "record 1 (before corruption) must survive; got {txn_ids:?}"
        );
        assert!(
            txn_ids.contains(&3) || txn_ids.contains(&4),
            "at least one post-corruption record must be recovered via scan-forward; got {txn_ids:?}"
        );
        // Sanity: the stop reason reflects either end-of-data (full
        // recovery past corruption) or checksum mismatch (scan window
        // exhausted before the tail). Either is acceptable — the
        // point of this test is that lossy does NOT bail as
        // PartialRecord on an outer_len_crc mismatch.
        assert!(
            !matches!(stop_reason, ReadStopReason::PartialRecord),
            "outer_len_crc mismatch must NOT demote to PartialRecord; got {stop_reason:?}"
        );
    }

    /// T3-E12 §D5: codec decode failures ALWAYS return
    /// `Err(WalReaderError::CodecDecode)` — in BOTH strict and lossy
    /// modes. No demotion to `Ok(stop_reason)` is permitted, because
    /// the coordinator iterates records via callback and ignores
    /// `ReadStopReason` — an `Ok(stop_reason=CodecDecode)` would
    /// silently short-read the segment and skip the T3-E10 wipe.
    #[test]
    fn test_codec_decode_failure_never_partial_tail() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write via identity codec so the on-disk envelope is valid
        // and the outer_len_crc passes. Then read with a codec that
        // refuses every payload — simulates the AES-GCM wrong-key
        // path at the reader layer, without needing env-var setup.
        let records: Vec<_> = (1..=3)
            .map(|i| WalRecord::new(TxnId(i), [1u8; 16], i * 1000, vec![i as u8; 16]))
            .collect();
        write_records(&wal_dir, &records);

        let strict = WalReader::new().with_codec(Box::new(AlwaysFailDecodeCodec));
        match strict.read_segment(&wal_dir, 1) {
            Err(WalReaderError::CodecDecode { detail, .. }) => {
                assert!(
                    detail.contains("forcibly fails decode")
                        || detail.contains("always-fail-decode"),
                    "CodecDecode detail must carry the codec's error message; got {detail:?}"
                );
            }
            other => panic!(
                "strict codec-decode failure must return Err(CodecDecode); got {:?}",
                other.map(|t| format!("Ok({} records)", t.0.len()))
            ),
        }

        let lossy = WalReader::new()
            .with_lossy_recovery()
            .with_codec(Box::new(AlwaysFailDecodeCodec));
        match lossy.read_segment(&wal_dir, 1) {
            Err(WalReaderError::CodecDecode { .. }) => { /* pass — lossy does NOT demote */ }
            other => panic!(
                "lossy codec-decode failure must ALSO return Err(CodecDecode) \
                 — no demotion to Ok(stop_reason); got {:?}",
                other.map(|t| format!("Ok({} records, stop_reason={:?})", t.0.len(), t.2,)),
            ),
        }
    }

    /// T3-E12 §D8: a pre-v3 segment header surfaces as the typed
    /// `WalReaderError::LegacyFormat`, not a stringified `IoError`.
    /// This is the load-bearing piece that lets the engine's lossy
    /// branch pattern-match on the error variant and skip the wipe
    /// (§D6 hard-fail).
    #[test]
    fn test_legacy_segment_format_typed_error() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Hand-craft a v2 segment header directly (36 bytes: magic +
        // version=2 + segment_number + database_uuid + header CRC
        // over the first 32 bytes).
        let mut header = [0u8; 36];
        header[0..4].copy_from_slice(b"STRA");
        header[4..8].copy_from_slice(&2u32.to_le_bytes());
        header[8..16].copy_from_slice(&1u64.to_le_bytes());
        header[16..32].copy_from_slice(&[0xAA; 16]);
        let crc = {
            let mut h = Hasher::new();
            h.update(&header[0..32]);
            h.finalize()
        };
        header[32..36].copy_from_slice(&crc.to_le_bytes());

        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        std::fs::write(&seg_path, header).unwrap();

        let reader = WalReader::new();
        match reader.read_segment(&wal_dir, 1) {
            Err(WalReaderError::LegacyFormat {
                found_version,
                hint,
            }) => {
                assert_eq!(found_version, 2);
                assert!(
                    hint.contains("requires segment format version"),
                    "hint must name the required version, got: {hint}"
                );
                assert!(
                    hint.contains("wal/"),
                    "hint must name the wal/ directory, got: {hint}"
                );
            }
            Ok(_) => panic!("pre-v3 segment open must return an error, not succeed",),
            Err(other) => panic!(
                "pre-v3 segment must surface as WalReaderError::LegacyFormat, got: {other:?}",
            ),
        }
    }

    /// T3-E12 §D8: `WalSegment::open_read` returns the TYPED
    /// `WalSegmentError::Header(SegmentHeaderError::LegacyFormat)`,
    /// NOT a stringified `io::Error`. This is the layer below
    /// `test_legacy_segment_format_typed_error` — it proves the typed
    /// error survives from segment-file open all the way up to
    /// `WalReader::read_segment` without collapsing into an
    /// `io::ErrorKind::InvalidData` string.
    #[test]
    fn test_wal_segment_open_read_typed_propagation() {
        use crate::durability::format::{SegmentHeaderError, WalSegmentError};

        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut header = [0u8; 36];
        header[0..4].copy_from_slice(b"STRA");
        header[4..8].copy_from_slice(&2u32.to_le_bytes()); // legacy v2
        header[8..16].copy_from_slice(&1u64.to_le_bytes());
        header[16..32].copy_from_slice(&[0xCC; 16]);
        let crc = {
            let mut h = Hasher::new();
            h.update(&header[0..32]);
            h.finalize()
        };
        header[32..36].copy_from_slice(&crc.to_le_bytes());

        let seg_path = WalSegment::segment_path(&wal_dir, 1);
        std::fs::write(&seg_path, header).unwrap();

        match WalSegment::open_read(&wal_dir, 1) {
            Err(WalSegmentError::Header(SegmentHeaderError::LegacyFormat {
                found_version,
                ..
            })) => {
                assert_eq!(found_version, 2);
            }
            Err(other) => panic!(
                "pre-v3 segment open_read must produce typed Header(LegacyFormat), got: {other:?}"
            ),
            Ok(_) => panic!("pre-v3 segment open_read must refuse, not succeed"),
        }
    }
}
