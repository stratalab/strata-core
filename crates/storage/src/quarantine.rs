//! Per-branch `quarantine.manifest` — durable inventory of in-flight reclaim.
//!
//! Format (v1):
//! ```text
//! "STRAQRTN" (8B magic) | u16 version (=1) | 6B reserved | u32 entry_count
//! For each entry:
//!   u64 segment_id
//!   u16 filename_len | filename bytes   // branch-local, e.g. "42.sst"
//! u32 CRC32 (over everything before CRC)
//! ```
//!
//! ## B5.1 retention contract (pinned format)
//!
//! One `quarantine.manifest` per branch directory, sibling of
//! `segments.manifest`, per
//! `docs/design/branching/branching-gc/branching-retention-contract.md`
//! §"B5.1 chosen format: per-branch `quarantine.manifest`". Written via
//! the same atomic temp + rename + `fsync(dir)` idiom the existing
//! segment manifest uses. Absence is treated as "no quarantined files"
//! (a healthy fresh state).
//!
//! Entries store only branch-local filenames, not absolute paths. Both
//! the original location (`<branch_dir>/<filename>`) and the quarantine
//! destination (`<branch_dir>/__quarantine__/<filename>`) are derived at
//! runtime, so quarantine state stays relocation-safe.
//!
//! This file records protocol state for in-flight reclaim. Manifest
//! reachability remains the only durable liveness proof per
//! Invariant 3 / §"Reclaimability rule".

use std::io;
use std::path::Path;

use crate::{StorageError, StorageResult};

/// Magic bytes for quarantine manifest: "STRAQRTN".
const QUARANTINE_MAGIC: [u8; 8] = *b"STRAQRTN";

/// Current quarantine manifest version.
const QUARANTINE_VERSION: u16 = 1;

/// Fixed header size: 8 (magic) + 2 (version) + 6 (reserved) + 4 (`entry_count`) = 20.
const HEADER_SIZE: usize = 20;

/// Manifest file name (sibling of `segments.manifest`).
pub const QUARANTINE_FILENAME: &str = "quarantine.manifest";

/// Directory name under the branch directory that holds quarantined segments.
pub const QUARANTINE_DIR: &str = "__quarantine__";

/// A single quarantined segment entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineEntry {
    /// Stable segment identity (matches `KVSegment::file_id()` derivation).
    pub segment_id: u64,
    /// Branch-local filename (e.g. `"42.sst"`). The on-disk quarantine
    /// location is always `<branch_dir>/__quarantine__/<filename>`.
    pub filename: String,
}

/// Parsed quarantine manifest contents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuarantineManifest {
    /// One entry per quarantined segment file, segment-identity-keyed.
    pub entries: Vec<QuarantineEntry>,
}

impl QuarantineManifest {
    /// Whether a segment with the given branch-local filename is in the inventory.
    pub fn contains_filename(&self, filename: &str) -> bool {
        self.entries.iter().any(|e| e.filename == filename)
    }

    /// Whether any entry records the given segment id.
    pub fn contains_segment_id(&self, segment_id: u64) -> bool {
        self.entries.iter().any(|e| e.segment_id == segment_id)
    }
}

/// Write `entries` to `<dir>/quarantine.manifest` atomically.
///
/// Uses the same temp-file + fsync + rename + fsync(dir) idiom as
/// `segments.manifest`. A failure at any step returns a typed
/// [`StorageError`]; partial state is safe because readers reject a
/// CRC-invalid temp file before rename, and the pre-rename final path
/// is untouched.
pub fn write_quarantine_manifest(dir: &Path, entries: &[QuarantineEntry]) -> StorageResult<()> {
    let mut buf = Vec::with_capacity(HEADER_SIZE + entries.len() * 18 + 4);

    buf.extend_from_slice(&QUARANTINE_MAGIC);
    buf.extend_from_slice(&QUARANTINE_VERSION.to_le_bytes());
    buf.extend_from_slice(&[0u8; 6]); // reserved
    let entry_count = u32::try_from(entries.len()).expect("quarantine entry count fits in u32");
    buf.extend_from_slice(&entry_count.to_le_bytes());

    for entry in entries {
        buf.extend_from_slice(&entry.segment_id.to_le_bytes());
        let name_bytes = entry.filename.as_bytes();
        let name_len =
            u16::try_from(name_bytes.len()).expect("quarantine filename fits in u16 length");
        buf.extend_from_slice(&name_len.to_le_bytes());
        buf.extend_from_slice(name_bytes);
    }

    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let final_path = dir.join(QUARANTINE_FILENAME);
    let tmp_path = dir.join(format!("{QUARANTINE_FILENAME}.tmp"));

    {
        let mut file = std::fs::File::create(&tmp_path).map_err(|inner| {
            StorageError::QuarantinePublishFailed {
                dir: dir.to_path_buf(),
                inner,
            }
        })?;
        std::io::Write::write_all(&mut file, &buf).map_err(|inner| {
            StorageError::QuarantinePublishFailed {
                dir: dir.to_path_buf(),
                inner,
            }
        })?;
        file.sync_all()
            .map_err(|inner| StorageError::QuarantinePublishFailed {
                dir: dir.to_path_buf(),
                inner,
            })?;
    }

    std::fs::rename(&tmp_path, &final_path).map_err(|inner| {
        StorageError::QuarantinePublishFailed {
            dir: dir.to_path_buf(),
            inner,
        }
    })?;

    let dir_fd = std::fs::File::open(dir).map_err(|inner| StorageError::DirFsync {
        dir: dir.to_path_buf(),
        inner,
    })?;
    dir_fd.sync_all().map_err(|inner| StorageError::DirFsync {
        dir: dir.to_path_buf(),
        inner,
    })?;

    Ok(())
}

/// Read `<dir>/quarantine.manifest`.
///
/// Returns `Ok(None)` if the file does not exist (the healthy fresh
/// state — no quarantined files). Returns `Err` on bad magic,
/// truncation, CRC mismatch, or unsupported version.
pub fn read_quarantine_manifest(dir: &Path) -> io::Result<Option<QuarantineManifest>> {
    let path = dir.join(QUARANTINE_FILENAME);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    if data.len() < HEADER_SIZE + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "quarantine manifest too short",
        ));
    }

    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
    let computed_crc = crc32fast::hash(&data[..crc_offset]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "quarantine manifest CRC mismatch",
        ));
    }

    if data[..8] != QUARANTINE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "quarantine manifest bad magic",
        ));
    }

    let version = u16::from_le_bytes(data[8..10].try_into().unwrap());
    if version > QUARANTINE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported quarantine manifest version {version} (this build supports up to {QUARANTINE_VERSION})"
            ),
        ));
    }
    let entry_count = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

    // Each entry is at least 11 bytes (u64 segment_id + u16 name_len + empty name).
    let max_possible = (crc_offset - HEADER_SIZE) / 11;
    if entry_count > max_possible {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "quarantine manifest entry_count exceeds file capacity",
        ));
    }

    let mut pos = HEADER_SIZE;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        if pos + 10 > crc_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "quarantine manifest truncated (entry header)",
            ));
        }
        let segment_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let name_len = u16::from_le_bytes(data[pos + 8..pos + 10].try_into().unwrap()) as usize;
        pos += 10;

        if pos + name_len > crc_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "quarantine manifest truncated (filename)",
            ));
        }
        let filename = String::from_utf8(data[pos..pos + name_len].to_vec()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "quarantine manifest non-UTF8 filename",
            )
        })?;
        pos += name_len;

        entries.push(QuarantineEntry {
            segment_id,
            filename,
        });
    }

    Ok(Some(QuarantineManifest { entries }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            QuarantineEntry {
                segment_id: 1,
                filename: "1.sst".into(),
            },
            QuarantineEntry {
                segment_id: 42,
                filename: "42.sst".into(),
            },
        ];

        write_quarantine_manifest(dir.path(), &entries).unwrap();
        let result = read_quarantine_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);
    }

    #[test]
    fn quarantine_manifest_absent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let result = read_quarantine_manifest(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn quarantine_manifest_empty_entries_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        write_quarantine_manifest(dir.path(), &[]).unwrap();
        let result = read_quarantine_manifest(dir.path()).unwrap().unwrap();
        assert!(result.entries.is_empty());
    }

    #[test]
    fn quarantine_manifest_bad_crc_errors() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![QuarantineEntry {
            segment_id: 1,
            filename: "1.sst".into(),
        }];
        write_quarantine_manifest(dir.path(), &entries).unwrap();

        let path = dir.path().join(QUARANTINE_FILENAME);
        let mut data = std::fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = read_quarantine_manifest(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn quarantine_manifest_bad_magic_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(QUARANTINE_FILENAME);
        std::fs::write(&path, b"NOTSTRAQRTN123456789012345678901234567890").unwrap();

        let result = read_quarantine_manifest(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn quarantine_manifest_unknown_version_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&QUARANTINE_MAGIC);
        buf.extend_from_slice(&999u16.to_le_bytes()); // version 999
        buf.extend_from_slice(&[0u8; 6]);
        buf.extend_from_slice(&0u32.to_le_bytes()); // entry_count = 0
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let path = dir.path().join(QUARANTINE_FILENAME);
        std::fs::write(&path, &buf).unwrap();

        let result = read_quarantine_manifest(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn quarantine_manifest_contains_helpers() {
        let mf = QuarantineManifest {
            entries: vec![
                QuarantineEntry {
                    segment_id: 10,
                    filename: "10.sst".into(),
                },
                QuarantineEntry {
                    segment_id: 20,
                    filename: "20.sst".into(),
                },
            ],
        };
        assert!(mf.contains_filename("10.sst"));
        assert!(mf.contains_filename("20.sst"));
        assert!(!mf.contains_filename("30.sst"));
        assert!(mf.contains_segment_id(10));
        assert!(!mf.contains_segment_id(30));
    }
}
