//! Binary manifest for level persistence.
//!
//! Format (v2):
//! ```text
//! "STRAMFST" (8B magic) | u16 version (=2) | 6B reserved | u32 entry_count
//! Own segment entries:
//!   For each: u16 filename_len | filename bytes | u8 level
//! Inherited layer section:
//!   u32 inherited_layer_count
//!   For each inherited layer:
//!     [u8; 16]  source_branch_id
//!     u64       fork_version (LE)
//!     u8        status (0=Active, 1=Materializing, 2=Materialized)
//!     u32       layer_entry_count
//!     For each layer entry:
//!       u16 filename_len | filename bytes | u8 level
//! u32 CRC32 (over everything before CRC)
//! ```

use std::io;
use std::path::Path;

use strata_core::types::BranchId;

/// Magic bytes for segment manifest: "STRAMFST"
const MANIFEST_MAGIC: [u8; 8] = *b"STRAMFST";

/// Current manifest version.
const MANIFEST_VERSION: u16 = 2;

/// Fixed header size: 8 (magic) + 2 (version) + 6 (reserved) + 4 (entry_count) = 20
const HEADER_SIZE: usize = 20;

/// Manifest file name.
const MANIFEST_FILENAME: &str = "segments.manifest";

/// A single entry in the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    /// Filename of the segment (e.g., "42.sst").
    pub filename: String,
    /// Level: 0 = L0, 1 = L1.
    pub level: u8,
}

/// An inherited layer entry in the manifest (COW branching).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestInheritedLayer {
    /// Branch ID of the source (parent) branch.
    pub source_branch_id: BranchId,
    /// Version counter of the source branch at fork time.
    pub fork_version: u64,
    /// Layer status: 0=Active, 1=Materializing, 2=Materialized.
    pub status: u8,
    /// Segment entries belonging to this inherited layer.
    pub entries: Vec<ManifestEntry>,
}

/// Parsed segment manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    /// Own segment entries in the manifest.
    pub entries: Vec<ManifestEntry>,
    /// Inherited layers from parent branches (COW fork).
    pub inherited_layers: Vec<ManifestInheritedLayer>,
}

/// Write a manifest file to `dir/segments.manifest` atomically (temp + rename).
pub fn write_manifest(
    dir: &Path,
    entries: &[ManifestEntry],
    inherited_layers: &[ManifestInheritedLayer],
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(HEADER_SIZE + entries.len() * 20 + 4);

    // Header
    buf.extend_from_slice(&MANIFEST_MAGIC);
    buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    buf.extend_from_slice(&[0u8; 6]); // reserved
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    // Own segment entries
    for entry in entries {
        write_entry(&mut buf, entry);
    }

    // Inherited layer section
    buf.extend_from_slice(&(inherited_layers.len() as u32).to_le_bytes());
    for layer in inherited_layers {
        buf.extend_from_slice(layer.source_branch_id.as_bytes());
        buf.extend_from_slice(&layer.fork_version.to_le_bytes());
        buf.push(layer.status);
        buf.extend_from_slice(&(layer.entries.len() as u32).to_le_bytes());
        for entry in &layer.entries {
            write_entry(&mut buf, entry);
        }
    }

    // CRC32 over everything before CRC
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Atomic write: temp file + fsync + rename + dir fsync
    let final_path = dir.join(MANIFEST_FILENAME);
    let tmp_path = dir.join(format!("{}.tmp", MANIFEST_FILENAME));

    // Write and fsync the temp file before rename — ensures data is
    // durable on disk, not just in the page cache.
    {
        let mut file = std::fs::File::create(&tmp_path)?;
        std::io::Write::write_all(&mut file, &buf)?;
        file.sync_all()?;
    }

    std::fs::rename(&tmp_path, &final_path)?;

    // Fsync the parent directory to make the rename durable.
    // Failure here is non-fatal (rename already succeeded).
    if let Ok(dir_fd) = std::fs::File::open(dir) {
        let _ = dir_fd.sync_all();
    }

    Ok(())
}

/// Write a single manifest entry to the buffer.
fn write_entry(buf: &mut Vec<u8>, entry: &ManifestEntry) {
    let name_bytes = entry.filename.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.push(entry.level);
}

/// Read a manifest file from `dir/segments.manifest`.
///
/// Returns `Ok(None)` if the file does not exist.
/// Returns `Err` if the file exists but is corrupt (bad magic, truncated, bad CRC).
pub fn read_manifest(dir: &Path) -> io::Result<Option<SegmentManifest>> {
    let path = dir.join(MANIFEST_FILENAME);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    if data.len() < HEADER_SIZE + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest too short",
        ));
    }

    // Verify CRC
    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
    let computed_crc = crc32fast::hash(&data[..crc_offset]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest CRC mismatch",
        ));
    }

    // Verify magic
    if data[..8] != MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest bad magic",
        ));
    }

    // Parse header
    let version = u16::from_le_bytes(data[8..10].try_into().unwrap());
    if version > MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported segment manifest version {} (this build supports up to {})",
                version, MANIFEST_VERSION
            ),
        ));
    }
    // skip 6 reserved bytes
    let entry_count = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

    // Sanity check: each entry is at least 3 bytes (u16 name_len + u8 level).
    let max_possible = (crc_offset - HEADER_SIZE) / 3;
    if entry_count > max_possible {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest entry_count exceeds file capacity",
        ));
    }

    // Parse own entries
    let mut pos = HEADER_SIZE;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let (entry, new_pos) = read_entry(&data, pos, crc_offset)?;
        entries.push(entry);
        pos = new_pos;
    }

    // Parse inherited layers (v2+)
    let inherited_layers = if version >= 2 && pos + 4 <= crc_offset {
        let layer_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Sanity check: each inherited layer is at least 29 bytes
        // (16 branch_id + 8 fork_version + 1 status + 4 entry_count).
        let remaining = crc_offset.saturating_sub(pos);
        if layer_count > remaining / 29 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest inherited_layer_count exceeds file capacity",
            ));
        }

        let mut layers = Vec::with_capacity(layer_count);
        for _ in 0..layer_count {
            // source_branch_id: [u8; 16]
            if pos + 16 > crc_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest truncated (inherited layer branch_id)",
                ));
            }
            let mut branch_bytes = [0u8; 16];
            branch_bytes.copy_from_slice(&data[pos..pos + 16]);
            let source_branch_id = BranchId::from_bytes(branch_bytes);
            pos += 16;

            // fork_version: u64 LE
            if pos + 8 > crc_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest truncated (inherited layer fork_version)",
                ));
            }
            let fork_version = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // status: u8
            if pos + 1 > crc_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest truncated (inherited layer status)",
                ));
            }
            let status = data[pos];
            pos += 1;

            // layer_entry_count: u32
            if pos + 4 > crc_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest truncated (inherited layer entry_count)",
                ));
            }
            let layer_entry_count =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Sanity check: each entry is at least 3 bytes.
            let layer_remaining = crc_offset.saturating_sub(pos);
            if layer_entry_count > layer_remaining / 3 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest inherited layer entry_count exceeds file capacity",
                ));
            }

            let mut layer_entries = Vec::with_capacity(layer_entry_count);
            for _ in 0..layer_entry_count {
                let (entry, new_pos) = read_entry(&data, pos, crc_offset)?;
                layer_entries.push(entry);
                pos = new_pos;
            }

            layers.push(ManifestInheritedLayer {
                source_branch_id,
                fork_version,
                status,
                entries: layer_entries,
            });
        }
        layers
    } else {
        Vec::new()
    };

    Ok(Some(SegmentManifest {
        entries,
        inherited_layers,
    }))
}

/// Read a single manifest entry from `data` at `pos`. Returns (entry, new_pos).
fn read_entry(data: &[u8], pos: usize, crc_offset: usize) -> io::Result<(ManifestEntry, usize)> {
    if pos + 2 > crc_offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest truncated (filename_len)",
        ));
    }
    let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    let pos = pos + 2;

    if pos + name_len + 1 > crc_offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest truncated (entry data)",
        ));
    }
    let filename = String::from_utf8(data[pos..pos + name_len].to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "manifest non-UTF8 filename"))?;
    let pos = pos + name_len;

    let level = data[pos];
    let pos = pos + 1;

    Ok((ManifestEntry { filename, level }, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            ManifestEntry {
                filename: "1.sst".into(),
                level: 0,
            },
            ManifestEntry {
                filename: "2.sst".into(),
                level: 1,
            },
            ManifestEntry {
                filename: "42.sst".into(),
                level: 1,
            },
        ];

        write_manifest(dir.path(), &entries, &[]).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);
        assert!(result.inherited_layers.is_empty());
    }

    #[test]
    fn manifest_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let result = read_manifest(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn manifest_corrupt_magic() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "1.sst".into(),
            level: 0,
        }];
        write_manifest(dir.path(), &entries, &[]).unwrap();

        // Corrupt the magic
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut data = std::fs::read(&path).unwrap();
        data[0] = b'X';
        // Also fix CRC so we hit magic check, not CRC check
        let crc = crc32fast::hash(&data[..data.len() - 4]);
        let crc_offset = data.len() - 4;
        data[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let result = read_manifest(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn manifest_corrupt_crc() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "1.sst".into(),
            level: 0,
        }];
        write_manifest(dir.path(), &entries, &[]).unwrap();

        // Corrupt a data byte (CRC won't match)
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut data = std::fs::read(&path).unwrap();
        data[10] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = read_manifest(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn manifest_empty_entries() {
        let dir = tempfile::tempdir().unwrap();
        let entries: Vec<ManifestEntry> = vec![];

        write_manifest(dir.path(), &entries, &[]).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert!(result.entries.is_empty());
        assert!(result.inherited_layers.is_empty());
    }

    #[test]
    fn manifest_multiple_levels() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            ManifestEntry {
                filename: "10.sst".into(),
                level: 0,
            },
            ManifestEntry {
                filename: "5.sst".into(),
                level: 0,
            },
            ManifestEntry {
                filename: "3.sst".into(),
                level: 1,
            },
            ManifestEntry {
                filename: "1.sst".into(),
                level: 1,
            },
        ];

        write_manifest(dir.path(), &entries, &[]).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);

        let l0_count = result.entries.iter().filter(|e| e.level == 0).count();
        let l1_count = result.entries.iter().filter(|e| e.level == 1).count();
        assert_eq!(l0_count, 2);
        assert_eq!(l1_count, 2);
    }

    #[test]
    fn manifest_inherited_layers_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "1.sst".into(),
            level: 0,
        }];
        let inherited = vec![ManifestInheritedLayer {
            source_branch_id: BranchId::from_bytes([0xAA; 16]),
            fork_version: 42,
            status: 0, // Active
            entries: vec![
                ManifestEntry {
                    filename: "parent_10.sst".into(),
                    level: 0,
                },
                ManifestEntry {
                    filename: "parent_5.sst".into(),
                    level: 1,
                },
            ],
        }];

        write_manifest(dir.path(), &entries, &inherited).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);
        assert_eq!(result.inherited_layers, inherited);
    }

    #[test]
    fn manifest_empty_inherited_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "1.sst".into(),
            level: 0,
        }];

        write_manifest(dir.path(), &entries, &[]).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);
        assert!(result.inherited_layers.is_empty());
    }

    #[test]
    fn manifest_multiple_inherited_layers() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "own.sst".into(),
            level: 0,
        }];
        let inherited = vec![
            ManifestInheritedLayer {
                source_branch_id: BranchId::from_bytes([1; 16]),
                fork_version: 10,
                status: 0,
                entries: vec![
                    ManifestEntry {
                        filename: "a.sst".into(),
                        level: 0,
                    },
                    ManifestEntry {
                        filename: "b.sst".into(),
                        level: 1,
                    },
                ],
            },
            ManifestInheritedLayer {
                source_branch_id: BranchId::from_bytes([2; 16]),
                fork_version: 20,
                status: 1, // Materializing
                entries: vec![ManifestEntry {
                    filename: "c.sst".into(),
                    level: 0,
                }],
            },
            ManifestInheritedLayer {
                source_branch_id: BranchId::from_bytes([3; 16]),
                fork_version: 30,
                status: 2, // Materialized
                entries: vec![
                    ManifestEntry {
                        filename: "d.sst".into(),
                        level: 0,
                    },
                    ManifestEntry {
                        filename: "e.sst".into(),
                        level: 1,
                    },
                    ManifestEntry {
                        filename: "f.sst".into(),
                        level: 2,
                    },
                ],
            },
        ];

        write_manifest(dir.path(), &entries, &inherited).unwrap();
        let result = read_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(result.entries, entries);
        assert_eq!(result.inherited_layers.len(), 3);
        assert_eq!(result.inherited_layers, inherited);
    }

    #[test]
    fn manifest_rejects_future_version() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![ManifestEntry {
            filename: "1.sst".into(),
            level: 0,
        }];
        write_manifest(dir.path(), &entries, &[]).unwrap();

        // Patch the version field to a future value (bytes 8-9)
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut data = std::fs::read(&path).unwrap();
        data[8..10].copy_from_slice(&99u16.to_le_bytes());
        // Recompute CRC so we hit the version check, not the CRC check
        let crc_offset = data.len() - 4;
        let crc = crc32fast::hash(&data[..crc_offset]);
        data[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let result = read_manifest(dir.path());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unsupported segment manifest version"),
            "Expected version error, got: {}",
            err_msg,
        );
    }
}
