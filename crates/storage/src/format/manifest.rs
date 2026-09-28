use super::{ByteReader, FormatError, MAX_CODEC_ID_LEN};
use strata_core::CommitVersion;

const FORMAT: &str = "database_manifest";
const MAGIC: [u8; 4] = *b"STRM";

/// #3658: the database manifest format this build writes. 1.2.6 changed the
/// durable format one way (event records v2, #3594/#3595; timeline segments,
/// #3643), so it writes a version a 1.2.5 binary refuses at open instead of
/// half-reading. Not 2: 0 and 2 are reserved for pre-V1 development
/// manifests, which every V1 build reports as pre-V1 (and a retired version
/// is never reused).
pub(crate) const DATABASE_MANIFEST_FORMAT_VERSION: u32 = 3;
/// The V1 manifest version through 1.2.5: still read, upgraded to the current
/// version by the first open that recovers it (#3658).
pub(crate) const DATABASE_MANIFEST_FORMAT_VERSION_V1: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseManifest {
    database_id: [u8; 16],
    codec_id: String,
    active_wal_segment: u64,
    snapshot_watermark: Option<u64>,
    snapshot_id: Option<u64>,
    flushed_through_commit_id: Option<CommitVersion>,
}

impl DatabaseManifest {
    pub(crate) fn new(
        database_id: [u8; 16],
        codec_id: impl Into<String>,
    ) -> Result<Self, FormatError> {
        let codec_id = codec_id.into();
        validate_codec_id(&codec_id)?;
        Ok(Self {
            database_id,
            codec_id,
            active_wal_segment: 1,
            snapshot_watermark: None,
            snapshot_id: None,
            flushed_through_commit_id: None,
        })
    }

    pub(crate) fn with_recovery_facts(
        mut self,
        active_wal_segment: u64,
        snapshot_watermark: Option<u64>,
        snapshot_id: Option<u64>,
        flushed_through_commit_id: Option<CommitVersion>,
    ) -> Result<Self, FormatError> {
        validate_recovery_facts(active_wal_segment, snapshot_watermark, snapshot_id)?;
        self.active_wal_segment = active_wal_segment;
        self.snapshot_watermark = snapshot_watermark;
        self.snapshot_id = snapshot_id;
        self.flushed_through_commit_id = flushed_through_commit_id;
        Ok(self)
    }

    pub(crate) const fn database_id(&self) -> &[u8; 16] {
        &self.database_id
    }

    pub(crate) fn codec_id(&self) -> &str {
        &self.codec_id
    }

    pub(crate) const fn active_wal_segment(&self) -> u64 {
        self.active_wal_segment
    }

    pub(crate) const fn snapshot_watermark(&self) -> Option<u64> {
        self.snapshot_watermark
    }

    pub(crate) const fn snapshot_id(&self) -> Option<u64> {
        self.snapshot_id
    }

    pub(crate) const fn flushed_through_commit_id(&self) -> Option<CommitVersion> {
        self.flushed_through_commit_id
    }
}

pub(crate) fn encode_manifest(manifest: &DatabaseManifest) -> Result<Vec<u8>, FormatError> {
    validate_codec_id(manifest.codec_id())?;
    validate_recovery_facts(
        manifest.active_wal_segment(),
        manifest.snapshot_watermark(),
        manifest.snapshot_id(),
    )?;
    let codec_len = u32::try_from(manifest.codec_id().len())
        .map_err(|_| FormatError::InvalidLength { field: "codec_id" })?;

    let mut bytes = Vec::with_capacity(4 + 4 + 16 + 4 + manifest.codec_id().len() + 32 + 4);
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&DATABASE_MANIFEST_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(manifest.database_id());
    bytes.extend_from_slice(&codec_len.to_le_bytes());
    bytes.extend_from_slice(manifest.codec_id().as_bytes());
    bytes.extend_from_slice(&manifest.active_wal_segment().to_le_bytes());
    // Zero is the durable sentinel for absent optional recovery facts. V1
    // rejects present-zero values at construction so decode can map zero back
    // to None unambiguously.
    bytes.extend_from_slice(&manifest.snapshot_watermark().unwrap_or(0).to_le_bytes());
    bytes.extend_from_slice(&manifest.snapshot_id().unwrap_or(0).to_le_bytes());
    bytes.extend_from_slice(
        &manifest
            .flushed_through_commit_id()
            .map_or(0, CommitVersion::as_u64)
            .to_le_bytes(),
    );

    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<DatabaseManifest, FormatError> {
    decode_manifest_with_version(bytes).map(|(manifest, _)| manifest)
}

/// #3658: decode, also reporting the on-disk format version (current or V1),
/// so an open can tell whether the manifest still needs its upgrade.
pub(crate) fn decode_manifest_with_version(
    bytes: &[u8],
) -> Result<(DatabaseManifest, u32), FormatError> {
    if bytes.len() < minimum_manifest_len() {
        return Err(FormatError::InsufficientBytes {
            format: FORMAT,
            needed: minimum_manifest_len(),
            actual: bytes.len(),
        });
    }

    let checksum_offset = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(
        bytes[checksum_offset..]
            .try_into()
            .map_err(|_| FormatError::InvalidLength { field: "crc32" })?,
    );

    let mut reader = ByteReader::new(FORMAT, &bytes[..checksum_offset]);
    let magic = reader.read_exact(4)?;
    if magic != MAGIC {
        return Err(FormatError::InvalidMagic { format: FORMAT });
    }

    let version = reader.read_u32_le()?;
    match version {
        DATABASE_MANIFEST_FORMAT_VERSION | DATABASE_MANIFEST_FORMAT_VERSION_V1 => {}
        0 | 2 => {
            // Known development manifests are rejected with a distinct error so
            // open-time policy can refuse pre-V1 databases deterministically.
            return Err(FormatError::PreV1Format {
                format: FORMAT,
                version,
            });
        }
        version => {
            return Err(FormatError::FutureFormat {
                format: FORMAT,
                version,
                max_supported: DATABASE_MANIFEST_FORMAT_VERSION,
            });
        }
    }

    let computed_crc = crc32fast::hash(&bytes[..checksum_offset]);
    if stored_crc != computed_crc {
        return Err(FormatError::ChecksumMismatch {
            format: FORMAT,
            expected: stored_crc,
            computed: computed_crc,
        });
    }

    let database_id = reader.read_exact(16)?;
    let database_id =
        <[u8; 16]>::try_from(database_id).map_err(|_| FormatError::InvalidLength {
            field: "database_id",
        })?;

    let codec_id_len = usize::try_from(reader.read_u32_le()?)
        .map_err(|_| FormatError::InvalidLength { field: "codec_id" })?;
    if codec_id_len > MAX_CODEC_ID_LEN {
        return Err(FormatError::InvalidLength { field: "codec_id" });
    }
    let codec_id = reader.read_exact(codec_id_len)?;
    let codec_id = std::str::from_utf8(codec_id)
        .map_err(|_| FormatError::InvalidUtf8 { field: "codec_id" })?
        .to_owned();
    validate_codec_id(&codec_id)?;

    let active_wal_segment = reader.read_u64_le()?;
    let snapshot_watermark = optional_nonzero(reader.read_u64_le()?);
    let snapshot_id = optional_nonzero(reader.read_u64_le()?);
    let flushed_through_commit_id = optional_nonzero(reader.read_u64_le()?).map(CommitVersion::new);
    reader.finish()?;
    // Pair validation happens after sentinel decoding because a single present
    // snapshot fact is not enough to restart recovery safely.
    validate_recovery_facts(active_wal_segment, snapshot_watermark, snapshot_id)?;

    Ok((
        DatabaseManifest {
            database_id,
            codec_id,
            active_wal_segment,
            snapshot_watermark,
            snapshot_id,
            flushed_through_commit_id,
        },
        version,
    ))
}

const fn minimum_manifest_len() -> usize {
    4 + 4 + 16 + 4 + 8 + 8 + 8 + 8 + 4
}

fn optional_nonzero(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn validate_codec_id(codec_id: &str) -> Result<(), FormatError> {
    if codec_id.is_empty() || codec_id.len() > MAX_CODEC_ID_LEN {
        return Err(FormatError::InvalidLength { field: "codec_id" });
    }
    if codec_id.as_bytes().contains(&0x00) {
        return Err(FormatError::InvalidUtf8 { field: "codec_id" });
    }
    Ok(())
}

fn validate_recovery_facts(
    active_wal_segment: u64,
    snapshot_watermark: Option<u64>,
    snapshot_id: Option<u64>,
) -> Result<(), FormatError> {
    if active_wal_segment == 0 {
        return Err(FormatError::InvalidValue {
            field: "active_wal_segment",
        });
    }
    if snapshot_watermark == Some(0) {
        return Err(FormatError::InvalidValue {
            field: "snapshot_watermark",
        });
    }
    if snapshot_id == Some(0) {
        return Err(FormatError::InvalidValue {
            field: "snapshot_id",
        });
    }
    if snapshot_watermark.is_some() != snapshot_id.is_some() {
        return Err(FormatError::InvalidValue {
            field: "snapshot_recovery_pair",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        decode_manifest, decode_manifest_with_version, encode_manifest, DatabaseManifest,
        DATABASE_MANIFEST_FORMAT_VERSION, FORMAT,
    };
    use crate::format::FormatError;
    use strata_core::CommitVersion;

    fn manifest() -> DatabaseManifest {
        DatabaseManifest::new([0x11; 16], "identity")
            .expect("database format")
            .with_recovery_facts(7, Some(42), Some(3), Some(CommitVersion::new(41)))
            .expect("recovery facts")
    }

    fn refresh_crc(bytes: &mut Vec<u8>) {
        bytes.truncate(bytes.len() - 4);
        let crc = crc32fast::hash(bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn manifest_round_trips() {
        let manifest = manifest();

        assert_eq!(
            decode_manifest(&encode_manifest(&manifest).expect("encode manifest")),
            Ok(manifest)
        );
    }

    /// #3658: this build writes format version 3 — not 1, which a 1.2.5 binary
    /// would open and half-read, and not 2, reserved for pre-V1 manifests.
    #[test]
    fn manifest_is_written_at_format_version_3() {
        let bytes = encode_manifest(&manifest()).expect("encode manifest");
        assert_eq!(&bytes[4..8], &3u32.to_le_bytes());
        assert_eq!(decode_manifest_with_version(&bytes), Ok((manifest(), 3)));
    }

    /// #3658: the version field, value by value: the V1 manifest (1) and the
    /// current one (3) decode, reporting which; 0 and 2 are pre-V1; anything
    /// above 3 was written by a newer build.
    #[test]
    fn manifest_format_version_truth_table() {
        for version in [0u32, 1, 2, 3, 4, 9, u32::MAX] {
            let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
            bytes[4..8].copy_from_slice(&version.to_le_bytes());
            refresh_crc(&mut bytes);
            let decoded = decode_manifest_with_version(&bytes);
            match version {
                1 | 3 => assert_eq!(decoded, Ok((manifest(), version)), "version {version}"),
                0 | 2 => assert_eq!(
                    decoded,
                    Err(FormatError::PreV1Format {
                        format: FORMAT,
                        version
                    }),
                    "version {version}"
                ),
                _ => assert_eq!(
                    decoded,
                    Err(FormatError::FutureFormat {
                        format: FORMAT,
                        version,
                        max_supported: 3
                    }),
                    "version {version}"
                ),
            }
        }
    }

    #[test]
    fn decode_rejects_invalid_magic() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes[0] = b'X';

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::InvalidMagic { format: FORMAT })
        );
    }

    #[test]
    fn decode_rejects_pre_v1_development_version() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes[4..8].copy_from_slice(&2u32.to_le_bytes());

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::PreV1Format {
                format: FORMAT,
                version: 2
            })
        );
    }

    #[test]
    fn decode_rejects_future_version() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes[4..8].copy_from_slice(&(DATABASE_MANIFEST_FORMAT_VERSION + 8).to_le_bytes());

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::FutureFormat {
                format: FORMAT,
                version: DATABASE_MANIFEST_FORMAT_VERSION + 8,
                max_supported: DATABASE_MANIFEST_FORMAT_VERSION
            })
        );
    }

    #[test]
    fn decode_rejects_checksum_mismatch() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes[20] ^= 0xff;

        assert!(matches!(
            decode_manifest(&bytes),
            Err(FormatError::ChecksumMismatch { format: FORMAT, .. })
        ));
    }

    #[test]
    fn with_recovery_facts_rejects_zero_active_wal_segment() {
        assert_eq!(
            DatabaseManifest::new([0x11; 16], "identity")
                .expect("database format")
                .with_recovery_facts(0, None, None, None),
            Err(FormatError::InvalidValue {
                field: "active_wal_segment"
            })
        );
    }

    #[test]
    fn with_recovery_facts_rejects_partial_snapshot_facts() {
        assert_eq!(
            DatabaseManifest::new([0x11; 16], "identity")
                .expect("database format")
                .with_recovery_facts(1, Some(42), None, None),
            Err(FormatError::InvalidValue {
                field: "snapshot_recovery_pair"
            })
        );
        assert_eq!(
            DatabaseManifest::new([0x11; 16], "identity")
                .expect("database format")
                .with_recovery_facts(1, None, Some(3), None),
            Err(FormatError::InvalidValue {
                field: "snapshot_recovery_pair"
            })
        );
    }

    #[test]
    fn with_recovery_facts_rejects_present_zero_snapshot_facts() {
        assert_eq!(
            DatabaseManifest::new([0x11; 16], "identity")
                .expect("database format")
                .with_recovery_facts(1, Some(0), Some(3), None),
            Err(FormatError::InvalidValue {
                field: "snapshot_watermark"
            })
        );
        assert_eq!(
            DatabaseManifest::new([0x11; 16], "identity")
                .expect("database format")
                .with_recovery_facts(1, Some(42), Some(0), None),
            Err(FormatError::InvalidValue {
                field: "snapshot_id"
            })
        );
    }

    #[test]
    fn decode_rejects_zero_active_wal_segment() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes[36..44].copy_from_slice(&0u64.to_le_bytes());
        refresh_crc(&mut bytes);

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::InvalidValue {
                field: "active_wal_segment"
            })
        );
    }

    #[test]
    fn decode_rejects_partial_snapshot_facts() {
        let mut watermark_only = encode_manifest(&manifest()).expect("encode manifest");
        watermark_only[52..60].copy_from_slice(&0u64.to_le_bytes());
        refresh_crc(&mut watermark_only);

        assert_eq!(
            decode_manifest(&watermark_only),
            Err(FormatError::InvalidValue {
                field: "snapshot_recovery_pair"
            })
        );

        let mut snapshot_only = encode_manifest(&manifest()).expect("encode manifest");
        snapshot_only[44..52].copy_from_slice(&0u64.to_le_bytes());
        refresh_crc(&mut snapshot_only);

        assert_eq!(
            decode_manifest(&snapshot_only),
            Err(FormatError::InvalidValue {
                field: "snapshot_recovery_pair"
            })
        );
    }

    #[test]
    fn decode_rejects_invalid_codec_utf8() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"STRM");
        bytes.extend_from_slice(&DATABASE_MANIFEST_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&[0x11; 16]);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::InvalidUtf8 { field: "codec_id" })
        );
    }

    #[test]
    fn decode_rejects_oversized_codec_len_before_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"STRM");
        bytes.extend_from_slice(&DATABASE_MANIFEST_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&[0x11; 16]);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::InvalidLength { field: "codec_id" })
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = encode_manifest(&manifest()).expect("encode manifest");
        bytes.truncate(bytes.len() - 4);
        bytes.push(0xee);
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(
            decode_manifest(&bytes),
            Err(FormatError::TrailingData {
                format: FORMAT,
                remaining: 1
            })
        );
    }
}
