use super::{
    ByteReader, FormatError, SNAPSHOT_FOOTER_SIZE, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_HEADER_SIZE,
    SNAPSHOT_SECTION_HEADER_SIZE,
};
use strata_core::{CommitVersion, Timestamp};

const SNAPSHOT_FORMAT: &str = "snapshot_container";
const SNAPSHOT_HEADER_FORMAT: &str = "snapshot_header";
const SNAPSHOT_SECTION_FORMAT: &str = "snapshot_section";
const SNAPSHOT_MAGIC: [u8; 4] = *b"SNAP";
const MAX_SNAPSHOT_CODEC_ID_LEN: usize = u8::MAX as usize;
const MAX_SNAPSHOT_SECTION_COUNT: usize = 4096;
/// The ceiling on a snapshot container's total section payload, enforced on
/// encode and decode so recovery's materialized decode stays bounded
/// (DUR-017). The checkpoint measures its delta against this before it
/// publishes (`lifecycle::checkpoint_delta_cap_decision`).
pub(crate) const MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotMaterializedLimits {
    max_sections: usize,
    max_payload_bytes: usize,
}

const DEFAULT_MATERIALIZED_LIMITS: SnapshotMaterializedLimits = SnapshotMaterializedLimits {
    max_sections: MAX_SNAPSHOT_SECTION_COUNT,
    max_payload_bytes: MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotHeader {
    snapshot_id: u64,
    watermark_commit_version: CommitVersion,
    created_at: Timestamp,
    database_id: [u8; 16],
    codec_id: String,
}

impl SnapshotHeader {
    pub(crate) fn new(
        snapshot_id: u64,
        watermark_commit_version: CommitVersion,
        created_at: Timestamp,
        database_id: [u8; 16],
        codec_id: impl Into<String>,
    ) -> Result<Self, FormatError> {
        let codec_id = codec_id.into();
        validate_snapshot_id(snapshot_id)?;
        validate_codec_id(&codec_id)?;
        Ok(Self {
            snapshot_id,
            watermark_commit_version,
            created_at,
            database_id,
            codec_id,
        })
    }

    pub(crate) const fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }

    pub(crate) const fn watermark_commit_version(&self) -> CommitVersion {
        self.watermark_commit_version
    }

    pub(crate) const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    pub(crate) const fn database_id(&self) -> &[u8; 16] {
        &self.database_id
    }

    pub(crate) fn codec_id(&self) -> &str {
        &self.codec_id
    }
}

pub(crate) fn encode_snapshot_header(header: &SnapshotHeader) -> Result<Vec<u8>, FormatError> {
    validate_snapshot_id(header.snapshot_id())?;
    validate_codec_id(header.codec_id())?;
    let codec_id_len = u8::try_from(header.codec_id().len())
        .map_err(|_| FormatError::InvalidLength { field: "codec_id" })?;

    let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_SIZE + header.codec_id().len());
    bytes.extend_from_slice(&SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&header.snapshot_id().to_le_bytes());
    bytes.extend_from_slice(&header.watermark_commit_version().as_u64().to_le_bytes());
    bytes.extend_from_slice(&header.created_at().as_micros().to_le_bytes());
    bytes.extend_from_slice(header.database_id());
    bytes.push(codec_id_len);
    bytes.extend_from_slice(&[0; 15]);
    bytes.extend_from_slice(header.codec_id().as_bytes());
    Ok(bytes)
}

pub(crate) fn decode_snapshot_header(bytes: &[u8]) -> Result<(SnapshotHeader, usize), FormatError> {
    if bytes.len() < SNAPSHOT_HEADER_SIZE {
        return Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_HEADER_FORMAT,
            needed: SNAPSHOT_HEADER_SIZE,
            actual: bytes.len(),
        });
    }

    let mut reader = ByteReader::new(SNAPSHOT_HEADER_FORMAT, &bytes[..SNAPSHOT_HEADER_SIZE]);
    if reader.read_exact(4)? != SNAPSHOT_MAGIC {
        return Err(FormatError::InvalidMagic {
            format: SNAPSHOT_HEADER_FORMAT,
        });
    }

    validate_snapshot_format_version(reader.read_u32_le()?)?;
    let snapshot_id = reader.read_u64_le()?;
    validate_snapshot_id(snapshot_id)?;
    let watermark_commit_version = CommitVersion::new(reader.read_u64_le()?);
    let created_at = Timestamp::from_micros(reader.read_u64_le()?);
    let database_id = reader.read_exact(16)?;
    let database_id =
        <[u8; 16]>::try_from(database_id).map_err(|_| FormatError::InvalidLength {
            field: "database_id",
        })?;
    let codec_id_len = usize::from(reader.read_u8()?);
    validate_codec_id_len(codec_id_len)?;
    let reserved = reader.read_exact(15)?;
    if reserved.iter().any(|byte| *byte != 0) {
        return Err(FormatError::InvalidValue {
            field: "snapshot_header_reserved",
        });
    }
    reader.finish()?;

    let codec_start = SNAPSHOT_HEADER_SIZE;
    let codec_end = codec_start
        .checked_add(codec_id_len)
        .ok_or(FormatError::InvalidLength { field: "codec_id" })?;
    if bytes.len() < codec_end {
        return Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_HEADER_FORMAT,
            needed: codec_end,
            actual: bytes.len(),
        });
    }
    let codec_id = std::str::from_utf8(&bytes[codec_start..codec_end])
        .map_err(|_| FormatError::InvalidUtf8 { field: "codec_id" })?
        .to_owned();
    validate_codec_id(&codec_id)?;

    Ok((
        SnapshotHeader {
            snapshot_id,
            watermark_commit_version,
            created_at,
            database_id,
            codec_id,
        },
        codec_end,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotSection {
    section_kind: u8,
    payload: Vec<u8>,
}

impl SnapshotSection {
    pub(crate) fn new(section_kind: u8, payload: impl Into<Vec<u8>>) -> Result<Self, FormatError> {
        validate_section_kind(section_kind)?;
        Ok(Self {
            section_kind,
            payload: payload.into(),
        })
    }

    pub(crate) const fn section_kind(&self) -> u8 {
        self.section_kind
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub(crate) fn encode_snapshot_section(section: &SnapshotSection) -> Result<Vec<u8>, FormatError> {
    encode_snapshot_section_with_payload_limit(section, MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES)
}

fn encode_snapshot_section_with_payload_limit(
    section: &SnapshotSection,
    max_payload_bytes: usize,
) -> Result<Vec<u8>, FormatError> {
    validate_section_kind(section.section_kind())?;
    // Enforce the materialization ceiling at encode time so a section that
    // decode would reject as unreadable is never written durably. Keeps encode
    // and decode symmetric: encode succeeds only if the bytes can be read back.
    if section.payload().len() > max_payload_bytes {
        return Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload",
        });
    }
    let payload_len =
        u64::try_from(section.payload().len()).map_err(|_| FormatError::InvalidLength {
            field: SNAPSHOT_SECTION_FORMAT,
        })?;
    let capacity = SNAPSHOT_SECTION_HEADER_SIZE
        .checked_add(section.payload().len())
        .ok_or(FormatError::InvalidLength {
            field: SNAPSHOT_SECTION_FORMAT,
        })?;

    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(section.section_kind());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(section.payload());
    Ok(bytes)
}

pub(crate) fn decode_snapshot_section(
    bytes: &[u8],
) -> Result<(SnapshotSection, usize), FormatError> {
    decode_snapshot_section_with_payload_limit(bytes, MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES)
}

fn decode_snapshot_section_with_payload_limit(
    bytes: &[u8],
    max_payload_bytes: usize,
) -> Result<(SnapshotSection, usize), FormatError> {
    let (section, consumed) = decode_snapshot_section_ref(bytes)?;
    // Borrowed section decoding is allowed to expose large slices, but
    // materializing a section must enforce an allocation ceiling.
    if section.payload().len() > max_payload_bytes {
        return Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload",
        });
    }

    Ok((section.to_owned(), consumed))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotSectionRef<'a> {
    section_kind: u8,
    payload: &'a [u8],
}

impl SnapshotSectionRef<'_> {
    pub(crate) const fn section_kind(&self) -> u8 {
        self.section_kind
    }

    pub(crate) const fn payload(&self) -> &[u8] {
        self.payload
    }

    fn to_owned(self) -> SnapshotSection {
        SnapshotSection {
            section_kind: self.section_kind,
            payload: self.payload.to_vec(),
        }
    }
}

pub(crate) fn decode_snapshot_section_ref(
    bytes: &[u8],
) -> Result<(SnapshotSectionRef<'_>, usize), FormatError> {
    if bytes.len() < SNAPSHOT_SECTION_HEADER_SIZE {
        return Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_SECTION_FORMAT,
            needed: SNAPSHOT_SECTION_HEADER_SIZE,
            actual: bytes.len(),
        });
    }

    let mut reader = ByteReader::new(
        SNAPSHOT_SECTION_FORMAT,
        &bytes[..SNAPSHOT_SECTION_HEADER_SIZE],
    );
    let section_kind = reader.read_u8()?;
    validate_section_kind(section_kind)?;
    let payload_len =
        usize::try_from(reader.read_u64_le()?).map_err(|_| FormatError::InvalidLength {
            field: SNAPSHOT_SECTION_FORMAT,
        })?;
    reader.finish()?;

    let total_len = SNAPSHOT_SECTION_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(FormatError::InvalidLength {
            field: SNAPSHOT_SECTION_FORMAT,
        })?;
    if bytes.len() < total_len {
        return Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_SECTION_FORMAT,
            needed: total_len,
            actual: bytes.len(),
        });
    }

    Ok((
        SnapshotSectionRef {
            section_kind,
            payload: &bytes[SNAPSHOT_SECTION_HEADER_SIZE..total_len],
        },
        total_len,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotContainer {
    header: SnapshotHeader,
    sections: Vec<SnapshotSection>,
}

impl SnapshotContainer {
    pub(crate) fn new(header: SnapshotHeader, sections: Vec<SnapshotSection>) -> Self {
        // Zero sections is a valid V1 empty-checkpoint container. The header
        // still carries the database, codec, timestamp, and watermark facts.
        Self { header, sections }
    }

    pub(crate) const fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    pub(crate) fn sections(&self) -> &[SnapshotSection] {
        &self.sections
    }
}

pub(crate) fn encode_snapshot_container(
    container: &SnapshotContainer,
) -> Result<Vec<u8>, FormatError> {
    encode_snapshot_container_with_materialized_limits(container, DEFAULT_MATERIALIZED_LIMITS)
}

fn encode_snapshot_container_with_materialized_limits(
    container: &SnapshotContainer,
    limits: SnapshotMaterializedLimits,
) -> Result<Vec<u8>, FormatError> {
    if container.sections().len() > limits.max_sections {
        return Err(FormatError::InvalidLength {
            field: "snapshot_section_count",
        });
    }

    // Mirror the cumulative decode ceiling so a container whose total section
    // payload exceeds what decode will materialize is never written durably.
    let mut total_payload_bytes = 0usize;
    let mut bytes = encode_snapshot_header(container.header())?;
    for section in container.sections() {
        total_payload_bytes = total_payload_bytes
            .checked_add(section.payload().len())
            .ok_or(FormatError::InvalidLength {
                field: "snapshot_materialized_payload",
            })?;
        if total_payload_bytes > limits.max_payload_bytes {
            return Err(FormatError::InvalidLength {
                field: "snapshot_materialized_payload",
            });
        }
        bytes.extend_from_slice(&encode_snapshot_section_with_payload_limit(
            section,
            limits.max_payload_bytes,
        )?);
    }
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn decode_snapshot_container(bytes: &[u8]) -> Result<SnapshotContainer, FormatError> {
    decode_snapshot_container_with_materialized_limits(bytes, DEFAULT_MATERIALIZED_LIMITS)
}

fn decode_snapshot_container_with_materialized_limits(
    bytes: &[u8],
    limits: SnapshotMaterializedLimits,
) -> Result<SnapshotContainer, FormatError> {
    if limits.max_sections == 0 {
        return Err(FormatError::InvalidLength {
            field: "snapshot_section_count",
        });
    }

    let mut sections = Vec::new();
    let mut total_payload_bytes = 0usize;
    let header = visit_snapshot_container_sections(bytes, limits.max_sections, |section| {
        total_payload_bytes = total_payload_bytes
            .checked_add(section.payload().len())
            .ok_or(FormatError::InvalidLength {
                field: "snapshot_materialized_payload",
            })?;
        if total_payload_bytes > limits.max_payload_bytes {
            return Err(FormatError::InvalidLength {
                field: "snapshot_materialized_payload",
            });
        }
        sections.push(section.to_owned());
        Ok(())
    })?;

    Ok(SnapshotContainer { header, sections })
}

pub(crate) fn visit_snapshot_container_sections(
    bytes: &[u8],
    max_sections: usize,
    mut visit_section: impl FnMut(SnapshotSectionRef<'_>) -> Result<(), FormatError>,
) -> Result<SnapshotHeader, FormatError> {
    if max_sections == 0 {
        return Err(FormatError::InvalidLength {
            field: "snapshot_section_count",
        });
    }

    let minimum_len = SNAPSHOT_HEADER_SIZE
        .checked_add(1)
        .and_then(|len| len.checked_add(SNAPSHOT_FOOTER_SIZE))
        .ok_or(FormatError::InvalidLength {
            field: SNAPSHOT_FORMAT,
        })?;
    if bytes.len() < minimum_len {
        return Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_FORMAT,
            needed: minimum_len,
            actual: bytes.len(),
        });
    }

    let footer_offset = bytes.len() - SNAPSHOT_FOOTER_SIZE;
    let (header, mut cursor) = decode_snapshot_header(&bytes[..footer_offset])?;
    let stored_crc = u32::from_le_bytes(bytes[footer_offset..].try_into().map_err(|_| {
        FormatError::InvalidLength {
            field: "snapshot_crc32",
        }
    })?);
    // The container footer is validated before section visitation. Callers
    // never see borrowed section bytes from a container with a bad footer.
    let computed_crc = crc32fast::hash(&bytes[..footer_offset]);
    if stored_crc != computed_crc {
        return Err(FormatError::ChecksumMismatch {
            format: SNAPSHOT_FORMAT,
            expected: stored_crc,
            computed: computed_crc,
        });
    }

    let mut section_count = 0usize;
    while cursor < footer_offset {
        let remaining = footer_offset - cursor;
        if remaining < SNAPSHOT_SECTION_HEADER_SIZE {
            return Err(FormatError::TrailingData {
                format: SNAPSHOT_SECTION_FORMAT,
                remaining,
            });
        }
        if section_count == max_sections {
            return Err(FormatError::InvalidLength {
                field: "snapshot_section_count",
            });
        }
        let (section, consumed) = decode_snapshot_section_ref(&bytes[cursor..footer_offset])?;
        cursor = cursor
            .checked_add(consumed)
            .ok_or(FormatError::InvalidLength {
                field: SNAPSHOT_FORMAT,
            })?;
        section_count = section_count
            .checked_add(1)
            .ok_or(FormatError::InvalidLength {
                field: "snapshot_section_count",
            })?;
        visit_section(section)?;
    }

    Ok(header)
}

fn validate_snapshot_format_version(version: u32) -> Result<(), FormatError> {
    match version {
        SNAPSHOT_FORMAT_VERSION => Ok(()),
        0 | 2 => Err(FormatError::PreV1Format {
            format: SNAPSHOT_HEADER_FORMAT,
            version,
        }),
        version => Err(FormatError::FutureFormat {
            format: SNAPSHOT_HEADER_FORMAT,
            version,
            max_supported: SNAPSHOT_FORMAT_VERSION,
        }),
    }
}

fn validate_snapshot_id(snapshot_id: u64) -> Result<(), FormatError> {
    if snapshot_id == 0 {
        return Err(FormatError::InvalidValue {
            field: "snapshot_id",
        });
    }
    Ok(())
}

fn validate_codec_id(codec_id: &str) -> Result<(), FormatError> {
    validate_codec_id_len(codec_id.len())?;
    if codec_id.as_bytes().contains(&0x00) {
        return Err(FormatError::InvalidUtf8 { field: "codec_id" });
    }
    Ok(())
}

fn validate_codec_id_len(codec_id_len: usize) -> Result<(), FormatError> {
    if codec_id_len == 0 || codec_id_len > MAX_SNAPSHOT_CODEC_ID_LEN {
        return Err(FormatError::InvalidLength { field: "codec_id" });
    }
    Ok(())
}

fn validate_section_kind(section_kind: u8) -> Result<(), FormatError> {
    if section_kind == 0 {
        // Kind zero is reserved as an impossible section marker, which makes an
        // all-zero or uninitialized section header fail closed.
        return Err(FormatError::InvalidValue {
            field: "snapshot_section_kind",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
