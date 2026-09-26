#![deny(unsafe_code)]

use super::{
    decode_snapshot_container, decode_snapshot_container_with_materialized_limits,
    decode_snapshot_header, decode_snapshot_section, decode_snapshot_section_ref,
    encode_snapshot_container, encode_snapshot_container_with_materialized_limits,
    encode_snapshot_header, encode_snapshot_section, encode_snapshot_section_with_payload_limit,
    visit_snapshot_container_sections, SnapshotContainer, SnapshotHeader,
    SnapshotMaterializedLimits, SnapshotSection, SNAPSHOT_FORMAT, SNAPSHOT_HEADER_FORMAT,
    SNAPSHOT_MAGIC, SNAPSHOT_SECTION_FORMAT,
};
use crate::format::{
    FormatError, SNAPSHOT_FOOTER_SIZE, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_HEADER_SIZE,
    SNAPSHOT_SECTION_HEADER_SIZE,
};
use strata_core::{CommitVersion, Timestamp};

fn database_id() -> [u8; 16] {
    [
        0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e,
        0x2f,
    ]
}

fn header() -> SnapshotHeader {
    SnapshotHeader::new(
        3,
        CommitVersion::new(42),
        Timestamp::from_micros(1_700_000_000_123_456),
        database_id(),
        "identity",
    )
    .expect("snapshot header")
}

fn refresh_container_crc(bytes: &mut Vec<u8>) {
    bytes.truncate(bytes.len() - SNAPSHOT_FOOTER_SIZE);
    let crc = crc32fast::hash(bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
}

fn snapshot_bytes_with_raw_sections(raw_sections: &[u8]) -> Vec<u8> {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes.extend_from_slice(raw_sections);
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

#[test]
fn header_round_trips_and_consumes_codec_id_only() {
    let header = header();
    let mut bytes = encode_snapshot_header(&header).expect("encode header");
    bytes.extend_from_slice(b"section bytes");

    assert_eq!(
        decode_snapshot_header(&bytes),
        Ok((header, SNAPSHOT_HEADER_SIZE + "identity".len()))
    );
}

#[test]
fn header_rejects_invalid_magic() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes[0] = b'X';

    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::InvalidMagic {
            format: SNAPSHOT_HEADER_FORMAT
        })
    );
}

#[test]
fn header_rejects_pre_v1_versions() {
    for version in [0u32, 2] {
        let mut bytes = encode_snapshot_header(&header()).expect("encode header");
        bytes[4..8].copy_from_slice(&version.to_le_bytes());

        assert_eq!(
            decode_snapshot_header(&bytes),
            Err(FormatError::PreV1Format {
                format: SNAPSHOT_HEADER_FORMAT,
                version
            })
        );
    }
}

#[test]
fn header_rejects_future_version() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes[4..8].copy_from_slice(&(SNAPSHOT_FORMAT_VERSION + 8).to_le_bytes());

    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::FutureFormat {
            format: SNAPSHOT_HEADER_FORMAT,
            version: SNAPSHOT_FORMAT_VERSION + 8,
            max_supported: SNAPSHOT_FORMAT_VERSION
        })
    );
}

#[test]
fn header_rejects_zero_snapshot_id() {
    let error = SnapshotHeader::new(
        0,
        CommitVersion::new(42),
        Timestamp::from_micros(1_700_000_000_123_456),
        database_id(),
        "identity",
    );
    assert_eq!(
        error,
        Err(FormatError::InvalidValue {
            field: "snapshot_id"
        })
    );

    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes[8..16].copy_from_slice(&0u64.to_le_bytes());
    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::InvalidValue {
            field: "snapshot_id"
        })
    );
}

#[test]
fn header_rejects_empty_or_invalid_codec_id() {
    assert_eq!(
        SnapshotHeader::new(
            3,
            CommitVersion::new(42),
            Timestamp::from_micros(1_700_000_000_123_456),
            database_id(),
            "",
        ),
        Err(FormatError::InvalidLength { field: "codec_id" })
    );

    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes[64] = 0xff;
    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::InvalidUtf8 { field: "codec_id" })
    );
}

#[test]
fn header_accepts_max_codec_id_and_rejects_too_large_or_nul() {
    let max_codec = "a".repeat(255);
    let header = SnapshotHeader::new(
        3,
        CommitVersion::new(42),
        Timestamp::from_micros(1_700_000_000_123_456),
        database_id(),
        max_codec.clone(),
    )
    .expect("max codec id");
    assert_eq!(header.codec_id(), max_codec);
    assert_eq!(
        encode_snapshot_header(&header)
            .expect("encode max codec")
            .len(),
        SNAPSHOT_HEADER_SIZE + 255
    );

    assert_eq!(
        SnapshotHeader::new(
            3,
            CommitVersion::new(42),
            Timestamp::from_micros(1_700_000_000_123_456),
            database_id(),
            "a".repeat(256),
        ),
        Err(FormatError::InvalidLength { field: "codec_id" })
    );
    assert_eq!(
        SnapshotHeader::new(
            3,
            CommitVersion::new(42),
            Timestamp::from_micros(1_700_000_000_123_456),
            database_id(),
            "id\0entity",
        ),
        Err(FormatError::InvalidUtf8 { field: "codec_id" })
    );
}

#[test]
fn header_rejects_nonzero_reserved_bytes() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes[49] = 1;

    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::InvalidValue {
            field: "snapshot_header_reserved"
        })
    );
}

#[test]
fn header_rejects_truncated_codec_id() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes.truncate(SNAPSHOT_HEADER_SIZE + 3);

    assert_eq!(
        decode_snapshot_header(&bytes),
        Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_HEADER_FORMAT,
            needed: SNAPSHOT_HEADER_SIZE + "identity".len(),
            actual: SNAPSHOT_HEADER_SIZE + 3
        })
    );
}

#[test]
fn section_round_trips_and_consumes_one_section() {
    let section = SnapshotSection::new(0x01, b"rows".to_vec()).expect("section");
    let mut bytes = encode_snapshot_section(&section).expect("encode section");
    bytes.extend_from_slice(b"next");

    assert_eq!(
        decode_snapshot_section(&bytes),
        Ok((section, SNAPSHOT_SECTION_HEADER_SIZE + 4))
    );
}

#[test]
fn borrowed_section_decoder_does_not_copy_payload() {
    let section = SnapshotSection::new(0x01, b"rows".to_vec()).expect("section");
    let bytes = encode_snapshot_section(&section).expect("encode section");
    let expected_payload = &bytes[SNAPSHOT_SECTION_HEADER_SIZE..];

    let (borrowed, consumed) = decode_snapshot_section_ref(&bytes).expect("decode ref");

    assert_eq!(borrowed.section_kind(), 0x01);
    assert_eq!(borrowed.payload(), expected_payload);
    assert_eq!(borrowed.payload().as_ptr(), expected_payload.as_ptr());
    assert_eq!(consumed, bytes.len());
}

#[test]
fn section_rejects_zero_kind() {
    assert_eq!(
        SnapshotSection::new(0, Vec::new()),
        Err(FormatError::InvalidValue {
            field: "snapshot_section_kind"
        })
    );

    let mut bytes =
        encode_snapshot_section(&SnapshotSection::new(0x01, Vec::new()).expect("section"))
            .expect("encode section");
    bytes[0] = 0;
    assert_eq!(
        decode_snapshot_section(&bytes),
        Err(FormatError::InvalidValue {
            field: "snapshot_section_kind"
        })
    );
}

#[test]
fn section_rejects_truncated_header_or_payload() {
    assert_eq!(
        decode_snapshot_section(&[0x01, 0x00]),
        Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_SECTION_FORMAT,
            needed: SNAPSHOT_SECTION_HEADER_SIZE,
            actual: 2
        })
    );

    let mut bytes =
        encode_snapshot_section(&SnapshotSection::new(0x01, b"rows".to_vec()).expect("section"))
            .expect("encode section");
    bytes.truncate(SNAPSHOT_SECTION_HEADER_SIZE + 1);
    assert_eq!(
        decode_snapshot_section(&bytes),
        Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_SECTION_FORMAT,
            needed: SNAPSHOT_SECTION_HEADER_SIZE + 4,
            actual: SNAPSHOT_SECTION_HEADER_SIZE + 1
        })
    );
}

#[test]
fn section_rejects_overflow_length_before_allocation() {
    let mut bytes = Vec::new();
    bytes.push(0x01);
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());

    assert_eq!(
        decode_snapshot_section(&bytes),
        Err(FormatError::InvalidLength {
            field: SNAPSHOT_SECTION_FORMAT
        })
    );
}

#[test]
fn materialized_section_decoder_rejects_payload_over_limit() {
    let bytes =
        encode_snapshot_section(&SnapshotSection::new(0x01, b"rows".to_vec()).expect("section"))
            .expect("encode section");

    assert_eq!(
        super::decode_snapshot_section_with_payload_limit(&bytes, 3),
        Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload"
        })
    );
}

#[test]
fn container_round_trips_with_footer_crc() {
    let container = SnapshotContainer::new(
        header(),
        vec![
            SnapshotSection::new(0x01, b"rows".to_vec()).expect("section"),
            SnapshotSection::new(0x80, Vec::new()).expect("section"),
        ],
    );
    let bytes = encode_snapshot_container(&container).expect("encode container");
    let stored_crc = u32::from_le_bytes(
        bytes[bytes.len() - SNAPSHOT_FOOTER_SIZE..]
            .try_into()
            .expect("crc bytes"),
    );
    let computed_crc = crc32fast::hash(&bytes[..bytes.len() - SNAPSHOT_FOOTER_SIZE]);

    assert_eq!(stored_crc, computed_crc);
    assert_eq!(decode_snapshot_container(&bytes), Ok(container));
}

#[test]
fn container_visitor_decodes_borrowed_sections() {
    let container = SnapshotContainer::new(
        header(),
        vec![
            SnapshotSection::new(0x01, b"rows".to_vec()).expect("section"),
            SnapshotSection::new(0x80, Vec::new()).expect("section"),
        ],
    );
    let bytes = encode_snapshot_container(&container).expect("encode container");
    let mut visited = Vec::new();

    let header = visit_snapshot_container_sections(&bytes, 2, |section| {
        visited.push((section.section_kind(), section.payload().to_vec()));
        Ok(())
    })
    .expect("visit container");

    assert_eq!(header, *container.header());
    assert_eq!(visited, vec![(0x01, b"rows".to_vec()), (0x80, Vec::new())]);
}

#[test]
fn container_rejects_materialized_payload_over_limit() {
    let container = SnapshotContainer::new(
        header(),
        vec![SnapshotSection::new(0x01, b"rows".to_vec()).expect("section")],
    );
    let bytes = encode_snapshot_container(&container).expect("encode container");

    assert_eq!(
        decode_snapshot_container_with_materialized_limits(
            &bytes,
            SnapshotMaterializedLimits {
                max_sections: 8,
                max_payload_bytes: 3,
            },
        ),
        Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload"
        })
    );
}

#[test]
fn encode_section_rejects_materialized_payload_over_limit() {
    // Encode must reject a section whose payload exceeds the materialization
    // ceiling, so we never write a section that decode would reject — encode and
    // decode stay symmetric.
    let section = SnapshotSection::new(0x01, b"rows".to_vec()).expect("section");
    assert_eq!(
        encode_snapshot_section_with_payload_limit(&section, 3),
        Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload"
        })
    );
    // Within the limit it still encodes.
    encode_snapshot_section_with_payload_limit(&section, 4).expect("encode within limit");
}

#[test]
fn encode_container_rejects_materialized_payload_over_limit() {
    // The cumulative section payload must be rejected at encode time when it
    // exceeds the ceiling decode enforces, so an unreadable container is never
    // written durably.
    let container = SnapshotContainer::new(
        header(),
        vec![
            SnapshotSection::new(0x01, b"aaa".to_vec()).expect("section"),
            SnapshotSection::new(0x02, b"bbb".to_vec()).expect("section"),
        ],
    );
    assert_eq!(
        encode_snapshot_container_with_materialized_limits(
            &container,
            SnapshotMaterializedLimits {
                max_sections: 8,
                max_payload_bytes: 5,
            },
        ),
        Err(FormatError::InvalidLength {
            field: "snapshot_materialized_payload"
        })
    );
    // The bytes a successful encode produces must round-trip through decode.
    let bytes = encode_snapshot_container_with_materialized_limits(
        &container,
        SnapshotMaterializedLimits {
            max_sections: 8,
            max_payload_bytes: 64,
        },
    )
    .expect("encode within limit");
    decode_snapshot_container(&bytes).expect("encoded container decodes");
}

#[test]
fn container_rejects_section_count_over_limit() {
    let section =
        encode_snapshot_section(&SnapshotSection::new(0x01, Vec::new()).expect("section"))
            .expect("encode section");
    let mut raw_sections = Vec::new();
    raw_sections.extend_from_slice(&section);
    raw_sections.extend_from_slice(&section);
    let bytes = snapshot_bytes_with_raw_sections(&raw_sections);

    assert_eq!(
        visit_snapshot_container_sections(&bytes, 1, |_section| Ok(())),
        Err(FormatError::InvalidLength {
            field: "snapshot_section_count"
        })
    );
    assert_eq!(
        decode_snapshot_container_with_materialized_limits(
            &bytes,
            SnapshotMaterializedLimits {
                max_sections: 1,
                max_payload_bytes: 64,
            },
        ),
        Err(FormatError::InvalidLength {
            field: "snapshot_section_count"
        })
    );
}

#[test]
fn container_encoder_rejects_section_count_over_limit() {
    let section = SnapshotSection::new(0x01, Vec::new()).expect("section");
    let container = SnapshotContainer::new(header(), vec![section; 4097]);

    assert_eq!(
        encode_snapshot_container(&container),
        Err(FormatError::InvalidLength {
            field: "snapshot_section_count"
        })
    );
}

#[test]
fn container_rejects_checksum_mismatch() {
    let container = SnapshotContainer::new(
        header(),
        vec![SnapshotSection::new(0x01, b"rows".to_vec()).expect("section")],
    );
    let mut bytes = encode_snapshot_container(&container).expect("encode container");
    bytes[SNAPSHOT_HEADER_SIZE + "identity".len() + SNAPSHOT_SECTION_HEADER_SIZE] ^= 0xff;

    assert!(matches!(
        decode_snapshot_container(&bytes),
        Err(FormatError::ChecksumMismatch {
            format: SNAPSHOT_FORMAT,
            ..
        })
    ));
}

#[test]
fn container_rejects_trailing_section_bytes_after_valid_crc() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes.extend_from_slice(&[0xaa, 0xbb]);
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    assert_eq!(
        decode_snapshot_container(&bytes),
        Err(FormatError::TrailingData {
            format: SNAPSHOT_SECTION_FORMAT,
            remaining: 2
        })
    );
}

#[test]
fn container_rejects_truncated_container() {
    assert_eq!(
        decode_snapshot_container(&SNAPSHOT_MAGIC),
        Err(FormatError::InsufficientBytes {
            format: SNAPSHOT_FORMAT,
            needed: SNAPSHOT_HEADER_SIZE + 1 + SNAPSHOT_FOOTER_SIZE,
            actual: 4
        })
    );
}

#[test]
fn container_section_errors_surface_after_crc_validation() {
    let mut bytes = encode_snapshot_header(&header()).expect("encode header");
    bytes.push(0);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    assert_eq!(
        decode_snapshot_container(&bytes),
        Err(FormatError::InvalidValue {
            field: "snapshot_section_kind"
        })
    );
}

#[test]
fn refresh_container_crc_helper_tracks_current_layout() {
    let container = SnapshotContainer::new(header(), Vec::new());
    let mut bytes = encode_snapshot_container(&container).expect("encode container");
    bytes[16..24].copy_from_slice(&43u64.to_le_bytes());
    refresh_container_crc(&mut bytes);

    assert_eq!(
        decode_snapshot_container(&bytes).expect("decode").header(),
        &SnapshotHeader::new(
            3,
            CommitVersion::new(43),
            Timestamp::from_micros(1_700_000_000_123_456),
            database_id(),
            "identity",
        )
        .expect("snapshot header")
    );
}

/// The materialized payload ceiling is a frozen V1 format limit (spec §12):
/// recovery's decode envelope and the checkpoint delta cap both derive from
/// it, so its value is pinned here rather than restated anywhere else.
#[test]
fn materialized_snapshot_payload_ceiling_is_sixty_four_mebibytes() {
    assert_eq!(super::MAX_MATERIALIZED_SNAPSHOT_PAYLOAD_BYTES, 67_108_864);
}
