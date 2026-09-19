use super::*;
use crate::format::{
    encode_wal_record, encode_wal_record_envelope, FormatError, WalRecordEnvelope,
};
use crate::service::WalTruncation;
use strata_core::CommitVersion;

fn open_seeded_service<'a>(backend: &'a StoredWalBackend, records: &[WalRecord]) -> WalService<'a> {
    let segment = ObjectLayout::wal_segment(1).expect("segment one");
    backend
        .write_object(&segment, &segment_bytes(1, records))
        .expect("seed WAL segment");
    WalService::open(
        backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL")
}

#[test]
fn read_after_zero_commit_version_returns_all_records() {
    let backend = StoredWalBackend::new();
    let records = vec![
        record(1, b"first".to_vec()),
        record(2, b"second".to_vec()),
        record(3, b"third".to_vec()),
    ];
    let service = open_seeded_service(&backend, &records);

    let read = service
        .read_after_commit_version(CommitVersion::ZERO)
        .expect("read after zero watermark");

    assert_eq!(read.records(), records.as_slice());
    assert_eq!(read.truncation(), None);
}

#[test]
fn read_after_max_commit_version_returns_no_records() {
    let backend = StoredWalBackend::new();
    let records = vec![record(1, b"first".to_vec()), record(2, b"second".to_vec())];
    let service = open_seeded_service(&backend, &records);

    let read = service
        .read_after_commit_version(CommitVersion::MAX)
        .expect("read after max watermark");

    assert!(read.records().is_empty());
    assert_eq!(read.truncation(), None);
}

#[test]
fn read_all_preserves_multi_row_commit_payload_order() {
    let backend = StoredWalBackend::new();
    let record = multi_row_record(5, &[b"first", b"second", b"third"]);
    let service = open_seeded_service(&backend, std::slice::from_ref(&record));

    let read = service.read_all().expect("read multi-row record");

    assert_eq!(read.records(), std::slice::from_ref(&record));
    let rows = read.records()[0].commit_payload().rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].value(), b"first");
    assert_eq!(rows[1].value(), b"second");
    assert_eq!(rows[2].value(), b"third");
    assert_eq!(read.truncation(), None);
}

#[test]
fn read_after_duplicate_commit_versions_filters_by_version() {
    let backend = StoredWalBackend::new();
    let records = vec![
        record(1, b"version one".to_vec()),
        record(2, b"version two a".to_vec()),
        record(2, b"version two b".to_vec()),
        record(3, b"version three".to_vec()),
    ];
    let service = open_seeded_service(&backend, &records);

    let after_one = service
        .read_after_commit_version(CommitVersion::new(1))
        .expect("read duplicate versions after one");
    let after_two = service
        .read_after_commit_version(CommitVersion::new(2))
        .expect("read duplicate versions after two");

    assert_eq!(
        after_one.records(),
        &[records[1].clone(), records[2].clone(), records[3].clone()]
    );
    assert_eq!(after_two.records(), std::slice::from_ref(&records[3]));
}

#[test]
fn read_after_out_of_order_commit_versions_keeps_append_order() {
    let backend = StoredWalBackend::new();
    let records = vec![
        record(7, b"first appended".to_vec()),
        record(2, b"second appended".to_vec()),
        record(5, b"third appended".to_vec()),
        record(3, b"fourth appended".to_vec()),
    ];
    let service = open_seeded_service(&backend, &records);

    let all = service.read_all().expect("read all out-of-order records");
    let after_three = service
        .read_after_commit_version(CommitVersion::new(3))
        .expect("read out-of-order records after three");

    assert_eq!(all.records(), records.as_slice());
    assert_eq!(
        after_three.records(),
        &[records[0].clone(), records[2].clone()]
    );
}

#[test]
fn read_after_preserves_records_from_different_branch_ids() {
    let backend = StoredWalBackend::new();
    let other_branch = alternate_branch_id();
    let records = vec![
        record(1, b"default branch".to_vec()),
        record_for_branch(2, other_branch, b"other branch".to_vec()),
        record(3, b"default branch again".to_vec()),
    ];
    let service = open_seeded_service(&backend, &records);

    let read = service
        .read_after_commit_version(CommitVersion::new(1))
        .expect("read after branch-mixed records");

    // WAL is a physical log; branch interpretation belongs to commit replay.
    assert_eq!(read.records(), &[records[1].clone(), records[2].clone()]);
    assert_eq!(read.records()[0].branch_id(), other_branch);
    assert_eq!(
        read.records()[0].commit_payload().rows()[0]
            .physical_key()
            .branch_id(),
        other_branch
    );
    assert_eq!(read.truncation(), None);
}

/// #2567 S3a (#3319): collects the streaming visitor's output so it can be
/// held against the materializing read — the equivalence oracle for the
/// chunked reader.
fn visited_records(
    service: &WalService<'_>,
    watermark: CommitVersion,
) -> (Vec<WalRecord>, Option<WalTruncation>) {
    let mut records = Vec::new();
    let truncation = service
        .visit_records_after(watermark, &mut |record| {
            records.push(record.clone());
            std::ops::ControlFlow::Continue(())
        })
        .expect("visit records");
    (records, truncation)
}

/// The streaming visitor and `read_after_commit_version` must agree exactly —
/// records, order, and watermark filter — including a record large enough to
/// straddle the ranged-read chunk boundary (the case a whole-object read
/// never exercises).
#[test]
fn visit_records_after_matches_the_materializing_read() {
    let backend = StoredWalBackend::new();
    let records = vec![
        record(1, b"first".to_vec()),
        // Larger than one 4 MiB read chunk: decoding must refill mid-envelope.
        record(2, vec![b'x'; 6 * 1024 * 1024]),
        record(3, b"third".to_vec()),
        // A SECOND straddler: its refill runs with a consumed prefix in the
        // buffer, exercising the compaction offset bookkeeping (a wrong
        // `buffer_start` here decodes garbage).
        record(4, vec![b'y'; 6 * 1024 * 1024]),
        record(5, b"fifth".to_vec()),
    ];
    let service = open_seeded_service(&backend, &records);

    for watermark in [
        CommitVersion::ZERO,
        CommitVersion::new(1),
        CommitVersion::new(2),
        CommitVersion::new(4),
        CommitVersion::MAX,
    ] {
        let read = service
            .read_after_commit_version(watermark)
            .expect("materializing read");
        let (visited, truncation) = visited_records(&service, watermark);
        assert_eq!(visited, read.records(), "records diverge at {watermark:?}");
        assert_eq!(
            truncation.as_ref(),
            read.truncation(),
            "truncation diverges at {watermark:?}"
        );
    }
}

/// A torn final envelope on the latest segment: the visitor reports the same
/// repairable-tail fact, with the same offsets, as the materializing read.
#[test]
fn visit_records_after_reports_the_same_torn_tail() {
    let backend = StoredWalBackend::new();
    let intact = vec![record(1, b"first".to_vec()), record(2, b"second".to_vec())];
    let segment = ObjectLayout::wal_segment(1).expect("segment one");
    let mut bytes = segment_bytes(1, &intact);
    let torn = record_frame(&record(3, b"torn-tail".to_vec()));
    bytes.extend_from_slice(&torn[..torn.len() - 7]);
    backend
        .write_object(&segment, &bytes)
        .expect("seed torn WAL segment");
    let service = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    )
    .expect("open WAL with torn tail");

    let read = service
        .read_after_commit_version(CommitVersion::ZERO)
        .expect("materializing read");
    let (visited, truncation) = visited_records(&service, CommitVersion::ZERO);

    assert_eq!(visited, read.records());
    assert_eq!(visited, intact);
    assert_eq!(truncation.as_ref(), read.truncation());
    let truncation = truncation.expect("torn tail reported");
    assert_eq!(truncation.segment_id(), 1);
}

/// The read chunk is a perf knob, invisible to correctness tests by design
/// (any positive chunk decodes identically) — so the chosen value is pinned
/// here: large enough to amortize ranged reads, small against every budget.
#[test]
fn recovery_read_chunk_is_the_documented_four_mebibytes() {
    assert_eq!(
        crate::service::wal::RECOVERY_READ_CHUNK_BYTES,
        4 * 1024 * 1024
    );
}

/// Corrupt inner-record trailing bytes: the visitor reports the same typed
/// `TrailingData` fact as the materializing read, with the EXACT remaining
/// count — the field is forensic, not prose.
#[test]
fn visit_records_after_reports_exact_trailing_data() {
    let backend = StoredWalBackend::new();
    let intact = record(1, b"first".to_vec());
    let mut corrupt_inner = encode_wal_record(&record(2, b"second".to_vec())).expect("record");
    corrupt_inner.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
    let envelope = WalRecordEnvelope::new(corrupt_inner).expect("envelope");
    let segment = ObjectLayout::wal_segment(1).expect("segment one");
    let mut bytes = segment_bytes(1, std::slice::from_ref(&intact));
    bytes.extend_from_slice(&encode_wal_record_envelope(&envelope).expect("encode envelope"));
    backend
        .write_object(&segment, &bytes)
        .expect("seed corrupt WAL segment");
    let Err(error) = WalService::open(
        &backend,
        database_id(),
        1,
        DurabilityPolicy::Standard,
        WalServiceConfig::default(),
    ) else {
        panic!("open streams the active segment and must refuse the corruption");
    };
    let WalServiceError::Format { source, .. } = error else {
        panic!("expected a format error, got: {error:?}");
    };
    assert!(
        matches!(
            source,
            FormatError::TrailingData {
                format: "wal_record",
                remaining: 3,
            }
        ),
        "the trailing-byte count is exact: {source:?}"
    );
}

/// A range read that comes back short (the object holds fewer bytes than
/// its metadata claims) must fail closed with the exact absolute extent
/// actually fetched — forensic fields, not prose (#3319's chunked reader).
#[test]
fn short_range_read_reports_the_exact_fetched_extent() {
    let backend = StoredWalBackend::new();
    let records = vec![record(1, b"first".to_vec())];
    let service = open_seeded_service(&backend, &records);
    let segment = ObjectLayout::wal_segment(1).expect("segment one");
    let real_len = backend.read_object(&segment).expect("segment bytes").len() as u64;
    backend.set_metadata_size(&segment, real_len + 64);

    let Err(error) = service.visit_records_after(CommitVersion::ZERO, &mut |_record| {
        std::ops::ControlFlow::Continue(())
    }) else {
        panic!("a short range read must fail closed");
    };

    let WalServiceError::UnexpectedObjectSize {
        expected, actual, ..
    } = error
    else {
        panic!("expected UnexpectedObjectSize, got: {error:?}");
    };
    assert_eq!(expected, real_len + 64, "expected = the metadata's claim");
    assert_eq!(
        actual, real_len,
        "actual = the absolute extent really fetched"
    );
}
