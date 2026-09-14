use super::*;

#[test]
fn commit_batch_accepts_valid_storage_mutations_and_options() {
    let branch = branch_id(1);
    let keep_last = NonZeroUsize::new(2).expect("nonzero");
    let options = CommitBatchOptions::new(
        CommitDurabilityMode::Always,
        CommitConflictValidationMode::Skip,
        CommitDuplicateKeyPolicy::Reject,
        CommitTimestampPolicy::Explicit(Timestamp::from_micros(41)),
        CommitOrigin::Diagnostic,
    );
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::put(
                physical_key(branch, 0x20, b"a".to_vec()),
                b"alpha".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(physical_key(branch, 0x21, b"b".to_vec())),
            CommitMutation::put(
                physical_key(branch, 0x22, vec![0x00, 0xff]),
                Vec::new(),
                CommitExpiry::At(Timestamp::from_micros(90)),
                CommitRetentionHint::KeepLastNonZero(keep_last),
            ),
        ],
        CommitValidationFacts::empty(),
        options,
    );

    let validated = batch
        .validate(&CommitRuntimeConfig::default())
        .expect("valid commit batch");

    assert_eq!(validated.batch().branch_id(), branch);
    assert_eq!(validated.batch().kind(), super::CommitBatchKind::Mutating);
    assert_eq!(validated.batch().mutations().len(), 3);
    assert_eq!(
        validated.batch().options().durability(),
        CommitDurabilityMode::Always
    );
    assert_eq!(
        validated.batch().options().conflict_validation(),
        CommitConflictValidationMode::Skip
    );
    assert_eq!(
        validated.batch().options().duplicate_policy(),
        CommitDuplicateKeyPolicy::Reject
    );
    assert_eq!(
        validated.batch().options().timestamp_policy(),
        CommitTimestampPolicy::Explicit(Timestamp::from_micros(41))
    );
    assert_eq!(
        validated.batch().options().origin(),
        CommitOrigin::Diagnostic
    );
    assert_eq!(
        validated.batch().mutations()[0].value(),
        Some(b"alpha".as_slice())
    );
    assert_eq!(
        validated.batch().mutations()[2].retention(),
        Some(CommitRetentionHint::KeepLastNonZero(keep_last))
    );
}

#[test]
fn validated_batch_reports_admission_pressure_facts() {
    let branch = branch_id(33);
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(Some(2), None, Some(1), None)
                .expect("thresholds"),
        )
        .expect("config");
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::put(
                physical_key(branch, 0x20, b"pressure-put".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(physical_key(branch, 0x21, b"pressure-delete".to_vec())),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    let facts = batch
        .validate(&config)
        .expect("valid batch")
        .admission_pressure_facts(&config)
        .expect("pressure facts");

    assert_eq!(facts.mutations(), 2);
    assert_eq!(facts.puts(), 1);
    assert_eq!(facts.deletes(), 1);
    assert!(facts.approximate_commit_bytes() >= b"value".len());
    assert!(facts.under_pressure());
    assert!(facts.would_require_maintenance());
}

#[test]
fn validated_batch_reports_byte_based_admission_pressure_facts() {
    let branch = branch_id(34);
    let config = CommitRuntimeConfig::default()
        .with_admission_pressure_thresholds(
            CommitAdmissionPressureThresholds::new(None, Some(64), None, Some(128))
                .expect("thresholds"),
        )
        .expect("config");
    let batch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"byte-pressure-put".to_vec()),
            vec![0x7a; 256],
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    let facts = batch
        .validate(&config)
        .expect("valid batch")
        .admission_pressure_facts(&config)
        .expect("pressure facts");

    assert_eq!(facts.mutations(), 1);
    assert_eq!(facts.puts(), 1);
    assert_eq!(facts.deletes(), 0);
    assert!(facts.approximate_commit_bytes() >= 256);
    assert!(facts.under_pressure());
    assert!(facts.would_require_maintenance());
}

#[test]
fn commit_batch_options_cover_all_durability_modes() {
    let branch = branch_id(32);
    let cases = [
        CommitBatchOptions::new(
            CommitDurabilityMode::Cache,
            CommitConflictValidationMode::Validate,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
        CommitBatchOptions::new(
            CommitDurabilityMode::Standard,
            CommitConflictValidationMode::Skip,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::Explicit(Timestamp::from_micros(55)),
            CommitOrigin::Diagnostic,
        ),
        CommitBatchOptions::new(
            CommitDurabilityMode::Always,
            CommitConflictValidationMode::Validate,
            CommitDuplicateKeyPolicy::Reject,
            CommitTimestampPolicy::RuntimeGenerated,
            CommitOrigin::StorageRuntime,
        ),
    ];

    for (index, options) in cases.into_iter().enumerate() {
        let batch = CommitBatch::mutating(
            branch,
            vec![CommitMutation::delete(physical_key(
                branch,
                0x20 + u8::try_from(index).expect("small index"),
                vec![u8::try_from(index).expect("small index")],
            ))],
            CommitValidationFacts::empty(),
            options,
        )
        .validate(&CommitRuntimeConfig::default())
        .expect("valid options");

        assert_eq!(batch.batch().options(), options);
    }
}

#[test]
fn commit_batch_read_only_shape_is_valid_but_not_stampable() {
    let branch = branch_id(2);
    let validation = CommitValidationFacts::new(
        vec![CommitReadFact::new(
            physical_key(branch, 0x20, b"read".to_vec()),
            CommitObservedVersion::Missing,
        )],
        vec![CommitCasFact::new(
            physical_key(branch, 0x20, b"cas".to_vec()),
            CommitObservedVersion::Present(CommitVersion::new(9)),
        )],
    );
    let batch =
        CommitBatch::read_only_diagnostic(branch, validation, CommitBatchOptions::default());
    let validated = batch
        .validate(&CommitRuntimeConfig::default())
        .expect("valid read-only diagnostic batch");
    let stamp = CommitStamp::new(branch, CommitVersion::new(10), Timestamp::from_micros(11))
        .expect("stamp");

    assert_eq!(
        validated.batch().kind(),
        super::CommitBatchKind::ReadOnlyDiagnostic
    );
    assert_eq!(validated.batch().mutations().len(), 0);
    assert_eq!(validated.batch().validation().read_set().len(), 1);
    assert_eq!(validated.batch().validation().cas_set().len(), 1);
    assert_eq!(
        validated.stamp_user_rows(stamp),
        Err(CommitRuntimeError::InvalidBatch {
            reason: "read-only diagnostic batch cannot stamp rows",
        })
    );
}

#[test]
fn commit_batch_rejects_limit_overruns_before_stamping() {
    let branch = branch_id(3);
    let config =
        CommitRuntimeConfig::new(1, 1, 1, CommitReadOnlyDiagnostics::Enabled).expect("config");
    let too_many_mutations = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::delete(physical_key(branch, 0x20, b"a".to_vec())),
            CommitMutation::delete(physical_key(branch, 0x20, b"b".to_vec())),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let too_many_facts = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"a".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                physical_key(branch, 0x20, b"a".to_vec()),
                CommitObservedVersion::Missing,
            )],
            vec![CommitCasFact::new(
                physical_key(branch, 0x20, b"b".to_vec()),
                CommitObservedVersion::Missing,
            )],
        ),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        too_many_mutations.validate(&config),
        Err(CommitRuntimeError::InvalidBatch {
            reason: "mutation count exceeds configured limit",
        })
    );
    assert_eq!(
        too_many_facts.validate(&config),
        Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "validation fact count exceeds configured limit",
        })
    );
}

#[test]
fn commit_batch_accepts_exact_limits_and_space_scoped_duplicate_bytes() {
    let branch = branch_id(30);
    let shared_bytes = b"same-user-key".to_vec();
    let key_engine_a = physical_key(branch, 0x20, shared_bytes.clone());
    let key_engine_b = physical_key(branch, 0x21, shared_bytes);
    let config =
        CommitRuntimeConfig::new(2, 2, 2, CommitReadOnlyDiagnostics::Enabled).expect("config");
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::delete(key_engine_a.clone()),
            CommitMutation::delete(key_engine_b.clone()),
        ],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                key_engine_a,
                CommitObservedVersion::Missing,
            )],
            vec![CommitCasFact::new(
                key_engine_b,
                CommitObservedVersion::Present(CommitVersion::new(7)),
            )],
        ),
        CommitBatchOptions::default(),
    );

    let validated = batch.validate(&config).expect("exact limits are valid");

    assert_eq!(validated.batch().mutations().len(), 2);
    assert_eq!(validated.batch().validation().read_set().len(), 1);
    assert_eq!(validated.batch().validation().cas_set().len(), 1);
}

#[test]
fn commit_batch_allows_read_and_cas_facts_to_share_one_physical_key() {
    let branch = branch_id(31);
    let key = physical_key(branch, 0x20, b"observed".to_vec());
    let batch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x21,
            b"mutation".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                key.clone(),
                CommitObservedVersion::Missing,
            )],
            vec![CommitCasFact::new(
                key,
                CommitObservedVersion::Present(CommitVersion::new(8)),
            )],
        ),
        CommitBatchOptions::default(),
    );

    let validated = batch
        .validate(&CommitRuntimeConfig::default())
        .expect("read-set and CAS facts are independent validation dimensions");

    assert_eq!(validated.batch().validation().read_set().len(), 1);
    assert_eq!(validated.batch().validation().cas_set().len(), 1);
}

#[test]
fn commit_batch_rejects_malformed_batch_shape_and_branch_mismatch() {
    let branch = branch_id(4);
    let other = branch_id(5);

    let empty_mutating = CommitBatch::mutating(
        branch,
        Vec::new(),
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    assert_eq!(
        empty_mutating.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::InvalidBatch {
            reason: "mutating batch must contain at least one mutation",
        })
    );

    let wrong_mutation_branch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            other,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    assert_eq!(
        wrong_mutation_branch.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::BranchMismatch {
            expected: branch,
            actual: other,
        })
    );

    let wrong_fact_branch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                physical_key(other, 0x20, b"x".to_vec()),
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
        CommitBatchOptions::default(),
    );
    assert_eq!(
        wrong_fact_branch.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::BranchMismatch {
            expected: branch,
            actual: other,
        })
    );
}

#[test]
fn commit_batch_reports_branch_mismatch_before_duplicate_key_policy() {
    let branch = branch_id(37);
    let other = branch_id(38);
    let wrong_branch_key = physical_key(other, 0x20, b"dup".to_vec());
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::put(
                wrong_branch_key.clone(),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(wrong_branch_key),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        batch.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::BranchMismatch {
            expected: branch,
            actual: other,
        })
    );
}

#[test]
fn commit_batch_rejects_every_branch_mismatched_caller_surface() {
    let branch = branch_id(33);
    let other = branch_id(34);
    let good_mutation = CommitMutation::delete(physical_key(branch, 0x20, b"ok".to_vec()));
    let cases = [
        CommitBatch::mutating(
            branch,
            vec![CommitMutation::put(
                physical_key(other, 0x20, b"put".to_vec()),
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
            CommitValidationFacts::empty(),
            CommitBatchOptions::default(),
        ),
        CommitBatch::mutating(
            branch,
            vec![CommitMutation::delete(physical_key(
                other,
                0x20,
                b"delete".to_vec(),
            ))],
            CommitValidationFacts::empty(),
            CommitBatchOptions::default(),
        ),
        CommitBatch::mutating(
            branch,
            vec![good_mutation.clone()],
            CommitValidationFacts::new(
                vec![CommitReadFact::new(
                    physical_key(other, 0x20, b"read".to_vec()),
                    CommitObservedVersion::Missing,
                )],
                Vec::new(),
            ),
            CommitBatchOptions::default(),
        ),
        CommitBatch::mutating(
            branch,
            vec![good_mutation],
            CommitValidationFacts::new(
                Vec::new(),
                vec![CommitCasFact::new(
                    physical_key(other, 0x20, b"cas".to_vec()),
                    CommitObservedVersion::Missing,
                )],
            ),
            CommitBatchOptions::default(),
        ),
    ];

    for batch in cases {
        assert_eq!(
            batch.validate(&CommitRuntimeConfig::default()),
            Err(CommitRuntimeError::BranchMismatch {
                expected: branch,
                actual: other,
            })
        );
    }
}

#[test]
fn commit_batch_rejects_storage_owned_spaces_for_caller_inputs() {
    let branch = branch_id(6);
    let timeline = storage_owned_key(branch, b"timeline".to_vec());
    let put = CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            timeline.clone(),
            b"value".to_vec(),
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let delete = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(timeline.clone())],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let read_fact = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                timeline.clone(),
                CommitObservedVersion::Missing,
            )],
            Vec::new(),
        ),
        CommitBatchOptions::default(),
    );
    let cas_fact = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            Vec::new(),
            vec![CommitCasFact::new(
                timeline.clone(),
                CommitObservedVersion::Missing,
            )],
        ),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        put.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::StorageOwnedMutationSpace {
            space_id: StorageSpaceId::COMMIT_TIMELINE,
        })
    );
    assert_eq!(
        delete.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::StorageOwnedMutationSpace {
            space_id: StorageSpaceId::COMMIT_TIMELINE,
        })
    );
    assert_eq!(
        read_fact.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::StorageOwnedMutationSpace {
            space_id: StorageSpaceId::COMMIT_TIMELINE,
        })
    );
    assert_eq!(
        cas_fact.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::StorageOwnedMutationSpace {
            space_id: StorageSpaceId::COMMIT_TIMELINE,
        })
    );
    assert!(StorageSpaceId::from_raw(StorageSpaceId::INVALID.raw()).is_err());
    assert!(StorageSpaceId::engine(StorageSpaceId::COMMIT_TIMELINE.raw()).is_err());
}

#[test]
fn commit_batch_rejects_all_duplicate_mutation_shapes() {
    let branch = branch_id(7);
    let key = physical_key(branch, 0x20, b"dup".to_vec());
    let put = CommitMutation::put(
        key.clone(),
        b"one".to_vec(),
        CommitExpiry::None,
        CommitRetentionHint::Append,
    );
    let delete = CommitMutation::delete(key.clone());
    let other = CommitMutation::delete(physical_key(branch, 0x21, b"other".to_vec()));
    let cases = [
        vec![put.clone(), put.clone()],
        vec![put.clone(), delete.clone()],
        vec![delete.clone(), put.clone()],
        vec![delete.clone(), delete],
        vec![put, other, CommitMutation::delete(key.clone())],
    ];

    for mutations in cases {
        assert_eq!(
            CommitBatch::mutating(
                branch,
                mutations,
                CommitValidationFacts::empty(),
                CommitBatchOptions::default(),
            )
            .validate(&CommitRuntimeConfig::default()),
            Err(CommitRuntimeError::DuplicateMutationKey {
                space_id: StorageSpaceId::engine(0x20).expect("engine id"),
            })
        );
    }
}

#[test]
fn commit_batch_reports_first_later_duplicate_in_input_order() {
    let branch = branch_id(39);
    let first_pair = physical_key(branch, 0x20, b"first-pair".to_vec());
    let earlier_second_pair = physical_key(branch, 0x21, b"earlier-second-pair".to_vec());
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::delete(first_pair.clone()),
            CommitMutation::delete(earlier_second_pair.clone()),
            CommitMutation::put(
                earlier_second_pair,
                b"value".to_vec(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(first_pair),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        batch.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::DuplicateMutationKey {
            space_id: StorageSpaceId::engine(0x21).expect("engine id"),
        })
    );
}

#[test]
fn commit_batch_rejects_duplicate_validation_facts() {
    let branch = branch_id(7);
    let key = physical_key(branch, 0x20, b"dup".to_vec());
    let duplicate_read_facts = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![
                CommitReadFact::new(key.clone(), CommitObservedVersion::Missing),
                CommitReadFact::new(
                    key.clone(),
                    CommitObservedVersion::Present(CommitVersion::new(1)),
                ),
            ],
            Vec::new(),
        ),
        CommitBatchOptions::default(),
    );
    let duplicate_cas_facts = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            Vec::new(),
            vec![
                CommitCasFact::new(key.clone(), CommitObservedVersion::Missing),
                CommitCasFact::new(key, CommitObservedVersion::Present(CommitVersion::new(1))),
            ],
        ),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        duplicate_read_facts.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "duplicate read fact",
        })
    );
    assert_eq!(
        duplicate_cas_facts.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "duplicate cas fact",
        })
    );
}

#[test]
fn commit_batch_rejects_epoch_expiry_and_zero_present_observed_version() {
    let branch = branch_id(8);
    let epoch_expiry = CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            physical_key(branch, 0x20, b"ttl".to_vec()),
            b"value".to_vec(),
            CommitExpiry::At(Timestamp::EPOCH),
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let zero_observed = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![CommitReadFact::new(
                physical_key(branch, 0x20, b"x".to_vec()),
                CommitObservedVersion::Present(CommitVersion::ZERO),
            )],
            Vec::new(),
        ),
        CommitBatchOptions::default(),
    );

    assert_eq!(
        epoch_expiry.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::InvalidMutation {
            reason: "expiry at epoch is reserved as no expiry",
        })
    );
    assert_eq!(
        zero_observed.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::InvalidValidationFacts {
            reason: "missing observed version must use Missing",
        })
    );
}

#[test]
fn commit_validation_facts_accept_versions_and_preserve_order() {
    let branch = branch_id(35);
    let read_missing = physical_key(branch, 0x20, b"read-missing".to_vec());
    let read_present = physical_key(branch, 0x20, b"read-present".to_vec());
    let cas_missing = physical_key(branch, 0x20, b"cas-missing".to_vec());
    let cas_present = physical_key(branch, 0x20, b"cas-present".to_vec());
    let batch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x21,
            b"mutation".to_vec(),
        ))],
        CommitValidationFacts::new(
            vec![
                CommitReadFact::new(read_missing.clone(), CommitObservedVersion::Missing),
                CommitReadFact::new(
                    read_present.clone(),
                    CommitObservedVersion::Present(CommitVersion::new(11)),
                ),
            ],
            vec![
                CommitCasFact::new(cas_missing.clone(), CommitObservedVersion::Missing),
                CommitCasFact::new(
                    cas_present.clone(),
                    CommitObservedVersion::Present(CommitVersion::new(12)),
                ),
            ],
        ),
        CommitBatchOptions::default(),
    );

    let validated = batch
        .validate(&CommitRuntimeConfig::default())
        .expect("valid validation facts");

    assert_eq!(
        validated.batch().validation().read_set()[0].physical_key(),
        &read_missing
    );
    assert_eq!(
        validated.batch().validation().read_set()[1].observed(),
        CommitObservedVersion::Present(CommitVersion::new(11))
    );
    assert_eq!(
        validated.batch().validation().cas_set()[0].physical_key(),
        &cas_missing
    );
    assert_eq!(
        validated.batch().validation().cas_set()[1].expected(),
        CommitObservedVersion::Present(CommitVersion::new(12))
    );
}

#[test]
fn validated_commit_batch_stamps_rows_with_supplied_commit_facts() {
    let branch = branch_id(9);
    let commit_version = CommitVersion::new(42);
    let commit_timestamp = Timestamp::from_micros(1_700_000_000_001);
    let expiry = Timestamp::from_micros(1_700_000_000_099);
    let value = vec![0x00, 0xff, 0x41, 0x42];
    let keep_last = NonZeroUsize::new(3).expect("nonzero");
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::put(
                physical_key(branch, 0x20, b"a".to_vec()),
                value.clone(),
                CommitExpiry::At(expiry),
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(physical_key(branch, 0x20, b"b".to_vec())),
            CommitMutation::put(
                physical_key(branch, 0x21, b"empty".to_vec()),
                Vec::new(),
                CommitExpiry::None,
                CommitRetentionHint::KeepLastNonZero(keep_last),
            ),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    )
    .validate(&CommitRuntimeConfig::default())
    .expect("valid batch");
    let stamped = batch
        .stamp_user_rows(CommitStamp::new(branch, commit_version, commit_timestamp).expect("stamp"))
        .expect("stamped rows");

    assert_eq!(stamped.len(), 3);
    assert_eq!(
        batch
            .batch()
            .mutations()
            .iter()
            .map(CommitMutation::retention)
            .collect::<Vec<_>>(),
        vec![
            Some(CommitRetentionHint::Append),
            None,
            Some(CommitRetentionHint::KeepLastNonZero(keep_last))
        ]
    );
    assert_eq!(stamped[0].physical_key().user_key(), b"a");
    assert_eq!(stamped[0].commit_version(), commit_version);
    assert_eq!(stamped[0].commit_timestamp(), commit_timestamp);
    assert_eq!(stamped[0].expires_at(), expiry);
    assert_eq!(stamped[0].value(), value.as_slice());
    assert!(!stamped[0].is_tombstone());
    assert_eq!(stamped[1].physical_key().user_key(), b"b");
    assert!(stamped[1].is_tombstone());
    assert_eq!(stamped[1].expires_at(), Timestamp::EPOCH);
    assert_eq!(stamped[1].value(), b"");
    assert_eq!(stamped[2].physical_key().user_key(), b"empty");
    assert!(!stamped[2].is_tombstone());
    assert_eq!(stamped[2].expires_at(), Timestamp::EPOCH);
    assert_eq!(stamped[2].value(), b"");
}

#[test]
fn validated_commit_batch_preserves_order_and_large_opaque_values() {
    let branch = branch_id(36);
    let long_value: Vec<u8> = (0..=255).cycle().take(2048).collect();
    let batch = CommitBatch::mutating(
        branch,
        vec![
            CommitMutation::put(
                physical_key(branch, 0x20, b"first".to_vec()),
                vec![0x00, 0xff],
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::put(
                physical_key(branch, 0x20, b"second".to_vec()),
                long_value.clone(),
                CommitExpiry::None,
                CommitRetentionHint::Append,
            ),
            CommitMutation::delete(physical_key(branch, 0x20, b"third".to_vec())),
        ],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    )
    .validate(&CommitRuntimeConfig::default())
    .expect("valid batch");

    let stamped = batch
        .stamp_user_rows(
            CommitStamp::new(branch, CommitVersion::new(77), Timestamp::from_micros(88))
                .expect("stamp"),
        )
        .expect("stamped rows");

    assert_eq!(stamped.len(), 3);
    assert_eq!(stamped[0].physical_key().user_key(), b"first");
    assert_eq!(stamped[1].physical_key().user_key(), b"second");
    assert_eq!(stamped[2].physical_key().user_key(), b"third");
    assert_eq!(stamped[0].value(), &[0x00, 0xff]);
    assert_eq!(stamped[1].value(), long_value.as_slice());
    assert!(stamped[2].is_tombstone());
    assert!(stamped
        .iter()
        .all(|row| row.physical_key().storage_space_id().is_engine_owned()));
}

#[test]
fn commit_stamp_rejects_zero_version_and_branch_mismatch() {
    let branch = branch_id(10);
    let other = branch_id(11);
    let batch = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"x".to_vec(),
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    )
    .validate(&CommitRuntimeConfig::default())
    .expect("valid batch");

    assert_eq!(
        CommitStamp::new(branch, CommitVersion::ZERO, Timestamp::from_micros(1)),
        Err(CommitRuntimeError::InvalidCommitState {
            reason: "commit version must be nonzero",
        })
    );
    assert_eq!(
        batch.stamp_user_rows(
            CommitStamp::new(other, CommitVersion::new(1), Timestamp::from_micros(1))
                .expect("stamp")
        ),
        Err(CommitRuntimeError::BranchMismatch {
            expected: branch,
            actual: other,
        })
    );
}

#[test]
fn commit_batch_debug_and_errors_do_not_dump_value_bytes() {
    let branch = branch_id(12);
    let mutation = CommitMutation::put(
        physical_key(branch, 0x20, b"debug".to_vec()),
        b"secret-value".to_vec(),
        CommitExpiry::None,
        CommitRetentionHint::Append,
    );
    let debug = format!("{mutation:?}");
    let display = CommitRuntimeError::InvalidMutation {
        reason: "bad payload",
    }
    .to_string();

    assert!(debug.contains("value_len"));
    assert!(!debug.contains("secret-value"));
    assert!(!display.contains("secret-value"));
    assert!(!display.contains("VersionedValue"));
}

/// The cap is the durable format's, but the rule is the product's: a write
/// cache mode accepts must be a write durable mode accepts. Both modes reach
/// this validation, so pinning the boundary here pins it for both (#3391).
#[test]
fn commit_batch_admits_the_largest_encodable_row_and_refuses_one_byte_more() {
    let branch = branch_id(60);
    let key = physical_key(branch, 0x20, b"boundary".to_vec());
    let overhead = crate::format::storage_row_encoded_len(&key, 0);
    let largest_value = crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES - overhead;

    let at_limit = CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            key.clone(),
            vec![b'x'; largest_value],
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let over_limit = CommitBatch::mutating(
        branch,
        vec![CommitMutation::put(
            key,
            vec![b'x'; largest_value + 1],
            CommitExpiry::None,
            CommitRetentionHint::Append,
        )],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    at_limit
        .validate(&CommitRuntimeConfig::default())
        .expect("a row exactly at the limit is encodable, so it must be admitted");
    let refusal = over_limit
        .validate(&CommitRuntimeConfig::default())
        .expect_err("one byte past the limit is not encodable");
    assert_eq!(
        refusal,
        CommitRuntimeError::MutationTooLarge {
            row_len: crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES + 1,
            max_row_len: crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES,
        }
    );
    assert_eq!(refusal.code(), "invalid_argument.commit.mutation_row_size");
}

/// The key is escaped before it is written — every `0x00` costs two bytes — so
/// sizing the budget off the raw key length would admit a row the WAL encoder
/// then refuses, which is the very divergence this check exists to close.
///
/// Differential, so it cannot be satisfied by a length function that agrees
/// with itself: two keys of identical length and one identical value, where
/// only escaping separates the verdicts.
#[test]
fn commit_batch_sizes_a_zero_byte_key_at_its_escaped_length() {
    const KEY_LEN: usize = 4096;
    let branch = branch_id(61);
    let plain_key = physical_key(branch, 0x20, vec![b'k'; KEY_LEN]);
    let zero_key = physical_key(branch, 0x20, vec![0x00; KEY_LEN]);

    // The largest value the un-escaped key of this length can carry. The
    // escaped key is KEY_LEN bytes longer, so the same value overruns it.
    let value_len = crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES
        - crate::format::storage_row_encoded_len(&plain_key, 0);
    let batch_of = |key| {
        CommitBatch::mutating(
            branch,
            vec![CommitMutation::put(
                key,
                vec![b'x'; value_len],
                CommitExpiry::None,
                CommitRetentionHint::Append,
            )],
            CommitValidationFacts::empty(),
            CommitBatchOptions::default(),
        )
    };

    batch_of(plain_key)
        .validate(&CommitRuntimeConfig::default())
        .expect("a key with nothing to escape fits exactly");
    assert_eq!(
        batch_of(zero_key).validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::MutationTooLarge {
            row_len: crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES + KEY_LEN,
            max_row_len: crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES,
        })
    );
}

/// A delete carries no value, so only its key can push it over — and the key
/// cap is the one it reaches. The row cap is unreachable for a delete: a
/// tombstone row is its key plus a few dozen fixed bytes, so any key big
/// enough to breach 16 MiB breached 64 KiB long before (#3396).
#[test]
fn commit_batch_sizes_a_delete_by_its_key_alone() {
    let branch = branch_id(62);
    let modest = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            b"small".to_vec(),
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let huge = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            vec![b'k'; crate::format::MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES],
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    modest
        .validate(&CommitRuntimeConfig::default())
        .expect("an ordinary delete is unaffected");
    assert!(matches!(
        huge.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::MutationKeyTooLarge { .. })
    ));
}

/// `CommitRuntimeError` hand-rolls `PartialEq`, and every assertion in this
/// suite that compares a returned error against an expected one rests on it.
/// Those assertions are all `assert_eq!` on a *matching* pair, so an equality
/// that says yes too readily — a field comparison widened to `or`, or the whole
/// relation collapsed to `true` — passes every one of them. Only asserting
/// inequality witnesses it.
#[test]
fn commit_runtime_error_equality_distinguishes_every_field() {
    let refusal = CommitRuntimeError::MutationTooLarge {
        row_len: 100,
        max_row_len: 64,
    };

    assert_eq!(
        refusal,
        CommitRuntimeError::MutationTooLarge {
            row_len: 100,
            max_row_len: 64,
        }
    );
    assert_ne!(
        refusal,
        CommitRuntimeError::MutationTooLarge {
            row_len: 101,
            max_row_len: 64,
        }
    );
    assert_ne!(
        refusal,
        CommitRuntimeError::MutationTooLarge {
            row_len: 100,
            max_row_len: 65,
        }
    );
    assert_ne!(
        refusal,
        CommitRuntimeError::InvalidMutation {
            reason: "something else",
        }
    );

    // The key refusal carries two fields of its own, and each must break
    // equality alone — an `or` here would let a refusal quoting the wrong size
    // compare equal to the right one.
    let key_refusal = CommitRuntimeError::MutationKeyTooLarge {
        key_len: 100,
        max_key_len: 64,
    };
    assert_eq!(
        key_refusal,
        CommitRuntimeError::MutationKeyTooLarge {
            key_len: 100,
            max_key_len: 64,
        }
    );
    assert_ne!(
        key_refusal,
        CommitRuntimeError::MutationKeyTooLarge {
            key_len: 101,
            max_key_len: 64,
        }
    );
    assert_ne!(
        key_refusal,
        CommitRuntimeError::MutationKeyTooLarge {
            key_len: 100,
            max_key_len: 65,
        }
    );
    // And the row refusal and the key refusal are not each other, even at
    // identical sizes: they send the caller to change different things.
    assert_ne!(refusal, key_refusal);
    assert_ne!(
        CommitRuntimeError::InvalidBatch { reason: "a" },
        CommitRuntimeError::InvalidBatch { reason: "b" }
    );
}

/// How long `user_key_len` bytes of non-escaping user key actually encode to as
/// an internal key, measured by running the encoder. Test fixtures size
/// themselves from this so they stay anchored to the format rather than to the
/// helper the production check happens to use.
fn encoded_internal_key_len(branch: BranchId, user_key_len: usize) -> usize {
    crate::format::encode_internal_key(&crate::row::InternalKey::new(
        physical_key(branch, 0x20, vec![b'k'; user_key_len]),
        CommitVersion::new(1),
    ))
    .len()
}

/// The table format caps the encoded internal key at `MAX_TABLE_KEY_BYTES`,
/// and nothing on the write path checked it: an oversized key was admitted,
/// written to the WAL and acknowledged, and only refused when the memtable it
/// landed in was sealed into a table — at which point the branch stopped
/// accepting any write at all, including small unrelated ones, until the
/// database was reopened (#3396).
#[test]
fn commit_batch_admits_the_largest_buildable_key_and_refuses_one_byte_more() {
    let branch = branch_id(63);
    // Size the fixture from the ENCODER, never from the length helper under
    // test: a helper that forgot the version suffix would move the fixture
    // with it and the boundary would never actually be probed.
    let largest_user_key = crate::format::MAX_TABLE_KEY_BYTES - encoded_internal_key_len(branch, 0);
    assert_eq!(
        encoded_internal_key_len(branch, largest_user_key),
        crate::format::MAX_TABLE_KEY_BYTES,
        "fixture is not actually sitting on the cap"
    );

    let at_limit = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            vec![b'k'; largest_user_key],
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let over_limit = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            vec![b'k'; largest_user_key + 1],
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    at_limit
        .validate(&CommitRuntimeConfig::default())
        .expect("a key exactly at the cap builds, so it must be admitted");
    let refusal = over_limit
        .validate(&CommitRuntimeConfig::default())
        .expect_err("one byte past the cap can never be built into a table");
    assert_eq!(
        refusal,
        CommitRuntimeError::MutationKeyTooLarge {
            key_len: crate::format::MAX_TABLE_KEY_BYTES + 1,
            max_key_len: crate::format::MAX_TABLE_KEY_BYTES,
        }
    );
    assert_eq!(refusal.code(), "invalid_argument.commit.mutation_key_size");
}

/// Escaping counts here for the same reason it counts for the row: the table
/// builder measures the ENCODED key. Differential — two keys of identical
/// length where only escaping separates the verdicts.
#[test]
fn commit_batch_sizes_a_zero_byte_key_against_the_table_cap_when_escaped() {
    let branch = branch_id(64);
    let plain_len = crate::format::MAX_TABLE_KEY_BYTES - encoded_internal_key_len(branch, 0);

    let plain = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            vec![b'k'; plain_len],
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );
    let escaping = CommitBatch::mutating(
        branch,
        vec![CommitMutation::delete(physical_key(
            branch,
            0x20,
            vec![0x00; plain_len],
        ))],
        CommitValidationFacts::empty(),
        CommitBatchOptions::default(),
    );

    plain
        .validate(&CommitRuntimeConfig::default())
        .expect("a key with nothing to escape fits exactly");
    assert_eq!(
        escaping.validate(&CommitRuntimeConfig::default()),
        Err(CommitRuntimeError::MutationKeyTooLarge {
            key_len: crate::format::MAX_TABLE_KEY_BYTES + plain_len,
            max_key_len: crate::format::MAX_TABLE_KEY_BYTES,
        })
    );
}
