use super::{
    LayoutError, ManifestObjectClassification, ObjectFamily, ObjectLayout,
    QuarantineObjectClassification, QuarantineObjectShape, QuarantineObjectShapeReason,
    SnapshotObjectClassification, TableObjectClassification, TimelineSegmentId,
    WalObjectClassification, MAX_TABLE_LEVEL,
};
use crate::object::{ObjectName, ObjectNameError};

#[test]
fn manifest_lock_and_meta_objects_use_reserved_names() {
    assert_eq!(
        ObjectLayout::database_manifest()
            .expect("manifest")
            .as_str(),
        "manifest/current"
    );
    assert_eq!(
        ObjectLayout::branch_catalog_manifest()
            .expect("branch catalog")
            .as_str(),
        "manifest/branch-catalog"
    );
    assert_eq!(
        ObjectLayout::pending_releases_manifest()
            .expect("pending releases")
            .as_str(),
        "manifest/pending-releases"
    );
    assert_eq!(
        ObjectLayout::writer_lock().expect("writer lock").as_str(),
        "locks/writer"
    );
    assert_eq!(
        ObjectLayout::database_meta()
            .expect("database meta")
            .as_str(),
        "meta/database"
    );
    assert_eq!(
        ObjectLayout::wal_segment_metadata(2)
            .expect("segment metadata")
            .as_str(),
        "meta/wal/0000000000000002"
    );
    assert_eq!(
        ObjectLayout::wal_segment_metadata_prefix()
            .expect("segment metadata prefix")
            .as_str(),
        "meta/wal/"
    );
}

#[test]
fn manifest_classifier_recognizes_all_manifest_roles() {
    assert_eq!(
        ObjectLayout::classify_manifest_object(
            &ObjectLayout::database_manifest().expect("database manifest")
        ),
        Ok(Some(ManifestObjectClassification::Database))
    );
    assert_eq!(
        ObjectLayout::classify_manifest_object(
            &ObjectLayout::branch_catalog_manifest().expect("branch catalog")
        ),
        Ok(Some(ManifestObjectClassification::BranchCatalog))
    );
    assert_eq!(
        ObjectLayout::classify_manifest_object(
            &ObjectLayout::pending_releases_manifest().expect("pending releases")
        ),
        Ok(Some(ManifestObjectClassification::PendingReleases))
    );
    assert_eq!(
        ObjectLayout::classify_manifest_object(&ObjectLayout::wal_segment(1).expect("wal segment")),
        Ok(None)
    );

    for malformed in ["manifest/current/extra", "manifest/unknown"] {
        assert!(
            matches!(
                ObjectLayout::classify_manifest_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Manifest,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
}

#[test]
fn manifest_classifier_round_trips_to_canonical_objects() {
    let cases = [
        (
            ObjectLayout::database_manifest().expect("database manifest"),
            ManifestObjectClassification::Database,
        ),
        (
            ObjectLayout::branch_catalog_manifest().expect("branch catalog"),
            ManifestObjectClassification::BranchCatalog,
        ),
        (
            ObjectLayout::pending_releases_manifest().expect("pending releases"),
            ManifestObjectClassification::PendingReleases,
        ),
    ];

    for (object, expected) in cases {
        let classification =
            ObjectLayout::classify_manifest_object(&object).expect("manifest classification");
        assert_eq!(classification, Some(expected.clone()));
        let reconstructed = match expected {
            ManifestObjectClassification::Database => {
                ObjectLayout::database_manifest().expect("database manifest")
            }
            ManifestObjectClassification::BranchCatalog => {
                ObjectLayout::branch_catalog_manifest().expect("branch catalog")
            }
            ManifestObjectClassification::PendingReleases => {
                ObjectLayout::pending_releases_manifest().expect("pending releases")
            }
        };
        assert_eq!(reconstructed, object);
    }
}

#[test]
fn ordered_ids_use_fixed_width_lower_hex() {
    assert_eq!(
        ObjectLayout::wal_segment(2).expect("wal segment").as_str(),
        "wal/0000000000000002"
    );
    assert_eq!(
        ObjectLayout::wal_segment_metadata(2)
            .expect("segment metadata")
            .as_str(),
        "meta/wal/0000000000000002"
    );
    assert_eq!(
        ObjectLayout::wal_segment(16).expect("wal segment").as_str(),
        "wal/0000000000000010"
    );
    assert!(
        ObjectLayout::wal_segment(2).expect("two")
            < ObjectLayout::wal_segment(16).expect("sixteen")
    );
    assert_eq!(
        ObjectLayout::snapshot(u64::MAX).expect("snapshot").as_str(),
        "snapshots/ffffffffffffffff"
    );
}

#[test]
fn wal_segment_metadata_names_sort_like_segment_ids() {
    for left in sample_u64_values() {
        for right in sample_u64_values() {
            let left_name =
                ObjectLayout::wal_segment_metadata(left).expect("left segment metadata");
            let right_name =
                ObjectLayout::wal_segment_metadata(right).expect("right segment metadata");

            assert_eq!(left_name.cmp(&right_name), left.cmp(&right));
        }
    }
}

#[test]
fn wal_segment_names_sort_like_segment_ids() {
    for left in sample_u64_values() {
        for right in sample_u64_values() {
            let left_name = ObjectLayout::wal_segment(left).expect("left segment");
            let right_name = ObjectLayout::wal_segment(right).expect("right segment");

            assert_eq!(left_name.cmp(&right_name), left.cmp(&right));
        }
    }
}

#[test]
fn wal_classifier_recognizes_fixed_width_segment_ids() {
    let segment = ObjectLayout::wal_segment(0x2a).expect("segment");
    assert_eq!(
        ObjectLayout::classify_wal_object(&segment),
        Ok(Some(WalObjectClassification::Segment { segment_id: 0x2a }))
    );
    assert_eq!(
        ObjectLayout::classify_wal_object(
            &ObjectLayout::database_manifest().expect("database manifest")
        ),
        Ok(None)
    );

    for malformed in [
        "wal/2a",
        "wal/000000000000000",
        "wal/00000000000000000",
        "wal/000000000000002A",
        "wal/00000000000000zz",
        "wal/000000000000002a/extra",
    ] {
        assert!(
            matches!(
                ObjectLayout::classify_wal_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Wal,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
}

#[test]
fn wal_classifier_round_trips_to_canonical_objects() {
    for segment_id in sample_u64_values() {
        let object = ObjectLayout::wal_segment(segment_id).expect("wal segment");
        let classification =
            ObjectLayout::classify_wal_object(&object).expect("wal classification");
        assert_eq!(
            classification,
            Some(WalObjectClassification::Segment { segment_id })
        );
        let reconstructed = match classification.expect("wal segment role") {
            WalObjectClassification::Segment { segment_id } => {
                ObjectLayout::wal_segment(segment_id).expect("reconstructed segment")
            }
        };
        assert_eq!(reconstructed, object);
    }
}

#[test]
fn snapshot_names_sort_like_snapshot_ids() {
    for left in sample_u64_values() {
        for right in sample_u64_values() {
            let left_name = ObjectLayout::snapshot(left).expect("left snapshot");
            let right_name = ObjectLayout::snapshot(right).expect("right snapshot");

            assert_eq!(left_name.cmp(&right_name), left.cmp(&right));
        }
    }
}

#[test]
fn snapshot_classifier_recognizes_fixed_width_snapshot_ids() {
    let snapshot = ObjectLayout::snapshot(0x2a).expect("snapshot");
    assert_eq!(
        ObjectLayout::classify_snapshot_object(&snapshot),
        Ok(Some(SnapshotObjectClassification::Snapshot {
            snapshot_id: 0x2a
        }))
    );
    assert_eq!(
        ObjectLayout::classify_snapshot_object(
            &ObjectLayout::database_manifest().expect("database manifest")
        ),
        Ok(None)
    );

    for malformed in [
        "snapshots/2a",
        "snapshots/000000000000000",
        "snapshots/00000000000000000",
        "snapshots/000000000000002A",
        "snapshots/00000000000000zz",
        "snapshots/000000000000002a/extra",
    ] {
        assert!(
            matches!(
                ObjectLayout::classify_snapshot_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Snapshots,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
}

#[test]
fn snapshot_classifier_round_trips_to_canonical_objects() {
    for snapshot_id in sample_u64_values() {
        let object = ObjectLayout::snapshot(snapshot_id).expect("snapshot");
        let classification =
            ObjectLayout::classify_snapshot_object(&object).expect("snapshot classification");
        assert_eq!(
            classification,
            Some(SnapshotObjectClassification::Snapshot { snapshot_id })
        );
        let reconstructed = match classification.expect("snapshot role") {
            SnapshotObjectClassification::Snapshot { snapshot_id } => {
                ObjectLayout::snapshot(snapshot_id).expect("reconstructed snapshot")
            }
        };
        assert_eq!(reconstructed, object);
    }
}

/// #3643: a timeline segment is `timeline/<sealing snapshot id>/<ordinal>`,
/// both fixed-width lowercase hex, and round-trips through its classifier.
#[test]
fn timeline_segment_layout_round_trips_and_rejects_malformed_names() {
    let id = TimelineSegmentId {
        sealing_snapshot_id: 0x2a,
        ordinal: 3,
    };
    let object = ObjectLayout::timeline_segment(id).expect("segment");
    assert_eq!(
        object.as_str(),
        "timeline/000000000000002a/0000000000000003"
    );
    assert_eq!(
        ObjectLayout::classify_timeline_segment_object(&object),
        Ok(Some(id))
    );
    assert_eq!(
        ObjectFamily::from_object_name(&object),
        Some(ObjectFamily::Timeline)
    );
    for sealing_snapshot_id in sample_u64_values() {
        for ordinal in [0, 1, u64::MAX] {
            let id = TimelineSegmentId {
                sealing_snapshot_id,
                ordinal,
            };
            let object = ObjectLayout::timeline_segment(id).expect("segment");
            assert_eq!(
                ObjectLayout::classify_timeline_segment_object(&object),
                Ok(Some(id))
            );
        }
    }
    assert_eq!(
        ObjectLayout::classify_timeline_segment_object(&ObjectLayout::snapshot(1).expect("snap")),
        Ok(None),
        "another family is not a timeline segment"
    );
    for malformed in [
        "timeline/000000000000002a",
        "timeline/000000000000002a/3",
        "timeline/000000000000002A/0000000000000003",
        "timeline/000000000000002a/0000000000000003/extra",
        "timeline/00000000000000zz/0000000000000003",
    ] {
        assert!(
            matches!(
                ObjectLayout::classify_timeline_segment_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Timeline,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
}

#[test]
fn fixed_width_id_classifiers_leave_zero_policy_to_services() {
    assert_eq!(
        ObjectLayout::classify_wal_object(&ObjectLayout::wal_segment(0).expect("wal zero")),
        Ok(Some(WalObjectClassification::Segment { segment_id: 0 }))
    );
    assert_eq!(
        ObjectLayout::classify_snapshot_object(&ObjectLayout::snapshot(0).expect("snapshot zero")),
        Ok(Some(SnapshotObjectClassification::Snapshot {
            snapshot_id: 0
        }))
    );
}

#[test]
fn table_layout_uses_branch_level_and_table_components() {
    assert_eq!(
        ObjectLayout::table_object("branch0001", 12, "table00ff")
            .expect("table")
            .as_str(),
        "tables/branch0001/l0012/table00ff"
    );
    assert_eq!(
        ObjectLayout::branch_table_manifest("branch0001")
            .expect("manifest")
            .as_str(),
        "tables/branch0001/manifest"
    );
    assert_eq!(
        ObjectLayout::branch_table_prefix("branch0001")
            .expect("prefix")
            .as_str(),
        "tables/branch0001/"
    );
}

#[test]
fn table_classifier_recognizes_manifests_data_and_malformed_shapes() {
    let manifest = ObjectLayout::branch_table_manifest("branch0001").expect("manifest");
    assert_eq!(
        ObjectLayout::classify_table_object(&manifest),
        Ok(Some(TableObjectClassification::Manifest {
            branch_id: "branch0001"
        }))
    );

    let table = ObjectLayout::table_object("branch0001", 12, "table00ff").expect("table");
    assert_eq!(
        ObjectLayout::classify_table_object(&table),
        Ok(Some(TableObjectClassification::Data {
            branch_id: "branch0001",
            level: 12,
            table_id: "table00ff"
        }))
    );
    assert_eq!(
        ObjectLayout::classify_table_object(
            &ObjectLayout::database_manifest().expect("database manifest")
        ),
        Ok(None)
    );

    for malformed in [
        "tables/branch0001",
        "tables/branch0001/manifest/extra",
        "tables/branch0001/L0/table0001",
        "tables/branch0001/l0000/table0001/extra",
        "tables/branch0001/l10000/table0001",
    ] {
        assert!(
            matches!(
                ObjectLayout::classify_table_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Tables,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
}

#[test]
fn table_classifier_round_trips_to_canonical_objects() {
    let manifest = ObjectLayout::branch_table_manifest("branch0001").expect("manifest");
    let classification =
        ObjectLayout::classify_table_object(&manifest).expect("table manifest classification");
    assert_eq!(
        classification,
        Some(TableObjectClassification::Manifest {
            branch_id: "branch0001"
        })
    );
    let reconstructed = match classification.expect("table manifest role") {
        TableObjectClassification::Manifest { branch_id } => {
            ObjectLayout::branch_table_manifest(branch_id).expect("reconstructed manifest")
        }
        TableObjectClassification::Data { .. } => panic!("manifest classified as data"),
    };
    assert_eq!(reconstructed, manifest);

    for level in sample_table_levels() {
        let table = ObjectLayout::table_object("branch0001", *level, "table00ff").expect("table");
        let classification =
            ObjectLayout::classify_table_object(&table).expect("table data classification");
        assert_eq!(
            classification,
            Some(TableObjectClassification::Data {
                branch_id: "branch0001",
                level: *level,
                table_id: "table00ff"
            })
        );
        let reconstructed = match classification.expect("table data role") {
            TableObjectClassification::Data {
                branch_id,
                level,
                table_id,
            } => {
                ObjectLayout::table_object(branch_id, level, table_id).expect("reconstructed table")
            }
            TableObjectClassification::Manifest { .. } => panic!("data classified as manifest"),
        };
        assert_eq!(reconstructed, table);
    }
}

#[test]
fn table_layout_rejects_level_width_overflow() {
    assert!(matches!(
        ObjectLayout::table_object("branch0001", MAX_TABLE_LEVEL + 1, "table00ff"),
        Err(LayoutError::LevelOutOfRange {
            level: 10_000,
            max: MAX_TABLE_LEVEL
        })
    ));
}

#[test]
fn table_objects_stay_under_their_branch_prefix() {
    for branch in sample_valid_components() {
        for table in sample_valid_components() {
            for level in sample_table_levels() {
                let object =
                    ObjectLayout::table_object(branch, *level, table).expect("table object");
                let prefix = ObjectLayout::branch_table_prefix(branch).expect("branch prefix");
                let expected_level = format!("l{level:04}");
                let components: Vec<_> = object.as_str().split('/').collect();

                assert!(object.as_str().starts_with(prefix.as_str()));
                assert_eq!(
                    ObjectFamily::from_object_name(&object),
                    Some(ObjectFamily::Tables)
                );
                assert_eq!(
                    components,
                    vec!["tables", branch, expected_level.as_str(), table]
                );
            }
        }
    }
}

#[test]
fn temporary_and_quarantine_layouts_are_global_families() {
    assert_eq!(
        ObjectLayout::temporary_object("op0001", "target0002")
            .expect("temporary")
            .as_str(),
        "tmp/op0001/target0002"
    );
    assert_eq!(
        ObjectLayout::operation_temporary_prefix("op0001")
            .expect("temporary prefix")
            .as_str(),
        "tmp/op0001/"
    );
    assert_eq!(
        ObjectLayout::quarantine_object("branch0001", "table00ff")
            .expect("quarantine object")
            .as_str(),
        "quarantine/branch0001/table00ff"
    );
    assert_eq!(
        ObjectLayout::quarantine_manifest("branch0001")
            .expect("quarantine manifest")
            .as_str(),
        "quarantine/branch0001/manifest"
    );
    assert_eq!(
        ObjectLayout::branch_quarantine_prefix("branch0001")
            .expect("quarantine prefix")
            .as_str(),
        "quarantine/branch0001/"
    );
    assert_eq!(ObjectLayout::quarantine_inventory_object_id(), "manifest");
    assert!(matches!(
        ObjectLayout::quarantine_object(
            "branch0001",
            ObjectLayout::quarantine_inventory_object_id()
        ),
        Err(LayoutError::InvalidObjectShape {
            family: ObjectFamily::Quarantine,
            ..
        })
    ));
}

#[test]
fn quarantine_classifier_recognizes_manifest_object_and_malformed_shapes() {
    let manifest = ObjectLayout::quarantine_manifest("branch0001").expect("manifest");
    assert_eq!(
        ObjectLayout::classify_quarantine_object(&manifest),
        Ok(Some(QuarantineObjectClassification::Manifest {
            branch_id: "branch0001"
        }))
    );
    let quarantined =
        ObjectLayout::quarantine_object("branch0001", "table00ff").expect("quarantine object");
    assert_eq!(
        ObjectLayout::classify_quarantine_object(&quarantined),
        Ok(Some(QuarantineObjectClassification::Object {
            branch_id: "branch0001",
            object_id: "table00ff"
        }))
    );
    assert_eq!(
        ObjectLayout::classify_quarantine_object(
            &ObjectLayout::database_manifest().expect("database manifest")
        ),
        Ok(None)
    );

    for malformed in [
        "quarantine/branch0001",
        "quarantine/branch0001/table00ff/extra",
    ] {
        assert!(
            matches!(
                ObjectLayout::classify_quarantine_object(&object_name(malformed)),
                Err(LayoutError::InvalidObjectShape {
                    family: ObjectFamily::Quarantine,
                    ..
                })
            ),
            "{malformed} should be malformed"
        );
    }
    assert_eq!(
        ObjectLayout::classify_quarantine_object_shape(&object_name("quarantine/branch0001")),
        Some(QuarantineObjectShape::Malformed {
            branch_id: Some("branch0001"),
            object_id: None,
            reason: QuarantineObjectShapeReason::Shape
        })
    );
    assert_eq!(
        ObjectLayout::classify_quarantine_object_shape(&object_name(
            "quarantine/branch0001/table00ff/extra"
        )),
        Some(QuarantineObjectShape::Malformed {
            branch_id: Some("branch0001"),
            object_id: Some("table00ff"),
            reason: QuarantineObjectShapeReason::ObjectId
        })
    );
}

#[test]
fn quarantine_classifier_round_trips_to_canonical_objects() {
    let manifest = ObjectLayout::quarantine_manifest("branch0001").expect("manifest");
    let classification =
        ObjectLayout::classify_quarantine_object(&manifest).expect("quarantine classification");
    assert_eq!(
        classification,
        Some(QuarantineObjectClassification::Manifest {
            branch_id: "branch0001"
        })
    );
    let reconstructed = match classification.expect("quarantine manifest role") {
        QuarantineObjectClassification::Manifest { branch_id } => {
            ObjectLayout::quarantine_manifest(branch_id).expect("reconstructed manifest")
        }
        QuarantineObjectClassification::Object { .. } => panic!("manifest classified as object"),
    };
    assert_eq!(reconstructed, manifest);

    for object_id in sample_valid_components() {
        let object =
            ObjectLayout::quarantine_object("branch0001", object_id).expect("quarantine object");
        let classification =
            ObjectLayout::classify_quarantine_object(&object).expect("quarantine classification");
        assert_eq!(
            classification,
            Some(QuarantineObjectClassification::Object {
                branch_id: "branch0001",
                object_id
            })
        );
        let reconstructed = match classification.expect("quarantine object role") {
            QuarantineObjectClassification::Object {
                branch_id,
                object_id,
            } => ObjectLayout::quarantine_object(branch_id, object_id)
                .expect("reconstructed quarantine object"),
            QuarantineObjectClassification::Manifest { .. } => {
                panic!("object classified as manifest")
            }
        };
        assert_eq!(reconstructed, object);
    }
}

#[test]
fn temporary_objects_stay_under_their_operation_prefix() {
    for operation in sample_valid_components() {
        for object_id in sample_valid_components() {
            let object =
                ObjectLayout::temporary_object(operation, object_id).expect("temporary object");
            let prefix =
                ObjectLayout::operation_temporary_prefix(operation).expect("operation prefix");

            assert!(object.as_str().starts_with(prefix.as_str()));
            assert_eq!(
                ObjectFamily::from_object_name(&object),
                Some(ObjectFamily::Temporary)
            );
        }
    }
}

#[test]
fn quarantine_objects_stay_under_their_branch_prefix() {
    for branch in sample_valid_components() {
        for object_id in sample_valid_components() {
            let object =
                ObjectLayout::quarantine_object(branch, object_id).expect("quarantine object");
            let prefix = ObjectLayout::branch_quarantine_prefix(branch).expect("quarantine prefix");

            assert!(object.as_str().starts_with(prefix.as_str()));
            assert_eq!(
                ObjectFamily::from_object_name(&object),
                Some(ObjectFamily::Quarantine)
            );
        }
    }
}

#[test]
fn every_reserved_family_has_a_prefix() {
    let families = [
        (ObjectFamily::Manifest, "manifest/"),
        (ObjectFamily::Wal, "wal/"),
        (ObjectFamily::Tables, "tables/"),
        (ObjectFamily::Snapshots, "snapshots/"),
        (ObjectFamily::Timeline, "timeline/"),
        (ObjectFamily::Temporary, "tmp/"),
        (ObjectFamily::Quarantine, "quarantine/"),
        (ObjectFamily::Locks, "locks/"),
        (ObjectFamily::Meta, "meta/"),
    ];

    for (family, prefix) in families {
        assert_eq!(
            ObjectLayout::family_prefix(family)
                .expect("family prefix")
                .as_str(),
            prefix
        );
    }

    let direct_prefixes = [
        ObjectLayout::manifest_prefix().expect("manifest prefix"),
        ObjectLayout::wal_prefix().expect("wal prefix"),
        ObjectLayout::table_prefix().expect("table prefix"),
        ObjectLayout::snapshot_prefix().expect("snapshot prefix"),
        ObjectLayout::timeline_prefix().expect("timeline prefix"),
        ObjectLayout::temporary_prefix().expect("temporary prefix"),
        ObjectLayout::quarantine_prefix().expect("quarantine prefix"),
        ObjectLayout::locks_prefix().expect("locks prefix"),
        ObjectLayout::meta_prefix().expect("meta prefix"),
    ];
    let direct_prefixes: Vec<_> = direct_prefixes
        .iter()
        .map(crate::object::ObjectPrefix::as_str)
        .collect();

    assert_eq!(
        direct_prefixes,
        vec![
            "manifest/",
            "wal/",
            "tables/",
            "snapshots/",
            "timeline/",
            "tmp/",
            "quarantine/",
            "locks/",
            "meta/",
        ]
    );
}

#[test]
fn family_detection_uses_first_component_only() {
    let table = ObjectLayout::table_object("branch0001", 0, "table0001").expect("table");
    let quarantine =
        ObjectLayout::quarantine_object("branch0001", "table0001").expect("quarantine");

    assert_eq!(
        ObjectFamily::from_object_name(&table),
        Some(ObjectFamily::Tables)
    );
    assert_eq!(
        ObjectFamily::from_object_name(&quarantine),
        Some(ObjectFamily::Quarantine)
    );
}

#[test]
fn invalid_components_cannot_escape_layout_namespace() {
    assert!(matches!(
        ObjectLayout::table_object("branch/child", 0, "table0001"),
        Err(LayoutError::ComponentContainsSeparator { role: "branch" })
    ));
    assert!(matches!(
        ObjectLayout::temporary_object("op0001", ".."),
        Err(LayoutError::InvalidComponent {
            role: "temporary object",
            source: ObjectNameError::TraversalComponent
        })
    ));
    assert!(matches!(
        ObjectLayout::quarantine_object("", "table0001"),
        Err(LayoutError::EmptyComponent { role: "branch" })
    ));
    assert!(matches!(
        ObjectLayout::branch_table_manifest("branch:bad"),
        Err(LayoutError::InvalidComponent {
            role: "branch",
            source: ObjectNameError::InvalidByte(b':')
        })
    ));
}

#[test]
fn invalid_components_are_rejected_before_name_construction() {
    for component in sample_invalid_components() {
        assert!(ObjectLayout::branch_table_prefix(component).is_err());
        assert!(ObjectLayout::table_object(component, 0, "table0001").is_err());
        assert!(ObjectLayout::table_object("branch0001", 0, component).is_err());
        assert!(ObjectLayout::temporary_object(component, "target0001").is_err());
        assert!(ObjectLayout::quarantine_object("branch0001", component).is_err());
    }
}

#[test]
fn follower_state_names_are_not_part_of_target_layout() {
    let layout_texts = [
        ObjectLayout::database_manifest()
            .expect("database manifest")
            .to_string(),
        ObjectLayout::wal_segment(1)
            .expect("wal segment")
            .to_string(),
        ObjectLayout::branch_table_manifest("branch0001")
            .expect("branch table manifest")
            .to_string(),
        ObjectLayout::table_object("branch0001", 0, "table0001")
            .expect("table object")
            .to_string(),
        ObjectLayout::snapshot(1).expect("snapshot").to_string(),
        ObjectLayout::temporary_object("op0001", "target0001")
            .expect("temporary")
            .to_string(),
        ObjectLayout::quarantine_manifest("branch0001")
            .expect("quarantine manifest")
            .to_string(),
        ObjectLayout::quarantine_object("branch0001", "table0001")
            .expect("quarantine")
            .to_string(),
        ObjectLayout::writer_lock()
            .expect("writer lock")
            .to_string(),
        ObjectLayout::database_meta()
            .expect("database meta")
            .to_string(),
        ObjectLayout::wal_segment_metadata(1)
            .expect("segment metadata")
            .to_string(),
    ];
    let forbidden_fragments = [
        "follower_state",
        "follower_audit",
        "MANIFEST",
        "wal-",
        "snap-",
        "segments.manifest",
        "quarantine.manifest",
        "__quarantine__",
    ];

    for text in layout_texts {
        for forbidden in forbidden_fragments {
            assert!(
                !text.contains(forbidden),
                "layout text {text:?} contains retired fragment {forbidden:?}"
            );
        }
    }
}

fn sample_u64_values() -> Vec<u64> {
    let mut values = vec![0, 1, 2, 15, 16, 255, 256, u64::from(u32::MAX), u64::MAX];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for _ in 0..32 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        values.push(state);
    }
    values.sort_unstable();
    values.dedup();
    values
}

fn object_name(raw: &str) -> ObjectName {
    ObjectName::new(raw).expect("test object name should be valid")
}

fn sample_valid_components() -> &'static [&'static str] {
    &[
        "a",
        "branch0001",
        "table-0001",
        "object_0001",
        "ABCxyz019",
        "component-with-32-characters-ok",
    ]
}

fn sample_table_levels() -> &'static [u32] {
    &[0, 1, 9, 10, 99, 100, 999, 1_000, MAX_TABLE_LEVEL]
}

fn sample_invalid_components() -> &'static [&'static str] {
    &[
        "",
        ".",
        "..",
        "/absolute",
        "branch/child",
        "branch//child",
        "branch:child",
        "branch child",
        "branch\\child",
        "branch/current/",
    ]
}
