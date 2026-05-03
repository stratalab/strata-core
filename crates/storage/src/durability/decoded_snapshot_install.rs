//! Generic decoded-row snapshot install mechanics.
//!
//! This module intentionally starts after higher layers have decoded any
//! snapshot format or primitive-specific section. Its input is only storage
//! routing metadata plus already-decoded storage rows.

use std::collections::HashMap;

use strata_core::{BranchId, CommitVersion};

use crate::{DecodedSnapshotEntry, SegmentedStore, StorageError, TypeTag};

type RowIdentity<'a> = (BranchId, TypeTag, &'a str, &'a [u8], CommitVersion);

/// A complete generic decoded snapshot install plan.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StorageDecodedSnapshotInstallPlan {
    /// Decoded row groups to install.
    pub groups: Vec<StorageDecodedSnapshotInstallGroup>,
}

impl StorageDecodedSnapshotInstallPlan {
    /// Construct a plan from decoded row groups.
    pub fn new(groups: Vec<StorageDecodedSnapshotInstallGroup>) -> Self {
        Self { groups }
    }

    /// Returns true when the plan has no row groups.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

/// A decoded storage row group for one branch and storage family.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageDecodedSnapshotInstallGroup {
    /// Branch whose storage state receives these rows.
    pub branch_id: BranchId,
    /// Storage family identifier used by `SegmentedStore` key routing.
    pub type_tag: TypeTag,
    /// Already-decoded rows for this branch and storage family.
    pub entries: Vec<DecodedSnapshotEntry>,
}

impl StorageDecodedSnapshotInstallGroup {
    /// Construct a decoded row group.
    pub fn new(branch_id: BranchId, type_tag: TypeTag, entries: Vec<DecodedSnapshotEntry>) -> Self {
        Self {
            branch_id,
            type_tag,
            entries,
        }
    }
}

/// Input for installing a decoded snapshot plan into storage.
#[derive(Debug, Clone, Copy)]
pub struct StorageDecodedSnapshotInstallInput<'a> {
    /// Target store.
    pub storage: &'a SegmentedStore,
    /// Generic decoded row plan to install.
    pub plan: &'a StorageDecodedSnapshotInstallPlan,
}

/// Generic storage install stats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageDecodedSnapshotInstallStats {
    /// Number of groups installed.
    pub groups_installed: usize,
    /// Number of rows installed.
    pub rows_installed: usize,
}

/// Storage-local errors from decoded-row snapshot install.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageDecodedSnapshotInstallError {
    /// A row group had no entries.
    #[error("decoded snapshot install group {group_index} for branch {branch_id} storage family 0x{storage_family:02x} is empty")]
    EmptyGroup {
        /// Index of the invalid group in the plan.
        group_index: usize,
        /// Branch receiving the invalid group.
        branch_id: BranchId,
        /// Opaque storage family byte.
        storage_family: u8,
    },

    /// A decoded row had no storage space.
    #[error("decoded snapshot install group {group_index} entry {entry_index} for branch {branch_id} storage family 0x{storage_family:02x} key 0x{user_key} ({user_key_len} bytes) has an empty storage space")]
    EmptySpace {
        /// Index of the invalid group in the plan.
        group_index: usize,
        /// Index of the invalid entry inside the group.
        entry_index: usize,
        /// Branch receiving the invalid row.
        branch_id: BranchId,
        /// Short user-key diagnostic.
        user_key: String,
        /// Full user-key length in bytes.
        user_key_len: usize,
        /// Opaque storage family byte.
        storage_family: u8,
    },

    /// A decoded row used the reserved zero commit version.
    #[error("decoded snapshot install group {group_index} entry {entry_index} for branch {branch_id} space {space:?} storage family 0x{storage_family:02x} key 0x{user_key} ({user_key_len} bytes) has zero commit version")]
    ZeroVersion {
        /// Index of the invalid group in the plan.
        group_index: usize,
        /// Index of the invalid entry inside the group.
        entry_index: usize,
        /// Branch receiving the invalid row.
        branch_id: BranchId,
        /// Storage space containing the invalid row.
        space: String,
        /// Short user-key diagnostic.
        user_key: String,
        /// Full user-key length in bytes.
        user_key_len: usize,
        /// Opaque storage family byte.
        storage_family: u8,
    },

    /// The same branch, storage family, space, key, and version appeared twice.
    #[error("decoded snapshot install duplicate row for branch {branch_id} space {space:?} storage family 0x{storage_family:02x} key 0x{user_key} ({user_key_len} bytes) version {version}: first at group {first_group_index} entry {first_entry_index}, duplicate at group {duplicate_group_index} entry {duplicate_entry_index}")]
    DuplicateRow {
        /// First group index where the row identity appeared.
        first_group_index: usize,
        /// First entry index where the row identity appeared.
        first_entry_index: usize,
        /// Duplicate group index.
        duplicate_group_index: usize,
        /// Duplicate entry index inside the duplicate group.
        duplicate_entry_index: usize,
        /// Branch receiving the duplicate row.
        branch_id: BranchId,
        /// Storage space containing the duplicate row.
        space: String,
        /// Short user-key diagnostic.
        user_key: String,
        /// Full user-key length in bytes.
        user_key_len: usize,
        /// Duplicate commit version.
        version: CommitVersion,
        /// Opaque storage family byte.
        storage_family: u8,
    },

    /// The lower storage install failed.
    #[error("decoded snapshot install group {group_index} for branch {branch_id} storage family 0x{storage_family:02x} failed: {source}")]
    Install {
        /// Index of the group in the plan.
        group_index: usize,
        /// Branch receiving the failed group.
        branch_id: BranchId,
        /// Opaque storage family byte.
        storage_family: u8,
        /// Underlying storage failure.
        #[source]
        source: StorageError,
    },

    /// Lower storage reported that it installed fewer or more rows than submitted.
    #[error("decoded snapshot install group {group_index} for branch {branch_id} storage family 0x{storage_family:02x} installed {actual} rows, expected {expected}")]
    RowCountMismatch {
        /// Index of the group in the plan.
        group_index: usize,
        /// Branch receiving the failed group.
        branch_id: BranchId,
        /// Opaque storage family byte.
        storage_family: u8,
        /// Rows submitted to lower storage.
        expected: usize,
        /// Rows reported by lower storage.
        actual: usize,
    },
}

/// Install already-decoded snapshot rows into `SegmentedStore`.
///
/// This helper validates all generic groups before mutation. It does not know
/// which snapshot section, primitive DTO, or higher-layer subsystem produced
/// the rows.
pub fn install_decoded_snapshot_rows(
    input: StorageDecodedSnapshotInstallInput<'_>,
) -> Result<StorageDecodedSnapshotInstallStats, StorageDecodedSnapshotInstallError> {
    validate_plan(input.plan)?;

    let mut stats = StorageDecodedSnapshotInstallStats::default();
    for (group_index, group) in input.plan.groups.iter().enumerate() {
        let storage_family = group.type_tag.as_byte();
        let rows_expected = group.entries.len();
        let rows_installed = input
            .storage
            .install_snapshot_entries(group.branch_id, group.type_tag, &group.entries)
            .map_err(|source| StorageDecodedSnapshotInstallError::Install {
                group_index,
                branch_id: group.branch_id,
                storage_family,
                source,
            })?;
        if rows_installed != rows_expected {
            return Err(StorageDecodedSnapshotInstallError::RowCountMismatch {
                group_index,
                branch_id: group.branch_id,
                storage_family,
                expected: rows_expected,
                actual: rows_installed,
            });
        }
        stats.groups_installed += 1;
        // Trust lower storage only after verifying it installed every row the
        // group submitted.
        stats.rows_installed += rows_installed;
    }

    Ok(stats)
}

fn validate_plan(
    plan: &StorageDecodedSnapshotInstallPlan,
) -> Result<(), StorageDecodedSnapshotInstallError> {
    let mut seen_rows: HashMap<RowIdentity<'_>, (usize, usize)> = HashMap::new();

    for (group_index, group) in plan.groups.iter().enumerate() {
        let storage_family = group.type_tag.as_byte();
        if group.entries.is_empty() {
            return Err(StorageDecodedSnapshotInstallError::EmptyGroup {
                group_index,
                branch_id: group.branch_id,
                storage_family,
            });
        }

        for (entry_index, entry) in group.entries.iter().enumerate() {
            if entry.space.is_empty() {
                return Err(StorageDecodedSnapshotInstallError::EmptySpace {
                    group_index,
                    entry_index,
                    branch_id: group.branch_id,
                    user_key: user_key_preview(&entry.user_key),
                    user_key_len: entry.user_key.len(),
                    storage_family,
                });
            }
            if entry.version == CommitVersion::ZERO {
                return Err(StorageDecodedSnapshotInstallError::ZeroVersion {
                    group_index,
                    entry_index,
                    branch_id: group.branch_id,
                    space: entry.space.clone(),
                    user_key: user_key_preview(&entry.user_key),
                    user_key_len: entry.user_key.len(),
                    storage_family,
                });
            }

            let identity = (
                group.branch_id,
                group.type_tag,
                entry.space.as_str(),
                entry.user_key.as_slice(),
                entry.version,
            );
            if let Some(&(first_group_index, first_entry_index)) = seen_rows.get(&identity) {
                return Err(StorageDecodedSnapshotInstallError::DuplicateRow {
                    first_group_index,
                    first_entry_index,
                    duplicate_group_index: group_index,
                    duplicate_entry_index: entry_index,
                    branch_id: group.branch_id,
                    space: entry.space.clone(),
                    user_key: user_key_preview(&entry.user_key),
                    user_key_len: entry.user_key.len(),
                    version: entry.version,
                    storage_family,
                });
            }
            seen_rows.insert(identity, (group_index, entry_index));
        }
    }

    Ok(())
}

fn user_key_preview(user_key: &[u8]) -> String {
    const MAX_PREVIEW_BYTES: usize = 32;
    let mut preview = String::with_capacity(MAX_PREVIEW_BYTES * 2 + 3);
    for byte in user_key.iter().take(MAX_PREVIEW_BYTES) {
        use std::fmt::Write as _;
        let _ = write!(&mut preview, "{byte:02x}");
    }
    if user_key.len() > MAX_PREVIEW_BYTES {
        preview.push_str("...");
    }
    preview
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use strata_core::id::CommitVersion;
    use strata_core::Value;

    use super::*;
    use crate::{DecodedSnapshotValue, Key, Namespace};

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; 16])
    }

    fn live(space: &str, key: &[u8], value: Value, version: u64) -> DecodedSnapshotEntry {
        live_with_metadata(space, key, value, version, 1_700_000 + version, 0)
    }

    fn live_with_metadata(
        space: &str,
        key: &[u8],
        value: Value,
        version: u64,
        timestamp_micros: u64,
        ttl_ms: u64,
    ) -> DecodedSnapshotEntry {
        DecodedSnapshotEntry {
            space: space.to_string(),
            user_key: key.to_vec(),
            payload: DecodedSnapshotValue::Value(value),
            version: CommitVersion(version),
            timestamp_micros,
            ttl_ms,
        }
    }

    fn tombstone(space: &str, key: &[u8], version: u64) -> DecodedSnapshotEntry {
        DecodedSnapshotEntry {
            space: space.to_string(),
            user_key: key.to_vec(),
            payload: DecodedSnapshotValue::Tombstone,
            version: CommitVersion(version),
            timestamp_micros: 1_800_000 + version,
            ttl_ms: 0,
        }
    }

    fn key(branch_id: BranchId, space: &str, type_tag: TypeTag, user_key: &[u8]) -> Key {
        Key::new(
            Arc::new(Namespace::for_branch_space(branch_id, space)),
            type_tag,
            user_key.to_vec(),
        )
    }

    #[test]
    fn decoded_snapshot_install_installs_generic_groups_and_stats() {
        let storage = SegmentedStore::new();
        let branch_a = branch(1);
        let branch_b = branch(2);
        let plan = StorageDecodedSnapshotInstallPlan::new(vec![
            StorageDecodedSnapshotInstallGroup::new(
                branch_a,
                TypeTag::KV,
                vec![
                    live("alpha", b"a", Value::Int(10), 7),
                    live("alpha", b"b", Value::String("value-b".into()), 8),
                ],
            ),
            StorageDecodedSnapshotInstallGroup::new(
                branch_b,
                TypeTag::Json,
                vec![live("docs", b"doc-1", Value::Bool(true), 9)],
            ),
        ]);

        let stats = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap();

        assert_eq!(
            stats,
            StorageDecodedSnapshotInstallStats {
                groups_installed: 2,
                rows_installed: 3,
            }
        );

        let observed_a = storage
            .get_versioned(
                &key(branch_a, "alpha", TypeTag::KV, b"a"),
                CommitVersion::MAX,
            )
            .unwrap()
            .unwrap();
        assert_eq!(observed_a.value, Value::Int(10));
        assert_eq!(observed_a.version, strata_core::Version::txn(7));

        let observed_b = storage
            .get_versioned(
                &key(branch_b, "docs", TypeTag::Json, b"doc-1"),
                CommitVersion::MAX,
            )
            .unwrap()
            .unwrap();
        assert_eq!(observed_b.value, Value::Bool(true));
        assert_eq!(observed_b.version, strata_core::Version::txn(9));
    }

    #[test]
    fn decoded_snapshot_install_preserves_tombstones() {
        let storage = SegmentedStore::new();
        let branch_id = branch(3);
        let plan =
            StorageDecodedSnapshotInstallPlan::new(vec![StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![
                    live("default", b"deleted", Value::String("before".into()), 4),
                    tombstone("default", b"deleted", 5),
                ],
            )]);

        let stats = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap();
        assert_eq!(stats.rows_installed, 2);

        let deleted_key = key(branch_id, "default", TypeTag::KV, b"deleted");
        let before_delete = storage
            .get_versioned(&deleted_key, CommitVersion(4))
            .unwrap()
            .unwrap();
        assert_eq!(before_delete.value, Value::String("before".into()));

        let latest = storage
            .get_versioned(&deleted_key, CommitVersion::MAX)
            .unwrap();
        assert!(
            latest.is_none(),
            "decoded tombstone row must hide the older live row"
        );
    }

    #[test]
    fn decoded_snapshot_install_allows_empty_plan() {
        let storage = SegmentedStore::new();
        let plan = StorageDecodedSnapshotInstallPlan::default();

        let stats = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap();

        assert_eq!(stats, StorageDecodedSnapshotInstallStats::default());
    }

    #[test]
    fn decoded_snapshot_install_rejects_empty_group_before_mutation() {
        let storage = SegmentedStore::new();
        let branch_id = branch(4);
        let plan = StorageDecodedSnapshotInstallPlan::new(vec![
            StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![live("default", b"valid-before-invalid", Value::Int(1), 1)],
            ),
            StorageDecodedSnapshotInstallGroup::new(branch_id, TypeTag::Graph, Vec::new()),
        ]);

        let err = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            StorageDecodedSnapshotInstallError::EmptyGroup { group_index: 1, .. }
        ));
        let observed = storage
            .get_versioned(
                &key(branch_id, "default", TypeTag::KV, b"valid-before-invalid"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "preflight failure must not install earlier valid groups"
        );
    }

    #[test]
    fn decoded_snapshot_install_rejects_empty_space_before_mutation() {
        let storage = SegmentedStore::new();
        let branch_id = branch(5);
        let plan =
            StorageDecodedSnapshotInstallPlan::new(vec![StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![
                    live("default", b"valid-before-invalid", Value::Int(1), 1),
                    live("", b"invalid-space", Value::Int(2), 2),
                ],
            )]);

        let err = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            StorageDecodedSnapshotInstallError::EmptySpace {
                group_index: 0,
                entry_index: 1,
                ..
            }
        ));
        let observed = storage
            .get_versioned(
                &key(branch_id, "default", TypeTag::KV, b"valid-before-invalid"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "preflight failure must not install earlier valid rows in the same group"
        );
    }

    #[test]
    fn decoded_snapshot_install_rejects_zero_version_before_mutation() {
        let storage = SegmentedStore::new();
        let branch_id = branch(6);
        let plan =
            StorageDecodedSnapshotInstallPlan::new(vec![StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![
                    live("default", b"valid-before-invalid", Value::Int(1), 1),
                    live("default", b"zero-version", Value::Int(2), 0),
                ],
            )]);

        let err = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            StorageDecodedSnapshotInstallError::ZeroVersion {
                group_index: 0,
                entry_index: 1,
                ..
            }
        ));
        let observed = storage
            .get_versioned(
                &key(branch_id, "default", TypeTag::KV, b"valid-before-invalid"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "preflight failure must not install earlier valid rows"
        );
    }

    #[test]
    fn decoded_snapshot_install_rejects_duplicate_rows_before_mutation() {
        let storage = SegmentedStore::new();
        let branch_id = branch(7);
        let plan = StorageDecodedSnapshotInstallPlan::new(vec![
            StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![live("default", b"dup", Value::Int(1), 11)],
            ),
            StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![live("default", b"dup", Value::Int(2), 11)],
            ),
        ]);

        let err = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            StorageDecodedSnapshotInstallError::DuplicateRow {
                first_group_index: 0,
                first_entry_index: 0,
                duplicate_group_index: 1,
                duplicate_entry_index: 0,
                ..
            }
        ));
        let observed = storage
            .get_versioned(
                &key(branch_id, "default", TypeTag::KV, b"dup"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "preflight failure must not install the first duplicate row"
        );
    }

    #[test]
    fn decoded_snapshot_install_rejects_duplicate_rows_in_same_group() {
        let storage = SegmentedStore::new();
        let branch_id = branch(8);
        let plan =
            StorageDecodedSnapshotInstallPlan::new(vec![StorageDecodedSnapshotInstallGroup::new(
                branch_id,
                TypeTag::KV,
                vec![
                    live("default", b"dup", Value::Int(1), 11),
                    live("default", b"dup", Value::Int(2), 11),
                ],
            )]);

        let err = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            StorageDecodedSnapshotInstallError::DuplicateRow {
                first_group_index: 0,
                first_entry_index: 0,
                duplicate_group_index: 0,
                duplicate_entry_index: 1,
                ..
            }
        ));
        let observed = storage
            .get_versioned(
                &key(branch_id, "default", TypeTag::KV, b"dup"),
                CommitVersion::MAX,
            )
            .unwrap();
        assert!(
            observed.is_none(),
            "preflight failure must not install any duplicate rows"
        );
    }

    #[test]
    fn decoded_snapshot_install_preserves_metadata_and_current_version_across_groups() {
        let storage = SegmentedStore::new();
        let branch_a = branch(9);
        let branch_b = branch(10);
        let timestamp_micros = 4_200_000;
        let expired_timestamp_micros = 1_000_000;
        let plan = StorageDecodedSnapshotInstallPlan::new(vec![
            StorageDecodedSnapshotInstallGroup::new(
                branch_a,
                TypeTag::KV,
                vec![
                    live_with_metadata(
                        "default",
                        b"timed",
                        Value::String("kept timestamp".into()),
                        6,
                        timestamp_micros,
                        0,
                    ),
                    live_with_metadata(
                        "default",
                        b"expired",
                        Value::String("short ttl".into()),
                        7,
                        expired_timestamp_micros,
                        1,
                    ),
                ],
            ),
            StorageDecodedSnapshotInstallGroup::new(
                branch_b,
                TypeTag::Json,
                vec![live_with_metadata(
                    "docs",
                    b"high-version",
                    Value::Bool(true),
                    12,
                    5_000_000,
                    0,
                )],
            ),
        ]);

        let stats = install_decoded_snapshot_rows(StorageDecodedSnapshotInstallInput {
            storage: &storage,
            plan: &plan,
        })
        .unwrap();

        assert_eq!(
            stats,
            StorageDecodedSnapshotInstallStats {
                groups_installed: 2,
                rows_installed: 3,
            }
        );
        assert_eq!(storage.current_version(), CommitVersion(12));

        let timed = storage
            .get_at_timestamp(
                &key(branch_a, "default", TypeTag::KV, b"timed"),
                timestamp_micros,
            )
            .unwrap()
            .unwrap();
        assert_eq!(timed.timestamp.as_micros(), timestamp_micros);
        assert_eq!(timed.version, strata_core::Version::txn(6));
        assert_eq!(timed.value, Value::String("kept timestamp".into()));

        let expired = storage
            .get_at_timestamp(
                &key(branch_a, "default", TypeTag::KV, b"expired"),
                expired_timestamp_micros + 1_001,
            )
            .unwrap();
        assert!(
            expired.is_none(),
            "decoded snapshot install must preserve TTL metadata"
        );

        let high_version = storage
            .get_versioned(
                &key(branch_b, "docs", TypeTag::Json, b"high-version"),
                CommitVersion::MAX,
            )
            .unwrap()
            .unwrap();
        assert_eq!(high_version.version, strata_core::Version::txn(12));
    }
}
