//! Durable object layout and namespace ownership.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "object layout constructors are consumed by durable services added later"
    )
)]

use crate::object::{ObjectName, ObjectNameError, ObjectPrefix};
use std::fmt;

const MAX_TABLE_LEVEL: u32 = 9_999;
const FIXED_U64_COMPONENT_LEN: usize = 16;
const QUARANTINE_MANIFEST_OBJECT_ID: &str = "manifest";

/// Root-level file names whose presence identifies a pre-V1 database layout
/// (hard rule 42). V1 layouts keep the manifest inside a `manifest/`
/// directory and never create either name, so these are unambiguous markers.
/// Read only by `reject_pre_v1_layout`, which is `localfs`-only: without a
/// filesystem there is no on-disk layout to reject.
#[cfg(feature = "localfs")]
pub(crate) const PRE_V1_LAYOUT_MARKER_FILES: &[&str] = &["strata.toml", "MANIFEST"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectFamily {
    Manifest,
    Wal,
    Tables,
    Snapshots,
    Temporary,
    Quarantine,
    Locks,
    Meta,
}

impl ObjectFamily {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Wal => "wal",
            Self::Tables => "tables",
            Self::Snapshots => "snapshots",
            Self::Temporary => "tmp",
            Self::Quarantine => "quarantine",
            Self::Locks => "locks",
            Self::Meta => "meta",
        }
    }

    pub(crate) fn from_object_name(name: &ObjectName) -> Option<Self> {
        let family = name.as_str().split('/').next()?;
        match family {
            "manifest" => Some(Self::Manifest),
            "wal" => Some(Self::Wal),
            "tables" => Some(Self::Tables),
            "snapshots" => Some(Self::Snapshots),
            "tmp" => Some(Self::Temporary),
            "quarantine" => Some(Self::Quarantine),
            "locks" => Some(Self::Locks),
            "meta" => Some(Self::Meta),
            _ => None,
        }
    }
}

impl fmt::Display for ObjectFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LayoutError {
    InvalidObjectName(ObjectNameError),
    EmptyComponent {
        role: &'static str,
    },
    ComponentContainsSeparator {
        role: &'static str,
    },
    InvalidComponent {
        role: &'static str,
        source: ObjectNameError,
    },
    LevelOutOfRange {
        level: u32,
        max: u32,
    },
    InvalidObjectShape {
        family: ObjectFamily,
        reason: &'static str,
    },
}

impl fmt::Display for LayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObjectName(source) => write!(formatter, "invalid object name: {source}"),
            Self::EmptyComponent { role } => write!(formatter, "{role} component is empty"),
            Self::ComponentContainsSeparator { role } => {
                write!(formatter, "{role} component must not contain '/'")
            }
            Self::InvalidComponent { role, source } => {
                write!(formatter, "{role} component is invalid: {source}")
            }
            Self::LevelOutOfRange { level, max } => {
                write!(formatter, "table level {level} exceeds maximum {max}")
            }
            Self::InvalidObjectShape { family, reason } => {
                write!(formatter, "invalid {family} object shape: {reason}")
            }
        }
    }
}

impl std::error::Error for LayoutError {}

pub(crate) type LayoutResult<T> = Result<T, LayoutError>;

pub(crate) struct ObjectLayout;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ManifestObjectClassification {
    Database,
    BranchCatalog,
    PendingReleases,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TableObjectClassification<'a> {
    Manifest {
        branch_id: &'a str,
    },
    Data {
        branch_id: &'a str,
        level: u32,
        table_id: &'a str,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalObjectClassification {
    Segment { segment_id: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotObjectClassification {
    Snapshot { snapshot_id: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum QuarantineObjectClassification<'a> {
    Manifest {
        branch_id: &'a str,
    },
    Object {
        branch_id: &'a str,
        object_id: &'a str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QuarantineObjectShapeReason {
    Shape,
    ObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum QuarantineObjectShape<'a> {
    Manifest {
        branch_id: &'a str,
    },
    Object {
        branch_id: &'a str,
        object_id: &'a str,
    },
    Malformed {
        branch_id: Option<&'a str>,
        object_id: Option<&'a str>,
        reason: QuarantineObjectShapeReason,
    },
}

impl ObjectLayout {
    pub(crate) fn family_prefix(family: ObjectFamily) -> LayoutResult<ObjectPrefix> {
        object_prefix(&[family.as_str()])
    }

    pub(crate) fn manifest_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Manifest)
    }

    pub(crate) fn database_manifest() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Manifest.as_str(), "current"])
    }

    pub(crate) fn branch_catalog_manifest() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Manifest.as_str(), "branch-catalog"])
    }

    pub(crate) fn pending_releases_manifest() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Manifest.as_str(), "pending-releases"])
    }

    pub(crate) fn classify_manifest_object(
        object: &ObjectName,
    ) -> LayoutResult<Option<ManifestObjectClassification>> {
        let Some(components) = components_for_family(object, ObjectFamily::Manifest)? else {
            return Ok(None);
        };
        let [role] = components.as_slice() else {
            return Err(invalid_shape(
                ObjectFamily::Manifest,
                "manifest objects must have one role component",
            ));
        };

        let classification = match *role {
            "current" => ManifestObjectClassification::Database,
            "branch-catalog" => ManifestObjectClassification::BranchCatalog,
            "pending-releases" => ManifestObjectClassification::PendingReleases,
            _ => {
                return Err(invalid_shape(
                    ObjectFamily::Manifest,
                    "unknown manifest role",
                ));
            }
        };
        Ok(Some(classification))
    }

    pub(crate) fn wal_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Wal)
    }

    pub(crate) fn has_wal_segment_prefix(object: &ObjectName) -> bool {
        match Self::wal_prefix() {
            Ok(prefix) => object.as_str().starts_with(prefix.as_str()),
            Err(_) => false,
        }
    }

    pub(crate) fn wal_segment(segment_id: u64) -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Wal.as_str(), &fixed_u64(segment_id)])
    }

    pub(crate) fn classify_wal_object(
        object: &ObjectName,
    ) -> LayoutResult<Option<WalObjectClassification>> {
        let Some(components) = components_for_family(object, ObjectFamily::Wal)? else {
            return Ok(None);
        };
        let [segment] = components.as_slice() else {
            return Err(invalid_shape(
                ObjectFamily::Wal,
                "WAL segment objects must have one segment id component",
            ));
        };
        let segment_id = parse_fixed_u64_component(ObjectFamily::Wal, "segment id", segment)?;
        Ok(Some(WalObjectClassification::Segment { segment_id }))
    }

    pub(crate) fn wal_segment_metadata_prefix() -> LayoutResult<ObjectPrefix> {
        object_prefix(&[ObjectFamily::Meta.as_str(), ObjectFamily::Wal.as_str()])
    }

    pub(crate) fn has_wal_segment_metadata_prefix(object: &ObjectName) -> bool {
        match Self::wal_segment_metadata_prefix() {
            Ok(prefix) => object.as_str().starts_with(prefix.as_str()),
            Err(_) => false,
        }
    }

    pub(crate) fn wal_segment_metadata(segment_id: u64) -> LayoutResult<ObjectName> {
        object_name(&[
            ObjectFamily::Meta.as_str(),
            ObjectFamily::Wal.as_str(),
            &fixed_u64(segment_id),
        ])
    }

    /// The durable WAL segment-loss watermark: a single object recording the
    /// highest WAL segment id ever created (#2690). It lives in the `meta`
    /// family as a fixed singleton (`meta/wal-watermark`), deliberately outside
    /// the `wal/` and `meta/wal/` prefixes so segment/sidecar listings never
    /// mistake it for a segment.
    pub(crate) fn wal_watermark() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Meta.as_str(), "wal-watermark"])
    }

    pub(crate) fn table_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Tables)
    }

    pub(crate) fn branch_table_prefix(branch_id: &str) -> LayoutResult<ObjectPrefix> {
        validate_component("branch", branch_id)?;
        object_prefix(&[ObjectFamily::Tables.as_str(), branch_id])
    }

    pub(crate) fn branch_table_manifest(branch_id: &str) -> LayoutResult<ObjectName> {
        validate_component("branch", branch_id)?;
        object_name(&[ObjectFamily::Tables.as_str(), branch_id, "manifest"])
    }

    pub(crate) fn table_object(
        branch_id: &str,
        level: u32,
        table_id: &str,
    ) -> LayoutResult<ObjectName> {
        validate_component("branch", branch_id)?;
        validate_component("table", table_id)?;
        let level = level_component(level)?;
        object_name(&[ObjectFamily::Tables.as_str(), branch_id, &level, table_id])
    }

    pub(crate) fn classify_table_object(
        object: &ObjectName,
    ) -> LayoutResult<Option<TableObjectClassification<'_>>> {
        let Some(components) = components_for_family(object, ObjectFamily::Tables)? else {
            return Ok(None);
        };
        match components.as_slice() {
            [branch_id, "manifest"] => {
                let expected = Self::branch_table_manifest(branch_id)?;
                if &expected != object {
                    return Err(invalid_shape(
                        ObjectFamily::Tables,
                        "branch table manifest is not canonical",
                    ));
                }
                Ok(Some(TableObjectClassification::Manifest { branch_id }))
            }
            [branch_id, level, table_id] => {
                let level = parse_level_component(level)?;
                let expected = Self::table_object(branch_id, level, table_id)?;
                if &expected != object {
                    return Err(invalid_shape(
                        ObjectFamily::Tables,
                        "table data object is not canonical",
                    ));
                }
                Ok(Some(TableObjectClassification::Data {
                    branch_id,
                    level,
                    table_id,
                }))
            }
            _ => Err(invalid_shape(
                ObjectFamily::Tables,
                "table objects must be a branch manifest or table data object",
            )),
        }
    }

    pub(crate) fn snapshot_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Snapshots)
    }

    pub(crate) fn snapshot(snapshot_id: u64) -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Snapshots.as_str(), &fixed_u64(snapshot_id)])
    }

    pub(crate) fn classify_snapshot_object(
        object: &ObjectName,
    ) -> LayoutResult<Option<SnapshotObjectClassification>> {
        let Some(components) = components_for_family(object, ObjectFamily::Snapshots)? else {
            return Ok(None);
        };
        let [snapshot] = components.as_slice() else {
            return Err(invalid_shape(
                ObjectFamily::Snapshots,
                "snapshot objects must have one snapshot id component",
            ));
        };
        let snapshot_id =
            parse_fixed_u64_component(ObjectFamily::Snapshots, "snapshot id", snapshot)?;
        Ok(Some(SnapshotObjectClassification::Snapshot { snapshot_id }))
    }

    pub(crate) fn temporary_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Temporary)
    }

    pub(crate) fn operation_temporary_prefix(operation_id: &str) -> LayoutResult<ObjectPrefix> {
        validate_component("operation", operation_id)?;
        object_prefix(&[ObjectFamily::Temporary.as_str(), operation_id])
    }

    pub(crate) fn temporary_object(
        operation_id: &str,
        object_id: &str,
    ) -> LayoutResult<ObjectName> {
        validate_component("operation", operation_id)?;
        validate_component("temporary object", object_id)?;
        object_name(&[ObjectFamily::Temporary.as_str(), operation_id, object_id])
    }

    pub(crate) fn quarantine_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Quarantine)
    }

    pub(crate) fn branch_quarantine_prefix(branch_id: &str) -> LayoutResult<ObjectPrefix> {
        validate_component("branch", branch_id)?;
        object_prefix(&[ObjectFamily::Quarantine.as_str(), branch_id])
    }

    pub(crate) fn quarantine_manifest(branch_id: &str) -> LayoutResult<ObjectName> {
        validate_component("branch", branch_id)?;
        object_name(&[
            ObjectFamily::Quarantine.as_str(),
            branch_id,
            QUARANTINE_MANIFEST_OBJECT_ID,
        ])
    }

    pub(crate) const fn quarantine_inventory_object_id() -> &'static str {
        QUARANTINE_MANIFEST_OBJECT_ID
    }

    pub(crate) fn quarantine_object(branch_id: &str, object_id: &str) -> LayoutResult<ObjectName> {
        validate_component("branch", branch_id)?;
        validate_component("quarantine object", object_id)?;
        if object_id == QUARANTINE_MANIFEST_OBJECT_ID {
            return Err(invalid_shape(
                ObjectFamily::Quarantine,
                "quarantine object id is reserved for inventory manifest",
            ));
        }
        object_name(&[ObjectFamily::Quarantine.as_str(), branch_id, object_id])
    }

    pub(crate) fn classify_quarantine_object(
        object: &ObjectName,
    ) -> LayoutResult<Option<QuarantineObjectClassification<'_>>> {
        let Some(shape) = Self::classify_quarantine_object_shape(object) else {
            return Ok(None);
        };
        match shape {
            QuarantineObjectShape::Manifest { branch_id } => {
                Ok(Some(QuarantineObjectClassification::Manifest { branch_id }))
            }
            QuarantineObjectShape::Object {
                branch_id,
                object_id,
            } => Ok(Some(QuarantineObjectClassification::Object {
                branch_id,
                object_id,
            })),
            QuarantineObjectShape::Malformed { .. } => Err(invalid_shape(
                ObjectFamily::Quarantine,
                "quarantine objects must have branch and object components",
            )),
        }
    }

    pub(crate) fn classify_quarantine_object_shape(
        object: &ObjectName,
    ) -> Option<QuarantineObjectShape<'_>> {
        let mut components = object.as_str().split('/');
        let family = components.next()?;
        if family != ObjectFamily::Quarantine.as_str() {
            return None;
        }

        let branch_id = components.next();
        let object_id = components.next();
        let extra = components.next();
        match (branch_id, object_id, extra) {
            (None, _, _) => Some(QuarantineObjectShape::Malformed {
                branch_id: None,
                object_id: None,
                reason: QuarantineObjectShapeReason::Shape,
            }),
            (Some(branch_id), None, _) => Some(QuarantineObjectShape::Malformed {
                branch_id: Some(branch_id),
                object_id: None,
                reason: QuarantineObjectShapeReason::Shape,
            }),
            (Some(branch_id), Some(object_id), None)
                if object_id == QUARANTINE_MANIFEST_OBJECT_ID =>
            {
                Some(QuarantineObjectShape::Manifest { branch_id })
            }
            (Some(branch_id), Some(object_id), None) => Some(QuarantineObjectShape::Object {
                branch_id,
                object_id,
            }),
            (Some(branch_id), Some(object_id), Some(_)) => Some(QuarantineObjectShape::Malformed {
                branch_id: Some(branch_id),
                object_id: Some(object_id),
                reason: QuarantineObjectShapeReason::ObjectId,
            }),
        }
    }

    pub(crate) fn locks_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Locks)
    }

    pub(crate) fn writer_lock() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Locks.as_str(), "writer"])
    }

    pub(crate) fn meta_prefix() -> LayoutResult<ObjectPrefix> {
        Self::family_prefix(ObjectFamily::Meta)
    }

    pub(crate) fn database_meta() -> LayoutResult<ObjectName> {
        object_name(&[ObjectFamily::Meta.as_str(), "database"])
    }
}

fn fixed_u64(value: u64) -> String {
    // Fixed-width lowercase hex preserves numeric ordering in ordinary
    // lexicographic object listings.
    format!("{value:016x}")
}

fn parse_fixed_u64_component(
    family: ObjectFamily,
    role: &'static str,
    component: &str,
) -> LayoutResult<u64> {
    if component.len() != FIXED_U64_COMPONENT_LEN
        || !component
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid_shape(family, role));
    }

    u64::from_str_radix(component, 16).map_err(|_| invalid_shape(family, role))
}

fn level_component(level: u32) -> LayoutResult<String> {
    if level > MAX_TABLE_LEVEL {
        return Err(LayoutError::LevelOutOfRange {
            level,
            max: MAX_TABLE_LEVEL,
        });
    }

    Ok(format!("l{level:04}"))
}

fn parse_level_component(component: &str) -> LayoutResult<u32> {
    if component.len() != 5
        || component.as_bytes()[0] != b'l'
        || !component.as_bytes()[1..].iter().all(u8::is_ascii_digit)
    {
        return Err(invalid_shape(
            ObjectFamily::Tables,
            "table level must use l0000 encoding",
        ));
    }
    let level = component[1..]
        .parse::<u32>()
        .map_err(|_| invalid_shape(ObjectFamily::Tables, "table level is invalid"))?;
    if level > MAX_TABLE_LEVEL {
        return Err(LayoutError::LevelOutOfRange {
            level,
            max: MAX_TABLE_LEVEL,
        });
    }
    Ok(level)
}

fn components_for_family(
    object: &ObjectName,
    family: ObjectFamily,
) -> LayoutResult<Option<Vec<&str>>> {
    let mut components = object.as_str().split('/');
    let Some(actual_family) = components.next() else {
        return Ok(None);
    };
    if actual_family != family.as_str() {
        return Ok(None);
    }
    let components = components.collect::<Vec<_>>();
    if components.is_empty() {
        return Err(invalid_shape(family, "missing object role components"));
    }
    Ok(Some(components))
}

const fn invalid_shape(family: ObjectFamily, reason: &'static str) -> LayoutError {
    LayoutError::InvalidObjectShape { family, reason }
}

fn object_name(components: &[&str]) -> LayoutResult<ObjectName> {
    for component in components {
        validate_component("object-name", component)?;
    }
    ObjectName::new(components.join("/")).map_err(LayoutError::InvalidObjectName)
}

fn object_prefix(components: &[&str]) -> LayoutResult<ObjectPrefix> {
    for component in components {
        validate_component("object-prefix", component)?;
    }
    ObjectPrefix::new(format!("{}/", components.join("/"))).map_err(LayoutError::InvalidObjectName)
}

fn validate_component(role: &'static str, component: &str) -> LayoutResult<()> {
    // Components are validated before joining so a caller cannot smuggle an
    // extra path segment into a branch, table, or quarantine object name.
    if component.is_empty() {
        return Err(LayoutError::EmptyComponent { role });
    }
    if component.contains('/') {
        return Err(LayoutError::ComponentContainsSeparator { role });
    }

    ObjectName::new(component)
        .map(|_| ())
        .map_err(|source| LayoutError::InvalidComponent { role, source })
}

#[cfg(test)]
mod tests;
