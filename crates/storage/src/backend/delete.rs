use super::{BackendError, BackendErrorKind};
use crate::object::ObjectName;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteStatus {
    Deleted,
    AlreadyMissing,
}

impl DeleteStatus {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Deleted => "deleted",
            Self::AlreadyMissing => "already_missing",
        }
    }
}

impl fmt::Display for DeleteStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteDurability {
    Durable,
    NonDurable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeleteOutcome {
    object: ObjectName,
    status: DeleteStatus,
    durability: DeleteDurability,
}

impl DeleteOutcome {
    pub(crate) fn new(
        object: ObjectName,
        status: DeleteStatus,
        durability: DeleteDurability,
    ) -> Self {
        Self {
            object,
            status,
            durability,
        }
    }

    pub(crate) fn deleted(object: ObjectName, durability: DeleteDurability) -> Self {
        Self::new(object, DeleteStatus::Deleted, durability)
    }

    pub(crate) fn already_missing(object: ObjectName, durability: DeleteDurability) -> Self {
        Self::new(object, DeleteStatus::AlreadyMissing, durability)
    }

    pub(crate) fn from_removed(
        object: ObjectName,
        durability: DeleteDurability,
        removed: bool,
    ) -> Self {
        if removed {
            Self::deleted(object, durability)
        } else {
            Self::already_missing(object, durability)
        }
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        &self.object
    }

    pub(crate) const fn status(&self) -> DeleteStatus {
        self.status
    }

    pub(crate) const fn durability(&self) -> DeleteDurability {
        self.durability
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteFailureKind {
    FailedBeforeRemoval,
    RemovalUnknown,
    RemovedDurabilityUnconfirmed,
}

impl DeleteFailureKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::FailedBeforeRemoval => "failed_before_removal",
            Self::RemovalUnknown => "removal_unknown",
            Self::RemovedDurabilityUnconfirmed => "removed_durability_unconfirmed",
        }
    }
}

impl fmt::Display for DeleteFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeleteError {
    object: ObjectName,
    kind: DeleteFailureKind,
    source: BackendError,
}

impl DeleteError {
    pub(crate) fn new(object: ObjectName, kind: DeleteFailureKind, source: BackendError) -> Self {
        Self {
            object,
            kind,
            source,
        }
    }

    pub(crate) fn failed_before_removal(object: &ObjectName, source: BackendError) -> Self {
        Self::new(
            object.clone(),
            DeleteFailureKind::FailedBeforeRemoval,
            source,
        )
    }

    /// Constructed only by the `local_fs` backend.
    #[cfg(feature = "localfs")]
    pub(crate) fn removal_unknown(object: &ObjectName, source: BackendError) -> Self {
        Self::new(object.clone(), DeleteFailureKind::RemovalUnknown, source)
    }

    pub(crate) fn removed_durability_unconfirmed(
        object: &ObjectName,
        source: BackendError,
    ) -> Self {
        Self::new(
            object.clone(),
            DeleteFailureKind::RemovedDurabilityUnconfirmed,
            source,
        )
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        &self.object
    }

    pub(crate) const fn kind(&self) -> DeleteFailureKind {
        self.kind
    }

    pub(crate) const fn source_error(&self) -> &BackendError {
        &self.source
    }
}

impl fmt::Display for DeleteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "delete {} failed for {}: {}",
            self.kind, self.object, self.source
        )
    }
}

impl std::error::Error for DeleteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<DeleteError> for BackendError {
    fn from(error: DeleteError) -> Self {
        error.source
    }
}

pub(crate) type DeleteResult = Result<DeleteOutcome, DeleteError>;

pub(crate) fn unsupported_delete(object: &ObjectName) -> DeleteError {
    DeleteError::failed_before_removal(
        object,
        BackendError::new(
            BackendErrorKind::UnsupportedOperation,
            "backend does not support delete_object",
        ),
    )
}

#[cfg(any(test, feature = "testkit"))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn durable_delete_result(object: &ObjectName, removed: bool) -> DeleteResult {
    Ok(DeleteOutcome::from_removed(
        object.clone(),
        DeleteDurability::Durable,
        removed,
    ))
}

#[cfg(any(test, feature = "testkit"))]
pub(crate) fn failed_delete_result(object: &ObjectName, source: BackendError) -> DeleteResult {
    Err(DeleteError::failed_before_removal(object, source))
}

#[cfg(test)]
mod tests {
    use super::{
        BackendError, BackendErrorKind, DeleteDurability, DeleteError, DeleteFailureKind,
        DeleteOutcome, DeleteStatus,
    };
    use crate::object::ObjectName;
    use std::error::Error as _;

    fn object() -> ObjectName {
        ObjectName::new("tests/delete-object").expect("valid object name")
    }

    #[test]
    fn delete_status_names_are_stable() {
        assert_eq!(DeleteStatus::Deleted.name(), "deleted");
        assert_eq!(DeleteStatus::AlreadyMissing.name(), "already_missing");
    }

    #[test]
    fn delete_failure_kind_names_are_stable() {
        assert_eq!(
            DeleteFailureKind::FailedBeforeRemoval.name(),
            "failed_before_removal"
        );
        assert_eq!(DeleteFailureKind::RemovalUnknown.name(), "removal_unknown");
        assert_eq!(
            DeleteFailureKind::RemovedDurabilityUnconfirmed.name(),
            "removed_durability_unconfirmed"
        );
    }

    #[test]
    fn delete_outcome_reports_object_status_and_durability() {
        let deleted = DeleteOutcome::deleted(object(), DeleteDurability::Durable);
        assert_eq!(deleted.object().as_str(), "tests/delete-object");
        assert_eq!(deleted.status(), DeleteStatus::Deleted);
        assert_eq!(deleted.durability(), DeleteDurability::Durable);

        let missing = DeleteOutcome::already_missing(object(), DeleteDurability::NonDurable);
        assert_eq!(missing.object().as_str(), "tests/delete-object");
        assert_eq!(missing.status(), DeleteStatus::AlreadyMissing);
        assert_eq!(missing.durability(), DeleteDurability::NonDurable);
    }

    #[test]
    fn delete_outcome_from_removed_maps_boolean_removal_fact() {
        assert_eq!(
            DeleteOutcome::from_removed(object(), DeleteDurability::Durable, true).status(),
            DeleteStatus::Deleted
        );
        assert_eq!(
            DeleteOutcome::from_removed(object(), DeleteDurability::NonDurable, false).status(),
            DeleteStatus::AlreadyMissing
        );
    }

    #[test]
    fn delete_error_preserves_failure_window_source_error_and_error_chain() {
        let source = BackendError::new(BackendErrorKind::Unavailable, "storage unavailable");
        let error = DeleteError::failed_before_removal(&object(), source.clone());

        assert_eq!(error.object().as_str(), "tests/delete-object");
        assert_eq!(error.kind(), DeleteFailureKind::FailedBeforeRemoval);
        assert_eq!(error.source_error(), &source);
        assert_eq!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<BackendError>())
                .map(BackendError::kind),
            Some(BackendErrorKind::Unavailable)
        );
        assert_eq!(BackendError::from(error), source);
    }

    #[test]
    fn removed_durability_unconfirmed_is_failure_not_success() {
        let source = BackendError::new(BackendErrorKind::Interrupted, "sync interrupted");
        let error = DeleteError::removed_durability_unconfirmed(&object(), source);

        assert_eq!(
            error.kind(),
            DeleteFailureKind::RemovedDurabilityUnconfirmed
        );
        assert_eq!(error.object().as_str(), "tests/delete-object");
    }
}
