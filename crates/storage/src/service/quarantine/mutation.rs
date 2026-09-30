use super::{
    require_capability, QuarantineService, QuarantineServiceError, QuarantineServiceResult,
};
use crate::backend::{
    Backend, BackendCapabilities, BackendCapability, BackendErrorKind, DeleteDurability,
    DeleteOutcome, DeleteStatus, PublishError, PublishFailureKind, PublishOutcome,
};
use crate::format::quarantine::{QuarantineEntry, QuarantineInventory};
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::ObjectName;
use crate::service::{durable_cleanup_failure, durable_cleanup_succeeded};
use strata_core::{BranchId, Timestamp};

mod types;

pub(crate) use types::{
    QuarantineDeleteOutcome, QuarantineGate, QuarantineObjectReport, QuarantineObjectRequest,
    QuarantineObjectStatus, QuarantinePublishFailure, QuarantinePurgeReport,
    QuarantinePurgeRequest,
};

impl QuarantineService<'_> {
    pub(crate) fn quarantine_object(
        &self,
        request: &QuarantineObjectRequest,
    ) -> QuarantineServiceResult<QuarantineObjectReport> {
        validate_gate(request.gate)?;
        validate_quarantine_request(request)?;
        require_capabilities(
            &self.backend,
            &[
                BackendCapability::ReadObject,
                BackendCapability::ObjectMetadata,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
                BackendCapability::DeleteObject,
            ],
        )?;

        let quarantine_object = quarantine_object_name(request.branch_id, &request.object_id)?;
        let inventory_load =
            self.load_inventory(request.branch_id, request.database_id, &request.codec_id)?;
        let entry = inventory_load
            .inventory()
            .entries()
            .iter()
            .find(|entry| entry.object_id() == request.object_id);

        if let Some(entry) = entry {
            let quarantine_bytes = read_object_optional(&self.backend, &quarantine_object)?;
            let source_bytes = read_object_optional(&self.backend, &request.source_object)?;
            return self.handle_existing_quarantine_entry(
                request,
                &quarantine_object,
                entry,
                quarantine_bytes,
                source_bytes,
                inventory_load.inventory().entries().len(),
            );
        }

        let quarantine_object_exists = object_exists(&self.backend, &quarantine_object)?;
        if quarantine_object_exists {
            return Err(QuarantineServiceError::InventoryMismatch {
                object_id: request.object_id.clone(),
                quarantine_object,
                source_object: request.source_object.clone(),
                reason: "quarantine object is not listed in inventory",
            });
        }
        self.handle_new_quarantine_entry(
            request,
            quarantine_object,
            inventory_load.inventory(),
            quarantine_stage_mode(self.backend.capabilities()),
        )
    }

    fn handle_new_quarantine_entry(
        &self,
        request: &QuarantineObjectRequest,
        quarantine_object: ObjectName,
        current_inventory: &QuarantineInventory,
        mode: QuarantineStageMode,
    ) -> Result<QuarantineObjectReport, QuarantineServiceError> {
        // #3721: a copy stage reads the source's bytes to re-publish them; a
        // link stage never reads them — the source's size is all the
        // inventory entry records, and the link re-uses the bytes in place.
        let (source_bytes, byte_count) = match mode {
            QuarantineStageMode::Copy => {
                let bytes = self.read_verified_source(request)?;
                let byte_count = bytes.len() as u64;
                (Some(bytes), byte_count)
            }
            QuarantineStageMode::Link => (None, self.source_size(request)?),
        };

        let updated_inventory =
            updated_inventory_with_entry(request, current_inventory, byte_count)?;

        self.publish_inventory_then_stage_source(
            request,
            quarantine_object,
            source_bytes.as_deref(),
            byte_count,
            &updated_inventory,
        )
    }

    /// The source's bytes, cross-checked against its metadata size.
    fn read_verified_source(
        &self,
        request: &QuarantineObjectRequest,
    ) -> Result<Vec<u8>, QuarantineServiceError> {
        let Some(source_bytes) = read_object_optional(&self.backend, &request.source_object)?
        else {
            return Err(QuarantineServiceError::Missing {
                object: request.source_object.clone(),
            });
        };
        let source_metadata = self
            .backend
            .object_metadata(&request.source_object)
            .map_err(|source| QuarantineServiceError::Metadata {
                object: request.source_object.clone(),
                source,
            })?;
        let byte_count = source_bytes.len() as u64;
        if source_metadata.size_bytes() != byte_count {
            return Err(QuarantineServiceError::BackendState {
                object: request.source_object.clone(),
                expected_size: byte_count,
                actual_size: source_metadata.size_bytes(),
            });
        }
        Ok(source_bytes)
    }

    /// #3721: the source's size from its metadata, for a link stage (which
    /// never reads the bytes); an absent source is `Missing`, as for a copy.
    fn source_size(
        &self,
        request: &QuarantineObjectRequest,
    ) -> Result<u64, QuarantineServiceError> {
        match self.backend.object_metadata(&request.source_object) {
            Ok(metadata) => Ok(metadata.size_bytes()),
            Err(source) if source.kind() == BackendErrorKind::NotFound => {
                Err(QuarantineServiceError::Missing {
                    object: request.source_object.clone(),
                })
            }
            Err(source) => Err(QuarantineServiceError::Metadata {
                object: request.source_object.clone(),
                source,
            }),
        }
    }

    /// #3721: make the quarantine object exist — a durable no-clobber link to
    /// the source where the backend has one, else a durable create-mode copy
    /// of `source_bytes`. Both leave the source in place (it is deleted only
    /// after this succeeds) and both are create-mode publishes of the
    /// quarantine name, so every crash state is one the copy stage already
    /// produces: quarantine absent, or quarantine present holding exactly the
    /// source's bytes (for a link, the same file). The outer error is a hard
    /// service failure; the inner one is a publish failure the caller reports.
    fn create_quarantine_object(
        &self,
        request: &QuarantineObjectRequest,
        quarantine_object: &ObjectName,
        source_bytes: Option<&[u8]>,
        byte_count: u64,
    ) -> Result<Result<PublishOutcome, PublishError>, QuarantineServiceError> {
        if let Some(source_bytes) = source_bytes {
            return Ok(self
                .publisher
                .publish_durable_create(quarantine_object, source_bytes));
        }
        let failure = match self
            .backend
            .link_object(&request.source_object, quarantine_object)
        {
            Ok(outcome) => return Ok(Ok(outcome)),
            Err(failure) => failure,
        };
        match link_failure_action(failure.kind(), failure.source_error().kind()) {
            LinkFailureAction::Report => Ok(Err(failure)),
            // Nothing became visible, so this is exactly the copy stage's
            // post-inventory starting state; copy from here.
            LinkFailureAction::FallBackToCopy => {
                let source_bytes = self.read_verified_source(request)?;
                if source_bytes.len() as u64 != byte_count {
                    return Err(QuarantineServiceError::BackendState {
                        object: request.source_object.clone(),
                        expected_size: byte_count,
                        actual_size: source_bytes.len() as u64,
                    });
                }
                Ok(self
                    .publisher
                    .publish_durable_create(quarantine_object, &source_bytes))
            }
        }
    }

    fn publish_inventory_then_stage_source(
        &self,
        request: &QuarantineObjectRequest,
        quarantine_object: ObjectName,
        source_bytes: Option<&[u8]>,
        byte_count: u64,
        updated_inventory: &QuarantineInventory,
    ) -> Result<QuarantineObjectReport, QuarantineServiceError> {
        // The inventory is published before object movement so reconciliation
        // can classify any later copy failure as an inventory/object mismatch
        // instead of losing track of an in-flight quarantine request.
        let inventory_write = match self.publish_inventory_replace(updated_inventory) {
            Ok(write) => write,
            Err(QuarantineServiceError::Publish { object, source }) => {
                let status = if publish_failure_is_uncertain(source.kind()) {
                    QuarantineObjectStatus::InventoryPublishUncertain
                } else {
                    QuarantineObjectStatus::InventoryPublishFailed
                };
                let mut report = QuarantineObjectReport::new(
                    status,
                    request,
                    quarantine_object,
                    byte_count,
                    updated_inventory.entries().len(),
                );
                report.inventory_publish_failure =
                    Some(QuarantinePublishFailure::new(object, source));
                return Ok(report);
            }
            Err(source) => return Err(source),
        };

        // Source bytes are deleted only after the quarantine object (a copy,
        // or a link to the same file) has been durably created. This preserves
        // at least one recoverable name for the bytes across every publish and
        // delete fault window.
        match self.create_quarantine_object(
            request,
            &quarantine_object,
            source_bytes,
            byte_count,
        )? {
            Ok(outcome) => {
                super::validate_publish_outcome(&quarantine_object, byte_count, &outcome).map_err(
                    |mismatch| QuarantineServiceError::InvalidPublishMetadata {
                        object: mismatch.object().clone(),
                        field: mismatch.field(),
                    },
                )?;
                let source_delete = delete_source(&self.backend, &request.source_object);
                let status = status_after_source_delete(&source_delete);
                let mut report = QuarantineObjectReport::new(
                    status,
                    request,
                    quarantine_object,
                    byte_count,
                    inventory_write.inventory().entries().len(),
                );
                report.inventory_write = Some(inventory_write);
                report.quarantine_publish_outcome = Some(outcome);
                report.source_delete = Some(source_delete);
                Ok(report)
            }
            Err(source) => {
                let status = if publish_failure_is_uncertain(source.kind()) {
                    // Both visibility-unknown and visible-but-unconfirmed
                    // publish windows share this report status. Callers that
                    // need the exact split must inspect the retained publish
                    // failure kind.
                    QuarantineObjectStatus::QuarantinePublishUncertain
                } else {
                    QuarantineObjectStatus::QuarantinePublishFailed
                };
                let mut report = QuarantineObjectReport::new(
                    status,
                    request,
                    quarantine_object.clone(),
                    byte_count,
                    inventory_write.inventory().entries().len(),
                );
                report.inventory_write = Some(inventory_write);
                report.quarantine_publish_failure =
                    Some(QuarantinePublishFailure::new(quarantine_object, source));
                Ok(report)
            }
        }
    }

    pub(crate) fn purge_quarantine(
        &self,
        request: QuarantinePurgeRequest,
    ) -> QuarantineServiceResult<QuarantinePurgeReport> {
        validate_gate(request.gate)?;
        let inventory =
            self.load_inventory(request.branch_id, request.database_id, &request.codec_id)?;
        validate_purge_inventory_token(&request, &inventory)?;
        let inventory_object = inventory.object().clone();
        let mut report = QuarantinePurgeReport::new(request.branch_id, inventory_object);
        if inventory.inventory().entries().is_empty() {
            return Ok(report);
        }

        require_capabilities(
            &self.backend,
            &[
                BackendCapability::DeleteObject,
                BackendCapability::DurablePublish,
                BackendCapability::DurableSync,
            ],
        )?;

        // Purge deletes only inventory-listed quarantine objects and rewrites
        // the inventory after attempting every delete. Delete failures are
        // represented as retained entries so callers can retry without
        // reconstructing reachability facts.
        for entry in inventory.inventory().entries() {
            let object = quarantine_object_name(request.branch_id, entry.object_id())?;
            match self.backend.delete_object(&object) {
                Ok(outcome)
                    if durable_cleanup_succeeded(&outcome)
                        && outcome.status() == DeleteStatus::Deleted =>
                {
                    report.reclaimed_bytes =
                        report.reclaimed_bytes.saturating_add(entry.byte_count());
                    report
                        .deleted
                        .push(QuarantineDeleteOutcome::from_outcome(outcome));
                }
                Ok(outcome) if durable_cleanup_succeeded(&outcome) => {
                    report.reclaimed_bytes =
                        report.reclaimed_bytes.saturating_add(entry.byte_count());
                    report
                        .already_missing
                        .push(QuarantineDeleteOutcome::from_outcome(outcome));
                }
                Ok(outcome) => {
                    report.retained_entries.push(entry.clone());
                    report.failed.push(QuarantineDeleteOutcome::failed(
                        object,
                        durable_cleanup_failure(&outcome),
                    ));
                }
                Err(source) if source.source_error().kind() == BackendErrorKind::NotFound => {
                    report.reclaimed_bytes =
                        report.reclaimed_bytes.saturating_add(entry.byte_count());
                    report
                        .already_missing
                        .push(QuarantineDeleteOutcome::from_outcome(
                            DeleteOutcome::already_missing(object, DeleteDurability::NonDurable),
                        ));
                }
                Err(source) => {
                    report.retained_entries.push(entry.clone());
                    report
                        .failed
                        .push(QuarantineDeleteOutcome::failed(object, source));
                }
            }
        }

        let rewritten = QuarantineInventory::new(
            request.database_id,
            request.branch_id,
            request.codec_id,
            report.retained_entries.clone(),
        )
        .map_err(|_| QuarantineServiceError::InvalidRequest { field: "inventory" })?;
        match self.publish_inventory_replace(&rewritten) {
            Ok(write) => report.inventory_write = Some(write),
            Err(QuarantineServiceError::Publish { object, source }) => {
                report.inventory_publish_failure =
                    Some(QuarantinePublishFailure::new(object, source));
            }
            Err(source) => return Err(source),
        }
        Ok(report)
    }

    fn handle_existing_quarantine_entry(
        &self,
        request: &QuarantineObjectRequest,
        quarantine_object: &ObjectName,
        entry: &QuarantineEntry,
        quarantine_bytes: Option<Vec<u8>>,
        source_bytes: Option<Vec<u8>>,
        entry_count: usize,
    ) -> QuarantineServiceResult<QuarantineObjectReport> {
        if entry.source_object() != &request.source_object {
            return Err(QuarantineServiceError::InventoryMismatch {
                object_id: request.object_id.clone(),
                quarantine_object: quarantine_object.clone(),
                source_object: request.source_object.clone(),
                reason: "inventory source object differs from request",
            });
        }

        let Some(quarantine_bytes) = quarantine_bytes else {
            return Err(QuarantineServiceError::InventoryMismatch {
                object_id: request.object_id.clone(),
                quarantine_object: quarantine_object.clone(),
                source_object: request.source_object.clone(),
                reason: "inventory entry has no quarantine object",
            });
        };

        let byte_count = entry.byte_count();
        if byte_count != quarantine_bytes.len() as u64 {
            return Err(QuarantineServiceError::InventoryMismatch {
                object_id: request.object_id.clone(),
                quarantine_object: quarantine_object.clone(),
                source_object: request.source_object.clone(),
                reason: "quarantine byte count differs from inventory",
            });
        }

        let Some(source_bytes) = source_bytes else {
            return Ok(QuarantineObjectReport::new(
                QuarantineObjectStatus::AlreadyQuarantined,
                request,
                quarantine_object.clone(),
                byte_count,
                entry_count,
            ));
        };

        if source_bytes != quarantine_bytes {
            return Err(QuarantineServiceError::InventoryMismatch {
                object_id: request.object_id.clone(),
                quarantine_object: quarantine_object.clone(),
                source_object: request.source_object.clone(),
                reason: "source and quarantine bytes differ",
            });
        }

        let source_delete = delete_source(&self.backend, &request.source_object);
        let status = status_after_retry_source_delete(&source_delete);
        let mut report = QuarantineObjectReport::new(
            status,
            request,
            quarantine_object.clone(),
            byte_count,
            entry_count,
        );
        report.source_delete = Some(source_delete);
        Ok(report)
    }
}

fn validate_purge_inventory_token(
    request: &QuarantinePurgeRequest,
    inventory: &super::QuarantineInventoryLoad,
) -> QuarantineServiceResult<()> {
    let Some(expected) = request.expected_inventory_token else {
        return Err(QuarantineServiceError::InvalidRequest {
            field: "inventory_token",
        });
    };
    if inventory.token() == expected {
        return Ok(());
    }
    Err(QuarantineServiceError::InventoryTokenMismatch {
        inventory_object: inventory.object().clone(),
    })
}

fn validate_gate(gate: QuarantineGate) -> QuarantineServiceResult<()> {
    if gate == QuarantineGate::Safe {
        return Ok(());
    }
    Err(QuarantineServiceError::UnsafeGate { gate })
}

fn validate_quarantine_request(request: &QuarantineObjectRequest) -> QuarantineServiceResult<()> {
    if request.object_id.is_empty() {
        return Err(QuarantineServiceError::InvalidRequest { field: "object_id" });
    }
    if request.object_id == ObjectLayout::quarantine_inventory_object_id() {
        return Err(QuarantineServiceError::InvalidRequest { field: "object_id" });
    }
    quarantine_object_name(request.branch_id, &request.object_id)?;
    if request.quarantined_at == Timestamp::EPOCH && !request.allow_epoch_timestamp {
        return Err(QuarantineServiceError::InvalidRequest {
            field: "quarantined_at",
        });
    }
    match ObjectFamily::from_object_name(&request.source_object) {
        Some(ObjectFamily::Quarantine) | None => Err(QuarantineServiceError::InvalidRequest {
            field: "source_object",
        }),
        Some(_) => Ok(()),
    }
}

fn updated_inventory_with_entry(
    request: &QuarantineObjectRequest,
    current_inventory: &QuarantineInventory,
    byte_count: u64,
) -> QuarantineServiceResult<QuarantineInventory> {
    let new_entry = QuarantineEntry::new(
        request.object_id.clone(),
        request.source_object.clone(),
        byte_count,
        request.quarantined_at,
    )
    .map_err(|_| QuarantineServiceError::InvalidRequest { field: "entry" })?;
    let mut entries = current_inventory.entries().to_vec();
    entries.push(new_entry);
    QuarantineInventory::new(
        request.database_id,
        request.branch_id,
        request.codec_id.clone(),
        entries,
    )
    .map_err(|_| QuarantineServiceError::InvalidRequest { field: "inventory" })
}

fn require_capabilities(
    backend: &dyn crate::backend::Backend,
    capabilities: &[BackendCapability],
) -> QuarantineServiceResult<()> {
    for capability in capabilities {
        require_capability(backend, *capability)?;
    }
    Ok(())
}

fn read_object_optional(
    backend: &dyn crate::backend::Backend,
    object: &ObjectName,
) -> QuarantineServiceResult<Option<Vec<u8>>> {
    match backend.read_object(object) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(source) if source.kind() == BackendErrorKind::NotFound => Ok(None),
        Err(source) => Err(QuarantineServiceError::Read {
            object: object.clone(),
            source,
        }),
    }
}

fn object_exists(
    backend: &dyn crate::backend::Backend,
    object: &ObjectName,
) -> QuarantineServiceResult<bool> {
    match backend.object_metadata(object) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == BackendErrorKind::NotFound => Ok(false),
        Err(source) => Err(QuarantineServiceError::Metadata {
            object: object.clone(),
            source,
        }),
    }
}

fn quarantine_object_name(
    branch_id: BranchId,
    object_id: &str,
) -> QuarantineServiceResult<ObjectName> {
    ObjectLayout::quarantine_object(&branch_id.to_string(), object_id)
        .map_err(|source| QuarantineServiceError::Layout { source })
}

fn delete_source(
    backend: &dyn crate::backend::Backend,
    source_object: &ObjectName,
) -> QuarantineDeleteOutcome {
    match backend.delete_object(source_object) {
        Ok(outcome) if durable_cleanup_succeeded(&outcome) => {
            QuarantineDeleteOutcome::from_outcome(outcome)
        }
        Ok(outcome) => QuarantineDeleteOutcome::failed(
            source_object.clone(),
            durable_cleanup_failure(&outcome),
        ),
        Err(source) if source.source_error().kind() == BackendErrorKind::NotFound => {
            QuarantineDeleteOutcome::from_outcome(DeleteOutcome::already_missing(
                source_object.clone(),
                DeleteDurability::NonDurable,
            ))
        }
        Err(source) => QuarantineDeleteOutcome::failed(source_object.clone(), source),
    }
}

fn status_after_source_delete(source_delete: &QuarantineDeleteOutcome) -> QuarantineObjectStatus {
    if source_delete.deleted {
        QuarantineObjectStatus::QuarantinedSourceDeleted
    } else if source_delete.already_missing {
        QuarantineObjectStatus::SourceAlreadyMissingAfterPublish
    } else {
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed
    }
}

fn status_after_retry_source_delete(
    source_delete: &QuarantineDeleteOutcome,
) -> QuarantineObjectStatus {
    if source_delete.deleted {
        QuarantineObjectStatus::SourceDeleteRetried
    } else if source_delete.already_missing {
        QuarantineObjectStatus::SourceAlreadyMissingAfterPublish
    } else {
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed
    }
}

/// #3721: how the quarantine object is created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QuarantineStageMode {
    /// A durable hard link to the source: no payload byte is written.
    Link,
    /// A durable create-mode copy of the source's bytes.
    Copy,
}

/// #3721: link where the backend advertises a durable link, copy elsewhere
/// (the in-memory and object-store shapes).
pub(super) const fn quarantine_stage_mode(
    capabilities: BackendCapabilities,
) -> QuarantineStageMode {
    if capabilities.contains(BackendCapability::DurableLink) {
        QuarantineStageMode::Link
    } else {
        QuarantineStageMode::Copy
    }
}

/// #3721: what a failed link means for the stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LinkFailureAction {
    /// This backend cannot link these names; nothing became visible, so copy.
    FallBackToCopy,
    /// A real failure: report it exactly as a failed copy publish is reported.
    Report,
}

/// #3721: a link that is unsupported here — the trait default (a wrapper that
/// forwards the capability but not the method), `EXDEV`/`ENOTSUP`
/// (`UnsupportedOperation`), or `EPERM` on a filesystem without hard links
/// (`PermissionDenied`) — and failed before anything became visible falls back
/// to the copy. Every other failure, and every failure at or after visibility,
/// is reported: falling back after a visible link would publish over it.
pub(super) const fn link_failure_action(
    kind: PublishFailureKind,
    source: BackendErrorKind,
) -> LinkFailureAction {
    match (kind, source) {
        (PublishFailureKind::Unsupported, _)
        | (
            PublishFailureKind::FailedBeforeVisibility,
            BackendErrorKind::UnsupportedOperation | BackendErrorKind::PermissionDenied,
        ) => LinkFailureAction::FallBackToCopy,
        _ => LinkFailureAction::Report,
    }
}

const fn publish_failure_is_uncertain(kind: PublishFailureKind) -> bool {
    matches!(
        kind,
        PublishFailureKind::VisibilityUnknown | PublishFailureKind::VisibleDurabilityUnconfirmed
    )
}
