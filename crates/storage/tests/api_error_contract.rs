//! Stripe-grade error contract conformance for the public storage API.
//!
//! Enforces the storage-owned half of the error contract in
//! `docs/architecture/v1-error-and-diagnostics-contract.md`: every public
//! `StorageApiError` variant must carry a typed class, a well-formed code whose
//! class prefix agrees with `class()`, a non-empty redacted human-readable
//! message, a non-empty mechanical remediation hint, and a preserved source
//! chain where one applies. Reference IDs, doc-link URLs, and user-facing
//! "suggested fix" phrasing are assigned at the engine/SDK boundary (the
//! reference ID at the log sink from an injected id source) and are enforced by
//! that layer's contract test, not here.
//!
//! Tests assert on class, code, and structured fields — never on display prose
//! (contract testing-plan rule: "no test asserts on display text when it can
//! assert on code and class").

#![deny(unsafe_code)]

use std::collections::BTreeSet;
use std::sync::Arc;

use strata_storage::api::{
    BranchId, CommitAdmissionPressureReason, CommitAdmissionPressureSeverity, StorageApiError,
    StorageApiErrorClass, StorageApiLowerLayer,
};

/// The source file that defines `StorageApiError`. The fixture's coverage is
/// checked against the variants *declared here*, not against a hand-typed
/// count: an integration test cannot match the `#[non_exhaustive]` enum
/// exhaustively, so the declaration itself is the code truth (#3701). The
/// lib's exhaustive `code()`/`class()`/`remediation()`/`Display` matches force
/// a new variant to be handled; this forces it to be sampled below.
const ERROR_SOURCE: &str = include_str!("../src/api/error.rs");

/// Variant names declared in `pub enum StorageApiError { .. }`, parsed from
/// [`ERROR_SOURCE`]. A variant is an identifier that opens a line at brace
/// depth 1 of the enum body; doc comments, attributes, and field lines (depth
/// 2) are skipped.
fn declared_variant_names() -> BTreeSet<String> {
    let start = ERROR_SOURCE
        .find("pub enum StorageApiError {")
        .expect("StorageApiError declaration must stay in src/api/error.rs");
    let mut names = BTreeSet::new();
    let mut depth = 0_usize;
    for line in ERROR_SOURCE[start..].lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("//") {
            continue;
        }
        if depth == 1 && !trimmed.starts_with('#') {
            let ident: String = trimmed
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.starts_with(|c: char| c.is_ascii_uppercase()) {
                names.insert(ident);
            }
        }
        depth += trimmed.matches('{').count();
        depth = depth
            .checked_sub(trimmed.matches('}').count())
            .expect("unbalanced braces while parsing StorageApiError");
        if depth == 0 {
            return names;
        }
    }
    panic!("StorageApiError declaration never closed");
}

/// The variant a sample actually constructs, read from its derived `Debug`
/// (`Variant { .. }` / `Variant`), so a fixture row cannot be mislabelled.
fn variant_name(error: &StorageApiError) -> String {
    format!("{error:?}")
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

/// Substrings that must never appear in a public message or remediation hint.
/// The full redaction matrix lives at the command/CLI/SDK surfaces; this is the
/// storage-layer floor.
const REDACTION_DENYLIST: &[&str] = &[
    "secret",
    "password",
    "api_key",
    "apikey",
    "bearer",
    "token",
    "access_key",
    "credential",
    "private_key",
];

fn branch() -> BranchId {
    BranchId::from_bytes([0x01; BranchId::BYTE_LEN])
}

fn io_source() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, "synthetic lower-layer failure")
}

/// One sample of every public variant, paired with the class it must report.
/// Source-bearing variants are constructed *with* a source so the source-chain
/// assertion is meaningful.
#[allow(
    clippy::too_many_lines,
    reason = "exhaustive public error fixture keeps one explicit sample per variant"
)]
fn sample_errors() -> Vec<(&'static str, StorageApiError, StorageApiErrorClass, bool)> {
    vec![
        (
            "InvalidArgument",
            StorageApiError::InvalidArgument {
                field: "maintenance_scheduling_policy",
                reason: "must select an owned backend handle",
            },
            StorageApiErrorClass::InvalidArgument,
            false,
        ),
        (
            "SizeLimitExceeded",
            StorageApiError::SizeLimitExceeded {
                field: "row",
                actual_bytes: 16 * 1024 * 1024 + 1,
                limit_bytes: 16 * 1024 * 1024,
                reason: "a single committed row exceeds the maximum encodable size",
            },
            StorageApiErrorClass::InvalidArgument,
            false,
        ),
        (
            "UnsupportedCapability",
            StorageApiError::UnsupportedCapability {
                capability: "commit_durability",
                reason: "cache runtime cannot satisfy durable commit requests",
            },
            StorageApiErrorClass::Unsupported,
            false,
        ),
        (
            "InvalidRuntimeState",
            StorageApiError::InvalidRuntimeState {
                reason: "commit requires an open runtime",
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "BranchNotFound",
            StorageApiError::BranchNotFound {
                branch_id: branch(),
            },
            StorageApiErrorClass::NotFound,
            false,
        ),
        (
            "BranchAlreadyExists",
            StorageApiError::BranchAlreadyExists {
                branch_id: branch(),
            },
            StorageApiErrorClass::AlreadyExists,
            false,
        ),
        (
            "BranchGenerationMismatch",
            StorageApiError::BranchGenerationMismatch {
                branch_id: branch(),
                expected: 2,
                actual: 1,
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "Conflict",
            StorageApiError::Conflict {
                branch_id: branch(),
                storage_space: Some(0x20),
                key_fingerprint: Some(0xdead_beef),
                user_key_len: Some(8),
                reason: "concurrent write to the same key",
            },
            StorageApiErrorClass::Conflict,
            false,
        ),
        (
            "RetainedHistoryUnavailable",
            StorageApiError::RetainedHistoryUnavailable {
                branch_id: branch(),
                reason: "version pruned by retention",
            },
            StorageApiErrorClass::HistoryUnavailable,
            false,
        ),
        (
            "TimestampHistoryUnavailable",
            StorageApiError::TimestampHistoryUnavailable {
                branch_id: branch(),
                reason: "timestamp predates covered history",
            },
            StorageApiErrorClass::HistoryUnavailable,
            false,
        ),
        (
            "DurableUncertain",
            StorageApiError::durable_uncertain_with(
                "wal sync outcome unknown after fault",
                io_source(),
            ),
            StorageApiErrorClass::AmbiguousCommit,
            true,
        ),
        (
            "RecoveryDegraded",
            StorageApiError::RecoveryDegraded {
                reason: "lossy recovery fallback used",
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "MaintenanceRejected",
            StorageApiError::MaintenanceRejected {
                reason: "conflicting maintenance in flight",
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "StoragePressure",
            StorageApiError::StoragePressure {
                branch_id: branch(),
                severity: CommitAdmissionPressureSeverity::Blocking,
                pressure_reason: CommitAdmissionPressureReason::NonZeroLevelTableBacklog,
                reason: "mutating commit admission requires maintenance progress",
                retryable: true,
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "ResourceExhausted",
            StorageApiError::ResourceExhausted {
                resource: "active_mutable",
                requested_bytes: 48 * 1024,
                used_bytes: 64 * 1024,
                limit_bytes: 70 * 1024,
                reason: "commit would exceed the database memory budget",
            },
            StorageApiErrorClass::ResourceExhausted,
            false,
        ),
        (
            "WriterLockHeld",
            StorageApiError::WriterLockHeld,
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "BranchHasDependentChildren",
            StorageApiError::BranchHasDependentChildren {
                branch_id: branch(),
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "IncompatibleLayout",
            StorageApiError::IncompatibleLayout {
                reason: "pre-V1 layout marker present",
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "IncompatibleFormat",
            StorageApiError::IncompatibleFormat {
                format: "manifest",
                version: 9,
                max_supported: 1,
            },
            StorageApiErrorClass::FailedPrecondition,
            false,
        ),
        (
            "LowerLayer",
            StorageApiError::lower_layer_with(
                StorageApiLowerLayer::Commit,
                "commit runtime failed",
                io_source(),
            ),
            StorageApiErrorClass::Internal,
            true,
        ),
    ]
}

/// Stable `<class>` code prefix for each public class.
fn expected_code_prefix(class: StorageApiErrorClass) -> &'static str {
    match class {
        StorageApiErrorClass::InvalidArgument => "invalid_argument",
        StorageApiErrorClass::FailedPrecondition => "failed_precondition",
        StorageApiErrorClass::NotFound => "not_found",
        StorageApiErrorClass::AlreadyExists => "already_exists",
        StorageApiErrorClass::Conflict => "conflict",
        StorageApiErrorClass::Unsupported => "unsupported",
        StorageApiErrorClass::HistoryUnavailable => "history_unavailable",
        StorageApiErrorClass::AmbiguousCommit => "ambiguous_commit",
        StorageApiErrorClass::ResourceExhausted => "resource_exhausted",
        StorageApiErrorClass::Internal => "internal",
        // `StorageApiErrorClass` is `#[non_exhaustive]`; external code cannot
        // match exhaustively. A new class must be given a code prefix here.
        _ => panic!("unhandled StorageApiErrorClass; extend expected_code_prefix"),
    }
}

fn is_code_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    segment
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn contains_redacted_secret(text: &str) -> Option<&'static str> {
    let lowered = text.to_ascii_lowercase();
    REDACTION_DENYLIST
        .iter()
        .copied()
        .find(|needle| lowered.contains(needle))
}

#[test]
fn error_contract_fixture_covers_every_variant() {
    let declared = declared_variant_names();
    // Parser floor: a parser that silently found nothing would let the set
    // comparison below agree with an equally empty fixture.
    for anchor in ["InvalidArgument", "WriterLockHeld", "LowerLayer"] {
        assert!(
            declared.contains(anchor),
            "variant parser lost `{anchor}`; parsed {declared:?}"
        );
    }

    let mut sampled = BTreeSet::new();
    for (name, error, _, _) in sample_errors() {
        let actual = variant_name(&error);
        assert_eq!(
            name, actual,
            "fixture row labelled `{name}` constructs `{actual}`"
        );
        assert!(sampled.insert(actual), "variant `{name}` is sampled twice");
    }

    let missing: Vec<_> = declared.difference(&sampled).collect();
    let unknown: Vec<_> = sampled.difference(&declared).collect();
    assert!(
        missing.is_empty() && unknown.is_empty(),
        "the error-contract fixture must sample exactly the declared StorageApiError variants; \
         add a sample_errors() row for each missing one. missing: {missing:?}, unknown: {unknown:?}"
    );
}

#[test]
fn every_error_code_is_well_formed_and_agrees_with_class() {
    for (name, error, expected_class, _) in sample_errors() {
        assert_eq!(
            error.class(),
            expected_class,
            "{name}: class() disagrees with the documented class"
        );

        let code = error.code();
        let segments: Vec<&str> = code.split('.').collect();
        assert_eq!(
            segments.len(),
            3,
            "{name}: code `{code}` must be exactly <class>.<area>.<detail>"
        );
        for segment in &segments {
            assert!(
                is_code_segment(segment),
                "{name}: code segment `{segment}` in `{code}` must be lowercase snake_case"
            );
        }
        assert_eq!(
            segments[0],
            expected_code_prefix(error.class()),
            "{name}: code `{code}` class prefix must match class() = {:?}",
            error.class()
        );

        // The code is the stable doc anchor; the boundary derives a doc link
        // from it. Grammar above already guarantees it is URL-path-safe, but
        // assert no whitespace defensively.
        assert!(
            !code.chars().any(char::is_whitespace),
            "{name}: code `{code}` must be doc-link safe (no whitespace)"
        );
    }
}

#[test]
fn every_error_has_a_nonempty_redacted_message() {
    for (name, error, _, _) in sample_errors() {
        let message = error.to_string();
        assert!(
            !message.trim().is_empty(),
            "{name}: Display message must be non-empty"
        );
        if let Some(secret) = contains_redacted_secret(&message) {
            panic!("{name}: Display message leaks redacted token `{secret}`: {message}");
        }
    }
}

#[test]
fn every_error_has_a_nonempty_mechanical_remediation() {
    for (name, error, _, _) in sample_errors() {
        let remediation = error.remediation();
        assert!(
            !remediation.trim().is_empty(),
            "{name}: remediation hint must be non-empty"
        );
        if let Some(secret) = contains_redacted_secret(remediation) {
            panic!("{name}: remediation leaks redacted token `{secret}`: {remediation}");
        }
        // Mechanical hint, not user-facing prose, but must read as guidance.
        assert!(
            remediation.ends_with('.'),
            "{name}: remediation should be a complete instruction ending in '.': {remediation}"
        );
    }
}

#[test]
fn source_bearing_errors_preserve_their_source_chain() {
    use std::error::Error;
    for (name, error, _, expects_source) in sample_errors() {
        let has_source = error.source().is_some();
        assert_eq!(
            has_source, expects_source,
            "{name}: source chain presence mismatch (expected {expects_source}, got {has_source})"
        );
    }
}

/// The two constructors that accept a source must thread it through.
#[test]
fn source_constructors_thread_the_source() {
    use std::error::Error;
    let lower = StorageApiError::lower_layer_with(
        StorageApiLowerLayer::Backend,
        "backend unavailable",
        io_source(),
    );
    assert!(lower.source().is_some());

    let durable = StorageApiError::durable_uncertain_with("sync uncertain", io_source());
    assert!(durable.source().is_some());

    // Sanity: the Arc-held source survives a clone (public errors are Clone).
    let cloned = lower.clone();
    let _ = Arc::new(cloned);
}

// --- TCP3.2a: lower-layer discriminants -----------------------------------

/// The defect this slice exists to fix (#2632): every unmapped lower-layer
/// failure reached the engine as the single code
/// `internal.storage_api.lower_layer`, so no rule-29-compliant test could
/// tell a branch failure from a commit failure. Each layer now carries its
/// own code, and they are distinct.
#[test]
fn each_lower_layer_has_a_distinct_boundary_code() {
    let layers = [
        StorageApiLowerLayer::Backend,
        StorageApiLowerLayer::Layout,
        StorageApiLowerLayer::Format,
        StorageApiLowerLayer::Service,
        StorageApiLowerLayer::Table,
        StorageApiLowerLayer::Branch,
        StorageApiLowerLayer::Commit,
        StorageApiLowerLayer::Lifecycle,
    ];
    let mut seen = BTreeSet::new();
    for layer in layers {
        let error = StorageApiError::lower_layer_with(layer, "failed", io_source());
        let code = error.code();
        assert!(
            seen.insert(code),
            "two layers share the boundary code {code} — the collapse this slice removes"
        );
        assert_eq!(
            code,
            layer.code(),
            "the boundary code must be the layer's own code"
        );
        // Hard Rule 29's precondition: code and class must keep agreeing.
        assert!(
            code.starts_with("internal."),
            "an unmapped lower-layer failure is Internal at the API: {code}"
        );
        assert_eq!(error.class(), StorageApiErrorClass::Internal);
    }
    assert_eq!(seen.len(), layers.len());
}

/// `inner_code` carries the specific inner failure across the boundary
/// WITHOUT reclassifying the error: `code()` stays class-consistent while
/// `inner_code()` names what actually went wrong.
#[test]
fn inner_code_carries_the_specific_failure_without_reclassifying() {
    let coded = StorageApiError::lower_layer_coded(
        StorageApiLowerLayer::Branch,
        "not_found.branch.branch_id",
        "branch read failed",
        io_source(),
    );
    assert_eq!(coded.inner_code(), Some("not_found.branch.branch_id"));
    // The API's own classification is unchanged: it does not model this
    // failure, so it stays Internal and its code stays class-consistent.
    assert_eq!(coded.class(), StorageApiErrorClass::Internal);
    assert_eq!(coded.code(), "internal.storage_api.branch");

    // A layer with no discriminant yet reports None rather than lying.
    let uncoded =
        StorageApiError::lower_layer_with(StorageApiLowerLayer::Commit, "failed", io_source());
    assert_eq!(uncoded.inner_code(), None);

    // Variants that are not LowerLayer have no inner code: their own
    // code() is already specific.
    assert_eq!(
        StorageApiError::durable_uncertain("uncertain").inner_code(),
        None
    );
}

/// Two different branch failures must be distinguishable at the boundary —
/// the property that did not exist before this slice.
#[test]
fn two_branch_failures_are_distinguishable_at_the_boundary() {
    let missing = StorageApiError::lower_layer_coded(
        StorageApiLowerLayer::Branch,
        "not_found.branch.branch_id",
        "branch read failed",
        io_source(),
    );
    let bad_state = StorageApiError::lower_layer_coded(
        StorageApiLowerLayer::Branch,
        "failed_precondition.branch.state",
        "branch read failed",
        io_source(),
    );
    assert_eq!(
        missing.code(),
        bad_state.code(),
        "same layer, same API code"
    );
    assert_ne!(
        missing.inner_code(),
        bad_state.inner_code(),
        "but the inner discriminant must tell them apart without reading display text"
    );
}

// --- TCP3.3c: literal code pins --------------------------------------------

/// Every `StorageApiError` variant's code, pinned as a literal. The existing
/// contract test checks the code's *class prefix* agrees with `class()`, and
/// the lower-layer test checks each layer's code via `layer.code()` — but
/// neither writes the full code string, so the workspace error-code guard
/// (which greps for literal codes) could not see them as asserted. This pins
/// each one: a code rename is a test failure here, and every storage-API code
/// is now a literal a test asserts.
#[expect(
    clippy::too_many_lines,
    reason = "the exhaustive variant/code table is the point: every storage-API code pinned in one place"
)]
#[test]
fn every_storage_api_code_is_pinned_as_a_literal() {
    // Top-level variants (one per `code()` arm).
    let top_level = [
        StorageApiError::InvalidArgument {
            field: "f",
            reason: "r",
        }
        .code(),
        StorageApiError::UnsupportedCapability {
            capability: "c",
            reason: "r",
        }
        .code(),
        StorageApiError::InvalidRuntimeState { reason: "r" }.code(),
        StorageApiError::BranchNotFound {
            branch_id: branch(),
        }
        .code(),
        StorageApiError::BranchAlreadyExists {
            branch_id: branch(),
        }
        .code(),
        StorageApiError::BranchGenerationMismatch {
            branch_id: branch(),
            expected: 1,
            actual: 2,
        }
        .code(),
        StorageApiError::Conflict {
            branch_id: branch(),
            storage_space: None,
            key_fingerprint: None,
            user_key_len: None,
            reason: "r",
        }
        .code(),
        StorageApiError::RetainedHistoryUnavailable {
            branch_id: branch(),
            reason: "r",
        }
        .code(),
        StorageApiError::TimestampHistoryUnavailable {
            branch_id: branch(),
            reason: "r",
        }
        .code(),
        StorageApiError::durable_uncertain("r").code(),
        StorageApiError::RecoveryDegraded { reason: "r" }.code(),
        StorageApiError::MaintenanceRejected { reason: "r" }.code(),
        StorageApiError::IncompatibleLayout { reason: "r" }.code(),
        StorageApiError::WriterLockHeld.code(),
        StorageApiError::BranchHasDependentChildren {
            branch_id: branch(),
        }
        .code(),
        StorageApiError::IncompatibleFormat {
            format: "f",
            version: 2,
            max_supported: 1,
        }
        .code(),
        StorageApiError::StoragePressure {
            branch_id: branch(),
            severity: CommitAdmissionPressureSeverity::Blocking,
            pressure_reason: CommitAdmissionPressureReason::NonZeroLevelTableBacklog,
            reason: "r",
            retryable: true,
        }
        .code(),
    ];
    // Assert the exact literals so a rename fails here (and the guard sees them).
    let expected_top: BTreeSet<&str> = [
        "invalid_argument.storage_api.argument",
        "unsupported.storage_api.capability",
        "failed_precondition.storage_api.state",
        "not_found.storage_api.branch",
        "already_exists.storage_api.branch",
        "failed_precondition.storage_api.branch_generation",
        "conflict.storage_api.conflict",
        "history_unavailable.storage_api.retained",
        "history_unavailable.storage_api.timestamp",
        "ambiguous_commit.storage_api.durable_uncertain",
        "failed_precondition.storage_api.recovery_degraded",
        "failed_precondition.storage_api.maintenance",
        "failed_precondition.storage_api.incompatible_layout",
        "failed_precondition.storage_api.writer_lock",
        "failed_precondition.storage_api.branch_dependent_children",
        "failed_precondition.storage_api.incompatible_format",
        "failed_precondition.storage_api.storage_pressure",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        top_level.iter().copied().collect::<BTreeSet<_>>(),
        expected_top,
        "storage-API top-level codes drifted from their pinned literals"
    );

    // Every lower-layer boundary code, pinned as a literal.
    let lower_layer = [
        (
            StorageApiLowerLayer::Backend,
            "internal.storage_api.backend",
        ),
        (StorageApiLowerLayer::Layout, "internal.storage_api.layout"),
        (StorageApiLowerLayer::Format, "internal.storage_api.format"),
        (
            StorageApiLowerLayer::Service,
            "internal.storage_api.service",
        ),
        (StorageApiLowerLayer::Table, "internal.storage_api.table"),
        (StorageApiLowerLayer::Branch, "internal.storage_api.branch"),
        (StorageApiLowerLayer::Commit, "internal.storage_api.commit"),
        (
            StorageApiLowerLayer::Lifecycle,
            "internal.storage_api.lifecycle",
        ),
    ];
    for (layer, expected) in lower_layer {
        let error = StorageApiError::lower_layer_with(layer, "r", io_source());
        assert_eq!(
            error.code(),
            expected,
            "lower-layer code drifted for {layer:?}"
        );
    }

    // ResourceExhausted's literal is asserted directly (its structured
    // fields make the array row noisy).
    assert_eq!(
        StorageApiError::ResourceExhausted {
            resource: "r",
            requested_bytes: 1,
            used_bytes: 1,
            limit_bytes: 1,
            reason: "r",
        }
        .code(),
        "resource_exhausted.storage_api.memory_budget"
    );
}
