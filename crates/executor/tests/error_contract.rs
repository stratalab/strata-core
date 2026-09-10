//! TCP4.8 — error-contract correctness harness.
//!
//! The Phase 3 error-code guard proves a code is asserted *somewhere*; #2699
//! showed that is insufficient — every code it implicated was asserted
//! elsewhere while the *conditions* mapped to the wrong code, so a permanent
//! corruption surfaced as a retryable outage and a caller obeying the advice
//! would retry forever. This harness asserts the *mapping*: each fixture is a
//! real failure condition driven through the public wire surface, checked
//! against the expected `(code, class, retryable, commit_outcome)` — plus an
//! envelope-coherence oracle and a redaction sweep applied to every observed
//! error, and a registry-wide semantic-coherence sweep over all public codes.
//!
//! Seed (gate 7): #2699's exact conditions — byte-corrupted WAL, regular file
//! as a database path, missing parent directory — are pinned at the wire layer
//! (the layer the bug was observed through). The sabotage test proves the
//! checker rejects the historical pre-fix envelope.
//!
//! Known divergences are pinned shrink-only: each carries a pin test asserting
//! today's broken behavior exactly, so landing the fix breaks the pin and
//! forces the exception's deletion. Open entries: #2749 (`data_loss.*` codes
//! surface `class=corruption`; no `DataLoss` variant), #2750 (feature-disabled
//! codes classed `invalid_argument` with a retry policy only a state change
//! can honor).

use std::path::{Path, PathBuf};

use serde_json::Value;
use strata_executor::{
    public_error_code_entries, Command, CommitOutcomeStatus, ErrorClass, Executor, ExecutorError,
    RetryPolicy,
};

// ---------------------------------------------------------------------------
// Wire support
// ---------------------------------------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Minimal standard base64 for wire KV keys/values (no dependency).
fn b64(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let mut buf = [0u8; 3];
        buf[..chunk.len()].copy_from_slice(chunk);
        let n = u32::from(buf[0]) << 16 | u32::from(buf[1]) << 8 | u32::from(buf[2]);
        let chars = [
            B64[(n >> 18) as usize & 63],
            B64[(n >> 12) as usize & 63],
            B64[(n >> 6) as usize & 63],
            B64[n as usize & 63],
        ];
        let keep = chunk.len() + 1;
        for (index, ch) in chars.iter().enumerate() {
            out.push(if index < keep { char::from(*ch) } else { '=' });
        }
    }
    out
}

fn status_json(error: &ExecutorError) -> Value {
    serde_json::to_value(error.status()).expect("serialize error status")
}

fn run(executor: &mut Executor, wire: &Value) -> Result<Value, Value> {
    let command: Command =
        serde_json::from_value(wire.clone()).expect("wire JSON parses as a command");
    match executor.execute(command) {
        Ok(output) => Ok(serde_json::to_value(&output).expect("serialize output")),
        Err(error) => Err(status_json(&error)),
    }
}

fn field<'a>(status: &'a Value, name: &str) -> &'a str {
    status[name]
        .as_str()
        .unwrap_or_else(|| panic!("error status carries string field `{name}`: {status}"))
}

// ---------------------------------------------------------------------------
// The checkers — non-panicking so the sabotage test can prove they reject a
// bad envelope without catch_unwind.
// ---------------------------------------------------------------------------

/// Storage-internal identifiers that must never surface in user-facing text
/// (same vocabulary as the engine redaction tests).
const STORAGE_LEAK_TERMS: &[&str] = &[
    "StorageRuntime",
    "CommitBatch",
    "StorageSpaceId",
    "StorageKey",
    "StorageValue",
    "BranchRequest",
    "StorageApiError",
    "WalService",
    "ManifestService",
    "TableRuntime",
    "storage_api",
];

/// Expected class-field value for a code prefix. The contract's rule is that a
/// code's class segment IS its class, so every prefix maps to itself. (#2749
/// removed the last exception: `data_loss` codes once folded onto `corruption`
/// because `ErrorClass` had no `DataLoss` variant; it now does.)
fn expected_class_for_prefix(prefix: &str) -> &str {
    prefix
}

const RETRYABLE_POLICIES: &[&str] = &["after_state_change", "same_request", "idempotent_only"];

/// Envelope-internal coherence: the fields of one observed error status must
/// agree with each other and leak nothing. Judges *consistency*, not the
/// condition mapping — a wrong-but-coherent envelope (the #2699 shape) passes
/// here and is caught by `expectation_violations` instead.
fn coherence_violations(status: &Value) -> Vec<String> {
    let mut violations = Vec::new();
    let code = field(status, "code");
    let class = field(status, "class");
    let retry_policy = field(status, "retry_policy");
    let message = field(status, "message");
    let suggested_fix = field(status, "suggested_fix");
    let retryable = status["retryable"]
        .as_bool()
        .unwrap_or_else(|| panic!("error status carries boolean `retryable`: {status}"));

    let prefix = code.split('.').next().expect("code has a class segment");
    if class != expected_class_for_prefix(prefix) {
        violations.push(format!(
            "class `{class}` contradicts code prefix `{prefix}` ({code})"
        ));
    }
    if retryable != RETRYABLE_POLICIES.contains(&retry_policy) {
        violations.push(format!(
            "retryable={retryable} incoherent with retry_policy `{retry_policy}` ({code})"
        ));
    }
    if message.trim().is_empty() {
        violations.push(format!("empty message ({code})"));
    }
    if suggested_fix.trim().is_empty() {
        violations.push(format!("empty suggested_fix ({code})"));
    }
    // The #2699 poison shape: a permanent condition whose remediation leads
    // with retry advice. Conditional retry after a correcting action
    // ("Correct the input and retry.") is legitimate; leading with "Retry"
    // as the primary action is not, on a policy that says retry never helps.
    if retry_policy == "never"
        && suggested_fix
            .trim_start()
            .to_lowercase()
            .starts_with("retry")
    {
        violations.push(format!(
            "retry_policy=never but suggested_fix leads with retry advice: `{suggested_fix}` ({code})"
        ));
    }
    for term in STORAGE_LEAK_TERMS {
        if message.contains(term) || suggested_fix.contains(term) {
            violations.push(format!("storage internal `{term}` leaked ({code})"));
        }
    }
    violations
}

/// Condition mapping: the observed envelope must carry the expected code and
/// retry direction. This is the check that catches #2699 — the historical
/// envelope was internally coherent; its *mapping* was wrong.
fn expectation_violations(status: &Value, code: &str, retryable: bool) -> Vec<String> {
    let mut violations = Vec::new();
    if field(status, "code") != code {
        violations.push(format!(
            "expected code `{code}`, observed `{}`",
            field(status, "code")
        ));
    }
    if status["retryable"].as_bool() != Some(retryable) {
        violations.push(format!(
            "expected retryable={retryable}, observed {}",
            status["retryable"]
        ));
    }
    violations
}

/// Full fixture assertion: mapping + coherence, with the fixture path barred
/// from user-facing text (structured details may carry paths; prose may not).
fn assert_fixture(label: &str, status: &Value, code: &str, retryable: bool, fixture_path: &Path) {
    let mut violations = expectation_violations(status, code, retryable);
    violations.extend(coherence_violations(status));
    let path_text = fixture_path.display().to_string();
    if !path_text.is_empty() {
        for text_field in ["message", "suggested_fix"] {
            if field(status, text_field).contains(&path_text) {
                violations.push(format!("fixture path leaked into {text_field}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "{label}: error-contract violations:\n  {}\nfull status: {status}",
        violations.join("\n  ")
    );
}

fn assert_commit_outcome(label: &str, status: &Value, outcome: &str) {
    assert_eq!(
        field(status, "commit_outcome"),
        outcome,
        "{label}: commit outcome diverges: {status}"
    );
}

// ---------------------------------------------------------------------------
// Open-path fixtures (#2699 seed conditions, pinned at the wire layer)
// ---------------------------------------------------------------------------

fn seeded_durable_db(root: &Path) {
    let mut executor = Executor::open_durable_local(root).expect("durable executor opens");
    for index in 0..64u32 {
        run(
            &mut executor,
            &serde_json::json!({
                "type": "kv_put",
                "key": b64(format!("seed-{index:04}").as_bytes()),
                "value": b64(&[b'v'; 64]),
            }),
        )
        .expect("seed write succeeds");
    }
    // Dropping the executor closes the database and flushes the WAL, so the
    // corruption lands in a durable record rather than an in-memory buffer.
}

/// The active WAL segment file, so a fixture can damage the durable log.
fn active_wal_segment(root: &Path) -> PathBuf {
    let wal_dir = root.join("wal");
    let mut best: Option<(String, PathBuf)> = None;
    for entry in std::fs::read_dir(&wal_dir).expect("wal dir").flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(id) = name.strip_suffix(".object@") else {
            continue;
        };
        if id.len() != 16 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let id = id.to_owned();
        if best
            .as_ref()
            .is_none_or(|(best_id, _)| id.as_str() > best_id.as_str())
        {
            best = Some((id, path));
        }
    }
    best.map(|(_, path)| path)
        .expect("an active WAL segment exists")
}

fn open_error(path: &Path) -> Value {
    let error = Executor::open_durable_local(path)
        .err()
        .expect("open must fail for this fixture");
    status_json(&error)
}

/// #2699 seed: a regular file is not a database directory — permanent,
/// caller-side, never retryable.
#[test]
fn opening_a_regular_file_reports_permanent_invalid_argument() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("not-a-db");
    std::fs::write(&path, b"plain file").expect("write regular file");
    let status = open_error(&path);
    assert_fixture(
        "regular file as database path",
        &status,
        "invalid_argument.engine.persistence",
        false,
        &path,
    );
    assert_commit_outcome("regular file as database path", &status, "not_started");
}

/// #2699 seed: a missing parent directory is a permanent path problem, not a
/// transient outage.
#[test]
fn opening_under_a_missing_parent_reports_permanent_invalid_argument() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("does-not-exist").join("db");
    let status = open_error(&path);
    assert_fixture(
        "missing parent directory",
        &status,
        "invalid_argument.engine.persistence",
        false,
        &path,
    );
}

/// #2699 seed: a byte-corrupted WAL is permanent corruption. This is the exact
/// condition that shipped as `unavailable` + "retry after the persistence
/// layer is available" — the envelope that retried forever.
#[test]
fn opening_a_byte_corrupted_wal_reports_permanent_corruption() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    seeded_durable_db(&root);
    let segment = active_wal_segment(&root);
    let mut bytes = std::fs::read(&segment).expect("read wal segment");
    assert!(bytes.len() > 128, "wal segment too small to corrupt safely");
    let start = bytes.len() / 4;
    for offset in start..(start + 32).min(bytes.len()) {
        bytes[offset] ^= 0xff;
    }
    std::fs::write(&segment, &bytes).expect("write corrupted wal segment");

    let status = open_error(&root);
    assert_fixture(
        "byte-corrupted WAL",
        &status,
        "corruption.engine.persistence_recovery",
        false,
        &root,
    );
    assert_commit_outcome("byte-corrupted WAL", &status, "not_applicable");
}

/// Hard rule 42: a pre-V1 layout is rejected with the structured layout code,
/// never a generic persistence failure.
#[test]
fn opening_a_pre_v1_layout_reports_layout_version_precondition() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("old-db");
    std::fs::create_dir_all(&root).expect("create dir");
    std::fs::write(root.join("strata.toml"), b"[database]\n").expect("write pre-V1 marker");
    let status = open_error(&root);
    assert_fixture(
        "pre-V1 layout",
        &status,
        "failed_precondition.engine.layout_version",
        false,
        &root,
    );
}

/// Transient direction control: a held writer lock is genuinely temporary —
/// the same request succeeds once the holder closes — so it must stay
/// retryable. A harness that only ever asserts `retryable=false` would be
/// blind to the opposite misclassification.
#[test]
fn opening_a_locked_database_reports_retryable_unavailable() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    let holder = Executor::open_durable_local(&root).expect("first open holds the writer lock");
    let status = open_error(&root);
    assert_fixture(
        "second open while writer lock held",
        &status,
        "unavailable.engine.persistence",
        true,
        &root,
    );
    drop(holder);
    Executor::open_durable_local(&root).expect("open succeeds after the holder closes");
}

// ---------------------------------------------------------------------------
// Registry-wide semantic coherence (all 200+ public codes)
// ---------------------------------------------------------------------------

#[test]
fn permanent_classes_never_advise_retry() {
    let mut violations = Vec::new();
    for entry in public_error_code_entries() {
        let prefix = entry.code.split('.').next().expect("code has a prefix");
        // #2750 removed the last exception: the feature-disabled codes moved to
        // `unsupported.*`, so every `invalid_argument.*` code is now permanent
        // with no carve-out.
        let permanent = matches!(prefix, "corruption" | "data_loss" | "invalid_argument");
        if permanent && entry.retry_policy != RetryPolicy::Never {
            violations.push(format!(
                "{}: {prefix} condition with {:?}",
                entry.code, entry.retry_policy
            ));
        }
        if entry.retry_policy == RetryPolicy::Never
            && entry
                .suggested_fix
                .trim_start()
                .to_lowercase()
                .starts_with("retry")
        {
            violations.push(format!(
                "{}: never-retry code whose fix leads with retry advice: `{}`",
                entry.code, entry.suggested_fix
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "permanent-condition registry violations:\n  {}",
        violations.join("\n  ")
    );
}

#[test]
fn transient_classes_always_allow_retry() {
    let mut violations = Vec::new();
    for entry in public_error_code_entries() {
        let prefix = entry.code.split('.').next().expect("code has a prefix");
        if matches!(prefix, "unavailable" | "resource_exhausted")
            && entry.retry_policy == RetryPolicy::Never
        {
            violations.push(format!("{}: {prefix} condition with Never", entry.code));
        }
    }
    assert!(
        violations.is_empty(),
        "transient-condition registry violations:\n  {}",
        violations.join("\n  ")
    );
}

/// `ambiguous_commit` exists precisely because the outcome is unknowable; its
/// registry metadata must say so on both axes. `internal` invariant failures
/// cannot classify their own retryability.
#[test]
fn unknowable_outcomes_stay_unknowable() {
    let mut violations = Vec::new();
    for entry in public_error_code_entries() {
        let prefix = entry.code.split('.').next().expect("code has a prefix");
        if prefix == "ambiguous_commit" {
            if entry.commit_outcome != CommitOutcomeStatus::MaybeCommitted {
                violations.push(format!(
                    "{}: ambiguous_commit with {:?}",
                    entry.code, entry.commit_outcome
                ));
            }
            if !matches!(
                entry.retry_policy,
                RetryPolicy::Unknown | RetryPolicy::IdempotentOnly
            ) {
                violations.push(format!(
                    "{}: ambiguous_commit with {:?}",
                    entry.code, entry.retry_policy
                ));
            }
        }
        if prefix == "internal" && entry.retry_policy != RetryPolicy::Unknown {
            violations.push(format!(
                "{}: internal with {:?}",
                entry.code, entry.retry_policy
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "unknowable-outcome registry violations:\n  {}",
        violations.join("\n  ")
    );
}

/// #2750 contract (promoted from `pin_2750_*`): a build without the hub/arrow
/// feature is `unsupported.executor.*` — the command is well-formed, the
/// capability is simply absent — and keeps `AfterStateChange` retry (rebuild
/// with the feature, then the same request works). It is NOT `invalid_argument`
/// (which means malformed input that no retry can fix).
#[test]
fn feature_disabled_codes_are_unsupported_with_state_change_retry() {
    for code in [
        "unsupported.executor.hub_feature_disabled",
        "unsupported.executor.arrow_feature_disabled",
    ] {
        let entry = public_error_code_entries()
            .find(|entry| entry.code == code)
            .unwrap_or_else(|| panic!("`{code}` must be registered"));
        assert_eq!(
            entry.class,
            ErrorClass::Unsupported,
            "{code}: a build-absent feature is unsupported, not invalid_argument"
        );
        assert_eq!(
            entry.retry_policy,
            RetryPolicy::AfterStateChange,
            "{code}: rebuilding with the feature is the state change that helps"
        );
    }
}

// ---------------------------------------------------------------------------
// Suggested-fix parity: the hint on the wire is the hint in the registry
// ---------------------------------------------------------------------------

fn registry_suggested_fix(code: &str) -> &'static str {
    public_error_code_entries()
        .find(|entry| entry.code == code)
        .unwrap_or_else(|| panic!("`{code}` must be registered"))
        .suggested_fix
}

/// Engine-raised errors cross the executor boundary carrying the registry's
/// per-code remediation, not class-generic wording. Driven through the wire
/// because that is the layer the divergence was observed at: `strata agents
/// errors` documented one hint while the live error carried another (#3237).
#[test]
fn test_engine_errors_reach_the_wire_with_the_registry_suggested_fix() {
    let mut executor = Executor::open_cache().expect("cache executor opens");
    let create = serde_json::json!({"type": "branch_create", "branch": "feature"});
    run(&mut executor, &create).expect("first create succeeds");
    let duplicate = run(&mut executor, &create).expect_err("duplicate create fails");
    assert_eq!(field(&duplicate, "code"), "already_exists.engine.branch");
    assert_eq!(
        field(&duplicate, "suggested_fix"),
        registry_suggested_fix("already_exists.engine.branch"),
        "duplicate branch: {duplicate}"
    );

    let missing = run(
        &mut executor,
        &serde_json::json!({"type": "branch_delete", "branch": "nope"}),
    )
    .expect_err("deleting a missing branch fails");
    assert_eq!(field(&missing, "code"), "not_found.engine.branch");
    assert_eq!(
        field(&missing, "suggested_fix"),
        registry_suggested_fix("not_found.engine.branch"),
        "missing branch: {missing}"
    );
}

/// #3244: the registry row is the whole authority for a code's class, retry
/// policy, commit outcome and suggested fix. `ExecutorError::new` takes only
/// the code and the message, so no construction site can restate any of the
/// four — the old `(class, retryable)` arguments were per-site copies of the
/// row, and three live sites had drifted from it. Swept over every registered
/// code, engine codes included, because `new` is code-driven.
#[test]
fn test_executor_errors_carry_the_whole_registry_row() {
    let mut violations = Vec::new();
    for entry in public_error_code_entries() {
        let error = ExecutorError::new(entry.code, "probe");
        let observed = (
            error.public_class(),
            error.retry_policy(),
            error.commit_outcome(),
            error.suggested_fix(),
        );
        let row = (
            entry.class,
            entry.retry_policy,
            entry.commit_outcome,
            entry.suggested_fix,
        );
        if observed != row {
            violations.push(format!(
                "{}: runtime {observed:?} != registry {row:?}",
                entry.code
            ));
        }
        assert_eq!(error.code(), entry.code);
        assert_eq!(error.message(), "probe");
        // `retryable` is derived from the row's policy, so it must agree with
        // it for every row — the registry has rows on both sides of the split.
        let wire = serde_json::to_value(error.status()).expect("status serializes");
        let expected_retryable = RETRYABLE_POLICIES.contains(&field(&wire, "retry_policy"));
        if error.retryable() != expected_retryable {
            violations.push(format!(
                "{}: retryable={} incoherent with retry_policy {:?}",
                entry.code,
                error.retryable(),
                entry.retry_policy
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "executor errors diverge from the registry row:\n  {}",
        violations.join("\n  ")
    );
}

/// #3244, engine half of the boundary: an engine error crosses with the
/// registry row for all four fields even when its construction site supplied
/// its own `suggested_fix` (the persistence adapter does, and its text for a
/// held writer lock differs from the row), while the site's `hints` — the one
/// channel a site owns — survive. Driven through the wire on the held-lock
/// open, the adapter arm that carries both.
#[test]
fn test_engine_site_fix_yields_to_the_row_while_site_hints_survive() {
    let dir = tempfile::tempdir().expect("tmp");
    let root = dir.path().join("db");
    let holder = Executor::open_durable_local(&root).expect("first open holds the writer lock");
    let status = open_error(&root);
    drop(holder);
    let code = "unavailable.engine.persistence";
    let entry = public_error_code_entries()
        .find(|entry| entry.code == code)
        .expect("unavailable.engine.persistence is registered");
    assert_eq!(field(&status, "code"), code);
    assert_eq!(
        field(&status, "suggested_fix"),
        entry.suggested_fix,
        "held lock: {status}"
    );
    assert_eq!(
        status["retry_policy"],
        serde_json::to_value(entry.retry_policy).expect("policy serializes"),
        "held lock: {status}"
    );
    assert_eq!(
        status["commit_outcome"],
        serde_json::to_value(entry.commit_outcome).expect("outcome serializes"),
        "held lock: {status}"
    );
    let hints = status["hints"]
        .as_array()
        .expect("site hints survive the boundary");
    assert!(
        hints.iter().any(|hint| hint
            .as_str()
            .is_some_and(|hint| hint.contains("persistence layer"))),
        "the adapter's site hint reaches the wire: {status}"
    );
    // The adapter's structured details (`layer`, `reason` for a lower-layer
    // failure) are site-owned too and cross the boundary alongside the hints.
    let detail_keys: Vec<&str> = status["details"]
        .as_array()
        .expect("site details survive the boundary")
        .iter()
        .map(|detail| field(detail, "key"))
        .collect();
    assert!(
        detail_keys.contains(&"layer") && detail_keys.contains(&"reason"),
        "the adapter's site details reach the wire: {status}"
    );
}

// ---------------------------------------------------------------------------
// Sabotage: the checker must reject the historical #2699 envelope
// ---------------------------------------------------------------------------

/// Positive control (gate 7). The pre-fix envelope for a byte-corrupted WAL
/// was internally coherent — `unavailable` + retryable + retry advice agree
/// with each other — so coherence alone cannot catch #2699. The condition
/// fixture must: expecting `corruption.engine.persistence_recovery`,
/// non-retryable, the checker must reject the historical shape on both axes.
#[test]
fn sabotage_historical_2699_envelope_is_rejected() {
    let historical = serde_json::json!({
        "class": "unavailable",
        "code": "unavailable.engine.persistence",
        "retry_policy": "same_request",
        "retryable": true,
        "commit_outcome": "not_applicable",
        "message": "persistence lower layer is unavailable",
        "suggested_fix": "Retry after the local persistence layer is available.",
    });
    assert!(
        coherence_violations(&historical).is_empty(),
        "the historical envelope was internally coherent; only the mapping was wrong"
    );
    let violations =
        expectation_violations(&historical, "corruption.engine.persistence_recovery", false);
    assert_eq!(
        violations.len(),
        2,
        "the corrupt-WAL fixture must reject the historical envelope on code and retryability: {violations:?}"
    );

    // And a self-contradictory envelope trips coherence on every axis it breaks.
    let incoherent = serde_json::json!({
        "class": "unavailable",
        "code": "corruption.engine.persistence_recovery",
        "retry_policy": "never",
        "retryable": true,
        "commit_outcome": "not_applicable",
        "message": "",
        "suggested_fix": "Retry the request.",
    });
    let violations = coherence_violations(&incoherent);
    assert_eq!(
        violations.len(),
        4,
        "class/code mismatch, retryable/policy mismatch, empty message, and retry-leading fix must all be flagged: {violations:?}"
    );
}

// ---------------------------------------------------------------------------
// Fault-seam fixtures (wire envelopes for injected storage faults). Runs in
// the explicit `--features testkit` CI lane.
// ---------------------------------------------------------------------------

#[cfg(feature = "testkit")]
mod fault_seam {
    use super::{assert_fixture, b64, coherence_violations, field, run, Executor};
    use serde_json::Value;
    use strata_engine::testkit::{RowCorruption, StorageFaultKind};
    use strata_engine::{CacheOpenOptions, Database};

    fn cache_database() -> Database {
        Database::open_cache(CacheOpenOptions::new())
            .expect("cache database opens")
            .into_database()
    }

    fn commit_fault_status(kind: StorageFaultKind) -> Value {
        let mut db = cache_database();
        db.inject_commit_fault_for_test(kind);
        let mut executor = Executor::from_database(db);
        run(
            &mut executor,
            &serde_json::json!({"type": "kv_put", "key": b64(b"k"), "value": b64(b"v")}),
        )
        .expect_err("injected commit fault must surface")
    }

    /// Every injected storage fault reaches the wire with the mapped code, the
    /// documented retry direction, and the documented commit outcome — the
    /// transient/permanent matrix the engine asserts internally, re-asserted
    /// at the surface SDK callers actually see.
    #[test]
    fn commit_fault_matrix_maps_to_documented_wire_envelopes() {
        let scratch = std::path::Path::new("");
        for (kind, code, retryable, outcome) in [
            (
                StorageFaultKind::ResourceExhausted,
                "resource_exhausted.engine.persistence_budget",
                true,
                "definitely_not_committed",
            ),
            (
                StorageFaultKind::AmbiguousCommit,
                "ambiguous_commit.engine.persistence",
                false,
                "maybe_committed",
            ),
            (
                StorageFaultKind::RecoveryDegraded,
                "corruption.engine.persistence_recovery",
                false,
                "not_applicable",
            ),
            (
                StorageFaultKind::Unavailable,
                "unavailable.engine.persistence",
                true,
                "not_applicable",
            ),
            (
                StorageFaultKind::NotFound,
                "not_found.engine.persistence",
                false,
                "not_applicable",
            ),
            (
                StorageFaultKind::Conflict,
                "conflict.engine.persistence",
                true,
                "definitely_not_committed",
            ),
        ] {
            let status = commit_fault_status(kind);
            assert_fixture(
                &format!("commit fault {kind:?}"),
                &status,
                code,
                retryable,
                scratch,
            );
            assert_eq!(
                field(&status, "commit_outcome"),
                outcome,
                "commit fault {kind:?}: commit outcome diverges: {status}"
            );
        }
    }

    /// A stored row whose value went missing is unrecoverable loss on the read
    /// path — non-retryable, and (as of #2749) surfaces its own `data_loss`
    /// class on the wire rather than folding onto `corruption`. Promoted from
    /// `pin_2749_*` when the fix landed: the wire `class` segment now equals the
    /// code's class segment for every code, `data_loss` included.
    #[test]
    fn data_loss_codes_surface_their_own_data_loss_class() {
        let mut db = cache_database();
        db.inject_scan_corruption_for_test(RowCorruption::DropValue);
        let mut executor = Executor::from_database(db);
        run(
            &mut executor,
            &serde_json::json!({"type": "kv_put", "key": b64(b"k"), "value": b64(b"v")}),
        )
        .expect("seed write succeeds");
        let status = run(&mut executor, &serde_json::json!({"type": "kv_scan"}))
            .expect_err("scanning a value-dropped row must fail loudly");

        assert_fixture(
            "scan over a value-dropped row",
            &status,
            "data_loss.engine.kv_value",
            false,
            std::path::Path::new(""),
        );
        // #2749 contract: the class field is the code's own class, not a fold
        // onto `corruption`. An SDK switching on `class` can now distinguish
        // unrecoverable loss from detected-inconsistency.
        assert_eq!(
            field(&status, "class"),
            "data_loss",
            "data_loss.* codes must surface class=data_loss, not corruption"
        );
        assert!(
            coherence_violations(&status).is_empty(),
            "the promoted contract must remain internally coherent"
        );
    }
}
