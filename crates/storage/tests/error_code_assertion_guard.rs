//! Workspace error-code assertion guard (TCP3.0, Phase 3 Tier-1 tracker).
//!
//! Every stable error code a user can receive (`<class>.<area>.<detail>`)
//! must be asserted by at least one test, or carry a shrink-only allowlist
//! entry naming the slice that will assert it. The Phase 3 gap analysis
//! measured 68 codes that no test had ever asserted — including most of the
//! engine's `data_loss.*` corruption detectors. This guard turns that
//! finding into a ratchet: new codes without a coverage decision fail CI,
//! and allowlist entries die the moment a test asserts their code.
//!
//! Co-located with `testing_charter_guard.rs`: program-level guards live in
//! the storage test tree because it is the program's home crate. The scan
//! is workspace-wide.
//!
//! Classification rules (deliberately simple, grep-grade — this is a
//! ratchet, not a proof):
//! - A code is DEFINED if its string literal appears in product source
//!   (crate `src/` outside test locations, before any `#[cfg(test)]`).
//! - A code is ASSERTED if its literal appears in a test location: a
//!   `tests/` tree, a `testkit` module, `test_support`, or the
//!   `#[cfg(test)]` tail of a product file. Doc-comment mentions count;
//!   the ratchet tolerates that looseness in exchange for zero build
//!   machinery.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Error classes that form the first segment of a stable 3-part code.
/// Sourced from `v1-error-and-diagnostics-contract.md` plus `data_loss`,
/// which the engine emits today (part of the D2 doc/code reconciliation
/// tracked in the Phase 3 plan).
const CLASSES: &[&str] = &[
    "not_found",
    "already_exists",
    "invalid_argument",
    "failed_precondition",
    "access_denied",
    "conflict",
    "ambiguous_commit",
    "history_unavailable",
    "unsupported",
    "resource_exhausted",
    "unavailable",
    "io",
    "corruption",
    "serialization",
    "internal",
    "data_loss",
];

/// Areas that form the middle segment of a stable 3-part code.
const AREAS: &[&str] = &[
    // Product areas.
    "cli",
    "engine",
    "storage",
    "executor",
    "inference",
    "intelligence",
    "hub",
    "wasm",
    "sdk",
    // Storage's internal layer areas. Storage does not use "storage" as its
    // area segment — it names the failing layer — so without these the guard
    // was blind to 106 of its codes (TCP3.2a).
    "storage_api",
    "lifecycle",
    "branch",
    "commit",
    "table",
    "backend",
    "layout",
    "format",
    "service",
];

/// Codes with no asserting test yet. Every entry names the Phase 3 slice
/// (or reason) that owns closing it. Shrink-only: the guard fails if an
/// entry's code becomes asserted (remove the entry) or stops existing
/// (delete the dead entry).
const ALLOWED_UNASSERTED: &[(&str, &str)] = &[
    // --- storage internal-layer codes (surfaced by the TCP3.2a area fix) ---
    ("data_loss.engine.branch_id", "unreachable defensive guard (TCP3.15d): the branch-record decoder's `branch_id` field always `take`s exactly `BranchId::BYTE_LEN` bytes, and `BranchId::try_from_slice` validates length only — so the invalid-payload arm cannot fire (a short row errors earlier as `data_loss.engine.control_plane`). Sibling `control_name` is asserted by `records.rs`"),
    ("data_loss.engine.vector_artifacts", "unreachable defensive guard (TCP3.15c): the sole site is a `path.parent() == None` check in `persist_raw_flat_payload_at`, but the durable store always joins subdirectories so the payload path always has a parent; the normal store path also discards its result via `let _`. Note this is the *plural* code — the singular `data_loss.engine.vector_artifact` is asserted"),
    ("invalid_argument.engine.event_batch", "internal invariant: the public empty batch short-circuits to success before this check (#2651)"),
    ("invalid_argument.engine.event_metadata", "defensive serde_json::to_vec encode arm (never fails on a valid record); unreachable (#2651)"),
    ("invalid_argument.engine.event_record", "defensive encode-error arm; unreachable with a valid record (#2651)"),
    ("invalid_argument.engine.graph_batch", "internal invariant: the public empty batch short-circuits to success before this defensive check; unreachable via the API (#2651)"),
    ("invalid_argument.engine.graph_binding_record", "defensive encode-error path; unreachable with valid records (#2651). Its decode counterpart data_loss.engine.graph_binding_record is asserted (graph/service.rs inline test)"),
    ("invalid_argument.engine.graph_edge_record", "defensive encode-error path; unreachable (#2651). Its decode counterpart data_loss.engine.graph_edge_record is asserted by TCP3.15b"),
    ("invalid_argument.engine.graph_metadata", "defensive encode-error path (encode_graph_metadata_record); unreachable with a valid record (#2651)"),
    ("invalid_argument.engine.graph_node_record", "defensive encode-error path (serde_json::to_vec on a plain struct never fails); unreachable (#2651). Its decode counterpart data_loss.engine.graph_node_record is asserted by TCP3.15b"),
    ("invalid_argument.engine.graph_ontology_record", "defensive encode-error path (serde_json::to_vec of a valid ontology); unreachable via the public API (#2651)"),
    ("invalid_argument.engine.graph_type_index_record", "defensive encode-error path (encode_graph_type_index_record); unreachable with a valid record (#2651)"),
    ("invalid_argument.engine.json_batch", "internal invariant: the public empty batch short-circuits to success before this check (#2651)"),
    ("invalid_argument.engine.json_document", "defensive encode-error arm (serde_json::to_vec of a stored doc); unreachable via the API (#2651)"),
    ("invalid_argument.engine.json_index", "defensive encode-error arm (serde_json::to_vec of a stored index); unreachable (#2651)"),
    ("invalid_argument.engine.json_value", "defensive serde_json::to_vec encode arm (never fails on a valid Value); the reachable depth check emits json_document_too_deep (#2651)"),
    ("invalid_argument.engine.space_catalog", "u16::try_from(spaces.len()) overflow — needs >65535 spaces in one branch; practically unreachable (#2651)"),
    ("invalid_argument.engine.vector_artifact", "defensive encode-error arm (serde_json::to_vec of artifact metadata); unreachable (#2651)"),
    ("invalid_argument.engine.vector_batch", "internal invariant: the public empty batch short-circuits to success (#2651)"),
    ("invalid_argument.engine.vector_index_manifest", "defensive encode-error arm; unreachable with a valid manifest (#2651)"),
    ("invalid_argument.engine.vector_record", "defensive encode-error arm; unreachable with a valid record (#2651)"),
    ("invalid_argument.executor.arrow_base64", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.arrow_json_key", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.arrow_vector_dimension", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.arrow_vector_key", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.graph_analytics_budget", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.hub_branch", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.hub_dataset", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.limit", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("invalid_argument.executor.vector_limit", "asserted by TCP3.8 error-envelope replay fixtures"),
    ("internal.hub.import", "unreachable defensive guard (#3070): `materialize` re-opens the staging directory after importing every branch artifact, then rejects the result if `outcome.summary().created()` — but a successful import loop always writes durable state, so the re-open recovers rather than creating. Kept as a structured code so an operator sees a named failure if that invariant ever breaks; the reachable staging failures propagate the engine's own code via `engine_stage_failure` and are asserted by `import_bundle`'s `multi_branch_import_failure_names_the_branch_and_reason`"),
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/storage.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root above crates/storage")
        .to_path_buf()
}

fn is_test_location(path: &Path) -> bool {
    let text = path.to_string_lossy();
    text.contains("/tests/")
        || text.contains("/testkit/")
        || text.contains("test_support")
        || text.ends_with("testkit.rs")
}

fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if name == "target" || name == ".git" {
                continue;
            }
            walk(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// Extract every `<class>.<area>.<detail>` string literal from `text`.
fn extract_codes(text: &str, into: &mut BTreeSet<String>) {
    for candidate in text.split('"').skip(1).step_by(2) {
        let mut parts = candidate.splitn(3, '.');
        let (Some(class), Some(area), Some(detail)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let segment_ok = |segment: &str| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        };
        if CLASSES.contains(&class)
            && AREAS.contains(&area)
            && segment_ok(class)
            && segment_ok(detail)
        {
            into.insert(candidate.to_owned());
        }
    }
}

fn collect() -> (BTreeMap<String, PathBuf>, BTreeSet<String>) {
    let root = repo_root();
    let mut files = Vec::new();
    walk(&root.join("crates"), &mut files);

    let guard_file = root.join("crates/storage/tests/error_code_assertion_guard.rs");
    let mut defined: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut asserted: BTreeSet<String> = BTreeSet::new();

    for path in files {
        // The guard's own allowlist must not count as assertions.
        if path == guard_file {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        if is_test_location(&path) {
            extract_codes(&text, &mut asserted);
            continue;
        }
        // Product file: the `#[cfg(test)]` tail counts as test content.
        let split = text.find("#[cfg(test)]").unwrap_or(text.len());
        let mut product_codes = BTreeSet::new();
        extract_codes(&text[..split], &mut product_codes);
        extract_codes(&text[split..], &mut asserted);
        for code in product_codes {
            defined.entry(code).or_insert_with(|| path.clone());
        }
    }
    (defined, asserted)
}

#[test]
fn every_error_code_is_asserted_or_carries_a_reasoned_allowlist_entry() {
    let (defined, asserted) = collect();
    let allowlist: BTreeMap<&str, &str> = ALLOWED_UNASSERTED.iter().copied().collect();

    let mut unasserted_and_unlisted = Vec::new();
    for (code, first_site) in &defined {
        if !asserted.contains(code) && !allowlist.contains_key(code.as_str()) {
            unasserted_and_unlisted
                .push(format!("  {code}  (defined in {})", first_site.display()));
        }
    }
    assert!(
        unasserted_and_unlisted.is_empty(),
        "error codes with no asserting test and no allowlist entry — add a test \
         asserting the code, or an ALLOWED_UNASSERTED entry naming the owning slice:\n{}",
        unasserted_and_unlisted.join("\n")
    );
}

#[test]
fn allowlist_entries_shrink_when_their_codes_gain_tests_or_die() {
    let (defined, asserted) = collect();

    let mut stale = Vec::new();
    for (code, reason) in ALLOWED_UNASSERTED {
        if asserted.contains(*code) {
            stale.push(format!(
                "  {code} is now asserted by a test — delete its allowlist entry ({reason})"
            ));
        } else if !defined.contains_key(*code) {
            stale.push(format!(
                "  {code} no longer exists in product sources — delete the dead entry ({reason})"
            ));
        }
    }
    assert!(
        stale.is_empty(),
        "shrink-only allowlist has stale entries:\n{}",
        stale.join("\n")
    );
}
