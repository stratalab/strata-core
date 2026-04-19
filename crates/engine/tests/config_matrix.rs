//! Regression test for T3-E7 / D-DR-11: every `StrataConfig` and `StorageConfig`
//! field must appear in the config truth-table doc, and the doc must not
//! mention fields that do not exist.
//!
//! The acceptance criterion from `durability-recovery-scope.md` §D-DR-11 is:
//!
//! > every public field on `StrataConfig` is listed in the config matrix; a
//! > new knob without a classification fails the regression test
//!
//! Adding a field to either struct without also adding a matrix row will
//! fail this test; removing a field without pruning the matrix will also
//! fail.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

/// Every public field on `StrataConfig` (top-level). Order does not matter;
/// this must stay in sync with `crates/engine/src/database/config.rs`.
const STRATA_CONFIG_FIELDS: &[&str] = &[
    "durability",
    "auto_embed",
    "model",
    "embed_batch_size",
    "embed_model",
    "provider",
    "default_model",
    "anthropic_api_key",
    "openai_api_key",
    "google_api_key",
    "storage",
    "allow_lossy_recovery",
    "telemetry",
    "default_vector_dtype",
    "snapshot_retention",
];

/// Every public field on `StorageConfig` (nested under `storage`).
const STORAGE_CONFIG_FIELDS: &[&str] = &[
    "memory_budget",
    "max_branches",
    "max_write_buffer_entries",
    "max_versions_per_key",
    "block_cache_size",
    "write_buffer_size",
    "max_immutable_memtables",
    "l0_slowdown_writes_trigger",
    "l0_stop_writes_trigger",
    "background_threads",
    "target_file_size",
    "level_base_bytes",
    "data_block_size",
    "bloom_bits_per_key",
    "compaction_rate_limit",
    "write_stall_timeout_ms",
    "codec",
];

/// Recognized class labels in the matrix. Any other label fails the test.
const VALID_CLASSES: &[&str] = &[
    "open-time-only",
    "live-safe",
    "unsupported/deferred",
    "non-durability",
];

fn matrix_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("docs/design/architecture-cleanup/durability-recovery-config-matrix.md")
}

fn read_matrix() -> String {
    let path = matrix_path();
    let display = path.display();
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {display}: {e}"))
}

/// Return the ordered list of field names that appear as a leading
/// backtick-wrapped identifier in any markdown table row, preserving
/// multiplicity so duplicate rows can be detected.
/// Rows whose first cell is prose (e.g. the `Classes` description table)
/// contain no backtick'd identifier and are ignored.
fn extract_table_field_rows(doc: &str) -> Vec<String> {
    let mut fields = Vec::new();
    for line in doc.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('|') {
            continue;
        }
        // Skip table header and separator rows.
        if trimmed.starts_with("|---") || trimmed.contains("| Field") {
            continue;
        }
        // First cell in the row.
        let first_cell = trimmed
            .trim_start_matches('|')
            .split('|')
            .next()
            .unwrap_or("")
            .trim();
        // Extract a backtick-wrapped identifier if present.
        if let Some(start) = first_cell.find('`') {
            let rest = &first_cell[start + 1..];
            if let Some(end) = rest.find('`') {
                let name = &rest[..end];
                // Ignore anything that looks like a placeholder/sub-class cell.
                if !name.is_empty() && !name.contains(' ') {
                    fields.push(name.to_string());
                }
            }
        }
    }
    fields
}

/// Return the set of class labels that appear in the Class column of the
/// two field tables (`StrataConfig` and `StorageConfig`). Identified by
/// looking for table rows whose first cell is a backtick-wrapped
/// identifier: those rows belong to a field table. Description tables use
/// prose in the first cell and are skipped.
fn extract_class_labels(doc: &str) -> HashSet<String> {
    let mut classes = HashSet::new();
    for line in doc.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('|') {
            continue;
        }
        if trimmed.starts_with("|---") {
            continue;
        }
        let cells: Vec<&str> = trimmed.trim_start_matches('|').split('|').collect();
        if cells.len() < 2 {
            continue;
        }
        // Accept only rows whose first cell is a backtick-wrapped identifier
        // (i.e. actual field rows, not Classes/description tables).
        let first = cells[0].trim();
        if !(first.starts_with('`') && first.ends_with('`') && first.len() > 2) {
            continue;
        }
        let class_cell = cells[1].trim();
        if class_cell.is_empty() || class_cell.starts_with('(') {
            continue;
        }
        let cleaned = class_cell.trim_matches('*').trim().to_string();
        if !cleaned.is_empty() {
            classes.insert(cleaned);
        }
    }
    classes
}

#[test]
fn every_config_field_is_classified_in_matrix_exactly_once() {
    let doc = read_matrix();
    let rows = extract_table_field_rows(&doc);

    // Exactly-once: every field appears in exactly one row, no duplicates.
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for r in &rows {
        *counts.entry(r.as_str()).or_insert(0) += 1;
    }
    let duplicates: Vec<(&&str, &usize)> = counts.iter().filter(|(_, &count)| count > 1).collect();
    assert!(
        duplicates.is_empty(),
        "config matrix has duplicate rows for these fields — each field must be classified \
         exactly once: {duplicates:?}"
    );

    let matrix_fields: HashSet<String> = rows.into_iter().collect();
    let mut expected: HashSet<String> = STRATA_CONFIG_FIELDS
        .iter()
        .map(ToString::to_string)
        .collect();
    for f in STORAGE_CONFIG_FIELDS {
        expected.insert(f.to_string());
    }

    let missing: Vec<&String> = expected.difference(&matrix_fields).collect();
    assert!(
        missing.is_empty(),
        "config matrix is missing these fields — add a row and a classification: {missing:?}"
    );

    let stray: Vec<&String> = matrix_fields.difference(&expected).collect();
    assert!(
        stray.is_empty(),
        "config matrix mentions fields that do not exist on StrataConfig/StorageConfig — \
         prune the row or fix the name: {stray:?}"
    );
}

#[test]
fn matrix_uses_only_recognized_class_labels() {
    let doc = read_matrix();
    let labels = extract_class_labels(&doc);

    let valid: HashSet<&str> = VALID_CLASSES.iter().copied().collect();
    let unrecognized: Vec<&String> = labels
        .iter()
        .filter(|l| !valid.contains(l.as_str()))
        .collect();

    assert!(
        unrecognized.is_empty(),
        "matrix uses class labels that are not in the approved set {VALID_CLASSES:?}: {unrecognized:?}"
    );
}

/// Sanity check: if someone adds a new field to one of the structs without
/// updating `STRATA_CONFIG_FIELDS` or `STORAGE_CONFIG_FIELDS`, the default
/// serde output will have a key the const list does not know about. This
/// catches drift between the const list and the structs themselves.
#[test]
fn struct_field_list_matches_default_serialization() {
    use serde_json::Value;
    use strata_engine::database::{StorageConfig, StrataConfig};

    let cfg_json = serde_json::to_value(StrataConfig::default()).unwrap();
    let storage_json = serde_json::to_value(StorageConfig::default()).unwrap();

    let cfg_keys: HashSet<String> = match cfg_json {
        Value::Object(map) => map.keys().cloned().collect(),
        _ => panic!("StrataConfig did not serialize as an object"),
    };
    let storage_keys: HashSet<String> = match storage_json {
        Value::Object(map) => map.keys().cloned().collect(),
        _ => panic!("StorageConfig did not serialize as an object"),
    };

    // Fields with `skip_serializing_if = "Option::is_none"` are absent at
    // their default None. These are the known Option-typed, skip-when-none
    // fields on StrataConfig — they're classified as non-durability in the
    // matrix and must still appear in the const list.
    let skip_when_none: HashSet<&str> = [
        "model",
        "default_model",
        "anthropic_api_key",
        "openai_api_key",
        "google_api_key",
    ]
    .into_iter()
    .collect();

    let const_cfg: HashSet<String> = STRATA_CONFIG_FIELDS
        .iter()
        .map(ToString::to_string)
        .collect();

    let mut effective_cfg = cfg_keys.clone();
    for k in &skip_when_none {
        effective_cfg.insert(k.to_string());
    }
    let missing_from_const: Vec<&String> = effective_cfg.difference(&const_cfg).collect();
    assert!(
        missing_from_const.is_empty(),
        "StrataConfig has fields that STRATA_CONFIG_FIELDS does not list — \
         add them to the const and to the matrix: {missing_from_const:?}"
    );
    let extra_in_const: Vec<&String> = const_cfg.difference(&effective_cfg).collect();
    assert!(
        extra_in_const.is_empty(),
        "STRATA_CONFIG_FIELDS lists names that are not on StrataConfig: {extra_in_const:?}"
    );

    let const_storage: HashSet<String> = STORAGE_CONFIG_FIELDS
        .iter()
        .map(ToString::to_string)
        .collect();
    let missing_storage: Vec<&String> = storage_keys.difference(&const_storage).collect();
    assert!(
        missing_storage.is_empty(),
        "StorageConfig has fields that STORAGE_CONFIG_FIELDS does not list: {missing_storage:?}"
    );
    let extra_storage: Vec<&String> = const_storage.difference(&storage_keys).collect();
    assert!(
        extra_storage.is_empty(),
        "STORAGE_CONFIG_FIELDS lists names that are not on StorageConfig: {extra_storage:?}"
    );
}
