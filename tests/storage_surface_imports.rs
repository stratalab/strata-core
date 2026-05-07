//! Ensure storage-facing types are imported from `strata_storage`.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const FORBIDDEN_DIRECT_PATTERNS: &[&str] = &[
    "strata_core::Storage",
    "strata_core::WriteMode",
    "strata_core::Key",
    "strata_core::Namespace",
    "strata_core::TypeTag",
    "strata_core::validate_space_name",
    "strata_core::traits::Storage",
    "strata_core::traits::WriteMode",
    "strata_core::types::Key",
    "strata_core::types::Namespace",
    "strata_core::types::TypeTag",
    "strata_concurrency::lock_ordering",
    "strata_concurrency::TransactionManager",
    "strata_durability::",
];

const ROOT_MOVED_TOKENS: &[&str] = &[
    "Storage",
    "WriteMode",
    "Key",
    "Namespace",
    "TypeTag",
    "validate_space_name",
];

const TYPES_MOVED_TOKENS: &[&str] = &["Key", "Namespace", "TypeTag"];
const CONCURRENCY_ROOT_MOVED_TOKENS: &[&str] = &[
    "TransactionContext",
    "TransactionStatus",
    "CommitError",
    "TransactionManager",
    "CoordinatorPlanError",
    "CoordinatorRecoveryError",
    "RecoveryCoordinator",
    "RecoveryPlan",
    "RecoveryResult",
    "RecoveryStats",
    "apply_wal_record_to_memory_storage",
];
const CONCURRENCY_TRANSACTION_MOVED_TOKENS: &[&str] = &[
    "ApplyResult",
    "CASOperation",
    "CommitError",
    "PendingOperations",
    "TransactionContext",
    "TransactionStatus",
];
const CONCURRENCY_VALIDATION_MOVED_TOKENS: &[&str] = &[
    "validate_cas_set",
    "validate_read_set",
    "validate_transaction",
    "ConflictType",
    "ValidationResult",
];
const CONCURRENCY_RECOVERY_MOVED_TOKENS: &[&str] = &[
    "CoordinatorPlanError",
    "CoordinatorRecoveryError",
    "RecoveryCoordinator",
    "RecoveryPlan",
    "RecoveryResult",
    "RecoveryStats",
    "apply_wal_record_to_memory_storage",
];

struct AllowedEngineConsolidationStorageUse {
    path: &'static str,
    removal_epic: &'static str,
}

const ENGINE_CONSOLIDATION_PRODUCTION_SCAN_ROOTS: &[&str] = &[
    "src",
    "crates/executor",
    "crates/intelligence",
    "crates/cli",
];

const ENGINE_CONSOLIDATION_UPPER_PRODUCTION_SOURCE_ROOTS: &[&str] = &[
    "src",
    "crates/executor/src",
    "crates/intelligence/src",
    "crates/cli/src",
];

const ENGINE_CONSOLIDATION_VECTOR_CONSUMER_SCAN_ROOTS: &[&str] = &[
    "src",
    "benchmarks",
    "crates/executor",
    "crates/intelligence",
    "crates/cli",
];

const ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES: &[AllowedEngineConsolidationStorageUse] = &[
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/Cargo.toml",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/bridge.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/compat.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/convert.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/handlers/event.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/handlers/json.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/handlers/kv.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/handlers/space_delete.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/lib.rs",
        removal_epic: "EG7",
    },
    AllowedEngineConsolidationStorageUse {
        path: "crates/executor/src/session.rs",
        removal_epic: "EG7",
    },
];

const RETIRED_SECURITY_PACKAGE: &str = concat!("strata", "-", "security");
const RETIRED_SECURITY_IMPORT: &str = concat!("strata", "_", "security");
const RETIRED_SECURITY_RELATIVE_PATHS: &[&str] =
    &[concat!("../", "security"), concat!("crates/", "security")];
const RETIRED_EXECUTOR_LEGACY_PACKAGE: &str = concat!("strata", "-", "executor", "-", "legacy");
const RETIRED_EXECUTOR_LEGACY_IMPORT: &str = concat!("strata", "_", "executor", "_", "legacy");
const RETIRED_EXECUTOR_LEGACY_RELATIVE_PATHS: &[&str] = &[
    concat!("../", "executor", "-", "legacy"),
    concat!("crates/", "executor", "-", "legacy"),
];
const RETIRED_GRAPH_PACKAGE: &str = concat!("strata", "-", "graph");
const RETIRED_GRAPH_IMPORT: &str = concat!("strata", "_", "graph");
const RETIRED_GRAPH_RELATIVE_PATHS: &[&str] =
    &[concat!("../", "graph"), concat!("crates/", "graph")];
const RETIRED_VECTOR_PACKAGE: &str = concat!("strata", "-", "vector");
const RETIRED_VECTOR_IMPORT: &str = concat!("strata", "_", "vector");
const RETIRED_VECTOR_RELATIVE_PATHS: &[&str] =
    &[concat!("../", "vector"), concat!("crates/", "vector")];
const RETIRED_SEARCH_PACKAGE: &str = concat!("strata", "-", "search");
const RETIRED_SEARCH_IMPORT: &str = concat!("strata", "_", "search");
const RETIRED_SEARCH_RELATIVE_PATHS: &[&str] =
    &[concat!("../", "search"), concat!("crates/", "search")];

#[test]
fn storage_facing_types_are_not_imported_from_strata_core() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&repo_root.join("crates"), &mut rust_files);
    collect_rust_files(&repo_root.join("tests"), &mut rust_files);

    let mut violations = Vec::new();
    for file in rust_files {
        if should_skip(&file) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read source file");
        for violation in find_violations(&contents) {
            violations.push(format!("{}:{violation}", file.display()));
        }
    }

    assert!(
        violations.is_empty(),
        "storage-facing types and the storage-owned transaction runtime must not be imported from legacy surfaces outside designated modules:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_security_surface_is_engine_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![repo_root.join("Cargo.toml")];
    collect_rust_files(&repo_root.join("src"), &mut files);
    collect_manifest_files(&repo_root.join("src"), &mut files);
    collect_rust_files(&repo_root.join("crates"), &mut files);
    collect_manifest_files(&repo_root.join("crates"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) || should_skip_manifest(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read production file");
        violations.extend(find_retired_security_surface_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "security/open surface must be consumed from `strata_engine`; retired crate references found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_legacy_executor_bootstrap_is_retired() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![repo_root.join("Cargo.toml")];
    collect_rust_files(&repo_root.join("src"), &mut files);
    collect_manifest_files(&repo_root.join("src"), &mut files);
    collect_rust_files(&repo_root.join("crates"), &mut files);
    collect_manifest_files(&repo_root.join("crates"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) || should_skip_manifest(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read production file");
        violations.extend(find_retired_executor_legacy_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "product open/bootstrap must be consumed from `strata_engine`; retired executor-legacy references found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_graph_crate_is_retired() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let retired_dir = repo_root.join(RETIRED_GRAPH_RELATIVE_PATHS[1]);
    assert!(
        !retired_dir.exists(),
        "graph implementation must live in `strata_engine`; retired graph crate directory still exists at {}",
        retired_dir.display()
    );

    let mut files = vec![repo_root.join("Cargo.toml")];
    collect_rust_files(&repo_root.join("src"), &mut files);
    collect_manifest_files(&repo_root.join("src"), &mut files);
    collect_rust_files(&repo_root.join("crates"), &mut files);
    collect_manifest_files(&repo_root.join("crates"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) || should_skip_manifest(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read production file");
        violations.extend(find_retired_graph_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "graph behavior must be consumed from `strata_engine`; retired graph crate references found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_graph_subsystem_is_engine_product_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in ENGINE_CONSOLIDATION_UPPER_PRODUCTION_SOURCE_ROOTS {
        collect_rust_files(&repo_root.join(root), &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read production source file");
        violations.extend(find_upper_subsystem_violations(
            &rel,
            &contents,
            "GraphSubsystem",
        ));
    }

    assert!(
        violations.is_empty(),
        "production crates above engine must not assemble `GraphSubsystem`; graph product runtime registration is engine-owned:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_vector_subsystem_is_engine_product_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in ENGINE_CONSOLIDATION_UPPER_PRODUCTION_SOURCE_ROOTS {
        collect_rust_files(&repo_root.join(root), &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read production source file");
        violations.extend(find_upper_subsystem_violations(
            &rel,
            &contents,
            "VectorSubsystem",
        ));
    }

    assert!(
        violations.is_empty(),
        "production crates above engine must not assemble `VectorSubsystem`; vector product runtime registration is engine-owned:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_search_subsystem_is_engine_product_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in ENGINE_CONSOLIDATION_UPPER_PRODUCTION_SOURCE_ROOTS {
        collect_rust_files(&repo_root.join(root), &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read production source file");
        violations.extend(find_upper_subsystem_violations(
            &rel,
            &contents,
            "SearchSubsystem",
        ));
    }

    assert!(
        violations.is_empty(),
        "production crates above engine must not assemble `SearchSubsystem`; search product runtime registration is engine-owned:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_vector_crate_is_retired() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let retired_dir = repo_root.join(RETIRED_VECTOR_RELATIVE_PATHS[1]);
    assert!(
        !retired_dir.exists(),
        "vector implementation must live in `strata_engine`; retired vector crate directory still exists at {}",
        retired_dir.display()
    );

    let mut files = vec![repo_root.join("Cargo.toml")];
    collect_rust_files(&repo_root.join("src"), &mut files);
    collect_manifest_files(&repo_root.join("src"), &mut files);
    collect_rust_files(&repo_root.join("benchmarks"), &mut files);
    collect_manifest_files(&repo_root.join("benchmarks"), &mut files);
    collect_rust_files(&repo_root.join("crates"), &mut files);
    collect_manifest_files(&repo_root.join("crates"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) || should_skip_manifest(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read production file");
        violations.extend(find_retired_vector_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "vector behavior must be consumed from `strata_engine`; retired vector crate references found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_search_crate_is_retired() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let retired_dir = repo_root.join(RETIRED_SEARCH_RELATIVE_PATHS[1]);
    assert!(
        !retired_dir.exists(),
        "search implementation must live in `strata_engine`; retired search crate directory still exists at {}",
        retired_dir.display()
    );

    let mut files = vec![repo_root.join("Cargo.toml")];
    collect_rust_files(&repo_root.join("src"), &mut files);
    collect_manifest_files(&repo_root.join("src"), &mut files);
    collect_rust_files(&repo_root.join("benchmarks"), &mut files);
    collect_manifest_files(&repo_root.join("benchmarks"), &mut files);
    collect_rust_files(&repo_root.join("crates"), &mut files);
    collect_manifest_files(&repo_root.join("crates"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) || should_skip_manifest(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read production file");
        violations.extend(find_retired_search_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "search behavior must be consumed from `strata_engine`; retired search crate references found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_vector_consumers_use_engine_surface() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    files.push(repo_root.join("Cargo.toml"));
    for root in ENGINE_CONSOLIDATION_VECTOR_CONSUMER_SCAN_ROOTS {
        let path = repo_root.join(root);
        collect_rust_files(&path, &mut files);
        collect_manifest_files(&path, &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read production source or manifest");
        for (line_no, line) in contents.lines().enumerate() {
            if line.contains("strata_vector") || line.contains("strata-vector") {
                violations.push(format!("{}:{}: {}", rel, line_no + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production crates above engine must consume vector surfaces from `strata_engine`, not the retired vector crate:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_executor_vector_transaction_surface_is_engine_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&repo_root.join("crates/executor/src"), &mut files);

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read executor source file");
        for (line_no, line) in contents.lines().enumerate() {
            if line.contains("strata_vector")
                || line.contains("VectorStoreExt")
                || line.contains("StagedVectorOp")
                || line.contains("apply_staged_vector_op")
            {
                violations.push(format!("{}:{}: {}", rel, line_no + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "executor production code must consume vector surfaces from `strata_engine`, not the retired vector crate or internal staged-op API:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_vector_storage_key_layout_is_engine_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in ENGINE_CONSOLIDATION_UPPER_PRODUCTION_SOURCE_ROOTS {
        collect_rust_files(&repo_root.join(root), &mut files);
    }

    let mut violations = Vec::new();
    for file in files {
        if should_skip(&file) {
            continue;
        }

        let rel = repo_relative_path(&repo_root, &file);
        if is_test_source_path(&rel) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read production source file");
        violations.extend(find_vector_storage_key_layout_violations(&rel, &contents));
    }

    assert!(
        violations.is_empty(),
        "production crates above engine must not construct vector storage key layout directly:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_surface_manifests_do_not_regress_to_deleted_or_transitional_crates() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut manifests = Vec::new();
    manifests.push(repo_root.join("Cargo.toml"));
    collect_manifest_files(&repo_root.join("benchmarks"), &mut manifests);
    collect_manifest_files(&repo_root.join("crates"), &mut manifests);

    let mut violations = Vec::new();
    for manifest in manifests {
        if should_skip_manifest(&manifest) {
            continue;
        }

        let contents = fs::read_to_string(&manifest).expect("read manifest");
        for violation in find_manifest_violations(&manifest, &contents) {
            violations.push(violation);
        }
    }

    assert!(
        violations.is_empty(),
        "workspace manifests must not regress to deleted or transitional crate edges outside designated seams:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_consolidation_direct_storage_bypasses_are_allowlisted() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    for root in ENGINE_CONSOLIDATION_PRODUCTION_SCAN_ROOTS {
        let path = repo_root.join(root);
        collect_rust_files(&path, &mut files);
        collect_manifest_files(&path, &mut files);
    }

    let mut violations = Vec::new();
    let mut observed = BTreeSet::new();

    for file in files {
        if should_skip(&file) {
            continue;
        }
        let rel = repo_relative_path(&repo_root, &file);
        let contents = fs::read_to_string(&file).expect("read source or manifest");

        for (line_no, line) in contents.lines().enumerate() {
            if !contains_direct_storage_reference(line) {
                continue;
            }

            match allowed_engine_consolidation_storage_use(&rel) {
                Some(allowed) => {
                    observed.insert(allowed.path);
                }
                None => violations.push(format!(
                    "{}:{}: direct `strata-storage` use is not in the EG1D allowlist: {}",
                    rel,
                    line_no + 1,
                    line.trim()
                )),
            }
        }
    }

    let stale: Vec<_> = ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES
        .iter()
        .filter(|allowed| !observed.contains(allowed.path))
        .map(|allowed| format!("{} ({})", allowed.path, allowed.removal_epic))
        .collect();

    assert!(
        violations.is_empty(),
        "production crates above engine may only use `strata-storage` through explicitly allowlisted engine-consolidation seams:\n{}",
        violations.join("\n")
    );
    assert!(
        stale.is_empty(),
        "EG1D storage bypass allowlist contains stale entries. Remove these entries when their epic lands:\n{}",
        stale.join("\n")
    );
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }

    for entry in fs::read_dir(dir).expect("read directory") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn collect_manifest_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }

    for entry in fs::read_dir(dir).expect("read directory") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_manifest_files(&path, out);
        } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            out.push(path);
        }
    }
}

fn should_skip(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/") || rel.ends_with("/tests/storage_surface_imports.rs")
}

fn should_skip_manifest(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/")
}

fn is_test_source_path(rel: &str) -> bool {
    rel == "tests" || rel.starts_with("tests/") || rel.contains("/tests/")
}

fn repo_relative_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .expect("path should be under repository root")
        .to_string_lossy()
        .replace('\\', "/")
}

fn contains_direct_storage_reference(line: &str) -> bool {
    line.contains("strata_storage")
        || line.contains("strata-storage")
        || line.contains("use strata_storage")
        || line.contains("pub use strata_storage")
}

fn find_retired_security_surface_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains(RETIRED_SECURITY_PACKAGE)
            || line.contains(RETIRED_SECURITY_IMPORT)
            || RETIRED_SECURITY_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: retired security/open crate reference: {}",
                rel,
                line_no + 1,
                line.trim()
            ));
        }
    }

    violations
}

fn find_retired_executor_legacy_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains(RETIRED_EXECUTOR_LEGACY_PACKAGE)
            || line.contains(RETIRED_EXECUTOR_LEGACY_IMPORT)
            || RETIRED_EXECUTOR_LEGACY_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: retired executor-legacy bootstrap reference: {}",
                rel,
                line_no + 1,
                line.trim()
            ));
        }
    }

    violations
}

fn find_retired_graph_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains(RETIRED_GRAPH_PACKAGE)
            || line.contains(RETIRED_GRAPH_IMPORT)
            || RETIRED_GRAPH_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: retired graph crate reference: {}",
                rel,
                line_no + 1,
                line.trim()
            ));
        }
    }

    violations
}

fn find_retired_vector_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains(RETIRED_VECTOR_PACKAGE)
            || line.contains(RETIRED_VECTOR_IMPORT)
            || RETIRED_VECTOR_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: retired vector crate reference: {}",
                rel,
                line_no + 1,
                line.trim()
            ));
        }
    }

    violations
}

fn find_retired_search_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains(RETIRED_SEARCH_PACKAGE)
            || line.contains(RETIRED_SEARCH_IMPORT)
            || RETIRED_SEARCH_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: retired search crate reference: {}",
                rel,
                line_no + 1,
                line.trim()
            ));
        }
    }

    violations
}

const VECTOR_STORAGE_KEY_LAYOUT_MARKERS: &[&str] = &[
    "TypeTag::Vector",
    "Key::new_vector",
    "Key::new_vector_config",
    "Key::vector_collection_prefix",
    "new_vector_config_prefix",
];

fn find_vector_storage_key_layout_violations(rel: &str, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let mut pending_cfg_test = false;
    let mut skipped_cfg_test_depth = None;
    let mut lex_state = RustCodeLexState::default();

    for (line_no, line) in contents.lines().enumerate() {
        let code = rust_code_only_line(line, &mut lex_state);

        if let Some(depth) = skipped_cfg_test_depth.as_mut() {
            update_brace_depth(depth, &code);
            if *depth == 0 {
                skipped_cfg_test_depth = None;
            }
            continue;
        }

        let trimmed = code.trim();
        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }

            if line_declares_inline_module(trimmed) {
                let mut depth = 0usize;
                update_brace_depth(&mut depth, &code);
                if depth > 0 {
                    skipped_cfg_test_depth = Some(depth);
                }
                pending_cfg_test = false;
                continue;
            }

            pending_cfg_test = false;
        }

        if VECTOR_STORAGE_KEY_LAYOUT_MARKERS
            .iter()
            .any(|marker| code.contains(marker))
        {
            violations.push(format!("{}:{}: {}", rel, line_no + 1, line.trim()));
        }
    }

    violations
}

fn find_upper_subsystem_violations(rel: &str, contents: &str, subsystem_name: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let mut pending_cfg_test = false;
    let mut skipped_cfg_test_depth = None;
    let mut lex_state = RustCodeLexState::default();

    for (line_no, line) in contents.lines().enumerate() {
        let code = rust_code_only_line(line, &mut lex_state);

        if let Some(depth) = skipped_cfg_test_depth.as_mut() {
            update_brace_depth(depth, &code);
            if *depth == 0 {
                skipped_cfg_test_depth = None;
            }
            continue;
        }

        let trimmed = code.trim();
        if is_cfg_test_attr(trimmed) {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }

            if line_declares_inline_module(trimmed) {
                let mut depth = 0usize;
                update_brace_depth(&mut depth, &code);
                if depth > 0 {
                    skipped_cfg_test_depth = Some(depth);
                }
                pending_cfg_test = false;
                continue;
            }

            pending_cfg_test = false;
        }

        if code.contains(subsystem_name) {
            violations.push(format!(
                "{}:{}: production upper crate names `{}`: {}",
                rel,
                line_no + 1,
                subsystem_name,
                line.trim()
            ));
        }
    }

    violations
}

fn is_cfg_test_attr(trimmed_code: &str) -> bool {
    let compact = trimmed_code
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();

    compact.starts_with("#[cfg(")
        && !compact.contains("not(test)")
        && contains_cfg_atom(&compact, "test")
}

fn contains_cfg_atom(compact_cfg_attr: &str, atom: &str) -> bool {
    let mut offset = 0usize;
    while let Some(relative) = compact_cfg_attr[offset..].find(atom) {
        let start = offset + relative;
        let end = start + atom.len();
        let before = compact_cfg_attr[..start].chars().next_back();
        let after = compact_cfg_attr[end..].chars().next();

        let before_is_boundary = before.is_none_or(|ch| !is_rust_identifier_continue(ch));
        let after_is_boundary = after.is_none_or(|ch| !is_rust_identifier_continue(ch));
        if before_is_boundary && after_is_boundary {
            return true;
        }

        offset = end;
    }

    false
}

fn line_declares_inline_module(trimmed_code: &str) -> bool {
    if !trimmed_code.contains('{') {
        return false;
    }

    let before_brace = trimmed_code.split('{').next().unwrap_or_default();
    let mut saw_mod = false;
    for token in before_brace
        .split(|ch: char| !is_rust_identifier_continue(ch))
        .filter(|token| !token.is_empty())
    {
        if saw_mod {
            return true;
        }
        saw_mod = token == "mod";
    }

    false
}

fn update_brace_depth(depth: &mut usize, line: &str) {
    for ch in line.chars() {
        match ch {
            '{' => *depth += 1,
            '}' => *depth = depth.saturating_sub(1),
            _ => {}
        }
    }
}

#[derive(Default)]
struct RustCodeLexState {
    mode: RustCodeLexMode,
}

#[derive(Default)]
enum RustCodeLexMode {
    #[default]
    Code,
    BlockComment {
        depth: usize,
    },
    CookedLiteral {
        delimiter: u8,
        escaped: bool,
    },
    RawString {
        hashes: usize,
    },
}

fn rust_code_only_line(line: &str, state: &mut RustCodeLexState) -> String {
    let bytes = line.as_bytes();
    let mut code = String::with_capacity(line.len());
    let mut index = 0;

    while index < bytes.len() {
        match &mut state.mode {
            RustCodeLexMode::Code => {
                if starts_with(bytes, index, b"//") {
                    push_spaces_for_remaining(&mut code, bytes, index);
                    break;
                }

                if starts_with(bytes, index, b"/*") {
                    state.mode = RustCodeLexMode::BlockComment { depth: 1 };
                    push_spaces(&mut code, 2);
                    index += 2;
                    continue;
                }

                if let Some((literal_len, hashes)) = raw_string_start(bytes, index) {
                    state.mode = RustCodeLexMode::RawString { hashes };
                    push_spaces(&mut code, literal_len);
                    index += literal_len;
                    continue;
                }

                if bytes[index] == b'"'
                    || (bytes[index] == b'\'' && !looks_like_lifetime(bytes, index))
                {
                    state.mode = RustCodeLexMode::CookedLiteral {
                        delimiter: bytes[index],
                        escaped: false,
                    };
                    code.push(' ');
                    index += 1;
                    continue;
                }

                code.push(bytes[index] as char);
                index += 1;
            }
            RustCodeLexMode::BlockComment { depth } => {
                if starts_with(bytes, index, b"/*") {
                    *depth += 1;
                    push_spaces(&mut code, 2);
                    index += 2;
                    continue;
                }

                if starts_with(bytes, index, b"*/") {
                    *depth = depth.saturating_sub(1);
                    push_spaces(&mut code, 2);
                    index += 2;
                    if *depth == 0 {
                        state.mode = RustCodeLexMode::Code;
                    }
                    continue;
                }

                code.push(' ');
                index += 1;
            }
            RustCodeLexMode::CookedLiteral { delimiter, escaped } => {
                code.push(' ');
                if *escaped {
                    *escaped = false;
                } else if bytes[index] == b'\\' {
                    *escaped = true;
                } else if bytes[index] == *delimiter {
                    state.mode = RustCodeLexMode::Code;
                }
                index += 1;
            }
            RustCodeLexMode::RawString { hashes } => {
                if raw_string_ends_at(bytes, index, *hashes) {
                    push_spaces(&mut code, 1 + *hashes);
                    index += 1 + *hashes;
                    state.mode = RustCodeLexMode::Code;
                    continue;
                }

                code.push(' ');
                index += 1;
            }
        }
    }

    code
}

fn raw_string_start(bytes: &[u8], index: usize) -> Option<(usize, usize)> {
    let mut cursor = match bytes.get(index) {
        Some(b'r') => index + 1,
        Some(b'b') if bytes.get(index + 1) == Some(&b'r') => index + 2,
        _ => return None,
    };

    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }

    if bytes.get(cursor) == Some(&b'"') {
        Some((
            cursor + 1 - index,
            cursor - index - usize::from(bytes[index] == b'b') - 1,
        ))
    } else {
        None
    }
}

fn raw_string_ends_at(bytes: &[u8], index: usize, hashes: usize) -> bool {
    bytes.get(index) == Some(&b'"')
        && (0..hashes).all(|offset| bytes.get(index + 1 + offset) == Some(&b'#'))
}

fn starts_with(bytes: &[u8], index: usize, needle: &[u8]) -> bool {
    bytes.get(index..index + needle.len()) == Some(needle)
}

fn looks_like_lifetime(bytes: &[u8], index: usize) -> bool {
    let Some(next) = bytes.get(index + 1).copied() else {
        return false;
    };

    is_rust_identifier_start(next as char) && bytes.get(index + 2) != Some(&b'\'')
}

fn is_rust_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_rust_identifier_continue(ch: char) -> bool {
    is_rust_identifier_start(ch) || ch.is_ascii_digit()
}

fn push_spaces(code: &mut String, count: usize) {
    code.extend(std::iter::repeat_n(' ', count));
}

fn push_spaces_for_remaining(code: &mut String, bytes: &[u8], index: usize) {
    push_spaces(code, bytes.len().saturating_sub(index));
}

fn allowed_engine_consolidation_storage_use(
    rel: &str,
) -> Option<&'static AllowedEngineConsolidationStorageUse> {
    ALLOWED_ENGINE_CONSOLIDATION_STORAGE_USES
        .iter()
        .find(|allowed| allowed.path == rel)
}

fn find_violations(contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    for pattern in FORBIDDEN_DIRECT_PATTERNS {
        for (line_no, line) in contents.lines().enumerate() {
            if line.contains(pattern) {
                violations.push(format!("{}: contains `{pattern}`", line_no + 1));
            }
        }
    }

    violations.extend(scan_import_blocks(
        contents,
        "strata_core::{",
        ROOT_MOVED_TOKENS,
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_core::types::{",
        TYPES_MOVED_TOKENS,
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_core::traits::{",
        &["Storage", "WriteMode"],
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_concurrency::{",
        CONCURRENCY_ROOT_MOVED_TOKENS,
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_concurrency::transaction::{",
        CONCURRENCY_TRANSACTION_MOVED_TOKENS,
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_concurrency::validation::{",
        CONCURRENCY_VALIDATION_MOVED_TOKENS,
    ));
    violations.extend(scan_import_blocks(
        contents,
        "strata_concurrency::recovery::{",
        CONCURRENCY_RECOVERY_MOVED_TOKENS,
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_core",
        ROOT_MOVED_TOKENS,
        &[
            "traits::Storage",
            "traits::WriteMode",
            "types::Key",
            "types::Namespace",
            "types::TypeTag",
        ],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_core::types",
        TYPES_MOVED_TOKENS,
        &[],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_core::traits",
        &["Storage", "WriteMode"],
        &[],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_concurrency",
        CONCURRENCY_ROOT_MOVED_TOKENS,
        &[],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_concurrency::transaction",
        CONCURRENCY_TRANSACTION_MOVED_TOKENS,
        &[],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_concurrency::validation",
        CONCURRENCY_VALIDATION_MOVED_TOKENS,
        &[],
    ));
    violations.extend(scan_alias_uses(
        contents,
        "strata_concurrency::recovery",
        CONCURRENCY_RECOVERY_MOVED_TOKENS,
        &[],
    ));

    violations
}

fn find_manifest_violations(path: &Path, contents: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let rel = path.to_string_lossy();

    for (line_no, line) in contents.lines().enumerate() {
        if line.contains("strata-concurrency")
            || line.contains("path = \"../concurrency\"")
            || line.contains("path = \"crates/concurrency\"")
        {
            violations.push(format!(
                "{}:{}: manifest references deleted `strata-concurrency` surface",
                rel,
                line_no + 1
            ));
        }

        if line.contains("strata-durability")
            || line.contains("path = \"../durability\"")
            || line.contains("path = \"crates/durability\"")
        {
            violations.push(format!(
                "{}:{}: manifest references deleted `strata-durability` surface",
                rel,
                line_no + 1
            ));
        }

        if line.contains(RETIRED_EXECUTOR_LEGACY_PACKAGE)
            || RETIRED_EXECUTOR_LEGACY_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: manifest references retired `strata-executor-legacy` bootstrap crate",
                rel,
                line_no + 1
            ));
        }

        if line.contains(RETIRED_GRAPH_PACKAGE)
            || RETIRED_GRAPH_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: manifest references retired `{}` crate",
                rel,
                line_no + 1,
                RETIRED_GRAPH_PACKAGE
            ));
        }

        if line.contains(RETIRED_VECTOR_PACKAGE)
            || RETIRED_VECTOR_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: manifest references retired `{}` crate",
                rel,
                line_no + 1,
                RETIRED_VECTOR_PACKAGE
            ));
        }

        if line.contains(RETIRED_SEARCH_PACKAGE)
            || RETIRED_SEARCH_RELATIVE_PATHS
                .iter()
                .any(|path| line.contains(path))
        {
            violations.push(format!(
                "{}:{}: manifest references retired `{}` crate",
                rel,
                line_no + 1,
                RETIRED_SEARCH_PACKAGE
            ));
        }

        if line.contains("strata-core-legacy") || line.contains("core-legacy") {
            violations.push(format!(
                "{}:{}: manifest references retired `strata-core-legacy` crate",
                rel,
                line_no + 1
            ));
        }
    }

    violations
}

fn scan_import_blocks(contents: &str, marker: &str, forbidden_tokens: &[&str]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut in_block = false;
    let mut block = String::new();
    let mut start_line = 0usize;

    for (line_no, line) in contents.lines().enumerate() {
        if !in_block
            && line.contains(marker)
            && (line.contains("use ") || line.contains("pub use "))
        {
            in_block = true;
            start_line = line_no + 1;
            block.clear();
        }

        if in_block {
            block.push_str(line);
            block.push('\n');

            if line.contains("};") || line.contains('}') {
                if forbidden_tokens
                    .iter()
                    .any(|token| block_contains_token(&block, token))
                {
                    violations.push(format!(
                        "{start_line}: forbidden {} import block",
                        marker.trim_end_matches('{')
                    ));
                }
                in_block = false;
            }
        }
    }

    violations
}

fn block_contains_token(block: &str, token: &str) -> bool {
    block.contains(&format!("{token},"))
        || block.contains(&format!("{token} "))
        || block.contains(&format!("{token}\n"))
        || block.contains(&format!("{token}}}"))
        || block.contains(&format!("{token} as "))
}

fn scan_alias_uses(
    contents: &str,
    target: &str,
    forbidden_symbols: &[&str],
    forbidden_paths: &[&str],
) -> Vec<String> {
    let mut violations = Vec::new();
    for alias in collect_aliases(contents, target) {
        for (line_no, line) in contents.lines().enumerate() {
            for symbol in forbidden_symbols {
                if line.contains(&format!("{alias}::{symbol}")) {
                    violations.push(format!(
                        "{}: contains aliased `{target}` symbol `{alias}::{symbol}`",
                        line_no + 1
                    ));
                }
            }
            for path in forbidden_paths {
                if line.contains(&format!("{alias}::{path}")) {
                    violations.push(format!(
                        "{}: contains aliased `{target}` path `{alias}::{path}`",
                        line_no + 1
                    ));
                }
            }
        }
    }
    violations
}

fn collect_aliases(contents: &str, target: &str) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    let direct_use = format!("use {target} as ");
    let direct_pub_use = format!("pub use {target} as ");

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.contains(&direct_use) || trimmed.contains(&direct_pub_use) {
            if let Some(alias) = extract_alias_after_as(trimmed) {
                aliases.insert(alias);
            }
        }
    }

    let block_marker = format!("{target}::{{");
    for (_, block) in collect_multiline_use_blocks(contents, &block_marker) {
        for alias in extract_self_aliases(&block) {
            aliases.insert(alias);
        }
    }

    aliases.into_iter().collect()
}

fn extract_alias_after_as(line: &str) -> Option<String> {
    let (_, alias) = line.split_once(" as ")?;
    let alias = alias
        .trim()
        .trim_end_matches(';')
        .trim_end_matches(',')
        .trim();
    if alias.is_empty() {
        None
    } else {
        Some(alias.to_string())
    }
}

fn collect_multiline_use_blocks(contents: &str, marker: &str) -> Vec<(usize, String)> {
    let mut blocks = Vec::new();
    let mut current = String::new();
    let mut start_line = 0usize;
    let mut depth = 0isize;
    let mut collecting = false;

    for (idx, line) in contents.lines().enumerate() {
        let trimmed = line.trim_start();
        if !collecting
            && (trimmed.contains(&format!("use {marker}"))
                || trimmed.contains(&format!("pub use {marker}")))
        {
            collecting = true;
            start_line = idx + 1;
            current.clear();
            depth = 0;
        }

        if collecting {
            current.push_str(line);
            current.push('\n');
            depth += brace_delta(line);
            if depth <= 0 && line.contains(';') {
                blocks.push((start_line, current.clone()));
                collecting = false;
            }
        }
    }

    blocks
}

fn extract_self_aliases(block: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    for part in block.split(',') {
        if let Some((_, alias)) = part.split_once("self as ") {
            let alias = alias
                .trim()
                .trim_end_matches('}')
                .trim_end_matches(';')
                .trim();
            if !alias.is_empty() {
                aliases.push(alias.to_string());
            }
        }
    }
    aliases
}

fn brace_delta(line: &str) -> isize {
    let opens = line.chars().filter(|c| *c == '{').count() as isize;
    let closes = line.chars().filter(|c| *c == '}').count() as isize;
    opens - closes
}

#[test]
fn aliased_storage_imports_are_detected() {
    let contents = r#"
use strata_core as sc;
use strata_core::types as types;
use strata_core::traits::{
    self as storage_traits,
};

fn sample() {
    let _ = sc::Key::new_space_prefix(sc::BranchId::new());
    let _ = types::Namespace::for_branch(sc::BranchId::new());
    let _storage: &dyn storage_traits::Storage;
}
"#;

    let violations = find_violations(contents);
    assert!(
        violations.iter().any(|line| line.contains("sc::Key")),
        "root alias should be detected"
    );
    assert!(
        violations
            .iter()
            .any(|line| line.contains("types::Namespace")),
        "module alias should be detected"
    );
    assert!(
        violations
            .iter()
            .any(|line| line.contains("storage_traits::Storage")),
        "self-alias import blocks should be detected"
    );
}

#[test]
fn vector_storage_key_layout_guard_detects_production_use() {
    let contents = r#"
fn product_runtime() {
    let _tag = TypeTag::Vector;
    let _row = Key::new_vector(namespace, collection, key);
    let _config = Key::new_vector_config(namespace, collection);
    let _prefix = Key::vector_collection_prefix(namespace, collection);
    let _config_prefix = new_vector_config_prefix(namespace);
}
"#;

    let violations =
        find_vector_storage_key_layout_violations("crates/example/src/lib.rs", contents);
    assert_eq!(violations.len(), 5, "{violations:?}");
}

#[test]
fn vector_storage_key_layout_guard_ignores_literals_comments_and_cfg_test_modules() {
    let contents = r###"
const TAG_TEXT: &str = "TypeTag::Vector";
const RAW_PREFIX: &str = r#"Key::vector_collection_prefix"#;

// Key::new_vector(namespace, collection, key)
/* Key::new_vector_config(namespace, collection) */

#[cfg(test)]
mod vector_storage_tests {
    fn test_runtime() {
        let _tag = TypeTag::Vector;
        let _row = Key::new_vector(namespace, collection, key);
    }
}

fn product_runtime() {
    let _tag = TypeTag::Vector;
}
"###;

    let violations =
        find_vector_storage_key_layout_violations("crates/example/src/lib.rs", contents);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("TypeTag::Vector"), "{violations:?}");
}

#[test]
fn graph_subsystem_guard_detects_production_use() {
    let contents = r#"
use strata_engine::GraphSubsystem;

fn product_runtime() {
    let _subsystem = GraphSubsystem;
}
"#;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert_eq!(violations.len(), 2);
}

#[test]
fn graph_subsystem_guard_ignores_cfg_test_modules() {
    let contents = r#"
fn production_code() {}

#[cfg(test)]
mod tests {
    use strata_engine::GraphSubsystem;

    fn test_runtime() {
        let _subsystem = GraphSubsystem;
    }
}
"#;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn graph_subsystem_guard_ignores_braces_inside_cfg_test_literals_and_comments() {
    let contents = r###"
#[cfg(test)]
mod tests {
    use strata_engine::GraphSubsystem;

    const CLOSE_BRACE: &str = "}";
    const RAW_BRACES: &str = r#"{ }"#;

    // }
    /* { */

    fn test_runtime() {
        let _subsystem = GraphSubsystem;
    }
}

fn product_runtime() {
    let _subsystem = strata_engine::GraphSubsystem;
}
"###;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains("product_runtime")
            || violations[0].contains("strata_engine::GraphSubsystem"),
        "{violations:?}"
    );
}

#[test]
fn graph_subsystem_guard_ignores_cfg_test_modules_with_nonstandard_names() {
    let contents = r#"
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod graph_tests {
    use strata_engine::GraphSubsystem;

    fn test_runtime() {
        let _subsystem = GraphSubsystem;
    }
}
"#;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn graph_subsystem_guard_does_not_ignore_not_test_cfg_modules() {
    let contents = r#"
#[cfg(not(test))]
mod runtime {
    fn product_runtime() {
        let _subsystem = strata_engine::GraphSubsystem;
    }
}
"#;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert_eq!(violations.len(), 1, "{violations:?}");
}

#[test]
fn graph_subsystem_guard_does_not_treat_test_substrings_as_test_cfg() {
    let contents = r#"
#[cfg(test_support)]
mod runtime {
    fn product_runtime() {
        let _subsystem = strata_engine::GraphSubsystem;
    }
}
"#;

    let violations =
        find_upper_subsystem_violations("crates/example/src/lib.rs", contents, "GraphSubsystem");
    assert_eq!(violations.len(), 1, "{violations:?}");
}
