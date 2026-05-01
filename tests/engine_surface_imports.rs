//! Ensure engine-facing domain and primitive types are imported from `strata_engine`.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const LEGACY_CORE_COMPAT_ALIAS: &str = concat!("strata_core_legacy_", "compat");
const LEGACY_CORE_PACKAGE: &str = concat!("strata-core-", "legacy");
const LEGACY_CORE_PACKAGE_DECL: &str = concat!("package = \"", "strata-core-", "legacy", "\"");

const FORBIDDEN_DIRECT_PATTERNS: &[&str] = &[
    "strata_core::branch::",
    "strata_core::branch_dag::",
    "strata_core::branch_types::",
    "strata_core::limits::",
    "strata_core::StrataError",
    "strata_core::StrataResult",
    "strata_core::PrimitiveDegradedReason",
    "strata_core::json::",
    "strata_core::primitives::json::",
    "strata_core::primitives::vector::",
    "strata_core::primitives::event::",
    "strata_core::extractable_text",
];

const FORBIDDEN_FOUNDATION_COMPAT_PATTERNS: &[&str] =
    &["strata_core::types::BranchId", "strata_core::value::Value"];

const FORBIDDEN_ROOT_BRANCH_SYMBOLS: &[&str] = &[
    "BranchRef",
    "BranchGeneration",
    "BranchControlRecord",
    "ForkAnchor",
    "BranchLifecycleStatus",
    "BranchMetadata",
    "BranchStatus",
    "BranchEventOffsets",
];

const FORBIDDEN_ROOT_LIMIT_SYMBOLS: &[&str] = &["Limits", "LimitError"];
const FORBIDDEN_ROOT_EVENT_SYMBOLS: &[&str] = &["Event", "ChainVerification"];
const FORBIDDEN_ROOT_ERROR_SYMBOLS: &[&str] =
    &["StrataError", "StrataResult", "PrimitiveDegradedReason"];
const FORBIDDEN_ROOT_RUNTIME_HELPERS: &[&str] = &["extractable_text"];
const FORBIDDEN_ROOT_JSON_SYMBOLS: &[&str] = &[
    "JsonPath",
    "JsonValue",
    "JsonPatch",
    "JsonPathError",
    "PathParseError",
    "PathSegment",
    "JsonLimitError",
];
const FORBIDDEN_ROOT_VECTOR_SYMBOLS: &[&str] = &[
    "VectorConfig",
    "DistanceMetric",
    "StorageDtype",
    "CollectionId",
    "CollectionInfo",
    "VectorEntry",
    "VectorId",
    "VectorMatch",
    "MetadataFilter",
    "JsonScalar",
    "FilterCondition",
    "FilterOp",
];

#[test]
fn engine_facing_types_are_not_imported_from_strata_core() {
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
        for pattern in FORBIDDEN_DIRECT_PATTERNS {
            for (line_no, line) in contents.lines().enumerate() {
                if is_comment_line(line) {
                    continue;
                }
                if line.contains(pattern) {
                    violations.push(format!(
                        "{}:{}: contains `{pattern}`",
                        file.display(),
                        line_no + 1
                    ));
                }
            }
        }

        for (line_no, line) in contents.lines().enumerate() {
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_BRANCH_SYMBOLS,
                "branch-domain",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_LIMIT_SYMBOLS,
                "limits",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_EVENT_SYMBOLS,
                "event",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_ERROR_SYMBOLS,
                "parent error",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_RUNTIME_HELPERS,
                "runtime helper",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_JSON_SYMBOLS,
                "json",
            );
            check_line_for_root_symbols(
                &mut violations,
                &file,
                line_no + 1,
                line,
                FORBIDDEN_ROOT_VECTOR_SYMBOLS,
                "vector",
            );
        }

        for (start_line, block) in collect_multiline_strata_core_use_blocks(&contents) {
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_BRANCH_SYMBOLS,
                "branch-domain",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_LIMIT_SYMBOLS,
                "limits",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_EVENT_SYMBOLS,
                "event",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_ERROR_SYMBOLS,
                "parent error",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_RUNTIME_HELPERS,
                "runtime helper",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_JSON_SYMBOLS,
                "json",
            );
            check_block_for_root_symbols(
                &mut violations,
                &file,
                start_line,
                &block,
                FORBIDDEN_ROOT_VECTOR_SYMBOLS,
                "vector",
            );
        }

        for (target, symbols, paths, family) in [
            (
                "strata_core",
                FORBIDDEN_ROOT_BRANCH_SYMBOLS,
                &["branch::", "branch_dag::", "branch_types::"][..],
                "branch-domain",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_LIMIT_SYMBOLS,
                &["limits::"][..],
                "limits",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_EVENT_SYMBOLS,
                &["primitives::event::"][..],
                "event",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_ERROR_SYMBOLS,
                &[][..],
                "parent error",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_RUNTIME_HELPERS,
                &["extractable_text"][..],
                "runtime helper",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_JSON_SYMBOLS,
                &["json::", "primitives::json::"][..],
                "json",
            ),
            (
                "strata_core",
                FORBIDDEN_ROOT_VECTOR_SYMBOLS,
                &["primitives::vector::"][..],
                "vector",
            ),
            (
                "strata_core::json",
                FORBIDDEN_ROOT_JSON_SYMBOLS,
                &["primitives::json::"][..],
                "json",
            ),
            (
                "strata_core::primitives::json",
                FORBIDDEN_ROOT_JSON_SYMBOLS,
                &[][..],
                "json",
            ),
            (
                "strata_core::primitives::vector",
                FORBIDDEN_ROOT_VECTOR_SYMBOLS,
                &[][..],
                "vector",
            ),
            (
                "strata_core::primitives::event",
                FORBIDDEN_ROOT_EVENT_SYMBOLS,
                &[][..],
                "event",
            ),
        ] {
            violations.extend(scan_alias_uses(
                &contents, &file, target, symbols, paths, family,
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "engine-facing types must not be imported from strata_core outside designated modules:\n{}",
        violations.join("\n")
    );
}

#[test]
fn direct_core_primitive_imports_remain_limited_to_designated_modules() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&repo_root.join("crates"), &mut rust_files);
    collect_rust_files(&repo_root.join("tests"), &mut rust_files);

    let direct_patterns = [
        "strata_core::primitives::json::",
        "strata_core::primitives::vector::",
    ];

    let mut unexpected = Vec::new();
    for file in rust_files {
        let contents = fs::read_to_string(&file).expect("read source file");
        let has_direct_core_primitive_import = direct_patterns
            .iter()
            .any(|pattern| contents.lines().any(|line| line.contains(pattern)));

        if !has_direct_core_primitive_import {
            continue;
        }

        let rel = file.to_string_lossy();
        let allowed = rel.ends_with("/tests/engine_surface_imports.rs");

        if !allowed {
            unexpected.push(rel.to_string());
        }
    }

    assert!(
        unexpected.is_empty(),
        "unexpected direct core primitive imports remain:\n{}",
        unexpected.join("\n")
    );
}

#[test]
fn legacy_core_manifest_edges_are_fully_removed() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut manifests = Vec::new();
    manifests.push(repo_root.join("Cargo.toml"));
    collect_manifest_files(&repo_root.join("crates"), &mut manifests);

    let mut violations = Vec::new();
    for manifest in manifests {
        if should_skip_manifest(&manifest) {
            continue;
        }

        let contents = fs::read_to_string(&manifest).expect("read manifest");
        violations.extend(find_legacy_core_manifest_violations(&manifest, &contents));
    }

    assert!(
        violations.is_empty(),
        "legacy core manifest edges must be fully removed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn upper_runtime_foundation_imports_use_modern_core_root_paths() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&repo_root.join("crates"), &mut rust_files);
    collect_rust_files(&repo_root.join("tests"), &mut rust_files);

    let mut violations = Vec::new();
    for file in rust_files {
        if should_skip_foundation_cutover(&file) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read source file");
        for pattern in FORBIDDEN_FOUNDATION_COMPAT_PATTERNS {
            for (line_no, line) in contents.lines().enumerate() {
                if is_comment_line(line) {
                    continue;
                }
                if line.contains(pattern) {
                    violations.push(format!(
                        "{}:{}: contains legacy foundational import path `{pattern}`",
                        file.display(),
                        line_no + 1
                    ));
                }
            }
        }

        violations.extend(scan_use_blocks_for_symbols(
            &contents,
            &file,
            "strata_core::types::{",
            &["BranchId"],
            "legacy foundational import path",
        ));
        violations.extend(scan_use_blocks_for_symbols(
            &contents,
            &file,
            "strata_core::value::{",
            &["Value"],
            "legacy foundational import path",
        ));
        violations.extend(scan_alias_uses(
            &contents,
            &file,
            "strata_core::types",
            &["BranchId"],
            &[][..],
            "legacy foundational import path",
        ));
        violations.extend(scan_alias_uses(
            &contents,
            &file,
            "strata_core::value",
            &["Value"],
            &[][..],
            "legacy foundational import path",
        ));
    }

    assert!(
        violations.is_empty(),
        "upper-runtime code must use modern core root imports for foundational types:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_legacy_core_compat_use_is_fully_removed() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&repo_root.join("crates/engine"), &mut rust_files);

    let mut violations = Vec::new();
    for file in rust_files {
        let contents = fs::read_to_string(&file).expect("read source file");
        for (line_no, line) in contents.lines().enumerate() {
            if is_comment_line(line) {
                continue;
            }
            if line.contains(LEGACY_CORE_COMPAT_ALIAS) {
                violations.push(format!(
                    "{}:{}: engine source references deleted legacy compat alias after ST7",
                    file.display(),
                    line_no + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "engine source must not retain legacy-compat seams after ST6E:\n{}",
        violations.join("\n")
    );
}

#[test]
fn legacy_core_compat_use_is_fully_removed_outside_core_legacy_shell() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_files = Vec::new();
    collect_rust_files(&repo_root.join("crates"), &mut rust_files);
    collect_rust_files(&repo_root.join("tests"), &mut rust_files);

    let mut violations = Vec::new();
    for file in rust_files {
        if should_skip_legacy_core_compat_quarantine(&file) {
            continue;
        }

        let contents = fs::read_to_string(&file).expect("read source file");
        for (line_no, line) in contents.lines().enumerate() {
            if is_comment_line(line) {
                continue;
            }
            if line.contains(LEGACY_CORE_COMPAT_ALIAS) {
                violations.push(format!(
                    "{}:{}: unexpected legacy compat alias reference after core-legacy deletion",
                    file.display(),
                    line_no + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "legacy compat references must be fully removed after core-legacy deletion:\n{}",
        violations.join("\n")
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

fn check_line_for_root_symbols(
    violations: &mut Vec<String>,
    file: &Path,
    line_no: usize,
    line: &str,
    symbols: &[&str],
    family: &str,
) {
    if is_comment_line(line) {
        return;
    }
    for symbol in symbols {
        if line.contains(&format!("strata_core::{symbol}")) {
            violations.push(format!(
                "{}:{}: contains core-root {family} symbol `{symbol}`",
                file.display(),
                line_no
            ));
        }
    }
}

fn check_block_for_root_symbols(
    violations: &mut Vec<String>,
    file: &Path,
    start_line: usize,
    block: &str,
    symbols: &[&str],
    family: &str,
) {
    for symbol in symbols {
        if block.contains(symbol) {
            violations.push(format!(
                "{}:{}: contains core-root {family} symbol `{symbol}` in multiline strata_core use block",
                file.display(),
                start_line
            ));
        }
    }
}

fn collect_multiline_strata_core_use_blocks(contents: &str) -> Vec<(usize, String)> {
    let mut blocks = Vec::new();
    let mut current = String::new();
    let mut start_line = 0usize;
    let mut depth = 0isize;
    let mut line_count = 0usize;
    let mut collecting = false;

    for (idx, line) in contents.lines().enumerate() {
        let trimmed = line.trim_start();
        if !collecting
            && (trimmed.contains("use strata_core::{")
                || trimmed.contains("pub use strata_core::{"))
        {
            collecting = true;
            start_line = idx + 1;
            current.clear();
            depth = 0;
            line_count = 0;
        }

        if collecting {
            current.push_str(line);
            current.push('\n');
            line_count += 1;
            depth += brace_delta(line);

            if depth <= 0 && line.contains(';') {
                if line_count > 1 {
                    blocks.push((start_line, current.clone()));
                }
                collecting = false;
                current.clear();
                depth = 0;
                line_count = 0;
            }
        }
    }

    blocks
}

fn brace_delta(line: &str) -> isize {
    let opens = line.chars().filter(|c| *c == '{').count() as isize;
    let closes = line.chars().filter(|c| *c == '}').count() as isize;
    opens - closes
}

fn is_comment_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("//")
        || trimmed.starts_with("///")
        || trimmed.starts_with("//!")
        || trimmed.starts_with("/*")
        || trimmed.starts_with('*')
}

fn should_skip(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/")
        || rel.ends_with("/crates/engine/tests/surface_regression.rs")
        || rel.ends_with("/tests/engine_surface_imports.rs")
}

fn should_skip_foundation_cutover(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/")
        || rel.contains("/crates/core/")
        || rel.contains("/crates/storage/")
        || rel.ends_with("/tests/engine_surface_imports.rs")
}

fn should_skip_manifest(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/")
}

fn should_skip_legacy_core_compat_quarantine(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/") || rel.ends_with("/tests/engine_surface_imports.rs")
}

fn find_legacy_core_manifest_violations(path: &Path, contents: &str) -> Vec<String> {
    let rel = path.to_string_lossy();
    let mut violations = Vec::new();

    for (line_no, line) in contents.lines().enumerate() {
        if !(line.contains(LEGACY_CORE_PACKAGE) || line.contains(LEGACY_CORE_PACKAGE_DECL)) {
            continue;
        }
        violations.push(format!(
            "{}:{}: manifest references deleted legacy-core package",
            rel,
            line_no + 1
        ));
    }

    violations
}

fn scan_use_blocks_for_symbols(
    contents: &str,
    file: &Path,
    marker: &str,
    symbols: &[&str],
    family: &str,
) -> Vec<String> {
    let mut violations = Vec::new();
    for (start_line, block) in collect_multiline_use_blocks(contents, marker) {
        for symbol in symbols {
            if block.contains(symbol) {
                violations.push(format!(
                    "{}:{}: contains {family} symbol `{symbol}` in `{marker}` use block",
                    file.display(),
                    start_line
                ));
            }
        }
    }
    violations
}

fn scan_alias_uses(
    contents: &str,
    file: &Path,
    target: &str,
    symbols: &[&str],
    paths: &[&str],
    family: &str,
) -> Vec<String> {
    let mut violations = Vec::new();
    for alias in collect_aliases(contents, target) {
        for (line_no, line) in contents.lines().enumerate() {
            for symbol in symbols {
                if line.contains(&format!("{alias}::{symbol}")) {
                    violations.push(format!(
                        "{}:{}: contains aliased {family} symbol `{alias}::{symbol}` from `{target}`",
                        file.display(),
                        line_no + 1
                    ));
                }
            }
            for path in paths {
                if line.contains(&format!("{alias}::{path}")) {
                    violations.push(format!(
                        "{}:{}: contains aliased {family} path `{alias}::{path}` from `{target}`",
                        file.display(),
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

#[test]
fn multiline_strata_core_use_blocks_are_detected() {
    let contents = r#"
use strata_core::{
    Event,
    JsonPath,
};
"#;

    let blocks = collect_multiline_strata_core_use_blocks(contents);
    assert_eq!(blocks.len(), 1);
}

#[test]
fn aliased_engine_imports_are_detected() {
    let contents = r#"
use strata_core as sc;
use strata_core::primitives::json as json_core;
use strata_core::{
    self as root_core,
};

fn sample() {
    let _ = sc::BranchRef::default();
    let _ = json_core::JsonPath::root();
    let _ = root_core::Event {
        sequence: 0,
        event_type: String::new(),
        payload: strata_core::Value::Null,
        timestamp: strata_core::Timestamp::from(0),
        prev_hash: [0; 32],
        hash: [0; 32],
    };
}
"#;

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file = repo_root.join("tests/engine_surface_imports.rs");
    let mut violations = Vec::new();

    for (target, symbols, paths, family) in [
        (
            "strata_core",
            FORBIDDEN_ROOT_BRANCH_SYMBOLS,
            &["branch::", "branch_dag::", "branch_types::"][..],
            "branch-domain",
        ),
        (
            "strata_core",
            FORBIDDEN_ROOT_EVENT_SYMBOLS,
            &["primitives::event::"][..],
            "event",
        ),
        (
            "strata_core::primitives::json",
            FORBIDDEN_ROOT_JSON_SYMBOLS,
            &[][..],
            "json",
        ),
    ] {
        violations.extend(scan_alias_uses(
            contents, &file, target, symbols, paths, family,
        ));
    }

    assert!(
        violations.iter().any(|line| line.contains("sc::BranchRef")),
        "root alias should be detected"
    );
    assert!(
        violations
            .iter()
            .any(|line| line.contains("json_core::JsonPath")),
        "module alias should be detected"
    );
    assert!(
        violations
            .iter()
            .any(|line| line.contains("root_core::Event")),
        "self alias import blocks should be detected"
    );
}
