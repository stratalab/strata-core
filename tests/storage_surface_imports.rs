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

fn should_skip(path: &Path) -> bool {
    let rel = path.to_string_lossy();
    rel.contains("/target/")
        || rel.contains("/crates/core-legacy/")
        || rel.ends_with("/tests/storage_surface_imports.rs")
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
