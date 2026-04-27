//! Guard against reintroducing storage-boundary imports from `strata_core`.

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

#[test]
fn moved_storage_surface_is_not_imported_from_legacy_core() {
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
        "moved storage surface must not be imported from strata_core outside compatibility seams:\n{}",
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
        || rel.ends_with("/tests/co4_storage_import_guard.rs")
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
