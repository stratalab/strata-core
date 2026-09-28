//! Object layout property harness entry point.

#![deny(unsafe_code)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn object_layout_property_harness_has_crate_root() {
    assert!(common::crate_root().join("src/object/mod.rs").is_file());
    assert!(common::crate_root().join("src/layout/mod.rs").is_file());
}

#[test]
fn reserved_layout_names_stay_owned_by_layout_module() {
    let root = common::crate_root();
    let src = root.join("src");
    let allowed_files = [
        src.join("layout/mod.rs"),
        src.join("layout/tests.rs"),
        src.join("format/tests.rs"),
    ];
    for file in rust_files(&src) {
        if allowed_files.iter().any(|allowed| allowed == &file) {
            continue;
        }

        let text = fs::read_to_string(&file).expect("read source file");
        if let Some(violation) = reserved_layout_violations(&file, &text).into_iter().next() {
            panic!(
                "{}:{} contains reserved layout fragment {:?}",
                file.strip_prefix(&root).unwrap_or(&file).display(),
                violation.line_number,
                violation.fragment
            );
        }
    }
}

#[test]
fn production_source_scan_skips_test_cfg_items_only() {
    let source = r#"
fn before() {
    let _ = "manifest/current";
}

#[cfg(test)]
mod tests {
    fn inside_test_module() {
        let _ = "tables/test-only";
    }
}

fn after() {
    let _ = "wal/after-test-module";
}

#[cfg(test)]
const TEST_ONLY: &str = "snapshots/test-only";

#[cfg(test)]
fn inline_test_only() {
    let _ = "tmp/inline-test-only";
}

fn final_item() {
    let _ = "locks/final";
}
"#;

    let production = production_source_lines(source);
    let production: Vec<_> = production.into_iter().map(|(_, line)| line).collect();
    let production = production.join("\n");

    assert!(production.contains("\"manifest/current\""));
    assert!(production.contains("\"wal/after-test-module\""));
    assert!(production.contains("\"locks/final\""));
    assert!(!production.contains("\"tables/test-only\""));
    assert!(!production.contains("\"snapshots/test-only\""));
    assert!(!production.contains("\"tmp/inline-test-only\""));
}

#[test]
fn reserved_layout_scan_allows_byte_string_family_labels_only() {
    let hash_label = r#"hasher.update(b"quarantine");"#;
    assert!(
        !line_without_allowed_byte_string_family_labels(hash_label, "\"quarantine\"")
            .contains("\"quarantine\"")
    );

    let mixed_line = r#"hasher.update(b"quarantine"); let _ = "quarantine";"#;
    assert!(
        line_without_allowed_byte_string_family_labels(mixed_line, "\"quarantine\"")
            .contains("\"quarantine\"")
    );

    let object_path = r#"hasher.update(b"quarantine"); let _ = "quarantine/branch";"#;
    assert!(
        line_without_allowed_byte_string_family_labels(object_path, "\"quarantine/")
            .contains("\"quarantine/")
    );
}

#[test]
fn reserved_layout_scan_allows_the_quarantine_kind_label_arm_only() {
    let label_arm = r#"            Self::Quarantine => "quarantine","#;
    assert!(
        reserved_layout_violations(&source_path("lifecycle/facts.rs"), label_arm).is_empty(),
        "the exact task-kind label arm is an allowed diagnostic label"
    );

    let smuggled_alongside = r#"let _ = "quarantine"; Self::Quarantine => "quarantine","#;
    assert_violation(
        &reserved_layout_violations(&source_path("lifecycle/facts.rs"), smuggled_alongside),
        "\"quarantine\"",
    );

    let smuggled_path = r#"            Self::Quarantine => "quarantine/branch","#;
    assert_violation(
        &reserved_layout_violations(&source_path("lifecycle/facts.rs"), smuggled_path),
        "\"quarantine/",
    );
}

#[test]
fn reserved_layout_scan_rejects_seeded_production_parsers() {
    let source = r#"
fn raw_object_name() {
    let _ = ObjectName::new("tables/branch/l0000/table");
}

fn raw_format(segment_id: u64) {
    let _ = format!("wal/{segment_id:016x}");
}

fn raw_prefix(object: &ObjectName) {
    let _ = object.as_str().starts_with("tables/");
}

fn raw_suffix(object: &ObjectName) {
    let _ = object.as_str().ends_with("/manifest");
}

fn raw_split(object: &ObjectName) {
    let _ = object.as_str().split('/').next() == Some("tables");
}

fn raw_rsplit(object: &ObjectName) {
    let _ = object.as_str().rsplit('/').next() == Some("manifest");
}
"#;

    let violations = reserved_layout_violations(&source_path("service/raw_layout.rs"), source);
    assert_violation(&violations, "\"tables/");
    assert_violation(&violations, "\"wal/");
    assert_violation(&violations, "\"/manifest\"");
    assert_violation(&violations, ".split('/')");
    assert_violation(&violations, ".rsplit('/')");
}

#[test]
fn reserved_layout_scan_allows_test_and_low_level_fixtures() {
    let test_source = r#"
#[cfg(test)]
mod tests {
    fn raw_fixture() {
        let _ = ObjectName::new("tables/branch/l0000/table");
        let _ = format!("wal/{segment_id:016x}");
        let _ = object.as_str().starts_with("tables/");
        let _ = object.as_str().ends_with("/manifest");
        let _ = object.as_str().split('/').next() == Some("tables");
        let _ = object.as_str().rsplit('/').next() == Some("manifest");
    }
}
"#;
    assert!(
        reserved_layout_violations(&source_path("service/raw_layout.rs"), test_source).is_empty()
    );

    let object_validation = r"
fn validate(value: &str) {
    let mut components = value.split('/').peekable();
}
";
    assert!(
        reserved_layout_violations(&source_path("object/mod.rs"), object_validation).is_empty()
    );

    let backend_mapping = r"
fn to_path(name: &ObjectName) {
    let mut components = name.as_str().split('/').peekable();
}
";
    assert!(
        reserved_layout_violations(&source_path("backend/local_fs.rs"), backend_mapping).is_empty()
    );
}

fn production_source_lines(source: &str) -> Vec<(usize, String)> {
    let mut lines = Vec::new();
    let mut pending_test_item = false;
    let mut skipping_header = false;
    let mut skipping_body_depth = None;

    for (index, line) in source.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim_start();

        if let Some(depth) = skipping_body_depth {
            let next_depth = depth + brace_delta(line);
            skipping_body_depth = (next_depth > 0).then_some(next_depth);
            continue;
        }

        if skipping_header {
            if trimmed.ends_with(';') {
                skipping_header = false;
                continue;
            }

            let depth = brace_delta(line);
            if depth > 0 {
                skipping_header = false;
                skipping_body_depth = Some(depth);
            } else if line.contains('{') {
                skipping_header = false;
            }
            continue;
        }

        if pending_test_item {
            if trimmed.is_empty() || trimmed.starts_with("#[") {
                continue;
            }

            pending_test_item = false;
            if trimmed.ends_with(';') {
                continue;
            }

            let depth = brace_delta(line);
            if depth > 0 {
                skipping_body_depth = Some(depth);
            } else if line.contains('{') {
                continue;
            } else {
                skipping_header = true;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_test_item = true;
            continue;
        }

        lines.push((line_number, line.to_owned()));
    }

    lines
}

#[derive(Debug)]
struct ReservedLayoutViolation {
    line_number: usize,
    fragment: &'static str,
}

/// The maintenance task-kind diagnostic label for the quarantine task spells
/// the same as the reserved layout directory. Only this exact match-arm form
/// is allowed — it is a kind label, not an object path.
const ALLOWED_QUARANTINE_KIND_LABEL_ARM: &str = "Self::Quarantine => \"quarantine\",";

fn reserved_layout_violations(path: &Path, source: &str) -> Vec<ReservedLayoutViolation> {
    let mut violations = Vec::new();
    for (line_number, line) in production_source_lines(source) {
        let line = line.replacen(ALLOWED_QUARANTINE_KIND_LABEL_ARM, "", 1);
        for fragment in reserved_layout_fragments() {
            let searchable_line = line_without_allowed_byte_string_family_labels(&line, fragment);
            if searchable_line.contains(fragment) {
                violations.push(ReservedLayoutViolation {
                    line_number,
                    fragment,
                });
            }
        }

        if !is_low_level_object_parser_file(path) {
            for fragment in role_parser_fragments() {
                if line.contains(fragment) {
                    violations.push(ReservedLayoutViolation {
                        line_number,
                        fragment,
                    });
                }
            }
        }
    }
    violations
}

fn assert_violation(violations: &[ReservedLayoutViolation], fragment: &'static str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.fragment == fragment),
        "missing violation for {fragment:?}; got {violations:?}"
    );
}

fn reserved_layout_fragments() -> &'static [&'static str] {
    &[
        "\"manifest\"",
        "\"manifest/",
        "\"wal\"",
        "\"wal/",
        "\"tables\"",
        "\"tables/",
        "\"snapshots\"",
        "\"snapshots/",
        // #3643: only the path form — bare "timeline" is also the commit
        // timeline KV space name (`COMMIT_TIMELINE_SPACE`).
        "\"timeline/",
        "\"tmp\"",
        "\"tmp/",
        "\"quarantine\"",
        "\"quarantine/",
        "\"locks\"",
        "\"locks/",
        "\"meta\"",
        "\"meta/",
        "\"/manifest\"",
        "\"MANIFEST\"",
        "\"wal-",
        "\"snap-",
        "\"segments.manifest\"",
        "\"quarantine.manifest\"",
        "\"__quarantine__\"",
        "\"follower_state",
        "\"follower_audit",
    ]
}

fn role_parser_fragments() -> &'static [&'static str] {
    &[".split('/')", ".rsplit('/')"]
}

fn source_path(relative: &str) -> PathBuf {
    common::crate_root().join("src").join(relative)
}

fn is_low_level_object_parser_file(path: &Path) -> bool {
    let root = common::crate_root();
    path == root.join("src/layout/mod.rs")
        || path == root.join("src/object/mod.rs")
        || path == root.join("src/backend/local_fs.rs")
}

fn brace_delta(line: &str) -> i32 {
    line.chars().fold(0, |depth, ch| match ch {
        '{' => depth + 1,
        '}' => depth - 1,
        _ => depth,
    })
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).expect("read source directory") {
            let entry = entry.expect("read source entry");
            let path = entry.path();
            if path.is_dir() {
                // `src/<layer>/tests/*.rs` are test files by convention —
                // the parent `mod.rs` gates them with `#[cfg(test)] mod
                // tests;`, so they hold test code rather than production
                // code. Likewise `src/testkit/*.rs` is gated by
                // `#[cfg(any(test, feature = "testkit"))]` in `lib.rs`
                // and exists only for test harnesses. The line-scanner
                // can't see the parent-mod attribute (it walks one
                // file at a time), so we skip these directories here
                // to keep test fixtures from tripping the
                // reserved-name guard.
                let dir_name = path.file_name().and_then(|name| name.to_str());
                if !matches!(dir_name, Some("tests" | "testkit")) {
                    pending.push(path);
                }
            } else if !is_test_source_file(&path)
                && path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

fn is_test_source_file(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    file_name == "tests.rs" || file_name.ends_with("_tests.rs")
}

fn line_without_allowed_byte_string_family_labels(line: &str, fragment: &str) -> String {
    let Some(family) = fragment
        .strip_prefix('"')
        .and_then(|fragment| fragment.strip_suffix('"'))
    else {
        return line.to_owned();
    };
    line.replace(&format!("b\"{family}\""), "b\"\"")
}
