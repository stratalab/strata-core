//! Boundary tests for the feature-gated storage testkit.

#![deny(unsafe_code)]

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct ProbeCase<'a> {
    name: &'a str,
    default_features: bool,
    features: &'a [&'a str],
    source: &'a str,
    expected: ProbeExpectation<'a>,
}

enum ProbeExpectation<'a> {
    Success,
    FailureContaining(&'a [&'a str]),
}

#[test]
fn testkit_visibility_matches_feature_selection() {
    let target_dir = tempfile::tempdir().expect("probe target dir");

    for case in probe_cases() {
        let output = run_probe(&case, target_dir.path());
        match case.expected {
            ProbeExpectation::Success => {
                assert!(
                    output.status.success(),
                    "probe {} should compile\nstdout:\n{}\nstderr:\n{}",
                    case.name,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            ProbeExpectation::FailureContaining(expected) => {
                assert!(
                    !output.status.success(),
                    "probe {} should fail to compile",
                    case.name
                );
                assert_failure_contains(case.name, &output, expected);
            }
        }
    }
}

#[test]
fn testkit_source_boundary_stays_feature_gated_and_hidden() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib = fs::read_to_string(root.join("src/lib.rs")).expect("read lib.rs");
    let testkit = fs::read_to_string(root.join("src/testkit/mod.rs")).expect("read testkit");

    assert!(
        lib.contains("#[cfg(any(test, feature = \"testkit\"))]\n#[doc(hidden)]\npub mod testkit;"),
        "crate root should gate and hide the testkit module"
    );
    assert!(
        testkit.contains("#![doc(hidden)]"),
        "testkit module should hide its feature-gated public surface from normal docs"
    );
    assert!(
        testkit.contains("#[cfg(any(test, feature = \"fault-injection\"))]\npub use fault::"),
        "fault-injection exports should remain behind the fault-injection feature"
    );
}

#[test]
fn localfs_feature_is_rejected_for_wasm_builds() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib = fs::read_to_string(root.join("src/lib.rs")).expect("read lib.rs");

    assert!(
        lib.contains(
            "#[cfg(all(target_arch = \"wasm32\", feature = \"localfs\"))]\n\
             compile_error!(\"the localfs feature is not supported on wasm32; \
             use default-features = false\");"
        ),
        "crate root should reject default localfs builds for wasm before any storage code is used"
    );
}

/// The nightly ASAN + LSAN lane runs `cargo test --tests` for
/// `strata-storage` and `strata-engine` with leak detection on. An
/// intentional fixture leak must go through `testkit::leak_static` /
/// `testkit::forget_registered`, which keep it reachable from a process
/// global; a bare leak is reported as a real one and turns the lane red
/// (#3495). This per-PR guard catches that before nightly does.
///
/// Scope = every `*.rs` file under the source and test roots of both
/// crates the lane runs, walked from disk. The only exemption is the
/// leak registry itself.
#[test]
fn fixture_leaks_route_through_the_leak_registry() {
    let storage = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let engine = storage.parent().expect("crates directory").join("engine");
    let registry = storage.join("src/testkit/leak.rs");
    assert!(
        fs::read_to_string(&registry)
            .expect("read leak registry")
            .contains("pub fn leak_static"),
        "the leak registry must stay at {}",
        registry.display()
    );

    // Assembled so this file does not match its own needles.
    let needles = [concat!("Box::", "leak("), concat!("mem::", "forget(")];
    let mut files = Vec::new();
    for root in [
        storage.join("src"),
        storage.join("tests"),
        engine.join("src"),
        engine.join("tests"),
    ] {
        assert!(
            root.is_dir(),
            "lane source root {} must exist",
            root.display()
        );
        collect_rs_files(&root, &mut files);
    }
    assert!(
        files.len() > 100,
        "the walk must see the whole tree (saw {} files)",
        files.len()
    );

    let mut offenders = Vec::new();
    for file in files.iter().filter(|file| **file != registry) {
        let source = fs::read_to_string(file).expect("read source file");
        for (index, line) in source.lines().enumerate() {
            if needles.iter().any(|needle| line.contains(needle)) {
                offenders.push(format!("{}:{}: {}", file.display(), index + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "bare fixture leaks fail the nightly LSAN lane; use \
         strata_storage::testkit::leak_static / forget_registered \
         (engine has no registry and must not leak):\n{}",
        offenders.join("\n")
    );
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn probe_cases<'a>() -> Vec<ProbeCase<'a>> {
    vec![
        ProbeCase {
            name: "default-features-without-testkit",
            default_features: true,
            features: &[],
            source: r#"
                use strata_storage::testkit::TestBackendKind;

                fn main() {
                    let _ = TestBackendKind::parse("memory");
                }
            "#,
            expected: ProbeExpectation::FailureContaining(&["testkit", "TestBackendKind"]),
        },
        ProbeCase {
            name: "memory-only-without-testkit",
            default_features: false,
            features: &[],
            source: r#"
                use strata_storage::testkit::TestBackendKind;

                fn main() {
                    let _ = TestBackendKind::parse("memory");
                }
            "#,
            expected: ProbeExpectation::FailureContaining(&["testkit", "TestBackendKind"]),
        },
        ProbeCase {
            name: "with-testkit",
            default_features: false,
            features: &["testkit"],
            source: r#"
                use strata_storage::testkit::{
                    FormatDecodeOutcome, FormatDecoder, TestBackendKind, decode_format_bytes,
                };

                fn main() -> Result<(), Box<dyn std::error::Error>> {
                    let backend = TestBackendKind::parse("memory")?;
                    assert_eq!(backend.name(), "memory");
                    assert_eq!(
                        decode_format_bytes(FormatDecoder::Manifest, &[]),
                        FormatDecodeOutcome::Rejected
                    );
                    Ok(())
                }
            "#,
            expected: ProbeExpectation::Success,
        },
        ProbeCase {
            name: "without-fault-injection",
            default_features: false,
            features: &["testkit"],
            source: r"
                use strata_storage::testkit::FaultScript;

                fn main() {
                    let _ = FaultScript::empty();
                }
            ",
            expected: ProbeExpectation::FailureContaining(&["testkit", "FaultScript"]),
        },
        ProbeCase {
            name: "with-fault-injection",
            default_features: false,
            features: &["fault-injection"],
            source: r#"
                use strata_storage::testkit::{
                    BackendOperation, FaultKind, FaultRule, FaultScript, FaultingBackend,
                };
                use std::num::NonZeroU64;

                fn main() -> Result<(), Box<dyn std::error::Error>> {
                    let one = NonZeroU64::new(1).ok_or("non-zero call number")?;
                    let script = FaultScript::new([FaultRule::new(
                        BackendOperation::WriteObject,
                        one,
                        FaultKind::Interrupted,
                    )]);
                    let backend = FaultingBackend::new((), script);
                    assert_eq!(
                        backend.before_operation(BackendOperation::WriteObject),
                        Err(FaultKind::Interrupted)
                    );
                    assert_eq!(
                        backend.before_operation(BackendOperation::WriteObject),
                        Ok(())
                    );
                    assert_eq!(backend.calls().len(), 2);
                    Ok(())
                }
            "#,
            expected: ProbeExpectation::Success,
        },
    ]
}

fn run_probe(case: &ProbeCase<'_>, shared_target_dir: &Path) -> Output {
    let temp = tempfile::tempdir().expect("probe package dir");
    write_probe_manifest(temp.path(), case.default_features, case.features);
    write_probe_source(temp.path(), case.source);

    run_cargo_check(temp.path(), shared_target_dir)
}

fn run_cargo_check(package_dir: &Path, shared_target_dir: &Path) -> Output {
    let mut command = Command::new(cargo());
    command
        .args(["check", "--quiet", "--offline"])
        .arg("--manifest-path")
        .arg(package_dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", shared_target_dir)
        .env("CARGO_TERM_COLOR", "never")
        .env("CARGO_INCREMENTAL", "0")
        // The probe proves feature-gate diagnostics; ambient build
        // instrumentation (e.g. the nightly sanitizer lane's RUSTFLAGS)
        // must not leak into it or the probe fails for unrelated reasons.
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_TARGET");

    command.output().expect("run cargo check probe")
}

fn write_probe_manifest(path: &Path, default_features: bool, features: &[&str]) {
    let feature_list = features
        .iter()
        .map(|feature| format!("\"{feature}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let storage_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let storage_path = storage_root.to_string_lossy().replace('\\', "\\\\");
    let manifest = format!(
        r#"[package]
name = "storage_next_boundary_probe"
version = "0.0.0"
edition = "2021"
publish = false

[workspace]

[dependencies]
strata-storage = {{ path = "{storage_path}", default-features = {default_features}, features = [{feature_list}] }}
"#
    );

    fs::write(path.join("Cargo.toml"), manifest).expect("write probe manifest");
}

fn write_probe_source(path: &Path, source: &str) {
    let source_dir = path.join("src");
    fs::create_dir(&source_dir).expect("create probe source dir");
    fs::write(source_dir.join("main.rs"), source).expect("write probe source");
}

fn cargo() -> OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"))
}

fn assert_failure_contains(probe_name: &str, output: &Output, expected_terms: &[&str]) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    for expected in expected_terms {
        assert!(
            stderr.contains(expected),
            "probe {probe_name} stderr should contain {expected:?}\nstderr:\n{stderr}"
        );
    }
}
