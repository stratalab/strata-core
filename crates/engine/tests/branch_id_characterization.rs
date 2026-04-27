//! Characterization test locking the byte-stable output of branch-name →
//! `BranchId` resolution.
//!
//! Branch ids must remain byte-stable so existing databases do not silently
//! re-namespace their data. This test freezes the 16-byte output of the
//! current engine-side path.
//!
//! If this test fails after a refactor, the refactor is NOT byte-stable.
//! Either fix the code or update the baseline with explicit reviewer approval
//! (and treat it as a breaking compatibility change).
//!
//! The companion test for the executor path lives inline in
//! `crates/executor/src/bridge.rs` (mod tests) because `to_core_branch_id`
//! is `pub(crate)`.

use std::sync::Arc;

use strata_core::types::BranchId;
use strata_core::Value;
use strata_engine::database::OpenSpec;
use strata_engine::primitives::{resolve_branch_name, BranchMetadata};
use strata_engine::{Database, SearchSubsystem};
use strata_storage::{Key, Namespace};
use uuid::Uuid;

/// The RFC 4122 namespace UUID used to derive branch IDs from names.
///
/// This array is the locked baseline — changing it is a breaking compatibility
/// change and invalidates every existing disk-backed database.
const BRANCH_NAMESPACE_BYTES: [u8; 16] = [
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
];

/// Compute the expected 16-byte output using the public `uuid` crate.
///
/// This is the documented algorithm (engine path source:
/// `crates/engine/src/primitives/branch/index.rs:50-59`):
/// 1. name == "default"              → all-zero bytes
/// 2. name parses as UUID             → verbatim parsed bytes
/// 3. otherwise                       → UUID v5 over (`BRANCH_NAMESPACE`, name)
///
/// Implementing it here as an independent oracle means the test catches
/// divergence in either direction: if the production function changes
/// algorithm, or if the `BRANCH_NAMESPACE` constant drifts.
fn expected_bytes(name: &str) -> [u8; 16] {
    if name == "default" {
        [0u8; 16]
    } else if let Ok(u) = Uuid::parse_str(name) {
        *u.as_bytes()
    } else {
        let ns = Uuid::from_bytes(BRANCH_NAMESPACE_BYTES);
        *Uuid::new_v5(&ns, name.as_bytes()).as_bytes()
    }
}

/// Locked input set. Covers: the special sentinel, common human names,
/// names with slash/underscore structure, the privileged system branch,
/// and a verbatim-UUID name.
const LOCKED_INPUTS: &[&str] = &[
    "default",
    "main",
    "production",
    "feature/abc",
    "_system_",
    "f47ac10b-58cc-4372-a567-0e02b2c3d479",
];

/// Hardcoded byte anchors for a subset of inputs. Generated once at B1
/// landing by running the test, reading the bytes printed on first-pass,
/// and pasting them here. Any later drift from these exact bytes is a
/// breaking change.
///
/// The algorithm-oracle (`expected_bytes`) catches drift from the
/// documented algorithm; these anchors catch drift in the algorithm
/// itself (e.g., someone changing both the production code and the
/// oracle in the same edit).
const HARDCODED_ANCHORS: &[(&str, [u8; 16])] = &[
    ("default", [0u8; 16]),
    (
        "main",
        [
            0x1f, 0x64, 0xc0, 0x67, 0xac, 0xf2, 0x50, 0x34, 0xa5, 0xd0, 0x92, 0xd5, 0x76, 0x41,
            0x38, 0xec,
        ],
    ),
    (
        "_system_",
        [
            0x0d, 0x39, 0x06, 0xa0, 0xd8, 0x09, 0x58, 0x17, 0xb9, 0x0b, 0x09, 0xb6, 0x0e, 0x63,
            0xc9, 0x7d,
        ],
    ),
    (
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        [
            0xf4, 0x7a, 0xc1, 0x0b, 0x58, 0xcc, 0x43, 0x72, 0xa5, 0x67, 0x0e, 0x02, 0xb2, 0xc3,
            0xd4, 0x79,
        ],
    ),
];

/// The engine's `resolve_branch_name` MUST produce the bytes the documented
/// algorithm specifies, for every locked input.
#[test]
fn engine_resolve_branch_name_matches_documented_algorithm() {
    for &name in LOCKED_INPUTS {
        let actual = *resolve_branch_name(name).as_bytes();
        let expected = expected_bytes(name);
        assert_eq!(
            actual, expected,
            "resolve_branch_name({name:?}) drifted from the documented algorithm\n\
             expected {expected:02x?}\n\
             actual   {actual:02x?}"
        );
    }
}

/// Hardcoded byte anchors pin the algorithm itself, not just its parity
/// with an independent oracle. If the `BRANCH_NAMESPACE` constant is ever
/// changed (or the UUID v5 input order flips, etc.) this test catches it.
#[test]
fn engine_resolve_branch_name_matches_hardcoded_anchors() {
    for &(name, expected) in HARDCODED_ANCHORS {
        let actual = *resolve_branch_name(name).as_bytes();
        assert_eq!(
            actual, expected,
            "resolve_branch_name({name:?}) drifted from its hardcoded anchor.\n\
             This is a BREAKING compatibility change — it renames every branch\n\
             in every existing database. Needs explicit reviewer approval.\n\
             expected {expected:02x?}\n\
             actual   {actual:02x?}"
        );
    }
}

/// The "default" branch is the nil UUID. This is a load-bearing special
/// case — storage isolation, default-branch resolution, and global-index
/// keys all depend on it. Locked explicitly.
#[test]
fn engine_default_branch_is_nil_uuid() {
    assert_eq!(*resolve_branch_name("default").as_bytes(), [0u8; 16]);
}

/// A string that already parses as a UUID is passed through verbatim,
/// not hashed via v5. This is how generated/synthetic branch IDs round-trip.
#[test]
fn engine_verbatim_uuid_passes_through() {
    let name = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    let expected = *Uuid::parse_str(name).unwrap().as_bytes();
    let actual = *resolve_branch_name(name).as_bytes();
    assert_eq!(actual, expected);
}

/// The `BRANCH_NAMESPACE` constant in the engine source MUST equal
/// `BRANCH_NAMESPACE_BYTES` declared in this test. We prove it indirectly:
/// if the engine path produces `Uuid::new_v5(BRANCH_NAMESPACE_BYTES, name)`
/// for an arbitrary non-UUID, non-"default" name, the engine's namespace
/// must match this test's namespace. If someone changes one without the
/// other, this test fails.
#[test]
fn engine_branch_namespace_matches_locked_bytes() {
    let sample = "b1-guardrail-sample-name";
    let ns = Uuid::from_bytes(BRANCH_NAMESPACE_BYTES);
    let expected = *Uuid::new_v5(&ns, sample.as_bytes()).as_bytes();
    let actual = *resolve_branch_name(sample).as_bytes();
    assert_eq!(
        actual, expected,
        "Engine's BRANCH_NAMESPACE constant has drifted from the locked\n\
         baseline at {BRANCH_NAMESPACE_BYTES:02x?}.\n\
         This is a BREAKING compatibility change."
    );
}

/// Stability under repeated calls — trivially required, locked anyway.
#[test]
fn engine_resolve_branch_name_is_deterministic() {
    let name = "feature/stability";
    let a = *resolve_branch_name(name).as_bytes();
    let b = *resolve_branch_name(name).as_bytes();
    assert_eq!(a, b);
}

/// Different names MUST produce different bytes (no v5 collisions on
/// realistic short names). Guards against an accidentally-constant
/// return value.
#[test]
fn engine_distinct_names_produce_distinct_ids() {
    let a = *resolve_branch_name("main").as_bytes();
    let b = *resolve_branch_name("production").as_bytes();
    let c = *resolve_branch_name("feature/abc").as_bytes();
    assert_ne!(a, b);
    assert_ne!(a, c);
    assert_ne!(b, c);
}

/// `Uuid::parse_str` accepts upper-case and hyphen-less variants and
/// produces the same underlying UUID. Lock that the engine path treats
/// these as identical to the canonical lower-case hyphenated form, so
/// callers passing "main-id" in any UUID flavor get one branch, not three.
#[test]
fn engine_uuid_parse_tolerates_case_and_hyphenation() {
    let canonical = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    let upper = "F47AC10B-58CC-4372-A567-0E02B2C3D479";
    let hyphenless = "f47ac10b58cc4372a5670e02b2c3d479";

    let bytes_canonical = *resolve_branch_name(canonical).as_bytes();
    let bytes_upper = *resolve_branch_name(upper).as_bytes();
    let bytes_hyphenless = *resolve_branch_name(hyphenless).as_bytes();

    assert_eq!(
        bytes_canonical, bytes_upper,
        "uppercase UUID MUST resolve to the same bytes as canonical lowercase"
    );
    assert_eq!(
        bytes_canonical, bytes_hyphenless,
        "hyphenless UUID MUST resolve to the same bytes as canonical hyphenated"
    );
}

/// The `"default"` sentinel is a load-bearing exact-match string. Locks
/// case-sensitivity: `"DEFAULT"` and `"Default"` MUST hit the v5 branch,
/// not the nil-UUID short-circuit. A future change that loosens this is
/// a behavior break and must update this test deliberately.
#[test]
fn engine_default_sentinel_is_case_sensitive() {
    let nil = [0u8; 16];
    assert_eq!(*resolve_branch_name("default").as_bytes(), nil);
    assert_ne!(
        *resolve_branch_name("DEFAULT").as_bytes(),
        nil,
        "DEFAULT (uppercase) MUST NOT collide with the nil-UUID sentinel"
    );
    assert_ne!(
        *resolve_branch_name("Default").as_bytes(),
        nil,
        "Default (mixed case) MUST NOT collide with the nil-UUID sentinel"
    );
    // And they MUST be distinct from each other (each gets its own v5 hash).
    assert_ne!(
        *resolve_branch_name("DEFAULT").as_bytes(),
        *resolve_branch_name("Default").as_bytes(),
    );
}

/// The empty string is a valid input (no validation at this layer; that
/// happens in `BranchService::validate_branch_name`). It hits the v5
/// branch and produces a stable hash. Lock the bytes so any future
/// behavior change (rejection, normalization to "default", etc.) is
/// caught.
#[test]
fn engine_empty_string_resolves_to_v5_of_empty_input() {
    let ns = Uuid::from_bytes(BRANCH_NAMESPACE_BYTES);
    let expected = *Uuid::new_v5(&ns, b"").as_bytes();
    let actual = *resolve_branch_name("").as_bytes();
    assert_eq!(actual, expected);
    // Empty MUST NOT collide with the default sentinel.
    assert_ne!(actual, [0u8; 16]);
}

/// Historical nil-UUID alias branch metadata must be quarantined on reopen
/// before branch resolution can silently map it back onto the reserved nil
/// sentinel namespace.
#[test]
fn engine_reopen_rejects_historical_default_branch_nil_uuid_alias_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open_runtime(OpenSpec::primary(temp.path()).with_subsystem(SearchSubsystem))
        .unwrap();

    let bad_name = "00000000-0000-0000-0000-000000000000";
    let nil_branch = BranchId::from_bytes([0u8; 16]);
    let namespace = Arc::new(Namespace::for_branch(nil_branch));
    let key = Key::new_branch_with_id(namespace, bad_name);
    let bad_meta = BranchMetadata::new(bad_name);
    let stored = serde_json::to_string(&bad_meta).unwrap();

    db.transaction(nil_branch, |txn| {
        txn.put(key.clone(), Value::String(stored.clone()))?;
        Ok(())
    })
    .unwrap();
    db.shutdown().unwrap();
    drop(db);

    let err = match Database::open_runtime(
        OpenSpec::primary(temp.path()).with_subsystem(SearchSubsystem),
    ) {
        Ok(_) => panic!("reopen should reject historical nil-UUID alias metadata"),
        Err(err) => err,
    };
    assert!(matches!(err, strata_core::StrataError::Corruption { .. }));
    assert!(err
        .to_string()
        .contains("branch metadata contains reserved default-branch sentinel alias"));
}
