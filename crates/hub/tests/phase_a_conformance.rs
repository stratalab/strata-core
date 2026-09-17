//! Phase A byte-compatibility CI (coordination doc §4 items 1-2).
//!
//! Proves that the canonicalizer and hasher THIS crate ships produce
//! byte-identical output to stratahub's pinned implementations. The
//! anchors mirror stratahub's own `hash_anchors.rs` /
//! `manifest_hash_anchor.rs` values — drift on either side breaks the
//! same constants in both repos.

#![deny(unsafe_code)]

use stratahub_protocol::{hash_bytes, Hash};
use stratahub_testkit::fixtures::titanic_manifest;

/// BLAKE3 known vectors, identical to stratahub's `hash_anchors.rs`.
const BLAKE3_VECTORS: &[(&[u8], &str)] = &[
    (
        b"",
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    ),
    (
        b"abc",
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85",
    ),
    (
        b"hello world",
        "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24",
    ),
];

/// The manifest hash pinned by stratahub's `manifest_hash_anchor.rs`.
const TITANIC_MANIFEST_HASH: &str =
    "blake3:8ac589d2e4965c755758414a8f952e1b0f2a01cefea8d6841d1ce8246fc41175";

#[test]
fn our_blake3_matches_stratahub_anchors() {
    for (input, expected_hex) in BLAKE3_VECTORS {
        // Our direct dependency...
        assert_eq!(&blake3::hash(input).to_hex().to_string(), expected_hex);
        // ...and stratahub's hash API agree byte-for-byte.
        let expected = Hash::parse(&format!("blake3:{expected_hex}")).expect("anchor parses");
        assert_eq!(hash_bytes(input), expected);
    }
}

#[test]
fn our_serde_jcs_matches_rfc8785_forms() {
    // Key ordering, number canonicalization, and escape minimalism per
    // RFC 8785 — the properties manifest hashing depends on.
    let value = serde_json::json!({
        "b": 1e30,
        "a": 4.50,
        "c": 2e-3,
        "nested": {"z": true, "y": null},
        "unicode": "€$\u{000F}A'B\"\\"
    });
    let canonical = serde_jcs::to_vec(&value).expect("canonicalizes");
    assert_eq!(
        String::from_utf8(canonical).expect("utf8"),
        "{\"a\":4.5,\"b\":1e+30,\"c\":0.002,\"nested\":{\"y\":null,\"z\":true},\"unicode\":\"€$\\u000fA'B\\\"\\\\\"}"
    );
}

#[test]
fn titanic_manifest_reproduces_stratahub_anchor_through_our_pins() {
    let manifest = titanic_manifest();

    // The protocol helper and our own serde_jcs pin agree byte-for-byte.
    let canonical = manifest.canonical_bytes().expect("canonical bytes");
    let ours = serde_jcs::to_vec(&manifest).expect("our canonicalizer");
    assert_eq!(canonical, ours, "serde_jcs pin drift between repos");

    // Our blake3 over those bytes reproduces the cross-repo anchor.
    let hash = format!("blake3:{}", blake3::hash(&canonical).to_hex());
    assert_eq!(hash, TITANIC_MANIFEST_HASH, "manifest hash anchor drift");
    assert_eq!(manifest.hash().expect("protocol hash").as_str(), hash);
}

#[test]
fn engine_info_is_stable_and_semver_shaped() {
    let info = strata_hub::engine_info();
    assert_eq!(
        info,
        strata_hub::engine_info(),
        "repeatable, side-effect free"
    );
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        info.capability_registry_version,
        strata_hub::CAPABILITY_REGISTRY_VERSION
    );
    // Semver `major.minor.patch` shape without pulling in the semver crate.
    let parts: Vec<&str> = info.version.split('.').collect();
    assert_eq!(parts.len(), 3);
    for part in parts {
        part.parse::<u64>().expect("numeric semver component");
    }
    assert_eq!(info.supported_primitives.len(), 5);
}

/// #3204: a dataset card naming the `graph` primitive must deserialize.
///
/// The CLI was pinned to a stratahub whose `PrimitiveType` predated `graph`,
/// and the enum deserializes strictly — so a graph-bearing dataset card did
/// not degrade to a partial listing, it failed the whole clone with
/// `unavailable.executor.hub_transport`.
#[test]
fn a_dataset_card_naming_graph_deserializes() {
    use stratahub_protocol::wire::PrimitiveType;

    let parsed: Vec<PrimitiveType> =
        serde_json::from_str(r#"["branches","graph","json","kv"]"#).expect("graph is a primitive");
    assert!(parsed.contains(&PrimitiveType::Graph));

    // And every spelling that already worked still does, at its own name —
    // the bump appends, it does not renumber.
    for (wire, expected) in [
        ("kv", PrimitiveType::Kv),
        ("json", PrimitiveType::Json),
        ("vectors", PrimitiveType::Vectors),
        ("events", PrimitiveType::Events),
        ("branches", PrimitiveType::Branches),
        ("graph", PrimitiveType::Graph),
    ] {
        let one: PrimitiveType =
            serde_json::from_str(&format!("\"{wire}\"")).expect("known primitive");
        assert_eq!(one, expected, "{wire}");
        assert_eq!(
            serde_json::to_string(&expected).expect("serializes"),
            format!("\"{wire}\""),
            "{wire} round-trips"
        );
    }
}
