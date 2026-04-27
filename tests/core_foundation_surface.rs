//! Downstream validation for the direct `strata-core` surface.

use strata_core as legacy_core;
use strata_core_foundation::{
    branch::aliases_default_branch_sentinel,
    contract::{EntityRef, PrimitiveType, Version, Versioned},
    id::BranchId,
    value::Value,
};

#[test]
fn foundation_surface_is_directly_usable_from_downstream_package() {
    let branch_id = BranchId::from_user_name("feature/downstream");
    let entity = EntityRef::kv(branch_id, "default", "doc-1");
    let versioned = Versioned::new(Value::from("hello"), Version::txn(7));

    assert_eq!(entity.branch_id(), branch_id);
    assert_eq!(entity.primitive_type(), PrimitiveType::Kv);
    assert_eq!(entity.space(), Some("default"));
    assert_eq!(versioned.value().as_str(), Some("hello"));
    assert_eq!(versioned.version(), Version::txn(7));
    assert!(!aliases_default_branch_sentinel("default"));
    assert!(aliases_default_branch_sentinel(
        "00000000-0000-0000-0000-000000000000"
    ));
}

#[test]
fn branch_id_derivation_matches_legacy_shell_on_locked_cases() {
    for name in [
        "",
        "default",
        "feature/downstream",
        "00000000-0000-0000-0000-000000000000",
    ] {
        assert_eq!(
            BranchId::from_user_name(name),
            legacy_core::BranchId::from_user_name(name),
            "branch derivation drifted for {name:?}"
        );
    }
}

#[test]
fn foundational_values_cross_legacy_shell_boundary_without_conversion() {
    let branch_id = BranchId::from_user_name("feature/cross-shell");
    let entity: legacy_core::EntityRef = EntityRef::json(branch_id, "profiles", "user-1");
    let versioned: legacy_core::Versioned<Value> = Versioned::new(
        Value::array(vec![Value::Int(1), Value::String("a".into())]),
        Version::txn(11),
    );

    assert_eq!(entity.branch_id(), branch_id);
    assert_eq!(entity.primitive_type(), PrimitiveType::Json);
    assert_eq!(entity.space(), Some("profiles"));
    assert_eq!(entity.json_doc_id(), Some("user-1"));

    let versioned_value = versioned.value().as_array().unwrap();
    assert_eq!(versioned.version(), legacy_core::Version::txn(11));
    assert_eq!(versioned_value, &[Value::Int(1), Value::String("a".into())]);
}
