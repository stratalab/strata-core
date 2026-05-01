//! Downstream validation for the direct `strata-core` surface.

use strata_core::{
    branch::aliases_default_branch_sentinel, BranchId, EntityRef, PrimitiveType, Value, Version,
    Versioned,
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
