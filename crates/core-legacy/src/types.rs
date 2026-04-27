//! Storage layout types shared across the workspace.

pub use strata_core_foundation::BranchId;
pub use strata_storage::{validate_space_name, Key, Namespace, TypeTag};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn storage_layout_types_preserve_behavior() {
        let branch_id = BranchId::from_user_name("storage-layout");
        let namespace = Namespace::for_branch_space(branch_id, "docs");
        let key = Key::new_json(Arc::new(namespace.clone()), "doc-1");

        assert_eq!(namespace.to_string(), format!("{branch_id}/docs"));
        assert_eq!(key.type_tag, TypeTag::Json);
        assert_eq!(key.user_key_string().as_deref(), Some("doc-1"));
        assert!(validate_space_name("docs").is_ok());
        assert!(validate_space_name("").is_err());
    }
}
