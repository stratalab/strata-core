//! Legacy compatibility forwarders for the storage layout surface.
//!
//! `Namespace`, `TypeTag`, `Key`, and `validate_space_name` are owned by
//! `strata-storage`. This module preserves the older `strata_core::types::*`
//! import path while pointing at the storage-owned definitions.

pub use strata_core_foundation::BranchId;
pub use strata_storage::{validate_space_name, Key, Namespace, TypeTag};

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::*;

    #[test]
    fn storage_layout_types_are_forwarded_from_storage() {
        assert_eq!(TypeId::of::<Key>(), TypeId::of::<strata_storage::Key>());
        assert_eq!(
            TypeId::of::<Namespace>(),
            TypeId::of::<strata_storage::Namespace>()
        );
        assert_eq!(
            TypeId::of::<TypeTag>(),
            TypeId::of::<strata_storage::TypeTag>()
        );
    }
}
