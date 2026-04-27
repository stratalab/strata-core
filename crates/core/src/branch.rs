//! Branch identity helpers shared across the stack.

use crate::BranchId;

/// Returns `true` when `name` resolves to the reserved nil-UUID branch
/// sentinel without being the literal `"default"` branch name.
pub fn aliases_default_branch_sentinel(name: &str) -> bool {
    name != "default" && BranchId::from_user_name(name) == BranchId::from_bytes([0u8; 16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_default_branch_sentinel_only_matches_non_literal_nil_aliases() {
        assert!(!aliases_default_branch_sentinel("default"));
        assert!(aliases_default_branch_sentinel(
            "00000000-0000-0000-0000-000000000000"
        ));
        assert!(aliases_default_branch_sentinel(
            "00000000000000000000000000000000"
        ));
        assert!(aliases_default_branch_sentinel(
            "00000000-0000-0000-0000-000000000000"
                .to_uppercase()
                .as_str()
        ));
        assert!(!aliases_default_branch_sentinel(
            "f47ac10b-58cc-4372-a567-0e02b2c3d479"
        ));
        assert!(!aliases_default_branch_sentinel("main"));
    }
}
