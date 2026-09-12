//! Product branch-name validation.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::diagnostics::EngineError;

pub(crate) const DEFAULT_BRANCH: &str = "default";
pub(crate) const SYSTEM_BRANCH: &str = "_system_";
const MAX_BRANCH_NAME_BYTES: usize = 255;

/// Validated product branch name.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct BranchName(String);

impl BranchName {
    /// Creates a branch name after rejecting reserved internal spellings.
    pub fn new(name: impl Into<String>) -> Result<Self, EngineError> {
        let name = name.into();
        validate_branch_name(&name)?;
        Ok(Self(name))
    }

    pub(crate) fn default_branch() -> Self {
        Self(DEFAULT_BRANCH.to_owned())
    }

    /// Returns the branch name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for BranchName {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for BranchName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn validate_branch_name(name: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name",
            "branch name must not be empty",
        ));
    }
    if name.trim().is_empty() {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name",
            "branch name must not be whitespace-only",
        ));
    }
    if name == SYSTEM_BRANCH || name.starts_with('_') {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name_reserved",
            "branch name is reserved for engine control data",
        ));
    }
    if name.len() > MAX_BRANCH_NAME_BYTES {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name",
            "branch name is too long",
        ));
    }
    if name.chars().any(char::is_control) {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name",
            "branch name contains an unsupported control byte",
        ));
    }
    if crate::branch::catalog::aliases_reserved_branch_identity(name) {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.branch_name",
            "branch name aliases a reserved branch identity",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::BranchName;
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn branch_name_rejects_reserved_internal_name() {
        let error = BranchName::new("_system_").expect_err("reserved branch must fail");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.branch_name_reserved");
    }

    #[test]
    fn branch_name_rejects_values_that_cannot_be_length_encoded() {
        let error = BranchName::new("a".repeat(256)).expect_err("oversized branch name must fail");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.branch_name");
    }

    #[test]
    fn branch_name_rejects_whitespace_only_names() {
        let error = BranchName::new(" \t ").expect_err("whitespace branch must fail");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.branch_name");
    }

    #[test]
    fn branch_name_rejects_nil_uuid_alias_for_default() {
        let error = BranchName::new("00000000-0000-0000-0000-000000000000")
            .expect_err("default alias must fail");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.branch_name");
    }

    #[test]
    fn branch_name_rejects_uuid_aliases_for_internal_storage_branches() {
        for rejected in [
            "01010101-0101-0101-0101-010101010101",
            "f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0",
        ] {
            let error = BranchName::new(rejected).expect_err("reserved alias must fail");
            assert_eq!(error.class(), EngineErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.engine.branch_name");
        }
    }
}
