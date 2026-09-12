//! KV input types.

use serde::{Deserialize, Deserializer, Serialize};

use crate::branch::SYSTEM_BRANCH;
use crate::diagnostics::EngineError;

const MAX_PRODUCT_SPACE_BYTES: usize = u16::MAX as usize;

/// Product KV space name.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct ProductSpace(String);

impl ProductSpace {
    /// Creates a product space after rejecting internal names.
    pub fn new(space: impl Into<String>) -> Result<Self, EngineError> {
        let space = space.into();
        if space.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.product_space",
                "product space must not be empty",
            ));
        }
        if space == SYSTEM_BRANCH || space.starts_with('_') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.product_space_reserved",
                "product space is reserved for engine control data",
            ));
        }
        if space.len() > MAX_PRODUCT_SPACE_BYTES {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.product_space",
                "product space is too long",
            ));
        }
        if space.bytes().any(|byte| byte == 0 || byte == b'\n') {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.product_space",
                "product space contains an unsupported control byte",
            ));
        }
        Ok(Self(space))
    }

    #[must_use]
    /// Returns the space as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ProductSpace {
    type Error = EngineError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for ProductSpace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Byte-oriented KV key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvKey(Vec<u8>);

impl KvKey {
    /// Creates a KV key.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, EngineError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(EngineError::invalid_input(
                "invalid_argument.engine.kv_key",
                "KV key must not be empty",
            ));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl TryFrom<&[u8]> for KvKey {
    type Error = EngineError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<const N: usize> TryFrom<&[u8; N]> for KvKey {
    type Error = EngineError;

    fn try_from(value: &[u8; N]) -> Result<Self, Self::Error> {
        Self::new(value.as_slice())
    }
}

/// Byte-oriented KV value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvValue(Vec<u8>);

impl KvValue {
    #[must_use]
    /// Creates a KV value.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    #[must_use]
    /// Returns the raw value bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    /// Consumes the value and returns raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::ProductSpace;
    use crate::diagnostics::EngineErrorClass;

    #[test]
    fn product_space_rejects_values_that_cannot_be_length_encoded() {
        let error = ProductSpace::new("s".repeat(usize::from(u16::MAX) + 1))
            .expect_err("oversized product space must fail");
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.product_space");
    }
}
