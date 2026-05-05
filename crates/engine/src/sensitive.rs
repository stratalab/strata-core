//! Sensitive string wrapper with redacted formatting and zeroize-on-drop.

use serde::{Deserialize, Serialize};
use std::ops::Deref;
use zeroize::Zeroize;

/// A string that holds sensitive data such as API keys, passwords, or tokens.
///
/// The inner bytes are zeroized on drop, while `Debug` and `Display` output are
/// redacted to avoid accidental log leakage.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SensitiveString(String);

impl SensitiveString {
    /// Consume the wrapper and return the inner `String`.
    ///
    /// The returned `String` is not zeroized by this wrapper; the caller assumes
    /// responsibility for its lifetime.
    pub fn into_inner(self) -> String {
        let mut this = std::mem::ManuallyDrop::new(self);
        std::mem::take(&mut this.0)
    }

    /// Borrow the inner value as `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Drop for SensitiveString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl std::fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl Deref for SensitiveString {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl PartialEq for SensitiveString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SensitiveString {}

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SensitiveString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
