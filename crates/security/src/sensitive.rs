//! Sensitive string wrapper with zeroize-on-drop and redacted Debug output.
//!
//! `SensitiveString` wraps a `String` that contains secrets (API keys,
//! passwords, tokens). It provides:
//!
//! - **Zeroize on drop**: The inner bytes are overwritten with zeros when
//!   the value is dropped, preventing secrets from lingering in freed memory.
//! - **Redacted Debug**: `Debug` output shows `[REDACTED]` instead of the
//!   actual value, preventing accidental logging.
//! - **Transparent access**: `Deref<Target=str>` gives seamless `&str` access
//!   for comparisons, formatting, and API calls.

use serde::{Deserialize, Serialize};
use std::ops::Deref;
use zeroize::Zeroize;

/// A string that holds sensitive data (API keys, passwords, tokens).
///
/// The inner bytes are zeroized on drop and `Debug` shows `[REDACTED]`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SensitiveString(String);

impl SensitiveString {
    /// Consume the wrapper and return the inner `String`.
    ///
    /// Use this when you must pass the raw value to an external API.
    /// The returned `String` is NOT zeroized — the caller assumes
    /// responsibility for the secret's lifetime.
    pub fn into_inner(self) -> String {
        let mut this = std::mem::ManuallyDrop::new(self);
        // Take the String out, replacing it with an empty String.
        // ManuallyDrop prevents our Drop (zeroize) from running.
        // The empty String left behind has no heap allocation to leak.
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
    fn from(s: String) -> Self {
        SensitiveString(s)
    }
}

impl From<&str> for SensitiveString {
    fn from(s: &str) -> Self {
        SensitiveString(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_is_redacted() {
        let s = SensitiveString::from("sk-ant-secret-key");
        assert_eq!(format!("{:?}", s), "[REDACTED]");
    }

    #[test]
    fn display_is_redacted() {
        let s = SensitiveString::from("sk-ant-secret-key");
        assert_eq!(format!("{}", s), "[REDACTED]");
    }

    #[test]
    fn deref_gives_str_access() {
        let s = SensitiveString::from("hello");
        let borrowed: &str = &s;
        assert_eq!(borrowed, "hello");
        assert_eq!(s.len(), 5);
        assert!(!s.is_empty());
    }

    #[test]
    fn as_str_works() {
        let s = SensitiveString::from("test");
        assert_eq!(s.as_str(), "test");
    }

    #[test]
    fn into_inner_returns_value() {
        let s = SensitiveString::from("secret");
        let inner = s.into_inner();
        assert_eq!(inner, "secret");
    }

    #[test]
    fn clone_is_independent() {
        let s1 = SensitiveString::from("original");
        let s2 = s1.clone();
        drop(s1);
        // s2 should still be accessible after s1 is dropped
        assert_eq!(s2.as_str(), "original");
    }

    #[test]
    fn equality() {
        let a = SensitiveString::from("same");
        let b = SensitiveString::from("same");
        let c = SensitiveString::from("different");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn from_string() {
        let s = SensitiveString::from("test".to_string());
        assert_eq!(s.as_str(), "test");
    }

    #[test]
    fn serde_roundtrip() {
        let original = SensitiveString::from("sk-ant-secret");
        let json = serde_json::to_string(&original).unwrap();
        // Serialized form should contain the actual value (transparent)
        assert_eq!(json, "\"sk-ant-secret\"");

        let deserialized: SensitiveString = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.as_str(), "sk-ant-secret");
    }

    #[test]
    fn zeroize_on_drop() {
        // Create a string and get a pointer to its buffer
        let s = SensitiveString::from("SENSITIVE_DATA_HERE");
        let ptr = s.0.as_ptr();
        let len = s.0.len();
        drop(s);
        // After drop, the memory at that location should be zeroed.
        // NOTE: This is a best-effort check — the allocator may have
        // reused the memory, but in practice this catches regressions.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        // At least some bytes should be zero (zeroize overwrites all)
        let all_nonzero = bytes.iter().all(|&b| b != 0);
        assert!(
            !all_nonzero,
            "Expected at least some zeroed bytes after drop"
        );
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SensitiveString>();
    }
}
