//! Branch name type
//!
//! Branches have dual identity:
//! - **BranchId**: Internal, immutable UUID assigned by the system
//! - **BranchName**: User-facing, semantic identifier set by the user
//!
//! BranchName is the "what" (semantic purpose), BranchId is the "which" (unique instance).
//!
//! ## Examples
//!
//! - BranchName: "training-gpt-v3-2024"
//! - BranchId: 550e8400-e29b-41d4-a716-446655440000
//!
//! ## Validation
//!
//! Branch names must:
//! - Be 1-256 characters
//! - Contain only alphanumeric, dash, underscore, dot
//! - Not start with a dash or dot

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum length of a branch name
pub const MAX_BRANCH_NAME_LENGTH: usize = 256;

/// User-facing semantic identifier for a branch
///
/// BranchName is the human-readable, meaningful name for a branch.
/// Unlike BranchId (which is a UUID), BranchName captures the semantic
/// purpose of the branch.
///
/// ## Validation Rules
///
/// - Length: 1-256 characters
/// - Characters: `[a-zA-Z0-9_.-]`
/// - Cannot start with `-` or `.`
///
/// ## Examples
///
/// Valid names:
/// - "training-run-1"
/// - "experiment.v2"
/// - "prod_agent_2024"
///
/// Invalid names:
/// - "" (empty)
/// - "-starts-with-dash"
/// - ".hidden"
/// - "has spaces"
/// - "has@special"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchName(String);

/// Error when validating a branch name
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchNameError {
    /// Name is empty
    Empty,
    /// Name exceeds maximum length
    TooLong {
        /// Actual length of the name
        length: usize,
        /// Maximum allowed length
        max: usize,
    },
    /// Name contains invalid character
    InvalidChar {
        /// The invalid character
        char: char,
        /// Position of the invalid character
        position: usize,
    },
    /// Name starts with invalid character
    InvalidStart {
        /// The invalid starting character
        char: char,
    },
}

impl std::fmt::Display for BranchNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BranchNameError::Empty => write!(f, "branch name cannot be empty"),
            BranchNameError::TooLong { length, max } => {
                write!(f, "branch name too long: {} chars (max {})", length, max)
            }
            BranchNameError::InvalidChar { char, position } => {
                write!(
                    f,
                    "invalid character '{}' at position {} (only alphanumeric, dash, underscore, dot allowed)",
                    char, position
                )
            }
            BranchNameError::InvalidStart { char } => {
                write!(
                    f,
                    "branch name cannot start with '{}' (must start with alphanumeric or underscore)",
                    char
                )
            }
        }
    }
}

impl std::error::Error for BranchNameError {}

impl BranchName {
    /// Create a new BranchName, validating the input
    ///
    /// # Errors
    ///
    /// Returns `BranchNameError` if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self, BranchNameError> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(BranchName(name))
    }

    /// Create a BranchName without validation
    ///
    /// # Safety
    ///
    /// The caller must ensure the name is valid. Use `new()` for untrusted input.
    pub fn new_unchecked(name: impl Into<String>) -> Self {
        BranchName(name.into())
    }

    /// Validate a branch name
    pub fn validate(name: &str) -> Result<(), BranchNameError> {
        // Check empty
        if name.is_empty() {
            return Err(BranchNameError::Empty);
        }

        // Check length
        if name.len() > MAX_BRANCH_NAME_LENGTH {
            return Err(BranchNameError::TooLong {
                length: name.len(),
                max: MAX_BRANCH_NAME_LENGTH,
            });
        }

        // Check first character
        let first = name.chars().next().unwrap();
        if !first.is_ascii_alphanumeric() && first != '_' {
            return Err(BranchNameError::InvalidStart { char: first });
        }

        // Check all characters
        for (pos, ch) in name.chars().enumerate() {
            if !Self::is_valid_char(ch) {
                return Err(BranchNameError::InvalidChar {
                    char: ch,
                    position: pos,
                });
            }
        }

        Ok(())
    }

    /// Check if a character is valid in a branch name
    #[inline]
    fn is_valid_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'
    }

    /// Get the name as a string slice
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Check if this name matches a pattern (simple prefix match)
    pub fn starts_with(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }

    /// Check if this name matches a pattern (simple suffix match)
    pub fn ends_with(&self, suffix: &str) -> bool {
        self.0.ends_with(suffix)
    }

    /// Check if this name contains a substring
    pub fn contains(&self, substr: &str) -> bool {
        self.0.contains(substr)
    }
}

impl AsRef<str> for BranchName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for BranchName {
    type Error = BranchNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        BranchName::new(value)
    }
}

impl TryFrom<&str> for BranchName {
    type Error = BranchNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        BranchName::new(value)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_branch_name_valid() {
        // Simple names
        assert!(BranchName::new("test").is_ok());
        assert!(BranchName::new("test-run").is_ok());
        assert!(BranchName::new("test_run").is_ok());
        assert!(BranchName::new("test.run").is_ok());
        assert!(BranchName::new("TestRun123").is_ok());

        // Starting with underscore
        assert!(BranchName::new("_private").is_ok());

        // Mixed
        assert!(BranchName::new("training-gpt-v3-2024").is_ok());
        assert!(BranchName::new("experiment.v2.1").is_ok());
        assert!(BranchName::new("prod_agent_2024_01_15").is_ok());
    }

    #[test]
    fn test_branch_name_empty() {
        let err = BranchName::new("").unwrap_err();
        assert_eq!(err, BranchNameError::Empty);
    }

    #[test]
    fn test_branch_name_too_long() {
        let long_name = "a".repeat(MAX_BRANCH_NAME_LENGTH + 1);
        let err = BranchName::new(long_name).unwrap_err();
        assert!(matches!(err, BranchNameError::TooLong { .. }));
    }

    #[test]
    fn test_branch_name_max_length_ok() {
        let max_name = "a".repeat(MAX_BRANCH_NAME_LENGTH);
        assert!(BranchName::new(max_name).is_ok());
    }

    #[test]
    fn test_branch_name_invalid_start_dash() {
        let err = BranchName::new("-starts-with-dash").unwrap_err();
        assert!(matches!(err, BranchNameError::InvalidStart { char: '-' }));
    }

    #[test]
    fn test_branch_name_invalid_start_dot() {
        let err = BranchName::new(".hidden").unwrap_err();
        assert!(matches!(err, BranchNameError::InvalidStart { char: '.' }));
    }

    #[test]
    fn test_branch_name_invalid_chars() {
        // Space
        let err = BranchName::new("has space").unwrap_err();
        assert!(matches!(
            err,
            BranchNameError::InvalidChar { char: ' ', .. }
        ));

        // Special characters
        let err = BranchName::new("has@special").unwrap_err();
        assert!(matches!(
            err,
            BranchNameError::InvalidChar { char: '@', .. }
        ));

        // Unicode
        let err = BranchName::new("has\u{1F600}emoji").unwrap_err();
        assert!(matches!(err, BranchNameError::InvalidChar { .. }));
    }

    #[test]
    fn test_branch_name_as_str() {
        let name = BranchName::new("test-run").unwrap();
        assert_eq!(name.as_str(), "test-run");
    }

    #[test]
    fn test_branch_name_into_inner() {
        let name = BranchName::new("test-run").unwrap();
        assert_eq!(name.into_inner(), "test-run".to_string());
    }

    #[test]
    fn test_branch_name_display() {
        let name = BranchName::new("test-run").unwrap();
        assert_eq!(format!("{}", name), "test-run");
    }

    #[test]
    fn test_branch_name_equality() {
        let name1 = BranchName::new("test").unwrap();
        let name2 = BranchName::new("test").unwrap();
        let name3 = BranchName::new("other").unwrap();

        assert_eq!(name1, name2);
        assert_ne!(name1, name3);
    }

    #[test]
    fn test_branch_name_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(BranchName::new("name1").unwrap());
        set.insert(BranchName::new("name2").unwrap());
        set.insert(BranchName::new("name1").unwrap()); // Duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_branch_name_serialization() {
        let name = BranchName::new("test-run").unwrap();
        let json = serde_json::to_string(&name).unwrap();
        let restored: BranchName = serde_json::from_str(&json).unwrap();
        assert_eq!(name, restored);
    }

    #[test]
    fn test_branch_name_try_from_string() {
        let name: Result<BranchName, _> = "test-run".to_string().try_into();
        assert!(name.is_ok());

        let name: Result<BranchName, _> = "".to_string().try_into();
        assert!(name.is_err());
    }

    #[test]
    fn test_branch_name_try_from_str() {
        let name: Result<BranchName, _> = "test-run".try_into();
        assert!(name.is_ok());

        let name: Result<BranchName, _> = "".try_into();
        assert!(name.is_err());
    }

    #[test]
    fn test_branch_name_starts_with() {
        let name = BranchName::new("training-run-1").unwrap();
        assert!(name.starts_with("training"));
        assert!(!name.starts_with("test"));
    }

    #[test]
    fn test_branch_name_ends_with() {
        let name = BranchName::new("training-run-1").unwrap();
        assert!(name.ends_with("-1"));
        assert!(!name.ends_with("-2"));
    }

    #[test]
    fn test_branch_name_contains() {
        let name = BranchName::new("training-run-1").unwrap();
        assert!(name.contains("run"));
        assert!(!name.contains("test"));
    }

    #[test]
    fn test_branch_name_error_display() {
        assert_eq!(
            format!("{}", BranchNameError::Empty),
            "branch name cannot be empty"
        );
        assert!(format!(
            "{}",
            BranchNameError::TooLong {
                length: 300,
                max: 256
            }
        )
        .contains("too long"));
        assert!(format!(
            "{}",
            BranchNameError::InvalidChar {
                char: '@',
                position: 5
            }
        )
        .contains("@"));
        assert!(format!("{}", BranchNameError::InvalidStart { char: '-' }).contains("start"));
    }

    #[test]
    fn test_branch_name_new_unchecked() {
        // This bypasses validation - use carefully
        let name = BranchName::new_unchecked("any-string-even-invalid!");
        assert_eq!(name.as_str(), "any-string-even-invalid!");
    }

    #[test]
    fn test_branch_name_new_unchecked_empty_string() {
        // new_unchecked bypasses validation, so empty is allowed
        let name = BranchName::new_unchecked("");
        assert_eq!(name.as_str(), "");
    }

    #[test]
    fn test_branch_name_multibyte_rejected() {
        // "é" is 2 bytes in UTF-8 but not ASCII alphanumeric
        // Short enough to not trigger TooLong, so InvalidChar should fire
        let name = "aé";
        let err = BranchName::new(name).unwrap_err();
        assert!(matches!(err, BranchNameError::InvalidChar { .. }));
    }

    #[test]
    fn test_branch_name_multibyte_too_long() {
        // 255 ASCII chars + 2-byte "é" = 257 bytes → TooLong fires before InvalidChar
        let name = "a".repeat(MAX_BRANCH_NAME_LENGTH - 1) + "é";
        let err = BranchName::new(name).unwrap_err();
        assert!(matches!(err, BranchNameError::TooLong { .. }));
    }

    #[test]
    fn test_branch_name_length_check_is_byte_length() {
        // Verify that length check uses byte length not char count
        // A string of 257 ASCII chars should fail
        let name = "a".repeat(MAX_BRANCH_NAME_LENGTH + 1);
        let err = BranchName::new(name).unwrap_err();
        assert!(matches!(
            err,
            BranchNameError::TooLong {
                length: 257,
                max: 256
            }
        ));
    }

    #[test]
    fn test_branch_name_consecutive_special_chars() {
        assert!(BranchName::new("a--b").is_ok());
        assert!(BranchName::new("a..b").is_ok());
        assert!(BranchName::new("a__b").is_ok());
        assert!(BranchName::new("a-._b").is_ok());
    }

    #[test]
    fn test_branch_name_single_char() {
        assert!(BranchName::new("a").is_ok());
        assert!(BranchName::new("Z").is_ok());
        assert!(BranchName::new("0").is_ok());
        assert!(BranchName::new("_").is_ok());
        assert!(BranchName::new("-").is_err()); // dash can't start
        assert!(BranchName::new(".").is_err()); // dot can't start
    }

    #[test]
    fn test_branch_name_as_ref() {
        let name = BranchName::new("test").unwrap();
        let s: &str = name.as_ref();
        assert_eq!(s, "test");
    }

    #[test]
    fn test_branch_name_error_is_std_error() {
        let err = BranchName::new("").unwrap_err();
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_branch_name_invalid_char_position_accuracy() {
        let err = BranchName::new("abc@def").unwrap_err();
        match err {
            BranchNameError::InvalidChar { char: c, position } => {
                assert_eq!(c, '@');
                assert_eq!(position, 3);
            }
            _ => panic!("Expected InvalidChar"),
        }
    }
}
