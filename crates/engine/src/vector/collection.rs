//! Collection management types and validation
//!
//! This module provides:
//! - Collection name validation
//! - Vector key validation
//!
//! CollectionId and CollectionInfo are defined in types.rs.

use crate::vector::VectorError;

/// Validate a collection name
///
/// # Validation Rules
/// - Cannot be empty
/// - Cannot exceed 256 characters
/// - Cannot contain '/' (used as key separator)
/// - Cannot contain null bytes
/// - Cannot start with '_' (reserved for system use)
pub fn validate_collection_name(name: &str) -> Result<(), VectorError> {
    if name.is_empty() {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot be empty".to_string(),
        });
    }

    if name.len() > 256 {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot exceed 256 characters".to_string(),
        });
    }

    // Forbidden characters that could cause key parsing issues
    if name.contains('/') {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot contain '/'".to_string(),
        });
    }

    if name.contains('\0') {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot contain null bytes".to_string(),
        });
    }

    // Names starting with underscore are reserved for system use
    if name.starts_with('_') {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection names starting with '_' are reserved".to_string(),
        });
    }

    Ok(())
}

/// Validate a system collection name (must start with `_system_`)
///
/// System collections bypass the `_` prefix restriction but must follow
/// the `_system_` prefix convention and all other naming rules.
pub fn validate_system_collection_name(name: &str) -> Result<(), VectorError> {
    if !name.starts_with("_system_") {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "System collection name must start with '_system_'".to_string(),
        });
    }

    if name.len() <= 8 {
        // "_system_" is 8 chars, need at least one more
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "System collection name must have content after '_system_' prefix".to_string(),
        });
    }

    if name.len() > 256 {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot exceed 256 characters".to_string(),
        });
    }

    if name.contains('/') {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot contain '/'".to_string(),
        });
    }

    if name.contains('\0') {
        return Err(VectorError::InvalidCollectionName {
            name: name.to_string(),
            reason: "Collection name cannot contain null bytes".to_string(),
        });
    }

    Ok(())
}

/// Validate a vector key
///
/// # Validation Rules
/// - Can be empty (empty string keys are allowed)
/// - Cannot exceed 1024 characters
/// - Cannot contain null bytes
pub fn validate_vector_key(key: &str) -> Result<(), VectorError> {
    // Empty string keys are allowed (consistent with other key-value stores)
    if key.len() > 1024 {
        return Err(VectorError::InvalidKey {
            key: key.to_string(),
            reason: "Vector key cannot exceed 1024 characters".to_string(),
        });
    }

    if key.contains('\0') {
        return Err(VectorError::InvalidKey {
            key: key.to_string(),
            reason: "Vector key cannot contain null bytes".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================
    // Collection Name Validation Tests (#346)
    // ========================================

    #[test]
    fn test_valid_collection_names() {
        assert!(validate_collection_name("valid_name").is_ok());
        assert!(validate_collection_name("collection-1").is_ok());
        assert!(validate_collection_name("MyCollection").is_ok());
        assert!(validate_collection_name("a").is_ok()); // Single char is valid
        assert!(validate_collection_name("embeddings").is_ok());
    }

    #[test]
    fn test_empty_collection_name() {
        let result = validate_collection_name("");
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { name, reason })
            if name.is_empty() && reason.contains("empty")
        ));
    }

    #[test]
    fn test_collection_name_too_long() {
        let long_name = "a".repeat(257);
        let result = validate_collection_name(&long_name);
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { reason, .. })
            if reason.contains("256")
        ));
    }

    #[test]
    fn test_collection_name_with_slash() {
        let result = validate_collection_name("has/slash");
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { reason, .. })
            if reason.contains("/")
        ));
    }

    #[test]
    fn test_collection_name_with_null() {
        let result = validate_collection_name("has\0null");
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { reason, .. })
            if reason.contains("null")
        ));
    }

    #[test]
    fn test_collection_name_reserved() {
        let result = validate_collection_name("_reserved");
        assert!(matches!(
            result,
            Err(VectorError::InvalidCollectionName { reason, .. })
            if reason.contains("reserved")
        ));

        // Just underscore is also reserved
        let result2 = validate_collection_name("_");
        assert!(matches!(
            result2,
            Err(VectorError::InvalidCollectionName { reason, .. })
            if reason.contains("reserved")
        ));
    }

    #[test]
    fn test_collection_name_max_length() {
        let max_name = "a".repeat(256);
        assert!(validate_collection_name(&max_name).is_ok());
    }

    // ========================================
    // Vector Key Validation Tests (#346)
    // ========================================

    #[test]
    fn test_valid_vector_keys() {
        assert!(validate_vector_key("valid_key").is_ok());
        assert!(validate_vector_key("doc-123").is_ok());
        assert!(validate_vector_key("path/to/doc").is_ok()); // slash is allowed in vector keys
        assert!(validate_vector_key("a").is_ok());
        assert!(validate_vector_key("doc:123:v1").is_ok());
    }

    #[test]
    fn test_empty_vector_key() {
        // Empty string keys are allowed (consistent with other key-value stores)
        assert!(validate_vector_key("").is_ok());
    }

    #[test]
    fn test_vector_key_too_long() {
        let long_key = "a".repeat(1025);
        let result = validate_vector_key(&long_key);
        assert!(matches!(
            result,
            Err(VectorError::InvalidKey { reason, .. })
            if reason.contains("1024")
        ));
    }

    #[test]
    fn test_vector_key_with_null() {
        let result = validate_vector_key("has\0null");
        assert!(matches!(
            result,
            Err(VectorError::InvalidKey { reason, .. })
            if reason.contains("null")
        ));
    }

    #[test]
    fn test_vector_key_max_length() {
        let max_key = "a".repeat(1024);
        assert!(validate_vector_key(&max_key).is_ok());
    }
}
