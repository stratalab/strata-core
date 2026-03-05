//! Storage codec abstraction.
//!
//! The codec seam provides a hook point for future encryption-at-rest and
//! compression. All bytes passing through the storage layer go through the
//! codec for encode/decode operations.
//!
//!
//!
//! Uses `IdentityCodec` which performs no transformation. This establishes
//! the codec seam without adding complexity. Future milestones can add:
//!
//! - `AesGcmCodec`: AES-256-GCM encryption at rest
//! - `Lz4Codec`: LZ4 compression
//! - `ChainedCodec`: Compression + encryption pipeline
//!
//! # Usage
//!
//! ```text
//! use strata_durability::codec::{StorageCodec, IdentityCodec};
//!
//! let codec = IdentityCodec;
//! let data = b"hello world";
//!
//! let encoded = codec.encode(data);
//! let decoded = codec.decode(&encoded)?;
//!
//! assert_eq!(data.as_slice(), decoded.as_slice());
//! ```

mod aes_gcm;
mod identity;
mod traits;

pub use aes_gcm::AesGcmCodec;
pub use identity::IdentityCodec;
pub use traits::{CodecError, StorageCodec};

/// Get a codec by its identifier.
///
/// Returns the codec if recognized, or an error for unknown codec IDs.
///
/// # Known Codecs
///
/// - `"identity"`: No-op codec (pass-through)
///
/// # Future Codecs
///
/// - `"aes-gcm-256"`: AES-256-GCM encryption
/// - `"lz4"`: LZ4 compression
pub fn get_codec(codec_id: &str) -> Result<Box<dyn StorageCodec>, CodecError> {
    match codec_id {
        "identity" => Ok(Box::new(IdentityCodec)),
        "aes-gcm-256" => {
            let hex =
                std::env::var("STRATA_ENCRYPTION_KEY").map_err(|_| CodecError::DecodeError {
                    detail: "STRATA_ENCRYPTION_KEY environment variable not set. \
                             Set it to a 64 hex-character (32-byte) key."
                        .to_string(),
                    codec_id: "aes-gcm-256".to_string(),
                    data_len: 0,
                })?;
            let key = AesGcmCodec::key_from_hex(&hex)?;
            Ok(Box::new(AesGcmCodec::new(key)))
        }
        _ => Err(CodecError::UnknownCodec(codec_id.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_identity_codec() {
        let codec = get_codec("identity").unwrap();
        assert_eq!(codec.codec_id(), "identity");
    }

    #[test]
    fn test_get_unknown_codec() {
        let result = get_codec("unknown");
        assert!(matches!(result, Err(CodecError::UnknownCodec(_))));
    }

    #[test]
    fn test_get_aes_gcm_codec_without_env_var() {
        // Ensure the env var is unset for this test
        std::env::remove_var("STRATA_ENCRYPTION_KEY");
        let result = get_codec("aes-gcm-256");
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("STRATA_ENCRYPTION_KEY"),
                    "Error should mention the env var: {}",
                    msg
                );
            }
            Ok(_) => panic!("Expected error when env var is not set"),
        }
    }

    #[test]
    fn test_get_aes_gcm_codec_with_valid_env_var() {
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        std::env::set_var("STRATA_ENCRYPTION_KEY", hex);
        let result = get_codec("aes-gcm-256");
        // Clean up immediately
        std::env::remove_var("STRATA_ENCRYPTION_KEY");
        let codec = result.expect("Should create codec from valid env var");
        assert_eq!(codec.codec_id(), "aes-gcm-256");

        // Verify it actually works
        let data = b"roundtrip via get_codec";
        let encoded = codec.encode(data);
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_get_aes_gcm_codec_with_invalid_env_var() {
        std::env::set_var("STRATA_ENCRYPTION_KEY", "too-short");
        let result = get_codec("aes-gcm-256");
        std::env::remove_var("STRATA_ENCRYPTION_KEY");
        assert!(result.is_err());
    }
}
