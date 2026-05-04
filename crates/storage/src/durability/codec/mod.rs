//! Storage codec abstraction.
//!
//! The codec seam provides a hook point for durability artifacts that encode
//! payload bytes, such as WAL records. Snapshot containers only record and
//! validate the configured codec id; their section payloads own separate
//! wire-format encoding.
//!
//! # Available Codecs
//!
//! | Codec ID | Description | Status |
//! |----------|-------------|--------|
//! | `"identity"` | No-op pass-through | Default |
//! | `"aes-gcm-256"` | AES-256-GCM authenticated encryption | Available |
//!
//! # Enabling Encryption
//!
//! To enable AES-256-GCM encryption at rest:
//!
//! 1. Set the `STRATA_ENCRYPTION_KEY` environment variable to a 64-character
//!    hex string (32 bytes):
//!    ```text
//!    export STRATA_ENCRYPTION_KEY="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
//!    ```
//!
//! 2. Obtain a codec via [`get_codec("aes-gcm-256")`](get_codec) and pass it
//!    into the engine's durability wiring.
//!
//! **Important:** A database created with one codec cannot be opened with a
//! different codec — the MANIFEST records the codec ID and validates on open.
//!
//! # Identity Codec Example
//!
//! ```text
//! use strata_storage::durability::codec::{IdentityCodec, StorageCodec};
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

use aes_gcm::AesGcmCodec;
pub use identity::IdentityCodec;
pub use traits::{clone_codec, CodecError, StorageCodec};

/// Get a codec by its identifier.
///
/// Returns the codec if recognized, or an error for unknown codec IDs.
///
/// # Known Codecs
///
/// - `"identity"`: No-op codec (pass-through)
///
/// # Encryption
///
/// - `"aes-gcm-256"`: AES-256-GCM encryption (requires `STRATA_ENCRYPTION_KEY` env var)
///
/// # Future Codecs
///
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

/// Validate that a codec id resolves in the storage codec registry.
///
/// This intentionally follows the same path as [`get_codec`] so validation
/// includes any storage-local constructor requirements for the codec.
pub fn validate_codec_id(codec_id: &str) -> Result<(), CodecError> {
    get_codec(codec_id).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn encryption_env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

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
    fn test_validate_codec_id() {
        assert!(validate_codec_id("identity").is_ok());
        assert!(matches!(
            validate_codec_id("unknown"),
            Err(CodecError::UnknownCodec(_))
        ));
    }

    #[test]
    fn test_get_aes_gcm_codec_without_env_var() {
        let _env_guard = encryption_env_lock();
        // Ensure the env var is unset for this test
        std::env::remove_var("STRATA_ENCRYPTION_KEY");
        let result = get_codec("aes-gcm-256");
        let validation = validate_codec_id("aes-gcm-256");
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
        match validation {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("STRATA_ENCRYPTION_KEY"),
                    "Validation error should mention the env var: {}",
                    msg
                );
            }
            Ok(_) => panic!("Expected validation error when env var is not set"),
        }
    }

    #[test]
    fn test_get_aes_gcm_codec_with_valid_env_var() {
        let _env_guard = encryption_env_lock();
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
        let _env_guard = encryption_env_lock();
        std::env::set_var("STRATA_ENCRYPTION_KEY", "too-short");
        let result = get_codec("aes-gcm-256");
        std::env::remove_var("STRATA_ENCRYPTION_KEY");
        assert!(result.is_err());
    }
}
