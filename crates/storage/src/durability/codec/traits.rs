//! Storage codec trait definitions.

/// Storage codec trait.
///
/// Durability artifacts that opt into storage codec protection pass their
/// payload bytes through this codec. WAL records use it for record payloads.
///
/// Snapshot files are different: their header records and validates the
/// database storage codec id, but the snapshot container does not wrap section
/// bytes in this codec. Each snapshot section owns its own wire-format
/// encoding; primitive checkpoint sections currently use the canonical
/// primitive-section codec.
///
/// # Thread Safety
///
/// Codecs must be `Send + Sync` to allow concurrent encoding/decoding
/// from multiple threads.
///
/// # Codec Identity
///
/// Each codec has a unique identifier that is stored in the MANIFEST.
/// This allows the database to verify it's using the correct codec
/// when reopening.
pub trait StorageCodec: Send + Sync {
    /// Encode bytes for storage.
    ///
    /// The returned bytes are what gets written to disk.
    /// For IdentityCodec, this is a no-op.
    fn encode(&self, data: &[u8]) -> Vec<u8>;

    /// Encode bytes for storage, returning a `Cow` to avoid allocation
    /// when the codec is a no-op (e.g., `IdentityCodec`).
    ///
    /// The default delegates to `encode()`. Override for zero-copy codecs.
    fn encode_cow<'a>(&self, data: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
        std::borrow::Cow::Owned(self.encode(data))
    }

    /// Decode bytes from storage.
    ///
    /// Reverses the encode operation. Returns an error if the data
    /// cannot be decoded (e.g., decryption failure, corruption).
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError>;

    /// Unique codec identifier.
    ///
    /// This is stored in the MANIFEST to ensure the correct codec
    /// is used when reopening a database.
    fn codec_id(&self) -> &str;

    /// Clone this codec into a new boxed trait object.
    ///
    /// This preserves internal state (e.g., encryption keys) that
    /// would be lost by reconstructing from `codec_id()` alone.
    fn clone_box(&self) -> Box<dyn StorageCodec>;
}

/// Codec errors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// Decoding failed (e.g., decryption failure, invalid format).
    ///
    /// Carries the codec identity and data length so callers can
    /// distinguish a wrong-codec error from data corruption.
    #[error("Decode error (codec={codec_id}, data_len={data_len}): {detail}")]
    DecodeError {
        /// Human-readable error description
        detail: String,
        /// Codec ID that attempted the decode
        codec_id: String,
        /// Length of the data that failed to decode
        data_len: usize,
    },

    /// Unknown codec identifier.
    #[error("Unknown codec: {0}")]
    UnknownCodec(String),

    /// Codec mismatch (database was created with different codec).
    #[error("Codec mismatch: expected {expected}, got {actual}")]
    CodecMismatch {
        /// Expected codec ID from MANIFEST
        expected: String,
        /// Actual codec ID being used
        actual: String,
    },
}

impl CodecError {
    /// Create a decode error with full diagnostic context.
    pub fn decode(detail: impl Into<String>, codec_id: impl Into<String>, data_len: usize) -> Self {
        CodecError::DecodeError {
            detail: detail.into(),
            codec_id: codec_id.into(),
            data_len,
        }
    }
}

/// Clone a boxed codec, preserving internal state (e.g., encryption keys).
///
/// Convenience wrapper around [`StorageCodec::clone_box()`] for use with
/// `&dyn StorageCodec` references.
pub fn clone_codec(codec: &dyn StorageCodec) -> Box<dyn StorageCodec> {
    codec.clone_box()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::codec::IdentityCodec;

    // Test that trait is object-safe
    fn _accepts_box_dyn_codec(_codec: Box<dyn StorageCodec>) {}

    #[test]
    fn test_encode_cow_matches_encode_via_trait_object() {
        // encode_cow through a trait object must return the same bytes as encode
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);
        let data = b"test data for cow";

        let encoded = codec.encode(data);
        let cow = codec.encode_cow(data);

        assert_eq!(&*cow, &*encoded);
    }

    #[test]
    fn test_codec_trait_object_safe() {
        // Verify we can create and use a boxed trait object
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);

        // Test encode/decode through trait object
        let data = b"test data";
        let encoded = codec.encode(data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_codec_trait_codec_id() {
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);
        assert_eq!(codec.codec_id(), "identity");
    }

    #[test]
    fn test_codec_error_display() {
        let err = CodecError::decode("test error", "identity", 42);
        let msg = err.to_string();
        assert!(msg.contains("test error"));
        assert!(msg.contains("identity"));
        assert!(msg.contains("42"));

        let err = CodecError::UnknownCodec("mystery".to_string());
        assert!(err.to_string().contains("mystery"));

        let err = CodecError::CodecMismatch {
            expected: "aes256".to_string(),
            actual: "identity".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("aes256"));
        assert!(msg.contains("identity"));
    }

    #[test]
    fn test_codec_error_equality() {
        let err1 = CodecError::decode("error", "identity", 10);
        let err2 = CodecError::decode("error", "identity", 10);
        let err3 = CodecError::decode("different", "identity", 10);

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_codec_roundtrip_empty_data() {
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);

        let data = b"";
        let encoded = codec.encode(data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_codec_roundtrip_large_data() {
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);

        // 1MB of data
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let encoded = codec.encode(&data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_codec_roundtrip_binary_data() {
        let codec: Box<dyn StorageCodec> = Box::new(IdentityCodec);

        // Data with all byte values including null bytes
        let data: Vec<u8> = (0..=255).collect();
        let encoded = codec.encode(&data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, data);
    }
}
