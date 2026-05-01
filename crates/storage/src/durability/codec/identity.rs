//! Identity codec (no transformation).
//!
//! This is the default codec. It performs no transformation on data,
//! simply passing bytes through unchanged. This establishes the codec seam
//! for future encryption-at-rest without adding complexity.

use super::traits::{CodecError, StorageCodec};

/// Identity codec - no transformation.
///
/// Bytes pass through unchanged. This is the default codec.
///
/// # Example
///
/// ```
/// use strata_storage::durability::codec::{IdentityCodec, StorageCodec};
///
/// let codec = IdentityCodec;
/// let data = b"hello world";
///
/// let encoded = codec.encode(data);
/// assert_eq!(data.as_slice(), encoded.as_slice());
///
/// let decoded = codec.decode(&encoded).unwrap();
/// assert_eq!(data.as_slice(), decoded.as_slice());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityCodec;

impl StorageCodec for IdentityCodec {
    fn encode(&self, data: &[u8]) -> Vec<u8> {
        data.to_vec()
    }

    fn encode_cow<'a>(&self, data: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
        std::borrow::Cow::Borrowed(data)
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(data.to_vec())
    }

    fn codec_id(&self) -> &str {
        "identity"
    }

    fn clone_box(&self) -> Box<dyn StorageCodec> {
        Box::new(IdentityCodec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_encode() {
        let codec = IdentityCodec;
        let data = vec![1, 2, 3, 4, 5];
        let encoded = codec.encode(&data);
        assert_eq!(data, encoded);
    }

    #[test]
    fn test_identity_decode() {
        let codec = IdentityCodec;
        let data = vec![1, 2, 3, 4, 5];
        let decoded = codec.decode(&data).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_identity_roundtrip() {
        let codec = IdentityCodec;
        let data = vec![0xFF, 0x00, 0xAB, 0xCD];

        let encoded = codec.encode(&data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_identity_empty() {
        let codec = IdentityCodec;
        let data: Vec<u8> = vec![];

        let encoded = codec.encode(&data);
        let decoded = codec.decode(&encoded).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_identity_large() {
        let codec = IdentityCodec;
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let encoded = codec.encode(&data);
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(data, decoded);
    }

    #[test]
    fn test_codec_id() {
        let codec = IdentityCodec;
        assert_eq!(codec.codec_id(), "identity");
    }

    #[test]
    fn test_identity_encode_cow_borrows() {
        let codec = IdentityCodec;
        let data = vec![1, 2, 3, 4, 5];
        let cow = codec.encode_cow(&data);

        // Must be Borrowed (zero-copy)
        assert!(matches!(cow, std::borrow::Cow::Borrowed(_)));
        // Content must match encode()
        assert_eq!(&*cow, &*codec.encode(&data));
    }

    #[test]
    fn test_identity_encode_cow_empty() {
        let codec = IdentityCodec;
        let data: Vec<u8> = vec![];
        let cow = codec.encode_cow(&data);
        assert!(matches!(cow, std::borrow::Cow::Borrowed(_)));
        assert!(cow.is_empty());
    }

    #[test]
    fn test_identity_is_copy() {
        let codec = IdentityCodec;
        let _codec2 = codec; // Should be Copy
        let _codec3 = codec; // Still valid
    }

    #[test]
    fn test_identity_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IdentityCodec>();
    }
}
