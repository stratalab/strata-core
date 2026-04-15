//! AES-256-GCM authenticated encryption codec.
//!
//! Provides encryption at rest using AES-256-GCM. The encryption key is
//! sourced from the `STRATA_ENCRYPTION_KEY` environment variable (64 hex
//! characters = 32 bytes).
//!
//! Wire format: `nonce (12 bytes) || ciphertext + tag (data_len + 16 bytes)`

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::RngCore;

use super::traits::{CodecError, StorageCodec};

/// AES-256-GCM authenticated encryption codec.
pub(super) struct AesGcmCodec {
    key: [u8; 32],
}

impl AesGcmCodec {
    /// Create a new codec with the given 32-byte key.
    pub(super) fn new(key: [u8; 32]) -> Self {
        AesGcmCodec { key }
    }

    /// Parse a 64-character hex string into a 32-byte key.
    pub(super) fn key_from_hex(hex: &str) -> Result<[u8; 32], CodecError> {
        let hex = hex.trim();
        if hex.len() != 64 {
            return Err(CodecError::DecodeError {
                detail: format!(
                    "STRATA_ENCRYPTION_KEY must be exactly 64 hex chars (32 bytes), got {}",
                    hex.len()
                ),
                codec_id: "aes-gcm-256".to_string(),
                data_len: 0,
            });
        }
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| {
                CodecError::DecodeError {
                    detail: format!("Invalid hex at position {}", i * 2),
                    codec_id: "aes-gcm-256".to_string(),
                    data_len: 0,
                }
            })?;
        }
        Ok(key)
    }
}

impl StorageCodec for AesGcmCodec {
    fn encode(&self, data: &[u8]) -> Vec<u8> {
        let cipher = Aes256Gcm::new((&self.key).into());

        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, data)
            .expect("AES-GCM encryption should not fail");

        // Wire format: nonce (12) || ciphertext+tag
        let mut output = Vec::with_capacity(12 + ciphertext.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);
        output
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.len() < 12 + 16 {
            return Err(CodecError::decode(
                format!(
                    "Ciphertext too short: need at least 28 bytes (12 nonce + 16 tag), got {}",
                    data.len()
                ),
                "aes-gcm-256",
                data.len(),
            ));
        }

        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let cipher = Aes256Gcm::new((&self.key).into());
        cipher.decrypt(nonce, ciphertext).map_err(|_| {
            CodecError::decode(
                "Decryption failed (wrong key or corrupted data)",
                "aes-gcm-256",
                data.len(),
            )
        })
    }

    fn codec_id(&self) -> &str {
        "aes-gcm-256"
    }

    fn clone_box(&self) -> Box<dyn StorageCodec> {
        Box::new(AesGcmCodec { key: self.key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = i as u8;
        }
        key
    }

    #[test]
    fn roundtrip() {
        let codec = AesGcmCodec::new(test_key());
        let data = b"hello world, this is a test of AES-256-GCM encryption";
        let encoded = codec.encode(data);
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_empty_data() {
        let codec = AesGcmCodec::new(test_key());
        let data = b"";
        let encoded = codec.encode(data);
        assert_eq!(encoded.len(), 12 + 16); // nonce + tag only
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn roundtrip_large_data() {
        let codec = AesGcmCodec::new(test_key());
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let encoded = codec.encode(&data);
        assert_eq!(encoded.len(), 12 + data.len() + 16);
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn wrong_key_fails_decode() {
        let codec1 = AesGcmCodec::new(test_key());
        let mut wrong_key = test_key();
        wrong_key[0] ^= 0xFF;
        let codec2 = AesGcmCodec::new(wrong_key);

        let encoded = codec1.encode(b"secret data");
        let result = codec2.decode(&encoded);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Decryption failed"));
    }

    #[test]
    fn truncated_data_fails() {
        let codec = AesGcmCodec::new(test_key());
        let result = codec.decode(&[0u8; 10]); // too short
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn corrupted_ciphertext_fails() {
        let codec = AesGcmCodec::new(test_key());
        let mut encoded = codec.encode(b"data");
        // Corrupt a byte in the ciphertext area
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        assert!(codec.decode(&encoded).is_err());
    }

    #[test]
    fn codec_id_is_correct() {
        let codec = AesGcmCodec::new(test_key());
        assert_eq!(codec.codec_id(), "aes-gcm-256");
    }

    #[test]
    fn clone_preserves_key() {
        let codec = AesGcmCodec::new(test_key());
        let cloned = codec.clone_box();

        let data = b"clone test";
        let encoded = codec.encode(data);
        let decoded = cloned.decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn each_encode_produces_different_output() {
        let codec = AesGcmCodec::new(test_key());
        let data = b"determinism check";
        let enc1 = codec.encode(data);
        let enc2 = codec.encode(data);
        // Different nonces → different ciphertext
        assert_ne!(enc1, enc2);
        // But both decrypt to the same plaintext
        assert_eq!(codec.decode(&enc1).unwrap(), data);
        assert_eq!(codec.decode(&enc2).unwrap(), data);
    }

    #[test]
    fn encode_cow_default_roundtrips() {
        // AesGcmCodec inherits the default encode_cow (Cow::Owned via encode()).
        // Verify it produces decodable output.
        let codec = AesGcmCodec::new(test_key());
        let data = b"cow roundtrip test";
        let cow = codec.encode_cow(data);
        assert!(matches!(cow, std::borrow::Cow::Owned(_)));
        let decoded = codec.decode(&cow).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn key_from_hex_valid() {
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let key = AesGcmCodec::key_from_hex(hex).unwrap();
        assert_eq!(key, test_key());
    }

    #[test]
    fn key_from_hex_wrong_length() {
        assert!(AesGcmCodec::key_from_hex("0011").is_err());
    }

    #[test]
    fn key_from_hex_invalid_chars() {
        let hex = "zz0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        assert!(AesGcmCodec::key_from_hex(hex).is_err());
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AesGcmCodec>();
    }
}
