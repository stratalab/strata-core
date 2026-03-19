//! Bloom filter for KV segment key existence checks.
//!
//! Standard bit-vector bloom filter with k hash functions derived from two
//! base hashes (Kirsch-Mitzenmacher optimization). Used to skip segments
//! that cannot contain a given key.

/// Hash algorithm used by this bloom filter.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum HashType {
    /// Legacy: CRC32 + FNV-1a dual hash.
    Legacy = 0,
    /// xxHash: split xxh3_64 into two 32-bit halves.
    XxHash = 1,
}

/// A bloom filter over byte-string keys.
///
/// Built once during segment creation, then queried during reads.
/// False positives are possible; false negatives are not.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hash_fns: u32,
    hash_type: HashType,
}

// FNV-1a hash for the second base hash (legacy path).
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Compute the two base hashes for the Kirsch-Mitzenmacher double-hashing scheme.
fn bloom_hashes(hash_type: HashType, key: &[u8]) -> (u64, u64) {
    match hash_type {
        HashType::Legacy => {
            let h1 = crc32fast::hash(key) as u64;
            let h2 = fnv1a(key);
            (h1, h2)
        }
        HashType::XxHash => {
            let h = xxhash_rust::xxh3::xxh3_64(key);
            let h1 = h;
            let h2 = (h >> 32) | 1; // odd for better distribution
            (h1, h2)
        }
    }
}

impl BloomFilter {
    /// Build a bloom filter from a set of keys.
    ///
    /// `bits_per_key` controls the false-positive rate. 10 bits/key gives ~1% FPR.
    /// An empty key set produces a minimal filter that always returns false.
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Self {
        if keys.is_empty() {
            return Self {
                bits: vec![0],
                num_hash_fns: 1,
                hash_type: HashType::XxHash,
            };
        }

        // Optimal number of hash functions: k = ln(2) * (m/n)
        // Clamped to [1, 30] for sanity.
        let k = ((bits_per_key as f64) * core::f64::consts::LN_2)
            .round()
            .clamp(1.0, 30.0) as u32;

        let num_bits = (keys.len() * bits_per_key).max(64);
        // Round up to whole bytes
        let num_bytes = (num_bits + 7) / 8;
        let actual_bits = num_bytes * 8;

        let mut bits = vec![0u8; num_bytes];

        for key in keys {
            let (h1, h2) = bloom_hashes(HashType::XxHash, key);
            for i in 0..k {
                let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
                bits[bit_pos as usize / 8] |= 1 << (bit_pos as usize % 8);
            }
        }

        Self {
            bits,
            num_hash_fns: k,
            hash_type: HashType::XxHash,
        }
    }

    /// Check if a key might be in the set.
    ///
    /// Returns `false` if the key is definitely not present.
    /// Returns `true` if the key is probably present (with FPR determined by bits_per_key).
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let actual_bits = self.bits.len() * 8;
        if actual_bits == 0 {
            return false;
        }

        let (h1, h2) = bloom_hashes(self.hash_type, key);
        for i in 0..self.num_hash_fns {
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
            if self.bits[bit_pos as usize / 8] & (1 << (bit_pos as usize % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter for writing into a segment file.
    ///
    /// New format: `[0xFF marker][hash_type: u8][num_hash_fns: u32 LE][bits...]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + 4 + self.bits.len());
        out.push(0xFF); // new-format marker
        out.push(self.hash_type as u8);
        out.extend_from_slice(&self.num_hash_fns.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a bloom filter from segment file bytes.
    ///
    /// Supports both legacy format `[num_hash_fns: u32 LE][bits...]` and new
    /// format `[0xFF][hash_type: u8][num_hash_fns: u32 LE][bits...]`.
    /// Returns `None` if the data is malformed.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        if data[0] == 0xFF {
            // New format: [0xFF][hash_type][num_hash_fns: u32][bits...]
            if data.len() < 6 {
                return None;
            }
            let hash_type = match data[1] {
                0 => HashType::Legacy,
                1 => HashType::XxHash,
                _ => return None,
            };
            let num_hash_fns = u32::from_le_bytes(data[2..6].try_into().ok()?);
            if num_hash_fns == 0 || num_hash_fns > 30 {
                return None;
            }
            let bits = data[6..].to_vec();
            Some(Self {
                bits,
                num_hash_fns,
                hash_type,
            })
        } else {
            // Legacy format: [num_hash_fns: u32][bits...]
            let num_hash_fns = u32::from_le_bytes(data[..4].try_into().ok()?);
            if num_hash_fns == 0 || num_hash_fns > 30 {
                return None;
            }
            let bits = data[4..].to_vec();
            Some(Self {
                bits,
                num_hash_fns,
                hash_type: HashType::Legacy,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_returns_false() {
        let filter = BloomFilter::build(&[], 10);
        assert!(!filter.maybe_contains(b"anything"));
    }

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        for key in &keys {
            assert!(filter.maybe_contains(key), "false negative for {:?}", key);
        }
    }

    #[test]
    fn fpr_under_two_percent_at_10_bits_per_key() {
        let keys: Vec<Vec<u8>> = (0..10_000u32)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        // Test with keys NOT in the set
        let mut false_positives = 0u32;
        let test_count = 100_000u32;
        for i in 0..test_count {
            let probe = format!("nonexistent_{}", i);
            if filter.maybe_contains(probe.as_bytes()) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / test_count as f64;
        assert!(
            fpr < 0.02,
            "FPR too high: {:.4} ({} false positives out of {})",
            fpr,
            false_positives,
            test_count,
        );
    }

    #[test]
    fn serialization_roundtrip() {
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        // Same behavior
        for key in &keys {
            assert!(restored.maybe_contains(key));
        }
        assert_eq!(filter.bits, restored.bits);
        assert_eq!(filter.num_hash_fns, restored.num_hash_fns);
        assert_eq!(restored.hash_type, HashType::XxHash);
    }

    #[test]
    fn from_bytes_rejects_too_short() {
        assert!(BloomFilter::from_bytes(&[]).is_none());
        assert!(BloomFilter::from_bytes(&[1, 2, 3]).is_none());
    }

    #[test]
    fn from_bytes_rejects_zero_hash_fns() {
        // num_hash_fns = 0 would make every query return true
        let mut data = vec![0u8; 12];
        // num_hash_fns = 0
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_rejects_excessive_hash_fns() {
        let mut data = vec![0u8; 12];
        data[0..4].copy_from_slice(&31u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn single_key_filter() {
        let filter = BloomFilter::build(&[b"hello"], 10);
        assert!(filter.maybe_contains(b"hello"));
    }

    // ── New tests for xxHash and versioning ──────────────────────────────

    #[test]
    fn xxhash_bloom_no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("xxkey_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        assert_eq!(filter.hash_type, HashType::XxHash);
        for key in &keys {
            assert!(filter.maybe_contains(key), "false negative for {:?}", key);
        }
    }

    #[test]
    fn xxhash_bloom_fpr_under_one_percent() {
        let keys: Vec<Vec<u8>> = (0..10_000u32)
            .map(|i| format!("xxfpr_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        let mut false_positives = 0u32;
        let test_count = 100_000u32;
        for i in 0..test_count {
            let probe = format!("xxprobe_{}", i);
            if filter.maybe_contains(probe.as_bytes()) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / test_count as f64;
        assert!(
            fpr < 0.01,
            "FPR too high for xxHash: {:.4} ({} false positives out of {})",
            fpr,
            false_positives,
            test_count,
        );
    }

    #[test]
    fn old_format_backward_compat_queries_work() {
        // Simulate what old code produced: build with legacy hashes, serialize
        // in the OLD format [num_hash_fns: u32 LE][bits...] (no 0xFF marker).
        let keys: Vec<Vec<u8>> = (0..500u32)
            .map(|i| format!("legacy_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        // Manually build with legacy hashes (mimics old BloomFilter::build)
        let bits_per_key = 10;
        let k = ((bits_per_key as f64) * core::f64::consts::LN_2)
            .round()
            .clamp(1.0, 30.0) as u32;
        let num_bits = (keys.len() * bits_per_key).max(64);
        let num_bytes = (num_bits + 7) / 8;
        let actual_bits = num_bytes * 8;
        let mut bits = vec![0u8; num_bytes];
        for key in &key_refs {
            let (h1, h2) = bloom_hashes(HashType::Legacy, key);
            for i in 0..k {
                let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
                bits[bit_pos as usize / 8] |= 1 << (bit_pos as usize % 8);
            }
        }

        // Serialize in OLD format (what old code wrote — no 0xFF marker)
        let mut old_bytes = Vec::with_capacity(4 + bits.len());
        old_bytes.extend_from_slice(&k.to_le_bytes());
        old_bytes.extend_from_slice(&bits);

        // Verify first byte is NOT 0xFF (it's a small num_hash_fns)
        assert_ne!(old_bytes[0], 0xFF);

        // Deserialize with new code
        let restored = BloomFilter::from_bytes(&old_bytes).unwrap();
        assert_eq!(restored.hash_type, HashType::Legacy);
        assert_eq!(restored.num_hash_fns, k);
        assert_eq!(restored.bits, bits);

        // All inserted keys must be found (the critical backward-compat check)
        for key in &keys {
            assert!(
                restored.maybe_contains(key),
                "false negative for legacy key {:?}",
                key
            );
        }

        // Absent keys should mostly NOT match (sanity check)
        let mut false_positives = 0u32;
        for i in 0..1000u32 {
            let probe = format!("absent_{}", i);
            if restored.maybe_contains(probe.as_bytes()) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 50,
            "legacy bloom has too many FPs: {}",
            false_positives
        );
    }

    #[test]
    fn new_format_with_legacy_hash_roundtrip() {
        // A bloom built with Legacy hashes but serialized with NEW format
        // (e.g., if we ever need to re-serialize an old bloom).
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("reser_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let bits_per_key = 10;
        let k = ((bits_per_key as f64) * core::f64::consts::LN_2)
            .round()
            .clamp(1.0, 30.0) as u32;
        let num_bits = (keys.len() * bits_per_key).max(64);
        let num_bytes = (num_bits + 7) / 8;
        let actual_bits = num_bytes * 8;
        let mut bits = vec![0u8; num_bytes];
        for key in &key_refs {
            let (h1, h2) = bloom_hashes(HashType::Legacy, key);
            for i in 0..k {
                let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
                bits[bit_pos as usize / 8] |= 1 << (bit_pos as usize % 8);
            }
        }
        let legacy_filter = BloomFilter {
            bits,
            num_hash_fns: k,
            hash_type: HashType::Legacy,
        };

        let bytes = legacy_filter.to_bytes();
        assert_eq!(bytes[0], 0xFF);
        assert_eq!(bytes[1], 0); // Legacy
        let restored = BloomFilter::from_bytes(&bytes).unwrap();
        assert_eq!(restored.hash_type, HashType::Legacy);
        for key in &keys {
            assert!(restored.maybe_contains(key));
        }
    }

    #[test]
    fn new_format_roundtrip() {
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("new_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        let bytes = filter.to_bytes();
        assert_eq!(bytes[0], 0xFF, "new format should start with 0xFF marker");
        assert_eq!(bytes[1], 1, "new builds should use XxHash (1)");

        let restored = BloomFilter::from_bytes(&bytes).unwrap();
        assert_eq!(restored.hash_type, HashType::XxHash);
        assert_eq!(restored.num_hash_fns, filter.num_hash_fns);
        assert_eq!(restored.bits, filter.bits);
        for key in &keys {
            assert!(restored.maybe_contains(key));
        }
    }

    #[test]
    fn cross_hash_no_false_match() {
        // Keys inserted with XxHash should NOT all be found when queried
        // with Legacy hashes (and vice versa). This verifies the two hash
        // schemes produce different bit patterns.
        let keys: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("cross_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);
        assert_eq!(filter.hash_type, HashType::XxHash);

        // Tamper: force Legacy hash type on the same bit array
        let tampered = BloomFilter {
            bits: filter.bits.clone(),
            num_hash_fns: filter.num_hash_fns,
            hash_type: HashType::Legacy,
        };

        // With wrong hash type, many keys should fail to match
        let mut misses = 0;
        for key in &keys {
            if !tampered.maybe_contains(key) {
                misses += 1;
            }
        }
        // If hashes were identical, misses would be 0. With different hashes,
        // the vast majority of keys should miss.
        assert!(
            misses > 100,
            "expected most keys to miss with wrong hash type, only {} missed out of {}",
            misses,
            keys.len()
        );
    }

    #[test]
    fn from_bytes_rejects_unknown_hash_type() {
        let mut data = vec![0u8; 20];
        data[0] = 0xFF;
        data[1] = 99; // unknown hash type
        data[2..6].copy_from_slice(&7u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_new_format_rejects_invalid_hash_fns() {
        // num_hash_fns = 0 in new format
        let mut data = vec![0u8; 20];
        data[0] = 0xFF;
        data[1] = 1; // XxHash
        data[2..6].copy_from_slice(&0u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());

        // num_hash_fns = 31 in new format
        data[2..6].copy_from_slice(&31u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_new_format_too_short() {
        // 0xFF marker but only 5 bytes total (need at least 6)
        assert!(BloomFilter::from_bytes(&[0xFF, 1, 7, 0, 0]).is_none());
        // Exactly 4 bytes starting with 0xFF
        assert!(BloomFilter::from_bytes(&[0xFF, 1, 7, 0]).is_none());
    }

    #[test]
    fn new_format_empty_bits_section() {
        // New format with valid header but zero-length bits
        let mut data = vec![0u8; 6];
        data[0] = 0xFF;
        data[1] = 1; // XxHash
        data[2..6].copy_from_slice(&7u32.to_le_bytes());
        let filter = BloomFilter::from_bytes(&data).unwrap();
        assert_eq!(filter.bits.len(), 0);
        // Empty bit array → always returns false
        assert!(!filter.maybe_contains(b"anything"));
    }
}
