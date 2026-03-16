//! Bloom filter for KV segment key existence checks.
//!
//! Standard bit-vector bloom filter with k hash functions derived from two
//! base hashes (Kirsch-Mitzenmacher optimization). Used to skip segments
//! that cannot contain a given key.

/// A bloom filter over byte-string keys.
///
/// Built once during segment creation, then queried during reads.
/// False positives are possible; false negatives are not.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hash_fns: u32,
}

// FNV-1a hash for the second base hash.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
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
            let h1 = crc32fast::hash(key) as u64;
            let h2 = fnv1a(key);
            for i in 0..k {
                let bit_pos =
                    (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
                bits[bit_pos as usize / 8] |= 1 << (bit_pos as usize % 8);
            }
        }

        Self {
            bits,
            num_hash_fns: k,
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

        let h1 = crc32fast::hash(key) as u64;
        let h2 = fnv1a(key);
        for i in 0..self.num_hash_fns {
            let bit_pos =
                (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (actual_bits as u64);
            if self.bits[bit_pos as usize / 8] & (1 << (bit_pos as usize % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter for writing into a segment file.
    ///
    /// Format: `[num_hash_fns: u32 LE][bits...]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len());
        out.extend_from_slice(&self.num_hash_fns.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a bloom filter from segment file bytes.
    ///
    /// Returns `None` if the data is too short.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }
        let num_hash_fns = u32::from_le_bytes(data[..4].try_into().ok()?);
        if num_hash_fns == 0 || num_hash_fns > 30 {
            return None;
        }
        let bits = data[4..].to_vec();
        Some(Self {
            bits,
            num_hash_fns,
        })
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
            assert!(
                filter.maybe_contains(key),
                "false negative for {:?}",
                key
            );
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
}
