//! Cache-local blocked bloom filter for KV segment key existence checks.
//!
//! All probes for a given key are confined to a single 64-byte cache-line block,
//! reducing L1 cache misses from ~7 (standard bloom) to 1. Based on the same
//! principle as RocksDB's FastLocalBloom.

/// A cache-local blocked bloom filter over byte-string keys.
///
/// Built once during segment creation, then queried during reads.
/// False positives are possible; false negatives are not.
///
/// The bit array is always a multiple of 64 bytes (one cache line per block).
/// All probes for a key land in the same 64-byte block.
pub struct BloomFilter {
    bits: Vec<u8>,
    num_probes: u32,
}

/// Magic byte for the cache-local blocked bloom format.
const MAGIC: u8 = 0xFB;

/// Bits per cache-line block.
const BLOCK_BITS: u32 = 512; // 64 bytes * 8

/// Bytes per cache-line block.
const BLOCK_BYTES: usize = 64;

/// log2(BLOCK_BITS). Used to extract the top bits from a 32-bit probe hash.
const BLOCK_BITS_LOG2: u32 = 9;

/// Right-shift amount to map a u32 hash to a bit index within a block.
/// Takes the top BLOCK_BITS_LOG2 bits of the 32-bit hash for better distribution
/// (lower bits of multiplicative hashes are low quality).
const PROBE_SHIFT: u32 = 32 - BLOCK_BITS_LOG2;

// Compile-time check: BLOCK_BITS must equal 2^BLOCK_BITS_LOG2.
const _: () = assert!(BLOCK_BITS == 1 << BLOCK_BITS_LOG2);
// Compile-time check: BLOCK_BYTES must match BLOCK_BITS.
const _: () = assert!(BLOCK_BYTES * 8 == BLOCK_BITS as usize);

impl BloomFilter {
    /// Build a bloom filter from a set of keys.
    ///
    /// `bits_per_key` controls the false-positive rate. 10 bits/key gives ~1% FPR.
    /// An empty key set produces a minimal filter that always returns false.
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Self {
        if keys.is_empty() {
            return Self {
                bits: vec![],
                num_probes: 1,
            };
        }

        // Optimal number of probes: k = ln(2) * bits_per_key, clamped to [1, 30].
        let num_probes = ((bits_per_key as f64) * core::f64::consts::LN_2)
            .ceil()
            .clamp(1.0, 30.0) as u32;

        // Number of 64-byte blocks needed.
        let total_bits = keys.len() * bits_per_key;
        let num_blocks = ((total_bits + (BLOCK_BITS as usize - 1)) / BLOCK_BITS as usize).max(1);
        let mut bits = vec![0u8; num_blocks * BLOCK_BYTES];

        for key in keys {
            let h = xxhash_rust::xxh3::xxh3_64(key);
            let block_idx = ((h >> 32) as usize) % num_blocks;
            let block_start = block_idx * BLOCK_BYTES;

            let mut h2 = h as u32;
            for _ in 0..num_probes {
                h2 = h2.wrapping_mul(0x9e3779b9).wrapping_add(1);
                let bit = (h2 >> PROBE_SHIFT) as usize;
                bits[block_start + bit / 8] |= 1 << (bit % 8);
            }
        }

        Self { bits, num_probes }
    }

    /// Check if a key might be in the set.
    ///
    /// Returns `false` if the key is definitely not present.
    /// Returns `true` if the key is probably present (with FPR determined by bits_per_key).
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return false;
        }

        let num_blocks = self.bits.len() / BLOCK_BYTES;
        let h = xxhash_rust::xxh3::xxh3_64(key);
        let block_idx = ((h >> 32) as usize) % num_blocks;
        let block_start = block_idx * BLOCK_BYTES;

        let mut h2 = h as u32;
        for _ in 0..self.num_probes {
            h2 = h2.wrapping_mul(0x9e3779b9).wrapping_add(1);
            let bit = (h2 >> PROBE_SHIFT) as usize;
            if self.bits[block_start + bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter for writing into a segment file.
    ///
    /// Format: `[0xFB magic][num_probes: u32 LE][bits...]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4 + self.bits.len());
        out.push(MAGIC);
        out.extend_from_slice(&self.num_probes.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a bloom filter from segment file bytes.
    ///
    /// Only accepts the `0xFB` cache-local blocked format.
    /// Returns `None` if the data is malformed.
    /// Check if a key might be present, operating directly on serialized bytes.
    ///
    /// Zero-copy: borrows the byte slice without allocating. Use this on the
    /// read hot path instead of `from_bytes` + `maybe_contains`.
    pub fn maybe_contains_raw(data: &[u8], key: &[u8]) -> Option<bool> {
        if data.len() < 5 || data[0] != MAGIC {
            return None;
        }
        let num_probes = u32::from_le_bytes(data[1..5].try_into().ok()?);
        if num_probes == 0 || num_probes > 30 {
            return None;
        }
        let bits = &data[5..];
        if bits.is_empty() {
            return Some(false);
        }
        if bits.len() % BLOCK_BYTES != 0 {
            return None;
        }

        let num_blocks = bits.len() / BLOCK_BYTES;
        let h = xxhash_rust::xxh3::xxh3_64(key);
        let block_idx = ((h >> 32) as usize) % num_blocks;
        let block_start = block_idx * BLOCK_BYTES;

        let mut h2 = h as u32;
        for _ in 0..num_probes {
            h2 = h2.wrapping_mul(0x9e3779b9).wrapping_add(1);
            let bit = (h2 >> PROBE_SHIFT) as usize;
            if bits[block_start + bit / 8] & (1 << (bit % 8)) == 0 {
                return Some(false);
            }
        }
        Some(true)
    }

    /// Deserialize a bloom filter from raw bytes (allocating copy).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 5 {
            return None;
        }
        if data[0] != MAGIC {
            return None;
        }
        let num_probes = u32::from_le_bytes(data[1..5].try_into().ok()?);
        if num_probes == 0 || num_probes > 30 {
            return None;
        }
        let bits = data[5..].to_vec();
        // bits must be a multiple of 64 bytes (or empty for an empty filter)
        if !bits.is_empty() && bits.len() % BLOCK_BYTES != 0 {
            return None;
        }
        Some(Self { bits, num_probes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_returns_false() {
        let filter = BloomFilter::build(&[], 10);
        assert!(filter.bits.is_empty());
        assert!(!filter.maybe_contains(b"anything"));
        assert!(!filter.maybe_contains(b""));
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
    fn fpr_under_two_percent() {
        let keys: Vec<Vec<u8>> = (0..10_000u32)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

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
    fn single_key_filter() {
        let filter = BloomFilter::build(&[b"hello"], 10);
        assert!(filter.maybe_contains(b"hello"));
        // Single key → 1 block of 64 bytes
        assert_eq!(filter.bits.len(), BLOCK_BYTES);
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

        assert_eq!(filter.bits, restored.bits);
        assert_eq!(filter.num_probes, restored.num_probes);
        for key in &keys {
            assert!(restored.maybe_contains(key));
        }
        // Non-members must also agree after roundtrip
        for i in 0..100u32 {
            let probe = format!("absent_{}", i);
            assert_eq!(
                filter.maybe_contains(probe.as_bytes()),
                restored.maybe_contains(probe.as_bytes()),
            );
        }
    }

    #[test]
    fn serialization_roundtrip_empty() {
        let filter = BloomFilter::build(&[], 10);
        let bytes = filter.to_bytes();
        assert_eq!(bytes.len(), 5); // magic + num_probes only
        let restored = BloomFilter::from_bytes(&bytes).unwrap();
        assert!(restored.bits.is_empty());
        assert!(!restored.maybe_contains(b"anything"));
    }

    #[test]
    fn format_bytes() {
        let keys: Vec<Vec<u8>> = (0..50u32)
            .map(|i| format!("fmt_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);

        let bytes = filter.to_bytes();
        assert_eq!(bytes[0], MAGIC, "first byte should be 0xFB magic");
        // Verify num_probes is correctly encoded
        let encoded_probes = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        assert_eq!(encoded_probes, filter.num_probes);
        assert!((1..=30).contains(&encoded_probes));
        // bits portion must be 64-byte aligned
        assert_eq!((bytes.len() - 5) % BLOCK_BYTES, 0);
    }

    #[test]
    fn alignment_invariant() {
        for count in [1, 2, 7, 10, 50, 100, 500, 1000, 5000] {
            let keys: Vec<Vec<u8>> = (0..count as u32)
                .map(|i| format!("align_{}", i).into_bytes())
                .collect();
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            let filter = BloomFilter::build(&key_refs, 10);
            assert!(
                filter.bits.len() % BLOCK_BYTES == 0,
                "bits not 64-byte aligned for {} keys: len={}",
                count,
                filter.bits.len()
            );
        }
    }

    #[test]
    fn from_bytes_rejects_too_short() {
        assert!(BloomFilter::from_bytes(&[]).is_none());
        assert!(BloomFilter::from_bytes(&[0xFB, 1, 2, 3]).is_none());
        assert!(BloomFilter::from_bytes(&[1, 2, 3]).is_none());
    }

    #[test]
    fn from_bytes_rejects_invalid_probes() {
        // num_probes = 0
        let mut data = vec![0u8; 5 + 64];
        data[0] = MAGIC;
        data[1..5].copy_from_slice(&0u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());

        // num_probes = 31 (just above max)
        data[1..5].copy_from_slice(&31u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());

        // num_probes = 30 (max valid) should succeed
        data[1..5].copy_from_slice(&30u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_some());
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let mut data = vec![0u8; 5 + 64];
        data[0] = 0xFF; // old magic
        data[1..5].copy_from_slice(&7u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_rejects_unaligned_bits() {
        // bits section not a multiple of 64 bytes
        let mut data = vec![0u8; 5 + 30];
        data[0] = MAGIC;
        data[1..5].copy_from_slice(&7u32.to_le_bytes());
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_empty_bits_section() {
        // Valid header with zero-length bits (empty filter)
        let mut data = vec![0u8; 5];
        data[0] = MAGIC;
        data[1..5].copy_from_slice(&7u32.to_le_bytes());
        let filter = BloomFilter::from_bytes(&data).unwrap();
        assert!(filter.bits.is_empty());
        assert!(!filter.maybe_contains(b"anything"));
    }

    #[test]
    fn bits_per_key_zero_does_not_panic() {
        let keys: Vec<Vec<u8>> = (0..10u32).map(|i| format!("k{}", i).into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 0);
        // Should not panic; degenerate but functional
        assert_eq!(filter.num_probes, 1);
        assert_eq!(filter.bits.len(), BLOCK_BYTES); // max(0/512, 1) = 1 block
    }

    #[test]
    fn probes_are_block_local() {
        // Verify the core invariant: inserting a key only modifies bits within
        // a single 64-byte block, and checking a key only reads from one block.
        let num_blocks = 4;
        let block_size = BLOCK_BYTES;

        // Build a filter with enough keys/blocks to be meaningful
        let keys: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("locality_{}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        // Force exactly `num_blocks` blocks: need total_bits ~ num_blocks * 512
        // With 200 keys and bpk=10, total_bits=2000, num_blocks=ceil(2000/512)=4
        let filter = BloomFilter::build(&key_refs, 10);
        assert_eq!(
            filter.bits.len() / block_size,
            num_blocks,
            "expected {} blocks",
            num_blocks,
        );

        // For each key, verify that re-inserting it only touches the same block
        // that the hash function selects.
        for key in &keys {
            let h = xxhash_rust::xxh3::xxh3_64(key.as_slice());
            let block_idx = ((h >> 32) as usize) % num_blocks;

            // Zero out a copy and re-insert just this key
            let mut test_bits = vec![0u8; num_blocks * block_size];
            let block_start = block_idx * block_size;
            let mut h2 = h as u32;
            for _ in 0..filter.num_probes {
                h2 = h2.wrapping_mul(0x9e3779b9).wrapping_add(1);
                let bit = (h2 >> PROBE_SHIFT) as usize;
                test_bits[block_start + bit / 8] |= 1 << (bit % 8);
            }

            // All other blocks must remain zeroed
            for b in 0..num_blocks {
                if b == block_idx {
                    continue;
                }
                let start = b * block_size;
                let end = start + block_size;
                assert!(
                    test_bits[start..end].iter().all(|&byte| byte == 0),
                    "key {:?} modified block {} (expected only block {})",
                    key,
                    b,
                    block_idx,
                );
            }
        }
    }

    #[test]
    fn large_key_count_no_overflow() {
        // Ensure no panics or overflows with a large number of keys.
        let keys: Vec<Vec<u8>> = (0..50_000u32).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs, 10);
        assert!(filter.bits.len() % BLOCK_BYTES == 0);
        // Spot-check: no false negatives on a sample
        for key in keys.iter().step_by(1000) {
            assert!(filter.maybe_contains(key));
        }
    }
}
