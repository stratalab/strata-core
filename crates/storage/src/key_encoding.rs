//! InternalKey encoding for memtables and KV segments.
//!
//! Encodes `(Key, commit_id)` into a byte sequence whose natural byte
//! ordering matches the MVCC sort contract:
//!
//! ```text
//! InternalKey = TypedKeyBytes || EncodeDesc(commit_id)
//! ```
//!
//! - Keys sort by `(namespace, type_tag, user_key)` ascending.
//! - Within the same logical key, higher `commit_id` sorts first (descending).
//!
//! ## Encoding format
//!
//! ```text
//! branch_id        (16 bytes, fixed)
//! space            (null-terminated, 0x00 sentinel — space names cannot contain 0x00)
//! type_tag         (1 byte, fixed)
//! user_key         (byte-stuffed: 0x00 → 0x00 0x01, terminated by 0x00 0x00)
//! !commit_id       (8 bytes, big-endian bitwise-NOT for descending order)
//! ```
//!
//! Variable-length fields use order-preserving encodings (null-terminated /
//! byte-stuffed) instead of length prefixes, because varint length prefixes
//! break lexicographic byte ordering.

use std::sync::Arc;
use strata_core::id::CommitVersion;
use strata_core::types::{BranchId, Key, Namespace, TypeTag};

/// Size in bytes of the trailing `!commit_id` suffix in an `InternalKey`.
pub const COMMIT_ID_SUFFIX_LEN: usize = 8;

// ---------------------------------------------------------------------------
// Byte-stuffed encoding (order-preserving for arbitrary binary data)
// ---------------------------------------------------------------------------

/// Encode arbitrary bytes with byte-stuffing for order-preserving comparison.
///
/// Escaping: `0x00` → `0x00 0x01`. Terminator: `0x00 0x00`.
///
/// This preserves lexicographic ordering: for any byte strings A < B,
/// `encode(A) < encode(B)` in byte comparison. Shorter prefixes sort
/// before longer strings that extend them.
fn encode_escaped(src: &[u8], dst: &mut Vec<u8>) {
    for &b in src {
        if b == 0x00 {
            dst.push(0x00);
            dst.push(0x01);
        } else {
            dst.push(b);
        }
    }
    // Terminator
    dst.push(0x00);
    dst.push(0x00);
}

/// Decode byte-stuffed bytes. Returns `(decoded_bytes, bytes_consumed)`.
fn decode_escaped(src: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < src.len() {
        if src[i] == 0x00 {
            if i + 1 >= src.len() {
                return None;
            }
            if src[i + 1] == 0x00 {
                // Terminator
                return Some((result, i + 2));
            } else if src[i + 1] == 0x01 {
                // Escaped 0x00
                result.push(0x00);
                i += 2;
            } else {
                return None; // Invalid escape
            }
        } else {
            result.push(src[i]);
            i += 1;
        }
    }
    None // No terminator found
}

// ---------------------------------------------------------------------------
// TypedKeyBytes encoding
// ---------------------------------------------------------------------------

/// Encode a `Key` into its `TypedKeyBytes` representation.
///
/// The encoding preserves the lexicographic ordering of `Key`:
/// namespace (branch_id, then space) → type_tag → user_key.
pub fn encode_typed_key(key: &Key) -> Vec<u8> {
    assert!(
        !key.namespace.space.as_bytes().contains(&0x00),
        "encode_typed_key: space name must not contain NUL bytes (space={:?})",
        key.namespace.space,
    );
    let mut buf =
        Vec::with_capacity(16 + key.namespace.space.len() + 1 + 1 + key.user_key.len() + 2);
    // Branch ID: 16 bytes, big-endian UUID
    buf.extend_from_slice(key.namespace.branch_id.as_bytes());
    // Space: null-terminated (space names are validated identifiers, no 0x00)
    buf.extend_from_slice(key.namespace.space.as_bytes());
    buf.push(0x00);
    // TypeTag: 1 byte
    buf.push(key.type_tag.as_byte());
    // User key: byte-stuffed with terminator (supports arbitrary binary keys)
    encode_escaped(&key.user_key, &mut buf);
    buf
}

/// Encode a `Key` into a prefix-match byte sequence.
///
/// Like `encode_typed_key` but omits the user_key terminator (`0x00 0x00`),
/// so the result can be used with `starts_with()` for prefix matching.
///
/// For exact key matching, use `encode_typed_key` (includes terminator).
/// For prefix matching in scans, use this function.
pub fn encode_typed_key_prefix(key: &Key) -> Vec<u8> {
    let full = encode_typed_key(key);
    // Remove the trailing 0x00 0x00 terminator
    full[..full.len() - 2].to_vec()
}

// ---------------------------------------------------------------------------
// InternalKey
// ---------------------------------------------------------------------------

/// An encoded internal key: `TypedKeyBytes || EncodeDesc(commit_id)`.
///
/// Byte comparison on `InternalKey` produces the correct MVCC ordering:
/// - Keys are ordered by `(namespace, type_tag, user_key)` ascending
/// - Within the same logical key, higher commit_ids sort first (descending)
///
/// The descending commit_id encoding uses `u64_be(!commit_id)`.
#[derive(Clone, PartialEq, Eq)]
pub struct InternalKey(Vec<u8>);

impl InternalKey {
    /// Encode a `(Key, commit_id)` pair into an `InternalKey`.
    pub fn encode(key: &Key, commit_id: CommitVersion) -> Self {
        let mut buf = encode_typed_key(key);
        // Append commit_id in descending order
        buf.extend_from_slice(&(!commit_id.as_u64()).to_be_bytes());
        InternalKey(buf)
    }

    /// Build an `InternalKey` from pre-encoded typed key bytes and a commit_id.
    ///
    /// Avoids re-encoding the key when the typed key bytes are already available.
    pub fn from_typed_key_bytes(typed_key: &[u8], commit_id: CommitVersion) -> Self {
        let mut buf = Vec::with_capacity(typed_key.len() + 8);
        buf.extend_from_slice(typed_key);
        buf.extend_from_slice(&(!commit_id.as_u64()).to_be_bytes());
        InternalKey(buf)
    }

    /// Create an InternalKey from raw bytes (e.g., read from a segment file).
    ///
    /// # Panics
    /// Panics if `bytes.len() < 28` (minimum: 16 branch + 1 space NUL + 1 tag +
    /// 2 user_key terminator + 8 commit_id).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        assert!(
            bytes.len() >= 28,
            "InternalKey too short: {} bytes (minimum 28)",
            bytes.len()
        );
        InternalKey(bytes)
    }

    /// Create an InternalKey from raw bytes, returning `None` if too short.
    ///
    /// Unlike [`from_bytes`](Self::from_bytes), this never panics on corrupt
    /// or truncated data — callers can propagate the `None` gracefully.
    pub fn try_from_bytes(bytes: Vec<u8>) -> Option<Self> {
        if bytes.len() < 28 {
            return None;
        }
        Some(InternalKey(bytes))
    }

    /// Access the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume and return the raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Extract the TypedKeyBytes portion (everything except trailing commit_id suffix).
    pub fn typed_key_prefix(&self) -> &[u8] {
        assert!(
            self.0.len() >= COMMIT_ID_SUFFIX_LEN,
            "InternalKey too short for typed_key_prefix: {} bytes (need ≥{})",
            self.0.len(),
            COMMIT_ID_SUFFIX_LEN,
        );
        &self.0[..self.0.len() - COMMIT_ID_SUFFIX_LEN]
    }

    /// Extract the TypeTag byte from the encoded key.
    ///
    /// Layout: `branch_id (16) | space (null-terminated) | type_tag (1) | ...`
    /// Finds the first NUL after offset 16 (end of space) and returns the
    /// byte immediately after it.
    pub fn type_tag_byte(&self) -> u8 {
        // Skip 16-byte branch_id, then find the NUL that terminates the space name.
        let after_branch = &self.0[16..];
        let nul_pos = after_branch
            .iter()
            .position(|&b| b == 0x00)
            .expect("InternalKey missing space NUL terminator");
        after_branch[nul_pos + 1]
    }

    /// Extract the commit_id from the trailing 8 bytes.
    pub fn commit_id(&self) -> u64 {
        let len = self.0.len();
        let bytes: [u8; 8] = self.0[len - 8..].try_into().unwrap();
        !u64::from_be_bytes(bytes)
    }

    /// Decode the full `(Key, commit_id)` pair.
    pub fn decode(&self) -> Option<(Key, u64)> {
        let buf = &self.0;
        if buf.len() < 16 + 1 + 1 + 2 + COMMIT_ID_SUFFIX_LEN {
            // minimum: 16 (branch) + 1 (space NUL) + 1 (tag) + 2 (user_key terminator) + commit_id
            return None;
        }

        let commit_id_start = buf.len() - COMMIT_ID_SUFFIX_LEN;
        let commit_id_bytes: [u8; COMMIT_ID_SUFFIX_LEN] = buf[commit_id_start..].try_into().ok()?;
        let commit_id = !u64::from_be_bytes(commit_id_bytes);

        let typed = &buf[..commit_id_start];
        let mut pos = 0;

        // Branch ID: 16 bytes
        let branch_bytes: [u8; 16] = typed[..16].try_into().ok()?;
        let branch_id = strata_core::types::BranchId::from_bytes(branch_bytes);
        pos += 16;

        // Space: null-terminated
        let nul_pos = typed[pos..].iter().position(|&b| b == 0x00)?;
        let space = std::str::from_utf8(&typed[pos..pos + nul_pos]).ok()?;
        pos += nul_pos + 1; // skip the NUL

        // TypeTag: 1 byte
        let type_tag = TypeTag::from_byte(typed[pos])?;
        pos += 1;

        // User key: byte-stuffed with terminator
        let (user_key, _consumed) = decode_escaped(&typed[pos..])?;

        let namespace = Arc::new(Namespace::new(branch_id, space.to_string()));
        let key = Key::new(namespace, type_tag, user_key);
        Some((key, commit_id))
    }

    /// Create a new InternalKey with the branch_id (first 16 bytes) replaced.
    ///
    /// Used by COW branching to rewrite keys from a source branch's namespace
    /// into the child branch's namespace, so that `MvccIterator` can group
    /// own + inherited entries by the same `typed_key_prefix`.
    pub fn with_rewritten_branch_id(&self, new_branch_id: &BranchId) -> Self {
        assert!(
            self.0.len() >= 16,
            "InternalKey too short for branch rewrite: {} bytes (need ≥16)",
            self.0.len()
        );
        let mut bytes = self.0.clone();
        bytes[..16].copy_from_slice(new_branch_id.as_bytes());
        InternalKey(bytes)
    }
}

/// Rewrite the branch_id (first 16 bytes) of an encoded typed key.
///
/// Used by COW branching point lookups to rewrite a child's `typed_key`
/// into the source branch's namespace for bloom probes and index searches.
///
/// Returns `None` if `encoded` is shorter than 16 bytes (e.g., corrupt
/// segment data), instead of panicking.
pub fn rewrite_branch_id_bytes(encoded: &[u8], new_branch_id: &BranchId) -> Option<Vec<u8>> {
    if encoded.len() < 16 {
        return None;
    }
    let mut rewritten = encoded.to_vec();
    rewritten[..16].copy_from_slice(new_branch_id.as_bytes());
    Some(rewritten)
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some((key, commit_id)) = self.decode() {
            write!(
                f,
                "InternalKey({:?}/{}/{:?} @{})",
                key.namespace.branch_id,
                key.namespace.space,
                String::from_utf8_lossy(&key.user_key),
                commit_id,
            )
        } else {
            write!(f, "InternalKey({} bytes)", self.0.len())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::id::CommitVersion;
    use strata_core::types::BranchId;

    fn make_key(space: &str, type_tag: TypeTag, user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(BranchId::new(), space.to_string()));
        Key::new(ns, type_tag, user_key.as_bytes().to_vec())
    }

    fn make_key_with_branch(
        branch: BranchId,
        space: &str,
        type_tag: TypeTag,
        user_key: &str,
    ) -> Key {
        let ns = Arc::new(Namespace::new(branch, space.to_string()));
        Key::new(ns, type_tag, user_key.as_bytes().to_vec())
    }

    // ===== Byte-stuffing tests =====

    #[test]
    fn escaped_roundtrip_empty() {
        let mut buf = Vec::new();
        encode_escaped(b"", &mut buf);
        assert_eq!(buf, vec![0x00, 0x00]); // just terminator
        let (decoded, consumed) = decode_escaped(&buf).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn escaped_roundtrip_no_nulls() {
        let mut buf = Vec::new();
        encode_escaped(b"hello", &mut buf);
        let (decoded, _) = decode_escaped(&buf).unwrap();
        assert_eq!(decoded, b"hello");
    }

    #[test]
    fn escaped_roundtrip_with_nulls() {
        let input = vec![0x00, 0x42, 0x00, 0x00, 0xFF];
        let mut buf = Vec::new();
        encode_escaped(&input, &mut buf);
        let (decoded, _) = decode_escaped(&buf).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn escaped_ordering_prefix_sorts_first() {
        // "abc" should sort before "abcd"
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_escaped(b"abc", &mut buf1);
        encode_escaped(b"abcd", &mut buf2);
        assert!(buf1 < buf2);
    }

    #[test]
    fn escaped_ordering_with_null_bytes() {
        // b"\x00" should sort before b"\x01"
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_escaped(&[0x00], &mut buf1);
        encode_escaped(&[0x01], &mut buf2);
        assert!(buf1 < buf2);
    }

    #[test]
    fn escaped_ordering_empty_before_nonempty() {
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_escaped(b"", &mut buf1);
        encode_escaped(b"a", &mut buf2);
        assert!(buf1 < buf2);
    }

    // ===== InternalKey encode/decode roundtrip =====

    #[test]
    fn encode_decode_roundtrip() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik = InternalKey::encode(&key, CommitVersion(42));
        let (decoded_key, decoded_commit) = ik.decode().unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_commit, 42);
    }

    #[test]
    fn encode_decode_roundtrip_max_commit() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik = InternalKey::encode(&key, CommitVersion::MAX);
        let (decoded_key, decoded_commit) = ik.decode().unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_commit, u64::MAX);
    }

    #[test]
    fn encode_decode_roundtrip_zero_commit() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik = InternalKey::encode(&key, CommitVersion(0));
        let (decoded_key, decoded_commit) = ik.decode().unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_commit, 0);
    }

    #[test]
    fn encode_decode_roundtrip_empty_user_key() {
        let key = make_key("default", TypeTag::KV, "");
        let ik = InternalKey::encode(&key, CommitVersion(5));
        let (decoded_key, decoded_commit) = ik.decode().unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_commit, 5);
    }

    #[test]
    fn encode_decode_roundtrip_binary_user_key() {
        let ns = Arc::new(Namespace::new(BranchId::new(), "default".to_string()));
        let key = Key::new(ns, TypeTag::Vector, vec![0x00, 0xFF, 0x80, 0x01]);
        let ik = InternalKey::encode(&key, CommitVersion(99));
        let (decoded_key, decoded_commit) = ik.decode().unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_commit, 99);
    }

    #[test]
    fn encode_decode_all_type_tags() {
        for tag in [
            TypeTag::KV,
            TypeTag::Event,
            TypeTag::Branch,
            TypeTag::Space,
            TypeTag::Vector,
            TypeTag::Json,
            TypeTag::Graph,
        ] {
            let key = make_key("test", tag, "k");
            let ik = InternalKey::encode(&key, CommitVersion(1));
            let (decoded_key, _) = ik.decode().unwrap();
            assert_eq!(decoded_key.type_tag, tag);
        }
    }

    // ===== Ordering invariants =====

    #[test]
    fn same_key_higher_commit_sorts_first() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik_high = InternalKey::encode(&key, CommitVersion(100));
        let ik_low = InternalKey::encode(&key, CommitVersion(1));
        assert!(
            ik_high < ik_low,
            "commit_id=100 should sort before commit_id=1 (descending)"
        );
    }

    #[test]
    fn max_commit_id_sorts_first() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik_max = InternalKey::encode(&key, CommitVersion::MAX);
        let ik_zero = InternalKey::encode(&key, CommitVersion(0));
        assert!(ik_max < ik_zero);
    }

    #[test]
    fn different_keys_sort_by_user_key() {
        let branch = BranchId::new();
        let k_a = make_key_with_branch(branch, "default", TypeTag::KV, "aaa");
        let k_b = make_key_with_branch(branch, "default", TypeTag::KV, "bbb");
        let ik_a = InternalKey::encode(&k_a, CommitVersion(1));
        let ik_b = InternalKey::encode(&k_b, CommitVersion(1));
        assert!(ik_a < ik_b, "key 'aaa' should sort before 'bbb'");
    }

    #[test]
    fn different_type_tags_sort_correctly() {
        let branch = BranchId::new();
        let k_kv = make_key_with_branch(branch, "default", TypeTag::KV, "k");
        let k_event = make_key_with_branch(branch, "default", TypeTag::Event, "k");
        let k_vector = make_key_with_branch(branch, "default", TypeTag::Vector, "k");
        let ik_kv = InternalKey::encode(&k_kv, CommitVersion(1));
        let ik_event = InternalKey::encode(&k_event, CommitVersion(1));
        let ik_vector = InternalKey::encode(&k_vector, CommitVersion(1));
        assert!(ik_kv < ik_event);
        assert!(ik_event < ik_vector);
    }

    #[test]
    fn different_spaces_sort_correctly() {
        let branch = BranchId::new();
        let k_a = make_key_with_branch(branch, "alpha", TypeTag::KV, "k");
        let k_b = make_key_with_branch(branch, "beta", TypeTag::KV, "k");
        let ik_a = InternalKey::encode(&k_a, CommitVersion(1));
        let ik_b = InternalKey::encode(&k_b, CommitVersion(1));
        assert!(ik_a < ik_b, "space 'alpha' should sort before 'beta'");
    }

    #[test]
    fn typed_key_prefix_extraction() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik = InternalKey::encode(&key, CommitVersion(42));
        let prefix = ik.typed_key_prefix();
        let expected = encode_typed_key(&key);
        assert_eq!(prefix, &expected[..]);
    }

    #[test]
    fn commit_id_extraction() {
        let key = make_key("default", TypeTag::KV, "hello");
        for commit_id in [0u64, 1, 42, 1000, u64::MAX / 2, u64::MAX] {
            let ik = InternalKey::encode(&key, CommitVersion(commit_id));
            assert_eq!(ik.commit_id(), commit_id);
        }
    }

    #[test]
    fn prefix_key_sorts_before_extended_key() {
        let branch = BranchId::new();
        let k_prefix = make_key_with_branch(branch, "default", TypeTag::KV, "user:");
        let k_full = make_key_with_branch(branch, "default", TypeTag::KV, "user:alice");
        let ik_prefix = InternalKey::encode(&k_prefix, CommitVersion(1));
        let ik_full = InternalKey::encode(&k_full, CommitVersion(1));
        assert!(
            ik_prefix < ik_full,
            "prefix 'user:' should sort before 'user:alice'"
        );
    }

    #[test]
    fn empty_user_key_sorts_first() {
        let branch = BranchId::new();
        let k_empty = make_key_with_branch(branch, "default", TypeTag::KV, "");
        let k_a = make_key_with_branch(branch, "default", TypeTag::KV, "a");
        let ik_empty = InternalKey::encode(&k_empty, CommitVersion(1));
        let ik_a = InternalKey::encode(&k_a, CommitVersion(1));
        assert!(ik_empty < ik_a);
    }

    #[test]
    fn typed_key_prefix_is_proper_prefix_of_typed_key() {
        let key = make_key("default", TypeTag::KV, "hello");
        let full = encode_typed_key(&key);
        let prefix = encode_typed_key_prefix(&key);
        // prefix should be full minus the 2-byte terminator
        assert_eq!(prefix.len(), full.len() - 2);
        assert!(full.starts_with(&prefix));
        // the removed bytes should be the terminator
        assert_eq!(&full[full.len() - 2..], &[0x00, 0x00]);
    }

    #[test]
    fn typed_key_prefix_enables_starts_with_matching() {
        let branch = BranchId::new();
        let prefix_key = make_key_with_branch(branch, "default", TypeTag::KV, "user:");
        let match_key = make_key_with_branch(branch, "default", TypeTag::KV, "user:alice");
        let non_match = make_key_with_branch(branch, "default", TypeTag::KV, "order:1");

        let prefix_bytes = encode_typed_key_prefix(&prefix_key);
        let match_typed = encode_typed_key(&match_key);
        let non_match_typed = encode_typed_key(&non_match);

        assert!(match_typed.starts_with(&prefix_bytes));
        assert!(!non_match_typed.starts_with(&prefix_bytes));
    }

    #[test]
    fn from_bytes_rejects_short_input() {
        let result = std::panic::catch_unwind(|| InternalKey::from_bytes(vec![1, 2, 3]));
        assert!(result.is_err());
    }

    #[test]
    fn try_from_bytes_rejects_short_input() {
        assert!(InternalKey::try_from_bytes(vec![1, 2, 3]).is_none());
    }

    #[test]
    fn try_from_bytes_accepts_valid_input() {
        let key = make_key("default", TypeTag::KV, "hello");
        let ik = InternalKey::encode(&key, CommitVersion(42));
        let bytes = ik.into_bytes();
        let recovered = InternalKey::try_from_bytes(bytes).unwrap();
        assert_eq!(recovered.commit_id(), 42);
    }

    #[test]
    fn user_key_with_null_bytes_sorts_correctly() {
        let branch = BranchId::new();
        let ns = Arc::new(Namespace::new(branch, "default".to_string()));
        let k_null = Key::new(ns.clone(), TypeTag::KV, vec![0x00]);
        let k_one = Key::new(ns, TypeTag::KV, vec![0x01]);
        let ik_null = InternalKey::encode(&k_null, CommitVersion(1));
        let ik_one = InternalKey::encode(&k_one, CommitVersion(1));
        assert!(ik_null < ik_one, "\\x00 should sort before \\x01");
    }

    // ===== Issue #1685: Malformed input handling =====

    #[test]
    fn test_issue_1685_rewrite_branch_id_returns_none_on_short_input() {
        // rewrite_branch_id_bytes must return None for input shorter than 16 bytes,
        // not panic. Corrupt segment data could produce short encoded keys.
        let branch = BranchId::new();
        assert!(rewrite_branch_id_bytes(&[1, 2, 3], &branch).is_none());
        assert!(rewrite_branch_id_bytes(&[], &branch).is_none());
        assert!(rewrite_branch_id_bytes(&[0u8; 15], &branch).is_none());
        // Exactly 16 bytes should succeed
        assert!(rewrite_branch_id_bytes(&[0u8; 16], &branch).is_some());
        // Normal-length input should succeed
        assert!(rewrite_branch_id_bytes(&[0u8; 28], &branch).is_some());
    }

    #[test]
    #[should_panic(expected = "NUL")]
    fn test_issue_1685_encode_typed_key_rejects_nul_in_space() {
        // Space names containing NUL bytes break the encoding format because
        // NUL is the space field terminator. encode_typed_key must reject them.
        let ns = Arc::new(Namespace {
            branch_id: BranchId::new(),
            space: "bad\x00space".to_string(),
        });
        let key = Key::new(ns, TypeTag::KV, b"test".to_vec());
        encode_typed_key(&key);
    }

    // ===== Property tests =====

    #[cfg(not(miri))]
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        fn arb_type_tag() -> impl Strategy<Value = TypeTag> {
            prop_oneof![
                Just(TypeTag::KV),
                Just(TypeTag::Event),
                Just(TypeTag::Branch),
                Just(TypeTag::Space),
                Just(TypeTag::Vector),
                Just(TypeTag::Json),
                Just(TypeTag::Graph),
            ]
        }

        proptest! {
            #[test]
            fn prop_encode_decode_roundtrip(
                space in "[a-z]{1,20}",
                tag in arb_type_tag(),
                user_key in prop::collection::vec(any::<u8>(), 0..100),
                commit_id: u64,
            ) {
                let ns = Arc::new(Namespace::new(BranchId::new(), space));
                let key = Key::new(ns, tag, user_key);
                let ik = InternalKey::encode(&key, CommitVersion(commit_id));
                let (decoded_key, decoded_commit) = ik.decode().unwrap();
                prop_assert_eq!(&decoded_key, &key);
                prop_assert_eq!(decoded_commit, commit_id);
            }

            #[test]
            fn prop_commit_id_descending_within_same_key(
                space in "[a-z]{1,10}",
                user_key in "[a-z]{0,20}",
                c1: u64,
                c2: u64,
            ) {
                let branch = BranchId::new();
                let key = make_key_with_branch(branch, &space, TypeTag::KV, &user_key);
                let ik1 = InternalKey::encode(&key, CommitVersion(c1));
                let ik2 = InternalKey::encode(&key, CommitVersion(c2));
                if c1 > c2 {
                    prop_assert!(ik1 < ik2, "higher commit_id must sort first");
                } else if c1 < c2 {
                    prop_assert!(ik1 > ik2, "lower commit_id must sort after");
                } else {
                    prop_assert_eq!(ik1, ik2);
                }
            }

            #[test]
            fn prop_typed_key_preserves_key_order(
                user_key1 in prop::collection::vec(any::<u8>(), 0..50),
                user_key2 in prop::collection::vec(any::<u8>(), 0..50),
            ) {
                let branch = BranchId::new();
                let ns = Arc::new(Namespace::new(branch, "default".to_string()));
                let k1 = Key::new(ns.clone(), TypeTag::KV, user_key1.clone());
                let k2 = Key::new(ns, TypeTag::KV, user_key2.clone());
                let ik1 = InternalKey::encode(&k1, CommitVersion(1));
                let ik2 = InternalKey::encode(&k2, CommitVersion(1));
                let key_order = user_key1.cmp(&user_key2);
                let ik_order = ik1.cmp(&ik2);
                prop_assert_eq!(key_order, ik_order,
                    "InternalKey ordering must match user_key ordering");
            }

            #[test]
            fn prop_escaped_roundtrip(data in prop::collection::vec(any::<u8>(), 0..100)) {
                let mut buf = Vec::new();
                encode_escaped(&data, &mut buf);
                let (decoded, consumed) = decode_escaped(&buf).unwrap();
                prop_assert_eq!(&decoded, &data);
                prop_assert_eq!(consumed, buf.len());
            }

            #[test]
            fn prop_escaped_preserves_order(
                a in prop::collection::vec(any::<u8>(), 0..50),
                b in prop::collection::vec(any::<u8>(), 0..50),
            ) {
                let mut buf_a = Vec::new();
                let mut buf_b = Vec::new();
                encode_escaped(&a, &mut buf_a);
                encode_escaped(&b, &mut buf_b);
                prop_assert_eq!(a.cmp(&b), buf_a.cmp(&buf_b),
                    "escaped encoding must preserve byte ordering");
            }
        }
    }
}
