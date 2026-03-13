//! Tombstone tracking for deleted/compacted entries
//!
//! Tombstones mark entries that have been deleted or removed by compaction.
//! They serve two purposes:
//!
//! 1. **Read filtering**: During reads, tombstoned versions are skipped
//! 2. **Audit trail**: Tombstones record why and when entries were removed
//!
//! # Storage
//!
//! Tombstones are stored in the system namespace and are included in
//! snapshots for persistence across restarts.
//!
//! # Cleanup
//!
//! Tombstones can be cleaned up after a configurable retention period
//! using the `cleanup_before` method.

use std::collections::HashMap;

/// Tombstone for a deleted/compacted entry
///
/// Records metadata about when and why an entry was removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// Run ID this tombstone belongs to
    pub branch_id: [u8; 16],

    /// Primitive type (0=KV, 1=Event, etc.)
    pub primitive_type: u8,

    /// Key or identifier for the entry
    pub key: Vec<u8>,

    /// Version that was tombstoned
    pub version: u64,

    /// Timestamp when tombstone was created (microseconds since epoch)
    pub created_at: u64,

    /// Reason for tombstone creation
    pub reason: TombstoneReason,
}

/// Reason for tombstone creation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TombstoneReason {
    /// User explicitly deleted the entry
    UserDelete,

    /// Retention policy removed the version
    RetentionPolicy,

    /// Compaction removed the version
    Compaction,
}

impl TombstoneReason {
    /// Convert to byte representation
    pub fn to_byte(&self) -> u8 {
        match self {
            TombstoneReason::UserDelete => 0x01,
            TombstoneReason::RetentionPolicy => 0x02,
            TombstoneReason::Compaction => 0x03,
        }
    }

    /// Parse from byte representation
    pub fn from_byte(byte: u8) -> Result<Self, TombstoneError> {
        match byte {
            0x01 => Ok(TombstoneReason::UserDelete),
            0x02 => Ok(TombstoneReason::RetentionPolicy),
            0x03 => Ok(TombstoneReason::Compaction),
            _ => Err(TombstoneError::InvalidReason(byte)),
        }
    }

    /// Get human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            TombstoneReason::UserDelete => "user_delete",
            TombstoneReason::RetentionPolicy => "retention_policy",
            TombstoneReason::Compaction => "compaction",
        }
    }
}

impl std::fmt::Display for TombstoneReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Tombstone {
    /// Create a new tombstone
    pub fn new(
        branch_id: [u8; 16],
        primitive_type: u8,
        key: Vec<u8>,
        version: u64,
        reason: TombstoneReason,
    ) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Tombstone {
            branch_id,
            primitive_type,
            key,
            version,
            created_at,
            reason,
        }
    }

    /// Create a tombstone with explicit timestamp
    pub fn with_timestamp(
        branch_id: [u8; 16],
        primitive_type: u8,
        key: Vec<u8>,
        version: u64,
        reason: TombstoneReason,
        created_at: u64,
    ) -> Self {
        Tombstone {
            branch_id,
            primitive_type,
            key,
            version,
            created_at,
            reason,
        }
    }

    /// Serialize tombstone to bytes
    ///
    /// Format:
    /// - branch_id: 16 bytes
    /// - primitive_type: 1 byte
    /// - key_len: 4 bytes (u32 LE)
    /// - key: variable
    /// - version: 8 bytes (u64 LE)
    /// - created_at: 8 bytes (u64 LE)
    /// - reason: 1 byte
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(38 + self.key.len());

        // Run ID
        bytes.extend_from_slice(&self.branch_id);

        // Primitive type
        bytes.push(self.primitive_type);

        // Key (length-prefixed)
        bytes.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.key);

        // Version
        bytes.extend_from_slice(&self.version.to_le_bytes());

        // Created at
        bytes.extend_from_slice(&self.created_at.to_le_bytes());

        // Reason
        bytes.push(self.reason.to_byte());

        bytes
    }

    /// Deserialize tombstone from bytes
    ///
    /// Returns (tombstone, bytes_consumed) on success.
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize), TombstoneError> {
        // Minimum size: 16 + 1 + 4 + 0 + 8 + 8 + 1 = 38 bytes
        if bytes.len() < 38 {
            return Err(TombstoneError::InsufficientData);
        }

        let mut cursor = 0;

        // Run ID
        let branch_id: [u8; 16] = bytes[cursor..cursor + 16]
            .try_into()
            .map_err(|_| TombstoneError::InvalidFormat)?;
        cursor += 16;

        // Primitive type
        let primitive_type = bytes[cursor];
        cursor += 1;

        // Key length
        let key_len = u32::from_le_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .map_err(|_| TombstoneError::InvalidFormat)?,
        ) as usize;
        cursor += 4;

        if bytes.len() < cursor + key_len + 17 {
            return Err(TombstoneError::InsufficientData);
        }

        // Key
        let key = bytes[cursor..cursor + key_len].to_vec();
        cursor += key_len;

        // Version
        let version = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| TombstoneError::InvalidFormat)?,
        );
        cursor += 8;

        // Created at
        let created_at = u64::from_le_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .map_err(|_| TombstoneError::InvalidFormat)?,
        );
        cursor += 8;

        // Reason
        let reason = TombstoneReason::from_byte(bytes[cursor])?;
        cursor += 1;

        let tombstone = Tombstone {
            branch_id,
            primitive_type,
            key,
            version,
            created_at,
            reason,
        };

        Ok((tombstone, cursor))
    }
}

/// Key for tombstone lookup
///
/// Combines branch_id, primitive_type, and key to form a unique identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TombstoneKey {
    branch_id: [u8; 16],
    primitive_type: u8,
    key: Vec<u8>,
}

impl TombstoneKey {
    fn new(branch_id: [u8; 16], primitive_type: u8, key: Vec<u8>) -> Self {
        TombstoneKey {
            branch_id,
            primitive_type,
            key,
        }
    }

    fn from_tombstone(tombstone: &Tombstone) -> Self {
        TombstoneKey {
            branch_id: tombstone.branch_id,
            primitive_type: tombstone.primitive_type,
            key: tombstone.key.clone(),
        }
    }
}

/// Tombstone index for tracking deletions
///
/// Provides efficient lookup to check if a specific version is tombstoned.
#[derive(Debug, Default)]
pub struct TombstoneIndex {
    /// Map from (branch_id, primitive_type, key) -> list of tombstones
    tombstones: HashMap<TombstoneKey, Vec<Tombstone>>,

    /// Total count of tombstones
    count: usize,
}

impl TombstoneIndex {
    /// Create a new empty tombstone index
    pub fn new() -> Self {
        TombstoneIndex {
            tombstones: HashMap::new(),
            count: 0,
        }
    }

    /// Add a tombstone to the index
    pub fn add(&mut self, tombstone: Tombstone) {
        let key = TombstoneKey::from_tombstone(&tombstone);

        self.tombstones.entry(key).or_default().push(tombstone);

        self.count += 1;
    }

    /// Check if a specific version is tombstoned
    pub fn is_tombstoned(
        &self,
        branch_id: &[u8; 16],
        primitive_type: u8,
        key: &[u8],
        version: u64,
    ) -> bool {
        let lookup_key = TombstoneKey::new(*branch_id, primitive_type, key.to_vec());

        self.tombstones
            .get(&lookup_key)
            .is_some_and(|ts| ts.iter().any(|t| t.version == version))
    }

    /// Get all tombstones for a specific entry
    pub fn get(
        &self,
        branch_id: &[u8; 16],
        primitive_type: u8,
        key: &[u8],
    ) -> Option<&[Tombstone]> {
        let lookup_key = TombstoneKey::new(*branch_id, primitive_type, key.to_vec());
        self.tombstones.get(&lookup_key).map(|v| v.as_slice())
    }

    /// Get all tombstones for a branch
    #[cfg(test)]
    pub fn get_by_branch(&self, branch_id: &[u8; 16]) -> Vec<&Tombstone> {
        self.tombstones
            .iter()
            .filter(|(k, _)| &k.branch_id == branch_id)
            .flat_map(|(_, v)| v.iter())
            .collect()
    }

    /// Get tombstones by reason
    #[cfg(test)]
    pub fn get_by_reason(&self, reason: TombstoneReason) -> Vec<&Tombstone> {
        self.tombstones
            .values()
            .flat_map(|v| v.iter())
            .filter(|t| t.reason == reason)
            .collect()
    }

    /// Remove tombstones created before the given cutoff time
    ///
    /// Returns the number of tombstones removed.
    pub fn cleanup_before(&mut self, cutoff: u64) -> usize {
        let mut removed = 0;

        for tombstones in self.tombstones.values_mut() {
            let before = tombstones.len();
            tombstones.retain(|t| t.created_at >= cutoff);
            removed += before - tombstones.len();
        }

        // Remove empty entries
        self.tombstones.retain(|_, v| !v.is_empty());

        self.count -= removed;
        removed
    }

    /// Get total number of tombstones
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Clear all tombstones
    pub fn clear(&mut self) {
        self.tombstones.clear();
        self.count = 0;
    }

    /// Serialize all tombstones to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Count (u32)
        bytes.extend_from_slice(&(self.count as u32).to_le_bytes());

        // All tombstones
        for tombstones in self.tombstones.values() {
            for tombstone in tombstones {
                let ts_bytes = tombstone.to_bytes();
                bytes.extend_from_slice(&(ts_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&ts_bytes);
            }
        }

        bytes
    }

    /// Deserialize tombstones from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TombstoneError> {
        if bytes.len() < 4 {
            return Err(TombstoneError::InsufficientData);
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut cursor = 4;
        let mut index = TombstoneIndex::new();

        for _ in 0..count {
            if cursor + 4 > bytes.len() {
                return Err(TombstoneError::InsufficientData);
            }

            let ts_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            if cursor + ts_len > bytes.len() {
                return Err(TombstoneError::InsufficientData);
            }

            let (tombstone, _) = Tombstone::from_bytes(&bytes[cursor..cursor + ts_len])?;
            index.add(tombstone);
            cursor += ts_len;
        }

        Ok(index)
    }

    /// Iterate over all tombstones
    pub fn iter(&self) -> impl Iterator<Item = &Tombstone> {
        self.tombstones.values().flat_map(|v| v.iter())
    }
}

/// Tombstone errors
#[derive(Debug, thiserror::Error)]
pub enum TombstoneError {
    /// Not enough data to parse tombstone
    #[error("Insufficient data to parse tombstone")]
    InsufficientData,

    /// Invalid tombstone format
    #[error("Invalid tombstone format")]
    InvalidFormat,

    /// Invalid reason byte
    #[error("Invalid tombstone reason: {0}")]
    InvalidReason(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_branch_id() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }

    #[test]
    fn test_tombstone_reason_roundtrip() {
        let reasons = [
            TombstoneReason::UserDelete,
            TombstoneReason::RetentionPolicy,
            TombstoneReason::Compaction,
        ];

        for reason in reasons {
            let byte = reason.to_byte();
            let parsed = TombstoneReason::from_byte(byte).unwrap();
            assert_eq!(parsed, reason);
        }
    }

    #[test]
    fn test_tombstone_reason_invalid() {
        let result = TombstoneReason::from_byte(0xFF);
        assert!(matches!(result, Err(TombstoneError::InvalidReason(0xFF))));
    }

    #[test]
    fn test_tombstone_reason_name() {
        assert_eq!(TombstoneReason::UserDelete.name(), "user_delete");
        assert_eq!(TombstoneReason::RetentionPolicy.name(), "retention_policy");
        assert_eq!(TombstoneReason::Compaction.name(), "compaction");
    }

    #[test]
    fn test_tombstone_new() {
        let ts = Tombstone::new(
            test_branch_id(),
            0,
            b"test-key".to_vec(),
            42,
            TombstoneReason::UserDelete,
        );

        assert_eq!(ts.branch_id, test_branch_id());
        assert_eq!(ts.primitive_type, 0);
        assert_eq!(ts.key, b"test-key");
        assert_eq!(ts.version, 42);
        assert!(ts.created_at > 0);
        assert_eq!(ts.reason, TombstoneReason::UserDelete);
    }

    #[test]
    fn test_tombstone_with_timestamp() {
        let ts = Tombstone::with_timestamp(
            test_branch_id(),
            1,
            b"key".to_vec(),
            100,
            TombstoneReason::Compaction,
            123456789,
        );

        assert_eq!(ts.created_at, 123456789);
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let ts = Tombstone::with_timestamp(
            test_branch_id(),
            2,
            b"my-key".to_vec(),
            42,
            TombstoneReason::RetentionPolicy,
            1000000,
        );

        let bytes = ts.to_bytes();
        let (parsed, consumed) = Tombstone::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.branch_id, ts.branch_id);
        assert_eq!(parsed.primitive_type, ts.primitive_type);
        assert_eq!(parsed.key, ts.key);
        assert_eq!(parsed.version, ts.version);
        assert_eq!(parsed.created_at, ts.created_at);
        assert_eq!(parsed.reason, ts.reason);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_tombstone_empty_key() {
        let ts = Tombstone::with_timestamp(
            test_branch_id(),
            0,
            Vec::new(),
            1,
            TombstoneReason::UserDelete,
            0,
        );

        let bytes = ts.to_bytes();
        let (parsed, _) = Tombstone::from_bytes(&bytes).unwrap();

        assert!(parsed.key.is_empty());
    }

    #[test]
    fn test_tombstone_insufficient_data() {
        let result = Tombstone::from_bytes(&[0u8; 10]);
        assert!(matches!(result, Err(TombstoneError::InsufficientData)));
    }

    #[test]
    fn test_tombstone_index_add_and_check() {
        let mut index = TombstoneIndex::new();

        let ts = Tombstone::new(
            test_branch_id(),
            0,
            b"key1".to_vec(),
            1,
            TombstoneReason::UserDelete,
        );

        index.add(ts);

        assert!(index.is_tombstoned(&test_branch_id(), 0, b"key1", 1));
        assert!(!index.is_tombstoned(&test_branch_id(), 0, b"key1", 2));
        assert!(!index.is_tombstoned(&test_branch_id(), 0, b"key2", 1));
        assert!(!index.is_tombstoned(&test_branch_id(), 1, b"key1", 1));
    }

    #[test]
    fn test_tombstone_index_multiple_versions() {
        let mut index = TombstoneIndex::new();

        for version in 1..=5 {
            let ts = Tombstone::new(
                test_branch_id(),
                0,
                b"key".to_vec(),
                version,
                TombstoneReason::Compaction,
            );
            index.add(ts);
        }

        assert_eq!(index.len(), 5);

        for version in 1..=5 {
            assert!(index.is_tombstoned(&test_branch_id(), 0, b"key", version));
        }
        assert!(!index.is_tombstoned(&test_branch_id(), 0, b"key", 6));
    }

    #[test]
    fn test_tombstone_index_get() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"key".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));
        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"key".to_vec(),
            2,
            TombstoneReason::Compaction,
        ));

        let tombstones = index.get(&test_branch_id(), 0, b"key").unwrap();
        assert_eq!(tombstones.len(), 2);
    }

    #[test]
    fn test_tombstone_index_get_by_branch() {
        let mut index = TombstoneIndex::new();
        let branch1 = test_branch_id();
        let mut branch2 = test_branch_id();
        branch2[0] = 99;

        index.add(Tombstone::new(
            branch1,
            0,
            b"key1".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));
        index.add(Tombstone::new(
            branch1,
            0,
            b"key2".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));
        index.add(Tombstone::new(
            branch2,
            0,
            b"key1".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));

        assert_eq!(index.get_by_branch(&branch1).len(), 2);
        assert_eq!(index.get_by_branch(&branch2).len(), 1);
    }

    #[test]
    fn test_tombstone_index_get_by_reason() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k1".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));
        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k2".to_vec(),
            1,
            TombstoneReason::Compaction,
        ));
        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k3".to_vec(),
            1,
            TombstoneReason::Compaction,
        ));

        assert_eq!(index.get_by_reason(TombstoneReason::UserDelete).len(), 1);
        assert_eq!(index.get_by_reason(TombstoneReason::Compaction).len(), 2);
        assert_eq!(
            index.get_by_reason(TombstoneReason::RetentionPolicy).len(),
            0
        );
    }

    #[test]
    fn test_tombstone_index_cleanup_before() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::with_timestamp(
            test_branch_id(),
            0,
            b"k1".to_vec(),
            1,
            TombstoneReason::UserDelete,
            100,
        ));
        index.add(Tombstone::with_timestamp(
            test_branch_id(),
            0,
            b"k2".to_vec(),
            1,
            TombstoneReason::UserDelete,
            200,
        ));
        index.add(Tombstone::with_timestamp(
            test_branch_id(),
            0,
            b"k3".to_vec(),
            1,
            TombstoneReason::UserDelete,
            300,
        ));

        assert_eq!(index.len(), 3);

        let removed = index.cleanup_before(250);
        assert_eq!(removed, 2);
        assert_eq!(index.len(), 1);

        assert!(!index.is_tombstoned(&test_branch_id(), 0, b"k1", 1));
        assert!(!index.is_tombstoned(&test_branch_id(), 0, b"k2", 1));
        assert!(index.is_tombstoned(&test_branch_id(), 0, b"k3", 1));
    }

    #[test]
    fn test_tombstone_index_clear() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));

        assert!(!index.is_empty());
        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_tombstone_index_roundtrip() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::with_timestamp(
            test_branch_id(),
            0,
            b"key1".to_vec(),
            1,
            TombstoneReason::UserDelete,
            100,
        ));
        index.add(Tombstone::with_timestamp(
            test_branch_id(),
            1,
            b"key2".to_vec(),
            2,
            TombstoneReason::Compaction,
            200,
        ));

        let bytes = index.to_bytes();
        let parsed = TombstoneIndex::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.len(), 2);
        assert!(parsed.is_tombstoned(&test_branch_id(), 0, b"key1", 1));
        assert!(parsed.is_tombstoned(&test_branch_id(), 1, b"key2", 2));
    }

    #[test]
    fn test_tombstone_index_iter() {
        let mut index = TombstoneIndex::new();

        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k1".to_vec(),
            1,
            TombstoneReason::UserDelete,
        ));
        index.add(Tombstone::new(
            test_branch_id(),
            0,
            b"k2".to_vec(),
            2,
            TombstoneReason::Compaction,
        ));

        let all: Vec<_> = index.iter().collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_tombstone_index_empty_roundtrip() {
        let index = TombstoneIndex::new();
        let bytes = index.to_bytes();
        let parsed = TombstoneIndex::from_bytes(&bytes).unwrap();

        assert!(parsed.is_empty());
    }
}
