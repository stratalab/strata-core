//! Search manifest for persisting segmented index state
//!
//! The manifest (`search.manifest`) stores:
//! - Segment list (segment_id, doc_count, total_doc_len)
//! - Global DocIdMap (EntityRef ↔ u32)
//! - Global stats (total_docs, total_doc_len, next_segment_id)
//! - Per-segment tombstone sets (deleted doc_ids)
//!
//! Written atomically via temp + rename + parent-dir fsync.

use super::types::EntityRef;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::path::Path;

/// Magic bytes for search manifest
const MANIFEST_MAGIC: &[u8; 4] = b"SMNF";
/// Current manifest version.
///
/// v2 was introduced when `EntityRef::{Kv,Event,Json,Vector}` gained a
/// `space` field (Phase 0 of the space-correctness fix). v1 manifests
/// stored space-blind `EntityRef` values that would silently collapse
/// non-default-space identities.
///
/// v3 adds per-document content fingerprints so fast-path reconcile can
/// detect graph updates without rewriting every graph document on every
/// startup. This also forces a one-time rebuild for pre-graph-hook caches.
const MANIFEST_VERSION: u32 = 3;

// ============================================================================
// Manifest Data (serializable)
// ============================================================================

/// Serializable representation of the entire search index state.
#[derive(Serialize, Deserialize)]
pub(crate) struct ManifestData {
    /// Format version
    pub version: u32,
    /// Total documents across all segments + active
    pub total_docs: u64,
    /// Total document length across all segments + active
    pub total_doc_len: u64,
    /// Next segment ID to assign
    pub next_segment_id: u64,
    /// Segment entries
    pub segments: Vec<SegmentManifestEntry>,
    /// Serialized DocIdMap: id_to_ref vec
    pub doc_id_map: Vec<EntityRef>,
    /// Global doc_lengths map (doc_id → doc_len), persists across seals
    /// for re-index detection and accurate total_doc_len on removal.
    #[serde(default)]
    pub doc_lengths: Vec<Option<u32>>,
    /// Forward index: doc_id → terms indexed for that document.
    /// Enables O(terms_in_doc) removal instead of O(vocabulary).
    #[serde(default)]
    pub doc_terms: Vec<Option<Vec<String>>>,
    /// Per-document content fingerprints used by fast-path reconcile.
    #[serde(default)]
    pub doc_content_hashes: Vec<Option<[u8; 32]>>,
}

/// Manifest entry for a single sealed segment.
#[derive(Serialize, Deserialize)]
pub(crate) struct SegmentManifestEntry {
    /// Unique segment identifier
    pub segment_id: u64,
    /// Number of documents in this segment
    pub doc_count: u32,
    /// Total document length in this segment
    pub total_doc_len: u64,
    /// Set of tombstoned (deleted) doc_ids
    pub tombstones: HashSet<u32>,
}

// ============================================================================
// Read / Write
// ============================================================================

/// Write manifest data to a file atomically (temp + rename + parent-dir fsync).
pub(crate) fn write_manifest(path: &Path, data: &ManifestData) -> io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;

    // Serialize with MessagePack
    let payload = rmp_serde::to_vec(data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize error: {}", e)))?;

    // Build final buffer: magic + version + payload
    let mut buf = Vec::with_capacity(8 + payload.len());
    buf.extend_from_slice(MANIFEST_MAGIC);
    buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    buf.extend_from_slice(&payload);

    // Atomic publish: temp + file fsync + rename + directory fsync.
    // Because the manifest is written after any new `.sidx` files, the final
    // directory fsync also makes those earlier rename operations durable.
    let tmp_path = path.with_extension("manifest.tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Load manifest data from a file.
pub(crate) fn load_manifest(path: &Path) -> io::Result<ManifestData> {
    let buf = std::fs::read(path)?;
    if buf.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest too small",
        ));
    }
    if &buf[0..4] != MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad manifest magic",
        ));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported manifest version {}", version),
        ));
    }
    let data: ManifestData = rmp_serde::from_slice(&buf[8..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode error: {}", e)))?;
    Ok(data)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;

    fn sample_manifest() -> ManifestData {
        let branch_id = BranchId::new();
        ManifestData {
            version: MANIFEST_VERSION,
            total_docs: 1000,
            total_doc_len: 50000,
            next_segment_id: 3,
            segments: vec![
                SegmentManifestEntry {
                    segment_id: 0,
                    doc_count: 500,
                    total_doc_len: 25000,
                    tombstones: HashSet::new(),
                },
                SegmentManifestEntry {
                    segment_id: 1,
                    doc_count: 500,
                    total_doc_len: 25000,
                    tombstones: {
                        let mut s = HashSet::new();
                        s.insert(42);
                        s.insert(99);
                        s
                    },
                },
            ],
            doc_id_map: vec![
                EntityRef::Kv {
                    branch_id,
                    space: "default".to_string(),
                    key: "key1".to_string(),
                },
                EntityRef::Kv {
                    branch_id,
                    space: "default".to_string(),
                    key: "key2".to_string(),
                },
            ],
            doc_lengths: vec![Some(10), Some(20)],
            doc_terms: vec![
                Some(vec!["key1".to_string(), "value1".to_string()]),
                Some(vec!["key2".to_string()]),
            ],
            doc_content_hashes: vec![Some([1u8; 32]), Some([2u8; 32])],
        }
    }

    #[test]
    fn test_manifest_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("search.manifest");

        let data = sample_manifest();
        write_manifest(&path, &data).unwrap();

        let loaded = load_manifest(&path).unwrap();
        assert_eq!(loaded.total_docs, 1000);
        assert_eq!(loaded.total_doc_len, 50000);
        assert_eq!(loaded.next_segment_id, 3);
        assert_eq!(loaded.segments.len(), 2);
        assert_eq!(loaded.segments[0].segment_id, 0);
        assert_eq!(loaded.segments[1].tombstones.len(), 2);
        assert!(loaded.segments[1].tombstones.contains(&42));
        assert_eq!(loaded.doc_id_map.len(), 2);
        assert_eq!(loaded.doc_lengths, vec![Some(10), Some(20)]);
        assert_eq!(loaded.doc_terms.len(), 2);
        assert_eq!(
            loaded.doc_terms[0],
            Some(vec!["key1".to_string(), "value1".to_string()])
        );
        assert_eq!(
            loaded.doc_content_hashes,
            vec![Some([1u8; 32]), Some([2u8; 32])]
        );
    }

    #[test]
    fn test_manifest_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("search.manifest");

        let data = ManifestData {
            version: MANIFEST_VERSION,
            total_docs: 0,
            total_doc_len: 0,
            next_segment_id: 0,
            segments: vec![],
            doc_id_map: vec![],
            doc_lengths: vec![],
            doc_terms: vec![],
            doc_content_hashes: vec![],
        };
        write_manifest(&path, &data).unwrap();

        let loaded = load_manifest(&path).unwrap();
        assert_eq!(loaded.total_docs, 0);
        assert!(loaded.segments.is_empty());
        assert!(loaded.doc_id_map.is_empty());
    }

    #[test]
    fn test_manifest_bad_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("bad.manifest");

        let mut buf = vec![0u8; 100];
        buf[0..4].copy_from_slice(b"XXXX");
        std::fs::write(&path, &buf).unwrap();

        assert!(load_manifest(&path).is_err());
    }

    #[test]
    fn test_manifest_too_small() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tiny.manifest");

        std::fs::write(&path, [0u8; 4]).unwrap();
        assert!(load_manifest(&path).is_err());
    }

    /// Pre-fix v1 manifests must be rejected outright. Phase 0 of the
    /// space-correctness fix bumped `MANIFEST_VERSION` to 2 because v1
    /// `EntityRef` values were space-blind; the recovery path
    /// (`recovery::recover_search_state`) catches this error and
    /// rebuilds the index from KV with space-aware refs.
    #[test]
    fn test_manifest_v1_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("legacy.manifest");

        // Forge a "v1" manifest: magic + 0x01 + arbitrary bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 16]); // junk payload
        std::fs::write(&path, &buf).unwrap();

        let result = load_manifest(&path);
        let err = match result {
            Ok(_) => panic!("v1 manifest should be rejected by version check"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("unsupported manifest version"),
            "expected unsupported version error, got: {err}"
        );
    }
}
