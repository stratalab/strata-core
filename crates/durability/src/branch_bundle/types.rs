//! BranchBundle core types
//!
//! Types for the BranchBundle archive format (.branchbundle.tar.zst)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use strata_core::BranchGeneration;

/// Current BranchBundle format version
pub const BRANCHBUNDLE_FORMAT_VERSION: u32 = 2;

/// File extension for BranchBundle archives
pub const BRANCHBUNDLE_EXTENSION: &str = ".branchbundle.tar.zst";

/// Archive paths within the bundle
pub mod paths {
    /// Root directory in the archive
    pub const ROOT: &str = "branchbundle";
    /// Bundle manifest file
    pub const MANIFEST: &str = "branchbundle/MANIFEST.json";
    /// Branch metadata file
    pub const BRANCH: &str = "branchbundle/BRANCH.json";
    /// WAL entries file
    pub const WAL: &str = "branchbundle/WAL.branchlog";
}

/// Magic bytes for WAL.branchlog header
pub const WAL_BRANCHLOG_MAGIC: &[u8; 10] = b"STRATA_WAL";

/// WAL.branchlog format version
pub const WAL_BRANCHLOG_VERSION: u16 = 2;

// =============================================================================
// MANIFEST.json
// =============================================================================

/// Bundle manifest - format metadata and checksums
///
/// This is the first file read when opening a bundle.
/// It contains version info, checksums for integrity verification,
/// and a summary of bundle contents.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BundleManifest {
    /// Format version (currently 1)
    pub format_version: u32,

    /// Strata version that created this bundle
    pub strata_version: String,

    /// ISO 8601 timestamp when bundle was created
    pub created_at: String,

    /// Checksum algorithm used (currently "xxh3")
    pub checksum_algorithm: String,

    /// Checksums for each file in the bundle
    /// Key: relative path (e.g., "RUN.json")
    /// Value: hex-encoded checksum
    pub checksums: HashMap<String, String>,

    /// Summary of bundle contents
    pub contents: BundleContents,
}

impl BundleManifest {
    /// Create a new manifest with current timestamp
    pub fn new(strata_version: impl Into<String>, contents: BundleContents) -> Self {
        Self {
            format_version: BRANCHBUNDLE_FORMAT_VERSION,
            strata_version: strata_version.into(),
            created_at: chrono_now_iso8601(),
            checksum_algorithm: "xxh3".to_string(),
            checksums: HashMap::new(),
            contents,
        }
    }

    /// Add a checksum for a file
    pub fn add_checksum(&mut self, path: impl Into<String>, checksum: impl Into<String>) {
        self.checksums.insert(path.into(), checksum.into());
    }
}

/// Summary of bundle contents
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct BundleContents {
    /// Number of WAL entries in the bundle
    pub wal_entry_count: u64,

    /// Size of WAL.runlog in bytes (uncompressed)
    pub wal_size_bytes: u64,
    // Post-MVP fields:
    // pub has_snapshot: bool,
    // pub has_index: bool,
}

// =============================================================================
// RUN.json
// =============================================================================

/// Branch metadata - human-readable branch information
///
/// This file contains all metadata about the branch, designed to be
/// readable with standard tools like `jq`.
///
/// ## B3.4 generation field (AD7)
///
/// `generation` records the source-DB lifecycle instance number. It is
/// `#[serde(default)]` so older bundles (which never wrote the field)
/// import as `generation: 0`. Per AD7, `branch_id` keeps its legacy
/// random-UUID meaning; the engine resolves the canonical id via
/// `BranchId::from_user_name(&self.name)` on import. On a name collision
/// in the target DB, the engine allocates a fresh generation rather than
/// honouring the bundle's value, so this field is informational on the
/// import side and authoritative only when the target has no prior
/// history for the name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BundleBranchInfo {
    /// Branch ID (UUID string)
    pub branch_id: String,

    /// Human-readable branch name
    pub name: String,

    /// Branch state: "active"
    pub state: String,

    /// ISO 8601 timestamp when branch was created
    pub created_at: String,

    /// ISO 8601 timestamp when branch was closed
    pub closed_at: String,

    /// Parent branch ID if this is a child branch
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_branch_id: Option<String>,

    /// Error message if branch failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Source-DB lifecycle generation (B3.4 / AD7).
    ///
    /// Older bundles serialized before B3.4 omit this field; serde
    /// defaults to `0` for those. On import, the target DB may discard
    /// the value if it already has any record for the same name (AD7).
    #[serde(default)]
    pub generation: BranchGeneration,
}

impl BundleBranchInfo {
    /// Check if the branch state is a valid terminal state
    pub fn is_terminal_state(&self) -> bool {
        matches!(
            self.state.as_str(),
            "completed" | "failed" | "cancelled" | "archived"
        )
    }
}

// =============================================================================
// Export Types
// =============================================================================

/// Information returned after exporting a branch
#[derive(Debug, Clone)]
pub struct BranchExportInfo {
    /// ID of the exported run
    pub branch_id: String,

    /// Path where the bundle was written
    pub path: PathBuf,

    /// Number of WAL entries in the bundle
    pub wal_entry_count: u64,

    /// Size of the bundle file in bytes
    pub bundle_size_bytes: u64,

    /// xxh3 checksum of the entire bundle file
    pub checksum: String,
}

/// Options for export operation
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Zstd compression level (1-22, default: 3)
    pub compression_level: i32,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            compression_level: 3,
        }
    }
}

// =============================================================================
// Verify Types
// =============================================================================

/// Information returned after verifying a bundle
#[derive(Debug, Clone)]
pub struct BundleVerifyInfo {
    /// Run ID from the bundle
    pub branch_id: String,

    /// User-visible branch name from the bundle metadata
    pub branch_name: String,

    /// Format version of the bundle
    pub format_version: u32,

    /// Number of WAL entries in the bundle
    pub wal_entry_count: u64,

    /// Whether all checksums are valid
    pub checksums_valid: bool,
}

// =============================================================================
// Import Types
// =============================================================================

/// Information returned after importing a branch
#[derive(Debug, Clone)]
pub struct ImportedBranchInfo {
    /// Branch ID of the imported branch
    pub branch_id: String,

    /// Number of WAL entries that were replayed
    pub wal_entries_replayed: u64,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get current time as ISO 8601 string
///
/// NOTE: This uses an approximate date calculation to avoid adding a chrono dependency.
/// The date may be off by a few days due to simplified leap year handling. This is
/// acceptable because this timestamp is only used for bundle metadata/display, not for
/// correctness. The actual WAL entries use proper microsecond timestamps from SystemTime.
fn chrono_now_iso8601() -> String {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();

    // Calculate time components (these are exact)
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Approximate year/month/day calculation
    // This doesn't account for leap years correctly, so dates may be off by a few days.
    // For bundle metadata this is acceptable - use proper datetime library if precision needed.
    let years = 1970 + (days / 365);
    let day_of_year = days % 365;
    let month = (day_of_year / 30).min(11) + 1;
    let day = (day_of_year % 30) + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        years, month, day, hours, minutes, seconds
    )
}

/// Compute xxh3 hash of data and return as hex string
pub fn xxh3_hex(data: &[u8]) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    format!("{:016x}", xxh3_64(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_new() {
        let contents = BundleContents {
            wal_entry_count: 100,
            wal_size_bytes: 5000,
        };
        let manifest = BundleManifest::new("0.12.0", contents.clone());

        assert_eq!(manifest.format_version, BRANCHBUNDLE_FORMAT_VERSION);
        assert_eq!(manifest.strata_version, "0.12.0");
        assert_eq!(manifest.checksum_algorithm, "xxh3");
        assert!(manifest.checksums.is_empty());
        assert_eq!(manifest.contents, contents);
    }

    #[test]
    fn test_manifest_add_checksum() {
        let mut manifest = BundleManifest::new(
            "0.12.0",
            BundleContents {
                wal_entry_count: 0,
                wal_size_bytes: 0,
            },
        );

        manifest.add_checksum("RUN.json", "abc123");
        manifest.add_checksum("WAL.runlog", "def456");

        assert_eq!(
            manifest.checksums.get("RUN.json"),
            Some(&"abc123".to_string())
        );
        assert_eq!(
            manifest.checksums.get("WAL.runlog"),
            Some(&"def456".to_string())
        );
    }

    #[test]
    fn test_manifest_json_roundtrip() {
        let mut manifest = BundleManifest::new(
            "0.12.0",
            BundleContents {
                wal_entry_count: 42,
                wal_size_bytes: 1234,
            },
        );
        manifest.add_checksum("RUN.json", "checksum123");

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: BundleManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(manifest, parsed);
    }

    #[test]
    fn test_branch_info_terminal_states() {
        let make_branch = |state: &str| BundleBranchInfo {
            branch_id: "test".to_string(),
            name: "test".to_string(),
            state: state.to_string(),
            created_at: "2025-01-24T00:00:00Z".to_string(),
            closed_at: "2025-01-24T01:00:00Z".to_string(),
            parent_branch_id: None,
            error: None,
            generation: 0,
        };

        assert!(make_branch("completed").is_terminal_state());
        assert!(make_branch("failed").is_terminal_state());
        assert!(make_branch("cancelled").is_terminal_state());
        assert!(make_branch("archived").is_terminal_state());

        assert!(!make_branch("active").is_terminal_state());
        assert!(!make_branch("unknown").is_terminal_state());
    }

    #[test]
    fn test_branch_info_json_roundtrip() {
        let branch_info = BundleBranchInfo {
            branch_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            name: "my-test-branch".to_string(),
            state: "active".to_string(),
            created_at: "2025-01-24T10:00:00Z".to_string(),
            closed_at: "2025-01-24T11:30:00Z".to_string(),
            parent_branch_id: None,
            error: None,
            generation: 3,
        };

        let json = serde_json::to_string_pretty(&branch_info).unwrap();
        let parsed: BundleBranchInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(branch_info, parsed);
        assert_eq!(parsed.generation, 3);
    }

    #[test]
    fn test_branch_info_with_error() {
        let branch_info = BundleBranchInfo {
            branch_id: "test".to_string(),
            name: "failed-branch".to_string(),
            state: "failed".to_string(),
            created_at: "2025-01-24T10:00:00Z".to_string(),
            closed_at: "2025-01-24T10:05:00Z".to_string(),
            parent_branch_id: Some("parent-id".to_string()),
            error: Some("Connection timeout".to_string()),
            generation: 0,
        };

        let json = serde_json::to_string(&branch_info).unwrap();
        assert!(json.contains("Connection timeout"));
        assert!(json.contains("parent-id"));
    }

    #[test]
    fn test_branch_info_generation_defaults_when_missing() {
        // A bundle written before B3.4 omits the `generation` field
        // entirely; serde must default to 0 so legacy bundles continue to
        // import cleanly.
        let legacy_json = r#"{
            "branch_id": "legacy-id",
            "name": "legacy-branch",
            "state": "active",
            "created_at": "2025-01-24T10:00:00Z",
            "closed_at": "2025-01-24T11:30:00Z"
        }"#;

        let parsed: BundleBranchInfo = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(parsed.generation, 0);
        assert_eq!(parsed.name, "legacy-branch");
    }

    #[test]
    fn test_xxh3_hex() {
        let hash = xxh3_hex(b"hello world");
        assert_eq!(hash.len(), 16); // 64 bits = 16 hex chars
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Same input should produce same hash
        assert_eq!(xxh3_hex(b"hello world"), xxh3_hex(b"hello world"));

        // Different input should produce different hash
        assert_ne!(xxh3_hex(b"hello"), xxh3_hex(b"world"));
    }

    #[test]
    fn test_export_options_default() {
        let opts = ExportOptions::default();
        assert_eq!(opts.compression_level, 3);
    }

    #[test]
    fn test_paths() {
        assert_eq!(paths::MANIFEST, "branchbundle/MANIFEST.json");
        assert_eq!(paths::BRANCH, "branchbundle/BRANCH.json");
        assert_eq!(paths::WAL, "branchbundle/WAL.branchlog");
    }
}
