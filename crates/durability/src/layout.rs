//! Canonical database directory layout.
//!
//! `DatabaseLayout` provides a single source of truth for database directory
//! structure, ensuring engine, recovery coordinator, and tests all agree on
//! path conventions.

use std::path::{Path, PathBuf};

/// Canonical directory layout for a Strata database.
///
/// Provides consistent path construction for all database directories and files,
/// eliminating ad-hoc path construction throughout the codebase.
///
/// # Directory Structure
///
/// ```text
/// <root>/
/// ├── wal/                  # Write-ahead log segments
/// ├── segments/             # On-disk KV segments
/// ├── snapshots/            # Checkpoint files
/// └── MANIFEST              # Database metadata
/// ```
///
/// # Example
///
/// ```
/// use strata_durability::DatabaseLayout;
/// use std::path::Path;
///
/// let layout = DatabaseLayout::from_root(Path::new("/data/mydb"));
/// assert_eq!(layout.wal_dir(), Path::new("/data/mydb/wal"));
/// assert_eq!(layout.segments_dir(), Path::new("/data/mydb/segments"));
/// assert_eq!(layout.snapshots_dir(), Path::new("/data/mydb/snapshots"));
/// assert_eq!(layout.manifest_path(), Path::new("/data/mydb/MANIFEST"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseLayout {
    /// Root directory of the database.
    root: PathBuf,
    /// Path to the WAL directory.
    wal_dir: PathBuf,
    /// Path to the segments directory (on-disk KV segments).
    segments_dir: PathBuf,
    /// Path to the snapshots directory (checkpoint files).
    snapshots_dir: PathBuf,
    /// Path to the MANIFEST file.
    manifest_path: PathBuf,
}

impl DatabaseLayout {
    /// Create a layout from the root database directory.
    ///
    /// Uses the canonical directory names:
    /// - `wal/` for WAL segments
    /// - `segments/` for on-disk KV segments
    /// - `snapshots/` for checkpoint files
    /// - `MANIFEST` for database metadata
    pub fn from_root(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            wal_dir: root.join("wal"),
            segments_dir: root.join("segments"),
            snapshots_dir: root.join("snapshots"),
            manifest_path: root.join("MANIFEST"),
            root,
        }
    }

    /// Returns the root database directory.
    #[inline]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the WAL directory path.
    ///
    /// WAL segments are stored as `wal-NNNNNN.seg` files within this directory.
    #[inline]
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Returns the segments directory path.
    ///
    /// On-disk KV segments are stored in per-branch subdirectories.
    #[inline]
    pub fn segments_dir(&self) -> &Path {
        &self.segments_dir
    }

    /// Returns the snapshots directory path.
    ///
    /// Checkpoint files are stored as `snap-NNNNNN.chk` files.
    #[inline]
    pub fn snapshots_dir(&self) -> &Path {
        &self.snapshots_dir
    }

    /// Returns the MANIFEST file path.
    ///
    /// The MANIFEST contains database UUID, codec ID, and watermarks.
    #[inline]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    /// Returns the follower state file path.
    ///
    /// Persisted follower watermarks are stored in this file.
    #[inline]
    pub fn follower_state_path(&self) -> PathBuf {
        self.root.join("follower_state.json")
    }

    /// Returns the follower audit log path.
    ///
    /// Administrative skip operations are logged here.
    #[inline]
    pub fn follower_audit_path(&self) -> PathBuf {
        self.root.join("follower_audit.log")
    }

    /// Ensure all directories in the layout exist.
    ///
    /// Creates `wal/`, `segments/`, and `snapshots/` directories if they don't
    /// exist. Does not create the MANIFEST file.
    ///
    /// # Errors
    ///
    /// Returns an IO error if directory creation fails.
    pub fn create_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.wal_dir)?;
        std::fs::create_dir_all(&self.segments_dir)?;
        std::fs::create_dir_all(&self.snapshots_dir)?;
        Ok(())
    }

    /// Check if the WAL directory exists.
    #[inline]
    pub fn wal_exists(&self) -> bool {
        self.wal_dir.exists()
    }

    /// Check if the MANIFEST file exists.
    #[inline]
    pub fn manifest_exists(&self) -> bool {
        self.manifest_path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_root_produces_expected_paths() {
        let layout = DatabaseLayout::from_root("/data/mydb");
        assert_eq!(layout.root(), Path::new("/data/mydb"));
        assert_eq!(layout.wal_dir(), Path::new("/data/mydb/wal"));
        assert_eq!(layout.segments_dir(), Path::new("/data/mydb/segments"));
        assert_eq!(layout.snapshots_dir(), Path::new("/data/mydb/snapshots"));
        assert_eq!(layout.manifest_path(), Path::new("/data/mydb/MANIFEST"));
    }

    #[test]
    fn test_follower_paths() {
        let layout = DatabaseLayout::from_root("/data/mydb");
        assert_eq!(
            layout.follower_state_path(),
            Path::new("/data/mydb/follower_state.json")
        );
        assert_eq!(
            layout.follower_audit_path(),
            Path::new("/data/mydb/follower_audit.log")
        );
    }

    #[test]
    fn test_create_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let layout = DatabaseLayout::from_root(dir.path());

        // Directories should not exist yet
        assert!(!layout.wal_dir().exists());
        assert!(!layout.segments_dir().exists());
        assert!(!layout.snapshots_dir().exists());

        // Create them
        layout.create_dirs().unwrap();

        // Now they should exist
        assert!(layout.wal_dir().exists());
        assert!(layout.segments_dir().exists());
        assert!(layout.snapshots_dir().exists());

        // MANIFEST is not created by create_dirs
        assert!(!layout.manifest_exists());
    }

    #[test]
    fn test_clone_and_eq() {
        let layout1 = DatabaseLayout::from_root("/data/db1");
        let layout2 = layout1.clone();
        assert_eq!(layout1, layout2);

        let layout3 = DatabaseLayout::from_root("/data/db2");
        assert_ne!(layout1, layout3);
    }
}
