//! Disk snapshot system for point-in-time persistence
//!
//! This module provides crash-safe snapshot creation and loading for disk storage.
//!
//! # Overview
//!
//! Disk snapshots capture the complete database state at a point in time.
//! They use the write-fsync-rename pattern for crash safety:
//!
//! 1. Write to temporary file
//! 2. fsync the temporary file
//! 3. Atomic rename to final path
//! 4. fsync the parent directory
//!
//! # File Naming
//!
//! - Snapshot files: `snap-NNNNNN.chk`
//! - Temporary files: `.snap-NNNNNN.tmp`
//!
//! # Components
//!
//! - `SnapshotWriter`: Creates crash-safe snapshots
//! - `SnapshotReader`: Loads and validates snapshots (for recovery)
//!
//! # Note
//!
//! This is distinct from the in-memory MVCC snapshot isolation (version-bounded
//! reads from SegmentedStore). This module handles persistence to disk.

pub mod checkpoint;
pub mod reader;
pub mod writer;

pub use checkpoint::{CheckpointCoordinator, CheckpointData, CheckpointError};
pub use reader::{LoadedSection, LoadedSnapshot, SnapshotReadError, SnapshotReader};
pub use writer::{SnapshotInfo, SnapshotSection, SnapshotWriter};
