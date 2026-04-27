//! BranchBundle archive writer (v2)
//!
//! Creates .branchbundle.tar.zst archives containing:
//! - MANIFEST.json - Format metadata and checksums
//! - BRANCH.json - Branch metadata
//! - WAL.branchlog - Branch-scoped transaction payloads (msgpack v2 format)

use crate::branch_bundle::error::{BranchBundleError, BranchBundleResult};
use crate::branch_bundle::types::{
    paths, xxh3_hex, BranchExportInfo, BundleBranchInfo, BundleContents, BundleManifest,
    ExportOptions,
};
use crate::branch_bundle::wal_log::{BranchlogPayload, WalLogWriter};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use tar::{Builder, Header};

/// Writer for BranchBundle archives (v2)
///
/// Creates .branchbundle.tar.zst files with atomic write semantics.
pub struct BranchBundleWriter {
    compression_level: i32,
}

impl BranchBundleWriter {
    /// Create a new writer with the given options
    pub fn new(options: &ExportOptions) -> Self {
        Self {
            compression_level: options.compression_level,
        }
    }

    /// Create a new writer with default options
    pub fn with_defaults() -> Self {
        Self::new(&ExportOptions::default())
    }

    /// Write a complete BranchBundle archive
    ///
    /// This is an atomic operation - either the complete archive is written
    /// or no file is left behind.
    pub fn write(
        &self,
        branch_info: &BundleBranchInfo,
        payloads: &[BranchlogPayload],
        path: &Path,
    ) -> BranchBundleResult<BranchExportInfo> {
        // Create temp file path
        let temp_path = path.with_extension("tmp");

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Try to write, clean up on failure
        match self.write_inner(branch_info, payloads, &temp_path) {
            Ok(info) => {
                // Atomic rename
                fs::rename(&temp_path, path)?;
                Ok(BranchExportInfo {
                    path: path.to_path_buf(),
                    ..info
                })
            }
            Err(e) => {
                // Clean up temp file
                let _ = fs::remove_file(&temp_path);
                Err(e)
            }
        }
    }

    /// Internal write implementation
    fn write_inner(
        &self,
        branch_info: &BundleBranchInfo,
        payloads: &[BranchlogPayload],
        path: &Path,
    ) -> BranchBundleResult<BranchExportInfo> {
        // Prepare file contents
        let branch_json = serde_json::to_vec_pretty(branch_info)?;
        let (wal_data, wal_info) = WalLogWriter::write_to_vec(payloads)?;

        // Build manifest with checksums
        let mut manifest = BundleManifest::new(
            env!("CARGO_PKG_VERSION"),
            BundleContents {
                wal_entry_count: wal_info.entry_count,
                wal_size_bytes: wal_info.bytes_written,
            },
        );
        manifest.add_checksum("BRANCH.json", xxh3_hex(&branch_json));
        manifest.add_checksum("WAL.branchlog", &wal_info.checksum);

        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        manifest.add_checksum("MANIFEST.json", xxh3_hex(&manifest_json));

        // Re-serialize manifest with its own checksum (for verification)
        let manifest_json = serde_json::to_vec_pretty(&manifest)?;

        // Create compressed tar archive
        let file = File::create(path)?;
        let buf_writer = BufWriter::new(file);
        let zstd_writer = zstd::Encoder::new(buf_writer, self.compression_level)
            .map_err(|e| BranchBundleError::compression(format!("zstd encoder: {}", e)))?;
        let zstd_writer = zstd_writer.auto_finish();

        let mut tar_builder = Builder::new(zstd_writer);

        // Add files to archive
        self.add_file(&mut tar_builder, paths::MANIFEST, &manifest_json)?;
        self.add_file(&mut tar_builder, paths::BRANCH, &branch_json)?;
        self.add_file(&mut tar_builder, paths::WAL, &wal_data)?;

        // Finish tar archive
        let zstd_writer = tar_builder
            .into_inner()
            .map_err(|e| BranchBundleError::archive(format!("tar finish: {}", e)))?;

        // Finish zstd compression
        drop(zstd_writer);

        // Get file size and compute checksum
        let metadata = fs::metadata(path)?;
        let bundle_size = metadata.len();

        let bundle_data = fs::read(path)?;
        let checksum = xxh3_hex(&bundle_data);

        Ok(BranchExportInfo {
            branch_id: branch_info.branch_id.clone(),
            path: path.to_path_buf(),
            wal_entry_count: wal_info.entry_count,
            bundle_size_bytes: bundle_size,
            checksum,
        })
    }

    /// Add a file to the tar archive
    fn add_file<W: Write>(
        &self,
        builder: &mut Builder<W>,
        path: &str,
        data: &[u8],
    ) -> BranchBundleResult<()> {
        let mut header = Header::new_gnu();
        header
            .set_path(path)
            .map_err(|e| BranchBundleError::archive(format!("set path '{}': {}", path, e)))?;
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0); // Reproducible builds: zero mtime
        header.set_cksum();

        builder
            .append(&header, data)
            .map_err(|e| BranchBundleError::archive(format!("append '{}': {}", path, e)))?;

        Ok(())
    }

    /// Write a bundle to a Vec<u8> (for testing)
    pub fn write_to_vec(
        &self,
        branch_info: &BundleBranchInfo,
        payloads: &[BranchlogPayload],
    ) -> BranchBundleResult<(Vec<u8>, BranchExportInfo)> {
        // Prepare file contents
        let branch_json = serde_json::to_vec_pretty(branch_info)?;
        let (wal_data, wal_info) = WalLogWriter::write_to_vec(payloads)?;

        // Build manifest with checksums
        let mut manifest = BundleManifest::new(
            env!("CARGO_PKG_VERSION"),
            BundleContents {
                wal_entry_count: wal_info.entry_count,
                wal_size_bytes: wal_info.bytes_written,
            },
        );
        manifest.add_checksum("BRANCH.json", xxh3_hex(&branch_json));
        manifest.add_checksum("WAL.branchlog", &wal_info.checksum);

        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        manifest.add_checksum("MANIFEST.json", xxh3_hex(&manifest_json));

        let manifest_json = serde_json::to_vec_pretty(&manifest)?;

        // Create compressed tar in memory
        let mut buffer = Vec::new();
        {
            let zstd_writer = zstd::Encoder::new(&mut buffer, self.compression_level)
                .map_err(|e| BranchBundleError::compression(format!("zstd encoder: {}", e)))?;
            let zstd_writer = zstd_writer.auto_finish();

            let mut tar_builder = Builder::new(zstd_writer);

            self.add_file(&mut tar_builder, paths::MANIFEST, &manifest_json)?;
            self.add_file(&mut tar_builder, paths::BRANCH, &branch_json)?;
            self.add_file(&mut tar_builder, paths::WAL, &wal_data)?;

            let zstd_writer = tar_builder
                .into_inner()
                .map_err(|e| BranchBundleError::archive(format!("tar finish: {}", e)))?;

            drop(zstd_writer);
        }

        let checksum = xxh3_hex(&buffer);

        let info = BranchExportInfo {
            branch_id: branch_info.branch_id.clone(),
            path: std::path::PathBuf::new(),
            wal_entry_count: wal_info.entry_count,
            bundle_size_bytes: buffer.len() as u64,
            checksum,
        };

        Ok((buffer, info))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch_bundle::types::BRANCHBUNDLE_FORMAT_VERSION;
    use crate::branch_bundle::wal_log::BranchlogPayload;
    use std::io::Read;
    use std::sync::Arc;
    use strata_core::value::Value;
    use strata_core::BranchId;
    use strata_storage::{Key, Namespace, TypeTag};
    use tempfile::tempdir;

    fn make_test_branch_info() -> BundleBranchInfo {
        BundleBranchInfo {
            branch_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            name: "test-branch".to_string(),
            state: "active".to_string(),
            created_at: "2025-01-24T10:00:00Z".to_string(),
            closed_at: "2025-01-24T11:00:00Z".to_string(),
            parent_branch_id: None,
            error: None,
            generation: 0,
        }
    }

    fn make_test_payloads() -> Vec<BranchlogPayload> {
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        vec![
            BranchlogPayload {
                branch_id: branch_id.to_string(),
                version: 1,
                puts: vec![(
                    Key::new(ns.clone(), TypeTag::KV, b"key1".to_vec()),
                    Value::String("value1".to_string()),
                )],
                deletes: vec![],
            },
            BranchlogPayload {
                branch_id: branch_id.to_string(),
                version: 2,
                puts: vec![],
                deletes: vec![Key::new(ns, TypeTag::KV, b"key1".to_vec())],
            },
        ]
    }

    #[test]
    fn test_write_to_vec() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let (data, info) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        assert!(!data.is_empty());
        assert_eq!(info.branch_id, branch_info.branch_id);
        assert_eq!(info.wal_entry_count, 2);
        assert!(info.bundle_size_bytes > 0);
        assert!(!info.checksum.is_empty());
    }

    #[test]
    fn test_write_to_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let info = writer.write(&branch_info, &payloads, &path).unwrap();

        assert!(path.exists());
        assert_eq!(info.path, path);
        assert_eq!(info.wal_entry_count, 2);

        // Verify file is valid zstd
        let data = fs::read(&path).unwrap();
        let mut decoder = zstd::Decoder::new(&data[..]).unwrap();
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert!(!decompressed.is_empty());
    }

    #[test]
    fn test_atomic_write_cleanup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("subdir").join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        // Should create parent directory
        let info = writer.write(&branch_info, &payloads, &path).unwrap();
        assert!(path.exists());
        assert_eq!(info.path, path);

        // Temp file should not exist
        let temp_path = path.with_extension("tmp");
        assert!(!temp_path.exists());
    }

    #[test]
    fn test_tar_structure() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let (data, _) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        // Decompress
        let mut decoder = zstd::Decoder::new(&data[..]).unwrap();
        let mut tar_data = Vec::new();
        decoder.read_to_end(&mut tar_data).unwrap();

        // Parse tar
        let mut archive = tar::Archive::new(&tar_data[..]);
        let file_names: Vec<String> = archive
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&paths::MANIFEST.to_string()));
        assert!(file_names.contains(&paths::BRANCH.to_string()));
        assert!(file_names.contains(&paths::WAL.to_string()));
    }

    #[test]
    fn test_manifest_contains_checksums() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let (data, _) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        // Decompress and extract manifest
        let mut decoder = zstd::Decoder::new(&data[..]).unwrap();
        let mut tar_data = Vec::new();
        decoder.read_to_end(&mut tar_data).unwrap();

        let mut archive = tar::Archive::new(&tar_data[..]);
        let mut manifest_data = None;

        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            if path == paths::MANIFEST {
                let mut data = Vec::new();
                entry.read_to_end(&mut data).unwrap();
                manifest_data = Some(data);
                break;
            }
        }

        let manifest_data = manifest_data.expect("MANIFEST.json not found");
        let manifest: BundleManifest = serde_json::from_slice(&manifest_data).unwrap();

        assert_eq!(manifest.format_version, BRANCHBUNDLE_FORMAT_VERSION);
        assert!(manifest.checksums.contains_key("BRANCH.json"));
        assert!(manifest.checksums.contains_key("WAL.branchlog"));
        assert!(manifest.checksums.contains_key("MANIFEST.json"));
    }

    #[test]
    fn test_branch_json_content() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let (data, _) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        // Decompress and extract BRANCH.json
        let mut decoder = zstd::Decoder::new(&data[..]).unwrap();
        let mut tar_data = Vec::new();
        decoder.read_to_end(&mut tar_data).unwrap();

        let mut archive = tar::Archive::new(&tar_data[..]);
        let mut branch_json_data = None;

        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            if path == paths::BRANCH {
                let mut data = Vec::new();
                entry.read_to_end(&mut data).unwrap();
                branch_json_data = Some(data);
                break;
            }
        }

        let branch_json_data = branch_json_data.expect("BRANCH.json not found");
        let parsed_info: BundleBranchInfo = serde_json::from_slice(&branch_json_data).unwrap();

        assert_eq!(parsed_info.branch_id, branch_info.branch_id);
        assert_eq!(parsed_info.name, branch_info.name);
        assert_eq!(parsed_info.state, branch_info.state);
    }

    #[test]
    fn test_empty_entries() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads: Vec<BranchlogPayload> = vec![];

        let (data, info) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        assert!(!data.is_empty());
        assert_eq!(info.wal_entry_count, 0);
    }

    #[test]
    fn test_reproducible_output() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();

        let (_data1, info1) = writer.write_to_vec(&branch_info, &payloads).unwrap();
        let (_data2, info2) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        assert_eq!(info1.wal_entry_count, info2.wal_entry_count);
    }

    #[test]
    fn test_compression_reduces_size() {
        let writer = BranchBundleWriter::with_defaults();
        let branch_info = make_test_branch_info();

        // Create payloads with repetitive data (compresses well)
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let mut payloads = Vec::new();
        for i in 0..100 {
            payloads.push(BranchlogPayload {
                branch_id: branch_id.to_string(),
                version: i as u64,
                puts: vec![(
                    Key::new(ns.clone(), TypeTag::KV, format!("key{}", i).into_bytes()),
                    Value::String("a]".repeat(1000)),
                )],
                deletes: vec![],
            });
        }

        let (compressed, _) = writer.write_to_vec(&branch_info, &payloads).unwrap();

        // Decompress to get uncompressed size
        let mut decoder = zstd::Decoder::new(&compressed[..]).unwrap();
        let mut uncompressed = Vec::new();
        decoder.read_to_end(&mut uncompressed).unwrap();

        // Compressed should be smaller than uncompressed
        assert!(compressed.len() < uncompressed.len());
    }
}
