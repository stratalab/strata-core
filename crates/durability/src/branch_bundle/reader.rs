//! BranchBundle archive reader (v2)
//!
//! Reads .branchbundle.tar.zst archives and validates their contents.

use crate::branch_bundle::error::{BranchBundleError, BranchBundleResult};
use crate::branch_bundle::types::{
    paths, xxh3_hex, BundleBranchInfo, BundleManifest, BundleVerifyInfo,
    BRANCHBUNDLE_FORMAT_VERSION,
};
use crate::branch_bundle::wal_log::{BranchlogPayload, WalLogReader};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use tar::Archive;

/// Reader for BranchBundle archives (v2)
///
/// Reads and validates .branchbundle.tar.zst files.
pub struct BranchBundleReader;

impl BranchBundleReader {
    /// Validate a bundle's integrity without fully parsing WAL entries
    ///
    /// Checks:
    /// - Archive can be decompressed
    /// - Required files exist (MANIFEST.json, BRANCH.json, WAL.branchlog)
    /// - Checksums match manifest
    /// - WAL.branchlog header is valid
    pub fn validate(path: &Path) -> BranchBundleResult<BundleVerifyInfo> {
        let files = Self::extract_all_files(path)?;

        // Check required files
        let manifest_data = files
            .get("MANIFEST.json")
            .ok_or_else(|| BranchBundleError::missing_file("MANIFEST.json"))?;
        let branch_data = files
            .get("BRANCH.json")
            .ok_or_else(|| BranchBundleError::missing_file("BRANCH.json"))?;
        let wal_data = files
            .get("WAL.branchlog")
            .ok_or_else(|| BranchBundleError::missing_file("WAL.branchlog"))?;

        // Parse manifest
        let manifest: BundleManifest = serde_json::from_slice(manifest_data)?;

        // Validate format version
        if manifest.format_version != BRANCHBUNDLE_FORMAT_VERSION {
            return Err(BranchBundleError::UnsupportedVersion {
                version: manifest.format_version,
            });
        }

        // Validate checksums
        let mut checksums_valid = true;

        if let Some(expected) = manifest.checksums.get("BRANCH.json") {
            let actual = xxh3_hex(branch_data);
            if expected != &actual {
                checksums_valid = false;
            }
        }

        if let Some(expected) = manifest.checksums.get("WAL.branchlog") {
            let actual = xxh3_hex(wal_data);
            if expected != &actual {
                checksums_valid = false;
            }
        }

        // Validate WAL header (without parsing entries)
        WalLogReader::validate(std::io::Cursor::new(wal_data))?;

        // Parse branch info for branch_id
        let branch_info: BundleBranchInfo = serde_json::from_slice(branch_data)?;

        Ok(BundleVerifyInfo {
            branch_id: branch_info.branch_id,
            branch_name: branch_info.name,
            format_version: manifest.format_version,
            wal_entry_count: manifest.contents.wal_entry_count,
            checksums_valid,
        })
    }

    /// Read and parse the manifest
    pub fn read_manifest(path: &Path) -> BranchBundleResult<BundleManifest> {
        let data = Self::extract_file(path, "MANIFEST.json")?;
        let manifest: BundleManifest = serde_json::from_slice(&data)?;

        if manifest.format_version != BRANCHBUNDLE_FORMAT_VERSION {
            return Err(BranchBundleError::UnsupportedVersion {
                version: manifest.format_version,
            });
        }

        Ok(manifest)
    }

    /// Read and parse the branch info
    pub fn read_branch_info(path: &Path) -> BranchBundleResult<BundleBranchInfo> {
        let data = Self::extract_file(path, "BRANCH.json")?;
        let branch_info: BundleBranchInfo = serde_json::from_slice(&data)?;
        Ok(branch_info)
    }

    /// Read and parse WAL payloads
    pub fn read_wal_entries(path: &Path) -> BranchBundleResult<Vec<BranchlogPayload>> {
        let data = Self::extract_file(path, "WAL.branchlog")?;
        WalLogReader::read_from_slice(&data)
    }

    /// Read and parse WAL payloads with checksum validation
    pub fn read_wal_entries_validated(path: &Path) -> BranchBundleResult<Vec<BranchlogPayload>> {
        let files = Self::extract_all_files(path)?;

        let manifest_data = files
            .get("MANIFEST.json")
            .ok_or_else(|| BranchBundleError::missing_file("MANIFEST.json"))?;
        let wal_data = files
            .get("WAL.branchlog")
            .ok_or_else(|| BranchBundleError::missing_file("WAL.branchlog"))?;

        let manifest: BundleManifest = serde_json::from_slice(manifest_data)?;

        // Validate WAL checksum
        if let Some(expected) = manifest.checksums.get("WAL.branchlog") {
            let actual = xxh3_hex(wal_data);
            if expected != &actual {
                return Err(BranchBundleError::ChecksumMismatch {
                    file: "WAL.branchlog".to_string(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }

        WalLogReader::read_from_slice(wal_data)
    }

    /// Read all components from the bundle
    pub fn read_all(path: &Path) -> BranchBundleResult<BundleContents> {
        let files = Self::extract_all_files(path)?;

        let manifest_data = files
            .get("MANIFEST.json")
            .ok_or_else(|| BranchBundleError::missing_file("MANIFEST.json"))?;
        let branch_data = files
            .get("BRANCH.json")
            .ok_or_else(|| BranchBundleError::missing_file("BRANCH.json"))?;
        let wal_data = files
            .get("WAL.branchlog")
            .ok_or_else(|| BranchBundleError::missing_file("WAL.branchlog"))?;

        let manifest: BundleManifest = serde_json::from_slice(manifest_data)?;
        let branch_info: BundleBranchInfo = serde_json::from_slice(branch_data)?;
        let payloads = WalLogReader::read_from_slice(wal_data)?;

        Ok(BundleContents {
            manifest,
            branch_info,
            payloads,
        })
    }

    /// Extract a single file from the archive
    fn extract_file(path: &Path, file_name: &str) -> BranchBundleResult<Vec<u8>> {
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let decoder = zstd::Decoder::new(buf_reader)
            .map_err(|e| BranchBundleError::compression(format!("zstd decode: {}", e)))?;

        let mut archive = Archive::new(decoder);
        let target_path = format!("{}/{}", paths::ROOT, file_name);

        for entry in archive
            .entries()
            .map_err(|e| BranchBundleError::archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| BranchBundleError::archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| BranchBundleError::archive(e.to_string()))?
                .to_string_lossy()
                .to_string();

            if entry_path == target_path {
                let mut data = Vec::new();
                entry.read_to_end(&mut data).map_err(|e| {
                    BranchBundleError::archive(format!("read {}: {}", file_name, e))
                })?;
                return Ok(data);
            }
        }

        Err(BranchBundleError::missing_file(file_name))
    }

    /// Extract all files from the archive into a HashMap
    fn extract_all_files(path: &Path) -> BranchBundleResult<HashMap<String, Vec<u8>>> {
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let decoder = zstd::Decoder::new(buf_reader)
            .map_err(|e| BranchBundleError::compression(format!("zstd decode: {}", e)))?;

        let mut archive = Archive::new(decoder);
        let mut files = HashMap::new();
        let prefix = format!("{}/", paths::ROOT);

        for entry in archive
            .entries()
            .map_err(|e| BranchBundleError::archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| BranchBundleError::archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| BranchBundleError::archive(e.to_string()))?
                .to_string_lossy()
                .to_string();

            // Strip prefix to get relative file name
            if let Some(name) = entry_path.strip_prefix(&prefix) {
                if !name.is_empty() {
                    let mut data = Vec::new();
                    entry
                        .read_to_end(&mut data)
                        .map_err(|e| BranchBundleError::archive(format!("read {}: {}", name, e)))?;
                    files.insert(name.to_string(), data);
                }
            }
        }

        Ok(files)
    }

    /// Read from a byte slice (for testing)
    pub fn read_manifest_from_bytes(data: &[u8]) -> BranchBundleResult<BundleManifest> {
        let decoder = zstd::Decoder::new(data)
            .map_err(|e| BranchBundleError::compression(format!("zstd decode: {}", e)))?;

        let mut archive = Archive::new(decoder);
        let target_path = paths::MANIFEST;

        for entry in archive
            .entries()
            .map_err(|e| BranchBundleError::archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| BranchBundleError::archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| BranchBundleError::archive(e.to_string()))?
                .to_string_lossy()
                .to_string();

            if entry_path == target_path {
                let mut data = Vec::new();
                entry.read_to_end(&mut data)?;
                let manifest: BundleManifest = serde_json::from_slice(&data)?;
                return Ok(manifest);
            }
        }

        Err(BranchBundleError::missing_file("MANIFEST.json"))
    }

    /// Read branch info from a byte slice (for testing)
    pub fn read_branch_info_from_bytes(data: &[u8]) -> BranchBundleResult<BundleBranchInfo> {
        let decoder = zstd::Decoder::new(data)
            .map_err(|e| BranchBundleError::compression(format!("zstd decode: {}", e)))?;

        let mut archive = Archive::new(decoder);
        let target_path = paths::BRANCH;

        for entry in archive
            .entries()
            .map_err(|e| BranchBundleError::archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| BranchBundleError::archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| BranchBundleError::archive(e.to_string()))?
                .to_string_lossy()
                .to_string();

            if entry_path == target_path {
                let mut data = Vec::new();
                entry.read_to_end(&mut data)?;
                let branch_info: BundleBranchInfo = serde_json::from_slice(&data)?;
                return Ok(branch_info);
            }
        }

        Err(BranchBundleError::missing_file("BRANCH.json"))
    }

    /// Read WAL payloads from a byte slice (for testing)
    pub fn read_wal_entries_from_bytes(data: &[u8]) -> BranchBundleResult<Vec<BranchlogPayload>> {
        let decoder = zstd::Decoder::new(data)
            .map_err(|e| BranchBundleError::compression(format!("zstd decode: {}", e)))?;

        let mut archive = Archive::new(decoder);
        let target_path = paths::WAL;

        for entry in archive
            .entries()
            .map_err(|e| BranchBundleError::archive(e.to_string()))?
        {
            let mut entry = entry.map_err(|e| BranchBundleError::archive(e.to_string()))?;
            let entry_path = entry
                .path()
                .map_err(|e| BranchBundleError::archive(e.to_string()))?
                .to_string_lossy()
                .to_string();

            if entry_path == target_path {
                let mut wal_data = Vec::new();
                entry.read_to_end(&mut wal_data)?;
                return WalLogReader::read_from_slice(&wal_data);
            }
        }

        Err(BranchBundleError::missing_file("WAL.branchlog"))
    }
}

/// Complete bundle contents after reading (v2)
#[derive(Debug)]
pub struct BundleContents {
    /// Bundle manifest
    pub manifest: BundleManifest,
    /// Branch metadata
    pub branch_info: BundleBranchInfo,
    /// Transaction payloads
    pub payloads: Vec<BranchlogPayload>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch_bundle::types::ExportOptions;
    use crate::branch_bundle::wal_log::BranchlogPayload;
    use crate::branch_bundle::writer::BranchBundleWriter;
    use std::sync::Arc;
    use strata_core::types::{BranchId, Key, Namespace, TypeTag};
    use strata_core::value::Value;
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

    fn create_test_bundle() -> (Vec<u8>, BundleBranchInfo, Vec<BranchlogPayload>) {
        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        let (data, _) = writer.write_to_vec(&branch_info, &payloads).unwrap();
        (data, branch_info, payloads)
    }

    #[test]
    fn test_read_manifest_from_bytes() {
        let (data, _, _) = create_test_bundle();

        let manifest = BranchBundleReader::read_manifest_from_bytes(&data).unwrap();

        assert_eq!(manifest.format_version, BRANCHBUNDLE_FORMAT_VERSION);
        assert!(manifest.checksums.contains_key("BRANCH.json"));
        assert!(manifest.checksums.contains_key("WAL.branchlog"));
    }

    #[test]
    fn test_read_branch_info_from_bytes() {
        let (data, expected_branch_info, _) = create_test_bundle();

        let branch_info = BranchBundleReader::read_branch_info_from_bytes(&data).unwrap();

        assert_eq!(branch_info.branch_id, expected_branch_info.branch_id);
        assert_eq!(branch_info.name, expected_branch_info.name);
        assert_eq!(branch_info.state, expected_branch_info.state);
    }

    #[test]
    fn test_read_wal_entries_from_bytes() {
        let (data, _, expected_payloads) = create_test_bundle();

        let payloads = BranchBundleReader::read_wal_entries_from_bytes(&data).unwrap();

        assert_eq!(payloads.len(), expected_payloads.len());
    }

    #[test]
    fn test_validate_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer.write(&branch_info, &payloads, &path).unwrap();

        let verify_info = BranchBundleReader::validate(&path).unwrap();

        assert_eq!(verify_info.branch_id, branch_info.branch_id);
        assert_eq!(verify_info.branch_name, branch_info.name);
        assert_eq!(verify_info.format_version, BRANCHBUNDLE_FORMAT_VERSION);
        assert_eq!(verify_info.wal_entry_count, 2);
        assert!(verify_info.checksums_valid);
    }

    #[test]
    fn test_read_manifest_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer.write(&branch_info, &payloads, &path).unwrap();

        let manifest = BranchBundleReader::read_manifest(&path).unwrap();

        assert_eq!(manifest.format_version, BRANCHBUNDLE_FORMAT_VERSION);
        assert_eq!(manifest.contents.wal_entry_count, 2);
    }

    #[test]
    fn test_read_branch_info_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let expected_branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer
            .write(&expected_branch_info, &payloads, &path)
            .unwrap();

        let branch_info = BranchBundleReader::read_branch_info(&path).unwrap();

        assert_eq!(branch_info.branch_id, expected_branch_info.branch_id);
        assert_eq!(branch_info.name, expected_branch_info.name);
    }

    #[test]
    fn test_read_wal_entries_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer.write(&branch_info, &payloads, &path).unwrap();

        let read_payloads = BranchBundleReader::read_wal_entries(&path).unwrap();

        assert_eq!(read_payloads.len(), payloads.len());
    }

    #[test]
    fn test_read_wal_entries_validated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer.write(&branch_info, &payloads, &path).unwrap();

        let read_payloads = BranchBundleReader::read_wal_entries_validated(&path).unwrap();

        assert_eq!(read_payloads.len(), payloads.len());
    }

    #[test]
    fn test_read_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads = make_test_payloads();
        writer.write(&branch_info, &payloads, &path).unwrap();

        let contents = BranchBundleReader::read_all(&path).unwrap();

        assert_eq!(contents.branch_info.branch_id, branch_info.branch_id);
        assert_eq!(contents.payloads.len(), payloads.len());
        assert_eq!(contents.manifest.contents.wal_entry_count, 2);
    }

    #[test]
    fn test_missing_file_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.branchbundle.tar.zst");

        let result = BranchBundleReader::validate(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupted_archive() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupted.branchbundle.tar.zst");

        // Write garbage
        std::fs::write(&path, b"not a valid archive").unwrap();

        let result = BranchBundleReader::validate(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_bundle() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.branchbundle.tar.zst");

        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let branch_info = make_test_branch_info();
        let payloads: Vec<BranchlogPayload> = vec![];
        writer.write(&branch_info, &payloads, &path).unwrap();

        let contents = BranchBundleReader::read_all(&path).unwrap();

        assert!(contents.payloads.is_empty());
        assert_eq!(contents.manifest.contents.wal_entry_count, 0);
    }

    #[test]
    fn test_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("roundtrip.branchbundle.tar.zst");

        // Write
        let writer = BranchBundleWriter::new(&ExportOptions::default());
        let original_branch_info = make_test_branch_info();
        let original_payloads = make_test_payloads();
        writer
            .write(&original_branch_info, &original_payloads, &path)
            .unwrap();

        // Read back
        let contents = BranchBundleReader::read_all(&path).unwrap();

        // Verify
        assert_eq!(
            contents.branch_info.branch_id,
            original_branch_info.branch_id
        );
        assert_eq!(contents.branch_info.name, original_branch_info.name);
        assert_eq!(contents.branch_info.state, original_branch_info.state);
        assert_eq!(contents.payloads.len(), original_payloads.len());

        // Verify payloads match
        for (original, read) in original_payloads.iter().zip(contents.payloads.iter()) {
            assert_eq!(original, read);
        }
    }
}
