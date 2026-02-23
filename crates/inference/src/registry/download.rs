//! HuggingFace model download with progress reporting and lock files.
//!
//! Gated behind the `download` feature flag.

use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::error::InferenceError;

/// Download a file from a HuggingFace repository to the local models directory.
///
/// The download uses a lock file to coordinate concurrent downloads and a
/// temporary directory to avoid partial files. If the file already exists
/// locally, this is a no-op.
pub fn download_hf_file(
    hf_repo: &str,
    hf_file: &str,
    models_dir: &Path,
    progress: &dyn Fn(u64, u64),
) -> Result<(), InferenceError> {
    let dest = models_dir.join(hf_file);

    // Already downloaded
    if dest.exists() {
        return Ok(());
    }

    // Ensure directories exist
    fs::create_dir_all(models_dir)
        .map_err(|e| InferenceError::Registry(format!("Failed to create models dir: {}", e)))?;

    let downloading_dir = models_dir.join(".downloading");
    fs::create_dir_all(&downloading_dir).map_err(|e| {
        InferenceError::Registry(format!("Failed to create .downloading dir: {}", e))
    })?;

    let lock_path = downloading_dir.join(format!("{}.lock", hf_file));
    let temp_path = downloading_dir.join(hf_file);

    // Check for lock file from another process
    if lock_path.exists() {
        let stale_threshold = Duration::from_secs(30 * 60); // 30 minutes
        let lock_age = lock_path
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| SystemTime::now().duration_since(t).ok())
            .unwrap_or(Duration::MAX);

        if lock_age < stale_threshold {
            // Another process is downloading — wait up to 10 minutes
            let max_wait = Duration::from_secs(10 * 60);
            let poll_interval = Duration::from_secs(5);
            let start = std::time::Instant::now();

            while lock_path.exists() && start.elapsed() < max_wait {
                std::thread::sleep(poll_interval);
            }

            // Check if the file appeared
            if dest.exists() {
                return Ok(());
            }
            // If still locked after timeout, fall through and try ourselves
        }

        // Stale lock or timed out — remove and proceed
        let _ = fs::remove_file(&lock_path);
    }

    // Write lock file with PID
    let _lock_guard = LockGuard::new(&lock_path)?;

    // Re-check after acquiring lock — another process may have finished
    // downloading while we were waiting for the lock or between our initial
    // check and lock acquisition (TOCTOU race).
    if dest.exists() {
        return Ok(());
    }

    // Download
    let url = format!(
        "https://huggingface.co/{}/resolve/main/{}",
        hf_repo, hf_file
    );

    let response = ureq::get(&url).call().map_err(|e| {
        InferenceError::Registry(format!(
            "Failed to download '{}' from HuggingFace: {}\n\n\
             Please check your internet connection, or manually download and place the file at:\n  \
             {}\n\n\
             Download URL: {}",
            hf_file,
            e,
            dest.display(),
            url
        ))
    })?;

    let total_bytes = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    let mut reader = response.into_body().into_reader();
    let mut file = fs::File::create(&temp_path)
        .map_err(|e| InferenceError::Registry(format!("Failed to create temp file: {}", e)))?;

    let result = stream_to_file(&mut reader, &mut file, progress, total_bytes);

    // Clean up temp file on any download error
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
        return result.map(|_| ());
    }

    let downloaded = result.unwrap();

    // Verify size if content-length was provided
    if total_bytes > 0 && downloaded != total_bytes {
        let _ = fs::remove_file(&temp_path);
        return Err(InferenceError::Registry(format!(
            "Download size mismatch: expected {} bytes, got {}",
            total_bytes, downloaded
        )));
    }

    // Atomic-ish rename to final location
    fs::rename(&temp_path, &dest).map_err(|e| {
        InferenceError::Registry(format!(
            "Failed to move downloaded file to {}: {}",
            dest.display(),
            e
        ))
    })?;

    Ok(())
}

/// Stream from reader to file, returning total bytes written.
fn stream_to_file(
    reader: &mut dyn Read,
    file: &mut fs::File,
    progress: &dyn Fn(u64, u64),
    total_bytes: u64,
) -> Result<u64, InferenceError> {
    let mut downloaded: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer

    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| InferenceError::Registry(format!("Download read error: {}", e)))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])
            .map_err(|e| InferenceError::Registry(format!("Failed to write temp file: {}", e)))?;
        downloaded += n as u64;
        progress(downloaded, total_bytes);
    }

    file.flush()
        .map_err(|e| InferenceError::Registry(format!("Failed to flush temp file: {}", e)))?;

    Ok(downloaded)
}

/// RAII guard that creates a lock file on construction and removes it on drop.
struct LockGuard {
    path: std::path::PathBuf,
}

impl LockGuard {
    fn new(path: &Path) -> Result<Self, InferenceError> {
        let pid = std::process::id();
        fs::write(path, format!("{}", pid))
            .map_err(|e| InferenceError::Registry(format!("Failed to write lock file: {}", e)))?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
