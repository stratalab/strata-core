//! HuggingFace model download with progress reporting and lock files.
//!
//! Gated behind the `download` feature flag.
//!
//! Endpoint and auth are environment-configurable, mirroring the
//! `STRATA_MODELS_DIR` precedent:
//!
//! - `STRATA_HF_ENDPOINT` — base endpoint for model downloads (mirrors,
//!   self-hosted proxies). Defaults to `https://huggingface.co`.
//! - `STRATA_HF_TOKEN` (falling back to the ecosystem-standard `HF_TOKEN`) —
//!   sent as a bearer token for gated/private repositories. Never logged.

use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

use super::model_file_is_downloaded;
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
    download_hf_file_with_size(hf_repo, hf_file, models_dir, progress, 0, None)
}

/// Download a file from a HuggingFace repository with an expected size for validation.
///
/// If `expected_size` is non-zero and the server reports a content-length that
/// differs by more than 2x, the download is aborted as a safety check.
///
/// If `sha256` is `Some(hex_hash)`, the downloaded file is verified against the
/// expected SHA-256 digest after completion. On mismatch the file is deleted.
pub fn download_hf_file_with_size(
    hf_repo: &str,
    hf_file: &str,
    models_dir: &Path,
    progress: &dyn Fn(u64, u64),
    expected_size: u64,
    sha256: Option<&str>,
) -> Result<(), InferenceError> {
    let dest = models_dir.join(hf_file);

    // Already downloaded — by the same test `resolve` applies, so what this
    // returns `Ok` for is what `resolve` will then load.
    if model_file_is_downloaded(&dest) {
        return Ok(());
    }
    if dest.exists() {
        // A zero-length leftover from a failed download: clear it so the
        // fresh download can land. Removal failing is not itself an error —
        // the rename at the end of the download reports it if it matters.
        let _ = std::fs::remove_file(&dest);
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
            // Whether the other process's download landed is decided once,
            // below, after our own lock is held — the same check that covers
            // a download finishing between our first look and the lock. A
            // separate check here used a bare `exists()`, so a zero-length
            // file the other process left behind counted as "appeared".
        }

        // Stale lock, or the other process finished or timed out — remove
        // whatever is left and proceed. Removal failing is not itself an
        // error: our own lock overwrites the file next, and a lock we cannot
        // write is reported there.
        let _ = fs::remove_file(&lock_path);
    }

    // Write lock file with PID
    let _lock_guard = LockGuard::new(&lock_path)?;

    // Re-check after acquiring lock — another process may have finished
    // downloading while we were waiting for the lock or between our initial
    // check and lock acquisition (TOCTOU race).
    if model_file_is_downloaded(&dest) {
        return Ok(());
    }

    // Download
    let url = hf_download_url(&hf_endpoint(), hf_repo, hf_file);

    let mut request = ureq::get(&url);
    if let Some(token) = hf_token() {
        request = request.header("authorization", &format!("Bearer {token}"));
    }
    let response = request.call().map_err(|e| {
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

    // Validate content-length against expected size from catalog (if both known)
    if total_bytes > 0 && expected_size > 0 {
        let ratio = total_bytes as f64 / expected_size as f64;
        if !(0.5..=2.0).contains(&ratio) {
            return Err(InferenceError::Registry(format!(
                "Unexpected file size: server reports {} bytes, catalog expects {} bytes",
                total_bytes, expected_size
            )));
        }
    }

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

    // Verify SHA-256 hash before rename (so concurrent processes never see
    // an unverified file at the final destination).
    if let Some(expected_hash) = sha256 {
        verify_sha256(&temp_path, expected_hash, hf_file)?;
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

/// Verifies a downloaded temp file against its expected SHA-256 digest,
/// deleting the file on mismatch so no unverified bytes survive.
fn verify_sha256(
    temp_path: &Path,
    expected_hash: &str,
    hf_file: &str,
) -> Result<(), InferenceError> {
    let mut hasher = Sha256::new();
    let mut f = fs::File::open(temp_path).map_err(|e| {
        InferenceError::Registry(format!(
            "Failed to open temp file for hash verification: {}",
            e
        ))
    })?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf).map_err(|e| {
            InferenceError::Registry(format!("Hash verification read error: {}", e))
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let actual_hash = format!("{:x}", hasher.finalize());

    if actual_hash != expected_hash {
        let _ = fs::remove_file(temp_path);
        return Err(InferenceError::Registry(format!(
            "SHA-256 hash mismatch for '{}': expected {}, got {}",
            hf_file, expected_hash, actual_hash
        )));
    }
    Ok(())
}

/// Base endpoint for model downloads: `STRATA_HF_ENDPOINT` or the public hub.
fn hf_endpoint() -> String {
    match std::env::var("STRATA_HF_ENDPOINT") {
        Ok(endpoint) if !endpoint.trim().is_empty() => endpoint,
        _ => "https://huggingface.co".to_owned(),
    }
}

/// Bearer token for gated/private repos: `STRATA_HF_TOKEN`, else `HF_TOKEN`.
fn hf_token() -> Option<String> {
    ["STRATA_HF_TOKEN", "HF_TOKEN"]
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .map(|token| token.trim().to_owned())
        .find(|token| !token.is_empty())
}

/// Builds the resolve URL for one file, tolerating trailing slashes on the
/// configured endpoint.
fn hf_download_url(endpoint: &str, hf_repo: &str, hf_file: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    format!("{endpoint}/{hf_repo}/resolve/main/{hf_file}")
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        download_hf_file_with_size, hf_download_url, model_file_is_downloaded, stream_to_file,
        verify_sha256, LockGuard,
    };
    use sha2::{Digest, Sha256};

    #[test]
    fn sha256_verification_accepts_a_matching_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("model.tmp");
        std::fs::write(&path, b"model bytes").expect("write");
        let expected = format!("{:x}", Sha256::digest(b"model bytes"));
        verify_sha256(&path, &expected, "model.gguf").expect("matching hash verifies");
        assert!(path.exists(), "a verified file must be kept");
    }

    #[test]
    fn sha256_mismatch_is_typed_corruption_and_removes_the_temp_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("model.tmp");
        std::fs::write(&path, b"tampered bytes").expect("write");
        let expected = format!("{:x}", Sha256::digest(b"model bytes"));
        let error = verify_sha256(&path, &expected, "model.gguf")
            .expect_err("mismatch must fail verification");
        assert_eq!(error.code(), "inference.download_verification_failed");
        assert!(
            !path.exists(),
            "a failed verification must remove the temp file"
        );
    }

    #[test]
    fn missing_temp_file_fails_verification_typed() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("never-written.tmp");
        let error =
            verify_sha256(&path, "00", "model.gguf").expect_err("missing temp file must fail");
        assert!(!error.code().is_empty());
    }

    #[test]
    fn streaming_writes_every_byte_and_reports_monotonic_progress() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("streamed.tmp");
        let payload = vec![0xA7u8; 200_000];
        let mut reader = std::io::Cursor::new(payload.clone());
        let mut file = std::fs::File::create(&path).expect("create");
        let observed = std::cell::RefCell::new(Vec::new());
        let written = stream_to_file(
            &mut reader,
            &mut file,
            &|done, total| observed.borrow_mut().push((done, total)),
            payload.len() as u64,
        )
        .expect("stream");
        assert_eq!(written, payload.len() as u64);
        assert_eq!(std::fs::read(&path).expect("read back"), payload);
        let observed = observed.into_inner();
        assert!(!observed.is_empty(), "progress must be reported");
        assert!(
            observed.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "progress must be monotonic: {observed:?}"
        );
        assert_eq!(
            observed.last().expect("final progress").0,
            payload.len() as u64
        );
    }

    #[test]
    fn stream_read_errors_are_typed_download_failures() {
        struct FailingReader;
        impl std::io::Read for FailingReader {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("connection reset by peer"))
            }
        }
        let dir = tempfile::tempdir().expect("tmp");
        let mut file = std::fs::File::create(dir.path().join("partial.tmp")).expect("create");
        let error = stream_to_file(&mut FailingReader, &mut file, &|_, _| {}, 100)
            .expect_err("read failure must propagate");
        assert_eq!(error.code(), "inference.download_failed");
    }

    #[test]
    fn lock_guard_writes_the_pid_and_removes_itself_on_drop() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("download.lock");
        {
            let _guard = LockGuard::new(&path).expect("acquire lock");
            let contents = std::fs::read_to_string(&path).expect("lock exists while held");
            assert_eq!(
                contents,
                std::process::id().to_string(),
                "the lock names its owner"
            );
        }
        assert!(!path.exists(), "the lock must vanish when the guard drops");
    }

    #[test]
    fn download_url_uses_default_hub_shape() {
        assert_eq!(
            hf_download_url(
                "https://huggingface.co",
                "stratalab-org/gpt2-GGUF",
                "gpt2.Q4_K_M.gguf"
            ),
            "https://huggingface.co/stratalab-org/gpt2-GGUF/resolve/main/gpt2.Q4_K_M.gguf"
        );
    }

    #[test]
    fn download_url_tolerates_trailing_slash_on_endpoint() {
        assert_eq!(
            hf_download_url("https://mirror.internal/hf/", "org/repo", "file.gguf"),
            "https://mirror.internal/hf/org/repo/resolve/main/file.gguf"
        );
    }

    /// Serializes the tests that point `STRATA_HF_ENDPOINT` at an unroutable
    /// address: the variable is process-global and tests run in parallel.
    static ENDPOINT_MUTEX: Mutex<()> = Mutex::new(());

    /// Runs `body` with downloads pointed at a closed loopback port, so a
    /// download that is attempted fails at once (connection refused) and one
    /// that is not attempted is the only way to succeed. Restores the
    /// variable afterwards, panic or not.
    fn without_a_reachable_hub<T>(body: impl FnOnce() -> T) -> T {
        struct Restore(Option<String>);
        impl Drop for Restore {
            fn drop(&mut self) {
                match self.0.take() {
                    Some(value) => unsafe { std::env::set_var("STRATA_HF_ENDPOINT", value) },
                    None => unsafe { std::env::remove_var("STRATA_HF_ENDPOINT") },
                }
            }
        }
        let _serialized = ENDPOINT_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _restore = Restore(std::env::var("STRATA_HF_ENDPOINT").ok());
        unsafe { std::env::set_var("STRATA_HF_ENDPOINT", "http://127.0.0.1:1") };
        body()
    }

    #[test]
    fn a_downloaded_file_is_not_downloaded_again() {
        without_a_reachable_hub(|| {
            let dir = tempfile::tempdir().expect("tmp");
            let dest = dir.path().join("model.gguf");
            std::fs::write(&dest, b"gguf").expect("write");
            assert!(model_file_is_downloaded(&dest));

            download_hf_file_with_size("org/repo", "model.gguf", dir.path(), &|_, _| {}, 0, None)
                .expect("a downloaded file needs no hub: the download is skipped");

            assert_eq!(
                std::fs::read(&dest).expect("read back"),
                b"gguf",
                "the file on disk is left as it was"
            );
            assert!(
                !dir.path().join(".downloading").exists(),
                "no download was started"
            );
        });
    }

    #[test]
    fn a_zero_length_leftover_is_cleared_and_downloaded_again() {
        without_a_reachable_hub(|| {
            let dir = tempfile::tempdir().expect("tmp");
            let dest = dir.path().join("model.gguf");
            std::fs::write(&dest, b"").expect("write");
            assert!(!model_file_is_downloaded(&dest));

            let error = download_hf_file_with_size(
                "org/repo",
                "model.gguf",
                dir.path(),
                &|_, _| {},
                0,
                None,
            )
            .expect_err("an interrupted download's leftover is not a model: the download is attempted, and there is no hub");

            assert_eq!(error.code(), "inference.download_failed");
            assert!(
                !dest.exists(),
                "the zero-length leftover is cleared before the download"
            );
            assert!(
                dir.path().join(".downloading").is_dir(),
                "the download was started"
            );
            assert!(
                !dir.path().join(".downloading/model.gguf.lock").exists(),
                "the lock is released when the download fails"
            );
        });
    }
}
