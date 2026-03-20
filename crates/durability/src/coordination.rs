//! WAL coordination primitives for multi-process access.
//!
//! Provides two types for inter-process coordination:
//!
//! - [`WalFileLock`]: Exclusive file lock on `{wal_dir}/.wal-lock` for serializing
//!   WAL writes across processes.
//! - [`CounterFile`]: Atomic read/write of `(max_version, max_txn_id)` at
//!   `{wal_dir}/counters` for globally-unique version and txn_id allocation.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Exclusive file lock on `{wal_dir}/.wal-lock`.
///
/// Only one process can hold this lock at a time. Used to serialize the
/// critical section of coordinated commits: refresh → validate → allocate
/// version → WAL append → update counters.
///
/// The lock is released when the value is dropped.
pub struct WalFileLock {
    _file: File,
}

impl WalFileLock {
    /// Acquire the WAL file lock, blocking until available.
    pub fn acquire(wal_dir: &Path) -> io::Result<Self> {
        let lock_path = wal_dir.join(".wal-lock");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::lock_exclusive(&file)?;
        // Write PID for stale-lock detection
        file.set_len(0)?;
        write!(file, "{}", std::process::id())?;
        file.sync_all()?;
        Ok(WalFileLock { _file: file })
    }

    /// Try to acquire the WAL file lock without blocking.
    ///
    /// Returns `Ok(Some(lock))` if acquired, `Ok(None)` if another process
    /// holds the lock.
    pub fn try_acquire(wal_dir: &Path) -> io::Result<Option<Self>> {
        let lock_path = wal_dir.join(".wal-lock");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                // Write PID for stale-lock detection
                file.set_len(0)?;
                write!(file, "{}", std::process::id())?;
                file.sync_all()?;
                Ok(Some(WalFileLock { _file: file }))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            // On some platforms, try_lock returns EAGAIN (11) or EACCES (13)
            // which may not map to WouldBlock.
            Err(ref e)
                if e.raw_os_error() == Some(11)
                    || e.raw_os_error() == Some(13)
                    || e.raw_os_error() == Some(35) =>
            {
                // 11=EAGAIN (Linux), 13=EACCES (Windows), 35=EAGAIN (macOS)
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Acquire the WAL file lock with stale-lock recovery.
    ///
    /// Attempts a non-blocking acquire first. If the lock is held, checks whether
    /// the holder PID is still alive. If the holder has crashed (PID dead), removes
    /// the stale lock file and re-acquires. Falls back to blocking acquire if the
    /// holder is alive or if the PID cannot be determined.
    pub fn acquire_with_stale_check(wal_dir: &Path) -> io::Result<Self> {
        // Fast path: try non-blocking acquisition
        if let Some(lock) = Self::try_acquire(wal_dir)? {
            return Ok(lock);
        }

        // Lock held — check if holder is alive
        let lock_path = wal_dir.join(".wal-lock");
        if let Ok(contents) = std::fs::read_to_string(&lock_path) {
            if let Ok(pid) = contents.trim().parse::<u32>() {
                if !is_process_alive(pid) {
                    tracing::warn!(
                        target: "strata::wal",
                        pid,
                        "Stale WAL lock — holder is dead, recovering"
                    );
                    // Remove stale lock file so acquire() can create a fresh one.
                    // Ignore NotFound — another process may have already recovered it.
                    if let Err(e) = std::fs::remove_file(&lock_path) {
                        if e.kind() != io::ErrorKind::NotFound {
                            return Err(e);
                        }
                    }
                    return Self::acquire(wal_dir);
                }
            }
        }

        // Holder is alive or PID unreadable — block normally
        Self::acquire(wal_dir)
    }
}

/// Check whether a process with the given PID is still alive.
#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    // kill(pid, 0) checks existence without sending a signal.
    // Returns 0 if we have permission and the process exists.
    // Returns -1 with ESRCH if the process does not exist.
    // Returns -1 with EPERM if the process exists but we lack permission.
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    // EPERM means the process exists but we can't signal it — still alive.
    // Use std::io::Error for portable errno access (works on Linux + macOS).
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Conservative fallback: assume alive on non-Unix platforms.
#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    true
}

// fs2 file locks are released on drop (when the File is closed).

/// Atomic read/write of `(max_version, max_txn_id)` stored at `{wal_dir}/counters`.
///
/// The file format is 16 bytes: two little-endian u64 values.
/// Reads and writes should only be performed while holding [`WalFileLock`].
pub struct CounterFile {
    path: PathBuf,
}

/// Size of the counter file in bytes: two u64 values.
const COUNTER_FILE_SIZE: usize = 16;

impl CounterFile {
    /// Open (or create) a counter file at `{wal_dir}/counters`.
    pub fn new(wal_dir: &Path) -> Self {
        CounterFile {
            path: wal_dir.join("counters"),
        }
    }

    /// Read the current `(max_version, max_txn_id)` from the file.
    ///
    /// Returns an error if the file exists but has invalid contents.
    pub fn read(&self) -> io::Result<(u64, u64)> {
        let mut file = File::open(&self.path)?;
        let mut buf = [0u8; COUNTER_FILE_SIZE];
        file.read_exact(&mut buf)?;
        let max_version = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let max_txn_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok((max_version, max_txn_id))
    }

    /// Read the counters, returning `(0, 0)` if the file is missing or corrupted.
    ///
    /// Handles truncated/corrupted counter files gracefully (e.g., from a crash
    /// during an older non-atomic write). The caller should scan the WAL to
    /// reconstruct the true maximums when `(0, 0)` is returned for an existing
    /// database.
    pub fn read_or_default(&self) -> io::Result<(u64, u64)> {
        match self.read() {
            Ok(v) => Ok(v),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok((0, 0)),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                tracing::warn!(
                    target: "strata::wal",
                    path = %self.path.display(),
                    "Counter file truncated (crash during write?) — defaulting to (0, 0)"
                );
                Ok((0, 0))
            }
            Err(e) => Err(e),
        }
    }

    /// Write `(max_version, max_txn_id)` atomically via write-fsync-rename.
    ///
    /// Uses a temporary file to ensure crash-safety: if a crash occurs
    /// mid-write, the original file is either fully intact or replaced
    /// atomically by the rename.
    pub fn write(&self, max_version: u64, max_txn_id: u64) -> io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        let mut file = File::create(&tmp)?;
        let mut buf = [0u8; COUNTER_FILE_SIZE];
        buf[0..8].copy_from_slice(&max_version.to_le_bytes());
        buf[8..16].copy_from_slice(&max_txn_id.to_le_bytes());
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &self.path)?;
        // Sync parent directory to make the rename durable on crash
        if let Some(parent) = self.path.parent() {
            if parent.exists() {
                let dir = File::open(parent)?;
                dir.sync_all()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_lock_acquire_and_release() {
        let dir = tempdir().unwrap();
        let lock = WalFileLock::acquire(dir.path());
        assert!(lock.is_ok());
        // Lock file should exist and contain our PID
        let lock_path = dir.path().join(".wal-lock");
        assert!(lock_path.exists());
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(
            contents.trim(),
            std::process::id().to_string(),
            "Lock file should contain current PID"
        );
    }

    #[test]
    fn test_wal_lock_second_acquire_blocks() {
        let dir = tempdir().unwrap();
        let _lock1 = WalFileLock::acquire(dir.path()).unwrap();

        // try_acquire should return None (lock is held)
        let lock2 = WalFileLock::try_acquire(dir.path()).unwrap();
        assert!(lock2.is_none(), "Second lock should not be acquired");
    }

    #[test]
    fn test_wal_lock_released_on_drop() {
        let dir = tempdir().unwrap();
        {
            let _lock = WalFileLock::acquire(dir.path()).unwrap();
            // Lock is held
        }
        // Lock should be released after drop
        let lock2 = WalFileLock::try_acquire(dir.path()).unwrap();
        assert!(lock2.is_some(), "Lock should be available after drop");
    }

    #[test]
    fn test_counter_file_read_write_roundtrip() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        cf.write(42, 100).unwrap();
        let (v, t) = cf.read().unwrap();
        assert_eq!(v, 42);
        assert_eq!(t, 100);
    }

    #[test]
    fn test_counter_file_missing_returns_default() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        let (v, t) = cf.read_or_default().unwrap();
        assert_eq!(v, 0);
        assert_eq!(t, 0);
    }

    #[test]
    fn test_counter_file_survives_reopen() {
        let dir = tempdir().unwrap();

        // Write with one instance
        {
            let cf = CounterFile::new(dir.path());
            cf.write(999, 888).unwrap();
        }

        // Read with a new instance
        {
            let cf = CounterFile::new(dir.path());
            let (v, t) = cf.read().unwrap();
            assert_eq!(v, 999);
            assert_eq!(t, 888);
        }
    }

    #[test]
    fn test_counter_file_overwrite() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        cf.write(1, 1).unwrap();
        cf.write(2, 2).unwrap();

        let (v, t) = cf.read().unwrap();
        assert_eq!(v, 2);
        assert_eq!(t, 2);
    }

    // ========================================================================
    // D-3: PID validation tests
    // ========================================================================

    #[test]
    fn test_acquire_writes_pid() {
        let dir = tempdir().unwrap();
        let _lock = WalFileLock::acquire(dir.path()).unwrap();

        let contents = std::fs::read_to_string(dir.path().join(".wal-lock")).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_try_acquire_writes_pid() {
        let dir = tempdir().unwrap();
        let _lock = WalFileLock::try_acquire(dir.path()).unwrap().unwrap();

        let contents = std::fs::read_to_string(dir.path().join(".wal-lock")).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_stale_lock_recovery_no_flock() {
        // Scenario: lock file exists with dead PID but NO flock is held.
        // This happens when a process crashed so hard the OS cleaned up
        // file descriptors (normal case on local filesystems).
        // acquire_with_stale_check should succeed via the try_acquire fast path.
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(".wal-lock");

        std::fs::write(&lock_path, format!("{}", u32::MAX - 1)).unwrap();

        let lock = WalFileLock::acquire_with_stale_check(dir.path());
        assert!(lock.is_ok(), "Should acquire lock when no flock is held");

        // Lock file should now contain our PID (written by try_acquire fast path)
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[cfg(unix)]
    #[test]
    fn test_stale_lock_recovery_with_held_flock() {
        // Scenario: flock is still held (simulating NFS/FUSE where flock doesn't
        // auto-release on crash) but the PID in the file is dead.
        // acquire_with_stale_check should detect the dead PID, remove the file,
        // and acquire a fresh lock on a new inode.
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(".wal-lock");

        // Step 1: Acquire the real flock (simulates an unreleased NFS lock)
        let _held_lock = WalFileLock::acquire(dir.path()).unwrap();

        // Step 2: Overwrite the PID with a dead PID.
        // This writes to the same inode that _held_lock's fd has flock'd.
        std::fs::write(&lock_path, format!("{}", u32::MAX - 1)).unwrap();

        // Step 3: From another thread, call acquire_with_stale_check.
        // - try_acquire() will fail (flock is held by _held_lock's fd)
        // - reads dead PID from file → detects stale lock
        // - removes the file (unlinks the path from the old inode)
        // - acquire() creates a NEW file (new inode) and gets flock on it
        let wal_dir = dir.path().to_path_buf();
        let handle = std::thread::spawn(move || WalFileLock::acquire_with_stale_check(&wal_dir));

        let result = handle.join().unwrap();
        assert!(result.is_ok(), "Should recover stale lock with dead PID");

        // The new lock file should contain our PID (same process, different thread)
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_acquire_with_stale_check_garbage_pid() {
        // If the lock file contains garbage (not a valid PID), the function
        // should fall through safely to blocking acquire.
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(".wal-lock");

        // Write garbage to the lock file (no flock held)
        std::fs::write(&lock_path, "not-a-pid\n").unwrap();

        // Should succeed — try_acquire fast path works since no flock is held
        let lock = WalFileLock::acquire_with_stale_check(dir.path());
        assert!(lock.is_ok());

        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_acquire_with_stale_check_empty_file() {
        // Empty lock file — PID parse fails, should fall through gracefully.
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(".wal-lock");

        std::fs::write(&lock_path, "").unwrap();

        let lock = WalFileLock::acquire_with_stale_check(dir.path());
        assert!(lock.is_ok());

        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_alive_pid_does_not_remove_lock() {
        let dir = tempdir().unwrap();

        // Acquire the lock in the current process
        let _lock = WalFileLock::acquire(dir.path()).unwrap();

        // Another try_acquire should return None (lock is held by us)
        let result = WalFileLock::try_acquire(dir.path()).unwrap();
        assert!(result.is_none(), "Lock should still be held");

        // Lock file should still exist with our PID
        let lock_path = dir.path().join(".wal-lock");
        assert!(lock_path.exists());
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(contents.trim(), std::process::id().to_string());
    }

    #[test]
    fn test_pid_overwritten_on_reacquire() {
        // When a lock is dropped and re-acquired, the PID should be
        // updated to the new holder (even if same process).
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join(".wal-lock");

        {
            let _lock = WalFileLock::acquire(dir.path()).unwrap();
            let contents = std::fs::read_to_string(&lock_path).unwrap();
            assert_eq!(contents.trim(), std::process::id().to_string());
        }
        // Lock dropped — write a fake old PID to simulate stale content
        std::fs::write(&lock_path, "99999").unwrap();

        // Re-acquire should overwrite with current PID
        let _lock = WalFileLock::acquire(dir.path()).unwrap();
        let contents = std::fs::read_to_string(&lock_path).unwrap();
        assert_eq!(
            contents.trim(),
            std::process::id().to_string(),
            "PID should be overwritten on re-acquire"
        );
    }

    // ========================================================================
    // E-7: Counter file corruption tests
    // ========================================================================

    #[test]
    fn test_counter_file_garbage_does_not_silently_zero() {
        // Counter format is 16 raw bytes (two LE u64). Any 16+ bytes are
        // technically valid — there's no magic number or checksum. This test
        // verifies that garbage bytes don't accidentally decode to (0,0),
        // which would silently reset counters. The truncation tests below
        // cover the case where corruption *does* produce an error.
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        std::fs::write(dir.path().join("counters"), b"garbage_data_here!!!").unwrap();

        let (v, t) = cf.read_or_default().unwrap();
        assert!(v != 0 || t != 0, "Garbage bytes should not decode to (0,0)");
    }

    #[test]
    fn test_counter_file_truncated_returns_default() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        // Write only 4 bytes (need 16) — simulates crash during old non-atomic write
        std::fs::write(dir.path().join("counters"), &[0xAA; 4]).unwrap();

        // read_or_default now handles UnexpectedEof gracefully
        let (v, t) = cf.read_or_default().unwrap();
        assert_eq!((v, t), (0, 0), "Truncated counter file should default to (0, 0)");
    }

    #[test]
    fn test_counter_file_empty_returns_default() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        // Write empty file — simulates crash right after file creation
        std::fs::write(dir.path().join("counters"), &[]).unwrap();

        // read_or_default now handles UnexpectedEof gracefully
        let (v, t) = cf.read_or_default().unwrap();
        assert_eq!((v, t), (0, 0), "Empty counter file should default to (0, 0)");
    }

    #[test]
    fn test_counter_file_atomic_write_uses_rename() {
        let dir = tempdir().unwrap();
        let cf = CounterFile::new(dir.path());

        cf.write(42, 100).unwrap();

        // Temp file should not remain after successful write
        assert!(!dir.path().join("counters.tmp").exists());
        // Counter file should contain the written values
        let (v, t) = cf.read().unwrap();
        assert_eq!((v, t), (42, 100));
    }

    #[cfg(unix)]
    #[test]
    fn test_is_process_alive_current_pid() {
        assert!(is_process_alive(std::process::id()));
    }

    #[cfg(unix)]
    #[test]
    fn test_is_process_alive_dead_pid() {
        // PID u32::MAX - 1 is almost certainly not alive on any system
        assert!(!is_process_alive(u32::MAX - 1));
    }

    #[cfg(unix)]
    #[test]
    fn test_is_process_alive_pid_1() {
        // PID 1 (init/systemd) should always be alive on Unix
        assert!(is_process_alive(1));
    }
}
