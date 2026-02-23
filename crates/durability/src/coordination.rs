//! WAL coordination primitives for multi-process access.
//!
//! Provides two types for inter-process coordination:
//!
//! - [`WalFileLock`]: Exclusive file lock on `{wal_dir}/.wal-lock` for serializing
//!   WAL writes across processes.
//! - [`CounterFile`]: Atomic read/write of `(max_version, max_txn_id)` at
//!   `{wal_dir}/counters` for globally-unique version and txn_id allocation.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
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
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::lock_exclusive(&file)?;
        Ok(WalFileLock { _file: file })
    }

    /// Try to acquire the WAL file lock without blocking.
    ///
    /// Returns `Ok(Some(lock))` if acquired, `Ok(None)` if another process
    /// holds the lock.
    pub fn try_acquire(wal_dir: &Path) -> io::Result<Option<Self>> {
        let lock_path = wal_dir.join(".wal-lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(WalFileLock { _file: file })),
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

    /// Read the counters, returning `(0, 0)` if the file does not exist.
    pub fn read_or_default(&self) -> io::Result<(u64, u64)> {
        match self.read() {
            Ok(v) => Ok(v),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok((0, 0)),
            Err(e) => Err(e),
        }
    }

    /// Write `(max_version, max_txn_id)` atomically.
    ///
    /// Writes to the file and fsyncs to ensure durability.
    pub fn write(&self, max_version: u64, max_txn_id: u64) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&self.path)?;
        let mut buf = [0u8; COUNTER_FILE_SIZE];
        buf[0..8].copy_from_slice(&max_version.to_le_bytes());
        buf[8..16].copy_from_slice(&max_txn_id.to_le_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)?;
        file.sync_all()?;
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
        // Lock file should exist
        assert!(dir.path().join(".wal-lock").exists());
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
}
