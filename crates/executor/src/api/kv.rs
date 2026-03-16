//! Key-value store operations.

use super::Strata;
use crate::{Command, Error, Output, Result, Value};

impl Strata {
    // =========================================================================
    // KV Operations (4 MVP)
    // =========================================================================

    /// Put a value in the KV store.
    ///
    /// Creates the key if it doesn't exist, overwrites if it does.
    /// Returns the version created by this write operation.
    ///
    /// Accepts any type that implements `Into<Value>`:
    /// - `&str`, `String` → `Value::String`
    /// - `i32`, `i64` → `Value::Int`
    /// - `f32`, `f64` → `Value::Float`
    /// - `bool` → `Value::Bool`
    /// - `Vec<u8>`, `&[u8]` → `Value::Bytes`
    ///
    /// # Example
    ///
    /// ```text
    /// db.kv_put("name", "Alice")?;
    /// db.kv_put("age", 30i64)?;
    /// db.kv_put("score", 95.5)?;
    /// db.kv_put("active", true)?;
    /// ```
    pub fn kv_put(&self, key: &str, value: impl Into<Value>) -> Result<u64> {
        match self.executor.execute(Command::KvPut {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            value: value.into(),
        })? {
            Output::WriteResult { version, .. } => Ok(version),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvPut".into(),
            }),
        }
    }

    /// Get a value from the KV store.
    ///
    /// Returns the latest value for the key, or None if it doesn't exist.
    ///
    /// Reads from the current branch context.
    pub fn kv_get(&self, key: &str) -> Result<Option<Value>> {
        match self.executor.execute(Command::KvGet {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            as_of: None,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvGet".into(),
            }),
        }
    }

    /// Delete a key from the KV store.
    ///
    /// Returns `true` if the key existed and was deleted, `false` if it didn't exist.
    ///
    /// Deletes from the current branch context.
    pub fn kv_delete(&self, key: &str) -> Result<bool> {
        match self.executor.execute(Command::KvDelete {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
        })? {
            Output::DeleteResult { deleted, .. } => Ok(deleted),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvDelete".into(),
            }),
        }
    }

    /// Get the full version history for a key.
    ///
    /// Returns all versions of the key, newest first, or None if the key
    /// doesn't exist.
    ///
    /// # Example
    ///
    /// ```text
    /// db.kv_put("counter", 1i64)?;
    /// db.kv_put("counter", 2i64)?;
    /// db.kv_put("counter", 3i64)?;
    ///
    /// let history = db.kv_getv("counter")?.unwrap();
    /// assert_eq!(history[0].value, Value::Int(3)); // newest first
    /// ```
    pub fn kv_getv(&self, key: &str) -> Result<Option<Vec<crate::types::VersionedValue>>> {
        match self.executor.execute(Command::KvGetv {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            as_of: None,
        })? {
            Output::VersionHistory(h) => Ok(h),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvGetv".into(),
            }),
        }
    }

    /// List keys with optional prefix filter.
    ///
    /// Returns all keys matching the prefix (or all keys if prefix is None).
    ///
    /// Lists from the current branch context.
    pub fn kv_list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        match self.executor.execute(Command::KvList {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
            cursor: None,
            limit: None,
            as_of: None,
        })? {
            Output::Keys(keys) => Ok(keys),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvList".into(),
            }),
        }
    }

    // =========================================================================
    // KV as_of Variants
    // =========================================================================

    /// Get a value from the KV store at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn kv_get_as_of(&self, key: &str, as_of: Option<u64>) -> Result<Option<Value>> {
        match self.executor.execute(Command::KvGet {
            branch: self.branch_id(),
            space: self.space_id(),
            key: key.to_string(),
            as_of,
        })? {
            Output::MaybeVersioned(v) => Ok(v.map(|vv| vv.value)),
            Output::Maybe(v) => Ok(v),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvGet".into(),
            }),
        }
    }

    /// List keys with optional prefix filter at a specific point in time.
    ///
    /// `as_of` is a timestamp in microseconds since epoch.
    pub fn kv_list_as_of(&self, prefix: Option<&str>, as_of: Option<u64>) -> Result<Vec<String>> {
        match self.executor.execute(Command::KvList {
            branch: self.branch_id(),
            space: self.space_id(),
            prefix: prefix.map(|s| s.to_string()),
            cursor: None,
            limit: None,
            as_of,
        })? {
            Output::Keys(keys) => Ok(keys),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvList".into(),
            }),
        }
    }

    // =========================================================================
    // KV Batch Operations
    // =========================================================================

    /// Batch put multiple key-value pairs in a single transaction.
    ///
    /// Returns per-item results positionally mapped to the input entries.
    pub fn kv_batch_put(
        &self,
        entries: Vec<crate::types::BatchKvEntry>,
    ) -> Result<Vec<crate::types::BatchItemResult>> {
        match self.executor.execute(Command::KvBatchPut {
            branch: self.branch_id(),
            space: self.space_id(),
            entries,
        })? {
            Output::BatchResults(results) => Ok(results),
            _ => Err(Error::Internal {
                reason: "Unexpected output for KvBatchPut".into(),
            }),
        }
    }
}
