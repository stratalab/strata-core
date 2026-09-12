use super::super::{BatchKvEntry, Bytes, Command, Executor, ExecutorError, Output};

impl Executor {
    /// Executes a default-branch put command.
    pub fn kv_put(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::KvPut {
            branch: None,
            space: None,
            key: key.into(),
            value: value.into(),
        })
    }

    /// Executes a default-branch get command.
    pub fn kv_get(&mut self, key: impl Into<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvGet {
            branch: None,
            space: None,
            key: key.into(),
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch delete command.
    pub fn kv_delete(&mut self, key: impl Into<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvDelete {
            branch: None,
            space: None,
            key: key.into(),
        })
    }

    /// Executes a default-branch list command.
    pub fn kv_list(&mut self, prefix: Option<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvList {
            branch: None,
            space: None,
            prefix,
            cursor: None,
            limit: None,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch scan command.
    pub fn kv_scan(
        &mut self,
        start: Option<Bytes>,
        limit: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::KvScan {
            branch: None,
            space: None,
            start,
            limit,
        })
    }

    /// Executes a default-branch batch put command.
    pub fn kv_batch_put(&mut self, entries: Vec<BatchKvEntry>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvBatchPut {
            branch: None,
            space: None,
            entries,
        })
    }

    /// Executes a default-branch batch get command.
    pub fn kv_batch_get(&mut self, keys: Vec<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvBatchGet {
            branch: None,
            space: None,
            keys,
        })
    }

    /// Executes a default-branch batch delete command.
    pub fn kv_batch_delete(&mut self, keys: Vec<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvBatchDelete {
            branch: None,
            space: None,
            keys,
        })
    }

    /// Executes a default-branch batch exists command.
    pub fn kv_batch_exists(&mut self, keys: Vec<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvBatchExists {
            branch: None,
            space: None,
            keys,
        })
    }

    /// Executes a default-branch exists command.
    pub fn kv_exists(&mut self, key: impl Into<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvExists {
            branch: None,
            space: None,
            key: key.into(),
        })
    }

    /// Executes a default-branch version-history command.
    pub fn kv_history(&mut self, key: impl Into<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvHistory {
            branch: None,
            space: None,
            key: key.into(),
        })
    }

    /// Executes a default-branch count command.
    pub fn kv_count(&mut self, prefix: Option<Bytes>) -> Result<Output, ExecutorError> {
        self.execute(Command::KvCount {
            branch: None,
            space: None,
            prefix,
            as_of: None,
            as_of_time: None,
        })
    }

    /// Executes a default-branch sample command.
    pub fn kv_sample(
        &mut self,
        prefix: Option<Bytes>,
        count: Option<u64>,
    ) -> Result<Output, ExecutorError> {
        self.execute(Command::KvSample {
            branch: None,
            space: None,
            prefix,
            count,
        })
    }
}
