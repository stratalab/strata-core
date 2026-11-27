//! Byte-oriented KV capability.

mod adapter;
#[cfg(test)]
mod batch_snapshot_tests;
mod outcome;
mod service;
mod types;

pub(crate) use adapter::KvBranchAdapter;
pub use outcome::{
    KvBatchDeleteOutcome, KvBatchPutOutcome, KvDeleteOutcome, KvHistory, KvHistoryRow, KvListPage,
    KvSample, KvScanRow, KvVersionedValue, KvWriteOutcome,
};
pub use service::KvService;
pub use types::{KvKey, KvValue, ProductSpace};
