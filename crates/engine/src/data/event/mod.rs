//! Event log primitive.

mod adapter;
#[cfg(test)]
mod concurrent_append_tests;
mod hash;
mod outcome;
mod record;
mod service;
mod types;

pub use outcome::{
    EventAppendOutcome, EventBatchAppendItemOutcome, EventBatchAppendOutcome,
    EventChainVerification, EventLength, EventRangePage, EventTypeList, EventVersionedRecord,
};
pub(crate) use record::{
    decode_event_metadata, decode_event_record, encode_event_metadata, encode_event_record,
    EventLogMetadata, EventRecordEnvelope,
};
pub use service::EventService;
pub use types::{
    EventBatchAppendEntry, EventPayload, EventRangeDirection, EventSequence, EventType,
};

pub(crate) use adapter::EventBranchAdapter;
pub(crate) use hash::compute_event_hash;
pub(crate) use types::EventHash;
