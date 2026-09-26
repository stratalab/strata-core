#![no_main]

use libfuzzer_sys::fuzz_target;
use strata_storage::testkit::{decode_format_bytes, FormatDecoder};

fuzz_target!(|data: &[u8]| {
    let _ = decode_format_bytes(FormatDecoder::SnapshotFlushedBranchesPayload, data);
});
