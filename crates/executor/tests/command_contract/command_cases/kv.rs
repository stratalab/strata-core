use crate::support::*;

pub(super) fn kv_commands() -> Vec<Command> {
    vec![
        Command::KvPut {
            branch: None,
            space: None,
            key: bytes("alpha"),
            value: bytes("one"),
        },
        Command::KvGet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            key: bytes("alpha"),
            as_of: Some(42),
            as_of_time: None,
        },
        Command::KvDelete {
            branch: Some("feature".to_owned()),
            space: None,
            key: bytes("alpha"),
        },
        Command::KvList {
            branch: None,
            space: Some("space-a".to_owned()),
            prefix: Some(bytes("a")),
            cursor: Some(bytes("alpha")),
            limit: Some(2),
            as_of: Some(99),
            as_of_time: None,
        },
        Command::KvScan {
            branch: None,
            space: None,
            start: Some(bytes("a")),
            limit: Some(10),
        },
        Command::KvBatchPut {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            entries: vec![BatchKvEntry::new(bytes("a"), bytes("one"))],
        },
        Command::KvBatchGet {
            branch: None,
            space: None,
            keys: vec![bytes("a"), bytes("missing")],
            as_of: None,
            as_of_time: None,
        },
        Command::KvBatchDelete {
            branch: None,
            space: None,
            keys: vec![bytes("a"), bytes("missing")],
        },
        Command::KvBatchExists {
            branch: None,
            space: None,
            keys: vec![bytes("a"), bytes("missing")],
        },
        Command::KvExists {
            branch: None,
            space: None,
            key: bytes("a"),
        },
        Command::KvHistory {
            branch: None,
            space: None,
            key: bytes("a"),
        },
        Command::KvCount {
            branch: None,
            space: None,
            prefix: Some(bytes("a")),
            as_of: None,
            as_of_time: None,
        },
        Command::KvSample {
            branch: None,
            space: None,
            prefix: Some(bytes("a")),
            count: Some(4),
        },
    ]
}

pub(super) fn kv_round_trip_edge_commands() -> Vec<Command> {
    vec![
        // #3581: the two as-of clocks must round-trip on the wire the same way
        // `KvGet`'s do — a populated `as_of` and a populated `as_of_time`,
        // each mutually exclusive with the other.
        Command::KvBatchGet {
            branch: Some("feature".to_owned()),
            space: Some("space-a".to_owned()),
            keys: vec![bytes("a")],
            as_of: Some(42),
            as_of_time: None,
        },
        Command::KvBatchGet {
            branch: None,
            space: None,
            keys: vec![bytes("a")],
            as_of: None,
            as_of_time: Some(1_700_000_000_000_000),
        },
    ]
}
