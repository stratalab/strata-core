//! Storage substrate for Strata.
//!
//! The supported public boundary is [`api`]. Native callers that have a
//! database directory should open durable local storage with
//! `api::StorageRuntime::open_local(root)`.
//!
//! Volatile storage is available only through explicit APIs such as
//! `api::StorageRuntime::open_ephemeral()` for tests, demos, and sessions that
//! intentionally do not persist after the runtime is dropped.

#![deny(unsafe_code)]

#[cfg(all(target_arch = "wasm32", feature = "localfs"))]
compile_error!("the localfs feature is not supported on wasm32; use default-features = false");

pub mod api;
mod backend;
mod branch;
mod commit;
mod config;
mod debug_trace;
mod error;
mod format;
mod host_memory;
mod layout;
mod lifecycle;
mod object;
mod observability;
mod row;
mod service;
mod sync;
mod table;
mod time_compat;
mod timeline_index;

#[cfg(feature = "perf-trace")]
#[doc(hidden)]
pub use observability::perf_probe;

#[cfg(feature = "perf-trace")]
#[doc(hidden)]
pub use observability::perf_trace;

#[cfg(test)]
mod test_support;

#[cfg(any(test, feature = "testkit"))]
#[doc(hidden)]
pub mod testkit;

#[cfg(test)]
mod mutation_gate_error_values {
    //! Mirrors the `error_values` entries the mutation gate substitutes for
    //! `Err(..)` in this crate (#3258). If an expression stops compiling,
    //! that lane entry has gone silently inert — every `Result` fn returning
    //! its error type reverts to unviable-only body mutants; if the strings
    //! drift, the entry no longer names the mirrored expression.

    #[test]
    fn configured_expressions_are_constructible_and_verbatim() {
        assert!(!format!("{:?}", crate::lifecycle::LifecycleError::WriterLockHeld).is_empty());
        assert!(!format!(
            "{:?}",
            crate::format::FormatError::InvalidEscape { field: "mutated" }
        )
        .is_empty());
        assert!(!format!("{:?}", crate::testkit::TestkitError::new("mutated")).is_empty());
        let declared = |expression: &str| {
            include_str!("../../../.cargo/mutants.toml")
                .lines()
                .map(str::trim)
                .filter(|line| !line.starts_with('#'))
                .any(|line| line.contains(expression))
        };
        for expression in [
            "crate::lifecycle::LifecycleError::WriterLockHeld",
            r#"crate::format::FormatError::InvalidEscape { field: "mutated" }"#,
            r#"crate::testkit::TestkitError::new("mutated")"#,
        ] {
            assert!(
                declared(expression),
                "lane A's error_values must carry this mirror verbatim, uncommented: {expression}"
            );
        }
    }
}
