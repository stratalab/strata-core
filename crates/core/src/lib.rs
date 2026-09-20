//! Shared identity and ordering atoms for Strata.
//!
//! This crate intentionally keeps a narrow public surface. It owns only the
//! identity and ordering atoms that must be shared below engine policy.

#![deny(unsafe_code)]

mod branch;
mod time;
mod version;

pub use branch::{BranchId, BranchIdError};
pub use time::{ParseTimestampError, Timestamp};
pub use version::{CommitVersion, ParseCommitVersionError};

#[cfg(test)]
mod mutation_gate_error_value {
    //! Mirrors the `error_values` entry the mutation gate substitutes for
    //! `Err(..)` in this crate (#3258). If the expression stops compiling,
    //! the lane entry has gone silently inert — every `Result` fn in the
    //! crate reverts to unviable-only body mutants; if the strings drift,
    //! the entry no longer names this expression.

    #[test]
    fn configured_expression_is_constructible_and_verbatim() {
        assert!(!format!("{:?}", crate::BranchIdError::InvalidText).is_empty());
        let declared = |expression: &str| {
            include_str!("../../../.cargo/mutants.toml")
                .lines()
                .map(str::trim)
                .filter(|line| !line.starts_with('#'))
                .any(|line| line.contains(expression))
        };
        assert!(
            declared("crate::BranchIdError::InvalidText"),
            "lane A's error_values must carry this mirror verbatim, uncommented"
        );
    }
}
