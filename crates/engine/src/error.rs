//! Canonical database/runtime error surface for the engine layer.
//!
//! Downstream layers should prefer importing `StrataError` and `StrataResult`
//! from `strata_engine`.

pub use strata_core::{StrataError, StrataResult};

#[cfg(test)]
mod tests {
    #[test]
    fn engine_error_constructors_remain_usable_from_engine_surface() {
        let err = super::StrataError::invalid_input("bad branch name");
        assert!(matches!(
            err,
            super::StrataError::InvalidInput { ref message } if message == "bad branch name"
        ));

        let result: super::StrataResult<()> = Err(super::StrataError::corruption("bad bytes"));
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            super::StrataError::Corruption { ref message } if message == "bad bytes"
        ));
    }
}
