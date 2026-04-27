//! Canonical database/runtime error surface for the engine layer.
//!
//! Downstream layers should prefer importing `StrataError` and `StrataResult`
//! from `strata_engine`.

pub use strata_core::{StrataError, StrataResult};

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    #[test]
    fn engine_error_surface_matches_canonical_definition() {
        assert_eq!(
            TypeId::of::<super::StrataError>(),
            TypeId::of::<strata_core::StrataError>()
        );
        assert_eq!(
            TypeId::of::<super::StrataResult<()>>(),
            TypeId::of::<strata_core::StrataResult<()>>()
        );
    }
}
