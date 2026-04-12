/// Errors that can occur during inference operations.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum InferenceError {
    #[error("llama.cpp: {0}")]
    LlamaCpp(String),

    #[error("provider error: {0}")]
    Provider(String),

    #[error("registry error: {0}")]
    Registry(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("not supported: {0}")]
    NotSupported(String),
}

impl From<std::io::Error> for InferenceError {
    fn from(e: std::io::Error) -> Self {
        InferenceError::Io(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Display tests: verify exact format, not just substring ---

    #[test]
    fn display_llamacpp_error() {
        let e = InferenceError::LlamaCpp("model not found".into());
        assert_eq!(e.to_string(), "llama.cpp: model not found");
    }

    #[test]
    fn display_provider_error() {
        let e = InferenceError::Provider("rate limited".into());
        assert_eq!(e.to_string(), "provider error: rate limited");
    }

    #[test]
    fn display_registry_error() {
        let e = InferenceError::Registry("unknown model".into());
        assert_eq!(e.to_string(), "registry error: unknown model");
    }

    #[test]
    fn display_io_error() {
        let e = InferenceError::Io("file not found".into());
        assert_eq!(e.to_string(), "IO error: file not found");
    }

    #[test]
    fn display_not_supported() {
        let e = InferenceError::NotSupported("tokenize on cloud".into());
        assert_eq!(e.to_string(), "not supported: tokenize on cloud");
    }

    // --- Each variant has a distinct prefix (no ambiguity) ---

    #[test]
    fn all_variants_have_distinct_prefixes() {
        let errors = [
            InferenceError::LlamaCpp("x".into()),
            InferenceError::Provider("x".into()),
            InferenceError::Registry("x".into()),
            InferenceError::Io("x".into()),
            InferenceError::NotSupported("x".into()),
        ];
        let messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        // Every pair should be different despite same inner "x"
        for i in 0..messages.len() {
            for j in (i + 1)..messages.len() {
                assert_ne!(messages[i], messages[j], "variants {i} and {j} collide");
            }
        }
    }

    // --- io::Error conversion ---

    #[test]
    fn io_error_conversion_preserves_full_message() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e: InferenceError = io_err.into();
        assert!(matches!(e, InferenceError::Io(_)));
        // Verify the full formatted string, not just a substring
        assert_eq!(e.to_string(), "IO error: gone");
    }

    #[test]
    fn io_error_conversion_preserves_kind_description() {
        // When io::Error has no custom message, to_string() includes the kind
        let io_err = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot read /etc/shadow",
        );
        let e: InferenceError = io_err.into();
        assert!(
            e.to_string().contains("cannot read /etc/shadow"),
            "got: {}",
            e
        );
    }

    #[test]
    fn io_error_from_real_fs_operation() {
        // Exercise the From impl with a real filesystem error
        let result: Result<Vec<u8>, InferenceError> =
            std::fs::read("/nonexistent/path/that/does/not/exist").map_err(Into::into);
        let e = result.unwrap_err();
        assert!(matches!(e, InferenceError::Io(_)));
        assert!(!e.to_string().is_empty());
    }

    // --- Clone across all variants ---

    #[test]
    fn clone_all_variants() {
        let cases: Vec<InferenceError> = vec![
            InferenceError::LlamaCpp("llama msg".into()),
            InferenceError::Provider("provider msg".into()),
            InferenceError::Registry("registry msg".into()),
            InferenceError::Io("io msg".into()),
            InferenceError::NotSupported("not supported msg".into()),
        ];
        for original in &cases {
            let cloned = original.clone();
            assert_eq!(original.to_string(), cloned.to_string());
            // Verify the clone is structurally the same variant
            assert_eq!(
                std::mem::discriminant(original),
                std::mem::discriminant(&cloned)
            );
        }
    }

    // --- Error messages contain original context ---

    #[test]
    fn every_variant_preserves_context_string() {
        let context = "specific context string with special chars: !@#$%";
        let cases: Vec<InferenceError> = vec![
            InferenceError::LlamaCpp(context.into()),
            InferenceError::Provider(context.into()),
            InferenceError::Registry(context.into()),
            InferenceError::Io(context.into()),
            InferenceError::NotSupported(context.into()),
        ];
        for e in &cases {
            assert!(
                e.to_string().contains(context),
                "variant {:?} lost context: {}",
                std::mem::discriminant(e),
                e
            );
        }
    }

    // --- Empty string edge case ---

    #[test]
    fn empty_string_variants_still_have_prefix() {
        // Empty inner string shouldn't produce empty Display output
        let cases: Vec<(InferenceError, &str)> = vec![
            (InferenceError::LlamaCpp(String::new()), "llama.cpp: "),
            (InferenceError::Provider(String::new()), "provider error: "),
            (InferenceError::Registry(String::new()), "registry error: "),
            (InferenceError::Io(String::new()), "IO error: "),
            (
                InferenceError::NotSupported(String::new()),
                "not supported: ",
            ),
        ];
        for (e, expected) in &cases {
            assert_eq!(&e.to_string(), expected);
        }
    }

    // --- std::error::Error trait ---

    #[test]
    fn implements_std_error_trait() {
        let e = InferenceError::Provider("test".into());
        // Verify it can be used as &dyn std::error::Error
        let dyn_err: &dyn std::error::Error = &e;
        assert!(!dyn_err.to_string().is_empty());
        // source() should be None for our string-wrapping variants
        assert!(dyn_err.source().is_none());
    }

    #[test]
    fn usable_with_question_mark_operator() {
        fn fallible() -> Result<(), InferenceError> {
            let _data = std::fs::read("/nonexistent/file/for/test")?;
            Ok(())
        }
        let result = fallible();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InferenceError::Io(_)));
    }
}
