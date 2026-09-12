//! Arrow file format helpers.

use std::path::Path;

use crate::error::ExecutorError;
use crate::types::ArrowFileFormat;

use super::invalid_input;

pub(crate) fn detect_format(path: &Path) -> Result<ArrowFileFormat, ExecutorError> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("parquet") => Ok(ArrowFileFormat::Parquet),
        Some("csv") => Ok(ArrowFileFormat::Csv),
        Some("json" | "jsonl") => Ok(ArrowFileFormat::Jsonl),
        Some(extension) => Err(invalid_input(
            "invalid_argument.executor.arrow_format",
            format!(
                "unrecognized file extension '.{extension}'; expected .parquet, .csv, .jsonl, or .json"
            ),
        )),
        None => Err(invalid_input(
            "invalid_argument.executor.arrow_format",
            "file has no extension; specify the Arrow file format",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::ExecutorErrorClass;

    use super::*;

    #[test]
    fn detects_supported_extensions_case_insensitively() {
        assert_eq!(
            detect_format(Path::new("data.parquet")).expect("parquet detects"),
            ArrowFileFormat::Parquet
        );
        assert_eq!(
            detect_format(Path::new("data.CSV")).expect("csv detects"),
            ArrowFileFormat::Csv
        );
        assert_eq!(
            detect_format(Path::new("data.jsonl")).expect("jsonl detects"),
            ArrowFileFormat::Jsonl
        );
        assert_eq!(
            detect_format(Path::new("data.json")).expect("json detects"),
            ArrowFileFormat::Jsonl
        );
    }

    #[test]
    fn rejects_unknown_or_missing_extensions_with_stable_error() {
        for path in [Path::new("data.arrow"), Path::new("data")] {
            let error = detect_format(path).expect_err("unsupported format fails");
            assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
            assert_eq!(error.code(), "invalid_argument.executor.arrow_format");
        }
    }
}
