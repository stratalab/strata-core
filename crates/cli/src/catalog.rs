//! The embedded CLI command catalog, parsed once per process.
//!
//! The catalog is the IDL's per-command CLI metadata (`cli-command-index.json`,
//! embedded by the executor at build time). The renderer reads each write's
//! `display:` declaration from it, and `strata agents` serves it whole. It is
//! parsed lazily on first use and shared for the life of the process: a
//! command that never consults it (a `--json` run, a read) never pays for the
//! parse.

use std::sync::OnceLock;

use strata_executor::cli_metadata::CliCommandCatalog;

use crate::CliError;

static CATALOG: OnceLock<Result<CliCommandCatalog, String>> = OnceLock::new();

/// The embedded catalog. An invalid embedded index is a build defect the IDL
/// drift gates prevent from shipping; it surfaces here as a usage-class error
/// rather than a panic so a broken binary still fails one command at a time.
pub(crate) fn embedded() -> Result<&'static CliCommandCatalog, CliError> {
    CATALOG
        .get_or_init(|| CliCommandCatalog::embedded().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|reason| CliError::usage(format!("embedded command catalog is invalid: {reason}")))
}

#[cfg(test)]
mod tests {
    use super::embedded;

    #[test]
    fn embedded_catalog_parses_once_and_resolves_wire_names() {
        let first = embedded().expect("embedded catalog parses");
        let second = embedded().expect("embedded catalog parses");
        assert!(std::ptr::eq(first, second), "the catalog is parsed once");
        assert!(first.command_by_wire("kv_put").is_some());
        assert!(first.command_by_wire("no_such_wire").is_none());
    }
}
