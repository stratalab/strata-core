//! `mutants-cfg-filter` — drop cargo-mutants survivors that land in `#[cfg(..)]`
//! spans the current lane does not compile (#3254).
//!
//! Usage:
//! ```text
//! mutants-cfg-filter --inactive-feature <name>... [--active-feature <name>...] \
//!     [--in-place] [<missed.txt> | -]
//! ```
//!
//! Reads a cargo-mutants `missed.txt` (a survivor per line, `path:line:col: ..`),
//! parses each referenced source file, and removes survivors inside a span whose
//! `cfg` provably evaluates to false under the given feature knowledge. With
//! `--in-place` it rewrites the file; otherwise it writes the kept survivors to
//! stdout. Dropped survivors are logged to stderr for reviewer auditability.
//! Always exits 0: it is a transformer, not a verdict. The verdict is read off
//! the filtered `missed.txt` by `scripts/mutation-verdict.sh`.

use std::collections::HashMap;
use std::io::Read;
use std::process::ExitCode;

use mutants_cfg_filter::{filter_missed, inactive_line_spans, parse_survivor_location, CfgContext};

fn main() -> ExitCode {
    let mut inactive = Vec::new();
    let mut active = Vec::new();
    let mut in_place = false;
    let mut path: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--inactive-feature" => match args.next() {
                Some(v) => inactive.push(v),
                None => return usage_error("--inactive-feature needs a value"),
            },
            "--active-feature" => match args.next() {
                Some(v) => active.push(v),
                None => return usage_error("--active-feature needs a value"),
            },
            "--in-place" => in_place = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: mutants-cfg-filter --inactive-feature <name>... \
                     [--active-feature <name>...] [--in-place] [<missed.txt> | -]"
                );
                return ExitCode::SUCCESS;
            }
            other if other.starts_with("--") => {
                return usage_error(&format!("unknown flag {other}"));
            }
            other => {
                if path.is_some() {
                    return usage_error("only one input path is accepted");
                }
                path = Some(other.to_string());
            }
        }
    }

    if in_place && matches!(path.as_deref(), None | Some("-")) {
        return usage_error("--in-place requires a file path, not stdin");
    }

    let missed = match read_input(path.as_deref()) {
        Ok(Some(body)) => body,
        // `--in-place` on a missing file (e.g. a lane that built no mutants
        // wrote no missed.txt): nothing to filter, succeed quietly.
        Ok(None) => return ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("mutants-cfg-filter: cannot read input: {e}");
            return ExitCode::FAILURE;
        }
    };

    let ctx = CfgContext::with_inactive(inactive).with_active(active);
    let index = build_span_index(&missed, &ctx);
    let outcome = filter_missed(&missed, &index);

    for dropped in &outcome.dropped {
        eprintln!("mutants-cfg-filter: dropped (cfg-inactive span): {dropped}");
    }
    if !outcome.dropped.is_empty() {
        eprintln!(
            "mutants-cfg-filter: dropped {} survivor(s) in cfg-inactive spans; {} kept",
            outcome.dropped.len(),
            outcome.kept.len()
        );
    }

    let mut rendered = outcome.kept.join("\n");
    if !rendered.is_empty() {
        rendered.push('\n');
    }

    match (in_place, path.as_deref()) {
        (true, Some(p)) => {
            if let Err(e) = std::fs::write(p, rendered) {
                eprintln!("mutants-cfg-filter: cannot rewrite {p}: {e}");
                return ExitCode::FAILURE;
            }
        }
        _ => print!("{rendered}"),
    }

    ExitCode::SUCCESS
}

/// Read the `missed.txt` body. `None`/`-` means stdin. Returns `Ok(None)` when a
/// named file is absent (a legitimate "no survivors" state for `--in-place`).
fn read_input(path: Option<&str>) -> std::io::Result<Option<String>> {
    match path {
        None | Some("-") => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            Ok(Some(buf))
        }
        Some(p) => match std::fs::read_to_string(p) {
            Ok(body) => Ok(Some(body)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        },
    }
}

/// Parse each distinct source file referenced by a survivor once, mapping its
/// path to the inactive line spans for this lane. A file that cannot be read is
/// skipped (its survivors are kept).
fn build_span_index(missed: &str, ctx: &CfgContext) -> HashMap<String, Vec<(usize, usize)>> {
    let mut index: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    for line in missed.lines() {
        let Some((path, _)) = parse_survivor_location(line) else {
            continue;
        };
        if index.contains_key(path) {
            continue;
        }
        match std::fs::read_to_string(path) {
            Ok(source) => {
                index.insert(path.to_string(), inactive_line_spans(&source, ctx));
            }
            Err(e) => {
                eprintln!("mutants-cfg-filter: cannot read {path}, keeping its survivors: {e}");
                index.insert(path.to_string(), Vec::new());
            }
        }
    }
    index
}

fn usage_error(msg: &str) -> ExitCode {
    eprintln!("mutants-cfg-filter: {msg}");
    ExitCode::FAILURE
}
