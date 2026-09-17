//! The `examples/` directory is executed, not just read (#3143).
//!
//! Every `console` block under `examples/` is a transcript: `$ ` lines are
//! commands, everything under them is what the binary printed. This runs each
//! command in order against the real binary and compares, so an example that
//! lies fails the build rather than reaching a reader.
//!
//! This is execute-and-compare, which is a different guarantee from
//! `prose_transcripts.rs`. That one holds hand-written prose against the
//! captured `command-examples.json` corpus — it proves prose does not *invent*
//! output, matching whole command lines against captures. It cannot run a
//! multi-step scenario, where the interesting commands are ones no
//! single-command capture contains (`--branch run-1 kv put …` after a fork).
//!
//! The site runs the same check on its own guides (`verify-transcripts.mjs`),
//! and these files are the copy it will fetch (stratalab/stratadb.org#21), so
//! the two surfaces stay one artifact rather than two that drift.

#![deny(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Stands in for output that varies by run or by machine — a timestamp, a
/// version, a path. Matches any text at that point on the line. The same
/// character the captured-example corpus uses for the same job.
const VARIES: char = '…';

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Every example file, `README.md` aside — it documents the directory rather
/// than demonstrating the binary.
fn example_files() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(examples_dir())
        .expect("examples/ is readable")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .filter(|path| path.file_name().is_some_and(|name| name != "README.md"))
        .collect();
    files.sort();
    files
}

/// One command and the output the example says it prints.
struct Exchange {
    line: usize,
    command: String,
    expected: Vec<String>,
}

/// Reads every `console` fence into exchanges. A fence with no `$ ` prompt is
/// not a transcript — a file's contents, a shape sketch — and yields nothing.
fn exchanges(text: &str) -> Vec<Exchange> {
    let lines: Vec<&str> = text.lines().collect();
    let mut found: Vec<Exchange> = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if lines[index].trim() != "```console" {
            index += 1;
            continue;
        }
        index += 1;
        while index < lines.len() && !lines[index].trim_start().starts_with("```") {
            let line = lines[index];
            if let Some(command) = line.strip_prefix("$ ") {
                found.push(Exchange {
                    line: index + 1,
                    command: command.to_owned(),
                    expected: Vec::new(),
                });
            } else if let Some(last) = found.last_mut() {
                last.expected.push(line.to_owned());
            }
            index += 1;
        }
        index += 1;
    }
    found
}

/// Whether one printed line matches what the example wrote for it, where
/// [`VARIES`] stands for any text.
fn line_matches(expected: &str, actual: &str) -> bool {
    if !expected.contains(VARIES) {
        return expected == actual;
    }
    let mut rest = actual;
    let parts: Vec<&str> = expected.split(VARIES).collect();
    for (position, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        match position {
            // Before the first `…`: the line must start this way.
            0 => match rest.strip_prefix(part) {
                Some(tail) => rest = tail,
                None => return false,
            },
            _ => match rest.find(part) {
                Some(at) => rest = &rest[at + part.len()..],
                None => return false,
            },
        }
    }
    // A trailing part must have ended the line.
    match parts.last() {
        Some(last) if !last.is_empty() => rest.is_empty(),
        _ => true,
    }
}

/// Splits a command line into arguments, honouring the single and double
/// quotes an example uses for JSON paths and multi-word values.
fn arguments(command: &str) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut started = false;
    for character in command.chars() {
        match (quote, character) {
            (Some(open), c) if c == open => quote = None,
            (None, '\'' | '"') => {
                quote = Some(character);
                started = true;
            }
            (None, ' ') => {
                if started || !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            // Inside quotes or out, any other character is part of the
            // argument being built.
            (_, c) => current.push(c),
        }
    }
    if started || !current.is_empty() {
        args.push(current);
    }
    args
}

#[test]
fn every_example_prints_what_it_says_it_prints() {
    let files = example_files();
    assert!(
        !files.is_empty(),
        "no examples found; this test would pass vacuously"
    );

    let mut failures: Vec<String> = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("example is readable");
        let found = exchanges(&text);
        assert!(
            !found.is_empty(),
            "{} holds no transcript, so nothing checks it",
            file.display()
        );

        // One database per example, in its own directory, so an example is
        // independent of the order the files happen to run in.
        let home = tempfile::tempdir().expect("temp home");
        for exchange in found {
            let args = arguments(&exchange.command);
            assert_eq!(
                args.first().map(String::as_str),
                Some("strata"),
                "{}:{} is not a strata command",
                file.display(),
                exchange.line
            );
            let output = Command::new(env!("CARGO_BIN_EXE_strata"))
                .args(&args[1..])
                .current_dir(home.path())
                .env("HOME", home.path())
                .env("XDG_CONFIG_HOME", home.path().join("config"))
                .env_remove("STRATA_HOME")
                .env_remove("STRATA_DB")
                .output()
                .expect("run strata");

            // Both streams, stdout first. A refusal prints on stderr, and an
            // example that shows one has to be checked like any other — an
            // unchecked error transcript is how a wrong error code reaches a
            // reader, which is exactly what happened while writing these.
            let printed: Vec<String> = String::from_utf8_lossy(&output.stdout)
                .lines()
                .chain(String::from_utf8_lossy(&output.stderr).lines())
                .map(str::to_owned)
                .collect();
            // Every expected line, blanks included. A blank inside a
            // transcript is real output — `branch diff` prints one between
            // its header and its table — and dropping blanks shifts every
            // comparison after one by a line. A trailing blank needs no
            // special case: it compares equal to the absent line it faces.
            let expected: &[String] = &exchange.expected;

            for (position, want) in expected.iter().enumerate() {
                let got = printed.get(position).map_or("", String::as_str);
                if !line_matches(want, got) {
                    failures.push(format!(
                        "{}:{}\n  $ {}\n  expected: {want}\n  printed:  {got}\n  stderr:   {}",
                        file.display(),
                        exchange.line,
                        exchange.command,
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "an example printed something other than what it says:\n\n{}",
        failures.join("\n\n")
    );
}

#[cfg(test)]
mod matching_tests {
    use super::{arguments, line_matches};

    #[test]
    fn a_line_without_a_placeholder_matches_exactly() {
        assert!(line_matches("created memory.goal", "created memory.goal"));
        assert!(!line_matches("created memory.goal", "updated memory.goal"));
        assert!(!line_matches("created", "created memory.goal"));
    }

    #[test]
    fn a_placeholder_stands_for_text_that_varies() {
        assert!(line_matches("pong …", "pong 1.2.2"));
        assert!(line_matches("…", "anything at all"));
        assert!(line_matches(
            "      5  …  200",
            "      5  2026-09-14 17:29 UTC  200"
        ));
        // The fixed parts still have to be there, in order.
        assert!(!line_matches(
            "      5  …  200",
            "      5  2026-09-14 17:29 UTC  150"
        ));
        assert!(!line_matches("pong …", "pang 1.2.2"));
        // A trailing fixed part must end the line.
        assert!(!line_matches("… applied", "1 applied, 0 deleted"));
    }

    #[test]
    fn quoted_arguments_survive_splitting() {
        assert_eq!(
            arguments(r#"strata ./agent kv put memory.goal "ship the feature""#),
            [
                "strata",
                "./agent",
                "kv",
                "put",
                "memory.goal",
                "ship the feature"
            ]
        );
        assert_eq!(
            arguments(r#"strata ./agent json set memory.user '$' '{"name":"Ada"}'"#),
            [
                "strata",
                "./agent",
                "json",
                "set",
                "memory.user",
                "$",
                r#"{"name":"Ada"}"#
            ]
        );
        // An empty quoted argument is an argument.
        assert_eq!(
            arguments(r#"strata kv put k """#),
            ["strata", "kv", "put", "k", ""]
        );
    }
}
