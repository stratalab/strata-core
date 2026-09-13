//! One session line, read with the binary's grammar.
//!
//! A line typed at the REPL prompt, piped to it, or sent from the browser
//! playground is the binary's own argv without the leading `strata`, so it
//! parses with the whole top-level grammar — session arguments, global flags
//! and command alike. Each reader used to run that parse itself and pick the
//! fields it wanted off `Cli`; whatever it forgot was parsed and dropped
//! (#3312 the playground's format, #3326 the REPL's, #3327 the session
//! arguments on both). This module is the one place a line becomes a command:
//! it names every field of `Cli`, so a flag added there does not compile
//! until it is carried on the line or refused.

use clap::Parser;

use crate::options::{Cli, Format, TopCommand};
use crate::CliError;

/// A command line read inside a session: the command and the per-line
/// choices that ride beside it. Each choice is `None` when the line made
/// none, so the reader falls back to its session — never to a default of its
/// own.
#[derive(Debug)]
pub(crate) struct SessionLine {
    /// The command the line names.
    pub(crate) command: TopCommand,
    /// The line's own `--branch`, over the session's.
    pub(crate) branch: Option<String>,
    /// The line's own `--space`, over the session's.
    pub(crate) space: Option<String>,
    /// The output format the line's `--json` / `--raw` / `--output-format`
    /// chose, over the session's.
    pub(crate) format: Option<Format>,
}

/// Splits a line into its words with shell quoting, as the binary's argv
/// would have arrived. `None` for a blank line or a `#` comment: shlex reads
/// `#` to the end of the line as a shell does, so both split to no words.
pub(crate) fn words(line: &str) -> Result<Option<Vec<String>>, CliError> {
    let words = shlex::split(line.trim())
        .ok_or_else(|| CliError::usage("could not parse the line (unbalanced quotes?)"))?;
    Ok((!words.is_empty()).then_some(words))
}

impl SessionLine {
    /// Reads a line's words with the binary's grammar. The `Err` is the text
    /// to show in place of running a command: a clap parse error, `--help` /
    /// `--version` output, a refused session argument (#3327), or flags with
    /// no command behind them.
    pub(crate) fn parse(words: Vec<String>) -> Result<Self, CliError> {
        let argv = std::iter::once("strata".to_owned()).chain(words);
        let cli = Cli::try_parse_from(argv)
            .map_err(|error| CliError::usage(error.render().to_string()))?;
        // Session arguments (`--db`, `--cache`, …) parse but cannot be
        // honoured inside a session (#3327): refuse, never answer from the
        // current one.
        if let Some(refusal) = cli.line_refusal() {
            return Err(CliError::usage(refusal.to_string()));
        }
        let format = cli.format_override();
        // Field by field, no `..`: a flag added to `Cli` fails to compile
        // here until it is carried on the line or refused above.
        let Cli {
            db_path: _,
            db: _,
            cache: _,
            durability: _,
            ipc: _,
            read_only: _,
            branch,
            space,
            json: _,
            raw: _,
            human: _,
            command,
        } = cli;
        let Some(command) = command else {
            return Err(CliError::usage(
                "type a command, e.g. `kv put greeting hello`",
            ));
        };
        Ok(Self {
            command,
            branch,
            space,
            format,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<SessionLine, CliError> {
        SessionLine::parse(words(line).expect(line).expect("the line has words"))
    }

    #[test]
    fn words_skip_blank_lines_and_comments_and_keep_shell_quoting() {
        // A comment runs to the end of the line, quotes and all.
        for blank in ["", "   ", "# a comment", "  # indented", "# don't"] {
            assert!(words(blank).expect(blank).is_none(), "{blank:?}");
        }
        let kv_put = Some(vec![
            "kv".to_owned(),
            "put".to_owned(),
            "k".to_owned(),
            "a b".to_owned(),
        ]);
        assert_eq!(words("kv put k 'a b'").expect("parses"), kv_put);
        assert_eq!(
            words("kv put k 'a b' # trailing comment").expect("parses"),
            kv_put,
            "a trailing comment is dropped, the words before it kept"
        );
        assert!(
            words("kv put k 'a b").is_err(),
            "an unbalanced quote is an error, not a silently dropped line"
        );
    }

    #[test]
    fn a_line_carries_the_format_its_flags_chose_and_none_otherwise() {
        // #3326: "no flag" is not "human" — the reader resolves it against
        // its session's format.
        let format = |line: &str| parse(line).expect(line).format;
        assert_eq!(format("kv get k"), None);
        assert_eq!(format("--json kv get k"), Some(Format::Json));
        assert_eq!(format("kv get k --json"), Some(Format::Json));
        assert_eq!(format("--raw kv get k"), Some(Format::Raw));
        // #3345: every format a line can render in has a name, so a line
        // inside a `--json` session can still ask for a reader's lines.
        assert_eq!(format("--human kv get k"), Some(Format::Human));
        // Q5 (#3314 S4): the hidden `--output-format` is gone, and with it
        // the second way to ask for a format. A line that still uses it is a
        // parse error, like any unknown flag.
        assert!(parse("--output-format pretty kv get k").is_err());
        assert!(parse("--human --json kv get k").is_err());
        // Conflicting flags are a parse error, as on the command line.
        assert!(parse("--json --raw kv get k").is_err());
    }

    #[test]
    fn a_line_carries_its_own_scope_and_none_otherwise() {
        let line = parse("--branch feature --space s kv get k").expect("parses");
        assert_eq!(line.branch.as_deref(), Some("feature"));
        assert_eq!(line.space.as_deref(), Some("s"));
        assert!(matches!(line.command, TopCommand::Kv(_)));
        let line = parse("kv get k").expect("parses");
        assert_eq!((line.branch, line.space), (None, None));
    }

    #[test]
    fn flags_without_a_command_are_refused_not_dropped() {
        // A bare `--json` or `--branch x` used to be a silent no-op in the
        // REPL: parsed, and nothing happened.
        for line in ["--json", "--branch feature", "--raw --space s"] {
            let error = parse(line).expect_err(line);
            assert!(
                matches!(&error, CliError::Usage(message) if message.contains("type a command")),
                "{line}: {error}"
            );
        }
    }
}
