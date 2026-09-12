//! #3112 S5: turning what a person types into a wall-clock instant, and back.
//!
//! The engine speaks UTC epoch microseconds — unambiguous, portable, and the
//! same number on every machine. Nobody types `1788732596132961`, so the CLI
//! owns the translation at both ends: a date string on the way in, a readable
//! local time on the way out.
//!
//! ## Which time zone a bare date means
//!
//! A string that carries its own offset (`...Z`, `...+05:30`) is honored
//! exactly — it already names an instant, and there is nothing to guess.
//!
//! A string without one (`2026-09-05 15:00`) is read in the machine's **local**
//! time zone, because that is what a person means when they type it. The
//! alternative — reading it as UTC — silently shifts every query by the user's
//! offset, and a time-travel read that lands on the wrong commit gives a
//! confidently wrong answer rather than an error.
//!
//! This does mean the same bare string names different instants in different
//! places. That is the correct reading of an ambiguous input, not a defect:
//! two people each mean their own 3pm. Anyone who needs one exact instant
//! everywhere writes the offset, and the stored value is always UTC regardless
//! — only the parsing of an ambiguous string is local.

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};

/// Offset-bearing formats beyond RFC-3339, most precise first. These cover the
/// space-separated spelling this module itself prints — what the tool shows,
/// the tool must accept back.
const OFFSET_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S%.f %:z",
    "%Y-%m-%d %H:%M:%S %:z",
    "%Y-%m-%d %H:%M %:z",
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%:z",
];

/// Formats accepted for a bare (offset-free) date-time, most precise first.
const NAIVE_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
];

/// Parses `--as-of-time` into UTC epoch microseconds.
///
/// Accepts, in order of preference:
/// - an offset-bearing timestamp (`2026-09-05T15:00:00Z`, `...+05:30`)
/// - a bare date-time labelled `UTC` (`2026-09-05 15:00:00.000000 UTC`, the
///   form table cells print), read as UTC
/// - a bare date-time (`2026-09-05 15:00`), read as local time
/// - a bare date (`2026-09-05`), read as local midnight
/// - raw epoch microseconds, for scripts that already hold the number
pub(crate) fn parse_instant(input: &str) -> Result<u64, String> {
    let text = input.trim();
    if text.is_empty() {
        return Err(unparseable(input));
    }

    // Raw micros first: a bare integer is never a valid date string, so this
    // cannot shadow a real date, and it keeps `committed_at` values round
    // -trippable straight from JSON output.
    if text.chars().all(|c| c.is_ascii_digit()) {
        return text.parse::<u64>().map_err(|_| {
            format!("`{input}` looks like epoch microseconds but does not fit in a u64")
        });
    }

    if let Ok(fixed) = DateTime::parse_from_rfc3339(text) {
        return micros_from_utc(fixed.timestamp_micros(), input);
    }

    for format in OFFSET_FORMATS {
        if let Ok(fixed) = DateTime::parse_from_str(text, format) {
            return micros_from_utc(fixed.timestamp_micros(), input);
        }
    }

    // The spelling a table cell prints (`format_utc_instant`): a bare reading
    // labelled `UTC` is read as UTC, not local — the label is exactly what
    // removes the ambiguity a bare reading has.
    if let Some(bare) = text.strip_suffix(" UTC") {
        for format in NAIVE_FORMATS {
            if let Ok(naive) = NaiveDateTime::parse_from_str(bare, format) {
                return micros_from_utc(naive.and_utc().timestamp_micros(), input);
            }
        }
    }

    for format in NAIVE_FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return local_micros(naive, input);
        }
    }

    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| unparseable(input))?;
        return local_micros(midnight, input);
    }

    Err(unparseable(input))
}

/// Resolves a local wall-clock reading to an instant.
///
/// A local time can be ambiguous (the repeated hour when DST ends) or
/// nonexistent (the skipped hour when it begins). The repeated hour resolves to
/// the earlier of the two instants — the same "at or before" bias the read
/// itself uses — while a nonexistent one is refused, because there is no
/// instant to resolve it to and inventing one would answer a question the user
/// did not ask.
fn local_micros(naive: NaiveDateTime, input: &str) -> Result<u64, String> {
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(local) => micros_from_utc(local.timestamp_micros(), input),
        chrono::LocalResult::Ambiguous(earlier, _) => {
            micros_from_utc(earlier.timestamp_micros(), input)
        }
        chrono::LocalResult::None => Err(format!(
            "`{input}` is not a real local time (it falls in a daylight-saving gap); \
             use an explicit offset such as `{}Z`",
            naive.format("%Y-%m-%dT%H:%M:%S")
        )),
    }
}

/// The engine's instants are unsigned, so anything before 1970 has no
/// representation — a clearer refusal than an overflow.
fn micros_from_utc(micros: i64, input: &str) -> Result<u64, String> {
    u64::try_from(micros)
        .map_err(|_| format!("`{input}` is before 1970 and has no wall-clock representation"))
}

fn unparseable(input: &str) -> String {
    format!(
        "`{input}` is not a time. Use a date (`2026-09-05`), a date and time \
         (`2026-09-05 15:00`), an offset-bearing timestamp \
         (`2026-09-05T15:00:00Z`), or raw epoch microseconds"
    )
}

/// Renders an instant as a readable local time with its offset, for human
/// output.
///
/// The offset is always shown: without it the reader cannot tell which zone the
/// time is in, which is the whole failure this feature exists to fix.
///
/// Microsecond precision is kept even though it is noisier to read, because a
/// truncated date is a trap: it looks like a valid input, and feeding it back
/// resolves to the commit BEFORE the one it came from — a silently wrong read
/// rather than an error.
pub(crate) fn format_instant(micros: u64) -> String {
    i64::try_from(micros)
        .ok()
        .and_then(DateTime::from_timestamp_micros)
        .map_or_else(
            || micros.to_string(),
            |utc| {
                utc.with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S%.6f %:z")
                    .to_string()
            },
        )
}

/// Renders an instant as a UTC date to the microsecond, for a table cell
/// (output contract Q3: `2026-09-10 20:19:44.123456 UTC`).
///
/// UTC, so the cell is the same on every machine and in the browser; labelled,
/// so the reader knows which zone it is in; to the microsecond, because a cell
/// is fed back — read history, hand a date off a row to `--as-of-time` — and a
/// truncated date resolves to the commit BEFORE the one it came from. What the
/// tool shows, `parse_instant` accepts. An instant past what a date can hold
/// prints its digits.
pub(crate) fn format_utc_instant(micros: u64) -> String {
    i64::try_from(micros)
        .ok()
        .and_then(DateTime::from_timestamp_micros)
        .map_or_else(
            || micros.to_string(),
            |utc| utc.format("%Y-%m-%d %H:%M:%S%.6f UTC").to_string(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An offset-bearing string names one instant, so it must parse to the same
    /// number no matter where the machine running the CLI happens to be.
    #[test]
    fn an_explicit_offset_is_honored_exactly() {
        let utc = parse_instant("2026-09-05T15:00:00Z").expect("parses");
        assert_eq!(utc, 1_788_620_400_000_000);

        // The same instant, written from a +05:30 desk.
        assert_eq!(
            parse_instant("2026-09-05T20:30:00+05:30").expect("parses"),
            utc,
            "the same instant written two ways must resolve identically"
        );

        // And a different offset is a genuinely different instant.
        assert_ne!(
            parse_instant("2026-09-05T15:00:00+01:00").expect("parses"),
            utc
        );
    }

    /// A bare date-time is read locally, so it agrees with the same wall-clock
    /// reading converted through the local zone — whatever that zone is on the
    /// machine running the test.
    #[test]
    fn a_bare_date_time_is_read_in_local_time() {
        let parsed = parse_instant("2026-09-05 15:00:00").expect("parses");
        let expected = Local
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2026, 9, 5)
                    .expect("valid date")
                    .and_hms_opt(15, 0, 0)
                    .expect("valid time"),
            )
            .earliest()
            .expect("2026-09-05 15:00 exists in every real zone");
        assert_eq!(
            parsed,
            u64::try_from(expected.timestamp_micros()).expect("after 1970")
        );
    }

    /// The separator and the precision are conveniences, not different
    /// meanings: every spelling of one reading resolves to one instant.
    #[test]
    fn accepted_spellings_of_the_same_reading_agree() {
        let base = parse_instant("2026-09-05 15:00:00").expect("parses");
        for spelling in [
            "2026-09-05T15:00:00",
            "2026-09-05 15:00",
            "2026-09-05T15:00",
            "  2026-09-05 15:00:00  ",
        ] {
            assert_eq!(
                parse_instant(spelling).expect("parses"),
                base,
                "{spelling} should mean the same instant"
            );
        }

        // A bare date is local midnight, which is strictly before 15:00.
        assert!(parse_instant("2026-09-05").expect("parses") < base);
    }

    /// Raw micros round-trip, so a value copied out of JSON output can be
    /// pasted straight back in.
    #[test]
    fn raw_epoch_microseconds_pass_through() {
        assert_eq!(
            parse_instant("1788732596132961").expect("parses"),
            1_788_732_596_132_961
        );
    }

    /// A bare integer cannot shadow a date, because no date spelling is all
    /// digits — the two input languages do not overlap.
    #[test]
    fn digits_and_dates_do_not_overlap() {
        assert!(parse_instant("2026").is_ok(), "all-digit input is micros");
        assert_ne!(
            parse_instant("2026").expect("parses"),
            parse_instant("2026-01-01").expect("parses"),
            "`2026` is 2026 microseconds after the epoch, not the year"
        );
    }

    /// Refusals name what was wrong and show a spelling that works, because a
    /// bare "invalid input" leaves the reader guessing which of several
    /// plausible formats the tool wanted.
    #[test]
    fn unparseable_input_is_refused_with_a_usable_message() {
        for bad in ["", "  ", "yesterday", "3pm", "2026-13-01", "not a time"] {
            let error = parse_instant(bad).expect_err("must be refused");
            assert!(
                error.contains("2026-09-05"),
                "the refusal for {bad:?} should show a working example: {error}"
            );
        }
    }

    /// Pre-epoch times have no representation in an unsigned instant, and say
    /// so rather than wrapping into an enormous future date.
    #[test]
    fn a_pre_epoch_time_is_refused_rather_than_wrapped() {
        let error = parse_instant("1969-07-20T20:17:00Z").expect_err("must be refused");
        assert!(error.contains("before 1970"), "got: {error}");
    }

    /// Formatting always carries the offset, so a rendered time is never
    /// ambiguous about which zone it is in.
    #[test]
    fn formatting_shows_the_zone_offset() {
        let rendered = format_instant(1_788_732_596_132_961);
        assert!(
            rendered.contains('+') || rendered.contains('-'),
            "rendered time must carry its offset: {rendered}"
        );
        assert!(rendered.starts_with("20"), "got: {rendered}");
    }

    /// Parsing and formatting are inverses at second precision, so a time read
    /// off human output can be typed back in.
    #[test]
    fn a_rendered_time_parses_back_to_the_same_second() {
        let original = 1_788_732_596_132_961;
        let rendered = format_instant(original);
        let reparsed = parse_instant(&rendered).expect("rendered output is accepted input");
        assert_eq!(
            reparsed, original,
            "rendered {rendered} must name the SAME instant, to the microsecond — \
             a truncated round trip resolves to the previous commit"
        );
    }

    /// A table cell is the same UTC instant on every machine, to the
    /// microsecond, labelled so the zone is never in question.
    #[test]
    fn a_table_cell_is_a_labelled_utc_instant() {
        assert_eq!(
            format_utc_instant(1_789_071_584_000_000),
            "2026-09-10 20:19:44.000000 UTC"
        );
        assert_eq!(
            format_utc_instant(1_789_071_584_999_999),
            "2026-09-10 20:19:44.999999 UTC",
            "every microsecond is shown"
        );
        assert_eq!(format_utc_instant(0), "1970-01-01 00:00:00.000000 UTC");
        assert_eq!(
            format_utc_instant(u64::MAX),
            u64::MAX.to_string(),
            "an instant no date can hold prints its digits"
        );
    }

    /// The spelling a cell prints is accepted back and names the SAME instant,
    /// so a date read off a history row is a read bound for that very commit.
    #[test]
    fn a_table_cell_parses_back_to_the_same_microsecond() {
        let original = 1_788_732_596_132_961;
        let rendered = format_utc_instant(original);
        assert_eq!(
            parse_instant(&rendered).expect("a printed cell is accepted input"),
            original,
            "{rendered} must round-trip to the microsecond"
        );
    }

    /// The `UTC` label is read as UTC, not as the machine's local zone — it
    /// means the same instant as the `Z` spelling of the same reading, on every
    /// machine, whatever a bare reading would have meant there.
    #[test]
    fn a_utc_labelled_reading_is_utc_not_local() {
        assert_eq!(
            parse_instant("2026-09-05 15:00:00.000000 UTC").expect("parses"),
            parse_instant("2026-09-05T15:00:00Z").expect("parses")
        );
        assert_eq!(
            parse_instant("2026-09-05 15:00 UTC").expect("parses"),
            parse_instant("2026-09-05T15:00:00Z").expect("parses"),
            "the label works with every accepted bare spelling"
        );
        assert!(
            parse_instant("2026-09-05 15:00:00 PDT").is_err(),
            "only the label the tool prints is a label the tool reads"
        );
    }
}
