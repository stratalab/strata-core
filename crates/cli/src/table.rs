//! The one layout for a list of records (output contract R1-table / R4).
//!
//! A reader gets an UPPERCASE header, two-space gutters, and numbers
//! right-aligned so magnitudes line up; a script gets the same cells
//! tab-separated with no header. The cells themselves — what a date, a byte
//! string or a float looks like — are decided by the renderer that fills the
//! table; this module only lays them out. Wasm-safe.

/// What a cell holds, for alignment: a column whose every cell is a number
/// (or a null standing in for one) right-aligns; every other column reads
/// left to right.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CellKind {
    /// Text, JSON, a date: left-aligned.
    Text,
    /// A number on the wire: right-aligned with the rest of its column.
    Number,
    /// Null or absent: follows the alignment of the cells around it.
    Null,
}

/// One presented cell. The text is final — escaped, formatted — by the time
/// it gets here; the kind only decides which way its column lines up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Cell {
    pub(crate) text: String,
    pub(crate) kind: CellKind,
}

impl Cell {
    pub(crate) fn text(text: String) -> Self {
        Self {
            text,
            kind: CellKind::Text,
        }
    }

    pub(crate) fn number(text: String) -> Self {
        Self {
            text,
            kind: CellKind::Number,
        }
    }

    pub(crate) fn null(text: &str) -> Self {
        Self {
            text: text.to_owned(),
            kind: CellKind::Null,
        }
    }
}

/// A header and its rows, every row as wide as the header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<Cell>>,
}

/// The widest a column pads to. A table is read in a terminal, and no terminal
/// is this wide, so beyond it padding buys nothing — while costing every row in
/// the table. One 60,000-character key in a thousand-row scan rendered 60 MB of
/// mostly spaces, against 147 KB of JSON for the same answer, built entirely in
/// memory and on the browser's path too (#3358 F4).
///
/// A value wider than the cap is never truncated: human output hides nothing
/// that `--raw` would show. It simply stops being something the other rows
/// align to.
const COLUMN_WIDTH_CAP: usize = 160;

/// How many terminal columns a cell occupies.
///
/// Not the number of characters: a CJK ideograph occupies two columns and a
/// combining mark none, so counting scalars misaligns every row after one
/// (#3358 F14).
fn display_width(text: &str) -> usize {
    unicode_width::UnicodeWidthStr::width(text)
}

impl Table {
    pub(crate) const fn new(headers: Vec<String>) -> Self {
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    /// Adds a row. A row narrower or wider than the header is a renderer bug
    /// — the declaration names one cell per column — not a data shape.
    pub(crate) fn push(&mut self, row: Vec<Cell>) {
        assert_eq!(
            row.len(),
            self.headers.len(),
            "a table row carries one cell per header"
        );
        self.rows.push(row);
    }

    /// The reader's layout: the header, then every row, columns padded to the
    /// wider of the header and the widest cell, two spaces between columns,
    /// no trailing whitespace, every line newline-terminated.
    ///
    /// A column pads to [`COLUMN_WIDTH_CAP`] at most. A value wider than that
    /// still prints in full and pushes the rest of *its own* row right; what
    /// the cap prevents is every other row being padded out to match it.
    pub(crate) fn human(&self) -> String {
        let widths: Vec<usize> = (0..self.headers.len())
            .map(|column| {
                self.rows
                    .iter()
                    .map(|row| display_width(&row[column].text))
                    .chain(std::iter::once(display_width(&self.headers[column])))
                    .max()
                    .unwrap_or(0)
                    .min(COLUMN_WIDTH_CAP)
            })
            .collect();
        let right_aligned: Vec<bool> = (0..self.headers.len())
            .map(|column| {
                let kinds = || self.rows.iter().map(|row| row[column].kind);
                kinds().any(|kind| kind == CellKind::Number)
                    && kinds().all(|kind| matches!(kind, CellKind::Number | CellKind::Null))
            })
            .collect();
        let mut out = String::new();
        // The header reads left to right whatever its column holds: a label
        // over a number column marks where the column starts.
        push_line(
            &mut out,
            self.headers.iter().map(|header| (header.as_str(), false)),
            &widths,
        );
        for row in &self.rows {
            push_line(
                &mut out,
                row.iter()
                    .zip(&right_aligned)
                    .map(|(cell, right)| (cell.text.as_str(), *right)),
                &widths,
            );
        }
        out
    }

    /// The script's layout: every row tab-separated, no header, no padding,
    /// every line newline-terminated.
    pub(crate) fn raw(&self) -> String {
        let mut out = String::new();
        for row in &self.rows {
            let cells: Vec<&str> = row.iter().map(|cell| cell.text.as_str()).collect();
            out.push_str(&cells.join("\t"));
            out.push('\n');
        }
        out
    }
}

/// One padded line. A left-aligned cell is padded after its text, a
/// right-aligned one before; whatever the last column leaves behind — its
/// own padding, or the gutter before an empty cell — is trimmed, so no line
/// ends in whitespace.
fn push_line<'a>(out: &mut String, cells: impl Iterator<Item = (&'a str, bool)>, widths: &[usize]) {
    let mut line = String::new();
    for (column, (text, right)) in cells.enumerate() {
        if column > 0 {
            line.push_str("  ");
        }
        let padding = widths[column].saturating_sub(display_width(text));
        if right {
            line.extend(std::iter::repeat_n(' ', padding));
            line.push_str(text);
        } else {
            line.push_str(text);
            line.extend(std::iter::repeat_n(' ', padding));
        }
    }
    let trimmed = line.trim_end().len();
    line.truncate(trimmed);
    line.push('\n');
    out.push_str(&line);
}

#[cfg(test)]
mod tests {
    use super::{display_width, Cell, Table, COLUMN_WIDTH_CAP};

    fn text(value: &str) -> Cell {
        Cell::text(value.to_owned())
    }

    fn number(value: &str) -> Cell {
        Cell::number(value.to_owned())
    }

    fn headers(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    /// #3358 F4: one wide value used to be paid for by every other row. A
    /// thousand-row scan holding a single 60,000-character key rendered 60 MB,
    /// nearly all of it padding, against 147 KB of JSON for the same answer.
    #[test]
    fn one_wide_value_does_not_pad_the_rest_of_the_table() {
        let wide = "w".repeat(60_000);
        let mut table = Table::new(headers(&["KEY", "VERSION", "VALUE"]));
        table.push(vec![text(&wide), number("1"), text("wide")]);
        for row in 0..999 {
            table.push(vec![text(&format!("k{row:04}")), number("2"), text("v")]);
        }
        let rendered = table.human();

        // Printed in full: human output hides nothing `--raw` would show.
        assert!(rendered.contains(&wide));
        // But it is not what the other thousand rows align to. Without the cap
        // this is above 60 MB.
        assert!(
            rendered.len() < 300_000,
            "rendered {} bytes; the cap is not holding",
            rendered.len()
        );
        let ordinary = rendered
            .lines()
            .find(|line| line.starts_with("k0000"))
            .expect("an ordinary row");
        assert!(ordinary.len() < COLUMN_WIDTH_CAP + 32, "{ordinary:?}");
    }

    /// #3358 F14: alignment is terminal columns, not Unicode scalars.
    ///
    /// Measured with `unicode_width` directly rather than through
    /// [`display_width`]: a test that measures with the same ruler it is
    /// checking passes whatever that ruler says, which is exactly how the
    /// first version of this test stayed green against `chars().count()`.
    #[test]
    fn a_wide_character_occupies_the_columns_it_draws() {
        let mut table = Table::new(headers(&["KEY", "VALUE"]));
        table.push(vec![text("ab"), text("ascii")]);
        table.push(vec![text("東京"), text("cjk")]);
        let rendered = table.human();
        let value_starts: Vec<usize> = rendered
            .lines()
            .skip(1)
            .map(|line| {
                let value = line.rsplit("  ").next().unwrap_or("");
                unicode_width::UnicodeWidthStr::width(line)
                    - unicode_width::UnicodeWidthStr::width(value)
            })
            .collect();
        assert_eq!(
            value_starts[0], value_starts[1],
            "the value column starts in a different place on each row:\n{rendered}"
        );
    }

    #[test]
    fn width_counts_columns_not_characters() {
        assert_eq!(display_width("ab"), 2);
        assert_eq!(display_width("東京"), 4, "two ideographs, four columns");
        assert_eq!(
            display_width("e\u{301}"),
            1,
            "a combining mark draws nothing"
        );
        assert_eq!(display_width(""), 0);
    }

    #[test]
    fn text_pads_right_and_numbers_pad_left_to_the_widest_of_header_and_cell() {
        let mut table = Table::new(headers(&["KEY", "VERSION", "VALUE"]));
        table.push(vec![text("a"), number("3"), text("one")]);
        table.push(vec![text("longer"), number("12"), text("two")]);
        assert_eq!(
            table.human(),
            "KEY     VERSION  VALUE\n\
             a             3  one\n\
             longer       12  two\n"
        );
        assert_eq!(table.raw(), "a\t3\tone\nlonger\t12\ttwo\n");
    }

    #[test]
    fn a_number_column_wider_than_its_header_right_aligns_the_header_column_too() {
        let mut table = Table::new(headers(&["NODE", "RANK"]));
        table.push(vec![text("c"), number("0.474412")]);
        table.push(vec![text("a"), number("0.31746")]);
        assert_eq!(
            table.human(),
            "NODE  RANK\n\
             c     0.474412\n\
             a      0.31746\n",
            "the header stays left-aligned; the numbers line up on the right"
        );
    }

    #[test]
    fn a_null_follows_its_column_and_a_mixed_column_reads_left_to_right() {
        let mut numbers = Table::new(headers(&["VERSION", "COMMITTED_AT"]));
        numbers.push(vec![number("4"), Cell::null("-")]);
        numbers.push(vec![
            Cell::null("-"),
            text("2026-09-10 20:19:44.000000 UTC"),
        ]);
        assert_eq!(
            numbers.human(),
            concat!(
                "VERSION  COMMITTED_AT\n",
                "      4  -\n",
                "      -  2026-09-10 20:19:44.000000 UTC\n",
            ),
            "a null in a number column right-aligns; a null in a text column does not"
        );
        // Headers wider than the cells, so the alignment is visible.
        let mut mixed = Table::new(headers(&["NUMBER"]));
        mixed.push(vec![number("1")]);
        mixed.push(vec![text("x")]);
        assert_eq!(
            mixed.human(),
            "NUMBER\n1\nx\n",
            "one text cell makes the whole column read left to right"
        );
        let mut nulls = Table::new(headers(&["NUMBER"]));
        nulls.push(vec![Cell::null("-")]);
        assert_eq!(
            nulls.human(),
            "NUMBER\n-\n",
            "a column of nulls is not numeric"
        );
    }

    #[test]
    fn no_line_ends_in_whitespace_and_width_counts_characters_not_bytes() {
        let mut table = Table::new(headers(&["NAME", "PARENT"]));
        table.push(vec![text("défaut"), Cell::null("")]);
        table.push(vec![text("a"), text("b")]);
        let human = table.human();
        for line in human.lines() {
            assert_eq!(line, line.trim_end(), "{line:?}");
        }
        assert_eq!(human, "NAME    PARENT\ndéfaut\na       b\n");
        assert_eq!(table.raw(), "défaut\t\na\tb\n");
    }

    #[test]
    fn an_empty_table_is_just_its_header_for_a_reader_and_nothing_for_a_script() {
        let table = Table::new(headers(&["KEY", "SCORE"]));
        assert_eq!(table.human(), "KEY  SCORE\n");
        assert_eq!(table.raw(), "");
    }

    #[test]
    #[should_panic(expected = "one cell per header")]
    fn a_row_of_the_wrong_width_is_a_renderer_bug() {
        let mut table = Table::new(headers(&["A", "B"]));
        table.push(vec![text("only one")]);
    }
}
