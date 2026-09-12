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
    pub(crate) fn human(&self) -> String {
        let widths: Vec<usize> = (0..self.headers.len())
            .map(|column| {
                self.rows
                    .iter()
                    .map(|row| row[column].text.chars().count())
                    .chain(std::iter::once(self.headers[column].chars().count()))
                    .max()
                    .unwrap_or(0)
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
        let padding = widths[column].saturating_sub(text.chars().count());
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
    use super::{Cell, Table};

    fn text(value: &str) -> Cell {
        Cell::text(value.to_owned())
    }

    fn number(value: &str) -> Cell {
        Cell::number(value.to_owned())
    }

    fn headers(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
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
