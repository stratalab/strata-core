//! String-ordered, page-bounded scans over a length-prefixed text component
//! (#3458, #3473, #3489).
//!
//! Row keys encode each text component as `u16 len || bytes`, so byte order
//! over a prefix is length-major: every one-byte value sorts before every
//! two-byte value. The paginated graph reads order their pages by the
//! component's *string* order and take a cursor that is a real position in
//! that order, so a page cannot be one seek. Within one length bucket, though,
//! the two orders agree, and a bucket is a contiguous key range — so a
//! string-ordered page after a cursor is a merge over the present length
//! buckets, each sought to the first value past the cursor and read only as
//! far as the page needs. Work is proportional to the page times the number
//! of distinct value lengths present, never to the prefix.
//!
//! Every seek target here is a pure function of the cursor bytes and the key
//! layout — never of the version being read — so a cursor names the same
//! position at every `as_of`.

use std::cmp::Ordering;
use std::collections::VecDeque;

use strata_core::BranchId;

use crate::diagnostics::EngineError;

use super::adapter::{PersistenceReadRow, StoragePersistence};
use super::key::{
    exclusive_after_key, next_prefix, next_text_component, text_bucket_key, text_bucket_start,
};
use super::row::ReadSelector;
use super::space::RowClass;

/// The longest text component any row key carries (a node id).
const MAX_TEXT_LEN: usize = 1024;

/// A string-ordered view of the text component that follows `fixed_prefix`
/// in one row class.
pub(crate) struct OrderedTextScan<'a> {
    persistence: &'a StoragePersistence,
    branch_id: BranchId,
    row_class: RowClass,
    fixed_prefix: Vec<u8>,
    selector: ReadSelector,
    /// The row class's own key-corruption code, raised when a returned key
    /// does not carry the component its prefix promises — so a malformed
    /// index key is reported exactly as the class's decoder would report it.
    malformed_key_code: &'static str,
}

impl<'a> OrderedTextScan<'a> {
    pub(crate) const fn new(
        persistence: &'a StoragePersistence,
        branch_id: BranchId,
        row_class: RowClass,
        fixed_prefix: Vec<u8>,
        selector: ReadSelector,
        malformed_key_code: &'static str,
    ) -> Self {
        Self {
            persistence,
            branch_id,
            row_class,
            fixed_prefix,
            selector,
            malformed_key_code,
        }
    }

    /// Up to `limit` visible rows whose component follows `after` in string
    /// order (all of them when `after` is `None`) and, when given, starts
    /// with `required_prefix` — in string order of the component. Tombstones
    /// are scanned past, never returned.
    pub(crate) fn rows_after(
        &self,
        after: Option<&[u8]>,
        required_prefix: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut buckets = self.open_buckets(after, false, required_prefix)?;
        let mut rows = Vec::with_capacity(limit);
        while rows.len() < limit {
            let Some(index) = self.fill_and_pick(&mut buckets, limit - rows.len())? else {
                break;
            };
            rows.push(buckets[index].pop());
        }
        Ok(rows)
    }

    /// Up to `limit` distinct component values at or after `from` (strictly
    /// after it when `exclusive`), in string order, each with the row it was
    /// discovered from. A value's rows are contiguous, so after yielding one
    /// the scan seeks past every row that shares it — one seek per distinct
    /// value, regardless of how many rows each has. Only live rows count: a
    /// value whose rows are all tombstones is not a value.
    ///
    /// The discovering row is handed back so the caller can run it through
    /// its decoder: a row this scan touched is still a row the read touched,
    /// and corruption in it must surface as it would from any other read.
    pub(crate) fn distinct_values(
        &self,
        from: Option<&[u8]>,
        exclusive: bool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PersistenceReadRow)>, EngineError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut buckets = self.open_buckets(from, !exclusive, None)?;
        let mut values = Vec::with_capacity(limit);
        while values.len() < limit {
            let Some(index) = self.fill_and_pick(&mut buckets, 1)? else {
                break;
            };
            let row = buckets[index].pop();
            let value = self.component(&row)?.to_vec();
            buckets[index].skip_past_value(&self.fixed_prefix, &value);
            values.push((value, row));
        }
        Ok(values)
    }

    /// The component bytes of a row this scan returned.
    fn component<'r>(&self, row: &'r PersistenceReadRow) -> Result<&'r [u8], EngineError> {
        next_text_component(row.key(), &self.fixed_prefix).ok_or_else(|| {
            EngineError::corruption(
                self.malformed_key_code,
                "stored row key does not carry the text component its prefix promises",
            )
        })
    }

    /// One cursor per length bucket that holds rows, each positioned at the
    /// first value the page may return. Buckets are discovered by probing
    /// from each length upward: a probe lands on the first row of the next
    /// bucket that exists, so absent lengths cost nothing. A probe row that
    /// lies inside the page's range is the page's first row from that
    /// bucket, and is kept rather than sought again — which is also what
    /// keeps every row a page reads on the decode path a caller validates.
    fn open_buckets(
        &self,
        bound: Option<&[u8]>,
        inclusive: bool,
        required_prefix: Option<&[u8]>,
    ) -> Result<Vec<BucketCursor>, EngineError> {
        let prefix_end = next_prefix(&self.fixed_prefix);
        let mut buckets = Vec::new();
        let mut len = 1;
        while len <= MAX_TEXT_LEN {
            let probe = self.scan(
                text_bucket_start(&self.fixed_prefix, len),
                prefix_end.clone(),
                1,
            )?;
            let Some(first) = probe.into_iter().next() else {
                break;
            };
            let present = self.component(&first)?.len();
            if let Some((start, end)) =
                self.bucket_range(present, bound, inclusive, required_prefix)
            {
                let in_range = (start.as_slice()..end.as_slice()).contains(&first.key());
                buckets.push(if in_range {
                    BucketCursor::seeded(first, end)
                } else {
                    BucketCursor::new(start, end)
                });
            }
            len = present + 1;
        }
        Ok(buckets)
    }

    /// The key range a page reads within the `len` bucket, or `None` when no
    /// value of that length can carry the required prefix. The range may be
    /// empty (start at or past end); a cursor over it simply finds nothing.
    fn bucket_range(
        &self,
        len: usize,
        bound: Option<&[u8]>,
        inclusive: bool,
        required_prefix: Option<&[u8]>,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut start = text_bucket_start(&self.fixed_prefix, len);
        let mut end = next_prefix(&start);
        if let Some(bound) = bound {
            start = start.max(bucket_seek_start(&self.fixed_prefix, len, bound, inclusive));
        }
        if let Some(required) = required_prefix {
            if len < required.len() {
                return None;
            }
            start = start.max(text_bucket_key(&self.fixed_prefix, len, required, 0x00));
            // Past every row of the largest value with this prefix — rows may
            // carry further components after it, so `next_prefix`, not a
            // trailing zero byte.
            end = end.min(next_prefix(&text_bucket_key(
                &self.fixed_prefix,
                len,
                required,
                0xFF,
            )));
        }
        Some((start, end))
    }

    /// Tops up every bucket that has run dry, then picks the bucket whose
    /// head value is smallest in string order. `None` once all are drained.
    fn fill_and_pick(
        &self,
        buckets: &mut [BucketCursor],
        want: usize,
    ) -> Result<Option<usize>, EngineError> {
        for bucket in buckets.iter_mut() {
            bucket.fill(self, want)?;
        }
        let mut heads = Vec::with_capacity(buckets.len());
        for (index, bucket) in buckets.iter().enumerate() {
            if let Some(head) = bucket.peek() {
                heads.push((index, self.component(head)?));
            }
        }
        // Distinct lengths never tie, so the minimum is unique.
        Ok(heads
            .into_iter()
            .min_by(|left, right| left.1.cmp(right.1))
            .map(|(index, _)| index))
    }

    fn scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<PersistenceReadRow>, EngineError> {
        self.persistence.scan_range(
            self.branch_id,
            self.row_class,
            Some(start),
            Some(end),
            self.selector,
            Some(limit),
        )
    }
}

/// Where the values of one length bucket that follow `bound` begin — at or
/// after it when `inclusive`, strictly after it otherwise. String order
/// against a value of another length reduces to a comparison on the shared
/// length: a shorter value follows `bound` only by exceeding `bound`'s own
/// prefix of that length (equal would make it a prefix of `bound`, hence
/// before it); a longer value follows `bound` whenever its leading bytes are
/// at least `bound`, the smallest such being `bound` padded with zeros.
///
/// "Strictly after a value" is `next_prefix` of the value's key, never the
/// key plus a zero byte: the component may be followed by further components
/// (an edge type by its `dst`), and every row of the value sorts after the
/// value's key alone — only `next_prefix` steps past all of them.
fn bucket_seek_start(fixed_prefix: &[u8], len: usize, bound: &[u8], inclusive: bool) -> Vec<u8> {
    match len.cmp(&bound.len()) {
        Ordering::Equal if inclusive => text_bucket_key(fixed_prefix, len, bound, 0x00),
        Ordering::Equal => next_prefix(&text_bucket_key(fixed_prefix, len, bound, 0x00)),
        Ordering::Less => next_prefix(&text_bucket_key(fixed_prefix, len, &bound[..len], 0x00)),
        Ordering::Greater => text_bucket_key(fixed_prefix, len, bound, 0x00),
    }
}

/// A re-seeking cursor over one bucket's key range: each refill scans from
/// where the last raw page ended, so tombstones are stepped over rather than
/// counted against the page.
struct BucketCursor {
    start: Vec<u8>,
    end: Vec<u8>,
    pending: VecDeque<PersistenceReadRow>,
    exhausted: bool,
}

impl BucketCursor {
    const fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self {
            start,
            end,
            pending: VecDeque::new(),
            exhausted: false,
        }
    }

    /// A cursor already holding its first row, resuming just past it.
    fn seeded(first: PersistenceReadRow, end: Vec<u8>) -> Self {
        let start = exclusive_after_key(first.key());
        let mut pending = VecDeque::new();
        if !first.is_tombstone() {
            pending.push_back(first);
        }
        Self {
            start,
            end,
            pending,
            exhausted: false,
        }
    }

    /// Scans until at least one live row is pending or the range is spent.
    fn fill(&mut self, scan: &OrderedTextScan<'_>, want: usize) -> Result<(), EngineError> {
        while self.pending.is_empty() && !self.exhausted {
            if self.start >= self.end {
                self.exhausted = true;
                break;
            }
            let rows = scan.scan(self.start.clone(), self.end.clone(), want.max(1))?;
            let Some(last) = rows.last() else {
                self.exhausted = true;
                break;
            };
            self.start = exclusive_after_key(last.key());
            self.pending
                .extend(rows.into_iter().filter(|row| !row.is_tombstone()));
        }
        Ok(())
    }

    fn peek(&self) -> Option<&PersistenceReadRow> {
        self.pending.front()
    }

    fn pop(&mut self) -> PersistenceReadRow {
        self.pending
            .pop_front()
            .expect("a picked bucket has a pending row")
    }

    /// Drops everything pending and resumes past every row sharing `value`.
    fn skip_past_value(&mut self, fixed_prefix: &[u8], value: &[u8]) {
        self.pending.clear();
        let through_value = text_bucket_key(fixed_prefix, value.len(), value, 0x00);
        self.start = self.start.clone().max(next_prefix(&through_value));
    }
}

/// Killers the mutation gate can see. The gate compiles this crate's own
/// `cfg(test)` modules but not testkit-gated integration tests, so the exact
/// cost of a page, the scan counter and reset behind it, and the
/// read-corruption injector are pinned here through the public read path.
#[cfg(test)]
mod scan_work_tests {
    use super::text_bucket_key;
    use crate::api::{CacheOpenOptions, Database};
    use crate::branch::BranchName;
    use crate::data::graph::{GraphEdgeData, GraphEdgeType, GraphName, GraphNodeData, GraphNodeId};
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::RowCorruption;

    fn open() -> Database {
        Database::open_cache(CacheOpenOptions::new())
            .expect("cache open")
            .into_database()
    }

    fn graph_name() -> GraphName {
        GraphName::new("g").expect("graph name")
    }

    fn node(id: &str) -> GraphNodeId {
        GraphNodeId::new(id).expect("node id")
    }

    fn scope() -> (BranchName, ProductSpace) {
        (
            BranchName::new("default").expect("branch"),
            ProductSpace::new("default").expect("space"),
        )
    }

    /// `count` nodes `n000..`, all four bytes long — one id-length bucket.
    fn seeded_nodes(count: usize) -> Database {
        let db = open();
        let (branch, space) = scope();
        let mut graph = db.graph(&branch, &space).expect("graph opens");
        graph.create_graph(graph_name()).expect("graph create");
        let nodes: Vec<_> = (0..count)
            .map(|index| (node(&format!("n{index:03}")), GraphNodeData::default()))
            .collect();
        graph
            .bulk_insert(&graph_name(), &nodes, &[], None)
            .expect("ingest");
        db
    }

    /// A first page keeps the bucket-probe row as its first row, so it costs
    /// `limit + 1` scan rows (its rows plus the lookahead); a cursor page's
    /// probe lands before the cursor and is discarded, so it costs `limit + 2`.
    /// Pinned exactly, not bounded: the numbers are what the seek design
    /// promises, and every extra row is a mutant.
    #[test]
    fn a_page_costs_exactly_its_rows_the_lookahead_and_the_probe() {
        let db = seeded_nodes(10);
        let (branch, space) = scope();
        let graph = db.graph(&branch, &space).expect("graph opens");

        db.reset_scanned_rows_for_test();
        let first = graph
            .list_nodes(&graph_name(), None, None, 3)
            .expect("first page");
        assert_eq!(first.nodes().len(), 3);
        assert_eq!(
            db.scanned_rows_for_test(),
            4,
            "first page: 3 rows + lookahead"
        );

        db.reset_scanned_rows_for_test();
        assert_eq!(db.scanned_rows_for_test(), 0, "reset zeroes the counter");
        let second = graph
            .list_nodes(&graph_name(), None, first.cursor(), 3)
            .expect("second page");
        assert_eq!(second.nodes().len(), 3);
        assert_eq!(
            db.scanned_rows_for_test(),
            5,
            "cursor page: discarded probe + 3 rows + lookahead"
        );
    }

    /// An unfiltered neighbor page enumerates each distinct edge type with one
    /// probe and reads each type's page with one seek — and enumerates no
    /// more types than the page can use. The counts are exact.
    #[test]
    fn an_unfiltered_neighbor_page_costs_one_probe_per_type_plus_its_rows() {
        let db = open();
        let (branch, space) = scope();
        let mut graph = db.graph(&branch, &space).expect("graph opens");
        graph.create_graph(graph_name()).expect("graph create");
        let nodes: Vec<_> = ["hub", "x1", "x2", "x3", "x4", "x5"]
            .iter()
            .map(|id| (node(id), GraphNodeData::default()))
            .collect();
        let edges: Vec<_> = [
            ("a", "x1"),
            ("b", "x2"),
            ("c", "x3"),
            ("d", "x4"),
            ("e", "x5"),
        ]
        .iter()
        .map(|(kind, dst)| {
            (
                node("hub"),
                GraphEdgeType::new(*kind).expect("edge type"),
                node(dst),
                GraphEdgeData::new(1.0, None).expect("edge data"),
            )
        })
        .collect();
        graph
            .bulk_insert(&graph_name(), &nodes, &edges, None)
            .expect("ingest");
        let page = |limit: usize| {
            db.reset_scanned_rows_for_test();
            let page = graph
                .neighbors(
                    &graph_name(),
                    &node("hub"),
                    crate::data::graph::GraphDirection::Outgoing,
                    None,
                    None,
                    limit,
                )
                .expect("page");
            (page.neighbors().len(), db.scanned_rows_for_test())
        };

        // A page of 2 wants 3 hits. Type enumeration stops at three types —
        // the probe seeds `a`, then one row each for `b` and `c` (3) — and
        // `d`, `e` are never touched. Each type's rows: one probe that is the
        // type's only row (3).
        assert_eq!(page(2), (2, 6));
        // A page of 10 takes every type: probe plus four rows (5), then one
        // probe per type (5).
        assert_eq!(page(10), (5, 10));
    }

    /// The read-corruption injector arms a real read: armed at zero, the very
    /// next point read comes back corrupt; unarmed, the same read is clean.
    #[test]
    fn a_read_corruption_armed_after_zero_reads_corrupts_the_next_read() {
        let mut db = seeded_nodes(1);
        let (branch, space) = scope();
        db.inject_read_corruption_after_for_test(0, RowCorruption::SetValue(vec![0xFF, 0x00]));
        let error = db
            .graph(&branch, &space)
            .expect("graph opens")
            .get_node(&graph_name(), &node("n000"))
            .expect_err("the next point read is corrupted");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        db.graph(&branch, &space)
            .expect("graph opens")
            .get_node(&graph_name(), &node("n000"))
            .expect("unarmed, the read is clean");
    }

    /// A bucket key is the prefix, the two-byte length, the bytes, and exactly
    /// enough fill to reach that length.
    #[test]
    fn text_bucket_key_pads_to_the_bucket_length_exactly() {
        assert_eq!(
            text_bucket_key(b"P", 2, b"aa", 0x00),
            [b'P', 0, 2, b'a', b'a']
        );
        assert_eq!(
            text_bucket_key(b"P", 3, b"a", 0xFF),
            [b'P', 0, 3, b'a', 0xFF, 0xFF]
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &[u8] = b"P";

    fn key(len: usize, bytes: &[u8], fill: u8) -> Vec<u8> {
        text_bucket_key(PREFIX, len, bytes, fill)
    }

    /// A value of the cursor's own length resumes strictly after the cursor
    /// — past every row of it, however the key continues — or at it when
    /// inclusive.
    #[test]
    fn same_length_bucket_seeks_past_or_at_the_bound() {
        let exclusive = bucket_seek_start(PREFIX, 2, b"aa", false);
        assert_eq!(exclusive, next_prefix(&key(2, b"aa", 0)));
        assert!(
            exclusive > continued(key(2, b"aa", 0)),
            "a continued `aa` row is excluded"
        );
        assert!(exclusive <= key(2, b"ab", 0), "the next value is included");
        assert_eq!(bucket_seek_start(PREFIX, 2, b"aa", true), key(2, b"aa", 0));
    }

    /// A row key that carries another component after the value — the shape
    /// of an adjacency key, where an edge type is followed by its `dst`.
    fn continued(mut value_key: Vec<u8>) -> Vec<u8> {
        value_key.extend(key(2, b"o3", 0)[PREFIX.len()..].iter());
        value_key
    }

    /// A shorter value follows the bound only by exceeding the bound's prefix
    /// of its length — `"b"` follows `"aa"`, `"a"` does not — inclusive or not.
    #[test]
    fn shorter_bucket_seeks_past_the_bounds_own_prefix() {
        let expected = next_prefix(&key(1, b"a", 0));
        assert_eq!(bucket_seek_start(PREFIX, 1, b"aa", false), expected);
        assert_eq!(bucket_seek_start(PREFIX, 1, b"aa", true), expected);
        assert!(
            expected > continued(key(1, b"a", 0)),
            "a continued `a` row is excluded"
        );
        assert!(expected <= key(1, b"b", 0));
    }

    /// A longer value follows the bound from the bound padded with zeros —
    /// `"aaa"` follows `"aa"`, `"ab"`-prefixed values follow, `"a"`-only ones
    /// do not — inclusive or not.
    #[test]
    fn longer_bucket_seeks_from_the_bound_padded_with_zeros() {
        let expected = key(3, b"aa", 0x00);
        assert_eq!(bucket_seek_start(PREFIX, 3, b"aa", false), expected);
        assert_eq!(bucket_seek_start(PREFIX, 3, b"aa", true), expected);
        assert!(expected < key(3, b"aaa", 0x00));
        assert!(expected < key(3, b"ab", 0x00));
        // `"a\xFF\xFF"` follows `"aa"` too — its second byte exceeds `'a'` —
        // so the seek lands below it; `"a\0\0"` precedes `"aa"` and sits above.
        assert!(expected < key(3, b"a", 0xFF));
        assert!(expected > key(3, b"a\x00", 0x00));
    }

    /// The seek targets order the way the values do: every seek for a bound
    /// lands at or before the first key that should follow it.
    #[test]
    fn seek_targets_respect_string_order_across_lengths() {
        // Values, in string order, that all follow "aa": aaa < ab < b < z.
        let following = [
            key(3, b"aaa", 0),
            key(2, b"ab", 0),
            key(1, b"b", 0),
            key(1, b"z", 0),
        ];
        for value in &following {
            let len = usize::from(u16::from_be_bytes([value[1], value[2]]));
            assert!(
                bucket_seek_start(PREFIX, len, b"aa", false) <= *value,
                "seek lands at or before a following value"
            );
        }
        // Values that precede "aa" sit before their bucket's seek target.
        assert!(bucket_seek_start(PREFIX, 1, b"aa", false) > key(1, b"a", 0));
        assert!(bucket_seek_start(PREFIX, 2, b"aa", false) > key(2, b"aa", 0));
    }

    /// The regression the `Both`-walk test caught: an exclusive seek after an
    /// edge type must step past that type's rows, whose keys continue with a
    /// `dst`. A trailing zero byte does not — it sorts *below* such a row, so
    /// the type was re-yielded and a page claimed a phantom `has_more`.
    #[test]
    fn exclusive_seek_steps_past_rows_that_continue_after_the_value() {
        let type_c_row = continued(key(1, b"c", 0));
        assert!(
            exclusive_after_key(&key(1, b"c", 0)) < type_c_row,
            "the trailing-zero form would re-include the row"
        );
        assert!(
            bucket_seek_start(PREFIX, 1, b"c", false) > type_c_row,
            "the exclusive seek excludes every row of the value"
        );
        assert!(
            bucket_seek_start(PREFIX, 1, b"c", true) <= type_c_row,
            "the inclusive seek still includes them"
        );
    }

    #[test]
    fn next_text_component_reads_the_component_after_the_prefix() {
        let trailing = key(2, b"ab", 0);
        assert_eq!(next_text_component(&trailing, PREFIX), Some(&b"ab"[..]));
        let mut continued = key(2, b"ab", 0);
        continued.extend(key(1, b"z", 0)[PREFIX.len()..].iter());
        assert_eq!(next_text_component(&continued, PREFIX), Some(&b"ab"[..]));
        assert_eq!(next_text_component(b"Q", PREFIX), None);
        assert_eq!(next_text_component(PREFIX, PREFIX), None);
    }
}
