//! Drop mutation-gate survivors that land in `#[cfg(...)]` spans a lane does
//! not compile (#3254).
//!
//! ## Why this exists
//!
//! cargo-mutants mutates source *text* and does not evaluate conditional
//! compilation (empirically true of 27.1.0: `--list --features download,testkit`
//! and `--list --features download,testkit,local` emit an identical mutant set).
//! A mutant on code the lane never compiles therefore always survives — and
//! vacuously, because no test could ever exercise it. Lane C of the
//! `mutation-on-diff` gate compiles `strata-inference` without the `local`
//! feature, so every `#[cfg(feature = "local")]` item is such a span. These
//! were suppressed by hand-maintained name regexes in
//! `.cargo/mutants-inference.toml`; this filter derives the suppression from the
//! source instead, so a new `local` item needs no new exclusion.
//!
//! ## Safety
//!
//! The evaluator is a *denylist*: a `feature = "X"` atom is [`Tri::False`] only
//! when `X` is explicitly named inactive, and every atom we do not recognise is
//! [`Tri::Unknown`], which is *kept*. A span is dropped only when its predicate
//! provably resolves to [`Tri::False`]. Under-marking an active span as inactive
//! is therefore impossible, so the filter can never hide a real survivor — its
//! worst case is failing to retire an exclusion, which is exactly today's
//! behaviour.

use std::collections::HashMap;
use std::ops::Not;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

/// Kleene three-valued logic. `Unknown` is the conservative value: a predicate
/// we cannot fully resolve is never treated as "definitely not compiled".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tri {
    True,
    False,
    Unknown,
}

impl std::ops::Not for Tri {
    type Output = Tri;

    /// Kleene negation — `Unknown` negates to `Unknown`.
    fn not(self) -> Tri {
        match self {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Unknown => Tri::Unknown,
        }
    }
}

impl Tri {
    /// `all(..)` — `False` if any child is `False`, `True` only if every child
    /// is `True`, else `Unknown`.
    pub fn all(children: impl IntoIterator<Item = Tri>) -> Tri {
        let mut result = Tri::True;
        for c in children {
            match c {
                Tri::False => return Tri::False,
                Tri::Unknown => result = Tri::Unknown,
                Tri::True => {}
            }
        }
        result
    }

    /// `any(..)` — `True` if any child is `True`, `False` only if every child is
    /// `False`, else `Unknown`.
    pub fn any(children: impl IntoIterator<Item = Tri>) -> Tri {
        let mut result = Tri::False;
        for c in children {
            match c {
                Tri::True => return Tri::True,
                Tri::Unknown => result = Tri::Unknown,
                Tri::False => {}
            }
        }
        result
    }
}

/// The feature knowledge a lane has: which features it is *known not* to compile
/// (the denylist) and, optionally, which it is *known* to compile.
#[derive(Debug, Default, Clone)]
pub struct CfgContext {
    inactive: Vec<String>,
    active: Vec<String>,
}

impl CfgContext {
    /// A context that knows only which features are inactive — the shape CI
    /// uses (`--inactive-feature local`).
    pub fn with_inactive<I, S>(inactive: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CfgContext {
            inactive: inactive.into_iter().map(Into::into).collect(),
            active: Vec::new(),
        }
    }

    /// Declare a feature the lane is known to compile (raises a `feature = "X"`
    /// atom to [`Tri::True`]). Used mainly by tests.
    pub fn with_active<I, S>(mut self, active: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.active = active.into_iter().map(Into::into).collect();
        self
    }

    fn eval_feature(&self, name: &str) -> Tri {
        if self.inactive.iter().any(|f| f == name) {
            Tri::False
        } else if self.active.iter().any(|f| f == name) {
            Tri::True
        } else {
            Tri::Unknown
        }
    }
}

/// Evaluate a `cfg` predicate — the [`syn::Meta`] parsed from inside `cfg(..)`.
///
/// `syn::Meta` *is* the cfg grammar: a bare ident (`test`, `unix`) is a
/// [`syn::Meta::Path`], `feature = "x"` is a [`syn::Meta::NameValue`], and
/// `all(..)`/`any(..)`/`not(..)` are [`syn::Meta::List`]s.
pub fn eval_cfg_meta(meta: &syn::Meta, ctx: &CfgContext) -> Tri {
    match meta {
        // A bare atom we do not model (`test`, `unix`, a custom cfg). Keep.
        syn::Meta::Path(_) => Tri::Unknown,
        syn::Meta::NameValue(nv) => {
            if nv.path.is_ident("feature") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = &nv.value
                {
                    return ctx.eval_feature(&s.value());
                }
            }
            // `target_os = "..."` and friends: unmodelled → keep.
            Tri::Unknown
        }
        syn::Meta::List(list) => {
            let children = match list.parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            ) {
                Ok(children) => children,
                // A cfg we cannot parse must not cause a drop.
                Err(_) => return Tri::Unknown,
            };
            let evaluated = children.iter().map(|m| eval_cfg_meta(m, ctx));
            if list.path.is_ident("all") {
                Tri::all(evaluated)
            } else if list.path.is_ident("any") {
                Tri::any(evaluated)
            } else if list.path.is_ident("not") {
                // `not` takes exactly one predicate.
                match children.len() {
                    1 => eval_cfg_meta(&children[0], ctx).not(),
                    _ => Tri::Unknown,
                }
            } else {
                Tri::Unknown
            }
        }
    }
}

/// Return the `cfg` predicate resolved by a node's attributes, if it has one.
/// `#[cfg_attr(..)]` is deliberately ignored: it conditionally applies *other*
/// attributes but never gates whether the item is compiled.
fn cfg_verdict(attrs: &[syn::Attribute], ctx: &CfgContext) -> Option<(Tri, proc_macro2::Span)> {
    let cfg = attrs.iter().find(|a| a.path().is_ident("cfg"))?;
    match cfg.parse_args::<syn::Meta>() {
        Ok(meta) => Some((eval_cfg_meta(&meta, ctx), cfg.span())),
        // Unparseable cfg → do not drop.
        Err(_) => None,
    }
}

/// Inclusive 1-based line ranges of items whose `cfg` resolves to `False`.
struct InactiveSpanVisitor<'a> {
    ctx: &'a CfgContext,
    spans: Vec<(usize, usize)>,
}

impl<'a> InactiveSpanVisitor<'a> {
    /// If `attrs` carry a `cfg` that is `False`, record the span covering the
    /// attribute through the node body and return `true` (caller must not
    /// recurse — the whole node is dead for this lane).
    fn record_if_inactive(&mut self, attrs: &[syn::Attribute], node: proc_macro2::Span) -> bool {
        if let Some((Tri::False, cfg_span)) = cfg_verdict(attrs, self.ctx) {
            // Cover from the `#[cfg]` attribute line through the node's last
            // line, so every mutable line the item owns is inside the range
            // regardless of where `node.span()` chooses to start.
            let start = cfg_span.start().line.min(node.start().line);
            let end = node.end().line;
            self.spans.push((start, end));
            true
        } else {
            false
        }
    }
}

impl<'ast, 'a> Visit<'ast> for InactiveSpanVisitor<'a> {
    fn visit_item(&mut self, i: &'ast syn::Item) {
        if self.record_if_inactive(item_attrs(i), i.span()) {
            return;
        }
        visit::visit_item(self, i);
    }

    fn visit_impl_item(&mut self, i: &'ast syn::ImplItem) {
        if self.record_if_inactive(impl_item_attrs(i), i.span()) {
            return;
        }
        visit::visit_impl_item(self, i);
    }

    fn visit_trait_item(&mut self, i: &'ast syn::TraitItem) {
        if self.record_if_inactive(trait_item_attrs(i), i.span()) {
            return;
        }
        visit::visit_trait_item(self, i);
    }

    fn visit_stmt(&mut self, s: &'ast syn::Stmt) {
        if self.record_if_inactive(stmt_attrs(s), s.span()) {
            return;
        }
        visit::visit_stmt(self, s);
    }

    fn visit_field(&mut self, f: &'ast syn::Field) {
        if self.record_if_inactive(&f.attrs, f.span()) {
            return;
        }
        visit::visit_field(self, f);
    }

    fn visit_variant(&mut self, v: &'ast syn::Variant) {
        if self.record_if_inactive(&v.attrs, v.span()) {
            return;
        }
        visit::visit_variant(self, v);
    }

    fn visit_arm(&mut self, a: &'ast syn::Arm) {
        if self.record_if_inactive(&a.attrs, a.span()) {
            return;
        }
        visit::visit_arm(self, a);
    }
}

fn item_attrs(i: &syn::Item) -> &[syn::Attribute] {
    use syn::Item::*;
    match i {
        Const(x) => &x.attrs,
        Enum(x) => &x.attrs,
        ExternCrate(x) => &x.attrs,
        Fn(x) => &x.attrs,
        ForeignMod(x) => &x.attrs,
        Impl(x) => &x.attrs,
        Macro(x) => &x.attrs,
        Mod(x) => &x.attrs,
        Static(x) => &x.attrs,
        Struct(x) => &x.attrs,
        Trait(x) => &x.attrs,
        TraitAlias(x) => &x.attrs,
        Type(x) => &x.attrs,
        Union(x) => &x.attrs,
        Use(x) => &x.attrs,
        _ => &[],
    }
}

fn impl_item_attrs(i: &syn::ImplItem) -> &[syn::Attribute] {
    use syn::ImplItem::*;
    match i {
        Const(x) => &x.attrs,
        Fn(x) => &x.attrs,
        Type(x) => &x.attrs,
        Macro(x) => &x.attrs,
        _ => &[],
    }
}

fn trait_item_attrs(i: &syn::TraitItem) -> &[syn::Attribute] {
    use syn::TraitItem::*;
    match i {
        Const(x) => &x.attrs,
        Fn(x) => &x.attrs,
        Type(x) => &x.attrs,
        Macro(x) => &x.attrs,
        _ => &[],
    }
}

fn stmt_attrs(s: &syn::Stmt) -> &[syn::Attribute] {
    match s {
        syn::Stmt::Local(x) => &x.attrs,
        // Item statements are re-dispatched through `visit_item`.
        syn::Stmt::Item(_) => &[],
        syn::Stmt::Expr(e, _) => expr_attrs(e),
        syn::Stmt::Macro(m) => &m.attrs,
    }
}

/// Attributes for the expression forms that carry a gating `#[cfg]` in practice
/// — chiefly `#[cfg(..)] { .. }` inline blocks. Unmodelled forms return `&[]`,
/// which keeps their mutants (conservative).
fn expr_attrs(e: &syn::Expr) -> &[syn::Attribute] {
    match e {
        syn::Expr::Block(x) => &x.attrs,
        syn::Expr::If(x) => &x.attrs,
        syn::Expr::Match(x) => &x.attrs,
        syn::Expr::Unsafe(x) => &x.attrs,
        syn::Expr::Call(x) => &x.attrs,
        syn::Expr::MethodCall(x) => &x.attrs,
        _ => &[],
    }
}

/// Inclusive 1-based line ranges in `source` that a lane with this `ctx` does
/// not compile. A parse failure yields no ranges (keep everything).
pub fn inactive_line_spans(source: &str, ctx: &CfgContext) -> Vec<(usize, usize)> {
    let file = match syn::parse_file(source) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };
    let mut visitor = InactiveSpanVisitor {
        ctx,
        spans: Vec::new(),
    };
    visitor.visit_file(&file);
    visitor.spans
}

/// Is `line` inside any inactive span?
pub fn line_is_inactive(line: usize, spans: &[(usize, usize)]) -> bool {
    spans
        .iter()
        .any(|&(start, end)| start <= line && line <= end)
}

/// A cargo-mutants survivor line parsed into its source location. Format is
/// `path:line:col: description`; the path never contains a colon.
pub fn parse_survivor_location(line: &str) -> Option<(&str, usize)> {
    let mut fields = line.splitn(4, ':');
    let path = fields.next()?;
    let line_no = fields.next()?.parse::<usize>().ok()?;
    // Require the column field too, so an accidental `a:b` cannot parse.
    fields.next()?;
    if path.is_empty() {
        return None;
    }
    Some((path, line_no))
}

/// The outcome of filtering a `missed.txt` body.
#[derive(Debug, Default)]
pub struct FilterOutcome {
    /// Survivor lines that are kept — the real verdict.
    pub kept: Vec<String>,
    /// Survivor lines dropped because they land in an inactive `cfg` span.
    pub dropped: Vec<String>,
}

/// Filter a `missed.txt` body against a per-path index of inactive line spans.
/// A line is dropped only when it parses to a `path:line` that the index marks
/// inactive; anything unparseable, or in a file not in the index, is kept.
pub fn filter_missed(
    missed: &str,
    inactive_by_path: &HashMap<String, Vec<(usize, usize)>>,
) -> FilterOutcome {
    let mut outcome = FilterOutcome::default();
    for line in missed.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let drop = match parse_survivor_location(line) {
            Some((path, line_no)) => inactive_by_path
                .get(path)
                .is_some_and(|spans| line_is_inactive(line_no, spans)),
            None => false,
        };
        if drop {
            outcome.dropped.push(line.to_string());
        } else {
            outcome.kept.push(line.to_string());
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(src: &str) -> syn::Meta {
        syn::parse_str(src).expect("valid cfg predicate")
    }

    #[test]
    fn tri_not_maps_unknown_to_unknown() {
        assert_eq!(Tri::True.not(), Tri::False);
        assert_eq!(Tri::False.not(), Tri::True);
        assert_eq!(Tri::Unknown.not(), Tri::Unknown);
    }

    #[test]
    fn tri_all_is_false_dominant_then_unknown_dominant() {
        use Tri::*;
        // A single False forces False even past an Unknown.
        assert_eq!(Tri::all([True, Unknown, False]), False);
        // No False, at least one Unknown → Unknown.
        assert_eq!(Tri::all([True, Unknown, True]), Unknown);
        // All True → True; empty → True.
        assert_eq!(Tri::all([True, True]), True);
        assert_eq!(Tri::all([]), True);
    }

    #[test]
    fn tri_any_is_true_dominant_then_unknown_dominant() {
        use Tri::*;
        assert_eq!(Tri::any([False, Unknown, True]), True);
        assert_eq!(Tri::any([False, Unknown, False]), Unknown);
        assert_eq!(Tri::any([False, False]), False);
        assert_eq!(Tri::any([]), False);
    }

    #[test]
    fn feature_atom_is_a_denylist() {
        let ctx = CfgContext::with_inactive(["local"]).with_active(["openai"]);
        assert_eq!(ctx.eval_feature("local"), Tri::False);
        assert_eq!(ctx.eval_feature("openai"), Tri::True);
        assert_eq!(ctx.eval_feature("anthropic"), Tri::Unknown);
    }

    #[test]
    fn cfg_predicates_resolve_against_an_inactive_local() {
        let ctx = CfgContext::with_inactive(["local"]);
        // The gated-out feature itself.
        assert_eq!(
            eval_cfg_meta(&meta("feature = \"local\""), &ctx),
            Tri::False
        );
        // `not(local)` is compiled when local is off.
        assert_eq!(
            eval_cfg_meta(&meta("not(feature = \"local\")"), &ctx),
            Tri::True
        );
        // `all(test, local)` is dead via the local child regardless of `test`.
        assert_eq!(
            eval_cfg_meta(&meta("all(test, feature = \"local\")"), &ctx),
            Tri::False
        );
        // `any(openai, local)` cannot be resolved without knowing openai → keep.
        assert_eq!(
            eval_cfg_meta(
                &meta("any(feature = \"openai\", feature = \"local\")"),
                &ctx
            ),
            Tri::Unknown
        );
        // `all(download, local)` is dead via local even though download is unknown.
        assert_eq!(
            eval_cfg_meta(
                &meta("all(feature = \"download\", feature = \"local\")"),
                &ctx
            ),
            Tri::False
        );
        // A cfg with no local mention and unknown atoms → keep.
        assert_eq!(
            eval_cfg_meta(&meta("not(feature = \"download\")"), &ctx),
            Tri::Unknown
        );
        assert_eq!(eval_cfg_meta(&meta("test"), &ctx), Tri::Unknown);
    }

    const SAMPLE: &str = r#"
struct Runtime;

impl Runtime {
    #[cfg(feature = "local")]
    fn lock_embeddings(&self) -> usize {
        let n = 1;
        n + 1
    }

    #[cfg(not(feature = "local"))]
    fn lock_embeddings(&self) -> usize {
        let n = 2;
        n + 2
    }

    fn always(&self) -> usize {
        let mut total = 0;
        #[cfg(feature = "local")]
        {
            total += 10;
        }
        #[cfg(not(feature = "local"))]
        {
            total += 20;
        }
        total
    }
}

#[cfg(all(test, feature = "local"))]
mod local_only {
    fn helper() -> usize {
        7
    }
}
"#;

    #[test]
    fn inactive_spans_cover_local_gated_code_only() {
        let ctx = CfgContext::with_inactive(["local"]);
        let spans = inactive_line_spans(SAMPLE, &ctx);

        // `#[cfg(feature = "local")] fn lock_embeddings` body (line 7: `let n = 1`).
        assert!(
            line_is_inactive(7, &spans),
            "local fn body must be inactive"
        );
        // `#[cfg(not(feature = "local"))]` twin body (line 13: `let n = 2`) stays.
        assert!(
            !line_is_inactive(13, &spans),
            "not(local) fn body must stay active"
        );
        // The compiled `always` signature/body outside the inline blocks stays.
        assert!(!line_is_inactive(17, &spans), "plain fn must stay active");
        // Inline `#[cfg(feature = "local")] { total += 10 }` (line 21) is dropped;
        // the `not(local)` inline block (line 25) is kept.
        assert!(
            line_is_inactive(21, &spans),
            "local inline block must be inactive"
        );
        assert!(
            !line_is_inactive(25, &spans),
            "not(local) inline block must stay active"
        );
        // `#[cfg(all(test, feature = "local"))] mod` body (line 35) is dropped.
        assert!(
            line_is_inactive(35, &spans),
            "test+local mod must be inactive"
        );
    }

    #[test]
    fn filter_drops_inactive_survivors_and_keeps_the_rest() {
        let ctx = CfgContext::with_inactive(["local"]);
        let mut index = HashMap::new();
        index.insert(
            "crates/inference/src/runtime.rs".to_string(),
            inactive_line_spans(SAMPLE, &ctx),
        );

        let missed = "\
crates/inference/src/runtime.rs:7:9: replace Runtime::lock_embeddings -> usize with 0
crates/inference/src/runtime.rs:21:13: replace += with -= in Runtime::always
crates/inference/src/runtime.rs:17:5: replace Runtime::always -> usize with 1
crates/inference/src/runtime.rs:25:13: replace += with -= in Runtime::always
this line does not parse as a mutant
crates/other/untracked.rs:5:1: replace foo -> () with ()";

        let outcome = filter_missed(missed, &index);

        // Dropped: the two lines inside local-gated spans.
        assert_eq!(outcome.dropped.len(), 2, "dropped: {:?}", outcome.dropped);
        assert!(outcome
            .dropped
            .iter()
            .all(|l| l.contains(":7:") || l.contains(":21:")));

        // Kept: the compiled fn, the not(local) inline block, the garbage line,
        // and a survivor in a file the index has no spans for.
        assert_eq!(outcome.kept.len(), 4, "kept: {:?}", outcome.kept);
        assert!(outcome.kept.iter().any(|l| l.contains(":17:")));
        assert!(outcome.kept.iter().any(|l| l.contains(":25:")));
        assert!(outcome.kept.iter().any(|l| l.contains("does not parse")));
        assert!(outcome.kept.iter().any(|l| l.contains("untracked.rs")));
    }

    #[test]
    fn unparseable_source_keeps_everything() {
        let ctx = CfgContext::with_inactive(["local"]);
        assert!(inactive_line_spans("fn broken( {", &ctx).is_empty());
    }
}
