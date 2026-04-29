# Principles

Strata's brand is summarized in five attributes. This document turns each one
into a working rule that you can apply when making a decision. When two
principles conflict, the order they appear here is the order they win in.

---

## 1. Quiet luxury

*Restrained, warm, premium.*

The reference point is a well-cut wool coat, not a sports car. Materials are
expensive but not obvious. Nothing announces itself. The product earns trust
by being calm.

**Apply this when:**

- Choosing a background color → warm near-black or warm off-white, never
  pure black or pure white. Pure values feel cheap.
- Choosing how much color to use → less than you think. Coral is the brand;
  use it for the one thing on the screen that matters most. Everywhere else,
  neutral.
- Choosing a shadow → soft, low contrast, slightly warm. Not crisp. Not gray.
- Choosing whether to add a gradient, glow, or animation flourish → usually
  no. If yes, make it almost imperceptible.

**Do**

- One coral element per screen.
- Warm grays. Off-blacks. Off-whites.
- Generous whitespace. Let things breathe.

**Don't**

- Multiple bright accent colors competing.
- Pure `#000000` or pure `#FFFFFF` as a primary surface.
- Drop-shadow-on-everything. Glassmorphism. Neon glows.

---

## 2. Sleek and classy

*Refined geometry, careful spacing, considered type.*

Every element is on a grid. Every weight pairing is intentional. Type sets
the tone — it should feel like an editorial magazine, not a SaaS dashboard.

**Apply this when:**

- Spacing two elements → use a token from the 4-px scale. If your eye says
  "needs 7 px", you mean 8.
- Picking a corner radius → use a token. Don't mix radii within the same
  component family.
- Setting a heading → Instrument Serif for display moments only (hero,
  empty state, onboarding). Inter for everything else. Never serif body.
- Aligning a column → optical alignment beats mathematical alignment, but
  only when you can articulate why.

**Do**

- 4-px grid. Always.
- One typeface per role (UI = Inter, display = Instrument Serif, code =
  JetBrains Mono).
- Left-aligned text in dense UI. Centered text only for hero moments.

**Don't**

- Arbitrary spacing values. ("It just looked right at 13.")
- Mixing two sans-serif families.
- Decorative dividers, ornamental rules, fake 3D.

---

## 3. Competence and intelligence

*Every element earns its place.*

We are building infrastructure. The user is trusting us with their data,
their queries, their reasoning. The interface should communicate that we know
what we're doing — by showing real information, not by performing seriousness.

**Apply this when:**

- Choosing what to put in an empty state → a useful next step, not a cute
  illustration.
- Designing an error → say what went wrong, in plain language, and what to
  do about it. No "Oops!". No exclamation marks.
- Adding a tooltip → only if the affordance genuinely needs explanation.
  Tooltips are an admission the UI failed; use them sparingly.
- Showing a number → show enough precision to be useful and a tooltip with
  the full value. Don't round silently.

**Do**

- Surface the actual data. Real query times. Real row counts. Real bytes.
- Use precise verbs. "Compact" not "Clean up". "Reindex" not "Refresh".
- Default to showing more, behind a clear disclosure. Power users earn
  density.

**Don't**

- Marketing copy in product UI.
- "Loading…" with no detail. ("Loading 2.3M rows from `events`" is better.)
- Hiding capability behind a "Pro" lock or a paywall tease.

---

## 4. Voracious appetite for knowledge

*Curious, generous, editorial.*

Strata is for people who like knowing things. The product should feel like a
good textbook — it teaches as you use it, and rewards reading.

**Apply this when:**

- Writing onboarding → explain why, not just what. ("Branches let you fork
  state without copying it. Foundry creates one per session.")
- Writing docs → write the doc you wish existed when you first hit this
  feature. Link to deeper reading. Cite papers when relevant.
- Designing an empty state → a link to the concept doc beats a cute
  illustration.
- Picking a serif moment → Instrument Serif for headlines that are *about
  ideas* (concepts, principles, philosophy). Inter for headlines that are
  *about state* (counts, status, navigation).

**Do**

- Link to primary sources. Cite the Raft paper, the LSM-tree paper, the
  HNSW paper. Show the work.
- Use the serif typeface to mark "this is something to read", not "this is
  important".
- Treat documentation as a first-class surface. The website's docs read like
  the product.

**Don't**

- Dumb things down.
- Hide complexity. Layer it.
- Buzzwords. ("AI-powered intelligent…")

---

## 5. A little whimsical

*A few moments of delight, used sparingly.*

This is the seasoning. The product is mostly serious; whimsy lives in the
margins — a turn of phrase, a small animation, a typographic flourish where
you didn't expect one. If you can name three whimsical moments in a session,
it's too many.

**Apply this when:**

- Naming an internal concept → "stratum", "foundry", "petrify" are good.
  "Database container handler" is bad.
- Adding motion → motion can have personality if it's short, deliberate, and
  obeys the easing curve. (See [space-shape-motion.md](space-shape-motion.md).)
- Writing empty-state copy → one editorial sentence is okay. "No queries
  yet — but the day is young." Once.
- Drawing a divider, illustration, or icon → the layered-strata motif from
  the logo is the one signature mark. Use it where it fits naturally; never
  decoratively.

**Do**

- Reach for the unexpected word when the obvious one is boring.
- Use the italic of Instrument Serif for one emphasis per page, max.
- Leave room for jokes that only a database person would get.

**Don't**

- Mascots, characters, anthropomorphized robots.
- Emoji in product UI. (Docs and marketing, sparingly.)
- Easter eggs that interfere with work. Whimsy is decoration, never gating.

---

## Conflict resolution

When two principles pull in different directions, the higher-numbered one
loses. Concretely:

- *Whimsy* loses to *competence*. A clever empty-state line that costs the
  user a useful next step is the wrong trade.
- *Curiosity* loses to *quiet luxury*. An editorial flourish that crowds the
  hero is the wrong trade.
- *Sleekness* loses to *competence*. A perfectly aligned column that hides
  data the user needs is the wrong trade.

Quiet luxury is the floor. Everything else is built on it.
