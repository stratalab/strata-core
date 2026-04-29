# Typography

Three families. Each has one job. They are chosen so the brand reads the
same on the website (already shipping) and in the Foundry app.

| Family | Role | License |
|---|---|---|
| **Inter** | UI, body, all running text | SIL OFL — free |
| **Instrument Serif** | Display, editorial, hero moments | SIL OFL — free |
| **JetBrains Mono** | Code, identifiers, numerics in tables | SIL OFL — free |

All three are free, open-source, and available on Google Fonts. None of them
are in macOS by default — bundle them with the Foundry app and self-host them
on the web.

## Inter — UI workhorse

Inter does almost everything. It's the typeface you read in product, in
docs, in toolbars, in dialogs, in forms. It pairs cleanly with both the
serif (for display contrast) and the mono (for code).

**Weights we ship:**

| Weight | Use |
|---|---|
| 400 Regular   | body, labels, table cells |
| 500 Medium    | UI labels that need a touch more presence |
| 600 SemiBold  | section headings, emphasized body |
| 700 Bold      | rare — only for `display.lg` and above |

We don't ship Light. Inter Light reads thin on macOS and the editorial
weight we want at large sizes comes from Instrument Serif, not from
Inter Light.

**Tabular numerals**: Inter has a `tnum` feature. Turn it on for any
number that appears in a column (table cells, metrics, timestamps).
Don't turn it on for prose.

## Instrument Serif — editorial

Instrument Serif is reserved for moments that earn it: a hero on the
website, an empty-state heading, an onboarding card, a docs section
marker. It is *not* a body face. Used at small sizes it gets muddy; used
at large sizes it gives the page a magazine quality that nothing else in
the system offers.

**The italic carries the whimsy.** Instrument Serif Italic has more
character than the upright. Use the italic for the one phrase per page
that you want to feel like a hand-set caption — never for emphasis in
running text. (See [principles.md](principles.md), section 5.)

**Weights:**

| Weight | Use |
|---|---|
| 400 Regular | display headings, hero |
| 400 Italic  | one-per-page editorial flourish |

We do not use Instrument Serif Bold. If a serif heading needs more weight,
use a larger size, not a heavier cut.

## JetBrains Mono — code

JetBrains Mono is the website's code typeface and the Foundry app's code
typeface. It has good ligatures, distinct `0`/`O`, and reads well at small
sizes. It's also generous enough at display sizes to use for "show the
query" hero moments.

**Weights:**

| Weight | Use |
|---|---|
| 400 Regular | code blocks, inline code, identifiers |
| 500 Medium  | code in dense UI where 400 disappears |

Disable ligatures (`ss01`, `calt`) in the app code editor — they confuse
power users. Keep them on in marketing code samples where they look nice.

## Type scale

The scale is anchored to a 16-px body. Each step is roughly a 1.2 ratio,
with rounding to friendly numbers.

| Token | Size / Line | Family | Weight | Use |
|---|---|---|---|---|
| `display.xl`   | 64 / 68 | Instrument Serif | 400 | website hero |
| `display.lg`   | 48 / 52 | Instrument Serif | 400 | section hero |
| `display.md`   | 36 / 42 | Instrument Serif | 400 | onboarding, empty state |
| `display.sm`   | 28 / 34 | Instrument Serif | 400 | docs section opener |
| `heading.xl`   | 24 / 30 | Inter | 600 | page title |
| `heading.lg`   | 20 / 26 | Inter | 600 | major section |
| `heading.md`   | 16 / 22 | Inter | 600 | sub-section |
| `heading.sm`   | 14 / 20 | Inter | 600 | label heading |
| `body.lg`      | 17 / 26 | Inter | 400 | docs body, marketing body |
| `body.md`      | 15 / 22 | Inter | 400 | **default UI body** |
| `body.sm`      | 13 / 18 | Inter | 400 | dense UI, secondary |
| `caption`      | 12 / 16 | Inter | 400 | timestamps, footnotes |
| `code.md`      | 14 / 22 | JetBrains Mono | 400 | code block default |
| `code.sm`      | 12 / 18 | JetBrains Mono | 400 | inline code, dense UI |
| `code.display` | 18 / 28 | JetBrains Mono | 400 | hero query, marketing |

`body.md` (15 px, not 16 px) is the default for the macOS app. 16 px is the
right web body size; 15 px reads correctly on macOS at standard zoom.

## Pairings

### Hero pattern

Instrument Serif display + Inter subtitle + Inter button label.

```
Strata                                    ← Instrument Serif display.xl
The database for the AI era.              ← Inter body.lg, fg.muted
[ Get started ]                           ← Inter heading.sm, on coral
```

### Empty state pattern

Instrument Serif display.sm + Inter body + a coral link.

```
No queries yet.                           ← Instrument Serif display.sm
Run your first one — or read              ← Inter body.md, fg.muted
about how Strata branches state.          ← coral link on "branches state"
```

### Section heading pattern (UI)

Inter heading + Inter caption.

```
Vectors                                   ← Inter heading.lg
12,438 embeddings · 1,536 dim             ← Inter caption, fg.muted
```

The serif does not appear here. UI section headings are functional, not
editorial.

## Rules

### Inter is the default. Reach for serif deliberately.

If you can't articulate why a heading is set in Instrument Serif, set it
in Inter. Reaching for the serif because "headings should feel special" is
the wrong reason. The serif appears when the text is *about ideas* — not
about state, navigation, or actions.

### Never set body in serif

Instrument Serif body is muddy. The smallest size we ever set it at is
`display.sm` (28 px). Below that it is Inter, always.

### One italic per page

Instrument Serif Italic is the most expressive type in the system. Used
twice on a page, it stops being expressive. Use it once or not at all.

### Tabular numbers in tables, proportional everywhere else

Turn on `tnum` for any column of numbers. Leave it off for prose. The
default Inter render is proportional, which is right for paragraphs and
wrong for stacks of figures.

### Code is JetBrains Mono, not Menlo, not Courier, not "monospace"

The system font stack falls back to platform-default mono. We don't want
that. Bundle JetBrains Mono and reference it explicitly.

### Don't fake weights or styles

Don't synthesize bold from regular. Don't slant Inter to make a
pseudo-italic. If a weight isn't in the table above, we don't use it.

## Do / don't

**Do**

- Use the named tokens (`StrataType.body` in Swift, `var(--type-body-md)`
  on the web).
- Pair Inter with Instrument Serif for any hero or onboarding moment.
- Enable `tnum` for tables and metrics.

**Don't**

- Mix Inter with another sans-serif (no SF Pro, no system stack).
- Set Instrument Serif below 28 px.
- Use the italic for emphasis in running text.
- Use code mono for non-code text (no "data" labels in JetBrains Mono).
