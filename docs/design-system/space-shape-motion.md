# Space, Shape, Motion

The non-color, non-type half of the system: how things are spaced, what
shapes they take, how they move.

## Spacing

A 4-px grid. Every margin, padding, gap, and offset uses one of these
tokens. If you find yourself reaching for `13` or `17`, you mean `12` or
`16` — trust the grid.

| Token | Px | Use |
|---|---|---|
| `space.xxs` | 4  | tight icon-to-label gaps, sub-line micro-adjustments |
| `space.xs`  | 8  | inside compact controls (button padding-x) |
| `space.sm`  | 12 | between related controls in a row |
| `space.md`  | 16 | **default gap** between content blocks |
| `space.lg`  | 24 | between content sections |
| `space.xl`  | 32 | major section breaks |
| `space.xxl` | 48 | hero spacing, page-level breathing room |

(These are already the values in `StrataFoundry/SharedUI/DesignSystem.swift`.
This doc is the authoritative reference; the Swift file mirrors it.)

### When to use which

- **Inside a component**: `xxs` to `sm`. A button's internal padding is
  `xs` × `sm` (8 vertical × 12 horizontal). A label's gap from its icon
  is `xxs`.
- **Between components in a group**: `sm` to `md`. Form rows are `md`
  apart. Toolbar items are `sm` apart.
- **Between sections**: `lg` to `xl`. The gap between the toolbar and the
  content area is `lg`. The gap between two top-level sections in a
  settings page is `xl`.
- **Page-level breathing**: `xxl`. The top margin of a hero. The padding
  around an empty state.

### The 4-px rule has one exception

Optical alignment. If a piece of glyph or shape needs to be nudged by 1 px
to *look* aligned with its neighbor, do it. Articulate why, and prefer
fixing the underlying alignment if possible. Optical exceptions are not a
license for arbitrary spacing.

## Shape (radii)

| Token | Px | Use |
|---|---|---|
| `radius.sm`   | 4  | dense controls (table cells, tags, chips) |
| `radius.md`   | 8  | **default** — buttons, inputs, cards |
| `radius.lg`   | 12 | dialogs, sheets, large cards |
| `radius.xl`   | 16 | hero cards, marketing surfaces |
| `radius.full` | 9999 | pills, avatars, circular badges |

(Existing tokens in Foundry: `sm`, `md`, `lg`. This doc adds `xl` and
`full`.)

### Don't mix radii within a family

A button group is all `radius.md`. A card containing a smaller card uses
`radius.lg` outside, `radius.md` inside — the inner radius is one step
smaller than the outer. Don't put a `radius.sm` element inside a
`radius.lg` container; it looks accidental.

### Pills are pills

`radius.full` is for elements where the radius is the whole point: avatars,
status dots, the coral primary button on the website. Use it deliberately;
not for "rounder buttons everywhere".

## Layout (Foundry-specific)

These are the load-bearing widths in the macOS app. They are tokens, not
guidelines — the layout is built around them.

| Token | Px | Use |
|---|---|---|
| `layout.sidebarIdealWidth`     | 220 | nav sidebar |
| `layout.inspectorIdealWidth`   | 320 | right-hand inspector |
| `layout.sheetMinWidth`         | 440 | standard modal sheet |
| `layout.sheetWideMinWidth`     | 540 | sheet that needs more room |

(All already exist in `DesignSystem.swift`.)

## Elevation

Strata's shadows are **soft, warm, and low-contrast**. Crisp gray drop
shadows feel like office software; we want shadows that feel like a heavy
piece of paper resting on a wood desk.

The trick: shadows are tinted toward the coral family at very low alpha,
not pure black. On dark theme, shadows are deeper but still warm.

| Token | Light | Dark | Use |
|---|---|---|---|
| `elevation.0` | none | none | flat — sits on the canvas |
| `elevation.1` | `0 1px 2px rgba(74, 36, 24, 0.04)` | `0 1px 2px rgba(0, 0, 0, 0.4)` | hover lift, default card |
| `elevation.2` | `0 4px 12px rgba(74, 36, 24, 0.08)` | `0 4px 12px rgba(0, 0, 0, 0.5)` | popover, dropdown, tooltip |
| `elevation.3` | `0 12px 32px rgba(74, 36, 24, 0.12)` | `0 12px 32px rgba(0, 0, 0, 0.6)` | dialog, sheet |
| `elevation.4` | `0 24px 64px rgba(74, 36, 24, 0.16)` | `0 24px 64px rgba(0, 0, 0, 0.7)` | full-screen modal, hero |

The shadow color on light is `#4A2418` (`coral.900`) at low alpha. That's
what makes them feel warm. Replacing with pure black undoes the effect.

### Use one elevation level above the surrounding surface

A card on the canvas is `elevation.1`. A popover from inside that card is
`elevation.2`. A dialog from the popover is `elevation.3`. Skipping levels
makes the hierarchy feel false.

## Motion

Motion is where most of the "whimsy" budget gets spent. Used well, it makes
the product feel alive without being distracting. Used poorly, it makes
everything feel slow and toy-like.

### Two curves, one duration

We use two easing curves, both based on the same family. The duration
is **180 ms** for almost everything. Faster than that and motion isn't
perceived; slower and the user notices waiting.

| Token | Bezier | Use |
|---|---|---|
| `motion.standard` | `cubic-bezier(0.32, 0.72, 0, 1)` | default — most transitions |
| `motion.entrance` | `cubic-bezier(0.16, 1, 0.3, 1)`  | element appearing (fade-in, slide-in) |

| Duration token | Value | Use |
|---|---|---|
| `duration.instant` | 0 ms    | reduced-motion preference |
| `duration.fast`    | 120 ms  | hover state, button press |
| `duration.default` | 180 ms  | **most transitions** |
| `duration.slow`    | 320 ms  | page-level, sheet present/dismiss |
| `duration.editorial` | 600 ms | one-per-session moments only |

`motion.standard` is the same curve Apple uses for many system animations.
Reusing it means our app feels like it belongs on macOS without being
indistinguishable.

### Reduce motion respect

When the user has "Reduce motion" enabled at the OS level, all durations
collapse to `duration.instant` and entrance curves become opacity-only.
Don't override this preference, ever.

### Don't animate things that don't need to move

A loading spinner is motion that earns its keep. A coral underline that
slides in on hover does not. Default to no animation; add motion when
something needs to communicate causality (this came from there) or state
change (this is now selected).

### The "editorial" moment

`duration.editorial` (600 ms) is for one moment per session — usually the
app's first run, or the moment a long operation completes. A graceful
fade, a serif heading writing itself in, a single layered-strata mark
settling onto the canvas. Use it once. Once.

## The whimsy budget

You have, per session:

- One use of Instrument Serif Italic for an editorial flourish.
- One `duration.editorial` motion.
- One unexpected microcopy line.
- One use of the layered-strata motif as decoration (not a logo).

Spend them all and the product feels twee. Spend none and it feels cold.
Spend one or two and it feels like it has a soul.

## Do / don't

**Do**

- Use space tokens. `xxs` to `xxl`, that's the entire vocabulary.
- Use one radius per component family.
- Use `elevation.1` to `elevation.4` and skip nothing.
- Honor reduced motion.

**Don't**

- Hardcode pixel values in margins, paddings, or gaps.
- Mix `radius.sm` inside `radius.lg`.
- Use pure-black shadows. Use `coral.900` at low alpha.
- Animate everything because it's possible.
- Make one screen carry the whole whimsy budget.
