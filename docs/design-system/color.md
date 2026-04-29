# Color

The Strata color system has two layers:

1. A **base scale** — the raw colors. Named by hue.
2. A **functional layer** — the names UI code uses. Named by purpose.

UI code only ever touches the functional layer. The base scale exists so the
functional layer can be remapped (light/dark, future themes) without touching
component code.

## Base scale

### Coral (brand)

The coral family is the singular brand color. `coral.400` is the exact
terra-cotta from the logo and the [stratadb.org](https://stratadb.org)
website. Everything else in the family is tuned around it.

| Token | Hex | Use |
|---|---|---|
| `coral.50`  | `#FDF1ED` | hover tint on light surfaces |
| `coral.100` | `#FAE0D6` | very subtle bg fill |
| `coral.200` | `#F4A58A` | peach — light highlights, glows |
| `coral.300` | `#EC8D6E` | light accent |
| `coral.400` | `#E07A5F` | **primary brand** — buttons, links, emphasis |
| `coral.500` | `#D26851` | hover on light theme |
| `coral.600` | `#C96A52` | pressed / active |
| `coral.700` | `#A8543F` | deep coral, dense text emphasis |
| `coral.800` | `#80402F` | terra-cotta dark |
| `coral.900` | `#4A2418` | deepest, rarely used |

### Warm neutrals (surfaces and text)

Strata's neutrals are warm — slightly biased toward the coral family rather
than cool blue-grays. This is where "quiet luxury" lives. Pure black and pure
white never appear as primary surfaces.

| Token | Hex | Use |
|---|---|---|
| `neutral.0`   | `#FFFFFF` | reserved — pure white, almost never used |
| `neutral.50`  | `#FAF9F7` | primary light surface (warm off-white) |
| `neutral.100` | `#F2EFEB` | recessed light, hover bg on light |
| `neutral.200` | `#E5E0D8` | borders on light |
| `neutral.300` | `#C9C2B5` | secondary border light, disabled |
| `neutral.400` | `#908778` | muted text on light |
| `neutral.500` | `#5C5448` | secondary text on light |
| `neutral.600` | `#3F3A30` | body text on light |
| `neutral.700` | `#2A2620` | primary text on light |
| `neutral.800` | `#1A1815` | display text on light, deep ink |
| `neutral.850` | `#111113` | elevated dark surface |
| `neutral.900` | `#0A0A0B` | primary dark surface |
| `neutral.950` | `#050506` | recessed dark, deepest panel |
| `neutral.999` | `#000000` | reserved — pure black, never primary |

### Cyan (intelligence accent)

The cyan glow on the logo is the only place a non-coral brand color appears.
In product, cyan is the **AI / intelligence** signal — used to mark
generation, embeddings, model output, anything that came from a model. Never
decorative.

| Token | Hex | Use |
|---|---|---|
| `cyan.200` | `#C2EAEA` | subtle tint, "model said this" bg |
| `cyan.400` | `#7EDDDD` | the logo glow — primary cyan |
| `cyan.600` | `#3FA8A8` | cyan that's usable on light bg |
| `cyan.800` | `#1F5252` | deep cyan, text emphasis on light |

### Semantic

Semantic colors are slightly desaturated from Tailwind defaults to keep
the palette feeling cohesive with the warm neutrals. Each has light and
dark variants for the two themes.

| Family | Light fg | Dark fg | Notes |
|---|---|---|---|
| Success | `#15803D` | `#22C55E` | green |
| Warning | `#B45309` | `#FBBF24` | amber |
| Danger  | `#B91C1C` | `#F87171` | red |
| Info    | `#1D4ED8` | `#60A5FA` | blue |

## Functional tokens

This is the layer UI code uses. Each token has a *light* and a *dark* mapping
to the base scale. Token names are stable; the underlying base hex can change.

### Surfaces

| Token | Light → | Dark → | Use |
|---|---|---|---|
| `bg.canvas`     | `neutral.50`  | `neutral.900` | page background |
| `bg.default`    | `neutral.50`  | `neutral.900` | default content surface |
| `bg.subtle`     | `neutral.100` | `neutral.850` | recessed (sidebar, code block) |
| `bg.muted`      | `neutral.100` | `neutral.850` | hover bg on default |
| `bg.raised`     | `#FFFFFF`     | `neutral.850` | one level above canvas (card) |
| `bg.overlay`    | `#FFFFFF`     | `neutral.850` | modal, dialog, popover |
| `bg.sunken`     | `neutral.100` | `neutral.950` | deeply recessed |
| `bg.emphasis`   | `neutral.800` | `neutral.50`  | high-contrast (toolbar, tooltip) |
| `bg.inverse`    | `neutral.900` | `neutral.50`  | opposite-theme surface |

### Text

| Token | Light → | Dark → | Use |
|---|---|---|---|
| `fg.default`      | `neutral.700` | `neutral.50`  | body |
| `fg.muted`        | `neutral.500` | `#A1A1AA`     | secondary |
| `fg.subtle`       | `neutral.400` | `neutral.400` | tertiary, captions |
| `fg.disabled`     | `neutral.300` | `neutral.500` | disabled state |
| `fg.on-emphasis`  | `neutral.50`  | `neutral.900` | text on `bg.emphasis` |
| `fg.on-accent`    | `#FFFFFF`     | `#FFFFFF`     | text on filled coral |
| `fg.display`      | `neutral.800` | `#FAFAFA`     | display headings |

### Borders

| Token | Light → | Dark → | Use |
|---|---|---|---|
| `border.default`  | `neutral.200` | `neutral.850` | primary border |
| `border.muted`    | `neutral.100` | `#1E1E22`     | dividers |
| `border.subtle`   | `neutral.100` | `#16161A`     | barely-there separators |
| `border.emphasis` | `coral.400`   | `coral.400`   | focus ring, selection |

### Accent (coral)

| Token | Light → | Dark → | Use |
|---|---|---|---|
| `accent.fg`       | `coral.500` | `coral.300` | coral text, links |
| `accent.emphasis` | `coral.400` | `coral.400` | filled (primary button bg) |
| `accent.muted`    | `coral.100` | `#3A1F18`   | tinted bg (selected row) |
| `accent.subtle`   | `coral.50`  | `#22130E`   | very subtle tint (hover bg) |

### Intelligence (cyan)

Used **only** for AI / model-generated content. Never decorative.

| Token | Light → | Dark → | Use |
|---|---|---|---|
| `intelligence.fg`       | `cyan.800` | `cyan.400` | text/icon for AI moments |
| `intelligence.emphasis` | `cyan.600` | `cyan.400` | filled (rare — model badges) |
| `intelligence.subtle`   | `cyan.200` | `#0E2A2A` | bg tint for model output |

### Semantic

Each family follows the same shape as `accent`: `.fg`, `.emphasis`,
`.muted`, `.subtle`. Mappings are in the implementation; this doc lists only
the foreground.

| Token | Light → | Dark → |
|---|---|---|
| `success.fg` | `#15803D` | `#22C55E` |
| `warning.fg` | `#B45309` | `#FBBF24` |
| `danger.fg`  | `#B91C1C` | `#F87171` |
| `info.fg`    | `#1D4ED8` | `#60A5FA` |

## Light vs dark

Both themes ship from day one. The website is dark-first; the Foundry app
follows the system theme. The palette is designed so the *same* component
code reads correctly in both themes by switching only the functional layer.

A few non-obvious choices worth calling out:

- **Light surface is `#FAF9F7`, not `#FFFFFF`.** Pure white is too cold and
  too cheap. The warm off-white is the "linen tablecloth" of the system.
- **Dark surface is `#0A0A0B`, not `#000000`.** Same reason in reverse —
  pure black on a modern OLED display reads as a void, not a background.
- **`fg.muted` is warmer on light than on dark.** Light theme uses
  `neutral.500` (warm), dark theme uses `#A1A1AA` (cooler) because warm
  grays go muddy on near-black.

## Usage rules

### One coral element per screen

Coral is the brand. If three things on a screen are coral, none of them are
the brand. Pick the most important affordance and let it carry the color.
Every other interactive element should rely on shape, weight, or position
for emphasis.

### Use coral on text, not on chrome

A coral link in body copy is luxurious. A coral toolbar is loud. When in
doubt, the coral lives on the *content*, not on the *frame*.

### Cyan only marks AI

`intelligence.*` tokens exist so the user can scan a screen and know which
content came from a model. Don't use cyan for "newness", "highlight",
"selection", or any other meaning. The signal degrades fast if you do.

### Semantic colors are for state, not categories

Green = success. Red = danger. Amber = warning. Blue = info. Don't use red
to mean "category B" or green to mean "category C" — once a user has seen
red on an error, they can't un-see it.

### Borders should be felt, not seen

Most borders use `border.muted` or `border.subtle`. `border.default` is for
real boundaries (input fields, cards). If you reach for a strong border to
separate two regions, try whitespace first.

## Do / don't

**Do**

- Use functional tokens (`bg.default`), not base tokens (`neutral.50`).
- Pair `bg.canvas` with `fg.default` and `bg.emphasis` with `fg.on-emphasis`.
  These pairs are tested for contrast (WCAG AA min, AAA for body).
- Use `accent.muted` for selected rows, not `accent.emphasis`.
- Use `intelligence.subtle` to mark a model-generated paragraph.

**Don't**

- Hardcode hex values in components.
- Use coral and cyan together as competing accents. Coral is brand; cyan
  marks AI. They mean different things.
- Use `neutral.0` (`#FFFFFF`) or `neutral.999` (`#000000`) as a primary
  surface. Both are reserved for edge cases.
- Mix two semantic colors in one component (e.g., a banner that's red and
  green at once). Pick one.
- Add a new color outside the functional layer. Propose it in this doc
  first, get it added, then use it.
