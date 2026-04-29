# Strata Design System

This is the design system for everything that wears the Strata name — the
Foundry app, the website, decks, docs, marketing, and anything we ship to
users. It exists so that Strata feels like one product across every surface,
and so that contributors can make confident visual decisions without
re-litigating the basics.

## What it is

A **Primer-architected, Strata-skinned** design system. We borrow the token
architecture and component patterns from [GitHub Primer](https://primer.style)
because they are battle-tested in a product more complex than ours. We replace
the visuals — palette, type, elevation, motion — with our own.

We did not invent a new system from scratch. We did not adopt Primer
unchanged. We took the bones and put our own skin on them.

## What it covers

| File | Topic |
|---|---|
| [principles.md](principles.md) | The five attributes, turned into design principles |
| [color.md](color.md) | Palette, functional tokens, light + dark pairs |
| [typography.md](typography.md) | Inter, Instrument Serif, JetBrains Mono — scale and usage |
| [space-shape-motion.md](space-shape-motion.md) | Spacing, radii, elevation, motion |
| [voice.md](voice.md) | How Strata speaks |

## The five attributes

Strata's design and writing should feel:

1. **Quiet luxury** — restrained, warm, premium. Not loud. Not flashy.
2. **Sleek and classy** — refined geometry, careful spacing, considered type.
3. **Competence and intelligence** — every element earns its place.
4. **Voracious appetite for knowledge** — curious, generous, editorial.
5. **A little whimsical** — a few moments of delight, used sparingly.

These attributes are not equally weighted. *Quiet luxury* and *competence* are
the load-bearing ones. *Whimsy* is the seasoning — too much and the system
stops feeling serious.

See [principles.md](principles.md) for how each attribute becomes an
actionable rule.

## How to use this system

**If you're shipping UI in the Foundry app**, the tokens live in
`StrataFoundry/SharedUI/DesignSystem.swift`. Use the named tokens
(`StrataColor.bg.default`, `StrataType.body`, `StrataSpacing.md`) — never
hardcode values. If a token is missing, propose it here first, then add it.

**If you're shipping web or marketing surfaces**, the tokens are mirrored as
CSS custom properties (see [color.md](color.md)). Same names, same values.

**If you're making a new brand surface** (deck, illustration, photography,
swag), start with [principles.md](principles.md) and
[color.md](color.md). When in doubt, lean toward less color, more space,
warmer neutrals.

## Token naming

We use **functional/aliased** token names, not raw color names. So
`bg.default`, not `coral-50`. The base scale exists, but UI code should never
reach into it — only the functional layer.

This is how Primer does it. It's worth the discipline because:

- Theming (light/dark, future themes) is a change in one place.
- Reading UI code tells you *what* a color is for, not just what hue it is.
- Refactoring the palette doesn't require touching every component.

## What's out of scope

- **Iconography** — we use [SF Symbols](https://developer.apple.com/sf-symbols/)
  in Foundry. A custom icon set is not justified at our stage.
- **Illustration system** — the layered-strata motif from the logo is the
  only brand mark. We do not have a broader illustration style yet, and won't
  invent one prematurely.
- **Component library** — patterns are described informally in each doc. A
  formal component spec lives in code, not in markdown.

## Changing the system

The design system is owned by whoever is shipping the surface most actively.
At time of writing, that's the Foundry team. Propose changes by editing these
docs in a PR; the change should land *before* the code that depends on it.

Don't sneak new tokens in through a feature PR. The tokens are the contract.
