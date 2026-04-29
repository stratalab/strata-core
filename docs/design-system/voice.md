# Voice & Tone

Strata writes the way a smart, well-read colleague talks: precise,
unhurried, occasionally funny. Not breathless. Not corporate. Not cute.

Voice is what we sound like everywhere. Tone is how we adapt — quieter in
errors, warmer in onboarding, sharper in marketing. The voice is constant.

## Voice

Strata is:

- **Precise.** "Compacted 12,438 rows in 2.4 s." Not "All done!"
- **Curious.** Strata likes ideas. It links to the paper. It cites the
  source. It explains why.
- **Warm.** Even when it's terse, it isn't cold. It assumes the reader is
  smart, and meets them there.
- **Unhurried.** Strata doesn't shout. No exclamation marks. No "Oops!"
  No urgency that doesn't exist in the system.
- **Occasionally funny.** A dry observation, an unexpected word, a turn of
  phrase. Once per surface, max. (See the [whimsy budget](space-shape-motion.md#the-whimsy-budget).)

Strata is *not*:

- **Breathless.** "Lightning-fast", "blazing", "supercharged" — we don't
  talk like that.
- **Apologetic.** "Sorry, something went wrong" is worse than saying what
  went wrong.
- **Reassuring with nothing to back it.** "Don't worry!" is a tell that
  the writer didn't have anything useful to add.
- **Marketingy in product.** Save the marketing voice for the website.
  Inside the app, we are an instrument, not a brochure.
- **Mascot-y.** No anthropomorphism. Strata is a tool, not a friend.

## Tone, by surface

### Product UI

**Tight, factual, second-person.**

| Example | Don't | Do |
|---|---|---|
| Empty state | "No queries yet — let's get started!" | "No queries yet." |
| Confirmation | "Are you sure you want to delete this branch? This action cannot be undone." | "Delete branch `feature/embeddings`? Existing queries against it will fail." |
| Loading | "Loading…" | "Loading 2.3M rows from `events`." |
| Success | "Yay! All done." | "Compacted in 2.4 s." |
| Error | "Oops! Something went wrong." | "Couldn't connect to `localhost:5432`. Server may be down." |

### Errors

The single most important tone in the product. Three rules:

1. **Say what happened, in plain language.** Use the actual identifier,
   the actual file, the actual reason.
2. **Say what to do about it.** If you can't, say what the user might
   try.
3. **Don't apologize.** "Sorry" is filler. The error is the apology.

| Don't | Do |
|---|---|
| "Sorry, an error occurred." | "Index `events_ts_idx` is locked by another process (PID 4821)." |
| "Connection failed!" | "Couldn't reach `localhost:5432` — is the server running?" |
| "Invalid input." | "`limit` must be a positive integer. Got `-1`." |

### Onboarding

**Slightly more editorial. Explain why, not just what.**

This is the place where Instrument Serif gets its biggest workout, and
where the writing should reward reading. Use full sentences. Tell the
user what the concept is *for*, and link to the paper or doc that goes
deeper.

| Don't | Do |
|---|---|
| "Branches let you isolate state." | "Branches let you fork state without copying it. Foundry creates one per session, so you can experiment without touching production data." |

### Docs

**Editorial. Generous. Linked.**

Docs read like the product. Show the work — link to the paper, the
benchmark, the source. Treat the reader as someone who likes knowing
things (because they do, or they wouldn't be here).

A good doc paragraph:

> Strata uses an LSM-tree under the hood, which is why writes are fast
> and reads against unflushed data go through a memtable. If you want
> the full picture, the
> [original LSM paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf) is
> short and worth your time.

A bad doc paragraph:

> Strata is a high-performance database with cutting-edge architecture.

### Marketing

**The voice is the same. The volume is slightly higher.**

The website can be more declarative, more confident, more visually
expressive. But it's still Strata — no breathless adjectives, no
mascots, no exclamation marks.

| Don't | Do |
|---|---|
| "🚀 Blazing-fast vector search!" | "Vector search at the speed of a real index. Because that's what it is." |
| "The future of databases is here!" | "We started Strata because the AI era needs a database that thinks the way models do. So we wrote one." |

## Microcopy patterns

Small things that come up a lot.

### Buttons

Buttons say what they do. **Verb + noun**, in title case.

| Don't | Do |
|---|---|
| "OK" | "Save changes" |
| "Submit" | "Run query" |
| "Cancel" | "Cancel" (this one is fine) |
| "Yes" | "Delete branch" |

The destructive button names what it destroys. "Delete branch", not
"Confirm".

### Links

Links use **the noun phrase**, not the verb.

| Don't | Do |
|---|---|
| "Click here to read about branches." | "Read about [how branches work](#)." |
| "Learn more →" | "Read the [LSM paper](#)." |

### Labels

Field labels are **nouns or short noun phrases**, no colon, sentence case.

| Don't | Do |
|---|---|
| "Enter your name:" | "Name" |
| "API KEY" | "API key" |
| "Choose a database" | "Database" |

### Numbers and units

Always include the unit. Always show enough precision to be useful, with
the full value on hover.

| Don't | Do |
|---|---|
| "Loaded 2.3 million rows" | "Loaded 2,341,892 rows" (or "2.34M rows" with `2,341,892` on hover) |
| "Ran in 2 seconds" | "Ran in 2.4 s" |
| "85%" | "85%" (this is fine) |
| "fast" | "12 ms" |

### Time

Use **relative time** for recent events, **absolute time** for older ones.
Always include the absolute time on hover.

| Don't | Do |
|---|---|
| "2026-04-28T14:32:11Z" (in product UI) | "2 minutes ago" (with full timestamp on hover) |
| "12 days ago" | "Apr 16" (this year) or "Apr 16, 2025" (different year) |

### Loading and progress

Show the user what's happening, by name and by number.

| Don't | Do |
|---|---|
| "Loading…" | "Indexing 12,438 rows… (3,200 / 12,438)" |
| "Please wait." | "Compacting `events_2026q1` — usually takes 30–60 s." |

## Names

**Internal concepts get good names.** A name is a tiny piece of
documentation; "stratum", "foundry", "petrify", "branch" all teach the
user something about the model.

A good name:

- Is one word, ideally a noun.
- Comes from a real concept (geology, metallurgy, paper-making) when it
  helps the user build a mental model.
- Doesn't try to be cute at the expense of being clear.

A bad name:

- Is a phrase. ("Database container handler.")
- Is a brand reference dressed as a verb. ("Stratafy.")
- Pretends to be technical when it's marketing. ("AI-powered intelligent
  caching layer.")

When in doubt, the boring noun beats the cute one.

## The "no" list

These do not appear in Strata writing, anywhere:

- **Exclamation marks.** Even one. The product doesn't shout.
- **Em-dash followed by exclamation.** "— it's amazing!" Just no.
- **"Powered by" / "Driven by".** Strata has features. They are features.
- **"Simply" / "Just".** They make the reader feel dumb when the thing
  isn't simple.
- **"Unleash" / "Supercharge" / "Turbocharge".**
- **"Don't worry".** If they shouldn't be worried, give them the reason.
- **Apologies the system isn't responsible for.** "Sorry, this is a beta
  feature." Either ship it or don't.
- **Emoji in product UI.** (Docs and marketing, sparingly. One per page.)
- **Sentence-ending ellipses for drama.** "Loading the future…"

## Do / don't

**Do**

- Read it aloud. If it sounds like a person, it's right. If it sounds
  like a press release, rewrite.
- Use real numbers, real names, real reasons.
- Trust the reader. They got here on purpose.

**Don't**

- Apologize for the system.
- Add adjectives where verbs would do.
- Use marketing voice in the product.
- Be cute when you should be clear.
