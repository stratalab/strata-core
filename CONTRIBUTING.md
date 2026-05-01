# Contributing to StrataDB

Thank you for your interest in contributing to StrataDB.

## Development Setup

### Prerequisites

- **Rust 1.70+** (install via [rustup](https://rustup.rs/))
- **Git**

### Clone and Build

```bash
git clone https://github.com/anibjoshi/strata.git
cd strata
cargo build --workspace
```

### Verify Setup

```bash
cargo test --workspace
```

## Workspace Structure

```
strata-core/
  Cargo.toml                  # Workspace root (re-exports strata-executor)
  crates/
    core/                     # Shared types: Value, BranchId, Key, Namespace
    storage/                  # ShardedStore (DashMap), inverted index, TTL
    concurrency/              # OCC transactions, conflict detection, CAS
    durability/               # WAL, snapshots, crash recovery, BranchBundle
    engine/                   # Database, primitives, transaction coordination
    intelligence/             # BM25, RRF, hybrid search
    executor/                 # Public API: Strata, Session, Command, Output
  docs/                       # Documentation
  tests/                      # Integration tests
```

Dependencies generally flow downward: `executor` → `engine` → `storage`.
`engine` still carries temporary direct seams to `core-legacy` and
`security` until the final convergence work lands.

## Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p strata-executor
cargo test -p strata-engine
cargo test -p strata-storage

# With output
cargo test --workspace -- --nocapture

# Single test
cargo test -p strata-executor test_kv_put_get
```

## Code Style

- Follow standard Rust formatting: `cargo fmt --all`
- No warnings: `cargo clippy --workspace`
- Use `thiserror` for error types
- Use `Into<Value>` ergonomics in public APIs
- Document public types and methods with doc comments
- Use `Strata::cache()` in test code

## Making Changes

### Branch Naming

Use descriptive prefixes:
- `feat/` — new features
- `fix/` — bug fixes
- `cleanup/` — refactoring, dead code removal
- `docs/` — documentation changes

### Commit Messages

Write clear, concise commit messages:
- Start with a verb in imperative mood ("Add", "Fix", "Remove", not "Added", "Fixed")
- First line under 72 characters
- Reference issues where relevant

### Pull Request Process

1. Create a branch from `main`
2. Make your changes
3. Run `cargo test --workspace` and `cargo clippy --workspace`
4. Push and open a PR against `main`
5. Describe what changed and why in the PR description
6. Link related issues

## Architecture Guidelines

- **Keep crate boundaries clean.** Don't add cross-crate dependencies unless necessary.
- **All public API goes through `strata-executor`.** Users should never need to import internal crates.
- **All data operations are branch-scoped.** Never write code that ignores branch isolation.
- **Prefer `Into<Value>` for public APIs.** Users shouldn't need to construct `Value` manually.
- **Test at the right level.** Unit tests in each crate, integration tests in `tests/`.

## Documentation

- User-facing docs live in `docs/`
- Internal design docs live in `docs/internal/`
- Use relative links between markdown files
- Code examples should be compilable (or clearly marked `ignore`)
- Use "StrataDB" as the product name, `stratadb` as the crate name, `Strata` as the struct name

## Questions?

Open an issue on [GitHub](https://github.com/anibjoshi/strata/issues) if you have questions about contributing.
