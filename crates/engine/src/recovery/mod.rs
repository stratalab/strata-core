//! Pluggable recovery + freeze via the `Subsystem` trait.
//!
//! A subsystem is an independent component (vector index, search index,
//! graph adjacency cache, …) that owns runtime state outside
//! `SegmentedStore` and needs to rebuild that state on database open and
//! optionally persist it on drop. Each `Database` holds a
//! `Vec<Box<dyn Subsystem>>` supplied at open time via [`DatabaseBuilder`];
//! the **same ordered list** drives recovery (in registration order) and
//! freeze (in reverse order), so callers cannot accidentally recover one
//! set of subsystems while freezing a different set — the guarantee the
//! lifecycle-unification refactor in stratalab/strata-core#2354 exists to
//! enforce.
//!
//! ## Composition lives above the engine
//!
//! The engine crate declares the trait and the builder but does not
//! compose the production subsystem list itself, because it cannot
//! depend on `strata-vector` (adding that edge would cycle —
//! `strata-vector → strata-engine`). The executor owns composition:
//! [`crates::executor::src::api::mod::strata_db_builder`] returns a
//! `DatabaseBuilder` preloaded with `[VectorSubsystem, SearchSubsystem]`
//! and every `Strata::open` / `open_with` / `cache` path routes through
//! it. Engine-internal tests that do not load the vector crate can
//! install `[SearchSubsystem]` directly via the builder.
//!
//! ## Lifecycle
//!
//! ```text
//! DatabaseBuilder::new()
//!     .with_subsystem(VectorSubsystem)
//!     .with_subsystem(SearchSubsystem)
//!     .open(path)
//!         │
//!         ▼
//! Database::open_internal_with_subsystems
//!     1. WAL replay + segment recovery (engine-owned, no subsystem)
//!     2. for subsystem in &subsystems { subsystem.recover(&db)?; }
//!     3. db.set_subsystems(subsystems)    // same list, same order
//!         │
//!         ▼
//!     // ... database handed to caller ...
//!         │
//!         ▼
//! Drop for Database (or Database::shutdown)
//!     4. run_freeze_hooks():
//!            for subsystem in subsystems.iter().rev() {
//!                subsystem.freeze(&db)    // best-effort, logs on failure
//!            }
//! ```
//!
//! See [`Subsystem`] for the trait definition and
//! `crates/engine/src/database/builder.rs` for the builder API.
//!
//! [`DatabaseBuilder`]: crate::DatabaseBuilder

mod subsystem;

pub use subsystem::Subsystem;
