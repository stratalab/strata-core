//! # StrataDB
//!
//! An embedded database for AI agents — six primitives, branch isolation,
//! and deterministic replay.
//!
//! StrataDB provides purpose-built data structures for AI agent state management:
//! KV Store, Event Log, State Cell, JSON Store, Vector Store, and Branches.
//! Data is organized into branches (isolation) and spaces (organization within branches).
//!
//! # Quick Start
//!
//! ```no_run
//! use stratadb::{Strata, Value};
//!
//! fn main() -> stratadb::Result<()> {
//!     let mut db = Strata::open("./my-data")?;
//!
//!     // Key-value storage
//!     db.kv_put("user:name", "Alice")?;
//!     assert_eq!(db.kv_get("user:name")?, Some(Value::String("Alice".into())));
//!
//!     // Append-only event log
//!     db.event_append("tool_call", serde_json::json!({"tool": "search"}).into())?;
//!
//!     // Branch isolation (like git branches)
//!     db.branches().create("experiment")?;
//!     db.set_branch("experiment")?;
//!     assert!(db.kv_get("user:name")?.is_none()); // isolated
//!
//!     // Space organization (within branches)
//!     db.set_space("conversations")?;
//!     db.kv_put("msg_001", "hello")?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Data Primitives
//!
//! | Primitive | Purpose | Key Methods |
//! |-----------|---------|-------------|
//! | **KV Store** | Working memory, config | `kv_put`, `kv_get`, `kv_delete`, `kv_list` |
//! | **Event Log** | Immutable audit trail | `event_append`, `event_get`, `event_get_by_type` |
//! | **State Cell** | CAS-based coordination | `state_set`, `state_get`, `state_cas` |
//! | **JSON Store** | Structured documents | `json_set`, `json_get`, `json_delete` |
//! | **Vector Store** | Embeddings, similarity search | `vector_upsert`, `vector_query` |
//! | **Branch** | Data isolation | `branches().create()`, `set_branch`, `branches().list()` |
//!
//! # Architecture
//!
//! The [`Strata`] struct is the main entry point. All operations go through it.
//! For low-level or cross-language use, the [`Command`]/[`Output`] enum pair
//! provides a serializable instruction set.
//!
//! Internal crates (storage, concurrency, durability, engine) are not exposed.
//! Only the public API surface in this crate is stable.

// Use jemalloc for better memory return behavior under heavy alloc/free churn.
// glibc malloc retains freed pages in the process address space, causing RSS to
// grow monotonically under transaction workloads. jemalloc uses madvise(MADV_DONTNEED)
// to return pages promptly.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Re-export the public API from strata-executor
pub use strata_executor::*;
