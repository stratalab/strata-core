//! Transaction management for performance and TransactionOps
//!
//! This module provides:
//! - Thread-local transaction pooling (zero allocations after warmup)
//! - Pool management utilities
//! - Transaction wrapper implementing TransactionOps
//!
//! # Architecture
//!
//! The pool uses thread-local storage to avoid synchronization overhead:
//! - Each thread has its own pool of up to 8 TransactionContext objects
//! - Contexts are reset (not reallocated) when reused
//! - HashMap/HashSet capacity is preserved across reuse
//!
//! # TransactionOps
//!
//! The Transaction type wraps TransactionContext and implements the
//! TransactionOps trait for unified primitive access within transactions.

pub mod context;
pub mod json_state;
pub mod owned;
pub mod pool;

pub use context::Transaction as ScopedTransaction;
pub use owned::Transaction;
pub use pool::{TransactionPool, MAX_POOL_SIZE};
