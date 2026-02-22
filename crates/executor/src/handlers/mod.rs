//! Command handlers organized by primitive category.
//!
//! Each submodule handles commands for a specific primitive:
//!
//! | Module | Commands | Primitive |
//! |--------|----------|-----------|
//! | `kv` | 15 | KVStore, KVStoreBatch |
//! | `json` | 17 | JsonStore |
//! | `event` | 11 | EventLog |
//! | `state` | 8 | StateCell |
//! | `vector` | 19 | VectorStore |
//! | `branch` | 24 | BranchIndex |
//! | `transaction` | 5 | TransactionControl |
//! | `retention` | 3 | RetentionSubstrate |
//! | `database` | 4 | Database-level |

pub mod branch;
pub mod config;
pub mod configure_model;
pub mod embed;
pub mod embed_hook;
pub mod event;
pub mod generate;
pub mod graph;
pub mod json;
pub mod kv;
pub mod models;
pub mod search;
pub mod space;
pub mod state;
pub mod vector;

// Transaction commands are deferred because the Executor is stateless by design.
// Transactions require session state management which would need additional design work.
//
// Retention commands (RetentionApply, RetentionStats, RetentionPreview) are deferred
// as they require additional infrastructure for garbage collection statistics.
//
// Database commands (Ping, Info, Flush, Compact) are implemented directly in executor.rs.
