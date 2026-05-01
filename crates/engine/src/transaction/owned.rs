//! Owned transaction handle for manual transaction lifecycle.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::context::Transaction as ScopedTransaction;
use super::json_state::JsonTxnState;
use crate::{StrataError, StrataResult};
use strata_core::BranchId;
use strata_storage::{Namespace, TransactionContext};

use crate::Database;

/// Owned manual transaction handle.
///
/// The handle owns the pooled `TransactionContext` and returns it to the
/// engine automatically on commit, abort, or drop.
pub struct Transaction {
    db: Arc<Database>,
    ctx: Option<TransactionContext>,
    json_state: JsonTxnState,
}

impl Transaction {
    /// Construct an owned transaction handle from an active context.
    pub(crate) fn new(db: Arc<Database>, ctx: TransactionContext) -> Self {
        Self {
            db,
            ctx: Some(ctx),
            json_state: JsonTxnState::new(),
        }
    }

    /// Commit the transaction and return its commit version.
    pub fn commit(&mut self) -> StrataResult<u64> {
        let mut ctx = self
            .ctx
            .take()
            .ok_or_else(|| StrataError::transaction_not_active("finished"))?;
        if let Err(error) = self
            .db
            .prepare_json_transaction_context(&mut ctx, &mut self.json_state)
        {
            self.db
                .abort_transaction_in_place(&mut ctx, format!("Commit failed: {}", error));
            self.db.end_transaction_context(ctx);
            return Err(error);
        }
        let result = self.db.commit_transaction_context(&mut ctx);
        self.db.end_transaction_context(ctx);
        result
    }

    /// Abort the transaction and release its pooled context.
    pub fn abort(&mut self) {
        if let Some(mut ctx) = self.ctx.take() {
            self.db
                .abort_transaction_in_place(&mut ctx, "Transaction aborted");
            self.db.end_transaction_context(ctx);
        }
    }

    /// Get the transaction's branch ID.
    pub fn branch_id(&self) -> BranchId {
        self.context()
            .expect("branch_id() requires an active transaction")
            .branch_id
    }

    /// Get a mutable reference to the underlying context.
    pub fn context_mut(&mut self) -> Option<&mut TransactionContext> {
        self.ctx.as_mut()
    }

    /// Create a scoped primitive wrapper that shares this manual transaction's
    /// engine-owned JSON state.
    pub fn scoped(&mut self, namespace: Arc<Namespace>) -> ScopedTransaction<'_> {
        let ctx = self
            .ctx
            .as_mut()
            .expect("transaction context not available after commit/abort");
        ScopedTransaction::with_json_state(
            ctx,
            namespace,
            &mut self.json_state,
            self.db.storage().clone(),
        )
    }

    fn context(&self) -> Option<&TransactionContext> {
        self.ctx.as_ref()
    }
}

impl Deref for Transaction {
    type Target = TransactionContext;

    fn deref(&self) -> &Self::Target {
        self.context()
            .expect("transaction context not available after commit/abort")
    }
}

impl DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.context_mut()
            .expect("transaction context not available after commit/abort")
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.abort();
    }
}
