//! Owned transaction handle for manual transaction lifecycle.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use strata_concurrency::TransactionContext;
use strata_core::types::BranchId;
use strata_core::{StrataError, StrataResult};

use crate::Database;

/// Owned manual transaction handle.
///
/// The handle owns the pooled `TransactionContext` and returns it to the
/// engine automatically on commit, abort, or drop.
pub struct Transaction {
    db: Arc<Database>,
    ctx: Option<TransactionContext>,
}

impl Transaction {
    /// Construct an owned transaction handle from an active context.
    pub(crate) fn new(db: Arc<Database>, ctx: TransactionContext) -> Self {
        Self { db, ctx: Some(ctx) }
    }

    /// Commit the transaction and return its commit version.
    pub fn commit(&mut self) -> StrataResult<u64> {
        let mut ctx = self
            .ctx
            .take()
            .ok_or_else(|| StrataError::transaction_not_active("finished"))?;
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
