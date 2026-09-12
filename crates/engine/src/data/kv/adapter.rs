//! KV capability branch adapter — the reference implementation of
//! [`CapabilityBranchAdapter`] (contract §KV: "KV is the reference branch
//! adapter").
//!
//! KV rows are authored user data compared by space and key. Decoding a row key
//! back through [`decode_kv_key`] rejects a key outside the requested space with
//! the same structured diagnostic the rest of the KV capability uses, so the
//! adapter inherits malformed-byte rejection for free.
//!
//! `KvBranchAdapter` is consumed in production by the M12B compare workflow;
//! until that lands it is exercised only by this module's tests.
use crate::branch::adapter::{
    CapabilityBranchAdapter, ComparableEntity, DerivedDisposition, EntitySummary,
};
use crate::diagnostics::EngineError;
use crate::persistence::{decode_kv_key, encode_kv_space_prefix, PersistenceReadRow, RowClass};

use super::ProductSpace;

/// The KV capability's branch adapter.
pub(crate) struct KvBranchAdapter;

impl CapabilityBranchAdapter for KvBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::Kv
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_kv_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        let key = decode_kv_key(space, row.key())?;
        let summary = if row.is_tombstone() {
            EntitySummary::Absent
        } else {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.kv_value",
                    "stored KV row is present but carries no value",
                )
            })?;
            EntitySummary::Present(value.to_vec())
        };
        Ok(ComparableEntity::new(
            key.into_bytes(),
            summary,
            row.commit_version(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{KvBranchAdapter, ProductSpace};

    use strata_core::CommitVersion;

    use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
    use crate::data::kv::KvKey;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{encode_kv_key, encode_kv_space_prefix, PersistenceReadRow, RowClass};

    fn space() -> ProductSpace {
        ProductSpace::new("default").expect("default is a valid space")
    }

    fn kv_row(
        space: &ProductSpace,
        key: &[u8],
        value: Option<&[u8]>,
        tombstone: bool,
    ) -> PersistenceReadRow {
        let encoded = encode_kv_key(space, &KvKey::new(key).expect("valid key"));
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), tombstone)
    }

    #[test]
    fn interpret_row_decodes_a_present_kv_row() {
        let space = space();
        let row = kv_row(&space, b"alpha", Some(b"one"), false);
        let entity = KvBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a present KV row");
        assert_eq!(entity.identity(), b"alpha");
        assert_eq!(entity.summary(), &EntitySummary::Present(b"one".to_vec()));
        assert_eq!(entity.version(), CommitVersion::new(1));
        assert!(!entity.is_tombstone());
    }

    #[test]
    fn interpret_row_maps_a_kv_tombstone_to_absent() {
        let space = space();
        let row = kv_row(&space, b"gone", None, true);
        let entity = KvBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a tombstone");
        assert!(entity.is_tombstone());
        assert_eq!(entity.summary(), &EntitySummary::Absent);
    }

    #[test]
    fn interpret_row_rejects_a_key_from_another_space() {
        let other = ProductSpace::new("other").expect("other is a valid space");
        let row = kv_row(&other, b"alpha", Some(b"one"), false);
        let error = KvBranchAdapter
            .interpret_row(&space(), &row)
            .expect_err("a key encoded for another space is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.kv_key");
    }

    #[test]
    fn interpret_row_rejects_a_present_row_without_a_value() {
        let space = space();
        let row = kv_row(&space, b"beta", None, false);
        let error = KvBranchAdapter
            .interpret_row(&space, &row)
            .expect_err("a present row without a value is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.kv_value");
    }

    #[test]
    fn space_prefix_matches_the_kv_encoding() {
        let space = space();
        assert_eq!(
            KvBranchAdapter.space_prefix(&space),
            encode_kv_space_prefix(&space)
        );
    }

    #[test]
    fn row_class_and_disposition_are_reported() {
        assert_eq!(KvBranchAdapter.row_class(), RowClass::Kv);
        assert_eq!(
            KvBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
    }
}
