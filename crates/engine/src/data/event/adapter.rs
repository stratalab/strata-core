//! Event capability branch adapter — comparison only.
//!
//! Event streams are append-only and sequenced: one authored [`RowClass::Event`]
//! row per event, keyed by the space prefix followed by the big-endian sequence,
//! with the event envelope as the value. Comparison is well defined — a diff
//! reports the sequences that differ between two branches (added where one branch
//! appended further, modified where both branches appended distinct events at the
//! same sequence). Promotion is not: divergent appends cannot be reordered or
//! merged without breaking the sequence and hash chain, so
//! [`CapabilityBranchAdapter::supports_promotion`] is `false` and events are
//! compared but never promoted.

use crate::branch::adapter::{
    CapabilityBranchAdapter, ComparableEntity, DerivedDisposition, EntitySummary,
};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_event_key_sequence, encode_event_space_prefix, PersistenceReadRow, RowClass,
};

/// The event capability's branch adapter (comparison only).
pub(crate) struct EventBranchAdapter;

impl CapabilityBranchAdapter for EventBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::Event
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn supports_promotion(&self) -> bool {
        false
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_event_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        // Validate the row is a well-formed event key in this space, rejecting
        // foreign-space and malformed keys with the event capability's diagnostic.
        decode_event_key_sequence(space, row.key())?;
        let identity = row
            .key()
            .strip_prefix(encode_event_space_prefix(space).as_slice())
            .ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.event_key",
                    "stored event row key is outside the requested space",
                )
            })?
            .to_vec();
        let summary = if row.is_tombstone() {
            EntitySummary::Absent
        } else {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.event_record",
                    "stored event row is present but carries no value",
                )
            })?;
            EntitySummary::Present(value.to_vec())
        };
        Ok(ComparableEntity::new(
            identity,
            summary,
            row.commit_version(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::EventBranchAdapter;

    use strata_core::CommitVersion;

    use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
    use crate::data::event::EventSequence;
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        encode_event_key, encode_event_space_prefix, PersistenceReadRow, RowClass,
    };

    fn space() -> ProductSpace {
        ProductSpace::new("default").expect("default is a valid space")
    }

    fn event_row(space: &ProductSpace, sequence: u64, value: Option<&[u8]>) -> PersistenceReadRow {
        let encoded = encode_event_key(space, EventSequence::new(sequence));
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), false)
    }

    #[test]
    fn interpret_row_decodes_a_present_event_row() {
        let space = space();
        let entity = EventBranchAdapter
            .interpret_row(&space, &event_row(&space, 7, Some(b"envelope")))
            .expect("decodes a present event row");
        assert_eq!(
            entity.summary(),
            &EntitySummary::Present(b"envelope".to_vec())
        );
        assert_eq!(entity.version(), CommitVersion::new(1));
        assert!(!entity.is_tombstone());
    }

    #[test]
    fn identity_distinguishes_sequences() {
        let space = space();
        let a = EventBranchAdapter
            .interpret_row(&space, &event_row(&space, 1, Some(b"x")))
            .expect("decodes");
        let b = EventBranchAdapter
            .interpret_row(&space, &event_row(&space, 2, Some(b"x")))
            .expect("decodes");
        assert_ne!(a.identity(), b.identity(), "the sequence is the identity");
    }

    #[test]
    fn interpret_row_rejects_a_key_from_another_space() {
        let other = ProductSpace::new("other").expect("other is a valid space");
        let error = EventBranchAdapter
            .interpret_row(&space(), &event_row(&other, 1, Some(b"x")))
            .expect_err("a key encoded for another space is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_key");
    }

    #[test]
    fn interpret_row_rejects_a_present_row_without_a_value() {
        let space = space();
        let error = EventBranchAdapter
            .interpret_row(&space, &event_row(&space, 3, None))
            .expect_err("a present row without a value is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.event_record");
    }

    #[test]
    fn events_are_compared_but_not_promoted() {
        assert_eq!(EventBranchAdapter.row_class(), RowClass::Event);
        assert_eq!(
            EventBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
        assert!(
            !EventBranchAdapter.supports_promotion(),
            "events are compared but never promoted"
        );
        assert_eq!(
            EventBranchAdapter.space_prefix(&space()),
            encode_event_space_prefix(&space())
        );
    }
}
