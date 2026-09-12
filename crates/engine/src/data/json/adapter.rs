//! JSON capability branch adapter (contract §JSON).
//!
//! V1 JSON branch comparison is document-level: the logical entity is the
//! document id and the value summary is the stored document bytes. Decoding a
//! row key through [`decode_json_document_id`] rejects a key outside the
//! requested space with a structured diagnostic, so the adapter inherits
//! malformed-byte rejection.

use crate::branch::adapter::{
    CapabilityBranchAdapter, ComparableEntity, DerivedDisposition, EntitySummary,
};
use crate::data::kv::ProductSpace;
use crate::diagnostics::EngineError;
use crate::persistence::{
    decode_json_document_id, encode_json_space_prefix, PersistenceReadRow, RowClass,
};

/// The JSON capability's branch adapter.
pub(crate) struct JsonBranchAdapter;

impl CapabilityBranchAdapter for JsonBranchAdapter {
    fn row_class(&self) -> RowClass {
        RowClass::Json
    }

    fn derived_disposition(&self) -> DerivedDisposition {
        DerivedDisposition::Authored
    }

    fn space_prefix(&self, space: &ProductSpace) -> Vec<u8> {
        encode_json_space_prefix(space)
    }

    fn interpret_row(
        &self,
        space: &ProductSpace,
        row: &PersistenceReadRow,
    ) -> Result<ComparableEntity, EngineError> {
        let id = decode_json_document_id(space, row.key())?;
        let summary = if row.is_tombstone() {
            EntitySummary::Absent
        } else {
            let value = row.value().ok_or_else(|| {
                EngineError::corruption(
                    "data_loss.engine.json_document",
                    "stored JSON document row is present but carries no value",
                )
            })?;
            EntitySummary::Present(value.to_vec())
        };
        Ok(ComparableEntity::new(
            id.as_str().as_bytes().to_vec(),
            summary,
            row.commit_version(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::JsonBranchAdapter;

    use strata_core::CommitVersion;

    use crate::branch::adapter::{CapabilityBranchAdapter, DerivedDisposition, EntitySummary};
    use crate::data::json::JsonDocumentId;
    use crate::data::kv::ProductSpace;
    use crate::diagnostics::EngineErrorClass;
    use crate::persistence::{
        encode_json_key, encode_json_space_prefix, PersistenceReadRow, RowClass,
    };

    fn space() -> ProductSpace {
        ProductSpace::new("default").expect("default is a valid space")
    }

    fn json_row(
        space: &ProductSpace,
        id: &str,
        value: Option<&[u8]>,
        tombstone: bool,
    ) -> PersistenceReadRow {
        let encoded = encode_json_key(space, &JsonDocumentId::new(id).expect("valid id"));
        PersistenceReadRow::for_test(encoded, value.map(<[u8]>::to_vec), tombstone)
    }

    #[test]
    fn interpret_row_decodes_a_present_json_document() {
        let space = space();
        let row = json_row(&space, "profile", Some(br#"{"a":1}"#), false);
        let entity = JsonBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a present JSON row");
        assert_eq!(entity.identity(), b"profile");
        assert_eq!(
            entity.summary(),
            &EntitySummary::Present(br#"{"a":1}"#.to_vec())
        );
        assert_eq!(entity.version(), CommitVersion::new(1));
        assert!(!entity.is_tombstone());
    }

    #[test]
    fn interpret_row_maps_a_json_tombstone_to_absent() {
        let space = space();
        let row = json_row(&space, "gone", None, true);
        let entity = JsonBranchAdapter
            .interpret_row(&space, &row)
            .expect("decodes a tombstone");
        assert!(entity.is_tombstone());
        assert_eq!(entity.summary(), &EntitySummary::Absent);
    }

    #[test]
    fn interpret_row_rejects_a_document_from_another_space() {
        let other = ProductSpace::new("other").expect("other is a valid space");
        let row = json_row(&other, "profile", Some(b"{}"), false);
        let error = JsonBranchAdapter
            .interpret_row(&space(), &row)
            .expect_err("a document encoded for another space is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.json_key");
    }

    #[test]
    fn interpret_row_rejects_a_present_document_without_a_value() {
        let space = space();
        let row = json_row(&space, "beta", None, false);
        let error = JsonBranchAdapter
            .interpret_row(&space, &row)
            .expect_err("a present document without a value is rejected");
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "data_loss.engine.json_document");
    }

    #[test]
    fn space_prefix_matches_the_json_encoding() {
        let space = space();
        assert_eq!(
            JsonBranchAdapter.space_prefix(&space),
            encode_json_space_prefix(&space)
        );
    }

    #[test]
    fn row_class_and_disposition_are_reported() {
        assert_eq!(JsonBranchAdapter.row_class(), RowClass::Json);
        assert_eq!(
            JsonBranchAdapter.derived_disposition(),
            DerivedDisposition::Authored
        );
    }
}
