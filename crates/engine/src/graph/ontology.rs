//! Ontology layer: typed schema for graphs.
//!
//! Provides CRUD for object types (node schemas) and link types (edge schemas),
//! a draft/frozen lifecycle, and schema introspection.

use std::collections::HashMap;

use strata_core::types::BranchId;
use strata_core::{StrataError, StrataResult, Value};

use super::keys;
use super::types::*;
use super::GraphStore;

impl GraphStore {
    // =========================================================================
    // Object Type CRUD
    // =========================================================================

    /// Define (or replace) an object type in the graph's ontology.
    ///
    /// If this is the first type defined on an untyped graph, implicitly sets
    /// `ontology_status = Draft` in GraphMeta.
    pub fn define_object_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        def: ObjectTypeDef,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_type_name(&def.name)?;

        self.ensure_not_frozen(branch_id, graph, "Cannot modify ontology: graph is frozen")?;
        self.ensure_draft_status(branch_id, graph)?;

        let type_json = serde_json::to_string(&def)
            .map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::object_type_key(graph, &def.name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            txn.put(storage_key.clone(), Value::String(type_json.clone()))
        })
    }

    /// Get an object type definition, or None if it doesn't exist.
    pub fn get_object_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        name: &str,
    ) -> StrataResult<Option<ObjectTypeDef>> {
        let user_key = keys::object_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            let val = txn.get(&storage_key)?;
            match val {
                Some(Value::String(s)) => {
                    let def: ObjectTypeDef = serde_json::from_str(&s)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(def))
                }
                Some(_) => Err(StrataError::serialization(
                    "Object type def is not a string".to_string(),
                )),
                None => Ok(None),
            }
        })
    }

    /// List all object type names in the graph's ontology.
    pub fn list_object_types(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::all_object_types_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut names = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(name) = keys::parse_object_type_key(graph, &user_key) {
                        names.push(name);
                    }
                }
            }
            Ok(names)
        })
    }

    /// Delete an object type definition during draft.
    pub fn delete_object_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        name: &str,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        self.ensure_not_frozen(branch_id, graph, "Cannot modify ontology: graph is frozen")?;

        let user_key = keys::object_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db
            .transaction(branch_id, |txn| txn.delete(storage_key.clone()))
    }

    // =========================================================================
    // Link Type CRUD
    // =========================================================================

    /// Define (or replace) a link type in the graph's ontology.
    pub fn define_link_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        def: LinkTypeDef,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_type_name(&def.name)?;

        self.ensure_not_frozen(branch_id, graph, "Cannot modify ontology: graph is frozen")?;
        self.ensure_draft_status(branch_id, graph)?;

        let type_json = serde_json::to_string(&def)
            .map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::link_type_key(graph, &def.name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            txn.put(storage_key.clone(), Value::String(type_json.clone()))
        })
    }

    /// Get a link type definition, or None if it doesn't exist.
    pub fn get_link_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        name: &str,
    ) -> StrataResult<Option<LinkTypeDef>> {
        let user_key = keys::link_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            let val = txn.get(&storage_key)?;
            match val {
                Some(Value::String(s)) => {
                    let def: LinkTypeDef = serde_json::from_str(&s)
                        .map_err(|e| StrataError::serialization(e.to_string()))?;
                    Ok(Some(def))
                }
                Some(_) => Err(StrataError::serialization(
                    "Link type def is not a string".to_string(),
                )),
                None => Ok(None),
            }
        })
    }

    /// List all link type names in the graph's ontology.
    pub fn list_link_types(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::all_link_types_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut names = Vec::new();
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some(name) = keys::parse_link_type_key(graph, &user_key) {
                        names.push(name);
                    }
                }
            }
            Ok(names)
        })
    }

    /// Delete a link type definition during draft.
    pub fn delete_link_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        name: &str,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        self.ensure_not_frozen(branch_id, graph, "Cannot modify ontology: graph is frozen")?;

        let user_key = keys::link_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db
            .transaction(branch_id, |txn| txn.delete(storage_key.clone()))
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Get the ontology status of a graph (None = untyped graph).
    pub fn ontology_status(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<Option<OntologyStatus>> {
        let meta = self.get_graph_meta(branch_id, graph)?;
        Ok(meta.and_then(|m| m.ontology_status))
    }

    /// Freeze the ontology: validates and transitions Draft → Frozen.
    ///
    /// Validates that:
    /// - At least one type is defined
    /// - All link type source/target reference existing object types
    ///
    /// Freezing an already-frozen graph is a no-op.
    pub fn freeze_ontology(&self, branch_id: BranchId, graph: &str) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        let meta = self
            .get_graph_meta(branch_id, graph)?
            .ok_or_else(|| StrataError::invalid_input("Graph does not exist"))?;

        // Already frozen → no-op
        if meta.ontology_status == Some(OntologyStatus::Frozen) {
            return Ok(());
        }

        // Must be draft to freeze
        if meta.ontology_status.is_none() {
            return Err(StrataError::invalid_input(
                "Cannot freeze ontology: no types defined (graph is untyped)",
            ));
        }

        // Collect object type names
        let object_types = self.list_object_types(branch_id, graph)?;
        let link_types = self.list_link_types(branch_id, graph)?;

        if object_types.is_empty() && link_types.is_empty() {
            return Err(StrataError::invalid_input(
                "Cannot freeze ontology: no types defined",
            ));
        }

        // Validate link type source/target references
        let obj_set: std::collections::HashSet<&str> =
            object_types.iter().map(|s| s.as_str()).collect();
        for lt_name in &link_types {
            let lt = self
                .get_link_type(branch_id, graph, lt_name)?
                .ok_or_else(|| {
                    StrataError::serialization(format!("Link type '{}' not found", lt_name))
                })?;
            if !obj_set.contains(lt.source.as_str()) {
                return Err(StrataError::invalid_input(format!(
                    "Cannot freeze: link type '{}' references undefined source type '{}'",
                    lt.name, lt.source
                )));
            }
            if !obj_set.contains(lt.target.as_str()) {
                return Err(StrataError::invalid_input(format!(
                    "Cannot freeze: link type '{}' references undefined target type '{}'",
                    lt.name, lt.target
                )));
            }
        }

        // Update meta to Frozen
        let new_meta = GraphMeta {
            ontology_status: Some(OntologyStatus::Frozen),
            ..meta
        };
        let meta_json = serde_json::to_string(&new_meta)
            .map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::meta_key(graph);
        let storage_key = keys::storage_key(branch_id, &user_key);

        self.db.transaction(branch_id, |txn| {
            txn.put(storage_key.clone(), Value::String(meta_json.clone()))
        })
    }

    // =========================================================================
    // Validation (Epic 4)
    // =========================================================================

    /// Validate a node against the frozen ontology.
    ///
    /// Only validates when ontology is frozen and node has an object_type
    /// that matches a defined type.
    pub(crate) fn validate_node(
        &self,
        branch_id: BranchId,
        graph: &str,
        _node_id: &str,
        data: &NodeData,
    ) -> StrataResult<()> {
        let object_type = match &data.object_type {
            Some(t) => t,
            None => return Ok(()), // untyped node: no validation
        };

        let type_def = match self.get_object_type(branch_id, graph, object_type)? {
            Some(def) => def,
            None => return Ok(()), // undeclared type: open-world, pass through
        };

        // Check required properties
        for (prop_name, prop_def) in &type_def.properties {
            if prop_def.required {
                let has_prop = data
                    .properties
                    .as_ref()
                    .and_then(|p| p.as_object())
                    .map(|obj| obj.contains_key(prop_name))
                    .unwrap_or(false);
                if !has_prop {
                    return Err(StrataError::invalid_input(format!(
                        "Node of type '{}' is missing required property '{}'",
                        object_type, prop_name
                    )));
                }
            }
        }

        Ok(())
    }

    /// Validate an edge against the frozen ontology.
    ///
    /// Only validates when ontology is frozen and edge_type matches a defined link type.
    pub(crate) fn validate_edge(
        &self,
        branch_id: BranchId,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
    ) -> StrataResult<()> {
        let link_def = match self.get_link_type(branch_id, graph, edge_type)? {
            Some(def) => def,
            None => return Ok(()), // undeclared edge type: no validation
        };

        // Check source node's object_type
        if let Some(src_data) = self.get_node(branch_id, graph, src)? {
            if let Some(src_type) = &src_data.object_type {
                if src_type != &link_def.source {
                    return Err(StrataError::invalid_input(format!(
                        "Edge type '{}' requires source type '{}', but node '{}' has type '{}'",
                        edge_type, link_def.source, src, src_type
                    )));
                }
            }
            // Untyped source: skip check
        }

        // Check target node's object_type
        if let Some(dst_data) = self.get_node(branch_id, graph, dst)? {
            if let Some(dst_type) = &dst_data.object_type {
                if dst_type != &link_def.target {
                    return Err(StrataError::invalid_input(format!(
                        "Edge type '{}' requires target type '{}', but node '{}' has type '{}'",
                        edge_type, link_def.target, dst, dst_type
                    )));
                }
            }
            // Untyped target: skip check
        }

        Ok(())
    }

    // =========================================================================
    // Introspection (Epic 5)
    // =========================================================================

    /// Get a complete ontology summary for AI orientation.
    ///
    /// Returns None for untyped graphs. Includes node/edge counts.
    pub fn ontology_summary(
        &self,
        branch_id: BranchId,
        graph: &str,
    ) -> StrataResult<Option<OntologySummary>> {
        let meta = match self.get_graph_meta(branch_id, graph)? {
            Some(m) => m,
            None => return Ok(None),
        };

        let status = match meta.ontology_status {
            Some(s) => s,
            None => return Ok(None), // untyped graph
        };

        // Collect object type summaries with node counts
        let mut object_types = HashMap::new();
        let obj_names = self.list_object_types(branch_id, graph)?;
        for name in &obj_names {
            let def = self
                .get_object_type(branch_id, graph, name)?
                .ok_or_else(|| {
                    StrataError::serialization(format!(
                        "Object type '{}' listed but definition not found",
                        name
                    ))
                })?;
            let node_ids = self.nodes_by_type(branch_id, graph, name)?;
            object_types.insert(
                name.clone(),
                ObjectTypeSummary {
                    properties: def.properties,
                    node_count: node_ids.len() as u64,
                },
            );
        }

        // Collect link type summaries with edge counts
        let mut link_types = HashMap::new();
        let link_names = self.list_link_types(branch_id, graph)?;
        for name in &link_names {
            let def = self
                .get_link_type(branch_id, graph, name)?
                .ok_or_else(|| {
                    StrataError::serialization(format!(
                        "Link type '{}' listed but definition not found",
                        name
                    ))
                })?;

            // Count edges by scanning forward edges filtered by edge_type
            let edge_count = self.count_edges_by_type(branch_id, graph, name)?;

            link_types.insert(
                name.clone(),
                LinkTypeSummary {
                    source: def.source,
                    target: def.target,
                    cardinality: def.cardinality,
                    properties: def.properties,
                    edge_count,
                },
            );
        }

        Ok(Some(OntologySummary {
            status,
            object_types,
            link_types,
        }))
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Ensure the ontology is not frozen. Returns an error with the given message if frozen.
    fn ensure_not_frozen(
        &self,
        branch_id: BranchId,
        graph: &str,
        msg: &str,
    ) -> StrataResult<()> {
        if let Some(meta) = self.get_graph_meta(branch_id, graph)? {
            if meta.ontology_status == Some(OntologyStatus::Frozen) {
                return Err(StrataError::invalid_input(msg));
            }
        }
        Ok(())
    }

    /// Ensure the graph's ontology status is set to Draft.
    /// If the graph is untyped (no ontology_status), set it to Draft.
    fn ensure_draft_status(&self, branch_id: BranchId, graph: &str) -> StrataResult<()> {
        let meta = self.get_graph_meta(branch_id, graph)?;

        match &meta {
            Some(m) if m.ontology_status.is_some() => {
                // Already has a status (Draft or Frozen — Frozen was rejected above)
                Ok(())
            }
            Some(m) => {
                // Untyped graph: set to Draft
                let new_meta = GraphMeta {
                    ontology_status: Some(OntologyStatus::Draft),
                    ..m.clone()
                };
                let meta_json = serde_json::to_string(&new_meta)
                    .map_err(|e| StrataError::serialization(e.to_string()))?;
                let user_key = keys::meta_key(graph);
                let storage_key = keys::storage_key(branch_id, &user_key);

                self.db.transaction(branch_id, |txn| {
                    txn.put(storage_key.clone(), Value::String(meta_json.clone()))
                })
            }
            None => {
                // Graph doesn't exist yet — create it with Draft status
                let new_meta = GraphMeta {
                    ontology_status: Some(OntologyStatus::Draft),
                    ..Default::default()
                };
                self.create_graph(branch_id, graph, Some(new_meta))
            }
        }
    }

    /// Count edges of a given type by scanning forward edges.
    fn count_edges_by_type(
        &self,
        branch_id: BranchId,
        graph: &str,
        edge_type: &str,
    ) -> StrataResult<u64> {
        let prefix = keys::all_edges_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut count = 0u64;
            for (key, _) in results {
                if let Some(user_key) = key.user_key_string() {
                    if let Some((_, et, _)) = keys::parse_forward_edge_key(graph, &user_key) {
                        if et == edge_type {
                            count += 1;
                        }
                    }
                }
            }
            Ok(count)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use std::sync::Arc;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::cache().unwrap();
        let graph = GraphStore::new(db.clone());
        (db, graph)
    }

    fn default_branch() -> BranchId {
        BranchId::from_bytes([0u8; 16])
    }

    fn patient_type() -> ObjectTypeDef {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            PropertyDef {
                r#type: Some("string".to_string()),
                required: true,
            },
        );
        props.insert(
            "age".to_string(),
            PropertyDef {
                r#type: Some("integer".to_string()),
                required: false,
            },
        );
        ObjectTypeDef {
            name: "Patient".to_string(),
            properties: props,
        }
    }

    fn lab_result_type() -> ObjectTypeDef {
        let mut props = HashMap::new();
        props.insert(
            "value".to_string(),
            PropertyDef {
                r#type: Some("number".to_string()),
                required: true,
            },
        );
        ObjectTypeDef {
            name: "LabResult".to_string(),
            properties: props,
        }
    }

    fn has_result_link() -> LinkTypeDef {
        LinkTypeDef {
            name: "HAS_RESULT".to_string(),
            source: "Patient".to_string(),
            target: "LabResult".to_string(),
            cardinality: Some("one-to-many".to_string()),
            properties: HashMap::new(),
        }
    }

    // =========================================================================
    // Object type CRUD
    // =========================================================================

    #[test]
    fn define_object_type_then_get() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        let def = patient_type();
        gs.define_object_type(b, "g", def.clone()).unwrap();

        let got = gs.get_object_type(b, "g", "Patient").unwrap().unwrap();
        assert_eq!(got, def);
    }

    #[test]
    fn list_object_types_returns_names() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();

        let mut names = gs.list_object_types(b, "g").unwrap();
        names.sort();
        assert_eq!(names, vec!["LabResult", "Patient"]);
    }

    #[test]
    fn delete_object_type_during_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.delete_object_type(b, "g", "Patient").unwrap();

        assert!(gs.get_object_type(b, "g", "Patient").unwrap().is_none());
    }

    #[test]
    fn get_nonexistent_object_type_returns_none() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        assert!(gs.get_object_type(b, "g", "DoesNotExist").unwrap().is_none());
    }

    // =========================================================================
    // Link type CRUD
    // =========================================================================

    #[test]
    fn define_link_type_then_get() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();

        let def = has_result_link();
        gs.define_link_type(b, "g", def.clone()).unwrap();

        let got = gs.get_link_type(b, "g", "HAS_RESULT").unwrap().unwrap();
        assert_eq!(got, def);
    }

    #[test]
    fn list_link_types_returns_names() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();

        let names = gs.list_link_types(b, "g").unwrap();
        assert_eq!(names, vec!["HAS_RESULT"]);
    }

    #[test]
    fn delete_link_type_during_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();

        gs.delete_link_type(b, "g", "HAS_RESULT").unwrap();
        assert!(gs.get_link_type(b, "g", "HAS_RESULT").unwrap().is_none());
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    #[test]
    fn first_define_sets_status_to_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // Initially untyped
        assert!(gs.ontology_status(b, "g").unwrap().is_none());

        gs.define_object_type(b, "g", patient_type()).unwrap();

        assert_eq!(
            gs.ontology_status(b, "g").unwrap(),
            Some(OntologyStatus::Draft)
        );
    }

    #[test]
    fn freeze_transitions_draft_to_frozen() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        assert_eq!(
            gs.ontology_status(b, "g").unwrap(),
            Some(OntologyStatus::Frozen)
        );
    }

    #[test]
    fn freeze_already_frozen_is_noop() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();
        gs.freeze_ontology(b, "g").unwrap(); // no error
    }

    #[test]
    fn define_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        let result = gs.define_object_type(b, "g", lab_result_type());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("frozen"), "Error should mention frozen: {}", err);
    }

    #[test]
    fn delete_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        let result = gs.delete_object_type(b, "g", "Patient");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("frozen"), "Error should mention frozen: {}", err);
    }

    #[test]
    fn freeze_with_no_types_errors() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        let result = gs.freeze_ontology(b, "g");
        assert!(result.is_err());
    }

    #[test]
    fn freeze_with_dangling_link_type_errors() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        // Link references "LabResult" which doesn't exist
        gs.define_link_type(b, "g", has_result_link()).unwrap();

        let result = gs.freeze_ontology(b, "g");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("LabResult"),
            "Error should mention missing type: {}",
            err
        );
    }

    #[test]
    fn untyped_graph_operations_unaffected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // All basic operations still work on untyped graph
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "n1", "n1", "SELF", EdgeData::default())
            .unwrap();
        gs.remove_edge(b, "g", "n1", "n1", "SELF").unwrap();
        gs.remove_node(b, "g", "n1").unwrap();
    }

    // =========================================================================
    // Validation (Epic 4)
    // =========================================================================

    #[test]
    fn frozen_node_with_required_props_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // "name" is required for Patient
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({"name": "Alice"})),
            ..Default::default()
        };
        gs.add_node(b, "g", "p1", data).unwrap();
    }

    #[test]
    fn frozen_node_missing_required_prop_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Missing required "name" property
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({"age": 42})),
            ..Default::default()
        };
        let result = gs.add_node(b, "g", "p1", data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("name"), "Error should mention missing prop: {}", err);
    }

    #[test]
    fn draft_node_missing_required_prop_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        // NOT frozen — draft mode

        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: None, // Missing required "name"
            ..Default::default()
        };
        gs.add_node(b, "g", "p1", data).unwrap(); // succeeds in draft
    }

    #[test]
    fn frozen_untyped_node_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // No object_type — no validation
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();
    }

    #[test]
    fn frozen_edge_correct_types_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        gs.add_node(
            b,
            "g",
            "p1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"name": "Alice"})),
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        )
        .unwrap();

        gs.add_edge(b, "g", "p1", "l1", "HAS_RESULT", EdgeData::default())
            .unwrap();
    }

    #[test]
    fn frozen_edge_wrong_source_type_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Both are LabResult, but HAS_RESULT requires source=Patient
        gs.add_node(
            b,
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "l2",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 99})),
                ..Default::default()
            },
        )
        .unwrap();

        let result = gs.add_edge(b, "g", "l1", "l2", "HAS_RESULT", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("source"),
            "Error should mention source type: {}",
            err
        );
    }

    #[test]
    fn frozen_edge_undeclared_type_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        gs.add_node(
            b,
            "g",
            "p1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"name": "Alice"})),
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();

        // FRIEND is not a declared link type → no validation
        gs.add_edge(b, "g", "p1", "n1", "FRIEND", EdgeData::default())
            .unwrap();
    }

    #[test]
    fn frozen_edge_untyped_endpoints_allowed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Both endpoints are untyped → HAS_RESULT validation skips type checks
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();
        gs.add_node(b, "g", "n2", NodeData::default()).unwrap();

        gs.add_edge(b, "g", "n1", "n2", "HAS_RESULT", EdgeData::default())
            .unwrap();
    }

    #[test]
    fn existing_untyped_graph_all_writes_succeed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // Add nodes and edges on untyped graph — no validation
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                object_type: Some("SomeType".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "g", "n2", NodeData::default()).unwrap();
        gs.add_edge(b, "g", "n1", "n2", "ANYTHING", EdgeData::default())
            .unwrap();
    }

    // =========================================================================
    // Introspection (Epic 5)
    // =========================================================================

    #[test]
    fn ontology_summary_includes_all_types() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        let summary = gs.ontology_summary(b, "g").unwrap().unwrap();
        assert_eq!(summary.status, OntologyStatus::Frozen);
        assert!(summary.object_types.contains_key("Patient"));
        assert!(summary.object_types.contains_key("LabResult"));
        assert!(summary.link_types.contains_key("HAS_RESULT"));
    }

    #[test]
    fn ontology_summary_node_counts_accurate() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();

        // Add some typed nodes
        for i in 0..3 {
            gs.add_node(
                b,
                "g",
                &format!("p{}", i),
                NodeData {
                    object_type: Some("Patient".to_string()),
                    properties: Some(serde_json::json!({"name": format!("P{}", i)})),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        gs.add_node(
            b,
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        )
        .unwrap();

        // Add edges
        gs.add_edge(b, "g", "p0", "l1", "HAS_RESULT", EdgeData::default())
            .unwrap();

        gs.freeze_ontology(b, "g").unwrap();

        let summary = gs.ontology_summary(b, "g").unwrap().unwrap();
        assert_eq!(summary.object_types["Patient"].node_count, 3);
        assert_eq!(summary.object_types["LabResult"].node_count, 1);
        assert_eq!(summary.link_types["HAS_RESULT"].edge_count, 1);
    }

    #[test]
    fn ontology_summary_untyped_graph_returns_none() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        assert!(gs.ontology_summary(b, "g").unwrap().is_none());
    }

    #[test]
    fn ontology_summary_draft_status_reflected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();

        let summary = gs.ontology_summary(b, "g").unwrap().unwrap();
        assert_eq!(summary.status, OntologyStatus::Draft);
    }

    // =========================================================================
    // Type index (nodes_by_type — Epic 3 tests)
    // =========================================================================

    #[test]
    fn nodes_by_type_basic() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.add_node(
            b,
            "g",
            "p1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(
            b,
            "g",
            "p2",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();

        let mut patients = gs.nodes_by_type(b, "g", "Patient").unwrap();
        patients.sort();
        assert_eq!(patients, vec!["p1", "p2"]);
    }

    #[test]
    fn nodes_by_type_no_results() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();

        let results = gs.nodes_by_type(b, "g", "Patient").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn nodes_by_type_nonexistent_type_returns_empty() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        let results = gs.nodes_by_type(b, "g", "DoesNotExist").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn type_index_updated_on_type_change() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                object_type: Some("TypeA".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(gs.nodes_by_type(b, "g", "TypeA").unwrap().len(), 1);

        // Change type
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                object_type: Some("TypeB".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(gs.nodes_by_type(b, "g", "TypeA").unwrap().is_empty());
        assert_eq!(gs.nodes_by_type(b, "g", "TypeB").unwrap().len(), 1);
    }

    #[test]
    fn type_index_cleared_on_remove() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.remove_node(b, "g", "n1").unwrap();

        assert!(gs.nodes_by_type(b, "g", "Patient").unwrap().is_empty());
    }

    #[test]
    fn bulk_insert_maintains_type_index() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        let nodes = vec![
            (
                "p1".to_string(),
                NodeData {
                    object_type: Some("Patient".to_string()),
                    properties: None,
                    ..Default::default()
                },
            ),
            (
                "p2".to_string(),
                NodeData {
                    object_type: Some("Patient".to_string()),
                    properties: None,
                    ..Default::default()
                },
            ),
            ("n1".to_string(), NodeData::default()),
            (
                "l1".to_string(),
                NodeData {
                    object_type: Some("LabResult".to_string()),
                    properties: None,
                    ..Default::default()
                },
            ),
        ];

        gs.bulk_insert(b, "g", &nodes, &[], None).unwrap();

        let mut patients = gs.nodes_by_type(b, "g", "Patient").unwrap();
        patients.sort();
        assert_eq!(patients, vec!["p1", "p2"]);
        assert_eq!(gs.nodes_by_type(b, "g", "LabResult").unwrap(), vec!["l1"]);
    }

    #[test]
    fn type_index_cleared_when_type_removed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // Add node with type
        gs.add_node(
            b,
            "g",
            "n1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(gs.nodes_by_type(b, "g", "Patient").unwrap().len(), 1);

        // Update node to remove type
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();
        assert!(gs.nodes_by_type(b, "g", "Patient").unwrap().is_empty());
    }

    #[test]
    fn redefine_type_with_different_properties() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();

        // Redefine with different properties
        let mut new_props = HashMap::new();
        new_props.insert(
            "email".to_string(),
            PropertyDef {
                r#type: Some("string".to_string()),
                required: true,
            },
        );
        let new_def = ObjectTypeDef {
            name: "Patient".to_string(),
            properties: new_props,
        };
        gs.define_object_type(b, "g", new_def.clone()).unwrap();

        let got = gs.get_object_type(b, "g", "Patient").unwrap().unwrap();
        assert_eq!(got.properties.len(), 1);
        assert!(got.properties.contains_key("email"));
        assert!(!got.properties.contains_key("name")); // old prop gone
    }

    #[test]
    fn delete_referenced_object_type_then_freeze_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();

        // Delete a type referenced by the link type
        gs.delete_object_type(b, "g", "LabResult").unwrap();

        // Freeze should fail: HAS_RESULT references non-existent LabResult
        let result = gs.freeze_ontology(b, "g");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("LabResult"),
            "Error should mention missing target type: {}",
            err
        );
    }

    #[test]
    fn frozen_undeclared_object_type_passes_validation() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Node with undeclared type should pass (open-world)
        let data = NodeData {
            object_type: Some("UnknownType".to_string()),
            properties: None,
            ..Default::default()
        };
        gs.add_node(b, "g", "n1", data).unwrap();
    }

    #[test]
    fn frozen_edge_nonexistent_endpoints_passes() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Create nodes but don't create the endpoints that the edge references
        gs.add_node(b, "g", "n1", NodeData::default()).unwrap();
        gs.add_node(b, "g", "n2", NodeData::default()).unwrap();

        // Edge from non-existent nodes should still succeed (get_node returns None,
        // so validation skips type checks)
        gs.add_edge(
            b,
            "g",
            "phantom_src",
            "phantom_dst",
            "HAS_RESULT",
            EdgeData::default(),
        )
        .unwrap();
    }

    #[test]
    fn bulk_insert_upsert_cleans_old_type_index() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // First bulk insert: node with TypeA
        let nodes1 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeA".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "g", &nodes1, &[], None).unwrap();
        assert_eq!(gs.nodes_by_type(b, "g", "TypeA").unwrap(), vec!["n1"]);

        // Second bulk insert: same node with TypeB
        let nodes2 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeB".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "g", &nodes2, &[], None).unwrap();

        // TypeA index must be cleaned up, TypeB should have the node
        assert!(
            gs.nodes_by_type(b, "g", "TypeA").unwrap().is_empty(),
            "Old type index entry should be cleaned up on bulk re-insert"
        );
        assert_eq!(gs.nodes_by_type(b, "g", "TypeB").unwrap(), vec!["n1"]);
    }

    #[test]
    fn bulk_insert_upsert_removes_type_index_when_type_cleared() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        // First: node with type
        let nodes1 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeA".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "g", &nodes1, &[], None).unwrap();
        assert_eq!(gs.nodes_by_type(b, "g", "TypeA").unwrap().len(), 1);

        // Second: same node without type
        let nodes2 = vec![("n1".to_string(), NodeData::default())];
        gs.bulk_insert(b, "g", &nodes2, &[], None).unwrap();

        assert!(
            gs.nodes_by_type(b, "g", "TypeA").unwrap().is_empty(),
            "Type index should be cleaned up when type removed via bulk upsert"
        );
    }

    #[test]
    fn frozen_node_no_properties_but_required_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Node with None properties when type requires "name"
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: None,
            ..Default::default()
        };
        let result = gs.add_node(b, "g", "p1", data);
        assert!(result.is_err());
    }

    #[test]
    fn frozen_node_empty_properties_but_required_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Node with empty properties object when type requires "name"
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({})),
            ..Default::default()
        };
        let result = gs.add_node(b, "g", "p1", data);
        assert!(result.is_err());
    }

    #[test]
    fn define_type_on_nonexistent_graph_creates_it() {
        let (_db, gs) = setup();
        let b = default_branch();

        // Don't call create_graph — define_object_type should create it
        gs.define_object_type(b, "g", patient_type()).unwrap();

        let got = gs.get_object_type(b, "g", "Patient").unwrap();
        assert!(got.is_some());
        assert_eq!(
            gs.ontology_status(b, "g").unwrap(),
            Some(OntologyStatus::Draft)
        );
    }

    #[test]
    fn delete_link_type_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.define_object_type(b, "g", lab_result_type()).unwrap();
        gs.define_link_type(b, "g", has_result_link()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        let result = gs.delete_link_type(b, "g", "HAS_RESULT");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("frozen"), "Error should mention frozen: {}", err);
    }

    #[test]
    fn define_link_type_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        let result = gs.define_link_type(b, "g", has_result_link());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("frozen"), "Error should mention frozen: {}", err);
    }

    #[test]
    fn bulk_insert_validates_on_frozen() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "g", None).unwrap();

        gs.define_object_type(b, "g", patient_type()).unwrap();
        gs.freeze_ontology(b, "g").unwrap();

        // Bulk insert with missing required prop should fail
        let nodes = vec![(
            "p1".to_string(),
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"age": 42})), // missing "name"
                ..Default::default()
            },
        )];

        let result = gs.bulk_insert(b, "g", &nodes, &[], None);
        assert!(result.is_err());
    }
}
