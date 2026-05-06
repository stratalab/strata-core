//! Ontology layer: typed schema for graphs.
//!
//! Provides CRUD for object types (node schemas) and link types (edge schemas),
//! a draft/frozen lifecycle, and schema introspection.

use std::collections::HashMap;

use crate::{StrataError, StrataResult};
use strata_core::BranchId;
use strata_core::Value;

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
        space: &str,
        graph: &str,
        def: ObjectTypeDef,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_type_name(&def.name)?;

        self.ensure_not_frozen(
            branch_id,
            space,
            graph,
            "Cannot modify ontology: graph is frozen",
        )?;
        self.ensure_draft_status(branch_id, space, graph)?;

        let type_json =
            serde_json::to_string(&def).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::object_type_key(graph, &def.name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        self.db.transaction(branch_id, |txn| {
            Ok(txn.put(storage_key.clone(), Value::String(type_json.clone()))?)
        })
    }

    /// Get an object type definition, or None if it doesn't exist.
    pub fn get_object_type(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        name: &str,
    ) -> StrataResult<Option<ObjectTypeDef>> {
        let user_key = keys::object_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

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
        space: &str,
        graph: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::all_object_types_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, space, &prefix);

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
        space: &str,
        graph: &str,
        name: &str,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        self.ensure_not_frozen(
            branch_id,
            space,
            graph,
            "Cannot modify ontology: graph is frozen",
        )?;

        let user_key = keys::object_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        self.db
            .transaction(branch_id, |txn| Ok(txn.delete(storage_key.clone())?))
    }

    // =========================================================================
    // Link Type CRUD
    // =========================================================================

    /// Define (or replace) a link type in the graph's ontology.
    pub fn define_link_type(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        def: LinkTypeDef,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;
        keys::validate_type_name(&def.name)?;

        self.ensure_not_frozen(
            branch_id,
            space,
            graph,
            "Cannot modify ontology: graph is frozen",
        )?;
        self.ensure_draft_status(branch_id, space, graph)?;

        let type_json =
            serde_json::to_string(&def).map_err(|e| StrataError::serialization(e.to_string()))?;
        let user_key = keys::link_type_key(graph, &def.name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        self.db.transaction(branch_id, |txn| {
            Ok(txn.put(storage_key.clone(), Value::String(type_json.clone()))?)
        })
    }

    /// Get a link type definition, or None if it doesn't exist.
    pub fn get_link_type(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        name: &str,
    ) -> StrataResult<Option<LinkTypeDef>> {
        let user_key = keys::link_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

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
        space: &str,
        graph: &str,
    ) -> StrataResult<Vec<String>> {
        let prefix = keys::all_link_types_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, space, &prefix);

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
        space: &str,
        graph: &str,
        name: &str,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        self.ensure_not_frozen(
            branch_id,
            space,
            graph,
            "Cannot modify ontology: graph is frozen",
        )?;

        let user_key = keys::link_type_key(graph, name);
        let storage_key = keys::storage_key(branch_id, space, &user_key);

        self.db
            .transaction(branch_id, |txn| Ok(txn.delete(storage_key.clone())?))
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Get the ontology status of a graph (None = untyped graph).
    pub fn ontology_status(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<Option<OntologyStatus>> {
        let meta = self.get_graph_meta(branch_id, space, graph)?;
        Ok(meta.and_then(|m| m.ontology_status))
    }

    /// Freeze the ontology: validates and transitions Draft → Frozen.
    ///
    /// Validates that:
    /// - At least one type is defined
    /// - All link type source/target reference existing object types
    ///
    /// Freezing an already-frozen graph is a no-op.
    pub fn freeze_ontology(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<()> {
        keys::validate_graph_name(graph)?;

        // Pre-validate outside the transaction (cheap, fast-fail for obvious errors).
        let meta = self
            .get_graph_meta(branch_id, space, graph)?
            .ok_or_else(|| StrataError::invalid_input("Graph does not exist"))?;

        if meta.ontology_status == Some(OntologyStatus::Frozen) {
            return Ok(());
        }
        if meta.ontology_status.is_none() {
            return Err(StrataError::invalid_input(
                "Cannot freeze ontology: no types defined (graph is untyped)",
            ));
        }

        // Collect and validate type definitions (reads are safe outside txn — they
        // are self-consistent and only checked against each other).
        let object_types = self.list_object_types(branch_id, space, graph)?;
        let link_types = self.list_link_types(branch_id, space, graph)?;

        if object_types.is_empty() && link_types.is_empty() {
            return Err(StrataError::invalid_input(
                "Cannot freeze ontology: no types defined",
            ));
        }

        let obj_set: std::collections::HashSet<&str> =
            object_types.iter().map(|s| s.as_str()).collect();
        for lt_name in &link_types {
            let lt = self
                .get_link_type(branch_id, space, graph, lt_name)?
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

        // Atomic read-modify-write: read meta inside the transaction so OCC will
        // abort if a concurrent define_object_type() modified meta between our
        // pre-check and this write.
        let meta_user_key = keys::meta_key(graph);
        let meta_storage_key = keys::storage_key(branch_id, space, &meta_user_key);

        self.db.transaction(branch_id, |txn| {
            // Re-read meta inside txn — OCC tracks this read.
            let current_val = txn.get(&meta_storage_key)?;
            let current_meta: GraphMeta = match current_val {
                Some(Value::String(s)) => serde_json::from_str(&s)
                    .map_err(|e| StrataError::serialization(e.to_string()))?,
                _ => {
                    return Err(StrataError::invalid_input("Graph does not exist"));
                }
            };

            // Re-check status inside txn (may have changed concurrently)
            if current_meta.ontology_status == Some(OntologyStatus::Frozen) {
                return Ok(());
            }

            let new_meta = GraphMeta {
                ontology_status: Some(OntologyStatus::Frozen),
                ..current_meta
            };
            let meta_json = serde_json::to_string(&new_meta)
                .map_err(|e| StrataError::serialization(e.to_string()))?;
            Ok(txn.put(meta_storage_key.clone(), Value::String(meta_json))?)
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
        space: &str,
        graph: &str,
        _node_id: &str,
        data: &NodeData,
    ) -> StrataResult<()> {
        let object_type = match &data.object_type {
            Some(t) => t,
            None => return Ok(()), // untyped node: no validation
        };

        let type_def = match self.get_object_type(branch_id, space, graph, object_type)? {
            Some(def) => def,
            None => {
                return Err(StrataError::invalid_input(format!(
                    "Object type '{}' is not declared in frozen ontology for graph '{}'",
                    object_type, graph
                )))
            }
        };

        // Check required properties and type enforcement
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

            // Type enforcement (G-16)
            Self::check_property_type(object_type, prop_name, prop_def, data.properties.as_ref())?;
        }

        Ok(())
    }

    /// Validate an edge against the frozen ontology.
    ///
    /// Only validates when ontology is frozen and edge_type matches a defined link type.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn validate_edge(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        edge_data: &EdgeData,
    ) -> StrataResult<()> {
        let link_def = match self.get_link_type(branch_id, space, graph, edge_type)? {
            Some(def) => def,
            None => {
                return Err(StrataError::invalid_input(format!(
                    "Edge type '{}' is not declared in frozen ontology for graph '{}'",
                    edge_type, graph
                )))
            }
        };

        // Check source node's object_type
        if let Some(src_data) = self.get_node(branch_id, space, graph, src)? {
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
        if let Some(dst_data) = self.get_node(branch_id, space, graph, dst)? {
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

        // Check edge property types (G-16)
        for (prop_name, prop_def) in &link_def.properties {
            if prop_def.required {
                let has_prop = edge_data
                    .properties
                    .as_ref()
                    .and_then(|p| p.as_object())
                    .map(|obj| obj.contains_key(prop_name))
                    .unwrap_or(false);
                if !has_prop {
                    return Err(StrataError::invalid_input(format!(
                        "Edge of type '{}' is missing required property '{}'",
                        edge_type, prop_name
                    )));
                }
            }
            Self::check_property_type(
                edge_type,
                prop_name,
                prop_def,
                edge_data.properties.as_ref(),
            )?;
        }

        Ok(())
    }

    // =========================================================================
    // Cached validation (for bulk operations)
    // =========================================================================

    /// Validate a node using pre-loaded type definitions (no KV reads).
    pub(crate) fn validate_node_cached(
        &self,
        graph: &str,
        _node_id: &str,
        data: &NodeData,
        object_type_cache: &HashMap<String, ObjectTypeDef>,
    ) -> StrataResult<()> {
        let object_type = match &data.object_type {
            Some(t) => t,
            None => return Ok(()),
        };

        let type_def = object_type_cache.get(object_type).ok_or_else(|| {
            StrataError::invalid_input(format!(
                "Object type '{}' is not declared in frozen ontology for graph '{}'",
                object_type, graph
            ))
        })?;

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

            // Type enforcement (G-16)
            Self::check_property_type(object_type, prop_name, prop_def, data.properties.as_ref())?;
        }

        Ok(())
    }

    /// Validate an edge using pre-loaded type definitions (no KV reads for types).
    ///
    /// Still reads nodes to check source/target object_type, but link type defs
    /// come from the cache.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn validate_edge_cached(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        src: &str,
        dst: &str,
        edge_type: &str,
        edge_data: &EdgeData,
        link_type_cache: &HashMap<String, LinkTypeDef>,
    ) -> StrataResult<()> {
        let link_def = link_type_cache.get(edge_type).ok_or_else(|| {
            StrataError::invalid_input(format!(
                "Edge type '{}' is not declared in frozen ontology for graph '{}'",
                edge_type, graph
            ))
        })?;

        if let Some(src_data) = self.get_node(branch_id, space, graph, src)? {
            if let Some(src_type) = &src_data.object_type {
                if src_type != &link_def.source {
                    return Err(StrataError::invalid_input(format!(
                        "Edge type '{}' requires source type '{}', but node '{}' has type '{}'",
                        edge_type, link_def.source, src, src_type
                    )));
                }
            }
        }

        if let Some(dst_data) = self.get_node(branch_id, space, graph, dst)? {
            if let Some(dst_type) = &dst_data.object_type {
                if dst_type != &link_def.target {
                    return Err(StrataError::invalid_input(format!(
                        "Edge type '{}' requires target type '{}', but node '{}' has type '{}'",
                        edge_type, link_def.target, dst, dst_type
                    )));
                }
            }
        }

        // Check edge property types (G-16)
        for (prop_name, prop_def) in &link_def.properties {
            if prop_def.required {
                let has_prop = edge_data
                    .properties
                    .as_ref()
                    .and_then(|p| p.as_object())
                    .map(|obj| obj.contains_key(prop_name))
                    .unwrap_or(false);
                if !has_prop {
                    return Err(StrataError::invalid_input(format!(
                        "Edge of type '{}' is missing required property '{}'",
                        edge_type, prop_name
                    )));
                }
            }
            Self::check_property_type(
                edge_type,
                prop_name,
                prop_def,
                edge_data.properties.as_ref(),
            )?;
        }

        Ok(())
    }

    /// Load all object type definitions into a cache for bulk validation.
    pub(crate) fn load_object_type_cache(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<HashMap<String, ObjectTypeDef>> {
        let names = self.list_object_types(branch_id, space, graph)?;
        let mut cache = HashMap::with_capacity(names.len());
        for name in names {
            if let Some(def) = self.get_object_type(branch_id, space, graph, &name)? {
                cache.insert(name, def);
            }
        }
        Ok(cache)
    }

    /// Load all link type definitions into a cache for bulk validation.
    pub(crate) fn load_link_type_cache(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<HashMap<String, LinkTypeDef>> {
        let names = self.list_link_types(branch_id, space, graph)?;
        let mut cache = HashMap::with_capacity(names.len());
        for name in names {
            if let Some(def) = self.get_link_type(branch_id, space, graph, &name)? {
                cache.insert(name, def);
            }
        }
        Ok(cache)
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
        space: &str,
        graph: &str,
    ) -> StrataResult<Option<OntologySummary>> {
        let meta = match self.get_graph_meta(branch_id, space, graph)? {
            Some(m) => m,
            None => return Ok(None),
        };

        let status = match meta.ontology_status {
            Some(s) => s,
            None => return Ok(None), // untyped graph
        };

        // Collect object type summaries with node counts
        let mut object_types = HashMap::new();
        let obj_names = self.list_object_types(branch_id, space, graph)?;
        for name in &obj_names {
            let def = self
                .get_object_type(branch_id, space, graph, name)?
                .ok_or_else(|| {
                    StrataError::serialization(format!(
                        "Object type '{}' listed but definition not found",
                        name
                    ))
                })?;
            let node_ids = self.nodes_by_type(branch_id, space, graph, name)?;
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
        let link_names = self.list_link_types(branch_id, space, graph)?;
        for name in &link_names {
            let def = self
                .get_link_type(branch_id, space, graph, name)?
                .ok_or_else(|| {
                    StrataError::serialization(format!(
                        "Link type '{}' listed but definition not found",
                        name
                    ))
                })?;

            // Count edges by scanning forward edges filtered by edge_type
            let edge_count = self.count_edges_by_type(branch_id, space, graph, name)?;

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
        space: &str,
        graph: &str,
        msg: &str,
    ) -> StrataResult<()> {
        if let Some(meta) = self.get_graph_meta(branch_id, space, graph)? {
            if meta.ontology_status == Some(OntologyStatus::Frozen) {
                return Err(StrataError::invalid_input(msg));
            }
        }
        Ok(())
    }

    /// Ensure the graph's ontology status is set to Draft.
    /// If the graph is untyped (no ontology_status), set it to Draft.
    fn ensure_draft_status(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
    ) -> StrataResult<()> {
        let meta = self.get_graph_meta(branch_id, space, graph)?;

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
                let storage_key = keys::storage_key(branch_id, space, &user_key);

                self.db.transaction(branch_id, |txn| {
                    Ok(txn.put(storage_key.clone(), Value::String(meta_json.clone()))?)
                })
            }
            None => {
                // Graph doesn't exist yet — create it with Draft status
                let new_meta = GraphMeta {
                    ontology_status: Some(OntologyStatus::Draft),
                    ..Default::default()
                };
                self.create_graph(branch_id, space, graph, Some(new_meta))
            }
        }
    }

    /// Check that a property value matches the declared type hint (G-16).
    ///
    /// Only enforces when the property exists AND has a type hint.
    /// Missing optional properties and unknown type hints are silently accepted.
    fn check_property_type(
        type_name: &str,
        prop_name: &str,
        prop_def: &PropertyDef,
        properties: Option<&serde_json::Value>,
    ) -> StrataResult<()> {
        let expected_type = match &prop_def.r#type {
            Some(t) => t.as_str(),
            None => return Ok(()),
        };

        // Get the actual value; if the property is absent, skip (handled by required check)
        let value = match properties
            .and_then(|p| p.as_object())
            .and_then(|obj| obj.get(prop_name))
        {
            Some(v) => v,
            None => return Ok(()),
        };

        let ok = match expected_type {
            "string" => value.is_string(),
            "integer" | "int" => value.is_i64() || value.is_u64(),
            "number" | "float" => value.is_number(),
            "boolean" | "bool" => value.is_boolean(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            _ => true, // unknown type hint — skip (backward compat)
        };

        if !ok {
            return Err(StrataError::invalid_input(format!(
                "Property '{}' on type '{}' must be of type '{}', got {}",
                prop_name,
                type_name,
                expected_type,
                json_type_name(value)
            )));
        }

        Ok(())
    }

    /// Count edges of a given type using the per-type counter (O(1) read).
    ///
    /// Falls back to full scan if counter is missing (legacy data without counters).
    pub(crate) fn count_edges_by_type(
        &self,
        branch_id: BranchId,
        space: &str,
        graph: &str,
        edge_type: &str,
    ) -> StrataResult<u64> {
        let count_uk = keys::edge_type_count_key(graph, edge_type);
        let count_sk = keys::storage_key(branch_id, space, &count_uk);

        // Try counter first (fast path)
        let counter_val = self
            .db
            .transaction(branch_id, |txn| Ok(txn.get(&count_sk)?))?;

        if let Some(Value::String(s)) = counter_val {
            return s
                .parse::<u64>()
                .map_err(|e| StrataError::serialization(e.to_string()));
        }

        // Fallback: scan forward adjacency lists (legacy data)
        use super::packed;
        let prefix = keys::all_forward_adj_prefix(graph);
        let prefix_key = keys::storage_key(branch_id, space, &prefix);

        self.db.transaction(branch_id, |txn| {
            let results = txn.scan_prefix(&prefix_key)?;
            let mut count = 0u64;
            for (_key, val) in results {
                if let Value::Bytes(bytes) = val {
                    let edges = packed::decode(&bytes)?;
                    for (_, et, _) in &edges {
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

/// Return a human-readable JSON type name for error messages.
fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::OpenSpec;
    use crate::{Database, SearchSubsystem};
    use std::sync::Arc;

    fn setup() -> (Arc<Database>, GraphStore) {
        let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
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
        gs.create_graph(b, "default", "g", None).unwrap();

        let def = patient_type();
        gs.define_object_type(b, "default", "g", def.clone())
            .unwrap();

        let got = gs
            .get_object_type(b, "default", "g", "Patient")
            .unwrap()
            .unwrap();
        assert_eq!(got, def);
    }

    #[test]
    fn list_object_types_returns_names() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();

        let mut names = gs.list_object_types(b, "default", "g").unwrap();
        names.sort();
        assert_eq!(names, vec!["LabResult", "Patient"]);
    }

    #[test]
    fn delete_object_type_during_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.delete_object_type(b, "default", "g", "Patient").unwrap();

        assert!(gs
            .get_object_type(b, "default", "g", "Patient")
            .unwrap()
            .is_none());
    }

    #[test]
    fn get_nonexistent_object_type_returns_none() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        assert!(gs
            .get_object_type(b, "default", "g", "DoesNotExist")
            .unwrap()
            .is_none());
    }

    // =========================================================================
    // Link type CRUD
    // =========================================================================

    #[test]
    fn define_link_type_then_get() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();

        let def = has_result_link();
        gs.define_link_type(b, "default", "g", def.clone()).unwrap();

        let got = gs
            .get_link_type(b, "default", "g", "HAS_RESULT")
            .unwrap()
            .unwrap();
        assert_eq!(got, def);
    }

    #[test]
    fn list_link_types_returns_names() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        let names = gs.list_link_types(b, "default", "g").unwrap();
        assert_eq!(names, vec!["HAS_RESULT"]);
    }

    #[test]
    fn delete_link_type_during_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        gs.delete_link_type(b, "default", "g", "HAS_RESULT")
            .unwrap();
        assert!(gs
            .get_link_type(b, "default", "g", "HAS_RESULT")
            .unwrap()
            .is_none());
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    #[test]
    fn first_define_sets_status_to_draft() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // Initially untyped
        assert!(gs.ontology_status(b, "default", "g").unwrap().is_none());

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();

        assert_eq!(
            gs.ontology_status(b, "default", "g").unwrap(),
            Some(OntologyStatus::Draft)
        );
    }

    #[test]
    fn freeze_transitions_draft_to_frozen() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        assert_eq!(
            gs.ontology_status(b, "default", "g").unwrap(),
            Some(OntologyStatus::Frozen)
        );
    }

    #[test]
    fn freeze_already_frozen_is_noop() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap(); // no error
    }

    #[test]
    fn define_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let result = gs.define_object_type(b, "default", "g", lab_result_type());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen"),
            "Error should mention frozen: {}",
            err
        );
    }

    #[test]
    fn delete_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let result = gs.delete_object_type(b, "default", "g", "Patient");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen"),
            "Error should mention frozen: {}",
            err
        );
    }

    #[test]
    fn freeze_with_no_types_errors() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let result = gs.freeze_ontology(b, "default", "g");
        assert!(result.is_err());
    }

    #[test]
    fn freeze_with_dangling_link_type_errors() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        // Link references "LabResult" which doesn't exist
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        let result = gs.freeze_ontology(b, "default", "g");
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
        gs.create_graph(b, "default", "g", None).unwrap();

        // All basic operations still work on untyped graph
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();
        gs.add_edge(b, "default", "g", "n1", "n1", "SELF", EdgeData::default())
            .unwrap();
        gs.remove_edge(b, "default", "g", "n1", "n1", "SELF")
            .unwrap();
        gs.remove_node(b, "default", "g", "n1").unwrap();
    }

    // =========================================================================
    // Validation (Epic 4)
    // =========================================================================

    #[test]
    fn frozen_node_with_required_props_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // "name" is required for Patient
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({"name": "Alice"})),
            ..Default::default()
        };
        gs.add_node(b, "default", "g", "p1", data).unwrap();
    }

    #[test]
    fn frozen_node_missing_required_prop_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Missing required "name" property
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({"age": 42})),
            ..Default::default()
        };
        let result = gs.add_node(b, "default", "g", "p1", data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("name"),
            "Error should mention missing prop: {}",
            err
        );
    }

    #[test]
    fn draft_node_missing_required_prop_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        // NOT frozen — draft mode

        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: None, // Missing required "name"
            ..Default::default()
        };
        gs.add_node(b, "default", "g", "p1", data).unwrap(); // succeeds in draft
    }

    #[test]
    fn frozen_untyped_node_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // No object_type — no validation
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();
    }

    #[test]
    fn frozen_edge_correct_types_succeeds() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        gs.add_node(
            b,
            "default",
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
            "default",
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        )
        .unwrap();

        gs.add_edge(
            b,
            "default",
            "g",
            "p1",
            "l1",
            "HAS_RESULT",
            EdgeData::default(),
        )
        .unwrap();
    }

    #[test]
    fn frozen_edge_wrong_source_type_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Both are LabResult, but HAS_RESULT requires source=Patient
        gs.add_node(
            b,
            "default",
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
            "default",
            "g",
            "l2",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 99})),
                ..Default::default()
            },
        )
        .unwrap();

        let result = gs.add_edge(
            b,
            "default",
            "g",
            "l1",
            "l2",
            "HAS_RESULT",
            EdgeData::default(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("source"),
            "Error should mention source type: {}",
            err
        );
    }

    #[test]
    fn frozen_edge_undeclared_type_rejected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        gs.add_node(
            b,
            "default",
            "g",
            "p1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"name": "Alice"})),
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();

        // FRIEND is not a declared link type → rejected (closed-world)
        let result = gs.add_edge(b, "default", "g", "p1", "n1", "FRIEND", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not declared") && err.contains("frozen ontology"),
            "Error should mention undeclared edge type in frozen ontology: {}",
            err
        );
    }

    #[test]
    fn frozen_edge_untyped_endpoints_allowed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Both endpoints are untyped → HAS_RESULT validation skips type checks
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();
        gs.add_node(b, "default", "g", "n2", NodeData::default())
            .unwrap();

        gs.add_edge(
            b,
            "default",
            "g",
            "n1",
            "n2",
            "HAS_RESULT",
            EdgeData::default(),
        )
        .unwrap();
    }

    #[test]
    fn existing_untyped_graph_all_writes_succeed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // Add nodes and edges on untyped graph — no validation
        gs.add_node(
            b,
            "default",
            "g",
            "n1",
            NodeData {
                object_type: Some("SomeType".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "default", "g", "n2", NodeData::default())
            .unwrap();
        gs.add_edge(
            b,
            "default",
            "g",
            "n1",
            "n2",
            "ANYTHING",
            EdgeData::default(),
        )
        .unwrap();
    }

    // =========================================================================
    // Introspection (Epic 5)
    // =========================================================================

    #[test]
    fn ontology_summary_includes_all_types() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let summary = gs.ontology_summary(b, "default", "g").unwrap().unwrap();
        assert_eq!(summary.status, OntologyStatus::Frozen);
        assert!(summary.object_types.contains_key("Patient"));
        assert!(summary.object_types.contains_key("LabResult"));
        assert!(summary.link_types.contains_key("HAS_RESULT"));
    }

    #[test]
    fn ontology_summary_node_counts_accurate() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        // Add some typed nodes
        for i in 0..3 {
            gs.add_node(
                b,
                "default",
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
            "default",
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
        gs.add_edge(
            b,
            "default",
            "g",
            "p0",
            "l1",
            "HAS_RESULT",
            EdgeData::default(),
        )
        .unwrap();

        gs.freeze_ontology(b, "default", "g").unwrap();

        let summary = gs.ontology_summary(b, "default", "g").unwrap().unwrap();
        assert_eq!(summary.object_types["Patient"].node_count, 3);
        assert_eq!(summary.object_types["LabResult"].node_count, 1);
        assert_eq!(summary.link_types["HAS_RESULT"].edge_count, 1);
    }

    #[test]
    fn ontology_summary_untyped_graph_returns_none() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        assert!(gs.ontology_summary(b, "default", "g").unwrap().is_none());
    }

    #[test]
    fn ontology_summary_draft_status_reflected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();

        let summary = gs.ontology_summary(b, "default", "g").unwrap().unwrap();
        assert_eq!(summary.status, OntologyStatus::Draft);
    }

    // =========================================================================
    // Type index (nodes_by_type — Epic 3 tests)
    // =========================================================================

    #[test]
    fn nodes_by_type_basic() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.add_node(
            b,
            "default",
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
            "default",
            "g",
            "p2",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();

        let mut patients = gs.nodes_by_type(b, "default", "g", "Patient").unwrap();
        patients.sort();
        assert_eq!(patients, vec!["p1", "p2"]);
    }

    #[test]
    fn nodes_by_type_no_results() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();

        let results = gs.nodes_by_type(b, "default", "g", "Patient").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn nodes_by_type_nonexistent_type_returns_empty() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let results = gs.nodes_by_type(b, "default", "g", "DoesNotExist").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn type_index_updated_on_type_change() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.add_node(
            b,
            "default",
            "g",
            "n1",
            NodeData {
                object_type: Some("TypeA".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "TypeA").unwrap().len(),
            1
        );

        // Change type
        gs.add_node(
            b,
            "default",
            "g",
            "n1",
            NodeData {
                object_type: Some("TypeB".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(gs
            .nodes_by_type(b, "default", "g", "TypeA")
            .unwrap()
            .is_empty());
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "TypeB").unwrap().len(),
            1
        );
    }

    #[test]
    fn type_index_cleared_on_remove() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.add_node(
            b,
            "default",
            "g",
            "n1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        gs.remove_node(b, "default", "g", "n1").unwrap();

        assert!(gs
            .nodes_by_type(b, "default", "g", "Patient")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn bulk_insert_maintains_type_index() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

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

        gs.bulk_insert(b, "default", "g", &nodes, &[], None)
            .unwrap();

        let mut patients = gs.nodes_by_type(b, "default", "g", "Patient").unwrap();
        patients.sort();
        assert_eq!(patients, vec!["p1", "p2"]);
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "LabResult").unwrap(),
            vec!["l1"]
        );
    }

    #[test]
    fn type_index_cleared_when_type_removed() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // Add node with type
        gs.add_node(
            b,
            "default",
            "g",
            "n1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: None,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "Patient")
                .unwrap()
                .len(),
            1
        );

        // Update node to remove type
        gs.add_node(b, "default", "g", "n1", NodeData::default())
            .unwrap();
        assert!(gs
            .nodes_by_type(b, "default", "g", "Patient")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn redefine_type_with_different_properties() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();

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
        gs.define_object_type(b, "default", "g", new_def.clone())
            .unwrap();

        let got = gs
            .get_object_type(b, "default", "g", "Patient")
            .unwrap()
            .unwrap();
        assert_eq!(got.properties.len(), 1);
        assert!(got.properties.contains_key("email"));
        assert!(!got.properties.contains_key("name")); // old prop gone
    }

    #[test]
    fn delete_referenced_object_type_then_freeze_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        // Delete a type referenced by the link type
        gs.delete_object_type(b, "default", "g", "LabResult")
            .unwrap();

        // Freeze should fail: HAS_RESULT references non-existent LabResult
        let result = gs.freeze_ontology(b, "default", "g");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("LabResult"),
            "Error should mention missing target type: {}",
            err
        );
    }

    #[test]
    fn frozen_undeclared_object_type_rejected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Node with undeclared type should be rejected (closed-world)
        let data = NodeData {
            object_type: Some("UnknownType".to_string()),
            properties: None,
            ..Default::default()
        };
        let result = gs.add_node(b, "default", "g", "n1", data);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not declared") && err.contains("frozen ontology"),
            "Error should mention undeclared type in frozen ontology: {}",
            err
        );
    }

    #[test]
    fn frozen_edge_nonexistent_endpoints_rejected() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Edge from non-existent nodes should be rejected (node existence check)
        let result = gs.add_edge(
            b,
            "default",
            "g",
            "phantom_src",
            "phantom_dst",
            "HAS_RESULT",
            EdgeData::default(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist"),
            "Error should mention non-existent node: {}",
            err
        );
    }

    #[test]
    fn bulk_insert_upsert_cleans_old_type_index() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // First bulk insert: node with TypeA
        let nodes1 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeA".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "default", "g", &nodes1, &[], None)
            .unwrap();
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "TypeA").unwrap(),
            vec!["n1"]
        );

        // Second bulk insert: same node with TypeB
        let nodes2 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeB".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "default", "g", &nodes2, &[], None)
            .unwrap();

        // TypeA index must be cleaned up, TypeB should have the node
        assert!(
            gs.nodes_by_type(b, "default", "g", "TypeA")
                .unwrap()
                .is_empty(),
            "Old type index entry should be cleaned up on bulk re-insert"
        );
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "TypeB").unwrap(),
            vec!["n1"]
        );
    }

    #[test]
    fn bulk_insert_upsert_removes_type_index_when_type_cleared() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // First: node with type
        let nodes1 = vec![(
            "n1".to_string(),
            NodeData {
                object_type: Some("TypeA".to_string()),
                ..Default::default()
            },
        )];
        gs.bulk_insert(b, "default", "g", &nodes1, &[], None)
            .unwrap();
        assert_eq!(
            gs.nodes_by_type(b, "default", "g", "TypeA").unwrap().len(),
            1
        );

        // Second: same node without type
        let nodes2 = vec![("n1".to_string(), NodeData::default())];
        gs.bulk_insert(b, "default", "g", &nodes2, &[], None)
            .unwrap();

        assert!(
            gs.nodes_by_type(b, "default", "g", "TypeA")
                .unwrap()
                .is_empty(),
            "Type index should be cleaned up when type removed via bulk upsert"
        );
    }

    #[test]
    fn frozen_node_no_properties_but_required_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Node with None properties when type requires "name"
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: None,
            ..Default::default()
        };
        let result = gs.add_node(b, "default", "g", "p1", data);
        assert!(result.is_err());
    }

    #[test]
    fn frozen_node_empty_properties_but_required_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Node with empty properties object when type requires "name"
        let data = NodeData {
            object_type: Some("Patient".to_string()),
            properties: Some(serde_json::json!({})),
            ..Default::default()
        };
        let result = gs.add_node(b, "default", "g", "p1", data);
        assert!(result.is_err());
    }

    #[test]
    fn define_type_on_nonexistent_graph_creates_it() {
        let (_db, gs) = setup();
        let b = default_branch();

        // Don't call create_graph — define_object_type should create it
        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();

        let got = gs.get_object_type(b, "default", "g", "Patient").unwrap();
        assert!(got.is_some());
        assert_eq!(
            gs.ontology_status(b, "default", "g").unwrap(),
            Some(OntologyStatus::Draft)
        );
    }

    #[test]
    fn delete_link_type_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let result = gs.delete_link_type(b, "default", "g", "HAS_RESULT");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen"),
            "Error should mention frozen: {}",
            err
        );
    }

    #[test]
    fn define_link_type_during_frozen_fails() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let result = gs.define_link_type(b, "default", "g", has_result_link());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen"),
            "Error should mention frozen: {}",
            err
        );
    }

    #[test]
    fn bulk_insert_validates_on_frozen() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Bulk insert with missing required prop should fail
        let nodes = vec![(
            "p1".to_string(),
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"age": 42})), // missing "name"
                ..Default::default()
            },
        )];

        let result = gs.bulk_insert(b, "default", "g", &nodes, &[], None);
        assert!(result.is_err());
    }

    // =========================================================================
    // G-14: Closed-world semantics for frozen ontology
    // =========================================================================

    #[test]
    fn test_frozen_ontology_rejects_undeclared_node_type() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Try adding a node with an undeclared type
        let result = gs.add_node(
            b,
            "default",
            "g",
            "x1",
            NodeData {
                object_type: Some("Unknown".to_string()),
                properties: None,
                ..Default::default()
            },
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not declared") && err.contains("frozen ontology"),
            "Error should mention undeclared type in frozen ontology: {}",
            err
        );
    }

    #[test]
    fn test_frozen_ontology_rejects_undeclared_edge_type() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Create nodes so the edge can reference them
        gs.add_node(
            b,
            "default",
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
            "default",
            "g",
            "p2",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"name": "Bob"})),
                ..Default::default()
            },
        )
        .unwrap();

        // Try adding an edge with an undeclared type
        let result = gs.add_edge(b, "default", "g", "p1", "p2", "LIKES", EdgeData::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not declared") && err.contains("frozen ontology"),
            "Error should mention undeclared edge type in frozen ontology: {}",
            err
        );
    }

    #[test]
    fn test_frozen_ontology_allows_declared_types() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Declared node type should succeed
        let result = gs.add_node(
            b,
            "default",
            "g",
            "p1",
            NodeData {
                object_type: Some("Patient".to_string()),
                properties: Some(serde_json::json!({"name": "Alice"})),
                ..Default::default()
            },
        );
        assert!(result.is_ok());

        let result = gs.add_node(
            b,
            "default",
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        );
        assert!(result.is_ok());

        // Declared edge type should succeed
        let result = gs.add_edge(
            b,
            "default",
            "g",
            "p1",
            "l1",
            "HAS_RESULT",
            EdgeData::default(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_unfrozen_ontology_allows_undeclared_types() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // No types defined, no freeze — undeclared types should pass
        let result = gs.add_node(
            b,
            "default",
            "g",
            "x1",
            NodeData {
                object_type: Some("Anything".to_string()),
                properties: None,
                ..Default::default()
            },
        );
        assert!(result.is_ok());

        gs.add_node(b, "default", "g", "x2", NodeData::default())
            .unwrap();
        let result = gs.add_edge(
            b,
            "default",
            "g",
            "x1",
            "x2",
            "WHATEVER",
            EdgeData::default(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_draft_ontology_allows_undeclared_types() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        // Define some types but do NOT freeze — ontology is in draft status
        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_link_type(b, "default", "g", has_result_link())
            .unwrap();

        // Undeclared node type should still pass (draft, not frozen)
        let result = gs.add_node(
            b,
            "default",
            "g",
            "x1",
            NodeData {
                object_type: Some("UnknownType".to_string()),
                properties: None,
                ..Default::default()
            },
        );
        assert!(result.is_ok());

        gs.add_node(b, "default", "g", "x2", NodeData::default())
            .unwrap();

        // Undeclared edge type should still pass (draft, not frozen)
        let result = gs.add_edge(
            b,
            "default",
            "g",
            "x1",
            "x2",
            "UNDECLARED_LINK",
            EdgeData::default(),
        );
        assert!(result.is_ok());
    }

    // =========================================================================
    // Property type enforcement (G-16)
    // =========================================================================

    #[test]
    fn property_type_string_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            PropertyDef {
                r#type: Some("string".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Item".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Correct type: string
        let ok_data = NodeData {
            object_type: Some("Item".to_string()),
            properties: Some(serde_json::json!({"name": "widget"})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "i1", ok_data).is_ok());

        // Wrong type: number
        let bad_data = NodeData {
            object_type: Some("Item".to_string()),
            properties: Some(serde_json::json!({"name": 42})),
            ..Default::default()
        };
        let err = gs.add_node(b, "default", "g", "i2", bad_data).unwrap_err();
        assert!(err.to_string().contains("must be of type 'string'"));
    }

    #[test]
    fn property_type_integer_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "count".to_string(),
            PropertyDef {
                r#type: Some("integer".to_string()),
                required: false,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Counter".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // integer accepted
        let ok = NodeData {
            object_type: Some("Counter".to_string()),
            properties: Some(serde_json::json!({"count": 5})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "c1", ok).is_ok());

        // float is not integer
        let bad = NodeData {
            object_type: Some("Counter".to_string()),
            properties: Some(serde_json::json!({"count": 2.78})),
            ..Default::default()
        };
        let err = gs.add_node(b, "default", "g", "c2", bad).unwrap_err();
        assert!(err.to_string().contains("must be of type 'integer'"));

        // Missing optional property: OK
        let missing = NodeData {
            object_type: Some("Counter".to_string()),
            properties: Some(serde_json::json!({})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "c3", missing).is_ok());
    }

    #[test]
    fn property_type_boolean_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "active".to_string(),
            PropertyDef {
                r#type: Some("boolean".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Flag".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        let ok = NodeData {
            object_type: Some("Flag".to_string()),
            properties: Some(serde_json::json!({"active": true})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "f1", ok).is_ok());

        let bad = NodeData {
            object_type: Some("Flag".to_string()),
            properties: Some(serde_json::json!({"active": "yes"})),
            ..Default::default()
        };
        let err = gs.add_node(b, "default", "g", "f2", bad).unwrap_err();
        assert!(err.to_string().contains("must be of type 'boolean'"));
    }

    #[test]
    fn property_type_number_accepts_int_and_float() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "score".to_string(),
            PropertyDef {
                r#type: Some("number".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Score".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // integer counts as number
        let int_data = NodeData {
            object_type: Some("Score".to_string()),
            properties: Some(serde_json::json!({"score": 42})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "s1", int_data).is_ok());

        // float counts as number
        let float_data = NodeData {
            object_type: Some("Score".to_string()),
            properties: Some(serde_json::json!({"score": 2.78})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "s2", float_data).is_ok());

        // string is not a number
        let bad = NodeData {
            object_type: Some("Score".to_string()),
            properties: Some(serde_json::json!({"score": "high"})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "s3", bad).is_err());
    }

    #[test]
    fn property_type_unknown_hint_is_ignored() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "data".to_string(),
            PropertyDef {
                r#type: Some("custom_type".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Flex".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Unknown type → no enforcement (backward compat)
        let data = NodeData {
            object_type: Some("Flex".to_string()),
            properties: Some(serde_json::json!({"data": [1, 2, 3]})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "x1", data).is_ok());
    }

    #[test]
    fn property_type_object_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "meta".to_string(),
            PropertyDef {
                r#type: Some("object".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Doc".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Correct: object value
        let ok = NodeData {
            object_type: Some("Doc".to_string()),
            properties: Some(serde_json::json!({"meta": {"key": "value"}})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "d1", ok).is_ok());

        // Wrong: array instead of object
        let bad = NodeData {
            object_type: Some("Doc".to_string()),
            properties: Some(serde_json::json!({"meta": [1, 2, 3]})),
            ..Default::default()
        };
        let err = gs.add_node(b, "default", "g", "d2", bad).unwrap_err();
        assert!(err.to_string().contains("must be of type 'object'"));

        // Wrong: string instead of object
        let bad2 = NodeData {
            object_type: Some("Doc".to_string()),
            properties: Some(serde_json::json!({"meta": "not-an-object"})),
            ..Default::default()
        };
        let err2 = gs.add_node(b, "default", "g", "d3", bad2).unwrap_err();
        assert!(err2.to_string().contains("must be of type 'object'"));
    }

    #[test]
    fn property_type_array_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "tags".to_string(),
            PropertyDef {
                r#type: Some("array".to_string()),
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Tagged".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Correct: array value
        let ok = NodeData {
            object_type: Some("Tagged".to_string()),
            properties: Some(serde_json::json!({"tags": ["a", "b", "c"]})),
            ..Default::default()
        };
        assert!(gs.add_node(b, "default", "g", "t1", ok).is_ok());

        // Wrong: object instead of array
        let bad = NodeData {
            object_type: Some("Tagged".to_string()),
            properties: Some(serde_json::json!({"tags": {"not": "array"}})),
            ..Default::default()
        };
        let err = gs.add_node(b, "default", "g", "t2", bad).unwrap_err();
        assert!(err.to_string().contains("must be of type 'array'"));

        // Wrong: string instead of array
        let bad2 = NodeData {
            object_type: Some("Tagged".to_string()),
            properties: Some(serde_json::json!({"tags": "not-an-array"})),
            ..Default::default()
        };
        let err2 = gs.add_node(b, "default", "g", "t3", bad2).unwrap_err();
        assert!(err2.to_string().contains("must be of type 'array'"));
    }

    #[test]
    fn edge_property_type_enforced() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        gs.define_object_type(b, "default", "g", patient_type())
            .unwrap();
        gs.define_object_type(b, "default", "g", lab_result_type())
            .unwrap();

        let mut link_props = HashMap::new();
        link_props.insert(
            "confidence".to_string(),
            PropertyDef {
                r#type: Some("number".to_string()),
                required: true,
            },
        );
        gs.define_link_type(
            b,
            "default",
            "g",
            LinkTypeDef {
                name: "HAS_RESULT".to_string(),
                source: "Patient".to_string(),
                target: "LabResult".to_string(),
                cardinality: None,
                properties: link_props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        gs.add_node(
            b,
            "default",
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
            "default",
            "g",
            "l1",
            NodeData {
                object_type: Some("LabResult".to_string()),
                properties: Some(serde_json::json!({"value": 42})),
                ..Default::default()
            },
        )
        .unwrap();

        // Correct: number property
        let ok_data = EdgeData {
            weight: 1.0,
            properties: Some(serde_json::json!({"confidence": 0.95})),
        };
        assert!(gs
            .add_edge(b, "default", "g", "p1", "l1", "HAS_RESULT", ok_data)
            .is_ok());

        // Wrong: string instead of number
        let bad_data = EdgeData {
            weight: 1.0,
            properties: Some(serde_json::json!({"confidence": "high"})),
        };
        let err = gs
            .add_edge(b, "default", "g", "p1", "l1", "HAS_RESULT", bad_data)
            .unwrap_err();
        assert!(err.to_string().contains("must be of type 'number'"));

        // Missing required property
        let missing_data = EdgeData {
            weight: 1.0,
            properties: None,
        };
        let err2 = gs
            .add_edge(b, "default", "g", "p1", "l1", "HAS_RESULT", missing_data)
            .unwrap_err();
        assert!(err2.to_string().contains("missing required property"));
    }

    #[test]
    fn property_type_no_hint_skips_check() {
        let (_db, gs) = setup();
        let b = default_branch();
        gs.create_graph(b, "default", "g", None).unwrap();

        let mut props = HashMap::new();
        props.insert(
            "anything".to_string(),
            PropertyDef {
                r#type: None,
                required: true,
            },
        );
        gs.define_object_type(
            b,
            "default",
            "g",
            ObjectTypeDef {
                name: "Any".to_string(),
                properties: props,
            },
        )
        .unwrap();
        gs.freeze_ontology(b, "default", "g").unwrap();

        // Any type accepted when no type hint
        for val in [
            serde_json::json!(42),
            serde_json::json!("text"),
            serde_json::json!(true),
            serde_json::json!([1, 2]),
        ] {
            let data = NodeData {
                object_type: Some("Any".to_string()),
                properties: Some(serde_json::json!({"anything": val})),
                ..Default::default()
            };
            assert!(gs.add_node(b, "default", "g", "a1", data).is_ok());
        }
    }
}
