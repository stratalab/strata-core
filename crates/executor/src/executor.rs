use std::sync::Arc;

use strata_engine::{AccessMode, Database};

use crate::bridge::{
    access_denied, database_info, is_read_only, reject_reserved_branch, requires_session,
    runtime_default_branch, session_required, Primitives,
};
use crate::handlers::{
    arrow_import, branch, config, configure_model, database, embed, embed_runtime, event, export,
    generate, graph, json, kv, maintenance, models, recipe, search, space, space_delete, vector,
};
use crate::{BranchId, Command, Error, Output, Result};

/// Transport-neutral command executor.
pub struct Executor {
    db: Arc<Database>,
    primitives: Arc<Primitives>,
    access_mode: AccessMode,
    default_branch: BranchId,
}

impl Executor {
    /// Create an executor over a database handle.
    pub fn new(db: Arc<Database>) -> Self {
        Self::new_with_mode(db, AccessMode::ReadWrite)
    }

    /// Create an executor with an explicit access mode.
    pub fn new_with_mode(db: Arc<Database>, access_mode: AccessMode) -> Self {
        let default_branch = runtime_default_branch(&db);
        let primitives = Arc::new(Primitives::new(db.clone()));
        Self {
            db,
            primitives,
            access_mode,
            default_branch,
        }
    }

    /// Execute a command and return its structured result.
    pub fn execute(&self, mut command: Command) -> Result<Output> {
        if is_read_only(self.access_mode) && command.is_write() {
            return Err(access_denied(&self.db, command.name()));
        }

        command.resolve_defaults_with(&self.default_branch);
        if let Some(branch) = command.resolved_branch() {
            reject_reserved_branch(branch)?;
        }
        if requires_session(&command) {
            return Err(session_required(command.name()));
        }

        self.execute_local(command)
    }

    /// Return the executor access mode.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    pub(crate) fn database(&self) -> &Arc<Database> {
        &self.db
    }

    pub(crate) fn primitives(&self) -> &Arc<Primitives> {
        &self.primitives
    }

    pub(crate) fn execute_local(&self, command: Command) -> Result<Output> {
        match command {
            Command::Ping => Ok(Output::Pong {
                version: env!("CARGO_PKG_VERSION").to_string(),
            }),
            Command::Info => Ok(Output::DatabaseInfo(database_info(
                &self.db,
                &self.default_branch,
            ))),
            Command::Health => Ok(Output::Health(self.db.health())),
            Command::Metrics => Ok(Output::Metrics(self.db.metrics())),
            Command::Flush => {
                embed_runtime::flush_embed_buffer(&self.primitives);
                self.db.scheduler().drain();
                self.db.flush().map_err(Error::from)?;
                Ok(Output::Unit)
            }
            Command::Compact => {
                self.db.compact().map_err(Error::from)?;
                Ok(Output::Unit)
            }
            Command::Describe { branch } => database::describe(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::BranchCreate {
                branch_id,
                metadata,
            } => branch::create(&self.db, branch_id, metadata),
            Command::BranchGet { branch } => branch::get(&self.db, branch),
            Command::BranchList {
                state,
                limit,
                offset,
            } => branch::list(&self.db, state, limit, offset),
            Command::BranchExists { branch } => branch::exists(&self.db, branch),
            Command::BranchDelete { branch } => branch::delete(&self.db, branch),
            Command::BranchFork {
                source,
                destination,
                message,
                creator,
            } => branch::fork(&self.primitives, source, destination, message, creator),
            Command::BranchDiff {
                branch_a,
                branch_b,
                filter_primitives,
                filter_spaces,
                as_of,
            } => branch::diff(
                &self.primitives,
                branch_a,
                branch_b,
                filter_primitives,
                filter_spaces,
                as_of,
            ),
            Command::BranchMerge {
                source,
                target,
                strategy,
                message,
                creator,
            } => branch::merge(&self.primitives, source, target, strategy, message, creator),
            Command::BranchDiffThreeWay { branch_a, branch_b } => {
                branch::diff_three_way(&self.primitives, branch_a, branch_b)
            }
            Command::BranchMergeBase { branch_a, branch_b } => {
                branch::merge_base(&self.primitives, branch_a, branch_b)
            }
            Command::BranchRevert {
                branch,
                from_version,
                to_version,
            } => branch::revert(&self.primitives, branch, from_version, to_version),
            Command::BranchCherryPick {
                source,
                target,
                keys,
                filter_spaces,
                filter_keys,
                filter_primitives,
            } => branch::cherry_pick(
                &self.primitives,
                source,
                target,
                keys,
                filter_spaces,
                filter_keys,
                filter_primitives,
            ),
            Command::BranchExport { branch_id, path } => {
                branch::export_bundle(&self.primitives, branch_id, path)
            }
            Command::BranchImport { path } => branch::import_bundle(&self.primitives, path),
            Command::BranchBundleValidate { path } => branch::validate_bundle(path),
            Command::TagCreate {
                branch,
                name,
                version,
                message,
                creator,
            } => branch::tag_create(&self.primitives, branch, name, version, message, creator),
            Command::TagDelete { branch, name } => {
                branch::tag_delete(&self.primitives, branch, name)
            }
            Command::TagList { branch } => branch::tag_list(&self.primitives, branch),
            Command::TagResolve { branch, name } => {
                branch::tag_resolve(&self.primitives, branch, name)
            }
            Command::NoteAdd {
                branch,
                version,
                message,
                author,
                metadata,
            } => branch::note_add(&self.primitives, branch, version, message, author, metadata),
            Command::NoteGet { branch, version } => {
                branch::note_get(&self.primitives, branch, version)
            }
            Command::NoteDelete { branch, version } => {
                branch::note_delete(&self.primitives, branch, version)
            }
            Command::SpaceList { branch } => space::list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::SpaceCreate { branch, space } => space::create(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space,
            ),
            Command::SpaceExists { branch, space } => space::exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space,
            ),
            Command::SpaceDelete {
                branch,
                space,
                force,
            } => space_delete::delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space,
                force,
            ),
            Command::KvPut {
                branch,
                space,
                key,
                value,
            } => kv::put(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
                value,
            ),
            Command::KvGet {
                branch,
                space,
                key,
                as_of,
            } => kv::get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
                as_of,
            ),
            Command::KvDelete { branch, space, key } => kv::delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
            ),
            Command::KvList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
            } => kv::list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
                cursor,
                limit,
                as_of,
            ),
            Command::KvScan {
                branch,
                space,
                start,
                limit,
            } => kv::scan(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                start,
                limit,
            ),
            Command::KvBatchPut {
                branch,
                space,
                entries,
            } => kv::batch_put(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                entries,
            ),
            Command::KvBatchGet {
                branch,
                space,
                keys,
            } => kv::batch_get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                keys,
            ),
            Command::KvBatchDelete {
                branch,
                space,
                keys,
            } => kv::batch_delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                keys,
            ),
            Command::KvBatchExists {
                branch,
                space,
                keys,
            } => kv::batch_exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                keys,
            ),
            Command::KvExists { branch, space, key } => kv::exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
            ),
            Command::KvGetv { branch, space, key } => kv::getv(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
            ),
            Command::KvCount {
                branch,
                space,
                prefix,
            } => kv::count(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
            ),
            Command::KvSample {
                branch,
                space,
                prefix,
                count,
            } => kv::sample(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
                count.unwrap_or(5),
            ),
            Command::JsonSet {
                branch,
                space,
                key,
                path,
                value,
            } => json::set(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
                path,
                value,
            ),
            Command::JsonGet {
                branch,
                space,
                key,
                path,
                as_of,
            } => json::get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
                path,
                as_of,
            ),
            Command::JsonDelete {
                branch,
                space,
                key,
                path,
            } => json::delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
                path,
            ),
            Command::JsonGetv { branch, space, key } => json::getv(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
            ),
            Command::JsonExists { branch, space, key } => json::exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                key,
            ),
            Command::JsonBatchSet {
                branch,
                space,
                entries,
            } => json::batch_set(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                entries,
            ),
            Command::JsonBatchGet {
                branch,
                space,
                entries,
            } => json::batch_get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                entries,
            ),
            Command::JsonBatchDelete {
                branch,
                space,
                entries,
            } => json::batch_delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                entries,
            ),
            Command::JsonList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
            } => json::list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
                cursor,
                limit,
                as_of,
            ),
            Command::JsonCount {
                branch,
                space,
                prefix,
            } => json::count(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
            ),
            Command::JsonSample {
                branch,
                space,
                prefix,
                count,
            } => json::sample(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                prefix,
                count.unwrap_or(5),
            ),
            Command::JsonCreateIndex {
                branch,
                space,
                name,
                field_path,
                index_type,
            } => json::create_index(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                name,
                field_path,
                index_type,
            ),
            Command::JsonDropIndex {
                branch,
                space,
                name,
            } => json::drop_index(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                name,
            ),
            Command::JsonListIndexes { branch, space } => json::list_indexes(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
            ),
            Command::EventBatchAppend {
                branch,
                space,
                entries,
            } => event::batch_append(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                entries,
            ),
            Command::EventAppend {
                branch,
                space,
                event_type,
                payload,
            } => event::append(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                event_type,
                payload,
            ),
            Command::EventGet {
                branch,
                space,
                sequence,
                as_of,
            } => event::get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                sequence,
                as_of,
            ),
            Command::EventExists {
                branch,
                space,
                sequence,
            } => event::exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                sequence,
            ),
            Command::EventGetByType {
                branch,
                space,
                event_type,
                limit,
                after_sequence,
                as_of,
            } => event::get_by_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                event_type,
                limit,
                after_sequence,
                as_of,
            ),
            Command::EventLen {
                branch,
                space,
                as_of,
            } => event::len(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                as_of,
            ),
            Command::EventRange {
                branch,
                space,
                start_seq,
                end_seq,
                limit,
                direction,
                event_type,
            } => event::range(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                start_seq,
                end_seq,
                limit,
                direction,
                event_type,
            ),
            Command::EventRangeByTime {
                branch,
                space,
                start_ts,
                end_ts,
                limit,
                direction,
                event_type,
            } => event::range_by_time(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                start_ts,
                end_ts,
                limit,
                direction,
                event_type,
            ),
            Command::EventListTypes {
                branch,
                space,
                as_of,
            } => event::list_types(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                as_of,
            ),
            Command::EventList {
                branch,
                space,
                event_type,
                limit,
                as_of,
            } => event::list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                event_type,
                limit,
                as_of,
            ),
            Command::VectorUpsert {
                branch,
                space,
                collection,
                key,
                vector: query_vector,
                metadata,
            } => vector::upsert(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                key,
                query_vector,
                metadata,
            ),
            Command::VectorGet {
                branch,
                space,
                collection,
                key,
                as_of,
            } => vector::get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                key,
                as_of,
            ),
            Command::VectorDelete {
                branch,
                space,
                collection,
                key,
            } => vector::delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                key,
            ),
            Command::VectorGetv {
                branch,
                space,
                collection,
                key,
            } => vector::getv(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                key,
            ),
            Command::VectorExists {
                branch,
                space,
                collection,
                key,
            } => vector::exists(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                key,
            ),
            Command::VectorQuery {
                branch,
                space,
                collection,
                query,
                k,
                filter,
                metric,
                as_of,
            } => vector::query(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                query,
                k,
                filter,
                metric,
                as_of,
            ),
            Command::VectorCreateCollection {
                branch,
                space,
                collection,
                dimension,
                metric,
            } => vector::create_collection(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                dimension,
                metric,
            ),
            Command::VectorDeleteCollection {
                branch,
                space,
                collection,
            } => vector::delete_collection(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
            ),
            Command::VectorListCollections { branch, space } => vector::list_collections(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
            ),
            Command::VectorCollectionStats {
                branch,
                space,
                collection,
            } => vector::collection_stats(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
            ),
            Command::VectorCount {
                branch,
                space,
                collection,
            } => vector::count(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
            ),
            Command::VectorBatchUpsert {
                branch,
                space,
                collection,
                entries,
            } => vector::batch_upsert(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                entries,
            ),
            Command::VectorBatchGet {
                branch,
                space,
                collection,
                keys,
            } => vector::batch_get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                keys,
            ),
            Command::VectorBatchDelete {
                branch,
                space,
                collection,
                keys,
            } => vector::batch_delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                keys,
            ),
            Command::VectorSample {
                branch,
                space,
                collection,
                count,
            } => vector::sample(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                collection,
                count.unwrap_or(5),
            ),
            Command::GraphCreate {
                branch,
                space,
                graph: graph_name,
                cascade_policy,
            } => graph::create(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                cascade_policy,
            ),
            Command::GraphDelete {
                branch,
                space,
                graph: graph_name,
            } => graph::delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphList { branch, space } => graph::list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
            ),
            Command::GraphGetMeta {
                branch,
                space,
                graph: graph_name,
            } => graph::get_meta(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphAddNode {
                branch,
                space,
                graph: graph_name,
                node_id,
                entity_ref,
                properties,
                object_type,
            } => graph::add_node(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                node_id,
                entity_ref,
                properties,
                object_type,
            ),
            Command::GraphGetNode {
                branch,
                space,
                graph: graph_name,
                node_id,
                as_of,
            } => graph::get_node(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                node_id,
                as_of,
            ),
            Command::GraphRemoveNode {
                branch,
                space,
                graph: graph_name,
                node_id,
            } => graph::remove_node(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                node_id,
            ),
            Command::GraphListNodes {
                branch,
                space,
                graph: graph_name,
                as_of,
            } => graph::list_nodes(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                as_of,
            ),
            Command::GraphListNodesPaginated {
                branch,
                space,
                graph: graph_name,
                limit,
                cursor,
            } => graph::list_nodes_paginated(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                limit,
                cursor,
            ),
            Command::GraphAddEdge {
                branch,
                space,
                graph: graph_name,
                src,
                dst,
                edge_type,
                weight,
                properties,
            } => graph::add_edge(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                src,
                dst,
                edge_type,
                weight,
                properties,
            ),
            Command::GraphRemoveEdge {
                branch,
                space,
                graph: graph_name,
                src,
                dst,
                edge_type,
            } => graph::remove_edge(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                src,
                dst,
                edge_type,
            ),
            Command::GraphNeighbors {
                branch,
                space,
                graph: graph_name,
                node_id,
                direction,
                edge_type,
                as_of,
            } => graph::neighbors(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                node_id,
                direction,
                edge_type,
                as_of,
            ),
            Command::GraphBulkInsert {
                branch,
                space,
                graph: graph_name,
                nodes,
                edges,
                chunk_size,
            } => graph::bulk_insert(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                nodes,
                edges,
                chunk_size,
            ),
            Command::GraphBfs {
                branch,
                space,
                graph: graph_name,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
            } => graph::bfs(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
            ),
            Command::GraphDefineObjectType {
                branch,
                space,
                graph: graph_name,
                definition,
            } => graph::define_object_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                definition,
            ),
            Command::GraphGetObjectType {
                branch,
                space,
                graph: graph_name,
                name,
            } => graph::get_object_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                name,
            ),
            Command::GraphListObjectTypes {
                branch,
                space,
                graph: graph_name,
            } => graph::list_object_types(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphDeleteObjectType {
                branch,
                space,
                graph: graph_name,
                name,
            } => graph::delete_object_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                name,
            ),
            Command::GraphDefineLinkType {
                branch,
                space,
                graph: graph_name,
                definition,
            } => graph::define_link_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                definition,
            ),
            Command::GraphGetLinkType {
                branch,
                space,
                graph: graph_name,
                name,
            } => graph::get_link_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                name,
            ),
            Command::GraphListLinkTypes {
                branch,
                space,
                graph: graph_name,
            } => graph::list_link_types(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphDeleteLinkType {
                branch,
                space,
                graph: graph_name,
                name,
            } => graph::delete_link_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                name,
            ),
            Command::GraphFreezeOntology {
                branch,
                space,
                graph: graph_name,
            } => graph::freeze_ontology(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphOntologyStatus {
                branch,
                space,
                graph: graph_name,
            } => graph::ontology_status(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphOntologySummary {
                branch,
                space,
                graph: graph_name,
            } => graph::ontology_summary(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphListOntologyTypes {
                branch,
                space,
                graph: graph_name,
            } => graph::list_ontology_types(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
            ),
            Command::GraphNodesByType {
                branch,
                space,
                graph: graph_name,
                object_type,
            } => graph::nodes_by_type(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                object_type,
            ),
            Command::GraphWcc {
                branch,
                space,
                graph: graph_name,
                top_n,
                include_all,
            } => graph::wcc(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                top_n,
                include_all,
            ),
            Command::GraphCdlp {
                branch,
                space,
                graph: graph_name,
                max_iterations,
                direction,
                top_n,
                include_all,
            } => graph::cdlp(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                max_iterations,
                direction,
                top_n,
                include_all,
            ),
            Command::GraphPagerank {
                branch,
                space,
                graph: graph_name,
                damping,
                max_iterations,
                tolerance,
                top_n,
                include_all,
            } => graph::pagerank(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                damping,
                max_iterations,
                tolerance,
                top_n,
                include_all,
            ),
            Command::GraphLcc {
                branch,
                space,
                graph: graph_name,
                top_n,
                include_all,
            } => graph::lcc(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                top_n,
                include_all,
            ),
            Command::GraphSssp {
                branch,
                space,
                graph: graph_name,
                source,
                direction,
                top_n,
                include_all,
            } => graph::sssp(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                graph_name,
                source,
                direction,
                top_n,
                include_all,
            ),
            Command::RetentionApply { branch } => maintenance::retention_apply(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::RetentionStats { .. } => maintenance::retention_unavailable("RetentionStats"),
            Command::RetentionPreview { .. } => {
                maintenance::retention_unavailable("RetentionPreview")
            }
            Command::TimeRange { branch } => maintenance::time_range(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::DbExport {
                branch,
                space,
                primitive,
                format,
                prefix,
                limit,
                path,
                collection,
                graph,
            } => export::db_export(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                primitive,
                format,
                prefix,
                limit,
                path,
                collection,
                graph,
            ),
            Command::ArrowImport {
                branch,
                space,
                file_path,
                target,
                key_column,
                value_column,
                collection,
                format,
            } => arrow_import::arrow_import(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                file_path,
                target,
                key_column,
                value_column,
                collection,
                format,
            ),
            Command::ConfigureModel {
                endpoint,
                model,
                api_key,
                timeout_ms,
            } => configure_model::configure_model(
                &self.primitives,
                endpoint,
                model,
                api_key,
                timeout_ms,
            ),
            Command::Search {
                branch,
                space,
                search: search_query,
            } => search::search(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                space.unwrap_or_else(|| "default".to_string()),
                search_query,
            ),
            Command::Embed { text } => embed::embed(&self.primitives, text),
            Command::EmbedBatch { texts } => embed::embed_batch(&self.primitives, texts),
            Command::ModelsList => models::models_list(&self.primitives),
            Command::ModelsPull { name } => models::models_pull(&self.primitives, name),
            Command::ModelsLocal => models::models_local(&self.primitives),
            Command::Generate {
                model,
                prompt,
                max_tokens,
                temperature,
                top_k,
                top_p,
                seed,
                stop_tokens,
                stop_sequences,
            } => generate::generate(
                &self.primitives,
                model,
                prompt,
                max_tokens,
                temperature,
                top_k,
                top_p,
                seed,
                stop_tokens,
                stop_sequences,
            ),
            Command::Tokenize {
                model,
                text,
                add_special_tokens,
            } => generate::tokenize(&self.primitives, model, text, add_special_tokens),
            Command::Detokenize { model, ids } => {
                generate::detokenize(&self.primitives, model, ids)
            }
            Command::GenerateUnload { model } => generate::generate_unload(&self.primitives, model),
            Command::EmbedStatus => Ok(Output::EmbedStatus(embed_runtime::embed_status(
                &self.primitives,
            ))),
            Command::ReindexEmbeddings { branch } => embed_runtime::reindex_embeddings(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::ConfigGet => config::config_get(&self.primitives),
            Command::ConfigureSet { key, value } => {
                config::configure_set(&self.primitives, key, value)
            }
            Command::ConfigureGetKey { key } => config::configure_get_key(&self.primitives, key),
            Command::ConfigSetAutoEmbed { enabled } => {
                config::config_set_auto_embed(&self.primitives, enabled)
            }
            Command::AutoEmbedStatus => config::auto_embed_status(&self.primitives),
            Command::DurabilityCounters => config::durability_counters(&self.primitives),
            Command::RecipeSet {
                branch,
                name,
                recipe_json,
            } => recipe::recipe_set(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                name,
                recipe_json,
            ),
            Command::RecipeGet { branch, name } => recipe::recipe_get(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                name,
            ),
            Command::RecipeGetDefault { branch } => recipe::recipe_get_default(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::RecipeList { branch } => recipe::recipe_list(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
            ),
            Command::RecipeDelete { branch, name } => recipe::recipe_delete(
                &self.primitives,
                branch.unwrap_or_else(|| self.default_branch.clone()),
                name,
            ),
            Command::RecipeSeed => recipe::recipe_seed(&self.primitives),
            other => Err(Error::Internal {
                reason: format!("{} is not handled by the local executor path", other.name()),
                hint: None,
            }),
        }
    }

    /// Return the default branch used for commands that omit a branch.
    pub fn default_branch(&self) -> &BranchId {
        &self.default_branch
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use strata_engine::AccessMode;

    use crate::{BranchId, Command, DistanceMetric, Error, Executor, Output, Strata};

    fn test_db() -> Arc<strata_engine::Database> {
        Strata::cache()
            .expect("cache database should open")
            .database()
    }

    #[test]
    fn read_only_executor_rejects_transaction_begin() {
        let executor = Executor::new_with_mode(test_db(), AccessMode::ReadOnly);
        let error = executor
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect_err("read-only executor should reject transaction begin");

        assert!(matches!(error, Error::AccessDenied { .. }));
    }

    #[test]
    fn transaction_commands_require_a_session() {
        let executor = Executor::new(test_db());
        let error = executor
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .expect_err("direct executor transactions should be rejected");

        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(
            format!("{error:?}").contains("requires a session"),
            "unexpected error: {error:?}",
        );
    }

    #[test]
    fn info_executes_on_local_path() {
        let executor = Executor::new(test_db());
        let output = executor
            .execute_local(Command::Info)
            .expect("info should execute locally");

        match output {
            Output::DatabaseInfo(info) => {
                assert!(!info.version.is_empty());
                assert!(info.branch_count >= 1);
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[test]
    fn reserved_system_branch_is_rejected() {
        let executor = Executor::new(test_db());

        let error = executor
            .execute(Command::KvGet {
                branch: Some(BranchId::from("_system")),
                space: None,
                key: "secret".into(),
                as_of: None,
            })
            .expect_err("system branch reads should be rejected");

        assert!(matches!(error, Error::InvalidInput { .. }));
    }

    #[test]
    fn branch_metadata_executes_on_local_path() {
        let executor = Executor::new(test_db());
        let tag_name = format!("local-tag-{}", uuid::Uuid::new_v4());
        executor
            .execute(Command::TagCreate {
                branch: "default".into(),
                name: tag_name.clone(),
                version: None,
                message: None,
                creator: None,
            })
            .expect("branch metadata commands should execute locally");

        let output = executor
            .execute(Command::TagList {
                branch: "default".into(),
            })
            .expect("branch metadata list should execute locally");
        match output {
            Output::TagList(tags) => assert!(tags.iter().any(|tag| tag.name == tag_name)),
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[test]
    fn auto_embed_sensitive_writes_still_execute() {
        let db = test_db();
        db.set_auto_embed(true);
        let executor = Executor::new(db);

        let output = executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "auto-embed-key".into(),
                value: strata_core::Value::String("value".into()),
            })
            .expect("auto-embed compatible write path should succeed");

        assert!(matches!(output, Output::WriteResult { .. }));
    }

    #[test]
    fn single_key_exists_and_vector_count_commands_execute() {
        let executor = Executor::new(test_db());
        let suffix = uuid::Uuid::new_v4();
        let kv_key = format!("exists-kv-{suffix}");
        let json_key = format!("exists-json-{suffix}");
        let vector_collection = format!("exists-vector-{suffix}");
        let vector_key = "vector-key".to_string();

        assert_eq!(
            executor
                .execute(Command::KvExists {
                    branch: None,
                    space: None,
                    key: kv_key.clone(),
                })
                .expect("kv exists should execute"),
            Output::Bool(false)
        );
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: kv_key.clone(),
                value: strata_core::Value::String("value".into()),
            })
            .expect("kv put should execute");
        assert_eq!(
            executor
                .execute(Command::KvExists {
                    branch: None,
                    space: None,
                    key: kv_key,
                })
                .expect("kv exists should execute"),
            Output::Bool(true)
        );

        assert_eq!(
            executor
                .execute(Command::JsonExists {
                    branch: None,
                    space: None,
                    key: json_key.clone(),
                })
                .expect("json exists should execute"),
            Output::Bool(false)
        );
        executor
            .execute(Command::JsonSet {
                branch: None,
                space: None,
                key: json_key.clone(),
                path: "$".into(),
                value: strata_core::Value::Bool(true),
            })
            .expect("json set should execute");
        assert_eq!(
            executor
                .execute(Command::JsonExists {
                    branch: None,
                    space: None,
                    key: json_key,
                })
                .expect("json exists should execute"),
            Output::Bool(true)
        );

        let mut payload = std::collections::HashMap::new();
        payload.insert("ok".to_string(), strata_core::Value::Bool(true));
        let event_output = executor
            .execute(Command::EventAppend {
                branch: None,
                space: None,
                event_type: "exists.test".into(),
                payload: strata_core::Value::Object(Box::new(payload)),
            })
            .expect("event append should execute");
        let sequence = match event_output {
            Output::EventAppendResult { sequence, .. } => sequence,
            other => panic!("unexpected event append output: {other:?}"),
        };
        assert_eq!(
            executor
                .execute(Command::EventExists {
                    branch: None,
                    space: None,
                    sequence,
                })
                .expect("event exists should execute"),
            Output::Bool(true)
        );

        executor
            .execute(Command::VectorCreateCollection {
                branch: None,
                space: None,
                collection: vector_collection.clone(),
                dimension: 3,
                metric: DistanceMetric::Cosine,
            })
            .expect("vector collection create should execute");
        assert_eq!(
            executor
                .execute(Command::VectorCount {
                    branch: None,
                    space: None,
                    collection: vector_collection.clone(),
                })
                .expect("vector count should execute"),
            Output::Uint(0)
        );
        assert_eq!(
            executor
                .execute(Command::VectorExists {
                    branch: None,
                    space: None,
                    collection: vector_collection.clone(),
                    key: vector_key.clone(),
                })
                .expect("vector exists should execute"),
            Output::Bool(false)
        );
        executor
            .execute(Command::VectorUpsert {
                branch: None,
                space: None,
                collection: vector_collection.clone(),
                key: vector_key.clone(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: None,
            })
            .expect("vector upsert should execute");
        assert_eq!(
            executor
                .execute(Command::VectorExists {
                    branch: None,
                    space: None,
                    collection: vector_collection.clone(),
                    key: vector_key,
                })
                .expect("vector exists should execute"),
            Output::Bool(true)
        );
        assert_eq!(
            executor
                .execute(Command::VectorCount {
                    branch: None,
                    space: None,
                    collection: vector_collection,
                })
                .expect("vector count should execute"),
            Output::Uint(1)
        );
    }
}
