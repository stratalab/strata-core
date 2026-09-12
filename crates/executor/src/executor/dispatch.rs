use super::{Command, Executor, ExecutorError, Output};
#[cfg(feature = "inference")]
use crate::PageInfo;

impl Executor {
    /// Executes one serialized command.
    pub fn execute(&mut self, command: Command) -> Result<Output, ExecutorError> {
        let is_write = command.is_write();
        let result = self.dispatch_command(command);
        if is_write {
            // The store-state watermark ticks on every write ATTEMPT, success
            // or failure — an itemwise batch can partially apply and an
            // in-doubt error may have committed, so for a refresh hint a
            // spurious tick (wasted re-read) is the safe direction and a
            // missed tick (stale observer) is not.
            self.state_version
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    fn dispatch_command(&mut self, command: Command) -> Result<Output, ExecutorError> {
        match command {
            Command::Ping {} => self.execute_ping(),
            Command::Info { branch } => self.execute_info(branch.as_deref()),
            Command::Health { branch } => self.execute_health(branch.as_deref()),
            Command::Metrics { branch } => self.execute_metrics(branch.as_deref()),
            Command::Describe { branch } => self.execute_describe(branch.as_deref()),
            Command::ConfigGet {} => self.execute_config_get(),
            Command::IpcStatus {} => Ok(self.execute_ipc_status()),
            Command::IpcStop {} => Ok(self.execute_ipc_stop()),
            Command::RemoteGet {} => self.execute_remote_get(),
            Command::HubClone {
                dataset,
                branch,
                dest,
                hub_url,
            } => self.execute_hub_clone(&dataset, branch.as_deref(), &dest, hub_url),
            Command::HubInfo { hub_url } => self.execute_hub_info(hub_url),
            Command::HubListDatasets {
                hub_url,
                tasks,
                tags,
                primitives,
                license,
                size_min_bytes,
                size_max_bytes,
                sort,
                limit,
                offset,
            } => self.execute_hub_list_datasets(
                hub_url,
                tasks,
                tags,
                primitives,
                license,
                size_min_bytes,
                size_max_bytes,
                sort,
                limit,
                offset,
            ),
            Command::HubGetDataset { name, hub_url } => {
                self.execute_hub_get_dataset(&name, hub_url)
            }
            Command::HubListRefs { dataset, hub_url } => {
                self.execute_hub_list_refs(&dataset, hub_url)
            }
            Command::HubListYanked { since, hub_url } => {
                self.execute_hub_list_yanked(since.as_deref(), hub_url)
            }
            Command::ConfigureGetKey { key } => self.execute_configure_get_key(&key),
            Command::SpaceList { branch } => self.execute_space_list(branch.as_deref()),
            Command::SpaceCreate { branch, space } => {
                self.execute_space_create(branch.as_deref(), &space)
            }
            Command::SpaceExists { branch, space } => {
                self.execute_space_exists(branch.as_deref(), &space)
            }
            Command::SpaceDelete {
                branch,
                space,
                force,
            } => self.execute_space_delete(branch.as_deref(), &space, force),
            Command::BranchList {} => self.execute_branch_list(),
            Command::BranchGet { branch } => self.execute_branch_get(&branch),
            Command::BranchDiff {
                branch_a,
                branch_b,
                at_timestamp,
            } => self.execute_branch_diff(&branch_a, &branch_b, at_timestamp),
            Command::BranchCreate { branch } => self.execute_branch_create(&branch),
            Command::BranchForkCurrent { source, branch } => {
                self.execute_branch_fork_current(&source, &branch)
            }
            Command::BranchForkAtVersion {
                source,
                branch,
                version,
            } => self.execute_branch_fork_at_version(&source, &branch, version),
            Command::BranchForkAtTimestamp {
                source,
                branch,
                timestamp,
            } => self.execute_branch_fork_at_timestamp(&source, &branch, timestamp),
            Command::BranchDelete { branch } => self.execute_branch_delete(&branch),
            Command::BranchMerge {
                source,
                target,
                strategy,
            } => self.execute_branch_merge(&source, &target, strategy),
            Command::BranchPreview {
                source,
                target,
                strategy,
            } => self.execute_branch_preview(&source, &target, strategy),
            Command::KvPut {
                branch,
                space,
                key,
                value,
            } => self.execute_kv_put(branch.as_deref(), space.as_deref(), key, value),
            Command::KvGet {
                branch,
                space,
                key,
                as_of,
                as_of_time,
            } => self.execute_kv_get(branch.as_deref(), space.as_deref(), key, as_of, as_of_time),
            Command::KvDelete { branch, space, key } => {
                self.execute_kv_delete(branch.as_deref(), space.as_deref(), key)
            }
            Command::KvList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_kv_list(
                branch.as_deref(),
                space.as_deref(),
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::KvScan {
                branch,
                space,
                start,
                limit,
            } => self.execute_kv_scan(branch.as_deref(), space.as_deref(), start, limit),
            Command::KvBatchPut {
                branch,
                space,
                entries,
            } => self.execute_kv_batch_put(branch.as_deref(), space.as_deref(), entries),
            Command::KvBatchGet {
                branch,
                space,
                keys,
            } => self.execute_kv_batch_get(branch.as_deref(), space.as_deref(), keys),
            Command::KvBatchDelete {
                branch,
                space,
                keys,
            } => self.execute_kv_batch_delete(branch.as_deref(), space.as_deref(), keys),
            Command::KvBatchExists {
                branch,
                space,
                keys,
            } => self.execute_kv_batch_exists(branch.as_deref(), space.as_deref(), keys),
            Command::KvExists { branch, space, key } => {
                self.execute_kv_exists(branch.as_deref(), space.as_deref(), key)
            }
            Command::KvHistory { branch, space, key } => {
                self.execute_kv_history(branch.as_deref(), space.as_deref(), key)
            }
            Command::KvCount {
                branch,
                space,
                prefix,
                as_of,
                as_of_time,
            } => self.execute_kv_count(
                branch.as_deref(),
                space.as_deref(),
                prefix,
                as_of,
                as_of_time,
            ),
            Command::KvSample {
                branch,
                space,
                prefix,
                count,
            } => self.execute_kv_sample(branch.as_deref(), space.as_deref(), prefix, count),
            Command::JsonSet {
                branch,
                space,
                key,
                path,
                value,
            } => self.execute_json_set(branch.as_deref(), space.as_deref(), &key, &path, value),
            Command::JsonGet {
                branch,
                space,
                key,
                path,
                as_of,
                as_of_time,
            } => self.execute_json_get(
                branch.as_deref(),
                space.as_deref(),
                &key,
                &path,
                as_of,
                as_of_time,
            ),
            Command::JsonDelete {
                branch,
                space,
                key,
                path,
            } => self.execute_json_delete(branch.as_deref(), space.as_deref(), &key, &path),
            Command::JsonHistory { branch, space, key } => {
                self.execute_json_history(branch.as_deref(), space.as_deref(), key)
            }
            Command::JsonExists { branch, space, key } => {
                self.execute_json_exists(branch.as_deref(), space.as_deref(), key)
            }
            Command::JsonBatchExists {
                branch,
                space,
                keys,
            } => self.execute_json_batch_exists(branch.as_deref(), space.as_deref(), keys),
            Command::JsonBatchSet {
                branch,
                space,
                entries,
            } => self.execute_json_batch_set(branch.as_deref(), space.as_deref(), entries),
            Command::JsonBatchGet {
                branch,
                space,
                entries,
            } => self.execute_json_batch_get(branch.as_deref(), space.as_deref(), entries),
            Command::JsonBatchDelete {
                branch,
                space,
                entries,
            } => self.execute_json_batch_delete(branch.as_deref(), space.as_deref(), entries),
            Command::JsonList {
                branch,
                space,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_json_list(
                branch.as_deref(),
                space.as_deref(),
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::JsonScan {
                branch,
                space,
                start,
                limit,
            } => self.execute_json_scan(branch.as_deref(), space.as_deref(), start, limit),
            Command::JsonCount {
                branch,
                space,
                prefix,
                as_of,
                as_of_time,
            } => self.execute_json_count(
                branch.as_deref(),
                space.as_deref(),
                prefix,
                as_of,
                as_of_time,
            ),
            Command::JsonSample {
                branch,
                space,
                prefix,
                count,
            } => self.execute_json_sample(branch.as_deref(), space.as_deref(), prefix, count),
            Command::JsonCreateIndex {
                branch,
                space,
                name,
                field_path,
                index_type,
            } => self.execute_json_create_index(
                branch.as_deref(),
                space.as_deref(),
                name,
                &field_path,
                index_type,
            ),
            Command::JsonDropIndex {
                branch,
                space,
                name,
            } => self.execute_json_drop_index(branch.as_deref(), space.as_deref(), name),
            Command::JsonListIndexes { branch, space } => {
                self.execute_json_list_indexes(branch.as_deref(), space.as_deref())
            }
            Command::VectorCreateCollection {
                branch,
                space,
                collection,
                dimension,
                metric,
                embedding_model,
            } => self.execute_vector_create_collection(
                branch.as_deref(),
                space.as_deref(),
                collection,
                dimension,
                metric,
                embedding_model,
            ),
            Command::VectorDeleteCollection {
                branch,
                space,
                collection,
            } => self.execute_vector_delete_collection(
                branch.as_deref(),
                space.as_deref(),
                collection,
            ),
            Command::VectorListCollections { branch, space } => {
                self.execute_vector_list_collections(branch.as_deref(), space.as_deref())
            }
            Command::VectorCollectionStats {
                branch,
                space,
                collection,
            } => self.execute_vector_collection_stats(
                branch.as_deref(),
                space.as_deref(),
                collection,
            ),
            Command::VectorSetEmbeddingModel {
                branch,
                space,
                collection,
                model,
            } => self.execute_vector_set_embedding_model(
                branch.as_deref(),
                space.as_deref(),
                collection,
                model,
            ),
            Command::VectorCount {
                branch,
                space,
                collection,
                as_of,
                as_of_time,
            } => self.execute_vector_count(
                branch.as_deref(),
                space.as_deref(),
                collection,
                as_of,
                as_of_time,
            ),
            Command::VectorSample {
                branch,
                space,
                collection,
                count,
            } => self.execute_vector_sample(branch.as_deref(), space.as_deref(), collection, count),
            Command::VectorUpsert {
                branch,
                space,
                collection,
                key,
                vector,
                text,
                metadata,
            } => self.execute_vector_upsert(
                branch.as_deref(),
                space.as_deref(),
                collection,
                key,
                vector,
                text,
                metadata,
            ),
            Command::VectorGet {
                branch,
                space,
                collection,
                key,
                as_of,
                as_of_time,
            } => self.execute_vector_get(
                branch.as_deref(),
                space.as_deref(),
                collection,
                key,
                as_of,
                as_of_time,
            ),
            Command::VectorHistory {
                branch,
                space,
                collection,
                key,
            } => self.execute_vector_history(branch.as_deref(), space.as_deref(), collection, key),
            Command::VectorExists {
                branch,
                space,
                collection,
                key,
            } => self.execute_vector_exists(branch.as_deref(), space.as_deref(), collection, key),
            Command::VectorBatchExists {
                branch,
                space,
                collection,
                keys,
            } => self.execute_vector_batch_exists(
                branch.as_deref(),
                space.as_deref(),
                collection,
                keys,
            ),
            Command::VectorListKeys {
                branch,
                space,
                collection,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_vector_list_keys(
                branch.as_deref(),
                space.as_deref(),
                collection,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::VectorScan {
                branch,
                space,
                collection,
                start,
                limit,
            } => self.execute_vector_scan(
                branch.as_deref(),
                space.as_deref(),
                collection,
                start,
                limit,
            ),
            Command::VectorUpdateMetadata {
                branch,
                space,
                collection,
                key,
                patch,
            } => self.execute_vector_update_metadata(
                branch.as_deref(),
                space.as_deref(),
                collection,
                key,
                patch,
            ),
            Command::VectorDelete {
                branch,
                space,
                collection,
                key,
            } => self.execute_vector_delete(branch.as_deref(), space.as_deref(), collection, key),
            Command::VectorDeleteByFilter {
                branch,
                space,
                collection,
                filter,
            } => self.execute_vector_delete_by_filter(
                branch.as_deref(),
                space.as_deref(),
                collection,
                filter,
            ),
            Command::VectorDeleteAll {
                branch,
                space,
                collection,
            } => self.execute_vector_delete_all(branch.as_deref(), space.as_deref(), collection),
            Command::VectorQuery {
                branch,
                space,
                collection,
                query,
                text,
                k,
                filter,
                as_of,
                as_of_time,
            } => self.execute_vector_query(
                branch.as_deref(),
                space.as_deref(),
                collection,
                query,
                text,
                k,
                filter,
                as_of,
                as_of_time,
            ),
            Command::VectorIndexQuery {
                branch,
                space,
                collection,
                query,
                k,
                filter,
                as_of,
                as_of_time,
            } => self.execute_vector_index_query(
                branch.as_deref(),
                space.as_deref(),
                collection,
                query,
                k,
                filter,
                as_of,
                as_of_time,
            ),
            Command::VectorBatchUpsert {
                branch,
                space,
                collection,
                entries,
            } => self.execute_vector_batch_upsert(
                branch.as_deref(),
                space.as_deref(),
                collection,
                entries,
            ),
            Command::VectorBatchGet {
                branch,
                space,
                collection,
                keys,
            } => {
                self.execute_vector_batch_get(branch.as_deref(), space.as_deref(), collection, keys)
            }
            Command::VectorBatchDelete {
                branch,
                space,
                collection,
                keys,
            } => self.execute_vector_batch_delete(
                branch.as_deref(),
                space.as_deref(),
                collection,
                keys,
            ),
            Command::EventBatchAppend {
                branch,
                space,
                entries,
            } => self.execute_event_batch_append(branch.as_deref(), space.as_deref(), entries),
            Command::EventAppend {
                branch,
                space,
                event_type,
                payload,
            } => {
                self.execute_event_append(branch.as_deref(), space.as_deref(), event_type, payload)
            }
            Command::EventGet {
                branch,
                space,
                sequence,
                as_of,
                as_of_time,
            } => self.execute_event_get(
                branch.as_deref(),
                space.as_deref(),
                sequence,
                as_of,
                as_of_time,
            ),
            Command::EventExists {
                branch,
                space,
                sequence,
            } => self.execute_event_exists(branch.as_deref(), space.as_deref(), sequence),
            Command::EventCount {
                branch,
                space,
                as_of,
                as_of_time,
            } => self.execute_event_count(branch.as_deref(), space.as_deref(), as_of, as_of_time),
            Command::EventRange {
                branch,
                space,
                start_seq,
                end_seq,
                limit,
                direction,
                event_type,
            } => self.execute_event_range(
                branch.as_deref(),
                space.as_deref(),
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
            } => self.execute_event_range_by_time(
                branch.as_deref(),
                space.as_deref(),
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
                as_of_time,
            } => self.execute_event_list_types(
                branch.as_deref(),
                space.as_deref(),
                as_of,
                as_of_time,
            ),
            Command::EventList {
                branch,
                space,
                event_type,
                limit,
                after_sequence,
                as_of,
                as_of_time,
            } => self.execute_event_list(
                branch.as_deref(),
                space.as_deref(),
                event_type,
                limit,
                after_sequence,
                as_of,
                as_of_time,
            ),
            Command::EventVerifyChain { branch, space } => {
                self.execute_event_verify_chain(branch.as_deref(), space.as_deref())
            }
            Command::GraphCreate {
                branch,
                space,
                graph,
            } => self.execute_graph_create(branch.as_deref(), space.as_deref(), graph),
            Command::GraphDelete {
                branch,
                space,
                graph,
            } => self.execute_graph_delete(branch.as_deref(), space.as_deref(), graph),
            Command::GraphList {
                branch,
                space,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_graph_list(
                branch.as_deref(),
                space.as_deref(),
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::GraphGetMeta {
                branch,
                space,
                graph,
                as_of,
                as_of_time,
            } => self.execute_graph_get_meta(
                branch.as_deref(),
                space.as_deref(),
                graph,
                as_of,
                as_of_time,
            ),
            Command::GraphAddNode {
                branch,
                space,
                graph,
                node_id,
                properties,
                binding,
                object_type,
            } => self.execute_graph_add_node(
                branch.as_deref(),
                space.as_deref(),
                graph,
                node_id,
                properties,
                binding,
                object_type,
            ),
            Command::GraphGetNode {
                branch,
                space,
                graph,
                node_id,
                as_of,
                as_of_time,
            } => self.execute_graph_get_node(
                branch.as_deref(),
                space.as_deref(),
                graph,
                node_id,
                as_of,
                as_of_time,
            ),
            Command::GraphRemoveNode {
                branch,
                space,
                graph,
                node_id,
            } => {
                self.execute_graph_remove_node(branch.as_deref(), space.as_deref(), graph, node_id)
            }
            Command::GraphListNodes {
                branch,
                space,
                graph,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_graph_list_nodes(
                branch.as_deref(),
                space.as_deref(),
                graph,
                prefix,
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::GraphSample {
                branch,
                space,
                graph,
                count,
            } => self.execute_graph_sample(branch.as_deref(), space.as_deref(), graph, count),
            Command::GraphAddEdge {
                branch,
                space,
                graph,
                src,
                edge_type,
                dst,
                weight,
                properties,
            } => self.execute_graph_add_edge(
                branch.as_deref(),
                space.as_deref(),
                graph,
                src,
                edge_type,
                dst,
                weight,
                properties,
            ),
            Command::GraphGetEdge {
                branch,
                space,
                graph,
                src,
                edge_type,
                dst,
                as_of,
                as_of_time,
            } => self.execute_graph_get_edge(
                branch.as_deref(),
                space.as_deref(),
                graph,
                src,
                edge_type,
                dst,
                as_of,
                as_of_time,
            ),
            Command::GraphRemoveEdge {
                branch,
                space,
                graph,
                src,
                edge_type,
                dst,
            } => self.execute_graph_remove_edge(
                branch.as_deref(),
                space.as_deref(),
                graph,
                src,
                edge_type,
                dst,
            ),
            Command::GraphNeighbors {
                branch,
                space,
                graph,
                node_id,
                direction,
                edge_type,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_graph_neighbors(
                branch.as_deref(),
                space.as_deref(),
                graph,
                node_id,
                direction,
                edge_type,
                cursor.as_deref(),
                limit,
                as_of,
                as_of_time,
            ),
            Command::GraphBindingsForEntity {
                branch,
                space,
                target,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_graph_bindings_for_entity(
                branch.as_deref(),
                space.as_deref(),
                target,
                cursor.as_deref(),
                limit,
                as_of,
                as_of_time,
            ),
            Command::GraphBatchWrite {
                branch,
                space,
                graph,
                operations,
            } => self.execute_graph_batch_write(
                branch.as_deref(),
                space.as_deref(),
                graph,
                operations,
            ),
            Command::GraphDefineObjectType {
                branch,
                space,
                graph,
                name,
                properties,
            } => self.execute_graph_define_object_type(
                branch.as_deref(),
                space.as_deref(),
                graph,
                name,
                properties,
            ),
            Command::GraphDefineLinkType {
                branch,
                space,
                graph,
                name,
                source,
                target,
                cardinality,
                properties,
            } => self.execute_graph_define_link_type(
                branch.as_deref(),
                space.as_deref(),
                graph,
                name,
                source,
                target,
                cardinality,
                properties,
            ),
            Command::GraphDeleteObjectType {
                branch,
                space,
                graph,
                name,
            } => self.execute_graph_delete_object_type(
                branch.as_deref(),
                space.as_deref(),
                graph,
                name,
            ),
            Command::GraphDeleteLinkType {
                branch,
                space,
                graph,
                name,
            } => self.execute_graph_delete_link_type(
                branch.as_deref(),
                space.as_deref(),
                graph,
                name,
            ),
            Command::GraphFreezeOntology {
                branch,
                space,
                graph,
            } => self.execute_graph_freeze_ontology(branch.as_deref(), space.as_deref(), graph),
            Command::GraphGetOntology {
                branch,
                space,
                graph,
                as_of,
                as_of_time,
            } => self.execute_graph_get_ontology(
                branch.as_deref(),
                space.as_deref(),
                graph,
                as_of,
                as_of_time,
            ),
            Command::GraphOntologySummary {
                branch,
                space,
                graph,
                as_of,
                as_of_time,
            } => self.execute_graph_ontology_summary(
                branch.as_deref(),
                space.as_deref(),
                graph,
                as_of,
                as_of_time,
            ),
            Command::GraphNodesByType {
                branch,
                space,
                graph,
                object_type,
                cursor,
                limit,
                as_of,
                as_of_time,
            } => self.execute_graph_nodes_by_type(
                branch.as_deref(),
                space.as_deref(),
                graph,
                object_type,
                cursor,
                limit,
                as_of,
                as_of_time,
            ),
            Command::GraphWcc {
                branch,
                space,
                graph,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_wcc(
                branch.as_deref(),
                space.as_deref(),
                graph,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphLcc {
                branch,
                space,
                graph,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_lcc(
                branch.as_deref(),
                space.as_deref(),
                graph,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphSssp {
                branch,
                space,
                graph,
                source,
                direction,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_sssp(
                branch.as_deref(),
                space.as_deref(),
                graph,
                source,
                direction,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphPagerank {
                branch,
                space,
                graph,
                damping,
                max_iterations,
                tolerance,
                personalization,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_pagerank(
                branch.as_deref(),
                space.as_deref(),
                graph,
                damping,
                max_iterations,
                tolerance,
                personalization,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphCdlp {
                branch,
                space,
                graph,
                max_iterations,
                direction,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_cdlp(
                branch.as_deref(),
                space.as_deref(),
                graph,
                max_iterations,
                direction,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphBfs {
                branch,
                space,
                graph,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
                budget,
                as_of,
                as_of_time,
            } => self.execute_graph_bfs(
                branch.as_deref(),
                space.as_deref(),
                graph,
                start,
                max_depth,
                max_nodes,
                edge_types,
                direction,
                budget,
                as_of,
                as_of_time,
            ),
            Command::GraphBulkInsert {
                branch,
                space,
                graph,
                nodes,
                edges,
                chunk_size,
            } => self.execute_graph_bulk_insert(
                branch.as_deref(),
                space.as_deref(),
                graph,
                nodes,
                edges,
                chunk_size,
            ),
            Command::GraphApplyDeletePolicy {
                branch,
                space,
                target,
                policy,
            } => self.execute_graph_apply_delete_policy(
                branch.as_deref(),
                space.as_deref(),
                target,
                policy,
            ),
            Command::ArrowImport {
                branch,
                space,
                file_path,
                format,
                target,
                key_column,
                value_column,
                collection,
                graph,
            } => self.execute_arrow_import(
                branch.as_deref(),
                space.as_deref(),
                file_path,
                format,
                target,
                key_column.as_deref(),
                value_column.as_deref(),
                collection.as_deref(),
                graph.as_deref(),
            ),
            Command::ArrowExport {
                branch,
                space,
                primitive,
                format,
                path,
                prefix,
                limit,
                collection,
                graph,
                event_type,
            } => self.execute_arrow_export(
                branch.as_deref(),
                space.as_deref(),
                primitive,
                format,
                path,
                prefix.as_deref(),
                limit,
                collection,
                graph,
                event_type,
            ),
            #[cfg(feature = "inference")]
            Command::InferenceModelsList {} => Ok(Output::InferenceModels {
                items: self.inference.list_models(),
                page: PageInfo::terminal(),
            }),
            #[cfg(feature = "inference")]
            Command::InferenceModelsLocal {} => Ok(Output::InferenceModels {
                items: self.inference.list_local_models(),
                page: PageInfo::terminal(),
            }),
            #[cfg(feature = "inference")]
            Command::InferenceModelsPull { model } => self
                .inference
                .pull_model(&model)
                .map(Output::InferenceModelPulled)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceModelCapability { model } => self
                .inference
                .capability(&model)
                .map(Output::InferenceCapability)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceGenerate { model, request } => self
                .inference
                .chat(&model, &request)
                .map(Output::InferenceGeneration)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceTokenize {
                model,
                text,
                add_special,
            } => self
                .inference
                .tokenize(&model, &text, add_special)
                .map(Output::InferenceTokenIds)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceDetokenize { model, ids } => self
                .inference
                .detokenize(&model, &ids)
                .map(Output::InferenceText)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceEmbed { model, request } => self
                .inference
                .embeddings(&model, &request)
                .map(Output::InferenceEmbeddings)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceRank { model, request } => self
                .inference
                .rank(&model, &request)
                .map(Output::InferenceRanking)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceUnload { model } => self
                .inference
                .unload(model.as_deref())
                .map(|unloaded| {
                    Output::InferenceUnloadResult(crate::output::InferenceUnloadResult { unloaded })
                })
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceCacheStatus {} => self
                .inference
                .cache_status()
                .map(Output::InferenceCacheStatus)
                .map_err(ExecutorError::from),
            #[cfg(feature = "inference")]
            Command::InferenceStatus {} => Ok(Output::InferenceStatus(self.inference.status())),
        }
    }
}
