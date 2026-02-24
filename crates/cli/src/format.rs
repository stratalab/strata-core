//! Output → human/json/raw string formatting.
//!
//! Three modes:
//! - **Human** (default on TTY): Redis-style, e.g. `"value"`, `(integer) 42`, `(nil)`
//! - **JSON** (`--json`): `serde_json::to_string_pretty`
//! - **Raw** (`--raw`): Bare values, no quotes, no type prefixes

use strata_executor::{
    BranchDiffResult, Error, ForkInfo, MergeInfo, Output, Value, VersionedValue,
};

/// Output formatting mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
    Raw,
}

/// Format a successful output.
pub fn format_output(output: &Output, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => format_json(output),
        OutputMode::Raw => format_raw(output),
        OutputMode::Human => format_human(output),
    }
}

/// Format an error.
pub fn format_error(err: &Error, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => serde_json::to_string_pretty(&serde_json::json!({
            "error": format!("{}", err)
        }))
        .unwrap_or_else(|_| format!("{{\"error\": \"{}\"}}", err)),
        OutputMode::Raw => format!("{}", err),
        OutputMode::Human => format!("(error) {}", err),
    }
}

/// Format output with version info when --with-version is specified.
pub fn format_versioned_output(output: &Output, mode: OutputMode, with_version: bool) -> String {
    if !with_version {
        return format_output(output, mode);
    }

    match output {
        Output::MaybeVersioned(Some(vv)) => format_versioned_value(vv, mode),
        Output::MaybeVersioned(None) => format_output(output, mode),
        _ => format_output(output, mode),
    }
}

/// Format a single versioned value with version info.
fn format_versioned_value(vv: &VersionedValue, mode: OutputMode) -> String {
    match mode {
        OutputMode::Human => {
            format!(
                "{} (v{}, ts={})",
                format_value_human(&vv.value),
                vv.version,
                vv.timestamp
            )
        }
        OutputMode::Json => serde_json::to_string_pretty(&serde_json::json!({
            "value": vv.value,
            "version": vv.version,
            "timestamp": vv.timestamp
        }))
        .unwrap(),
        OutputMode::Raw => format_value_raw(&vv.value),
    }
}

/// Format multiple outputs (for multi-key operations).
pub fn format_multi_output(outputs: &[Output], mode: OutputMode) -> String {
    outputs
        .iter()
        .map(|o| format_output(o, mode))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format multiple versioned outputs.
pub fn format_multi_versioned_output(
    outputs: &[Output],
    mode: OutputMode,
    with_version: bool,
) -> String {
    outputs
        .iter()
        .map(|o| format_versioned_output(o, mode, with_version))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format branch fork info.
pub fn format_fork_info(info: &ForkInfo, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => serde_json::to_string_pretty(&serde_json::json!({
            "source": info.source,
            "destination": info.destination,
            "keys_copied": info.keys_copied,
            "spaces_copied": info.spaces_copied,
        }))
        .unwrap(),
        OutputMode::Raw => format!("{}", info.keys_copied),
        OutputMode::Human => format!(
            "Forked \"{}\" -> \"{}\" ({} keys, {} spaces)",
            info.source, info.destination, info.keys_copied, info.spaces_copied
        ),
    }
}

/// Format branch diff result.
pub fn format_diff(diff: &BranchDiffResult, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => serde_json::to_string_pretty(&serde_json::json!({
            "branch_a": diff.branch_a,
            "branch_b": diff.branch_b,
            "summary": {
                "total_added": diff.summary.total_added,
                "total_removed": diff.summary.total_removed,
                "total_modified": diff.summary.total_modified,
            },
            "spaces": diff.spaces.iter().map(|sd| serde_json::json!({
                "space": sd.space,
                "added": sd.added.len(),
                "removed": sd.removed.len(),
                "modified": sd.modified.len(),
            })).collect::<Vec<_>>(),
        }))
        .unwrap(),
        OutputMode::Raw => format!(
            "{}\t{}\t{}",
            diff.summary.total_added, diff.summary.total_removed, diff.summary.total_modified
        ),
        OutputMode::Human => {
            let mut lines = Vec::new();
            lines.push(format!(
                "Branch \"{}\" vs \"{}\":",
                diff.branch_a, diff.branch_b
            ));
            lines.push(format!(
                "  +{} added, -{} removed, ~{} modified",
                diff.summary.total_added, diff.summary.total_removed, diff.summary.total_modified
            ));
            for sd in &diff.spaces {
                if !sd.added.is_empty() || !sd.removed.is_empty() || !sd.modified.is_empty() {
                    lines.push(format!("  Space \"{}\":", sd.space));
                    for entry in &sd.added {
                        lines.push(format!("    + {} ({})", entry.key, entry.primitive));
                    }
                    for entry in &sd.removed {
                        lines.push(format!("    - {} ({})", entry.key, entry.primitive));
                    }
                    for entry in &sd.modified {
                        lines.push(format!("    ~ {} ({})", entry.key, entry.primitive));
                    }
                }
            }
            lines.join("\n")
        }
    }
}

/// Format merge info.
pub fn format_merge_info(info: &MergeInfo, mode: OutputMode) -> String {
    match mode {
        OutputMode::Json => serde_json::to_string_pretty(&serde_json::json!({
            "source": info.source,
            "target": info.target,
            "keys_applied": info.keys_applied,
            "conflicts": info.conflicts.len(),
            "spaces_merged": info.spaces_merged,
        }))
        .unwrap(),
        OutputMode::Raw => format!("{}", info.keys_applied),
        OutputMode::Human => {
            let conflict_note = if info.conflicts.is_empty() {
                String::new()
            } else {
                format!(", {} conflicts resolved", info.conflicts.len())
            };
            format!(
                "Merged \"{}\" -> \"{}\" ({} keys, {} spaces{})",
                info.source, info.target, info.keys_applied, info.spaces_merged, conflict_note
            )
        }
    }
}

// =========================================================================
// JSON mode
// =========================================================================

fn format_json(output: &Output) -> String {
    serde_json::to_string_pretty(output).unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
}

// =========================================================================
// Raw mode
// =========================================================================

fn format_raw(output: &Output) -> String {
    match output {
        Output::Unit => String::new(),
        Output::Maybe(None) => String::new(),
        Output::Maybe(Some(v)) => format_value_raw(v),
        Output::MaybeVersioned(None) => String::new(),
        Output::MaybeVersioned(Some(vv)) => format_value_raw(&vv.value),
        Output::MaybeVersion(None) => String::new(),
        Output::MaybeVersion(Some(v)) => v.to_string(),
        Output::Version(v) => v.to_string(),
        Output::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Output::Uint(n) => n.to_string(),
        Output::VersionedValues(vals) => vals
            .iter()
            .map(|vv| format_value_raw(&vv.value))
            .collect::<Vec<_>>()
            .join("\n"),
        Output::VersionHistory(None) => String::new(),
        Output::VersionHistory(Some(vals)) => vals
            .iter()
            .map(|vv| format_value_raw(&vv.value))
            .collect::<Vec<_>>()
            .join("\n"),
        Output::Keys(keys) => keys.join("\n"),
        Output::JsonListResult { keys, .. } => keys.join("\n"),
        Output::VectorMatches(matches) => matches
            .iter()
            .map(|m| format!("{}\t{}", m.key, m.score))
            .collect::<Vec<_>>()
            .join("\n"),
        Output::VectorData(None) => String::new(),
        Output::VectorData(Some(vd)) => format!("{:?}", vd.data.embedding),
        Output::VectorCollectionList(colls) => colls
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join("\n"),
        Output::Versions(vs) => vs
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        Output::MaybeBranchInfo(None) => String::new(),
        Output::MaybeBranchInfo(Some(bi)) => bi.info.id.0.clone(),
        Output::BranchInfoList(branches) => branches
            .iter()
            .map(|b| b.info.id.0.clone())
            .collect::<Vec<_>>()
            .join("\n"),
        Output::BranchWithVersion { info, version } => {
            format!("{}\t{}", info.id, version)
        }
        Output::BranchForked(info) => {
            format!(
                "{}\t{}\t{}",
                info.source, info.destination, info.keys_copied
            )
        }
        Output::BranchDiff(result) => {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                result.branch_a,
                result.branch_b,
                result.summary.total_added,
                result.summary.total_removed,
                result.summary.total_modified
            )
        }
        Output::BranchMerged(info) => {
            format!("{}\t{}\t{}", info.source, info.target, info.keys_applied)
        }
        Output::Config(_) | Output::DurabilityCounters(_) => {
            serde_json::to_string(output).unwrap_or_default()
        }
        Output::TxnInfo(None) => String::new(),
        Output::TxnInfo(Some(info)) => info.id.clone(),
        Output::TxnBegun => "OK".to_string(),
        Output::TxnCommitted { version } => version.to_string(),
        Output::TxnAborted => "OK".to_string(),
        Output::DatabaseInfo(info) => {
            format!(
                "{}\t{}\t{}\t{}",
                info.version, info.uptime_secs, info.branch_count, info.total_keys
            )
        }
        Output::Pong { version } => version.clone(),
        Output::SearchResults(hits) => hits
            .iter()
            .map(|h| format!("{}\t{}\t{}", h.entity, h.primitive, h.score))
            .collect::<Vec<_>>()
            .join("\n"),
        Output::SpaceList(spaces) => spaces.join("\n"),
        Output::BranchExported(r) => format!("{}\t{}", r.path, r.entry_count),
        Output::BranchImported(r) => format!("{}\t{}", r.branch_id, r.keys_written),
        Output::BundleValidated(r) => {
            if r.checksums_valid {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Output::TimeRange {
            oldest_ts,
            latest_ts,
        } => match (oldest_ts, latest_ts) {
            (Some(o), Some(l)) => format!("{}\t{}", o, l),
            (Some(o), None) => format!("{}\t", o),
            (None, Some(l)) => format!("\t{}", l),
            (None, None) => String::new(),
        },
        Output::EmbedStatus(info) => {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                info.auto_embed,
                info.pending,
                info.total_queued,
                info.total_embedded,
                info.total_failed,
                info.scheduler_queue_depth
            )
        }
        Output::BatchResults(results) => results
            .iter()
            .map(|r| match (&r.version, &r.error) {
                (Some(v), _) => v.to_string(),
                (_, Some(e)) => format!("ERR:{}", e),
                _ => "ERR".to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Output::Embedding(vec) => vec
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(" "),
        Output::Embeddings(vecs) => vecs
            .iter()
            .map(|v| {
                v.iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Output::ModelsList(models) => models
            .iter()
            .map(|m| {
                let local = if m.is_local { "local" } else { "remote" };
                format!("{}\t{}\t{}\t{}", m.name, m.task, m.architecture, local)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Output::ModelsPulled { name, path } => format!("{}\t{}", name, path),
        Output::Generated(r) => r.text.clone(),
        Output::TokenIds(r) => r
            .ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(" "),
        Output::Text(t) => t.clone(),
        Output::GraphNeighbors(hits) => serde_json::to_string(&hits).unwrap_or_default(),
        Output::GraphBfs(result) => serde_json::to_string(&result).unwrap_or_default(),
        Output::GraphBulkInsertResult {
            nodes_inserted,
            edges_inserted,
        } => {
            format!(
                "{{\"nodes_inserted\":{},\"edges_inserted\":{}}}",
                nodes_inserted, edges_inserted
            )
        }
        Output::ConfigValue(None) => String::new(),
        Output::ConfigValue(Some(v)) => v.clone(),
    }
}

fn format_value_raw(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => base64_encode(b),
        Value::Array(arr) => arr
            .iter()
            .map(format_value_raw)
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(obj) => serde_json::to_string(obj).unwrap_or_default(),
    }
}

// =========================================================================
// Human mode
// =========================================================================

fn format_human(output: &Output) -> String {
    match output {
        Output::Unit => "OK".to_string(),
        Output::Maybe(None) => "(nil)".to_string(),
        Output::Maybe(Some(v)) => format_value_human(v),
        Output::MaybeVersioned(None) => "(nil)".to_string(),
        Output::MaybeVersioned(Some(vv)) => format_value_human(&vv.value),
        Output::MaybeVersion(None) => "(nil)".to_string(),
        Output::MaybeVersion(Some(v)) => format!("(version) {}", v),
        Output::Version(v) => format!("(version) {}", v),
        Output::Bool(b) => format!("(boolean) {}", b),
        Output::Uint(n) => format!("(integer) {}", n),
        Output::VersionedValues(vals) => {
            if vals.is_empty() {
                "(empty list)".to_string()
            } else {
                vals.iter()
                    .enumerate()
                    .map(|(i, vv)| {
                        format!(
                            "{}) {} (v{})",
                            i + 1,
                            format_value_human(&vv.value),
                            vv.version
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::VersionHistory(None) => "(nil)".to_string(),
        Output::VersionHistory(Some(vals)) => {
            if vals.is_empty() {
                "(empty list)".to_string()
            } else {
                // Show newest first
                let mut sorted = vals.clone();
                sorted.sort_by(|a, b| b.version.cmp(&a.version));
                sorted
                    .iter()
                    .enumerate()
                    .map(|(i, vv)| {
                        format!(
                            "{}) v{}: {}",
                            i + 1,
                            vv.version,
                            format_value_human(&vv.value)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::Keys(keys) => format_string_list(keys),
        Output::JsonListResult { keys, cursor } => {
            let mut out = format_string_list(keys);
            if let Some(c) = cursor {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&format!("(cursor) {}", c));
            }
            out
        }
        Output::VectorMatches(matches) => {
            if matches.is_empty() {
                "(empty list)".to_string()
            } else {
                matches
                    .iter()
                    .enumerate()
                    .map(|(i, m)| format!("{}) \"{}\" (score: {:.3})", i + 1, m.key, m.score))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::VectorData(None) => "(nil)".to_string(),
        Output::VectorData(Some(vd)) => {
            let mut lines = vec![
                format!("key: \"{}\"", vd.key),
                format!("embedding: {:?}", vd.data.embedding),
                format!("version: {}", vd.version),
            ];
            if let Some(meta) = &vd.data.metadata {
                lines.push(format!("metadata: {}", format_value_human(meta)));
            }
            lines.join("\n")
        }
        Output::VectorCollectionList(colls) => {
            if colls.is_empty() {
                "(empty list)".to_string()
            } else {
                colls
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        format!(
                            "{}) \"{}\" (dim: {}, metric: {:?}, count: {})",
                            i + 1,
                            c.name,
                            c.dimension,
                            c.metric,
                            c.count
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::Versions(vs) => {
            if vs.is_empty() {
                "(empty list)".to_string()
            } else {
                vs.iter()
                    .enumerate()
                    .map(|(i, v)| format!("{}) (version) {}", i + 1, v))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::MaybeBranchInfo(None) => "(nil)".to_string(),
        Output::MaybeBranchInfo(Some(bi)) => {
            let mut lines = vec![
                format!("id: \"{}\"", bi.info.id),
                format!("status: {:?}", bi.info.status),
                format!("version: {}", bi.version),
                format!("created_at: {}", bi.info.created_at),
                format!("updated_at: {}", bi.info.updated_at),
            ];
            if let Some(parent) = &bi.info.parent_id {
                lines.push(format!("parent: \"{}\"", parent));
            }
            lines.join("\n")
        }
        Output::BranchInfoList(branches) => {
            if branches.is_empty() {
                "(empty list)".to_string()
            } else {
                branches
                    .iter()
                    .enumerate()
                    .map(|(i, b)| format!("{}) \"{}\"", i + 1, b.info.id))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::BranchWithVersion { info, version } => {
            format!("Branch \"{}\" created (v{})", info.id, version)
        }
        Output::BranchForked(info) => {
            format!(
                "Forked \"{}\" → \"{}\" ({} keys, {} spaces)",
                info.source, info.destination, info.keys_copied, info.spaces_copied
            )
        }
        Output::BranchDiff(result) => {
            let mut lines = vec![format!(
                "Diff: \"{}\" vs \"{}\"",
                result.branch_a, result.branch_b
            )];
            lines.push(format!(
                "  added: {}, removed: {}, modified: {}",
                result.summary.total_added,
                result.summary.total_removed,
                result.summary.total_modified
            ));
            for space in &result.spaces {
                lines.push(format!(
                    "  space \"{}\": +{} -{} ~{}",
                    space.space,
                    space.added.len(),
                    space.removed.len(),
                    space.modified.len()
                ));
            }
            lines.join("\n")
        }
        Output::BranchMerged(info) => {
            let conflict_msg = if info.conflicts.is_empty() {
                String::new()
            } else {
                format!(", {} conflicts", info.conflicts.len())
            };
            format!(
                "Merged \"{}\" → \"{}\" ({} keys applied, {} spaces{})",
                info.source, info.target, info.keys_applied, info.spaces_merged, conflict_msg
            )
        }
        Output::Config(cfg) => {
            serde_json::to_string_pretty(cfg).unwrap_or_else(|_| format!("{:?}", cfg))
        }
        Output::DurabilityCounters(counters) => {
            format!(
                "wal_appends: {}\nsync_calls: {}\nbytes_written: {}\nsync_nanos: {}",
                counters.wal_appends,
                counters.sync_calls,
                counters.bytes_written,
                counters.sync_nanos
            )
        }
        Output::TxnInfo(None) => "(nil)".to_string(),
        Output::TxnInfo(Some(info)) => {
            format!(
                "id: {}\nstatus: {:?}\nstarted_at: {}",
                info.id, info.status, info.started_at
            )
        }
        Output::TxnBegun => "OK".to_string(),
        Output::TxnCommitted { version } => format!("Committed (v{})", version),
        Output::TxnAborted => "OK".to_string(),
        Output::DatabaseInfo(info) => {
            format!(
                "version: {}\nuptime_secs: {}\nbranches: {}\ntotal_keys: {}",
                info.version, info.uptime_secs, info.branch_count, info.total_keys
            )
        }
        Output::Pong { version } => format!("PONG {}", version),
        Output::SearchResults(hits) => {
            if hits.is_empty() {
                "(empty list)".to_string()
            } else {
                hits.iter()
                    .enumerate()
                    .map(|(i, h)| {
                        let snippet = h
                            .snippet
                            .as_deref()
                            .map(|s| format!(" - {}", s))
                            .unwrap_or_default();
                        format!(
                            "{}) \"{}\" [{}] (score: {:.3}){}",
                            i + 1,
                            h.entity,
                            h.primitive,
                            h.score,
                            snippet
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::SpaceList(spaces) => format_string_list(spaces),
        Output::BranchExported(r) => {
            format!(
                "Exported branch \"{}\" to {} ({} entries, {} bytes)",
                r.branch_id, r.path, r.entry_count, r.bundle_size
            )
        }
        Output::BranchImported(r) => {
            format!(
                "Imported branch \"{}\" ({} transactions, {} keys)",
                r.branch_id, r.transactions_applied, r.keys_written
            )
        }
        Output::BundleValidated(r) => {
            format!(
                "Bundle valid: branch=\"{}\", format_version={}, entries={}, checksums={}",
                r.branch_id,
                r.format_version,
                r.entry_count,
                if r.checksums_valid { "OK" } else { "FAILED" }
            )
        }
        Output::TimeRange {
            oldest_ts,
            latest_ts,
        } => match (oldest_ts, latest_ts) {
            (Some(o), Some(l)) => format!("oldest: {}  latest: {}", o, l),
            (Some(o), None) => format!("oldest: {}  latest: (none)", o),
            (None, Some(l)) => format!("oldest: (none)  latest: {}", l),
            (None, None) => "(no data)".to_string(),
        },
        Output::EmbedStatus(info) => {
            format!(
                "auto_embed: {}\npending: {}\ntotal_queued: {}\ntotal_embedded: {}\ntotal_failed: {}\nscheduler_queue_depth: {}\nscheduler_active_tasks: {}",
                info.auto_embed,
                info.pending,
                info.total_queued,
                info.total_embedded,
                info.total_failed,
                info.scheduler_queue_depth,
                info.scheduler_active_tasks
            )
        }
        Output::BatchResults(results) => {
            if results.is_empty() {
                "(empty list)".to_string()
            } else {
                results
                    .iter()
                    .enumerate()
                    .map(|(i, r)| match (&r.version, &r.error) {
                        (Some(v), _) => format!("{}) OK (v{})", i + 1, v),
                        (_, Some(e)) => format!("{}) ERR: {}", i + 1, e),
                        _ => format!("{}) ERR", i + 1),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::Embedding(vec) => {
            if vec.is_empty() {
                "(empty embedding)".to_string()
            } else {
                let preview: Vec<String> =
                    vec.iter().take(5).map(|v| format!("{:.6}", v)).collect();
                format!(
                    "(embedding) [{} dimensions] [{}{}]",
                    vec.len(),
                    preview.join(", "),
                    if vec.len() > 5 { ", ..." } else { "" }
                )
            }
        }
        Output::Embeddings(vecs) => {
            if vecs.is_empty() {
                "(empty list)".to_string()
            } else {
                vecs.iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let preview: Vec<String> =
                            v.iter().take(5).map(|f| format!("{:.6}", f)).collect();
                        format!(
                            "{}) [{} dimensions] [{}{}]",
                            i + 1,
                            v.len(),
                            preview.join(", "),
                            if v.len() > 5 { ", ..." } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::ModelsList(models) => {
            if models.is_empty() {
                "(empty list)".to_string()
            } else {
                models
                    .iter()
                    .enumerate()
                    .map(|(i, m)| {
                        let local_indicator = if m.is_local { " [local]" } else { "" };
                        let dim_info = if m.embedding_dim > 0 {
                            format!(", dim: {}", m.embedding_dim)
                        } else {
                            String::new()
                        };
                        format!(
                            "{}) \"{}\" ({}, {}{}){}",
                            i + 1,
                            m.name,
                            m.task,
                            m.architecture,
                            dim_info,
                            local_indicator
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Output::ModelsPulled { name, path } => {
            format!("Model \"{}\" downloaded to {}", name, path)
        }
        Output::Generated(r) => {
            format!(
                "(generated) [{}, stop: {}, prompt: {} tok, completion: {} tok]\n{}",
                r.model, r.stop_reason, r.prompt_tokens, r.completion_tokens, r.text
            )
        }
        Output::TokenIds(r) => {
            format!(
                "(tokens) [{}, {} tokens] [{}]",
                r.model,
                r.count,
                r.ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Output::Text(t) => format!("\"{}\"", t),
        Output::GraphNeighbors(hits) => {
            format!(
                "(neighbors) {} result(s)\n{}",
                hits.len(),
                serde_json::to_string_pretty(&hits).unwrap_or_default()
            )
        }
        Output::GraphBfs(result) => {
            format!(
                "(bfs) {} node(s) visited\n{}",
                result.visited.len(),
                serde_json::to_string_pretty(&result).unwrap_or_default()
            )
        }
        Output::GraphBulkInsertResult {
            nodes_inserted,
            edges_inserted,
        } => {
            format!(
                "(bulk insert) {} node(s), {} edge(s) inserted",
                nodes_inserted, edges_inserted
            )
        }
        Output::ConfigValue(None) => "(nil)".to_string(),
        Output::ConfigValue(Some(v)) => format!("\"{}\"", v),
    }
}

fn format_value_human(v: &Value) -> String {
    match v {
        Value::Null => "(nil)".to_string(),
        Value::Bool(b) => format!("(boolean) {}", b),
        Value::Int(i) => format!("(integer) {}", i),
        Value::Float(f) => format!("(float) {}", f),
        Value::String(s) => format!("\"{}\"", s),
        Value::Bytes(b) => format!("(bytes) {}", base64_encode(b)),
        Value::Array(arr) => {
            if arr.is_empty() {
                "(empty array)".to_string()
            } else {
                arr.iter()
                    .enumerate()
                    .map(|(i, v)| format!("{}) {}", i + 1, format_value_human(v)))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
        Value::Object(obj) => {
            if obj.is_empty() {
                "(empty object)".to_string()
            } else {
                let mut entries: Vec<_> = obj.iter().collect();
                entries.sort_by_key(|(k, _): &(&String, &Value)| (*k).clone());
                entries
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, format_value_human(v)))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        }
    }
}

fn format_string_list(items: &[String]) -> String {
    if items.is_empty() {
        "(empty list)".to_string()
    } else {
        items
            .iter()
            .enumerate()
            .map(|(i, s)| format!("{}) \"{}\"", i + 1, s))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn base64_encode(data: &[u8]) -> String {
    // Simple base64 encoding without external dependency
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_executor::VersionedValue;

    #[test]
    fn test_format_unit() {
        assert_eq!(format_output(&Output::Unit, OutputMode::Human), "OK");
        assert_eq!(format_output(&Output::Unit, OutputMode::Raw), "");
    }

    #[test]
    fn test_format_version() {
        assert_eq!(
            format_output(&Output::Version(3), OutputMode::Human),
            "(version) 3"
        );
        assert_eq!(format_output(&Output::Version(3), OutputMode::Raw), "3");
    }

    #[test]
    fn test_format_bool() {
        assert_eq!(
            format_output(&Output::Bool(true), OutputMode::Human),
            "(boolean) true"
        );
        assert_eq!(format_output(&Output::Bool(true), OutputMode::Raw), "1");
    }

    #[test]
    fn test_format_nil() {
        assert_eq!(
            format_output(&Output::MaybeVersioned(None), OutputMode::Human),
            "(nil)"
        );
        assert_eq!(
            format_output(&Output::MaybeVersioned(None), OutputMode::Raw),
            ""
        );
    }

    #[test]
    fn test_format_versioned_value() {
        let vv = VersionedValue {
            value: Value::String("hello".into()),
            version: 1,
            timestamp: 0,
        };
        assert_eq!(
            format_output(&Output::MaybeVersioned(Some(vv)), OutputMode::Human),
            "\"hello\""
        );
    }

    #[test]
    fn test_format_keys() {
        let keys = vec!["key1".to_string(), "key2".to_string()];
        assert_eq!(
            format_output(&Output::Keys(keys.clone()), OutputMode::Human),
            "1) \"key1\"\n2) \"key2\""
        );
        assert_eq!(
            format_output(&Output::Keys(keys), OutputMode::Raw),
            "key1\nkey2"
        );
    }

    #[test]
    fn test_format_pong() {
        let pong = Output::Pong {
            version: "0.6.0".to_string(),
        };
        assert_eq!(format_output(&pong, OutputMode::Human), "PONG 0.6.0");
    }
}
