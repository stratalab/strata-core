use std::sync::Arc;

use strata_core::limits::Limits;
use strata_core::primitives::json::{JsonPath, JsonValue};
use strata_core::{
    validate_space_name, StrataError, StrataResult, Value, VersionedValue as CoreVersionedValue,
};
use strata_engine::{
    BranchStatus as EngineBranchStatus, Database, EventLog as PrimitiveEventLog,
    JsonStore as PrimitiveJsonStore, KVStore as PrimitiveKVStore,
    SpaceIndex as PrimitiveSpaceIndex,
};
use strata_graph::PrimitiveGraphStore;
use strata_security::AccessMode;
use strata_vector::VectorStore as PrimitiveVectorStore;

use crate::{
    BranchId, BranchStatus, Command, DatabaseInfo, DistanceMetric, Error, FilterOp, MetadataFilter,
    Result, VersionedValue,
};

const RESERVED_KEY_PREFIX: &str = "_strata/";
const RESERVED_BRANCH_PREFIX: &str = "_system";

#[derive(Clone)]
pub(crate) struct Primitives {
    pub(crate) db: Arc<Database>,
    pub(crate) kv: PrimitiveKVStore,
    pub(crate) json: PrimitiveJsonStore,
    pub(crate) event: PrimitiveEventLog,
    pub(crate) vector: PrimitiveVectorStore,
    pub(crate) space: PrimitiveSpaceIndex,
    pub(crate) graph: PrimitiveGraphStore,
    pub(crate) limits: Limits,
}

impl Primitives {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self {
            kv: PrimitiveKVStore::new(db.clone()),
            json: PrimitiveJsonStore::new(db.clone()),
            event: PrimitiveEventLog::new(db.clone()),
            vector: PrimitiveVectorStore::new(db.clone()),
            space: PrimitiveSpaceIndex::new(db.clone()),
            graph: PrimitiveGraphStore::new(db.clone()),
            db,
            limits: Limits::default(),
        }
    }
}

pub(crate) fn to_core_branch_id(branch: &BranchId) -> Result<strata_core::types::BranchId> {
    if strata_core::branch::aliases_default_branch_sentinel(branch.as_str()) {
        return Err(StrataError::invalid_input(
            "branch name aliases reserved default-branch sentinel",
        )
        .into());
    }

    Ok(strata_core::types::BranchId::from_user_name(
        branch.as_str(),
    ))
}

pub(crate) fn runtime_default_branch(db: &Database) -> BranchId {
    BranchId::from(
        db.default_branch_name()
            .unwrap_or_else(|| "default".to_string()),
    )
}

pub(crate) fn access_denied(db: &Database, command: &str) -> Error {
    let hint = if db.is_follower() {
        Some(
            "This database is a read-only follower. Writes must go through the primary instance."
                .to_string(),
        )
    } else {
        Some("Database is in read-only mode.".to_string())
    };
    Error::AccessDenied {
        command: command.to_string(),
        hint,
    }
}

pub(crate) fn is_read_only(access_mode: AccessMode) -> bool {
    matches!(access_mode, AccessMode::ReadOnly)
}

pub(crate) fn requires_session(command: &Command) -> bool {
    matches!(
        command,
        Command::TxnBegin { .. }
            | Command::TxnCommit
            | Command::TxnRollback
            | Command::TxnInfo
            | Command::TxnIsActive
    )
}

pub(crate) fn bypasses_active_transaction(command: &Command) -> bool {
    matches!(
        command,
        Command::Ping
            | Command::Info
            | Command::Health
            | Command::Metrics
            | Command::Flush
            | Command::Compact
            | Command::Describe { .. }
            | Command::SpaceList { .. }
            | Command::SpaceCreate { .. }
            | Command::SpaceExists { .. }
            | Command::SpaceDelete { .. }
            | Command::TagCreate { .. }
            | Command::TagDelete { .. }
            | Command::TagList { .. }
            | Command::TagResolve { .. }
            | Command::NoteAdd { .. }
            | Command::NoteGet { .. }
            | Command::NoteDelete { .. }
    )
}

pub(crate) fn session_required(command: &str) -> Error {
    Error::InvalidInput {
        reason: format!("{command} requires a session"),
        hint: Some("Use Session::execute for transaction-scoped commands.".to_string()),
    }
}

pub(crate) fn branch_is_reserved(branch: &str) -> bool {
    branch.starts_with(RESERVED_BRANCH_PREFIX)
}

pub(crate) fn reject_reserved_branch(branch: &BranchId) -> Result<()> {
    reject_reserved_branch_name(branch.as_str())
}

pub(crate) fn reject_reserved_branch_name(branch: &str) -> Result<()> {
    if branch_is_reserved(branch) {
        return Err(Error::InvalidInput {
            reason: format!("Branch '{branch}' is reserved for system use"),
            hint: Some(
                "Branches starting with '_system' are internal and cannot be accessed directly."
                    .to_string(),
            ),
        });
    }
    Ok(())
}

pub(crate) fn extract_version(v: &strata_core::Version) -> u64 {
    match v {
        strata_core::Version::Txn(n)
        | strata_core::Version::Sequence(n)
        | strata_core::Version::Counter(n) => *n,
    }
}

pub(crate) fn to_versioned_value(v: CoreVersionedValue) -> VersionedValue {
    VersionedValue {
        value: v.value,
        version: extract_version(&v.version),
        timestamp: v.timestamp.into(),
    }
}

pub(crate) fn from_engine_branch_status(_status: EngineBranchStatus) -> BranchStatus {
    BranchStatus::Active
}

pub(crate) fn validate_key(key: &str) -> StrataResult<()> {
    validate_key_with_limits(key, &Limits::default())
}

pub(crate) fn validate_key_with_limits(key: &str, limits: &Limits) -> StrataResult<()> {
    if key.is_empty() {
        return Err(StrataError::invalid_input("Key must not be empty"));
    }
    if let Err(e) = limits.validate_key_length(key) {
        return Err(StrataError::capacity_exceeded("key", e.max(), e.actual()));
    }
    if key.contains('\0') {
        return Err(StrataError::invalid_input("Key must not contain NUL bytes"));
    }
    if key.starts_with(RESERVED_KEY_PREFIX) {
        return Err(StrataError::invalid_input(format!(
            "Key must not start with reserved prefix '{}'",
            RESERVED_KEY_PREFIX
        )));
    }
    Ok(())
}

pub(crate) fn validate_value(value: &Value, limits: &Limits) -> StrataResult<()> {
    limits
        .validate_value(value)
        .map_err(|e| StrataError::capacity_exceeded(e.reason_code(), e.max(), e.actual()))
}

pub(crate) fn validate_vector(vector: &[f32], limits: &Limits) -> StrataResult<()> {
    limits
        .validate_vector(vector)
        .map_err(|e| StrataError::capacity_exceeded(e.reason_code(), e.max(), e.actual()))
}

pub(crate) fn is_internal_collection(name: &str) -> bool {
    name.starts_with('_')
}

pub(crate) fn validate_not_internal_collection(name: &str) -> StrataResult<()> {
    if is_internal_collection(name) {
        return Err(StrataError::invalid_input(format!(
            "Collection '{}' is internal and cannot be accessed directly",
            name
        )));
    }
    Ok(())
}

pub(crate) fn value_to_json(value: Value) -> StrataResult<JsonValue> {
    let json_val = value_to_serde_json(value)?;
    Ok(JsonValue::from(json_val))
}

pub(crate) fn json_to_value(json: JsonValue) -> StrataResult<Value> {
    let serde_val: serde_json::Value = json.into();
    serde_json_to_value(serde_val)
}

fn value_to_serde_json(value: Value) -> StrataResult<serde_json::Value> {
    use serde_json::Map;
    use serde_json::Value as Json;

    match value {
        Value::Null => Ok(Json::Null),
        Value::Bool(b) => Ok(Json::Bool(b)),
        Value::Int(i) => Ok(Json::Number(i.into())),
        Value::Float(f) => {
            if f.is_infinite() || f.is_nan() {
                return Err(StrataError::serialization(format!(
                    "Cannot convert {} to JSON: not a valid JSON number",
                    f
                )));
            }
            serde_json::Number::from_f64(f)
                .map(Json::Number)
                .ok_or_else(|| {
                    StrataError::serialization(format!("Cannot convert {} to JSON number", f))
                })
        }
        Value::String(s) => Ok(Json::String(s)),
        Value::Bytes(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&b);
            Ok(Json::String(format!("__bytes__:{}", encoded)))
        }
        Value::Array(arr) => {
            let converted: std::result::Result<Vec<_>, _> =
                (*arr).into_iter().map(value_to_serde_json).collect();
            Ok(Json::Array(converted?))
        }
        Value::Object(obj) => {
            let mut map = Map::new();
            for (k, v) in *obj {
                map.insert(k, value_to_serde_json(v)?);
            }
            Ok(Json::Object(map))
        }
    }
}

fn serde_json_to_value(json: serde_json::Value) -> StrataResult<Value> {
    use serde_json::Value as Json;

    match json {
        Json::Null => Ok(Value::Null),
        Json::Bool(b) => Ok(Value::Bool(b)),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(StrataError::serialization(format!(
                    "Cannot convert JSON number {} to Value",
                    n
                )))
            }
        }
        Json::String(s) => {
            if let Some(encoded) = s.strip_prefix("__bytes__:") {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| {
                        StrataError::serialization(format!("Invalid base64 in bytes value: {}", e))
                    })?;
                Ok(Value::Bytes(bytes))
            } else {
                Ok(Value::String(s))
            }
        }
        Json::Array(arr) => {
            let converted: std::result::Result<Vec<_>, _> =
                arr.into_iter().map(serde_json_to_value).collect();
            Ok(Value::array(converted?))
        }
        Json::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(k, serde_json_to_value(v)?);
            }
            Ok(Value::object(map))
        }
    }
}

pub(crate) fn value_to_serde_json_public(value: Value) -> StrataResult<serde_json::Value> {
    value_to_serde_json(value)
}

pub(crate) fn serde_json_to_value_public(json: serde_json::Value) -> StrataResult<Value> {
    serde_json_to_value(json)
}

pub(crate) fn to_engine_metric(metric: DistanceMetric) -> strata_vector::DistanceMetric {
    match metric {
        DistanceMetric::Cosine => strata_vector::DistanceMetric::Cosine,
        DistanceMetric::Euclidean => strata_vector::DistanceMetric::Euclidean,
        DistanceMetric::DotProduct => strata_vector::DistanceMetric::DotProduct,
    }
}

pub(crate) fn from_engine_metric(metric: strata_vector::DistanceMetric) -> DistanceMetric {
    match metric {
        strata_vector::DistanceMetric::Cosine => DistanceMetric::Cosine,
        strata_vector::DistanceMetric::Euclidean => DistanceMetric::Euclidean,
        strata_vector::DistanceMetric::DotProduct => DistanceMetric::DotProduct,
    }
}

pub(crate) fn to_engine_filter(
    filters: &[MetadataFilter],
) -> Option<strata_vector::MetadataFilter> {
    if filters.is_empty() {
        return None;
    }

    let mut engine_filter = strata_vector::MetadataFilter::new();
    for filter in filters {
        let scalar = value_to_json_scalar(&filter.value);
        match filter.op {
            FilterOp::Eq => {
                engine_filter.equals.insert(filter.field.clone(), scalar);
            }
            _ => {
                let op = match filter.op {
                    FilterOp::Eq => strata_vector::FilterOp::Eq,
                    FilterOp::Ne => strata_vector::FilterOp::Ne,
                    FilterOp::Gt => strata_vector::FilterOp::Gt,
                    FilterOp::Gte => strata_vector::FilterOp::Gte,
                    FilterOp::Lt => strata_vector::FilterOp::Lt,
                    FilterOp::Lte => strata_vector::FilterOp::Lte,
                    FilterOp::In => strata_vector::FilterOp::In,
                    FilterOp::Contains => strata_vector::FilterOp::Contains,
                };
                engine_filter
                    .conditions
                    .push(strata_vector::FilterCondition {
                        field: filter.field.clone(),
                        op,
                        value: scalar,
                    });
            }
        }
    }

    if engine_filter.is_empty() {
        None
    } else {
        Some(engine_filter)
    }
}

fn value_to_json_scalar(value: &Value) -> strata_vector::JsonScalar {
    match value {
        Value::Null => strata_vector::JsonScalar::Null,
        Value::Bool(boolean) => strata_vector::JsonScalar::Bool(*boolean),
        Value::Int(integer) => strata_vector::JsonScalar::Number(*integer as f64),
        Value::Float(float) => strata_vector::JsonScalar::Number(*float),
        Value::String(string) => strata_vector::JsonScalar::String(string.clone()),
        _ => strata_vector::JsonScalar::Null,
    }
}

pub(crate) fn parse_path(path: &str) -> StrataResult<JsonPath> {
    if path.is_empty() || path == "$" {
        return Ok(JsonPath::root());
    }

    let normalized = path.strip_prefix('$').unwrap_or(path);
    normalized
        .parse()
        .map_err(|e| StrataError::invalid_input(format!("Invalid JSON path '{}': {:?}", path, e)))
}

pub(crate) fn transaction_already_active() -> Error {
    Error::TransactionAlreadyActive {
        hint: Some(
            "Commit or rollback the active transaction before starting another one.".to_string(),
        ),
    }
}

pub(crate) fn transaction_not_active() -> Error {
    Error::TransactionNotActive {
        hint: Some(
            "Use TxnBegin to begin a transaction before issuing transaction control commands."
                .to_string(),
        ),
    }
}

pub(crate) fn transaction_branch_mismatch(
    command_branch: &BranchId,
    txn_branch: &BranchId,
) -> Error {
    Error::InvalidInput {
        reason: format!(
            "command targets branch '{}' while the active transaction is bound to '{}'",
            command_branch, txn_branch
        ),
        hint: Some(
            "Commit or roll back the active transaction before using a different branch."
                .to_string(),
        ),
    }
}

pub(crate) fn validate_branch_exists(db: &Arc<Database>, branch: &BranchId) -> Result<()> {
    if db.branches().exists(branch.as_str()).map_err(Error::from)? {
        Ok(())
    } else {
        Err(Error::BranchNotFound {
            branch: branch.to_string(),
            hint: None,
        })
    }
}

pub(crate) fn require_branch_exists(p: &Arc<Primitives>, branch: &BranchId) -> Result<()> {
    let default_branch =
        p.db.default_branch_name()
            .unwrap_or_else(|| "default".to_string());
    if default_branch == branch.as_str() {
        return Ok(());
    }

    if p.db
        .branches()
        .exists(branch.as_str())
        .map_err(Error::from)?
    {
        Ok(())
    } else {
        Err(Error::BranchNotFound {
            branch: branch.to_string(),
            hint: None,
        })
    }
}

pub(crate) fn validate_session_space(space: &str) -> Result<()> {
    validate_space_name(space).map_err(|reason| Error::InvalidInput { reason, hint: None })
}

pub(crate) fn database_info(db: &Arc<Database>, default_branch: &BranchId) -> DatabaseInfo {
    let branch_count = db
        .branches()
        .list()
        .map(|ids| {
            let has_default = ids.iter().any(|id| id == default_branch.as_str());
            ids.len() as u64 + u64::from(!has_default)
        })
        .unwrap_or(0);

    DatabaseInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: db.uptime_secs(),
        branch_count,
        total_keys: db.approximate_total_keys(),
        default_branch: default_branch.as_str().to_string(),
    }
}
