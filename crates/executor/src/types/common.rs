use std::fmt;

use base64::Engine as _;
use serde::de::{Error, SeqAccess, Visitor};
use serde::Serializer;

use super::{Deserialize, Deserializer, ErrorStatus, Serialize};

/// Default product branch used when a command omits its branch.
pub const DEFAULT_BRANCH: &str = "default";

/// Default product space used when a command omits its space.
pub const DEFAULT_SPACE: &str = "default";

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Byte-preserving command payload.
///
/// On the JSON wire it is a standard base64 string, so every consumer sees a
/// single unambiguous, compact, human-legible encoding rather than an integer
/// array (DSGN-5/DTO-2). Deserialization also accepts a raw byte array for
/// forward-compatibility with legacy payloads.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Bytes(Vec<u8>);

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&BASE64.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a base64 string or an array of byte values")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Bytes, E> {
                BASE64
                    .decode(value)
                    .map(Bytes)
                    .map_err(|error| E::custom(format!("invalid base64 bytes: {error}")))
            }

            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(value.to_vec()))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Bytes, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or_default());
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(Bytes(bytes))
            }
        }

        deserializer.deserialize_any(BytesVisitor)
    }
}

/// The custom serde impls above put base64 strings on the wire; the schema
/// must say the same thing, so it is written by hand rather than derived.
#[cfg(feature = "idl-tooling")]
impl schemars::JsonSchema for Bytes {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Bytes".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "contentEncoding": "base64",
            "description": "Binary payload encoded as a standard base64 string."
        })
    }
}

impl Bytes {
    /// Creates a byte payload.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the raw bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the wrapper and returns raw bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    /// Returns true when this payload has no bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<&[u8]> for Bytes {
    fn from(value: &[u8]) -> Self {
        Self::new(value)
    }
}

impl<const N: usize> From<&[u8; N]> for Bytes {
    fn from(value: &[u8; N]) -> Self {
        Self::new(value.as_slice())
    }
}

impl From<&str> for Bytes {
    fn from(value: &str) -> Self {
        Self::new(value.as_bytes())
    }
}

/// Shared pagination continuation facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct PageInfo<C> {
    has_more: bool,
    cursor: Option<C>,
}

impl<C> PageInfo<C> {
    /// Creates pagination facts.
    ///
    /// A non-terminal page must carry the cursor needed to request the next
    /// page. Terminal pages serialize an explicit `cursor: null`.
    pub fn new(has_more: bool, cursor: Option<C>) -> Self {
        assert!(
            has_more || cursor.is_none(),
            "terminal pages must not carry a continuation cursor"
        );
        assert!(
            !has_more || cursor.is_some(),
            "non-terminal pages must carry a continuation cursor"
        );
        Self { has_more, cursor }
    }

    /// Creates terminal pagination facts.
    pub const fn terminal() -> Self {
        Self {
            has_more: false,
            cursor: None,
        }
    }

    /// Returns true when another page is available.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the continuation cursor, when another page is available.
    pub const fn cursor(&self) -> Option<&C> {
        self.cursor.as_ref()
    }
}

impl<'de, C> Deserialize<'de> for PageInfo<C>
where
    C: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
        struct RawPageInfo<C> {
            has_more: bool,
            cursor: Option<C>,
        }

        let value = RawPageInfo::deserialize(deserializer)?;
        if value.has_more && value.cursor.is_none() {
            return Err(serde::de::Error::custom(
                "non-terminal pages must carry a continuation cursor",
            ));
        }
        if !value.has_more && value.cursor.is_some() {
            return Err(serde::de::Error::custom(
                "terminal pages must not carry a continuation cursor",
            ));
        }
        Ok(Self {
            has_more: value.has_more,
            cursor: value.cursor,
        })
    }
}

/// Point-read envelope shared by every capability's single-record `get`.
///
/// `found` is the authoritative presence flag; `value` carries the record only
/// when it exists. A missing record serializes as `{found: false, value: null}`
/// and a present one as `{found: true, value: <record>}`, so absence never
/// aliases a bare `null` payload and every primitive answers a point read the
/// same shape.
///
/// The payload is `Option<T>` because these primitives never store a JSON
/// `null` as a record — struct payloads round-trip cleanly through
/// `Option<T>`. JSON is the one exception: a stored JSON `null` is a real value
/// that `Option<serde_json::Value>` would collapse to absence on deserialize,
/// so a JSON read answers with
/// [`MaybeJsonVersionedValue`](super::MaybeJsonVersionedValue), whose payload
/// carries a non-optional `value` to keep found-null distinct from absent.
/// Both envelopes serialize the same `{found, value}` wire shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct Maybe<T> {
    found: bool,
    value: Option<T>,
}

impl<T> Maybe<T> {
    /// Creates a present point-read result.
    pub fn found(value: T) -> Self {
        Self {
            found: true,
            value: Some(value),
        }
    }

    /// Creates an absent point-read result.
    pub const fn missing() -> Self {
        Self {
            found: false,
            value: None,
        }
    }

    /// Creates a result from an optional record.
    pub fn from_option(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::found(value),
            None => Self::missing(),
        }
    }

    /// Returns true when the record exists.
    pub const fn is_found(&self) -> bool {
        self.found
    }

    /// Returns the record when it exists.
    pub const fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Consumes the result and returns the record when it exists.
    pub fn into_option(self) -> Option<T> {
        self.value
    }
}

/// Positional batch-existence item: whether the key at this position exists.
/// json/vector batch reads are positional (no echoed key), so `batch_exists`
/// carries only the presence bool; callers correlate by the `BatchItem` index.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchExistsPresence {
    exists: bool,
}

impl BatchExistsPresence {
    /// Creates a presence item.
    pub const fn new(exists: bool) -> Self {
        Self { exists }
    }

    /// Returns whether the key exists.
    pub const fn exists(&self) -> bool {
        self.exists
    }
}

/// Batch execution semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    /// The command is one atomic unit. Invalid operations fail the whole
    /// command before item results are returned.
    Atomic,
    /// Valid items are applied together while invalid items are reported in
    /// positional item results.
    Itemwise,
}

/// Normalized batch-level outcome status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    /// The batch completed without item-level errors or mixed apply/no-op
    /// mutation results.
    Ok,
    /// The batch contains a mix of successful and failed, missed, or no-op
    /// items.
    Partial,
    /// Every item failed.
    Error,
}

/// Normalized positional item status within a batch response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BatchItemStatus {
    /// The item was accepted by the command.
    Ok,
    /// The item was valid, but the requested record was not found.
    Miss,
    /// The item failed validation or execution.
    Error,
}

/// Shared positional item wrapper for all public batch responses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchItem<T> {
    index: u64,
    status: BatchItemStatus,
    applied: bool,
    effect: Option<MutationEffect>,
    commit: Option<CommitReceipt>,
    result: Option<T>,
    error: Option<ErrorStatus>,
}

impl<T> BatchItem<T> {
    /// Creates a batch item wrapper.
    pub fn new(
        index: u64,
        applied: bool,
        effect: Option<MutationEffect>,
        commit: Option<CommitReceipt>,
        result: Option<T>,
        error: Option<ErrorStatus>,
    ) -> Self {
        let status = if error.is_some() {
            BatchItemStatus::Error
        } else {
            BatchItemStatus::Ok
        };
        let (applied, effect, commit) = if status == BatchItemStatus::Error {
            (false, None, None)
        } else {
            (applied, effect, commit)
        };
        Self {
            index,
            status,
            applied,
            effect,
            commit,
            result,
            error,
        }
    }

    /// Creates a successful item wrapper with a primitive item payload.
    pub fn ok(
        index: u64,
        applied: bool,
        effect: Option<MutationEffect>,
        commit: Option<CommitReceipt>,
        result: T,
    ) -> Self {
        Self::new(index, applied, effect, commit, Some(result), None)
    }

    /// Creates a failed item wrapper with a primitive item payload.
    pub fn failed(index: u64, result: Option<T>, error: ErrorStatus) -> Self {
        Self::new(index, false, None, None, result, Some(error))
    }

    /// Creates a valid read item whose requested record was not found.
    pub fn miss(index: u64, result: T) -> Self {
        Self {
            index,
            status: BatchItemStatus::Miss,
            applied: false,
            effect: None,
            commit: None,
            result: Some(result),
            error: None,
        }
    }

    /// Returns the input item position.
    pub const fn index(&self) -> u64 {
        self.index
    }

    /// Returns the normalized item status.
    pub const fn status(&self) -> BatchItemStatus {
        self.status
    }

    /// Returns true when this item changed logical state.
    pub const fn applied(&self) -> bool {
        self.applied
    }

    /// Returns mutation effect facts for successful mutation items.
    pub const fn effect(&self) -> Option<&MutationEffect> {
        self.effect.as_ref()
    }

    /// Returns item commit facts when the item participates in a commit.
    pub const fn commit(&self) -> Option<&CommitReceipt> {
        self.commit.as_ref()
    }

    /// Returns the primitive-specific item result payload.
    pub const fn result(&self) -> Option<&T> {
        self.result.as_ref()
    }

    /// Consumes the item and returns the primitive-specific result payload.
    pub fn into_result(self) -> Option<T> {
        self.result
    }

    /// Returns the normalized item error.
    pub const fn error_status(&self) -> Option<&ErrorStatus> {
        self.error.as_ref()
    }

    /// Returns the normalized item error message.
    pub fn error(&self) -> Option<&str> {
        self.error.as_ref().map(ErrorStatus::message)
    }
}

impl<T> std::ops::Deref for BatchItem<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.result
            .as_ref()
            .expect("batch item primitive result payload is present")
    }
}

/// Shared batch response wrapper for all public batch commands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct BatchResult<T> {
    mode: BatchMode,
    status: BatchStatus,
    applied: bool,
    commit: Option<CommitReceipt>,
    items: Vec<BatchItem<T>>,
}

impl<T> BatchResult<T> {
    /// Creates a batch response and derives status, applied, and shared commit
    /// facts from positional item wrappers.
    pub fn from_items(mode: BatchMode, items: Vec<BatchItem<T>>) -> Self {
        let status = batch_status(&items);
        let applied = items.iter().any(BatchItem::applied);
        let commit = shared_batch_commit(&items);
        Self {
            mode,
            status,
            applied,
            commit,
            items,
        }
    }

    /// Returns batch execution semantics.
    pub const fn mode(&self) -> BatchMode {
        self.mode
    }

    /// Returns normalized batch status.
    pub const fn status(&self) -> BatchStatus {
        self.status
    }

    /// Returns true when at least one item changed logical state.
    pub const fn applied(&self) -> bool {
        self.applied
    }

    /// Returns the shared commit receipt when all committed items share one
    /// commit.
    pub const fn commit(&self) -> Option<&CommitReceipt> {
        self.commit.as_ref()
    }

    /// Returns positional batch items.
    pub fn items(&self) -> &[BatchItem<T>] {
        &self.items
    }

    /// Iterates over positional batch items.
    pub fn iter(&self) -> std::slice::Iter<'_, BatchItem<T>> {
        self.items.iter()
    }

    /// Returns positional batch item count.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true when the batch returned no item results.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Consumes the response and returns positional batch items.
    pub fn into_items(self) -> Vec<BatchItem<T>> {
        self.items
    }
}

impl<T> std::ops::Index<usize> for BatchResult<T> {
    type Output = BatchItem<T>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.items[index]
    }
}

impl<'a, T> IntoIterator for &'a BatchResult<T> {
    type Item = &'a BatchItem<T>;
    type IntoIter = std::slice::Iter<'a, BatchItem<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<T> IntoIterator for BatchResult<T> {
    type Item = BatchItem<T>;
    type IntoIter = std::vec::IntoIter<BatchItem<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

fn batch_status<T>(items: &[BatchItem<T>]) -> BatchStatus {
    if items.is_empty() {
        return BatchStatus::Ok;
    }
    let error_count = items
        .iter()
        .filter(|item| item.status == BatchItemStatus::Error)
        .count();
    if error_count == items.len() {
        return BatchStatus::Error;
    }
    if error_count > 0 {
        return BatchStatus::Partial;
    }
    if items
        .iter()
        .any(|item| item.status == BatchItemStatus::Miss)
    {
        return BatchStatus::Partial;
    }
    let applied_count = items.iter().filter(|item| item.applied()).count();
    if applied_count > 0 && applied_count < items.len() {
        BatchStatus::Partial
    } else {
        BatchStatus::Ok
    }
}

fn shared_batch_commit<T>(items: &[BatchItem<T>]) -> Option<CommitReceipt> {
    let mut shared = None;
    for item in items {
        let Some(commit) = item.commit else {
            continue;
        };
        match shared {
            None => shared = Some(commit),
            Some(existing) if existing == commit => {}
            Some(_) => return None,
        }
    }
    shared
}

/// Per-commit durability, as storage attested it at acknowledgement time.
///
/// `standard` is an admission fact, not a survival guarantee: the commit
/// becomes durable at the next sync point (close, threshold, rotation) and
/// can be lost to process kill until then. Only `always` attests the commit
/// was synced before acknowledgement. `uncertain` means storage could not
/// attest either way (#2756: the former `durable` boolean reported
/// `standard` commits as durable, so SDK callers treated unsynced
/// acknowledgements as crash-safe).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CommitDurability {
    /// Volatile commit (cache mode): gone when the process exits.
    NotDurable,
    /// Durable after the next sync point; lost with the process until then.
    Standard,
    /// Synced to durable storage before acknowledgement.
    Always,
    /// Storage could not attest this commit's durability.
    Uncertain,
}

/// Commit facts returned by mutating operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct CommitReceipt {
    version: u64,
    /// Commit's position on the logical commit timeline: a monotonic counter
    /// assigned per commit, so a fresh database starts small (near 1) and it is
    /// never a calendar date. Pass it to `--as-of` and match it against
    /// `history`; do not format it as a Unix/epoch timestamp. For the real
    /// wall-clock time, use `committed_at`.
    timestamp: u64,
    /// The wall-clock instant the commit was applied (UTC epoch microseconds) —
    /// the value to format as a calendar date. `null` when unknown (a replayed
    /// or imported commit). Distinct from the logical `timestamp` (#3112).
    #[serde(default)]
    committed_at: Option<u64>,
    durability: CommitDurability,
    put_count: u64,
    delete_count: u64,
}

impl CommitReceipt {
    /// Creates a commit receipt.
    pub const fn new(
        version: u64,
        timestamp: u64,
        durability: CommitDurability,
        put_count: u64,
        delete_count: u64,
    ) -> Self {
        Self {
            version,
            timestamp,
            committed_at: None,
            durability,
            put_count,
            delete_count,
        }
    }

    /// Attaches the wall-clock instant (UTC epoch micros) the commit was
    /// applied. `None` leaves it unknown (#3112).
    #[must_use]
    pub const fn with_committed_at(mut self, committed_at: Option<u64>) -> Self {
        self.committed_at = committed_at;
        self
    }

    /// Returns the commit version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the wall-clock instant (UTC epoch micros) the commit was applied,
    /// or `None` when unknown. This is the value to format as a date; the
    /// logical `timestamp` is not (#3112).
    pub const fn committed_at(&self) -> Option<u64> {
        self.committed_at
    }

    /// Returns the commit's logical commit-timeline position (a monotonic
    /// per-commit counter), not a wall-clock time. See the `timestamp` field.
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the durability storage attested for this commit.
    pub const fn durability(&self) -> CommitDurability {
        self.durability
    }

    /// Returns the number of put rows in the commit.
    pub const fn put_count(&self) -> u64 {
        self.put_count
    }

    /// Returns the number of delete rows in the commit.
    pub const fn delete_count(&self) -> u64 {
        self.delete_count
    }
}

/// High-level mutation effect for idempotent and conditional operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MutationEffectKind {
    /// A new logical entity was created.
    Created,
    /// An existing logical entity was updated.
    Updated,
    /// An existing logical entity was deleted.
    Deleted,
    /// The operation matched but left state unchanged.
    Unchanged,
    /// The operation did not match a visible entity.
    NotFound,
}

/// Normalized mutation effect facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "idl-tooling", derive(schemars::JsonSchema))]
pub struct MutationEffect {
    applied: bool,
    kind: MutationEffectKind,
    matched: bool,
    affected_count: u64,
}

impl MutationEffect {
    /// Creates mutation effect facts.
    pub const fn new(
        applied: bool,
        kind: MutationEffectKind,
        matched: bool,
        affected_count: u64,
    ) -> Self {
        Self {
            applied,
            kind,
            matched,
            affected_count,
        }
    }

    /// Creates effect facts for a newly created entity.
    pub const fn created() -> Self {
        Self::new(true, MutationEffectKind::Created, false, 1)
    }

    /// Creates effect facts for an updated entity.
    pub const fn updated() -> Self {
        Self::new(true, MutationEffectKind::Updated, true, 1)
    }

    /// Creates effect facts for a deleted entity.
    pub const fn deleted() -> Self {
        Self::new(true, MutationEffectKind::Deleted, true, 1)
    }

    /// Creates effect facts for a missing mutation target.
    pub const fn not_found() -> Self {
        Self::new(false, MutationEffectKind::NotFound, false, 0)
    }

    /// Returns true when the operation changed durable logical state.
    pub const fn applied(&self) -> bool {
        self.applied
    }

    /// Returns the normalized mutation kind.
    pub const fn kind(&self) -> MutationEffectKind {
        self.kind
    }

    /// Returns true when the operation matched an existing logical entity.
    pub const fn matched(&self) -> bool {
        self.matched
    }

    /// Returns the number of affected logical entities.
    pub const fn affected_count(&self) -> u64 {
        self.affected_count
    }
}
