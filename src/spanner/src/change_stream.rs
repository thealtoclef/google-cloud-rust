// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Change stream query support for Spanner.
//!
//! This module provides a high-level API for querying Spanner
//! [change streams](https://cloud.google.com/spanner/docs/change-streams).
//! It wraps the underlying `ExecuteStreamingSql` RPC and yields
//! [`ChangeStreamEntry`] values that cover both partition modes.
//!
//! # Partition modes
//!
//! Spanner change streams can be created with one of two partition modes:
//!
//! - **`MUTABLE_KEY_RANGE`** (newer) — returns proto-encoded
//!   [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) bytes
//!   (`TypeCode::Proto`). Supports richer lifecycle events:
//!   [`PartitionStartRecord`], [`PartitionEndRecord`], [`PartitionEventRecord`].
//!   Requires a non-null `end_timestamp`.
//!
//! - **`IMMUTABLE_KEY_RANGE`** (legacy, the default) — returns
//!   `ARRAY<STRUCT<...>>` JSON. Partition lifecycle uses
//!   [`ChildPartitionsRecord`] (no proto definition; hand-written in this
//!   module). Allows null `end_timestamp` for indefinite streaming.
//!
//! The API auto-detects the mode from the column type metadata and
//! deserializes accordingly. All record types are unified under the
//! [`ChangeStreamEntry`] enum.
//!
//! [`PartitionStartRecord`]: crate::model::change_stream_record::PartitionStartRecord
//! [`PartitionEndRecord`]: crate::model::change_stream_record::PartitionEndRecord
//! [`PartitionEventRecord`]: crate::model::change_stream_record::PartitionEventRecord
//!
//! # Example
//!
//! ```no_run
//! # use google_cloud_spanner::client::Spanner;
//! # use google_cloud_spanner::change_stream::ChangeStreamEntry;
//! # async fn example() -> anyhow::Result<()> {
//! let spanner = Spanner::builder().build().await?;
//! let db = spanner
//!     .database_client("projects/p/instances/i/databases/d")
//!     .build()
//!     .await?;
//!
//! let now = time::OffsetDateTime::now_utc();
//! let end = now + time::Duration::minutes(2);
//!
//! let mut stream = db
//!     .change_stream_query("MyChangeStream")
//!     .with_start_timestamp(now)
//!     .with_end_timestamp(end)
//!     .with_heartbeat_milliseconds(10_000)
//!     .execute()
//!     .await?;
//!
//! while let Some(entry) = stream.next().await {
//!     match entry? {
//!         ChangeStreamEntry::DataChangeRecord(dcr) => {
//!             println!("data change in {}", dcr.table);
//!         }
//!         ChangeStreamEntry::HeartbeatRecord(hb) => {
//!             println!("heartbeat: {:?}", hb.timestamp);
//!         }
//!         other => println!("{other:?}"),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Partition management
//!
//! A change stream is split across partitions whose lifetimes are controlled
//! by Spanner. Callers are responsible for spawning additional queries when
//! partition lifecycle records are received:
//! - `MUTABLE_KEY_RANGE`: [`PartitionStartRecord`](crate::model::change_stream_record::PartitionStartRecord)
//!   contains tokens for new partitions.
//! - `IMMUTABLE_KEY_RANGE`: [`ChildPartitionsRecord`] contains child partition
//!   tokens after a split.

use crate::database_client::DatabaseClient;
use crate::model::ChangeStreamRecord;
use crate::result_set::ResultSet;
use crate::statement::Statement;
use gaxi::prost::FromProto;
use prost::Message;

// ── ChildPartitionsRecord (IMMUTABLE_KEY_RANGE only, no proto definition) ──

/// A child partition from a partition split in an `IMMUTABLE_KEY_RANGE`
/// change stream.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ChildPartition {
    /// The partition token for this child partition.
    pub token: String,
    /// Tokens of the parent partition(s) that were split to produce this child.
    pub parent_partition_tokens: Vec<String>,
}

/// Partition split record returned by `IMMUTABLE_KEY_RANGE` change streams.
///
/// This type has no proto definition; it is parsed from the `ARRAY<STRUCT>`
/// JSON wire format. It is the legacy counterpart to the proto-defined
/// [`PartitionStartRecord`](crate::model::change_stream_record::PartitionStartRecord)
/// and [`PartitionEndRecord`](crate::model::change_stream_record::PartitionEndRecord)
/// used by `MUTABLE_KEY_RANGE`.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ChildPartitionsRecord {
    /// The timestamp at which the partition split occurred.
    pub start_timestamp: Option<String>,
    /// Sequence number for ordering within the same timestamp.
    pub record_sequence: String,
    /// The child partitions produced by the split.
    pub child_partitions: Vec<ChildPartition>,
}

// ── ChangeStreamEntry (unified return type) ──

/// A single entry from a change stream query.
///
/// This enum unifies the record types from both partition modes:
/// - `DataChangeRecord` and `HeartbeatRecord` are shared across both modes.
/// - `PartitionStartRecord`, `PartitionEndRecord`, `PartitionEventRecord`
///   are specific to `MUTABLE_KEY_RANGE`.
/// - `ChildPartitionsRecord` is specific to `IMMUTABLE_KEY_RANGE`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ChangeStreamEntry {
    /// A data change (insert/update/delete). Both modes.
    DataChangeRecord(Box<crate::model::change_stream_record::DataChangeRecord>),
    /// A heartbeat indicating the stream is alive. Both modes.
    HeartbeatRecord(Box<crate::model::change_stream_record::HeartbeatRecord>),
    /// A new partition has started. `MUTABLE_KEY_RANGE` only.
    PartitionStartRecord(Box<crate::model::change_stream_record::PartitionStartRecord>),
    /// A partition has ended. `MUTABLE_KEY_RANGE` only.
    PartitionEndRecord(Box<crate::model::change_stream_record::PartitionEndRecord>),
    /// A partition event (key range move). `MUTABLE_KEY_RANGE` only.
    PartitionEventRecord(Box<crate::model::change_stream_record::PartitionEventRecord>),
    /// A partition split. `IMMUTABLE_KEY_RANGE` only.
    ChildPartitionsRecord(Box<ChildPartitionsRecord>),
}

impl ChangeStreamEntry {
    /// Returns the inner [`DataChangeRecord`] if this is one, or `None`.
    pub fn as_data_change_record(
        &self,
    ) -> Option<&crate::model::change_stream_record::DataChangeRecord> {
        match self {
            Self::DataChangeRecord(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner [`HeartbeatRecord`] if this is one, or `None`.
    pub fn as_heartbeat_record(
        &self,
    ) -> Option<&crate::model::change_stream_record::HeartbeatRecord> {
        match self {
            Self::HeartbeatRecord(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner [`PartitionStartRecord`] if this is one, or `None`.
    pub fn as_partition_start_record(
        &self,
    ) -> Option<&crate::model::change_stream_record::PartitionStartRecord> {
        match self {
            Self::PartitionStartRecord(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner [`PartitionEndRecord`] if this is one, or `None`.
    pub fn as_partition_end_record(
        &self,
    ) -> Option<&crate::model::change_stream_record::PartitionEndRecord> {
        match self {
            Self::PartitionEndRecord(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner [`PartitionEventRecord`] if this is one, or `None`.
    pub fn as_partition_event_record(
        &self,
    ) -> Option<&crate::model::change_stream_record::PartitionEventRecord> {
        match self {
            Self::PartitionEventRecord(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner [`ChildPartitionsRecord`] if this is one, or `None`.
    pub fn as_child_partitions_record(&self) -> Option<&ChildPartitionsRecord> {
        match self {
            Self::ChildPartitionsRecord(v) => Some(v),
            _ => None,
        }
    }
}

/// Regex-like allowlist: a valid GoogleSQL identifier contains only ASCII
/// letters, digits, underscores, and hyphens.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Validates that `name` looks like a legal Spanner identifier and returns it
/// backtick-escaped for safe interpolation into SQL.
///
/// Only ASCII alphanumeric characters, underscores, and hyphens are accepted.
fn escape_identifier(name: &str) -> crate::Result<String> {
    if !is_valid_identifier(name) {
        return Err(crate::error::internal_error(format!(
            "invalid change stream name: {name:?} \
             (must be non-empty and contain only ASCII alphanumeric, underscore, or hyphen)",
        )));
    }
    Ok(format!("`{name}`"))
}

/// A builder for change stream queries.
///
/// Created by [`DatabaseClient::change_stream_query`]. The builder constructs
/// the `SELECT ChangeRecord FROM READ_<stream>(...)` TVF query and returns a
/// [`ChangeStreamRecordStream`] that yields [`ChangeStreamEntry`] values.
///
/// # Partition mode
///
/// Both `MUTABLE_KEY_RANGE` and `IMMUTABLE_KEY_RANGE` change streams are
/// supported. The wire format is auto-detected from the column type metadata.
/// See the [module documentation](self) for details.
#[derive(Clone, Debug)]
#[must_use]
pub struct ChangeStreamQueryBuilder {
    client: DatabaseClient,
    change_stream_name: String,
    start_timestamp: Option<time::OffsetDateTime>,
    end_timestamp: Option<time::OffsetDateTime>,
    partition_token: Option<String>,
    heartbeat_milliseconds: i64,
}

impl ChangeStreamQueryBuilder {
    pub(crate) fn new(client: DatabaseClient, change_stream_name: impl Into<String>) -> Self {
        Self {
            client,
            change_stream_name: change_stream_name.into(),
            start_timestamp: None,
            end_timestamp: None,
            partition_token: None,
            heartbeat_milliseconds: 10_000,
        }
    }

    /// Sets the start timestamp for the change stream query.
    ///
    /// If not set, Spanner uses the change stream's creation timestamp.
    /// Must be within the change stream retention period and ≤ now.
    pub fn with_start_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.start_timestamp = Some(ts);
        self
    }

    /// Sets the end timestamp for the change stream query.
    ///
    /// - `MUTABLE_KEY_RANGE` change streams **require** a non-null end
    ///   timestamp. The Apache Beam connector uses a rolling window of
    ///   `now + 2 minutes`.
    /// - `IMMUTABLE_KEY_RANGE` change streams accept `None` / NULL for
    ///   indefinite streaming.
    ///
    /// Defaults to `None`. Callers querying `MUTABLE_KEY_RANGE` streams
    /// **must** call this method (Spanner rejects NULL).
    pub fn with_end_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.end_timestamp = Some(ts);
        self
    }

    /// Sets the partition token for reading a specific partition.
    ///
    /// If not set, Spanner begins reading from the initial set of partitions.
    pub fn with_partition_token(mut self, token: impl Into<String>) -> Self {
        self.partition_token = Some(token.into());
        self
    }

    /// Sets the heartbeat interval in milliseconds. Defaults to 10 000 (10 s).
    ///
    /// # Errors
    ///
    /// Returns an error from [`execute`](Self::execute) if Spanner rejects the
    /// value. The valid range is 1 000 (1 s) to 300 000 (5 min).
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `ms` is outside the 1 000..=300 000 range.
    pub fn with_heartbeat_milliseconds(mut self, ms: i64) -> Self {
        debug_assert!(
            (1_000..=300_000).contains(&ms),
            "heartbeat_milliseconds must be between 1000 and 300000, got {ms}"
        );
        self.heartbeat_milliseconds = ms;
        self
    }

    /// Executes the change stream query and returns a stream of
    /// [`ChangeStreamEntry`] values.
    ///
    /// The partition mode (`MUTABLE_KEY_RANGE` vs `IMMUTABLE_KEY_RANGE`) is
    /// auto-detected from the result column type: `PROTO` for mutable,
    /// `ARRAY<STRUCT>` for immutable.
    ///
    /// # Errors
    ///
    /// Returns an error if the change stream name is invalid or the
    /// underlying `ExecuteStreamingSql` RPC fails.
    pub async fn execute(self) -> crate::Result<ChangeStreamRecordStream> {
        let escaped_name = escape_identifier(&self.change_stream_name)?;

        let sql = format!(
            "SELECT ChangeRecord FROM READ_{escaped_name}(\
             start_timestamp => @start_timestamp, \
             end_timestamp => @end_timestamp, \
             partition_token => @partition_token, \
             read_options => null, \
             heartbeat_milliseconds => @heartbeat_milliseconds\
             )"
        );

        let stmt = Statement::builder(&sql)
            .add_param("start_timestamp", &self.start_timestamp)
            .add_param("end_timestamp", &self.end_timestamp)
            .add_param("partition_token", &self.partition_token)
            .add_param("heartbeat_milliseconds", &self.heartbeat_milliseconds)
            .build();

        let tx = self.client.single_use().build();
        let result_set = tx.execute_query(stmt).await?;

        Ok(ChangeStreamRecordStream {
            result_set,
            pending: Vec::new(),
        })
    }
}

/// A stream of [`ChangeStreamEntry`] values from a change stream query.
///
/// Each call to [`next`](Self::next) pulls the next row from the underlying
/// `ResultSet`, auto-detects the wire format (`PROTO` for `MUTABLE_KEY_RANGE`,
/// `ARRAY<STRUCT>` for `IMMUTABLE_KEY_RANGE`), and yields a
/// [`ChangeStreamEntry`].
///
/// For `IMMUTABLE_KEY_RANGE`, a single row may contain multiple records in
/// the `ARRAY<STRUCT>` column; these are buffered internally and yielded
/// one at a time.
#[derive(Debug)]
pub struct ChangeStreamRecordStream {
    result_set: ResultSet,
    /// Buffer for IMMUTABLE_KEY_RANGE mode: a single row can contain
    /// multiple records in the ARRAY<STRUCT> column.
    pending: Vec<ChangeStreamEntry>,
}

impl ChangeStreamRecordStream {
    /// Returns the next [`ChangeStreamEntry`] from the stream.
    ///
    /// Returns `None` when the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying RPC stream fails or if record
    /// deserialization fails.
    pub async fn next(&mut self) -> Option<crate::Result<ChangeStreamEntry>> {
        loop {
            // Drain buffered entries first (from IMMUTABLE multi-record rows).
            if let Some(entry) = self.pending.pop() {
                return Some(Ok(entry));
            }

            let row = match self.result_set.next().await? {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };

            // Detect wire format from the first column's type code.
            let is_proto = row
                .metadata
                .column_types()
                .first()
                .map(|t| t.code() == crate::types::TypeCode::Proto)
                .unwrap_or(false);

            if is_proto {
                // MUTABLE_KEY_RANGE: single PROTO column → protobuf bytes.
                let proto_bytes: Vec<u8> = match row.try_get("ChangeRecord") {
                    Ok(b) => b,
                    Err(e) => return Some(Err(e)),
                };
                return match decode_proto_record(&proto_bytes) {
                    Ok(entry) => Some(Ok(entry)),
                    Err(e) => Some(Err(e)),
                };
            }

            // IMMUTABLE_KEY_RANGE: ARRAY<STRUCT<...>> → serde_json::Value.
            let json_value: serde_json::Value = match row.try_get("ChangeRecord") {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(crate::error::internal_error(format!(
                        "failed to read ChangeRecord column: {e}"
                    ))));
                }
            };

            match parse_json_records(&json_value) {
                Ok(mut entries) => {
                    if entries.is_empty() {
                        // Empty array row — loop to pull the next row.
                        continue;
                    }
                    // Reverse so we can pop from the back in order.
                    entries.reverse();
                    let first = entries.pop().unwrap();
                    self.pending = entries;
                    return Some(Ok(first));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ── MUTABLE_KEY_RANGE: proto decode ──

/// Decodes a protobuf-serialized `ChangeStreamRecord` into a
/// [`ChangeStreamEntry`].
fn decode_proto_record(bytes: &[u8]) -> crate::Result<ChangeStreamEntry> {
    let prost_record =
        crate::google::spanner::v1::ChangeStreamRecord::decode(bytes).map_err(|e| {
            crate::error::internal_error(format!(
                "failed to decode ChangeStreamRecord protobuf: {e}"
            ))
        })?;
    let gapic_record: ChangeStreamRecord = prost_record.cnv().map_err(|e| {
        crate::error::internal_error(format!(
            "failed to convert ChangeStreamRecord from prost to gapic: {e}"
        ))
    })?;
    change_stream_record_to_entry(gapic_record)
}

/// Converts a gapic `ChangeStreamRecord` (5-variant oneof) into our
/// unified `ChangeStreamEntry`.
fn change_stream_record_to_entry(record: ChangeStreamRecord) -> crate::Result<ChangeStreamEntry> {
    use crate::model::change_stream_record::Record;
    match record.record {
        Some(Record::DataChangeRecord(v)) => Ok(ChangeStreamEntry::DataChangeRecord(v)),
        Some(Record::HeartbeatRecord(v)) => Ok(ChangeStreamEntry::HeartbeatRecord(v)),
        Some(Record::PartitionStartRecord(v)) => Ok(ChangeStreamEntry::PartitionStartRecord(v)),
        Some(Record::PartitionEndRecord(v)) => Ok(ChangeStreamEntry::PartitionEndRecord(v)),
        Some(Record::PartitionEventRecord(v)) => Ok(ChangeStreamEntry::PartitionEventRecord(v)),
        None => Err(crate::error::internal_error(
            "ChangeStreamRecord has no record variant set".to_string(),
        )),
    }
}

// ── IMMUTABLE_KEY_RANGE: JSON / ARRAY<STRUCT> parsing ──

/// The oneof field names in `ChangeStreamRecord`. When Spanner returns a
/// struct with all fields present (inactive ones as null), the generated
/// serde deserializer errors on duplicate oneof fields. We strip null
/// keys before deserializing.
///
/// **Maintenance:** keep this list in sync with the `record` oneof in
/// `google/spanner/v1/change_stream.proto`. Both camelCase (JSON default)
/// and snake_case (proto field name) variants are listed because Spanner
/// may return either depending on the client library configuration.
const ONEOF_FIELDS: &[&str] = &[
    "dataChangeRecord",
    "data_change_record",
    "heartbeatRecord",
    "heartbeat_record",
    "partitionStartRecord",
    "partition_start_record",
    "partitionEndRecord",
    "partition_end_record",
    "partitionEventRecord",
    "partition_event_record",
];

/// Field names for the legacy `ChildPartitionsRecord`.
const CHILD_PARTITIONS_FIELDS: &[&str] = &["childPartitionsRecord", "child_partitions_record"];

/// Parses the `ARRAY<STRUCT<...>>` JSON value from an `IMMUTABLE_KEY_RANGE`
/// change stream row into a list of [`ChangeStreamEntry`] values.
fn parse_json_records(json_value: &serde_json::Value) -> crate::Result<Vec<ChangeStreamEntry>> {
    let array = match json_value {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Null => return Ok(vec![]),
        other => {
            return Err(crate::error::internal_error(format!(
                "expected array for ChangeRecord column, got: {}",
                value_type_name(other),
            )));
        }
    };

    let mut entries = Vec::with_capacity(array.len());
    for element in array {
        // Check for ChildPartitionsRecord first.
        if let Some(entry) = try_parse_child_partitions(element)? {
            entries.push(entry);
            continue;
        }
        // Otherwise parse as a standard ChangeStreamRecord (shared types).
        let cleaned = strip_null_oneof_fields(element);
        let record: ChangeStreamRecord = serde_json::from_value(cleaned).map_err(|e| {
            crate::error::internal_error(format!("failed to deserialize ChangeStreamRecord: {e}"))
        })?;
        entries.push(change_stream_record_to_entry(record)?);
    }

    Ok(entries)
}

/// If the JSON object contains a `childPartitionsRecord` (or snake_case
/// variant) key, extract and parse it as a [`ChildPartitionsRecord`].
fn try_parse_child_partitions(
    value: &serde_json::Value,
) -> crate::Result<Option<ChangeStreamEntry>> {
    let map = match value.as_object() {
        Some(m) => m,
        None => return Ok(None),
    };
    for key in CHILD_PARTITIONS_FIELDS {
        if let Some(inner) = map.get(*key) {
            if inner.is_null() {
                continue;
            }
            let cpr: ChildPartitionsRecord =
                serde_json::from_value(inner.clone()).map_err(|e| {
                    crate::error::internal_error(format!(
                        "failed to deserialize ChildPartitionsRecord: {e}"
                    ))
                })?;
            return Ok(Some(ChangeStreamEntry::ChildPartitionsRecord(Box::new(
                cpr,
            ))));
        }
    }
    Ok(None)
}

/// Remove null-valued oneof fields so the generated deserializer does not
/// see them as duplicate `record` assignments.
fn strip_null_oneof_fields(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter(|(k, v)| !(v.is_null() && ONEOF_FIELDS.contains(&k.as_str())))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            serde_json::Value::Object(filtered)
        }
        other => other.clone(),
    }
}

fn value_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── static assertions ──

    static_assertions::assert_impl_all!(ChangeStreamQueryBuilder: Send, Sync, Clone, std::fmt::Debug);
    static_assertions::assert_impl_all!(ChangeStreamRecordStream: Send, Sync, std::fmt::Debug);
    static_assertions::assert_not_impl_any!(ChangeStreamRecordStream: Clone);

    // ── identifier escaping (allowlist) ──

    #[test]
    fn escape_valid_identifier() {
        assert_eq!(escape_identifier("MyStream").unwrap(), "`MyStream`");
    }

    #[test]
    fn escape_alphanumeric_with_underscore() {
        assert_eq!(
            escape_identifier("My_Stream_123").unwrap(),
            "`My_Stream_123`"
        );
    }

    #[test]
    fn accept_name_with_hyphen() {
        assert_eq!(escape_identifier("my-stream").unwrap(), "`my-stream`");
    }

    #[test]
    fn reject_empty_name() {
        assert!(escape_identifier("").is_err());
    }

    #[test]
    fn reject_name_with_backtick() {
        assert!(escape_identifier("my`stream").is_err());
    }

    #[test]
    fn reject_name_with_semicolon() {
        assert!(escape_identifier("stream;DROP").is_err());
    }

    #[test]
    fn reject_name_with_space() {
        assert!(escape_identifier("my stream").is_err());
    }

    #[test]
    fn reject_name_with_emoji() {
        assert!(escape_identifier("My\u{1f389}Stream").is_err());
    }

    // ── protobuf decode tests ──

    /// Helper: encode a prost ChangeStreamRecord to bytes.
    fn encode_prost_record(record: crate::google::spanner::v1::ChangeStreamRecord) -> Vec<u8> {
        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn decode_heartbeat_record() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::HeartbeatRecord(
                proto::change_stream_record::HeartbeatRecord {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let hb = entry
            .as_heartbeat_record()
            .expect("should be HeartbeatRecord");
        assert!(hb.timestamp.is_some());
    }

    #[test]
    fn decode_data_change_record() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::DataChangeRecord(
                proto::change_stream_record::DataChangeRecord {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                    record_sequence: "00000001".to_string(),
                    server_transaction_id: "txn-123".to_string(),
                    is_last_record_in_transaction_in_partition: true,
                    table: "Users".to_string(),
                    column_metadata: vec![],
                    mods: vec![],
                    mod_type: proto::change_stream_record::data_change_record::ModType::Insert
                        as i32,
                    value_capture_type:
                        proto::change_stream_record::data_change_record::ValueCaptureType::OldAndNewValues
                            as i32,
                    number_of_records_in_transaction: 1,
                    number_of_partitions_in_transaction: 1,
                    transaction_tag: "".to_string(),
                    is_system_transaction: false,
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let dcr = entry
            .as_data_change_record()
            .expect("should be DataChangeRecord");
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.server_transaction_id, "txn-123");
        assert!(dcr.is_last_record_in_transaction_in_partition);
    }

    #[test]
    fn decode_partition_start_record() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::PartitionStartRecord(
                proto::change_stream_record::PartitionStartRecord {
                    start_timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                    record_sequence: "00000001".to_string(),
                    partition_tokens: vec!["token-1".to_string(), "token-2".to_string()],
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let psr = entry
            .as_partition_start_record()
            .expect("should be PartitionStartRecord");
        assert_eq!(psr.partition_tokens.len(), 2);
        assert_eq!(psr.partition_tokens[0], "token-1");
        assert_eq!(psr.partition_tokens[1], "token-2");
    }

    #[test]
    fn decode_partition_end_record() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::PartitionEndRecord(
                proto::change_stream_record::PartitionEndRecord {
                    end_timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                    record_sequence: "00000001".to_string(),
                    partition_token: "token-1".to_string(),
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let per = entry
            .as_partition_end_record()
            .expect("should be PartitionEndRecord");
        assert_eq!(per.partition_token, "token-1");
    }

    #[test]
    fn decode_partition_event_record() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::PartitionEventRecord(
                proto::change_stream_record::PartitionEventRecord {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                    record_sequence: "00000001".to_string(),
                    partition_token: "token-1".to_string(),
                    move_in_events: vec![
                        proto::change_stream_record::partition_event_record::MoveInEvent {
                            source_partition_token: "source-token".to_string(),
                        },
                    ],
                    move_out_events: vec![],
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let per = entry
            .as_partition_event_record()
            .expect("should be PartitionEventRecord");
        assert_eq!(per.partition_token, "token-1");
        assert_eq!(per.move_in_events.len(), 1);
    }

    #[test]
    fn decode_data_change_record_with_mods() {
        use crate::google::spanner::v1 as proto;

        let prost_record = proto::ChangeStreamRecord {
            record: Some(proto::change_stream_record::Record::DataChangeRecord(
                proto::change_stream_record::DataChangeRecord {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1705312200,
                        nanos: 0,
                    }),
                    record_sequence: "00000001".to_string(),
                    server_transaction_id: "txn-456".to_string(),
                    is_last_record_in_transaction_in_partition: true,
                    table: "Users".to_string(),
                    column_metadata: vec![
                        proto::change_stream_record::data_change_record::ColumnMetadata {
                            name: "Id".to_string(),
                            r#type: None,
                            is_primary_key: true,
                            ordinal_position: 1,
                        },
                        proto::change_stream_record::data_change_record::ColumnMetadata {
                            name: "Name".to_string(),
                            r#type: None,
                            is_primary_key: false,
                            ordinal_position: 2,
                        },
                    ],
                    mods: vec![proto::change_stream_record::data_change_record::Mod {
                        keys: vec![
                            proto::change_stream_record::data_change_record::ModValue {
                                column_metadata_index: 0,
                                value: Some(prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "42".to_string(),
                                    )),
                                }),
                            },
                        ],
                        old_values: vec![],
                        new_values: vec![
                            proto::change_stream_record::data_change_record::ModValue {
                                column_metadata_index: 1,
                                value: Some(prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "Alice".to_string(),
                                    )),
                                }),
                            },
                        ],
                    }],
                    mod_type: proto::change_stream_record::data_change_record::ModType::Insert
                        as i32,
                    value_capture_type:
                        proto::change_stream_record::data_change_record::ValueCaptureType::OldAndNewValues
                            as i32,
                    number_of_records_in_transaction: 1,
                    number_of_partitions_in_transaction: 1,
                    transaction_tag: "my-tag".to_string(),
                    is_system_transaction: false,
                },
            )),
        };

        let bytes = encode_prost_record(prost_record);
        let entry = decode_proto_record(&bytes).expect("decode should succeed");
        let dcr = entry.as_data_change_record().unwrap();
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.column_metadata.len(), 2);
        assert_eq!(dcr.mods.len(), 1);
        assert_eq!(dcr.mods[0].keys[0].column_metadata_index, 0);
        assert!(dcr.mods[0].keys[0].value.is_some());
        assert_eq!(dcr.mods[0].new_values[0].column_metadata_index, 1);
        assert!(dcr.mods[0].new_values[0].value.is_some());
        assert_eq!(dcr.transaction_tag, "my-tag");
    }

    #[test]
    fn decode_empty_bytes_returns_no_variant() {
        // Empty bytes decode to default proto (no record set), which
        // change_stream_record_to_entry rejects.
        let result = decode_proto_record(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_invalid_bytes_returns_error() {
        let result = decode_proto_record(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    // ── mock-server integration tests ──

    /// Verifies that `ChangeStreamQueryBuilder::execute()` sends the correct SQL
    /// and parameters to `ExecuteStreamingSql`, and that the returned
    /// `ChangeStreamRecordStream` yields correctly deserialized records.
    ///
    /// The mock returns data in the MUTABLE_KEY_RANGE wire format:
    /// - Column type: `PROTO(google.spanner.v1.ChangeStreamRecord)`
    /// - Value: base64-encoded protobuf bytes
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_heartbeat() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use base64::Engine;
        use base64::prelude::BASE64_STANDARD;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |req| {
                let req = req.into_inner();

                assert!(
                    req.sql.contains("READ_`MyStream`"),
                    "SQL should contain backtick-escaped name, got: {}",
                    req.sql
                );
                assert!(
                    req.sql.contains("start_timestamp => @start_timestamp"),
                    "SQL should use named parameters, got: {}",
                    req.sql
                );
                assert!(
                    req.sql.contains("read_options => null"),
                    "SQL should contain read_options => null, got: {}",
                    req.sql
                );

                let params = req.params.as_ref().expect("params should be set");
                let hb = params
                    .fields
                    .get("heartbeat_milliseconds")
                    .expect("heartbeat_milliseconds param missing");
                assert_eq!(
                    hb.kind,
                    Some(prost_types::value::Kind::StringValue("5000".to_string()))
                );

                // end_timestamp should be present as a param.
                assert!(
                    params.fields.contains_key("end_timestamp"),
                    "end_timestamp param missing"
                );

                // Build a proto-encoded HeartbeatRecord.
                let prost_record = crate::google::spanner::v1::ChangeStreamRecord {
                    record: Some(
                        crate::google::spanner::v1::change_stream_record::Record::HeartbeatRecord(
                            crate::google::spanner::v1::change_stream_record::HeartbeatRecord {
                                timestamp: Some(prost_types::Timestamp {
                                    seconds: 1705312200,
                                    nanos: 0,
                                }),
                            },
                        ),
                    ),
                };
                let mut proto_bytes = Vec::new();
                prost_record.encode(&mut proto_bytes).unwrap();
                let b64 = BASE64_STANDARD.encode(&proto_bytes);

                // Column type: PROTO with proto_type_fqn
                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Proto as i32,
                    proto_type_fqn: "google.spanner.v1.ChangeStreamRecord".to_string(),
                    ..Default::default()
                };

                let proto_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(b64)),
                };

                let prs = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "ChangeRecord".to_string(),
                                r#type: Some(column_type),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![proto_value],
                    last: true,
                    ..Default::default()
                };

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(prs)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mut stream = db_client
            .change_stream_query("MyStream")
            .with_heartbeat_milliseconds(5_000)
            .execute()
            .await?;

        let entry = stream.next().await.expect("should have one row")?;
        let heartbeat = entry
            .as_heartbeat_record()
            .expect("should be a heartbeat record");
        assert!(heartbeat.timestamp.is_some(), "timestamp should be present");

        assert!(stream.next().await.is_none());

        Ok(())
    }

    /// Exercises DataChangeRecord deserialization through the proto wire format.
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_data_change() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use base64::Engine;
        use base64::prelude::BASE64_STANDARD;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |_req| {
                let prost_record = crate::google::spanner::v1::ChangeStreamRecord {
                    record: Some(
                        crate::google::spanner::v1::change_stream_record::Record::DataChangeRecord(
                            crate::google::spanner::v1::change_stream_record::DataChangeRecord {
                                commit_timestamp: Some(prost_types::Timestamp {
                                    seconds: 1705312200,
                                    nanos: 0,
                                }),
                                record_sequence: "00000001".to_string(),
                                server_transaction_id: "txn-789".to_string(),
                                is_last_record_in_transaction_in_partition: true,
                                table: "Users".to_string(),
                                column_metadata: vec![],
                                mods: vec![],
                                mod_type: 10,           // INSERT
                                value_capture_type: 10, // OLD_AND_NEW_VALUES
                                number_of_records_in_transaction: 1,
                                number_of_partitions_in_transaction: 1,
                                transaction_tag: "".to_string(),
                                is_system_transaction: false,
                            },
                        ),
                    ),
                };
                let mut proto_bytes = Vec::new();
                prost_record.encode(&mut proto_bytes).unwrap();
                let b64 = BASE64_STANDARD.encode(&proto_bytes);

                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Proto as i32,
                    proto_type_fqn: "google.spanner.v1.ChangeStreamRecord".to_string(),
                    ..Default::default()
                };

                let prs = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "ChangeRecord".to_string(),
                                r#type: Some(column_type),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(b64)),
                    }],
                    last: true,
                    ..Default::default()
                };

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(prs)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mut stream = db_client.change_stream_query("MyStream").execute().await?;

        let entry = stream.next().await.expect("should have one row")?;
        let dcr = entry
            .as_data_change_record()
            .expect("should be a data change record");
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.server_transaction_id, "txn-789");
        assert!(dcr.is_last_record_in_transaction_in_partition);

        assert!(stream.next().await.is_none());

        Ok(())
    }

    /// Exercises DataChangeRecord with mods through the proto wire format.
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_data_change_with_mods() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use base64::Engine;
        use base64::prelude::BASE64_STANDARD;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |_req| {
                use crate::google::spanner::v1::change_stream_record::data_change_record as dcr;

                let prost_record = crate::google::spanner::v1::ChangeStreamRecord {
                    record: Some(
                        crate::google::spanner::v1::change_stream_record::Record::DataChangeRecord(
                            crate::google::spanner::v1::change_stream_record::DataChangeRecord {
                                commit_timestamp: Some(prost_types::Timestamp {
                                    seconds: 1705312200,
                                    nanos: 0,
                                }),
                                record_sequence: "00000001".to_string(),
                                server_transaction_id: "txn-mods".to_string(),
                                is_last_record_in_transaction_in_partition: true,
                                table: "Users".to_string(),
                                column_metadata: vec![
                                    dcr::ColumnMetadata {
                                        name: "Id".to_string(),
                                        r#type: None,
                                        is_primary_key: true,
                                        ordinal_position: 1,
                                    },
                                    dcr::ColumnMetadata {
                                        name: "Name".to_string(),
                                        r#type: None,
                                        is_primary_key: false,
                                        ordinal_position: 2,
                                    },
                                ],
                                mods: vec![dcr::Mod {
                                    keys: vec![dcr::ModValue {
                                        column_metadata_index: 0,
                                        value: Some(prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "42".to_string(),
                                            )),
                                        }),
                                    }],
                                    old_values: vec![],
                                    new_values: vec![dcr::ModValue {
                                        column_metadata_index: 1,
                                        value: Some(prost_types::Value {
                                            kind: Some(prost_types::value::Kind::StringValue(
                                                "Alice".to_string(),
                                            )),
                                        }),
                                    }],
                                }],
                                mod_type: dcr::ModType::Insert as i32,
                                value_capture_type: dcr::ValueCaptureType::OldAndNewValues as i32,
                                number_of_records_in_transaction: 1,
                                number_of_partitions_in_transaction: 1,
                                transaction_tag: "my-tag".to_string(),
                                is_system_transaction: false,
                            },
                        ),
                    ),
                };
                let mut proto_bytes = Vec::new();
                prost_record.encode(&mut proto_bytes).unwrap();
                let b64 = BASE64_STANDARD.encode(&proto_bytes);

                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Proto as i32,
                    proto_type_fqn: "google.spanner.v1.ChangeStreamRecord".to_string(),
                    ..Default::default()
                };

                let prs = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "ChangeRecord".to_string(),
                                r#type: Some(column_type),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(b64)),
                    }],
                    last: true,
                    ..Default::default()
                };

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(prs)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mut stream = db_client.change_stream_query("MyStream").execute().await?;

        let entry = stream.next().await.expect("should have one row")?;
        let dcr = entry
            .as_data_change_record()
            .expect("should be a data change record");
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.server_transaction_id, "txn-mods");
        assert_eq!(dcr.mods.len(), 1);

        let m = &dcr.mods[0];
        assert_eq!(m.keys.len(), 1);
        assert_eq!(m.keys[0].column_metadata_index, 0);
        assert!(m.keys[0].value.is_some());
        assert_eq!(m.old_values.len(), 0);
        assert_eq!(m.new_values.len(), 1);
        assert_eq!(m.new_values[0].column_metadata_index, 1);
        assert!(m.new_values[0].value.is_some());

        assert!(stream.next().await.is_none());

        Ok(())
    }

    // ── JSON / IMMUTABLE_KEY_RANGE parsing tests ──

    #[test]
    fn json_parse_heartbeat_record() {
        let json = serde_json::json!([{
            "heartbeatRecord": {
                "timestamp": "2024-01-15T10:30:00Z"
            },
            "dataChangeRecord": null,
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].as_heartbeat_record().is_some());
    }

    #[test]
    fn json_parse_data_change_record() {
        let json = serde_json::json!([{
            "dataChangeRecord": {
                "commitTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "serverTransactionId": "txn-abc",
                "isLastRecordInTransactionInPartition": true,
                "table": "Users",
                "columnMetadata": [],
                "mods": [],
                "modType": "INSERT",
                "valueCaptureType": "OLD_AND_NEW_VALUES",
                "numberOfRecordsInTransaction": 1,
                "numberOfPartitionsInTransaction": 1,
                "transactionTag": "",
                "isSystemTransaction": false
            },
            "heartbeatRecord": null,
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 1);
        let dcr = entries[0]
            .as_data_change_record()
            .expect("should be DataChangeRecord");
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.server_transaction_id, "txn-abc");
    }

    #[test]
    fn json_parse_child_partitions_record() {
        let json = serde_json::json!([{
            "childPartitionsRecord": {
                "startTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "childPartitions": [
                    {
                        "token": "child-token-1",
                        "parentPartitionTokens": ["parent-token-1"]
                    },
                    {
                        "token": "child-token-2",
                        "parentPartitionTokens": ["parent-token-1"]
                    }
                ]
            }
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 1);
        let cpr = entries[0]
            .as_child_partitions_record()
            .expect("should be ChildPartitionsRecord");
        assert_eq!(cpr.record_sequence, "00000001");
        assert_eq!(cpr.child_partitions.len(), 2);
        assert_eq!(cpr.child_partitions[0].token, "child-token-1");
        assert_eq!(
            cpr.child_partitions[0].parent_partition_tokens,
            vec!["parent-token-1"]
        );
    }

    #[test]
    fn json_parse_child_partitions_snake_case() {
        let json = serde_json::json!([{
            "child_partitions_record": {
                "startTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000002",
                "childPartitions": [
                    { "token": "tok-a", "parentPartitionTokens": [] }
                ]
            }
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].as_child_partitions_record().is_some());
    }

    #[test]
    fn json_parse_null_child_partitions_not_matched() {
        let json = serde_json::json!([{
            "childPartitionsRecord": null,
            "heartbeatRecord": {
                "timestamp": "2024-01-15T10:30:00Z"
            },
            "dataChangeRecord": null,
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].as_heartbeat_record().is_some());
    }

    #[test]
    fn json_parse_empty_array() {
        let json = serde_json::json!([]);
        let entries = parse_json_records(&json).expect("should parse");
        assert!(entries.is_empty());
    }

    #[test]
    fn json_parse_null_value() {
        let json = serde_json::Value::Null;
        let entries = parse_json_records(&json).expect("should parse");
        assert!(entries.is_empty());
    }

    #[test]
    fn json_parse_non_array_fails() {
        let json = serde_json::json!("not an array");
        assert!(parse_json_records(&json).is_err());
    }

    #[test]
    fn strip_null_oneof_keeps_populated() {
        let json = serde_json::json!({
            "heartbeatRecord": { "timestamp": "2024-01-15T10:30:00Z" },
            "dataChangeRecord": null,
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        });
        let cleaned = strip_null_oneof_fields(&json);
        let obj = cleaned.as_object().unwrap();
        assert!(obj.contains_key("heartbeatRecord"));
        assert!(!obj.contains_key("dataChangeRecord"));
        assert!(!obj.contains_key("partitionStartRecord"));
    }

    #[test]
    fn json_parse_multiple_records_in_array() {
        let json = serde_json::json!([
            {
                "heartbeatRecord": { "timestamp": "2024-01-15T10:30:00Z" },
                "dataChangeRecord": null,
                "partitionStartRecord": null,
                "partitionEndRecord": null,
                "partitionEventRecord": null
            },
            {
                "dataChangeRecord": {
                    "commitTimestamp": "2024-01-15T10:30:01Z",
                    "recordSequence": "00000001",
                    "serverTransactionId": "txn-multi",
                    "isLastRecordInTransactionInPartition": true,
                    "table": "Orders",
                    "columnMetadata": [],
                    "mods": [],
                    "modType": "INSERT",
                    "valueCaptureType": "OLD_AND_NEW_VALUES",
                    "numberOfRecordsInTransaction": 1,
                    "numberOfPartitionsInTransaction": 1,
                    "transactionTag": "",
                    "isSystemTransaction": false
                },
                "heartbeatRecord": null,
                "partitionStartRecord": null,
                "partitionEndRecord": null,
                "partitionEventRecord": null
            }
        ]);
        let entries = parse_json_records(&json).expect("should parse");
        assert_eq!(entries.len(), 2);
        assert!(entries[0].as_heartbeat_record().is_some());
        let dcr = entries[1]
            .as_data_change_record()
            .expect("second should be DataChangeRecord");
        assert_eq!(dcr.table, "Orders");
    }

    #[test]
    fn change_stream_entry_accessor_returns_none_for_wrong_variant() {
        let json = serde_json::json!([{
            "heartbeatRecord": { "timestamp": "2024-01-15T10:30:00Z" },
            "dataChangeRecord": null,
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        }]);
        let entries = parse_json_records(&json).expect("should parse");
        let entry = &entries[0];
        assert!(entry.as_heartbeat_record().is_some());
        assert!(entry.as_data_change_record().is_none());
        assert!(entry.as_partition_start_record().is_none());
        assert!(entry.as_partition_end_record().is_none());
        assert!(entry.as_partition_event_record().is_none());
        assert!(entry.as_child_partitions_record().is_none());
    }

    // ── IMMUTABLE_KEY_RANGE mock-server integration test ──

    /// Exercises the IMMUTABLE_KEY_RANGE wire format (ARRAY<STRUCT> / JSON).
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_immutable_heartbeat() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |_req| {
                // IMMUTABLE_KEY_RANGE: column type is ARRAY<STRUCT<...>>
                // The value is a JSON list_value containing one struct element.
                let heartbeat_struct = prost_types::Value {
                    kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                        fields: [
                            (
                                "heartbeatRecord".to_string(),
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StructValue(
                                        prost_types::Struct {
                                            fields: [(
                                                "timestamp".to_string(),
                                                prost_types::Value {
                                                    kind: Some(
                                                        prost_types::value::Kind::StringValue(
                                                            "2024-01-15T10:30:00Z".to_string(),
                                                        ),
                                                    ),
                                                },
                                            )]
                                            .into_iter()
                                            .collect(),
                                        },
                                    )),
                                },
                            ),
                            (
                                "dataChangeRecord".to_string(),
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NullValue(0)),
                                },
                            ),
                            (
                                "partitionStartRecord".to_string(),
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NullValue(0)),
                                },
                            ),
                            (
                                "partitionEndRecord".to_string(),
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NullValue(0)),
                                },
                            ),
                            (
                                "partitionEventRecord".to_string(),
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::NullValue(0)),
                                },
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    })),
                };

                let array_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![heartbeat_struct],
                        },
                    )),
                };

                // Column type: ARRAY<STRUCT<...>>
                let struct_type = mock_v1::StructType {
                    fields: vec![
                        mock_v1::struct_type::Field {
                            name: "heartbeatRecord".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::Struct as i32,
                                ..Default::default()
                            }),
                        },
                        mock_v1::struct_type::Field {
                            name: "dataChangeRecord".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::Struct as i32,
                                ..Default::default()
                            }),
                        },
                    ],
                };
                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Array as i32,
                    array_element_type: Some(Box::new(mock_v1::Type {
                        code: mock_v1::TypeCode::Struct as i32,
                        struct_type: Some(struct_type),
                        ..Default::default()
                    })),
                    ..Default::default()
                };

                let prs = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "ChangeRecord".to_string(),
                                r#type: Some(column_type),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![array_value],
                    last: true,
                    ..Default::default()
                };

                Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(prs)])))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mut stream = db_client
            .change_stream_query("MyStream")
            .with_heartbeat_milliseconds(5_000)
            .execute()
            .await?;

        let entry = stream.next().await.expect("should have one row")?;
        let heartbeat = entry
            .as_heartbeat_record()
            .expect("should be a heartbeat record");
        assert!(heartbeat.timestamp.is_some());

        assert!(stream.next().await.is_none());

        Ok(())
    }
}
