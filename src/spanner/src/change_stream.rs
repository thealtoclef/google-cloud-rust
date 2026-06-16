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
//! It wraps the underlying `ExecuteStreamingSql` RPC and yields fully-deserialized
//! [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
//!
//! # Example
//!
//! ```no_run
//! # use google_cloud_spanner::client::Spanner;
//! # async fn example() -> anyhow::Result<()> {
//! let spanner = Spanner::builder().build().await?;
//! let db = spanner
//!     .database_client("projects/p/instances/i/databases/d")
//!     .build()
//!     .await?;
//!
//! let mut stream = db
//!     .change_stream_query("MyChangeStream")
//!     .with_start_timestamp(time::OffsetDateTime::now_utc())
//!     .with_heartbeat_milliseconds(10_000)
//!     .execute()
//!     .await?;
//!
//! while let Some(records) = stream.next().await {
//!     for record in records? {
//!         println!("{:?}", record);
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Partition management
//!
//! A change stream is split across partitions whose lifetimes are controlled
//! by Spanner.  A query for a single partition may return
//! [`PartitionStartRecord`](crate::model::change_stream_record::PartitionStartRecord)s
//! that contain tokens for child partitions.  It is the caller's
//! responsibility to spawn additional queries for those child partitions —
//! this is the same model used by the other Spanner SDKs (Java, Go, Python,
//! Node.js), none of which include a built-in concurrent partition scheduler.
//! The Apache Beam Spanner Change Streams connector handles partition
//! management at the Dataflow pipeline level.

use crate::database_client::DatabaseClient;
use crate::model::ChangeStreamRecord;
use crate::result_set::ResultSet;
use crate::statement::Statement;

// Characters that are never valid in a GoogleSQL identifier.
// Used to reject obviously-bad change stream names.
const INVALID_IDENT_CHARS: &[char] = &['`', '"', '\'', ';', ' ', '\n', '\r', '\t', '\0'];

/// Validates that `name` looks like a legal Spanner identifier and returns it
/// backtick-escaped for safe interpolation into SQL.
fn escape_identifier(name: &str) -> crate::Result<String> {
    if name.is_empty() {
        return Err(crate::error::internal_error(
            "change stream name must not be empty",
        ));
    }
    if name.contains(INVALID_IDENT_CHARS) {
        return Err(crate::error::internal_error(format!(
            "change stream name contains invalid characters: {name:?}",
        )));
    }
    Ok(format!("`{name}`"))
}

/// A builder for change stream queries.
///
/// Created by [`DatabaseClient::change_stream_query`]. The builder constructs
/// the `SELECT ChangeRecord FROM READ_<stream>(...)` TVF query and returns a
/// [`ChangeStreamRecordStream`] that yields deserialized
/// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
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
    /// Required. Must be within the change stream retention period and ≤ now.
    pub fn with_start_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.start_timestamp = Some(ts);
        self
    }

    /// Sets the end timestamp for the change stream query.
    ///
    /// If not set (or set to `None`), the stream runs indefinitely until
    /// cancelled.
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
    /// Must be between 1 000 (1 s) and 300 000 (5 min).
    pub fn with_heartbeat_milliseconds(mut self, ms: i64) -> Self {
        self.heartbeat_milliseconds = ms;
        self
    }

    /// Executes the change stream query and returns a stream of
    /// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
    ///
    /// Internally this builds a TVF query of the form
    /// ```sql
    /// SELECT ChangeRecord FROM READ_`<stream>`(
    ///   start_timestamp => @start_timestamp,
    ///   end_timestamp   => @end_timestamp,
    ///   partition_token  => @partition_token,
    ///   read_options     => null,
    ///   heartbeat_milliseconds => @heartbeat_milliseconds
    /// )
    /// ```
    /// and executes it via `ExecuteStreamingSql` on a single-use read-only
    /// transaction (matching the approach used by the Apache Beam Spanner
    /// Change Streams connector).
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

        Ok(ChangeStreamRecordStream { result_set })
    }
}

/// A stream of [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values
/// from a change stream query.
///
/// Each call to [`next`](Self::next) pulls the next row from the underlying
/// `ResultSet`, parses the `ChangeRecord` column, and yields one or more
/// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
///
/// The stream may return
/// [`PartitionStartRecord`](crate::model::change_stream_record::PartitionStartRecord)s
/// that contain tokens for child partitions. Callers should spawn new
/// `ChangeStreamRecordStream` queries for those tokens to read the full
/// change stream.
#[derive(Debug)]
pub struct ChangeStreamRecordStream {
    result_set: ResultSet,
}

impl ChangeStreamRecordStream {
    /// Returns the next batch of [`ChangeStreamRecord`](crate::model::ChangeStreamRecord)
    /// values from the stream.
    ///
    /// Each row from the change stream TVF contains an
    /// `ARRAY<STRUCT<...>>` in its single `ChangeRecord` column.
    /// Per the Spanner docs the array always contains exactly one element,
    /// but this method returns a `Vec` for forward compatibility.
    ///
    /// Returns `None` when the stream is exhausted.
    pub async fn next(&mut self) -> Option<crate::Result<Vec<ChangeStreamRecord>>> {
        let row = match self.result_set.next().await? {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        // The change stream TVF returns a single column "ChangeRecord" that is
        // ARRAY<STRUCT<...>>. Use try_get::<serde_json::Value, _> to get the
        // full JSON representation, then deserialize each array element into
        // ChangeStreamRecord.
        let json_value: serde_json::Value = match row.try_get(0_usize) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        match parse_change_stream_records(&json_value) {
            Ok(records) => Some(Ok(records)),
            Err(e) => Some(Err(e)),
        }
    }
}

/// The oneof field names in `ChangeStreamRecord`. When Spanner returns a
/// struct with all 5 fields present (4 as null, 1 populated), the generated
/// serde deserializer treats each non-`None` variant as "record already set"
/// and errors. We strip null oneof keys before deserializing to avoid this.
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

fn parse_change_stream_records(
    json_value: &serde_json::Value,
) -> crate::Result<Vec<ChangeStreamRecord>> {
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

    let mut records = Vec::with_capacity(array.len());
    for element in array {
        let cleaned = strip_null_oneof_fields(element);
        let record: ChangeStreamRecord = serde_json::from_value(cleaned).map_err(|e| {
            crate::error::internal_error(format!("failed to deserialize ChangeStreamRecord: {e}"))
        })?;
        records.push(record);
    }

    Ok(records)
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

    // ── identifier escaping ──

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

    // ── JSON deserialization tests ──

    #[test]
    fn parse_data_change_record() {
        let json = serde_json::json!([{
            "dataChangeRecord": {
                "commitTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "serverTransactionId": "txn-123",
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
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].data_change_record().is_some());
    }

    #[test]
    fn parse_heartbeat_record() {
        let json = serde_json::json!([{
            "heartbeatRecord": {
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].heartbeat_record().is_some());
    }

    #[test]
    fn parse_partition_start_record() {
        let json = serde_json::json!([{
            "partitionStartRecord": {
                "startTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "partitionTokens": ["token-1", "token-2"]
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].partition_start_record().is_some());
    }

    #[test]
    fn parse_partition_end_record() {
        let json = serde_json::json!([{
            "partitionEndRecord": {
                "endTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "partitionToken": "token-1"
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].partition_end_record().is_some());
    }

    #[test]
    fn parse_partition_event_record() {
        let json = serde_json::json!([{
            "partitionEventRecord": {
                "commitTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "partitionToken": "token-1",
                "moveInEvents": [],
                "moveOutEvents": []
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].partition_event_record().is_some());
    }

    #[test]
    fn parse_empty_array() {
        let json = serde_json::json!([]);
        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert!(records.is_empty());
    }

    #[test]
    fn parse_null_value() {
        let json = serde_json::Value::Null;
        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert!(records.is_empty());
    }

    #[test]
    fn parse_multiple_records() {
        let json = serde_json::json!([
            {
                "dataChangeRecord": {
                    "commitTimestamp": "2024-01-15T10:30:00Z",
                    "recordSequence": "00000001",
                    "serverTransactionId": "txn-123",
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
                }
            },
            {
                "heartbeatRecord": {
                    "timestamp": "2024-01-15T10:31:00Z"
                }
            }
        ]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 2);
        assert!(records[0].data_change_record().is_some());
        assert!(records[1].heartbeat_record().is_some());
    }

    #[test]
    fn parse_invalid_type_returns_error() {
        let json = serde_json::json!("not an array");
        let result = parse_change_stream_records(&json);
        assert!(result.is_err());
    }

    /// When Spanner's struct_type declares all 5 oneof fields and sends null
    /// for the 4 inactive ones, the generated deserializer would error on the
    /// second null field ("multiple values for record"). The null-stripping in
    /// `parse_change_stream_records` must handle this.
    #[test]
    fn parse_record_with_null_oneof_fields() {
        let json = serde_json::json!([{
            "dataChangeRecord": null,
            "heartbeatRecord": {
                "timestamp": "2024-01-15T10:30:00Z"
            },
            "partitionStartRecord": null,
            "partitionEndRecord": null,
            "partitionEventRecord": null
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].heartbeat_record().is_some());
    }

    #[test]
    fn parse_record_with_null_oneof_fields_snake_case() {
        let json = serde_json::json!([{
            "data_change_record": null,
            "heartbeat_record": null,
            "partition_start_record": {
                "startTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "partitionTokens": ["token-1"]
            },
            "partition_end_record": null,
            "partition_event_record": null
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        assert!(records[0].partition_start_record().is_some());
    }

    #[test]
    fn parse_data_change_record_with_mods() {
        let json = serde_json::json!([{
            "dataChangeRecord": {
                "commitTimestamp": "2024-01-15T10:30:00Z",
                "recordSequence": "00000001",
                "serverTransactionId": "txn-456",
                "isLastRecordInTransactionInPartition": true,
                "table": "Users",
                "columnMetadata": [
                    {
                        "name": "Id",
                        "type": {"code": "INT64"},
                        "isPrimaryKey": true,
                        "ordinalPosition": 1
                    },
                    {
                        "name": "Name",
                        "type": {"code": "STRING"},
                        "isPrimaryKey": false,
                        "ordinalPosition": 2
                    }
                ],
                "mods": [
                    {
                        "keys": [{"columnMetadataIndex": 0, "value": {"stringValue": "42"}}],
                        "newValues": [{"columnMetadataIndex": 1, "value": {"stringValue": "Alice"}}],
                        "oldValues": []
                    }
                ],
                "modType": "INSERT",
                "valueCaptureType": "OLD_AND_NEW_VALUES",
                "numberOfRecordsInTransaction": 1,
                "numberOfPartitionsInTransaction": 1,
                "transactionTag": "my-tag",
                "isSystemTransaction": false
            }
        }]);

        let records = parse_change_stream_records(&json).expect("parse should succeed");
        assert_eq!(records.len(), 1);
        let dcr = records[0].data_change_record().unwrap();
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.column_metadata.len(), 2);
        assert_eq!(dcr.mods.len(), 1);
        // Verify the ModValue fields are correctly deserialized (not absorbed by _unknown_fields).
        assert_eq!(dcr.mods[0].keys[0].column_metadata_index, 0);
        assert!(dcr.mods[0].keys[0].value.is_some());
        assert_eq!(dcr.mods[0].new_values[0].column_metadata_index, 1);
        assert!(dcr.mods[0].new_values[0].value.is_some());
        assert_eq!(dcr.transaction_tag, "my-tag");
    }

    // ── mock-server integration tests ──

    /// Verifies that `ChangeStreamQueryBuilder::execute()` sends the correct SQL
    /// and parameters to `ExecuteStreamingSql`, and that the returned
    /// `ChangeStreamRecordStream` yields correctly deserialized records.
    ///
    /// The mock returns data in the production wire format:
    /// - Column type: `ARRAY<STRUCT<...>>` (not JSON)
    /// - Value: `ListValue` containing positional struct values
    /// This exercises the real `FromValue for serde_json::Value` code path
    /// through `list_value_to_json` with struct metadata.
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_heartbeat() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |req| {
                let req = req.into_inner();

                // Verify the SQL contains the backtick-escaped TVF name.
                assert!(
                    req.sql.contains("READ_`MyStream`"),
                    "SQL should contain backtick-escaped name, got: {}",
                    req.sql
                );

                // Verify named parameter syntax is used.
                assert!(
                    req.sql.contains("start_timestamp => @start_timestamp"),
                    "SQL should use named parameters, got: {}",
                    req.sql
                );

                // Verify read_options => null is present.
                assert!(
                    req.sql.contains("read_options => null"),
                    "SQL should contain read_options => null, got: {}",
                    req.sql
                );

                // Verify heartbeat_milliseconds param is set.
                let params = req.params.as_ref().expect("params should be set");
                let hb = params
                    .fields
                    .get("heartbeat_milliseconds")
                    .expect("heartbeat_milliseconds param missing");
                assert_eq!(
                    hb.kind,
                    Some(prost_types::value::Kind::StringValue("5000".to_string()))
                );

                // Production wire format: the ChangeRecord column is
                // ARRAY<STRUCT<heartbeat_record STRUCT<timestamp TIMESTAMP>, ...>>.
                //
                // The inner struct has a single field "heartbeat_record" which
                // is itself a struct with a "timestamp" field.
                //
                // In protobuf wire format, STRUCT values are encoded as
                // ListValue with positional elements matching the struct_type
                // fields.

                // Heartbeat record struct type: { timestamp: TIMESTAMP }
                let heartbeat_struct_type = mock_v1::StructType {
                    fields: vec![mock_v1::struct_type::Field {
                        name: "timestamp".to_string(),
                        r#type: Some(mock_v1::Type {
                            code: mock_v1::TypeCode::Timestamp as i32,
                            ..Default::default()
                        }),
                    }],
                };

                // Outer ChangeRecord struct type with one field:
                // heartbeat_record: STRUCT<timestamp TIMESTAMP>
                let change_record_struct_type = mock_v1::StructType {
                    fields: vec![mock_v1::struct_type::Field {
                        name: "heartbeat_record".to_string(),
                        r#type: Some(mock_v1::Type {
                            code: mock_v1::TypeCode::Struct as i32,
                            struct_type: Some(heartbeat_struct_type),
                            ..Default::default()
                        }),
                    }],
                };

                // Column type: ARRAY<STRUCT<...>>
                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Array as i32,
                    array_element_type: Some(Box::new(mock_v1::Type {
                        code: mock_v1::TypeCode::Struct as i32,
                        struct_type: Some(change_record_struct_type),
                        ..Default::default()
                    })),
                    ..Default::default()
                };

                // Value: an array (ListValue) containing one struct.
                // The struct is itself a ListValue with positional values:
                //   position 0 = heartbeat_record = STRUCT = ListValue[timestamp]
                let heartbeat_struct_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue(
                                    "2024-01-15T10:30:00Z".to_string(),
                                )),
                            }],
                        },
                    )),
                };

                let change_record_struct = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![heartbeat_struct_value],
                        },
                    )),
                };

                let array_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![change_record_struct],
                        },
                    )),
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

        let records = stream.next().await.expect("should have one row")?;
        assert_eq!(records.len(), 1);
        let heartbeat = records[0]
            .heartbeat_record()
            .expect("should be a heartbeat record");
        assert!(heartbeat.timestamp.is_some(), "timestamp should be present");

        // Stream should be exhausted.
        assert!(stream.next().await.is_none());

        Ok(())
    }

    /// Same as heartbeat test but exercises DataChangeRecord deserialization
    /// through the production ARRAY<STRUCT<...>> wire format.
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock_data_change() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        mock.expect_execute_streaming_sql()
            .once()
            .returning(move |_req| {
                // Simplified DataChangeRecord struct type.
                // Real wire has many fields; we include the key ones.
                let dcr_struct_type = mock_v1::StructType {
                    fields: vec![
                        mock_v1::struct_type::Field {
                            name: "commit_timestamp".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::Timestamp as i32,
                                ..Default::default()
                            }),
                        },
                        mock_v1::struct_type::Field {
                            name: "record_sequence".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::String as i32,
                                ..Default::default()
                            }),
                        },
                        mock_v1::struct_type::Field {
                            name: "server_transaction_id".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::String as i32,
                                ..Default::default()
                            }),
                        },
                        mock_v1::struct_type::Field {
                            name: "is_last_record_in_transaction_in_partition".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::Bool as i32,
                                ..Default::default()
                            }),
                        },
                        mock_v1::struct_type::Field {
                            name: "table".to_string(),
                            r#type: Some(mock_v1::Type {
                                code: mock_v1::TypeCode::String as i32,
                                ..Default::default()
                            }),
                        },
                    ],
                };

                let change_record_struct_type = mock_v1::StructType {
                    fields: vec![mock_v1::struct_type::Field {
                        name: "data_change_record".to_string(),
                        r#type: Some(mock_v1::Type {
                            code: mock_v1::TypeCode::Struct as i32,
                            struct_type: Some(dcr_struct_type),
                            ..Default::default()
                        }),
                    }],
                };

                let column_type = mock_v1::Type {
                    code: mock_v1::TypeCode::Array as i32,
                    array_element_type: Some(Box::new(mock_v1::Type {
                        code: mock_v1::TypeCode::Struct as i32,
                        struct_type: Some(change_record_struct_type),
                        ..Default::default()
                    })),
                    ..Default::default()
                };

                // DataChangeRecord struct value (positional):
                // [commit_timestamp, record_sequence, server_transaction_id,
                //  is_last_record, table]
                let dcr_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "2024-01-15T10:30:00Z".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "00000001".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "txn-789".to_string(),
                                    )),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::BoolValue(true)),
                                },
                                prost_types::Value {
                                    kind: Some(prost_types::value::Kind::StringValue(
                                        "Users".to_string(),
                                    )),
                                },
                            ],
                        },
                    )),
                };

                let change_record = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![dcr_value],
                        },
                    )),
                };

                let array_value = prost_types::Value {
                    kind: Some(prost_types::value::Kind::ListValue(
                        prost_types::ListValue {
                            values: vec![change_record],
                        },
                    )),
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

        let mut stream = db_client.change_stream_query("MyStream").execute().await?;

        let records = stream.next().await.expect("should have one row")?;
        assert_eq!(records.len(), 1);
        let dcr = records[0]
            .data_change_record()
            .expect("should be a data change record");
        assert_eq!(dcr.table, "Users");
        assert_eq!(dcr.server_transaction_id, "txn-789");
        assert!(dcr.is_last_record_in_transaction_in_partition);

        assert!(stream.next().await.is_none());

        Ok(())
    }
}
