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
//! this is the same model used by the Java and Go SDKs, which do not include
//! a built-in concurrent partition scheduler either.

use crate::database_client::DatabaseClient;
use crate::model::ChangeStreamRecord;
use crate::result_set::ResultSet;
use crate::statement::Statement;

// Characters that are never valid in a GoogleSQL identifier.
// Used to reject obviously-bad change stream names.
const INVALID_IDENT_CHARS: &[char] = &['`', '"', '\'', ';', '-', ' ', '\n', '\r', '\t', '\0'];

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
        let record: ChangeStreamRecord = serde_json::from_value(element.clone()).map_err(|e| {
            crate::error::internal_error(format!("failed to deserialize ChangeStreamRecord: {e}"))
        })?;
        records.push(record);
    }

    Ok(records)
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
                        "keys": [{"column": "Id", "value": {"stringValue": "42"}}],
                        "newValues": [{"column": "Name", "value": {"stringValue": "Alice"}}],
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
        assert_eq!(dcr.transaction_tag, "my-tag");
    }

    // ── mock-server integration test ──

    /// Verifies that `ChangeStreamQueryBuilder::execute()` sends the correct SQL
    /// and parameters to `ExecuteStreamingSql`, and that the returned
    /// `ChangeStreamRecordStream` yields a correctly deserialized heartbeat record.
    #[google_cloud_test_macros::tokio_test_no_panics]
    async fn execute_end_to_end_mock() -> anyhow::Result<()> {
        use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
        use crate::result_set::tests::adapt;
        use spanner_grpc_mock::google::spanner::v1 as mock_v1;

        let mut mock = create_session_mock();

        // The change stream TVF returns a single column "ChangeRecord" of type
        // JSON. We encode a heartbeat record as a JSON string inside a
        // single-element array.
        let heartbeat_json = r#"[{"heartbeatRecord":{"timestamp":"2024-01-15T10:30:00Z"}}]"#;

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

                // Return a PartialResultSet with:
                // - metadata: 1 column (ChangeRecord) of type JSON
                // - values: the heartbeat JSON string
                let prs = mock_v1::PartialResultSet {
                    metadata: Some(mock_v1::ResultSetMetadata {
                        row_type: Some(mock_v1::StructType {
                            fields: vec![mock_v1::struct_type::Field {
                                name: "ChangeRecord".to_string(),
                                r#type: Some(mock_v1::Type {
                                    code: mock_v1::TypeCode::Json as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            heartbeat_json.to_string(),
                        )),
                    }],
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
}
