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
//! let mut stream = db.change_stream_query("MyChangeStream")
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

use crate::database_client::DatabaseClient;
use crate::model::ChangeStreamRecord;
use crate::model::execute_sql_request::QueryMode;
use crate::result_set::ResultSet;
use crate::statement::Statement;

/// A builder for change stream queries.
///
/// Created by [`DatabaseClient::change_stream_query`]. The builder constructs
/// the `SELECT ChangeRecord FROM READ_<stream>(...)` TVF query with
/// `queryMode = PROFILE` so that Spanner returns structured
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
    /// If not set, Spanner uses the current time.
    pub fn with_start_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.start_timestamp = Some(ts);
        self
    }

    /// Sets the end timestamp for the change stream query.
    ///
    /// If not set, the stream runs indefinitely (until cancelled).
    pub fn with_end_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.end_timestamp = Some(ts);
        self
    }

    /// Sets the partition token for resuming a specific partition.
    pub fn with_partition_token(mut self, token: impl Into<String>) -> Self {
        self.partition_token = Some(token.into());
        self
    }

    /// Sets the heartbeat interval in milliseconds. Defaults to 10,000 (10s).
    pub fn with_heartbeat_milliseconds(mut self, ms: i64) -> Self {
        self.heartbeat_milliseconds = ms;
        self
    }

    /// Executes the change stream query and returns a stream of
    /// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
    pub async fn execute(self) -> crate::Result<ChangeStreamRecordStream> {
        let sql = format!(
            "SELECT ChangeRecord FROM READ_{}(\
             @start_timestamp, @end_timestamp, @partition_token, @heartbeat_milliseconds\
             )",
            self.change_stream_name
        );

        let start_ts_str = match self.start_timestamp {
            Some(ts) => ts
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| {
                    crate::error::internal_error(format!("failed to format start_timestamp: {e}"))
                })?,
            None => String::new(),
        };
        let end_ts_str = match self.end_timestamp {
            Some(ts) => ts
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| {
                    crate::error::internal_error(format!("failed to format end_timestamp: {e}"))
                })?,
            None => String::new(),
        };
        let partition_token = self.partition_token.unwrap_or_default();

        let stmt = Statement::builder(&sql)
            .set_query_mode(QueryMode::Profile)
            .add_param("start_timestamp", &start_ts_str)
            .add_param("end_timestamp", &end_ts_str)
            .add_param("partition_token", &partition_token)
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
    /// This method deserializes that array into a `Vec<ChangeStreamRecord>`.
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
}
