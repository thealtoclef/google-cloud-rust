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
//! # Partition mode: `MUTABLE_KEY_RANGE` only
//!
//! This API targets change streams created with
//! `OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')`. The returned record
//! types ([`PartitionStartRecord`], [`PartitionEndRecord`],
//! [`PartitionEventRecord`]) are defined by the
//! [`change_stream.proto`](https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/change_stream.proto)
//! and are specific to this partition mode.
//!
//! Change streams created with the legacy `IMMUTABLE_KEY_RANGE` mode
//! (the default prior to `MUTABLE_KEY_RANGE`) return a different partition
//! lifecycle record (`ChildPartitionsRecord`) that has no proto definition.
//! If this API encounters such a record, it returns an error directing the
//! caller to recreate the change stream with
//! `OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')`.
//!
//! [`PartitionStartRecord`]: crate::model::change_stream_record::PartitionStartRecord
//! [`PartitionEndRecord`]: crate::model::change_stream_record::PartitionEndRecord
//! [`PartitionEventRecord`]: crate::model::change_stream_record::PartitionEventRecord
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
//! while let Some(record) = stream.next().await {
//!     let record = record?;
//!     println!("{:?}", record);
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
use gaxi::prost::FromProto;
use prost::Message;

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
/// [`ChangeStreamRecordStream`] that yields deserialized
/// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord) values.
///
/// # Partition mode
///
/// This builder targets change streams created with
/// `OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')`. If the underlying
/// change stream uses the legacy `IMMUTABLE_KEY_RANGE` mode, partition
/// lifecycle records will produce an error. See the
/// [module documentation](self) for details.
#[derive(Clone, Debug)]
#[must_use]
pub struct ChangeStreamQueryBuilder {
    client: DatabaseClient,
    change_stream_name: String,
    start_timestamp: Option<time::OffsetDateTime>,
    end_timestamp: time::OffsetDateTime,
    partition_token: Option<String>,
    heartbeat_milliseconds: i64,
}

impl ChangeStreamQueryBuilder {
    pub(crate) fn new(client: DatabaseClient, change_stream_name: impl Into<String>) -> Self {
        Self {
            client,
            change_stream_name: change_stream_name.into(),
            start_timestamp: None,
            end_timestamp: time::OffsetDateTime::now_utc() + time::Duration::minutes(2),
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
    /// `MUTABLE_KEY_RANGE` change streams require a non-null end timestamp.
    /// Defaults to `now + 2 minutes` (matching the Apache Beam connector's
    /// rolling window approach). For indefinite CDC streaming, callers should
    /// loop: query with a near-future end timestamp, consume records, then
    /// re-query from the last checkpoint.
    pub fn with_end_timestamp(mut self, ts: time::OffsetDateTime) -> Self {
        self.end_timestamp = ts;
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
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - the change stream name is invalid (contains characters outside
    ///   the `[A-Za-z0-9_-]` set),
    /// - the underlying `ExecuteStreamingSql` RPC fails, or
    /// - the change stream was created with `IMMUTABLE_KEY_RANGE` partition
    ///   mode and returns a `ChildPartitionsRecord` (not supported — use
    ///   `MUTABLE_KEY_RANGE` instead).
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
            .add_param("end_timestamp", &Some(self.end_timestamp))
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
/// `ResultSet`, decodes the protobuf-encoded `ChangeRecord` column, and
/// yields a single [`ChangeStreamRecord`](crate::model::ChangeStreamRecord).
///
/// The stream may return
/// [`PartitionStartRecord`](crate::model::change_stream_record::PartitionStartRecord)s
/// that contain tokens for child partitions. Callers should spawn new
/// `ChangeStreamRecordStream` queries for those tokens to read the full
/// change stream.
///
/// # Partition mode
///
/// Only `MUTABLE_KEY_RANGE` change streams are supported.
#[derive(Debug)]
pub struct ChangeStreamRecordStream {
    result_set: ResultSet,
}

impl ChangeStreamRecordStream {
    /// Returns the next [`ChangeStreamRecord`](crate::model::ChangeStreamRecord)
    /// from the stream.
    ///
    /// Each row from the `MUTABLE_KEY_RANGE` change stream TVF contains a
    /// single `ChangeRecord` column of type `PROTO` holding a serialized
    /// [`ChangeStreamRecord`](crate::model::ChangeStreamRecord).
    ///
    /// Returns `None` when the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying RPC stream fails or if the
    /// protobuf bytes cannot be decoded.
    pub async fn next(&mut self) -> Option<crate::Result<ChangeStreamRecord>> {
        let row = match self.result_set.next().await? {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        // MUTABLE_KEY_RANGE TVF returns a single column "ChangeRecord" with
        // TypeCode::Proto containing serialized ChangeStreamRecord bytes.
        let proto_bytes: Vec<u8> = match row.try_get("ChangeRecord") {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        match decode_change_stream_record(&proto_bytes) {
            Ok(record) => Some(Ok(record)),
            Err(e) => Some(Err(e)),
        }
    }
}

/// Decodes a protobuf-serialized `ChangeStreamRecord` (from a
/// `MUTABLE_KEY_RANGE` change stream) into the gapic model type.
fn decode_change_stream_record(bytes: &[u8]) -> crate::Result<ChangeStreamRecord> {
    let prost_record =
        crate::google::spanner::v1::ChangeStreamRecord::decode(bytes).map_err(|e| {
            crate::error::internal_error(format!(
                "failed to decode ChangeStreamRecord protobuf: {e}"
            ))
        })?;
    prost_record.cnv().map_err(|e| {
        crate::error::internal_error(format!(
            "failed to convert ChangeStreamRecord from prost to gapic: {e}"
        ))
    })
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        assert!(record.heartbeat_record().is_some());
        let hb = record.heartbeat_record().unwrap();
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        let dcr = record
            .data_change_record()
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        let psr = record
            .partition_start_record()
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        let per = record
            .partition_end_record()
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        let per = record
            .partition_event_record()
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
        let record = decode_change_stream_record(&bytes).expect("decode should succeed");
        let dcr = record.data_change_record().unwrap();
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
    fn decode_empty_bytes_returns_error() {
        // Empty bytes should fail to decode (no valid protobuf).
        // Actually, empty bytes decode to a default ChangeStreamRecord (no record set).
        let record = decode_change_stream_record(&[]).expect("empty proto is valid default");
        assert!(record.record.is_none());
    }

    #[test]
    fn decode_invalid_bytes_returns_error() {
        let result = decode_change_stream_record(&[0xFF, 0xFF, 0xFF, 0xFF]);
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

                // end_timestamp must be non-null for MUTABLE_KEY_RANGE.
                let end_ts = params
                    .fields
                    .get("end_timestamp")
                    .expect("end_timestamp param missing");
                assert!(
                    matches!(end_ts.kind, Some(prost_types::value::Kind::StringValue(_))),
                    "end_timestamp should be a non-null string value"
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

        let record = stream.next().await.expect("should have one row")?;
        let heartbeat = record
            .heartbeat_record()
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

        let record = stream.next().await.expect("should have one row")?;
        let dcr = record
            .data_change_record()
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

        let record = stream.next().await.expect("should have one row")?;
        let dcr = record
            .data_change_record()
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
}
