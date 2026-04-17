use crate::pb::sqlite_value::Kind as ProtoSqliteValueKind;
use crate::pb::{ExecuteBatchItem, SqliteValue as ProtoSqliteValue};
use arrow::array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use rusqlite::types::{ToSql, Value as SqliteValue, ValueRef as SqliteValueRef};
use rusqlite::{Connection, Error as RusqliteError};
use serde::Serialize;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// 默认 Arrow IPC chunk 大小，供流式查询在内存与下游消费之间折中。
/// Default Arrow IPC chunk size used to balance memory and downstream consumption.
pub const DEFAULT_IPC_CHUNK_BYTES: usize = 1024 * 1024;

/// 单批次物化的最大行数，避免单个 Arrow record batch 过大。
/// Maximum number of rows materialized per batch to avoid oversized Arrow record batches.
pub const STREAMING_BATCH_ROWS: usize = 1000;

/// 通用 SQL 执行结果。
/// Shared SQL execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteScriptResult {
    /// 是否执行成功。
    /// Whether the execution succeeded.
    pub success: bool,
    /// 结果消息。
    /// Result message.
    pub message: String,
    /// 受影响行数。
    /// Number of affected rows.
    pub rows_changed: i64,
    /// 最近一次插入行 ID。
    /// Last inserted row id.
    pub last_insert_rowid: i64,
}

/// 通用批量执行结果。
/// Shared batch-execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteBatchResult {
    /// 是否执行成功。
    /// Whether the batch execution succeeded.
    pub success: bool,
    /// 结果消息。
    /// Result message.
    pub message: String,
    /// 受影响行数。
    /// Number of affected rows.
    pub rows_changed: i64,
    /// 最近一次插入行 ID。
    /// Last inserted row id.
    pub last_insert_rowid: i64,
    /// 实际执行的语句次数。
    /// Number of statements executed.
    pub statements_executed: i64,
}

/// JSON 查询结果。
/// JSON query result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryJsonResult {
    /// JSON 行集字符串。
    /// JSON row-set string.
    pub json_data: String,
    /// 返回行数。
    /// Number of rows returned.
    pub row_count: u64,
}

/// Arrow IPC chunk 查询结果。
/// Arrow IPC chunk query result.
#[derive(Debug)]
#[allow(dead_code)]
pub struct QueryStreamResult {
    /// 临时文件后端与 chunk 索引。
    /// Temporary-file backend and chunk index metadata.
    storage: QueryStreamStorage,
    /// 返回行数。
    /// Number of rows returned.
    pub row_count: u64,
    /// chunk 数量。
    /// Number of emitted chunks.
    pub chunk_count: u64,
    /// 总字节数。
    /// Total byte size of all chunks.
    pub total_bytes: u64,
}

/// QueryStream 单个 chunk 的文件偏移与长度信息。
/// File offset and length metadata for a single QueryStream chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryStreamChunkDescriptor {
    /// chunk 在暂存文件中的起始偏移。
    /// Starting offset of the chunk in the spool file.
    offset: u64,
    /// chunk 的字节长度。
    /// Byte length of the chunk.
    len: u64,
}

/// QueryStream 暂存后端。
/// QueryStream spool backend.
#[derive(Debug)]
#[allow(dead_code)]
struct QueryStreamStorage {
    /// 暂存文件路径。
    /// Spool file path.
    file_path: PathBuf,
    /// chunk 偏移索引。
    /// Chunk offset index.
    chunks: Vec<QueryStreamChunkDescriptor>,
}

#[allow(dead_code)]
impl QueryStreamResult {
    /// 读取指定下标的 chunk 内容。
    /// Read the chunk content at the specified index.
    pub fn read_chunk(&self, index: usize) -> Result<Vec<u8>, SqlExecCoreError> {
        let descriptor = self.chunks_descriptor(index)?;
        let mut file = File::open(&self.storage.file_path).map_err(|error| {
            SqlExecCoreError::Internal(format!(
                "open query stream spool file failed: {error}"
            ))
        })?;
        file.seek(SeekFrom::Start(descriptor.offset)).map_err(|error| {
            SqlExecCoreError::Internal(format!(
                "seek query stream spool file failed: {error}"
            ))
        })?;
        let chunk_len = usize::try_from(descriptor.len).map_err(|_| {
            SqlExecCoreError::Internal(
                "query stream chunk length exceeds usize / QueryStream chunk 长度超过 usize"
                    .to_string(),
            )
        })?;
        let mut chunk = vec![0_u8; chunk_len];
        file.read_exact(&mut chunk).map_err(|error| {
            SqlExecCoreError::Internal(format!(
                "read query stream spool chunk failed: {error}"
            ))
        })?;
        Ok(chunk)
    }

    /// 返回指定下标的 chunk 描述信息。
    /// Return the chunk descriptor at the specified index.
    fn chunks_descriptor(&self, index: usize) -> Result<QueryStreamChunkDescriptor, SqlExecCoreError> {
        self.storage.chunks.get(index).copied().ok_or_else(|| {
            SqlExecCoreError::InvalidArgument(
                "chunk index out of bounds / chunk 下标越界".to_string(),
            )
        })
    }
}

impl Drop for QueryStreamResult {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.storage.file_path);
    }
}

/// QueryStream 执行过程中的共享统计信息。
/// Shared metrics produced during QueryStream execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryStreamMetrics {
    /// 返回行数。
    /// Number of rows returned.
    pub row_count: u64,
    /// chunk 数量。
    /// Number of emitted chunks.
    pub chunk_count: u64,
    /// 总字节数。
    /// Total emitted byte size.
    pub total_bytes: u64,
}

/// 通用 SQL 核心错误。
/// Shared SQL core error.
#[derive(Debug)]
pub enum SqlExecCoreError {
    /// 调用参数无效。
    /// Invalid caller input.
    InvalidArgument(String),
    /// SQLite 执行错误。
    /// SQLite execution error.
    Sqlite {
        /// 错误前缀。
        /// Error prefix.
        prefix: &'static str,
        /// 底层 SQLite 错误。
        /// Underlying SQLite error.
        error: RusqliteError,
    },
    /// 内部序列化或 Arrow 处理错误。
    /// Internal serialization or Arrow processing error.
    Internal(String),
}

impl fmt::Display for SqlExecCoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(f, "{message}"),
            Self::Sqlite { prefix, error } => write!(f, "{prefix}: {error}"),
            Self::Internal(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for SqlExecCoreError {}

/// 从 gRPC typed params 与 `params_json` 中解析最终的 SQLite 参数列表。
/// Parse the final SQLite parameter list from gRPC typed params and `params_json`.
pub fn parse_request_params(
    params: &[ProtoSqliteValue],
    params_json: &str,
) -> Result<Vec<SqliteValue>, SqlExecCoreError> {
    if !params.is_empty() {
        if !params_json.trim().is_empty() {
            return Err(SqlExecCoreError::InvalidArgument(
                "provide either flat params or params_json, but not both".to_string(),
            ));
        }

        return params
            .iter()
            .map(proto_param_to_sqlite_value)
            .collect::<Result<Vec<_>, _>>();
    }

    parse_legacy_params_json(params_json)
}

/// 从 gRPC 批量参数中解析最终批量 SQLite 参数列表。
/// Parse the final batch SQLite parameter list from gRPC batch params.
pub fn parse_batch_params(
    items: &[ExecuteBatchItem],
) -> Result<Vec<Vec<SqliteValue>>, SqlExecCoreError> {
    items
        .iter()
        .map(|item| parse_request_params(&item.params, ""))
        .collect()
}

/// 从 legacy `params_json` 字符串解析参数列表。
/// Parse parameter list from the legacy `params_json` string.
pub fn parse_legacy_params_json(params_json: &str) -> Result<Vec<SqliteValue>, SqlExecCoreError> {
    if params_json.trim().is_empty() {
        return Ok(Vec::new());
    }

    let params = serde_json::from_str::<JsonValue>(params_json).map_err(|err| {
        SqlExecCoreError::InvalidArgument(format!(
            "params_json must be a JSON array of scalar values: {err}"
        ))
    })?;

    let items = params.as_array().ok_or_else(|| {
        SqlExecCoreError::InvalidArgument(
            "params_json must be a JSON array of scalar values".to_string(),
        )
    })?;

    items
        .iter()
        .cloned()
        .map(json_param_to_sqlite_value)
        .collect()
}

/// 解析 typed 单值到 SQLite 值。
/// Convert a typed protobuf value to a SQLite value.
pub fn proto_param_to_sqlite_value(
    value: &ProtoSqliteValue,
) -> Result<SqliteValue, SqlExecCoreError> {
    match value.kind.as_ref() {
        Some(ProtoSqliteValueKind::Int64Value(value)) => Ok(SqliteValue::Integer(*value)),
        Some(ProtoSqliteValueKind::Float64Value(value)) => Ok(SqliteValue::Real(*value)),
        Some(ProtoSqliteValueKind::StringValue(value)) => Ok(SqliteValue::Text(value.clone())),
        Some(ProtoSqliteValueKind::BytesValue(value)) => Ok(SqliteValue::Blob(value.clone())),
        Some(ProtoSqliteValueKind::BoolValue(value)) => Ok(SqliteValue::Integer(i64::from(*value))),
        Some(ProtoSqliteValueKind::NullValue(_)) => Ok(SqliteValue::Null),
        None => Err(SqlExecCoreError::InvalidArgument(
            "SqliteValue.kind must be set for every bound parameter".to_string(),
        )),
    }
}

/// 解析 JSON 标量到 SQLite 值。
/// Convert a JSON scalar into a SQLite value.
pub fn json_param_to_sqlite_value(value: JsonValue) -> Result<SqliteValue, SqlExecCoreError> {
    match value {
        JsonValue::Null => Ok(SqliteValue::Null),
        JsonValue::Bool(value) => Ok(SqliteValue::Integer(i64::from(value))),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqliteValue::Integer(value))
            } else if let Some(value) = value.as_u64() {
                Ok(SqliteValue::Integer(i64::try_from(value).map_err(|_| {
                    SqlExecCoreError::InvalidArgument(
                        "params_json contains an unsigned integer larger than SQLite signed 64-bit range"
                            .to_string(),
                    )
                })?))
            } else if let Some(value) = value.as_f64() {
                Ok(SqliteValue::Real(value))
            } else {
                Err(SqlExecCoreError::InvalidArgument(
                    "params_json contains an unsupported numeric value".to_string(),
                ))
            }
        }
        JsonValue::String(value) => Ok(SqliteValue::Text(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(SqlExecCoreError::InvalidArgument(
            "params_json only supports scalar JSON values (null, bool, number, string)".to_string(),
        )),
    }
}

/// 执行脚本或单条语句。
/// Execute a script or a single statement.
pub fn execute_script(
    conn: &mut Connection,
    sql: &str,
    bound_values: &[SqliteValue],
) -> Result<ExecuteScriptResult, SqlExecCoreError> {
    if sql.trim().is_empty() {
        return Err(SqlExecCoreError::InvalidArgument(
            "sql must not be empty".to_string(),
        ));
    }

    if bound_values.is_empty() {
        conn.execute_batch(sql).map_err(|error| SqlExecCoreError::Sqlite {
            prefix: "sqlite execute_batch failed",
            error,
        })?;

        return Ok(ExecuteScriptResult {
            success: true,
            message: "script executed successfully".to_string(),
            rows_changed: i64::try_from(conn.changes()).unwrap_or(i64::MAX),
            last_insert_rowid: conn.last_insert_rowid(),
        });
    }

    if has_multiple_sql_statements(sql) {
        return Err(SqlExecCoreError::InvalidArgument(
            "flat params or params_json are only supported for a single SQL statement".to_string(),
        ));
    }

    let mut stmt = conn.prepare(sql).map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite prepare failed",
        error,
    })?;
    let params = bind_values_as_params(bound_values);
    let rows_changed = stmt
        .execute(params.as_slice())
        .map_err(|error| SqlExecCoreError::Sqlite {
            prefix: "sqlite execute failed",
            error,
        })?;

    Ok(ExecuteScriptResult {
        success: true,
        message: format!("statement executed successfully (rows_changed={rows_changed})"),
        rows_changed: i64::try_from(rows_changed).unwrap_or(i64::MAX),
        last_insert_rowid: conn.last_insert_rowid(),
    })
}

/// 执行同 SQL 多组参数的批量执行。
/// Execute a single SQL statement repeatedly with multiple parameter groups.
pub fn execute_batch(
    conn: &mut Connection,
    sql: &str,
    batch_params: &[Vec<SqliteValue>],
) -> Result<ExecuteBatchResult, SqlExecCoreError> {
    if sql.trim().is_empty() {
        return Err(SqlExecCoreError::InvalidArgument(
            "sql must not be empty".to_string(),
        ));
    }
    if batch_params.is_empty() {
        return Err(SqlExecCoreError::InvalidArgument(
            "items must not be empty".to_string(),
        ));
    }
    if has_multiple_sql_statements(sql) {
        return Err(SqlExecCoreError::InvalidArgument(
            "execute_batch only supports a single SQL statement".to_string(),
        ));
    }

    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|error| SqlExecCoreError::Sqlite {
            prefix: "sqlite BEGIN TRANSACTION failed",
            error,
        })?;

    let batch_result = (|| -> Result<ExecuteBatchResult, SqlExecCoreError> {
        let mut stmt = conn.prepare(sql).map_err(|error| SqlExecCoreError::Sqlite {
            prefix: "sqlite prepare failed",
            error,
        })?;

        let mut rows_changed = 0_i64;
        for params in batch_params {
            let params = bind_values_as_params(params);
            let changed = stmt
                .execute(params.as_slice())
                .map_err(|error| SqlExecCoreError::Sqlite {
                    prefix: "sqlite execute failed",
                    error,
                })?;
            rows_changed = rows_changed.saturating_add(i64::try_from(changed).unwrap_or(i64::MAX));
        }

        drop(stmt);
        conn.execute_batch("COMMIT")
            .map_err(|error| SqlExecCoreError::Sqlite {
                prefix: "sqlite COMMIT failed",
                error,
            })?;

        Ok(ExecuteBatchResult {
            success: true,
            message: format!(
                "batch executed successfully (statements_executed={} rows_changed={rows_changed})",
                batch_params.len()
            ),
            rows_changed,
            last_insert_rowid: conn.last_insert_rowid(),
            statements_executed: i64::try_from(batch_params.len()).unwrap_or(i64::MAX),
        })
    })();

    if batch_result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }

    batch_result
}

/// 执行查询并返回 JSON 行集。
/// Execute a query and return a JSON row set.
pub fn query_json(
    conn: &mut Connection,
    sql: &str,
    bound_values: &[SqliteValue],
) -> Result<QueryJsonResult, SqlExecCoreError> {
    if sql.trim().is_empty() {
        return Err(SqlExecCoreError::InvalidArgument(
            "sql must not be empty".to_string(),
        ));
    }
    if has_multiple_sql_statements(sql) {
        return Err(SqlExecCoreError::InvalidArgument(
            "query_json only supports a single SQL statement".to_string(),
        ));
    }

    let mut stmt = conn.prepare(sql).map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite prepare failed",
        error,
    })?;
    let column_names = stmt
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let params = bind_values_as_params(bound_values);
    let mut rows = stmt.query(params.as_slice()).map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite query failed",
        error,
    })?;

    let mut json_rows = Vec::<JsonValue>::new();
    while let Some(row) = rows.next().map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite row fetch failed",
        error,
    })? {
        let mut object = JsonMap::new();
        for (index, column_name) in column_names.iter().enumerate() {
            let value = row
                .get_ref(index)
                .map_err(|error| SqlExecCoreError::Sqlite {
                    prefix: "sqlite value access failed",
                    error,
                })?;
            object.insert(column_name.clone(), sqlite_value_ref_to_json(value));
        }
        json_rows.push(JsonValue::Object(object));
    }

    let row_count = u64::try_from(json_rows.len()).unwrap_or(u64::MAX);
    let json_data = serde_json::to_string(&json_rows).map_err(|error| {
        SqlExecCoreError::Internal(format!("serialize JSON result failed: {error}"))
    })?;

    Ok(QueryJsonResult {
        json_data,
        row_count,
    })
}

/// 执行查询并返回 Arrow IPC chunk 列表。
/// Execute a query and return Arrow IPC chunks.
#[allow(dead_code)]
pub fn query_stream(
    conn: &mut Connection,
    sql: &str,
    bound_values: &[SqliteValue],
    target_chunk_size: usize,
) -> Result<QueryStreamResult, SqlExecCoreError> {
    let writer = TempFileChunkWriter::new(target_chunk_size)?;
    let (writer, metrics) = query_stream_with_writer(conn, sql, bound_values, writer)?;

    Ok(QueryStreamResult {
        storage: writer.into_storage(),
        row_count: metrics.row_count,
        chunk_count: metrics.chunk_count,
        total_bytes: metrics.total_bytes,
    })
}

/// 执行查询并把 Arrow IPC chunk 写入任意 `Write` 接口。
/// Execute a query and write Arrow IPC chunks into any `Write` sink.
pub fn query_stream_with_writer<W: QueryStreamChunkWriter>(
    conn: &mut Connection,
    sql: &str,
    bound_values: &[SqliteValue],
    writer: W,
) -> Result<(W, QueryStreamMetrics), SqlExecCoreError> {
    if sql.trim().is_empty() {
        return Err(SqlExecCoreError::InvalidArgument(
            "sql must not be empty".to_string(),
        ));
    }
    if has_multiple_sql_statements(sql) {
        return Err(SqlExecCoreError::InvalidArgument(
            "query_stream only supports a single SQL statement".to_string(),
        ));
    }

    let mut stmt = conn.prepare(sql).map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite prepare failed",
        error,
    })?;
    let columns = stmt.columns();
    let column_names = columns
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    let declared_types = columns
        .iter()
        .map(|column| column.decl_type().map(|value| value.to_string()))
        .collect::<Vec<_>>();
    let params = bind_values_as_params(bound_values);
    let mut rows = stmt.query(params.as_slice()).map_err(|error| SqlExecCoreError::Sqlite {
        prefix: "sqlite query failed",
        error,
    })?;

    let mut chunk_writer = Some(writer);
    let mut ipc_writer: Option<StreamWriter<W>> = None;
    let mut schema: Option<Arc<Schema>> = None;
    let mut column_kinds: Option<Vec<ArrowColumnKind>> = None;
    let mut total_rows: usize = 0;

    loop {
        let mut batch_rows = Vec::<Vec<SqliteValue>>::new();
        while batch_rows.len() < STREAMING_BATCH_ROWS {
            match rows.next().map_err(|error| SqlExecCoreError::Sqlite {
                prefix: "sqlite row fetch failed",
                error,
            })? {
                Some(row) => {
                    let mut values = Vec::with_capacity(column_names.len());
                    for index in 0..column_names.len() {
                        let value = row
                            .get_ref(index)
                            .map_err(|error| SqlExecCoreError::Sqlite {
                                prefix: "sqlite value access failed",
                                error,
                            })?;
                        values.push(SqliteValue::try_from(value).map_err(|error| {
                            SqlExecCoreError::Sqlite {
                                prefix: "sqlite value conversion failed while materializing rows",
                                error: RusqliteError::FromSqlConversionFailure(
                                    index,
                                    value.data_type(),
                                    Box::new(error),
                                ),
                            }
                        })?);
                    }
                    batch_rows.push(values);
                }
                None => break,
            }
        }

        if batch_rows.is_empty() {
            break;
        }

        total_rows += batch_rows.len();

        if ipc_writer.is_none() {
            column_kinds = Some(infer_column_kinds(&declared_types, &batch_rows));
            schema = Some(Arc::new(Schema::new(
                column_names
                    .iter()
                    .zip(column_kinds.as_ref().unwrap().iter())
                    .map(|(name, kind)| {
                        Field::new(
                            name,
                            match kind {
                                ArrowColumnKind::Int64 => DataType::Int64,
                                ArrowColumnKind::Float64 => DataType::Float64,
                                ArrowColumnKind::Utf8 => DataType::Utf8,
                                ArrowColumnKind::Binary => DataType::Binary,
                            },
                            true,
                        )
                    })
                    .collect::<Vec<_>>(),
            )));

            let writer = StreamWriter::try_new(chunk_writer.take().unwrap(), schema.as_ref().unwrap())
                .map_err(|error| {
                SqlExecCoreError::Internal(format!(
                    "arrow stream header write failed: {error}"
                ))
            })?;
            ipc_writer = Some(writer);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(schema.as_ref().unwrap()),
            build_arrow_arrays(column_kinds.as_ref().unwrap(), &batch_rows),
        )
        .map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow record batch build failed: {error}"))
        })?;

        let writer = ipc_writer.as_mut().unwrap();
        writer.write(&batch).map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow batch write failed: {error}"))
        })?;
        writer.flush().map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow batch flush failed: {error}"))
        })?;
    }

    let (writer, chunk_count, total_bytes) = if let Some(mut writer) = ipc_writer {
        writer.finish().map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow stream finish failed: {error}"))
        })?;
        writer.flush().map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow final flush failed: {error}"))
        })?;
        let writer = writer.into_inner().map_err(|error| {
            SqlExecCoreError::Internal(format!("arrow stream finalize failed: {error}"))
        })?;
        let chunk_count = writer.emitted_chunk_count();
        let total_bytes = writer.emitted_total_bytes();
        (writer, chunk_count, total_bytes)
    } else {
        (chunk_writer.take().expect("writer should remain available"), 0, 0)
    };

    Ok((writer, QueryStreamMetrics {
        row_count: u64::try_from(total_rows).unwrap_or(u64::MAX),
        chunk_count,
        total_bytes,
    }))
}

/// 检测 SQL 是否包含多条语句。
/// Detect whether the SQL string contains multiple statements.
pub fn has_multiple_sql_statements(sql: &str) -> bool {
    count_sql_statements(sql) > 1
}

/// 统计 SQL 中实际包含的有效语句数量。
/// Count the number of effective SQL statements contained in the input string.
pub fn count_sql_statements(sql: &str) -> usize {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut statement_count = 0;
    let mut has_content = false;

    while i < len {
        match chars[i] {
            '\'' => {
                has_content = true;
                i += 1;
                while i < len {
                    if chars[i] == '\'' {
                        if i + 1 < len && chars[i + 1] == '\'' {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            '"' => {
                has_content = true;
                i += 1;
                while i < len {
                    if chars[i] == '"' {
                        if i + 1 < len && chars[i + 1] == '"' {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            '-' if i + 1 < len && chars[i + 1] == '-' => {
                i += 2;
                while i < len && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < len && chars[i + 1] == '*' => {
                i += 2;
                while i + 1 < len {
                    if chars[i] == '*' && chars[i + 1] == '/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            ';' => {
                if has_content {
                    statement_count += 1;
                }
                has_content = false;
                i += 1;
            }
            c if !c.is_whitespace() => {
                has_content = true;
                i += 1;
            }
            _ => i += 1,
        }
    }

    if has_content {
        statement_count += 1;
    }

    statement_count
}

/// 把 SQLite 值转换为 JSON 值。
/// Convert a SQLite value to a JSON value.
pub fn sqlite_value_to_json(value: &SqliteValue) -> JsonValue {
    match value {
        SqliteValue::Null => JsonValue::Null,
        SqliteValue::Integer(value) => JsonValue::from(*value),
        SqliteValue::Real(value) => json_float(*value),
        SqliteValue::Text(value) => JsonValue::String(value.clone()),
        SqliteValue::Blob(value) => JsonValue::Array(
            value
                .iter()
                .map(|byte| JsonValue::from(u64::from(*byte)))
                .collect(),
        ),
    }
}

fn bind_values_as_params(values: &[SqliteValue]) -> Vec<&dyn ToSql> {
    values.iter().map(|value| value as &dyn ToSql).collect()
}

fn sqlite_value_ref_to_json(value: SqliteValueRef<'_>) -> JsonValue {
    match SqliteValue::try_from(value) {
        Ok(value) => sqlite_value_to_json(&value),
        Err(_) => JsonValue::Null,
    }
}

#[derive(Copy, Clone, Debug)]
enum ArrowColumnKind {
    Int64,
    Float64,
    Utf8,
    Binary,
}

fn infer_column_kinds(
    declared_types: &[Option<String>],
    rows: &[Vec<SqliteValue>],
) -> Vec<ArrowColumnKind> {
    let column_count = declared_types.len();
    let mut kinds = Vec::with_capacity(column_count);

    for index in 0..column_count {
        let mut current = declared_type_hint(declared_types[index].as_deref());
        for row in rows {
            current = merge_column_kind(current, &row[index]);
        }
        kinds.push(current.unwrap_or(ArrowColumnKind::Utf8));
    }

    kinds
}

fn declared_type_hint(value: Option<&str>) -> Option<ArrowColumnKind> {
    let normalized = value?.trim().to_ascii_uppercase();

    if normalized.contains("INT") || normalized.contains("BOOL") {
        Some(ArrowColumnKind::Int64)
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
        || normalized.contains("NUMERIC")
        || normalized.contains("DEC")
    {
        Some(ArrowColumnKind::Float64)
    } else if normalized.contains("BLOB") {
        Some(ArrowColumnKind::Binary)
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
        || normalized.contains("JSON")
        || normalized.contains("DATE")
        || normalized.contains("TIME")
    {
        Some(ArrowColumnKind::Utf8)
    } else {
        None
    }
}

fn merge_column_kind(
    current: Option<ArrowColumnKind>,
    value: &SqliteValue,
) -> Option<ArrowColumnKind> {
    match value {
        SqliteValue::Null => current,
        SqliteValue::Integer(_) => Some(match current {
            None => ArrowColumnKind::Int64,
            Some(ArrowColumnKind::Int64) => ArrowColumnKind::Int64,
            Some(ArrowColumnKind::Float64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Utf8) => ArrowColumnKind::Utf8,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Utf8,
        }),
        SqliteValue::Real(_) => Some(match current {
            None => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Int64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Float64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Utf8) => ArrowColumnKind::Utf8,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Utf8,
        }),
        SqliteValue::Text(_) => Some(ArrowColumnKind::Utf8),
        SqliteValue::Blob(_) => Some(match current {
            None => ArrowColumnKind::Binary,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Binary,
            _ => ArrowColumnKind::Utf8,
        }),
    }
}

fn build_arrow_arrays(kinds: &[ArrowColumnKind], rows: &[Vec<SqliteValue>]) -> Vec<ArrayRef> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(kinds.len());

    for (index, kind) in kinds.iter().enumerate() {
        match kind {
            ArrowColumnKind::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Integer(value) => builder.append_value(*value),
                        SqliteValue::Real(value) => builder.append_value(*value as i64),
                        SqliteValue::Text(_) | SqliteValue::Blob(_) => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Integer(value) => builder.append_value(*value as f64),
                        SqliteValue::Real(value) => builder.append_value(*value),
                        SqliteValue::Text(_) | SqliteValue::Blob(_) => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    match sqlite_value_to_text(&row[index]) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Binary => {
                let mut builder = BinaryBuilder::new();
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Blob(value) => builder.append_value(value),
                        other => builder.append_value(
                            sqlite_value_to_text(other).unwrap_or_default().as_bytes(),
                        ),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
        }
    }

    arrays
}

fn sqlite_value_to_text(value: &SqliteValue) -> Option<String> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Integer(value) => Some(value.to_string()),
        SqliteValue::Real(value) => Some(format_float(*value)),
        SqliteValue::Text(value) => Some(value.clone()),
        SqliteValue::Blob(value) => Some(format!("x'{}'", blob_to_hex(value))),
    }
}

fn blob_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn json_float(value: f64) -> JsonValue {
    if value.is_nan() || value.is_infinite() {
        return JsonValue::String(format_float(value));
    }
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or_else(|| JsonValue::String(format_float(value)))
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// Arrow IPC chunk 收集器。
/// Arrow IPC chunk collector.
pub trait QueryStreamChunkWriter: Write {
    /// 返回当前已经写出的 chunk 数量。
    /// Return the number of chunks emitted so far.
    fn emitted_chunk_count(&self) -> u64;

    /// 返回当前已经写出的总字节数。
    /// Return the total emitted byte size so far.
    fn emitted_total_bytes(&self) -> u64;
}

/// Arrow IPC chunk 收集器。
/// Arrow IPC chunk collector.
#[allow(dead_code)]
pub struct ChunkCollector {
    chunks: Vec<Vec<u8>>,
    pending: Vec<u8>,
    target_chunk_size: usize,
    emitted_chunks: usize,
    emitted_bytes: usize,
}

#[allow(dead_code)]
impl ChunkCollector {
    fn new(target_chunk_size: usize) -> Self {
        let chunk_size = target_chunk_size.max(64 * 1024);
        Self {
            chunks: Vec::new(),
            pending: Vec::with_capacity(chunk_size),
            target_chunk_size: chunk_size,
            emitted_chunks: 0,
            emitted_bytes: 0,
        }
    }

    fn emit_full_chunks(&mut self) {
        while self.pending.len() >= self.target_chunk_size {
            let remainder = self.pending.split_off(self.target_chunk_size);
            let chunk = std::mem::replace(&mut self.pending, remainder);
            self.send_chunk(chunk);
        }
    }

    fn emit_remaining(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let chunk = std::mem::take(&mut self.pending);
        self.send_chunk(chunk);
    }

    fn send_chunk(&mut self, chunk: Vec<u8>) {
        self.emitted_chunks += 1;
        self.emitted_bytes += chunk.len();
        self.chunks.push(chunk);
    }
}

/// 基于临时文件的 QueryStream chunk 写入器。
/// Temporary-file-backed QueryStream chunk writer.
pub struct TempFileChunkWriter {
    file: File,
    file_path: PathBuf,
    pending: Vec<u8>,
    target_chunk_size: usize,
    emitted_chunks: usize,
    emitted_bytes: usize,
    current_offset: u64,
    chunk_descriptors: Vec<QueryStreamChunkDescriptor>,
}

static NEXT_QUERY_STREAM_SPOOL_ID: AtomicU64 = AtomicU64::new(1);

impl TempFileChunkWriter {
    fn new(target_chunk_size: usize) -> Result<Self, SqlExecCoreError> {
        let chunk_size = target_chunk_size.max(64 * 1024);
        let file_path = make_query_stream_spool_path();
        let file = File::create(&file_path).map_err(|error| {
            SqlExecCoreError::Internal(format!(
                "create query stream spool file failed: {error}"
            ))
        })?;
        Ok(Self {
            file,
            file_path,
            pending: Vec::with_capacity(chunk_size),
            target_chunk_size: chunk_size,
            emitted_chunks: 0,
            emitted_bytes: 0,
            current_offset: 0,
            chunk_descriptors: Vec::new(),
        })
    }

    fn into_storage(self) -> QueryStreamStorage {
        QueryStreamStorage {
            file_path: self.file_path,
            chunks: self.chunk_descriptors,
        }
    }

    fn emit_full_chunks(&mut self) -> io::Result<()> {
        while self.pending.len() >= self.target_chunk_size {
            let remainder = self.pending.split_off(self.target_chunk_size);
            let chunk = std::mem::replace(&mut self.pending, remainder);
            self.write_chunk(chunk)?;
        }
        Ok(())
    }

    fn emit_remaining(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let chunk = std::mem::take(&mut self.pending);
        self.write_chunk(chunk)
    }

    fn write_chunk(&mut self, chunk: Vec<u8>) -> io::Result<()> {
        self.file.write_all(&chunk)?;
        let chunk_len_u64 = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.chunk_descriptors.push(QueryStreamChunkDescriptor {
            offset: self.current_offset,
            len: chunk_len_u64,
        });
        self.current_offset = self.current_offset.saturating_add(chunk_len_u64);
        self.emitted_chunks += 1;
        self.emitted_bytes += chunk.len();
        Ok(())
    }
}

impl QueryStreamChunkWriter for TempFileChunkWriter {
    fn emitted_chunk_count(&self) -> u64 {
        u64::try_from(self.emitted_chunks).unwrap_or(u64::MAX)
    }

    fn emitted_total_bytes(&self) -> u64 {
        u64::try_from(self.emitted_bytes).unwrap_or(u64::MAX)
    }
}

impl Write for TempFileChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.pending.extend_from_slice(buf);
        self.emit_full_chunks()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_remaining()?;
        self.file.flush()
    }
}

fn make_query_stream_spool_path() -> PathBuf {
    let unique = NEXT_QUERY_STREAM_SPOOL_ID.fetch_add(1, Ordering::Relaxed);
    let file_name = format!(
        "vldb-sqlite-query-stream-{}-{}-{}.bin",
        std::process::id(),
        unique,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    std::env::temp_dir().join(file_name)
}

impl QueryStreamChunkWriter for ChunkCollector {
    fn emitted_chunk_count(&self) -> u64 {
        u64::try_from(self.emitted_chunks).unwrap_or(u64::MAX)
    }

    fn emitted_total_bytes(&self) -> u64 {
        u64::try_from(self.emitted_bytes).unwrap_or(u64::MAX)
    }
}

impl Write for ChunkCollector {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.pending.extend_from_slice(buf);
        self.emit_full_chunks();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_remaining();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_IPC_CHUNK_BYTES, ExecuteBatchResult, ExecuteScriptResult, count_sql_statements,
        has_multiple_sql_statements, json_param_to_sqlite_value, parse_legacy_params_json, query_json,
        query_stream,
    };
    use rusqlite::Connection;
    use rusqlite::types::Value as SqliteValue;
    use serde_json::json;

    /// 创建临时内存连接并初始化一张测试表。
    /// Create a temporary in-memory connection and initialize a test table.
    fn open_test_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory sqlite should open");
        conn.execute_batch(
            "CREATE TABLE demo(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score REAL, ok INTEGER);",
        )
        .expect("demo schema should initialize");
        conn
    }

    #[test]
    fn parse_legacy_params_json_supports_scalar_values() {
        let parsed = parse_legacy_params_json("[1,2.5,true,\"hello\",null]")
            .expect("params_json should parse");
        assert_eq!(
            parsed,
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Real(2.5),
                SqliteValue::Integer(1),
                SqliteValue::Text("hello".to_string()),
                SqliteValue::Null,
            ]
        );
    }

    #[test]
    fn json_param_to_sqlite_value_rejects_nested_values() {
        let err = json_param_to_sqlite_value(json!({"nested":true})).expect_err("nested JSON should fail");
        assert!(err.to_string().contains("scalar JSON values"));
    }

    #[test]
    fn execute_and_query_round_trip() {
        let mut conn = open_test_connection();
        let execute = super::execute_script(
            &mut conn,
            "INSERT INTO demo(name, score, ok) VALUES (?1, ?2, ?3)",
            &[
                SqliteValue::Text("alpha".to_string()),
                SqliteValue::Real(7.5),
                SqliteValue::Integer(1),
            ],
        )
        .expect("insert should succeed");
        assert_eq!(
            execute,
            ExecuteScriptResult {
                success: true,
                message: "statement executed successfully (rows_changed=1)".to_string(),
                rows_changed: 1,
                last_insert_rowid: 1,
            }
        );

        let queried = query_json(
            &mut conn,
            "SELECT id, name, score, ok FROM demo ORDER BY id",
            &[],
        )
        .expect("query_json should succeed");
        assert_eq!(queried.row_count, 1);
        assert!(queried.json_data.contains("\"alpha\""));
    }

    #[test]
    fn execute_batch_runs_multiple_parameter_sets() {
        let mut conn = open_test_connection();
        let batch = super::execute_batch(
            &mut conn,
            "INSERT INTO demo(name, score, ok) VALUES (?1, ?2, ?3)",
            &[
                vec![
                    SqliteValue::Text("alpha".to_string()),
                    SqliteValue::Real(1.5),
                    SqliteValue::Integer(1),
                ],
                vec![
                    SqliteValue::Text("beta".to_string()),
                    SqliteValue::Real(2.5),
                    SqliteValue::Integer(0),
                ],
            ],
        )
        .expect("batch should succeed");
        assert_eq!(
            batch,
            ExecuteBatchResult {
                success: true,
                message:
                    "batch executed successfully (statements_executed=2 rows_changed=2)".to_string(),
                rows_changed: 2,
                last_insert_rowid: 2,
                statements_executed: 2,
            }
        );
    }

    #[test]
    fn query_stream_returns_ipc_chunks() {
        let mut conn = open_test_connection();
        conn.execute(
            "INSERT INTO demo(name, score, ok) VALUES (?1, ?2, ?3)",
            ("alpha", 7.5_f64, 1_i64),
        )
        .expect("insert should succeed");
        let result = query_stream(
            &mut conn,
            "SELECT id, name, score, ok FROM demo ORDER BY id",
            &[],
            DEFAULT_IPC_CHUNK_BYTES,
        )
        .expect("query_stream should succeed");
        assert_eq!(result.row_count, 1);
        assert!(result.chunk_count >= 1);
        let first_chunk = result.read_chunk(0).expect("first chunk should be readable");
        assert!(!first_chunk.is_empty());
    }

    #[test]
    fn has_multiple_sql_statements_detects_multiple_statements() {
        assert!(has_multiple_sql_statements("SELECT 1; SELECT 2;"));
        assert!(!has_multiple_sql_statements("SELECT ';'"));
    }

    #[test]
    fn count_sql_statements_ignores_empty_segments_and_comments() {
        assert_eq!(count_sql_statements(""), 0);
        assert_eq!(count_sql_statements(" ; \n "), 0);
        assert_eq!(count_sql_statements("SELECT 1"), 1);
        assert_eq!(count_sql_statements("SELECT 1; SELECT 2"), 2);
        assert_eq!(
            count_sql_statements("SELECT 1; -- ignored ;\n/* hidden ; */ SELECT 2;"),
            2
        );
    }
}
