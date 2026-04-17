use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use crate::fts::{
    delete_fts_document, ensure_fts_index, rebuild_fts_index, search_fts, upsert_fts_document,
};
use crate::library::library_info;
use crate::runtime::{
    SqliteDatabaseHandle, SqliteOpenOptions, SqliteRuntime, open_sqlite_connection,
};
use crate::sql_exec::{
    DEFAULT_IPC_CHUNK_BYTES, QueryJsonResult, QueryStreamResult, SqlExecCoreError,
    count_sql_statements, execute_batch as execute_batch_core, execute_script as execute_script_core,
    parse_legacy_params_json, query_json as query_json_core, query_stream as query_stream_core,
};
use crate::tokenizer::{
    ListCustomWordsResult, TokenizeOutput, TokenizerMode, list_custom_words, remove_custom_word,
    tokenize_text, upsert_custom_word,
};
use rusqlite::Connection;
use rusqlite::types::Value as SqliteValue;
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString, c_char};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// FFI 状态码，供 Go / C 调用方判断调用是否成功。
/// FFI status code used by Go / C callers to determine whether an invocation succeeded.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VldbSqliteStatusCode {
    /// 调用成功。
    /// The invocation completed successfully.
    Success = 0,
    /// 调用失败，可通过最近一次错误消息读取原因。
    /// The invocation failed; the reason can be read from the latest error message.
    Failure = 1,
}

/// FFI 分词模式枚举，作为非 JSON 主接口的稳定入参。
/// FFI tokenizer-mode enum used as a stable input for the non-JSON main interface.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VldbSqliteFfiTokenizerMode {
    /// 不启用 Jieba，仅做轻量规范化。
    /// Do not enable Jieba; only perform light normalization.
    None = 0,
    /// 启用 Jieba 与内建词典能力。
    /// Enable Jieba and the built-in dictionary capabilities.
    Jieba = 1,
}

/// FFI Runtime 句柄，负责纯库多库管理。
/// FFI runtime handle responsible for pure-library multi-database management.
pub struct VldbSqliteRuntimeHandle {
    /// 内部多库运行时实例。
    /// Internal multi-database runtime instance.
    inner: SqliteRuntime,
}

/// FFI 数据库句柄，负责复用指定数据库的打开选项与连接策略。
/// FFI database handle used to reuse open options and connection rules for a specific database.
pub struct VldbSqliteDatabaseHandle {
    /// 内部数据库句柄引用。
    /// Internal database-handle reference.
    inner: Arc<SqliteDatabaseHandle>,
}

/// FFI 分词结果句柄，供调用方通过 getter 读取。
/// FFI tokenize-result handle read by callers through getter functions.
pub struct VldbSqliteTokenizeResultHandle {
    /// 内部分词结果。
    /// Internal tokenize result.
    inner: TokenizeOutput,
}

/// FFI 自定义词列表句柄，供调用方通过 getter 读取。
/// FFI custom-word list handle read by callers through getter functions.
pub struct VldbSqliteCustomWordListHandle {
    /// 内部自定义词列表结果。
    /// Internal custom-word list result.
    inner: ListCustomWordsResult,
}

/// FFI FTS 检索结果句柄，供调用方通过 getter 读取。
/// FFI FTS search-result handle read by callers through getter functions.
pub struct VldbSqliteSearchResultHandle {
    /// 内部 FTS 检索结果。
    /// Internal FTS search result.
    inner: crate::fts::SearchFtsResult,
}

/// FFI 通用 SQL 执行结果句柄。
/// FFI shared SQL execution-result handle.
pub struct VldbSqliteExecuteResultHandle {
    /// 内部执行结果。
    /// Internal execution result.
    inner: VldbSqliteExecuteResult,
}

/// FFI JSON 查询结果句柄。
/// FFI JSON-query result handle.
pub struct VldbSqliteQueryJsonResultHandle {
    /// 内部 JSON 查询结果。
    /// Internal JSON query result.
    inner: QueryJsonResult,
}

/// FFI Arrow IPC chunk 查询结果句柄。
/// FFI Arrow IPC chunk query-result handle.
pub struct VldbSqliteQueryStreamHandle {
    /// 内部 Arrow IPC chunk 查询结果句柄。
    /// Internal Arrow IPC chunk query-result handle.
    inner: QueryStreamResult,
}

/// 非 JSON FFI 的 SQL 执行结果。
/// SQL execution result used by the non-JSON FFI.
#[derive(Debug, Clone)]
struct VldbSqliteExecuteResult {
    /// 是否执行成功。
    /// Whether the execution succeeded.
    success: bool,
    /// 结果消息。
    /// Result message.
    message: String,
    /// 受影响行数。
    /// Number of affected rows.
    rows_changed: i64,
    /// 最近一次插入行 ID。
    /// Last inserted row id.
    last_insert_rowid: i64,
    /// 已执行语句次数，脚本执行为 1 或 0。
    /// Number of executed statements; script execution uses 1 or 0.
    statements_executed: i64,
}

/// 自定义词修改结果结构。
/// Custom-word mutation result structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteDictionaryMutationResultPod {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: u8,
    /// 受影响行数。
    /// Number of affected rows.
    pub affected_rows: u64,
}

/// FTS 索引确保结果结构。
/// FTS ensure-index result structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteEnsureFtsIndexResultPod {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: u8,
    /// 最终分词模式。
    /// Effective tokenizer mode.
    pub tokenizer_mode: u32,
}

/// FTS 索引重建结果结构。
/// FTS rebuild-index result structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteRebuildFtsIndexResultPod {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: u8,
    /// 最终分词模式。
    /// Effective tokenizer mode.
    pub tokenizer_mode: u32,
    /// 重建回写的文档数量。
    /// Number of documents reindexed during rebuild.
    pub reindexed_rows: u64,
}

/// FTS 文档写入/删除结果结构。
/// FTS document mutation result structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteFtsMutationResultPod {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: u8,
    /// 受影响行数。
    /// Number of affected rows.
    pub affected_rows: u64,
}

/// 字节视图结构，供 FFI 输入 bytes 参数使用。
/// Byte-view structure used by the FFI for input bytes parameters.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteByteView {
    /// 字节数据指针。
    /// Pointer to the byte data.
    pub data: *const u8,
    /// 字节长度。
    /// Length of the byte data.
    pub len: u64,
}

/// 可释放字节缓冲区结构，供 QueryStream chunk getter 返回。
/// Releasable byte-buffer structure returned by QueryStream chunk getters.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteByteBuffer {
    /// 字节数据指针。
    /// Pointer to the byte data.
    pub data: *mut u8,
    /// 字节长度。
    /// Length of the byte data.
    pub len: u64,
    /// 原始容量，用于恢复 `Vec<u8>`。
    /// Original capacity used to rebuild the source `Vec<u8>`.
    pub cap: u64,
}

/// FFI SQL 值类型枚举。
/// FFI SQL value-kind enum.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VldbSqliteFfiValueKind {
    /// Null 值。
    /// Null value.
    Null = 0,
    /// 64 位有符号整数。
    /// Signed 64-bit integer.
    Int64 = 1,
    /// 64 位浮点数。
    /// 64-bit floating point number.
    Float64 = 2,
    /// UTF-8 字符串。
    /// UTF-8 string.
    String = 3,
    /// 原始字节数组。
    /// Raw byte array.
    Bytes = 4,
    /// 布尔值。
    /// Boolean value.
    Bool = 5,
}

/// FFI SQL 参数值结构。
/// FFI SQL parameter value structure.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VldbSqliteFfiValue {
    /// 当前值类型。
    /// Current value kind.
    pub kind: VldbSqliteFfiValueKind,
    /// int64 值。
    /// int64 value.
    pub int64_value: i64,
    /// float64 值。
    /// float64 value.
    pub float64_value: f64,
    /// string 值，要求为 NUL 结尾 UTF-8 字符串。
    /// string value, expected to be a NUL-terminated UTF-8 string.
    pub string_value: *const c_char,
    /// bytes 值。
    /// bytes value.
    pub bytes_value: VldbSqliteByteView,
    /// bool 值，0 为 false，非 0 为 true。
    /// bool value, 0 for false and non-zero for true.
    pub bool_value: u8,
}

/// FFI SQL 参数切片结构。
/// FFI SQL parameter-slice structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VldbSqliteFfiValueSlice {
    /// 参数数组指针。
    /// Pointer to the parameter array.
    pub values: *const VldbSqliteFfiValue,
    /// 参数数量。
    /// Number of parameters.
    pub len: u64,
}

/// 最近一次 FFI 错误消息缓存。
/// Cache for the latest FFI error message.
static LAST_ERROR: OnceLock<Mutex<Option<CString>>> = OnceLock::new();
static NEXT_JSON_STREAM_ID: AtomicU64 = AtomicU64::new(1);
static JSON_QUERY_STREAMS: OnceLock<Mutex<std::collections::HashMap<u64, QueryStreamResult>>> =
    OnceLock::new();

fn last_error_slot() -> &'static Mutex<Option<CString>> {
    LAST_ERROR.get_or_init(|| Mutex::new(None))
}

fn json_query_stream_registry(
) -> &'static Mutex<std::collections::HashMap<u64, QueryStreamResult>> {
    JSON_QUERY_STREAMS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn update_last_error(message: impl Into<String>) {
    let message = sanitize_for_c_string(message.into());
    if let Ok(mut guard) = last_error_slot().lock() {
        *guard = CString::new(message).ok();
    }
}

fn clear_last_error_inner() {
    if let Ok(mut guard) = last_error_slot().lock() {
        *guard = None;
    }
}

fn sanitize_for_c_string(value: String) -> String {
    value.replace('\0', " ")
}

fn json_to_c_string(value: impl Into<String>) -> *mut c_char {
    match CString::new(sanitize_for_c_string(value.into())) {
        Ok(c_string) => c_string.into_raw(),
        Err(error) => {
            update_last_error(format!("failed to allocate FFI string: {error}"));
            std::ptr::null_mut()
        }
    }
}

fn c_json_arg_to_string(value: *const c_char) -> Result<String, String> {
    if value.is_null() {
        return Err("JSON argument must not be null / JSON 参数不能为空指针".to_string());
    }

    // SAFETY:
    // 调用方保证传入指向一个以 NUL 结尾的只读字符串。
    // The caller guarantees the pointer refers to a NUL-terminated read-only string.
    let raw = unsafe { CStr::from_ptr(value) };
    raw.to_str()
        .map(ToOwned::to_owned)
        .map_err(|error| format!("invalid UTF-8 JSON argument: {error}"))
}

fn open_connection_for_db_path(db_path: &str) -> Result<Connection, String> {
    if db_path.trim().is_empty() {
        return Err("db_path must not be empty / db_path 不能为空".to_string());
    }

    if db_path != ":memory:"
        && let Some(parent) = Path::new(db_path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create database directory: {error}"))?;
    }

    open_sqlite_connection(db_path, &SqliteOpenOptions::default())
        .map_err(|error| format!("failed to open sqlite database with runtime defaults: {error}"))
}

/// 基于已打开的数据库句柄创建符合运行时规则的连接。
/// Create a connection that follows runtime rules from an opened database handle.
fn open_connection_for_handle(handle: *mut VldbSqliteDatabaseHandle) -> Result<Connection, String> {
    let database = database_handle_ref(handle)?;
    database
        .inner
        .open_connection()
        .map_err(|error| format!("failed to open runtime-managed sqlite connection: {error}"))
}

/// 读取 runtime 句柄引用。
/// Read a runtime-handle reference.
fn runtime_handle_ref(
    handle: *mut VldbSqliteRuntimeHandle,
) -> Result<&'static VldbSqliteRuntimeHandle, String> {
    if handle.is_null() {
        return Err("runtime handle must not be null / runtime 句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取数据库句柄引用。
/// Read a database-handle reference.
fn database_handle_ref(
    handle: *mut VldbSqliteDatabaseHandle,
) -> Result<&'static VldbSqliteDatabaseHandle, String> {
    if handle.is_null() {
        return Err("database handle must not be null / database 句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取分词结果句柄引用。
/// Read a tokenize-result handle reference.
fn tokenize_result_handle_ref(
    handle: *mut VldbSqliteTokenizeResultHandle,
) -> Result<&'static VldbSqliteTokenizeResultHandle, String> {
    if handle.is_null() {
        return Err("tokenize result handle must not be null / 分词结果句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取自定义词列表句柄引用。
/// Read a custom-word list handle reference.
fn custom_word_list_handle_ref(
    handle: *mut VldbSqliteCustomWordListHandle,
) -> Result<&'static VldbSqliteCustomWordListHandle, String> {
    if handle.is_null() {
        return Err("custom word list handle must not be null / 自定义词列表句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取 FTS 检索结果句柄引用。
/// Read an FTS search-result handle reference.
fn search_result_handle_ref(
    handle: *mut VldbSqliteSearchResultHandle,
) -> Result<&'static VldbSqliteSearchResultHandle, String> {
    if handle.is_null() {
        return Err("search result handle must not be null / 检索结果句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取通用 SQL 执行结果句柄引用。
/// Read a shared SQL execution-result handle reference.
fn execute_result_handle_ref(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> Result<&'static VldbSqliteExecuteResultHandle, String> {
    if handle.is_null() {
        return Err("execute result handle must not be null / 执行结果句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取 JSON 查询结果句柄引用。
/// Read a JSON-query result handle reference.
fn query_json_result_handle_ref(
    handle: *mut VldbSqliteQueryJsonResultHandle,
) -> Result<&'static VldbSqliteQueryJsonResultHandle, String> {
    if handle.is_null() {
        return Err("query json result handle must not be null / JSON 查询结果句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 读取 Arrow IPC chunk 查询结果句柄引用。
/// Read an Arrow IPC chunk query-result handle reference.
fn query_stream_handle_ref(
    handle: *mut VldbSqliteQueryStreamHandle,
) -> Result<&'static VldbSqliteQueryStreamHandle, String> {
    if handle.is_null() {
        return Err("query stream handle must not be null / QueryStream 句柄不能为空".to_string());
    }

    // SAFETY:
    // 指针由 `Box::into_raw` 创建，且调用期间保持有效。
    // The pointer is created by `Box::into_raw` and remains valid during the call.
    Ok(unsafe { &*handle })
}

/// 将 FFI 分词模式映射到内部模式。
/// Map the FFI tokenizer mode into the internal tokenizer mode.
fn parse_ffi_tokenizer_mode(mode: VldbSqliteFfiTokenizerMode) -> TokenizerMode {
    match mode {
        VldbSqliteFfiTokenizerMode::None => TokenizerMode::None,
        VldbSqliteFfiTokenizerMode::Jieba => TokenizerMode::Jieba,
    }
}

/// 将内部模式映射回 FFI 枚举值。
/// Map the internal tokenizer mode back to the FFI enum value.
fn ffi_tokenizer_mode_code(mode: TokenizerMode) -> u32 {
    match mode {
        TokenizerMode::None => VldbSqliteFfiTokenizerMode::None as u32,
        TokenizerMode::Jieba => VldbSqliteFfiTokenizerMode::Jieba as u32,
    }
}

/// 读取索引参数中的有效标题字段。
/// Read the effective title field from input arguments.
fn optional_title_arg(value: *const c_char) -> Result<String, String> {
    if value.is_null() {
        return Ok(String::new());
    }

    c_json_arg_to_string(value)
}

/// 把 JSON typed value 转成 SQLite 值。
/// Convert a JSON typed value into a SQLite value.
fn json_typed_value_to_sqlite_value(value: FfiJsonSqliteValue) -> Result<SqliteValue, String> {
    match value {
        FfiJsonSqliteValue::Int64 { value } => Ok(SqliteValue::Integer(value)),
        FfiJsonSqliteValue::Float64 { value } => Ok(SqliteValue::Real(value)),
        FfiJsonSqliteValue::String { value } => Ok(SqliteValue::Text(value)),
        FfiJsonSqliteValue::Bytes { value } => Ok(SqliteValue::Blob(value)),
        FfiJsonSqliteValue::Bool { value } => Ok(SqliteValue::Integer(i64::from(value))),
        FfiJsonSqliteValue::Null => Ok(SqliteValue::Null),
    }
}

/// 解析 JSON 兼容接口中的参数。
/// Parse parameters from the JSON compatibility interface.
fn parse_json_compat_params(
    params: Option<Vec<FfiJsonSqliteValue>>,
    params_json: Option<&str>,
) -> Result<Vec<SqliteValue>, String> {
    if let Some(params) = params {
        if let Some(params_json) = params_json
            && !params_json.trim().is_empty()
        {
            return Err(
                "provide either typed params or params_json, but not both / 不能同时提供 typed params 与 params_json"
                    .to_string(),
            );
        }

        return params
            .into_iter()
            .map(json_typed_value_to_sqlite_value)
            .collect();
    }

    parse_legacy_params_json(params_json.unwrap_or_default()).map_err(|error| error.to_string())
}

/// 把 FFI SQL 值转换为内部 SQLite 值。
/// Convert an FFI SQL value into an internal SQLite value.
fn ffi_value_to_sqlite_value(value: &VldbSqliteFfiValue) -> Result<SqliteValue, String> {
    match value.kind {
        VldbSqliteFfiValueKind::Null => Ok(SqliteValue::Null),
        VldbSqliteFfiValueKind::Int64 => Ok(SqliteValue::Integer(value.int64_value)),
        VldbSqliteFfiValueKind::Float64 => Ok(SqliteValue::Real(value.float64_value)),
        VldbSqliteFfiValueKind::String => {
            Ok(SqliteValue::Text(c_json_arg_to_string(value.string_value)?))
        }
        VldbSqliteFfiValueKind::Bytes => {
            if value.bytes_value.data.is_null() && value.bytes_value.len > 0 {
                return Err("bytes_value.data must not be null when len > 0 / len > 0 时 bytes_value.data 不能为空".to_string());
            }
            let bytes = if value.bytes_value.data.is_null() || value.bytes_value.len == 0 {
                Vec::new()
            } else {
                // SAFETY:
                // 调用方保证 data/len 描述一段有效的只读字节切片。
                // The caller guarantees that data/len describe a valid read-only byte slice.
                unsafe {
                    std::slice::from_raw_parts(
                        value.bytes_value.data,
                        usize::try_from(value.bytes_value.len)
                            .map_err(|_| "bytes length exceeds usize / bytes 长度超过 usize".to_string())?,
                    )
                    .to_vec()
                }
            };
            Ok(SqliteValue::Blob(bytes))
        }
        VldbSqliteFfiValueKind::Bool => Ok(SqliteValue::Integer(i64::from(value.bool_value != 0))),
    }
}

/// 从 FFI 指针参数解析参数数组。
/// Parse a parameter array from FFI pointer inputs.
fn ffi_values_from_parts(
    params: *const VldbSqliteFfiValue,
    params_len: u64,
    params_json: *const c_char,
) -> Result<Vec<SqliteValue>, String> {
    let params_json_string = if params_json.is_null() {
        String::new()
    } else {
        c_json_arg_to_string(params_json)?
    };

    if !params.is_null() && params_len > 0 {
        if !params_json_string.trim().is_empty() {
            return Err("provide either typed params or params_json, but not both / 不能同时提供 typed params 与 params_json".to_string());
        }

        // SAFETY:
        // 调用方保证 params/params_len 描述一段有效的只读数组。
        // The caller guarantees that params/params_len describe a valid read-only array.
        let values = unsafe {
            std::slice::from_raw_parts(
                params,
                usize::try_from(params_len)
                    .map_err(|_| "params_len exceeds usize / params_len 超过 usize".to_string())?,
            )
        };
        return values.iter().map(ffi_value_to_sqlite_value).collect();
    }

    parse_legacy_params_json(&params_json_string).map_err(|error| error.to_string())
}

/// 从 FFI 批量参数切片解析批量参数。
/// Parse batch parameters from FFI parameter slices.
fn ffi_batch_values_from_parts(
    items: *const VldbSqliteFfiValueSlice,
    items_len: u64,
) -> Result<Vec<Vec<SqliteValue>>, String> {
    if items.is_null() || items_len == 0 {
        return Err("items must not be empty / items 不能为空".to_string());
    }

    // SAFETY:
    // 调用方保证 items/items_len 描述一段有效的只读切片。
    // The caller guarantees that items/items_len describe a valid read-only slice.
    let item_slices = unsafe {
        std::slice::from_raw_parts(
            items,
            usize::try_from(items_len)
                .map_err(|_| "items_len exceeds usize / items_len 超过 usize".to_string())?,
        )
    };

    item_slices
        .iter()
        .map(|item| {
            if item.values.is_null() && item.len > 0 {
                return Err(
                    "item.values must not be null when item.len > 0 / item.len > 0 时 item.values 不能为空"
                        .to_string(),
                );
            }
            if item.values.is_null() || item.len == 0 {
                return Ok(Vec::new());
            }
            // SAFETY:
            // 调用方保证 item.values/item.len 描述一段有效的只读数组。
            // The caller guarantees that item.values/item.len describe a valid read-only array.
            let values = unsafe {
                std::slice::from_raw_parts(
                    item.values,
                    usize::try_from(item.len)
                        .map_err(|_| "item.len exceeds usize / item.len 超过 usize".to_string())?,
                )
            };
            values.iter().map(ffi_value_to_sqlite_value).collect()
        })
        .collect()
}

/// 把原始 chunk 字节复制为可释放缓冲区。
/// Copy raw chunk bytes into a releasable byte buffer.
fn bytes_to_buffer(bytes: &[u8]) -> VldbSqliteByteBuffer {
    let mut owned = bytes.to_vec();
    let buffer = VldbSqliteByteBuffer {
        data: owned.as_mut_ptr(),
        len: u64::try_from(owned.len()).unwrap_or(u64::MAX),
        cap: u64::try_from(owned.capacity()).unwrap_or(u64::MAX),
    };
    std::mem::forget(owned);
    buffer
}

fn sql_exec_error_to_string(error: SqlExecCoreError) -> String {
    error.to_string()
}

/// 注册 JSON 兼容层的 QueryStream 结果并返回流句柄 ID。
/// Register a JSON-compat QueryStream result and return its stream-handle ID.
fn register_json_query_stream(result: QueryStreamResult) -> Result<u64, String> {
    let stream_id = NEXT_JSON_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let mut guard = json_query_stream_registry()
        .lock()
        .map_err(|_| "failed to lock JSON query stream registry / 无法锁定 JSON QueryStream 注册表".to_string())?;
    guard.insert(stream_id, result);
    Ok(stream_id)
}

/// 读取 JSON 兼容层的 QueryStream 结果。
/// Read a JSON-compat QueryStream result from the registry.
fn with_json_query_stream<T>(
    stream_id: u64,
    f: impl FnOnce(&QueryStreamResult) -> Result<T, String>,
) -> Result<T, String> {
    let guard = json_query_stream_registry()
        .lock()
        .map_err(|_| "failed to lock JSON query stream registry / 无法锁定 JSON QueryStream 注册表".to_string())?;
    let result = guard
        .get(&stream_id)
        .ok_or_else(|| format!("query stream handle not found: {stream_id} / QueryStream 句柄不存在"))?;
    f(result)
}

/// 关闭并移除 JSON 兼容层的 QueryStream 结果。
/// Close and remove a JSON-compat QueryStream result from the registry.
fn close_json_query_stream(stream_id: u64) -> Result<bool, String> {
    let mut guard = json_query_stream_registry()
        .lock()
        .map_err(|_| "failed to lock JSON query stream registry / 无法锁定 JSON QueryStream 注册表".to_string())?;
    Ok(guard.remove(&stream_id).is_some())
}

#[derive(Debug, Deserialize)]
struct FfiTokenizeTextRequest {
    text: String,
    tokenizer_mode: Option<String>,
    search_mode: Option<bool>,
    db_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfiUpsertCustomWordRequest {
    db_path: String,
    word: String,
    weight: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FfiRemoveCustomWordRequest {
    db_path: String,
    word: String,
}

#[derive(Debug, Deserialize)]
struct FfiListCustomWordsRequest {
    db_path: String,
}

#[derive(Debug, Deserialize)]
struct FfiEnsureFtsIndexRequest {
    db_path: String,
    index_name: String,
    tokenizer_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfiRebuildFtsIndexRequest {
    db_path: String,
    index_name: String,
    tokenizer_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfiUpsertFtsDocumentRequest {
    db_path: String,
    index_name: String,
    tokenizer_mode: Option<String>,
    id: String,
    file_path: String,
    title: Option<String>,
    content: String,
}

#[derive(Debug, Deserialize)]
struct FfiDeleteFtsDocumentRequest {
    db_path: String,
    index_name: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct FfiSearchFtsRequest {
    db_path: String,
    index_name: String,
    tokenizer_mode: Option<String>,
    query: String,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum FfiJsonSqliteValue {
    Int64 { value: i64 },
    Float64 { value: f64 },
    String { value: String },
    Bytes { value: Vec<u8> },
    Bool { value: bool },
    Null,
}

#[derive(Debug, Deserialize)]
struct FfiExecuteScriptJsonRequest {
    db_path: String,
    sql: String,
    params_json: Option<String>,
    params: Option<Vec<FfiJsonSqliteValue>>,
}

#[derive(Debug, Deserialize)]
struct FfiExecuteBatchJsonRequest {
    db_path: String,
    sql: String,
    items: Vec<Vec<FfiJsonSqliteValue>>,
}

#[derive(Debug, Deserialize)]
struct FfiQueryJsonJsonRequest {
    db_path: String,
    sql: String,
    params_json: Option<String>,
    params: Option<Vec<FfiJsonSqliteValue>>,
}

#[derive(Debug, Deserialize)]
struct FfiQueryStreamJsonRequest {
    db_path: String,
    sql: String,
    params_json: Option<String>,
    params: Option<Vec<FfiJsonSqliteValue>>,
    chunk_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FfiQueryStreamChunkJsonRequest {
    stream_id: u64,
    index: u64,
}

#[derive(Debug, Deserialize)]
struct FfiQueryStreamCloseJsonRequest {
    stream_id: u64,
}

#[derive(Debug, Serialize)]
struct FfiExecuteJsonResponse {
    success: bool,
    message: String,
    rows_changed: i64,
    last_insert_rowid: i64,
}

#[derive(Debug, Serialize)]
struct FfiExecuteBatchJsonResponse {
    success: bool,
    message: String,
    rows_changed: i64,
    last_insert_rowid: i64,
    statements_executed: i64,
}

#[derive(Debug, Serialize)]
struct FfiQueryStreamJsonResponse {
    success: bool,
    message: String,
    stream_id: u64,
    row_count: u64,
    chunk_count: u64,
    total_bytes: u64,
}

#[derive(Debug, Serialize)]
struct FfiQueryStreamChunkJsonResponse {
    success: bool,
    message: String,
    stream_id: u64,
    index: u64,
    byte_count: u64,
    chunk_base64: String,
}

#[derive(Debug, Serialize)]
struct FfiQueryStreamCloseJsonResponse {
    success: bool,
    message: String,
    stream_id: u64,
}

/// 导出库模式基础元信息。
/// Export bootstrap library metadata as a JSON string.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_library_info_json() -> *mut c_char {
    clear_last_error_inner();
    match serde_json::to_string(&library_info()) {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(format!("failed to serialize library info: {error}"));
            std::ptr::null_mut()
        }
    }
}

/// 释放由本库分配的 JSON/C 字符串。
/// Free a JSON/C string allocated by this library.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_string_free(value: *mut c_char) {
    if value.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `CString::into_raw`，这里回收所有权即可。
    // The pointer comes from `CString::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(CString::from_raw(value));
    }
}

/// 获取最近一次 FFI 错误消息。
/// Get the latest FFI error message.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_last_error_message() -> *const c_char {
    match last_error_slot().lock() {
        Ok(guard) => guard
            .as_ref()
            .map(|value| value.as_ptr())
            .unwrap_or(std::ptr::null()),
        Err(_) => std::ptr::null(),
    }
}

/// 清理最近一次 FFI 错误消息。
/// Clear the latest FFI error message.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_clear_last_error() {
    clear_last_error_inner();
}

/// 返回 JSON 指针是否为空，便于上层快速探测调用是否成功。
/// Return whether the JSON pointer is null so callers can quickly detect failure.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_json_is_null(value: *const c_char) -> u8 {
    if value.is_null() { 1 } else { 0 }
}

/// 创建默认多库 runtime 句柄。
/// Create a default multi-database runtime handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_runtime_create_default() -> *mut VldbSqliteRuntimeHandle {
    clear_last_error_inner();
    Box::into_raw(Box::new(VldbSqliteRuntimeHandle {
        inner: SqliteRuntime::new(),
    }))
}

/// 释放多库 runtime 句柄。
/// Destroy a multi-database runtime handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_runtime_destroy(handle: *mut VldbSqliteRuntimeHandle) {
    if handle.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `Box::into_raw`，此处按原路径回收所有权。
    // The pointer comes from `Box::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 打开或复用指定路径的数据库句柄。
/// Open or reuse a database handle for the specified path.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_runtime_open_database(
    runtime: *mut VldbSqliteRuntimeHandle,
    db_path: *const c_char,
) -> *mut VldbSqliteDatabaseHandle {
    clear_last_error_inner();
    let result = (|| -> Result<*mut VldbSqliteDatabaseHandle, String> {
        let runtime = runtime_handle_ref(runtime)?;
        let db_path = c_json_arg_to_string(db_path)?;
        let handle = runtime
            .inner
            .open_database(db_path.as_str())
            .map_err(|error| format!("failed to open database via runtime: {error}"))?;
        Ok(Box::into_raw(Box::new(VldbSqliteDatabaseHandle {
            inner: handle,
        })))
    })();

    match result {
        Ok(handle) => handle,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 关闭 runtime 中缓存的数据库。
/// Close a cached database from the runtime.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_runtime_close_database(
    runtime: *mut VldbSqliteRuntimeHandle,
    db_path: *const c_char,
) -> u8 {
    clear_last_error_inner();
    let result = (|| -> Result<u8, String> {
        let runtime = runtime_handle_ref(runtime)?;
        let db_path = c_json_arg_to_string(db_path)?;
        Ok(if runtime.inner.close_database(db_path.as_str()) {
            1
        } else {
            0
        })
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 释放数据库句柄。
/// Destroy a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_destroy(handle: *mut VldbSqliteDatabaseHandle) {
    if handle.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `Box::into_raw`，此处按原路径回收所有权。
    // The pointer comes from `Box::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回数据库句柄绑定的路径字符串。
/// Return the bound database path string for a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_db_path(
    handle: *mut VldbSqliteDatabaseHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match database_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.db_path().to_string()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 释放由 QueryStream chunk getter 返回的字节缓冲区。
/// Free a byte buffer returned by a QueryStream chunk getter.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_bytes_free(buffer: VldbSqliteByteBuffer) {
    if buffer.data.is_null() {
        return;
    }

    if let (Ok(len), Ok(cap)) = (usize::try_from(buffer.len), usize::try_from(buffer.cap)) {
        // SAFETY:
        // 指针由 `bytes_to_buffer` 基于 `Vec<u8>` 导出，len/cap 与原始 Vec 布局匹配。
        // The pointer is produced by `bytes_to_buffer` from a `Vec<u8>`, and len/cap match the original Vec layout.
        unsafe {
            let _ = Vec::from_raw_parts(buffer.data, len, cap);
        }
    }
}

/// 通过数据库句柄执行脚本或单条 SQL。
/// Execute a script or a single SQL statement through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_execute_script(
    handle: *mut VldbSqliteDatabaseHandle,
    sql: *const c_char,
    params: *const VldbSqliteFfiValue,
    params_len: u64,
    params_json: *const c_char,
) -> *mut VldbSqliteExecuteResultHandle {
    clear_last_error_inner();
    match (|| -> Result<VldbSqliteExecuteResultHandle, String> {
        let sql = c_json_arg_to_string(sql)?;
        let mut connection = open_connection_for_handle(handle)?;
        let bound_values = ffi_values_from_parts(params, params_len, params_json)?;
        let result = execute_script_core(&mut connection, &sql, &bound_values)
            .map_err(sql_exec_error_to_string)?;
        Ok(VldbSqliteExecuteResultHandle {
            inner: VldbSqliteExecuteResult {
                success: result.success,
                message: result.message,
                rows_changed: result.rows_changed,
                last_insert_rowid: result.last_insert_rowid,
                statements_executed: i64::try_from(count_sql_statements(&sql)).unwrap_or(i64::MAX),
            },
        })
    })() {
        Ok(result) => Box::into_raw(Box::new(result)),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过数据库句柄执行批量 SQL。
/// Execute batch SQL through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_execute_batch(
    handle: *mut VldbSqliteDatabaseHandle,
    sql: *const c_char,
    items: *const VldbSqliteFfiValueSlice,
    items_len: u64,
) -> *mut VldbSqliteExecuteResultHandle {
    clear_last_error_inner();
    match (|| -> Result<VldbSqliteExecuteResultHandle, String> {
        let sql = c_json_arg_to_string(sql)?;
        let mut connection = open_connection_for_handle(handle)?;
        let batch_params = ffi_batch_values_from_parts(items, items_len)?;
        let result = execute_batch_core(&mut connection, &sql, &batch_params)
            .map_err(sql_exec_error_to_string)?;
        Ok(VldbSqliteExecuteResultHandle {
            inner: VldbSqliteExecuteResult {
                success: result.success,
                message: result.message,
                rows_changed: result.rows_changed,
                last_insert_rowid: result.last_insert_rowid,
                statements_executed: result.statements_executed,
            },
        })
    })() {
        Ok(result) => Box::into_raw(Box::new(result)),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过数据库句柄执行 JSON 查询。
/// Execute a JSON query through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_query_json(
    handle: *mut VldbSqliteDatabaseHandle,
    sql: *const c_char,
    params: *const VldbSqliteFfiValue,
    params_len: u64,
    params_json: *const c_char,
) -> *mut VldbSqliteQueryJsonResultHandle {
    clear_last_error_inner();
    match (|| -> Result<VldbSqliteQueryJsonResultHandle, String> {
        let sql = c_json_arg_to_string(sql)?;
        let mut connection = open_connection_for_handle(handle)?;
        let bound_values = ffi_values_from_parts(params, params_len, params_json)?;
        let result = query_json_core(&mut connection, &sql, &bound_values)
            .map_err(sql_exec_error_to_string)?;
        Ok(VldbSqliteQueryJsonResultHandle { inner: result })
    })() {
        Ok(result) => Box::into_raw(Box::new(result)),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过数据库句柄执行 Arrow IPC chunk 查询。
/// Execute an Arrow IPC chunk query through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_query_stream(
    handle: *mut VldbSqliteDatabaseHandle,
    sql: *const c_char,
    params: *const VldbSqliteFfiValue,
    params_len: u64,
    params_json: *const c_char,
    chunk_bytes: u64,
) -> *mut VldbSqliteQueryStreamHandle {
    clear_last_error_inner();
    match (|| -> Result<VldbSqliteQueryStreamHandle, String> {
        let sql = c_json_arg_to_string(sql)?;
        let mut connection = open_connection_for_handle(handle)?;
        let bound_values = ffi_values_from_parts(params, params_len, params_json)?;
        let target_chunk_size = if chunk_bytes == 0 {
            DEFAULT_IPC_CHUNK_BYTES
        } else {
            usize::try_from(chunk_bytes)
                .map_err(|_| "chunk_bytes exceeds usize / chunk_bytes 超过 usize".to_string())?
        };
        let result = query_stream_core(&mut connection, &sql, &bound_values, target_chunk_size)
            .map_err(sql_exec_error_to_string)?;
        Ok(VldbSqliteQueryStreamHandle { inner: result })
    })() {
        Ok(result) => Box::into_raw(Box::new(result)),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 释放通用 SQL 执行结果句柄。
/// Destroy a shared SQL execution-result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_destroy(handle: *mut VldbSqliteExecuteResultHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回通用 SQL 执行结果中的 success 标志。
/// Return the success flag from a shared SQL execution result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_success(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> u8 {
    match execute_result_handle_ref(handle) {
        Ok(handle) => u8::from(handle.inner.success),
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回通用 SQL 执行结果中的 message。
/// Return the message from a shared SQL execution result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_message(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match execute_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.message.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回通用 SQL 执行结果中的 rows_changed。
/// Return rows_changed from a shared SQL execution result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_rows_changed(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> i64 {
    match execute_result_handle_ref(handle) {
        Ok(handle) => handle.inner.rows_changed,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回通用 SQL 执行结果中的 last_insert_rowid。
/// Return last_insert_rowid from a shared SQL execution result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_last_insert_rowid(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> i64 {
    match execute_result_handle_ref(handle) {
        Ok(handle) => handle.inner.last_insert_rowid,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回通用 SQL 执行结果中的 statements_executed。
/// Return statements_executed from a shared SQL execution result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_result_statements_executed(
    handle: *mut VldbSqliteExecuteResultHandle,
) -> i64 {
    match execute_result_handle_ref(handle) {
        Ok(handle) => handle.inner.statements_executed,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 释放 JSON 查询结果句柄。
/// Destroy a JSON-query result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_json_result_destroy(
    handle: *mut VldbSqliteQueryJsonResultHandle,
) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回 JSON 查询结果中的 JSON 行集字符串。
/// Return the JSON row-set string from a JSON-query result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_json_result_json_data(
    handle: *mut VldbSqliteQueryJsonResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match query_json_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.json_data.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回 JSON 查询结果中的行数。
/// Return the row count from a JSON-query result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_json_result_row_count(
    handle: *mut VldbSqliteQueryJsonResultHandle,
) -> u64 {
    match query_json_result_handle_ref(handle) {
        Ok(handle) => handle.inner.row_count,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 释放 Arrow IPC chunk 查询结果句柄。
/// Destroy an Arrow IPC chunk query-result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_destroy(handle: *mut VldbSqliteQueryStreamHandle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回 Arrow IPC chunk 数量。
/// Return the number of Arrow IPC chunks.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_chunk_count(
    handle: *mut VldbSqliteQueryStreamHandle,
) -> u64 {
    match query_stream_handle_ref(handle) {
        Ok(handle) => handle.inner.chunk_count,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回 Arrow IPC chunk 查询结果中的行数。
/// Return the row count from an Arrow IPC chunk query result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_row_count(
    handle: *mut VldbSqliteQueryStreamHandle,
) -> u64 {
    match query_stream_handle_ref(handle) {
        Ok(handle) => handle.inner.row_count,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回 Arrow IPC chunk 查询结果的总字节数。
/// Return the total byte count from an Arrow IPC chunk query result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_total_bytes(
    handle: *mut VldbSqliteQueryStreamHandle,
) -> u64 {
    match query_stream_handle_ref(handle) {
        Ok(handle) => handle.inner.total_bytes,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回指定下标的 Arrow IPC chunk。
/// Return the Arrow IPC chunk at the specified index.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_get_chunk(
    handle: *mut VldbSqliteQueryStreamHandle,
    index: u64,
) -> VldbSqliteByteBuffer {
    clear_last_error_inner();
    match (|| -> Result<VldbSqliteByteBuffer, String> {
        let handle = query_stream_handle_ref(handle)?;
        let chunk = handle
            .inner
            .read_chunk(
                usize::try_from(index)
                    .map_err(|_| "chunk index exceeds usize / chunk 下标超过 usize".to_string())?,
            )
            .map_err(sql_exec_error_to_string)?;
        Ok(bytes_to_buffer(&chunk))
    })() {
        Ok(buffer) => buffer,
        Err(error) => {
            update_last_error(error);
            VldbSqliteByteBuffer::default()
        }
    }
}

/// 通过数据库句柄执行分词主接口，返回结果句柄。
/// Execute the main tokenize interface from a database handle and return a result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_tokenize_text(
    handle: *mut VldbSqliteDatabaseHandle,
    tokenizer_mode: VldbSqliteFfiTokenizerMode,
    text: *const c_char,
    search_mode: u8,
) -> *mut VldbSqliteTokenizeResultHandle {
    clear_last_error_inner();
    let result = (|| -> Result<*mut VldbSqliteTokenizeResultHandle, String> {
        let text = c_json_arg_to_string(text)?;
        let connection = open_connection_for_handle(handle)?;
        let result = tokenize_text(
            Some(&connection),
            parse_ffi_tokenizer_mode(tokenizer_mode),
            text.as_str(),
            search_mode != 0,
        )
        .map_err(|error| format!("tokenize_text failed: {error}"))?;
        Ok(Box::into_raw(Box::new(VldbSqliteTokenizeResultHandle {
            inner: result,
        })))
    })();

    match result {
        Ok(handle) => handle,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 释放分词结果句柄。
/// Destroy a tokenize-result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_result_destroy(
    handle: *mut VldbSqliteTokenizeResultHandle,
) {
    if handle.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `Box::into_raw`，此处按原路径回收所有权。
    // The pointer comes from `Box::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回分词结果中的规范化文本。
/// Return the normalized text from a tokenize result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_result_normalized_text(
    handle: *mut VldbSqliteTokenizeResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match tokenize_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.normalized_text.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回分词结果中的 FTS 查询表达式。
/// Return the FTS query expression from a tokenize result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_result_fts_query(
    handle: *mut VldbSqliteTokenizeResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match tokenize_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.fts_query.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回分词结果中的词元数量。
/// Return the token count from a tokenize result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_result_token_count(
    handle: *mut VldbSqliteTokenizeResultHandle,
) -> u64 {
    clear_last_error_inner();
    match tokenize_result_handle_ref(handle) {
        Ok(handle) => handle.inner.tokens.len() as u64,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回指定下标的词元字符串。
/// Return the token string at the specified index.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_result_get_token(
    handle: *mut VldbSqliteTokenizeResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = tokenize_result_handle_ref(handle)?;
        let token = handle
            .inner
            .tokens
            .get(index as usize)
            .ok_or_else(|| format!("token index out of range: {index}"))?;
        Ok(json_to_c_string(token.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过数据库句柄热更新自定义词。
/// Hot-update a custom word through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_upsert_custom_word(
    handle: *mut VldbSqliteDatabaseHandle,
    word: *const c_char,
    weight: u64,
    out_result: *mut VldbSqliteDictionaryMutationResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteDictionaryMutationResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let word = c_json_arg_to_string(word)?;
        let connection = open_connection_for_handle(handle)?;
        let result = upsert_custom_word(
            &connection,
            word.as_str(),
            usize::try_from(weight.max(1)).unwrap_or(usize::MAX),
        )
        .map_err(|error| format!("upsert_custom_word failed: {error}"))?;
        Ok(VldbSqliteDictionaryMutationResultPod {
            success: if result.success { 1 } else { 0 },
            affected_rows: result.affected_rows,
        })
    })();

    match result {
        Ok(result) => {
            // SAFETY:
            // `out_result` 已经过空指针检查，且指向调用方可写内存。
            // `out_result` has been null-checked and points to caller-writable memory.
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄删除自定义词。
/// Remove a custom word through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_remove_custom_word(
    handle: *mut VldbSqliteDatabaseHandle,
    word: *const c_char,
    out_result: *mut VldbSqliteDictionaryMutationResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteDictionaryMutationResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let word = c_json_arg_to_string(word)?;
        let connection = open_connection_for_handle(handle)?;
        let result = remove_custom_word(&connection, word.as_str())
            .map_err(|error| format!("remove_custom_word failed: {error}"))?;
        Ok(VldbSqliteDictionaryMutationResultPod {
            success: if result.success { 1 } else { 0 },
            affected_rows: result.affected_rows,
        })
    })();

    match result {
        Ok(result) => {
            // SAFETY:
            // `out_result` 已经过空指针检查，且指向调用方可写内存。
            // `out_result` has been null-checked and points to caller-writable memory.
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄列出自定义词，返回列表句柄。
/// List custom words through a database handle and return a list handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_list_custom_words(
    handle: *mut VldbSqliteDatabaseHandle,
) -> *mut VldbSqliteCustomWordListHandle {
    clear_last_error_inner();
    let result = (|| -> Result<*mut VldbSqliteCustomWordListHandle, String> {
        let connection = open_connection_for_handle(handle)?;
        let result = list_custom_words(&connection)
            .map_err(|error| format!("list_custom_words failed: {error}"))?;
        Ok(Box::into_raw(Box::new(VldbSqliteCustomWordListHandle {
            inner: result,
        })))
    })();

    match result {
        Ok(handle) => handle,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 释放自定义词列表句柄。
/// Destroy a custom-word list handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_custom_word_list_destroy(
    handle: *mut VldbSqliteCustomWordListHandle,
) {
    if handle.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `Box::into_raw`，此处按原路径回收所有权。
    // The pointer comes from `Box::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回自定义词列表长度。
/// Return the number of custom words in the list.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_custom_word_list_len(
    handle: *mut VldbSqliteCustomWordListHandle,
) -> u64 {
    clear_last_error_inner();
    match custom_word_list_handle_ref(handle) {
        Ok(handle) => handle.inner.words.len() as u64,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回指定自定义词条目的词文本。
/// Return the word text of a specific custom-word entry.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_custom_word_list_get_word(
    handle: *mut VldbSqliteCustomWordListHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = custom_word_list_handle_ref(handle)?;
        let entry = handle
            .inner
            .words
            .get(index as usize)
            .ok_or_else(|| format!("custom word index out of range: {index}"))?;
        Ok(json_to_c_string(entry.word.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定自定义词条目的权重。
/// Return the weight of a specific custom-word entry.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_custom_word_list_get_weight(
    handle: *mut VldbSqliteCustomWordListHandle,
    index: u64,
) -> u64 {
    clear_last_error_inner();
    let result = (|| -> Result<u64, String> {
        let handle = custom_word_list_handle_ref(handle)?;
        let entry = handle
            .inner
            .words
            .get(index as usize)
            .ok_or_else(|| format!("custom word index out of range: {index}"))?;
        Ok(entry.weight as u64)
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 通过数据库句柄确保 FTS 索引存在。
/// Ensure an FTS index exists through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_ensure_fts_index(
    handle: *mut VldbSqliteDatabaseHandle,
    index_name: *const c_char,
    tokenizer_mode: VldbSqliteFfiTokenizerMode,
    out_result: *mut VldbSqliteEnsureFtsIndexResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteEnsureFtsIndexResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let index_name = c_json_arg_to_string(index_name)?;
        let connection = open_connection_for_handle(handle)?;
        let result = ensure_fts_index(
            &connection,
            index_name.as_str(),
            parse_ffi_tokenizer_mode(tokenizer_mode),
        )
        .map_err(|error| format!("ensure_fts_index failed: {error}"))?;
        let effective_mode = TokenizerMode::parse(result.tokenizer_mode.as_str()).unwrap_or_default();
        Ok(VldbSqliteEnsureFtsIndexResultPod {
            success: if result.success { 1 } else { 0 },
            tokenizer_mode: ffi_tokenizer_mode_code(effective_mode),
        })
    })();

    match result {
        Ok(result) => {
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄重建 FTS 索引。
/// Rebuild an FTS index through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_rebuild_fts_index(
    handle: *mut VldbSqliteDatabaseHandle,
    index_name: *const c_char,
    tokenizer_mode: VldbSqliteFfiTokenizerMode,
    out_result: *mut VldbSqliteRebuildFtsIndexResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteRebuildFtsIndexResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let index_name = c_json_arg_to_string(index_name)?;
        let connection = open_connection_for_handle(handle)?;
        let result = rebuild_fts_index(
            &connection,
            index_name.as_str(),
            parse_ffi_tokenizer_mode(tokenizer_mode),
        )
        .map_err(|error| format!("rebuild_fts_index failed: {error}"))?;
        let effective_mode = TokenizerMode::parse(result.tokenizer_mode.as_str()).unwrap_or_default();
        Ok(VldbSqliteRebuildFtsIndexResultPod {
            success: if result.success { 1 } else { 0 },
            tokenizer_mode: ffi_tokenizer_mode_code(effective_mode),
            reindexed_rows: result.reindexed_rows,
        })
    })();

    match result {
        Ok(result) => {
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄写入或更新 FTS 文档。
/// Upsert an FTS document through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_upsert_fts_document(
    handle: *mut VldbSqliteDatabaseHandle,
    index_name: *const c_char,
    tokenizer_mode: VldbSqliteFfiTokenizerMode,
    id: *const c_char,
    file_path: *const c_char,
    title: *const c_char,
    content: *const c_char,
    out_result: *mut VldbSqliteFtsMutationResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteFtsMutationResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let index_name = c_json_arg_to_string(index_name)?;
        let id = c_json_arg_to_string(id)?;
        let file_path = c_json_arg_to_string(file_path)?;
        let title = optional_title_arg(title)?;
        let content = c_json_arg_to_string(content)?;
        let connection = open_connection_for_handle(handle)?;
        let result = upsert_fts_document(
            &connection,
            index_name.as_str(),
            parse_ffi_tokenizer_mode(tokenizer_mode),
            id.as_str(),
            file_path.as_str(),
            title.as_str(),
            content.as_str(),
        )
        .map_err(|error| format!("upsert_fts_document failed: {error}"))?;
        Ok(VldbSqliteFtsMutationResultPod {
            success: if result.success { 1 } else { 0 },
            affected_rows: result.affected_rows,
        })
    })();

    match result {
        Ok(result) => {
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄删除 FTS 文档。
/// Delete an FTS document through a database handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_delete_fts_document(
    handle: *mut VldbSqliteDatabaseHandle,
    index_name: *const c_char,
    id: *const c_char,
    out_result: *mut VldbSqliteFtsMutationResultPod,
) -> i32 {
    clear_last_error_inner();
    let result = (|| -> Result<VldbSqliteFtsMutationResultPod, String> {
        if out_result.is_null() {
            return Err("out_result must not be null / out_result 不能为空".to_string());
        }
        let index_name = c_json_arg_to_string(index_name)?;
        let id = c_json_arg_to_string(id)?;
        let connection = open_connection_for_handle(handle)?;
        let result = delete_fts_document(&connection, index_name.as_str(), id.as_str())
            .map_err(|error| format!("delete_fts_document failed: {error}"))?;
        Ok(VldbSqliteFtsMutationResultPod {
            success: if result.success { 1 } else { 0 },
            affected_rows: result.affected_rows,
        })
    })();

    match result {
        Ok(result) => {
            unsafe {
                *out_result = result;
            }
            VldbSqliteStatusCode::Success as i32
        }
        Err(error) => {
            update_last_error(error);
            VldbSqliteStatusCode::Failure as i32
        }
    }
}

/// 通过数据库句柄执行 FTS 检索并返回结果句柄。
/// Execute FTS search through a database handle and return a result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_database_search_fts(
    handle: *mut VldbSqliteDatabaseHandle,
    index_name: *const c_char,
    tokenizer_mode: VldbSqliteFfiTokenizerMode,
    query: *const c_char,
    limit: u32,
    offset: u32,
) -> *mut VldbSqliteSearchResultHandle {
    clear_last_error_inner();
    let result = (|| -> Result<*mut VldbSqliteSearchResultHandle, String> {
        let index_name = c_json_arg_to_string(index_name)?;
        let query = c_json_arg_to_string(query)?;
        let connection = open_connection_for_handle(handle)?;
        let result = search_fts(
            &connection,
            index_name.as_str(),
            parse_ffi_tokenizer_mode(tokenizer_mode),
            query.as_str(),
            limit,
            offset,
        )
        .map_err(|error| format!("search_fts failed: {error}"))?;
        Ok(Box::into_raw(Box::new(VldbSqliteSearchResultHandle {
            inner: result,
        })))
    })();

    match result {
        Ok(handle) => handle,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 释放 FTS 检索结果句柄。
/// Destroy an FTS search-result handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_destroy(
    handle: *mut VldbSqliteSearchResultHandle,
) {
    if handle.is_null() {
        return;
    }

    // SAFETY:
    // 指针来自 `Box::into_raw`，此处按原路径回收所有权。
    // The pointer comes from `Box::into_raw`, so reclaiming ownership here is valid.
    unsafe {
        drop(Box::from_raw(handle));
    }
}

/// 返回 FTS 检索总命中数。
/// Return the total hit count of an FTS search result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_total(
    handle: *mut VldbSqliteSearchResultHandle,
) -> u64 {
    clear_last_error_inner();
    match search_result_handle_ref(handle) {
        Ok(handle) => handle.inner.total,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回 FTS 检索当前页命中数。
/// Return the page hit count of an FTS search result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_len(
    handle: *mut VldbSqliteSearchResultHandle,
) -> u64 {
    clear_last_error_inner();
    match search_result_handle_ref(handle) {
        Ok(handle) => handle.inner.hits.len() as u64,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回检索结果的来源标签。
/// Return the source label of a search result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_source(
    handle: *mut VldbSqliteSearchResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match search_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.source.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回检索结果的查询模式标签。
/// Return the query-mode label of a search result.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_query_mode(
    handle: *mut VldbSqliteSearchResultHandle,
) -> *mut c_char {
    clear_last_error_inner();
    match search_result_handle_ref(handle) {
        Ok(handle) => json_to_c_string(handle.inner.query_mode.clone()),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的业务 ID。
/// Return the business ID of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_id(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(json_to_c_string(hit.id.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的文件路径。
/// Return the file path of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_file_path(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(json_to_c_string(hit.file_path.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的标题。
/// Return the title of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_title(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(json_to_c_string(hit.title.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的高亮标题。
/// Return the highlighted title of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_title_highlight(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(json_to_c_string(hit.title_highlight.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的内容片段。
/// Return the content snippet of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_content_snippet(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<*mut c_char, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(json_to_c_string(hit.content_snippet.clone()))
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 返回指定命中的标准化分数。
/// Return the normalized score of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_score(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> f64 {
    clear_last_error_inner();
    let result = (|| -> Result<f64, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(hit.score)
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            0.0
        }
    }
}

/// 返回指定命中的名次。
/// Return the rank of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_rank(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> u64 {
    clear_last_error_inner();
    let result = (|| -> Result<u64, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(hit.rank)
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            0
        }
    }
}

/// 返回指定命中的原始 BM25 分数。
/// Return the raw BM25 score of the specified hit.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_result_get_raw_score(
    handle: *mut VldbSqliteSearchResultHandle,
    index: u64,
) -> f64 {
    clear_last_error_inner();
    let result = (|| -> Result<f64, String> {
        let handle = search_result_handle_ref(handle)?;
        let hit = handle
            .inner
            .hits
            .get(index as usize)
            .ok_or_else(|| format!("search hit index out of range: {index}"))?;
        Ok(hit.raw_score)
    })();

    match result {
        Ok(value) => value,
        Err(error) => {
            update_last_error(error);
            0.0
        }
    }
}

/// 通过 JSON 请求执行脚本或单条 SQL。
/// Execute a script or a single SQL statement from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_script_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiExecuteScriptJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse execute_script request JSON: {error}"))?;
        let mut connection = open_connection_for_db_path(request.db_path.as_str())?;
        let bound_values = parse_json_compat_params(request.params, request.params_json.as_deref())?;
        let response = execute_script_core(&mut connection, &request.sql, &bound_values)
            .map_err(sql_exec_error_to_string)?;
        serde_json::to_string(&FfiExecuteJsonResponse {
            success: response.success,
            message: response.message,
            rows_changed: response.rows_changed,
            last_insert_rowid: response.last_insert_rowid,
        })
        .map_err(|error| format!("failed to serialize execute_script response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求执行批量 SQL。
/// Execute batch SQL from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_execute_batch_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiExecuteBatchJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse execute_batch request JSON: {error}"))?;
        let mut connection = open_connection_for_db_path(request.db_path.as_str())?;
        let batch_params = request
            .items
            .into_iter()
            .map(|row| row.into_iter().map(json_typed_value_to_sqlite_value).collect())
            .collect::<Result<Vec<Vec<SqliteValue>>, String>>()?;
        let response =
            execute_batch_core(&mut connection, &request.sql, &batch_params).map_err(sql_exec_error_to_string)?;
        serde_json::to_string(&FfiExecuteBatchJsonResponse {
            success: response.success,
            message: response.message,
            rows_changed: response.rows_changed,
            last_insert_rowid: response.last_insert_rowid,
            statements_executed: response.statements_executed,
        })
        .map_err(|error| format!("failed to serialize execute_batch response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求执行 JSON 查询。
/// Execute a JSON query from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_json_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiQueryJsonJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse query_json request JSON: {error}"))?;
        let mut connection = open_connection_for_db_path(request.db_path.as_str())?;
        let bound_values = parse_json_compat_params(request.params, request.params_json.as_deref())?;
        let response =
            query_json_core(&mut connection, &request.sql, &bound_values).map_err(sql_exec_error_to_string)?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize query_json response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求执行 Arrow IPC chunk 查询。
/// Execute an Arrow IPC chunk query from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiQueryStreamJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse query_stream request JSON: {error}"))?;
        let mut connection = open_connection_for_db_path(request.db_path.as_str())?;
        let bound_values = parse_json_compat_params(request.params, request.params_json.as_deref())?;
        let chunk_bytes = request
            .chunk_bytes
            .map(|value| usize::try_from(value).map_err(|_| "chunk_bytes exceeds usize / chunk_bytes 超过 usize".to_string()))
            .transpose()?
            .unwrap_or(DEFAULT_IPC_CHUNK_BYTES);
        let response = query_stream_core(&mut connection, &request.sql, &bound_values, chunk_bytes)
            .map_err(sql_exec_error_to_string)?;
        let row_count = response.row_count;
        let chunk_count = response.chunk_count;
        let total_bytes = response.total_bytes;
        let stream_id = register_json_query_stream(response)?;
        serde_json::to_string(&FfiQueryStreamJsonResponse {
            success: true,
            message: format!(
                "query_stream executed successfully (chunk_count={} total_bytes={})",
                chunk_count, total_bytes
            ),
            stream_id,
            row_count,
            chunk_count,
            total_bytes,
        })
        .map_err(|error| format!("failed to serialize query_stream response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求读取指定 QueryStream 句柄中的单个 chunk。
/// Read a single chunk from a JSON QueryStream handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_chunk_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiQueryStreamChunkJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse query_stream_chunk request JSON: {error}"))?;
        let response = with_json_query_stream(request.stream_id, |result| {
            let chunk = result
                .read_chunk(
                    usize::try_from(request.index)
                        .map_err(|_| "chunk index exceeds usize / chunk 下标超过 usize".to_string())?,
                )
                .map_err(sql_exec_error_to_string)?;
            Ok(FfiQueryStreamChunkJsonResponse {
                success: true,
                message: format!(
                    "query_stream chunk {} loaded successfully ({} bytes)",
                    request.index,
                    chunk.len()
                ),
                stream_id: request.stream_id,
                index: request.index,
                byte_count: u64::try_from(chunk.len()).unwrap_or(u64::MAX),
                chunk_base64: BASE64_STANDARD.encode(chunk),
            })
        })?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize query_stream_chunk response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求关闭 QueryStream 句柄。
/// Close a JSON QueryStream handle.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_query_stream_close_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiQueryStreamCloseJsonRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse query_stream_close request JSON: {error}"))?;
        let removed = close_json_query_stream(request.stream_id)?;
        if !removed {
            return Err(format!(
                "query stream handle not found: {} / QueryStream 句柄不存在",
                request.stream_id
            ));
        }
        serde_json::to_string(&FfiQueryStreamCloseJsonResponse {
            success: true,
            message: format!("query_stream handle {} closed successfully", request.stream_id),
            stream_id: request.stream_id,
        })
        .map_err(|error| format!("failed to serialize query_stream_close response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求执行分词，并返回 JSON 结果。
/// Execute tokenization from a JSON request and return a JSON response.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_tokenize_text_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiTokenizeTextRequest =
            serde_json::from_str(&request_json).map_err(|error| {
                format!("failed to parse tokenize request JSON: {error}")
            })?;
        let mode = request
            .tokenizer_mode
            .as_deref()
            .and_then(TokenizerMode::parse)
            .unwrap_or_default();

        let response_json = if let Some(db_path) = request.db_path.as_deref() {
            let connection = open_connection_for_db_path(db_path)?;
            let response = tokenize_text(
                Some(&connection),
                mode,
                request.text.as_str(),
                request.search_mode.unwrap_or(false),
            )
            .map_err(|error| format!("tokenize_text failed: {error}"))?;
            serde_json::to_string(&response)
                .map_err(|error| format!("failed to serialize tokenize response: {error}"))?
        } else {
            let response = tokenize_text(
                None,
                mode,
                request.text.as_str(),
                request.search_mode.unwrap_or(false),
            )
            .map_err(|error| format!("tokenize_text failed: {error}"))?;
            serde_json::to_string(&response)
                .map_err(|error| format!("failed to serialize tokenize response: {error}"))?
        };

        Ok(response_json)
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求热更新自定义词。
/// Hot-update a custom dictionary word from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_upsert_custom_word_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiUpsertCustomWordRequest =
            serde_json::from_str(&request_json).map_err(|error| {
                format!("failed to parse upsert request JSON: {error}")
            })?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let response = upsert_custom_word(
            &connection,
            request.word.as_str(),
            usize::try_from(request.weight.unwrap_or(1).max(1)).unwrap_or(1),
        )
        .map_err(|error| format!("upsert_custom_word failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize upsert response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求删除自定义词。
/// Remove a custom dictionary word from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_remove_custom_word_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiRemoveCustomWordRequest =
            serde_json::from_str(&request_json).map_err(|error| {
                format!("failed to parse remove request JSON: {error}")
            })?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let response = remove_custom_word(&connection, request.word.as_str())
            .map_err(|error| format!("remove_custom_word failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize remove response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求列出当前库中的自定义词。
/// List custom dictionary words from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_list_custom_words_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiListCustomWordsRequest =
            serde_json::from_str(&request_json).map_err(|error| {
                format!("failed to parse list_custom_words request JSON: {error}")
            })?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let response = list_custom_words(&connection)
            .map_err(|error| format!("list_custom_words failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize list_custom_words response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求确保 FTS 索引存在。
/// Ensure an FTS index exists from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_ensure_fts_index_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiEnsureFtsIndexRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse ensure_fts_index request JSON: {error}"))?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let mode = request
            .tokenizer_mode
            .as_deref()
            .and_then(TokenizerMode::parse)
            .unwrap_or_default();
        let response = ensure_fts_index(&connection, request.index_name.as_str(), mode)
            .map_err(|error| format!("ensure_fts_index failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize ensure_fts_index response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求重建 FTS 索引。
/// Rebuild an FTS index from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_rebuild_fts_index_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiRebuildFtsIndexRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse rebuild_fts_index request JSON: {error}"))?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let mode = request
            .tokenizer_mode
            .as_deref()
            .and_then(TokenizerMode::parse)
            .unwrap_or_default();
        let response = rebuild_fts_index(&connection, request.index_name.as_str(), mode)
            .map_err(|error| format!("rebuild_fts_index failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize rebuild_fts_index response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求写入 FTS 文档。
/// Upsert an FTS document from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_upsert_fts_document_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiUpsertFtsDocumentRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse upsert_fts_document request JSON: {error}"))?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let mode = request
            .tokenizer_mode
            .as_deref()
            .and_then(TokenizerMode::parse)
            .unwrap_or_default();
        let response = upsert_fts_document(
            &connection,
            request.index_name.as_str(),
            mode,
            request.id.as_str(),
            request.file_path.as_str(),
            request.title.as_deref().unwrap_or_default(),
            request.content.as_str(),
        )
        .map_err(|error| format!("upsert_fts_document failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize upsert_fts_document response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求删除 FTS 文档。
/// Delete an FTS document from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_delete_fts_document_json(
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiDeleteFtsDocumentRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse delete_fts_document request JSON: {error}"))?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let response = delete_fts_document(
            &connection,
            request.index_name.as_str(),
            request.id.as_str(),
        )
        .map_err(|error| format!("delete_fts_document failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize delete_fts_document response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

/// 通过 JSON 请求执行标准化 FTS 检索。
/// Execute normalized FTS search from a JSON request.
#[unsafe(no_mangle)]
pub extern "C" fn vldb_sqlite_search_fts_json(request_json: *const c_char) -> *mut c_char {
    clear_last_error_inner();
    let result = (|| -> Result<String, String> {
        let request_json = c_json_arg_to_string(request_json)?;
        let request: FfiSearchFtsRequest = serde_json::from_str(&request_json)
            .map_err(|error| format!("failed to parse search_fts request JSON: {error}"))?;
        let connection = open_connection_for_db_path(request.db_path.as_str())?;
        let mode = request
            .tokenizer_mode
            .as_deref()
            .and_then(TokenizerMode::parse)
            .unwrap_or_default();
        let response = search_fts(
            &connection,
            request.index_name.as_str(),
            mode,
            request.query.as_str(),
            request.limit.unwrap_or(20),
            request.offset.unwrap_or(0),
        )
        .map_err(|error| format!("search_fts failed: {error}"))?;
        serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize search_fts response: {error}"))
    })();

    match result {
        Ok(json) => json_to_c_string(json),
        Err(error) => {
            update_last_error(error);
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        VldbSqliteFfiValue, VldbSqliteFfiValueKind, VldbSqliteFfiValueSlice,
        vldb_sqlite_clear_last_error, vldb_sqlite_database_execute_batch,
        vldb_sqlite_database_execute_script, vldb_sqlite_database_query_json,
        vldb_sqlite_database_query_stream, vldb_sqlite_execute_result_destroy,
        vldb_sqlite_execute_result_rows_changed, vldb_sqlite_execute_result_statements_executed,
        vldb_sqlite_json_is_null, vldb_sqlite_last_error_message, vldb_sqlite_library_info_json,
        vldb_sqlite_query_json_json, vldb_sqlite_query_json_result_destroy,
        vldb_sqlite_query_json_result_json_data, vldb_sqlite_query_stream_chunk_count,
        vldb_sqlite_query_stream_close_json, vldb_sqlite_query_stream_destroy,
        vldb_sqlite_query_stream_get_chunk, vldb_sqlite_query_stream_json,
        vldb_sqlite_query_stream_chunk_json, vldb_sqlite_runtime_create_default,
        vldb_sqlite_runtime_destroy, vldb_sqlite_runtime_open_database, vldb_sqlite_string_free,
        vldb_sqlite_bytes_free,
    };
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;
    use serde_json::Value as JsonValue;
    use std::ffi::{CStr, CString, c_char};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// 仅用于测试：把 C 字符串安全转回 Rust 字符串。
    /// Test helper: convert a C string back to a Rust string safely.
    fn c_string_to_string(value: *const c_char) -> Option<String> {
        if value.is_null() {
            return None;
        }

        // SAFETY:
        // 调用方保证传入指向一个以 NUL 结尾的只读字符串。
        // The caller guarantees the pointer refers to a NUL-terminated read-only string.
        let raw = unsafe { CStr::from_ptr(value) };
        raw.to_str().ok().map(ToOwned::to_owned)
    }

    fn make_c_string(value: &str) -> CString {
        CString::new(value).expect("test CString should stay valid")
    }

    fn temp_db_path(label: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should move forward")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("vldb-sqlite-ffi-{label}-{nanos}.sqlite3"))
            .to_string_lossy()
            .to_string()
    }

    #[test]
    fn library_info_json_contains_expected_fields() {
        let raw = vldb_sqlite_library_info_json();
        assert_eq!(vldb_sqlite_json_is_null(raw), 0);

        let json = c_string_to_string(raw).expect("library info JSON should be readable");
        let value: JsonValue =
            serde_json::from_str(&json).expect("library info JSON should stay valid");

        assert_eq!(value["name"], "vldb-sqlite");
        assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(value["ffi_stage"], "sqlite-runtime-go-ffi");
        assert!(
            value["capabilities"]
                .as_array()
                .expect("capabilities should stay an array")
                .iter()
                .any(|entry| entry == "runtime_create_default")
        );
        assert!(
            value["capabilities"]
                .as_array()
                .expect("capabilities should stay an array")
                .iter()
                .any(|entry| entry == "database_search_fts")
        );
        assert!(
            value["capabilities"]
                .as_array()
                .expect("capabilities should stay an array")
                .iter()
                .any(|entry| entry == "database_execute_script")
        );
        assert!(
            value["capabilities"]
                .as_array()
                .expect("capabilities should stay an array")
                .iter()
                .any(|entry| entry == "query_stream_json")
        );
        assert!(
            value["capabilities"]
                .as_array()
                .expect("capabilities should stay an array")
                .iter()
                .any(|entry| entry == "query_stream_chunk_json")
        );

        vldb_sqlite_string_free(raw);
    }

    #[test]
    fn last_error_is_empty_after_success() {
        vldb_sqlite_clear_last_error();
        let raw = vldb_sqlite_last_error_message();
        assert!(raw.is_null());
    }

    #[test]
    fn ffi_execute_script_and_query_json_round_trip() {
        let runtime = vldb_sqlite_runtime_create_default();
        assert!(!runtime.is_null());
        let db_path = temp_db_path("execute-query");
        let db_path_c = make_c_string(&db_path);
        let db = vldb_sqlite_runtime_open_database(runtime, db_path_c.as_ptr());
        assert!(!db.is_null());

        let create_sql = make_c_string(
            "CREATE TABLE demo(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score REAL);",
        );
        let create_result =
            vldb_sqlite_database_execute_script(db, create_sql.as_ptr(), std::ptr::null(), 0, std::ptr::null());
        assert!(!create_result.is_null());
        assert_eq!(vldb_sqlite_execute_result_statements_executed(create_result), 1);
        vldb_sqlite_execute_result_destroy(create_result);

        let insert_sql = make_c_string("INSERT INTO demo(name, score) VALUES (?1, ?2)");
        let name = make_c_string("alpha");
        let params = [
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::String,
                int64_value: 0,
                float64_value: 0.0,
                string_value: name.as_ptr(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::Float64,
                int64_value: 0,
                float64_value: 7.5,
                string_value: std::ptr::null(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
        ];
        let insert_result = vldb_sqlite_database_execute_script(
            db,
            insert_sql.as_ptr(),
            params.as_ptr(),
            params.len() as u64,
            std::ptr::null(),
        );
        assert!(!insert_result.is_null());
        assert_eq!(vldb_sqlite_execute_result_rows_changed(insert_result), 1);
        assert_eq!(vldb_sqlite_execute_result_statements_executed(insert_result), 1);
        vldb_sqlite_execute_result_destroy(insert_result);

        let query_sql = make_c_string("SELECT id, name, score FROM demo ORDER BY id");
        let query_result =
            vldb_sqlite_database_query_json(db, query_sql.as_ptr(), std::ptr::null(), 0, std::ptr::null());
        assert!(!query_result.is_null());
        let json_ptr = vldb_sqlite_query_json_result_json_data(query_result);
        let json = c_string_to_string(json_ptr).expect("query_json result should be readable");
        assert!(json.contains("\"alpha\""));
        vldb_sqlite_string_free(json_ptr);
        vldb_sqlite_query_json_result_destroy(query_result);

        vldb_sqlite_runtime_destroy(runtime);
    }

    #[test]
    fn ffi_execute_batch_and_query_stream_work() {
        let runtime = vldb_sqlite_runtime_create_default();
        assert!(!runtime.is_null());
        let db_path = temp_db_path("batch-stream");
        let db_path_c = make_c_string(&db_path);
        let db = vldb_sqlite_runtime_open_database(runtime, db_path_c.as_ptr());
        assert!(!db.is_null());

        let create_sql = make_c_string(
            "CREATE TABLE demo(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score REAL);",
        );
        let create_result =
            vldb_sqlite_database_execute_script(db, create_sql.as_ptr(), std::ptr::null(), 0, std::ptr::null());
        assert!(!create_result.is_null());
        vldb_sqlite_execute_result_destroy(create_result);

        let sql = make_c_string("INSERT INTO demo(name, score) VALUES (?1, ?2)");
        let alpha = make_c_string("alpha");
        let beta = make_c_string("beta");
        let row1 = [
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::String,
                int64_value: 0,
                float64_value: 0.0,
                string_value: alpha.as_ptr(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::Float64,
                int64_value: 0,
                float64_value: 1.5,
                string_value: std::ptr::null(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
        ];
        let row2 = [
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::String,
                int64_value: 0,
                float64_value: 0.0,
                string_value: beta.as_ptr(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
            VldbSqliteFfiValue {
                kind: VldbSqliteFfiValueKind::Float64,
                int64_value: 0,
                float64_value: 2.5,
                string_value: std::ptr::null(),
                bytes_value: Default::default(),
                bool_value: 0,
            },
        ];
        let items = [
            VldbSqliteFfiValueSlice {
                values: row1.as_ptr(),
                len: row1.len() as u64,
            },
            VldbSqliteFfiValueSlice {
                values: row2.as_ptr(),
                len: row2.len() as u64,
            },
        ];
        let batch_result =
            vldb_sqlite_database_execute_batch(db, sql.as_ptr(), items.as_ptr(), items.len() as u64);
        assert!(!batch_result.is_null());
        assert_eq!(vldb_sqlite_execute_result_statements_executed(batch_result), 2);
        vldb_sqlite_execute_result_destroy(batch_result);

        let query_sql = make_c_string("SELECT id, name, score FROM demo ORDER BY id");
        let stream =
            vldb_sqlite_database_query_stream(db, query_sql.as_ptr(), std::ptr::null(), 0, std::ptr::null(), 0);
        assert!(!stream.is_null());
        assert!(vldb_sqlite_query_stream_chunk_count(stream) >= 1);
        let chunk = vldb_sqlite_query_stream_get_chunk(stream, 0);
        assert!(chunk.len > 0);
        vldb_sqlite_bytes_free(chunk);
        vldb_sqlite_query_stream_destroy(stream);

        vldb_sqlite_runtime_destroy(runtime);
    }

    #[test]
    fn json_and_typed_params_produce_equivalent_query_results() {
        let runtime = vldb_sqlite_runtime_create_default();
        let db_path = temp_db_path("typed-json-equivalent");
        let db_path_c = make_c_string(&db_path);
        let db = vldb_sqlite_runtime_open_database(runtime, db_path_c.as_ptr());

        let create_sql = make_c_string(
            "CREATE TABLE demo(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score REAL);",
        );
        let create_result =
            vldb_sqlite_database_execute_script(db, create_sql.as_ptr(), std::ptr::null(), 0, std::ptr::null());
        vldb_sqlite_execute_result_destroy(create_result);

        let insert_json = serde_json::json!({
            "db_path": db_path,
            "sql": "INSERT INTO demo(name, score) VALUES (?1, ?2)",
            "params": [
                {"kind":"string", "value":"alpha"},
                {"kind":"float64", "value":7.5}
            ]
        })
        .to_string();
        let insert_json_c = make_c_string(&insert_json);
        let insert_json_result = super::vldb_sqlite_execute_script_json(insert_json_c.as_ptr());
        assert!(!insert_json_result.is_null());
        vldb_sqlite_string_free(insert_json_result);

        let query_sql = make_c_string("SELECT id, name, score FROM demo WHERE name = ?1");
        let name = make_c_string("alpha");
        let typed_params = [VldbSqliteFfiValue {
            kind: VldbSqliteFfiValueKind::String,
            int64_value: 0,
            float64_value: 0.0,
            string_value: name.as_ptr(),
            bytes_value: Default::default(),
            bool_value: 0,
        }];
        let typed_result = vldb_sqlite_database_query_json(
            db,
            query_sql.as_ptr(),
            typed_params.as_ptr(),
            typed_params.len() as u64,
            std::ptr::null(),
        );
        let typed_json_ptr = vldb_sqlite_query_json_result_json_data(typed_result);
        let typed_json = c_string_to_string(typed_json_ptr).expect("typed query json should exist");
        vldb_sqlite_string_free(typed_json_ptr);
        vldb_sqlite_query_json_result_destroy(typed_result);

        let query_json_request = serde_json::json!({
            "db_path": db_path,
            "sql": "SELECT id, name, score FROM demo WHERE name = ?1",
            "params_json": "[\"alpha\"]"
        })
        .to_string();
        let query_json_request_c = make_c_string(&query_json_request);
        let query_json_ptr = vldb_sqlite_query_json_json(query_json_request_c.as_ptr());
        let query_json = c_string_to_string(query_json_ptr).expect("json query should exist");
        vldb_sqlite_string_free(query_json_ptr);

        assert_eq!(typed_json, serde_json::from_str::<JsonValue>(&query_json).expect("query_json_json should return serializable response")["json_data"]);

        let stream_json_request = serde_json::json!({
            "db_path": db_path,
            "sql": "SELECT id, name, score FROM demo ORDER BY id",
            "params_json": "[]"
        })
        .to_string();
        let stream_json_request_c = make_c_string(&stream_json_request);
        let stream_json_ptr = vldb_sqlite_query_stream_json(stream_json_request_c.as_ptr());
        let stream_json = c_string_to_string(stream_json_ptr).expect("query_stream_json should exist");
        let stream_payload: JsonValue = serde_json::from_str(&stream_json).expect("stream JSON should parse");
        let stream_id = stream_payload["stream_id"].as_u64().expect("stream_id should exist");
        assert!(stream_payload["chunk_count"].as_u64().unwrap_or(0) >= 1);
        assert!(stream_payload.get("chunks").is_none());
        vldb_sqlite_string_free(stream_json_ptr);

        let chunk_request = serde_json::json!({
            "stream_id": stream_id,
            "index": 0
        })
        .to_string();
        let chunk_request_c = make_c_string(&chunk_request);
        let chunk_json_ptr = vldb_sqlite_query_stream_chunk_json(chunk_request_c.as_ptr());
        let chunk_json = c_string_to_string(chunk_json_ptr).expect("query_stream_chunk_json should exist");
        let chunk_payload: JsonValue = serde_json::from_str(&chunk_json).expect("chunk JSON should parse");
        assert!(chunk_payload["byte_count"].as_u64().unwrap_or(0) > 0);
        let chunk_base64 = chunk_payload["chunk_base64"]
            .as_str()
            .expect("chunk_base64 should exist");
        let decoded_chunk = BASE64_STANDARD
            .decode(chunk_base64)
            .expect("chunk_base64 should decode");
        assert!(!decoded_chunk.is_empty());
        vldb_sqlite_string_free(chunk_json_ptr);

        let close_request = serde_json::json!({
            "stream_id": stream_id
        })
        .to_string();
        let close_request_c = make_c_string(&close_request);
        let close_ptr = vldb_sqlite_query_stream_close_json(close_request_c.as_ptr());
        assert!(!close_ptr.is_null());
        vldb_sqlite_string_free(close_ptr);

        vldb_sqlite_runtime_destroy(runtime);
    }

    #[test]
    fn json_execute_script_reuses_runtime_initialization_for_jieba_fts() {
        let db_path = temp_db_path("json-jieba-init");
        let create_json = serde_json::json!({
            "db_path": db_path,
            "sql": "CREATE VIRTUAL TABLE demo_fts USING fts5(content, tokenize='jieba')"
        })
        .to_string();
        let create_json_c = make_c_string(&create_json);
        let create_ptr = super::vldb_sqlite_execute_script_json(create_json_c.as_ptr());
        let create_payload = c_string_to_string(create_ptr).expect("execute_script_json should succeed");
        let create_value: JsonValue = serde_json::from_str(&create_payload).expect("create response should parse");
        assert_eq!(create_value["success"], true);
        vldb_sqlite_string_free(create_ptr);
    }
}
