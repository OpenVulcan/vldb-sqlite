use crate::fts::{
    delete_fts_document, ensure_fts_index, rebuild_fts_index, search_fts, upsert_fts_document,
};
use crate::library::library_info;
use crate::runtime::{SqliteDatabaseHandle, SqliteRuntime};
use crate::tokenizer::{
    ListCustomWordsResult, TokenizeOutput, TokenizerMode, list_custom_words, remove_custom_word,
    tokenize_text, upsert_custom_word,
};
use rusqlite::Connection;
use serde::Deserialize;
use std::ffi::{CStr, CString, c_char};
use std::path::Path;
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

/// 最近一次 FFI 错误消息缓存。
/// Cache for the latest FFI error message.
static LAST_ERROR: OnceLock<Mutex<Option<CString>>> = OnceLock::new();

fn last_error_slot() -> &'static Mutex<Option<CString>> {
    LAST_ERROR.get_or_init(|| Mutex::new(None))
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

    Connection::open(db_path).map_err(|error| format!("failed to open sqlite database: {error}"))
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
        vldb_sqlite_clear_last_error, vldb_sqlite_json_is_null,
        vldb_sqlite_last_error_message, vldb_sqlite_library_info_json, vldb_sqlite_string_free,
    };
    use serde_json::Value as JsonValue;
    use std::ffi::{CStr, c_char};

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

        vldb_sqlite_string_free(raw);
    }

    #[test]
    fn last_error_is_empty_after_success() {
        vldb_sqlite_clear_last_error();
        let raw = vldb_sqlite_last_error_message();
        assert!(raw.is_null());
    }
}
