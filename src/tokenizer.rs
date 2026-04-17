use jieba_rs::{Jieba, TokenizeMode as JiebaTokenizeMode};
use rusqlite::ffi::{self, fts5_api, fts5_tokenizer_v2};
use rusqlite::types::ToSqlOutput;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, c_char, c_int, c_void};
use std::ptr;
use std::slice;
use std::str;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// 伴生系统词典表名（中英双语）。
/// System companion dictionary table name (bilingual).
pub const VULCAN_DICT_TABLE: &str = "_vulcan_dict";

/// SQLite FTS5 中注册的 Jieba tokenizer 名称（中英双语）。
/// Registered SQLite FTS5 tokenizer name for Jieba (bilingual).
const SQLITE_JIEBA_TOKENIZER_NAME: &CStr = c"jieba";

/// SQLite FTS5 API 指针绑定类型名（中英双语）。
/// SQLite FTS5 API pointer binding type tag (bilingual).
const SQLITE_FTS5_API_PTR_TYPE: &CStr = c"fts5_api_ptr";

/// 分词模式（中英双语）。
/// Tokenizer mode selector (bilingual).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TokenizerMode {
    /// 常规模式：不做中文切词，仅做轻量规范化。
    /// Plain mode: no Chinese segmentation, only light normalization.
    #[default]
    None,
    /// Jieba 模式：使用 Jieba 与内部伴生词典进行切词。
    /// Jieba mode: tokenize with Jieba and the internal companion dictionary.
    Jieba,
}

impl TokenizerMode {
    /// 获取稳定的字符串模式名（中英双语）。
    /// Return the stable string representation for the tokenizer mode (bilingual).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Jieba => "jieba",
        }
    }

    /// 从字符串解析分词模式（中英双语）。
    /// Parse tokenizer mode from string (bilingual).
    #[allow(dead_code)]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "none" | "plain" | "default" => Some(Self::None),
            "jieba" | "zh" | "zh_cn" => Some(Self::Jieba),
            _ => None,
        }
    }
}

/// 自定义词记录（中英双语）。
/// Custom dictionary entry (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomWordEntry {
    /// 业务专有词。
    /// Domain specific custom word.
    pub word: String,
    /// 词频权重，最终会映射到 Jieba 的频率。
    /// Token weight, mapped to Jieba frequency internally.
    pub weight: usize,
}

/// 分词输出（中英双语）。
/// Tokenization output payload (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenizeOutput {
    /// 实际使用的分词模式。
    /// Effective tokenizer mode used by the engine.
    pub tokenizer_mode: String,
    /// 轻量规范化后的原文。
    /// Lightly normalized text.
    pub normalized_text: String,
    /// 切分后的词元列表。
    /// Token list after segmentation.
    pub tokens: Vec<String>,
    /// 面向 SQLite FTS 查询的安全表达式。
    /// Safe SQLite FTS query expression derived from the tokens.
    pub fts_query: String,
}

/// 词典热更新结果（中英双语）。
/// Dictionary mutation result payload (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DictionaryMutationResult {
    /// 是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 操作说明。
    /// Human readable operation message.
    pub message: String,
    /// 受影响行数。
    /// Number of affected rows.
    pub affected_rows: u64,
}

/// 词典列表结果（中英双语）。
/// Custom dictionary listing result payload (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListCustomWordsResult {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 返回消息。
    /// Human readable response message.
    pub message: String,
    /// 当前启用的词典条目。
    /// Currently enabled custom dictionary entries.
    pub words: Vec<CustomWordEntry>,
}

/// 共享词典状态（中英双语）。
/// Shared dictionary state reused by all SQLite connections for the same database (bilingual).
#[derive(Debug, Default)]
struct SharedDictionaryState {
    custom_words: Vec<CustomWordEntry>,
}

/// SQLite 注册阶段持有的 tokenizer 上下文（中英双语）。
/// Tokenizer registration context held by SQLite during tokenizer registration (bilingual).
#[derive(Debug)]
struct RegisteredTokenizerContext {
    connection_handle: usize,
    shared_state: Arc<RwLock<SharedDictionaryState>>,
}

/// SQLite 实际创建出的 tokenizer 实例（中英双语）。
/// Concrete tokenizer instance created by SQLite FTS5 (bilingual).
#[derive(Debug)]
struct JiebaTokenizerInstance {
    shared_state: Arc<RwLock<SharedDictionaryState>>,
}

/// 词条切分片段（中英双语）。
/// Token span with UTF-8 byte offsets (bilingual).
#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenSpan {
    token: String,
    start_byte: usize,
    end_byte: usize,
}

/// 全局数据库词典注册表（中英双语）。
/// Global shared dictionary registry keyed by logical database identity (bilingual).
fn shared_dictionary_registry() -> &'static Mutex<HashMap<String, Arc<RwLock<SharedDictionaryState>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<RwLock<SharedDictionaryState>>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 已完成 SQLite tokenizer 注册的连接集合（中英双语）。
/// Set of SQLite connection handles that already registered the tokenizer (bilingual).
fn registered_connection_handles() -> &'static Mutex<HashSet<usize>> {
    static REGISTRY: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashSet::new()))
}

/// 确保伴生词典表存在（中英双语）。
/// Ensure the companion dictionary table exists (bilingual).
pub fn ensure_vulcan_dict_table(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (
            word TEXT PRIMARY KEY,
            weight INTEGER NOT NULL DEFAULT 1,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );",
        table_name = VULCAN_DICT_TABLE
    ))
}

/// 确保当前 SQLite 连接已注册 `jieba` tokenizer（中英双语）。
/// Ensure the current SQLite connection registered the `jieba` tokenizer (bilingual).
pub fn ensure_jieba_tokenizer_registered(connection: &Connection) -> rusqlite::Result<()> {
    ensure_vulcan_dict_table(connection)?;

    let db_key = connection_registry_key(connection)?;
    let shared_state = shared_dictionary_state_for_key(&db_key);
    refresh_shared_dictionary_state(connection, &shared_state)?;

    let connection_handle = sqlite_connection_handle(connection);
    {
        let registered = registered_connection_handles()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if registered.contains(&connection_handle) {
            return Ok(());
        }
    }

    let fts_api = fetch_fts5_api(connection)?;
    let registration_context = Box::new(RegisteredTokenizerContext {
        connection_handle,
        shared_state,
    });
    let registration_context_ptr = Box::into_raw(registration_context) as *mut c_void;
    let tokenizer = fts5_tokenizer_v2 {
        iVersion: 2,
        xCreate: Some(sqlite_jieba_tokenizer_create),
        xDelete: Some(sqlite_jieba_tokenizer_delete),
        xTokenize: Some(sqlite_jieba_tokenizer_tokenize),
    };

    let create = unsafe {
        (*fts_api)
            .xCreateTokenizer_v2
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?
    };

    let rc = unsafe {
        create(
            fts_api,
            SQLITE_JIEBA_TOKENIZER_NAME.as_ptr(),
            registration_context_ptr,
            &tokenizer as *const fts5_tokenizer_v2 as *mut fts5_tokenizer_v2,
            Some(sqlite_jieba_tokenizer_registration_destroy),
        )
    };
    if rc != ffi::SQLITE_OK {
        unsafe {
            drop(Box::from_raw(
                registration_context_ptr as *mut RegisteredTokenizerContext,
            ));
        }
        return Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(rc),
            Some("register jieba tokenizer failed / 注册 jieba tokenizer 失败".to_string()),
        ));
    }

    registered_connection_handles()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(connection_handle);

    Ok(())
}

/// 写入或更新自定义词（中英双语）。
/// Insert or update a custom dictionary word (bilingual).
pub fn upsert_custom_word(
    connection: &Connection,
    word: &str,
    weight: usize,
) -> rusqlite::Result<DictionaryMutationResult> {
    ensure_jieba_tokenizer_registered(connection)?;
    let trimmed = word.trim();
    if trimmed.is_empty() {
        return Ok(DictionaryMutationResult {
            success: false,
            message: "custom word must not be empty / 自定义词不能为空".to_string(),
            affected_rows: 0,
        });
    }

    let affected_rows = connection.execute(
        &format!(
            "INSERT INTO {table_name} (word, weight, enabled, created_at, updated_at)
             VALUES (?1, ?2, 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
             ON CONFLICT(word) DO UPDATE SET
                weight = excluded.weight,
                enabled = 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
            table_name = VULCAN_DICT_TABLE
        ),
        params![trimmed, weight as i64],
    )?;
    refresh_registered_dictionary(connection)?;

    Ok(DictionaryMutationResult {
        success: true,
        message: "custom word upserted / 自定义词已写入".to_string(),
        affected_rows: affected_rows as u64,
    })
}

/// 删除自定义词（中英双语）。
/// Remove a custom dictionary word (bilingual).
pub fn remove_custom_word(
    connection: &Connection,
    word: &str,
) -> rusqlite::Result<DictionaryMutationResult> {
    ensure_jieba_tokenizer_registered(connection)?;
    let trimmed = word.trim();
    if trimmed.is_empty() {
        return Ok(DictionaryMutationResult {
            success: false,
            message: "custom word must not be empty / 自定义词不能为空".to_string(),
            affected_rows: 0,
        });
    }

    let affected_rows = connection.execute(
        &format!("DELETE FROM {table_name} WHERE word = ?1", table_name = VULCAN_DICT_TABLE),
        params![trimmed],
    )?;
    refresh_registered_dictionary(connection)?;

    Ok(DictionaryMutationResult {
        success: true,
        message: if affected_rows > 0 {
            "custom word removed / 自定义词已删除".to_string()
        } else {
            "custom word not found / 自定义词不存在".to_string()
        },
        affected_rows: affected_rows as u64,
    })
}

/// 列出当前启用的自定义词（中英双语）。
/// List currently enabled custom dictionary words (bilingual).
pub fn load_custom_words(connection: &Connection) -> rusqlite::Result<Vec<CustomWordEntry>> {
    ensure_vulcan_dict_table(connection)?;
    let mut statement = connection.prepare(&format!(
        "SELECT word, weight
         FROM {table_name}
         WHERE enabled = 1
         ORDER BY word ASC",
        table_name = VULCAN_DICT_TABLE
    ))?;

    let mut rows = statement.query([])?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next()? {
        entries.push(CustomWordEntry {
            word: row.get::<_, String>(0)?,
            weight: row.get::<_, i64>(1)?.max(1) as usize,
        });
    }

    Ok(entries)
}

/// 列出当前启用的自定义词（中英双语）。
/// List enabled custom words with a structured result payload (bilingual).
pub fn list_custom_words(connection: &Connection) -> rusqlite::Result<ListCustomWordsResult> {
    let words = load_custom_words(connection)?;
    Ok(ListCustomWordsResult {
        success: true,
        message: format!(
            "listed {} custom words / 已列出 {} 个自定义词",
            words.len(),
            words.len()
        ),
        words,
    })
}

/// 执行分词并构造 FTS 查询表达式（中英双语）。
/// Perform tokenization and build an FTS query expression (bilingual).
pub fn tokenize_text(
    connection: Option<&Connection>,
    mode: TokenizerMode,
    text: &str,
    search_mode: bool,
) -> rusqlite::Result<TokenizeOutput> {
    let normalized_text = normalize_text(text);
    let tokens = match mode {
        TokenizerMode::None => tokenize_plain(&normalized_text),
        TokenizerMode::Jieba => tokenize_with_jieba(connection, &normalized_text, search_mode)?,
    };
    let fts_query = build_fts_query(&tokens, search_mode);

    Ok(TokenizeOutput {
        tokenizer_mode: mode.as_str().to_string(),
        normalized_text,
        tokens,
        fts_query,
    })
}

/// 轻量规范化文本（中英双语）。
/// Perform lightweight text normalization (bilingual).
fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// 常规模式下的分词实现（中英双语）。
/// Plain-mode tokenization implementation (bilingual).
fn tokenize_plain(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }

    let split = text
        .split(|ch: char| ch.is_whitespace() || ch.is_ascii_punctuation())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect::<Vec<_>>();

    if split.is_empty() {
        vec![text.to_string()]
    } else {
        split
    }
}

/// Jieba 模式下的分词实现（中英双语）。
/// Jieba-mode tokenization implementation (bilingual).
fn tokenize_with_jieba(
    connection: Option<&Connection>,
    text: &str,
    search_mode: bool,
) -> rusqlite::Result<Vec<String>> {
    if text.is_empty() {
        return Ok(Vec::new());
    }

    let custom_words = if let Some(connection) = connection {
        ensure_jieba_tokenizer_registered(connection)?;
        current_custom_words(connection)?
    } else {
        Vec::new()
    };

    Ok(jieba_token_spans(text, search_mode, &custom_words)
        .into_iter()
        .map(|span| span.token)
        .collect())
}

/// 构造安全的 FTS MATCH 表达式（中英双语）。
/// Build a safe FTS MATCH expression from token list (bilingual).
fn build_fts_query(tokens: &[String], search_mode: bool) -> String {
    tokens
        .iter()
        .filter(|token| !token.is_empty())
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(if search_mode { " OR " } else { " " })
}

/// 获取当前连接对应的共享自定义词快照（中英双语）。
/// Fetch the current shared custom word snapshot for the connection (bilingual).
fn current_custom_words(connection: &Connection) -> rusqlite::Result<Vec<CustomWordEntry>> {
    let db_key = connection_registry_key(connection)?;
    let shared_state = shared_dictionary_state_for_key(&db_key);
    Ok(shared_state
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .custom_words
        .clone())
}

/// 刷新当前数据库的共享词典状态（中英双语）。
/// Refresh the shared dictionary state for the current database (bilingual).
fn refresh_registered_dictionary(connection: &Connection) -> rusqlite::Result<()> {
    let db_key = connection_registry_key(connection)?;
    let shared_state = shared_dictionary_state_for_key(&db_key);
    refresh_shared_dictionary_state(connection, &shared_state)
}

/// 获取或创建指定数据库键的共享状态（中英双语）。
/// Get or create the shared state for a database registry key (bilingual).
fn shared_dictionary_state_for_key(
    db_key: &str,
) -> Arc<RwLock<SharedDictionaryState>> {
    let mut registry = shared_dictionary_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registry
        .entry(db_key.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(SharedDictionaryState::default())))
        .clone()
}

/// 从 SQLite 伴生表刷新共享状态（中英双语）。
/// Refresh shared state from the SQLite companion dictionary table (bilingual).
fn refresh_shared_dictionary_state(
    connection: &Connection,
    shared_state: &Arc<RwLock<SharedDictionaryState>>,
) -> rusqlite::Result<()> {
    let custom_words = load_custom_words(connection)?;
    let mut guard = shared_state
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.custom_words = custom_words;
    Ok(())
}

/// 生成数据库注册键（中英双语）。
/// Build a stable registry key for the current SQLite database (bilingual).
fn connection_registry_key(connection: &Connection) -> rusqlite::Result<String> {
    let handle = sqlite_connection_handle(connection);
    match connection.path() {
        Some(path) if !path.trim().is_empty() => Ok(path.to_string()),
        _ => Ok(format!(":memory:#{handle:x}")),
    }
}

/// 获取 SQLite 原生连接句柄数值（中英双语）。
/// Read the raw SQLite connection handle value (bilingual).
fn sqlite_connection_handle(connection: &Connection) -> usize {
    // SAFETY: `handle()` only returns the raw sqlite3 pointer for identity comparison.
    unsafe { connection.handle() as usize }
}

/// 读取当前连接的 FTS5 API 指针（中英双语）。
/// Fetch the FTS5 API pointer from the current SQLite connection (bilingual).
fn fetch_fts5_api(connection: &Connection) -> rusqlite::Result<*mut fts5_api> {
    let p_ret: *mut fts5_api = ptr::null_mut();
    let ptr_arg = ToSqlOutput::Pointer((&p_ret as *const *mut fts5_api as _, SQLITE_FTS5_API_PTR_TYPE, None));
    connection.query_row("SELECT fts5(?)", [ptr_arg], |_| Ok(()))?;
    if p_ret.is_null() {
        return Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some("fts5() returned a null API pointer / fts5() 返回了空指针".to_string()),
        ));
    }
    Ok(p_ret)
}

/// 使用当前共享词典生成带 UTF-8 偏移量的 Jieba 切分结果（中英双语）。
/// Produce Jieba token spans with UTF-8 byte offsets using current shared dictionary state (bilingual).
fn jieba_token_spans(
    text: &str,
    search_mode: bool,
    custom_words: &[CustomWordEntry],
) -> Vec<TokenSpan> {
    if text.is_empty() {
        return Vec::new();
    }

    let mut jieba = Jieba::new();
    for entry in custom_words {
        jieba.add_word(&entry.word, Some(entry.weight), None);
    }

    let char_to_byte = unicode_char_to_byte_offsets(text);
    let mode = if search_mode {
        JiebaTokenizeMode::Search
    } else {
        JiebaTokenizeMode::Default
    };

    jieba
        .tokenize(text, mode, true)
        .into_iter()
        .filter_map(|token| {
            let trimmed = token.word.trim();
            if trimmed.is_empty() {
                return None;
            }
            let start_byte = *char_to_byte.get(token.start)?;
            let end_byte = *char_to_byte.get(token.end)?;
            Some(TokenSpan {
                token: trimmed.to_string(),
                start_byte,
                end_byte,
            })
        })
        .collect()
}

/// 预计算 Unicode 字符索引到 UTF-8 字节索引的映射（中英双语）。
/// Precompute a mapping from Unicode character indices to UTF-8 byte indices (bilingual).
fn unicode_char_to_byte_offsets(text: &str) -> Vec<usize> {
    let mut offsets = text.char_indices().map(|(index, _)| index).collect::<Vec<_>>();
    offsets.push(text.len());
    offsets
}

/// SQLite 注册阶段：创建 tokenizer 实例（中英双语）。
/// SQLite registration callback: create a tokenizer instance (bilingual).
unsafe extern "C" fn sqlite_jieba_tokenizer_create(
    user_data: *mut c_void,
    _args: *mut *const c_char,
    _arg_count: c_int,
    out_tokenizer: *mut *mut ffi::Fts5Tokenizer,
) -> c_int {
    if user_data.is_null() || out_tokenizer.is_null() {
        return ffi::SQLITE_MISUSE;
    }

    // SAFETY: SQLite passes back the exact pointer that we registered in `xCreateTokenizer_v2`.
    let context = unsafe { &*(user_data as *const RegisteredTokenizerContext) };
    let tokenizer = Box::new(JiebaTokenizerInstance {
        shared_state: Arc::clone(&context.shared_state),
    });
    // SAFETY: SQLite owns the opaque tokenizer pointer until `xDelete` is called.
    unsafe {
        *out_tokenizer = Box::into_raw(tokenizer) as *mut ffi::Fts5Tokenizer;
    }
    ffi::SQLITE_OK
}

/// SQLite 注册阶段：销毁 tokenizer 实例（中英双语）。
/// SQLite registration callback: destroy a tokenizer instance (bilingual).
unsafe extern "C" fn sqlite_jieba_tokenizer_delete(tokenizer: *mut ffi::Fts5Tokenizer) {
    if tokenizer.is_null() {
        return;
    }
    // SAFETY: `tokenizer` originates from `Box::into_raw` in `sqlite_jieba_tokenizer_create`.
    unsafe {
        drop(Box::from_raw(tokenizer as *mut JiebaTokenizerInstance));
    }
}

/// SQLite 注册阶段：释放 tokenizer 注册上下文（中英双语）。
/// SQLite registration callback: destroy tokenizer registration context (bilingual).
unsafe extern "C" fn sqlite_jieba_tokenizer_registration_destroy(user_data: *mut c_void) {
    if user_data.is_null() {
        return;
    }

    // SAFETY: SQLite returns the exact user-data pointer registered in `xCreateTokenizer_v2`.
    let context = unsafe { Box::from_raw(user_data as *mut RegisteredTokenizerContext) };
    registered_connection_handles()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&context.connection_handle);
}

/// SQLite 注册阶段：执行真正的 Jieba tokenization（中英双语）。
/// SQLite registration callback: execute Jieba tokenization for FTS5 (bilingual).
#[allow(non_snake_case)]
unsafe extern "C" fn sqlite_jieba_tokenizer_tokenize(
    tokenizer: *mut ffi::Fts5Tokenizer,
    token_context: *mut c_void,
    flags: c_int,
    text_ptr: *const c_char,
    text_len: c_int,
    _locale_ptr: *const c_char,
    _locale_len: c_int,
    token_callback: Option<
        unsafe extern "C" fn(
            pCtx: *mut c_void,
            tflags: c_int,
            pToken: *const c_char,
            nToken: c_int,
            iStart: c_int,
            iEnd: c_int,
        ) -> c_int,
    >,
) -> c_int {
    if tokenizer.is_null() || token_context.is_null() || text_ptr.is_null() || text_len < 0 {
        return ffi::SQLITE_MISUSE;
    }
    let Some(token_callback) = token_callback else {
        return ffi::SQLITE_MISUSE;
    };

    // SAFETY: SQLite passes the tokenizer pointer created in `sqlite_jieba_tokenizer_create`.
    let tokenizer = unsafe { &*(tokenizer as *const JiebaTokenizerInstance) };
    let shared_state = tokenizer
        .shared_state
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // SAFETY: SQLite supplies `text_ptr` and `text_len` for the duration of this callback.
    let text_bytes = unsafe { slice::from_raw_parts(text_ptr as *const u8, text_len as usize) };
    let Ok(text) = str::from_utf8(text_bytes) else {
        return ffi::SQLITE_ERROR;
    };

    let search_mode = (flags & ffi::FTS5_TOKENIZE_QUERY) != 0 || (flags & ffi::FTS5_TOKENIZE_AUX) != 0;
    let spans = jieba_token_spans(text, search_mode, &shared_state.custom_words);
    for span in spans {
        let token_bytes = span.token.as_bytes();
        let rc = unsafe {
            token_callback(
                token_context,
                0,
                token_bytes.as_ptr() as *const c_char,
                token_bytes.len() as c_int,
                span.start_byte as c_int,
                span.end_byte as c_int,
            )
        };
        if rc != ffi::SQLITE_OK {
            return rc;
        }
    }

    ffi::SQLITE_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证伴生词典表热更新与 Jieba 分词协同工作（中英双语）。
    /// Verify companion dictionary hot updates cooperate with Jieba segmentation (bilingual).
    #[test]
    fn custom_words_affect_jieba_tokenization() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        ensure_jieba_tokenizer_registered(&connection)?;

        let before =
            tokenize_text(Some(&connection), TokenizerMode::Jieba, "市民田-女士急匆匆", false)?;
        assert!(!before.tokens.iter().any(|token| token == "田-女士"));

        let mutation = upsert_custom_word(&connection, "田-女士", 42)?;
        assert!(mutation.success);

        let after =
            tokenize_text(Some(&connection), TokenizerMode::Jieba, "市民田-女士急匆匆", false)?;
        assert!(after.tokens.iter().any(|token| token == "田-女士"));

        let removed = remove_custom_word(&connection, "田-女士")?;
        assert!(removed.success);
        Ok(())
    }

    /// 验证 SQLite FTS5 `tokenize='jieba'` 已真正注册生效（中英双语）。
    /// Verify SQLite FTS5 `tokenize='jieba'` is truly registered and active (bilingual).
    #[test]
    fn sqlite_fts_jieba_tokenizer_is_registered() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        ensure_jieba_tokenizer_registered(&connection)?;
        upsert_custom_word(&connection, "田-女士", 42)?;

        connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS mcp_memory_fts USING fts5(
                content,
                tokenize='jieba'
            );",
        )?;
        connection.execute(
            "INSERT INTO mcp_memory_fts (content) VALUES (?1)",
            params!["市民田-女士急匆匆"],
        )?;

        connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS mcp_memory_vocab USING fts5vocab(
                mcp_memory_fts,
                'instance'
            );",
        )?;

        let count: i64 = connection.query_row(
            "SELECT count(*) FROM mcp_memory_vocab WHERE term = ?1",
            params!["田-女士"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        Ok(())
    }
}
