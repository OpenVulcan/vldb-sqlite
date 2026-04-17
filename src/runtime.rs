use crate::db_lock::DatabaseFileLock;
use crate::tokenizer::ensure_jieba_tokenizer_registered;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// 库模式与运行时模式通用错误类型。
/// Shared error type for library mode and runtime mode.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// SQLite 连接层 pragma 配置。
/// SQLite pragma configuration used by the library/runtime layer.
#[derive(Debug, Clone)]
pub struct SqlitePragmaOptions {
    /// 期望的 journal 模式。
    /// Requested journal mode.
    pub journal_mode: String,
    /// 期望的 synchronous 模式。
    /// Requested synchronous mode.
    pub synchronous: String,
    /// 是否启用外键。
    /// Whether foreign keys are enabled.
    pub foreign_keys: bool,
    /// 临时存储模式。
    /// Temporary storage mode.
    pub temp_store: String,
    /// WAL 自动 checkpoint 页数。
    /// WAL auto-checkpoint page count.
    pub wal_autocheckpoint_pages: u32,
    /// SQLite cache 大小（KiB）。
    /// SQLite cache size in KiB.
    pub cache_size_kib: i64,
    /// mmap 大小（字节）。
    /// mmap size in bytes.
    pub mmap_size_bytes: u64,
}

/// SQLite 连接层硬化配置。
/// SQLite hardening options used by the library/runtime layer.
#[derive(Debug, Clone)]
pub struct SqliteHardeningOptions {
    /// 是否启用数据库文件锁。
    /// Whether a database file lock should be enforced.
    pub enforce_db_file_lock: bool,
    /// 是否以只读模式打开数据库。
    /// Whether the database should be opened in read-only mode.
    pub read_only: bool,
    /// 是否允许 SQLite URI 文件名。
    /// Whether SQLite URI filenames are allowed.
    pub allow_uri_filenames: bool,
    /// 是否启用 trusted_schema。
    /// Whether trusted_schema is enabled.
    pub trusted_schema: bool,
    /// 是否启用 defensive 模式。
    /// Whether SQLite defensive mode is enabled.
    pub defensive: bool,
}

/// 单个 SQLite 库的程序化打开选项。
/// Programmatic open options for a single SQLite database.
#[derive(Debug, Clone)]
pub struct SqliteOpenOptions {
    /// 建议连接池大小；库 runtime 当前主要将其作为元信息保留。
    /// Suggested connection-pool size; currently retained as metadata by the library runtime.
    pub connection_pool_size: usize,
    /// busy_timeout，单位毫秒。
    /// busy_timeout in milliseconds.
    pub busy_timeout_ms: u64,
    /// SQLite pragma 选项。
    /// SQLite pragma options.
    pub pragmas: SqlitePragmaOptions,
    /// SQLite 安全硬化选项。
    /// SQLite hardening options.
    pub hardening: SqliteHardeningOptions,
}

impl Default for SqlitePragmaOptions {
    fn default() -> Self {
        Self {
            journal_mode: "WAL".to_string(),
            synchronous: "NORMAL".to_string(),
            foreign_keys: true,
            temp_store: "MEMORY".to_string(),
            wal_autocheckpoint_pages: 1_000,
            cache_size_kib: 65_536,
            mmap_size_bytes: 268_435_456,
        }
    }
}

impl Default for SqliteHardeningOptions {
    fn default() -> Self {
        Self {
            enforce_db_file_lock: true,
            read_only: false,
            allow_uri_filenames: false,
            trusted_schema: false,
            defensive: true,
        }
    }
}

impl Default for SqliteOpenOptions {
    fn default() -> Self {
        Self {
            connection_pool_size: 8,
            busy_timeout_ms: 5_000,
            pragmas: SqlitePragmaOptions::default(),
            hardening: SqliteHardeningOptions::default(),
        }
    }
}

/// `vldb-sqlite` 纯库多库运行时。
/// Pure library multi-database runtime for `vldb-sqlite`.
#[derive(Debug)]
pub struct SqliteRuntime {
    default_options: SqliteOpenOptions,
    databases: Mutex<HashMap<String, Arc<SqliteDatabaseHandle>>>,
}

/// 由 runtime 管理的单库句柄。
/// A single database handle managed by the runtime.
#[derive(Debug)]
pub struct SqliteDatabaseHandle {
    db_path: String,
    options: SqliteOpenOptions,
    file_lock: Option<DatabaseFileLock>,
}

impl Default for SqliteRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteRuntime {
    /// 使用默认参数创建一个新的多库 runtime。
    /// Create a new multi-database runtime with default options.
    pub fn new() -> Self {
        Self::with_default_options(SqliteOpenOptions::default())
    }

    /// 使用给定默认参数创建多库 runtime。
    /// Create a multi-database runtime with caller-provided default options.
    pub fn with_default_options(default_options: SqliteOpenOptions) -> Self {
        Self {
            default_options,
            databases: Mutex::new(HashMap::new()),
        }
    }

    /// 获取默认打开选项。
    /// Get the default open options.
    pub fn default_options(&self) -> &SqliteOpenOptions {
        &self.default_options
    }

    /// 打开或复用指定路径的数据库句柄。
    /// Open or reuse a database handle for the specified path.
    pub fn open_database(&self, db_path: impl AsRef<str>) -> Result<Arc<SqliteDatabaseHandle>, BoxError> {
        self.open_database_with_options(db_path, self.default_options.clone())
    }

    /// 使用显式选项打开或复用指定路径的数据库句柄。
    /// Open or reuse a database handle for the specified path with explicit options.
    pub fn open_database_with_options(
        &self,
        db_path: impl AsRef<str>,
        options: SqliteOpenOptions,
    ) -> Result<Arc<SqliteDatabaseHandle>, BoxError> {
        let normalized = normalize_db_path(
            db_path.as_ref(),
            options.hardening.allow_uri_filenames,
        )?;

        let mut guard = self
            .databases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(existing) = guard.get(&normalized) {
            return Ok(Arc::clone(existing));
        }

        let handle = Arc::new(SqliteDatabaseHandle::new(normalized.clone(), options)?);
        guard.insert(normalized, Arc::clone(&handle));
        Ok(handle)
    }

    /// 获取已缓存的数据库句柄。
    /// Get an already-cached database handle.
    pub fn get_database(&self, db_path: impl AsRef<str>) -> Option<Arc<SqliteDatabaseHandle>> {
        let normalized = normalize_db_path(
            db_path.as_ref(),
            self.default_options.hardening.allow_uri_filenames,
        )
        .ok()?;
        self.databases
            .lock()
            .ok()
            .and_then(|guard| guard.get(&normalized).cloned())
    }

    /// 关闭并移除指定数据库句柄。
    /// Close and remove a database handle.
    pub fn close_database(&self, db_path: impl AsRef<str>) -> bool {
        let normalized = match normalize_db_path(
            db_path.as_ref(),
            self.default_options.hardening.allow_uri_filenames,
        ) {
            Ok(path) => path,
            Err(_) => return false,
        };

        self.databases
            .lock()
            .map(|mut guard| guard.remove(&normalized).is_some())
            .unwrap_or(false)
    }

    /// 列出当前 runtime 中已注册的数据库路径。
    /// List database paths currently registered in the runtime.
    pub fn list_databases(&self) -> Vec<String> {
        self.databases
            .lock()
            .map(|guard| guard.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 返回当前 runtime 中已注册的数据库数量。
    /// Return the number of databases currently registered in the runtime.
    pub fn database_count(&self) -> usize {
        self.databases
            .lock()
            .map(|guard| guard.len())
            .unwrap_or_default()
    }
}

impl SqliteDatabaseHandle {
    /// 创建单库句柄。
    /// Create a single database handle.
    pub fn new(db_path: String, options: SqliteOpenOptions) -> Result<Self, BoxError> {
        if !is_special_db_path(&db_path) && !looks_like_sqlite_uri(&db_path) {
            if let Some(parent) = Path::new(&db_path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
        }

        let file_lock = if options.hardening.enforce_db_file_lock && !is_special_db_path(&db_path) {
            Some(DatabaseFileLock::acquire(Path::new(&db_path))?)
        } else {
            None
        };

        Ok(Self {
            db_path,
            options,
            file_lock,
        })
    }

    /// 获取数据库路径。
    /// Get the database path.
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// 获取当前句柄的打开选项。
    /// Get the current handle's open options.
    pub fn options(&self) -> &SqliteOpenOptions {
        &self.options
    }

    /// 返回关联的锁文件路径（如果有）。
    /// Return the associated lock-file path, if any.
    pub fn lock_path(&self) -> Option<&Path> {
        self.file_lock.as_ref().map(DatabaseFileLock::path)
    }

    /// 打开一个已按运行时规则初始化的 SQLite 连接。
    /// Open a SQLite connection initialized with runtime rules.
    pub fn open_connection(&self) -> Result<Connection, BoxError> {
        open_sqlite_connection(self.db_path.as_str(), &self.options)
    }
}

/// 判断特殊数据库路径（例如内存库或共享内存 URI）。
/// Detect special database paths such as in-memory or shared-memory URI paths.
pub fn is_special_db_path(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed == ":memory:" || trimmed.starts_with("file:") && trimmed.contains("mode=memory")
}

/// 按路径与选项打开 SQLite 连接，并自动应用 pragma 与 tokenizer 初始化。
/// Open a SQLite connection from a path and options, then apply pragmas and tokenizer setup.
pub fn open_sqlite_connection(
    db_path: &str,
    options: &SqliteOpenOptions,
) -> Result<Connection, BoxError> {
    let flags = build_sqlite_open_flags(options);
    let conn = if db_path == ":memory:" {
        Connection::open_in_memory_with_flags(flags)?
    } else {
        Connection::open_with_flags(db_path, flags)?
    };

    apply_sqlite_connection_pragmas(&conn, db_path, options)?;
    Ok(conn)
}

/// 对已打开连接应用运行时级别的 pragma 和 tokenizer 初始化。
/// Apply runtime-level pragmas and tokenizer initialization to an opened connection.
pub fn apply_sqlite_connection_pragmas(
    conn: &Connection,
    db_path: &str,
    options: &SqliteOpenOptions,
) -> Result<(), BoxError> {
    let mut effective_journal_mode = None;
    if !options.hardening.read_only {
        effective_journal_mode = Some(conn.pragma_update_and_check(
            None,
            "journal_mode",
            options.pragmas.journal_mode.as_str(),
            |row| row.get::<_, String>(0),
        )?);
        conn.pragma_update(None, "synchronous", options.pragmas.synchronous.as_str())?;
        conn.pragma_update(
            None,
            "wal_autocheckpoint",
            options.pragmas.wal_autocheckpoint_pages,
        )?;
    }

    conn.pragma_update(
        None,
        "busy_timeout",
        i64::try_from(options.busy_timeout_ms).unwrap_or(i64::MAX),
    )?;
    conn.pragma_update(None, "foreign_keys", options.pragmas.foreign_keys)?;
    conn.pragma_update(None, "temp_store", options.pragmas.temp_store.as_str())?;
    conn.pragma_update(None, "trusted_schema", options.hardening.trusted_schema)?;
    conn.pragma_update(None, "defensive", options.hardening.defensive)?;
    conn.pragma_update(None, "cache_size", -options.pragmas.cache_size_kib)?;
    conn.pragma_update(
        None,
        "mmap_size",
        i64::try_from(options.pragmas.mmap_size_bytes).unwrap_or(i64::MAX),
    )?;

    if options.hardening.read_only {
        conn.pragma_update(None, "query_only", true)?;
    }

    ensure_requested_wal_mode(conn, db_path, options, effective_journal_mode.as_deref())?;
    ensure_jieba_tokenizer_registered(conn)?;

    Ok(())
}

fn ensure_requested_wal_mode(
    conn: &Connection,
    db_path: &str,
    options: &SqliteOpenOptions,
    effective_journal_mode: Option<&str>,
) -> Result<(), BoxError> {
    if !options.pragmas.journal_mode.eq_ignore_ascii_case("WAL") || is_special_db_path(db_path) {
        return Ok(());
    }

    let effective_mode = match effective_journal_mode {
        Some(mode) => mode.trim().to_ascii_uppercase(),
        None => conn
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?
            .trim()
            .to_ascii_uppercase(),
    };

    if effective_mode == "WAL" {
        return Ok(());
    }

    Err(Box::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "SQLite journal_mode=WAL was requested for file database {db_path}, but SQLite reported journal_mode={effective_mode}"
        ),
    )))
}

/// 基于运行时打开选项构造 SQLite open flags。
/// Build SQLite open flags from runtime open options.
pub fn build_sqlite_open_flags(options: &SqliteOpenOptions) -> OpenFlags {
    let mut flags = OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if options.hardening.read_only {
        flags |= OpenFlags::SQLITE_OPEN_READ_ONLY;
    } else {
        flags |= OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    }
    if options.hardening.allow_uri_filenames {
        flags |= OpenFlags::SQLITE_OPEN_URI;
    }
    flags
}

fn normalize_db_path(raw: &str, allow_uri_filenames: bool) -> Result<String, BoxError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "db_path must not be empty",
        )));
    }

    if trimmed == ":memory:" {
        return Ok(trimmed.to_string());
    }

    if looks_like_sqlite_uri(trimmed) {
        if !allow_uri_filenames {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SQLite URI filenames are disabled for the current library runtime options",
            )));
        }
        return Ok(trimmed.to_string());
    }

    let path = Path::new(trimmed);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    Ok(normalize_path_like_string(&absolute))
}

fn normalize_path_like_string(path: &Path) -> String {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized.to_string_lossy().to_string()
}

fn looks_like_sqlite_uri(value: &str) -> bool {
    value.starts_with("file:")
}

#[cfg(test)]
mod tests {
    use super::{BoxError, SqliteRuntime, is_special_db_path};
    use crate::tokenizer::{list_custom_words, upsert_custom_word};
    use rusqlite::Connection;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_db_path(prefix: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_millis();
        std::env::temp_dir().join(format!("vldb-sqlite-runtime-{prefix}-{millis}.db"))
    }

    #[test]
    fn special_db_path_detection_matches_memory_variants() {
        assert!(is_special_db_path(":memory:"));
        assert!(is_special_db_path("file:demo.db?mode=memory&cache=shared"));
        assert!(!is_special_db_path("./data/demo.db"));
    }

    #[test]
    fn runtime_can_manage_multiple_databases_without_config_file() -> Result<(), BoxError> {
        let runtime = SqliteRuntime::new();
        let db_a_path = unique_test_db_path("a");
        let db_b_path = unique_test_db_path("b");
        let _cleanup_a = std::fs::remove_file(&db_a_path);
        let _cleanup_b = std::fs::remove_file(&db_b_path);

        let db_a = runtime.open_database(db_a_path.to_string_lossy())?;
        let db_b = runtime.open_database(db_b_path.to_string_lossy())?;

        {
            let conn_a = db_a.open_connection()?;
            upsert_custom_word(&conn_a, "田-女士", 42)?;
            let listed = list_custom_words(&conn_a)?;
            assert_eq!(listed.words.len(), 1);
        }

        {
            let conn_b = db_b.open_connection()?;
            let listed = list_custom_words(&conn_b)?;
            assert!(listed.words.is_empty());
            Connection::execute_batch(&conn_b, "CREATE TABLE IF NOT EXISTS marker(id INTEGER);")?;
        }

        assert_eq!(runtime.database_count(), 2);
        assert!(runtime.get_database(db_a_path.to_string_lossy()).is_some());
        assert!(runtime.get_database(db_b_path.to_string_lossy()).is_some());
        assert!(runtime.close_database(db_a_path.to_string_lossy()));
        assert_eq!(runtime.database_count(), 1);

        let _ = std::fs::remove_file(&db_a_path);
        let _ = std::fs::remove_file(&db_b_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_a_path.to_string_lossy()));
        let _ = std::fs::remove_file(format!("{}-shm", db_a_path.to_string_lossy()));
        let _ = std::fs::remove_file(format!("{}-wal", db_b_path.to_string_lossy()));
        let _ = std::fs::remove_file(format!("{}-shm", db_b_path.to_string_lossy()));
        Ok(())
    }
}
