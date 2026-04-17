use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Component, Path, PathBuf};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

const PRIMARY_CONFIG_NAME: &str = "vldb-sqlite.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub connection_pool_size: usize,
    pub busy_timeout_ms: u64,
    pub pragmas: PragmaConfig,
    pub hardening: HardeningConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PragmaConfig {
    pub journal_mode: String,
    pub synchronous: String,
    pub foreign_keys: bool,
    pub temp_store: String,
    pub wal_autocheckpoint_pages: u32,
    pub cache_size_kib: i64,
    pub mmap_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HardeningConfig {
    pub enforce_db_file_lock: bool,
    pub read_only: bool,
    pub allow_uri_filenames: bool,
    pub trusted_schema: bool,
    pub defensive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub enabled: bool,
    pub file_enabled: bool,
    pub stderr_enabled: bool,
    pub request_log_enabled: bool,
    pub log_sql: bool,
    pub sql_masking: bool,
    pub slow_query_log_enabled: bool,
    pub slow_query_threshold_ms: u64,
    pub slow_query_full_sql_enabled: bool,
    pub sql_preview_chars: usize,
    pub log_dir: PathBuf,
    pub log_file_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 19501,
            db_path: "./data/sqlite.db".to_string(),
            connection_pool_size: 8,
            busy_timeout_ms: 5_000,
            pragmas: PragmaConfig::default(),
            hardening: HardeningConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for PragmaConfig {
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

impl Default for HardeningConfig {
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

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            file_enabled: true,
            stderr_enabled: true,
            request_log_enabled: true,
            log_sql: true,
            sql_masking: false,
            slow_query_log_enabled: true,
            slow_query_threshold_ms: 1_000,
            slow_query_full_sql_enabled: true,
            sql_preview_chars: 160,
            log_dir: PathBuf::new(),
            log_file_name: "vldb-sqlite.log".to_string(),
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), BoxError> {
        if self.host.trim().is_empty() {
            return Err(invalid_input("config.host must not be empty"));
        }
        if self.port == 0 {
            return Err(invalid_input("config.port must be greater than 0"));
        }
        if self.db_path.trim().is_empty() {
            return Err(invalid_input("config.db_path must not be empty"));
        }
        if self.connection_pool_size == 0 {
            return Err(invalid_input(
                "config.connection_pool_size must be greater than 0",
            ));
        }
        if self.pragmas.cache_size_kib <= 0 {
            return Err(invalid_input(
                "config.pragmas.cache_size_kib must be greater than 0",
            ));
        }
        validate_membership(
            &self.pragmas.journal_mode,
            "config.pragmas.journal_mode",
            &["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"],
        )?;
        validate_membership(
            &self.pragmas.synchronous,
            "config.pragmas.synchronous",
            &["OFF", "NORMAL", "FULL", "EXTRA"],
        )?;
        validate_membership(
            &self.pragmas.temp_store,
            "config.pragmas.temp_store",
            &["DEFAULT", "FILE", "MEMORY"],
        )?;
        if self.pragmas.mmap_size_bytes > i64::MAX as u64 {
            return Err(invalid_input(
                "config.pragmas.mmap_size_bytes must fit within signed 64-bit range",
            ));
        }
        self.logging.validate()?;
        Ok(())
    }
}

impl LoggingConfig {
    fn validate(&self) -> Result<(), BoxError> {
        if self.sql_preview_chars == 0 {
            return Err(invalid_input(
                "config.logging.sql_preview_chars must be greater than 0",
            ));
        }
        if self.slow_query_threshold_ms == 0 {
            return Err(invalid_input(
                "config.logging.slow_query_threshold_ms must be greater than 0",
            ));
        }
        if self.file_enabled && self.log_file_name.trim().is_empty() {
            return Err(invalid_input(
                "config.logging.log_file_name must not be empty when file logging is enabled",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: Config,
    pub source: Option<PathBuf>,
}

pub fn load_config() -> Result<LoadedConfig, BoxError> {
    if let Some(explicit_path) = parse_config_arg()? {
        return load_config_file(explicit_path);
    }

    for candidate in default_search_paths()? {
        if candidate.is_file() {
            return load_config_file(candidate);
        }
    }

    let cwd = env::current_dir()?;
    let mut config = Config::default();
    resolve_config_paths(&mut config, &cwd)?;
    config.validate()?;

    Ok(LoadedConfig {
        config,
        source: None,
    })
}

fn load_config_file(path: PathBuf) -> Result<LoadedConfig, BoxError> {
    let cwd = env::current_dir()?;
    let resolved_path = expand_path(path, &cwd)?;
    let raw = fs::read_to_string(&resolved_path)?;
    let mut config: Config = serde_json::from_str(&raw)?;

    let base_dir = resolved_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| cwd.clone());

    resolve_config_paths(&mut config, &base_dir)?;
    config.validate()?;

    Ok(LoadedConfig {
        config,
        source: Some(resolved_path),
    })
}

fn resolve_config_paths(config: &mut Config, base_dir: &Path) -> Result<(), BoxError> {
    config.pragmas.journal_mode = config.pragmas.journal_mode.trim().to_ascii_uppercase();
    config.pragmas.synchronous = config.pragmas.synchronous.trim().to_ascii_uppercase();
    config.pragmas.temp_store = config.pragmas.temp_store.trim().to_ascii_uppercase();
    config.db_path = resolve_db_path(
        &config.db_path,
        base_dir,
        config.hardening.allow_uri_filenames,
    )?;
    config.logging.log_dir = resolve_log_dir(&config.logging.log_dir, &config.db_path, base_dir)?;
    Ok(())
}

fn resolve_db_path(
    raw: &str,
    base_dir: &Path,
    allow_uri_filenames: bool,
) -> Result<String, BoxError> {
    let trimmed = raw.trim();
    if trimmed == ":memory:" {
        return Ok(trimmed.to_string());
    }
    if looks_like_sqlite_uri(trimmed) {
        if !allow_uri_filenames {
            return Err(invalid_input(
                "config.db_path uses a SQLite URI filename but config.hardening.allow_uri_filenames is false",
            ));
        }
        return Ok(trimmed.to_string());
    }

    Ok(expand_path(trimmed, base_dir)?
        .to_string_lossy()
        .to_string())
}

fn resolve_log_dir(
    configured_dir: &Path,
    db_path: &str,
    base_dir: &Path,
) -> Result<PathBuf, BoxError> {
    if !configured_dir.as_os_str().is_empty() {
        return expand_path(configured_dir, base_dir);
    }

    if is_special_db_path(db_path) {
        return Ok(base_dir.join("vldb-sqlite-logs"));
    }

    Ok(derive_default_log_dir(Path::new(db_path)))
}

fn parse_config_arg() -> Result<Option<PathBuf>, BoxError> {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0usize;

    while i < args.len() {
        let arg = &args[i];
        if arg == "-config" || arg == "--config" {
            let value = args
                .get(i + 1)
                .ok_or_else(|| invalid_input("missing file path after -config/--config"))?;
            return Ok(Some(PathBuf::from(value)));
        }

        if let Some(value) = arg.strip_prefix("-config=") {
            return Ok(Some(PathBuf::from(value)));
        }

        if let Some(value) = arg.strip_prefix("--config=") {
            return Ok(Some(PathBuf::from(value)));
        }

        i += 1;
    }

    Ok(None)
}

fn default_search_paths() -> Result<Vec<PathBuf>, BoxError> {
    let cwd = env::current_dir()?;
    let mut candidates = vec![cwd.join(PRIMARY_CONFIG_NAME)];

    if let Ok(exe) = env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let exe_config = dir.join(PRIMARY_CONFIG_NAME);
        if exe_config != candidates[0] {
            candidates.push(exe_config);
        }
    }

    Ok(candidates)
}

fn derive_default_log_dir(db_path: &Path) -> PathBuf {
    let parent = db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    match db_path.file_stem().or_else(|| db_path.file_name()) {
        Some(stem) => parent.join(format!("{}_log", stem.to_string_lossy())),
        None => parent.join("sqlite_log"),
    }
}

fn expand_path<P: AsRef<Path>>(path: P, base_dir: &Path) -> Result<PathBuf, BoxError> {
    let with_home = expand_tilde(path.as_ref())?;
    let absolute_or_relative = if with_home.is_absolute() {
        with_home
    } else {
        base_dir.join(with_home)
    };

    Ok(normalize_path(absolute_or_relative))
}

fn expand_tilde(path: &Path) -> Result<PathBuf, BoxError> {
    let raw = path.to_string_lossy();

    if raw == "~" {
        let home = home_dir().ok_or_else(|| invalid_input("cannot expand '~': HOME is not set"))?;
        return Ok(home);
    }

    if let Some(rest) = raw.strip_prefix("~/").or_else(|| raw.strip_prefix("~\\")) {
        let home =
            home_dir().ok_or_else(|| invalid_input("cannot expand '~/': HOME is not set"))?;
        return Ok(home.join(rest));
    }

    Ok(path.to_path_buf())
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .or_else(
            || match (env::var_os("HOMEDRIVE"), env::var_os("HOMEPATH")) {
                (Some(drive), Some(path)) => {
                    let mut buf = PathBuf::from(drive);
                    buf.push(path);
                    Some(buf)
                }
                _ => None,
            },
        )
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }

    normalized
}

fn validate_membership(
    value: &str,
    field_name: &str,
    allowed_values: &[&str],
) -> Result<(), BoxError> {
    if allowed_values.iter().any(|candidate| *candidate == value) {
        Ok(())
    } else {
        Err(invalid_input(format!(
            "{field_name} must be one of: {}",
            allowed_values.join(", ")
        )))
    }
}

fn invalid_input(message: impl Into<String>) -> BoxError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

pub fn looks_like_sqlite_uri(value: &str) -> bool {
    value.to_ascii_lowercase().starts_with("file:")
}

pub fn is_special_db_path(value: &str) -> bool {
    value == ":memory:" || looks_like_sqlite_uri(value)
}

#[cfg(test)]
mod tests {
    use super::{
        HardeningConfig, PragmaConfig, derive_default_log_dir, is_special_db_path, resolve_db_path,
        resolve_log_dir,
    };
    use std::path::{Path, PathBuf};

    #[test]
    fn default_log_dir_uses_db_file_stem() {
        let db_path = PathBuf::from("/srv/vldb/sqlite.db");
        assert_eq!(
            derive_default_log_dir(&db_path),
            PathBuf::from("/srv/vldb/sqlite_log")
        );
    }

    #[test]
    fn explicit_relative_log_dir_is_resolved_from_config_dir() {
        let resolved = resolve_log_dir(
            Path::new("./logs"),
            "/srv/vldb/sqlite.db",
            Path::new("/etc/vldb"),
        )
        .expect("resolve log dir");

        assert_eq!(resolved, PathBuf::from("/etc/vldb/logs"));
    }

    #[test]
    fn in_memory_db_uses_config_relative_log_dir() {
        let resolved = resolve_log_dir(Path::new(""), ":memory:", Path::new("/etc/vldb"))
            .expect("resolve log dir");
        assert_eq!(resolved, PathBuf::from("/etc/vldb/vldb-sqlite-logs"));
    }

    #[test]
    fn sqlite_uri_requires_opt_in() {
        let error = resolve_db_path("file:demo.db?mode=ro", Path::new("/etc/vldb"), false)
            .expect_err("uri path should require opt-in");
        assert!(error.to_string().contains("allow_uri_filenames"));
    }

    #[test]
    fn sqlite_uri_is_preserved_when_enabled() {
        let resolved = resolve_db_path("file:demo.db?mode=ro", Path::new("/etc/vldb"), true)
            .expect("resolve sqlite uri");
        assert_eq!(resolved, "file:demo.db?mode=ro");
    }

    #[test]
    fn relative_db_path_is_resolved_from_config_dir() {
        let resolved =
            resolve_db_path("./data/demo.db", Path::new("/etc/vldb"), false).expect("resolve path");
        assert_eq!(
            PathBuf::from(resolved),
            PathBuf::from("/etc/vldb/data/demo.db")
        );
    }

    #[test]
    fn special_db_path_detection_matches_memory_and_uri_paths() {
        assert!(is_special_db_path(":memory:"));
        assert!(is_special_db_path("file:demo.db?mode=memory&cache=shared"));
        assert!(!is_special_db_path("./data/demo.db"));
    }

    #[test]
    fn default_sqlite_profiles_are_sqlite_focused() {
        let pragmas = PragmaConfig::default();
        let hardening = HardeningConfig::default();

        assert_eq!(pragmas.journal_mode, "WAL");
        assert_eq!(pragmas.synchronous, "NORMAL");
        assert!(pragmas.foreign_keys);
        assert!(hardening.defensive);
    }
}
