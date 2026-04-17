use crate::config::{BoxError, LoggingConfig};
use chrono::Local;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct ServiceLogger {
    service_name: &'static str,
    config: LoggingConfig,
    file_state: Option<Mutex<LogFileState>>,
}

#[derive(Debug)]
struct LogFileState {
    file: File,
    path: PathBuf,
    date_key: String,
}

impl ServiceLogger {
    pub fn new(service_name: &'static str, config: &LoggingConfig) -> Result<Arc<Self>, BoxError> {
        if !config.enabled {
            return Ok(Arc::new(Self {
                service_name,
                config: config.clone(),
                file_state: None,
            }));
        }

        let file_state = if config.file_enabled {
            fs::create_dir_all(&config.log_dir)?;
            let date_key = current_log_date();
            let state = open_log_file_state(&config.log_dir, &config.log_file_name, &date_key)?;
            Some(Mutex::new(state))
        } else {
            None
        };

        Ok(Arc::new(Self {
            service_name,
            config: config.clone(),
            file_state,
        }))
    }

    pub fn config(&self) -> &LoggingConfig {
        &self.config
    }

    pub fn log_path(&self) -> Option<PathBuf> {
        self.file_state
            .as_ref()
            .and_then(|state| state.lock().ok().map(|guard| guard.path.clone()))
    }

    pub fn log(&self, category: &str, message: impl AsRef<str>) {
        if !self.config.enabled {
            return;
        }

        let line = format!(
            "[{}][{}][{}] {}",
            unix_millis_timestamp(),
            self.service_name,
            category,
            message.as_ref()
        );

        if self.config.stderr_enabled {
            eprintln!("{line}");
        }

        if let Some(file_state) = &self.file_state
            && let Ok(mut guard) = file_state.lock()
        {
            let current_date = current_log_date();
            if guard.date_key != current_date {
                if let Ok(rotated) = open_log_file_state(
                    &self.config.log_dir,
                    &self.config.log_file_name,
                    &current_date,
                ) {
                    *guard = rotated;
                }
            }

            let _ = writeln!(guard.file, "{line}");
        }
    }
}

fn open_log_file_state(
    log_dir: &Path,
    base_file_name: &str,
    date_key: &str,
) -> Result<LogFileState, BoxError> {
    let path = build_dated_log_path(log_dir, base_file_name, date_key);
    let file = OpenOptions::new().create(true).append(true).open(&path)?;

    Ok(LogFileState {
        file,
        path,
        date_key: date_key.to_string(),
    })
}

fn build_dated_log_path(log_dir: &Path, base_file_name: &str, date_key: &str) -> PathBuf {
    log_dir.join(build_dated_log_file_name(base_file_name, date_key))
}

fn build_dated_log_file_name(base_file_name: &str, date_key: &str) -> String {
    let base_path = Path::new(base_file_name);

    match (
        base_path.file_stem().and_then(|value| value.to_str()),
        base_path.extension().and_then(|value| value.to_str()),
    ) {
        (Some(stem), Some(extension)) if !extension.is_empty() => {
            format!("{stem}_{date_key}.{extension}")
        }
        (Some(stem), _) => format!("{stem}_{date_key}"),
        _ => format!("{base_file_name}_{date_key}"),
    }
}

fn current_log_date() -> String {
    Local::now().format("%Y-%m-%d").to_string()
}

fn unix_millis_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "{}.{}",
        now.as_secs(),
        format!("{:03}", now.subsec_millis())
    )
}

#[cfg(test)]
mod tests {
    use super::build_dated_log_file_name;

    #[test]
    fn dated_log_file_name_keeps_extension() {
        assert_eq!(
            build_dated_log_file_name("vldb-sqlite.log", "2026-03-31"),
            "vldb-sqlite_2026-03-31.log"
        );
    }

    #[test]
    fn dated_log_file_name_supports_names_without_extension() {
        assert_eq!(
            build_dated_log_file_name("vldb-sqlite", "2026-03-31"),
            "vldb-sqlite_2026-03-31"
        );
    }
}
