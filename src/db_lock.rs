use fs4::fs_std::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct DatabaseFileLock {
    path: PathBuf,
    file: File,
}

impl DatabaseFileLock {
    pub fn acquire(db_path: &Path) -> io::Result<Self> {
        let path = derive_lock_path(db_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        file.try_lock_exclusive().map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to acquire exclusive SQLite database lock at {}: {err}",
                    path.display()
                ),
            )
        })?;

        let payload = format!("pid={} db_path={}\n", std::process::id(), db_path.display());
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(payload.as_bytes())?;

        Ok(Self { path, file })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DatabaseFileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn derive_lock_path(db_path: &Path) -> PathBuf {
    let parent = db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let file_name = db_path
        .file_name()
        .map(|name| format!("{}.vldb.lock", name.to_string_lossy()))
        .unwrap_or_else(|| "sqlite.vldb.lock".to_string());
    parent.join(file_name)
}

#[cfg(test)]
mod tests {
    use super::derive_lock_path;
    use std::path::PathBuf;

    #[test]
    fn derive_lock_path_places_lock_next_to_database() {
        assert_eq!(
            derive_lock_path(PathBuf::from("/srv/vldb/sqlite.db").as_path()),
            PathBuf::from("/srv/vldb/sqlite.db.vldb.lock")
        );
    }
}
