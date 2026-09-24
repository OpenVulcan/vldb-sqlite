// The runtime uses this module to hold one exclusive owner for each on-disk SQLite database.
// 运行时通过本模块为每个磁盘 SQLite 数据库保持唯一的独占持有者。
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
    /// Acquire the sidecar lock for db_path without blocking, returning its owner or an I/O error.
    /// 非阻塞获取 db_path 对应的旁路锁，返回持有者或输入输出错误。
    /// Contention returns WouldBlock before any lock payload is changed.
    /// 锁竞争在修改任何锁内容之前返回 WouldBlock。
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

        // fs4 reports contention as Ok(false), so success of the I/O call alone does not establish ownership.
        // fs4 通过 Ok(false) 表示锁竞争，输入输出调用未报错并不代表已取得所有权。
        let acquired = file.try_lock_exclusive().map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to acquire exclusive SQLite database lock at {}: {err}",
                    path.display()
                ),
            )
        })?;
        if !acquired {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "failed to acquire exclusive SQLite database lock at {}: already locked",
                    path.display()
                ),
            ));
        }

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
    use super::{DatabaseFileLock, derive_lock_path};
    use std::io;
    use std::path::PathBuf;

    #[test]
    fn derive_lock_path_places_lock_next_to_database() {
        assert_eq!(
            derive_lock_path(PathBuf::from("/srv/vldb/sqlite.db").as_path()),
            PathBuf::from("/srv/vldb/sqlite.db.vldb.lock")
        );
    }

    /// Require contention to report WouldBlock before touching another owner's lock payload.
    /// 要求锁竞争在触碰原持有者的锁内容之前返回 WouldBlock。
    /// Uses an isolated temporary database path and returns only when release and reacquisition succeed.
    /// 使用隔离的临时数据库路径，仅在释放与重新获取都成功时结束。
    #[test]
    fn contention_returns_would_block_and_owner_can_release() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("vldb-sqlite-lock-{}-{unique}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let database = root.join("source.db");
        let owner = DatabaseFileLock::acquire(&database).unwrap();

        // Both attempts must fail, and dropping a rejected contender must not release the owner.
        // 两次竞争都必须失败，失败的竞争者析构也不得释放原持有者的锁。
        for _ in 0..2 {
            let error = DatabaseFileLock::acquire(&database)
                .expect_err("a second owner must not acquire the database lock");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        }
        drop(owner);
        let next_owner = DatabaseFileLock::acquire(&database).unwrap();
        drop(next_owner);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
