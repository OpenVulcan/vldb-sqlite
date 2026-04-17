mod config;
mod db_lock;
mod fts;
mod logging;
#[allow(dead_code)]
mod runtime;
mod service;
mod tokenizer;

pub mod pb {
    tonic::include_proto!("vldb.sqlite.v1");
}

use crate::config::{BoxError, is_special_db_path, load_config};
use crate::db_lock::DatabaseFileLock;
use crate::logging::ServiceLogger;
use crate::pb::sqlite_service_server::SqliteServiceServer;
use crate::service::{SqliteGrpcService, effective_connection_pool_size, open_connection_pool};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let loaded = load_config()?;
    let config = loaded.config;

    if let Some(source) = &loaded.source {
        println!("loaded config from {}", source.display());
    } else {
        println!(
            "no config file found; using defaults (or provide -config <path> / place vldb-sqlite.json in the working directory or executable directory)"
        );
    }

    if !is_special_db_path(&config.db_path)
        && let Some(parent) = std::path::Path::new(&config.db_path).parent()
    {
        std::fs::create_dir_all(parent)?;
    }

    let db_file_lock =
        if config.hardening.enforce_db_file_lock && !is_special_db_path(&config.db_path) {
            Some(DatabaseFileLock::acquire(std::path::Path::new(
                &config.db_path,
            ))?)
        } else {
            None
        };

    let logger = ServiceLogger::new("vldb-sqlite", &config.logging)?;
    let connection_pool = open_connection_pool(&config)?;
    let addr = format!("{}:{}", config.host, config.port).parse()?;
    let effective_pool_size = effective_connection_pool_size(&config);

    println!("sqlite database: {}", config.db_path);
    println!(
        "connection_pool_size: {}",
        if effective_pool_size == config.connection_pool_size {
            effective_pool_size.to_string()
        } else {
            format!(
                "{} (effective {}, in-memory SQLite is forced to a single shared connection)",
                config.connection_pool_size, effective_pool_size
            )
        }
    );
    println!(
        "busy_timeout_ms: {} | journal_mode: {} | synchronous: {} | temp_store: {}",
        config.busy_timeout_ms,
        config.pragmas.journal_mode,
        config.pragmas.synchronous,
        config.pragmas.temp_store,
    );
    println!(
        "sqlite hardening: db_file_lock={} read_only={} uri_filenames={} trusted_schema={} defensive={} foreign_keys={} wal_autocheckpoint_pages={} cache_size_kib={} mmap_size_bytes={}",
        if config.hardening.enforce_db_file_lock {
            "enabled"
        } else {
            "disabled"
        },
        if config.hardening.read_only {
            "enabled"
        } else {
            "disabled"
        },
        if config.hardening.allow_uri_filenames {
            "enabled"
        } else {
            "disabled"
        },
        if config.hardening.trusted_schema {
            "enabled"
        } else {
            "disabled"
        },
        if config.hardening.defensive {
            "enabled"
        } else {
            "disabled"
        },
        if config.pragmas.foreign_keys {
            "enabled"
        } else {
            "disabled"
        },
        config.pragmas.wal_autocheckpoint_pages,
        config.pragmas.cache_size_kib,
        config.pragmas.mmap_size_bytes,
    );
    if let Some(db_file_lock) = &db_file_lock {
        println!("database lock file: {}", db_file_lock.path().display());
    } else if config.hardening.enforce_db_file_lock {
        println!("database lock file: skipped for in-memory or URI database path");
    }
    if let Some(log_path) = logger.log_path() {
        println!("request log file: {}", log_path.display());
    } else if config.logging.enabled {
        println!("request log file: disabled");
    }
    println!("gRPC listening on {addr}");

    let svc = SqliteGrpcService::new(connection_pool, logger, config.clone());

    Server::builder()
        .add_service(SqliteServiceServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    drop(db_file_lock);

    Ok(())
}

async fn shutdown_signal() {
    if tokio::signal::ctrl_c().await.is_ok() {
        println!("shutdown signal received, stopping vldb-sqlite");
    }
}
