# vldb-sqlite Docs

This folder contains the detailed service guide for `vldb-sqlite`.

## Overview

`vldb-sqlite` is a Rust gRPC microservice that wraps SQLite and exposes four main RPCs:

Main project:

- [OpenVulcan/vulcan-local-db](https://github.com/OpenVulcan/vulcan-local-db)

- `ExecuteScript`: run DDL, DML, or a single parameterized SQL statement without returning rows
- `ExecuteBatch`: run one prepared statement repeatedly inside a single explicit SQLite transaction
- `QueryJson`: execute a lightweight query and return JSON directly
- `QueryStream`: execute a single query and stream the result as Arrow IPC chunks

The current version also aligns **generic SQL capability across gRPC mode and library/FFI mode**:

- gRPC keeps the full SQLite gateway surface
- lib / FFI now covers `ExecuteScript`, `ExecuteBatch`, `QueryJson`, and `QueryStream`
- Rust typed API, non-JSON FFI, and JSON compatibility FFI all reuse the same shared SQL execution core
- native hosts can now replace the old SQLite gRPC client path without losing generic SQL support

Unlike `vldb-duckdb`, this service is tuned around SQLite semantics:

- SQLite URI filenames are optional and gated by config
- PRAGMA-based tuning uses SQLite-native knobs such as `journal_mode`, `synchronous`, `foreign_keys`, and `defensive`
- Flat scalar parameters are available through protobuf `oneof`; nested payloads should be pre-serialized to JSON strings by the client and stored as `TEXT`
- `ExecuteResponse` includes `rows_changed` and `last_insert_rowid`
- `ExecuteBatchResponse` reports both `rows_changed` and `statements_executed`
- `QueryJson` and `QueryStream` intentionally reject multi-statement SQL because SQLite prepares one statement at a time

## Key Files

- Project directory: `vldb-sqlite/`
- Main project repository: `OpenVulcan/vulcan-local-db`
- Top-level README: `vldb-sqlite/README.md`
- Example config: `vldb-sqlite/vldb-sqlite.json.example`
- Service entrypoint: `vldb-sqlite/src/main.rs`
- Config loader: `vldb-sqlite/src/config.rs`
- gRPC contract: `vldb-sqlite/proto/v1/sqlite.proto`
- gRPC integration guide: [grpc-integration.en.md](./grpc-integration.en.md)
- Library mode guide: [LIBRARY_USAGE.zh-CN.md](./LIBRARY_USAGE.zh-CN.md)
- Fix plan: [fix-plan.md](./fix-plan.md)

## Build

```bash
cd ./vldb-sqlite
cargo build
cargo build --release
```

## Start The Service

```bash
cd ./vldb-sqlite
cargo run --release -- --config ./vldb-sqlite.json
```

## Start With Docker

Pull and run the published image:

```bash
docker pull openvulcan/vldb-sqlite:latest
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  openvulcan/vldb-sqlite:latest
```

You can replace `latest` with a fixed release tag such as `vX.Y.Z`.

Use a custom config file:

```bash
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  -v "$(pwd)/docker/vldb-sqlite.json:/app/config/vldb-sqlite.json:ro" \
  openvulcan/vldb-sqlite:latest
```

Notes:

- The published image already includes a default config at `/app/config/vldb-sqlite.json`.
- SQLite data is stored under `/app/data`.
- The published Docker image address is `openvulcan/vldb-sqlite`.
- The bind-mount example uses Bash syntax; on Windows or PowerShell, use an absolute host path instead of `$(pwd)`.

## Default Config Shape

```json
{
  "host": "0.0.0.0",
  "port": 19501,
  "db_path": "./data/sqlite.db",
  "connection_pool_size": 8,
  "busy_timeout_ms": 5000,
  "pragmas": {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "foreign_keys": true,
    "temp_store": "MEMORY",
    "wal_autocheckpoint_pages": 1000,
    "cache_size_kib": 65536,
    "mmap_size_bytes": 268435456
  },
  "hardening": {
    "enforce_db_file_lock": true,
    "read_only": false,
    "allow_uri_filenames": false,
    "trusted_schema": false,
    "defensive": true
  },
  "logging": {
    "enabled": true,
    "log_sql": true,
    "sql_masking": false
  }
}
```

## Notes

- `db_path` supports local files, `:memory:`, and optional SQLite `file:` URI filenames.
- SQLite access stays off the async reactor by using `spawn_blocking` together with a pre-opened connection pool.
- File-backed databases default to `journal_mode=WAL`; if SQLite does not actually switch into WAL during startup, the service fails fast instead of continuing with a mismatched mode.
- Each pooled connection injects `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, and `PRAGMA busy_timeout`.
- `ExecuteBatch` uses `BEGIN TRANSACTION`, a reused prepared statement, and a single `COMMIT` for batch writes.
- `SQLITE_BUSY` / `SQLITE_LOCKED` errors carry gRPC trailer metadata such as `x-vldb-retryable=true` so callers can back off and retry.
- The service uses bundled SQLite only, which already includes `json1` and `fts5`; dynamic extension loading is intentionally disabled.
- When `logging.log_dir` is empty and `db_path` is a normal file path, logs default to a sibling `_log` directory.
- For `:memory:` and SQLite URI filenames, logs default to `vldb-sqlite-logs/` relative to the config file.
- `QueryStream` now uses batched materialization (1000 rows per batch) instead of full materialization, reducing peak memory from O(N) to O(batch_size).
- `ConnectionLease::Drop` checks for active transactions before returning connections to the pool, rolling back any uncommitted transactions to prevent dirty state after panics.

## SQL Log Masking

New configuration fields `logging.log_sql` and `logging.sql_masking`:

| Config | `log_sql` | `sql_masking` | SQL in logs |
|--------|-----------|---------------|-------------|
| No SQL logging | `false` | any | `<redacted>` |
| Plain text | `true` | `false` | Original SQL |
| Masked | `true` | `true` | String literals replaced with `***` |

Masking example:
```
Original: SELECT * FROM users WHERE password = 's3cret123'
Masked:   SELECT * FROM users WHERE password = ***
```

Enable `sql_masking: true` in production environments.
