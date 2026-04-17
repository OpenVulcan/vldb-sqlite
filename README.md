# vldb-sqlite

`vldb-sqlite` is a standalone Rust gRPC gateway for SQLite. It keeps the RPC surface close to `vldb-duckdb`, but the runtime behavior is adapted for SQLite features such as PRAGMAs, WAL, dynamic typing, and single-statement prepare semantics.

## What It Provides

- `ExecuteScript` for DDL, DML, and transaction scripts
- `ExecuteBatch` for high-throughput repeated writes inside one explicit SQLite transaction
- `QueryJson` for lightweight result sets returned as JSON text
- `QueryStream` for larger result sets returned as Arrow IPC over gRPC
- SQLite-oriented responses including `rows_changed` and `last_insert_rowid`
- Flat scalar parameter binding via protobuf `oneof`, while legacy `params_json` remains accepted for compatibility

## Dual-Mode SQL Parity

`vldb-sqlite` now keeps **gRPC mode** and **library/FFI mode** aligned on the same shared SQL core:

- gRPC mode still provides the full SQLite gateway surface
- Rust typed API can call the same execution core directly
- non-JSON FFI now covers `ExecuteScript`, `ExecuteBatch`, `QueryJson`, and `QueryStream`
- JSON FFI remains available as a compatibility layer for scripting and lightweight hosts

This means native hosts can now replace the old gRPC SQLite client path without losing generic SQL capability.

## Quick Start

Build:

```bash
cargo build
cargo build --release
```

Run:

```bash
cargo run --release -- --config ./vldb-sqlite.json
```

Run with Docker:

```bash
docker pull openvulcan/vldb-sqlite:latest
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  openvulcan/vldb-sqlite:latest
```

You can replace `latest` with a fixed release tag such as `vX.Y.Z`.

Run with Docker and a custom config:

```bash
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  -v "$(pwd)/docker/vldb-sqlite.json:/app/config/vldb-sqlite.json:ro" \
  openvulcan/vldb-sqlite:latest
```

The bind-mount example above uses Bash syntax; on Windows or PowerShell, replace `$(pwd)` with an absolute host path.

Default endpoint:

- gRPC: `127.0.0.1:19501`

## Project Docs

- 中文说明: [./docs/README.zh-CN.md](./docs/README.zh-CN.md)
- English guide: [./docs/README.en.md](./docs/README.en.md)
- gRPC 对接文档: [./docs/grpc-integration.zh-CN.md](./docs/grpc-integration.zh-CN.md)
- English gRPC integration guide: [./docs/grpc-integration.en.md](./docs/grpc-integration.en.md)
- 库模式说明: [./docs/LIBRARY_USAGE.zh-CN.md](./docs/LIBRARY_USAGE.zh-CN.md)
- Go FFI 示例: [./examples/go-ffi/README.md](./examples/go-ffi/README.md)
- 风险项修复计划: [./docs/fix-plan.md](./docs/fix-plan.md)

## Key Files

- Config: [`./vldb-sqlite.json`](./vldb-sqlite.json)
- Example config: [`./vldb-sqlite.json.example`](./vldb-sqlite.json.example)
- Service entrypoint: [`./src/main.rs`](./src/main.rs)
- Config loader: [`./src/config.rs`](./src/config.rs)
- gRPC contract: [`./proto/v1/sqlite.proto`](./proto/v1/sqlite.proto)
- FFI header: [`./include/vldb_sqlite.h`](./include/vldb_sqlite.h)
- Docker config: [`./docker/vldb-sqlite.json`](./docker/vldb-sqlite.json)
- Docker image: `openvulcan/vldb-sqlite`

## Current Support

### gRPC RPCs

| RPC | 用途 | 是否支持参数 | 返回类型 |
|-----|------|-------------|---------|
| `ExecuteScript` | DDL / 单条 DML / 无参数脚本 | 是 (`params` / `params_json`) | `ExecuteResponse` |
| `ExecuteBatch` | 高频重复写入（事务包裹） | 是 (`ExecuteBatchItem[]`) | `ExecuteBatchResponse` |
| `QueryJson` | 轻量查询（小结果集） | 是 | `QueryJsonResponse` (JSON) |
| `QueryStream` | 大结果集流式返回 | 是 | `stream QueryResponse` (Arrow IPC) |
| `TokenizeText` | 统一分词能力（`none/jieba`） | 否 | `TokenizeTextResponse` |
| `UpsertCustomWord` | 热更新专有词 | 否 | `DictionaryMutationResponse` |
| `RemoveCustomWord` | 删除专有词 | 否 | `DictionaryMutationResponse` |
| `ListCustomWords` | 列出当前库中的专有词 | 否 | `ListCustomWordsResponse` |
| `EnsureFtsIndex` | 确保某个 FTS 索引存在 | 否 | `EnsureFtsIndexResponse` |
| `RebuildFtsIndex` | 使用当前词典重建现有 FTS 索引 | 否 | `RebuildFtsIndexResponse` |
| `UpsertFtsDocument` | 写入或更新 FTS 文档 | 否 | `FtsMutationResponse` |
| `DeleteFtsDocument` | 删除 FTS 文档 | 否 | `FtsMutationResponse` |
| `SearchFts` | 标准化 BM25 检索 | 否 | `SearchFtsResponse` |

### Library / FFI Bootstrap

当前仓库已经开始提供独立库模式构建：

- 常规服务包：`vldb-sqlite-v<version>-<target>`
- 独立库包：`vldb-sqlite-lib-v<version>-<target>`

当前阶段库模式主要提供：

- `cdylib` / `rlib` 构建产物
- 头文件 [`./include/vldb_sqlite.h`](./include/vldb_sqlite.h)
- 纯库多库运行时 [`./src/runtime.rs`](./src/runtime.rs)
- Rust typed API：
  - `SqliteRuntime`
  - `SqliteDatabaseHandle`
- Go / 原生宿主主接口：
  - `vldb_sqlite_runtime_create_default()`
  - `vldb_sqlite_runtime_open_database()`
  - `vldb_sqlite_database_execute_script()` / `vldb_sqlite_database_execute_batch()`
  - `vldb_sqlite_database_query_json()` / `vldb_sqlite_database_query_stream()`
  - `vldb_sqlite_database_tokenize_text()`
  - `vldb_sqlite_database_upsert_custom_word()` / `vldb_sqlite_database_remove_custom_word()` / `vldb_sqlite_database_list_custom_words()`
  - `vldb_sqlite_database_ensure_fts_index()` / `vldb_sqlite_database_rebuild_fts_index()`
  - `vldb_sqlite_database_upsert_fts_document()` / `vldb_sqlite_database_delete_fts_document()` / `vldb_sqlite_database_search_fts()`
- JSON 兼容层：
  - `vldb_sqlite_library_info_json()` 等基础 FFI 引导接口
  - `vldb_sqlite_execute_script_json()` / `vldb_sqlite_execute_batch_json()`
  - `vldb_sqlite_query_json_json()`
  - `vldb_sqlite_query_stream_json()` / `vldb_sqlite_query_stream_chunk_json()` / `vldb_sqlite_query_stream_close_json()`
  - `vldb_sqlite_tokenize_text_json()`
  - `vldb_sqlite_upsert_custom_word_json()` / `vldb_sqlite_remove_custom_word_json()` / `vldb_sqlite_list_custom_words_json()`
  - `vldb_sqlite_ensure_fts_index_json()` / `vldb_sqlite_rebuild_fts_index_json()` / `vldb_sqlite_upsert_fts_document_json()` / `vldb_sqlite_delete_fts_document_json()` / `vldb_sqlite_search_fts_json()`

当前已经提供 `_vulcan_dict` 伴生词典表、可选 `none/jieba` 分词能力、SQLite 连接级 `tokenize='jieba'` tokenizer 注册，以及最小闭环的 FTS 建表 / 重建 / 写入 / 删除 / 检索接口。词典热更新后，可通过 `RebuildFtsIndex` 使用当前词典重新写入既有文档索引，避免旧文档仍停留在旧分词结果。`SearchFts` 当前已返回 `id / file_path / title / title_highlight / content_snippet / score / rank / raw_score / source / query_mode`，后续仍会继续把结果结构和融合体验打磨到更适合 RRF 混合检索的状态。对于 JSON 兼容层，`query_stream_json` 现在只返回流元信息与 `stream_id`；调用方需要继续使用 `query_stream_chunk_json` 逐块读取，并在完成后调用 `query_stream_close_json` 释放缓存结果。

### gRPC 与 lib 的边界

- **gRPC 模式**
  - 继续通过 `vldb-sqlite.json` 管理
  - 一个服务实例绑定一个 `db_path`
  - 不提供多库配置模型
- **lib 模式**
  - 不依赖配置文件
  - 通过 [`./src/runtime.rs`](./src/runtime.rs) 暴露 `SqliteRuntime`
  - 可以在同一进程内动态管理多个数据库句柄
  - 多库管理不反向干扰现有 gRPC 配置和运行逻辑

### Rust / Go / JSON 的推荐分层

- **Rust / MCP**
  - 优先直接使用 typed Rust API
  - 不建议为了统一风格再绕行 JSON FFI
- **Go / 其他原生宿主**
  - 优先使用非 JSON FFI 主接口
  - 高频路径不建议再走 JSON
- **JSON FFI**
  - 保留为兼容层
  - 主要服务 Lua / Python / 调试 / 其他潜在项目

### 参数绑定

| 方式 | 说明 |
|------|------|
| `params` (推荐) | protobuf `oneof` 标量: `int64`, `float64`, `string`, `bytes`, `bool`, `null` |
| `params_json` (兼容) | JSON 数组，仅允许标量: `null`/`bool`/`number`/`string` |

### 日志配置 (新增)

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `logging.log_sql` | `true` | 是否在日志中记录 SQL 语句 |
| `logging.sql_masking` | `false` | 是否对日志中的 SQL 字符串字面量脱敏（替换为 `***`） |

### 安全加固

| 特性 | 默认状态 |
|------|---------|
| WAL 模式 | 开启 (文件数据库) |
| Foreign keys | 开启 |
| Defensive 模式 | 开启 |
| Trusted schema | 关闭 |
| 数据库文件锁 | 开启 |
| gRPC deadline 中断 | 支持 (SQLite interrupt) |
| 错误重试元数据 | 支持 (`x-vldb-retryable`, `x-vldb-sqlite-code`) |
| panic 路径事务回滚 | 支持 |

### 承载能力 (默认 8 连接配置)

| 场景 | 推荐 QPS |
|------|---------|
| 简单读 (< 1ms) | ~8,000 |
| 普通读 (~10ms) | ~1,600 |
| 简单写 (~2ms) | ~250 |
| 批量写 (事务) | ~50 |

## Known Issues & Limitations

| 问题 | 严重度 | 说明 |
|------|--------|------|
| SQLite 单写者限制 | 架构级 | 写操作串行化，`connection_pool_size` 对写 QPS 无提升 |
| 流式分批仍物化 | 中 | `QueryStream` 按 1000 行/批物化，宽表大字段可能占用较多内存 |
| 多语句检测边界 | 低 | 状态机跳过分号但不处理 `x'hex'` 十六进制 blob 字面量中的分号 |
| SQL 日志脱敏范围 | 低 | `mask_sql` 只脱敏单引号字符串，不处理数字字面量中的敏感数据 |
| 原有 clippy 警告 | 极低 | `config.rs` 和 `logging.rs` 存在 3 个风格警告，不影响正确性 |

## Optimizations Applied

| 优化项 | 说明 |
|--------|------|
| 分批流式输出 | `QueryStream` 从全量物化改为 1000 行/批处理，内存占用从 O(N) 降为 O(batch_size) |
| panic 安全 | `ConnectionLease::Drop` 增加事务回滚，防止 panic 导致池中连接带脏事务 |
| 多语句检测 | `has_multiple_sql_statements` 从简单 `split(';')` 升级为状态机，正确处理字符串内分号 |
| NaN/Inf 安全 | 浮点数 NaN/Inf 转为 JSON 字符串，避免产生非法 JSON |
| SQL 脱敏 | 新增 `log_sql` / `sql_masking` 配置，防止敏感数据泄露到日志 |

## Integration Notes

- **所有请求设置 gRPC deadline** — 避免慢查询长期占用连接池
- **高频写入使用 `ExecuteBatch`** — 事务包裹减少 WAL 写竞争
- **`ExecuteScript` 是特权接口** — 无参数时可执行多语句 DDL/DML，生产环境建议配置 `hardening.read_only=true` 或通过网关做访问控制
- **复杂对象转 `TEXT`** — JSON/嵌套结构先在客户端序列化，再通过 `string_value` 写入
- **可重试错误识别** — 检查 trailer `x-vldb-retryable=true`，仅对 `SQLITE_BUSY`/`SQLITE_LOCKED`/`SQLITE_SCHEMA` 做指数退避重试
- **不带 TLS** — 当前服务为明文 gRPC，如对外暴露需通过反向代理 (如 Nginx/Envoy) 加 TLS
- **不包含向量/Embedding** — 该服务不处理高维向量数据
- **bundled SQLite** — 自带 `json1` 和 `fts5` 扩展，不支持动态加载扩展

## Runtime Notes

- SQLite work is isolated from tonic's async runtime through `spawn_blocking` plus a pre-opened connection pool.
- File-backed databases default to `journal_mode=WAL`.
- Every pooled connection injects `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, and `PRAGMA busy_timeout=5000` (configurable) during initialization.
- Startup fails fast if SQLite does not actually enter WAL mode for a file-backed database.
- `ExecuteBatch` wraps repeated prepared-statement execution in `BEGIN TRANSACTION` / `COMMIT`.
- `SQLITE_BUSY` / `SQLITE_LOCKED` failures are returned as gRPC `Unavailable` with retry metadata trailers: `x-vldb-retryable=true`.
- SQLite URI filenames remain opt-in through `hardening.allow_uri_filenames`.
- The service stays on bundled SQLite only; it does not expose dynamic extension loading.
- `QueryJson` and `QueryStream` intentionally reject multi-statement SQL.
- `ConnectionLease::Drop` now rolls back any active transaction before returning the connection to the pool, preventing dirty state after panics.
- `QueryStream` now processes rows in batches of 1000 instead of materializing the entire result set, reducing peak memory usage for large queries.
