# vldb-sqlite 文档

这个目录用于存放 `vldb-sqlite` 子项目的详细说明。

## 项目是什么

`vldb-sqlite` 是一个基于 Rust 和 gRPC 的 SQLite 微服务。它提供四个核心 RPC：

主项目：

- [OpenVulcan/vulcan-local-db](https://github.com/OpenVulcan/vulcan-local-db)

- `ExecuteScript`：执行 DDL、DML 或单条参数化 SQL，不返回结果集
- `ExecuteBatch`：在一个显式 SQLite 事务里重复执行同一条预编译语句
- `QueryJson`：执行轻量查询，并把结果直接转成 JSON 字符串返回
- `QueryStream`：执行单条查询，并把结果以 Arrow IPC 字节流返回

当前版本还新增了 **库模式与 gRPC 模式的通用 SQL 双态对齐**：

- gRPC 继续保留完整 SQLite 网关能力
- lib / FFI 已补齐 `ExecuteScript`、`ExecuteBatch`、`QueryJson`、`QueryStream`
- Rust typed API、非 JSON FFI、JSON 兼容接口共同复用同一套 SQL 核心执行逻辑
- 上层宿主现在可以不再依赖旧的 SQLite gRPC 客户端，也能完整覆盖通用 SQL 能力

相对 `vldb-duckdb`，它更贴近 SQLite 自身特性：

- 可选支持 SQLite URI filename，但必须通过配置显式开启
- 通过 SQLite 原生 PRAGMA 暴露 `journal_mode`、`synchronous`、`foreign_keys`、`defensive` 等能力
- 参数契约支持 protobuf `oneof` 扁平标量；复杂对象或数组应在客户端先序列化成 JSON 字符串，再以 `TEXT` 写入 SQLite
- `ExecuteResponse` 增加了 `rows_changed` 和 `last_insert_rowid`
- `ExecuteBatchResponse` 额外返回 `statements_executed`
- `QueryJson` / `QueryStream` 会主动拒绝多语句 SQL，避免 SQLite 单语句 prepare 语义带来的歧义

## 关键文件

- 工程目录：`vldb-sqlite/`
- 主项目仓库：`OpenVulcan/vulcan-local-db`
- 顶层 README：`vldb-sqlite/README.md`
- 配置示例：`vldb-sqlite/vldb-sqlite.json.example`
- 服务入口：`vldb-sqlite/src/main.rs`
- 配置加载：`vldb-sqlite/src/config.rs`
- gRPC 协议：`vldb-sqlite/proto/v1/sqlite.proto`
- gRPC 对接文档：[grpc-integration.zh-CN.md](./grpc-integration.zh-CN.md)
- 库模式说明：[LIBRARY_USAGE.zh-CN.md](./LIBRARY_USAGE.zh-CN.md)
- 风险项修复计划：[fix-plan.md](./fix-plan.md)

## 如何构建

```bash
cd ./vldb-sqlite
cargo build
cargo build --release
```

## 如何启动

```bash
cd ./vldb-sqlite
cargo run --release -- --config ./vldb-sqlite.json
```

## 如何使用 Docker 启动

直接拉取并运行镜像：

```bash
docker pull openvulcan/vldb-sqlite:latest
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  openvulcan/vldb-sqlite:latest
```

如果你希望固定版本，也可以把 `latest` 换成具体标签，例如 `vX.Y.Z`。

如果要覆盖默认配置，可以挂载自定义配置文件：

```bash
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  -v "$(pwd)/docker/vldb-sqlite.json:/app/config/vldb-sqlite.json:ro" \
  openvulcan/vldb-sqlite:latest
```

补充说明：

- 发布镜像内已经自带默认配置文件 `/app/config/vldb-sqlite.json`。
- SQLite 数据目录在容器内是 `/app/data`。
- 对外发布的 Docker 镜像地址是 `openvulcan/vldb-sqlite`。
- 上面的挂载示例按 Bash 写法展示；如果是在 Windows 或 PowerShell 下运行，建议直接改成宿主机绝对路径。

## 默认配置结构

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

## 说明

- `db_path` 支持普通本地文件、`:memory:`，以及可选的 SQLite `file:` URI filename。
- SQLite 实际执行始终放在 `spawn_blocking` 工作线程里，前面再配一个预建连接池，避免阻塞 tonic/tokio 的异步运行时。
- 普通文件数据库默认使用 `journal_mode=WAL`；如果服务启动时 SQLite 没有真正切换到 WAL，进程会直接报错退出，避免"配置是 WAL、实际不是 WAL"的情况。
- 每个池化连接在初始化时都会注入 `PRAGMA journal_mode=WAL`、`PRAGMA synchronous=NORMAL` 和 `PRAGMA busy_timeout`。
- `ExecuteBatch` 会显式执行 `BEGIN TRANSACTION`，复用一条 Prepared Statement 循环绑定参数，最后统一 `COMMIT`。
- `SQLITE_BUSY` / `SQLITE_LOCKED` 会映射成带 `x-vldb-retryable=true` trailer 的 gRPC 错误，便于 Go 客户端做退避重试。
- 服务只使用 bundled SQLite，自带 `json1` 和 `fts5`，不会开放动态扩展加载。
- `logging.log_dir` 为空且 `db_path` 是普通文件路径时，日志默认写到同级 `_log` 目录。
- `:memory:` 和 SQLite URI filename 会默认把日志写到配置文件目录下的 `vldb-sqlite-logs/`。
- `QueryStream` 采用分批物化策略（每批 1000 行），避免全量加载大结果集，内存占用从 O(N) 降为 O(batch_size)。
- `ConnectionLease::Drop` 会在连接返回池前检查事务状态，如有未提交的事务则自动 ROLLBACK，防止 panic 导致脏连接。

## 日志脱敏

新增 `logging.log_sql` 和 `logging.sql_masking` 配置项：

| 配置 | `log_sql` | `sql_masking` | 日志中的 SQL |
|------|-----------|---------------|-------------|
| 不记录 SQL | `false` | 任意 | `<redacted>` |
| 记录明文 | `true` | `false` | 原始 SQL |
| 记录脱敏 | `true` | `true` | 字符串字面量替换为 `***` |

脱敏示例：
```
原始: SELECT * FROM users WHERE password = 's3cret123'
脱敏: SELECT * FROM users WHERE password = ***
```

生产环境建议开启 `sql_masking: true`。
