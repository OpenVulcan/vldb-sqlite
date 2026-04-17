# vldb-sqlite gRPC 对接文档

本文面向 `vldb-sqlite` 的客户端接入方，重点说明如何通过 gRPC 调用 SQLite 微服务、如何绑定参数、以及如何处理可重试错误。

补充说明：当前仓库已经把通用 SQL 核心下沉为共享模块，因此：

- gRPC 仍然是完整的远程 SQLite 网关
- lib / FFI 也已经补齐 `ExecuteScript`、`ExecuteBatch`、`QueryJson`、`QueryStream`
- 两条形态在通用 SQL 语义上尽量保持一致，避免长期维护两套漂移实现

## 0. 文档索引

- 中文说明: [./docs/README.zh-CN.md](./README.zh-CN.md)
- English README: [./docs/README.en.md](./README.en.md)
- 风险项修复计划: [./docs/fix-plan.md](./fix-plan.md)

## 1. 服务信息

- gRPC Service: `vldb.sqlite.v1.SqliteService`
- 默认监听地址: `127.0.0.1:19501`
- 传输方式: 明文 gRPC，无 TLS
- 协议文件: `proto/v1/sqlite.proto`
- Docker 镜像地址: `openvulcan/vldb-sqlite`
- 主项目: [OpenVulcan/vulcan-local-db](https://github.com/OpenVulcan/vulcan-local-db)

默认建议:

- 小结果集查询使用 `QueryJson`
- 大结果集查询使用 `QueryStream`
- 高频重复写入优先使用 `ExecuteBatch`
- 所有请求都应设置 gRPC deadline

## 当前支持项

### RPC 概览

| RPC | 用途 | 参数支持 | 返回类型 |
|-----|------|---------|---------|
| `ExecuteScript` | DDL / 单条 DML / 无参数脚本 | `params` / `params_json` | `ExecuteResponse` |
| `ExecuteBatch` | 高频重复写入（事务包裹） | `ExecuteBatchItem[]` | `ExecuteBatchResponse` |
| `QueryJson` | 轻量查询（小结果集） | `params` / `params_json` | `QueryJsonResponse` |
| `QueryStream` | 大结果集流式返回 | `params` / `params_json` | `stream QueryResponse` |

### 安全加固（默认开启）

| 特性 | 说明 |
|------|------|
| WAL 模式 | 文件数据库默认 WAL，支持并发读 |
| Foreign keys | 默认开启外键约束 |
| Defensive 模式 | 关闭危险操作（如直接写入 WAL） |
| Trusted schema | 默认关闭，防止通过 schema 对象执行任意代码 |
| 数据库文件锁 | 防止多进程同时访问同一数据库文件 |
| gRPC deadline 中断 | 超时自动中断正在执行的 SQLite 查询 |
| 错误重试元数据 | 返回 `x-vldb-retryable` 和 `x-vldb-sqlite-code` trailer |
| panic 事务回滚 | 连接返回池前自动回滚未提交事务 |

### 日志配置

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `logging.log_sql` | `true` | 是否在日志中记录 SQL 语句 |
| `logging.sql_masking` | `false` | 是否对日志中的 SQL 字符串字面量脱敏 |

> 开启 `sql_masking` 后，日志中的单引号字符串字面量会被替换为 `***`，防止敏感数据泄露。

## 已知问题与限制

| 问题 | 严重度 | 说明 |
|------|--------|------|
| SQLite 单写者 | 架构级 | 写操作串行化，增加连接数不提升写 QPS |
| 流式分批物化 | 中 | `QueryStream` 按 1000 行/批物化，宽表大字段可能占用较多内存 |
| 明文 gRPC | 中 | 无 TLS，对外暴露需通过反向代理加 TLS |
| 多语句检测边界 | 低 | 状态机不处理 `x'hex'` 十六进制 blob 字面量中的分号 |
| 动态扩展 | 低 | 不支持 `sqlite3_load_extension` |

## 2. 用 Docker 先拉起服务

如果只是要联调客户端，最简单的方式是直接跑公开镜像：

```bash
docker pull openvulcan/vldb-sqlite:latest
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  openvulcan/vldb-sqlite:latest
```

如果你想固定到某个版本，也可以把 `latest` 换成明确标签，例如 `vX.Y.Z`。

如果要用自己的配置文件：

```bash
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  -v "$(pwd)/docker/vldb-sqlite.json:/app/config/vldb-sqlite.json:ro" \
  openvulcan/vldb-sqlite:latest
```

说明：

- 镜像内默认配置文件路径是 `/app/config/vldb-sqlite.json`
- 数据目录是 `/app/data`
- 客户端连 `127.0.0.1:19501` 即可
- 上面的挂载示例按 Bash 写法展示；Windows 或 PowerShell 建议直接使用宿主机绝对路径

## 3. 代码生成

### Go

```bash
protoc -I ./proto \
  --go_out=. \
  --go-grpc_out=. \
  ./proto/v1/sqlite.proto
```

当前 `go_package` 为:

```proto
option go_package = "vldb-sqlite-go-demo/proto/v1;sqlitev1";
```

如果你的 Go 项目包路径不同，可以在复制 `.proto` 时按自身仓库结构调整 `go_package`。

## 4. 建连示例

```go
package main

import (
	"context"
	"time"

	sqlitev1 "vldb-sqlite-go-demo/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newClient() (sqlitev1.SqliteServiceClient, *grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"127.0.0.1:19501",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}

	return sqlitev1.NewSqliteServiceClient(conn), conn, nil
}
```

## 5. 参数绑定规则

服务支持两种参数输入方式:

1. 推荐方式: `params`
2. 兼容方式: `params_json`

### 5.1 `params` 的 protobuf 标量映射

`SqliteValue` 使用 `oneof` 承载标量类型:

| 字段 | SQLite 类型 | 说明 |
| --- | --- | --- |
| `int64_value` | `INTEGER` | 64 位有符号整数 |
| `float64_value` | `REAL` | 双精度浮点 |
| `string_value` | `TEXT` | UTF-8 字符串 |
| `bytes_value` | `BLOB` | 原始字节 |
| `bool_value` | `INTEGER` | 服务端映射为 `1` / `0` |
| `null_value` | `NULL` | 显式空值 |

Go 中构造参数时，通常写成:

```go
func S(v string) *sqlitev1.SqliteValue {
	return &sqlitev1.SqliteValue{
		Kind: &sqlitev1.SqliteValue_StringValue{StringValue: v},
	}
}

func I(v int64) *sqlitev1.SqliteValue {
	return &sqlitev1.SqliteValue{
		Kind: &sqlitev1.SqliteValue_Int64Value{Int64Value: v},
	}
}
```

### 5.2 `params_json` 的兼容规则

- `params_json` 必须是 JSON 数组
- 数组元素只允许标量: `null` / `bool` / `number` / `string`
- 不允许对象或数组嵌套
- `params` 和 `params_json` 不能同时传

合法示例:

```json
[1, true, "agent-1", null]
```

非法示例:

```json
[{"role":"planner"}]
```

### 5.3 复杂对象如何传

像 Agent 配置、标签、画像、TTL 策略这类复杂结构，统一在客户端先序列化成 JSON 字符串，再通过 `string_value` 写入 SQLite `TEXT` 列。

例如:

```json
{"role":"planner","ttl":30,"tags":["alpha","beta"]}
```

应作为一个字符串参数发送，而不是拆成嵌套 protobuf 结构。

## 6. RPC 说明

### 6.1 ExecuteScript

用途:

- 执行 DDL
- 执行单条 DML
- 执行无参数 SQL 脚本

请求:

```proto
message ExecuteRequest {
  string sql = 1;
  string params_json = 2;
  repeated SqliteValue params = 3;
}
```

规则:

- `sql` 不能为空
- 当 `params` / `params_json` 都为空时，可以执行多语句脚本
- 当传参数时，只允许单条 SQL

响应:

- `success`
- `message`
- `rows_changed`
- `last_insert_rowid`

Go 示例:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

resp, err := client.ExecuteScript(ctx, &sqlitev1.ExecuteRequest{
	Sql: "INSERT INTO kv_state(key, value_json) VALUES(?, ?)",
	Params: []*sqlitev1.SqliteValue{
		S("agent:profile:1"),
		S(`{"role":"planner","ttl":30}`),
	},
})
```

### 6.2 ExecuteBatch

用途:

- 高频重复写入
- 多条同构写请求合并成一次 gRPC
- 避免“一条 SQL 一次 RPC”的 N+1 开销

请求:

```proto
message ExecuteBatchRequest {
  string sql = 1;
  repeated ExecuteBatchItem items = 2;
}

message ExecuteBatchItem {
  repeated SqliteValue params = 1;
}
```

服务端行为:

1. 显式执行 `BEGIN TRANSACTION`
2. 预编译一次 SQL
3. 循环绑定 `items[i].params`
4. 全部成功后执行 `COMMIT`
5. 中途失败则回滚

规则:

- 只允许单条 SQL
- 所有 `items` 都按同一条 SQL 的参数顺序绑定
- `items` 不能为空

Go 示例:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

resp, err := client.ExecuteBatch(ctx, &sqlitev1.ExecuteBatchRequest{
	Sql: "INSERT INTO kv_state(key, value_json, expire_at) VALUES(?, ?, ?) " +
		"ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, expire_at = excluded.expire_at",
	Items: []*sqlitev1.ExecuteBatchItem{
		{
			Params: []*sqlitev1.SqliteValue{
				S("agent:profile:1"),
				S(`{"role":"planner"}`),
				I(1711929600),
			},
		},
		{
			Params: []*sqlitev1.SqliteValue{
				S("agent:profile:2"),
				S(`{"role":"writer"}`),
				I(1711933200),
			},
		},
	},
})
```

适用场景:

- 配置刷新
- TTL 批量推进
- 状态机小事务合并写入

### 6.3 QueryJson

用途:

- 控制面查询
- 少量结果集直接返回 JSON

请求:

```proto
message QueryRequest {
  string sql = 1;
  string params_json = 2;
  repeated SqliteValue params = 3;
}
```

规则:

- 只允许单条 SQL
- 结果返回为 JSON 字符串
- 每一行会映射为一个 JSON object
- 最终整体是 JSON 数组

Go 示例:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

resp, err := client.QueryJson(ctx, &sqlitev1.QueryRequest{
	Sql: "SELECT key, value_json, expire_at FROM kv_state WHERE key = ?",
	Params: []*sqlitev1.SqliteValue{
		S("agent:profile:1"),
	},
})

// resp.JsonData 类似:
// [{"key":"agent:profile:1","value_json":"{\"role\":\"planner\"}","expire_at":1711929600}]
```

### 6.4 QueryStream

用途:

- 较大结果集
- Arrow IPC 下游处理

规则:

- 只允许单条 SQL
- 服务端会按顺序返回多个 `QueryResponse`
- 每个 `QueryResponse.arrow_ipc_chunk` 是一个 IPC 字节块
- 客户端应按收到的顺序拼接或直接流式喂给 Arrow IPC Reader
- 服务端按每批 1000 行分批处理，避免全量物化占用过多内存

返回结构:

```proto
message QueryResponse {
  bytes arrow_ipc_chunk = 1;
}
```

注意:

- 服务端采用分批物化策略（每批 1000 行），内存占用从 O(N) 降为 O(1000)
- `QueryStream` 适合”结果大于 JSON，但仍是标量列”的场景
- 该服务不处理 embedding/vector 数据
- 空结果集会返回空的 Arrow 流（无 chunk）

## 7. grpcurl 调试示例

### 7.1 ExecuteScript

```bash
grpcurl -plaintext \
  -d "{\"sql\":\"INSERT INTO kv_state(key, value_json) VALUES(?, ?)\",\"params\":[{\"stringValue\":\"agent:profile:1\"},{\"stringValue\":\"{\\\"role\\\":\\\"planner\\\"}\"}]}" \
  127.0.0.1:19501 \
  vldb.sqlite.v1.SqliteService/ExecuteScript
```

### 7.2 ExecuteBatch

```bash
grpcurl -plaintext \
  -d "{\"sql\":\"INSERT INTO ttl_queue(key, expire_at) VALUES(?, ?)\",\"items\":[{\"params\":[{\"stringValue\":\"agent:1\"},{\"int64Value\":\"1711929600\"}]},{\"params\":[{\"stringValue\":\"agent:2\"},{\"int64Value\":\"1711933200\"}]}]}" \
  127.0.0.1:19501 \
  vldb.sqlite.v1.SqliteService/ExecuteBatch
```

### 7.3 QueryJson

```bash
grpcurl -plaintext \
  -d "{\"sql\":\"SELECT key, expire_at FROM ttl_queue WHERE key = ?\",\"params\":[{\"stringValue\":\"agent:1\"}]}" \
  127.0.0.1:19501 \
  vldb.sqlite.v1.SqliteService/QueryJson
```

说明:

- `grpcurl` 使用 protobuf JSON 映射
- `int64Value` 在 JSON 里建议使用字符串形式
- `bytesValue` 需要使用 base64 字符串

## 8. 错误与重试语义

### 8.1 可重试错误

当底层 SQLite 返回以下错误时，服务会显式标记为可重试:

- `SQLITE_BUSY`
- `SQLITE_LOCKED`
- `SQLITE_SCHEMA`

典型映射:

| SQLite 错误 | gRPC Code | trailer |
| --- | --- | --- |
| `SQLITE_BUSY` | `Unavailable` | `x-vldb-retryable=true` |
| `SQLITE_LOCKED` | `Unavailable` | `x-vldb-retryable=true` |
| `SQLITE_SCHEMA` | `Aborted` | `x-vldb-retryable=true` |

同时还会附带:

- `x-vldb-sqlite-code=SQLITE_BUSY`
- `x-vldb-sqlite-code=SQLITE_LOCKED`
- `x-vldb-sqlite-code=SQLITE_SCHEMA`

### 8.2 不建议重试的典型错误

| 场景 | gRPC Code |
| --- | --- |
| SQL 语法错误 / 参数错误 | `InvalidArgument` |
| 只读库写入 / 约束冲突 | `FailedPrecondition` |
| 查询无结果 | `NotFound` |
| 数据库损坏 | `DataLoss` |

### 8.3 Go 侧读取 trailer 示例

```go
import (
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func executeWithRetry(ctx context.Context, client sqlitev1.SqliteServiceClient, req *sqlitev1.ExecuteRequest) error {
	var trailer metadata.MD
	_, err := client.ExecuteScript(ctx, req, grpc.Trailer(&trailer))
	if err == nil {
		return nil
	}

	st, _ := status.FromError(err)
	_ = st

	retryable := false
	if values := trailer.Get("x-vldb-retryable"); len(values) > 0 {
		retryable = strings.EqualFold(values[0], "true")
	}

	if retryable {
		// 在业务队列中做指数退避重试
	}

	return err
}
```

## 9. 接入建议

- 高频写请求优先合并成 `ExecuteBatch`
- 复杂对象统一先 JSON 序列化，再作为 `string_value` 存 `TEXT`
- 所有请求设置 deadline，避免慢查询长期占用连接
- 对 `x-vldb-retryable=true` 的错误做指数退避重试
- 小结果集走 `QueryJson`，大结果集走 `QueryStream`
- 不要把向量或 embedding 数据写进本服务
- 生产环境建议开启 `logging.sql_masking=true` 防止日志泄露敏感数据
- `ExecuteScript` 是特权接口，建议通过网关做访问控制或配置 `hardening.read_only=true`

## 10. 承载能力参考

默认配置（8 连接池）下的估算：

| 场景 | 单查询耗时 | 推荐 QPS |
|------|-----------|---------|
| 简单读（主键查询） | ~0.5ms | ~8,000 |
| 普通读（索引扫描） | ~5ms | ~1,600 |
| 复杂读（全表扫描） | ~50ms | ~160 |
| 简单写（单行 INSERT） | ~2ms | ~250 |
| 批量写（100 行/批） | ~20ms | ~50 |

> 写 QPS 受限于 SQLite 单写者模型，增加连接数不会提升写入性能。
> 如需更高写入吞吐，考虑写请求队列化批量合并或多 SQLite 文件分片。

## 11. 对接注意事项

- **无 TLS**: 当前服务为明文 gRPC，对外暴露需通过 Nginx/Envoy 等反向代理加 TLS
- **bundled SQLite**: 自带 `json1` 和 `fts5` 扩展，不支持动态加载扩展
- **URI 文件名**: 默认关闭，需配置 `hardening.allow_uri_filenames=true` 才能使用 `file:` 前缀
- **日志路径**: 默认在数据库文件同级 `_log` 目录，`:memory:` 模式在配置目录 `vldb-sqlite-logs/`
- **连接池调优**: 读密集型场景可增大 `connection_pool_size`（16~32），写密集型保持默认即可
- **WAL checkpoint**: 默认 `wal_autocheckpoint_pages=1000`，高写入场景可适当增大

## 12. 一句话选型

- 写单条 SQL: `ExecuteScript`
- 写很多同构 SQL: `ExecuteBatch`
- 查少量结果: `QueryJson`
- 查大量标量结果并需要 Arrow: `QueryStream`
