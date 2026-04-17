# vldb-sqlite gRPC Integration Guide

This guide is for client-side integrators of `vldb-sqlite`. It explains how to connect to the gRPC service, how to bind parameters, and how to handle retryable SQLite errors correctly.

## 0. Document Index

- Chinese README: [./docs/README.zh-CN.md](./README.zh-CN.md)
- English README: [./docs/README.en.md](./README.en.md)
- Fix Plan: [./docs/fix-plan.md](./fix-plan.md)

## 1. Service Info

- gRPC service: `vldb.sqlite.v1.SqliteService`
- Default address: `127.0.0.1:19501`
- Transport: plain gRPC without TLS
- Proto file: `proto/v1/sqlite.proto`
- Docker image: `openvulcan/vldb-sqlite`
- Main project: [OpenVulcan/vulcan-local-db](https://github.com/OpenVulcan/vulcan-local-db)

Recommended defaults:

- Use `QueryJson` for small result sets
- Use `QueryStream` for larger result sets
- Prefer `ExecuteBatch` for repeated high-frequency writes
- Set a gRPC deadline on every request

## Current Support

### RPC Overview

| RPC | Purpose | Parameters | Response |
|-----|---------|-----------|----------|
| `ExecuteScript` | DDL / single DML / parameter-less script | `params` / `params_json` | `ExecuteResponse` |
| `ExecuteBatch` | High-frequency repeated writes (transactional) | `ExecuteBatchItem[]` | `ExecuteBatchResponse` |
| `QueryJson` | Lightweight query (small results) | `params` / `params_json` | `QueryJsonResponse` |
| `QueryStream` | Large result sets streamed | `params` / `params_json` | `stream QueryResponse` |

### Security Hardening (enabled by default)

| Feature | Description |
|---------|-------------|
| WAL mode | File databases default to WAL for concurrent reads |
| Foreign keys | Enabled by default |
| Defensive mode | Disables dangerous operations (e.g., direct WAL writes) |
| Trusted schema | Disabled by default, prevents arbitrary code via schema objects |
| Database file lock | Prevents multiple processes from accessing the same database |
| gRPC deadline interrupt | Automatically interrupts running SQLite queries on timeout |
| Error retry metadata | Returns `x-vldb-retryable` and `x-vldb-sqlite-code` trailers |
| Panic transaction rollback | Automatically rolls back uncommitted transactions before returning connections to the pool |

### Logging Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `logging.log_sql` | `true` | Whether to log SQL statements |
| `logging.sql_masking` | `false` | Whether to mask string literals in logged SQL (replaces with `***`) |

> When `sql_masking` is enabled, single-quoted string literals in SQL logs are replaced with `***` to prevent sensitive data leakage.

## Known Issues & Limitations

| Issue | Severity | Notes |
|-------|----------|-------|
| SQLite single writer | Architectural | Writes are serialized; increasing `connection_pool_size` does not improve write QPS |
| Streaming batch materialization | Medium | `QueryStream` materializes in 1000-row batches; wide tables with large columns may use significant memory |
| Plain gRPC | Medium | No TLS; use a reverse proxy for external exposure |
| Multi-statement detection edge case | Low | State machine does not handle semicolons inside `x'hex'` blob literals |
| Dynamic extensions | Low | `sqlite3_load_extension` is not supported |

## 2. Start The Service With Docker

If you only want to integrate and test against a running service, the fastest path is the published image:

```bash
docker pull openvulcan/vldb-sqlite:latest
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  openvulcan/vldb-sqlite:latest
```

You can replace `latest` with a fixed release tag such as `vX.Y.Z`.

If you want to override the default config:

```bash
docker run -d \
  --name vldb-sqlite \
  -p 19501:19501 \
  -v vldb-sqlite-data:/app/data \
  -v "$(pwd)/docker/vldb-sqlite.json:/app/config/vldb-sqlite.json:ro" \
  openvulcan/vldb-sqlite:latest
```

Notes:

- The image already includes a default config at `/app/config/vldb-sqlite.json`
- SQLite data is stored in `/app/data`
- Clients can connect to `127.0.0.1:19501`
- The bind-mount example uses Bash syntax; on Windows or PowerShell, use an absolute host path instead of `$(pwd)`

## 3. Code Generation

### Go

```bash
protoc -I ./proto \
  --go_out=. \
  --go-grpc_out=. \
  ./proto/v1/sqlite.proto
```

The current `go_package` is:

```proto
option go_package = "vldb-sqlite-go-demo/proto/v1;sqlitev1";
```

If your Go repository uses a different package path, adjust `go_package` to match your own layout before generating code.

## 4. Connection Example

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

## 5. Parameter Binding Rules

The service supports two input styles:

1. Preferred: `params`
2. Compatibility path: `params_json`

### 5.1 `params` Protobuf Scalar Mapping

`SqliteValue` uses a protobuf `oneof` to carry scalar values:

| Field | SQLite type | Notes |
| --- | --- | --- |
| `int64_value` | `INTEGER` | Signed 64-bit integer |
| `float64_value` | `REAL` | Double precision float |
| `string_value` | `TEXT` | UTF-8 string |
| `bytes_value` | `BLOB` | Raw bytes |
| `bool_value` | `INTEGER` | Mapped to `1` / `0` on the server |
| `null_value` | `NULL` | Explicit null |

Typical Go helper constructors:

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

### 5.2 `params_json` Compatibility Rules

- `params_json` must be a JSON array
- Array items may only be scalar values: `null`, `bool`, `number`, or `string`
- Nested objects and arrays are not allowed
- `params` and `params_json` must not be sent together

Valid example:

```json
[1, true, "agent-1", null]
```

Invalid example:

```json
[{"role":"planner"}]
```

### 5.3 How To Send Complex Objects

For complex payloads such as agent profiles, configuration maps, labels, or TTL policies, serialize them to JSON on the client first, then pass them as a single `string_value` and store them in a SQLite `TEXT` column.

Example payload:

```json
{"role":"planner","ttl":30,"tags":["alpha","beta"]}
```

That payload should be sent as one string parameter, not as nested protobuf data.

## 6. RPC Reference

### 6.1 ExecuteScript

Use cases:

- DDL statements
- Single DML statements
- Script execution when no parameters are needed

Request:

```proto
message ExecuteRequest {
  string sql = 1;
  string params_json = 2;
  repeated SqliteValue params = 3;
}
```

Rules:

- `sql` must not be empty
- Multi-statement scripts are allowed only when both `params` and `params_json` are empty
- If parameters are provided, only a single SQL statement is allowed

Response fields:

- `success`
- `message`
- `rows_changed`
- `last_insert_rowid`

Go example:

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

Use cases:

- High-frequency repeated writes
- Merging many homogeneous writes into one RPC
- Avoiding the N+1 pattern of sending one SQL per gRPC round trip

Request:

```proto
message ExecuteBatchRequest {
  string sql = 1;
  repeated ExecuteBatchItem items = 2;
}

message ExecuteBatchItem {
  repeated SqliteValue params = 1;
}
```

Server behavior:

1. Execute `BEGIN TRANSACTION`
2. Prepare the SQL once
3. Bind each `items[i].params`
4. Execute all items
5. Finish with one `COMMIT`
6. Roll back on failure

Rules:

- Only one SQL statement is allowed
- All items are bound against the same SQL parameter order
- `items` must not be empty

Go example:

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

Typical scenarios:

- Config refresh
- TTL state transitions
- Small state-machine transactions merged into one batch

### 6.3 QueryJson

Use cases:

- Control-plane queries
- Small result sets returned directly as JSON

Request:

```proto
message QueryRequest {
  string sql = 1;
  string params_json = 2;
  repeated SqliteValue params = 3;
}
```

Rules:

- Only one SQL statement is allowed
- The response is JSON text
- Each row becomes one JSON object
- The full result is returned as a JSON array

Go example:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()

resp, err := client.QueryJson(ctx, &sqlitev1.QueryRequest{
	Sql: "SELECT key, value_json, expire_at FROM kv_state WHERE key = ?",
	Params: []*sqlitev1.SqliteValue{
		S("agent:profile:1"),
	},
})

// resp.JsonData looks like:
// [{"key":"agent:profile:1","value_json":"{\"role\":\"planner\"}","expire_at":1711929600}]
```

### 6.4 QueryStream

Use cases:

- Larger result sets
- Arrow IPC consumers

Rules:

- Only one SQL statement is allowed
- The server returns multiple `QueryResponse` messages in order
- Each `QueryResponse.arrow_ipc_chunk` is one Arrow IPC byte chunk
- The client should concatenate the chunks in order or stream them into an Arrow IPC reader
- The server processes rows in batches of 1000 to avoid full materialization

Response:

```proto
message QueryResponse {
  bytes arrow_ipc_chunk = 1;
}
```

Notes:

- The server uses batched materialization (1000 rows per batch), reducing memory from O(N) to O(1000)
- `QueryStream` is intended for large scalar/tabular results, not vector payloads
- This service does not handle embeddings or high-dimensional vector data
- Empty result sets return an empty Arrow stream (no chunks)

## 7. grpcurl Examples

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

Notes:

- `grpcurl` uses protobuf JSON mapping
- `int64Value` is best passed as a string in JSON
- `bytesValue` must be base64-encoded

## 8. Error And Retry Semantics

### 8.1 Retryable Errors

The service explicitly marks these SQLite failures as retryable:

- `SQLITE_BUSY`
- `SQLITE_LOCKED`
- `SQLITE_SCHEMA`

Typical mappings:

| SQLite error | gRPC code | trailer |
| --- | --- | --- |
| `SQLITE_BUSY` | `Unavailable` | `x-vldb-retryable=true` |
| `SQLITE_LOCKED` | `Unavailable` | `x-vldb-retryable=true` |
| `SQLITE_SCHEMA` | `Aborted` | `x-vldb-retryable=true` |

The service also includes:

- `x-vldb-sqlite-code=SQLITE_BUSY`
- `x-vldb-sqlite-code=SQLITE_LOCKED`
- `x-vldb-sqlite-code=SQLITE_SCHEMA`

### 8.2 Typical Non-Retryable Errors

| Scenario | gRPC code |
| --- | --- |
| SQL syntax or parameter issues | `InvalidArgument` |
| Read-only writes or constraint violations | `FailedPrecondition` |
| No row found | `NotFound` |
| Database corruption | `DataLoss` |

### 8.3 Reading Trailers In Go

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
		// Apply exponential backoff in your queue or worker layer
	}

	return err
}
```

## 9. Integration Recommendations

- Prefer `ExecuteBatch` for repeated writes
- Serialize complex objects to JSON and store them as `TEXT`
- Set deadlines on all requests
- Back off and retry when `x-vldb-retryable=true`
- Use `QueryJson` for small results and `QueryStream` for larger ones
- Do not send embeddings or vector payloads to this service
- Enable `logging.sql_masking=true` in production to prevent sensitive data leakage in logs
- `ExecuteScript` is a privileged endpoint; restrict access via gateway or enable `hardening.read_only=true`

## 10. Capacity Reference

Estimated throughput under default configuration (8-connection pool):

| Scenario | Per-query latency | Recommended QPS |
|----------|------------------|-----------------|
| Simple read (PK lookup) | ~0.5ms | ~8,000 |
| Normal read (index scan) | ~5ms | ~1,600 |
| Complex read (full scan) | ~50ms | ~160 |
| Simple write (single INSERT) | ~2ms | ~250 |
| Batch write (100 rows/batch) | ~20ms | ~50 |

> Write QPS is limited by SQLite's single-writer model; increasing `connection_pool_size` will not improve write throughput.
> For higher write throughput, consider write request queuing with batch merging or multiple SQLite file sharding.

## 11. Integration Notes

- **No TLS**: The service uses plain gRPC; add TLS via a reverse proxy (Nginx/Envoy) for external exposure
- **Bundled SQLite**: Ships with `json1` and `fts5`; dynamic extension loading is not supported
- **URI filenames**: Disabled by default; set `hardening.allow_uri_filenames=true` to use `file:` prefix
- **Log paths**: Default to a sibling `_log` directory next to the database file; `:memory:` mode uses `vldb-sqlite-logs/` relative to the config
- **Connection pool tuning**: Increase `connection_pool_size` (16-32) for read-heavy workloads; default is fine for write-heavy
- **WAL checkpoint**: Default `wal_autocheckpoint_pages=1000`; increase for high-write scenarios

## 12. Quick RPC Choice Guide

- One write statement: `ExecuteScript`
- Many homogeneous writes: `ExecuteBatch`
- Small query result: `QueryJson`
- Large scalar result with Arrow consumption: `QueryStream`
