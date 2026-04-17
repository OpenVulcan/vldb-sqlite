# vldb-sqlite Go FFI 示例

这个示例演示如何在 **Go** 中通过 **动态库 + 非 JSON FFI 主接口** 使用 `vldb-sqlite`。

## 目标

示例覆盖以下能力：

- 加载 `vldb_sqlite.dll/.so/.dylib`
- 创建 `SqliteRuntime`
- 打开数据库句柄
- 建表与普通 SQL 执行
- 批量写入
- JSON 查询
- 写入自定义词
- 建立 FTS 索引
- 写入 FTS 文档
- 执行 FTS 检索
- Arrow IPC chunk 查询
- 读取 `id / file_path / score / rank / raw_score`

## 运行前提

1. 已构建 `vldb-sqlite` 动态库
2. 将动态库路径传给示例程序
3. 当前系统已安装 Go 1.24+

## 运行示例

Windows:

```powershell
go run . D:\projects\VulcanLocalDataGateway\vldb-sqlite\target\debug\vldb_sqlite.dll
```

Linux:

```bash
go run . /path/to/libvldb_sqlite.so
```

macOS:

```bash
go run . /path/to/libvldb_sqlite.dylib
```

## 说明

- 这个示例刻意使用 **非 JSON 主接口**
- 示例现在同时演示：
  - `ExecuteScript`
  - `ExecuteBatch`
  - `QueryJSON`
  - `QueryStream` 句柄式渐进读取
  - tokenizer + FTS
- `QueryStream()` 现在返回流句柄，调用方可以按 chunk 逐块消费结果，而不是一次性把全部 chunk 聚合进内存
- `QueryStream()` 不会在创建时立刻阻塞等待最终统计信息；如果调用方需要最终 `row/chunk/bytes` 统计，请显式调用 `WaitMetrics()`
- 如果调用方确实需要历史上的“全量聚合”行为，可显式使用 `CollectQueryStream()` 辅助接口
- JSON 兼容层未作为主路径使用
- 如果后续要给生产 Go 项目接入，建议把 `sqliteffi` 目录抽成独立内部包，再补：
  - 错误码枚举
  - 更完整的 getter
  - 更稳的资源托管
