# vldb-sqlite 库模式使用说明

本文档说明 `vldb-sqlite` 的库模式产物、FFI 入口、头文件与当前阶段能力边界。

## 当前定位

当前 `vldb-sqlite` 已同时支持：

- **服务模式**：以 gRPC 进程方式运行
- **库模式**：以 `rlib + cdylib` 形式编译并发布

本阶段库模式的重点是先把以下基础链路打通：

1. 独立库产物可构建
2. 头文件可分发
3. FFI 边界同时支持非 JSON 主接口与 JSON 兼容接口
4. Docker 与 GitHub Release 能同时输出服务包与库包

当前推荐调用分层如下：

- **Rust / MCP**
  - 优先直接使用 typed Rust API
  - 不建议再经过 JSON FFI 绕行
- **Go / 其他原生宿主**
  - 优先使用非 JSON FFI 主接口
  - 高频 FTS / 词典操作不建议继续走 JSON
- **JSON FFI**
  - 保留为兼容层
  - 主要给 Lua / Python / 调试 / 其他潜在项目预留

如果需要一个最小可运行的 Go 对接参考，可以直接查看：

- [`../examples/go-ffi/README.md`](../examples/go-ffi/README.md)
- [`../examples/go-ffi/sqliteffi/sqliteffi.go`](../examples/go-ffi/sqliteffi/sqliteffi.go)

## gRPC 与 lib 的职责边界

### 1. gRPC 模式

- 继续由 `vldb-sqlite.json` 驱动
- 一个服务实例只绑定一个 `db_path`
- 数据库创建、目录初始化、日志和端口等行为都沿用原始配置文件逻辑

### 2. lib 模式

- **不依赖配置文件**
- 通过纯 Rust 运行时模块：
  - [`../src/runtime.rs`](../src/runtime.rs)
  暴露多库管理能力
- 调用方可以在同一进程里动态打开多个库，而不需要改动 gRPC 的单库配置方式

这意味着：

- gRPC 和 lib 是两条并行能力线
- lib 的多库能力不会反向污染现有 gRPC 配置模型
- gRPC 继续保持“单库服务网关”定位
- lib 则作为“程序化多库 SQLite 引擎”存在

## 产物说明

发布时同一平台会产生两类包：

### 1. 常规服务包

- 名称：`vldb-sqlite-v<version>-<target>`
- 内容：
  - `vldb-sqlite` 可执行文件
  - 示例配置
  - README / LICENSE（如存在）

### 2. 独立库包

- 名称：`vldb-sqlite-lib-v<version>-<target>`
- 内容：
  - Windows: `vldb_sqlite.dll`
  - Linux: `libvldb_sqlite.so`
  - macOS: `libvldb_sqlite.dylib`
  - 头文件：`include/vldb_sqlite.h`
  - 本文档

## 头文件

头文件位置：

- [`../include/vldb_sqlite.h`](../include/vldb_sqlite.h)

当前导出的接口分为两层：

### 1. 非 JSON 主接口

- `vldb_sqlite_runtime_create_default`
- `vldb_sqlite_runtime_open_database`
- `vldb_sqlite_runtime_close_database`
- `vldb_sqlite_database_destroy`
- `vldb_sqlite_database_db_path`
- `vldb_sqlite_database_tokenize_text`
- `vldb_sqlite_database_upsert_custom_word`
- `vldb_sqlite_database_remove_custom_word`
- `vldb_sqlite_database_list_custom_words`
- `vldb_sqlite_database_ensure_fts_index`
- `vldb_sqlite_database_rebuild_fts_index`
- `vldb_sqlite_database_upsert_fts_document`
- `vldb_sqlite_database_delete_fts_document`
- `vldb_sqlite_database_search_fts`
- `vldb_sqlite_tokenize_result_*`
- `vldb_sqlite_custom_word_list_*`
- `vldb_sqlite_search_result_*`

### 2. JSON 兼容接口

- `vldb_sqlite_library_info_json`
- `vldb_sqlite_string_free`
- `vldb_sqlite_last_error_message`
- `vldb_sqlite_clear_last_error`
- `vldb_sqlite_json_is_null`
- `vldb_sqlite_tokenize_text_json`
- `vldb_sqlite_upsert_custom_word_json`
- `vldb_sqlite_remove_custom_word_json`
- `vldb_sqlite_list_custom_words_json`
- `vldb_sqlite_ensure_fts_index_json`
- `vldb_sqlite_rebuild_fts_index_json`
- `vldb_sqlite_upsert_fts_document_json`
- `vldb_sqlite_delete_fts_document_json`
- `vldb_sqlite_search_fts_json`

另外，纯 Rust 库模式已经额外提供：

- `SqliteRuntime`
- `SqliteDatabaseHandle`

## FFI 设计原则

### 1. Rust 主路径优先

对于 Rust / MCP：

- 优先直接使用 `SqliteRuntime` / `SqliteDatabaseHandle`
- 不建议为了统一调用风格而再走 JSON FFI
- Rust typed API 才是主路径

### 2. Go 主路径使用非 JSON FFI

对于 Go / 其他原生宿主：

- 优先使用 runtime / database / result handle 风格的非 JSON FFI
- 高频路径通过：
  - 扁平参数
  - POD 结果结构
  - handle + getter
  完成调用
- 不要求调用方理解 Rust 内部数据结构

### 3. JSON 层仅作为兼容层

当前与后续新增的 JSON 接口都定位为：

- LuaJIT / Python 兼容层
- 调试层
- 其他潜在项目的快速集成层

它们不再是 Rust 或 Go 的主设计中心

### 4. 不向上层暴露复杂结构体指针

除少量必要的 POD/C 结构外，库模式不会向上层泄漏复杂 Rust 结构体内存布局。

### 5. 释放对上层尽量无感

底层仍保留统一释放函数：

- `vldb_sqlite_string_free`

但推荐由宿主包装层统一调用，而不是要求 Lua / Python 业务代码手工管理底层内存。

### 6. 多库能力属于 runtime，不属于配置文件

当前新增的 `SqliteRuntime` 只接受程序化参数或数据库路径：

- 不读取 `vldb-sqlite.json`
- 不关心服务端口
- 不关心 gRPC 生命周期

它的职责仅仅是：

- 打开数据库
- 缓存数据库句柄
- 在同一进程中管理多个库
- 复用统一的 SQLite pragma / tokenizer 初始化规则

## 当前能力

当前 FFI 目前已经进入 **SQLite Runtime + Go FFI 主接口阶段**，供宿主确认：

- 动态库已正确加载
- 版本信息可读
- 基础错误通道可用
- 可选 `none/jieba` 分词能力可通过 JSON 接口直接调用
- `_vulcan_dict` 伴生词典表可通过热更新接口维护

例如可调用 `vldb_sqlite_library_info_json()` 获得类似：

```json
{
  "name": "vldb-sqlite",
  "version": "0.1.2",
  "ffi_stage": "sqlite-runtime-go-ffi",
  "capabilities": [
    "library_info_json",
    "runtime_create_default",
    "runtime_open_database",
    "runtime_close_database",
    "database_tokenize_text",
    "database_upsert_custom_word",
    "database_remove_custom_word",
    "database_list_custom_words",
    "database_ensure_fts_index",
    "database_rebuild_fts_index",
    "database_upsert_fts_document",
    "database_delete_fts_document",
    "database_search_fts",
    "tokenize_text_json",
    "upsert_custom_word_json",
    "remove_custom_word_json",
    "list_custom_words_json",
    "ensure_fts_index_json",
    "rebuild_fts_index_json",
    "upsert_fts_document_json",
    "delete_fts_document_json",
    "search_fts_json"
  ]
}
```

## 非 JSON 主接口摘要

### 1. Runtime / Database

推荐 Go / 原生宿主先：

1. `vldb_sqlite_runtime_create_default()`
2. `vldb_sqlite_runtime_open_database(runtime, db_path)`
3. 在数据库句柄上执行分词、词典、FTS 操作
4. `vldb_sqlite_database_destroy()`
5. `vldb_sqlite_runtime_destroy()`

这条路径：

- 不依赖配置文件
- 支持同一进程内动态打开多个库
- 不会反向干扰 gRPC 的单库配置模式

### 2. 分词主接口

- `vldb_sqlite_database_tokenize_text`
- 返回 `VldbSqliteTokenizeResultHandle`

调用方通过 getter 读取：

- `normalized_text`
- `fts_query`
- `token_count`
- `token[index]`

### 3. 词典主接口

- `vldb_sqlite_database_upsert_custom_word`
- `vldb_sqlite_database_remove_custom_word`
- `vldb_sqlite_database_list_custom_words`

其中列表结果通过：

- `vldb_sqlite_custom_word_list_len`
- `vldb_sqlite_custom_word_list_get_word`
- `vldb_sqlite_custom_word_list_get_weight`

读取

### 4. FTS 主接口

- `vldb_sqlite_database_ensure_fts_index`
- `vldb_sqlite_database_rebuild_fts_index`
- `vldb_sqlite_database_upsert_fts_document`
- `vldb_sqlite_database_delete_fts_document`
- `vldb_sqlite_database_search_fts`

检索结果通过：

- `vldb_sqlite_search_result_total`
- `vldb_sqlite_search_result_len`
- `vldb_sqlite_search_result_source`
- `vldb_sqlite_search_result_query_mode`
- `vldb_sqlite_search_result_get_id`
- `vldb_sqlite_search_result_get_file_path`
- `vldb_sqlite_search_result_get_title`
- `vldb_sqlite_search_result_get_title_highlight`
- `vldb_sqlite_search_result_get_content_snippet`
- `vldb_sqlite_search_result_get_score`
- `vldb_sqlite_search_result_get_rank`
- `vldb_sqlite_search_result_get_raw_score`

读取

## 新增 JSON FFI 接口

### 1. `vldb_sqlite_tokenize_text_json`

请求示例：

```json
{
  "text": "市民田-女士急匆匆",
  "tokenizer_mode": "jieba",
  "search_mode": false,
  "db_path": "D:/data/demo.sqlite3"
}
```

返回示例：

```json
{
  "tokenizer_mode": "jieba",
  "normalized_text": "市民田-女士急匆匆",
  "tokens": ["市", "民", "田-女士", "急", "匆", "匆"],
  "fts_query": "\"市\" \"民\" \"田-女士\" \"急\" \"匆\" \"匆\""
}
```

说明：

- `db_path` 可选；如果不传，则只使用内置词典。
- `tokenizer_mode` 当前支持：
  - `none`
  - `jieba`

### 2. `vldb_sqlite_upsert_custom_word_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "word": "田-女士",
  "weight": 42
}
```

### 3. `vldb_sqlite_remove_custom_word_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "word": "田-女士"
}
```

词典数据会保存在当前库实例的伴生表：

- `_vulcan_dict`

这意味着：

- 词典与业务数据同源
- 词典可热更新
- 备份、迁移、复制时无需额外同步外部词典文件

### 4. `vldb_sqlite_list_custom_words_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3"
}
```

### 5. `vldb_sqlite_ensure_fts_index_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "index_name": "memory_docs",
  "tokenizer_mode": "jieba"
}
```

### 6. `vldb_sqlite_rebuild_fts_index_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "index_name": "memory_docs",
  "tokenizer_mode": "jieba"
}
```

说明：

- 当 `_vulcan_dict` 更新后，旧文档已经写入的 FTS 词元不会自动重建。
- `vldb_sqlite_rebuild_fts_index_json` 会使用当前词典重新创建并回填索引内容。
- 这一步对“先写文档，后补专有词”的场景尤其重要。

### 7. `vldb_sqlite_upsert_fts_document_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "index_name": "memory_docs",
  "tokenizer_mode": "jieba",
  "id": "doc-1",
  "file_path": "/demo/file.md",
  "title": "测试标题",
  "content": "市民田-女士急匆匆"
}
```

### 8. `vldb_sqlite_delete_fts_document_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "index_name": "memory_docs",
  "id": "doc-1"
}
```

### 9. `vldb_sqlite_search_fts_json`

请求示例：

```json
{
  "db_path": "D:/data/demo.sqlite3",
  "index_name": "memory_docs",
  "tokenizer_mode": "jieba",
  "query": "田-女士",
  "limit": 10,
  "offset": 0
}
```

返回示例（节选）：

```json
{
  "success": true,
  "index_name": "memory_docs",
  "tokenizer_mode": "jieba",
  "normalized_query": "田-女士",
  "fts_query": "\"田-女士\"",
  "source": "sqlite_fts",
  "query_mode": "fts",
  "total": 1,
  "hits": [
    {
      "id": "doc-1",
      "file_path": "/demo/file.md",
      "title": "测试标题",
      "title_highlight": "测试标题",
      "content_snippet": "市民<mark>田-女士</mark>急匆匆",
      "score": 8.42,
      "rank": 1,
      "raw_score": -8.42
    }
  ]
}
```

## 后续扩展方向

后续库模式会继续扩展以下能力：

- SQLite 引擎实例创建与销毁
- gRPC 与 FFI 双出口的一致能力面
- FTS / BM25 检索接口
- 可选分词模式（如 `none` / `jieba`）
- `_vulcan_dict` 伴生表
- 词典热更新接口：
  - `UpsertCustomWord`
  - `RemoveCustomWord`
- 标准化检索结果字段：
  - `id`
  - `file_path`
  - `title_highlight`
  - `content_snippet`
  - `score`
  - `rank`
  - `raw_score`
  - `source`
  - `query_mode`

这些字段会为未来与 LanceDB 做 RRF 混合检索预留统一融合面。

## 最小调用顺序

当前阶段的最小调用流程如下：

1. 动态加载库文件
2. 调用 `vldb_sqlite_library_info_json()`
3. 解析返回的 JSON
4. 用 `vldb_sqlite_string_free()` 释放返回字符串
5. 如失败，则调用 `vldb_sqlite_last_error_message()` 读取错误

## 注意事项

- 当前阶段已经完成：
  - `_vulcan_dict` 伴生词典闭环
  - SQLite 连接级 `tokenize='jieba'` tokenizer 注册
  - gRPC / FFI 分词与词典热更新接口
  - FTS 索引建表、写入、删除、检索的最小统一接口
- 当前 JSON FFI 主要已经可以解决：
  - 统一分词实现
  - 统一词典热更新实现
  - 避免上层语言各自重复实现 Jieba
  - 让宿主与外部语言直接复用 SQLite 原生 `jieba` tokenizer 能力
- 后续会继续扩展到：
  - 更完整的 RRF 友好融合字段与调度信息
