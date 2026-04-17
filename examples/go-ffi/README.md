# vldb-sqlite Go FFI 示例

这个示例演示如何在 **Go** 中通过 **动态库 + 非 JSON FFI 主接口** 使用 `vldb-sqlite`。

## 目标

示例覆盖以下能力：

- 加载 `vldb_sqlite.dll/.so/.dylib`
- 创建 `SqliteRuntime`
- 打开数据库句柄
- 写入自定义词
- 建立 FTS 索引
- 写入 FTS 文档
- 执行 FTS 检索
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
- JSON 兼容层未作为主路径使用
- 如果后续要给生产 Go 项目接入，建议把 `sqliteffi` 目录抽成独立内部包，再补：
  - 错误码枚举
  - 更完整的 getter
  - 更稳的资源托管
