# 风险项修复计划

> 创建时间: 2026-04-12
> 状态: 已完成

## 概述

全盘代码审核识别出 5 个主要风险项，本计划针对中高风险项设计修复方案。

---

## 修复项

### 1. SQL 日志脱敏（高风险）

**状态**: 已完成

**涉及文件**:
- `src/config.rs` — 新增 `log_sql`、`sql_masking` 配置字段
- `src/service.rs` — 所有日志记录函数

**改动**:
- `LoggingConfig` 新增 `log_sql: bool`（默认 true）和 `sql_masking: bool`（默认 false）
- 新增 `mask_sql()` 函数：使用状态机将 SQL 中的字符串字面量替换为 `***`
- 所有日志点根据 `log_sql` 和 `sql_masking` 决定输出：
  - `!log_sql` → `<redacted>`
  - `log_sql && !sql_masking` → 原样
  - `log_sql && sql_masking` → 脱敏后

### 2. 分批物化 + 流式输出（中风险）

**状态**: 已完成

**涉及文件**:
- `src/service.rs` — `run_query_streaming` 函数

**改动**:
- 定义 `STREAMING_BATCH_ROWS = 1000`
- 循环分批读取行 → 转 Arrow batch → 发送 chunk
- 首次 batch 确定 schema，后续批次复用
- 避免大结果集全量加载到内存

### 3. 改进多语句检测（中风险）

**状态**: 已完成

**涉及文件**:
- `src/service.rs` — `has_multiple_sql_statements` 函数

**改动**:
- 使用状态机跳过字符串字面量内的分号（`'...'`、`"..."`、`--注释`、`/* 注释 */`）
- 修复了原 `split(';')` 方案误判字符串内分号的问题

### 4. panic 路径事务回滚（中风险）

**状态**: 已完成

**涉及文件**:
- `src/service.rs` — `ConnectionLease::Drop`

**改动**:
- drop 时检查 `is_autocommit()`，不在 autocommit 模式则先 ROLLBACK
- 防止 panic 导致连接返回池时带有未回滚事务

### 5. NaN/Inf 浮点数 JSON 序列化（中风险）

**状态**: 已完成

**涉及文件**:
- `src/service.rs` — `sqlite_value_to_json`、`json_float`

**改动**:
- NaN/Inf 转为 JSON 字符串（如 `"NaN"`, `"inf"`）而非尝试编码为数字
- 避免产生非法 JSON 输出

---

## 验证方案

1. `cargo test` 确保所有现有测试通过 — **通过 (31/31)**
2. 为每项修复新增单元测试 — **新增 8 个测试**
3. `cargo clippy` 无警告

---

## 进度

| # | 修复项 | 状态 | 新增测试 |
|---|--------|------|----------|
| 1 | SQL 日志脱敏 | 已完成 | `mask_sql_*` × 4 |
| 2 | 分批物化+流式 | 已完成 | — |
| 3 | 改进多语句检测 | 已完成 | `has_multiple_sql_statements_*` × 3 |
| 4 | panic 路径事务回滚 | 已完成 | — |
| 5 | NaN/Inf JSON 序列化 | 已完成 | `json_float_handles_nan_and_inf` |
