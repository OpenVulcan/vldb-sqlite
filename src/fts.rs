use crate::tokenizer::{TokenizerMode, ensure_jieba_tokenizer_registered, tokenize_text};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

/// FTS 索引元信息返回（中英双语）。
/// FTS index metadata response (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureFtsIndexResult {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 返回消息。
    /// Human readable response message.
    pub message: String,
    /// 最终使用的索引名。
    /// Effective sanitized index name.
    pub index_name: String,
    /// 最终使用的分词模式。
    /// Effective tokenizer mode.
    pub tokenizer_mode: String,
}

/// FTS 重建结果（中英双语）。
/// FTS rebuild result payload (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildFtsIndexResult {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 返回消息。
    /// Human readable response message.
    pub message: String,
    /// 索引名。
    /// Effective index name.
    pub index_name: String,
    /// 使用的分词模式。
    /// Effective tokenizer mode.
    pub tokenizer_mode: String,
    /// 重建时重新写回的文档数。
    /// Number of documents reindexed during rebuild.
    pub reindexed_rows: u64,
}

/// FTS 文档变更结果（中英双语）。
/// FTS document mutation result (bilingual).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FtsMutationResult {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 返回消息。
    /// Human readable response message.
    pub message: String,
    /// 受影响行数。
    /// Number of affected rows.
    pub affected_rows: u64,
    /// 索引名。
    /// Index name.
    pub index_name: String,
}

/// FTS 命中文档（中英双语）。
/// FTS hit payload (bilingual).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchFtsHit {
    /// 业务 ID。
    /// Business identifier.
    pub id: String,
    /// 文件路径或逻辑路径。
    /// File path or logical path.
    pub file_path: String,
    /// 标题。
    /// Title field.
    pub title: String,
    /// 带命中高亮的标题。
    /// Highlighted title text.
    pub title_highlight: String,
    /// 带上下文片段的正文摘要。
    /// Content snippet with query highlights.
    pub content_snippet: String,
    /// 标准化分数，统一约定为“越大越好”。
    /// Normalized score, always “higher is better”.
    pub score: f64,
    /// 当前结果中的排序名次。
    /// Rank inside the current result set.
    pub rank: u64,
    /// SQLite `bm25()` 原始分值。
    /// Raw SQLite `bm25()` score.
    pub raw_score: f64,
}

/// FTS 检索结果（中英双语）。
/// FTS search response payload (bilingual).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchFtsResult {
    /// 操作是否成功。
    /// Whether the operation succeeded.
    pub success: bool,
    /// 返回消息。
    /// Human readable response message.
    pub message: String,
    /// 索引名。
    /// Index name.
    pub index_name: String,
    /// 使用的分词模式。
    /// Effective tokenizer mode.
    pub tokenizer_mode: String,
    /// 规范化后的查询文本。
    /// Normalized query text.
    pub normalized_query: String,
    /// 最终传给 SQLite MATCH 的表达式。
    /// Final FTS MATCH expression passed into SQLite.
    pub fts_query: String,
    /// 检索结果来源标识，供混合检索层识别。
    /// Result source label for hybrid retrieval layers.
    pub source: String,
    /// 查询模式标识，便于上层做统一调度。
    /// Query mode label for upper-layer orchestration.
    pub query_mode: String,
    /// 命中总数。
    /// Total number of hits.
    pub total: u64,
    /// 命中列表。
    /// Search hit list.
    pub hits: Vec<SearchFtsHit>,
}

/// 确保某个 FTS 索引存在（中英双语）。
/// Ensure an FTS index exists for the requested logical name (bilingual).
pub fn ensure_fts_index(
    connection: &Connection,
    index_name: &str,
    tokenizer_mode: TokenizerMode,
) -> rusqlite::Result<EnsureFtsIndexResult> {
    if tokenizer_mode == TokenizerMode::Jieba {
        ensure_jieba_tokenizer_registered(connection)?;
    }

    let index_name = sanitize_index_name(index_name)?;
    let quoted_index_name = quote_identifier(&index_name);
    let tokenizer_sql = tokenizer_sql(tokenizer_mode);

    connection.execute_batch(&format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS {index_name} USING fts5(
            id UNINDEXED,
            file_path UNINDEXED,
            title,
            content,
            tokenize={tokenizer_sql}
        );",
        index_name = quoted_index_name,
        tokenizer_sql = tokenizer_sql,
    ))?;

    Ok(EnsureFtsIndexResult {
        success: true,
        message: "fts index ensured / FTS 索引已确认存在".to_string(),
        index_name,
        tokenizer_mode: tokenizer_mode.as_str().to_string(),
    })
}

/// 重建某个 FTS 索引，使已有文档重新吃到新的分词与词典策略（中英双语）。
/// Rebuild an FTS index so existing rows pick up new tokenizer and dictionary behavior (bilingual).
pub fn rebuild_fts_index(
    connection: &Connection,
    index_name: &str,
    tokenizer_mode: TokenizerMode,
) -> rusqlite::Result<RebuildFtsIndexResult> {
    if tokenizer_mode == TokenizerMode::Jieba {
        ensure_jieba_tokenizer_registered(connection)?;
    }

    let index_name = sanitize_index_name(index_name)?;
    let quoted_index_name = quote_identifier(&index_name);
    let exists: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![index_name.as_str()],
        |row| row.get(0),
    )?;

    if exists == 0 {
        let ensured = ensure_fts_index(connection, index_name.as_str(), tokenizer_mode)?;
        return Ok(RebuildFtsIndexResult {
            success: true,
            message: "fts index created during rebuild / FTS 索引在重建过程中已创建".to_string(),
            index_name: ensured.index_name,
            tokenizer_mode: ensured.tokenizer_mode,
            reindexed_rows: 0,
        });
    }

    let mut statement = connection.prepare(&format!(
        "SELECT id, file_path, title, content FROM {index_name} ORDER BY rowid ASC",
        index_name = quoted_index_name
    ))?;
    let mut rows = statement.query([])?;
    let mut documents = Vec::new();
    while let Some(row) = rows.next()? {
        documents.push((
            row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            row.get::<_, Option<String>>(3)?.unwrap_or_default(),
        ));
    }
    drop(rows);
    drop(statement);

    connection.execute_batch("BEGIN IMMEDIATE TRANSACTION;")?;
    let rebuild_result = (|| -> rusqlite::Result<RebuildFtsIndexResult> {
        connection.execute_batch(&format!(
            "DROP TABLE IF EXISTS {index_name};",
            index_name = quoted_index_name
        ))?;
        let ensured = ensure_fts_index(connection, index_name.as_str(), tokenizer_mode)?;
        let mut reindexed_rows = 0_u64;
        for (id, file_path, title, content) in documents {
            upsert_fts_document(
                connection,
                ensured.index_name.as_str(),
                tokenizer_mode,
                id.as_str(),
                file_path.as_str(),
                title.as_str(),
                content.as_str(),
            )?;
            reindexed_rows += 1;
        }
        Ok(RebuildFtsIndexResult {
            success: true,
            message: format!(
                "fts index rebuilt (rows={}) / FTS 索引已重建",
                reindexed_rows
            ),
            index_name: ensured.index_name,
            tokenizer_mode: ensured.tokenizer_mode,
            reindexed_rows,
        })
    })();

    match rebuild_result {
        Ok(result) => {
            connection.execute_batch("COMMIT;")?;
            Ok(result)
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK;");
            Err(error)
        }
    }
}

/// 写入或更新 FTS 文档（中英双语）。
/// Insert or update an FTS document (bilingual).
pub fn upsert_fts_document(
    connection: &Connection,
    index_name: &str,
    tokenizer_mode: TokenizerMode,
    id: &str,
    file_path: &str,
    title: &str,
    content: &str,
) -> rusqlite::Result<FtsMutationResult> {
    let ensured = ensure_fts_index(connection, index_name, tokenizer_mode)?;
    let quoted_index_name = quote_identifier(&ensured.index_name);

    let mut affected_rows = 0_u64;
    affected_rows += connection.execute(
        &format!("DELETE FROM {index_name} WHERE id = ?1", index_name = quoted_index_name),
        params![id],
    )? as u64;
    affected_rows += connection.execute(
        &format!(
            "INSERT INTO {index_name} (id, file_path, title, content) VALUES (?1, ?2, ?3, ?4)",
            index_name = quoted_index_name
        ),
        params![id, file_path, title, content],
    )? as u64;

    Ok(FtsMutationResult {
        success: true,
        message: "fts document upserted / FTS 文档已写入".to_string(),
        affected_rows,
        index_name: ensured.index_name,
    })
}

/// 删除 FTS 文档（中英双语）。
/// Delete an FTS document by business id (bilingual).
pub fn delete_fts_document(
    connection: &Connection,
    index_name: &str,
    id: &str,
) -> rusqlite::Result<FtsMutationResult> {
    let index_name = sanitize_index_name(index_name)?;
    let quoted_index_name = quote_identifier(&index_name);
    let affected_rows = connection.execute(
        &format!("DELETE FROM {index_name} WHERE id = ?1", index_name = quoted_index_name),
        params![id],
    )? as u64;

    Ok(FtsMutationResult {
        success: true,
        message: if affected_rows > 0 {
            "fts document removed / FTS 文档已删除".to_string()
        } else {
            "fts document not found / FTS 文档不存在".to_string()
        },
        affected_rows,
        index_name,
    })
}

/// 执行标准化的 FTS 检索（中英双语）。
/// Execute normalized FTS search with RRF-friendly fields (bilingual).
pub fn search_fts(
    connection: &Connection,
    index_name: &str,
    tokenizer_mode: TokenizerMode,
    query: &str,
    limit: u32,
    offset: u32,
) -> rusqlite::Result<SearchFtsResult> {
    let ensured = ensure_fts_index(connection, index_name, tokenizer_mode)?;
    let tokenized_query = tokenize_text(Some(connection), tokenizer_mode, query, true)?;
    let quoted_index_name = quote_identifier(&ensured.index_name);
    let effective_limit = limit.clamp(1, 200);

    let total: u64 = connection.query_row(
        &format!(
            "SELECT COUNT(*) FROM {index_name} WHERE {index_name} MATCH ?1",
            index_name = quoted_index_name,
        ),
        params![tokenized_query.fts_query.as_str()],
        |row| row.get::<_, i64>(0),
    )? as u64;

    let mut statement = connection.prepare(&format!(
        "SELECT
            id,
            file_path,
            title,
            highlight({index_name}, 2, '<mark>', '</mark>') AS title_highlight,
            snippet({index_name}, 3, '<mark>', '</mark>', '...', 12) AS content_snippet,
            bm25({index_name}, 2.0, 1.0) AS raw_score
         FROM {index_name}
         WHERE {index_name} MATCH ?1
         ORDER BY raw_score ASC, file_path ASC, id ASC
         LIMIT ?2 OFFSET ?3",
        index_name = quoted_index_name,
    ))?;

    let mut rows = statement.query(params![
        tokenized_query.fts_query.as_str(),
        effective_limit as i64,
        offset as i64
    ])?;
    let mut hits = Vec::new();
    let mut rank = offset as u64 + 1;
    while let Some(row) = rows.next()? {
        let raw_score = row.get::<_, f64>(5)?;
        hits.push(SearchFtsHit {
            id: row.get(0)?,
            file_path: row.get(1)?,
            title: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            title_highlight: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            content_snippet: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            score: -raw_score,
            rank,
            raw_score,
        });
        rank += 1;
    }

    Ok(SearchFtsResult {
        success: true,
        message: format!("fts search completed (hits={}) / FTS 检索完成", hits.len()),
        index_name: ensured.index_name,
        tokenizer_mode: ensured.tokenizer_mode,
        normalized_query: tokenized_query.normalized_text,
        fts_query: tokenized_query.fts_query,
        source: "sqlite_fts".to_string(),
        query_mode: "fts".to_string(),
        total,
        hits,
    })
}

/// 校验并规范化索引名（中英双语）。
/// Validate and normalize an FTS index name (bilingual).
fn sanitize_index_name(index_name: &str) -> rusqlite::Result<String> {
    let trimmed = index_name.trim();
    if trimmed.is_empty() {
        return Err(rusqlite::Error::InvalidParameterName(
            "index_name must not be empty / index_name 不能为空".to_string(),
        ));
    }

    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err(rusqlite::Error::InvalidParameterName(
            "index_name must not be empty / index_name 不能为空".to_string(),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(rusqlite::Error::InvalidParameterName(
            "index_name must start with [A-Za-z_] / index_name 必须以字母或下划线开头".to_string(),
        ));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(rusqlite::Error::InvalidParameterName(
            "index_name only supports [A-Za-z0-9_] / index_name 仅支持字母数字下划线".to_string(),
        ));
    }
    if trimmed.starts_with("_vulcan_") {
        return Err(rusqlite::Error::InvalidParameterName(
            "reserved index_name prefix / 保留索引名前缀".to_string(),
        ));
    }

    Ok(trimmed.to_string())
}

/// 为 SQLite 标识符加引号（中英双语）。
/// Quote a validated SQLite identifier (bilingual).
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// 生成 FTS5 的 tokenizer SQL 片段（中英双语）。
/// Build the FTS5 tokenizer SQL fragment (bilingual).
fn tokenizer_sql(tokenizer_mode: TokenizerMode) -> &'static str {
    match tokenizer_mode {
        TokenizerMode::None => "'unicode61 remove_diacritics 2'",
        TokenizerMode::Jieba => "'jieba'",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 FTS 索引建表、写入与检索的最小闭环（中英双语）。
    /// Verify the minimal end-to-end FTS flow: ensure index, upsert, then search (bilingual).
    #[test]
    fn ensure_upsert_and_search_fts() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        let ensured = ensure_fts_index(&connection, "memory_docs", TokenizerMode::Jieba)?;
        assert!(ensured.success);
        assert_eq!(ensured.index_name, "memory_docs");

        upsert_fts_document(
            &connection,
            "memory_docs",
            TokenizerMode::Jieba,
            "doc-1",
            "/demo/file.md",
            "测试标题",
            "市民田-女士急匆匆",
        )?;
        let _ = crate::tokenizer::upsert_custom_word(&connection, "田-女士", 42)?;
        upsert_fts_document(
            &connection,
            "memory_docs",
            TokenizerMode::Jieba,
            "doc-1",
            "/demo/file.md",
            "测试标题",
            "市民田-女士急匆匆",
        )?;

        let result = search_fts(
            &connection,
            "memory_docs",
            TokenizerMode::Jieba,
            "田-女士",
            10,
            0,
        )?;
        assert!(result.success);
        assert_eq!(result.total, 1);
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].id, "doc-1");
        assert_eq!(result.hits[0].file_path, "/demo/file.md");
        assert_eq!(result.hits[0].rank, 1);
        assert!(result.hits[0].content_snippet.contains("mark"));
        assert_eq!(result.source, "sqlite_fts");
        assert_eq!(result.query_mode, "fts");

        Ok(())
    }

    /// 验证词典热更新后可通过重建索引让旧文档重新吃到新分词策略（中英双语）。
    /// Verify index rebuild applies updated dictionary behavior to previously indexed documents (bilingual).
    #[test]
    fn rebuild_fts_index_reindexes_existing_documents() -> rusqlite::Result<()> {
        let connection = Connection::open_in_memory()?;
        ensure_fts_index(&connection, "memory_docs", TokenizerMode::Jieba)?;
        upsert_fts_document(
            &connection,
            "memory_docs",
            TokenizerMode::Jieba,
            "doc-1",
            "/demo/file.md",
            "测试标题",
            "市民田-女士急匆匆",
        )?;

        connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS memory_docs_vocab USING fts5vocab(
                memory_docs,
                'instance'
            );",
        )?;
        let before_count: i64 = connection.query_row(
            "SELECT count(*) FROM memory_docs_vocab WHERE term = ?1",
            params!["田-女士"],
            |row| row.get(0),
        )?;
        assert_eq!(before_count, 0);

        crate::tokenizer::upsert_custom_word(&connection, "田-女士", 42)?;
        let rebuild = rebuild_fts_index(&connection, "memory_docs", TokenizerMode::Jieba)?;
        assert!(rebuild.success);
        assert_eq!(rebuild.reindexed_rows, 1);

        connection.execute_batch("DROP TABLE IF EXISTS memory_docs_vocab;")?;
        connection.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS memory_docs_vocab USING fts5vocab(
                memory_docs,
                'instance'
            );",
        )?;
        let after_count: i64 = connection.query_row(
            "SELECT count(*) FROM memory_docs_vocab WHERE term = ?1",
            params!["田-女士"],
            |row| row.get(0),
        )?;
        assert_eq!(after_count, 1);

        Ok(())
    }
}
