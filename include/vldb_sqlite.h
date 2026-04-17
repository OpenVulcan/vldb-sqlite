#ifndef VLDB_SQLITE_H
#define VLDB_SQLITE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * FFI 状态码；0 表示成功，1 表示失败。
 * FFI status code; 0 means success and 1 means failure.
 */
typedef enum VldbSqliteStatusCode {
    VLDB_SQLITE_STATUS_SUCCESS = 0,
    VLDB_SQLITE_STATUS_FAILURE = 1
} VldbSqliteStatusCode;

/*
 * FFI 分词模式枚举。
 * FFI tokenizer mode enum.
 */
typedef enum VldbSqliteFfiTokenizerMode {
    VLDB_SQLITE_TOKENIZER_NONE = 0,
    VLDB_SQLITE_TOKENIZER_JIEBA = 1
} VldbSqliteFfiTokenizerMode;

/*
 * Runtime 句柄前置声明。
 * Forward declaration of the runtime handle.
 */
typedef struct VldbSqliteRuntimeHandle VldbSqliteRuntimeHandle;

/*
 * 数据库句柄前置声明。
 * Forward declaration of the database handle.
 */
typedef struct VldbSqliteDatabaseHandle VldbSqliteDatabaseHandle;

/*
 * 分词结果句柄前置声明。
 * Forward declaration of the tokenize-result handle.
 */
typedef struct VldbSqliteTokenizeResultHandle VldbSqliteTokenizeResultHandle;

/*
 * 自定义词列表句柄前置声明。
 * Forward declaration of the custom-word list handle.
 */
typedef struct VldbSqliteCustomWordListHandle VldbSqliteCustomWordListHandle;

/*
 * 检索结果句柄前置声明。
 * Forward declaration of the search-result handle.
 */
typedef struct VldbSqliteSearchResultHandle VldbSqliteSearchResultHandle;

/*
 * 自定义词修改结果结构。
 * Custom-word mutation result structure.
 */
typedef struct VldbSqliteDictionaryMutationResultPod {
    uint8_t success;
    uint64_t affected_rows;
} VldbSqliteDictionaryMutationResultPod;

/*
 * FTS 索引确保结果结构。
 * FTS ensure-index result structure.
 */
typedef struct VldbSqliteEnsureFtsIndexResultPod {
    uint8_t success;
    uint32_t tokenizer_mode;
} VldbSqliteEnsureFtsIndexResultPod;

/*
 * FTS 索引重建结果结构。
 * FTS rebuild-index result structure.
 */
typedef struct VldbSqliteRebuildFtsIndexResultPod {
    uint8_t success;
    uint32_t tokenizer_mode;
    uint64_t reindexed_rows;
} VldbSqliteRebuildFtsIndexResultPod;

/*
 * FTS 文档写入/删除结果结构。
 * FTS document mutation result structure.
 */
typedef struct VldbSqliteFtsMutationResultPod {
    uint8_t success;
    uint64_t affected_rows;
} VldbSqliteFtsMutationResultPod;

/*
 * 返回 `vldb-sqlite` 当前库模式元信息 JSON。
 * Return the current library-mode metadata JSON for `vldb-sqlite`.
 */
char* vldb_sqlite_library_info_json(void);

/*
 * 创建默认多库 runtime 句柄。
 * Create a default multi-database runtime handle.
 */
VldbSqliteRuntimeHandle* vldb_sqlite_runtime_create_default(void);

/*
 * 释放多库 runtime 句柄。
 * Destroy a multi-database runtime handle.
 */
void vldb_sqlite_runtime_destroy(VldbSqliteRuntimeHandle* handle);

/*
 * 打开或复用指定路径的数据库句柄。
 * Open or reuse a database handle for the specified path.
 */
VldbSqliteDatabaseHandle* vldb_sqlite_runtime_open_database(
    VldbSqliteRuntimeHandle* runtime,
    const char* db_path
);

/*
 * 关闭 runtime 中缓存的数据库。
 * Close a cached database from the runtime.
 */
uint8_t vldb_sqlite_runtime_close_database(
    VldbSqliteRuntimeHandle* runtime,
    const char* db_path
);

/*
 * 释放数据库句柄。
 * Destroy a database handle.
 */
void vldb_sqlite_database_destroy(VldbSqliteDatabaseHandle* handle);

/*
 * 返回数据库句柄绑定的路径字符串。
 * Return the bound database path string for a database handle.
 */
char* vldb_sqlite_database_db_path(VldbSqliteDatabaseHandle* handle);

/*
 * 释放由本库分配的字符串。
 * Free a string allocated by this library.
 */
void vldb_sqlite_string_free(char* value);

/*
 * 获取最近一次 FFI 错误消息；返回指针在下一次错误更新或 clear 后失效。
 * Return the latest FFI error message; the pointer becomes invalid after the next error update or clear.
 */
const char* vldb_sqlite_last_error_message(void);

/*
 * 清理最近一次 FFI 错误消息。
 * Clear the latest FFI error message.
 */
void vldb_sqlite_clear_last_error(void);

/*
 * 返回 JSON 指针是否为空。
 * Return whether the JSON pointer is null.
 */
uint8_t vldb_sqlite_json_is_null(const char* value);

/*
 * 通过数据库句柄执行分词主接口，返回结果句柄。
 * Execute the main tokenize interface from a database handle and return a result handle.
 */
VldbSqliteTokenizeResultHandle* vldb_sqlite_database_tokenize_text(
    VldbSqliteDatabaseHandle* handle,
    VldbSqliteFfiTokenizerMode tokenizer_mode,
    const char* text,
    uint8_t search_mode
);

/*
 * 释放分词结果句柄。
 * Destroy a tokenize-result handle.
 */
void vldb_sqlite_tokenize_result_destroy(VldbSqliteTokenizeResultHandle* handle);

/*
 * 返回分词结果中的规范化文本。
 * Return the normalized text from a tokenize result.
 */
char* vldb_sqlite_tokenize_result_normalized_text(VldbSqliteTokenizeResultHandle* handle);

/*
 * 返回分词结果中的 FTS 查询表达式。
 * Return the FTS query expression from a tokenize result.
 */
char* vldb_sqlite_tokenize_result_fts_query(VldbSqliteTokenizeResultHandle* handle);

/*
 * 返回分词结果中的词元数量。
 * Return the token count from a tokenize result.
 */
uint64_t vldb_sqlite_tokenize_result_token_count(VldbSqliteTokenizeResultHandle* handle);

/*
 * 返回指定下标的词元字符串。
 * Return the token string at the specified index.
 */
char* vldb_sqlite_tokenize_result_get_token(
    VldbSqliteTokenizeResultHandle* handle,
    uint64_t index
);

/*
 * 通过数据库句柄写入或更新自定义词。
 * Upsert a custom dictionary word through a database handle.
 */
int32_t vldb_sqlite_database_upsert_custom_word(
    VldbSqliteDatabaseHandle* handle,
    const char* word,
    uint64_t weight,
    VldbSqliteDictionaryMutationResultPod* out_result
);

/*
 * 通过数据库句柄删除自定义词。
 * Remove a custom dictionary word through a database handle.
 */
int32_t vldb_sqlite_database_remove_custom_word(
    VldbSqliteDatabaseHandle* handle,
    const char* word,
    VldbSqliteDictionaryMutationResultPod* out_result
);

/*
 * 通过数据库句柄列出自定义词，返回列表句柄。
 * List custom words through a database handle and return a list handle.
 */
VldbSqliteCustomWordListHandle* vldb_sqlite_database_list_custom_words(
    VldbSqliteDatabaseHandle* handle
);

/*
 * 释放自定义词列表句柄。
 * Destroy a custom-word list handle.
 */
void vldb_sqlite_custom_word_list_destroy(VldbSqliteCustomWordListHandle* handle);

/*
 * 返回自定义词列表长度。
 * Return the number of custom words in the list.
 */
uint64_t vldb_sqlite_custom_word_list_len(VldbSqliteCustomWordListHandle* handle);

/*
 * 返回指定自定义词条目的词文本。
 * Return the word text of a specific custom-word entry.
 */
char* vldb_sqlite_custom_word_list_get_word(
    VldbSqliteCustomWordListHandle* handle,
    uint64_t index
);

/*
 * 返回指定自定义词条目的权重。
 * Return the weight of a specific custom-word entry.
 */
uint64_t vldb_sqlite_custom_word_list_get_weight(
    VldbSqliteCustomWordListHandle* handle,
    uint64_t index
);

/*
 * 通过数据库句柄确保 FTS 索引存在。
 * Ensure an FTS index exists through a database handle.
 */
int32_t vldb_sqlite_database_ensure_fts_index(
    VldbSqliteDatabaseHandle* handle,
    const char* index_name,
    VldbSqliteFfiTokenizerMode tokenizer_mode,
    VldbSqliteEnsureFtsIndexResultPod* out_result
);

/*
 * 通过数据库句柄重建 FTS 索引。
 * Rebuild an FTS index through a database handle.
 */
int32_t vldb_sqlite_database_rebuild_fts_index(
    VldbSqliteDatabaseHandle* handle,
    const char* index_name,
    VldbSqliteFfiTokenizerMode tokenizer_mode,
    VldbSqliteRebuildFtsIndexResultPod* out_result
);

/*
 * 通过数据库句柄写入或更新 FTS 文档。
 * Upsert an FTS document through a database handle.
 */
int32_t vldb_sqlite_database_upsert_fts_document(
    VldbSqliteDatabaseHandle* handle,
    const char* index_name,
    VldbSqliteFfiTokenizerMode tokenizer_mode,
    const char* id,
    const char* file_path,
    const char* title,
    const char* content,
    VldbSqliteFtsMutationResultPod* out_result
);

/*
 * 通过数据库句柄删除 FTS 文档。
 * Delete an FTS document through a database handle.
 */
int32_t vldb_sqlite_database_delete_fts_document(
    VldbSqliteDatabaseHandle* handle,
    const char* index_name,
    const char* id,
    VldbSqliteFtsMutationResultPod* out_result
);

/*
 * 通过数据库句柄执行 FTS 检索并返回结果句柄。
 * Execute FTS search through a database handle and return a result handle.
 */
VldbSqliteSearchResultHandle* vldb_sqlite_database_search_fts(
    VldbSqliteDatabaseHandle* handle,
    const char* index_name,
    VldbSqliteFfiTokenizerMode tokenizer_mode,
    const char* query,
    uint32_t limit,
    uint32_t offset
);

/*
 * 释放 FTS 检索结果句柄。
 * Destroy an FTS search-result handle.
 */
void vldb_sqlite_search_result_destroy(VldbSqliteSearchResultHandle* handle);

/*
 * 返回 FTS 检索总命中数。
 * Return the total hit count of an FTS search result.
 */
uint64_t vldb_sqlite_search_result_total(VldbSqliteSearchResultHandle* handle);

/*
 * 返回 FTS 检索当前页命中数。
 * Return the page hit count of an FTS search result.
 */
uint64_t vldb_sqlite_search_result_len(VldbSqliteSearchResultHandle* handle);

/*
 * 返回检索结果的来源标签。
 * Return the source label of a search result.
 */
char* vldb_sqlite_search_result_source(VldbSqliteSearchResultHandle* handle);

/*
 * 返回检索结果的查询模式标签。
 * Return the query-mode label of a search result.
 */
char* vldb_sqlite_search_result_query_mode(VldbSqliteSearchResultHandle* handle);

/*
 * 返回指定命中的业务 ID。
 * Return the business ID of the specified hit.
 */
char* vldb_sqlite_search_result_get_id(VldbSqliteSearchResultHandle* handle, uint64_t index);

/*
 * 返回指定命中的文件路径。
 * Return the file path of the specified hit.
 */
char* vldb_sqlite_search_result_get_file_path(
    VldbSqliteSearchResultHandle* handle,
    uint64_t index
);

/*
 * 返回指定命中的标题。
 * Return the title of the specified hit.
 */
char* vldb_sqlite_search_result_get_title(VldbSqliteSearchResultHandle* handle, uint64_t index);

/*
 * 返回指定命中的高亮标题。
 * Return the highlighted title of the specified hit.
 */
char* vldb_sqlite_search_result_get_title_highlight(
    VldbSqliteSearchResultHandle* handle,
    uint64_t index
);

/*
 * 返回指定命中的内容片段。
 * Return the content snippet of the specified hit.
 */
char* vldb_sqlite_search_result_get_content_snippet(
    VldbSqliteSearchResultHandle* handle,
    uint64_t index
);

/*
 * 返回指定命中的标准化分数。
 * Return the normalized score of the specified hit.
 */
double vldb_sqlite_search_result_get_score(VldbSqliteSearchResultHandle* handle, uint64_t index);

/*
 * 返回指定命中的名次。
 * Return the rank of the specified hit.
 */
uint64_t vldb_sqlite_search_result_get_rank(VldbSqliteSearchResultHandle* handle, uint64_t index);

/*
 * 返回指定命中的原始 BM25 分数。
 * Return the raw BM25 score of the specified hit.
 */
double vldb_sqlite_search_result_get_raw_score(
    VldbSqliteSearchResultHandle* handle,
    uint64_t index
);

/*
 * 通过 JSON 请求执行分词；请求和响应都使用 UTF-8 JSON 字符串。
 * Execute tokenization from a JSON request; both request and response are UTF-8 JSON strings.
 */
char* vldb_sqlite_tokenize_text_json(const char* request_json);

/*
 * 通过 JSON 请求写入或更新自定义词。
 * Upsert a custom dictionary word from a JSON request.
 */
char* vldb_sqlite_upsert_custom_word_json(const char* request_json);

/*
 * 通过 JSON 请求删除自定义词。
 * Remove a custom dictionary word from a JSON request.
 */
char* vldb_sqlite_remove_custom_word_json(const char* request_json);

/*
 * 通过 JSON 请求列出当前库中的自定义词。
 * List custom dictionary words from a JSON request.
 */
char* vldb_sqlite_list_custom_words_json(const char* request_json);

/*
 * 通过 JSON 请求确保 FTS 索引存在。
 * Ensure an FTS index exists from a JSON request.
 */
char* vldb_sqlite_ensure_fts_index_json(const char* request_json);

/*
 * 通过 JSON 请求重建 FTS 索引。
 * Rebuild an FTS index from a JSON request.
 */
char* vldb_sqlite_rebuild_fts_index_json(const char* request_json);

/*
 * 通过 JSON 请求写入或更新 FTS 文档。
 * Upsert an FTS document from a JSON request.
 */
char* vldb_sqlite_upsert_fts_document_json(const char* request_json);

/*
 * 通过 JSON 请求删除 FTS 文档。
 * Delete an FTS document from a JSON request.
 */
char* vldb_sqlite_delete_fts_document_json(const char* request_json);

/*
 * 通过 JSON 请求执行标准化 FTS 检索。
 * Execute normalized FTS search from a JSON request.
 */
char* vldb_sqlite_search_fts_json(const char* request_json);

#ifdef __cplusplus
}
#endif

#endif /* VLDB_SQLITE_H */
