use serde::Serialize;

/// `vldb-sqlite` 库模式信息对象。
/// Library-mode metadata object for `vldb-sqlite`.
#[derive(Debug, Clone, Serialize)]
pub struct VldbSqliteLibraryInfo {
    /// 库名称，供宿主与调试输出识别。
    /// Library name used by hosts and debugging output.
    pub name: &'static str,
    /// 库当前版本，对齐 Cargo 包版本。
    /// Current library version aligned with the Cargo package version.
    pub version: &'static str,
    /// 当前 FFI 层阶段说明。
    /// Current lifecycle stage description for the FFI layer.
    pub ffi_stage: &'static str,
    /// 当前导出的能力类别，便于上层探测是否具备库模式。
    /// Exported capability categories so callers can detect library-mode support.
    pub capabilities: &'static [&'static str],
}

/// 返回 `vldb-sqlite` 当前的库模式元信息。
/// Return the current library-mode metadata for `vldb-sqlite`.
pub fn library_info() -> VldbSqliteLibraryInfo {
    VldbSqliteLibraryInfo {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        ffi_stage: "sqlite-runtime-go-ffi",
        capabilities: &[
            "library_info_json",
            "runtime_create_default",
            "runtime_open_database",
            "runtime_close_database",
            "database_execute_script",
            "database_execute_batch",
            "database_query_json",
            "database_query_stream",
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
            "search_fts_json",
            "execute_script_json",
            "execute_batch_json",
            "query_json_json",
            "query_stream_json",
            "query_stream_chunk_json",
            "query_stream_close_json",
        ],
    }
}
