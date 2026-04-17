package sqliteffi

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// StatusCode 表示 FFI 调用状态码。
// StatusCode represents an FFI invocation status code.
type StatusCode int32

const (
	// StatusSuccess 表示调用成功。
	// StatusSuccess indicates a successful invocation.
	StatusSuccess StatusCode = 0
	// StatusFailure 表示调用失败。
	// StatusFailure indicates a failed invocation.
	StatusFailure StatusCode = 1
)

// TokenizerMode 表示 FFI 分词模式。
// TokenizerMode represents the FFI tokenizer mode.
type TokenizerMode int32

const (
	// TokenizerNone 表示不启用 Jieba。
	// TokenizerNone means Jieba is not enabled.
	TokenizerNone TokenizerMode = 0
	// TokenizerJieba 表示启用 Jieba。
	// TokenizerJieba means Jieba is enabled.
	TokenizerJieba TokenizerMode = 1
)

// DictionaryMutationResult 表示自定义词修改结果。
// DictionaryMutationResult represents the result of a custom-word mutation.
type DictionaryMutationResult struct {
	// Success 表示操作是否成功。
	// Success reports whether the operation succeeded.
	Success bool
	// AffectedRows 表示受影响行数。
	// AffectedRows reports the number of affected rows.
	AffectedRows uint64
}

// EnsureFtsIndexResult 表示索引确保结果。
// EnsureFtsIndexResult represents the ensure-index result.
type EnsureFtsIndexResult struct {
	// Success 表示操作是否成功。
	// Success reports whether the operation succeeded.
	Success bool
	// TokenizerMode 表示最终分词模式。
	// TokenizerMode reports the effective tokenizer mode.
	TokenizerMode TokenizerMode
}

// RebuildFtsIndexResult 表示索引重建结果。
// RebuildFtsIndexResult represents the rebuild-index result.
type RebuildFtsIndexResult struct {
	// Success 表示操作是否成功。
	// Success reports whether the operation succeeded.
	Success bool
	// TokenizerMode 表示最终分词模式。
	// TokenizerMode reports the effective tokenizer mode.
	TokenizerMode TokenizerMode
	// ReindexedRows 表示回写文档数。
	// ReindexedRows reports the number of reindexed documents.
	ReindexedRows uint64
}

// FtsMutationResult 表示文档写入或删除结果。
// FtsMutationResult represents the document upsert/delete result.
type FtsMutationResult struct {
	// Success 表示操作是否成功。
	// Success reports whether the operation succeeded.
	Success bool
	// AffectedRows 表示受影响行数。
	// AffectedRows reports the number of affected rows.
	AffectedRows uint64
}

// SearchHit 表示单条检索命中。
// SearchHit represents a single search hit.
type SearchHit struct {
	// ID 表示业务主键。
	// ID is the business identifier.
	ID string
	// FilePath 表示文件路径或逻辑路径。
	// FilePath is the file path or logical path.
	FilePath string
	// Title 表示标题。
	// Title is the title field.
	Title string
	// TitleHighlight 表示带高亮的标题。
	// TitleHighlight is the highlighted title.
	TitleHighlight string
	// ContentSnippet 表示正文片段。
	// ContentSnippet is the highlighted content snippet.
	ContentSnippet string
	// Score 表示标准化后的分数。
	// Score is the normalized score.
	Score float64
	// Rank 表示当前结果中的名次。
	// Rank is the rank inside the current result set.
	Rank uint64
	// RawScore 表示原始 BM25 分数。
	// RawScore is the raw BM25 score.
	RawScore float64
}

// SearchResult 表示完整检索结果。
// SearchResult represents the full search result.
type SearchResult struct {
	// Total 表示命中总数。
	// Total reports the total number of hits.
	Total uint64
	// Source 表示结果来源。
	// Source is the result source label.
	Source string
	// QueryMode 表示查询模式。
	// QueryMode is the query-mode label.
	QueryMode string
	// Hits 表示当前页命中列表。
	// Hits contains the hits in the current page.
	Hits []SearchHit
}

// Library 表示已加载的 vldb-sqlite 动态库。
// Library represents a loaded vldb-sqlite dynamic library.
type Library struct {
	// handle 表示底层动态库句柄。
	// handle is the underlying dynamic-library handle.
	handle uintptr

	// ffi 函数绑定。
	// Bound ffi functions.
	runtimeCreateDefault          func() unsafe.Pointer
	runtimeDestroy                func(unsafe.Pointer)
	runtimeOpenDatabase           func(unsafe.Pointer, *byte) unsafe.Pointer
	runtimeCloseDatabase          func(unsafe.Pointer, *byte) uint8
	databaseDestroy               func(unsafe.Pointer)
	databaseDBPath                func(unsafe.Pointer) *byte
	databaseTokenizeText          func(unsafe.Pointer, TokenizerMode, *byte, uint8) unsafe.Pointer
	tokenizeResultDestroy         func(unsafe.Pointer)
	tokenizeResultNormalizedText  func(unsafe.Pointer) *byte
	tokenizeResultFtsQuery        func(unsafe.Pointer) *byte
	tokenizeResultTokenCount      func(unsafe.Pointer) uint64
	tokenizeResultGetToken        func(unsafe.Pointer, uint64) *byte
	databaseUpsertCustomWord      func(unsafe.Pointer, *byte, uint64, *dictionaryMutationResultPod) int32
	databaseRemoveCustomWord      func(unsafe.Pointer, *byte, *dictionaryMutationResultPod) int32
	databaseListCustomWords       func(unsafe.Pointer) unsafe.Pointer
	customWordListDestroy         func(unsafe.Pointer)
	customWordListLen             func(unsafe.Pointer) uint64
	customWordListGetWord         func(unsafe.Pointer, uint64) *byte
	customWordListGetWeight       func(unsafe.Pointer, uint64) uint64
	databaseEnsureFtsIndex        func(unsafe.Pointer, *byte, TokenizerMode, *ensureFtsIndexResultPod) int32
	databaseRebuildFtsIndex       func(unsafe.Pointer, *byte, TokenizerMode, *rebuildFtsIndexResultPod) int32
	databaseUpsertFtsDocument     func(unsafe.Pointer, *byte, TokenizerMode, *byte, *byte, *byte, *byte, *ftsMutationResultPod) int32
	databaseDeleteFtsDocument     func(unsafe.Pointer, *byte, *byte, *ftsMutationResultPod) int32
	databaseSearchFts             func(unsafe.Pointer, *byte, TokenizerMode, *byte, uint32, uint32) unsafe.Pointer
	searchResultDestroy           func(unsafe.Pointer)
	searchResultTotal             func(unsafe.Pointer) uint64
	searchResultLen               func(unsafe.Pointer) uint64
	searchResultSource            func(unsafe.Pointer) *byte
	searchResultQueryMode         func(unsafe.Pointer) *byte
	searchResultGetID             func(unsafe.Pointer, uint64) *byte
	searchResultGetFilePath       func(unsafe.Pointer, uint64) *byte
	searchResultGetTitle          func(unsafe.Pointer, uint64) *byte
	searchResultGetTitleHighlight func(unsafe.Pointer, uint64) *byte
	searchResultGetContentSnippet func(unsafe.Pointer, uint64) *byte
	searchResultGetScore          func(unsafe.Pointer, uint64) float64
	searchResultGetRank           func(unsafe.Pointer, uint64) uint64
	searchResultGetRawScore       func(unsafe.Pointer, uint64) float64
	stringFree                    func(*byte)
	lastErrorMessage              func() *byte
	clearLastError                func()
}

// Runtime 表示多库运行时。
// Runtime represents the multi-database runtime.
type Runtime struct {
	// lib 表示所属动态库。
	// lib is the owning dynamic library.
	lib *Library
	// handle 表示底层 runtime 句柄。
	// handle is the underlying runtime handle.
	handle unsafe.Pointer
}

// Database 表示单库句柄。
// Database represents a single database handle.
type Database struct {
	// lib 表示所属动态库。
	// lib is the owning dynamic library.
	lib *Library
	// handle 表示底层 database 句柄。
	// handle is the underlying database handle.
	handle unsafe.Pointer
}

// dictionaryMutationResultPod 对应 C ABI 的自定义词修改结构。
// dictionaryMutationResultPod mirrors the C ABI custom-word mutation structure.
type dictionaryMutationResultPod struct {
	Success      uint8
	_            [7]byte
	AffectedRows uint64
}

// ensureFtsIndexResultPod 对应 C ABI 的索引确保结构。
// ensureFtsIndexResultPod mirrors the C ABI ensure-index structure.
type ensureFtsIndexResultPod struct {
	Success       uint8
	_             [3]byte
	TokenizerMode uint32
}

// rebuildFtsIndexResultPod 对应 C ABI 的索引重建结构。
// rebuildFtsIndexResultPod mirrors the C ABI rebuild-index structure.
type rebuildFtsIndexResultPod struct {
	Success       uint8
	_             [3]byte
	TokenizerMode uint32
	ReindexedRows uint64
}

// ftsMutationResultPod 对应 C ABI 的文档修改结构。
// ftsMutationResultPod mirrors the C ABI FTS mutation structure.
type ftsMutationResultPod struct {
	Success      uint8
	_            [7]byte
	AffectedRows uint64
}

// Open 加载 vldb-sqlite 动态库并完成函数绑定。
// Open loads the vldb-sqlite dynamic library and binds all required functions.
func Open(path string) (*Library, error) {
	handle, err := openLibrary(path)
	if err != nil {
		return nil, fmt.Errorf("加载动态库失败 / failed to load dynamic library: %w", err)
	}

	lib := &Library{handle: handle}
	bind := func(target any, name string) {
		purego.RegisterLibFunc(target, handle, name)
	}

	bind(&lib.runtimeCreateDefault, "vldb_sqlite_runtime_create_default")
	bind(&lib.runtimeDestroy, "vldb_sqlite_runtime_destroy")
	bind(&lib.runtimeOpenDatabase, "vldb_sqlite_runtime_open_database")
	bind(&lib.runtimeCloseDatabase, "vldb_sqlite_runtime_close_database")
	bind(&lib.databaseDestroy, "vldb_sqlite_database_destroy")
	bind(&lib.databaseDBPath, "vldb_sqlite_database_db_path")
	bind(&lib.databaseTokenizeText, "vldb_sqlite_database_tokenize_text")
	bind(&lib.tokenizeResultDestroy, "vldb_sqlite_tokenize_result_destroy")
	bind(&lib.tokenizeResultNormalizedText, "vldb_sqlite_tokenize_result_normalized_text")
	bind(&lib.tokenizeResultFtsQuery, "vldb_sqlite_tokenize_result_fts_query")
	bind(&lib.tokenizeResultTokenCount, "vldb_sqlite_tokenize_result_token_count")
	bind(&lib.tokenizeResultGetToken, "vldb_sqlite_tokenize_result_get_token")
	bind(&lib.databaseUpsertCustomWord, "vldb_sqlite_database_upsert_custom_word")
	bind(&lib.databaseRemoveCustomWord, "vldb_sqlite_database_remove_custom_word")
	bind(&lib.databaseListCustomWords, "vldb_sqlite_database_list_custom_words")
	bind(&lib.customWordListDestroy, "vldb_sqlite_custom_word_list_destroy")
	bind(&lib.customWordListLen, "vldb_sqlite_custom_word_list_len")
	bind(&lib.customWordListGetWord, "vldb_sqlite_custom_word_list_get_word")
	bind(&lib.customWordListGetWeight, "vldb_sqlite_custom_word_list_get_weight")
	bind(&lib.databaseEnsureFtsIndex, "vldb_sqlite_database_ensure_fts_index")
	bind(&lib.databaseRebuildFtsIndex, "vldb_sqlite_database_rebuild_fts_index")
	bind(&lib.databaseUpsertFtsDocument, "vldb_sqlite_database_upsert_fts_document")
	bind(&lib.databaseDeleteFtsDocument, "vldb_sqlite_database_delete_fts_document")
	bind(&lib.databaseSearchFts, "vldb_sqlite_database_search_fts")
	bind(&lib.searchResultDestroy, "vldb_sqlite_search_result_destroy")
	bind(&lib.searchResultTotal, "vldb_sqlite_search_result_total")
	bind(&lib.searchResultLen, "vldb_sqlite_search_result_len")
	bind(&lib.searchResultSource, "vldb_sqlite_search_result_source")
	bind(&lib.searchResultQueryMode, "vldb_sqlite_search_result_query_mode")
	bind(&lib.searchResultGetID, "vldb_sqlite_search_result_get_id")
	bind(&lib.searchResultGetFilePath, "vldb_sqlite_search_result_get_file_path")
	bind(&lib.searchResultGetTitle, "vldb_sqlite_search_result_get_title")
	bind(&lib.searchResultGetTitleHighlight, "vldb_sqlite_search_result_get_title_highlight")
	bind(&lib.searchResultGetContentSnippet, "vldb_sqlite_search_result_get_content_snippet")
	bind(&lib.searchResultGetScore, "vldb_sqlite_search_result_get_score")
	bind(&lib.searchResultGetRank, "vldb_sqlite_search_result_get_rank")
	bind(&lib.searchResultGetRawScore, "vldb_sqlite_search_result_get_raw_score")
	bind(&lib.stringFree, "vldb_sqlite_string_free")
	bind(&lib.lastErrorMessage, "vldb_sqlite_last_error_message")
	bind(&lib.clearLastError, "vldb_sqlite_clear_last_error")

	return lib, nil
}

// Close 关闭动态库句柄。
// Close closes the dynamic-library handle.
func (lib *Library) Close() error {
	if lib == nil || lib.handle == 0 {
		return nil
	}
	err := closeLibrary(lib.handle)
	lib.handle = 0
	return err
}

// CreateRuntime 创建默认 runtime。
// CreateRuntime creates the default runtime.
func (lib *Library) CreateRuntime() (*Runtime, error) {
	if lib == nil {
		return nil, errors.New("library is nil / library 不能为空")
	}
	handle := lib.runtimeCreateDefault()
	if handle == nil {
		return nil, lib.lastError()
	}
	runtimeHandle := &Runtime{lib: lib, handle: handle}
	runtime.SetFinalizer(runtimeHandle, func(rt *Runtime) {
		_ = rt.Close()
	})
	return runtimeHandle, nil
}

// OpenDatabase 打开或复用指定数据库。
// OpenDatabase opens or reuses the specified database.
func (rt *Runtime) OpenDatabase(path string) (*Database, error) {
	ptr, keep := makeCString(path)
	defer keep()
	handle := rt.lib.runtimeOpenDatabase(rt.handle, ptr)
	if handle == nil {
		return nil, rt.lib.lastError()
	}
	db := &Database{lib: rt.lib, handle: handle}
	runtime.SetFinalizer(db, func(database *Database) {
		_ = database.Close()
	})
	return db, nil
}

// Close 销毁 runtime 句柄。
// Close destroys the runtime handle.
func (rt *Runtime) Close() error {
	if rt == nil || rt.handle == nil {
		return nil
	}
	rt.lib.runtimeDestroy(rt.handle)
	rt.handle = nil
	runtime.SetFinalizer(rt, nil)
	return nil
}

// CloseDatabase 从 runtime 缓存中关闭指定数据库。
// CloseDatabase closes the specified database from the runtime cache.
func (rt *Runtime) CloseDatabase(path string) bool {
	ptr, keep := makeCString(path)
	defer keep()
	return rt.lib.runtimeCloseDatabase(rt.handle, ptr) != 0
}

// Close 销毁数据库句柄。
// Close destroys the database handle.
func (db *Database) Close() error {
	if db == nil || db.handle == nil {
		return nil
	}
	db.lib.databaseDestroy(db.handle)
	db.handle = nil
	runtime.SetFinalizer(db, nil)
	return nil
}

// DBPath 返回数据库句柄绑定的路径。
// DBPath returns the database path bound to the handle.
func (db *Database) DBPath() (string, error) {
	return db.lib.takeOwnedString(func() *byte {
		return db.lib.databaseDBPath(db.handle)
	})
}

// Tokenize 通过数据库句柄执行分词。
// Tokenize executes tokenization through the database handle.
func (db *Database) Tokenize(mode TokenizerMode, text string, searchMode bool) (TokenizeResult, error) {
	ptr, keep := makeCString(text)
	defer keep()
	result := db.lib.databaseTokenizeText(db.handle, mode, ptr, boolToUint8(searchMode))
	if result == nil {
		return TokenizeResult{}, db.lib.lastError()
	}
	defer db.lib.tokenizeResultDestroy(result)

	normalized, err := db.lib.takeOwnedString(func() *byte {
		return db.lib.tokenizeResultNormalizedText(result)
	})
	if err != nil {
		return TokenizeResult{}, err
	}
	ftsQuery, err := db.lib.takeOwnedString(func() *byte {
		return db.lib.tokenizeResultFtsQuery(result)
	})
	if err != nil {
		return TokenizeResult{}, err
	}
	count := db.lib.tokenizeResultTokenCount(result)
	tokens := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		token, err := db.lib.takeOwnedString(func() *byte {
			return db.lib.tokenizeResultGetToken(result, i)
		})
		if err != nil {
			return TokenizeResult{}, err
		}
		tokens = append(tokens, token)
	}

	return TokenizeResult{
		NormalizedText: normalized,
		FtsQuery:       ftsQuery,
		Tokens:         tokens,
	}, nil
}

// UpsertCustomWord 写入或更新专有词。
// UpsertCustomWord inserts or updates a custom dictionary word.
func (db *Database) UpsertCustomWord(word string, weight uint64) (DictionaryMutationResult, error) {
	ptr, keep := makeCString(word)
	defer keep()
	var pod dictionaryMutationResultPod
	status := StatusCode(db.lib.databaseUpsertCustomWord(db.handle, ptr, weight, &pod))
	if status != StatusSuccess {
		return DictionaryMutationResult{}, db.lib.lastError()
	}
	return DictionaryMutationResult{
		Success:      pod.Success != 0,
		AffectedRows: pod.AffectedRows,
	}, nil
}

// RemoveCustomWord 删除专有词。
// RemoveCustomWord removes a custom dictionary word.
func (db *Database) RemoveCustomWord(word string) (DictionaryMutationResult, error) {
	ptr, keep := makeCString(word)
	defer keep()
	var pod dictionaryMutationResultPod
	status := StatusCode(db.lib.databaseRemoveCustomWord(db.handle, ptr, &pod))
	if status != StatusSuccess {
		return DictionaryMutationResult{}, db.lib.lastError()
	}
	return DictionaryMutationResult{
		Success:      pod.Success != 0,
		AffectedRows: pod.AffectedRows,
	}, nil
}

// ListCustomWords 列出当前库中的专有词。
// ListCustomWords lists custom words from the current database.
func (db *Database) ListCustomWords() ([]CustomWordEntry, error) {
	handle := db.lib.databaseListCustomWords(db.handle)
	if handle == nil {
		return nil, db.lib.lastError()
	}
	defer db.lib.customWordListDestroy(handle)

	count := db.lib.customWordListLen(handle)
	items := make([]CustomWordEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		word, err := db.lib.takeOwnedString(func() *byte {
			return db.lib.customWordListGetWord(handle, i)
		})
		if err != nil {
			return nil, err
		}
		items = append(items, CustomWordEntry{
			Word:   word,
			Weight: db.lib.customWordListGetWeight(handle, i),
		})
	}
	return items, nil
}

// EnsureFtsIndex 确保指定 FTS 索引存在。
// EnsureFtsIndex ensures that the specified FTS index exists.
func (db *Database) EnsureFtsIndex(indexName string, mode TokenizerMode) (EnsureFtsIndexResult, error) {
	ptr, keep := makeCString(indexName)
	defer keep()
	var pod ensureFtsIndexResultPod
	status := StatusCode(db.lib.databaseEnsureFtsIndex(db.handle, ptr, mode, &pod))
	if status != StatusSuccess {
		return EnsureFtsIndexResult{}, db.lib.lastError()
	}
	return EnsureFtsIndexResult{
		Success:       pod.Success != 0,
		TokenizerMode: TokenizerMode(pod.TokenizerMode),
	}, nil
}

// RebuildFtsIndex 使用当前词典重建 FTS 索引。
// RebuildFtsIndex rebuilds the FTS index with the current dictionary.
func (db *Database) RebuildFtsIndex(indexName string, mode TokenizerMode) (RebuildFtsIndexResult, error) {
	ptr, keep := makeCString(indexName)
	defer keep()
	var pod rebuildFtsIndexResultPod
	status := StatusCode(db.lib.databaseRebuildFtsIndex(db.handle, ptr, mode, &pod))
	if status != StatusSuccess {
		return RebuildFtsIndexResult{}, db.lib.lastError()
	}
	return RebuildFtsIndexResult{
		Success:       pod.Success != 0,
		TokenizerMode: TokenizerMode(pod.TokenizerMode),
		ReindexedRows: pod.ReindexedRows,
	}, nil
}

// UpsertFtsDocument 写入或更新 FTS 文档。
// UpsertFtsDocument inserts or updates an FTS document.
func (db *Database) UpsertFtsDocument(indexName string, mode TokenizerMode, id string, filePath string, title string, content string) (FtsMutationResult, error) {
	indexPtr, keepIndex := makeCString(indexName)
	defer keepIndex()
	idPtr, keepID := makeCString(id)
	defer keepID()
	filePathPtr, keepFilePath := makeCString(filePath)
	defer keepFilePath()
	titlePtr, keepTitle := makeCString(title)
	defer keepTitle()
	contentPtr, keepContent := makeCString(content)
	defer keepContent()

	var pod ftsMutationResultPod
	status := StatusCode(db.lib.databaseUpsertFtsDocument(
		db.handle,
		indexPtr,
		mode,
		idPtr,
		filePathPtr,
		titlePtr,
		contentPtr,
		&pod,
	))
	if status != StatusSuccess {
		return FtsMutationResult{}, db.lib.lastError()
	}
	return FtsMutationResult{
		Success:      pod.Success != 0,
		AffectedRows: pod.AffectedRows,
	}, nil
}

// DeleteFtsDocument 删除 FTS 文档。
// DeleteFtsDocument deletes an FTS document.
func (db *Database) DeleteFtsDocument(indexName string, id string) (FtsMutationResult, error) {
	indexPtr, keepIndex := makeCString(indexName)
	defer keepIndex()
	idPtr, keepID := makeCString(id)
	defer keepID()

	var pod ftsMutationResultPod
	status := StatusCode(db.lib.databaseDeleteFtsDocument(db.handle, indexPtr, idPtr, &pod))
	if status != StatusSuccess {
		return FtsMutationResult{}, db.lib.lastError()
	}
	return FtsMutationResult{
		Success:      pod.Success != 0,
		AffectedRows: pod.AffectedRows,
	}, nil
}

// SearchFts 执行标准化 BM25 检索。
// SearchFts executes normalized BM25 search.
func (db *Database) SearchFts(indexName string, mode TokenizerMode, query string, limit uint32, offset uint32) (SearchResult, error) {
	indexPtr, keepIndex := makeCString(indexName)
	defer keepIndex()
	queryPtr, keepQuery := makeCString(query)
	defer keepQuery()

	handle := db.lib.databaseSearchFts(db.handle, indexPtr, mode, queryPtr, limit, offset)
	if handle == nil {
		return SearchResult{}, db.lib.lastError()
	}
	defer db.lib.searchResultDestroy(handle)

	source, err := db.lib.takeOwnedString(func() *byte {
		return db.lib.searchResultSource(handle)
	})
	if err != nil {
		return SearchResult{}, err
	}
	queryMode, err := db.lib.takeOwnedString(func() *byte {
		return db.lib.searchResultQueryMode(handle)
	})
	if err != nil {
		return SearchResult{}, err
	}
	total := db.lib.searchResultTotal(handle)
	length := db.lib.searchResultLen(handle)
	hits := make([]SearchHit, 0, length)
	for i := uint64(0); i < length; i++ {
		id, err := db.lib.takeOwnedString(func() *byte { return db.lib.searchResultGetID(handle, i) })
		if err != nil {
			return SearchResult{}, err
		}
		filePath, err := db.lib.takeOwnedString(func() *byte { return db.lib.searchResultGetFilePath(handle, i) })
		if err != nil {
			return SearchResult{}, err
		}
		title, err := db.lib.takeOwnedString(func() *byte { return db.lib.searchResultGetTitle(handle, i) })
		if err != nil {
			return SearchResult{}, err
		}
		titleHighlight, err := db.lib.takeOwnedString(func() *byte { return db.lib.searchResultGetTitleHighlight(handle, i) })
		if err != nil {
			return SearchResult{}, err
		}
		contentSnippet, err := db.lib.takeOwnedString(func() *byte { return db.lib.searchResultGetContentSnippet(handle, i) })
		if err != nil {
			return SearchResult{}, err
		}
		hits = append(hits, SearchHit{
			ID:             id,
			FilePath:       filePath,
			Title:          title,
			TitleHighlight: titleHighlight,
			ContentSnippet: contentSnippet,
			Score:          sanitizeFiniteFloat(db.lib.searchResultGetScore(handle, i)),
			Rank:           db.lib.searchResultGetRank(handle, i),
			RawScore:       sanitizeFiniteFloat(db.lib.searchResultGetRawScore(handle, i)),
		})
	}

	return SearchResult{
		Total:     total,
		Source:    source,
		QueryMode: queryMode,
		Hits:      hits,
	}, nil
}

// TokenizeResult 表示 Go 侧的分词结果。
// TokenizeResult represents the tokenize result on the Go side.
type TokenizeResult struct {
	// NormalizedText 表示规范化后的原文。
	// NormalizedText is the normalized source text.
	NormalizedText string
	// FtsQuery 表示最终 FTS MATCH 表达式。
	// FtsQuery is the final FTS MATCH expression.
	FtsQuery string
	// Tokens 表示词元列表。
	// Tokens contains the token list.
	Tokens []string
}

// CustomWordEntry 表示 Go 侧自定义词条目。
// CustomWordEntry represents a custom-word entry on the Go side.
type CustomWordEntry struct {
	// Word 表示词文本。
	// Word is the custom-word text.
	Word string
	// Weight 表示词权重。
	// Weight is the custom-word weight.
	Weight uint64
}

// takeOwnedString 调用返回 char* 的函数并自动释放返回值。
// takeOwnedString calls a function that returns char* and frees the returned value automatically.
func (lib *Library) takeOwnedString(getter func() *byte) (string, error) {
	ptr := getter()
	if ptr == nil {
		return "", lib.lastError()
	}
	defer lib.stringFree(ptr)
	return readCString(ptr), nil
}

// lastError 读取最近一次错误信息。
// lastError reads the latest error message.
func (lib *Library) lastError() error {
	if lib == nil {
		return errors.New("library is nil / library 不能为空")
	}
	ptr := lib.lastErrorMessage()
	if ptr == nil {
		return errors.New("ffi call failed without error message / FFI 调用失败但未返回错误消息")
	}
	return errors.New(readCString(ptr))
}

// makeCString 创建临时 C 风格字符串缓冲区。
// makeCString creates a temporary C-style string buffer.
func makeCString(value string) (*byte, func()) {
	buffer := append([]byte(value), 0)
	return &buffer[0], func() {
		runtime.KeepAlive(buffer)
	}
}

// readCString 读取 NUL 结尾字符串。
// readCString reads a NUL-terminated string.
func readCString(ptr *byte) string {
	if ptr == nil {
		return ""
	}
	base := uintptr(unsafe.Pointer(ptr))
	length := 0
	for {
		value := *(*byte)(unsafe.Pointer(base + uintptr(length)))
		if value == 0 {
			break
		}
		length++
	}
	return string(unsafe.Slice(ptr, length))
}

// boolToUint8 将布尔值转成 FFI 所需的 0/1。
// boolToUint8 converts a boolean to the 0/1 FFI form.
func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

// sanitizeFiniteFloat 把 NaN/Inf 收敛成 0，避免上层继续传播异常值。
// sanitizeFiniteFloat collapses NaN/Inf to 0 to avoid propagating invalid values upstream.
func sanitizeFiniteFloat(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}
