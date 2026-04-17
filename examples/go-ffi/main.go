package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"vldb-sqlite-go-ffi-demo/sqliteffi"
)

// main 演示如何通过 Go + 非 JSON FFI 主接口使用 vldb-sqlite。
// main demonstrates how to use vldb-sqlite from Go through the non-JSON main FFI interface.
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("用法: go run . <dynamic-library-path> / usage: go run . <dynamic-library-path>")
	}

	libraryPath := os.Args[1]
	lib, err := sqliteffi.Open(libraryPath)
	if err != nil {
		log.Fatalf("加载动态库失败 / failed to load dynamic library: %v", err)
	}
	defer func() {
		if closeErr := lib.Close(); closeErr != nil {
			log.Printf("关闭动态库失败 / failed to close dynamic library: %v", closeErr)
		}
	}()

	runtimeHandle, err := lib.CreateRuntime()
	if err != nil {
		log.Fatalf("创建 runtime 失败 / failed to create runtime: %v", err)
	}
	defer func() {
		if closeErr := runtimeHandle.Close(); closeErr != nil {
			log.Printf("关闭 runtime 失败 / failed to close runtime: %v", closeErr)
		}
	}()

	dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("vldb-sqlite-go-demo-%d.sqlite3", time.Now().UnixNano()))
	db, err := runtimeHandle.OpenDatabase(dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败 / failed to open database: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("关闭数据库句柄失败 / failed to close database handle: %v", closeErr)
		}
	}()

	log.Printf("数据库路径 / database path: %s", dbPath)

	executeResult, err := db.ExecuteScript(
		"CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, body TEXT, score REAL)",
		nil,
		"",
	)
	if err != nil {
		log.Fatalf("建表失败 / failed to create table: %v", err)
	}
	log.Printf("建表结果 / execute result: %+v", executeResult)

	batchResult, err := db.ExecuteBatch(
		"INSERT INTO notes(title, body, score) VALUES (?1, ?2, ?3)",
		[][]sqliteffi.SQLValue{
			{
				{Kind: sqliteffi.SQLValueString, String: "第一条"},
				{Kind: sqliteffi.SQLValueString, String: "市民田-女士急匆匆地赶往会议室"},
				{Kind: sqliteffi.SQLValueFloat64, Float64: 7.5},
			},
			{
				{Kind: sqliteffi.SQLValueString, String: "第二条"},
				{Kind: sqliteffi.SQLValueString, String: "今天的议题围绕代码优化与交付效率展开"},
				{Kind: sqliteffi.SQLValueFloat64, Float64: 9.5},
			},
		},
	)
	if err != nil {
		log.Fatalf("批量写入失败 / failed to execute batch: %v", err)
	}
	log.Printf("批量执行结果 / batch result: %+v", batchResult)

	queryJSONResult, err := db.QueryJSON(
		"SELECT id, title, body, score FROM notes WHERE score >= ?1 ORDER BY id",
		[]sqliteffi.SQLValue{{Kind: sqliteffi.SQLValueFloat64, Float64: 7.5}},
		"",
	)
	if err != nil {
		log.Fatalf("JSON 查询失败 / failed to execute query json: %v", err)
	}
	log.Printf("JSON 查询结果 / query json result: rows=%d payload=%s", queryJSONResult.RowCount, queryJSONResult.JSONData)

	mutation, err := db.UpsertCustomWord("田-女士", 64)
	if err != nil {
		log.Fatalf("写入专有词失败 / failed to upsert custom word: %v", err)
	}
	log.Printf("专有词写入结果 / custom word result: %+v", mutation)

	tokenized, err := db.Tokenize(sqliteffi.TokenizerJieba, "市民田-女士急匆匆", false)
	if err != nil {
		log.Fatalf("分词失败 / failed to tokenize text: %v", err)
	}
	log.Printf("分词结果 / tokenize result: normalized=%q query=%q tokens=%v", tokenized.NormalizedText, tokenized.FtsQuery, tokenized.Tokens)

	ensured, err := db.EnsureFtsIndex("memory_docs", sqliteffi.TokenizerJieba)
	if err != nil {
		log.Fatalf("确保索引失败 / failed to ensure index: %v", err)
	}
	log.Printf("索引结果 / ensure index result: %+v", ensured)

	if _, err = db.UpsertFtsDocument(
		"memory_docs",
		sqliteffi.TokenizerJieba,
		"doc-1",
		"/demo/a.md",
		"测试标题",
		"市民田-女士急匆匆地赶往会议室",
	); err != nil {
		log.Fatalf("写入文档失败 / failed to upsert document: %v", err)
	}

	if _, err = db.UpsertFtsDocument(
		"memory_docs",
		sqliteffi.TokenizerJieba,
		"doc-2",
		"/demo/b.md",
		"另一条记录",
		"今天的议题围绕代码优化与交付效率展开",
	); err != nil {
		log.Fatalf("写入第二条文档失败 / failed to upsert second document: %v", err)
	}

	searchResult, err := db.SearchFts("memory_docs", sqliteffi.TokenizerJieba, "田-女士", 10, 0)
	if err != nil {
		log.Fatalf("检索失败 / failed to search fts: %v", err)
	}
	log.Printf("检索总数 / total hits: %d, source=%s, query_mode=%s", searchResult.Total, searchResult.Source, searchResult.QueryMode)
	for _, hit := range searchResult.Hits {
		log.Printf(
			"命中 / hit: id=%s file=%s score=%.4f rank=%d raw=%.4f title=%q snippet=%q",
			hit.ID,
			hit.FilePath,
			hit.Score,
			hit.Rank,
			hit.RawScore,
			hit.TitleHighlight,
			hit.ContentSnippet,
		)
	}

	streamHandle, err := db.QueryStream(
		"SELECT id, title, score FROM notes ORDER BY id",
		nil,
		"",
		0,
	)
	if err != nil {
		log.Fatalf("流式查询失败 / failed to execute query stream: %v", err)
	}
	defer func() {
		if closeErr := streamHandle.Close(); closeErr != nil {
			log.Printf("关闭流式查询句柄失败 / failed to close query stream handle: %v", closeErr)
		}
	}()
	log.Printf(
		"流式查询结果 / query stream result: rows=%d chunks=%d total_bytes=%d",
		streamHandle.RowCount,
		streamHandle.ChunkCount,
		streamHandle.TotalBytes,
	)
	for i := uint64(0); i < streamHandle.ChunkCount; i++ {
		chunk, chunkErr := streamHandle.ReadChunk(i)
		if chunkErr != nil {
			log.Fatalf("读取流式查询 chunk 失败 / failed to read query stream chunk: %v", chunkErr)
		}
		log.Printf("流式查询 chunk / query stream chunk: index=%d bytes=%d", i, len(chunk))
	}
}
