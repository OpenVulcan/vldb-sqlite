use crate::config::{BoxError, Config};
use crate::fts::{
    delete_fts_document, ensure_fts_index, rebuild_fts_index, search_fts, upsert_fts_document,
};
use crate::logging::ServiceLogger;
use crate::pb::sqlite_service_server::SqliteService;
use crate::pb::sqlite_value::Kind as ProtoSqliteValueKind;
use crate::pb::{
    DeleteFtsDocumentRequest, DictionaryMutationResponse, EnsureFtsIndexRequest,
    EnsureFtsIndexResponse, ExecuteBatchItem, ExecuteBatchRequest, ExecuteBatchResponse,
    ExecuteRequest, ExecuteResponse, FtsMutationResponse, ListCustomWordsRequest,
    ListCustomWordsResponse, QueryJsonResponse, QueryRequest, QueryResponse,
    RebuildFtsIndexRequest, RebuildFtsIndexResponse, RemoveCustomWordRequest,
    SearchFtsHit as ProtoSearchFtsHit, SearchFtsRequest, SearchFtsResponse,
    SqliteValue as ProtoSqliteValue, TokenizeTextRequest, TokenizeTextResponse,
    TokenizerMode as ProtoTokenizerMode, UpsertCustomWordRequest, UpsertFtsDocumentRequest,
};
use crate::runtime::{
    SqliteHardeningOptions, SqliteOpenOptions, SqlitePragmaOptions, apply_sqlite_connection_pragmas,
    build_sqlite_open_flags, open_sqlite_connection,
};
use crate::tokenizer::{
    TokenizerMode, list_custom_words, remove_custom_word, tokenize_text, upsert_custom_word,
};
use arrow::array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use rusqlite::ffi::ErrorCode as SqliteErrorCode;
use rusqlite::types::{ToSql, Value as SqliteValue, ValueRef as SqliteValueRef};
use rusqlite::{Connection, Error as RusqliteError, InterruptHandle, OpenFlags};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::{Code, Request, Response, Status};

const STREAM_CHANNEL_CAPACITY: usize = 8;
const DEFAULT_IPC_CHUNK_BYTES: usize = 1024 * 1024;
const RETRYABLE_METADATA_KEY: &str = "x-vldb-retryable";
const SQLITE_CODE_METADATA_KEY: &str = "x-vldb-sqlite-code";
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct AppState {
    connection_pool: Arc<SqliteConnectionPool>,
    logger: Arc<ServiceLogger>,
}

#[derive(Debug)]
pub(crate) struct SqliteConnectionPool {
    idle_connections: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
}

#[derive(Debug)]
struct ConnectionLease {
    pool: Arc<SqliteConnectionPool>,
    connection: Option<Connection>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Debug)]
pub struct SqliteGrpcService {
    state: Arc<AppState>,
}

#[derive(Clone, Debug)]
struct RequestLogContext {
    logger: Arc<ServiceLogger>,
    progress: Arc<RequestProgress>,
    request_id: u64,
    operation: &'static str,
    remote_addr: Option<SocketAddr>,
    grpc_timeout: Option<Duration>,
    started_at: Instant,
    sql_full: String,
    sql_preview: String,
    param_count: usize,
    params_json_bytes: usize,
    request_log_enabled: bool,
    log_sql: bool,
    sql_masking: bool,
    slow_query_log_enabled: bool,
    slow_query_threshold: Duration,
    slow_query_full_sql_enabled: bool,
}

#[derive(Debug)]
struct RequestProgress {
    stage: Mutex<&'static str>,
}

impl RequestProgress {
    fn new(initial_stage: &'static str) -> Self {
        Self {
            stage: Mutex::new(initial_stage),
        }
    }

    fn set(&self, stage: &'static str) {
        if let Ok(mut guard) = self.stage.lock() {
            *guard = stage;
        }
    }

    fn snapshot(&self) -> &'static str {
        self.stage.lock().map(|guard| *guard).unwrap_or("unknown")
    }
}

struct WorkerCompletionSignal(Option<oneshot::Sender<()>>);

impl WorkerCompletionSignal {
    fn new(tx: oneshot::Sender<()>) -> Self {
        Self(Some(tx))
    }
}

impl Drop for WorkerCompletionSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ArrowColumnKind {
    Int64,
    Float64,
    Utf8,
    Binary,
}

#[derive(Debug)]
enum RequestFailure {
    Status(Status),
    Sqlite {
        prefix: &'static str,
        error: RusqliteError,
    },
}

impl RequestFailure {
    fn sqlite(prefix: &'static str, error: RusqliteError) -> Self {
        Self::Sqlite { prefix, error }
    }
}

#[derive(Copy, Clone, Debug)]
struct StreamMetrics {
    emitted_chunks: usize,
    emitted_bytes: usize,
}

impl SqliteConnectionPool {
    fn new(config: &Config) -> Result<Self, BoxError> {
        let max_size = effective_connection_pool_size(config);
        let mut idle_connections = Vec::with_capacity(max_size);
        for _ in 0..max_size {
            idle_connections.push(open_connection(config)?);
        }

        Ok(Self {
            idle_connections: Mutex::new(idle_connections),
            permits: Arc::new(Semaphore::new(max_size)),
        })
    }

    fn checkout(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Result<ConnectionLease, Status> {
        let mut guard = self
            .idle_connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let connection = guard.pop().ok_or_else(|| {
            Status::internal("sqlite connection pool is empty despite an acquired permit")
        })?;

        Ok(ConnectionLease {
            pool: Arc::clone(self),
            connection: Some(connection),
            _permit: permit,
        })
    }

    fn return_connection(&self, connection: Connection) {
        let mut guard = self
            .idle_connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(connection);
    }
}

impl ConnectionLease {
    fn connection_mut(&mut self) -> &mut Connection {
        self.connection
            .as_mut()
            .expect("pooled sqlite connection should always be present while leased")
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if !connection.is_autocommit() {
                let _ = connection.execute_batch("ROLLBACK");
            }
            self.pool.return_connection(connection);
        }
    }
}

impl SqliteGrpcService {
    pub(crate) fn new(
        connection_pool: SqliteConnectionPool,
        logger: Arc<ServiceLogger>,
        _config: Config,
    ) -> Self {
        Self {
            state: Arc::new(AppState {
                connection_pool: Arc::new(connection_pool),
                logger,
            }),
        }
    }
}

#[tonic::async_trait]
impl SqliteService for SqliteGrpcService {
    async fn execute_script(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "execute_script",
            request.get_ref().sql.as_str(),
            request.get_ref().params.len(),
            request.get_ref().params_json.as_str(),
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            log_request_invalid_argument(&context, "sql must not be empty");
            return Err(Status::invalid_argument("sql must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let deadline_triggered = Arc::new(AtomicBool::new(false));
        let (interrupt_tx, interrupt_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        spawn_deadline_interrupt_watcher(
            context.clone(),
            interrupt_rx,
            done_rx,
            Arc::clone(&deadline_triggered),
        );

        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            let _completion = WorkerCompletionSignal::new(done_tx);
            run_execute_script(
                worker_context,
                lease,
                req.sql,
                req.params,
                req.params_json,
                Some(interrupt_tx),
            )
        })
        .await
        .map_err(|err| Status::internal(format!("execute worker join failed: {err}")))?;
        let response = remap_deadline_status_if_needed(response, &deadline_triggered)?;

        Ok(response_with_default_metadata(response))
    }

    async fn execute_batch(
        &self,
        request: Request<ExecuteBatchRequest>,
    ) -> Result<Response<ExecuteBatchResponse>, Status> {
        let batch_param_count = request
            .get_ref()
            .items
            .iter()
            .map(|item| item.params.len())
            .sum();
        let context = build_request_context(
            &self.state,
            &request,
            "execute_batch",
            request.get_ref().sql.as_str(),
            batch_param_count,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            log_request_invalid_argument(&context, "sql must not be empty");
            return Err(Status::invalid_argument("sql must not be empty"));
        }
        if req.items.is_empty() {
            log_request_invalid_argument(&context, "items must not be empty");
            return Err(Status::invalid_argument("items must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let deadline_triggered = Arc::new(AtomicBool::new(false));
        let (interrupt_tx, interrupt_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        spawn_deadline_interrupt_watcher(
            context.clone(),
            interrupt_rx,
            done_rx,
            Arc::clone(&deadline_triggered),
        );

        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            let _completion = WorkerCompletionSignal::new(done_tx);
            run_execute_batch(
                worker_context,
                lease,
                req.sql,
                req.items,
                Some(interrupt_tx),
            )
        })
        .await
        .map_err(|err| Status::internal(format!("execute_batch worker join failed: {err}")))?;
        let response = remap_deadline_status_if_needed(response, &deadline_triggered)?;

        Ok(response_with_default_metadata(response))
    }

    type QueryStreamStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query_stream(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "query_stream",
            request.get_ref().sql.as_str(),
            request.get_ref().params.len(),
            request.get_ref().params_json.as_str(),
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            log_request_invalid_argument(&context, "sql must not be empty");
            return Err(Status::invalid_argument("sql must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let worker_tx = tx.clone();
        let join_tx = tx.clone();
        let deadline_triggered = Arc::new(AtomicBool::new(false));
        let (interrupt_tx, interrupt_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        spawn_deadline_interrupt_watcher(
            context.clone(),
            interrupt_rx,
            done_rx,
            Arc::clone(&deadline_triggered),
        );

        let worker_context = context.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _completion = WorkerCompletionSignal::new(done_tx);
            run_query_streaming(
                worker_context,
                lease,
                req.sql,
                req.params,
                req.params_json,
                worker_tx,
                Some(interrupt_tx),
            )
        });

        let join_context = context.clone();
        let join_deadline_triggered = Arc::clone(&deadline_triggered);
        tokio::spawn(async move {
            match worker.await {
                Ok(Ok(())) => {}
                Ok(Err(status)) => {
                    let mapped = remap_deadline_status(status, &join_deadline_triggered);
                    let _ = join_tx.send(Err(mapped)).await;
                }
                Err(err) => {
                    let status = Status::internal(format!("query worker join failed: {err}"));
                    log_request_failed(&join_context, &status);
                    let _ = join_tx.send(Err(status)).await;
                }
            }
        });

        drop(tx);

        Ok(response_with_default_metadata(ReceiverStream::new(rx)))
    }

    async fn query_json(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryJsonResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "query_json",
            request.get_ref().sql.as_str(),
            request.get_ref().params.len(),
            request.get_ref().params_json.as_str(),
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.sql.trim().is_empty() {
            log_request_invalid_argument(&context, "sql must not be empty");
            return Err(Status::invalid_argument("sql must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let deadline_triggered = Arc::new(AtomicBool::new(false));
        let (interrupt_tx, interrupt_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        spawn_deadline_interrupt_watcher(
            context.clone(),
            interrupt_rx,
            done_rx,
            Arc::clone(&deadline_triggered),
        );

        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            let _completion = WorkerCompletionSignal::new(done_tx);
            run_query_json(
                worker_context,
                lease,
                req.sql,
                req.params,
                req.params_json,
                Some(interrupt_tx),
            )
        })
        .await
        .map_err(|err| Status::internal(format!("query_json worker join failed: {err}")))?;
        let response = remap_deadline_status_if_needed(response, &deadline_triggered)?;

        Ok(response_with_default_metadata(response))
    }

    async fn tokenize_text(
        &self,
        request: Request<TokenizeTextRequest>,
    ) -> Result<Response<TokenizeTextResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "tokenize_text",
            request.get_ref().text.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.text.trim().is_empty() {
            log_request_invalid_argument(&context, "text must not be empty");
            return Err(Status::invalid_argument("text must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_tokenize_text(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("tokenize_text worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn upsert_custom_word(
        &self,
        request: Request<UpsertCustomWordRequest>,
    ) -> Result<Response<DictionaryMutationResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "upsert_custom_word",
            request.get_ref().word.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.word.trim().is_empty() {
            log_request_invalid_argument(&context, "word must not be empty");
            return Err(Status::invalid_argument("word must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_upsert_custom_word(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("upsert_custom_word worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn remove_custom_word(
        &self,
        request: Request<RemoveCustomWordRequest>,
    ) -> Result<Response<DictionaryMutationResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "remove_custom_word",
            request.get_ref().word.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.word.trim().is_empty() {
            log_request_invalid_argument(&context, "word must not be empty");
            return Err(Status::invalid_argument("word must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_remove_custom_word(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("remove_custom_word worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn list_custom_words(
        &self,
        request: Request<ListCustomWordsRequest>,
    ) -> Result<Response<ListCustomWordsResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "list_custom_words",
            "_vulcan_dict",
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_list_custom_words(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("list_custom_words worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn ensure_fts_index(
        &self,
        request: Request<EnsureFtsIndexRequest>,
    ) -> Result<Response<EnsureFtsIndexResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "ensure_fts_index",
            request.get_ref().index_name.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.index_name.trim().is_empty() {
            log_request_invalid_argument(&context, "index_name must not be empty");
            return Err(Status::invalid_argument("index_name must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_ensure_fts_index(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("ensure_fts_index worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn rebuild_fts_index(
        &self,
        request: Request<RebuildFtsIndexRequest>,
    ) -> Result<Response<RebuildFtsIndexResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "rebuild_fts_index",
            request.get_ref().index_name.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.index_name.trim().is_empty() {
            log_request_invalid_argument(&context, "index_name must not be empty");
            return Err(Status::invalid_argument("index_name must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_rebuild_fts_index(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("rebuild_fts_index worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn upsert_fts_document(
        &self,
        request: Request<UpsertFtsDocumentRequest>,
    ) -> Result<Response<FtsMutationResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "upsert_fts_document",
            request.get_ref().id.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.index_name.trim().is_empty() {
            log_request_invalid_argument(&context, "index_name must not be empty");
            return Err(Status::invalid_argument("index_name must not be empty"));
        }
        if req.id.trim().is_empty() {
            log_request_invalid_argument(&context, "id must not be empty");
            return Err(Status::invalid_argument("id must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_upsert_fts_document(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("upsert_fts_document worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn delete_fts_document(
        &self,
        request: Request<DeleteFtsDocumentRequest>,
    ) -> Result<Response<FtsMutationResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "delete_fts_document",
            request.get_ref().id.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.index_name.trim().is_empty() {
            log_request_invalid_argument(&context, "index_name must not be empty");
            return Err(Status::invalid_argument("index_name must not be empty"));
        }
        if req.id.trim().is_empty() {
            log_request_invalid_argument(&context, "id must not be empty");
            return Err(Status::invalid_argument("id must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_delete_fts_document(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("delete_fts_document worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }

    async fn search_fts(
        &self,
        request: Request<SearchFtsRequest>,
    ) -> Result<Response<SearchFtsResponse>, Status> {
        let context = build_request_context(
            &self.state,
            &request,
            "search_fts",
            request.get_ref().query.as_str(),
            0,
            "",
        );
        log_request_started(&context);

        let req = request.into_inner();
        if req.index_name.trim().is_empty() {
            log_request_invalid_argument(&context, "index_name must not be empty");
            return Err(Status::invalid_argument("index_name must not be empty"));
        }
        if req.query.trim().is_empty() {
            log_request_invalid_argument(&context, "query must not be empty");
            return Err(Status::invalid_argument("query must not be empty"));
        }

        let lease = acquire_connection_lease(&context, &self.state)
            .await
            .inspect_err(|status| log_request_failed(&context, status))?;
        let worker_context = context.clone();
        let response = tokio::task::spawn_blocking(move || {
            run_search_fts(worker_context, lease, req)
        })
        .await
        .map_err(|err| Status::internal(format!("search_fts worker join failed: {err}")))??;

        Ok(response_with_default_metadata(response))
    }
}

pub(crate) fn effective_connection_pool_size(config: &Config) -> usize {
    if config.db_path == ":memory:" {
        1
    } else {
        config.connection_pool_size
    }
}

pub(crate) fn open_connection_pool(config: &Config) -> Result<SqliteConnectionPool, BoxError> {
    SqliteConnectionPool::new(config)
}

pub fn open_connection(config: &Config) -> Result<Connection, BoxError> {
    open_sqlite_connection(&config.db_path, &runtime_options_from_config(config))
}

#[allow(dead_code)]
pub fn apply_connection_pragmas(conn: &Connection, config: &Config) -> Result<(), BoxError> {
    apply_sqlite_connection_pragmas(conn, &config.db_path, &runtime_options_from_config(config))
}

#[allow(dead_code)]
fn build_open_flags(config: &Config) -> OpenFlags {
    build_sqlite_open_flags(&runtime_options_from_config(config))
}

fn runtime_options_from_config(config: &Config) -> SqliteOpenOptions {
    SqliteOpenOptions {
        connection_pool_size: config.connection_pool_size,
        busy_timeout_ms: config.busy_timeout_ms,
        pragmas: SqlitePragmaOptions {
            journal_mode: config.pragmas.journal_mode.clone(),
            synchronous: config.pragmas.synchronous.clone(),
            foreign_keys: config.pragmas.foreign_keys,
            temp_store: config.pragmas.temp_store.clone(),
            wal_autocheckpoint_pages: config.pragmas.wal_autocheckpoint_pages,
            cache_size_kib: config.pragmas.cache_size_kib,
            mmap_size_bytes: config.pragmas.mmap_size_bytes,
        },
        hardening: SqliteHardeningOptions {
            enforce_db_file_lock: config.hardening.enforce_db_file_lock,
            read_only: config.hardening.read_only,
            allow_uri_filenames: config.hardening.allow_uri_filenames,
            trusted_schema: config.hardening.trusted_schema,
            defensive: config.hardening.defensive,
        },
    }
}

async fn acquire_connection_lease(
    context: &RequestLogContext,
    state: &Arc<AppState>,
) -> Result<ConnectionLease, Status> {
    set_request_stage(context, "waiting_for_connection");
    let acquire = Arc::clone(&state.connection_pool.permits).acquire_owned();

    let permit = if let Some(grpc_timeout) = context.grpc_timeout {
        let deadline = tokio::time::Instant::from_std(context.started_at + grpc_timeout);
        match tokio::time::timeout_at(deadline, acquire).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(Status::internal("sqlite connection pool is closed")),
            Err(_) => {
                log_request_timeout(context);
                return Err(Status::deadline_exceeded(
                    "SQLite request exceeded the gRPC deadline while waiting for a pooled SQLite connection",
                ));
            }
        }
    } else {
        acquire
            .await
            .map_err(|_| Status::internal("sqlite connection pool is closed"))?
    };

    set_request_stage(context, "checking_out_connection");
    state.connection_pool.checkout(permit)
}

fn run_execute_script(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    sql: String,
    params: Vec<ProtoSqliteValue>,
    params_json: String,
    interrupt_tx: Option<oneshot::Sender<InterruptHandle>>,
) -> Result<ExecuteResponse, Status> {
    let conn = lease.connection_mut();
    if let Some(tx) = interrupt_tx {
        let _ = tx.send(conn.get_interrupt_handle());
    }

    let result = (|| -> Result<ExecuteResponse, RequestFailure> {
        set_request_stage(&context, "parsing_params");
        let bound_values =
            parse_request_params(&params, &params_json).map_err(RequestFailure::Status)?;

        if bound_values.is_empty() {
            set_request_stage(&context, "executing_batch");
            conn.execute_batch(&sql)
                .map_err(|err| RequestFailure::sqlite("sqlite execute_batch failed", err))?;

            let rows_changed = i64::try_from(conn.changes()).unwrap_or(i64::MAX);
            return Ok(ExecuteResponse {
                success: true,
                message: "script executed successfully".to_string(),
                rows_changed,
                last_insert_rowid: conn.last_insert_rowid(),
            });
        }

        if has_multiple_sql_statements(&sql) {
            return Err(RequestFailure::Status(Status::invalid_argument(
                "flat params or params_json are only supported for a single SQL statement",
            )));
        }

        set_request_stage(&context, "preparing_statement");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| RequestFailure::sqlite("sqlite prepare failed", err))?;
        let params = bind_values_as_params(&bound_values);
        set_request_stage(&context, "executing_statement");
        let rows_changed = stmt
            .execute(params.as_slice())
            .map_err(|err| RequestFailure::sqlite("sqlite execute failed", err))?;

        Ok(ExecuteResponse {
            success: true,
            message: format!("statement executed successfully (rows_changed={rows_changed})"),
            rows_changed: i64::try_from(rows_changed).unwrap_or(i64::MAX),
            last_insert_rowid: conn.last_insert_rowid(),
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(&context, response.message.as_str()),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_execute_batch(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    sql: String,
    items: Vec<ExecuteBatchItem>,
    interrupt_tx: Option<oneshot::Sender<InterruptHandle>>,
) -> Result<ExecuteBatchResponse, Status> {
    let conn = lease.connection_mut();
    if let Some(tx) = interrupt_tx {
        let _ = tx.send(conn.get_interrupt_handle());
    }

    let result = (|| -> Result<ExecuteBatchResponse, RequestFailure> {
        if has_multiple_sql_statements(&sql) {
            return Err(RequestFailure::Status(Status::invalid_argument(
                "execute_batch only supports a single SQL statement",
            )));
        }

        set_request_stage(&context, "parsing_batch_params");
        let batch_params = parse_batch_params(&items).map_err(RequestFailure::Status)?;
        execute_prepared_batch(conn, &context, &sql, &batch_params)
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(&context, response.message.as_str()),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_query_json(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    sql: String,
    params: Vec<ProtoSqliteValue>,
    params_json: String,
    interrupt_tx: Option<oneshot::Sender<InterruptHandle>>,
) -> Result<QueryJsonResponse, Status> {
    let conn = lease.connection_mut();
    if let Some(tx) = interrupt_tx {
        let _ = tx.send(conn.get_interrupt_handle());
    }

    let result = (|| -> Result<QueryJsonResponse, RequestFailure> {
        set_request_stage(&context, "parsing_params");
        let bound_values =
            parse_request_params(&params, &params_json).map_err(RequestFailure::Status)?;
        if has_multiple_sql_statements(&sql) {
            return Err(RequestFailure::Status(Status::invalid_argument(
                "query_json only supports a single SQL statement",
            )));
        }

        set_request_stage(&context, "preparing_statement");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| RequestFailure::sqlite("sqlite prepare failed", err))?;
        let column_names = stmt
            .column_names()
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        let params = bind_values_as_params(&bound_values);
        set_request_stage(&context, "executing_query");
        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|err| RequestFailure::sqlite("sqlite query failed", err))?;

        let mut json_rows = Vec::<JsonValue>::new();
        set_request_stage(&context, "fetching_rows");
        while let Some(row) = rows
            .next()
            .map_err(|err| RequestFailure::sqlite("sqlite row fetch failed", err))?
        {
            let mut object = JsonMap::new();
            for (index, column_name) in column_names.iter().enumerate() {
                let value = row
                    .get_ref(index)
                    .map_err(|err| RequestFailure::sqlite("sqlite value access failed", err))?;
                object.insert(column_name.clone(), sqlite_value_ref_to_json(value));
            }
            json_rows.push(JsonValue::Object(object));
        }

        set_request_stage(&context, "serializing_json");
        let json_data = serde_json::to_string(&json_rows).map_err(|err| {
            RequestFailure::Status(status_internal("serialize JSON result failed", err))
        })?;

        Ok(QueryJsonResponse { json_data })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!("returned JSON payload ({} bytes)", response.json_data.len()),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_tokenize_text(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: TokenizeTextRequest,
) -> Result<TokenizeTextResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<TokenizeTextResponse, RequestFailure> {
        set_request_stage(&context, "tokenizing_text");
        let tokenizer_mode = tokenizer_mode_from_proto(request.tokenizer_mode)
            .map_err(RequestFailure::Status)?;
        let output = tokenize_text(
            Some(conn),
            tokenizer_mode,
            request.text.as_str(),
            request.search_mode,
        )
        .map_err(|err| RequestFailure::sqlite("sqlite tokenize failed", err))?;

        Ok(TokenizeTextResponse {
            tokenizer_mode: output.tokenizer_mode,
            normalized_text: output.normalized_text,
            tokens: output.tokens,
            fts_query: output.fts_query,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!(
                "tokenized {} terms with mode {}",
                response.tokens.len(),
                response.tokenizer_mode
            ),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_upsert_custom_word(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: UpsertCustomWordRequest,
) -> Result<DictionaryMutationResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<DictionaryMutationResponse, RequestFailure> {
        set_request_stage(&context, "upserting_custom_word");
        let output = upsert_custom_word(
            conn,
            request.word.as_str(),
            usize::try_from(request.weight.max(1)).unwrap_or(1),
        )
        .map_err(|err| RequestFailure::sqlite("sqlite upsert custom word failed", err))?;

        Ok(DictionaryMutationResponse {
            success: output.success,
            message: output.message,
            affected_rows: output.affected_rows,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!("custom word updated (affected_rows={})", response.affected_rows),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_remove_custom_word(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: RemoveCustomWordRequest,
) -> Result<DictionaryMutationResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<DictionaryMutationResponse, RequestFailure> {
        set_request_stage(&context, "removing_custom_word");
        let output = remove_custom_word(conn, request.word.as_str())
            .map_err(|err| RequestFailure::sqlite("sqlite remove custom word failed", err))?;

        Ok(DictionaryMutationResponse {
            success: output.success,
            message: output.message,
            affected_rows: output.affected_rows,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!("custom word removed (affected_rows={})", response.affected_rows),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

/// 执行列出自定义词请求，并返回结构化响应。
/// Execute the list-custom-words request and return a structured response.
fn run_list_custom_words(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    _request: ListCustomWordsRequest,
) -> Result<ListCustomWordsResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<ListCustomWordsResponse, RequestFailure> {
        set_request_stage(&context, "listing_custom_words");
        let output = list_custom_words(conn)
            .map_err(|err| RequestFailure::sqlite("sqlite list custom words failed", err))?;

        Ok(ListCustomWordsResponse {
            success: output.success,
            message: output.message,
            words: output
                .words
                .into_iter()
                .map(|entry| crate::pb::CustomWordItem {
                    word: entry.word,
                    weight: u64::try_from(entry.weight).unwrap_or(u64::MAX),
                })
                .collect(),
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!("custom words listed (count={})", response.words.len()),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_ensure_fts_index(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: EnsureFtsIndexRequest,
) -> Result<EnsureFtsIndexResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<EnsureFtsIndexResponse, RequestFailure> {
        set_request_stage(&context, "ensuring_fts_index");
        let tokenizer_mode =
            tokenizer_mode_from_proto(request.tokenizer_mode).map_err(RequestFailure::Status)?;
        let output = ensure_fts_index(conn, request.index_name.as_str(), tokenizer_mode)
            .map_err(|err| RequestFailure::sqlite("sqlite ensure fts index failed", err))?;

        Ok(EnsureFtsIndexResponse {
            success: output.success,
            message: output.message,
            index_name: output.index_name,
            tokenizer_mode: output.tokenizer_mode,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!("fts index ensured ({})", response.index_name),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

/// 执行重建 FTS 索引请求，并返回结构化响应。
/// Execute the rebuild-FTS-index request and return a structured response.
fn run_rebuild_fts_index(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: RebuildFtsIndexRequest,
) -> Result<RebuildFtsIndexResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<RebuildFtsIndexResponse, RequestFailure> {
        set_request_stage(&context, "rebuilding_fts_index");
        let tokenizer_mode =
            tokenizer_mode_from_proto(request.tokenizer_mode).map_err(RequestFailure::Status)?;
        let output = rebuild_fts_index(conn, request.index_name.as_str(), tokenizer_mode)
            .map_err(|err| RequestFailure::sqlite("sqlite rebuild fts index failed", err))?;

        Ok(RebuildFtsIndexResponse {
            success: output.success,
            message: output.message,
            index_name: output.index_name,
            tokenizer_mode: output.tokenizer_mode,
            reindexed_rows: output.reindexed_rows,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!(
                "fts index rebuilt (index={}, rows={})",
                response.index_name, response.reindexed_rows
            ),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_upsert_fts_document(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: UpsertFtsDocumentRequest,
) -> Result<FtsMutationResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<FtsMutationResponse, RequestFailure> {
        set_request_stage(&context, "upserting_fts_document");
        let tokenizer_mode =
            tokenizer_mode_from_proto(request.tokenizer_mode).map_err(RequestFailure::Status)?;
        let output = upsert_fts_document(
            conn,
            request.index_name.as_str(),
            tokenizer_mode,
            request.id.as_str(),
            request.file_path.as_str(),
            request.title.as_str(),
            request.content.as_str(),
        )
        .map_err(|err| RequestFailure::sqlite("sqlite upsert fts document failed", err))?;

        Ok(FtsMutationResponse {
            success: output.success,
            message: output.message,
            affected_rows: output.affected_rows,
            index_name: output.index_name,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!(
                "fts document upserted (index={}, affected_rows={})",
                response.index_name, response.affected_rows
            ),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_delete_fts_document(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: DeleteFtsDocumentRequest,
) -> Result<FtsMutationResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<FtsMutationResponse, RequestFailure> {
        set_request_stage(&context, "deleting_fts_document");
        let output = delete_fts_document(conn, request.index_name.as_str(), request.id.as_str())
            .map_err(|err| RequestFailure::sqlite("sqlite delete fts document failed", err))?;

        Ok(FtsMutationResponse {
            success: output.success,
            message: output.message,
            affected_rows: output.affected_rows,
            index_name: output.index_name,
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!(
                "fts document deleted (index={}, affected_rows={})",
                response.index_name, response.affected_rows
            ),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn run_search_fts(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    request: SearchFtsRequest,
) -> Result<SearchFtsResponse, Status> {
    let conn = lease.connection_mut();
    let result = (|| -> Result<SearchFtsResponse, RequestFailure> {
        set_request_stage(&context, "searching_fts");
        let tokenizer_mode =
            tokenizer_mode_from_proto(request.tokenizer_mode).map_err(RequestFailure::Status)?;
        let output = search_fts(
            conn,
            request.index_name.as_str(),
            tokenizer_mode,
            request.query.as_str(),
            request.limit,
            request.offset,
        )
        .map_err(|err| RequestFailure::sqlite("sqlite search fts failed", err))?;

        Ok(SearchFtsResponse {
            success: output.success,
            message: output.message,
            index_name: output.index_name,
            tokenizer_mode: output.tokenizer_mode,
            normalized_query: output.normalized_query,
            fts_query: output.fts_query,
            total: output.total,
            source: output.source,
            query_mode: output.query_mode,
            hits: output
                .hits
                .into_iter()
                .map(|hit| ProtoSearchFtsHit {
                    id: hit.id,
                    file_path: hit.file_path,
                    title: hit.title,
                    title_highlight: hit.title_highlight,
                    content_snippet: hit.content_snippet,
                    score: hit.score,
                    rank: hit.rank,
                    raw_score: hit.raw_score,
                })
                .collect(),
        })
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    match &result {
        Ok(response) => log_request_succeeded(
            &context,
            format!(
                "fts search returned {} hits (total={})",
                response.hits.len(),
                response.total
            ),
        ),
        Err(status) => log_request_failed(&context, status),
    }

    result
}

fn tokenizer_mode_from_proto(raw_mode: i32) -> Result<TokenizerMode, Status> {
    match ProtoTokenizerMode::try_from(raw_mode).unwrap_or(ProtoTokenizerMode::Unspecified) {
        ProtoTokenizerMode::Unspecified | ProtoTokenizerMode::None => Ok(TokenizerMode::None),
        ProtoTokenizerMode::Jieba => Ok(TokenizerMode::Jieba),
    }
}

const STREAMING_BATCH_ROWS: usize = 1000;

fn run_query_streaming(
    context: RequestLogContext,
    mut lease: ConnectionLease,
    sql: String,
    params: Vec<ProtoSqliteValue>,
    params_json: String,
    tx: mpsc::Sender<Result<QueryResponse, Status>>,
    interrupt_tx: Option<oneshot::Sender<InterruptHandle>>,
) -> Result<(), Status> {
    let conn = lease.connection_mut();
    if let Some(tx) = interrupt_tx {
        let _ = tx.send(conn.get_interrupt_handle());
    }

    let result = (|| -> Result<(), RequestFailure> {
        set_request_stage(&context, "parsing_params");
        let bound_values =
            parse_request_params(&params, &params_json).map_err(RequestFailure::Status)?;
        if has_multiple_sql_statements(&sql) {
            return Err(RequestFailure::Status(Status::invalid_argument(
                "query_stream only supports a single SQL statement",
            )));
        }

        set_request_stage(&context, "preparing_statement");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| RequestFailure::sqlite("sqlite prepare failed", err))?;
        let columns = stmt.columns();
        let column_names = columns
            .iter()
            .map(|column| column.name().to_string())
            .collect::<Vec<_>>();
        let declared_types = columns
            .iter()
            .map(|column| column.decl_type().map(|value| value.to_string()))
            .collect::<Vec<_>>();
        let params = bind_values_as_params(&bound_values);
        set_request_stage(&context, "executing_query");
        let mut rows = stmt
            .query(params.as_slice())
            .map_err(|err| RequestFailure::sqlite("sqlite query failed", err))?;

        let mut chunk_writer = Some(GrpcChunkWriter::new(tx, DEFAULT_IPC_CHUNK_BYTES));
        let mut ipc_writer: Option<StreamWriter<GrpcChunkWriter>> = None;
        let mut schema: Option<Arc<Schema>> = None;
        let mut column_kinds: Option<Vec<ArrowColumnKind>> = None;
        let mut total_rows: usize = 0;

        set_request_stage(&context, "streaming_batches");

        loop {
            let mut batch_rows = Vec::<Vec<SqliteValue>>::new();
            while batch_rows.len() < STREAMING_BATCH_ROWS {
                match rows
                    .next()
                    .map_err(|err| RequestFailure::sqlite("sqlite row fetch failed", err))?
                {
                    Some(row) => {
                        let mut values = Vec::with_capacity(column_names.len());
                        for index in 0..column_names.len() {
                            let value = row.get_ref(index).map_err(|err| {
                                RequestFailure::sqlite("sqlite value access failed", err)
                            })?;
                            values.push(SqliteValue::try_from(value).map_err(|err| {
                                RequestFailure::sqlite(
                                    "sqlite value conversion failed while materializing rows",
                                    RusqliteError::FromSqlConversionFailure(
                                        index,
                                        value.data_type(),
                                        Box::new(err),
                                    ),
                                )
                            })?);
                        }
                        batch_rows.push(values);
                    }
                    None => break,
                }
            }

            if batch_rows.is_empty() {
                break;
            }

            total_rows += batch_rows.len();

            if ipc_writer.is_none() {
                column_kinds = Some(infer_column_kinds(&declared_types, &batch_rows));
                schema = Some(Arc::new(Schema::new(
                    column_names
                        .iter()
                        .zip(column_kinds.as_ref().unwrap().iter())
                        .map(|(name, kind)| {
                            Field::new(
                                name,
                                match kind {
                                    ArrowColumnKind::Int64 => DataType::Int64,
                                    ArrowColumnKind::Float64 => DataType::Float64,
                                    ArrowColumnKind::Utf8 => DataType::Utf8,
                                    ArrowColumnKind::Binary => DataType::Binary,
                                },
                                true,
                            )
                        })
                        .collect::<Vec<_>>(),
                )));

                let writer = StreamWriter::try_new(chunk_writer.take().unwrap(), schema.as_ref().unwrap())
                    .map_err(|err| {
                        RequestFailure::Status(status_internal("arrow stream header write failed", err))
                    })?;
                ipc_writer = Some(writer);
            }

            let batch = RecordBatch::try_new(
                Arc::clone(schema.as_ref().unwrap()),
                build_arrow_arrays(column_kinds.as_ref().unwrap(), &batch_rows),
            )
            .map_err(|err| {
                RequestFailure::Status(status_internal("arrow record batch build failed", err))
            })?;

            let writer = ipc_writer.as_mut().unwrap();
            writer.write(&batch).map_err(|err| {
                RequestFailure::Status(status_internal("arrow batch write failed", err))
            })?;
            writer.flush().map_err(|err| {
                RequestFailure::Status(status_internal("arrow batch flush failed", err))
            })?;
        }

        if let Some(mut writer) = ipc_writer {
            writer.finish().map_err(|err| {
                RequestFailure::Status(status_internal("arrow stream finish failed", err))
            })?;
            writer.flush().map_err(|err| {
                RequestFailure::Status(status_internal("arrow final flush failed", err))
            })?;

            let metrics = writer.get_ref().metrics();
            log_request_succeeded(
                &context,
                format!(
                    "streamed {rows} rows in batches ({chunks} chunks, {bytes} bytes)",
                    rows = total_rows,
                    chunks = metrics.emitted_chunks,
                    bytes = metrics.emitted_bytes,
                ),
            );
        } else {
            log_request_succeeded(&context, "streamed 0 rows (empty result)");
        }

        Ok(())
    })();
    let result = result.map_err(|failure| finalize_request_failure(&context, conn, failure));

    if let Err(status) = &result {
        log_request_failed(&context, status);
    }

    result
}

fn finalize_request_failure(
    context: &RequestLogContext,
    conn: &mut Connection,
    failure: RequestFailure,
) -> Status {
    match failure {
        RequestFailure::Status(status) => status,
        RequestFailure::Sqlite { prefix, error } => {
            best_effort_rollback(context, conn);
            sqlite_status(prefix, &error)
        }
    }
}

fn sqlite_status(prefix: &str, error: &RusqliteError) -> Status {
    match error {
        RusqliteError::ExecuteReturnedResults => Status::invalid_argument(format!(
            "{prefix}: ExecuteScript cannot return rows; use QueryJson or QueryStream"
        )),
        RusqliteError::InvalidQuery => {
            Status::invalid_argument(format!("{prefix}: SQL is not a query statement"))
        }
        RusqliteError::MultipleStatement => {
            Status::invalid_argument(format!("{prefix}: multiple SQL statements are not allowed"))
        }
        RusqliteError::InvalidParameterCount(got, expected) => Status::invalid_argument(format!(
            "{prefix}: expected {expected} SQL parameters but received {got}"
        )),
        RusqliteError::InvalidParameterName(name) => {
            Status::invalid_argument(format!("{prefix}: invalid SQL parameter name {name}"))
        }
        RusqliteError::IntegralValueOutOfRange(index, value) => Status::internal(format!(
            "{prefix}: column {index} contains integer value {value} outside the target Rust range"
        )),
        RusqliteError::InvalidColumnIndex(index) => {
            Status::internal(format!("{prefix}: invalid column index {index}"))
        }
        RusqliteError::InvalidColumnName(name) => {
            Status::internal(format!("{prefix}: invalid column name {name}"))
        }
        RusqliteError::InvalidColumnType(index, name, ty) => Status::internal(format!(
            "{prefix}: invalid type for column {index} ({name}), SQLite type {ty:?}"
        )),
        RusqliteError::FromSqlConversionFailure(..)
        | RusqliteError::Utf8Error(..)
        | RusqliteError::NulError(..)
        | RusqliteError::ToSqlConversionFailure(..)
        | RusqliteError::QueryReturnedMoreThanOneRow
        | RusqliteError::StatementChangedRows(..) => Status::internal(format!("{prefix}: {error}")),
        RusqliteError::QueryReturnedNoRows => Status::not_found(format!("{prefix}: {error}")),
        RusqliteError::InvalidPath(_) => Status::invalid_argument(format!("{prefix}: {error}")),
        RusqliteError::SqlInputError { msg, offset, .. } => {
            Status::invalid_argument(format!("{prefix}: {msg} (offset={offset})"))
        }
        _ => match error.sqlite_error_code() {
            Some(code @ (SqliteErrorCode::DatabaseBusy | SqliteErrorCode::DatabaseLocked)) => {
                status_with_sqlite_metadata(
                    Code::Unavailable,
                    format!("{prefix}: {error}"),
                    true,
                    Some(sqlite_error_code_label(code)),
                )
            }
            Some(code @ SqliteErrorCode::ReadOnly) => status_with_sqlite_metadata(
                Code::FailedPrecondition,
                format!("{prefix}: {error}"),
                false,
                Some(sqlite_error_code_label(code)),
            ),
            Some(code @ SqliteErrorCode::ConstraintViolation) => status_with_sqlite_metadata(
                Code::FailedPrecondition,
                format!("{prefix}: {error}"),
                false,
                Some(sqlite_error_code_label(code)),
            ),
            Some(code @ SqliteErrorCode::OperationInterrupted) => status_with_sqlite_metadata(
                Code::Cancelled,
                format!("{prefix}: {error}"),
                false,
                Some(sqlite_error_code_label(code)),
            ),
            Some(code @ SqliteErrorCode::PermissionDenied) => status_with_sqlite_metadata(
                Code::PermissionDenied,
                format!("{prefix}: {error}"),
                false,
                Some(sqlite_error_code_label(code)),
            ),
            Some(code @ (SqliteErrorCode::DatabaseCorrupt | SqliteErrorCode::NotADatabase)) => {
                status_with_sqlite_metadata(
                    Code::DataLoss,
                    format!("{prefix}: {error}"),
                    false,
                    Some(sqlite_error_code_label(code)),
                )
            }
            Some(code @ SqliteErrorCode::DiskFull) => status_with_sqlite_metadata(
                Code::ResourceExhausted,
                format!("{prefix}: {error}"),
                false,
                Some(sqlite_error_code_label(code)),
            ),
            Some(code @ (SqliteErrorCode::CannotOpen | SqliteErrorCode::SystemIoFailure)) => {
                status_with_sqlite_metadata(
                    Code::Unavailable,
                    format!("{prefix}: {error}"),
                    false,
                    Some(sqlite_error_code_label(code)),
                )
            }
            Some(code @ SqliteErrorCode::SchemaChanged) => status_with_sqlite_metadata(
                Code::Aborted,
                format!("{prefix}: {error}; retry the request"),
                true,
                Some(sqlite_error_code_label(code)),
            ),
            _ => Status::internal(format!("{prefix}: {error}")),
        },
    }
}

fn status_with_sqlite_metadata(
    code: Code,
    message: impl Into<String>,
    retryable: bool,
    sqlite_code: Option<&str>,
) -> Status {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        RETRYABLE_METADATA_KEY,
        if retryable {
            AsciiMetadataValue::from_static("true")
        } else {
            AsciiMetadataValue::from_static("false")
        },
    );
    if let Some(sqlite_code) = sqlite_code {
        metadata.insert(
            SQLITE_CODE_METADATA_KEY,
            sqlite_code
                .parse()
                .expect("sqlite status metadata must stay ASCII"),
        );
    }

    Status::with_metadata(code, message, metadata)
}

fn sqlite_error_code_label(code: SqliteErrorCode) -> &'static str {
    match code {
        SqliteErrorCode::DatabaseBusy => "SQLITE_BUSY",
        SqliteErrorCode::DatabaseLocked => "SQLITE_LOCKED",
        SqliteErrorCode::ReadOnly => "SQLITE_READONLY",
        SqliteErrorCode::ConstraintViolation => "SQLITE_CONSTRAINT",
        SqliteErrorCode::OperationInterrupted => "SQLITE_INTERRUPT",
        SqliteErrorCode::PermissionDenied => "SQLITE_PERM",
        SqliteErrorCode::DatabaseCorrupt => "SQLITE_CORRUPT",
        SqliteErrorCode::NotADatabase => "SQLITE_NOTADB",
        SqliteErrorCode::DiskFull => "SQLITE_FULL",
        SqliteErrorCode::CannotOpen => "SQLITE_CANTOPEN",
        SqliteErrorCode::SystemIoFailure => "SQLITE_IOERR",
        SqliteErrorCode::SchemaChanged => "SQLITE_SCHEMA",
        _ => "SQLITE_ERROR",
    }
}

fn best_effort_rollback(context: &RequestLogContext, conn: &mut Connection) {
    if conn.is_autocommit() {
        return;
    }

    match conn.execute_batch("ROLLBACK") {
        Ok(()) => {
            context.logger.log(
                "recover",
                format!(
                    "request_id={} op={} stage={} detail=issued best-effort ROLLBACK after SQLite error",
                    context.request_id,
                    context.operation,
                    context.progress.snapshot(),
                ),
            );
        }
        Err(err) => {
            if is_no_active_transaction_error(err.to_string().as_str()) {
                return;
            }

            context.logger.log(
                "recover_error",
                format!(
                    "request_id={} op={} stage={} detail=best-effort ROLLBACK after SQLite error failed rollback_error=\"{}\"",
                    context.request_id,
                    context.operation,
                    context.progress.snapshot(),
                    err,
                ),
            );
        }
    }
}

fn is_no_active_transaction_error(error_message: &str) -> bool {
    let lowered = error_message.to_ascii_lowercase();
    lowered.contains("no transaction is active")
        || lowered.contains("cannot rollback - no transaction is active")
}

fn parse_request_params(
    params: &[ProtoSqliteValue],
    params_json: &str,
) -> Result<Vec<SqliteValue>, Status> {
    if !params.is_empty() {
        if !params_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "provide either flat params or params_json, but not both",
            ));
        }

        return params
            .iter()
            .map(proto_param_to_sqlite_value)
            .collect::<Result<Vec<_>, _>>();
    }

    parse_legacy_params_json(params_json)
}

fn parse_batch_params(items: &[ExecuteBatchItem]) -> Result<Vec<Vec<SqliteValue>>, Status> {
    items
        .iter()
        .map(|item| parse_request_params(&item.params, ""))
        .collect()
}

fn parse_legacy_params_json(params_json: &str) -> Result<Vec<SqliteValue>, Status> {
    if params_json.trim().is_empty() {
        return Ok(Vec::new());
    }

    let params = serde_json::from_str::<JsonValue>(params_json).map_err(|err| {
        Status::invalid_argument(format!(
            "params_json must be a JSON array of scalar values: {err}"
        ))
    })?;

    let items = params.as_array().ok_or_else(|| {
        Status::invalid_argument("params_json must be a JSON array of scalar values")
    })?;

    items
        .iter()
        .cloned()
        .map(json_param_to_sqlite_value)
        .collect()
}

fn proto_param_to_sqlite_value(value: &ProtoSqliteValue) -> Result<SqliteValue, Status> {
    match value.kind.as_ref() {
        Some(ProtoSqliteValueKind::Int64Value(value)) => Ok(SqliteValue::Integer(*value)),
        Some(ProtoSqliteValueKind::Float64Value(value)) => Ok(SqliteValue::Real(*value)),
        Some(ProtoSqliteValueKind::StringValue(value)) => Ok(SqliteValue::Text(value.clone())),
        Some(ProtoSqliteValueKind::BytesValue(value)) => Ok(SqliteValue::Blob(value.clone())),
        Some(ProtoSqliteValueKind::BoolValue(value)) => Ok(SqliteValue::Integer(i64::from(*value))),
        Some(ProtoSqliteValueKind::NullValue(_)) => Ok(SqliteValue::Null),
        None => Err(Status::invalid_argument(
            "SqliteValue.kind must be set for every bound parameter",
        )),
    }
}

fn json_param_to_sqlite_value(value: JsonValue) -> Result<SqliteValue, Status> {
    match value {
        JsonValue::Null => Ok(SqliteValue::Null),
        JsonValue::Bool(value) => Ok(SqliteValue::Integer(i64::from(value))),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqliteValue::Integer(value))
            } else if let Some(value) = value.as_u64() {
                Ok(SqliteValue::Integer(i64::try_from(value).map_err(|_| {
                    Status::invalid_argument(
                        "params_json contains an unsigned integer larger than SQLite signed 64-bit range",
                    )
                })?))
            } else if let Some(value) = value.as_f64() {
                Ok(SqliteValue::Real(value))
            } else {
                Err(Status::invalid_argument(
                    "params_json contains an unsupported numeric value",
                ))
            }
        }
        JsonValue::String(value) => Ok(SqliteValue::Text(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(Status::invalid_argument(
            "params_json only supports scalar JSON values (null, bool, number, string)",
        )),
    }
}

fn bind_values_as_params(values: &[SqliteValue]) -> Vec<&dyn ToSql> {
    values.iter().map(|value| value as &dyn ToSql).collect()
}

fn execute_prepared_batch(
    conn: &mut Connection,
    context: &RequestLogContext,
    sql: &str,
    batch_params: &[Vec<SqliteValue>],
) -> Result<ExecuteBatchResponse, RequestFailure> {
    set_request_stage(context, "beginning_transaction");
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|err| RequestFailure::sqlite("sqlite BEGIN TRANSACTION failed", err))?;

    set_request_stage(context, "preparing_statement");
    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| RequestFailure::sqlite("sqlite prepare failed", err))?;

    let mut rows_changed = 0_i64;
    set_request_stage(context, "executing_batch_items");
    for params in batch_params {
        let params = bind_values_as_params(params);
        let changed = stmt
            .execute(params.as_slice())
            .map_err(|err| RequestFailure::sqlite("sqlite execute failed", err))?;
        rows_changed = rows_changed.saturating_add(i64::try_from(changed).unwrap_or(i64::MAX));
    }

    drop(stmt);

    set_request_stage(context, "committing_transaction");
    conn.execute_batch("COMMIT")
        .map_err(|err| RequestFailure::sqlite("sqlite COMMIT failed", err))?;

    Ok(ExecuteBatchResponse {
        success: true,
        message: format!(
            "batch executed successfully (statements_executed={} rows_changed={rows_changed})",
            batch_params.len()
        ),
        rows_changed,
        last_insert_rowid: conn.last_insert_rowid(),
        statements_executed: i64::try_from(batch_params.len()).unwrap_or(i64::MAX),
    })
}

fn has_multiple_sql_statements(sql: &str) -> bool {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut statement_count = 0;
    let mut has_content = false;

    while i < len {
        match chars[i] {
            '\'' => {
                has_content = true;
                i += 1;
                while i < len {
                    if chars[i] == '\'' {
                        if i + 1 < len && chars[i + 1] == '\'' {
                            i += 2; // escaped ''
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            '"' => {
                has_content = true;
                i += 1;
                while i < len {
                    if chars[i] == '"' {
                        if i + 1 < len && chars[i + 1] == '"' {
                            i += 2; // escaped ""
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            '-' if i + 1 < len && chars[i + 1] == '-' => {
                // line comment
                i += 2;
                while i < len && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < len && chars[i + 1] == '*' => {
                // block comment
                i += 2;
                while i + 1 < len {
                    if chars[i] == '*' && chars[i + 1] == '/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            ';' => {
                if has_content {
                    statement_count += 1;
                    if statement_count > 1 {
                        return true;
                    }
                }
                has_content = false;
                i += 1;
            }
            c if !c.is_whitespace() => {
                has_content = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    if has_content {
        statement_count += 1;
    }

    statement_count > 1
}

fn sqlite_value_ref_to_json(value: SqliteValueRef<'_>) -> JsonValue {
    match SqliteValue::try_from(value) {
        Ok(value) => sqlite_value_to_json(&value),
        Err(_) => JsonValue::Null,
    }
}

fn sqlite_value_to_json(value: &SqliteValue) -> JsonValue {
    match value {
        SqliteValue::Null => JsonValue::Null,
        SqliteValue::Integer(value) => JsonValue::from(*value),
        SqliteValue::Real(value) => json_float(*value),
        SqliteValue::Text(value) => JsonValue::String(value.clone()),
        SqliteValue::Blob(value) => JsonValue::Array(
            value
                .iter()
                .map(|byte| JsonValue::from(u64::from(*byte)))
                .collect(),
        ),
    }
}

fn infer_column_kinds(
    declared_types: &[Option<String>],
    rows: &[Vec<SqliteValue>],
) -> Vec<ArrowColumnKind> {
    let column_count = declared_types.len();
    let mut kinds = Vec::with_capacity(column_count);

    for index in 0..column_count {
        let mut current = declared_type_hint(declared_types[index].as_deref());
        for row in rows {
            current = merge_column_kind(current, &row[index]);
        }
        kinds.push(current.unwrap_or(ArrowColumnKind::Utf8));
    }

    kinds
}

fn declared_type_hint(value: Option<&str>) -> Option<ArrowColumnKind> {
    let normalized = value?.trim().to_ascii_uppercase();

    if normalized.contains("INT") || normalized.contains("BOOL") {
        Some(ArrowColumnKind::Int64)
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
        || normalized.contains("NUMERIC")
        || normalized.contains("DEC")
    {
        Some(ArrowColumnKind::Float64)
    } else if normalized.contains("BLOB") {
        Some(ArrowColumnKind::Binary)
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
        || normalized.contains("JSON")
        || normalized.contains("DATE")
        || normalized.contains("TIME")
    {
        Some(ArrowColumnKind::Utf8)
    } else {
        None
    }
}

fn merge_column_kind(
    current: Option<ArrowColumnKind>,
    value: &SqliteValue,
) -> Option<ArrowColumnKind> {
    match value {
        SqliteValue::Null => current,
        SqliteValue::Integer(_) => Some(match current {
            None => ArrowColumnKind::Int64,
            Some(ArrowColumnKind::Int64) => ArrowColumnKind::Int64,
            Some(ArrowColumnKind::Float64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Utf8) => ArrowColumnKind::Utf8,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Utf8,
        }),
        SqliteValue::Real(_) => Some(match current {
            None => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Int64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Float64) => ArrowColumnKind::Float64,
            Some(ArrowColumnKind::Utf8) => ArrowColumnKind::Utf8,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Utf8,
        }),
        SqliteValue::Text(_) => Some(ArrowColumnKind::Utf8),
        SqliteValue::Blob(_) => Some(match current {
            None => ArrowColumnKind::Binary,
            Some(ArrowColumnKind::Binary) => ArrowColumnKind::Binary,
            _ => ArrowColumnKind::Utf8,
        }),
    }
}

fn build_arrow_arrays(kinds: &[ArrowColumnKind], rows: &[Vec<SqliteValue>]) -> Vec<ArrayRef> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(kinds.len());

    for (index, kind) in kinds.iter().enumerate() {
        match kind {
            ArrowColumnKind::Int64 => {
                let mut builder = Int64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Integer(value) => builder.append_value(*value),
                        SqliteValue::Real(value) => builder.append_value(*value as i64),
                        SqliteValue::Text(_) | SqliteValue::Blob(_) => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Float64 => {
                let mut builder = Float64Builder::with_capacity(rows.len());
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Integer(value) => builder.append_value(*value as f64),
                        SqliteValue::Real(value) => builder.append_value(*value),
                        SqliteValue::Text(_) | SqliteValue::Blob(_) => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    match sqlite_value_to_text(&row[index]) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            ArrowColumnKind::Binary => {
                let mut builder = BinaryBuilder::new();
                for row in rows {
                    match &row[index] {
                        SqliteValue::Null => builder.append_null(),
                        SqliteValue::Blob(value) => builder.append_value(value),
                        other => builder.append_value(
                            sqlite_value_to_text(other).unwrap_or_default().as_bytes(),
                        ),
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
        }
    }

    arrays
}

fn sqlite_value_to_text(value: &SqliteValue) -> Option<String> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Integer(value) => Some(value.to_string()),
        SqliteValue::Real(value) => Some(format_float(*value)),
        SqliteValue::Text(value) => Some(value.clone()),
        SqliteValue::Blob(value) => Some(format!("x'{}'", blob_to_hex(value))),
    }
}

fn blob_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn json_float(value: f64) -> JsonValue {
    if value.is_nan() || value.is_infinite() {
        return JsonValue::String(format_float(value));
    }
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .unwrap_or_else(|| JsonValue::String(format_float(value)))
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn response_with_default_metadata<T>(payload: T) -> Response<T> {
    let mut response = Response::new(payload);
    response.metadata_mut().insert(
        RETRYABLE_METADATA_KEY,
        AsciiMetadataValue::from_static("false"),
    );
    response
}

fn status_internal<E: std::fmt::Display>(prefix: &str, error: E) -> Status {
    Status::internal(format!("{prefix}: {error}"))
}

fn build_request_context<T>(
    state: &Arc<AppState>,
    request: &Request<T>,
    operation: &'static str,
    sql: &str,
    param_count: usize,
    params_json: &str,
) -> RequestLogContext {
    let logging = state.logger.config();
    RequestLogContext {
        logger: Arc::clone(&state.logger),
        progress: Arc::new(RequestProgress::new("queued")),
        request_id: NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
        operation,
        remote_addr: request.remote_addr(),
        grpc_timeout: request
            .metadata()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout_header),
        started_at: Instant::now(),
        sql_full: sql.trim().to_string(),
        sql_preview: preview_sql(sql, logging.sql_preview_chars),
        param_count,
        params_json_bytes: params_json.len(),
        request_log_enabled: logging.request_log_enabled,
        log_sql: logging.log_sql,
        sql_masking: logging.sql_masking,
        slow_query_log_enabled: logging.slow_query_log_enabled,
        slow_query_threshold: Duration::from_millis(logging.slow_query_threshold_ms),
        slow_query_full_sql_enabled: logging.slow_query_full_sql_enabled,
    }
}

fn set_request_stage(context: &RequestLogContext, stage: &'static str) {
    context.progress.set(stage);
}

fn spawn_deadline_interrupt_watcher(
    context: RequestLogContext,
    interrupt_rx: oneshot::Receiver<InterruptHandle>,
    done_rx: oneshot::Receiver<()>,
    deadline_triggered: Arc<AtomicBool>,
) {
    let Some(grpc_timeout) = context.grpc_timeout else {
        return;
    };

    let deadline = tokio::time::Instant::from_std(context.started_at + grpc_timeout);
    tokio::spawn(async move {
        let Ok(interrupt_handle) = interrupt_rx.await else {
            return;
        };
        let mut done_rx = done_rx;

        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                deadline_triggered.store(true, Ordering::Relaxed);
                log_request_timeout(&context);
                interrupt_handle.interrupt();
                let _ = done_rx.await;
            }
            _ = &mut done_rx => {}
        }
    });
}

fn remap_deadline_status_if_needed<T>(
    result: Result<T, Status>,
    deadline_triggered: &Arc<AtomicBool>,
) -> Result<T, Status> {
    result.map_err(|status| remap_deadline_status(status, deadline_triggered))
}

fn remap_deadline_status(status: Status, deadline_triggered: &Arc<AtomicBool>) -> Status {
    if deadline_triggered.load(Ordering::Relaxed)
        && status.message().to_ascii_lowercase().contains("interrupt")
    {
        return Status::deadline_exceeded(
            "SQLite query exceeded the gRPC deadline and was interrupted",
        );
    }

    status
}

fn preview_sql(sql: &str, max_chars: usize) -> String {
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return "<empty>".to_string();
    }

    let mut preview = String::new();
    for (index, ch) in normalized.chars().enumerate() {
        if index >= max_chars {
            preview.push_str("...");
            return preview;
        }
        preview.push(ch);
    }

    preview
}

/// Mask SQL string literals to prevent sensitive data leakage in logs.
/// Replaces content inside single-quoted strings with `***`.
fn mask_sql(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;

    while i < len {
        match chars[i] {
            '\'' => {
                result.push_str("***");
                i += 1;
                while i < len {
                    if chars[i] == '\'' {
                        if i + 1 < len && chars[i + 1] == '\'' {
                            i += 2; // escaped ''
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            '-' if i + 1 < len && chars[i + 1] == '-' => {
                result.push_str("--");
                i += 2;
                while i < len && chars[i] != '\n' {
                    result.push(chars[i]);
                    i += 1;
                }
            }
            '/' if i + 1 < len && chars[i + 1] == '*' => {
                result.push_str("/* ... */");
                i += 2;
                while i + 1 < len {
                    if chars[i] == '*' && chars[i + 1] == '/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            c => {
                result.push(c);
                i += 1;
            }
        }
    }

    result
}

/// Format SQL for logging based on context settings.
fn format_sql_for_log(context: &RequestLogContext, sql: &str) -> String {
    if !context.log_sql {
        return "<redacted>".to_string();
    }
    if context.sql_masking {
        mask_sql(sql)
    } else {
        sql.to_string()
    }
}

fn format_sql_preview_for_log(context: &RequestLogContext) -> String {
    if !context.log_sql {
        return "<redacted>".to_string();
    }
    if context.sql_masking {
        mask_sql(&context.sql_preview)
    } else {
        context.sql_preview.clone()
    }
}

fn parse_grpc_timeout_header(raw: &str) -> Option<Duration> {
    let unit = raw.chars().last()?;
    let digits = raw.get(..raw.len().checked_sub(1)?)?;
    let value = digits.parse::<u64>().ok()?;

    match unit {
        'H' => value.checked_mul(60 * 60).map(Duration::from_secs),
        'M' => value.checked_mul(60).map(Duration::from_secs),
        'S' => Some(Duration::from_secs(value)),
        'm' => Some(Duration::from_millis(value)),
        'u' => Some(Duration::from_micros(value)),
        'n' => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

fn log_request_started(context: &RequestLogContext) {
    if !context.request_log_enabled {
        return;
    }

    context.logger.log(
        "start",
        format!(
            "request_id={} op={} remote={} grpc_timeout={} param_count={} params_json_bytes={} sql=\"{}\"",
            context.request_id,
            context.operation,
            format_remote_addr(context.remote_addr),
            format_optional_duration(context.grpc_timeout),
            context.param_count,
            context.params_json_bytes,
            format_sql_preview_for_log(context),
        ),
    );
}

fn log_request_invalid_argument(context: &RequestLogContext, message: &str) {
    if context.request_log_enabled {
        context.logger.log(
            "invalid",
            format!(
                "request_id={} op={} elapsed_ms={} remote={} message={} sql=\"{}\"",
                context.request_id,
                context.operation,
                context.started_at.elapsed().as_millis(),
                format_remote_addr(context.remote_addr),
                message,
                format_sql_preview_for_log(context),
            ),
        );
    }
}

fn log_request_timeout(context: &RequestLogContext) {
    context.logger.log(
        "timeout",
        format!(
            "request_id={} op={} elapsed_ms={} remote={} grpc_timeout={} stage={} sql=\"{}\" message=interrupting running SQLite query because the gRPC deadline expired",
            context.request_id,
            context.operation,
            context.started_at.elapsed().as_millis(),
            format_remote_addr(context.remote_addr),
            format_optional_duration(context.grpc_timeout),
            context.progress.snapshot(),
            format_sql_preview_for_log(context),
        ),
    );
}

fn log_request_succeeded(context: &RequestLogContext, detail: impl AsRef<str>) {
    let elapsed = context.started_at.elapsed();
    if context.request_log_enabled {
        context.logger.log(
            "ok",
            format!(
                "request_id={} op={} elapsed_ms={} remote={} stage={} detail={} sql=\"{}\"",
                context.request_id,
                context.operation,
                elapsed.as_millis(),
                format_remote_addr(context.remote_addr),
                context.progress.snapshot(),
                detail.as_ref(),
                format_sql_preview_for_log(context),
            ),
        );
    }
    maybe_log_slow_query(context, elapsed, "completed", detail.as_ref());
}

fn log_request_failed(context: &RequestLogContext, status: &Status) {
    let elapsed = context.started_at.elapsed();
    context.logger.log(
        "error",
        format!(
            "request_id={} op={} elapsed_ms={} remote={} stage={} code={:?} message={} sql=\"{}\"",
            context.request_id,
            context.operation,
            elapsed.as_millis(),
            format_remote_addr(context.remote_addr),
            context.progress.snapshot(),
            status.code(),
            status.message(),
            format_sql_preview_for_log(context),
        ),
    );
    maybe_log_slow_query(context, elapsed, "failed", status.message());
}

fn maybe_log_slow_query(
    context: &RequestLogContext,
    elapsed: Duration,
    final_state: &str,
    detail: &str,
) {
    if !context.slow_query_log_enabled || elapsed < context.slow_query_threshold {
        return;
    }

    let sql_text = format_sql_for_log(context, if context.slow_query_full_sql_enabled {
        &context.sql_full
    } else {
        &context.sql_preview
    });

    context.logger.log(
        "slow_query",
        format!(
            "request_id={} op={} elapsed_ms={} threshold_ms={} remote={} stage={} state={} detail={} sql=\"{}\"",
            context.request_id,
            context.operation,
            elapsed.as_millis(),
            context.slow_query_threshold.as_millis(),
            format_remote_addr(context.remote_addr),
            context.progress.snapshot(),
            final_state,
            detail,
            sql_text,
        ),
    );
}

fn format_remote_addr(remote_addr: Option<SocketAddr>) -> String {
    remote_addr
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(|value| format!("{}ms", value.as_millis()))
        .unwrap_or_else(|| "none".to_string())
}

struct GrpcChunkWriter {
    tx: mpsc::Sender<Result<QueryResponse, Status>>,
    pending: Vec<u8>,
    target_chunk_size: usize,
    emitted_chunks: usize,
    emitted_bytes: usize,
}

impl GrpcChunkWriter {
    fn new(tx: mpsc::Sender<Result<QueryResponse, Status>>, target_chunk_size: usize) -> Self {
        let chunk_size = target_chunk_size.max(64 * 1024);
        Self {
            tx,
            pending: Vec::with_capacity(chunk_size),
            target_chunk_size: chunk_size,
            emitted_chunks: 0,
            emitted_bytes: 0,
        }
    }

    fn metrics(&self) -> StreamMetrics {
        StreamMetrics {
            emitted_chunks: self.emitted_chunks,
            emitted_bytes: self.emitted_bytes,
        }
    }

    fn emit_full_chunks(&mut self) -> io::Result<()> {
        while self.pending.len() >= self.target_chunk_size {
            let remainder = self.pending.split_off(self.target_chunk_size);
            let chunk = std::mem::replace(&mut self.pending, remainder);
            self.send_chunk(chunk)?;
        }
        Ok(())
    }

    fn emit_remaining(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let chunk = std::mem::take(&mut self.pending);
        self.send_chunk(chunk)
    }

    fn send_chunk(&mut self, chunk: Vec<u8>) -> io::Result<()> {
        let chunk_len = chunk.len();
        self.tx
            .blocking_send(Ok(QueryResponse {
                arrow_ipc_chunk: Bytes::from(chunk),
            }))
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("gRPC stream closed: {err}"),
                )
            })?;
        self.emitted_chunks += 1;
        self.emitted_bytes += chunk_len;

        Ok(())
    }
}

impl Write for GrpcChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        self.pending.extend_from_slice(buf);
        self.emit_full_chunks()?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_remaining()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_connection_pragmas, blob_to_hex, build_open_flags, declared_type_hint,
        effective_connection_pool_size, has_multiple_sql_statements, infer_column_kinds,
        mask_sql, parse_grpc_timeout_header, parse_request_params, preview_sql, sqlite_status,
    };
    use crate::config::Config;
    use crate::pb::sqlite_value::Kind as ProtoSqliteValueKind;
    use crate::pb::{NullValue, SqliteValue as ProtoSqliteValue};
    use rusqlite::Error as RusqliteError;
    use rusqlite::ffi::Error as SqliteFfiError;
    use rusqlite::ffi::ErrorCode as SqliteErrorCode;
    use rusqlite::types::Value as SqliteValue;
    use rusqlite::{Connection, OpenFlags};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parse_grpc_timeout_supports_all_units() {
        assert_eq!(
            parse_grpc_timeout_header("2H"),
            Some(Duration::from_secs(7200))
        );
        assert_eq!(
            parse_grpc_timeout_header("3M"),
            Some(Duration::from_secs(180))
        );
        assert_eq!(
            parse_grpc_timeout_header("4S"),
            Some(Duration::from_secs(4))
        );
        assert_eq!(
            parse_grpc_timeout_header("5m"),
            Some(Duration::from_millis(5))
        );
        assert_eq!(
            parse_grpc_timeout_header("6u"),
            Some(Duration::from_micros(6))
        );
        assert_eq!(
            parse_grpc_timeout_header("7n"),
            Some(Duration::from_nanos(7))
        );
    }

    #[test]
    fn parse_grpc_timeout_rejects_invalid_values() {
        assert_eq!(parse_grpc_timeout_header(""), None);
        assert_eq!(parse_grpc_timeout_header("abc"), None);
        assert_eq!(parse_grpc_timeout_header("10x"), None);
    }

    #[test]
    fn preview_sql_compacts_whitespace_and_truncates() {
        let preview = preview_sql("select   *\nfrom   demo\twhere id = 1", 160);
        assert_eq!(preview, "select * from demo where id = 1");

        let long_sql = format!("select {}", "x".repeat(300));
        let preview = preview_sql(&long_sql, 32);
        assert!(preview.ends_with("..."));
    }

    #[test]
    fn apply_connection_pragmas_supports_default_profile() {
        let conn = Connection::open_in_memory().expect("open sqlite connection");
        let mut config = Config::default();
        config.db_path = ":memory:".to_string();

        apply_connection_pragmas(&conn, &config).expect("apply sqlite pragmas");

        let foreign_keys: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .expect("read foreign_keys");
        let trusted_schema: i64 = conn
            .query_row("PRAGMA trusted_schema", [], |row| row.get(0))
            .expect("read trusted_schema");
        let busy_timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .expect("read busy_timeout");

        assert_eq!(foreign_keys, 1);
        assert_eq!(trusted_schema, 0);
        assert_eq!(busy_timeout, 5_000);
    }

    #[test]
    fn apply_connection_pragmas_enables_wal_for_file_database() {
        let db_path = unique_test_db_path("wal");
        let _cleanup = TempSqliteFiles::new(&db_path);
        let conn = Connection::open(&db_path).expect("open sqlite file database");
        let mut config = Config::default();
        config.db_path = db_path.to_string_lossy().to_string();

        apply_connection_pragmas(&conn, &config).expect("apply sqlite pragmas");

        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal_mode");
        assert_eq!(journal_mode.to_ascii_uppercase(), "WAL");
    }

    #[test]
    fn open_flags_follow_config_hardening() {
        let mut config = Config::default();
        config.hardening.read_only = true;
        config.hardening.allow_uri_filenames = true;
        let flags = build_open_flags(&config);

        assert!(flags.contains(OpenFlags::SQLITE_OPEN_READ_ONLY));
        assert!(flags.contains(OpenFlags::SQLITE_OPEN_URI));
        assert!(!flags.contains(OpenFlags::SQLITE_OPEN_CREATE));
    }

    #[test]
    fn declared_type_hint_maps_common_sqlite_types() {
        assert_eq!(
            declared_type_hint(Some("INTEGER")),
            Some(super::ArrowColumnKind::Int64)
        );
        assert_eq!(
            declared_type_hint(Some("REAL")),
            Some(super::ArrowColumnKind::Float64)
        );
        assert_eq!(
            declared_type_hint(Some("TEXT")),
            Some(super::ArrowColumnKind::Utf8)
        );
        assert_eq!(
            declared_type_hint(Some("BLOB")),
            Some(super::ArrowColumnKind::Binary)
        );
    }

    #[test]
    fn infer_column_kinds_promotes_mixed_numeric_and_text_columns() {
        let kinds = infer_column_kinds(
            &[Some("INTEGER".to_string()), None],
            &[
                vec![SqliteValue::Integer(1), SqliteValue::Real(1.5)],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Text("alpha".to_string()),
                ],
            ],
        );

        assert_eq!(kinds[0], super::ArrowColumnKind::Int64);
        assert_eq!(kinds[1], super::ArrowColumnKind::Utf8);
    }

    #[test]
    fn blob_hex_encoding_matches_sqlite_blob_literal_style() {
        assert_eq!(blob_to_hex(&[0xDE, 0xAD, 0xBE, 0xEF]), "DEADBEEF");
    }

    #[test]
    fn flat_proto_params_map_to_sqlite_scalars() {
        let params = vec![
            ProtoSqliteValue {
                kind: Some(ProtoSqliteValueKind::Int64Value(7)),
            },
            ProtoSqliteValue {
                kind: Some(ProtoSqliteValueKind::BoolValue(true)),
            },
            ProtoSqliteValue {
                kind: Some(ProtoSqliteValueKind::StringValue(
                    "{\"ttl\":30}".to_string(),
                )),
            },
            ProtoSqliteValue {
                kind: Some(ProtoSqliteValueKind::NullValue(NullValue {})),
            },
        ];

        let parsed = parse_request_params(&params, "").expect("parse flat params");
        assert_eq!(
            parsed,
            vec![
                SqliteValue::Integer(7),
                SqliteValue::Integer(1),
                SqliteValue::Text("{\"ttl\":30}".to_string()),
                SqliteValue::Null,
            ]
        );
    }

    #[test]
    fn sqlite_busy_is_marked_retryable_in_trailers() {
        let status = sqlite_status(
            "sqlite execute failed",
            &RusqliteError::SqliteFailure(
                SqliteFfiError {
                    code: SqliteErrorCode::DatabaseBusy,
                    extended_code: 5,
                },
                None,
            ),
        );

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            status
                .metadata()
                .get("x-vldb-retryable")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        assert_eq!(
            status
                .metadata()
                .get("x-vldb-sqlite-code")
                .and_then(|value| value.to_str().ok()),
            Some("SQLITE_BUSY")
        );
    }

    #[test]
    fn in_memory_database_forces_single_connection_pool() {
        let mut config = Config::default();
        config.db_path = ":memory:".to_string();
        config.connection_pool_size = 8;

        assert_eq!(effective_connection_pool_size(&config), 1);
    }

    struct TempSqliteFiles {
        db_path: PathBuf,
    }

    impl TempSqliteFiles {
        fn new(db_path: &Path) -> Self {
            Self {
                db_path: db_path.to_path_buf(),
            }
        }
    }

    impl Drop for TempSqliteFiles {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let candidate = if suffix.is_empty() {
                    self.db_path.clone()
                } else {
                    PathBuf::from(format!("{}{}", self.db_path.to_string_lossy(), suffix))
                };
                let _ = fs::remove_file(candidate);
            }
        }
    }

    fn unique_test_db_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "vldb-sqlite-{prefix}-{}-{unique}.db",
            std::process::id()
        ))
    }

    #[test]
    fn has_multiple_sql_statements_detects_single_statement() {
        assert!(!has_multiple_sql_statements("SELECT * FROM users"));
        assert!(!has_multiple_sql_statements("SELECT * FROM users WHERE name = 'a;b'"));
        assert!(!has_multiple_sql_statements("SELECT * FROM users WHERE name = \"a;b\""));
        assert!(!has_multiple_sql_statements("SELECT * FROM users -- comment; more\nWHERE id = 1"));
        assert!(!has_multiple_sql_statements("SELECT * FROM users /* comment ; here */ WHERE id = 1"));
        assert!(!has_multiple_sql_statements("INSERT INTO t VALUES ('hello''world')"));
    }

    #[test]
    fn has_multiple_sql_statements_detects_multiple_statements() {
        assert!(has_multiple_sql_statements("SELECT 1; SELECT 2"));
        assert!(has_multiple_sql_statements("SELECT 1; SELECT 2; SELECT 3"));
        assert!(has_multiple_sql_statements("INSERT INTO t VALUES (1); DELETE FROM t"));
    }

    #[test]
    fn has_multiple_sql_statements_handles_empty_and_whitespace() {
        assert!(!has_multiple_sql_statements(""));
        assert!(!has_multiple_sql_statements("   "));
        assert!(!has_multiple_sql_statements(";\n"));
        assert!(!has_multiple_sql_statements("SELECT 1"));
    }

    #[test]
    fn mask_sql_masks_single_quoted_strings() {
        assert_eq!(
            mask_sql("SELECT * FROM users WHERE name = 'secret'"),
            "SELECT * FROM users WHERE name = ***"
        );
    }

    #[test]
    fn mask_sql_handles_escaped_quotes() {
        assert_eq!(
            mask_sql("SELECT * FROM t WHERE name = 'it''s secret'"),
            "SELECT * FROM t WHERE name = ***"
        );
    }

    #[test]
    fn mask_sql_preserves_comments() {
        assert_eq!(
            mask_sql("SELECT * FROM t -- this is a comment"),
            "SELECT * FROM t -- this is a comment"
        );
    }

    #[test]
    fn mask_sql_handles_block_comments() {
        assert_eq!(
            mask_sql("SELECT /* hidden */ * FROM t"),
            "SELECT /* ... */ * FROM t"
        );
    }

    #[test]
    fn json_float_handles_nan_and_inf() {
        use super::json_float;
        assert!(json_float(f64::NAN).is_string());
        assert!(json_float(f64::INFINITY).is_string());
        assert!(json_float(f64::NEG_INFINITY).is_string());
        assert!(json_float(1.5).is_number());
    }
}
