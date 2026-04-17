mod db_lock;

pub mod ffi;
pub mod fts;
pub mod library;
pub mod runtime;
pub mod tokenizer;

/// 由库模式直接导出的 protobuf 模块。
/// Protobuf module exported directly by library mode.
pub mod pb {
    tonic::include_proto!("vldb.sqlite.v1");
}
