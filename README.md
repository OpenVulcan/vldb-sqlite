# vldb-sqlite

`vldb-sqlite` is a Rust SQLite gateway that can be used as a gRPC service, an embeddable Rust library, or a C-compatible FFI dynamic library. It is part of the OpenVulcan local data gateway stack and focuses on predictable SQLite access from multi-language hosts.

The crate provides JSON result queries, Arrow IPC streaming queries, batch execution, script execution, SQLite FTS helpers, and stable FFI entry points for native callers.

## Features

- SQLite gRPC gateway built with `tonic`
- Embeddable Rust library API for local hosts
- C ABI / FFI exports for Go, C, Lua, and other native runtimes
- `ExecuteScript`, `ExecuteBatch`, `QueryJson`, and `QueryStream` execution paths
- Arrow IPC output for larger result sets
- JSON compatibility interface for scripting and diagnostics
- SQLite FTS5 helpers with Jieba tokenizer support
- Configurable PRAGMA and hardening options

## Installation

Add the crate to a Rust project:

```bash
cargo add vldb-sqlite
```

Or add it manually:

```toml
[dependencies]
vldb-sqlite = "0.1.5"
```

## Run the Gateway

Build and run the gRPC service from this repository:

```bash
cargo run --release -- --config ./vldb-sqlite.json
```

The default gRPC endpoint is:

```text
127.0.0.1:19501
```

Use `vldb-sqlite.json.example` as the starting point for a custom service configuration.

## Rust Library Usage

The library mode exposes the same SQLite execution core used by the gRPC service. Typical callers open a runtime/database handle and invoke the typed SQL execution helpers from the public modules.

Important modules:

- `runtime`: programmatic SQLite runtime and database opening options
- `sql_exec`: shared SQL execution core
- `fts`: FTS index and document helpers
- `tokenizer`: tokenizer and custom dictionary helpers
- `ffi`: exported FFI symbols and FFI-safe types

Generated protobuf types are available under `vldb_sqlite::pb`.

## FFI Usage

The crate also builds a `cdylib` and ships the C header in `include/vldb_sqlite.h`. Native hosts should prefer the non-JSON FFI entry points for hot paths and keep the JSON FFI interface for compatibility, scripting, and diagnostics.

Build the dynamic library:

```bash
cargo build --release
```

The platform-specific shared library is emitted under `target/release`.

## Documentation

- [English guide](https://github.com/OpenVulcan/vldb-sqlite/blob/main/docs/README.en.md)
- [Chinese guide](https://github.com/OpenVulcan/vldb-sqlite/blob/main/docs/README.zh-CN.md)
- [Library and FFI guide](https://github.com/OpenVulcan/vldb-sqlite/blob/main/docs/LIBRARY_USAGE.zh-CN.md)
- [gRPC integration guide](https://github.com/OpenVulcan/vldb-sqlite/blob/main/docs/grpc-integration.en.md)
- [Go FFI example](https://github.com/OpenVulcan/vldb-sqlite/tree/main/examples/go-ffi)
- [API documentation](https://docs.rs/vldb-sqlite)

## Package Contents

The crates.io package intentionally includes only the files required for Rust builds, generated documentation, protobuf compilation, FFI headers, and user-facing crate documentation. CI, Docker packaging, local runtime data, and release automation files are kept out of the published crate.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
