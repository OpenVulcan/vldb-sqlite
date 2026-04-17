fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: the build script sets PROTOC once during process startup, before any
    // threads are spawned or foreign code observes the environment.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=Rstrtmgr");
    }

    println!("cargo:rerun-if-changed=proto/v1/sqlite.proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .bytes(".vldb.sqlite.v1.QueryResponse.arrow_ipc_chunk")
        .compile_protos(&["proto/v1/sqlite.proto"], &["proto"])?;

    Ok(())
}
