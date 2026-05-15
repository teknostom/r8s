//! Compiles `proto/OpenAPIv2.proto` (the gnostic schema for Swagger 2.0) into
//! Rust types via prost. The generated code lands in `$OUT_DIR/openapi.v2.rs`
//! and is included by `crates/r8s-types/src/openapi_proto.rs`.

fn main() {
    println!("cargo:rerun-if-changed=proto/OpenAPIv2.proto");
    prost_build::compile_protos(&["proto/OpenAPIv2.proto"], &["proto"])
        .expect("failed to compile OpenAPIv2.proto — make sure protoc is installed and on PATH");
}
