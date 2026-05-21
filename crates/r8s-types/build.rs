//! Compiles two distinct protobuf trees:
//!
//! 1. `proto/OpenAPIv2.proto` — the gnostic schema for Swagger 2.0. Used to
//!    serve `/openapi/v2` in protobuf form. Stored at `$OUT_DIR/openapi.v2.rs`,
//!    included by `crates/r8s-types/src/openapi_proto.rs`.
//!
//! 2. `proto/k8s.io/**/*.proto` — Kubernetes' generated protobuf schemas at
//!    v1.32.0. We compile them with serde derives so that decoded messages
//!    serialize to the same JSON shape kubectl/client-go produce, which is
//!    what r8s' store and HTTP layer speak. Stored at
//!    `$OUT_DIR/k8s.io.*.rs`, included by `crates/r8s-types/src/k8s_pb.rs`.
//!
//! The k8s tree is what `r8s-api` uses to round-trip protobuf request bodies
//! (CRD POSTs, etc.) without silently dropping the resource's spec.

use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=proto/OpenAPIv2.proto");
    prost_build::compile_protos(&["proto/OpenAPIv2.proto"], &["proto"])
        .expect("failed to compile OpenAPIv2.proto — make sure protoc is installed and on PATH");

    let proto_root = PathBuf::from("proto");
    let k8s_protos = [
        "k8s.io/apimachinery/pkg/runtime/schema/generated.proto",
        "k8s.io/apimachinery/pkg/runtime/generated.proto",
        "k8s.io/apimachinery/pkg/util/intstr/generated.proto",
        "k8s.io/apimachinery/pkg/api/resource/generated.proto",
        "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto",
        "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto",
    ];
    for p in &k8s_protos {
        println!("cargo:rerun-if-changed=proto/{p}");
    }

    let mut cfg = prost_build::Config::new();
    // Plain prost types — no auto serde. The JSON shape for k8s resources has
    // acronym and `x-kubernetes-*` field-name conventions that prost's
    // `rename_all = "camelCase"` can't reproduce (e.g. `openAPIV3Schema`,
    // `serverAddressByClientCIDRs`). Each resource type that needs protobuf
    // round-trip gets a hand-rolled converter in `src/k8s_pb_serde.rs` that
    // walks the prost-generated struct and produces correct JSON.
    //
    // `include_file` collapses every package into one `k8s_pb.rs` with the
    // full nested `mod k8s::io::...` hierarchy, so prost's cross-package
    // `super::super::...` references resolve correctly.
    cfg.include_file("k8s_pb.rs");
    cfg.compile_protos(
        &k8s_protos
            .iter()
            .map(|p| proto_root.join(p))
            .collect::<Vec<_>>(),
        &[proto_root],
    )
    .expect("failed to compile k8s .proto files");
}
