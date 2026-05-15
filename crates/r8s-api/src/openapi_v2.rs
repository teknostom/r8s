//! Serves OpenAPI v2 (Swagger 2.0) at `/openapi/v2`.
//!
//! Composition: load the vendored upstream `swagger.json` (built-in resources)
//! and merge in every CRD schema from the registry under `definitions`. The
//! base doc is parsed once and reused; the per-request work is just cloning
//! that value and adding entries for each CRD with a schema attached.
//!
//! Content negotiation:
//! - `Accept: application/json` or wildcard → composed JSON doc
//! - `Accept: application/com.github.proto-openapi.spec.v2@v1.0+protobuf` →
//!   the same doc encoded with the gnostic protobuf schema (this is the
//!   default for client-go-based clients like Helm)
//! - Other Accept-only headers → 406

use std::sync::OnceLock;

use axum::{
    body::Body,
    extract::State,
    http::{self, StatusCode},
    response::Response,
};
use prost::Message;
use serde_json::Value;

use crate::discovery::AppState;
use crate::response::{json_response, status_error};

/// Media type gnostic uses for OpenAPI v2 protobuf. This is what client-go
/// sends in its default Accept header.
const PROTO_OPENAPI_V2: &str = "application/com.github.proto-openapi.spec.v2@v1.0+protobuf";

static BASE_V2: OnceLock<Value> = OnceLock::new();

fn base_v2() -> &'static Value {
    BASE_V2.get_or_init(|| {
        serde_json::from_slice(r8s_types::openapi::SWAGGER_V2_JSON).unwrap_or_else(|e| {
            tracing::error!("failed to parse vendored swagger.json: {e}");
            serde_json::json!({
                "swagger": "2.0",
                "info": {"title": "r8s", "version": "v1.32.0-r8s"},
                "definitions": {},
                "paths": {},
            })
        })
    })
}

/// GET /openapi/v2 — composed Swagger 2.0 doc covering built-ins + CRDs.
pub async fn get_openapi_v2(headers: http::HeaderMap, State(state): State<AppState>) -> Response {
    let prefers_proto = wants_proto(&headers);
    let accepts_json = accepts_json(&headers);

    if !prefers_proto && !accepts_json {
        return status_error(
            StatusCode::NOT_ACCEPTABLE,
            "NotAcceptable",
            "r8s serves OpenAPI v2 as application/json or \
             application/com.github.proto-openapi.spec.v2@v1.0+protobuf",
        );
    }

    let mut doc = base_v2().clone();
    merge_crd_definitions(&state, &mut doc);

    if prefers_proto {
        let pb = r8s_types::openapi_proto::json_to_document(&doc);
        let mut buf = Vec::with_capacity(pb.encoded_len());
        if let Err(e) = pb.encode(&mut buf) {
            tracing::error!("openapi v2 protobuf encode failed: {e}");
            return status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "failed to encode OpenAPI v2 as protobuf",
            );
        }
        return Response::builder()
            .status(200)
            .header("content-type", PROTO_OPENAPI_V2)
            .body(Body::from(buf))
            .expect("valid response");
    }

    json_response(200, &doc)
}

fn merge_crd_definitions(state: &AppState, doc: &mut Value) {
    let Some(defs) = doc.get_mut("definitions").and_then(|v| v.as_object_mut()) else {
        return;
    };
    for rt in state.registry.iter() {
        // Built-in schemas are already in the vendored definitions; only fold
        // in resources whose GV isn't covered by a vendored spec (CRDs).
        if r8s_types::openapi::spec_bytes_for(&rt.gvr.group, &rt.gvr.version).is_some() {
            continue;
        }
        let Some(schema) = rt.schema.clone() else {
            continue;
        };
        defs.insert(definition_key(&rt), schema);
    }
}

fn definition_key(rt: &std::sync::Arc<r8s_types::ResourceType>) -> String {
    if rt.gvr.group.is_empty() {
        format!("{}.{}", rt.gvr.version, rt.kind)
    } else {
        format!("{}.{}.{}", rt.gvr.group, rt.gvr.version, rt.kind)
    }
}

fn wants_proto(headers: &http::HeaderMap) -> bool {
    accept_has_mime(headers, |mime| mime == PROTO_OPENAPI_V2)
}

fn accepts_json(headers: &http::HeaderMap) -> bool {
    if headers.get(http::header::ACCEPT).is_none() {
        return true;
    }
    accept_has_mime(headers, |mime| {
        mime == "application/json" || mime == "*/*" || mime == "application/*"
    })
}

fn accept_has_mime<F: Fn(&str) -> bool>(headers: &http::HeaderMap, f: F) -> bool {
    headers
        .get(http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',').any(|part| {
                let mime = part.split(';').next().unwrap_or("").trim();
                f(mime)
            })
        })
        .unwrap_or(false)
}
