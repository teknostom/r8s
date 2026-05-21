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

/// Media type clients send in their Accept header to request the proto-encoded
/// OpenAPI v2 document. This is the canonical k8s string.
const PROTO_OPENAPI_V2_ACCEPT: &str = "application/com.github.proto-openapi.spec.v2@v1.0+protobuf";

/// Content-Type advertised on the response. The canonical k8s string contains
/// '@', which Go's `mime.ParseMediaType` rejects as an invalid token character.
/// Older client-go paths (helm's OpenAPI fetcher, kubectl validators) run the
/// response Content-Type through that parser before handing the bytes back to
/// the caller, so advertising the canonical value causes those clients to
/// abort with "mime: unexpected content after media subtype" even though the
/// body is fine. Substituting '.' for '@' keeps the rest of the string
/// recognizable while making the value RFC 2045-compliant.
const PROTO_OPENAPI_V2_CONTENT_TYPE: &str =
    "application/com.github.proto-openapi.spec.v2.v1.0+protobuf";

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
            .header("content-type", PROTO_OPENAPI_V2_CONTENT_TYPE)
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
        defs.insert(definition_key(&rt), transform_for_v2(&rt));
    }
}

/// Build the OpenAPI v2 definition for a CRD from the user-provided
/// `openAPIV3Schema`. Mirrors what upstream kube-openapi does when publishing
/// CRD schemas to v2:
///
/// - At the *root* level, `x-kubernetes-preserve-unknown-fields:true` means
///   the whole CRD is a loose object. Emit bare `{type:object}` with no
///   GVK extension — the `works for CRD without validation schema` test
///   compares the published value byte-for-byte against that shape, and
///   kubectl explain falls back to /openapi/v3 (where the GVK *is* set) so
///   discovery still works.
/// - At non-root levels, *keep* `x-kubernetes-preserve-unknown-fields:true`.
///   kubectl client-side validation understands the extension and treats
///   anything inside that subtree as opaque (so a CR with random fields
///   inside `spec` doesn't trip "unknown field" or "unknown object type
///   nil" errors).
/// - For schemas that don't preserve at root, wrap the root with the
///   standard `apiVersion`/`kind`/`metadata` properties (CR objects always
///   carry these, but the user's schema doesn't list them, so kubectl
///   strict validation otherwise rejects valid CRs). Attach the GVK
///   extension so kubectl explain resolves GVR → schema via v2.
fn transform_for_v2(rt: &std::sync::Arc<r8s_types::ResourceType>) -> Value {
    let Some(mut schema) = rt.schema.clone() else {
        return serde_json::json!({"type": "object"});
    };
    if root_preserves_unknown_fields(&schema) {
        return serde_json::json!({"type": "object"});
    }
    // kubectl client-side strict validation doesn't honor
    // `x-kubernetes-preserve-unknown-fields` when `properties` are also
    // listed at that level — it just walks `properties` and reports any
    // sibling key as unknown. Drop `properties` (and `required`) at every
    // preserves-unknown level so kubectl has nothing to strict-validate
    // against, while leaving the extension itself in place for clients that
    // do honor it.
    drop_properties_at_preserve_levels(&mut schema);
    if let Some(obj) = schema.as_object_mut() {
        let props = obj
            .entry("properties".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(props) = props.as_object_mut() {
            props
                .entry("apiVersion".to_string())
                .or_insert_with(|| serde_json::json!({"type": "string"}));
            props
                .entry("kind".to_string())
                .or_insert_with(|| serde_json::json!({"type": "string"}));
            props.entry("metadata".to_string()).or_insert_with(|| {
                serde_json::json!({
                    "$ref": "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
                })
            });
        }
        obj.insert(
            "x-kubernetes-group-version-kind".to_string(),
            serde_json::json!([{
                "group": rt.gvr.group,
                "version": rt.gvr.version,
                "kind": rt.kind,
            }]),
        );
    }
    schema
}

fn root_preserves_unknown_fields(schema: &Value) -> bool {
    schema
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn drop_properties_at_preserve_levels(schema: &mut Value) {
    let Some(obj) = schema.as_object_mut() else {
        return;
    };
    let preserves = obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if preserves {
        // Reduce to just the preserve extension — no type, no properties,
        // no additionalProperties. Kubectl's strict validator otherwise
        // trips on `null` children with "unknown object type nil" because
        // it can't decide what schema applies. Stripping back to bare
        // preserve-unknown-fields tells the validator "treat this subtree
        // as opaque".
        obj.clear();
        obj.insert(
            "x-kubernetes-preserve-unknown-fields".to_string(),
            serde_json::json!(true),
        );
        return;
    }
    if let Some(props) = obj.get_mut("properties").and_then(|p| p.as_object_mut()) {
        for v in props.values_mut() {
            drop_properties_at_preserve_levels(v);
        }
    }
    if let Some(items) = obj.get_mut("items") {
        drop_properties_at_preserve_levels(items);
    }
    if let Some(ap) = obj.get_mut("additionalProperties") {
        drop_properties_at_preserve_levels(ap);
    }
}

/// Upstream's OpenAPI v2 definitions key for `Group/Version/Kind` is the
/// reversed-domain form of the group joined with version and kind by dots —
/// e.g. `stable.example.com/v6/Foo` becomes `com.example.stable.v6.Foo`.
/// Conformance polls this exact key (kube-openapi's `ToRESTFriendlyName`).
fn definition_key(rt: &std::sync::Arc<r8s_types::ResourceType>) -> String {
    to_rest_friendly_name(&rt.gvr.group, &rt.gvr.version, &rt.kind)
}

fn to_rest_friendly_name(group: &str, version: &str, kind: &str) -> String {
    let head = if group.contains('.') {
        let mut parts: Vec<&str> = group.split('.').collect();
        parts.reverse();
        parts.join(".")
    } else {
        group.to_string()
    };
    let mut name = String::with_capacity(head.len() + version.len() + kind.len() + 2);
    if !head.is_empty() {
        name.push_str(&head);
        name.push('.');
    }
    name.push_str(version);
    name.push('.');
    name.push_str(kind);
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_group_is_reversed() {
        assert_eq!(
            to_rest_friendly_name("stable.example.com", "v6", "Foo"),
            "com.example.stable.v6.Foo"
        );
    }

    #[test]
    fn single_segment_group_is_kept_as_is() {
        assert_eq!(to_rest_friendly_name("apps", "v1", "Deployment"), "apps.v1.Deployment");
    }

    #[test]
    fn empty_group_uses_version_kind_only() {
        assert_eq!(to_rest_friendly_name("", "v1", "Pod"), "v1.Pod");
    }
}

fn wants_proto(headers: &http::HeaderMap) -> bool {
    accept_has_mime(headers, |mime| mime == PROTO_OPENAPI_V2_ACCEPT)
}

fn accepts_json(headers: &http::HeaderMap) -> bool {
    if headers
        .get_all(http::header::ACCEPT)
        .iter()
        .next()
        .is_none()
    {
        return true;
    }
    accept_has_mime(headers, |mime| {
        mime == "application/json" || mime == "*/*" || mime == "application/*"
    })
}

fn accept_has_mime<F: Fn(&str) -> bool>(headers: &http::HeaderMap, f: F) -> bool {
    headers
        .get_all(http::header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .any(|part| {
            let mime = part.split(';').next().unwrap_or("").trim();
            f(mime)
        })
}
