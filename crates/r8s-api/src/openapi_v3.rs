//! Serves OpenAPI v3 discovery and per-GroupVersion schema documents.
//!
//! Discovery (`/openapi/v3`) lists every group/version in the registry with a
//! `serverRelativeURL` that carries a content hash, so clients can cache the
//! per-GV docs and skip re-fetching when nothing changed.
//!
//! Per-GV docs (`/openapi/v3/api/v1` and `/openapi/v3/apis/{group}/{version}`)
//! are served two ways:
//! - For built-in group/versions r8s has vendored upstream specs for, we serve
//!   the upstream file verbatim — it already contains all the schemas plus the
//!   internal $refs they depend on.
//! - For CRD-defined group/versions, we synthesize a minimal v3 doc from the
//!   schemas attached to the registry's ResourceTypes.
//!
//! The hash on the discovery URLs is SHA-256 of the per-GV doc body, hex,
//! truncated to 16 chars — matches what `kubectl` and `helm` expect.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    response::Response,
};
use rustc_hash::FxHashSet;
use sha2::{Digest, Sha256};

use crate::discovery::AppState;
use crate::response::object_response;
use r8s_types::ResourceType;

/// GET /openapi/v3 — discovery doc enumerating every served group/version.
pub async fn get_openapi_v3_discovery(State(state): State<AppState>) -> Response {
    let mut paths = serde_json::Map::new();

    for (path, body) in collect_gv_docs(&state) {
        let hash = short_hash(&body);
        paths.insert(
            path.clone(),
            serde_json::json!({
                "serverRelativeURL": format!("/openapi/v3/{path}?hash={hash}"),
            }),
        );
    }

    object_response(&serde_json::json!({ "paths": paths }))
}

/// GET /openapi/v3/api/v1 — core group's spec.
pub async fn get_openapi_v3_core(State(state): State<AppState>) -> Response {
    let body = doc_for(&state, "", "v1");
    object_response(&body)
}

/// GET /openapi/v3/apis/{group}/{version} — named group's spec.
pub async fn get_openapi_v3_group(
    State(state): State<AppState>,
    Path((group, version)): Path<(String, String)>,
) -> Response {
    let body = doc_for(&state, &group, &version);
    object_response(&body)
}

/// Compute every served group/version doc as `(path, body)` pairs. Used by
/// the discovery endpoint to build URLs and per-doc hashes.
fn collect_gv_docs(state: &AppState) -> Vec<(String, serde_json::Value)> {
    let mut seen: FxHashSet<(String, String)> = FxHashSet::default();
    let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for rt in state.registry.iter() {
        let gv = (rt.gvr.group.clone(), rt.gvr.version.clone());
        if !seen.insert(gv.clone()) {
            continue;
        }
        let path = gv_path(&gv.0, &gv.1);
        out.insert(path, doc_for(state, &gv.0, &gv.1));
    }
    out.into_iter().collect()
}

fn gv_path(group: &str, version: &str) -> String {
    if group.is_empty() {
        format!("api/{version}")
    } else {
        format!("apis/{group}/{version}")
    }
}

/// Build a v3 doc for a single (group, version). Prefers the vendored upstream
/// spec; falls back to synthesizing from registry schemas (CRDs).
fn doc_for(state: &AppState, group: &str, version: &str) -> serde_json::Value {
    if let Some(bytes) = r8s_types::openapi::spec_bytes_for(group, version)
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes)
    {
        return v;
    }
    synthesize_doc(&state.registry, group, version)
}

/// Build an OpenAPI v3 doc from registry schemas for the given GV. Used for
/// CRD-defined groups r8s has no vendored upstream spec for.
fn synthesize_doc(
    registry: &r8s_types::registry::ResourceRegistry,
    group: &str,
    version: &str,
) -> serde_json::Value {
    let resources = registry.resources_for_group_version(group, version);
    let mut schemas = serde_json::Map::new();
    let mut paths = serde_json::Map::new();
    // Copy in the ObjectMeta family from core/v1. The synthesized CRD schemas
    // $ref into ObjectMeta for `metadata`, and kubectl resolves $ref locally
    // within the doc — without these the `kubectl explain CRD.metadata`
    // conformance check finds no fields under metadata.
    inject_metav1_schemas(&mut schemas);
    for rt in resources {
        let key = schema_key(&rt);
        // kubectl explain (1.32+) reads /openapi/v3 and looks up GVR via the
        // `paths` map, then follows the GET response `$ref` to the schema.
        // Always synthesize a schema entry — even CRDs with no validation
        // schema or with x-kubernetes-preserve-unknown-fields at the root
        // need a discoverable schema for explain.
        let mut schema = rt
            .schema
            .clone()
            .unwrap_or_else(|| serde_json::json!({"type": "object"}));
        if let Some(obj) = schema.as_object_mut() {
            // CR objects always carry apiVersion/kind/metadata, but the
            // user's openAPIV3Schema doesn't list them. kubectl explain
            // walks the published schema's `properties`, so without these
            // fields the explain output misses the standard envelope
            // (which the conformance test asserts is present).
            let props = obj
                .entry("properties".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if let Some(props) = props.as_object_mut() {
                // Canonical descriptions verbatim from upstream k8s. The
                // conformance suite checks `kubectl explain` output for
                // these substrings ("APIVersion defines...", "Kind is a
                // string...") and fails if they're missing.
                props
                    .entry("apiVersion".to_string())
                    .or_insert_with(|| serde_json::json!({
                        "type": "string",
                        "description": "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
                    }));
                props
                    .entry("kind".to_string())
                    .or_insert_with(|| serde_json::json!({
                        "type": "string",
                        "description": "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
                    }));
                props.entry("metadata".to_string()).or_insert_with(|| {
                    // OpenAPI v3 forbids sibling fields next to `$ref`, so
                    // upstream wraps the ref in `allOf` to let the override
                    // description survive. Without this, kubectl explain
                    // surfaces the ObjectMeta schema's own description
                    // ("ObjectMeta is metadata that...") instead of the
                    // standard "Standard object's metadata..." text the
                    // conformance regex matches against.
                    serde_json::json!({
                        "allOf": [
                            {"$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"}
                        ],
                        "default": {},
                        "description": "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata",
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
        schemas.insert(key.clone(), schema);

        // Synthesize the single-item GET path. kubectl's openapi3 explain
        // walks `paths`, matches by the operation's GVK extension, and
        // follows the response schema $ref. We don't need every CRUD verb —
        // one GET that names the GVK is enough.
        let path_str = if rt.namespaced {
            format!(
                "/apis/{}/{}/namespaces/{{namespace}}/{}/{{name}}",
                rt.gvr.group, rt.gvr.version, rt.gvr.resource
            )
        } else {
            format!(
                "/apis/{}/{}/{}/{{name}}",
                rt.gvr.group, rt.gvr.version, rt.gvr.resource
            )
        };
        let ref_str = format!("#/components/schemas/{key}");
        paths.insert(
            path_str,
            serde_json::json!({
                "get": {
                    "operationId": format!("read{}{}{}", rt.gvr.group, rt.gvr.version, rt.kind),
                    "responses": {
                        "200": {
                            "description": "OK",
                            "content": {
                                "application/json": {"schema": {"$ref": ref_str}},
                                "application/yaml": {"schema": {"$ref": ref_str}},
                            }
                        }
                    },
                    "x-kubernetes-action": "get",
                    "x-kubernetes-group-version-kind": {
                        "group": rt.gvr.group,
                        "version": rt.gvr.version,
                        "kind": rt.kind,
                    }
                }
            }),
        );
    }
    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": format!("r8s {group}/{version}"),
            "version": "v1.32.0-r8s",
        },
        "paths": paths,
        "components": {
            "schemas": schemas,
        },
    })
}

/// Copy every `io.k8s.apimachinery.pkg.apis.meta.v1.*` schema from the
/// vendored core/v1 OpenAPI v3 doc into the target map. Synthesized CRD
/// docs $ref into these (notably ObjectMeta for `metadata`), and kubectl
/// resolves $refs locally per-doc — without copying them over, kubectl
/// explain can't walk into `metadata`.
fn inject_metav1_schemas(out: &mut serde_json::Map<String, serde_json::Value>) {
    let Some(bytes) = r8s_types::openapi::spec_bytes_for("", "v1") else {
        return;
    };
    let Ok(doc) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return;
    };
    let Some(schemas) = doc
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(|s| s.as_object())
    else {
        return;
    };
    for (k, v) in schemas {
        if k.starts_with("io.k8s.apimachinery.pkg.apis.meta.v1.")
            || k.starts_with("io.k8s.apimachinery.pkg.runtime.")
            || k.starts_with("io.k8s.apimachinery.pkg.util.intstr.")
            || k.starts_with("io.k8s.apimachinery.pkg.api.resource.")
        {
            out.insert(k.clone(), v.clone());
        }
    }
}

/// Naming scheme for synthesized CRD schemas. Kubernetes upstream uses
/// reverse-DNS-style keys (e.g. `io.k8s.api.core.v1.Pod`); for CRDs the
/// convention is less rigid, so we use `<group>.<version>.<kind>` for
/// readability.
fn schema_key(rt: &Arc<ResourceType>) -> String {
    if rt.gvr.group.is_empty() {
        format!("{}.{}", rt.gvr.version, rt.kind)
    } else {
        format!("{}.{}.{}", rt.gvr.group, rt.gvr.version, rt.kind)
    }
}

fn short_hash(v: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(v).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    let hex: String = digest.iter().map(|b| format!("{b:02X}")).collect();
    hex.chars().take(16).collect()
}
