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
    for rt in resources {
        if let Some(schema) = rt.schema.clone() {
            schemas.insert(schema_key(&rt), schema);
        }
    }
    serde_json::json!({
        "openapi": "3.0.0",
        "info": {
            "title": format!("r8s {group}/{version}"),
            "version": "v1.32.0-r8s",
        },
        "paths": {},
        "components": {
            "schemas": schemas,
        },
    })
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
