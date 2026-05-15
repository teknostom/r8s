//! Handlers for the `/scale` subresource on Deployments / ReplicaSets /
//! StatefulSets. The Scale resource lives under `autoscaling/v1` but is
//! advertised under its parent's group/version so clients (kubectl scale,
//! HorizontalPodAutoscaler) can resolve it from the parent's URL.
//!
//! GET reads the parent's `.spec.replicas` and reports `.status.replicas`.
//! PUT replaces, PATCH merges into the parent's `.spec.replicas` and goes
//! through the regular store update path so watches fire normally.
//!
//! The mapping from URL path segment to parent GVR:
//!   deployments  → apps/v1/deployments
//!   replicasets  → apps/v1/replicasets
//!   statefulsets → apps/v1/statefulsets

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use r8s_store::backend::ResourceRef;
use r8s_types::GroupVersionResource;
use serde_json::Value;

use crate::discovery::AppState;
use crate::patch::json_merge_patch;
use crate::response::{self, object_response, status_error};

fn gvr_for(resource: &str) -> Option<GroupVersionResource> {
    match resource {
        "deployments" => Some(GroupVersionResource::deployments()),
        "replicasets" => Some(GroupVersionResource::replica_sets()),
        "statefulsets" => Some(GroupVersionResource::stateful_sets()),
        _ => None,
    }
}

fn selector_to_string(selector: &Value) -> String {
    let Some(match_labels) = selector.get("matchLabels").and_then(|v| v.as_object()) else {
        return String::new();
    };
    let mut parts: Vec<String> = match_labels
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|val| format!("{k}={val}")))
        .collect();
    parts.sort();
    parts.join(",")
}

fn build_scale(parent: &Value, namespace: &str, name: &str) -> Value {
    let spec_replicas = parent
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let status_replicas = parent
        .get("status")
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_i64())
        .unwrap_or(spec_replicas);
    let selector = selector_to_string(
        parent
            .get("spec")
            .and_then(|s| s.get("selector"))
            .unwrap_or(&Value::Null),
    );
    let resource_version = parent
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .cloned()
        .unwrap_or_else(|| Value::String(String::new()));
    let uid = parent
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .cloned()
        .unwrap_or_else(|| Value::String(String::new()));

    serde_json::json!({
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "resourceVersion": resource_version,
            "uid": uid,
        },
        "spec": {"replicas": spec_replicas},
        "status": {
            "replicas": status_replicas,
            "selector": selector,
        },
    })
}

pub async fn get_scale(
    State(state): State<AppState>,
    Path((ns, resource, name)): Path<(String, String, String)>,
) -> Response {
    let Some(gvr) = gvr_for(&resource) else {
        return status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("scale not supported for '{resource}'"),
        );
    };
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some(&ns),
        name: &name,
    };
    match state.store.get(&rref) {
        Ok(Some(p)) => object_response(&build_scale(&p, &ns, &name)),
        Ok(None) => status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("{resource}/{name} not found"),
        ),
        Err(e) => response::anyhow_error_response(e),
    }
}

pub async fn put_scale(
    State(state): State<AppState>,
    Path((ns, resource, name)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    let Some(gvr) = gvr_for(&resource) else {
        return status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("scale not supported for '{resource}'"),
        );
    };
    let incoming: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return status_error(StatusCode::BAD_REQUEST, "BadRequest", &e.to_string()),
    };
    let replicas = incoming
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_i64());
    apply_replicas(&state, &gvr, &resource, &ns, &name, replicas).await
}

pub async fn patch_scale(
    State(state): State<AppState>,
    Path((ns, resource, name)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    let Some(gvr) = gvr_for(&resource) else {
        return status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("scale not supported for '{resource}'"),
        );
    };
    let patch: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return status_error(StatusCode::BAD_REQUEST, "BadRequest", &e.to_string()),
    };

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some(&ns),
        name: &name,
    };
    let parent = match state.store.get(&rref) {
        Ok(Some(p)) => p,
        Ok(None) => {
            return status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{resource}/{name} not found"),
            );
        }
        Err(e) => return response::anyhow_error_response(e),
    };

    let mut current_scale = build_scale(&parent, &ns, &name);
    json_merge_patch(&mut current_scale, &patch);

    let replicas = current_scale
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_i64());
    apply_replicas(&state, &gvr, &resource, &ns, &name, replicas).await
}

async fn apply_replicas(
    state: &AppState,
    gvr: &GroupVersionResource,
    resource: &str,
    ns: &str,
    name: &str,
    replicas: Option<i64>,
) -> Response {
    let Some(replicas) = replicas else {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "scale spec.replicas missing or not an integer",
        );
    };
    if replicas < 0 {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "scale spec.replicas must be >= 0",
        );
    }

    let rref = ResourceRef {
        gvr,
        namespace: Some(ns),
        name,
    };
    let mut parent = match state.store.get(&rref) {
        Ok(Some(p)) => p,
        Ok(None) => {
            return status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{resource}/{name} not found"),
            );
        }
        Err(e) => return response::anyhow_error_response(e),
    };

    if let Some(spec) = parent.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("replicas".to_string(), serde_json::json!(replicas));
    } else {
        return status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("{resource}/{name} has no spec"),
        );
    }

    match state.store.update(&rref, &parent) {
        Ok(updated) => object_response(&build_scale(&updated, ns, name)),
        Err(e) => response::anyhow_error_response(e),
    }
}
