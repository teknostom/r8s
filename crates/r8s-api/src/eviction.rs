//! `POST pods/{name}/eviction`: delete a pod only if its PodDisruptionBudget
//! allows another disruption, decrementing the budget and recording the pod
//! as disrupted; otherwise 429 with a `DisruptionBudget` cause.

use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use r8s_store::{StoreError, backend::ResourceRef};
use r8s_types::{GroupVersionResource, Pod, PodDisruptionBudget, Time};
use serde_json::json;

use crate::handler::{PropagationPolicy, RouteContext, delete_impl, require_json};
use crate::response;
use crate::discovery::AppState;

pub async fn evict_pod(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let eviction = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Some(n) = eviction
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        && n != name
    {
        return response::status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "name in URL does not match name in Eviction object",
        );
    }
    let dry_run = eviction
        .get("deleteOptions")
        .and_then(|d| d.get("dryRun"))
        .and_then(|d| d.as_array())
        .is_some_and(|d| !d.is_empty());

    let pod_gvr = GroupVersionResource::pods();
    let pod: Pod = match state.store.get_as(&ResourceRef {
        gvr: &pod_gvr,
        namespace: Some(&ns),
        name: &name,
    }) {
        Ok(Some(p)) => p,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("pods \"{name}\" not found"),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
    };

    let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
    let can_ignore_pdb = matches!(phase, Some("Succeeded" | "Failed" | "Pending"))
        || pod.metadata.deletion_timestamp.is_some();
    if !can_ignore_pdb {
        let pdb_gvr = GroupVersionResource::pod_disruption_budgets();
        let pdbs: Vec<PodDisruptionBudget> = match state
            .store
            .list_as::<PodDisruptionBudget>(&pdb_gvr, Some(&ns))
        {
            Ok(list) => list
                .into_iter()
                .filter(|pdb| {
                    pdb.spec
                        .as_ref()
                        .and_then(|s| s.selector.as_ref())
                        .is_some_and(|sel| {
                            r8s_controllers::selector_matches(sel, pod.metadata.labels.as_ref())
                        })
                })
                .collect(),
            Err(err) => return response::anyhow_error_response(err),
        };
        if pdbs.len() > 1 {
            return response::json_response(
                500,
                &json!({
                    "kind": "Status", "apiVersion": "v1", "metadata": {},
                    "status": "Failure",
                    "message": "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support.",
                    "code": 500
                }),
            );
        }
        if let Some(pdb) = pdbs.into_iter().next()
            && let Some(resp) = check_and_decrement(&state, &pdb, &pod, &name, dry_run)
        {
            return resp;
        }
    }

    if dry_run {
        return success();
    }
    let resp = delete_impl(
        &state,
        &ctx,
        Some(&ns),
        &name,
        PropagationPolicy::Background,
        false,
    )
    .await;
    if resp.status().is_success() {
        success()
    } else {
        resp
    }
}

/// None when the budget allows the eviction (and has been charged for it).
fn check_and_decrement(
    state: &AppState,
    pdb: &PodDisruptionBudget,
    pod: &Pod,
    pod_name: &str,
    dry_run: bool,
) -> Option<Response> {
    let pdb_name = pdb.metadata.name.as_deref().unwrap_or("");
    let status = pdb.status.clone().unwrap_or_default();
    let spec = pdb.spec.clone().unwrap_or_default();

    // An unhealthy pod may go when the budget is already met.
    if !r8s_controllers::pod_ready(pod)
        && (spec.unhealthy_pod_eviction_policy.as_deref() == Some("AlwaysAllow")
            || (status.current_healthy >= status.desired_healthy && status.desired_healthy > 0))
    {
        return None;
    }
    if status.observed_generation.unwrap_or(0) < pdb.metadata.generation.unwrap_or(0) {
        return Some(too_many_requests(&format!(
            "The disruption budget {pdb_name} is still being processed by the server."
        )));
    }
    if status.disruptions_allowed <= 0 {
        return Some(too_many_requests(&format!(
            "The disruption budget {pdb_name} needs {} healthy pods and has {} currently",
            status.desired_healthy, status.current_healthy
        )));
    }
    if dry_run {
        return None;
    }

    let gvr = GroupVersionResource::pod_disruption_budgets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: pdb.metadata.namespace.as_deref(),
        name: pdb_name,
    };
    let mut value = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        _ => return None,
    };
    value["status"]["disruptionsAllowed"] = json!(status.disruptions_allowed - 1);
    if !value["status"]["disruptedPods"].is_object() {
        value["status"]["disruptedPods"] = json!({});
    }
    value["status"]["disruptedPods"][pod_name] = json!(Time(chrono::Utc::now()));
    if status.disruptions_allowed - 1 == 0
        && let Some(conds) = value["status"]["conditions"].as_array_mut()
        && let Some(c) = conds.iter_mut().find(|c| c["type"] == "DisruptionAllowed")
    {
        c["status"] = json!("False");
        c["reason"] = json!("InsufficientPods");
        c["lastTransitionTime"] = json!(Time(chrono::Utc::now()));
    }
    match state.store.update(&rref, &value) {
        Ok(_) => None,
        Err(StoreError::Conflict { .. }) => Some(too_many_requests(&format!(
            "The disruption budget {pdb_name} is still being processed by the server."
        ))),
        Err(err) => Some(response::anyhow_error_response(err)),
    }
}

fn too_many_requests(cause: &str) -> Response {
    response::json_response(
        429,
        &json!({
            "kind": "Status", "apiVersion": "v1", "metadata": {},
            "status": "Failure",
            "message": "Cannot evict pod as it would violate the pod's disruption budget.",
            "reason": "TooManyRequests",
            "details": {"causes": [{"reason": "DisruptionBudget", "message": cause}], "retryAfterSeconds": 0},
            "code": 429
        }),
    )
}

fn success() -> Response {
    response::json_response(
        200,
        &json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Success", "code": 200}),
    )
}
