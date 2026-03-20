use std::sync::Arc;

use axum::{
    Extension,
    body::{Body, Bytes},
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};
use hyper::StatusCode;
use r8s_store::{
    backend::ResourceRef,
    index::{FieldSelector, LabelSelector},
    watch::WatchEventType,
};
use r8s_types::ResourceType;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    discovery::AppState,
    params::ListParams,
    patch::json_merge_patch,
    protobuf::decode_k8s_protobuf,
    response::{self, status_error},
    table,
};
use axum::extract::Query;
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct RouteContext {
    pub resource_type: Arc<ResourceType>,
}

#[derive(Debug, Default, Deserialize)]
pub struct LogParams {
    pub container: Option<String>,
    #[serde(rename = "tailLines")]
    pub tail_lines: Option<u64>,
}

#[allow(clippy::result_large_err)]
pub(crate) fn require_json(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<serde_json::Value, Response> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if content_type.contains("protobuf") {
        return decode_k8s_protobuf(body).ok_or_else(|| {
            status_error(
                StatusCode::BAD_REQUEST,
                "Invalid",
                "failed to decode kubernetes protobuf body",
            )
        });
    }
    serde_json::from_slice(body).map_err(|e| {
        status_error(
            StatusCode::BAD_REQUEST,
            "Invalid",
            &format!("invalid body: {e}"),
        )
    })
}

fn wants_table(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("as=Table") && accept.contains("g=meta.k8s.io"))
}

pub(crate) fn get_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    headers: &HeaderMap,
) -> Response {
    let resource_ref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    match state.store.get(&resource_ref) {
        Ok(Some(obj)) => {
            if wants_table(headers) {
                table::single_object_table_response(
                    &table::columns_for(&ctx.resource_type.gvr.resource),
                    &obj,
                    &ctx.resource_type.gvr.resource,
                )
            } else {
                response::object_response(&obj)
            }
        }
        Ok(None) => response::status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("{} '{}' not found", ctx.resource_type.kind, name),
        ),
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn get_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, Some(&ns), &name, &headers)
}

pub async fn get_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, None, &name, &headers)
}

pub(crate) fn create_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    mut body: serde_json::Value,
) -> Response {
    let name = match body["metadata"]["name"].as_str() {
        Some(n) => n.to_string(),
        None => {
            return status_error(
                StatusCode::BAD_REQUEST,
                "Invalid",
                "metadata.name is required",
            );
        }
    };
    if let Some(ns) = namespace {
        body["metadata"]["namespace"] = serde_json::json!(ns);
    }

    if ctx.resource_type.gvr.resource == "services" {
        let svc_type = body["spec"]["type"].as_str().unwrap_or("ClusterIP");
        let has_cluster_ip = body["spec"]["clusterIP"]
            .as_str()
            .is_some_and(|ip| !ip.is_empty() && ip != "None");
        if (svc_type == "ClusterIP" || svc_type == "LoadBalancer" || svc_type == "NodePort")
            && !has_cluster_ip
        {
            let ip = state.allocate_cluster_ip();
            body["spec"]["clusterIP"] = serde_json::json!(ip);
            body["spec"]["clusterIPs"] = serde_json::json!([ip]);
        }
    }

    let resource_ref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name: &name,
    };
    match state.store.create(resource_ref, &body) {
        Ok(obj) => response::created_response(&obj),
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn create_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    create_impl(&state, &ctx, Some(&ns), body)
}

pub async fn create_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    create_impl(&state, &ctx, None, body)
}

pub(crate) fn update_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    body: serde_json::Value,
) -> Response {
    let resource_ref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    match state.store.update(&resource_ref, &body) {
        Ok(obj) => response::object_response(&obj),
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn update_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    update_impl(&state, &ctx, Some(&ns), &name, body)
}

pub async fn update_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    update_impl(&state, &ctx, None, &name, body)
}
pub(crate) fn delete_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
) -> Response {
    let rref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    match state.store.delete(&rref) {
        Ok(Some(obj)) => response::object_response(&obj),
        Ok(None) => response::status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("{} '{}' not found", ctx.resource_type.kind, name),
        ),
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn delete_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    delete_impl(&state, &ctx, Some(&ns), &name)
}

pub async fn delete_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
) -> Response {
    delete_impl(&state, &ctx, None, &name)
}

pub(crate) fn patch_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    bytes: Bytes,
) -> Response {
    let rref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    let patch: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(e) => {
            return response::status_error(
                StatusCode::BAD_REQUEST,
                "Invalid",
                &format!("invalid patch body: {e}"),
            );
        }
    };
    match state.store.get(&rref) {
        Ok(Some(mut current)) => {
            json_merge_patch(&mut current, &patch);
            match state.store.update(&rref, &current) {
                Ok(obj) => response::object_response(&obj),
                Err(err) => response::anyhow_error_response(err),
            }
        }
        Ok(None) => {
            // Server-side apply: create if not found
            let mut body = patch;
            body["metadata"]["name"] = serde_json::json!(name);
            if let Some(ns) = namespace {
                body["metadata"]["namespace"] = serde_json::json!(ns);
            }
            match state.store.create(rref, &body) {
                Ok(obj) => response::created_response(&obj),
                Err(err) => response::anyhow_error_response(err),
            }
        }
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn patch_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    patch_impl(&state, &ctx, Some(&ns), &name, body)
}

pub async fn patch_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    patch_impl(&state, &ctx, None, &name, body)
}

fn api_version(ctx: &RouteContext) -> String {
    if ctx.resource_type.gvr.group.is_empty() {
        ctx.resource_type.gvr.version.clone()
    } else {
        format!(
            "{}/{}",
            ctx.resource_type.gvr.group, ctx.resource_type.gvr.version
        )
    }
}

pub(crate) fn list_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    params: ListParams,
    headers: &HeaderMap,
) -> Response {
    if params.is_watch() {
        return watch_impl(state, ctx, namespace);
    }

    let label_sel = params
        .label_selector
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(LabelSelector::parse)
        .transpose();
    let label_sel = match label_sel {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };

    let field_sel = params
        .field_selector
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(FieldSelector::parse)
        .transpose();
    let field_sel = match field_sel {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };

    let limit = params.limit.map(|l| l as usize);

    match state.store.list(
        &ctx.resource_type.gvr,
        namespace,
        label_sel.as_ref(),
        field_sel.as_ref(),
        limit,
        params.continue_token.as_deref(),
    ) {
        Ok(result) => {
            if wants_table(headers) {
                table::table_response(
                    &table::columns_for(&ctx.resource_type.gvr.resource),
                    &result.items,
                    &ctx.resource_type.gvr.resource,
                    Some(result.resource_version),
                )
            } else {
                response::list_response(
                    &api_version(ctx),
                    &ctx.resource_type.kind,
                    result.resource_version,
                    result.continue_token.as_deref(),
                    result.items,
                )
            }
        }
        Err(err) => response::anyhow_error_response(err),
    }
}

fn watch_impl(state: &AppState, ctx: &RouteContext, namespace: Option<&str>) -> Response {
    // Subscribe before listing so we don't miss events between the list and the watch.
    let rx = state.store.watch(&ctx.resource_type.gvr);

    let (items, rv) = state
        .store
        .list(&ctx.resource_type.gvr, namespace, None, None, None, None)
        .map(|r| (r.items, r.resource_version))
        .unwrap_or_default();

    let initial = items
        .into_iter()
        .map(|obj| Ok::<_, std::io::Error>(response::watch_event_line("ADDED", &obj)));
    let initial_stream = tokio_stream::iter(initial);

    let av = api_version(ctx);
    let bookmark = tokio_stream::iter(std::iter::once(Ok::<_, std::io::Error>(
        response::watch_event_line(
            "BOOKMARK",
            &serde_json::json!({
                "apiVersion": av,
                "kind": ctx.resource_type.kind,
                "metadata": {"resourceVersion": rv.to_string()}
            }),
        ),
    )));

    let ns_filter: Option<String> = namespace.map(|s| s.to_string());
    // On broadcast lag, terminate so the client reconnects (standard K8s behavior).
    let live_stream = BroadcastStream::new(rx)
        .take_while(|result| result.is_ok())
        .filter_map(move |result| {
            let event = result.ok()?;
            if let Some(ref ns) = ns_filter
                && event.object["metadata"]["namespace"].as_str() != Some(ns.as_str())
            {
                return None;
            }
            let type_str = match event.event_type {
                WatchEventType::Added => "ADDED",
                WatchEventType::Modified => "MODIFIED",
                WatchEventType::Deleted => "DELETED",
            };
            Some(Ok::<_, std::io::Error>(response::watch_event_line(
                type_str,
                &event.object,
            )))
        });

    let stream = initial_stream.chain(bookmark).chain(live_stream);

    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .header("transfer-encoding", "chunked")
        .body(Body::from_stream(stream))
        .expect("valid response")
}

pub async fn list_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    Path(ns): Path<String>,
    headers: HeaderMap,
) -> Response {
    list_impl(&state, &ctx, Some(&ns), params, &headers)
}

pub async fn list_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Response {
    list_impl(&state, &ctx, None, params, &headers)
}

pub async fn list_all_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Response {
    list_impl(&state, &ctx, None, params, &headers)
}

pub async fn pod_logs_ns(
    State(state): State<AppState>,
    Path((ns, name)): Path<(String, String)>,
    Query(params): Query<LogParams>,
) -> Response {
    let gvr = r8s_types::GroupVersionResource::new("", "v1", "pods");
    let resource_ref = r8s_store::backend::ResourceRef {
        gvr: &gvr,
        namespace: Some(&ns),
        name: &name,
    };

    let pod = match state.store.get(&resource_ref) {
        Ok(Some(p)) => p,
        Ok(None) => {
            return status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("pod '{name}' not found"),
            );
        }
        Err(e) => return response::anyhow_error_response(e),
    };

    let statuses = pod["status"]["containerStatuses"].as_array();
    let status = statuses.and_then(|s| {
        if let Some(ref c) = params.container {
            s.iter().find(|cs| cs["name"].as_str() == Some(c.as_str()))
        } else {
            s.first()
        }
    });

    let container_id = match status.and_then(|s| s["containerID"].as_str()) {
        Some(id) => id,
        None => {
            return Response::builder()
                .status(200)
                .header("content-type", "text/plain")
                .body(Body::empty())
                .expect("valid response");
        }
    };

    let log_path = state
        .data_dir
        .join("logs")
        .join(format!("{container_id}.stdout"));
    let content = tokio::fs::read_to_string(&log_path)
        .await
        .unwrap_or_default();

    let output = if let Some(tail) = params.tail_lines {
        let lines: Vec<&str> = content.lines().collect();
        let start = lines.len().saturating_sub(tail as usize);
        lines[start..].join("\n")
    } else {
        content
    };

    Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Body::from(output))
        .expect("valid response")
}
