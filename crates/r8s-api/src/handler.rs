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

fn random_name_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
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

/// Real k8s allocates a ClusterIP for every `Service` with a real (non-`None`)
/// `clusterIP` that doesn't already have one — including `LoadBalancer` and
/// `NodePort` types. Called from both POST and server-side-apply create paths.
fn maybe_allocate_cluster_ip(state: &AppState, ctx: &RouteContext, body: &mut serde_json::Value) {
    if ctx.resource_type.gvr.resource != "services" {
        return;
    }
    let svc_type = body
        .get("spec")
        .and_then(|s| s.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("ClusterIP");
    let has_cluster_ip = body
        .get("spec")
        .and_then(|s| s.get("clusterIP"))
        .and_then(|v| v.as_str())
        .is_some_and(|ip| !ip.is_empty() && ip != "None");
    if !matches!(svc_type, "ClusterIP" | "LoadBalancer" | "NodePort") || has_cluster_ip {
        return;
    }
    let ip = state.allocate_cluster_ip();
    if let Some(spec) = body.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("clusterIP".to_string(), serde_json::json!(ip));
        spec.insert("clusterIPs".to_string(), serde_json::json!([ip]));
    }
}

pub(crate) fn create_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    mut body: serde_json::Value,
) -> Response {
    let name = match body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
    {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            // No explicit name — synthesize one from metadata.generateName if
            // present, matching the k8s convention of `<prefix><5-rand>`.
            let prefix = body
                .get("metadata")
                .and_then(|m| m.get("generateName"))
                .and_then(|n| n.as_str())
                .filter(|p| !p.is_empty());
            match prefix {
                Some(prefix) => {
                    let generated = format!("{prefix}{}", random_name_suffix());
                    if let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut()) {
                        meta.insert("name".to_string(), serde_json::json!(generated));
                    }
                    generated
                }
                None => {
                    return status_error(
                        StatusCode::BAD_REQUEST,
                        "Invalid",
                        "metadata.name or metadata.generateName is required",
                    );
                }
            }
        }
    };
    if let Some(ns) = namespace
        && let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut())
    {
        meta.insert("namespace".to_string(), serde_json::json!(ns));
    }

    maybe_allocate_cluster_ip(state, ctx, &mut body);
    if ctx.resource_type.gvr.resource == "pods" {
        r8s_controllers::pod_admission::inject_sa_token(&state.store, &mut body);
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
            if let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut()) {
                meta.insert("name".to_string(), serde_json::json!(name));
                if let Some(ns) = namespace {
                    meta.insert("namespace".to_string(), serde_json::json!(ns));
                }
            } else if let Some(obj) = body.as_object_mut() {
                let mut meta = serde_json::Map::new();
                meta.insert("name".to_string(), serde_json::json!(name));
                if let Some(ns) = namespace {
                    meta.insert("namespace".to_string(), serde_json::json!(ns));
                }
                obj.insert("metadata".to_string(), serde_json::Value::Object(meta));
            }
            maybe_allocate_cluster_ip(state, ctx, &mut body);
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
        return watch_impl(
            state,
            ctx,
            namespace,
            headers,
            params.resource_version.as_deref(),
            params.wants_initial_events(),
        );
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

fn watch_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    headers: &HeaderMap,
    resource_version: Option<&str>,
    send_initial_events: bool,
) -> Response {
    // Subscribe before listing so we don't miss events between the list and the watch.
    let rx = state.store.watch(&ctx.resource_type.gvr);

    // When `sendInitialEvents=true` (k9s and the WatchList protocol), the
    // client expects the current state streamed as ADDED events followed by
    // a sentinel BOOKMARK before live events resume — even if a resource
    // version was supplied. Otherwise, a client that has already done a list
    // (or passed an explicit rv) does NOT want the initial state replayed
    // (that's what gives `kubectl get -w` duplicate rows).
    let replay_initial =
        send_initial_events || matches!(resource_version, None | Some("") | Some("0"));

    let (items, rv) = if replay_initial {
        state
            .store
            .list(&ctx.resource_type.gvr, namespace, None, None, None, None)
            .map(|r| (r.items, r.resource_version))
            .unwrap_or_default()
    } else {
        let parsed = resource_version
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        (Vec::new(), parsed)
    };

    let as_table = wants_table(headers);
    let resource = ctx.resource_type.gvr.resource.clone();
    let columns = if as_table {
        Some(std::sync::Arc::new(table::columns_for(&resource)))
    } else {
        None
    };
    let format_object = {
        let columns = columns.clone();
        let resource = resource.clone();
        move |obj: &serde_json::Value| -> serde_json::Value {
            match columns.as_ref() {
                Some(cols) => table::watch_table_object(cols, obj, &resource),
                None => obj.clone(),
            }
        }
    };

    let format_initial = format_object.clone();
    let initial = items.into_iter().map(move |obj| {
        Ok::<_, std::io::Error>(response::watch_event_line("ADDED", &format_initial(&obj)))
    });
    let initial_stream = tokio_stream::iter(initial);

    let av = api_version(ctx);
    let mut bookmark_meta = serde_json::json!({"resourceVersion": rv.to_string()});
    if send_initial_events && let Some(obj) = bookmark_meta.as_object_mut() {
        // KEP-3157: signals end of the initial replay so clients (k9s, the
        // WatchList reflector) know they have a consistent snapshot and can
        // start rendering.
        obj.insert(
            "annotations".to_string(),
            serde_json::json!({"k8s.io/initial-events-end": "true"}),
        );
    }
    let bookmark_obj = serde_json::json!({
        "apiVersion": av,
        "kind": ctx.resource_type.kind,
        "metadata": bookmark_meta,
    });
    let bookmark_formatted = format_object(&bookmark_obj);
    let bookmark = tokio_stream::iter(std::iter::once(Ok::<_, std::io::Error>(
        response::watch_event_line("BOOKMARK", &bookmark_formatted),
    )));

    let ns_filter: Option<String> = namespace.map(|s| s.to_string());
    // On broadcast lag, terminate so the client reconnects (standard K8s behavior).
    let live_stream = BroadcastStream::new(rx)
        .take_while(|result| result.is_ok())
        .filter_map(move |result| {
            let event = result.ok()?;
            if let Some(ref ns) = ns_filter
                && event
                    .object
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|v| v.as_str())
                    != Some(ns.as_str())
            {
                return None;
            }
            let type_str = match event.event_type {
                WatchEventType::Added => "ADDED",
                WatchEventType::Modified => "MODIFIED",
                WatchEventType::Deleted => "DELETED",
            };
            let payload = format_object(&event.object);
            Some(Ok::<_, std::io::Error>(response::watch_event_line(
                type_str, &payload,
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

    let statuses = pod
        .get("status")
        .and_then(|s| s.get("containerStatuses"))
        .and_then(|v| v.as_array());
    let status = statuses.and_then(|s| {
        if let Some(ref c) = params.container {
            s.iter()
                .find(|cs| cs.get("name").and_then(|v| v.as_str()) == Some(c.as_str()))
        } else {
            s.first()
        }
    });

    let container_id = match status
        .and_then(|s| s.get("containerID"))
        .and_then(|v| v.as_str())
    {
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
