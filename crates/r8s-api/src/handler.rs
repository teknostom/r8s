use std::sync::Arc;

use axum::{
    Extension, Json,
    body::{Body, Bytes},
    extract::{Path, State},
    response::Response,
};
use hyper::StatusCode;
use r8s_store::{
    backend::ResourceRef,
    index::{FieldSelector, LabelSelector},
    watch::WatchEventType,
};
use r8s_types::ResourceType;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    discovery::AppState,
    params::ListParams,
    patch::json_merge_patch,
    response::{self, status_error},
};
use axum::extract::Query;
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct RouteContext {
    pub resource_type: Arc<ResourceType>,
}

fn get_impl(state: &AppState, ctx: &RouteContext, namespace: Option<&str>, name: &str) -> Response {
    let resource_ref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    match state.store.get(&resource_ref) {
        Ok(Some(obj)) => response::object_response(&obj),
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
) -> Response {
    get_impl(&state, &ctx, Some(&ns), &name)
}

pub async fn get_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
) -> Response {
    get_impl(&state, &ctx, None, &name)
}

fn create_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    body: serde_json::Value,
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
    Json(body): Json<serde_json::Value>,
) -> Response {
    create_impl(&state, &ctx, Some(&ns), body)
}

pub async fn create_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    create_impl(&state, &ctx, None, body)
}

fn update_impl(
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
    Json(body): Json<serde_json::Value>,
) -> Response {
    update_impl(&state, &ctx, Some(&ns), &name, body)
}

pub async fn update_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    update_impl(&state, &ctx, None, &name, body)
}
fn delete_impl(
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

fn patch_impl(
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
    let mut current = match state.store.get(&rref) {
        Ok(Some(obj)) => obj,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{} '{}' not found", ctx.resource_type.kind, name),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
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
    json_merge_patch(&mut current, &patch);
    match state.store.update(&rref, &current) {
        Ok(obj) => response::object_response(&obj),
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

fn list_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    params: ListParams,
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
        Ok(result) => response::list_response(
            &api_version(ctx),
            &ctx.resource_type.kind,
            result.resource_version,
            result.continue_token.as_deref(),
            result.items,
        ),
        Err(err) => response::anyhow_error_response(err),
    }
}

fn watch_impl(state: &AppState, ctx: &RouteContext, namespace: Option<&str>) -> Response {
    // Subscribe to live events BEFORE listing, so we don't miss anything.
    let rx = state.store.watch(&ctx.resource_type.gvr);

    // List existing resources to emit as initial ADDED events.
    let items = state
        .store
        .list(&ctx.resource_type.gvr, namespace, None, None, None, None)
        .map(|r| r.items)
        .unwrap_or_default();

    let initial = items
        .into_iter()
        .map(|obj| Ok::<_, std::io::Error>(response::watch_event_line("ADDED", &obj)));
    let initial_stream = tokio_stream::iter(initial);

    let ns_filter: Option<String> = namespace.map(|s| s.to_string());
    let live_stream = BroadcastStream::new(rx).filter_map(move |result| {
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

    let stream = initial_stream.chain(live_stream);

    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .header("transfer-encoding", "chunked")
        .body(Body::from_stream(stream))
        .unwrap()
}

pub async fn list_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    Path(ns): Path<String>,
) -> Response {
    list_impl(&state, &ctx, Some(&ns), params)
}

pub async fn list_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
) -> Response {
    list_impl(&state, &ctx, None, params)
}

pub async fn list_all_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
) -> Response {
    list_impl(&state, &ctx, None, params)
}
