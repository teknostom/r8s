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

use crate::field_validation;
use crate::{
    admission::{self, AdmissionCtx, AdmissionError, Operation},
    discovery::AppState,
    params::ListParams,
    patch::json_merge_patch,
    response::{self, status_error},
    table,
};

/// Translate an admission failure into the HTTP response real k8s returns.
/// `Denied` forwards the webhook's own `metav1.Status` verbatim (clients like
/// cert-manager's startupapicheck inspect `details.causes[].field`); webhook
/// transport / decode failures surface as 500.
/// Canonical `reason` for a webhook denial's status code.
fn reason_for(code: u16) -> &'static str {
    match code {
        400 => "BadRequest",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "NotFound",
        409 => "Conflict",
        422 => "Invalid",
        _ => "",
    }
}

pub(crate) fn admission_error_response(err: AdmissionError) -> Response {
    match err {
        AdmissionError::Denied {
            webhook,
            message,
            status,
        } => {
            // Two things real apiservers do that clients depend on:
            //
            // 1. Wrap the denial message as
            //      admission webhook "<name>" denied the request: <message>
            //    cert-manager's startupapicheck regex-matches exactly this to
            //    recognize that the validating webhook rejected its test
            //    request; the bare webhook message fails the match.
            //
            // 2. Return a complete metav1.Status (kind/apiVersion/status:
            //    Failure), not the webhook's bare status fragment — otherwise
            //    client-go can't decode it and reports a generic "unknown".
            let wrapped = if message.is_empty() {
                format!("admission webhook \"{webhook}\" denied the request without explanation")
            } else {
                format!("admission webhook \"{webhook}\" denied the request: {message}")
            };
            // A code below 400 can't carry a rejection; upstream clamps to 400.
            let code = status
                .as_ref()
                .and_then(|s| s.get("code"))
                .and_then(|v| v.as_u64())
                .map(|c| c as u16)
                .filter(|c| (400..600).contains(c))
                .unwrap_or(StatusCode::BAD_REQUEST.as_u16());
            match status {
                Some(serde_json::Value::Object(mut obj)) => {
                    obj.insert("kind".into(), serde_json::json!("Status"));
                    obj.insert("apiVersion".into(), serde_json::json!("v1"));
                    obj.insert("status".into(), serde_json::json!("Failure"));
                    obj.entry("metadata")
                        .or_insert_with(|| serde_json::json!({}));
                    obj.entry("reason")
                        .or_insert_with(|| serde_json::json!(reason_for(code)));
                    obj.insert("message".into(), serde_json::json!(wrapped));
                    obj.insert("code".into(), serde_json::json!(code));
                    response::json_response(code, &serde_json::Value::Object(obj))
                }
                _ => response::status_error(
                    StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST),
                    reason_for(code),
                    &wrapped,
                ),
            }
        }
        AdmissionError::CallFailed { webhook, error } => response::status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("Internal error occurred: failed calling webhook \"{webhook}\": {error}"),
        ),
        AdmissionError::InvalidResponse { webhook, error } => response::status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("admission webhook '{webhook}' returned invalid response: {error}"),
        ),
        AdmissionError::Internal(msg) => {
            response::status_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &msg)
        }
        AdmissionError::PolicyDenied { message, reason } => {
            let code = match reason.as_str() {
                "Unauthorized" => StatusCode::UNAUTHORIZED,
                "Forbidden" => StatusCode::FORBIDDEN,
                "RequestEntityTooLarge" => StatusCode::PAYLOAD_TOO_LARGE,
                _ => StatusCode::UNPROCESSABLE_ENTITY,
            };
            response::status_error(code, &reason, &message)
        }
        AdmissionError::MatchConditionFailed(msg) => {
            response::status_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &msg)
        }
    }
}

/// A webhook configuration or ValidatingAdmissionPolicy whose expressions
/// do not compile is invalid.
fn validate_cel_expressions(ctx: &RouteContext, body: &serde_json::Value) -> Option<Response> {
    let gvr = &ctx.resource_type.gvr;
    let errors = crate::vap::compile_errors(&gvr.group, &gvr.resource, body);
    if errors.is_empty() {
        return None;
    }
    let details: Vec<String> = errors
        .iter()
        .map(|(field, expression, error)| {
            format!("{field}: Invalid value: \"{expression}\": compilation failed: {error}")
        })
        .collect();
    let summary = if details.len() == 1 {
        details[0].clone()
    } else {
        format!("[{}]", details.join(", "))
    };
    Some(response::status_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        "Invalid",
        &format!(
            "{}.{} \"{}\" is invalid: {summary}",
            ctx.resource_type.kind,
            gvr.group,
            body["metadata"]["name"].as_str().unwrap_or("")
        ),
    ))
}

/// Parse `?dryRun=All` from a raw query string. Anything else (including
/// missing) is treated as a real (non-dry-run) request.
fn parse_dry_run(raw_query: Option<&str>) -> bool {
    let Some(q) = raw_query else { return false };
    q.split('&').any(|p| p == "dryRun=All")
}
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
    #[serde(rename = "limitBytes")]
    pub limit_bytes: Option<usize>,
    #[serde(rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    #[serde(rename = "sinceTime")]
    pub since_time: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_flag")]
    pub timestamps: bool,
    #[serde(default, deserialize_with = "deserialize_bool_flag")]
    pub follow: bool,
}

/// kubectl sends booleans as "true"/"false" strings in query params.
fn deserialize_bool_flag<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(matches!(s.as_str(), "true" | "1"))
}

/// Render CRI-format log file content into the bytes `kubectl logs` returns.
///
/// Each line is `<RFC3339Nano> <stream> <P|F> <content>`. Strip the prefix
/// (unless `timestamps` is set), re-add a newline at every `F` entry. Partial
/// (`P`) entries on the same stream are concatenated until an `F` closes them.
///
/// `tail_lines` counts logical lines (i.e. `F` entries) from the end.
pub(crate) fn render_cri_log(raw: &str, timestamps: bool, tail_lines: Option<u64>) -> String {
    render_cri_log_window(raw, timestamps, tail_lines, None, None)
}

/// The lines logged since `since`, the last `tail_lines` of them, cut to
/// `limit_bytes`: `kubectl logs --since/--tail/--limit-bytes`.
pub(crate) fn render_cri_log_window(
    raw: &str,
    timestamps: bool,
    tail_lines: Option<u64>,
    since: Option<chrono::DateTime<chrono::Utc>>,
    limit_bytes: Option<usize>,
) -> String {
    let mut entries: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut current_ts: Option<&str> = None;
    let logged_since = |ts: Option<&str>| match (since, ts) {
        (Some(since), Some(ts)) => chrono::DateTime::parse_from_rfc3339(ts)
            .map(|t| t.with_timezone(&chrono::Utc) >= since)
            .unwrap_or(true),
        _ => true,
    };

    for line in raw.split_inclusive('\n') {
        let line = line.strip_suffix('\n').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(4, ' ');
        let ts = parts.next();
        let _stream = parts.next();
        let tag = parts.next();
        let content = parts.next().unwrap_or("");
        let (Some(ts), Some(tag)) = (ts, tag) else {
            // Not a CRI line; emit as-is so we don't silently swallow output
            // from an older log file that pre-dates this format.
            entries.push(line.to_string());
            continue;
        };
        if current.is_empty() {
            current_ts = Some(ts);
        }
        current.push_str(content);
        if tag == "F" {
            let mut out = String::new();
            if timestamps && let Some(t) = current_ts {
                out.push_str(t);
                out.push(' ');
            }
            out.push_str(&current);
            if logged_since(current_ts) {
                entries.push(out);
            }
            current.clear();
            current_ts = None;
        }
    }
    // Trailing partial fragment (container hasn't flushed a newline yet).
    if !current.is_empty() && logged_since(current_ts) {
        let mut out = String::new();
        if timestamps && let Some(t) = current_ts {
            out.push_str(t);
            out.push(' ');
        }
        out.push_str(&current);
        entries.push(out);
    }

    let start = match tail_lines {
        Some(n) => entries.len().saturating_sub(n as usize),
        None => 0,
    };

    let mut output = String::new();
    for entry in &entries[start..] {
        output.push_str(entry);
        output.push('\n');
    }
    if let Some(limit) = limit_bytes
        && output.len() > limit
    {
        let mut cut = limit;
        while !output.is_char_boundary(cut) {
            cut -= 1;
        }
        output.truncate(cut);
    }
    output
}

/// The `since` instant of a logs request: `sinceTime`, else now minus
/// `sinceSeconds`.
fn log_since(params: &LogParams) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(t) = params.since_time.as_deref() {
        return chrono::DateTime::parse_from_rfc3339(t)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc));
    }
    params
        .since_seconds
        .map(|secs| chrono::Utc::now() - chrono::Duration::seconds(secs))
}

/// Stream `<id>.log` for a `?follow=true` request: emit current content (with
/// `tailLines` applied), then poll for new bytes every 200ms and emit them.
/// Exits when the client disconnects (channel send fails).
fn follow_log_stream(
    log_path: std::path::PathBuf,
    timestamps: bool,
    tail_lines: Option<u64>,
) -> tokio_stream::wrappers::ReceiverStream<Result<axum::body::Bytes, std::io::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<axum::body::Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let initial = tokio::fs::read_to_string(&log_path)
            .await
            .unwrap_or_default();
        let mut offset = initial.len() as u64;
        let rendered = render_cri_log(&initial, timestamps, tail_lines);
        if !rendered.is_empty()
            && tx
                .send(Ok(axum::body::Bytes::from(rendered)))
                .await
                .is_err()
        {
            return;
        }

        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let len = match tokio::fs::metadata(&log_path).await {
                Ok(m) => m.len(),
                Err(_) => continue,
            };
            // Truncation (e.g. container restart re-prepared the log file)
            // would underflow; rewind to the new shorter file.
            if len < offset {
                offset = 0;
            }
            if len == offset {
                continue;
            }
            let mut file = match tokio::fs::File::open(&log_path).await {
                Ok(f) => f,
                Err(_) => continue,
            };
            if file.seek(SeekFrom::Start(offset)).await.is_err() {
                continue;
            }
            let mut buf = Vec::with_capacity((len - offset) as usize);
            if file.take(len - offset).read_to_end(&mut buf).await.is_err() {
                continue;
            }
            // Keep only the run that ends in '\n' so we never emit a partial
            // CRI record. Any trailing bytes wait for the next poll.
            let trim_to = match buf.iter().rposition(|&b| b == b'\n') {
                Some(p) => p + 1,
                None => continue,
            };
            let usable = match std::str::from_utf8(&buf[..trim_to]) {
                Ok(s) => s,
                Err(_) => continue,
            };
            // tailLines was already applied to the initial read; live tail
            // emits everything new.
            let rendered = render_cri_log(usable, timestamps, None);
            offset += trim_to as u64;
            if !rendered.is_empty()
                && tx
                    .send(Ok(axum::body::Bytes::from(rendered)))
                    .await
                    .is_err()
            {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
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
        // Decode to JSON: unknown GVKs get 415 (client retries with JSON),
        // malformed bodies get 400.
        use r8s_types::k8s_pb_serde::DecodeError;
        return r8s_types::k8s_pb_serde::decode_k8s_protobuf_to_json(body).map_err(|e| match e {
            DecodeError::UnsupportedGvk { .. } => status_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UnsupportedMediaType",
                "r8s does not yet accept application/vnd.kubernetes.protobuf request bodies \
                 for this resource type; retry with application/json",
            ),
            DecodeError::Malformed => status_error(
                StatusCode::BAD_REQUEST,
                "BadRequest",
                "malformed application/vnd.kubernetes.protobuf request body",
            ),
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

pub(crate) fn wants_table(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("as=Table") && accept.contains("g=meta.k8s.io"))
}

/// True if the client asked for metadata-only objects via the Accept header,
/// e.g. `application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1`. This is
/// the scheme client-go's metadata informers use; cert-manager's cainjector
/// watches webhook configs / its CA source this way, and the watch fails to
/// decode if we hand back full objects instead.
fn wants_partial_metadata(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| {
            accept.contains("g=meta.k8s.io")
                && (accept.contains("as=PartialObjectMetadata")
                    || accept.contains("as=PartialObjectMetadataList"))
        })
}

/// Strip everything but metadata and rewrite TypeMeta to `PartialObjectMetadata`
/// so client-go's metadata-informer decoder accepts it. We answer in JSON
/// regardless of the client's protobuf preference (we have no protobuf encoder),
/// which client-go honors via the response Content-Type.
fn to_partial_object_metadata(obj: &serde_json::Value) -> serde_json::Value {
    let metadata = obj
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "PartialObjectMetadata",
        "metadata": metadata,
    })
}

/// `obj` as served in the requested version: converted through the CRD's
/// conversion webhook when stored in another version, then defaulted.
#[allow(clippy::result_large_err)]
async fn present_async(
    state: &AppState,
    ctx: &RouteContext,
    obj: serde_json::Value,
) -> Result<serde_json::Value, Response> {
    Ok(present_all(state, ctx, vec![obj])
        .await?
        .pop()
        .unwrap_or_default())
}

#[allow(clippy::result_large_err)]
async fn present_all(
    state: &AppState,
    ctx: &RouteContext,
    items: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, Response> {
    let items = convert_to(state, ctx, items, &api_version(ctx)).await?;
    Ok(items.into_iter().map(|o| presented(ctx, o)).collect())
}

/// `items` converted to `desired` through the CRD's conversion webhook where
/// their stored version differs; built-ins pass through.
#[allow(clippy::result_large_err)]
async fn convert_to(
    state: &AppState,
    ctx: &RouteContext,
    items: Vec<serde_json::Value>,
    desired: &str,
) -> Result<Vec<serde_json::Value>, Response> {
    if crate::event_conversion::applies(ctx) {
        return Ok(items
            .into_iter()
            .map(|o| crate::event_conversion::convert(o, desired))
            .collect());
    }
    if ctx.resource_type.storage_gvr.is_none() {
        return Ok(items);
    }
    let Some(conv) = crate::conversion::Conversion::for_resource(&state.store, ctx) else {
        return Ok(items);
    };
    let needed = items.iter().any(|o| {
        o.get("apiVersion")
            .and_then(|v| v.as_str())
            .is_some_and(|v| conv.needs_webhook(v, desired))
    });
    if !needed {
        return Ok(items);
    }
    conv.convert(&state.store, items, desired)
        .await
        .map_err(|e| {
            response::status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!(
                    "conversion webhook for {}, Kind={} failed: {e}",
                    ctx.resource_type.gvr.api_version(),
                    ctx.resource_type.kind
                ),
            )
        })
}

/// Split a field selector into the part the store evaluates and the part
/// that waits for conversion: a custom resource's selectable fields belong
/// to the request version, so a webhook-converted resource is matched after
/// the webhook has produced that version.
fn split_field_selector(
    store: &r8s_store::Store,
    ctx: &RouteContext,
    field_sel: Option<FieldSelector>,
) -> (Option<FieldSelector>, Option<FieldSelector>) {
    let Some(fs) = field_sel else {
        return (None, None);
    };
    let deferred = crate::conversion::Conversion::for_resource(store, ctx)
        .is_some_and(|c| c.needs_webhook(&c.storage_version, &api_version(ctx)));
    if deferred {
        (None, Some(fs))
    } else {
        (Some(fs), None)
    }
}

/// `items` without those a deferred field selector rejects.
fn apply_field_selector(
    ctx: &RouteContext,
    field_sel: Option<&FieldSelector>,
    items: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    match field_sel {
        Some(fs) => {
            let key = ctx.resource_type.gvr.key_prefix();
            items.into_iter().filter(|o| fs.matches(o, &key)).collect()
        }
        None => items,
    }
}

/// `events` with their objects (and previous objects) converted to `desired`.
#[allow(clippy::result_large_err)]
async fn convert_events(
    state: &AppState,
    ctx: &RouteContext,
    events: Vec<r8s_store::watch::WatchEvent>,
    desired: &str,
) -> Result<Vec<r8s_store::watch::WatchEvent>, Response> {
    let mut objects = Vec::with_capacity(events.len());
    for event in &events {
        objects.push((*event.object).clone());
        if let Some(prev) = &event.prev_object {
            objects.push((**prev).clone());
        }
    }
    let mut converted = convert_to(state, ctx, objects, desired).await?.into_iter();
    Ok(events
        .into_iter()
        .map(|mut event| {
            if let Some(object) = converted.next() {
                event.object = std::sync::Arc::new(object);
            }
            if event.prev_object.is_some() {
                event.prev_object = converted.next().map(std::sync::Arc::new);
            }
            event
        })
        .collect())
}

/// A live watch event converted to `desired`; `None` when the webhook fails.
async fn convert_event(
    conv: &crate::conversion::Conversion,
    store: &r8s_store::Store,
    mut event: r8s_store::watch::WatchEvent,
    desired: &str,
) -> Option<r8s_store::watch::WatchEvent> {
    let stored = event
        .object
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !conv.needs_webhook(stored, desired) {
        return Some(event);
    }
    let mut objects = vec![(*event.object).clone()];
    if let Some(prev) = &event.prev_object {
        objects.push((**prev).clone());
    }
    match conv.convert(store, objects, desired).await {
        Ok(out) => {
            let mut out = out.into_iter();
            event.object = std::sync::Arc::new(out.next()?);
            if event.prev_object.is_some() {
                event.prev_object = out.next().map(std::sync::Arc::new);
            }
            Some(event)
        }
        Err(e) => {
            tracing::warn!("watch: conversion to {desired} failed: {e}");
            None
        }
    }
}

/// `body` in the CRD's storage version, ready to store.
#[allow(clippy::result_large_err)]
async fn to_storage_version(
    state: &AppState,
    ctx: &RouteContext,
    body: serde_json::Value,
) -> Result<serde_json::Value, Response> {
    if crate::event_conversion::applies(ctx) {
        return Ok(crate::event_conversion::convert(body, "v1"));
    }
    if ctx.resource_type.storage_gvr.is_none() {
        return Ok(body);
    }
    let Some(conv) = crate::conversion::Conversion::for_resource(&state.store, ctx) else {
        return Ok(body);
    };
    let storage = conv.storage_version.clone();
    let mut out = convert_to(state, ctx, vec![body], &storage).await?;
    Ok(out.pop().unwrap_or_default())
}

pub(crate) async fn get_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    headers: &HeaderMap,
) -> Response {
    let resource_ref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };
    match state.store.get(&resource_ref) {
        Ok(Some(obj)) => {
            let obj = match present_async(state, ctx, obj).await {
                Ok(o) => o,
                Err(resp) => return resp,
            };
            if wants_table(headers) {
                table::single_object_table_response(
                    &table::columns_for(&ctx.resource_type.gvr.resource),
                    &obj,
                    &ctx.resource_type.gvr.resource,
                )
            } else if wants_partial_metadata(headers) {
                response::object_response(&to_partial_object_metadata(&obj))
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
    get_impl(&state, &ctx, Some(&ns), &name, &headers).await
}

pub async fn get_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, None, &name, &headers).await
}

/// Real k8s allocates a ClusterIP for every `Service` that doesn't specify one
/// — including `LoadBalancer` and `NodePort` types. An explicit `clusterIP`
/// blocks allocation, and that includes the literal string `"None"`: that is
/// how headless Services are declared, and overwriting it would silently turn
/// them into ClusterIP Services (DNS would then serve one virtual IP instead
/// of the per-pod records StatefulSet charts rely on). Called from both POST
/// and server-side-apply create paths.
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
        .is_some_and(|ip| !ip.is_empty());
    if !matches!(svc_type, "ClusterIP" | "LoadBalancer" | "NodePort") || has_cluster_ip {
        return;
    }
    let ip = state.allocate_cluster_ip();
    if let Some(spec) = body.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("clusterIP".to_string(), serde_json::json!(ip));
        spec.insert("clusterIPs".to_string(), serde_json::json!([ip]));
    }
}

/// On update, a Service that became ExternalName drops its ClusterIP and
/// one that stopped being ExternalName gets one, as upstream's strategy does.
fn reconcile_service_addresses(state: &AppState, ctx: &RouteContext, body: &mut serde_json::Value) {
    if ctx.resource_type.gvr.resource != "services" {
        return;
    }
    if body["spec"]["type"].as_str() == Some("ExternalName") {
        if let Some(spec) = body["spec"].as_object_mut() {
            spec.remove("clusterIP");
            spec.remove("clusterIPs");
        }
        return;
    }
    maybe_allocate_cluster_ip(state, ctx, body);
}

/// `status.qosClass` from the containers' requests and limits, as upstream
/// computes it: Guaranteed when every container limits cpu and memory and
/// requests no less; BestEffort without any; Burstable otherwise.
/// The Priority admission plugin: a pod's `priorityClassName` (else the
/// global default class, else none) sets `spec.priority` and
/// `spec.preemptionPolicy`; an unknown class is refused.
#[allow(clippy::result_large_err)]
fn admit_priority(
    store: &r8s_store::Store,
    ctx: &RouteContext,
    body: &mut serde_json::Value,
) -> Option<Response> {
    if ctx.resource_type.gvr.resource != "pods" || !ctx.resource_type.gvr.group.is_empty() {
        return None;
    }
    let gvr = r8s_types::GroupVersionResource::new("scheduling.k8s.io", "v1", "priorityclasses");
    let wanted = body
        .pointer("/spec/priorityClassName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    let class = match &wanted {
        Some(name) => match store.get(&ResourceRef {
            gvr: &gvr,
            namespace: None,
            name,
        }) {
            Ok(Some(class)) => Some(class),
            _ => {
                return Some(status_error(
                    StatusCode::FORBIDDEN,
                    "Forbidden",
                    &format!(
                        "pods \"{}\" is forbidden: no PriorityClass with name {name} was found",
                        body.pointer("/metadata/name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                    ),
                ));
            }
        },
        None => store
            .list(&gvr, None, None, None, None, None)
            .ok()
            .and_then(|r| {
                r.items
                    .into_iter()
                    .find(|c| c.get("globalDefault").and_then(|v| v.as_bool()) == Some(true))
            }),
    };
    let (value, policy) = match &class {
        Some(c) => (
            c.get("value").and_then(|v| v.as_i64()).unwrap_or(0),
            c.get("preemptionPolicy")
                .and_then(|v| v.as_str())
                .unwrap_or("PreemptLowerPriority")
                .to_string(),
        ),
        None => (0, "PreemptLowerPriority".to_string()),
    };
    if let Some(spec) = body.get_mut("spec").and_then(|s| s.as_object_mut()) {
        if wanted.is_none()
            && let Some(name) = class
                .as_ref()
                .and_then(|c| c.pointer("/metadata/name"))
                .and_then(|v| v.as_str())
        {
            spec.insert("priorityClassName".into(), serde_json::json!(name));
        }
        spec.insert("priority".into(), serde_json::json!(value));
        spec.entry("preemptionPolicy")
            .or_insert_with(|| serde_json::json!(policy));
    }
    None
}

/// The namespace's LimitRanges fill a new pod's containers' missing limits
/// (`default`) and requests (`defaultRequest`), then refuse a container
/// whose request is under `min` or whose limit is over `max`, as the
/// LimitRanger admission plugin does for `type: Container` limits.
#[allow(clippy::result_large_err)]
fn apply_limit_ranges(
    store: &r8s_store::Store,
    ctx: &RouteContext,
    body: &mut serde_json::Value,
) -> Option<Response> {
    if ctx.resource_type.gvr.resource != "pods" || !ctx.resource_type.gvr.group.is_empty() {
        return None;
    }
    let namespace = body
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(String::from);
    let Ok(ranges) = store.list(
        &r8s_types::GroupVersionResource::new("", "v1", "limitranges"),
        namespace.as_deref(),
        None,
        None,
        None,
        None,
    ) else {
        return None;
    };
    let mut default_limits = serde_json::Map::new();
    let mut default_requests = serde_json::Map::new();
    let mut bounds: Vec<(String, String, bool)> = Vec::new();
    for range in &ranges.items {
        for limit in range
            .pointer("/spec/limits")
            .and_then(|l| l.as_array())
            .into_iter()
            .flatten()
        {
            if limit.get("type").and_then(|t| t.as_str()) != Some("Container") {
                continue;
            }
            for (field, is_min) in [("min", true), ("max", false)] {
                for (key, value) in limit
                    .get(field)
                    .and_then(|d| d.as_object())
                    .into_iter()
                    .flatten()
                {
                    if let Some(q) = value.as_str() {
                        bounds.push((key.clone(), q.to_string(), is_min));
                    }
                }
            }
            for (key, value) in limit
                .get("default")
                .and_then(|d| d.as_object())
                .into_iter()
                .flatten()
            {
                default_limits
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
            for (key, value) in limit
                .get("defaultRequest")
                .and_then(|d| d.as_object())
                .into_iter()
                .flatten()
            {
                default_requests
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
        }
    }
    if default_limits.is_empty() && default_requests.is_empty() && bounds.is_empty() {
        return requests_within_limits(body);
    }
    for list in ["containers", "initContainers"] {
        for container in body
            .pointer_mut(&format!("/spec/{list}"))
            .and_then(|c| c.as_array_mut())
            .into_iter()
            .flatten()
        {
            let resources = container.as_object_mut().map(|c| {
                c.entry("resources")
                    .or_insert_with(|| serde_json::json!({}))
            });
            let Some(resources) = resources.and_then(|r| {
                if !r.is_object() {
                    *r = serde_json::json!({});
                }
                r.as_object_mut()
            }) else {
                continue;
            };
            for (field, defaults) in [("limits", &default_limits), ("requests", &default_requests)]
            {
                let target = resources
                    .entry(field)
                    .or_insert_with(|| serde_json::json!({}));
                if !target.is_object() {
                    *target = serde_json::json!({});
                }
                if let Some(map) = target.as_object_mut() {
                    for (key, value) in defaults {
                        map.entry(key.clone()).or_insert_with(|| value.clone());
                    }
                }
            }
        }
    }
    for list in ["containers", "initContainers"] {
        for container in body
            .pointer(&format!("/spec/{list}"))
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
        {
            for (resource, bound, is_min) in &bounds {
                let (field, kind) = if *is_min {
                    ("requests", "request")
                } else {
                    ("limits", "limit")
                };
                let given = container
                    .get("resources")
                    .and_then(|r| r.get(field))
                    .and_then(|m| m.get(resource))
                    .and_then(|v| {
                        v.as_str()
                            .map(String::from)
                            .or_else(|| v.as_f64().map(|n| n.to_string()))
                    });
                let (Some(limit), given_q) = (
                    r8s_types::quantity::parse_quantity(bound),
                    given
                        .as_deref()
                        .and_then(r8s_types::quantity::parse_quantity),
                ) else {
                    continue;
                };
                let violated = match given_q {
                    None => true,
                    Some(q) if *is_min => q < limit,
                    Some(q) => q > limit,
                };
                if violated {
                    let which = if *is_min { "minimum" } else { "maximum" };
                    let message = match &given {
                        Some(g) => format!(
                            "pods \"{}\" is forbidden: {which} {resource} usage per Container is {bound}, but {kind} is {g}",
                            body.pointer("/metadata/name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                        ),
                        None => format!(
                            "pods \"{}\" is forbidden: {which} {resource} usage per Container is {bound}.  No {kind} is specified",
                            body.pointer("/metadata/name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                        ),
                    };
                    return Some(status_error(StatusCode::FORBIDDEN, "Forbidden", &message));
                }
            }
        }
    }
    requests_within_limits(body)
}

/// A container's request may not exceed its limit for the same resource.
#[allow(clippy::result_large_err)]
fn requests_within_limits(body: &serde_json::Value) -> Option<Response> {
    let name = body
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    for list in ["containers", "initContainers"] {
        for (index, container) in body
            .pointer(&format!("/spec/{list}"))
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
            .enumerate()
        {
            let resources = container.get("resources");
            let quantity = |field: &str, key: &str| {
                resources
                    .and_then(|r| r.get(field))
                    .and_then(|m| m.get(key))
                    .and_then(|v| v.as_str())
                    .map(String::from)
            };
            for (key, _) in resources
                .and_then(|r| r.get("requests"))
                .and_then(|m| m.as_object())
                .into_iter()
                .flatten()
            {
                let (Some(request), Some(limit)) =
                    (quantity("requests", key), quantity("limits", key))
                else {
                    continue;
                };
                let (Some(r), Some(l)) = (
                    r8s_types::quantity::parse_quantity(&request),
                    r8s_types::quantity::parse_quantity(&limit),
                ) else {
                    continue;
                };
                if r > l {
                    return Some(status_error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "Invalid",
                        &format!(
                            "Pod \"{name}\" is invalid: spec.{list}[{index}].resources.requests: Invalid value: \"{request}\": must be less than or equal to {key} limit of {limit}"
                        ),
                    ));
                }
            }
        }
    }
    None
}

/// Claims and volumes start in phase Pending, as the apiserver stamps them
/// before any controller binds them.
fn set_initial_phase(ctx: &RouteContext, body: &mut serde_json::Value) {
    let gvr = &ctx.resource_type.gvr;
    if !gvr.group.is_empty()
        || !matches!(
            gvr.resource.as_str(),
            "persistentvolumeclaims" | "persistentvolumes"
        )
    {
        return;
    }
    let has_phase = body
        .pointer("/status/phase")
        .and_then(|v| v.as_str())
        .is_some_and(|p| !p.is_empty());
    if !has_phase {
        body["status"]["phase"] = serde_json::json!("Pending");
    }
}

fn set_pod_qos_class(ctx: &RouteContext, body: &mut serde_json::Value) {
    if ctx.resource_type.gvr.resource != "pods" || !ctx.resource_type.gvr.group.is_empty() {
        return;
    }
    r8s_controllers::pod_admission::set_initial_status(body);
}

const NODE_PORT_RANGE: std::ops::RangeInclusive<i64> = 30000..=32767;

/// Give every port of a NodePort or LoadBalancer Service a `nodePort`: the
/// one the client set, else the one the same-named port held before, else a
/// free one from the node port range. Other Service types carry none.
fn allocate_node_ports(
    state: &AppState,
    ctx: &RouteContext,
    old: Option<&serde_json::Value>,
    body: &mut serde_json::Value,
) {
    if ctx.resource_type.gvr.resource != "services" {
        return;
    }
    let svc_type = body["spec"]["type"].as_str().unwrap_or("ClusterIP");
    let wants = matches!(svc_type, "NodePort" | "LoadBalancer");
    let previous: Vec<(String, i64)> = old
        .and_then(|o| o["spec"]["ports"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|p| {
            Some((
                p["name"].as_str().unwrap_or("").to_string(),
                p["nodePort"].as_i64().filter(|n| *n > 0)?,
            ))
        })
        .collect();
    let Some(ports) = body["spec"]["ports"].as_array_mut() else {
        return;
    };
    if !wants {
        for port in ports.iter_mut() {
            if let Some(obj) = port.as_object_mut() {
                obj.remove("nodePort");
            }
        }
        return;
    }
    let mut used: Vec<i64> = state
        .store
        .list(
            ctx.resource_type.storage_gvr(),
            None,
            None,
            None,
            None,
            None,
        )
        .map(|r| r.items)
        .unwrap_or_default()
        .iter()
        .filter(|s| old.is_none_or(|o| o["metadata"]["uid"] != s["metadata"]["uid"]))
        .flat_map(|s| s["spec"]["ports"].as_array().cloned().unwrap_or_default())
        .filter_map(|p| p["nodePort"].as_i64())
        .collect();
    used.extend(ports.iter().filter_map(|p| p["nodePort"].as_i64()));
    for port in ports.iter_mut() {
        if port["nodePort"].as_i64().is_some_and(|n| n > 0) {
            continue;
        }
        let name = port["name"].as_str().unwrap_or("");
        let kept = previous
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, p)| *p)
            .filter(|p| !used.contains(p));
        let chosen = kept.or_else(|| NODE_PORT_RANGE.clone().find(|p| !used.contains(p)));
        if let (Some(chosen), Some(obj)) = (chosen, port.as_object_mut()) {
            used.push(chosen);
            obj.insert("nodePort".to_string(), serde_json::json!(chosen));
        }
    }
}

/// Fill the system-owned metadata on create, mirroring upstream's
/// `FillObjectMetaSystemFields`: after mutating admission, before validating
/// admission. Ordering is load-bearing — validating webhooks must see the
/// object as it will be persisted (ingress-nginx's validator rejects any
/// Ingress whose UID isn't a well-formed UUID), while mutating webhooks run
/// pre-UID exactly like upstream. Client-supplied values are overwritten;
/// these fields are the server's, and the store keeps what's set here.
fn fill_system_fields_on_create(body: &mut serde_json::Value) {
    if let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        meta.insert(
            "uid".to_string(),
            serde_json::json!(uuid::Uuid::new_v4().to_string()),
        );
        meta.insert(
            "creationTimestamp".to_string(),
            serde_json::json!(chrono::Utc::now().to_rfc3339()),
        );
    }
}

/// Typed clients send no apiVersion/kind; stored objects always carry them.
fn stamp_type_meta(ctx: &RouteContext, body: &mut serde_json::Value) {
    if let Some(obj) = body.as_object_mut() {
        obj.entry("apiVersion")
            .or_insert_with(|| serde_json::json!(api_version(ctx)));
        obj.entry("kind")
            .or_insert_with(|| serde_json::json!(ctx.resource_type.kind));
    }
}

/// Resources whose `status` is written only through the `/status`
/// subresource: every built-in except the status-less ones, and custom
/// resources whose CRD enables the status subresource.
pub(crate) fn has_status_subresource(ctx: &RouteContext) -> bool {
    if is_custom_resource(ctx) {
        return ctx.resource_type.subresources.iter().any(|s| s == "status");
    }
    if ctx.resource_type.gvr.group == "admissionregistration.k8s.io"
        && ctx.resource_type.gvr.resource == "validatingadmissionpolicies"
    {
        return true;
    }
    !matches!(
        (
            ctx.resource_type.gvr.group.as_str(),
            ctx.resource_type.gvr.resource.as_str()
        ),
        (
            "",
            "configmaps"
                | "secrets"
                | "serviceaccounts"
                | "endpoints"
                | "events"
                | "podtemplates"
                | "limitranges"
                | "bindings"
        ) | ("apps", "controllerrevisions")
            | (
                "coordination.k8s.io" | "discovery.k8s.io" | "rbac.authorization.k8s.io",
                _
            )
            | (
                "admissionregistration.k8s.io" | "scheduling.k8s.io" | "node.k8s.io",
                _
            )
            | (
                "authentication.k8s.io" | "authorization.k8s.io" | "events.k8s.io",
                _
            )
            | (
                "storage.k8s.io",
                "storageclasses" | "csidrivers" | "csinodes" | "csistoragecapacities"
            )
            | (
                "networking.k8s.io",
                "ingressclasses" | "networkpolicies" | "ipaddresses" | "servicecidrs"
            )
    )
}

/// Resources whose `metadata.generation` counts desired-state changes.
fn tracks_generation(ctx: &RouteContext) -> bool {
    is_custom_resource(ctx)
        || matches!(
            (
                ctx.resource_type.gvr.group.as_str(),
                ctx.resource_type.gvr.resource.as_str()
            ),
            ("", "replicationcontrollers")
                | (
                    "apps",
                    "deployments" | "replicasets" | "daemonsets" | "statefulsets"
                )
                | ("batch", "jobs" | "cronjobs")
                | ("policy", "poddisruptionbudgets")
                | ("autoscaling", "horizontalpodautoscalers")
                | ("networking.k8s.io", "ingresses")
                | ("apiextensions.k8s.io", "customresourcedefinitions")
        )
}

/// Set `metadata.generation`: 1 on create, bumped when anything outside
/// metadata and status changed, otherwise carried over from `old`.
/// An update keeps the object's uid, creationTimestamp and deletion stamp
/// as upstream's BeforeUpdate does: a body without a uid takes the stored
/// one, a different uid is refused.
#[allow(clippy::result_large_err)]
fn keep_system_fields(old: &serde_json::Value, new: &mut serde_json::Value) -> Option<Response> {
    let old_meta = old.get("metadata")?;
    if !new.get("metadata").is_some_and(|m| m.is_object()) {
        new["metadata"] = serde_json::json!({});
    }
    let meta = new.get_mut("metadata")?.as_object_mut()?;
    if let Some(uid) = old_meta.get("uid") {
        match meta.get("uid") {
            Some(given) if given != uid => {
                return Some(status_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Invalid",
                    "metadata.uid: Invalid value: field is immutable",
                ));
            }
            _ => {
                meta.insert("uid".to_string(), uid.clone());
            }
        }
    }
    for field in [
        "creationTimestamp",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
    ] {
        if let Some(value) = old_meta.get(field) {
            meta.insert(field.to_string(), value.clone());
        }
    }
    None
}

fn set_generation(
    ctx: &RouteContext,
    old: Option<&serde_json::Value>,
    new: &mut serde_json::Value,
) {
    if !tracks_generation(ctx) {
        return;
    }
    let desired = |v: &serde_json::Value| -> serde_json::Map<String, serde_json::Value> {
        v.as_object()
            .map(|o| {
                o.iter()
                    .filter(|(k, _)| !matches!(k.as_str(), "metadata" | "status"))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let generation = match old {
        None => 1,
        Some(old) => {
            let current = old
                .get("metadata")
                .and_then(|m| m.get("generation"))
                .and_then(|g| g.as_i64())
                .unwrap_or(1);
            if desired(old) != desired(new) {
                current + 1
            } else {
                current
            }
        }
    };
    if let Some(meta) = new.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("generation".to_string(), serde_json::json!(generation));
    }
}

/// Upstream's pod and pod-template defaults for the resource's type.
fn apply_type_defaults(ctx: &RouteContext, body: &mut serde_json::Value) {
    crate::defaults::apply(
        &ctx.resource_type.gvr.group,
        &ctx.resource_type.gvr.resource,
        body,
    );
}

/// A new Namespace is Active.
fn default_namespace_status(ctx: &RouteContext, body: &mut serde_json::Value) {
    if !(ctx.resource_type.gvr.group.is_empty() && ctx.resource_type.gvr.resource == "namespaces") {
        return;
    }
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let status = obj.entry("status").or_insert_with(|| serde_json::json!({}));
    if let Some(status) = status.as_object_mut() {
        status
            .entry("phase")
            .or_insert_with(|| serde_json::json!("Active"));
    }
}

/// `namespaces/{name}/finalize`: replaces `spec.finalizers`, nothing else.
pub async fn finalize_cluster(
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
    let rref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace: None,
        name: &name,
    };
    let mut current = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{} '{}' not found", ctx.resource_type.kind, name),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
    };
    let finalizers = body
        .get("spec")
        .and_then(|s| s.get("finalizers"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    if let Some(obj) = current.as_object_mut() {
        let spec = obj.entry("spec").or_insert_with(|| serde_json::json!({}));
        if let Some(spec) = spec.as_object_mut() {
            spec.insert("finalizers".to_string(), finalizers);
        }
    }
    match state.store.update(&rref, &current) {
        Ok(obj) => response::object_response(&presented(&ctx, obj)),
        Err(err) => response::anyhow_error_response(err),
    }
}

/// Default ServicePort fields the apiserver fills in but clients commonly
/// omit: `protocol` → `TCP`, and `targetPort` → the `port` value. The protocol
/// default in particular matters because controllers match a Service's ports
/// to its EndpointSlice ports *by protocol* (ingress-nginx does
/// `*epPort.Protocol == servicePort.Protocol`); an absent protocol reads as ""
/// on the Service but `TCP` on the slice, so no endpoint matches and the
/// controller serves 503.
/// Service defaults as upstream applies them on create and update: the
/// type, session affinity, port protocol and targetPort, and for Services
/// with a cluster address the single-stack IPv4 family and traffic policy.
fn default_service_ports(ctx: &RouteContext, body: &mut serde_json::Value) {
    if ctx.resource_type.gvr.resource != "services" {
        return;
    }
    if let Some(spec) = body.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.entry("type")
            .or_insert_with(|| serde_json::json!("ClusterIP"));
        spec.entry("sessionAffinity")
            .or_insert_with(|| serde_json::json!("None"));
        if spec.get("type").and_then(|t| t.as_str()) != Some("ExternalName") {
            spec.entry("internalTrafficPolicy")
                .or_insert_with(|| serde_json::json!("Cluster"));
            if spec.get("clusterIP").and_then(|v| v.as_str()) != Some("None") {
                spec.entry("ipFamilies")
                    .or_insert_with(|| serde_json::json!(["IPv4"]));
                spec.entry("ipFamilyPolicy")
                    .or_insert_with(|| serde_json::json!("SingleStack"));
            }
        }
    }
    if let Some(ports) = body
        .get_mut("spec")
        .and_then(|s| s.get_mut("ports"))
        .and_then(|p| p.as_array_mut())
    {
        for port in ports.iter_mut() {
            if let Some(obj) = port.as_object_mut() {
                obj.entry("protocol")
                    .or_insert_with(|| serde_json::json!("TCP"));
                // Go clients send the zero value, which means the port.
                let unset = match obj.get("targetPort") {
                    None => true,
                    Some(v) => v.as_i64() == Some(0) || v.as_str() == Some(""),
                };
                if unset && let Some(p) = obj.get("port").cloned() {
                    obj.insert("targetPort".to_string(), p);
                }
            }
        }
    }
}

/// For CRDs, fill in `default` values from the openAPIV3Schema for any absent
/// field. Skipped for built-ins (same rationale as `validate_cr` — their
/// vendored schemas are handled separately). Operators depend on this: the
/// prometheus-operator reads `spec.scrapeInterval` expecting the CRD default
/// `30s`, and emits an invalid `scrape_interval: ""` without it.
fn default_cr(ctx: &RouteContext, body: &mut serde_json::Value) {
    if r8s_types::openapi::spec_bytes_for(
        &ctx.resource_type.gvr.group,
        &ctx.resource_type.gvr.version,
    )
    .is_some()
    {
        return;
    }
    if let Some(schema) = ctx.resource_type.schema.as_ref() {
        crate::schema_default::apply_defaults(body, schema);
    }
}

/// For CRDs, validate the incoming object against the CRD's openAPIV3Schema.
/// Skipped for built-in resources (their vendored schemas are very strict and
/// we don't currently want to reject otherwise-valid input there).
/// Fields a type never lets change: a PriorityClass's value and
/// preemptionPolicy.
#[allow(clippy::result_large_err)]
fn check_immutable_fields(
    ctx: &RouteContext,
    old: &serde_json::Value,
    new: &serde_json::Value,
) -> Option<Response> {
    let gvr = &ctx.resource_type.gvr;
    let fields: &[&str] = match (gvr.group.as_str(), gvr.resource.as_str()) {
        ("scheduling.k8s.io", "priorityclasses") => &["value", "preemptionPolicy"],
        _ => return None,
    };
    let name = new
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    fields
        .iter()
        .find(|field| {
            let (before, after) = (old.get(**field), new.get(**field));
            before.is_some() && after.is_some() && before != after
        })
        .map(|field| {
            status_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Invalid",
                &format!(
                    "{} \"{name}\" is invalid: {field}: Forbidden: field is immutable",
                    ctx.resource_type.kind
                ),
            )
        })
}

/// An immutable ConfigMap or Secret keeps its data and stays immutable, as
/// upstream enforces.
#[allow(clippy::result_large_err)]
fn check_immutable(
    ctx: &RouteContext,
    old: &serde_json::Value,
    new: &serde_json::Value,
) -> Option<Response> {
    if !ctx.resource_type.gvr.group.is_empty()
        || !matches!(
            ctx.resource_type.gvr.resource.as_str(),
            "configmaps" | "secrets"
        )
        || old["immutable"] != true
    {
        return None;
    }
    let invalid = |field: &str| {
        response::status_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid",
            &format!(
                "{} \"{}\" is invalid: {field}: Forbidden: field is immutable when `immutable` is set",
                ctx.resource_type.kind,
                old["metadata"]["name"].as_str().unwrap_or("")
            ),
        )
    };
    if new["immutable"] != true {
        return Some(invalid("immutable"));
    }
    for field in ["data", "binaryData", "stringData"] {
        if old.get(field) != new.get(field)
            && !(old.get(field).is_none() && new[field].as_object().is_some_and(|m| m.is_empty()))
        {
            return Some(invalid(field));
        }
    }
    None
}

/// Upstream field validation for the built-ins it matters for: ConfigMap and
/// Secret keys, and sysctl names.
#[allow(clippy::result_large_err)]
fn validate_builtin(ctx: &RouteContext, body: &serde_json::Value) -> Option<Response> {
    if !ctx.resource_type.gvr.group.is_empty() {
        return None;
    }
    let invalid = |field: String, value: &str, detail: &str| {
        response::status_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid",
            &format!(
                "{} \"{}\" is invalid: {field}: Invalid value: \"{value}\": {detail}",
                ctx.resource_type.kind,
                body["metadata"]["name"].as_str().unwrap_or("")
            ),
        )
    };
    match ctx.resource_type.gvr.resource.as_str() {
        "configmaps" | "secrets" => {
            for section in ["data", "binaryData", "stringData"] {
                for key in body[section].as_object().into_iter().flat_map(|m| m.keys()) {
                    let ok = !key.is_empty()
                        && key
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
                    if !ok {
                        return Some(invalid(
                            format!("{section}[{key}]"),
                            key,
                            "a valid config key must consist of alphanumeric characters, '-', '_' or '.' (e.g. 'key.name',  or 'KEY_NAME',  or 'key-name', regex used for validation is '[-._a-zA-Z0-9]+')",
                        ));
                    }
                }
            }
        }
        "pods" => {
            let sysctls = body["spec"]["securityContext"]["sysctls"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let errors: Vec<String> = sysctls
                .iter()
                .enumerate()
                .filter_map(|(i, sysctl)| {
                    let name = sysctl["name"].as_str().unwrap_or("");
                    (!valid_sysctl_name(name)).then(|| {
                        format!(
                            "spec.securityContext.sysctls[{i}].name: Invalid value: \"{name}\": must have at most 253 characters and match regex ^([a-z0-9]([-_a-z0-9]*[a-z0-9])?[./])*[a-z0-9]([-_a-z0-9]*[a-z0-9])?$"
                        )
                    })
                })
                .collect();
            if !errors.is_empty() {
                let detail = if errors.len() == 1 {
                    errors[0].clone()
                } else {
                    format!("[{}]", errors.join(", "))
                };
                return Some(response::status_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Invalid",
                    &format!(
                        "{} \"{}\" is invalid: {detail}",
                        ctx.resource_type.kind,
                        body["metadata"]["name"].as_str().unwrap_or("")
                    ),
                ));
            }
        }
        _ => {}
    }
    None
}

/// Dot- or slash-separated segments of `[a-z0-9_-]` that start and end
/// alphanumeric, at most 253 characters in all.
fn valid_sysctl_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name.split(['.', '/']).all(|seg| {
            let bytes = seg.as_bytes();
            !seg.is_empty()
                && bytes[0].is_ascii_lowercase() | bytes[0].is_ascii_digit()
                && bytes[bytes.len() - 1].is_ascii_lowercase()
                    | bytes[bytes.len() - 1].is_ascii_digit()
                && seg
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        })
}

/// Upstream's RuntimeClass admission on pod creation: the class must exist,
/// and its pod overhead becomes the pod's.
fn admit_runtime_class(
    store: &r8s_store::Store,
    ctx: &RouteContext,
    body: &mut serde_json::Value,
) -> Option<Response> {
    if ctx.resource_type.gvr.resource != "pods" || !ctx.resource_type.gvr.group.is_empty() {
        return None;
    }
    let class = body["spec"]["runtimeClassName"].as_str()?.to_string();
    let gvr = r8s_types::GroupVersionResource::new("node.k8s.io", "v1", "runtimeclasses");
    let rc = match store.get(&ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: &class,
    }) {
        Ok(Some(rc)) => rc,
        _ => {
            return Some(response::status_error(
                StatusCode::FORBIDDEN,
                "Forbidden",
                &format!(
                    "pods \"{}\" is forbidden: pod rejected: RuntimeClass \"{class}\" not found",
                    body["metadata"]["name"].as_str().unwrap_or("")
                ),
            ));
        }
    };
    if let Some(overhead) = rc["overhead"]["podFixed"].as_object()
        && !body["spec"]["overhead"].is_object()
    {
        body["spec"]["overhead"] = serde_json::Value::Object(overhead.clone());
    }
    None
}

fn validate_cr(ctx: &RouteContext, body: &serde_json::Value) -> Option<Response> {
    if r8s_types::openapi::spec_bytes_for(
        &ctx.resource_type.gvr.group,
        &ctx.resource_type.gvr.version,
    )
    .is_some()
    {
        return None;
    }
    let schema = ctx.resource_type.schema.as_ref()?;
    match crate::schema_validate::validate(body, schema) {
        Ok(()) => None,
        Err(msg) => Some(status_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid",
            &msg,
        )),
    }
}

pub(crate) async fn create_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    mut body: serde_json::Value,
    dry_run: bool,
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

    // NamespaceLifecycle admission: a namespaced resource can only be created
    // in a namespace that exists. Without this, r8s stores objects keyed by a
    // namespace string that was never created — masking setup errors and
    // orphaning resources (a StatefulSet whose namespace is missing strands its
    // pods). Cluster-scoped resources (namespace == None) and Namespaces
    // themselves are naturally exempt; bootstrap writes go straight to the
    // store and bypass this path.
    if let Some(ns) = namespace.filter(|n| !n.is_empty()) {
        let ns_gvr = r8s_types::GroupVersionResource::namespaces();
        let ns_ref = ResourceRef {
            gvr: &ns_gvr,
            namespace: None,
            name: ns,
        };
        match state.store.get(&ns_ref) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return status_error(
                    StatusCode::NOT_FOUND,
                    "NotFound",
                    &format!("namespaces \"{ns}\" not found"),
                );
            }
            Err(err) => return response::anyhow_error_response(err),
        }
    }

    stamp_type_meta(ctx, &mut body);
    set_generation(ctx, None, &mut body);
    maybe_allocate_cluster_ip(state, ctx, &mut body);
    default_service_ports(ctx, &mut body);
    allocate_node_ports(state, ctx, None, &mut body);
    default_cr(ctx, &mut body);
    default_namespace_status(ctx, &mut body);
    apply_type_defaults(ctx, &mut body);
    if ctx.resource_type.gvr.resource == "pods" {
        r8s_controllers::pod_admission::inject_sa_token(&state.store, &mut body);
    }

    // Mutating webhooks run first — let cert-manager-style mutators patch
    // the body before validation / storage. Webhooks with sideEffects !=
    // None/NoneOnDryRun are skipped on dry-run by admission.rs.
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Create,
        namespace,
        name: &name,
        subresource: None,
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut body, None).await {
        return admission_error_response(e);
    }
    // Fields a mutating webhook added take their defaults too, and a custom
    // resource loses what its schema does not declare.
    apply_type_defaults(ctx, &mut body);
    field_validation::prune(ctx, &mut body);
    if let Some(resp) = admit_runtime_class(&state.store, ctx, &mut body) {
        return resp;
    }
    if let Some(resp) = apply_limit_ranges(&state.store, ctx, &mut body) {
        return resp;
    }
    if let Some(resp) = admit_priority(&state.store, ctx, &mut body) {
        return resp;
    }
    set_pod_qos_class(ctx, &mut body);
    set_initial_phase(ctx, &mut body);
    fill_system_fields_on_create(&mut body);
    if let Err(e) = admission::invoke_validating(&admission_ctx, &body, None).await {
        return admission_error_response(e);
    }

    if let Some(resp) =
        validate_builtin(ctx, &body).or_else(|| validate_cel_expressions(ctx, &body))
    {
        return resp;
    }
    if let Some(resp) = validate_cr(ctx, &body) {
        return resp;
    }

    // ResourceQuota admission: reject creates that would push usage past any
    // hard limit in the namespace. Cluster-scoped resources skip the check
    // entirely (quotas only constrain namespaced state).
    if let Some(ns) = namespace
        && let Err(msg) = r8s_controllers::quota::check_admission(
            &state.store,
            ctx.resource_type.storage_gvr(),
            ns,
            &body,
        )
    {
        return response::status_error(StatusCode::FORBIDDEN, "Forbidden", &msg);
    }

    // dryRun=All: return the (now-mutated) body without persisting. Matches
    // upstream apiserver semantics — clients use this to preview admission.
    if dry_run {
        return response::created_response(&body);
    }

    let resource_ref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name: &name,
    };
    let body = match to_storage_version(state, ctx, body).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    match state.store.create(resource_ref, &body) {
        Ok(obj) => match present_async(state, ctx, obj).await {
            Ok(o) => response::created_response(&o),
            Err(resp) => resp,
        },
        Err(err) => response::anyhow_error_response(err),
    }
}

/// Parse a create/update body, applying the request's `fieldValidation`.
/// Protobuf bodies are decoded as before and not checked.
#[allow(clippy::result_large_err)]
pub(crate) fn decode_body(
    ctx: &RouteContext,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(serde_json::Value, field_validation::Findings), Response> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if content_type.contains("protobuf") {
        return require_json(headers, body).map(|v| (v, field_validation::Findings::default()));
    }
    let decoded = field_validation::decode_json(body).map_err(|e| {
        status_error(
            StatusCode::BAD_REQUEST,
            "Invalid",
            &format!("invalid body: {e}"),
        )
    })?;
    field_validation::check(ctx, field_validation::mode(raw_query), decoded)
}

pub async fn create_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(ns): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (body, findings) = match decode_body(&ctx, &headers, raw_query.as_deref(), &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    let resp = create_impl(&state, &ctx, Some(&ns), body, dry_run).await;
    field_validation::attach_warnings(resp, &findings)
}

pub async fn create_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (body, findings) = match decode_body(&ctx, &headers, raw_query.as_deref(), &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    let resp = create_impl(&state, &ctx, None, body, dry_run).await;
    field_validation::attach_warnings(resp, &findings)
}

pub(crate) async fn update_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    mut body: serde_json::Value,
    dry_run: bool,
) -> Response {
    let resource_ref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };
    // Normalize metadata.name/namespace to the request path, like create_impl.
    // The apiserver always derives these from the URL; trusting the body breaks
    // for protobuf clients (controller-runtime) whose gogo-encoded ObjectMeta
    // carries a present-but-empty `namespace`, which would otherwise overwrite
    // the real namespace with "" and orphan child objects (e.g. a StatefulSet's
    // pods get created with an empty namespace).
    if let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        meta.insert("name".to_string(), serde_json::json!(name));
        if let Some(ns) = namespace {
            meta.insert("namespace".to_string(), serde_json::json!(ns));
        }
    }
    stamp_type_meta(ctx, &mut body);
    let old_object = match state.store.get(&resource_ref) {
        Ok(v) => v,
        Err(err) => return response::anyhow_error_response(err),
    };
    set_generation(ctx, old_object.as_ref(), &mut body);
    if let Some(old) = old_object.as_ref()
        && let Some(resp) = keep_system_fields(old, &mut body)
    {
        return resp;
    }
    default_service_ports(ctx, &mut body);
    reconcile_service_addresses(state, ctx, &mut body);
    allocate_node_ports(state, ctx, old_object.as_ref(), &mut body);
    if let Some(old) = old_object.as_ref()
        && let Some(resp) =
            check_immutable(ctx, old, &body).or_else(|| check_immutable_fields(ctx, old, &body))
    {
        return resp;
    }
    if let Some(old) = old_object.as_ref()
        && has_status_subresource(ctx)
    {
        // Status is written through /status only.
        copy_field(&mut body, old, &["status"]);
    }
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Update,
        namespace,
        name,
        subresource: None,
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut body, old_object.as_ref()).await
    {
        return admission_error_response(e);
    }
    apply_type_defaults(ctx, &mut body);
    field_validation::prune(ctx, &mut body);
    if let Err(e) = admission::invoke_validating(&admission_ctx, &body, old_object.as_ref()).await {
        return admission_error_response(e);
    }
    if let Some(resp) =
        validate_builtin(ctx, &body).or_else(|| validate_cel_expressions(ctx, &body))
    {
        return resp;
    }
    if let Some(resp) = validate_cr(ctx, &body) {
        return resp;
    }
    if dry_run {
        return response::object_response(&body);
    }
    let body = match to_storage_version(state, ctx, body).await {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    match state.store.update(&resource_ref, &body) {
        Ok(obj) => match present_async(state, ctx, obj).await {
            Ok(o) => response::object_response(&o),
            Err(resp) => resp,
        },
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn update_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (body, findings) = match decode_body(&ctx, &headers, raw_query.as_deref(), &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    let resp = update_impl(&state, &ctx, Some(&ns), &name, body, dry_run).await;
    field_validation::attach_warnings(resp, &findings)
}

/// DELETE on a collection endpoint — k8s' `deletecollection` verb. Honors
/// the same label/field selectors as LIST (the test uses LabelSelector to
/// bound which resources to nuke) plus the propagation policy from the
/// query/body. Returns a Status object on success per upstream.
pub(crate) async fn delete_collection_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    params: &ListParams,
    policy: PropagationPolicy,
) -> Response {
    let label_sel = match params
        .label_selector
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(LabelSelector::parse)
        .transpose()
    {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
    let field_sel = match params
        .field_selector
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(parse_field_selector)
        .transpose()
    {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
    let (field_sel, post_field) = split_field_selector(&state.store, ctx, field_sel);
    let list = match state.store.list(
        ctx.resource_type.storage_gvr(),
        namespace,
        label_sel.as_ref(),
        field_sel.as_ref(),
        None,
        None,
    ) {
        Ok(r) => r,
        Err(err) => return response::anyhow_error_response(err),
    };
    let items = if post_field.is_some() {
        match convert_to(state, ctx, list.items, &api_version(ctx)).await {
            Ok(items) => apply_field_selector(ctx, post_field.as_ref(), items),
            Err(resp) => return resp,
        }
    } else {
        list.items
    };
    for item in items {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if name.is_empty() {
            continue;
        }
        let ns = item
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // Use the item's own namespace if listing across all namespaces;
        // otherwise honour the URL-scoped namespace.
        let effective_ns = namespace.or(ns.as_deref().filter(|s| !s.is_empty()));
        let resp = delete_impl(state, ctx, effective_ns, &name, policy, false).await;
        if !resp.status().is_success() {
            tracing::warn!(
                "deletecollection {}: '{name}' -> {}",
                ctx.resource_type.gvr.resource,
                resp.status()
            );
        }
    }
    response::object_response(&serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Success",
        "details": {"kind": ctx.resource_type.gvr.resource},
    }))
}

pub async fn delete_collection_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(ns): Path<String>,
    Query(params): Query<ListParams>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let policy = extract_propagation_policy(query.as_deref(), &body, content_type);
    delete_collection_impl(&state, &ctx, Some(&ns), &params, policy).await
}

pub async fn delete_collection_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let policy = extract_propagation_policy(query.as_deref(), &body, content_type);
    delete_collection_impl(&state, &ctx, None, &params, policy).await
}

pub async fn update_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (body, findings) = match decode_body(&ctx, &headers, raw_query.as_deref(), &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    let resp = update_impl(&state, &ctx, None, &name, body, dry_run).await;
    field_validation::attach_warnings(resp, &findings)
}

// ─── /status subresource ────────────────────────────────────────────────────
//
// GET returns the whole object. PUT and PATCH write `status` and leave `spec`
// as stored (see `merge_status_write`); admission runs with subResource
// "status", so only webhook rules naming `<resource>/status` or `*/*` fire.

/// True for resources served from a CRD rather than a vendored built-in schema.
pub(crate) fn is_custom_resource(ctx: &RouteContext) -> bool {
    r8s_types::openapi::spec_bytes_for(&ctx.resource_type.gvr.group, &ctx.resource_type.gvr.version)
        .is_none()
}

/// Set `dst[path]` to `src[path]`, removing it when `src` lacks the field.
fn copy_field(dst: &mut serde_json::Value, src: &serde_json::Value, path: &[&str]) {
    let (last, parents) = path.split_last().expect("non-empty path");
    let mut from = Some(src);
    for p in parents {
        from = from.and_then(|v| v.get(p));
    }
    let value = from.and_then(|v| v.get(last)).cloned();
    let mut to = dst;
    for p in parents {
        if !to.get(p).is_some_and(|v| v.is_object()) {
            if value.is_none() {
                return;
            }
            to[p] = serde_json::json!({});
        }
        to = &mut to[p];
    }
    if let Some(obj) = to.as_object_mut() {
        match value {
            Some(v) => {
                obj.insert((*last).to_string(), v);
            }
            None => {
                obj.remove(*last);
            }
        }
    }
}

/// Shape the object a status write stores, mirroring upstream's per-type
/// `statusStrategy.PrepareForUpdate`: a custom resource keeps everything but
/// `status` from the stored object; a built-in takes the request's metadata and
/// status with `spec` from storage, except that Deployments keep their labels
/// and Pods keep deletionTimestamp and ownerReferences.
fn merge_status_write(
    ctx: &RouteContext,
    current: &serde_json::Value,
    candidate: serde_json::Value,
) -> serde_json::Value {
    if is_custom_resource(ctx) {
        let mut merged = current.clone();
        copy_field(&mut merged, &candidate, &["status"]);
        if candidate
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .is_some()
        {
            copy_field(&mut merged, &candidate, &["metadata", "resourceVersion"]);
        }
        return merged;
    }
    let mut merged = candidate;
    copy_field(&mut merged, current, &["spec"]);
    copy_field(&mut merged, current, &["metadata", "generation"]);
    match (
        ctx.resource_type.gvr.group.as_str(),
        ctx.resource_type.gvr.resource.as_str(),
    ) {
        ("apps", "deployments") => copy_field(&mut merged, current, &["metadata", "labels"]),
        ("", "pods") => {
            copy_field(&mut merged, current, &["metadata", "deletionTimestamp"]);
            copy_field(&mut merged, current, &["metadata", "ownerReferences"]);
        }
        _ => {}
    }
    merged
}

pub(crate) async fn status_put_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    mut body: serde_json::Value,
) -> Response {
    let rref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };
    let current = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{} '{}' not found", ctx.resource_type.kind, name),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
    };
    if let Some(meta) = body.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        meta.insert("name".to_string(), serde_json::json!(name));
        if let Some(ns) = namespace {
            meta.insert("namespace".to_string(), serde_json::json!(ns));
        }
    }
    stamp_type_meta(ctx, &mut body);
    let mut merged = merge_status_write(ctx, &current, body);
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Update,
        namespace,
        name,
        subresource: Some("status"),
        dry_run: false,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut merged, Some(&current)).await {
        return admission_error_response(e);
    }
    if let Err(e) = admission::invoke_validating(&admission_ctx, &merged, Some(&current)).await {
        return admission_error_response(e);
    }
    if let Some(resp) = validate_cr(ctx, &merged) {
        return resp;
    }
    match state.store.update(&rref, &merged) {
        Ok(obj) => response::object_response(&presented(ctx, obj)),
        Err(err) => response::anyhow_error_response(err),
    }
}

/// The pod with `candidate`'s ephemeral containers, which may only add to
/// the ones it has: an existing one changed or removed, or a name clashing
/// with another container, is refused.
#[allow(clippy::result_large_err)]
fn merge_ephemeral_containers(
    current: &serde_json::Value,
    candidate: &serde_json::Value,
) -> Result<serde_json::Value, Response> {
    let invalid = |msg: &str| {
        response::status_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid",
            &format!(
                "Pod \"{}\" is invalid: spec.ephemeralContainers: {msg}",
                current["metadata"]["name"].as_str().unwrap_or("")
            ),
        )
    };
    let old: Vec<serde_json::Value> = current["spec"]["ephemeralContainers"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let new: Vec<serde_json::Value> = candidate["spec"]["ephemeralContainers"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for existing in &old {
        if !new.contains(existing) {
            return Err(invalid(&format!(
                "Forbidden: existing ephemeral containers \"{}\" may not be changed",
                existing["name"].as_str().unwrap_or("")
            )));
        }
    }
    let taken: Vec<&str> = ["containers", "initContainers"]
        .iter()
        .flat_map(|list| current["spec"][*list].as_array().into_iter().flatten())
        .filter_map(|c| c["name"].as_str())
        .collect();
    let mut seen: Vec<&str> = Vec::new();
    for ec in &new {
        let name = ec["name"].as_str().unwrap_or("");
        if name.is_empty() {
            return Err(invalid("name: Required value"));
        }
        if taken.contains(&name) || seen.contains(&name) {
            return Err(invalid(&format!("Duplicate value: \"{name}\"")));
        }
        seen.push(name);
    }
    let mut merged = current.clone();
    merged["spec"]["ephemeralContainers"] = serde_json::Value::Array(new);
    Ok(merged)
}

/// PUT on `pods/{name}/ephemeralcontainers`.
pub(crate) async fn ephemeral_containers_put_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    body: serde_json::Value,
) -> Response {
    let rref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };
    let current = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{} '{}' not found", ctx.resource_type.kind, name),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
    };
    let mut merged = match merge_ephemeral_containers(&current, &body) {
        Ok(m) => m,
        Err(resp) => return resp,
    };
    apply_type_defaults(ctx, &mut merged);
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Update,
        namespace,
        name,
        subresource: Some("ephemeralcontainers"),
        dry_run: false,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_validating(&admission_ctx, &merged, Some(&current)).await {
        return admission_error_response(e);
    }
    match state.store.update(&rref, &merged) {
        Ok(obj) => response::object_response(&presented(ctx, obj)),
        Err(err) => response::anyhow_error_response(err),
    }
}

/// PUT on `pods/{name}/ephemeralcontainers`.
pub async fn put_ephemeral_containers_ns(
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
    ephemeral_containers_put_impl(&state, &ctx, Some(&ns), &name, body).await
}

/// PATCH on `pods/{name}/ephemeralcontainers`.
pub async fn patch_ephemeral_containers_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    patch_impl(
        &state,
        &ctx,
        Some(&ns),
        &name,
        &headers,
        body,
        Some("ephemeralcontainers"),
        field_validation::Mode::Ignore,
        false,
    )
    .await
}

pub async fn get_status_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, Some(&ns), &name, &headers).await
}

pub async fn get_status_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, None, &name, &headers).await
}

pub async fn put_status_ns(
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
    status_put_impl(&state, &ctx, Some(&ns), &name, body).await
}

pub async fn put_status_cluster(
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
    status_put_impl(&state, &ctx, None, &name, body).await
}

pub async fn patch_status_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    patch_impl(
        &state,
        &ctx,
        Some(&ns),
        &name,
        &headers,
        body,
        Some("status"),
        field_validation::Mode::Ignore,
        false,
    )
    .await
}

pub async fn patch_status_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    patch_impl(
        &state,
        &ctx,
        None,
        &name,
        &headers,
        body,
        Some("status"),
        field_validation::Mode::Ignore,
        false,
    )
    .await
}
/// How an object's dependents are handled when the object is deleted.
/// Sourced from `DeleteOptions.PropagationPolicy` (or the legacy
/// `?propagationPolicy=` query param). Background is the default upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PropagationPolicy {
    /// Default: return as soon as the object is gone; GC controller cleans
    /// dependents asynchronously via owner-reference cascades.
    Background,
    /// Strip the object's UID from every dependent's `ownerReferences` so
    /// the GC controller doesn't see them as orphans. The dependents stay.
    Orphan,
    /// Set a `foregroundDeletion` finalizer on the object and return; the
    /// object stays around until the GC controller removes the finalizer
    /// after all dependents are gone. (Not yet implemented in r8s — treated
    /// as Background.)
    Foreground,
}

impl PropagationPolicy {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "Background" => Some(Self::Background),
            "Orphan" => Some(Self::Orphan),
            "Foreground" => Some(Self::Foreground),
            _ => None,
        }
    }
}

pub(crate) fn extract_propagation_policy(
    query: Option<&str>,
    body: &Bytes,
    content_type: &str,
) -> PropagationPolicy {
    // 1. Explicit query parameter wins — that's how kubectl and client-go's
    //    DeleteOptions encoder primarily ship the policy.
    if let Some(q) = query {
        for kv in q.split('&') {
            if let Some(v) = kv.strip_prefix("propagationPolicy=")
                && let Some(p) = PropagationPolicy::parse(v)
            {
                return p;
            }
        }
    }
    // 2. JSON body (DELETE may carry a DeleteOptions object).
    if !body.is_empty()
        && !content_type.contains("protobuf")
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(body)
    {
        if let Some(p) = v
            .get("propagationPolicy")
            .and_then(|v| v.as_str())
            .and_then(PropagationPolicy::parse)
        {
            return p;
        }
        // 3. Legacy `orphanDependents: bool`.
        if let Some(b) = v.get("orphanDependents").and_then(|v| v.as_bool()) {
            return if b {
                PropagationPolicy::Orphan
            } else {
                PropagationPolicy::Background
            };
        }
    }
    PropagationPolicy::Background
}

/// Walk every registered GVR and remove `uid` from any dependent's
/// `metadata.ownerReferences`. Called before the actual delete when
/// propagationPolicy=Orphan so the GC controller's cascade no longer sees
/// the children as belonging to a deleted owner.
fn orphan_dependents(state: &AppState, uid: &str) {
    if uid.is_empty() {
        return;
    }
    for rt in state.registry.iter() {
        let items = match state
            .store
            .list(rt.storage_gvr(), None, None, None, None, None)
        {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for mut item in items {
            let Some(refs) = item
                .get("metadata")
                .and_then(|m| m.get("ownerReferences"))
                .and_then(|v| v.as_array())
                .cloned()
            else {
                continue;
            };
            let new_refs: Vec<serde_json::Value> = refs
                .into_iter()
                .filter(|r| r.get("uid").and_then(|v| v.as_str()) != Some(uid))
                .collect();
            // Nothing to strip on this object.
            if new_refs.len()
                == item
                    .get("metadata")
                    .and_then(|m| m.get("ownerReferences"))
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0)
            {
                continue;
            }
            let name = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ns = item
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(meta) = item.get_mut("metadata").and_then(|v| v.as_object_mut()) {
                if new_refs.is_empty() {
                    meta.remove("ownerReferences");
                } else {
                    meta.insert(
                        "ownerReferences".to_string(),
                        serde_json::Value::Array(new_refs),
                    );
                }
                // Blind write — the store would otherwise reject if a
                // controller raced our list-snapshot with another update.
                // Orphaning must not silently no-op or the GC follow-up
                // will see the still-attached ref and delete the dependent.
                meta.remove("resourceVersion");
            }
            let rref = ResourceRef {
                gvr: rt.storage_gvr(),
                namespace: ns.as_deref(),
                name: &name,
            };
            let _ = state.store.update(&rref, &item);
        }
    }
}

/// Read-modify-write of a stored object, re-read and retried when a
/// concurrent writer wins: the server's own writes (deletion marks) never
/// surface as a client conflict. `mutate` returns false when there is
/// nothing to change; `Ok(None)` then, or when the object is gone.
fn update_retrying(
    store: &r8s_store::Store,
    rref: &ResourceRef,
    mut mutate: impl FnMut(&mut serde_json::Value) -> bool,
) -> r8s_store::error::Result<Option<serde_json::Value>> {
    let mut attempts = 0;
    loop {
        let Some(mut current) = store.get(rref)? else {
            return Ok(None);
        };
        if !mutate(&mut current) {
            return Ok(None);
        }
        match store.update(rref, &current) {
            Ok(obj) => return Ok(Some(obj)),
            Err(r8s_store::StoreError::Conflict { .. }) if attempts < 10 => attempts += 1,
            Err(e) => return Err(e),
        }
    }
}

pub(crate) async fn delete_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    policy: PropagationPolicy,
    dry_run: bool,
) -> Response {
    let rref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };

    // Look up the object once: validating webhooks need oldObject; orphan
    // propagation needs the uid; the missing-resource case wants a 404.
    let existing = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return response::status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("{} '{}' not found", ctx.resource_type.kind, name),
            );
        }
        Err(err) => return response::anyhow_error_response(err),
    };

    // Only validating webhooks fire for DELETE (no mutation possible).
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Delete,
        namespace,
        name,
        subresource: None,
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_validating(&admission_ctx, &existing, Some(&existing)).await {
        return admission_error_response(e);
    }

    if dry_run {
        return response::object_response(&presented(ctx, existing));
    }

    if matches!(policy, PropagationPolicy::Orphan) {
        let uid = existing
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        if !uid.is_empty() {
            // A terminating owner adopts nothing, so the references stripped
            // below stay off until it is gone.
            let stamped = update_retrying(&state.store, &rref, |current| {
                if !current["metadata"]["deletionTimestamp"].is_null() {
                    return false;
                }
                current["metadata"]["deletionTimestamp"] =
                    serde_json::json!(chrono::Utc::now().to_rfc3339());
                true
            });
            if let Err(e) = stamped {
                tracing::warn!("orphan delete of '{name}': marking terminating failed: {e}");
            }
            orphan_dependents(state, &uid);
        }
    }

    if matches!(policy, PropagationPolicy::Foreground) {
        // Foreground deletion: stamp `metadata.deletionTimestamp` and add the
        // `foregroundDeletion` finalizer instead of actually removing the
        // object. The GC controller watches for this state, cascades to
        // dependents, then strips the finalizer once they're gone — at which
        // point a follow-up delete (issued by the GC) finally erases the
        // parent. Foreground returns 200 with the marked-for-deletion object.
        match state.store.get(&rref) {
            Ok(Some(_)) => {
                match update_retrying(&state.store, &rref, mark_foreground_deletion) {
                    Ok(Some(obj)) => return response::object_response(&presented(ctx, obj)),
                    // Nothing to mark (already marked) — fall through to actual delete.
                    Ok(None) => {}
                    Err(err) => return response::anyhow_error_response(err),
                }
            }
            Ok(None) => {
                return response::status_error(
                    StatusCode::NOT_FOUND,
                    "NotFound",
                    &format!("{} '{}' not found", ctx.resource_type.kind, name),
                );
            }
            Err(err) => return response::anyhow_error_response(err),
        }
    }

    // A deleted pod leaves with deletionTimestamp and deletionGracePeriodSeconds
    // set, as upstream's graceful deletion stamps it before the kubelet removes it.
    if ctx.resource_type.gvr.resource == "pods" && ctx.resource_type.gvr.group.is_empty() {
        let stamped = update_retrying(&state.store, &rref, |current| {
            if !current["metadata"]["deletionTimestamp"].is_null() {
                return false;
            }
            let grace = current["spec"]["terminationGracePeriodSeconds"]
                .as_i64()
                .unwrap_or(30);
            current["metadata"]["deletionTimestamp"] =
                serde_json::json!(chrono::Utc::now().to_rfc3339());
            current["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(grace);
            true
        });
        if let Err(e) = stamped {
            tracing::debug!("pod '{name}': deletion stamp skipped: {e}");
        }
    }

    match state.store.delete(&rref) {
        Ok(Some(obj)) => response::object_response(&presented(ctx, obj)),
        // The orphan delete's terminating mark lets the garbage collector
        // remove the object first; the delete still succeeded.
        Ok(None) if matches!(policy, PropagationPolicy::Orphan) => {
            response::object_response(&presented(ctx, existing))
        }
        Ok(None) => response::status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("{} '{}' not found", ctx.resource_type.kind, name),
        ),
        Err(err) => response::anyhow_error_response(err),
    }
}

/// Stamp `metadata.deletionTimestamp` (RFC3339) and add the
/// `foregroundDeletion` finalizer to `obj`. Returns true if the object was
/// modified (false if it was already marked).
fn mark_foreground_deletion(obj: &mut serde_json::Value) -> bool {
    let meta = match obj.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        Some(m) => m,
        None => return false,
    };
    let mut changed = false;
    if !meta.contains_key("deletionTimestamp") {
        meta.insert(
            "deletionTimestamp".to_string(),
            serde_json::json!(chrono::Utc::now().to_rfc3339()),
        );
        changed = true;
    }
    let finalizers = meta
        .entry("finalizers".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    if let Some(arr) = finalizers.as_array_mut() {
        let has = arr
            .iter()
            .any(|v| v.as_str() == Some(r8s_types::FOREGROUND_FINALIZER));
        if !has {
            arr.push(serde_json::Value::String(
                r8s_types::FOREGROUND_FINALIZER.into(),
            ));
            changed = true;
        }
    }
    changed
}

pub async fn delete_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let policy = extract_propagation_policy(query.as_deref(), &body, content_type);
    let dry_run = parse_dry_run(query.as_deref());
    delete_impl(&state, &ctx, Some(&ns), &name, policy, dry_run).await
}

pub async fn delete_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let policy = extract_propagation_policy(query.as_deref(), &body, content_type);
    let dry_run = parse_dry_run(query.as_deref());
    delete_impl(&state, &ctx, None, &name, policy, dry_run).await
}

/// Root schema for a server-side apply merge: the vendored definition for a
/// built-in, else the CRD's structural schema.
fn apply_schema(ctx: &RouteContext) -> Option<&serde_json::Value> {
    let rt = &ctx.resource_type;
    r8s_types::openapi::definition_for(&rt.gvr.group, &rt.gvr.version, &rt.kind)
        .or(rt.schema.as_ref())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn patch_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    bytes: Bytes,
    subresource: Option<&str>,
    validation: field_validation::Mode,
    dry_run: bool,
) -> Response {
    let rref = ResourceRef {
        gvr: ctx.resource_type.storage_gvr(),
        namespace,
        name,
    };
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // RFC 6902 JSON Patch sends an array of ops; RFC 7396 JSON Merge Patch and
    // strategic merge patch send an object. Apply and strategic bodies merge
    // list-maps by key (`crate::apply`); strategic patches are built-in only.
    let is_json_patch = content_type.contains("json-patch+json");
    let is_apply = content_type.contains("apply-patch");
    let is_strategic = content_type.contains("strategic-merge-patch");
    if is_strategic && is_custom_resource(ctx) {
        return response::status_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UnsupportedMediaType",
            "the body of the request was in an unknown format - accepted media types include: \
             application/json-patch+json, application/merge-patch+json, application/apply-patch+yaml",
        );
    }
    // Apply bodies are YAML, of which JSON is a subset; the other patch types
    // are JSON. A repeated YAML key fails the conversion outright.
    let parsed: Result<field_validation::Decoded, String> = if is_apply {
        field_validation::decode_yaml(&bytes).map_err(|e| e.to_string())
    } else {
        field_validation::decode_json(&bytes).map_err(|e| e.to_string())
    };
    let field_validation::Decoded {
        value: patch,
        duplicates,
    } = match parsed {
        Ok(v) => v,
        Err(e) => {
            return response::status_error(
                StatusCode::BAD_REQUEST,
                "Invalid",
                &format!("invalid patch body: {e}"),
            );
        }
    };
    if is_apply && !duplicates.is_empty() {
        return response::status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            &field_validation::yaml_duplicate_error(&bytes, &duplicates),
        );
    }
    // Apply configurations are checked as sent; other patches are checked as
    // the object they produce.
    #[allow(clippy::result_large_err)]
    let findings_for =
        |checked: &serde_json::Value| -> Result<field_validation::Findings, Response> {
            field_validation::check(
                ctx,
                validation,
                field_validation::Decoded {
                    value: checked.clone(),
                    duplicates: duplicates.clone(),
                },
            )
            .map(|(_, findings)| findings)
        };
    // Patches in k8s are blind by default — only when the client explicitly
    // included a `metadata.resourceVersion` in the patch body should the
    // server enforce optimistic concurrency. The store does this check based
    // on the incoming object's metadata.resourceVersion, so unless the patch
    // bumped it (rare), we strip it before update to avoid spurious 409s
    // when a controller (e.g. quota status) races the client.
    let patch_carries_rv = match &patch {
        serde_json::Value::Object(obj) => obj
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .is_some(),
        _ => false,
    };
    match state.store.get(&rref) {
        Ok(Some(stored)) => {
            // Patches apply to the object in the requested version.
            let mut current = match convert_to(state, ctx, vec![stored], &api_version(ctx)).await {
                Ok(mut v) => v.pop().unwrap_or_default(),
                Err(resp) => return resp,
            };
            let old_object = current.clone();
            if is_json_patch {
                if let Err(e) = crate::jsonpatch::apply(&mut current, &patch) {
                    return response::status_error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "Invalid",
                        &format!("json patch failed: {e}"),
                    );
                }
            } else if is_apply {
                crate::apply::apply(&mut current, &patch, apply_schema(ctx));
            } else if is_strategic {
                crate::apply::strategic_merge(&mut current, &patch, apply_schema(ctx));
            } else {
                json_merge_patch(&mut current, &patch);
            }
            if !patch_carries_rv
                && let Some(meta) = current.get_mut("metadata").and_then(|v| v.as_object_mut())
            {
                meta.remove("resourceVersion");
            }
            let findings = match findings_for(if is_apply { &patch } else { &current }) {
                Ok(f) => f,
                Err(resp) => return resp,
            };
            if subresource == Some("status") {
                current = merge_status_write(ctx, &old_object, current);
            } else if subresource == Some("ephemeralcontainers") {
                current = match merge_ephemeral_containers(&old_object, &current) {
                    Ok(c) => c,
                    Err(resp) => return resp,
                };
            } else {
                set_generation(ctx, Some(&old_object), &mut current);
                if has_status_subresource(ctx) {
                    copy_field(&mut current, &old_object, &["status"]);
                }
                if let Some(resp) = check_immutable(ctx, &old_object, &current)
                    .or_else(|| check_immutable_fields(ctx, &old_object, &current))
                {
                    return resp;
                }
            }
            let admission_ctx = AdmissionCtx {
                store: &state.store,
                gvr: &ctx.resource_type.gvr,
                kind: &ctx.resource_type.kind,
                operation: Operation::Update,
                namespace,
                name,
                subresource,
                dry_run,
                user_info: serde_json::json!({}),
            };
            if let Err(e) =
                admission::invoke_mutating(&admission_ctx, &mut current, Some(&old_object)).await
            {
                return admission_error_response(e);
            }
            apply_type_defaults(ctx, &mut current);
            field_validation::prune(ctx, &mut current);
            if let Err(e) =
                admission::invoke_validating(&admission_ctx, &current, Some(&old_object)).await
            {
                return admission_error_response(e);
            }
            if let Some(resp) = validate_cr(ctx, &current) {
                return resp;
            }
            if dry_run {
                return field_validation::attach_warnings(
                    response::object_response(&presented(ctx, current)),
                    &findings,
                );
            }
            let current = match to_storage_version(state, ctx, current).await {
                Ok(b) => b,
                Err(resp) => return resp,
            };
            match state.store.update(&rref, &current) {
                Ok(obj) => match present_async(state, ctx, obj).await {
                    Ok(o) => {
                        field_validation::attach_warnings(response::object_response(&o), &findings)
                    }
                    Err(resp) => resp,
                },
                Err(err) => response::anyhow_error_response(err),
            }
        }
        Ok(None) => {
            if is_json_patch {
                return response::status_error(
                    StatusCode::NOT_FOUND,
                    "NotFound",
                    &format!("{} '{}' not found", ctx.resource_type.kind, name),
                );
            }
            let findings = match findings_for(&patch) {
                Ok(f) => f,
                Err(resp) => return resp,
            };
            // Server-side apply: create if not found
            let mut body = if is_apply {
                let mut fresh = serde_json::Value::Null;
                crate::apply::apply(&mut fresh, &patch, apply_schema(ctx));
                fresh
            } else {
                patch
            };
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
            stamp_type_meta(ctx, &mut body);
            set_generation(ctx, None, &mut body);
            maybe_allocate_cluster_ip(state, ctx, &mut body);
            default_service_ports(ctx, &mut body);
            allocate_node_ports(state, ctx, None, &mut body);
            default_cr(ctx, &mut body);
            default_namespace_status(ctx, &mut body);
            apply_type_defaults(ctx, &mut body);
            let admission_ctx = AdmissionCtx {
                store: &state.store,
                gvr: &ctx.resource_type.gvr,
                kind: &ctx.resource_type.kind,
                operation: Operation::Create,
                namespace,
                name,
                subresource: None,
                dry_run,
                user_info: serde_json::json!({}),
            };
            if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut body, None).await {
                return admission_error_response(e);
            }
            apply_type_defaults(ctx, &mut body);
            field_validation::prune(ctx, &mut body);
            if let Some(resp) = admit_runtime_class(&state.store, ctx, &mut body) {
                return resp;
            }
            if let Some(resp) = apply_limit_ranges(&state.store, ctx, &mut body) {
                return resp;
            }
            if let Some(resp) = admit_priority(&state.store, ctx, &mut body) {
                return resp;
            }
            set_pod_qos_class(ctx, &mut body);
            set_initial_phase(ctx, &mut body);
            fill_system_fields_on_create(&mut body);
            if let Err(e) = admission::invoke_validating(&admission_ctx, &body, None).await {
                return admission_error_response(e);
            }
            if let Some(resp) =
                validate_builtin(ctx, &body).or_else(|| validate_cel_expressions(ctx, &body))
            {
                return resp;
            }
            if let Some(resp) = validate_cr(ctx, &body) {
                return resp;
            }
            if dry_run {
                return field_validation::attach_warnings(
                    response::created_response(&body),
                    &findings,
                );
            }
            let body = match to_storage_version(state, ctx, body).await {
                Ok(b) => b,
                Err(resp) => return resp,
            };
            match state.store.create(rref, &body) {
                Ok(obj) => match present_async(state, ctx, obj).await {
                    Ok(o) => {
                        field_validation::attach_warnings(response::created_response(&o), &findings)
                    }
                    Err(resp) => resp,
                },
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
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let dry_run = parse_dry_run(raw_query.as_deref());
    patch_impl(
        &state,
        &ctx,
        Some(&ns),
        &name,
        &headers,
        body,
        None,
        field_validation::mode(raw_query.as_deref()),
        dry_run,
    )
    .await
}

pub async fn patch_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let dry_run = parse_dry_run(raw_query.as_deref());
    patch_impl(
        &state,
        &ctx,
        None,
        &name,
        &headers,
        body,
        None,
        field_validation::mode(raw_query.as_deref()),
        dry_run,
    )
    .await
}

fn api_version(ctx: &RouteContext) -> String {
    ctx.resource_type.gvr.api_version()
}

/// Objects are stored in the apiVersion they were written with. A type whose
/// served versions share storage is presented in the version requested, and a
/// custom resource gets its schema defaults applied on the way out, so a
/// default added to the CRD later shows up on objects already stored.
fn presented(ctx: &RouteContext, mut obj: serde_json::Value) -> serde_json::Value {
    default_cr(ctx, &mut obj);
    match ctx.resource_type.storage_gvr {
        Some(_) => present_as(obj, &api_version(ctx)),
        None => obj,
    }
}

fn present_as(mut obj: serde_json::Value, api_version: &str) -> serde_json::Value {
    if let Some(o) = obj.as_object_mut() {
        o.insert("apiVersion".to_string(), serde_json::json!(api_version));
    }
    obj
}

pub(crate) async fn list_impl(
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
            params.allow_watch_bookmarks(),
            params.label_selector.as_deref(),
            params.field_selector.as_deref(),
        )
        .await;
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
        .map(parse_field_selector)
        .transpose();
    let field_sel = match field_sel {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };

    let limit = params.limit.map(|l| l as usize);
    let (field_sel, post_field) = split_field_selector(&state.store, ctx, field_sel);

    match state.store.list(
        ctx.resource_type.storage_gvr(),
        namespace,
        label_sel.as_ref(),
        field_sel.as_ref(),
        limit,
        params.continue_token.as_deref(),
    ) {
        Ok(result) => {
            let items = match present_all(state, ctx, result.items).await {
                Ok(items) => items,
                Err(resp) => return resp,
            };
            let items = apply_field_selector(ctx, post_field.as_ref(), items);
            let result = r8s_store::backend::ListResult { items, ..result };
            if wants_table(headers) {
                table::table_response(
                    &table::columns_for(&ctx.resource_type.gvr.resource),
                    &result.items,
                    &ctx.resource_type.gvr.resource,
                    Some(result.resource_version),
                )
            } else if wants_partial_metadata(headers) {
                let items: Vec<_> = result
                    .items
                    .iter()
                    .map(to_partial_object_metadata)
                    .collect();
                response::list_response(
                    "meta.k8s.io/v1",
                    "PartialObjectMetadata",
                    result.resource_version,
                    result.continue_token.as_deref(),
                    result.remaining_item_count,
                    items,
                )
            } else {
                response::list_response(
                    &api_version(ctx),
                    &ctx.resource_type.kind,
                    result.resource_version,
                    result.continue_token.as_deref(),
                    result.remaining_item_count,
                    result.items,
                )
            }
        }
        Err(err) => response::anyhow_error_response(err),
    }
}

/// Field-default policy for built-in Kubernetes types: the value to assume when
/// an object omits a selectable field, mirroring apiserver defaulting. The store
/// is type-agnostic and injects this when building a field selector.
fn k8s_field_defaults(object: &serde_json::Value, gvr: &str, field: &str) -> Option<String> {
    match (gvr, field) {
        ("/v1/nodes", "spec.unschedulable") => Some("false".to_string()),
        ("/v1/pods", "spec.nodeName") => Some(String::new()),
        // An Event's `source` falls back to its reporting controller.
        ("/v1/events", "source.component") => Some(
            object
                .get("reportingComponent")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        ),
        _ => None,
    }
}

/// Parse a field selector and attach this distribution's field defaults.
fn parse_field_selector(raw: &str) -> r8s_store::error::Result<FieldSelector> {
    FieldSelector::parse(&selectable_fields(raw))
        .map(|selector| selector.with_defaults(k8s_field_defaults))
}

/// The selector with the selectable-field aliases that don't name a JSON
/// path resolved: an Event's `source` is its `source.component`.
fn selectable_fields(raw: &str) -> String {
    const ALIASES: &[(&str, &str)] = &[
        ("source", "source.component"),
        ("reportingController", "reportingComponent"),
        ("regarding.", "involvedObject."),
        ("deprecatedSource", "source.component"),
    ];
    raw.split(',')
        .map(|term| {
            let key = term
                .trim()
                .split_once('=')
                .map(|(k, _)| k.trim_end_matches('!'));
            for (alias, stored) in ALIASES {
                let hit = match key {
                    Some(k) if alias.ends_with('.') => k.starts_with(alias),
                    Some(k) => k == *alias,
                    None => false,
                };
                if hit {
                    return term.trim().replacen(alias, stored, 1);
                }
            }
            term.to_string()
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Apply the namespace path component plus the optional label/field selectors
/// from the watch query to a candidate event object. Returns true if the
/// client should see this event.
fn event_matches(
    obj: &serde_json::Value,
    namespace: Option<&str>,
    label_sel: Option<&LabelSelector>,
    field_sel: Option<&FieldSelector>,
    gvr_key: &str,
) -> bool {
    if let Some(ns) = namespace {
        let obj_ns = obj
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str());
        if obj_ns != Some(ns) {
            return false;
        }
    }
    if let Some(ls) = label_sel
        && !ls.matches(obj)
    {
        return false;
    }
    if let Some(fs) = field_sel
        && !fs.matches(obj, gvr_key)
    {
        return false;
    }
    true
}

/// What a filtered watcher sees for a store event: an object entering the
/// selector is ADDED, one leaving it is DELETED (its previous state at the
/// event's resourceVersion), one outside it is nothing.
fn filtered_event(
    event: &r8s_store::watch::WatchEvent,
    namespace: Option<&str>,
    label_sel: Option<&LabelSelector>,
    field_sel: Option<&FieldSelector>,
    gvr_key: &str,
) -> Option<(&'static str, std::sync::Arc<serde_json::Value>)> {
    let matches =
        |obj: &serde_json::Value| event_matches(obj, namespace, label_sel, field_sel, gvr_key);
    let current = matches(&event.object);
    match event.event_type {
        WatchEventType::Added => current.then(|| ("ADDED", event.object.clone())),
        WatchEventType::Deleted => current.then(|| ("DELETED", event.object.clone())),
        WatchEventType::Modified => {
            let previous = event
                .prev_object
                .as_ref()
                .map(|p| matches(p))
                .unwrap_or(current);
            match (previous, current) {
                (false, true) => Some(("ADDED", event.object.clone())),
                (true, true) => Some(("MODIFIED", event.object.clone())),
                (true, false) => {
                    let mut gone = (**event.prev_object.as_ref()?).clone();
                    if let Some(meta) = gone.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                        meta.insert(
                            "resourceVersion".into(),
                            serde_json::json!(event.resource_version.to_string()),
                        );
                    }
                    Some(("DELETED", std::sync::Arc::new(gone)))
                }
                (false, false) => None,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn watch_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    headers: &HeaderMap,
    resource_version: Option<&str>,
    send_initial_events: bool,
    allow_watch_bookmarks: bool,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Response {
    // Parse selectors upfront so a malformed query string returns 400 instead
    // of silently degrading to an unfiltered watch.
    let label_sel = match label_selector
        .filter(|s| !s.is_empty())
        .map(LabelSelector::parse)
        .transpose()
    {
        Ok(s) => s.map(std::sync::Arc::new),
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
    let field_sel = match field_selector
        .filter(|s| !s.is_empty())
        .map(parse_field_selector)
        .transpose()
    {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
    let (field_sel, post_field) = split_field_selector(&state.store, ctx, field_sel);
    let field_sel = field_sel.map(std::sync::Arc::new);
    // Events are filtered once their objects are in the request version, so
    // either half of the selector applies there.
    let event_field = field_sel
        .clone()
        .or_else(|| post_field.map(std::sync::Arc::new));
    let gvr_key = ctx.resource_type.gvr.key_prefix();
    // Three watch modes, depending on what the client asked for:
    //
    //   1. `sendInitialEvents=true` or no resourceVersion / rv=0 (the "fresh
    //      reflector" case): list the current state, emit each as ADDED, then
    //      tail the live broadcast. KEP-3157 BOOKMARK is emitted between the
    //      replay and the live tail when sendInitialEvents=true.
    //
    //   2. resourceVersion=N > 0: replay buffered events with rv > N from the
    //      WatchHub's history (so an apiextensions test that creates+deletes
    //      then watches at the created rv sees the DELETE), then live tail.
    //      If N is older than the oldest buffered event we return 410 Gone so
    //      the client falls back to list-then-watch.
    //
    //   3. (Fallthrough) — the watch is just a live tail, no replay.
    let replay_initial =
        send_initial_events || matches!(resource_version, None | Some("") | Some("0"));
    let since_rv = if replay_initial {
        0
    } else {
        resource_version
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };

    let (items, rv, history, rx) = if replay_initial {
        match state.store.list_and_watch(
            ctx.resource_type.storage_gvr(),
            namespace,
            label_sel.as_deref(),
            field_sel.as_deref(),
        ) {
            Ok((result, rx)) => (result.items, result.resource_version, Vec::new(), rx),
            Err(err) => return response::store_error_response(&err),
        }
    } else {
        match state
            .store
            .watch_from(ctx.resource_type.storage_gvr(), since_rv)
        {
            Ok((history, rx)) => (Vec::new(), since_rv, history, rx),
            Err(err) => return response::store_error_response(&err),
        }
    };

    // Objects stored in another served version are converted on the way out.
    let desired_version = api_version(ctx);
    let items = match convert_to(state, ctx, items, &desired_version).await {
        Ok(items) => apply_field_selector(ctx, event_field.as_deref(), items),
        Err(resp) => return resp,
    };
    let history = match convert_events(state, ctx, history, &desired_version).await {
        Ok(history) => history,
        Err(resp) => return resp,
    };
    let live_conversion = ctx
        .resource_type
        .storage_gvr
        .as_ref()
        .and_then(|_| crate::conversion::Conversion::for_resource(&state.store, ctx));
    let live_store = state.store.clone();
    let live_events = crate::event_conversion::applies(ctx);

    let as_table = wants_table(headers);
    let as_partial = wants_partial_metadata(headers);
    let resource = ctx.resource_type.gvr.resource.clone();
    let columns = if as_table {
        Some(std::sync::Arc::new(table::columns_for(&resource)))
    } else {
        None
    };
    let served_api_version: Option<String> = ctx
        .resource_type
        .storage_gvr
        .as_ref()
        .map(|_| api_version(ctx));
    // Custom resources get their schema defaults on the way out, like reads do.
    let cr_schema: Option<serde_json::Value> = ctx.resource_type.schema.clone().filter(|_| {
        r8s_types::openapi::spec_bytes_for(
            &ctx.resource_type.gvr.group,
            &ctx.resource_type.gvr.version,
        )
        .is_none()
    });
    let format_object = {
        let columns = columns.clone();
        let resource = resource.clone();
        move |obj: &serde_json::Value| -> serde_json::Value {
            let owned;
            let obj = if cr_schema.is_some() || served_api_version.is_some() {
                let mut o = obj.clone();
                if let Some(schema) = &cr_schema {
                    crate::schema_default::apply_defaults(&mut o, schema);
                }
                if let Some(av) = &served_api_version {
                    o = present_as(o, av);
                }
                owned = o;
                &owned
            } else {
                obj
            };
            if let Some(cols) = columns.as_ref() {
                table::watch_table_object(cols, obj, &resource)
            } else if as_partial {
                to_partial_object_metadata(obj)
            } else {
                obj.clone()
            }
        }
    };

    let format_initial = format_object.clone();
    let initial = items.into_iter().map(move |obj| {
        Ok::<_, std::io::Error>(response::watch_event_line("ADDED", &format_initial(&obj)))
    });
    let initial_stream = tokio_stream::iter(initial);

    let format_history = format_object.clone();
    let history_ns_filter: Option<String> = namespace.map(|s| s.to_string());
    let history_label = label_sel.clone();
    let history_field = event_field.clone();
    let history_gvr = gvr_key.clone();
    let history_iter = history.into_iter().filter_map(move |event| {
        let (type_str, object) = filtered_event(
            &event,
            history_ns_filter.as_deref(),
            history_label.as_deref(),
            history_field.as_deref(),
            &history_gvr,
        )?;
        let payload = format_history(&object);
        Some(Ok::<_, std::io::Error>(response::watch_event_line(
            type_str, &payload,
        )))
    });
    let history_stream = tokio_stream::iter(history_iter);

    // BOOKMARK emission policy:
    //   - sendInitialEvents=true → MUST emit one BOOKMARK after the initial
    //     replay carrying `k8s.io/initial-events-end: true` (KEP-3157, used
    //     by k9s and the WatchList reflector).
    //   - allowWatchBookmarks=true → MAY emit BOOKMARK at the server's
    //     discretion; we send one to mark the watch's starting RV so
    //     reflectors can resync efficiently.
    //   - Neither → MUST NOT emit BOOKMARK. The apiextensions watch-cache
    //     primer expects the next event after the initial state to be the
    //     actual resource event (e.g. DELETE) and treats a BOOKMARK as a
    //     test failure.
    let emit_bookmark = send_initial_events || allow_watch_bookmarks;
    let av = api_version(ctx);
    let bookmark = if emit_bookmark {
        let mut bookmark_meta = serde_json::json!({"resourceVersion": rv.to_string()});
        if send_initial_events && let Some(obj) = bookmark_meta.as_object_mut() {
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
        tokio_stream::iter(Some(Ok::<_, std::io::Error>(response::watch_event_line(
            "BOOKMARK",
            &bookmark_formatted,
        ))))
    } else {
        tokio_stream::iter(None)
    };

    let ns_filter: Option<String> = namespace.map(|s| s.to_string());
    let pre_ns = ns_filter.clone();
    let pre_label = label_sel.clone();
    let pre_gvr = gvr_key.clone();
    let live_label = label_sel.clone();
    let live_field = event_field.clone();
    let live_gvr = gvr_key.clone();
    // On broadcast lag, terminate so the client reconnects (standard K8s behavior).
    let live_stream = BroadcastStream::new(rx)
        .take_while(|result| result.is_ok())
        .filter_map(move |result| {
            // Namespace and labels do not change with the version: events
            // neither side of a transition could match skip conversion.
            let event = result.ok()?;
            let meta_matches = |obj: &serde_json::Value| {
                event_matches(obj, pre_ns.as_deref(), pre_label.as_deref(), None, &pre_gvr)
            };
            let relevant = meta_matches(&event.object)
                || event.prev_object.as_deref().is_some_and(meta_matches);
            relevant.then_some(event)
        })
        .then(move |event| {
            let conversion = live_conversion.clone();
            let store = live_store.clone();
            let desired = desired_version.clone();
            async move {
                match conversion {
                    Some(c) => convert_event(&c, &store, event, &desired).await,
                    None if live_events => {
                        let mut event = event;
                        event.object = std::sync::Arc::new(crate::event_conversion::convert(
                            (*event.object).clone(),
                            &desired,
                        ));
                        if let Some(prev) = event.prev_object.take() {
                            event.prev_object = Some(std::sync::Arc::new(
                                crate::event_conversion::convert((*prev).clone(), &desired),
                            ));
                        }
                        Some(event)
                    }
                    None => Some(event),
                }
            }
        })
        .filter_map(std::convert::identity)
        .filter_map(move |event| {
            let (type_str, object) = filtered_event(
                &event,
                ns_filter.as_deref(),
                live_label.as_deref(),
                live_field.as_deref(),
                &live_gvr,
            )?;
            let payload = format_object(&object);
            Some(Ok::<_, std::io::Error>(response::watch_event_line(
                type_str, &payload,
            )))
        });

    let stream = initial_stream
        .chain(history_stream)
        .chain(bookmark)
        .chain(live_stream);

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
    list_impl(&state, &ctx, Some(&ns), params, &headers).await
}

pub async fn list_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Response {
    list_impl(&state, &ctx, None, params, &headers).await
}

pub async fn list_all_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
) -> Response {
    list_impl(&state, &ctx, None, params, &headers).await
}

pub async fn pod_logs_ns(
    State(state): State<AppState>,
    Path((ns, name)): Path<(String, String)>,
    Query(params): Query<LogParams>,
    req: axum::extract::Request,
) -> Response {
    // A WebSocket client (the `binary.k8s.io` subprotocol) gets the log
    // bytes as binary frames.
    let ws = if req
        .headers()
        .get(axum::http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
    {
        use axum::extract::FromRequestParts;
        let (mut parts, _) = req.into_parts();
        axum::extract::ws::WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .ok()
    } else {
        None
    };
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

    let all_statuses: Vec<&serde_json::Value> = [
        "containerStatuses",
        "initContainerStatuses",
        "ephemeralContainerStatuses",
    ]
    .iter()
    .flat_map(|list| pod["status"][*list].as_array().into_iter().flatten())
    .collect();
    let status = if let Some(ref c) = params.container {
        all_statuses
            .iter()
            .find(|cs| cs.get("name").and_then(|v| v.as_str()) == Some(c.as_str()))
            .copied()
    } else {
        pod["status"]["containerStatuses"]
            .as_array()
            .and_then(|s| s.first())
    };

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
        .join(format!("{container_id}.log"));

    if let Some(ws) = ws {
        let follow = params.follow;
        let timestamps = params.timestamps;
        let tail_lines = params.tail_lines;
        let since = log_since(&params);
        let limit_bytes = params.limit_bytes;
        return ws
            .protocols(["binary.k8s.io"])
            .on_upgrade(move |mut socket| async move {
                use axum::extract::ws::Message;
                use futures_util::SinkExt;
                use tokio_stream::StreamExt;
                if follow {
                    let mut stream = follow_log_stream(log_path, timestamps, tail_lines);
                    while let Some(Ok(chunk)) = stream.next().await {
                        if socket.send(Message::Binary(chunk)).await.is_err() {
                            return;
                        }
                    }
                } else {
                    let raw = tokio::fs::read_to_string(&log_path)
                        .await
                        .unwrap_or_default();
                    let output =
                        render_cri_log_window(&raw, timestamps, tail_lines, since, limit_bytes);
                    let _ = socket.send(Message::Binary(output.into())).await;
                }
                let _ = socket.close().await;
            });
    }

    if params.follow {
        let stream = follow_log_stream(log_path, params.timestamps, params.tail_lines);
        return Response::builder()
            .status(200)
            .header("content-type", "text/plain")
            .header("transfer-encoding", "chunked")
            .body(Body::from_stream(stream))
            .expect("valid response");
    }

    let raw = tokio::fs::read_to_string(&log_path)
        .await
        .unwrap_or_default();

    let output = render_cri_log_window(
        &raw,
        params.timestamps,
        params.tail_lines,
        log_since(&params),
        params.limit_bytes,
    );

    Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Body::from(output))
        .expect("valid response")
}

#[cfg(test)]
mod field_default_tests {
    use super::parse_field_selector;

    #[test]
    fn event_source_selects_the_component() {
        let sel = parse_field_selector("involvedObject.kind=Pod,source=kubelet").unwrap();
        let kubelet = serde_json::json!({
            "involvedObject": {"kind": "Pod"},
            "source": {"component": "kubelet", "host": "n1"},
        });
        let scheduler = serde_json::json!({
            "involvedObject": {"kind": "Pod"},
            "source": {"component": "default-scheduler"},
        });
        assert!(sel.matches(&kubelet, "/v1/events"));
        assert!(!sel.matches(&scheduler, "/v1/events"));
        let not = parse_field_selector("source!=kubelet").unwrap();
        assert!(not.matches(&scheduler, "/v1/events"));
        assert!(!not.matches(&kubelet, "/v1/events"));
    }

    #[test]
    fn immutable_configmaps_keep_their_data() {
        use super::{RouteContext, check_immutable};
        use r8s_types::{GroupVersionResource, ResourceType};
        use std::sync::Arc;
        let ctx = RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::configmaps(),
                kind: "ConfigMap".into(),
                namespaced: true,
                singular: "configmap".into(),
                short_names: vec![],
                subresources: vec![],
                schema: None,
                storage_gvr: None,
            }),
        };
        let old =
            serde_json::json!({"metadata": {"name": "c"}, "immutable": true, "data": {"a": "1"}});
        assert!(check_immutable(&ctx, &old, &old).is_none());
        let relabeled = serde_json::json!({"metadata": {"name": "c", "labels": {"x": "y"}}, "immutable": true, "data": {"a": "1"}});
        assert!(check_immutable(&ctx, &old, &relabeled).is_none());
        let changed =
            serde_json::json!({"metadata": {"name": "c"}, "immutable": true, "data": {"a": "2"}});
        assert!(check_immutable(&ctx, &old, &changed).is_some());
        let mutable_again =
            serde_json::json!({"metadata": {"name": "c"}, "immutable": false, "data": {"a": "1"}});
        assert!(check_immutable(&ctx, &old, &mutable_again).is_some());
        let was_mutable = serde_json::json!({"metadata": {"name": "c"}, "data": {"a": "1"}});
        assert!(check_immutable(&ctx, &was_mutable, &changed).is_none());
    }

    #[test]
    fn ephemeral_containers_only_grow() {
        use super::merge_ephemeral_containers;
        let current = serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "ephemeralContainers": [{"name": "dbg", "image": "busybox"}],
            }
        });
        let with =
            |ecs: serde_json::Value| serde_json::json!({"spec": {"ephemeralContainers": ecs}});

        let grown = merge_ephemeral_containers(
            &current,
            &with(serde_json::json!([{"name": "dbg", "image": "busybox"}, {"name": "dbg2", "image": "busybox"}])),
        )
        .unwrap();
        assert_eq!(
            grown["spec"]["ephemeralContainers"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(grown["spec"]["containers"][0]["name"], "app");

        assert!(merge_ephemeral_containers(&current, &with(serde_json::json!([]))).is_err());
        assert!(
            merge_ephemeral_containers(
                &current,
                &with(serde_json::json!([{"name": "dbg", "image": "alpine"}]))
            )
            .is_err()
        );
        assert!(
            merge_ephemeral_containers(
                &current,
                &with(serde_json::json!([{"name": "dbg", "image": "busybox"}, {"name": "app", "image": "x"}]))
            )
            .is_err()
        );
    }

    #[test]
    fn sysctl_names_follow_upstream_shape() {
        use super::valid_sysctl_name;
        assert!(valid_sysctl_name("kernel.shm_rmid_forced"));
        assert!(valid_sysctl_name("net.ipv4.ip_local_port_range"));
        assert!(valid_sysctl_name("net/ipv4/tcp_syncookies"));
        assert!(!valid_sysctl_name("foo-"));
        assert!(!valid_sysctl_name("bar.."));
        assert!(!valid_sysctl_name(""));
        assert!(!valid_sysctl_name("Net.ipv4"));
    }

    #[test]
    fn updates_keep_uid_and_creation_timestamp() {
        use super::keep_system_fields;
        let old = serde_json::json!({"metadata": {"uid": "u1", "creationTimestamp": "2026-01-01T00:00:00Z",
                                                  "deletionTimestamp": "2026-01-02T00:00:00Z"}});
        let mut fresh = serde_json::json!({"metadata": {"name": "d"}, "spec": {"replicas": 2}});
        assert!(keep_system_fields(&old, &mut fresh).is_none());
        assert_eq!(fresh["metadata"]["uid"], "u1");
        assert_eq!(
            fresh["metadata"]["creationTimestamp"],
            "2026-01-01T00:00:00Z"
        );
        assert_eq!(
            fresh["metadata"]["deletionTimestamp"],
            "2026-01-02T00:00:00Z"
        );
        let mut other = serde_json::json!({"metadata": {"uid": "u2"}});
        assert!(keep_system_fields(&old, &mut other).is_some());
    }

    #[test]
    fn claims_and_volumes_start_pending() {
        use super::{RouteContext, set_initial_phase};
        use r8s_types::{GroupVersionResource, ResourceType};
        use std::sync::Arc;
        let ctx = |gvr: GroupVersionResource, kind: &str| RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr,
                kind: kind.into(),
                namespaced: true,
                singular: kind.to_lowercase(),
                short_names: vec![],
                subresources: vec![],
                schema: None,
                storage_gvr: None,
            }),
        };
        let mut pvc = serde_json::json!({"spec": {}});
        set_initial_phase(
            &ctx(
                GroupVersionResource::persistent_volume_claims(),
                "PersistentVolumeClaim",
            ),
            &mut pvc,
        );
        assert_eq!(pvc["status"]["phase"], "Pending");
        let mut bound = serde_json::json!({"status": {"phase": "Bound"}});
        set_initial_phase(
            &ctx(
                GroupVersionResource::persistent_volumes(),
                "PersistentVolume",
            ),
            &mut bound,
        );
        assert_eq!(bound["status"]["phase"], "Bound");
        let mut pod = serde_json::json!({"spec": {}});
        set_initial_phase(&ctx(GroupVersionResource::pods(), "Pod"), &mut pod);
        assert!(pod.get("status").is_none());
    }

    #[test]
    fn pod_qos_class_follows_requests_and_limits() {
        use super::{RouteContext, set_pod_qos_class};
        use r8s_types::{GroupVersionResource, ResourceType};
        use std::sync::Arc;
        let ctx = RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::pods(),
                kind: "Pod".into(),
                namespaced: true,
                singular: "pod".into(),
                short_names: vec![],
                subresources: vec![],
                schema: None,
                storage_gvr: None,
            }),
        };
        let class = |containers: serde_json::Value| {
            let mut pod = serde_json::json!({"spec": {"containers": containers}});
            set_pod_qos_class(&ctx, &mut pod);
            pod["status"]["qosClass"].as_str().unwrap().to_string()
        };
        assert_eq!(class(serde_json::json!([{"name": "a"}])), "BestEffort");
        assert_eq!(
            class(serde_json::json!([{"name": "a", "resources": {"requests": {"cpu": "100m"}}}])),
            "Burstable"
        );
        assert_eq!(
            class(serde_json::json!([{"name": "a", "resources": {
                "limits": {"cpu": "100m", "memory": "64Mi"},
                "requests": {"cpu": "100m", "memory": "64Mi"}}}])),
            "Guaranteed"
        );
        assert_eq!(
            class(serde_json::json!([
                {"name": "a", "resources": {"limits": {"cpu": "100m", "memory": "64Mi"}}},
                {"name": "b", "resources": {"limits": {"cpu": "1"}}}
            ])),
            "Burstable"
        );
    }

    #[test]
    fn custom_resource_field_selector_waits_for_webhook_conversion() {
        use super::{RouteContext, split_field_selector};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::{GroupVersionResource, ResourceType};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let crd_gvr =
            GroupVersionResource::new("apiextensions.k8s.io", "v1", "customresourcedefinitions");
        store
            .create(
                ResourceRef {
                    gvr: &crd_gvr,
                    namespace: None,
                    name: "foos.stable.example.com",
                },
                &serde_json::json!({
                    "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
                    "metadata": {"name": "foos.stable.example.com"},
                    "spec": {
                        "group": "stable.example.com",
                        "names": {"plural": "foos", "kind": "Foo"},
                        "scope": "Namespaced",
                        "versions": [{"name": "v1", "served": true, "storage": true},
                                     {"name": "v2", "served": true, "storage": false}],
                        "conversion": {"strategy": "Webhook", "webhook": {
                            "clientConfig": {"url": "https://127.0.0.1:1/convert"},
                            "conversionReviewVersions": ["v1"]}},
                    }
                }),
            )
            .unwrap();
        let ctx = |version: &str| RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::new("stable.example.com", version, "foos"),
                kind: "Foo".into(),
                namespaced: true,
                singular: "foo".into(),
                short_names: vec![],
                subresources: vec![],
                schema: None,
                storage_gvr: Some(GroupVersionResource::storage("stable.example.com", "foos")),
            }),
        };
        let sel = || Some(parse_field_selector("host=host1").unwrap());

        let (stored, deferred) = split_field_selector(&store, &ctx("v2"), sel());
        assert!(stored.is_none() && deferred.is_some());

        let (stored, deferred) = split_field_selector(&store, &ctx("v1"), sel());
        assert!(stored.is_some() && deferred.is_none());

        let (stored, deferred) = split_field_selector(&store, &ctx("v2"), None);
        assert!(stored.is_none() && deferred.is_none());
    }

    #[test]
    fn status_subresource_detection() {
        use super::{RouteContext, has_status_subresource};
        use r8s_types::{GroupVersionResource, ResourceType};
        use std::sync::Arc;
        let rt = |group: &str, resource: &str, subresources: &[&str], storage: bool| RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::new(group, "v1", resource),
                kind: "K".into(),
                namespaced: true,
                singular: String::new(),
                short_names: vec![],
                subresources: subresources.iter().map(|s| s.to_string()).collect(),
                schema: None,
                storage_gvr: storage.then(|| GroupVersionResource::storage(group, resource)),
            }),
        };
        assert!(has_status_subresource(&rt(
            "apps",
            "deployments",
            &["scale"],
            false
        )));
        assert!(has_status_subresource(&rt("", "pods", &[], false)));
        assert!(!has_status_subresource(&rt("", "configmaps", &[], false)));
        assert!(!has_status_subresource(&rt(
            "rbac.authorization.k8s.io",
            "roles",
            &[],
            false
        )));
        assert!(has_status_subresource(&rt(
            "example.com",
            "foos",
            &["status"],
            true
        )));
        assert!(!has_status_subresource(&rt(
            "example.com",
            "foos",
            &[],
            true
        )));
    }

    #[test]
    fn node_unschedulable_defaults_to_false() {
        let sel = parse_field_selector("spec.unschedulable=false").unwrap();
        let node = serde_json::json!({"metadata": {"name": "n1"}, "spec": {}});
        assert!(
            sel.matches(&node, "/v1/nodes"),
            "absent spec.unschedulable on a Node must match `=false`"
        );

        let cordoned =
            serde_json::json!({"metadata": {"name": "n2"}, "spec": {"unschedulable": true}});
        assert!(!sel.matches(&cordoned, "/v1/nodes"));
    }

    #[test]
    fn defaults_do_not_leak_across_types() {
        // The node default must not apply to an unrelated type.
        let sel = parse_field_selector("spec.unschedulable=false").unwrap();
        let other = serde_json::json!({"metadata": {"name": "x"}, "spec": {}});
        assert!(!sel.matches(&other, "/v1/configmaps"));
    }
}
