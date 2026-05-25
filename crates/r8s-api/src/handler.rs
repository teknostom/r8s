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
fn admission_error_response(err: AdmissionError) -> Response {
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
            let wrapped =
                format!("admission webhook \"{webhook}\" denied the request: {message}");
            let code = status
                .as_ref()
                .and_then(|s| s.get("code"))
                .and_then(|v| v.as_u64())
                .map(|c| c as u16)
                .unwrap_or(StatusCode::FORBIDDEN.as_u16());
            match status {
                Some(serde_json::Value::Object(mut obj)) => {
                    obj.insert("kind".into(), serde_json::json!("Status"));
                    obj.insert("apiVersion".into(), serde_json::json!("v1"));
                    obj.insert("status".into(), serde_json::json!("Failure"));
                    obj.entry("metadata").or_insert_with(|| serde_json::json!({}));
                    obj.entry("reason").or_insert_with(|| serde_json::json!("Forbidden"));
                    obj.insert("message".into(), serde_json::json!(wrapped));
                    obj.entry("code").or_insert_with(|| serde_json::json!(code));
                    response::json_response(code, &serde_json::Value::Object(obj))
                }
                _ => response::status_error(
                    StatusCode::from_u16(code).unwrap_or(StatusCode::FORBIDDEN),
                    "Forbidden",
                    &wrapped,
                ),
            }
        }
        AdmissionError::CallFailed { webhook, error } => response::status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("admission webhook '{webhook}' failed: {error}"),
        ),
        AdmissionError::InvalidResponse { webhook, error } => response::status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("admission webhook '{webhook}' returned invalid response: {error}"),
        ),
        AdmissionError::Internal(msg) => response::status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &msg,
        ),
    }
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
    let mut entries: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut current_ts: Option<&str> = None;

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
            if timestamps {
                if let Some(t) = current_ts {
                    out.push_str(t);
                    out.push(' ');
                }
            }
            out.push_str(&current);
            entries.push(out);
            current.clear();
            current_ts = None;
        }
    }
    // Trailing partial fragment (container hasn't flushed a newline yet).
    if !current.is_empty() {
        let mut out = String::new();
        if timestamps {
            if let Some(t) = current_ts {
                out.push_str(t);
                out.push(' ');
            }
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
    output
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
        // We have hand-rolled protobuf-to-JSON converters for a small set of
        // resource types (currently just CustomResourceDefinition — see
        // `r8s_types::k8s_pb_serde::decode_k8s_protobuf_to_json`). For
        // unsupported GVKs return 415 so the client falls back to JSON; a
        // partial decode would silently drop `spec`/`status` and we'd rather
        // be loud than wrong.
        return r8s_types::k8s_pb_serde::decode_k8s_protobuf_to_json(body).ok_or_else(|| {
            status_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UnsupportedMediaType",
                "r8s does not yet accept application/vnd.kubernetes.protobuf request bodies \
                 for this resource type; retry with application/json",
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

/// Default ServicePort fields the apiserver fills in but clients commonly
/// omit: `protocol` → `TCP`, and `targetPort` → the `port` value. The protocol
/// default in particular matters because controllers match a Service's ports
/// to its EndpointSlice ports *by protocol* (ingress-nginx does
/// `*epPort.Protocol == servicePort.Protocol`); an absent protocol reads as ""
/// on the Service but `TCP` on the slice, so no endpoint matches and the
/// controller serves 503.
fn default_service_ports(ctx: &RouteContext, body: &mut serde_json::Value) {
    if ctx.resource_type.gvr.resource != "services" {
        return;
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
                if !obj.contains_key("targetPort")
                    && let Some(p) = obj.get("port").cloned()
                {
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
    if r8s_types::openapi::spec_bytes_for(&ctx.resource_type.gvr.group, &ctx.resource_type.gvr.version)
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
fn validate_cr(ctx: &RouteContext, body: &serde_json::Value) -> Option<Response> {
    if r8s_types::openapi::spec_bytes_for(&ctx.resource_type.gvr.group, &ctx.resource_type.gvr.version)
        .is_some()
    {
        return None;
    }
    let Some(schema) = ctx.resource_type.schema.as_ref() else {
        return None;
    };
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

    maybe_allocate_cluster_ip(state, ctx, &mut body);
    default_service_ports(ctx, &mut body);
    default_cr(ctx, &mut body);
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
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut body, None).await {
        return admission_error_response(e);
    }
    if let Err(e) = admission::invoke_validating(&admission_ctx, &body, None).await {
        return admission_error_response(e);
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
            &ctx.resource_type.gvr,
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
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    create_impl(&state, &ctx, Some(&ns), body, dry_run).await
}

pub async fn create_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    create_impl(&state, &ctx, None, body, dry_run).await
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
        gvr: &ctx.resource_type.gvr,
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
    let old_object = match state.store.get(&resource_ref) {
        Ok(v) => v,
        Err(err) => return response::anyhow_error_response(err),
    };
    let admission_ctx = AdmissionCtx {
        store: &state.store,
        gvr: &ctx.resource_type.gvr,
        kind: &ctx.resource_type.kind,
        operation: Operation::Update,
        namespace,
        name,
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) =
        admission::invoke_mutating(&admission_ctx, &mut body, old_object.as_ref()).await
    {
        return admission_error_response(e);
    }
    if let Err(e) =
        admission::invoke_validating(&admission_ctx, &body, old_object.as_ref()).await
    {
        return admission_error_response(e);
    }
    if let Some(resp) = validate_cr(ctx, &body) {
        return resp;
    }
    if dry_run {
        return response::object_response(&body);
    }
    match state.store.update(&resource_ref, &body) {
        Ok(obj) => response::object_response(&obj),
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
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    update_impl(&state, &ctx, Some(&ns), &name, body, dry_run).await
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
        .map(FieldSelector::parse)
        .transpose()
    {
        Ok(s) => s,
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
    let list = match state.store.list(
        &ctx.resource_type.gvr,
        namespace,
        label_sel.as_ref(),
        field_sel.as_ref(),
        None,
        None,
    ) {
        Ok(r) => r,
        Err(err) => return response::anyhow_error_response(err),
    };
    for item in list.items {
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
        let effective_ns = namespace.or(ns.as_deref());
        let _ = delete_impl(state, ctx, effective_ns, &name, policy, false).await;
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
    let body = match require_json(&headers, &body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let dry_run = parse_dry_run(raw_query.as_deref());
    update_impl(&state, &ctx, None, &name, body, dry_run).await
}

// ─── /status subresource ────────────────────────────────────────────────────
//
// Upstream lets clients GET/PUT/PATCH `{resource}/{name}/status` separately
// from the main object. PUT replaces only the `status` field of the stored
// object (everything else in the body is ignored); PATCH applies the patch
// directly. GET returns the whole object (matching upstream — `/status` is a
// view, not a separate document). We don't enforce the spec/status split with
// validation or RBAC; just route the subresource so clients can use it.

pub(crate) fn status_put_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    body: serde_json::Value,
) -> Response {
    let rref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
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
    let new_status = body.get("status").cloned().unwrap_or(serde_json::Value::Null);
    if let Some(obj) = current.as_object_mut() {
        if matches!(new_status, serde_json::Value::Null) {
            obj.remove("status");
        } else {
            obj.insert("status".to_string(), new_status);
        }
    }
    match state.store.update(&rref, &current) {
        Ok(obj) => response::object_response(&obj),
        Err(err) => response::anyhow_error_response(err),
    }
}

pub async fn get_status_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, Some(&ns), &name, &headers)
}

pub async fn get_status_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    get_impl(&state, &ctx, None, &name, &headers)
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
    status_put_impl(&state, &ctx, Some(&ns), &name, body)
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
    status_put_impl(&state, &ctx, None, &name, body)
}

pub async fn patch_status_ns(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path((ns, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Status subresource updates don't go through the admission chain in
    // upstream k8s; we'd skip mutating webhooks here too. dryRun is unused.
    patch_impl(&state, &ctx, Some(&ns), &name, &headers, body, false).await
}

pub async fn patch_status_cluster(
    State(state): State<AppState>,
    Extension(ctx): Extension<RouteContext>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    patch_impl(&state, &ctx, None, &name, &headers, body, false).await
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
    if !body.is_empty() && !content_type.contains("protobuf")
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
            .list(&rt.gvr, None, None, None, None, None)
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
                    meta.insert("ownerReferences".to_string(), serde_json::Value::Array(new_refs));
                }
                // Blind write — the store would otherwise reject if a
                // controller raced our list-snapshot with another update.
                // Orphaning must not silently no-op or the GC follow-up
                // will see the still-attached ref and delete the dependent.
                meta.remove("resourceVersion");
            }
            let rref = ResourceRef {
                gvr: &rt.gvr,
                namespace: ns.as_deref(),
                name: &name,
            };
            let _ = state.store.update(&rref, &item);
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
        gvr: &ctx.resource_type.gvr,
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
        dry_run,
        user_info: serde_json::json!({}),
    };
    if let Err(e) =
        admission::invoke_validating(&admission_ctx, &existing, Some(&existing)).await
    {
        return admission_error_response(e);
    }

    if dry_run {
        return response::object_response(&existing);
    }

    if matches!(policy, PropagationPolicy::Orphan) {
        let uid = existing
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        if !uid.is_empty() {
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
            Ok(Some(mut current)) => {
                if mark_foreground_deletion(&mut current) {
                    match state.store.update(&rref, &current) {
                        Ok(obj) => return response::object_response(&obj),
                        Err(err) => return response::anyhow_error_response(err),
                    }
                }
                // Nothing to mark (already marked) — fall through to actual delete.
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
            .any(|v| v.as_str() == Some("foregroundDeletion"));
        if !has {
            arr.push(serde_json::Value::String("foregroundDeletion".into()));
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

pub(crate) async fn patch_impl(
    state: &AppState,
    ctx: &RouteContext,
    namespace: Option<&str>,
    name: &str,
    headers: &HeaderMap,
    bytes: Bytes,
    dry_run: bool,
) -> Response {
    let rref = ResourceRef {
        gvr: &ctx.resource_type.gvr,
        namespace,
        name,
    };
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // RFC 6902 JSON Patch sends an array of ops; RFC 7396 JSON Merge Patch
    // (and strategic-merge-patch, which we don't yet differentiate) sends an
    // object. kubectl uses json-patch+json for CRD version renames and other
    // structural edits — treating that body as a merge patch would replace
    // the whole resource with the array.
    let is_json_patch = content_type.contains("json-patch+json");
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
        Ok(Some(mut current)) => {
            let old_object = current.clone();
            if is_json_patch {
                if let Err(e) = crate::jsonpatch::apply(&mut current, &patch) {
                    return response::status_error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "Invalid",
                        &format!("json patch failed: {e}"),
                    );
                }
            } else {
                json_merge_patch(&mut current, &patch);
            }
            if !patch_carries_rv && let Some(meta) =
                current.get_mut("metadata").and_then(|v| v.as_object_mut())
            {
                meta.remove("resourceVersion");
            }
            let admission_ctx = AdmissionCtx {
                store: &state.store,
                gvr: &ctx.resource_type.gvr,
                kind: &ctx.resource_type.kind,
                operation: Operation::Update,
                namespace,
                name,
                dry_run,
                user_info: serde_json::json!({}),
            };
            if let Err(e) =
                admission::invoke_mutating(&admission_ctx, &mut current, Some(&old_object))
                    .await
            {
                return admission_error_response(e);
            }
            if let Err(e) =
                admission::invoke_validating(&admission_ctx, &current, Some(&old_object)).await
            {
                return admission_error_response(e);
            }
            if let Some(resp) = validate_cr(ctx, &current) {
                return resp;
            }
            if dry_run {
                return response::object_response(&current);
            }
            match state.store.update(&rref, &current) {
                Ok(obj) => response::object_response(&obj),
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
    default_service_ports(ctx, &mut body);
            default_cr(ctx, &mut body);
            let admission_ctx = AdmissionCtx {
                store: &state.store,
                gvr: &ctx.resource_type.gvr,
                kind: &ctx.resource_type.kind,
                operation: Operation::Create,
                namespace,
                name,
                dry_run,
                user_info: serde_json::json!({}),
            };
            if let Err(e) = admission::invoke_mutating(&admission_ctx, &mut body, None).await {
                return admission_error_response(e);
            }
            if let Err(e) = admission::invoke_validating(&admission_ctx, &body, None).await {
                return admission_error_response(e);
            }
            if let Some(resp) = validate_cr(ctx, &body) {
                return resp;
            }
            if dry_run {
                return response::created_response(&body);
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
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let dry_run = parse_dry_run(raw_query.as_deref());
    patch_impl(&state, &ctx, Some(&ns), &name, &headers, body, dry_run).await
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
    patch_impl(&state, &ctx, None, &name, &headers, body, dry_run).await
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
            params.allow_watch_bookmarks(),
            params.label_selector.as_deref(),
            params.field_selector.as_deref(),
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
            } else if wants_partial_metadata(headers) {
                let items: Vec<_> = result.items.iter().map(to_partial_object_metadata).collect();
                response::list_response(
                    "meta.k8s.io/v1",
                    "PartialObjectMetadata",
                    result.resource_version,
                    result.continue_token.as_deref(),
                    items,
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

#[allow(clippy::too_many_arguments)]
fn watch_impl(
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
        .map(FieldSelector::parse)
        .transpose()
    {
        Ok(s) => s.map(std::sync::Arc::new),
        Err(e) => {
            return response::status_error(StatusCode::BAD_REQUEST, "Invalid", &e.to_string());
        }
    };
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
        let rx = state.store.watch(&ctx.resource_type.gvr);
        let (items, rv) = state
            .store
            .list(
                &ctx.resource_type.gvr,
                namespace,
                label_sel.as_deref(),
                field_sel.as_deref(),
                None,
                None,
            )
            .map(|r| (r.items, r.resource_version))
            .unwrap_or_default();
        (items, rv, Vec::new(), rx)
    } else {
        match state.store.watch_from(&ctx.resource_type.gvr, since_rv) {
            Ok((history, rx)) => (Vec::new(), since_rv, history, rx),
            Err(too_old) => {
                return response::status_error(
                    StatusCode::GONE,
                    "Expired",
                    &format!(
                        "too old resource version: {} ({})",
                        too_old.requested, too_old.oldest_available
                    ),
                );
            }
        }
    };

    let as_table = wants_table(headers);
    let as_partial = wants_partial_metadata(headers);
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
    let history_field = field_sel.clone();
    let history_gvr = gvr_key.clone();
    let history_iter = history.into_iter().filter_map(move |event| {
        if !event_matches(
            &event.object,
            history_ns_filter.as_deref(),
            history_label.as_deref(),
            history_field.as_deref(),
            &history_gvr,
        ) {
            return None;
        }
        let type_str = match event.event_type {
            WatchEventType::Added => "ADDED",
            WatchEventType::Modified => "MODIFIED",
            WatchEventType::Deleted => "DELETED",
        };
        let payload = format_history(&event.object);
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
        tokio_stream::iter(Some(Ok::<_, std::io::Error>(
            response::watch_event_line("BOOKMARK", &bookmark_formatted),
        )))
    } else {
        tokio_stream::iter(None)
    };

    let ns_filter: Option<String> = namespace.map(|s| s.to_string());
    let live_label = label_sel.clone();
    let live_field = field_sel.clone();
    let live_gvr = gvr_key.clone();
    // On broadcast lag, terminate so the client reconnects (standard K8s behavior).
    let live_stream = BroadcastStream::new(rx)
        .take_while(|result| result.is_ok())
        .filter_map(move |result| {
            let event = result.ok()?;
            if !event_matches(
                &event.object,
                ns_filter.as_deref(),
                live_label.as_deref(),
                live_field.as_deref(),
                &live_gvr,
            ) {
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
        .join(format!("{container_id}.log"));

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

    let output = render_cri_log(&raw, params.timestamps, params.tail_lines);

    Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(Body::from(output))
        .expect("valid response")
}
