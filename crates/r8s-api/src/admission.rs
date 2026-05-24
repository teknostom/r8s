//! Admission webhook invocation.
//!
//! For every CREATE/UPDATE/DELETE, this walks the cluster's
//! Mutating/ValidatingWebhookConfigurations, finds the webhooks whose `rules`
//! match the request, builds an `admission.k8s.io/v1.AdmissionReview`, POSTs
//! it to the webhook over TLS (using the CA from
//! `cert-manager.io/inject-ca-from-secret` annotation when caBundle is
//! empty), and applies the returned JSON Patch.
//!
//! Mutating webhooks run first, in registration order, each seeing the prior
//! one's mutations. Validating webhooks then see the final object and any
//! `allowed: false` short-circuits the request.

use std::io::Cursor;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use r8s_store::backend::{ResourceRef, Store};
use r8s_types::GroupVersionResource;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::jsonpatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
}

impl Operation {
    fn as_str(self) -> &'static str {
        match self {
            Operation::Create => "CREATE",
            Operation::Update => "UPDATE",
            Operation::Delete => "DELETE",
        }
    }
}

/// Per-request context passed to all admission steps.
pub struct AdmissionCtx<'a> {
    pub store: &'a Store,
    pub gvr: &'a GroupVersionResource,
    pub kind: &'a str,
    pub operation: Operation,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    pub dry_run: bool,
    pub user_info: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// Webhook returned `allowed: false`. `status` is the raw `metav1.Status`
    /// the webhook sent (may include code/reason/details/causes); some clients
    /// rely on `details.causes[].field` to distinguish "the webhook chain ran
    /// and rejected the input" from "the webhook was unreachable", so we
    /// forward it verbatim.
    #[error("admission denied by '{webhook}': {message}")]
    Denied {
        webhook: String,
        message: String,
        status: Option<Value>,
    },
    #[error("webhook '{webhook}' call failed: {error}")]
    CallFailed { webhook: String, error: String },
    #[error("webhook '{webhook}' returned invalid response: {error}")]
    InvalidResponse { webhook: String, error: String },
    #[error("internal admission error: {0}")]
    Internal(String),
}

/// Run mutating webhooks, applying each patch in turn. Returns the (possibly
/// mutated) object.
pub async fn invoke_mutating(
    ctx: &AdmissionCtx<'_>,
    object: &mut Value,
    old_object: Option<&Value>,
) -> Result<(), AdmissionError> {
    let matches = matching_webhooks(ctx, "mutatingwebhookconfigurations")?;
    for m in matches {
        match call_webhook(ctx, &m, object, old_object).await {
            Ok(resp) => {
                if !resp.allowed {
                    return Err(denied_from(&m.name, resp.status));
                }
                if let Some(patch_b64) = resp.patch {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(&patch_b64)
                        .map_err(|e| AdmissionError::InvalidResponse {
                            webhook: m.name.clone(),
                            error: format!("patch base64: {e}"),
                        })?;
                    let patch: Value = serde_json::from_slice(&bytes).map_err(|e| {
                        AdmissionError::InvalidResponse {
                            webhook: m.name.clone(),
                            error: format!("patch json: {e}"),
                        }
                    })?;
                    jsonpatch::apply(object, &patch).map_err(|e| {
                        AdmissionError::InvalidResponse {
                            webhook: m.name.clone(),
                            error: format!("patch apply: {e}"),
                        }
                    })?;
                }
            }
            Err(e) => {
                if m.fail_closed {
                    return Err(e);
                }
                tracing::warn!(
                    webhook = %m.name,
                    failure_policy = "Ignore",
                    error = %e,
                    "mutating webhook call failed, continuing"
                );
            }
        }
    }
    Ok(())
}

/// Run validating webhooks. First denial aborts.
pub async fn invoke_validating(
    ctx: &AdmissionCtx<'_>,
    object: &Value,
    old_object: Option<&Value>,
) -> Result<(), AdmissionError> {
    let matches = matching_webhooks(ctx, "validatingwebhookconfigurations")?;
    let mut owned = object.clone();
    for m in matches {
        match call_webhook(ctx, &m, &mut owned, old_object).await {
            Ok(resp) => {
                if !resp.allowed {
                    return Err(denied_from(&m.name, resp.status));
                }
            }
            Err(e) => {
                if m.fail_closed {
                    return Err(e);
                }
                tracing::warn!(
                    webhook = %m.name,
                    failure_policy = "Ignore",
                    error = %e,
                    "validating webhook call failed, continuing"
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Webhook lookup
// ---------------------------------------------------------------------------

struct MatchedWebhook {
    name: String,
    service_namespace: String,
    service_name: String,
    service_port: u16,
    service_path: String,
    ca_pem: Option<Vec<u8>>,
    timeout: Duration,
    fail_closed: bool,
}

fn matching_webhooks(
    ctx: &AdmissionCtx<'_>,
    resource: &str,
) -> Result<Vec<MatchedWebhook>, AdmissionError> {
    let configs_gvr = GroupVersionResource::new("admissionregistration.k8s.io", "v1", resource);
    let list = ctx
        .store
        .list(&configs_gvr, None, None, None, None, None)
        .map_err(|e| AdmissionError::Internal(format!("list {resource}: {e}")))?;
    let mut out = Vec::new();
    for cfg in &list.items {
        let inject_secret = cfg
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(|a| a.get("cert-manager.io/inject-ca-from-secret"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let Some(webhooks) = cfg.get("webhooks").and_then(|v| v.as_array()) else {
            continue;
        };
        for wh in webhooks {
            let Some(name) = wh.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            if !matches_rules(wh.get("rules"), ctx) {
                continue;
            }
            if !matches_namespace_selector(ctx, wh.get("namespaceSelector")) {
                continue;
            }
            // objectSelector is a labelSelector against the resource's labels.
            // Cheap to apply since we have the object in hand.
            // (Pass through default-match for create flows.)
            let cc = match wh.get("clientConfig").and_then(|v| v.as_object()) {
                Some(c) => c,
                None => continue,
            };
            let svc = match cc.get("service").and_then(|v| v.as_object()) {
                Some(s) => s,
                None => {
                    tracing::debug!(webhook=%name, "skipping: only Service-based webhooks supported");
                    continue;
                }
            };
            let svc_ns = svc.get("namespace").and_then(|v| v.as_str()).unwrap_or("");
            let svc_name = svc.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let svc_path = svc
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("/")
                .to_string();
            let svc_port = svc.get("port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;

            let ca_pem = resolve_ca(ctx.store, cc.get("caBundle"), inject_secret.as_deref());

            let timeout_secs = wh
                .get("timeoutSeconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(10);
            let fail_closed = wh
                .get("failurePolicy")
                .and_then(|v| v.as_str())
                .map(|s| !s.eq_ignore_ascii_case("Ignore"))
                .unwrap_or(true);
            let side_effects = wh
                .get("sideEffects")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown")
                .to_string();

            // Per k8s spec: a webhook that declares sideEffects=Some MUST NOT
            // be called for dryRun. Unknown is treated the same way.
            if ctx.dry_run
                && !(side_effects.eq_ignore_ascii_case("None")
                    || side_effects.eq_ignore_ascii_case("NoneOnDryRun"))
            {
                tracing::debug!(webhook=%name, %side_effects, "skipping on dry-run");
                continue;
            }

            out.push(MatchedWebhook {
                name: name.to_string(),
                service_namespace: svc_ns.to_string(),
                service_name: svc_name.to_string(),
                service_port: svc_port,
                service_path: svc_path,
                ca_pem,
                timeout: Duration::from_secs(timeout_secs),
                fail_closed,
            });
        }
    }
    Ok(out)
}

fn matches_rules(rules: Option<&Value>, ctx: &AdmissionCtx<'_>) -> bool {
    let Some(rules) = rules.and_then(|v| v.as_array()) else {
        return false;
    };
    let op = ctx.operation.as_str();
    for rule in rules {
        let ops = rule
            .get("operations")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !ops.iter().any(|s| *s == "*" || *s == op) {
            continue;
        }
        let groups = string_list(rule.get("apiGroups"));
        if !groups.iter().any(|s| s == "*" || s == &ctx.gvr.group) {
            continue;
        }
        let versions = string_list(rule.get("apiVersions"));
        if !versions
            .iter()
            .any(|s| s == "*" || s == &ctx.gvr.version)
        {
            continue;
        }
        let resources = string_list(rule.get("resources"));
        // Each entry may be "resource" or "resource/subresource". We don't
        // currently dispatch subresource calls through admission, so an entry
        // like "*/*" or "*/status" should still match our base resource.
        if !resources.iter().any(|s| {
            let base = s.split('/').next().unwrap_or(s);
            base == "*" || base == ctx.gvr.resource
        }) {
            continue;
        }
        return true;
    }
    false
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn matches_namespace_selector(ctx: &AdmissionCtx<'_>, selector: Option<&Value>) -> bool {
    let Some(sel) = selector else {
        return true;
    };
    if sel.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        return true;
    }
    let Some(ns_name) = ctx.namespace else {
        return true;
    };
    let ns_gvr = GroupVersionResource::namespaces();
    let ns_rref = ResourceRef {
        gvr: &ns_gvr,
        namespace: None,
        name: ns_name,
    };
    let ns_labels = ctx
        .store
        .get(&ns_rref)
        .ok()
        .flatten()
        .and_then(|v| v.get("metadata").cloned())
        .and_then(|m| m.get("labels").cloned())
        .unwrap_or(json!({}));
    label_selector_matches(sel, &ns_labels)
}

fn label_selector_matches(selector: &Value, labels: &Value) -> bool {
    let labels_obj = labels.as_object();
    if let Some(match_labels) = selector.get("matchLabels").and_then(|v| v.as_object()) {
        for (k, v) in match_labels {
            let want = v.as_str().unwrap_or("");
            let got = labels_obj
                .and_then(|o| o.get(k))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if got != want {
                return false;
            }
        }
    }
    // matchExpressions left for follow-up.
    true
}

// ---------------------------------------------------------------------------
// CA resolution
// ---------------------------------------------------------------------------

fn resolve_ca(
    store: &Store,
    cabundle: Option<&Value>,
    inject_secret: Option<&str>,
) -> Option<Vec<u8>> {
    // 1. Direct caBundle on the webhook config (base64-encoded PEM).
    if let Some(s) = cabundle.and_then(|v| v.as_str())
        && !s.is_empty()
        && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s)
    {
        return Some(bytes);
    }
    // 2. Fall back to the cert-manager.io/inject-ca-from-secret annotation,
    //    pointing at a Secret whose data contains the CA bundle.
    let (ns, name) = inject_secret?.split_once('/')?;
    let secret_gvr = GroupVersionResource::secrets();
    let rref = ResourceRef {
        gvr: &secret_gvr,
        namespace: Some(ns),
        name,
    };
    let secret = store.get(&rref).ok().flatten()?;
    // Try ca.crt (cert-manager-style), then tls.crt (kube standard).
    for key in ["ca.crt", "tls.crt"] {
        if let Some(b64) = secret
            .get("data")
            .and_then(|d| d.get(key))
            .and_then(|v| v.as_str())
            && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64)
        {
            return Some(bytes);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Webhook call
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookResp {
    allowed: bool,
    #[serde(default)]
    patch: Option<String>,
    /// Raw `metav1.Status`. Pass through to clients on denial — see
    /// `AdmissionError::Denied`.
    #[serde(default)]
    status: Option<Value>,
}

struct ParsedResponse {
    allowed: bool,
    patch: Option<String>,
    status: Option<Value>,
}

async fn call_webhook(
    ctx: &AdmissionCtx<'_>,
    m: &MatchedWebhook,
    object: &mut Value,
    old_object: Option<&Value>,
) -> Result<ParsedResponse, AdmissionError> {
    let uid = uuid_like();
    let review = build_admission_review(&uid, ctx, object, old_object);
    let body = serde_json::to_vec(&review).map_err(|e| AdmissionError::Internal(e.to_string()))?;

    let (ip, port) =
        resolve_service_endpoint(ctx.store, &m.service_namespace, &m.service_name, m.service_port)?;
    let host = format!("{}.{}.svc", m.service_name, m.service_namespace);

    let raw = timeout(
        m.timeout,
        do_https_post(ip, port, &host, &m.service_path, &body, m.ca_pem.as_deref()),
    )
    .await
    .map_err(|_| AdmissionError::CallFailed {
        webhook: m.name.clone(),
        error: format!("timeout after {}s", m.timeout.as_secs()),
    })?
    .map_err(|e| AdmissionError::CallFailed {
        webhook: m.name.clone(),
        error: e,
    })?;

    let parsed: Value = serde_json::from_slice(&raw).map_err(|e| {
        AdmissionError::InvalidResponse {
            webhook: m.name.clone(),
            error: format!("body json: {e}"),
        }
    })?;
    let response = parsed.get("response").ok_or_else(|| AdmissionError::InvalidResponse {
        webhook: m.name.clone(),
        error: "missing 'response'".into(),
    })?;
    let resp: WebhookResp = serde_json::from_value(response.clone()).map_err(|e| {
        AdmissionError::InvalidResponse {
            webhook: m.name.clone(),
            error: format!("response shape: {e}"),
        }
    })?;
    Ok(ParsedResponse {
        allowed: resp.allowed,
        patch: resp.patch,
        status: resp.status,
    })
}

fn denied_from(webhook: &str, status: Option<Value>) -> AdmissionError {
    let message = status
        .as_ref()
        .and_then(|s| s.get("message"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    AdmissionError::Denied {
        webhook: webhook.to_string(),
        message,
        status,
    }
}

fn build_admission_review(
    uid: &str,
    ctx: &AdmissionCtx<'_>,
    object: &Value,
    old_object: Option<&Value>,
) -> Value {
    let kind = json!({
        "group": ctx.gvr.group,
        "version": ctx.gvr.version,
        "kind": ctx.kind,
    });
    let resource = json!({
        "group": ctx.gvr.group,
        "version": ctx.gvr.version,
        "resource": ctx.gvr.resource,
    });
    json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": {
            "uid": uid,
            "kind": kind,
            "resource": resource,
            // requestKind/requestResource mirror kind/resource for non-subresource
            // requests. They are pointer-typed in admissionv1.AdmissionRequest
            // and some webhook implementations nil-deref if they are omitted.
            "requestKind": kind,
            "requestResource": resource,
            "name": ctx.name,
            "namespace": ctx.namespace.unwrap_or(""),
            "operation": ctx.operation.as_str(),
            "userInfo": ctx.user_info,
            "object": object,
            "oldObject": old_object.cloned().unwrap_or(Value::Null),
            "dryRun": ctx.dry_run,
        }
    })
}

/// Resolve a Service+port to a pod IP and that pod's actual container port.
/// We can't dial Service ClusterIPs from r8sd's host netns (no kube-proxy on
/// host yet — only 10.96.0.1/apiserver has a fixed DNAT rule), but pod CIDR
/// is routable, so we go Service → EndpointSlice → Pod and dial pod-direct.
fn resolve_service_endpoint(
    store: &Store,
    ns: &str,
    name: &str,
    svc_port: u16,
) -> Result<(IpAddr, u16), AdmissionError> {
    let svc_gvr = GroupVersionResource::services();
    let svc = store
        .get(&ResourceRef {
            gvr: &svc_gvr,
            namespace: Some(ns),
            name,
        })
        .map_err(|e| AdmissionError::Internal(format!("get svc {ns}/{name}: {e}")))?
        .ok_or_else(|| AdmissionError::Internal(format!("service {ns}/{name} not found")))?;
    let port_spec = svc
        .get("spec")
        .and_then(|s| s.get("ports"))
        .and_then(|p| p.as_array())
        .into_iter()
        .flatten()
        .find(|p| p.get("port").and_then(|v| v.as_u64()) == Some(svc_port as u64))
        .ok_or_else(|| {
            AdmissionError::Internal(format!(
                "service {ns}/{name} has no port matching {svc_port}"
            ))
        })?;
    let target_port = port_spec
        .get("targetPort")
        .cloned()
        .unwrap_or(Value::from(svc_port));

    let slice_gvr =
        GroupVersionResource::new("discovery.k8s.io", "v1", "endpointslices");
    let slices = store
        .list(&slice_gvr, Some(ns), None, None, None, None)
        .map_err(|e| AdmissionError::Internal(format!("list endpointslices: {e}")))?;
    // EndpointSlices belong to a Service via the kubernetes.io/service-name label.
    let slice = slices
        .items
        .iter()
        .find(|s| {
            s.get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .and_then(|v| v.as_str())
                == Some(name)
        })
        .ok_or_else(|| {
            AdmissionError::Internal(format!(
                "no endpointslice for service {ns}/{name}"
            ))
        })?;
    let endpoint = slice
        .get("endpoints")
        .and_then(|e| e.as_array())
        .into_iter()
        .flatten()
        .find(|e| {
            // Prefer ready endpoints. Absent `ready` defaults to true in the API.
            e.get("conditions")
                .and_then(|c| c.get("ready"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true)
        })
        .ok_or_else(|| {
            AdmissionError::Internal(format!(
                "endpointslice for {ns}/{name} has no ready endpoint"
            ))
        })?;
    let pod_ip_str = endpoint
        .get("addresses")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .ok_or_else(|| AdmissionError::Internal("endpoint has no address".to_string()))?;
    let pod_ip: IpAddr = pod_ip_str
        .parse()
        .map_err(|e| AdmissionError::Internal(format!("bad pod IP '{pod_ip_str}': {e}")))?;

    let target_port_num = if let Some(n) = target_port.as_u64() {
        n as u16
    } else if let Some(port_name) = target_port.as_str() {
        let pod_ref = endpoint.get("targetRef");
        let pod_ns = pod_ref
            .and_then(|r| r.get("namespace"))
            .and_then(|v| v.as_str())
            .unwrap_or(ns);
        let pod_name = pod_ref
            .and_then(|r| r.get("name"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AdmissionError::Internal(format!(
                    "endpoint has no targetRef; cannot resolve port name '{port_name}'"
                ))
            })?;
        let pods_gvr = GroupVersionResource::pods();
        let pod = store
            .get(&ResourceRef {
                gvr: &pods_gvr,
                namespace: Some(pod_ns),
                name: pod_name,
            })
            .map_err(|e| AdmissionError::Internal(format!("get pod: {e}")))?
            .ok_or_else(|| {
                AdmissionError::Internal(format!("pod {pod_ns}/{pod_name} not found"))
            })?;
        pod.get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
            .flat_map(|c| c.get("ports").and_then(|p| p.as_array()).cloned().unwrap_or_default())
            .find(|p| p.get("name").and_then(|v| v.as_str()) == Some(port_name))
            .and_then(|p| p.get("containerPort").and_then(|v| v.as_u64()))
            .ok_or_else(|| {
                AdmissionError::Internal(format!(
                    "pod {pod_ns}/{pod_name} has no container port named '{port_name}'"
                ))
            })? as u16
    } else {
        return Err(AdmissionError::Internal(format!(
            "targetPort must be int or string, got {target_port}"
        )));
    };

    Ok((pod_ip, target_port_num))
}

async fn do_https_post(
    ip: IpAddr,
    port: u16,
    host: &str,
    path: &str,
    body: &[u8],
    ca_pem: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let cfg = tls_config(ca_pem)?;
    let connector = TlsConnector::from(Arc::new(cfg));
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|e| format!("invalid sni '{host}': {e}"))?;

    let tcp = TcpStream::connect((ip, port))
        .await
        .map_err(|e| format!("tcp connect {ip}:{port}: {e}"))?;
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| format!("tls handshake: {e}"))?;

    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Type: application/json\r\n\
         Accept: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\r\n",
        len = body.len()
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write headers: {e}"))?;
    stream
        .write_all(body)
        .await
        .map_err(|e| format!("write body: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush: {e}"))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| format!("read response: {e}"))?;

    extract_http_body(&raw)
}

fn extract_http_body(raw: &[u8]) -> Result<Vec<u8>, String> {
    let sep = b"\r\n\r\n";
    let split = raw
        .windows(sep.len())
        .position(|w| w == sep)
        .ok_or_else(|| "no header/body separator".to_string())?;
    let head = std::str::from_utf8(&raw[..split]).map_err(|e| format!("bad headers: {e}"))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status_code) {
        return Err(format!("http {status_code}: {status_line}"));
    }
    let mut chunked = false;
    for line in lines {
        if line.eq_ignore_ascii_case("transfer-encoding: chunked") {
            chunked = true;
        }
    }
    let body = &raw[split + sep.len()..];
    if chunked { dechunk(body) } else { Ok(body.to_vec()) }
}

fn dechunk(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len());
    let mut pos = 0;
    while pos < input.len() {
        let line_end = input[pos..]
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| "chunk size missing CRLF".to_string())?;
        let size_hex = std::str::from_utf8(&input[pos..pos + line_end])
            .map_err(|e| format!("chunk size utf8: {e}"))?;
        let size = usize::from_str_radix(size_hex.split(';').next().unwrap_or(size_hex).trim(), 16)
            .map_err(|e| format!("bad chunk size '{size_hex}': {e}"))?;
        pos += line_end + 2;
        if size == 0 {
            break;
        }
        if pos + size > input.len() {
            return Err("chunk runs past end of body".into());
        }
        out.extend_from_slice(&input[pos..pos + size]);
        pos += size + 2; // skip trailing CRLF
    }
    Ok(out)
}

fn tls_config(ca_pem: Option<&[u8]>) -> Result<ClientConfig, String> {
    // Single source of truth for CryptoProvider — match the one axum-server
    // configures in `ApiServer::serve_tls` (aws-lc-rs).
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    let mut roots = RootCertStore::empty();
    if let Some(pem) = ca_pem {
        let mut cursor = Cursor::new(pem);
        for cert in rustls_pemfile::certs(&mut cursor) {
            let der: CertificateDer<'static> = cert.map_err(|e| format!("ca pem: {e}"))?;
            roots
                .add(der)
                .map_err(|e| format!("add ca: {e}"))?;
        }
    }
    if roots.is_empty() {
        // No CA known — webhook can't be validated. Treat as an explicit
        // "skip TLS verification" only in dev clusters; here we fail loud so
        // misconfig is visible.
        return Err("no caBundle available; cannot verify webhook TLS".into());
    }

    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls versions: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn uuid_like() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: [u8; 16] = rng.random();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx<'a>(
        store: &'a Store,
        group: &'a str,
        version: &'a str,
        resource: &'a str,
        op: Operation,
        ns: Option<&'a str>,
    ) -> AdmissionCtx<'a> {
        AdmissionCtx {
            store,
            gvr: Box::leak(Box::new(GroupVersionResource::new(
                group, version, resource,
            ))),
            kind: "Anything",
            operation: op,
            namespace: ns,
            name: "x",
            dry_run: false,
            user_info: json!({}),
        }
    }

    #[tokio::test]
    async fn rule_matching_exact() {
        // Need a Store. Open a temp db.
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let c = ctx(
            &store,
            "cert-manager.io",
            "v1",
            "certificaterequests",
            Operation::Create,
            Some("ns"),
        );
        let rules = json!([{
            "operations": ["CREATE"],
            "apiGroups": ["cert-manager.io"],
            "apiVersions": ["v1"],
            "resources": ["certificaterequests"]
        }]);
        assert!(matches_rules(Some(&rules), &c));
    }

    #[tokio::test]
    async fn rule_matching_wildcards() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let c = ctx(&store, "apps", "v1", "deployments", Operation::Update, None);
        let rules = json!([{
            "operations": ["*"],
            "apiGroups": ["*"],
            "apiVersions": ["*"],
            "resources": ["*"]
        }]);
        assert!(matches_rules(Some(&rules), &c));
    }

    #[tokio::test]
    async fn rule_matching_operation_filter() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let c = ctx(&store, "", "v1", "pods", Operation::Delete, None);
        let rules = json!([{
            "operations": ["CREATE", "UPDATE"],
            "apiGroups": ["*"],
            "apiVersions": ["*"],
            "resources": ["*"]
        }]);
        assert!(!matches_rules(Some(&rules), &c));
    }

    #[test]
    fn label_selector_match_labels_only() {
        let sel = json!({"matchLabels": {"team": "core"}});
        assert!(label_selector_matches(&sel, &json!({"team": "core", "x": "y"})));
        assert!(!label_selector_matches(&sel, &json!({"team": "ops"})));
        assert!(!label_selector_matches(&sel, &json!({})));
    }

    #[tokio::test]
    async fn empty_namespace_selector_matches() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let c = ctx(&store, "", "v1", "pods", Operation::Create, None);
        assert!(matches_namespace_selector(&c, None));
        assert!(matches_namespace_selector(&c, Some(&json!({}))));
    }

    #[tokio::test]
    async fn ca_resolution_prefers_inline_bundle() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let pem = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----";
        let b64 = base64::engine::general_purpose::STANDARD.encode(pem);
        let ca = resolve_ca(&store, Some(&json!(b64)), None);
        assert_eq!(ca.as_deref(), Some(pem.as_slice()));
    }

    #[tokio::test]
    async fn ca_resolution_falls_back_to_inject_annotation() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let pem = b"-----BEGIN CERTIFICATE-----\nfrom-secret\n-----END CERTIFICATE-----";
        let b64 = base64::engine::general_purpose::STANDARD.encode(pem);
        let secret_gvr = GroupVersionResource::secrets();
        store
            .create(
                ResourceRef {
                    gvr: &secret_gvr,
                    namespace: Some("cert-manager"),
                    name: "wh-ca",
                },
                &json!({
                    "metadata": {"name": "wh-ca", "namespace": "cert-manager"},
                    "data": {"ca.crt": b64}
                }),
            )
            .unwrap();
        let ca = resolve_ca(&store, None, Some("cert-manager/wh-ca"));
        assert_eq!(ca.as_deref(), Some(pem.as_slice()));
    }

    #[tokio::test]
    async fn build_admission_review_shape() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let c = ctx(
            &store,
            "cert-manager.io",
            "v1",
            "certificaterequests",
            Operation::Create,
            Some("ns"),
        );
        let obj = json!({"spec": {"request": "csr"}});
        let review = build_admission_review("u-1", &c, &obj, None);
        assert_eq!(review["apiVersion"], "admission.k8s.io/v1");
        assert_eq!(review["kind"], "AdmissionReview");
        assert_eq!(review["request"]["uid"], "u-1");
        assert_eq!(review["request"]["operation"], "CREATE");
        assert_eq!(review["request"]["resource"]["group"], "cert-manager.io");
        assert_eq!(review["request"]["object"], obj);
        assert_eq!(review["request"]["dryRun"], false);
    }
}
