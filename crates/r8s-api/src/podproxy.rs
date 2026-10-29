//! `pods/{name}/proxy` and `services/{name}/proxy`: forward a request to a
//! pod's IP, or to one of a Service's endpoints. `kubectl proxy` style
//! access and the e2e framework's response checks go through here.
//!
//! The name segment is `[scheme:]name[:port]`. A pod without a port takes its
//! first declared containerPort, else 80; a service its first port, and a
//! port may be a port name. An `https` scheme talks TLS to the backend
//! without verifying its certificate, as the apiserver does.
//!
//! As upstream's proxy transport, `GET`/`HEAD` of the bare `/proxy` redirect
//! to `/proxy/`, and absolute links in an HTML body or a `Location` header
//! are prefixed with the proxy path.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, HeaderName, Method, Uri},
    response::Response,
};
use http_body_util::{BodyExt, Full, Limited};
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use r8s_store::backend::ResourceRef;
use r8s_types::GroupVersionResource;

use crate::discovery::AppState;
use crate::response::status_error;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// Hop-by-hop headers are not forwarded in either direction (RFC 9110 §7.6.1).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// HTML attributes holding URLs, rewritten in proxied pages.
const URL_ATTRIBUTES: &[&str] = &[
    "href",
    "src",
    "action",
    "formaction",
    "cite",
    "codebase",
    "background",
    "longdesc",
    "usemap",
    "icon",
    "manifest",
    "poster",
    "data",
    "profile",
];

/// `pods/{name}/proxy` and `pods/{name}/proxy/`.
pub async fn pod_proxy_root(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(redirect) = root_redirect(&uri, &method) {
        return redirect;
    }
    let target = match pod_target(&state, &namespace, &name) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let prefix = format!("/api/v1/namespaces/{namespace}/pods/{name}/proxy");
    forward_to(target, &prefix, "/", uri.query(), method, &headers, body).await
}

/// `pods/{name}/proxy/{path}`.
pub async fn pod_proxy_path(
    State(state): State<AppState>,
    Path((namespace, name, rest)): Path<(String, String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let target = match pod_target(&state, &namespace, &name) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let prefix = format!("/api/v1/namespaces/{namespace}/pods/{name}/proxy");
    let path = format!("/{rest}");
    forward_to(target, &prefix, &path, uri.query(), method, &headers, body).await
}

/// `services/{name}/proxy` and `services/{name}/proxy/`.
pub async fn service_proxy_root(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(redirect) = root_redirect(&uri, &method) {
        return redirect;
    }
    let target = match service_target(&state, &namespace, &name) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let prefix = format!("/api/v1/namespaces/{namespace}/services/{name}/proxy");
    forward_to(target, &prefix, "/", uri.query(), method, &headers, body).await
}

/// `services/{name}/proxy/{path}`.
pub async fn service_proxy_path(
    State(state): State<AppState>,
    Path((namespace, name, rest)): Path<(String, String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let target = match service_target(&state, &namespace, &name) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let prefix = format!("/api/v1/namespaces/{namespace}/services/{name}/proxy");
    let path = format!("/{rest}");
    forward_to(target, &prefix, &path, uri.query(), method, &headers, body).await
}

/// Where a proxied request goes.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    addr: SocketAddr,
    tls: bool,
}

/// `GET`/`HEAD` of the bare `/proxy` path: a redirect to `/proxy/`, query
/// kept.
fn root_redirect(uri: &Uri, method: &Method) -> Option<Response> {
    if uri.path().ends_with('/') || !(method == Method::GET || method == Method::HEAD) {
        return None;
    }
    let location = match uri.query() {
        Some(q) if !q.is_empty() => format!("{}/?{q}", uri.path()),
        _ => format!("{}/", uri.path()),
    };
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("location", location)
        .body(axum::body::Body::empty())
        .ok()
}

#[allow(clippy::result_large_err)]
fn pod_target(state: &AppState, namespace: &str, name_spec: &str) -> Result<Target, Response> {
    let (scheme, pod_name, port) = split_name_spec(name_spec);
    let tls = scheme_is_tls(scheme)?;
    let pods_gvr = GroupVersionResource::pods();
    let pod = match state.store.get(&ResourceRef {
        gvr: &pods_gvr,
        namespace: Some(namespace),
        name: pod_name,
    }) {
        Ok(Some(pod)) => pod,
        Ok(None) => {
            return Err(status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("pods \"{pod_name}\" not found"),
            ));
        }
        Err(e) => return Err(crate::response::anyhow_error_response(e)),
    };
    let Some(ip) = pod
        .get("status")
        .and_then(|s| s.get("podIP"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<IpAddr>().ok())
    else {
        return Err(status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            &format!("address not allocated for pod {namespace}/{pod_name}"),
        ));
    };
    let port = match port {
        Some(p) => p.parse::<u16>().map_err(|_| {
            status_error(
                StatusCode::BAD_REQUEST,
                "BadRequest",
                &format!("invalid port \"{p}\" in pod proxy name"),
            )
        })?,
        None => first_container_port(&pod).unwrap_or(80),
    };
    Ok(Target {
        addr: SocketAddr::new(ip, port),
        tls,
    })
}

/// A Service's endpoint for one of its ports, by name or number (the first
/// port when unnamed), as the apiserver resolves `services/{name}/proxy`.
#[allow(clippy::result_large_err)]
fn service_target(state: &AppState, namespace: &str, name_spec: &str) -> Result<Target, Response> {
    let (scheme, svc_name, port_spec) = split_name_spec(name_spec);
    let tls = scheme_is_tls(scheme)?;
    let services_gvr = GroupVersionResource::services();
    let service = match state.store.get(&ResourceRef {
        gvr: &services_gvr,
        namespace: Some(namespace),
        name: svc_name,
    }) {
        Ok(Some(svc)) => svc,
        Ok(None) => {
            return Err(status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("services \"{svc_name}\" not found"),
            ));
        }
        Err(e) => return Err(crate::response::anyhow_error_response(e)),
    };
    let endpoints_gvr = GroupVersionResource::endpoints();
    let endpoints = state
        .store
        .get(&ResourceRef {
            gvr: &endpoints_gvr,
            namespace: Some(namespace),
            name: svc_name,
        })
        .ok()
        .flatten()
        .unwrap_or_default();
    match resolve_service_endpoint(&service, &endpoints, port_spec) {
        Ok(addr) => Ok(Target { addr, tls }),
        Err(ServiceResolution::NoSuchPort) => Err(status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!(
                "no service port \"{}\" found for service \"{svc_name}\"",
                port_spec.unwrap_or_default()
            ),
        )),
        Err(ServiceResolution::NoEndpoints) => Err(status_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            &format!("no endpoints available for service \"{svc_name}\""),
        )),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ServiceResolution {
    NoSuchPort,
    NoEndpoints,
}

/// The address of a ready endpoint carrying the Service port named or
/// numbered `port_spec` (the first port when absent): the endpoint subset
/// whose port shares the Service port's name.
fn resolve_service_endpoint(
    service: &serde_json::Value,
    endpoints: &serde_json::Value,
    port_spec: Option<&str>,
) -> Result<SocketAddr, ServiceResolution> {
    let ports = service
        .get("spec")
        .and_then(|s| s.get("ports"))
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();
    let svc_port = match port_spec {
        None => ports.first().cloned(),
        Some(spec) => ports
            .iter()
            .find(|p| {
                name_of(p) == spec
                    || p.get("port")
                        .and_then(|n| n.as_i64())
                        .is_some_and(|n| spec.parse::<i64>() == Ok(n))
            })
            .cloned(),
    }
    .ok_or(ServiceResolution::NoSuchPort)?;
    let port_name = name_of(&svc_port);
    for subset in endpoints
        .get("subsets")
        .and_then(|s| s.as_array())
        .into_iter()
        .flatten()
    {
        let Some(port) = subset
            .get("ports")
            .and_then(|p| p.as_array())
            .into_iter()
            .flatten()
            .find(|p| name_of(p) == port_name)
            .and_then(|p| p.get("port")?.as_u64())
            .and_then(|p| u16::try_from(p).ok())
        else {
            continue;
        };
        let address = subset
            .get("addresses")
            .and_then(|a| a.as_array())
            .into_iter()
            .flatten()
            .find_map(|a| a.get("ip")?.as_str()?.parse::<IpAddr>().ok());
        if let Some(ip) = address {
            return Ok(SocketAddr::new(ip, port));
        }
    }
    Err(ServiceResolution::NoEndpoints)
}

/// A port's `name`, empty when unnamed.
fn name_of(port: &serde_json::Value) -> &str {
    port.get("name").and_then(|n| n.as_str()).unwrap_or("")
}

#[allow(clippy::result_large_err)]
fn scheme_is_tls(scheme: &str) -> Result<bool, Response> {
    match scheme {
        "http" => Ok(false),
        "https" => Ok(true),
        other => Err(status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            &format!("unsupported scheme \"{other}\" in proxy name"),
        )),
    }
}

/// `[scheme:]name[:port]`; the scheme defaults to http.
fn split_name_spec(spec: &str) -> (&str, &str, Option<&str>) {
    let parts: Vec<&str> = spec.split(':').collect();
    match parts.as_slice() {
        [name] => ("http", name, None),
        [scheme, name] if *scheme == "http" || *scheme == "https" => (scheme, name, None),
        [name, port] => ("http", name, Some(port)),
        [scheme, name, port] => (scheme, name, Some(port)),
        _ => ("http", spec, None),
    }
}

fn first_container_port(pod: &serde_json::Value) -> Option<u16> {
    pod.get("spec")?
        .get("containers")?
        .as_array()?
        .iter()
        .filter_map(|c| c.get("ports")?.as_array()?.first())
        .filter_map(|p| p.get("containerPort")?.as_u64())
        .find_map(|p| u16::try_from(p).ok())
}

#[allow(clippy::too_many_arguments)]
async fn forward_to(
    target: Target,
    prefix: &str,
    path: &str,
    query: Option<&str>,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let path_and_query = match query {
        Some(q) if !q.is_empty() => format!("{path}?{q}"),
        _ => path.to_string(),
    };
    match forward(&target, prefix, &path_and_query, method, headers, body).await {
        Ok(resp) => resp,
        Err(e) => status_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            &format!("error trying to reach service: {e}"),
        ),
    }
}

async fn forward(
    target: &Target,
    prefix: &str,
    path_and_query: &str,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, String> {
    let addr = target.addr;
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| format!("dial tcp {addr}: i/o timeout"))?
        .map_err(|e| format!("dial tcp {addr}: {e}"))?;

    let mut req = hyper::Request::builder()
        .method(method)
        .uri(path_and_query)
        .header("host", addr.to_string());
    for (name, value) in headers {
        if name == "host" || name == "content-length" || is_hop_by_hop(name) {
            continue;
        }
        req = req.header(name, value);
    }
    let req = req
        .body(Full::new(body))
        .map_err(|e| format!("build request: {e}"))?;

    let resp = if target.tls {
        let connector = tokio_rustls::TlsConnector::from(insecure_tls_config());
        let server_name = rustls::pki_types::ServerName::IpAddress(addr.ip().into());
        let tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| format!("tls handshake with {addr}: {e}"))?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
            .await
            .map_err(|e| format!("handshake: {e}"))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        tokio::time::timeout(RESPONSE_TIMEOUT, sender.send_request(req))
            .await
            .map_err(|_| "upstream response timeout".to_string())?
            .map_err(|e| format!("send request: {e}"))?
    } else {
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| format!("handshake: {e}"))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        tokio::time::timeout(RESPONSE_TIMEOUT, sender.send_request(req))
            .await
            .map_err(|_| "upstream response timeout".to_string())?
            .map_err(|e| format!("send request: {e}"))?
    };
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let bytes = Limited::new(resp.into_body(), MAX_RESPONSE_BYTES)
        .collect()
        .await
        .map_err(|e| format!("read response: {e}"))?
        .to_bytes();

    let is_html = resp_headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").trim())
        == Some("text/html");
    let bytes = if is_html {
        match std::str::from_utf8(&bytes) {
            Ok(text) => Bytes::from(rewrite_html(text, prefix)),
            Err(_) => bytes,
        }
    } else {
        bytes
    };

    let mut out = Response::builder().status(status);
    for (name, value) in &resp_headers {
        if name == "content-length" || is_hop_by_hop(name) {
            continue;
        }
        if name == "location"
            && let Ok(location) = value.to_str()
        {
            out = out.header(name, rewrite_link(location, prefix));
            continue;
        }
        out = out.header(name, value);
    }
    out.body(axum::body::Body::from(bytes))
        .map_err(|e| format!("build response: {e}"))
}

/// A TLS client that accepts any backend certificate, as the apiserver's
/// proxy to pods and services does.
fn insecure_tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .expect("default TLS versions")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(crate::aggregation::NoVerify(provider)))
                .with_no_client_auth();
            Arc::new(config)
        })
        .clone()
}

/// An absolute-path link, prefixed with the proxy path; other links as they
/// are.
fn rewrite_link(link: &str, prefix: &str) -> String {
    if link.starts_with('/') && !link.starts_with("//") && !link.starts_with(prefix) {
        format!("{prefix}{link}")
    } else {
        link.to_string()
    }
}

/// `body` with the absolute-path URLs of its link attributes prefixed with
/// the proxy path, as upstream's proxy transport rewrites HTML.
fn rewrite_html(body: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(body.len() + 64);
    let mut rest = body;
    while let Some(eq) = rest.find('=') {
        let (before, after) = rest.split_at(eq);
        let name_end = before.trim_end();
        let name_start = name_end
            .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
            .map(|i| i + 1)
            .unwrap_or(0);
        let name = &name_end[name_start..];
        let after_eq = &after[1..];
        let trimmed = after_eq.trim_start();
        let quote = trimmed.chars().next();
        let is_link = name_start > 0
            && before.as_bytes()[name_start - 1].is_ascii_whitespace()
            && URL_ATTRIBUTES.contains(&name.to_ascii_lowercase().as_str())
            && matches!(quote, Some('"') | Some('\''));
        let value_start = after_eq.len() - trimmed.len() + 1;
        let value_len = if is_link {
            after_eq[value_start..].find(quote.unwrap_or('"'))
        } else {
            None
        };
        let Some(value_len) = value_len else {
            out.push_str(&rest[..eq + 1]);
            rest = after_eq;
            continue;
        };
        let value = &after_eq[value_start..value_start + value_len];
        out.push_str(&rest[..eq + 1]);
        out.push_str(&after_eq[..value_start]);
        out.push_str(&rewrite_link(value, prefix));
        rest = &after_eq[value_start + value_len..];
    }
    out.push_str(rest);
    out
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(&name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_spec_forms() {
        assert_eq!(split_name_spec("web-0"), ("http", "web-0", None));
        assert_eq!(
            split_name_spec("web-0:8080"),
            ("http", "web-0", Some("8080"))
        );
        assert_eq!(
            split_name_spec("https:web-0:8443"),
            ("https", "web-0", Some("8443"))
        );
        assert_eq!(split_name_spec("http:web-0"), ("http", "web-0", None));
        assert_eq!(
            split_name_spec("svc:portname1"),
            ("http", "svc", Some("portname1"))
        );
    }

    #[test]
    fn default_port_is_first_declared_container_port() {
        let pod = serde_json::json!({"spec": {"containers": [
            {"name": "a"},
            {"name": "b", "ports": [{"containerPort": 9376}, {"containerPort": 80}]}
        ]}});
        assert_eq!(first_container_port(&pod), Some(9376));
        assert_eq!(
            first_container_port(&serde_json::json!({"spec": {"containers": []}})),
            None
        );
    }

    #[test]
    fn service_endpoint_follows_the_port_name() {
        let service = serde_json::json!({"spec": {"ports": [
            {"name": "portname1", "port": 80, "targetPort": "dest1"},
            {"name": "tlsportname1", "port": 443, "targetPort": "tlsdest1"}
        ]}});
        let endpoints = serde_json::json!({"subsets": [{
            "addresses": [{"ip": "10.244.0.9"}],
            "ports": [{"name": "portname1", "port": 160}, {"name": "tlsportname1", "port": 460}]
        }]});
        let addr = |spec: Option<&str>| resolve_service_endpoint(&service, &endpoints, spec);
        assert_eq!(addr(None), Ok("10.244.0.9:160".parse().unwrap()));
        assert_eq!(
            addr(Some("tlsportname1")),
            Ok("10.244.0.9:460".parse().unwrap())
        );
        assert_eq!(addr(Some("443")), Ok("10.244.0.9:460".parse().unwrap()));
        assert_eq!(addr(Some("nope")), Err(ServiceResolution::NoSuchPort));
        let empty = serde_json::json!({});
        assert_eq!(
            resolve_service_endpoint(&service, &empty, None),
            Err(ServiceResolution::NoEndpoints)
        );
    }

    #[test]
    fn unnamed_single_port_matches_unnamed_endpoint_port() {
        let service = serde_json::json!({"spec": {"ports": [{"port": 80, "targetPort": 80}]}});
        let endpoints = serde_json::json!({"subsets": [{
            "addresses": [{"ip": "10.244.0.3"}], "ports": [{"port": 80}]}]});
        assert_eq!(
            resolve_service_endpoint(&service, &endpoints, None),
            Ok("10.244.0.3:80".parse().unwrap())
        );
    }

    #[test]
    fn html_links_take_the_proxy_prefix() {
        let prefix = "/api/v1/namespaces/ns/pods/web/proxy";
        assert_eq!(
            rewrite_html(r#"<a href="/rewriteme">test</a>"#, prefix),
            r#"<a href="/api/v1/namespaces/ns/pods/web/proxy/rewriteme">test</a>"#
        );
        assert_eq!(
            rewrite_html(
                r#"<img src='/a.png'><a href="rel">x</a><a href="//h/p">y</a>"#,
                prefix
            ),
            r#"<img src='/api/v1/namespaces/ns/pods/web/proxy/a.png'><a href="rel">x</a><a href="//h/p">y</a>"#
        );
        let already = format!(r#"<a href="{prefix}/x">"#);
        assert_eq!(rewrite_html(&already, prefix), already);
        assert_eq!(
            rewrite_html("a = b; if (x==1) {}", prefix),
            "a = b; if (x==1) {}"
        );
        assert_eq!(rewrite_link("/login", prefix), format!("{prefix}/login"));
        assert_eq!(rewrite_link("http://x/", prefix), "http://x/");
    }

    #[test]
    fn bare_proxy_path_redirects_only_get_and_head() {
        let uri: Uri = "/api/v1/namespaces/ns/pods/web/proxy?method=GET"
            .parse()
            .unwrap();
        let redirect = root_redirect(&uri, &Method::GET).unwrap();
        assert_eq!(redirect.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            redirect.headers()["location"],
            "/api/v1/namespaces/ns/pods/web/proxy/?method=GET"
        );
        assert!(root_redirect(&uri, &Method::POST).is_none());
        let slash: Uri = "/api/v1/namespaces/ns/pods/web/proxy/".parse().unwrap();
        assert!(root_redirect(&slash, &Method::GET).is_none());
    }
}
