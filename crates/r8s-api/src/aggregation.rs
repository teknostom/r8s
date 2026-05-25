//! API aggregation (the "aggregator").
//!
//! When a request targets a group/version that no local resource serves, but a
//! registered `apiregistration.k8s.io/v1 APIService` declares a backing
//! `spec.service`, we reverse-proxy the request to that service's pod over
//! HTTPS — exactly how the upstream kube-aggregator forwards e.g.
//! `/apis/metrics.k8s.io/*` to metrics-server.

use std::net::IpAddr;
use std::sync::Arc;

use axum::{body::Body, http::HeaderMap, http::Method, response::Response};
use base64::Engine;
use hyper::StatusCode;
use r8s_types::GroupVersionResource;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::admission::resolve_service_endpoint;
use crate::discovery::AppState;
use crate::response;

struct Backend {
    namespace: String,
    name: String,
    port: u16,
    /// PEM CA to verify the backend, unless `insecure` is set.
    ca_pem: Option<Vec<u8>>,
    insecure: bool,
}

impl Backend {
    /// SNI / Host header — the in-cluster Service DNS name the backend's serving
    /// cert is issued for.
    fn host(&self) -> String {
        format!("{}.{}.svc", self.name, self.namespace)
    }
}

/// If a registered APIService backs `group/version`, reverse-proxy the request
/// to it and return the response. `None` means no aggregated backend — the
/// caller should fall through to its normal 404.
pub async fn try_proxy(
    state: &AppState,
    method: &Method,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<Response> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // Aggregated APIs are always under /apis/<group>/<version>/...
    if parts.len() < 3 || parts[0] != "apis" {
        return None;
    }
    let backend = find_backend(state, parts[1], parts[2])?;

    let (ip, port) = match resolve_service_endpoint(
        &state.store,
        &backend.namespace,
        &backend.name,
        backend.port,
    ) {
        Ok(ep) => ep,
        Err(e) => {
            return Some(response::status_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                &format!("aggregated backend unavailable: {e}"),
            ));
        }
    };

    let path_and_query = match query {
        Some(q) if !q.is_empty() => format!("{path}?{q}"),
        _ => path.to_string(),
    };
    Some(proxy(ip, port, &backend, &state.data_dir, method, &path_and_query, headers, body).await)
}

fn find_backend(state: &AppState, group: &str, version: &str) -> Option<Backend> {
    let gvr = GroupVersionResource::new("apiregistration.k8s.io", "v1", "apiservices");
    let list = state.store.list(&gvr, None, None, None, None, None).ok()?;
    for item in &list.items {
        let Some(spec) = item.get("spec") else { continue };
        if spec.get("group").and_then(|v| v.as_str()) != Some(group)
            || spec.get("version").and_then(|v| v.as_str()) != Some(version)
        {
            continue;
        }
        // A `service` is what makes this an *aggregated* (proxied) API; local
        // APIServices (the built-in groups) have none.
        let Some(svc) = spec.get("service").and_then(|v| v.as_object()) else {
            continue;
        };
        let (Some(name), Some(namespace)) = (
            svc.get("name").and_then(|v| v.as_str()),
            svc.get("namespace").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let port = svc.get("port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;
        let insecure = spec
            .get("insecureSkipTLSVerify")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ca_pem = spec
            .get("caBundle")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .and_then(|b64| base64::engine::general_purpose::STANDARD.decode(b64).ok());
        return Some(Backend {
            namespace: namespace.to_string(),
            name: name.to_string(),
            port,
            ca_pem,
            insecure,
        });
    }
    None
}

async fn proxy(
    ip: IpAddr,
    port: u16,
    backend: &Backend,
    data_dir: &std::path::Path,
    method: &Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    let cfg = match proxy_tls_config(data_dir, backend.ca_pem.as_deref(), backend.insecure) {
        Ok(c) => c,
        Err(e) => {
            return response::status_error(
                StatusCode::BAD_GATEWAY,
                "BadGateway",
                &format!("aggregated backend TLS: {e}"),
            );
        }
    };
    let host = backend.host();
    let server_name = match ServerName::try_from(host.clone()) {
        Ok(n) => n,
        Err(e) => {
            return response::status_error(
                StatusCode::BAD_GATEWAY,
                "BadGateway",
                &format!("invalid sni '{host}': {e}"),
            );
        }
    };

    let result = async {
        let tcp = TcpStream::connect((ip, port))
            .await
            .map_err(|e| format!("connect {ip}:{port}: {e}"))?;
        let mut stream = TlsConnector::from(Arc::new(cfg))
            .connect(server_name, tcp)
            .await
            .map_err(|e| format!("tls handshake: {e}"))?;

        // Forward the request line, Host, the handful of headers backends care
        // about, and the body. Connection: close keeps response framing simple.
        let mut req = format!("{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\n");
        for h in ["accept", "content-type"] {
            if let Some(v) = headers.get(h).and_then(|v| v.to_str().ok()) {
                req.push_str(&format!("{h}: {v}\r\n"));
            }
        }
        // Front-proxy identity: r8s has no per-request auth, so present a fixed
        // privileged identity. metrics-server trusts these headers because we
        // also present a client cert from the requestheader CA (mTLS, above).
        req.push_str("X-Remote-User: system:apiserver\r\n");
        req.push_str("X-Remote-Group: system:masters\r\n");
        req.push_str("X-Remote-Group: system:authenticated\r\n");
        req.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));

        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;
        if !body.is_empty() {
            stream.write_all(body).await.map_err(|e| format!("write body: {e}"))?;
        }
        stream.flush().await.map_err(|e| format!("flush: {e}"))?;

        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .map_err(|e| format!("read: {e}"))?;
        Ok::<Vec<u8>, String>(raw)
    }
    .await;

    match result {
        Ok(raw) => parse_response(&raw),
        Err(e) => response::status_error(
            StatusCode::BAD_GATEWAY,
            "BadGateway",
            &format!("aggregated backend request failed: {e}"),
        ),
    }
}

/// Turn a raw HTTP/1.1 response into an axum Response, preserving the status
/// code and Content-Type and de-chunking the body if needed.
fn parse_response(raw: &[u8]) -> Response {
    let sep = b"\r\n\r\n";
    let Some(pos) = raw.windows(4).position(|w| w == sep) else {
        return response::status_error(
            StatusCode::BAD_GATEWAY,
            "BadGateway",
            "malformed response from aggregated backend",
        );
    };
    let head = String::from_utf8_lossy(&raw[..pos]);
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(502);
    let mut content_type = "application/json".to_string();
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            if k == "content-type" {
                content_type = v.to_string();
            } else if k == "transfer-encoding" && v.eq_ignore_ascii_case("chunked") {
                chunked = true;
            }
        }
    }
    let raw_body = &raw[pos + 4..];
    let body = if chunked { dechunk(raw_body) } else { raw_body.to_vec() };

    Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY))
        .header("content-type", content_type)
        .body(Body::from(body))
        .expect("valid response")
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    loop {
        let Some(nl) = data.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size = usize::from_str_radix(
            std::str::from_utf8(&data[..nl]).unwrap_or("0").trim(),
            16,
        )
        .unwrap_or(0);
        data = &data[nl + 2..];
        if size == 0 || data.len() < size {
            break;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size..];
        // skip trailing CRLF after the chunk
        if data.starts_with(b"\r\n") {
            data = &data[2..];
        }
    }
    out
}

/// Build the client TLS config for proxying to an aggregated backend:
/// server verification (the APIService's caBundle, or skipped when it sets
/// `insecureSkipTLSVerify: true`), plus **client auth** with the front-proxy
/// cert so the backend authenticates the aggregator (requestheader auth).
fn proxy_tls_config(
    data_dir: &std::path::Path,
    ca_pem: Option<&[u8]>,
    insecure: bool,
) -> Result<rustls::ClientConfig, String> {
    use rustls::pki_types::CertificateDer;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls versions: {e}"))?;

    let want_client = if insecure {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
    } else {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(pem) = ca_pem {
            let mut cur = std::io::Cursor::new(pem);
            for der in rustls_pemfile::certs(&mut cur).flatten() {
                let _ = roots.add(der);
            }
        }
        builder.with_root_certificates(roots)
    };

    // Front-proxy client cert (minted by certs.rs). If absent, proceed without
    // mTLS — the backend will reject, but we don't want to hard-fail here.
    let cert_path = data_dir.join("certs/front-proxy-client.crt");
    let key_path = data_dir.join("certs/front-proxy-client.key");
    match (std::fs::read(&cert_path), std::fs::read(&key_path)) {
        (Ok(cert_pem), Ok(key_pem)) => {
            let chain: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
                    .flatten()
                    .collect();
            let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
                .map_err(|e| format!("front-proxy key: {e}"))?
                .ok_or("front-proxy key: none found")?;
            want_client
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("client auth: {e}"))
        }
        _ => Ok(want_client.with_no_client_auth()),
    }
}

#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
