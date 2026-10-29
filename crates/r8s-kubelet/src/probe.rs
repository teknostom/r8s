use std::sync::{Arc, OnceLock};
use std::time::Duration;

use r8s_runtime::{ContainerId, ContainerRuntime};
use r8s_types::{ContainerPort, IntOrString, Probe};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

pub(crate) fn resolve_port(ios: &IntOrString, ports: &[ContainerPort]) -> i32 {
    match ios {
        IntOrString::Int(i) => *i,
        IntOrString::String(s) => {
            if let Ok(n) = s.parse() {
                return n;
            }
            ports
                .iter()
                .find(|p| p.name.as_deref() == Some(s.as_str()))
                .map(|p| p.container_port)
                .unwrap_or(0)
        }
    }
}

/// Execute a probe against a container. Returns true if the probe succeeds.
pub async fn exec_probe<R: ContainerRuntime>(
    probe: &Probe,
    pod_ip: &str,
    runtime: &R,
    container_id: &ContainerId,
    ports: &[ContainerPort],
) -> bool {
    let timeout = Duration::from_secs(probe.timeout_seconds.unwrap_or(1) as u64);

    if let Some(http) = &probe.http_get {
        let port = resolve_port(&http.port, ports);
        let path = http
            .path
            .as_deref()
            .filter(|p| !p.is_empty())
            .unwrap_or("/");
        let host = http
            .host
            .as_deref()
            .filter(|h| !h.is_empty())
            .unwrap_or(pod_ip);
        // Kubernetes probes don't validate certificates and accept any scheme
        // value case-insensitively; default is HTTP per the v1 spec.
        let https = http
            .scheme
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("HTTPS"));
        return http_get_probe(host, port, path, timeout, https).await;
    }

    if let Some(tcp) = &probe.tcp_socket {
        let port = resolve_port(&tcp.port, ports);
        return tcp_probe(pod_ip, port, timeout).await;
    }

    if let Some(grpc) = &probe.grpc {
        return grpc_probe(pod_ip, grpc.port, grpc.service.as_deref(), timeout).await;
    }

    if let Some(exec_action) = &probe.exec
        && let Some(command) = &exec_action.command
    {
        // Run inside the container (via the runtime's exec), not a host-side
        // nsenter: the command resolves against the container's PATH and runs
        // in its network namespace, so checks like `pg_isready -h 127.0.0.1`
        // reach the container's own server.
        return matches!(
            runtime.exec_sync(container_id, command, timeout).await,
            Ok(0)
        );
    }

    // No probe handler configured — treat as success
    true
}

/// Run a lifecycle hook (postStart/preStop): exec in the container, an HTTP
/// GET (against the pod unless a host is named), or a TCP connect. True when
/// it succeeded.
pub(crate) async fn run_lifecycle_handler<R: ContainerRuntime>(
    handler: &r8s_types::LifecycleHandler,
    pod_ip: &str,
    runtime: &R,
    container_id: &ContainerId,
    ports: &[ContainerPort],
    timeout: Duration,
) -> bool {
    if let Some(exec) = &handler.exec
        && let Some(command) = &exec.command
    {
        return matches!(
            runtime.exec_sync(container_id, command, timeout).await,
            Ok(0)
        );
    }
    if let Some(http) = &handler.http_get {
        let port = resolve_port(&http.port, ports);
        let path = http
            .path
            .as_deref()
            .filter(|p| !p.is_empty())
            .unwrap_or("/");
        let host = http
            .host
            .as_deref()
            .filter(|h| !h.is_empty())
            .unwrap_or(pod_ip);
        let https = http
            .scheme
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("HTTPS"));
        return http_get_probe(host, port, path, timeout, https).await;
    }
    if let Some(tcp) = &handler.tcp_socket {
        let port = resolve_port(&tcp.port, ports);
        return tcp_probe(pod_ip, port, timeout).await;
    }
    if let Some(sleep) = &handler.sleep {
        tokio::time::sleep(Duration::from_secs(sleep.seconds.max(0) as u64)).await;
    }
    true
}

async fn http_get_probe(host: &str, port: i32, path: &str, timeout: Duration, https: bool) -> bool {
    let addr = format!("{host}:{port}");
    let result = tokio::time::timeout(timeout, async {
        let tcp = TcpStream::connect(&addr).await?;
        if https {
            // Sample-webhook and many webhook-style pods serve TLS with a
            // self-signed cert that's never in any system trust store, so
            // skip verification — matches kubelet, which only cares about
            // the HTTP status code.
            let connector = TlsConnector::from(tls_client_config());
            let dns_name = ServerName::try_from(host.to_string())
                .unwrap_or_else(|_| ServerName::try_from("invalid.local").unwrap());
            let stream = connector.connect(dns_name, tcp).await?;
            run_http_request(stream, host, path).await
        } else {
            run_http_request(tcp, host, path).await
        }
    })
    .await;

    matches!(result, Ok(Ok(true)))
}

async fn run_http_request<S>(mut stream: S, host: &str, path: &str) -> std::io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf[..n]);
    if let Some(code_str) = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        && let Ok(code) = code_str.parse::<u16>()
    {
        return Ok((200..400).contains(&code));
    }
    Ok(false)
}

fn tls_client_config() -> Arc<ClientConfig> {
    static CFG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| {
        let cfg = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth();
        Arc::new(cfg)
    })
    .clone()
}

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

/// A gRPC health check (`grpc.health.v1.Health/Check`) over plain HTTP/2:
/// succeeds when the service reports SERVING.
async fn grpc_probe(host: &str, port: i32, service: Option<&str>, timeout: Duration) -> bool {
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;

    let check = async {
        let tcp = TcpStream::connect((host, port as u16)).await.ok()?;
        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(tcp),
        )
        .await
        .ok()?;
        tokio::spawn(conn);

        // HealthCheckRequest { service = 1 }, length-prefixed as gRPC frames it.
        let mut message = Vec::new();
        if let Some(service) = service.filter(|s| !s.is_empty()) {
            message.push(0x0a);
            message.extend(encode_varint(service.len() as u64));
            message.extend_from_slice(service.as_bytes());
        }
        let mut body = vec![0u8];
        body.extend_from_slice(&(message.len() as u32).to_be_bytes());
        body.extend_from_slice(&message);

        let req = hyper::Request::builder()
            .method("POST")
            .uri(format!("http://{host}:{port}/grpc.health.v1.Health/Check"))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(body)))
            .ok()?;
        let resp = sender.send_request(req).await.ok()?;
        if resp.status() != 200 {
            return Some(false);
        }
        let grpc_status = |headers: &hyper::HeaderMap| {
            headers
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .map(|s| s == "0")
        };
        if let Some(status) = grpc_status(resp.headers()) {
            // Trailers-only response: the call failed before any body.
            return Some(status);
        }
        let collected = resp.into_body().collect().await.ok()?;
        if collected
            .trailers()
            .and_then(grpc_status)
            .is_some_and(|ok| !ok)
        {
            return Some(false);
        }
        let payload = collected.to_bytes();
        // HealthCheckResponse { status = 1 }: SERVING is 1.
        let message = payload.get(5..)?;
        let serving = match message.first() {
            Some(0x08) => message.get(1) == Some(&1),
            _ => false,
        };
        Some(serving)
    };
    matches!(tokio::time::timeout(timeout, check).await, Ok(Some(true)))
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

async fn tcp_probe(host: &str, port: i32, timeout: Duration) -> bool {
    let addr = format!("{host}:{port}");
    tokio::time::timeout(timeout, TcpStream::connect(&addr))
        .await
        .is_ok_and(|r| r.is_ok())
}
