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

fn resolve_port(ios: &IntOrString, ports: &[ContainerPort]) -> i32 {
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
        let path = http.path.as_deref().unwrap_or("/");
        let host = http.host.as_deref().unwrap_or(pod_ip);
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

async fn http_get_probe(
    host: &str,
    port: i32,
    path: &str,
    timeout: Duration,
    https: bool,
) -> bool {
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

async fn tcp_probe(host: &str, port: i32, timeout: Duration) -> bool {
    let addr = format!("{host}:{port}");
    tokio::time::timeout(timeout, TcpStream::connect(&addr))
        .await
        .is_ok_and(|r| r.is_ok())
}

