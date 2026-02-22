use std::net::Ipv4Addr;

use r8s_store::Store;
use r8s_types::GroupVersionResource;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

fn services_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "services")
}

/// Run a minimal DNS server on the bridge IP (10.244.0.1:53).
///
/// Resolves `<service>.<namespace>.svc.cluster.local` by looking up Service
/// objects in the store. Forwards all other queries to the host's upstream resolver.
pub async fn run_dns_server(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    let upstream = std::fs::read_to_string("/tmp/r8s/upstream_dns")
        .unwrap_or_else(|_| "8.8.8.8".to_string());
    let upstream = upstream.trim().to_string();

    let socket = UdpSocket::bind("10.244.0.1:53").await?;
    tracing::info!(upstream, "DNS server listening on 10.244.0.1:53");

    let mut buf = [0u8; 512];
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("DNS server shutting down");
                return Ok(());
            }
            result = socket.recv_from(&mut buf) => {
                let (len, src) = result?;
                let query = &buf[..len];

                let response = match parse_query(query) {
                    Some(name) if name.ends_with(".svc.cluster.local") => {
                        resolve_service(&store, query, &name)
                    }
                    _ => None,
                };

                let response = match response {
                    Some(r) => r,
                    None => {
                        // Forward to upstream
                        match forward_query(query, &upstream).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::debug!("DNS forward error: {e}");
                                build_nxdomain(query)
                            }
                        }
                    }
                };

                let _ = socket.send_to(&response, src).await;
            }
        }
    }
}

/// Parse the QNAME from a DNS query packet. Returns the dotted name.
fn parse_query(packet: &[u8]) -> Option<String> {
    if packet.len() < 12 {
        return None;
    }
    // Check QDCOUNT >= 1
    let qdcount = u16::from_be_bytes([packet[4], packet[5]]);
    if qdcount == 0 {
        return None;
    }

    let mut pos = 12;
    let mut labels = Vec::new();
    loop {
        if pos >= packet.len() {
            return None;
        }
        let label_len = packet[pos] as usize;
        if label_len == 0 {
            break;
        }
        // Pointer (compression) — not expected in queries but handle it
        if label_len & 0xC0 == 0xC0 {
            return None; // Don't handle compressed queries
        }
        pos += 1;
        if pos + label_len > packet.len() {
            return None;
        }
        labels.push(std::str::from_utf8(&packet[pos..pos + label_len]).ok()?);
        pos += label_len;
    }

    Some(labels.join("."))
}

/// Try to resolve a `<svc>.<ns>.svc.cluster.local` query from the store.
fn resolve_service(store: &Store, query: &[u8], name: &str) -> Option<Vec<u8>> {
    // Parse: service.namespace.svc.cluster.local
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() < 5 || parts[2] != "svc" || parts[3] != "cluster" || parts[4] != "local" {
        return None;
    }
    let svc_name = parts[0];
    let namespace = parts[1];

    let gvr = services_gvr();
    let resource_ref = r8s_store::backend::ResourceRef {
        gvr: &gvr,
        namespace: Some(namespace),
        name: svc_name,
    };

    let service = store.get(&resource_ref).ok()??;
    let cluster_ip = service["spec"]["clusterIP"].as_str()?;
    let ip: Ipv4Addr = cluster_ip.parse().ok()?;

    Some(build_a_response(query, ip))
}

/// Build a DNS A record response for the given query.
fn build_a_response(query: &[u8], ip: Ipv4Addr) -> Vec<u8> {
    let mut resp = Vec::with_capacity(query.len() + 16);
    resp.extend_from_slice(query);

    // Set response flags: QR=1, AA=1, RCODE=0
    resp[2] = 0x84; // QR=1, AA=1
    resp[3] = 0x00; // RCODE=0 (no error)
    // ANCOUNT = 1
    resp[6] = 0x00;
    resp[7] = 0x01;

    // Answer section: pointer to QNAME in question section
    resp.extend_from_slice(&[0xC0, 0x0C]); // Name pointer to offset 12
    resp.extend_from_slice(&[0x00, 0x01]); // TYPE = A
    resp.extend_from_slice(&[0x00, 0x01]); // CLASS = IN
    resp.extend_from_slice(&5u32.to_be_bytes()); // TTL = 5 seconds
    resp.extend_from_slice(&[0x00, 0x04]); // RDLENGTH = 4
    resp.extend_from_slice(&ip.octets()); // RDATA = IP address

    resp
}

/// Build an NXDOMAIN response.
fn build_nxdomain(query: &[u8]) -> Vec<u8> {
    let mut resp = query.to_vec();
    if resp.len() >= 4 {
        resp[2] = 0x84; // QR=1, AA=1
        resp[3] = 0x03; // RCODE=3 (NXDOMAIN)
    }
    resp
}

/// Forward a DNS query to the upstream resolver and return the response.
async fn forward_query(query: &[u8], upstream: &str) -> anyhow::Result<Vec<u8>> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.send_to(query, format!("{upstream}:53")).await?;

    let mut buf = [0u8; 512];
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), sock.recv_from(&mut buf));
    let (len, _) = timeout.await??;
    Ok(buf[..len].to_vec())
}
