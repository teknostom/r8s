use std::net::Ipv4Addr;

use r8s_store::Store;
use r8s_types::{GroupVersionResource, Service};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

pub async fn run_dns_server(
    store: Store,
    shutdown: CancellationToken,
    data_dir: std::path::PathBuf,
) -> anyhow::Result<()> {
    let upstream = std::fs::read_to_string(data_dir.join("upstream_dns"))
        .unwrap_or_else(|_| "8.8.8.8".to_string());
    let upstream = upstream.trim().to_string();

    let socket = UdpSocket::bind("10.244.0.1:53").await?;
    let fwd_socket = UdpSocket::bind("0.0.0.0:0").await?;
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
                    None => match forward_query(query, &upstream, &fwd_socket).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::debug!("DNS forward error: {e}");
                            build_nxdomain(query)
                        }
                    },
                };

                let _ = socket.send_to(&response, src).await;
            }
        }
    }
}

fn parse_query(packet: &[u8]) -> Option<String> {
    if packet.len() < 12 {
        return None;
    }
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
        if label_len & 0xC0 == 0xC0 {
            return None;
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

fn resolve_service(store: &Store, query: &[u8], name: &str) -> Option<Vec<u8>> {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() < 5 || parts[2] != "svc" || parts[3] != "cluster" || parts[4] != "local" {
        return None;
    }

    let gvr = GroupVersionResource::services();
    let resource_ref = r8s_store::backend::ResourceRef {
        gvr: &gvr,
        namespace: Some(parts[1]),
        name: parts[0],
    };

    let svc: Service = store.get_as::<Service>(&resource_ref).ok()??;
    let cluster_ip = svc.spec.as_ref().and_then(|s| s.cluster_ip.as_deref())?;
    let ip: Ipv4Addr = cluster_ip.parse().ok()?;

    Some(build_a_response(query, ip))
}

fn build_a_response(query: &[u8], ip: Ipv4Addr) -> Vec<u8> {
    // Only copy up to the end of the question section. Anything after (e.g. an
    // EDNS OPT record in the additional section) must not appear before the answer.
    let q_end = match question_end(query) {
        Some(p) => p,
        None => return build_nxdomain(query),
    };

    let mut resp = Vec::with_capacity(q_end + 16);
    resp.extend_from_slice(&query[..q_end]);

    // QR=1, AA=1, RCODE=0
    resp[2] = 0x84;
    resp[3] = 0x00;
    // ANCOUNT=1, NSCOUNT=0, ARCOUNT=0
    resp[6] = 0x00;
    resp[7] = 0x01;
    resp[8] = 0x00;
    resp[9] = 0x00;
    resp[10] = 0x00;
    resp[11] = 0x00;

    // Answer: pointer to QNAME, TYPE=A, CLASS=IN, TTL=5s
    resp.extend_from_slice(&[0xC0, 0x0C]);
    resp.extend_from_slice(&[0x00, 0x01]);
    resp.extend_from_slice(&[0x00, 0x01]);
    resp.extend_from_slice(&5u32.to_be_bytes());
    resp.extend_from_slice(&[0x00, 0x04]);
    resp.extend_from_slice(&ip.octets());

    resp
}

fn question_end(packet: &[u8]) -> Option<usize> {
    if packet.len() < 12 {
        return None;
    }
    let mut pos = 12;
    loop {
        if pos >= packet.len() {
            return None;
        }
        let label_len = packet[pos] as usize;
        if label_len == 0 {
            pos += 1;
            break;
        }
        if label_len & 0xC0 == 0xC0 {
            return None;
        }
        pos += 1 + label_len;
    }
    // QTYPE (2) + QCLASS (2)
    let end = pos + 4;
    if end > packet.len() {
        return None;
    }
    Some(end)
}

fn build_nxdomain(query: &[u8]) -> Vec<u8> {
    let mut resp = query.to_vec();
    if resp.len() >= 4 {
        resp[2] = 0x84; // QR=1, AA=1
        resp[3] = 0x03; // RCODE=NXDOMAIN
    }
    resp
}

async fn forward_query(query: &[u8], upstream: &str, sock: &UdpSocket) -> anyhow::Result<Vec<u8>> {
    sock.send_to(query, format!("{upstream}:53")).await?;

    let mut buf = [0u8; 512];
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(2), sock.recv_from(&mut buf));
    let (len, _) = timeout.await??;
    Ok(buf[..len].to_vec())
}
