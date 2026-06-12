use std::net::Ipv4Addr;

use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_types::{Endpoints, GroupVersionResource, Service};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

const QTYPE_A: u16 = 1;

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

                // Anything under `cluster.local` is our zone and answered
                // authoritatively — including NXDOMAIN. Forwarding those
                // upstream would leak cluster names and add a 2s-timeout
                // round-trip to every ndots:5 search expansion of an external
                // lookup (`github.com.cluster.local` and friends).
                let response = match parse_query(query) {
                    Some((name, qtype)) if is_cluster_local(&name) => {
                        answer_cluster_query(&store, query, &name, qtype)
                    }
                    _ => match forward_query(query, &upstream, &fwd_socket).await {
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

fn is_cluster_local(name: &str) -> bool {
    name == "cluster.local" || name.ends_with(".cluster.local")
}

/// Extract the question name (lowercased — DNS names are case-insensitive)
/// and QTYPE. The response builders echo the original query bytes, so the
/// caller's casing is preserved on the wire.
fn parse_query(packet: &[u8]) -> Option<(String, u16)> {
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

    let qtype = u16::from_be_bytes([*packet.get(pos + 1)?, *packet.get(pos + 2)?]);
    Some((labels.join(".").to_ascii_lowercase(), qtype))
}

/// Authoritative answer for a name in the `cluster.local` zone: A records for
/// names that resolve, an empty NOERROR answer ("NODATA") for names that exist
/// but were asked a type we don't serve (AAAA — the cluster is IPv4-only), and
/// NXDOMAIN for everything else.
fn answer_cluster_query(store: &Store, query: &[u8], name: &str, qtype: u16) -> Vec<u8> {
    match resolve_cluster_name(store, name) {
        Some(ips) => {
            let ips = if qtype == QTYPE_A { ips } else { Vec::new() };
            build_answer(query, &ips)
        }
        None => build_nxdomain(query),
    }
}

/// Resolve a `cluster.local` name to IPs, or `None` if the name doesn't exist.
///
/// Two shapes are served, following the Kubernetes DNS spec:
///   `<svc>.<ns>.svc.cluster.local` — the Service's ClusterIP; for headless
///       Services (`clusterIP: None`) every ready backing pod IP instead.
///   `<hostname>.<svc>.<ns>.svc.cluster.local` — the single pod published
///       under that endpoint hostname. This is how StatefulSet peers find
///       each other (`redis-master-0.redis-headless.<ns>.svc.cluster.local`).
fn resolve_cluster_name(store: &Store, name: &str) -> Option<Vec<Ipv4Addr>> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [svc, ns, "svc", "cluster", "local"] => {
            let gvr = GroupVersionResource::services();
            let service: Service = store
                .get_as(&ResourceRef {
                    gvr: &gvr,
                    namespace: Some(ns),
                    name: svc,
                })
                .ok()??;
            match service.spec.as_ref().and_then(|s| s.cluster_ip.as_deref()) {
                Some(ip) if !ip.is_empty() && ip != "None" => Some(vec![ip.parse().ok()?]),
                // Headless: the Service name resolves straight to the pods.
                _ => non_empty(endpoint_ips(store, ns, svc, None)),
            }
        }
        [host, svc, ns, "svc", "cluster", "local"] => {
            non_empty(endpoint_ips(store, ns, svc, Some(host)))
        }
        _ => None,
    }
}

fn non_empty(ips: Vec<Ipv4Addr>) -> Option<Vec<Ipv4Addr>> {
    (!ips.is_empty()).then_some(ips)
}

/// Ready endpoint IPs for a Service, optionally narrowed to the endpoint
/// published with a given hostname. The endpoints controller only stamps a
/// hostname when the pod's `subdomain` names this Service, so per-pod records
/// stay scoped to the governing Service exactly like upstream k8s.
fn endpoint_ips(store: &Store, ns: &str, svc: &str, hostname: Option<&str>) -> Vec<Ipv4Addr> {
    let gvr = GroupVersionResource::endpoints();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some(ns),
        name: svc,
    };
    let Ok(Some(ep)) = store.get_as::<Endpoints>(&rref) else {
        return Vec::new();
    };

    let mut ips = Vec::new();
    for subset in ep.subsets.unwrap_or_default() {
        for addr in subset.addresses.unwrap_or_default() {
            if let Some(want) = hostname
                && addr.hostname.as_deref() != Some(want)
            {
                continue;
            }
            if let Ok(ip) = addr.ip.parse::<Ipv4Addr>() {
                ips.push(ip);
            }
        }
    }
    ips
}

/// Build a NOERROR response carrying one A record per IP. With no IPs this is
/// a "NODATA" answer: the name exists but has no records of the asked type.
fn build_answer(query: &[u8], ips: &[Ipv4Addr]) -> Vec<u8> {
    // Only copy up to the end of the question section. Anything after (e.g. an
    // EDNS OPT record in the additional section) must not appear before the answer.
    let q_end = match question_end(query) {
        Some(p) => p,
        None => return build_nxdomain(query),
    };

    // Stay inside one 512-byte UDP datagram (16 bytes per A record); drop
    // whole records past the cap rather than truncating mid-record.
    let max = 512usize.saturating_sub(q_end) / 16;
    let ips = &ips[..ips.len().min(max)];

    let mut resp = Vec::with_capacity(q_end + 16 * ips.len());
    resp.extend_from_slice(&query[..q_end]);

    // QR=1, AA=1, RCODE=0
    resp[2] = 0x84;
    resp[3] = 0x00;
    // ANCOUNT=n, NSCOUNT=0, ARCOUNT=0
    let ancount = (ips.len() as u16).to_be_bytes();
    resp[6] = ancount[0];
    resp[7] = ancount[1];
    resp[8] = 0x00;
    resp[9] = 0x00;
    resp[10] = 0x00;
    resp[11] = 0x00;

    for ip in ips {
        // Answer: pointer to QNAME, TYPE=A, CLASS=IN, TTL=5s
        resp.extend_from_slice(&[0xC0, 0x0C]);
        resp.extend_from_slice(&[0x00, 0x01]);
        resp.extend_from_slice(&[0x00, 0x01]);
        resp.extend_from_slice(&5u32.to_be_bytes());
        resp.extend_from_slice(&[0x00, 0x04]);
        resp.extend_from_slice(&ip.octets());
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        (dir, store)
    }

    fn create(store: &Store, gvr: &GroupVersionResource, ns: &str, name: &str, value: serde_json::Value) {
        store
            .create(
                ResourceRef {
                    gvr,
                    namespace: Some(ns),
                    name,
                },
                &value,
            )
            .unwrap();
    }

    /// Minimal DNS query packet: header + one question for `name`/`qtype`.
    fn query_packet(name: &str, qtype: u16) -> Vec<u8> {
        let mut p = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
        for label in name.split('.') {
            p.push(label.len() as u8);
            p.extend_from_slice(label.as_bytes());
        }
        p.push(0);
        p.extend_from_slice(&qtype.to_be_bytes());
        p.extend_from_slice(&1u16.to_be_bytes()); // CLASS=IN
        p
    }

    fn rcode(resp: &[u8]) -> u8 {
        resp[3] & 0x0F
    }

    fn answer_ips(query: &[u8], resp: &[u8]) -> Vec<Ipv4Addr> {
        let q_end = question_end(query).unwrap();
        let ancount = u16::from_be_bytes([resp[6], resp[7]]) as usize;
        (0..ancount)
            .map(|i| {
                // Each answer is 16 bytes: ptr(2) type(2) class(2) ttl(4)
                // rdlen(2) rdata(4); the IP is the last 4.
                let off = q_end + i * 16 + 12;
                Ipv4Addr::new(resp[off], resp[off + 1], resp[off + 2], resp[off + 3])
            })
            .collect()
    }

    fn seed_headless_redis(store: &Store) {
        create(
            store,
            &GroupVersionResource::services(),
            "redis",
            "redis-headless",
            json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "redis-headless", "namespace": "redis"},
                "spec": {"clusterIP": "None", "selector": {"app": "redis"}}
            }),
        );
        create(
            store,
            &GroupVersionResource::endpoints(),
            "redis",
            "redis-headless",
            json!({
                "apiVersion": "v1", "kind": "Endpoints",
                "metadata": {"name": "redis-headless", "namespace": "redis"},
                "subsets": [{"addresses": [
                    {"ip": "10.244.0.10", "hostname": "redis-master-0"},
                    {"ip": "10.244.0.11", "hostname": "redis-replicas-0"},
                    {"ip": "10.244.0.12", "hostname": "redis-replicas-1"}
                ]}]
            }),
        );
    }

    #[test]
    fn clusterip_service_resolves_to_single_a_record() {
        let (_dir, store) = temp_store();
        create(
            &store,
            &GroupVersionResource::services(),
            "demo",
            "web",
            json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "web", "namespace": "demo"},
                "spec": {"clusterIP": "10.96.0.50"}
            }),
        );

        let q = query_packet("web.demo.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 0);
        assert_eq!(answer_ips(&q, &resp), vec![Ipv4Addr::new(10, 96, 0, 50)]);
    }

    #[test]
    fn headless_service_resolves_to_all_ready_pod_ips() {
        let (_dir, store) = temp_store();
        seed_headless_redis(&store);

        let q = query_packet("redis-headless.redis.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 0);
        assert_eq!(
            answer_ips(&q, &resp),
            vec![
                Ipv4Addr::new(10, 244, 0, 10),
                Ipv4Addr::new(10, 244, 0, 11),
                Ipv4Addr::new(10, 244, 0, 12),
            ]
        );
    }

    #[test]
    fn per_pod_record_resolves_to_that_pod_only() {
        let (_dir, store) = temp_store();
        seed_headless_redis(&store);

        let q = query_packet("redis-master-0.redis-headless.redis.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 0);
        assert_eq!(answer_ips(&q, &resp), vec![Ipv4Addr::new(10, 244, 0, 10)]);
    }

    #[test]
    fn unknown_pod_hostname_is_nxdomain() {
        let (_dir, store) = temp_store();
        seed_headless_redis(&store);

        let q = query_packet("redis-master-7.redis-headless.redis.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 3);
    }

    #[test]
    fn aaaa_on_existing_name_is_nodata_not_nxdomain() {
        let (_dir, store) = temp_store();
        seed_headless_redis(&store);

        const QTYPE_AAAA: u16 = 28;
        let q = query_packet("redis-headless.redis.svc.cluster.local", QTYPE_AAAA);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        // NOERROR with zero answers tells the resolver "name exists, no AAAA"
        // so it settles for the A result instead of erroring the whole lookup.
        assert_eq!(rcode(&resp), 0);
        assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 0);
    }

    #[test]
    fn missing_service_is_nxdomain() {
        let (_dir, store) = temp_store();

        let q = query_packet("nope.demo.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 3);
    }

    #[test]
    fn search_expansion_of_external_name_is_nxdomain() {
        let (_dir, store) = temp_store();
        seed_headless_redis(&store);

        // ndots:5 makes pods try `github.com.<ns>.svc.cluster.local` before
        // the absolute name; it must NXDOMAIN fast and never resolve.
        let q = query_packet("github.com.redis.svc.cluster.local", QTYPE_A);
        let (name, qtype) = parse_query(&q).unwrap();
        let resp = answer_cluster_query(&store, &q, &name, qtype);

        assert_eq!(rcode(&resp), 3);
    }

    #[test]
    fn parse_query_lowercases_and_extracts_qtype() {
        let q = query_packet("Web.Demo.SVC.cluster.LOCAL", 28);
        let (name, qtype) = parse_query(&q).unwrap();
        assert_eq!(name, "web.demo.svc.cluster.local");
        assert_eq!(qtype, 28);
        assert!(is_cluster_local(&name));
    }
}
