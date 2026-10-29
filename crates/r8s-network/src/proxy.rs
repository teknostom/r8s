//! ClusterIP, LoadBalancer and pod hostPort DNAT via nftables. The DNAT
//! chains are rebuilt from the store on every Service, Endpoints or Pod
//! event and every `RESYNC`, and swapped in as one `nft -f` transaction.

use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::process::{Command, Stdio};
use std::time::Duration;

use r8s_store::Store;
use r8s_store::watch::WatchEvent;
use r8s_types::{
    BRIDGE_NAME, GroupVersionResource, POD_CIDR, Pod, SERVICE_CIDR, Service, nft_table_name,
};
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::endpoints::{ReadyAddr, ready_endpoint_addresses};
use crate::error::{NetworkError, Result};

const RESYNC: Duration = Duration::from_secs(5);

pub fn setup_nat_table(cluster: &str) -> Result<()> {
    let table = nft_table_name(cluster);
    let _ = nft(&["delete", "table", "ip", &table]);

    nft(&["add", "table", "ip", &table])?;
    nft(&[
        "add",
        "chain",
        "ip",
        &table,
        "prerouting",
        "{ type nat hook prerouting priority -100 ; }",
    ])?;
    nft(&[
        "add",
        "chain",
        "ip",
        &table,
        "output",
        "{ type nat hook output priority -100 ; }",
    ])?;
    nft(&[
        "add",
        "chain",
        "ip",
        &table,
        "postrouting",
        "{ type nat hook postrouting priority 100 ; }",
    ])?;

    // Masquerade outbound pod traffic for internet access
    nft(&[
        "add",
        "rule",
        "ip",
        &table,
        "postrouting",
        "ip",
        "saddr",
        POD_CIDR,
        "oifname",
        "!=",
        BRIDGE_NAME,
        "masquerade",
    ])?;

    // Hairpin: when pod->ClusterIP is DNAT'd to a pod on the same bridge,
    // masquerade so the return path goes through conntrack
    nft(&[
        "add",
        "rule",
        "ip",
        &table,
        "postrouting",
        "ip",
        "saddr",
        POD_CIDR,
        "ip",
        "daddr",
        POD_CIDR,
        "oifname",
        BRIDGE_NAME,
        "masquerade",
    ])?;

    // Loopback-sourced traffic DNATed to a pod by a 127.0.0.1 hostPort needs
    // a routable source for the reply.
    nft(&[
        "add",
        "rule",
        "ip",
        &table,
        "postrouting",
        "ip",
        "saddr",
        "127.0.0.0/8",
        "oifname",
        BRIDGE_NAME,
        "masquerade",
    ])?;

    tracing::info!(table, "nftables NAT table ready");
    Ok(())
}

/// Apply the rules on every Service/Endpoints/Pod event that changes them,
/// and every `RESYNC` regardless.
pub async fn run(store: Store, shutdown: CancellationToken, cluster: String) -> Result<()> {
    let mut svc_rx = store.watch(&GroupVersionResource::services());
    let mut ep_rx = store.watch(&GroupVersionResource::endpoints());
    let mut pod_rx = store.watch(&GroupVersionResource::pods());
    let mut resync = tokio::time::interval(RESYNC);
    resync.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut applied = String::new();

    loop {
        let forced = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = resync.tick() => true,
            r = svc_rx.recv() => if matches!(r, Err(broadcast::error::RecvError::Closed)) {
                return Ok(());
            } else {
                false
            },
            r = ep_rx.recv() => if matches!(r, Err(broadcast::error::RecvError::Closed)) {
                return Ok(());
            } else {
                false
            },
            r = pod_rx.recv() => if matches!(r, Err(broadcast::error::RecvError::Closed)) {
                return Ok(());
            } else {
                false
            },
        };
        drain(&mut svc_rx);
        drain(&mut ep_rx);
        drain(&mut pod_rx);
        let ruleset = render_rules(&store, &cluster);
        if !forced && ruleset == applied {
            continue;
        }
        match nft_apply(&ruleset) {
            Ok(()) => applied = ruleset,
            Err(e) => tracing::warn!("service proxy sync: {e}"),
        }
    }
}

/// Discard queued events so a burst produces one sync.
fn drain(rx: &mut broadcast::Receiver<WatchEvent>) {
    use broadcast::error::TryRecvError;
    while matches!(rx.try_recv(), Ok(_) | Err(TryRecvError::Lagged(_))) {}
}

pub fn sync_service_rules(store: &Store, cluster: &str) -> Result<()> {
    nft_apply(&render_rules(store, cluster))
}

/// The complete DNAT ruleset for the store's current Services and Pods.
fn render_rules(store: &Store, cluster: &str) -> String {
    let table = nft_table_name(cluster);
    let services: Vec<Service> = store
        .list_as::<Service>(&GroupVersionResource::services(), None)
        .unwrap_or_default();
    let forwards = collect_forwards(store, &services);
    let host_ports = collect_host_ports(store);
    render_ruleset(&table, &forwards, &host_ports)
}

/// One container hostPort and the pod address behind it.
#[derive(Debug, PartialEq, Eq)]
struct HostPortForward {
    /// `hostIP`; `None` means every local address.
    host_ip: Option<Ipv4Addr>,
    proto: String,
    host_port: i32,
    backend: SocketAddrV4,
}

/// hostPorts of pods that hold a pod IP and are neither finished nor being
/// deleted. hostNetwork pods listen on the host directly.
fn collect_host_ports(store: &Store) -> Vec<HostPortForward> {
    let pods: Vec<Pod> = store
        .list_as::<Pod>(&GroupVersionResource::pods(), None)
        .unwrap_or_default();
    let mut out = Vec::new();
    for pod in &pods {
        let Some(spec) = pod.spec.as_ref() else {
            continue;
        };
        if spec.host_network == Some(true) || pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        if matches!(status.phase.as_deref(), Some("Succeeded" | "Failed")) {
            continue;
        }
        let Some(pod_ip) = status
            .pod_ip
            .as_deref()
            .and_then(|ip| ip.parse::<Ipv4Addr>().ok())
        else {
            continue;
        };
        for port in spec
            .containers
            .iter()
            .flat_map(|c| c.ports.iter().flatten())
        {
            let Some(host_port) = port.host_port.filter(|p| *p > 0) else {
                continue;
            };
            let host_ip = match port.host_ip.as_deref().filter(|ip| !ip.is_empty()) {
                None | Some("0.0.0.0") => None,
                Some(ip) => match ip.parse::<Ipv4Addr>() {
                    Ok(ip) => Some(ip),
                    Err(_) => continue,
                },
            };
            out.push(HostPortForward {
                host_ip,
                proto: port.protocol.as_deref().unwrap_or("TCP").to_lowercase(),
                host_port,
                backend: SocketAddrV4::new(pod_ip, port.container_port as u16),
            });
        }
    }
    out
}

/// One Service port and its backends.
struct Forward {
    cluster_ip: String,
    proto: String,
    port: i32,
    load_balancer: bool,
    /// `nodePort` of a NodePort or LoadBalancer Service: reachable on every
    /// local address.
    node_port: Option<i32>,
    /// `sessionAffinity: ClientIP`: a client keeps its backend.
    client_ip_affinity: bool,
    backends: Vec<SocketAddrV4>,
}

fn collect_forwards(store: &Store, services: &[Service]) -> Vec<Forward> {
    let mut out = Vec::new();
    for svc in services {
        let Some(svc_name) = svc.metadata.name.as_deref() else {
            continue;
        };
        let Some(spec) = svc.spec.as_ref() else {
            continue;
        };
        let cluster_ip = match spec.cluster_ip.as_deref() {
            Some(ip) if ip != "None" && !ip.is_empty() => ip,
            _ => continue,
        };
        let ports = spec.ports.as_deref().unwrap_or_default();
        if ports.is_empty() {
            continue;
        }
        let addrs = ready_endpoint_addresses(store, svc.metadata.namespace.as_deref(), svc_name);
        if addrs.is_empty() {
            continue;
        }
        let svc_type = spec.type_.as_deref().unwrap_or("ClusterIP");
        let load_balancer = svc_type == "LoadBalancer";
        let node_ports = matches!(svc_type, "NodePort" | "LoadBalancer");
        let client_ip_affinity = spec.session_affinity.as_deref() == Some("ClientIP");

        for (index, port) in ports.iter().enumerate() {
            let name = port.name.as_deref().filter(|n| !n.is_empty());
            let backends = backends_for(&addrs, index, name);
            if backends.is_empty() {
                continue;
            }
            out.push(Forward {
                cluster_ip: cluster_ip.to_string(),
                proto: port.protocol.as_deref().unwrap_or("TCP").to_lowercase(),
                port: port.port,
                load_balancer,
                node_port: port.node_port.filter(|p| *p > 0 && node_ports),
                client_ip_affinity,
                backends,
            });
        }
    }
    out
}

/// Backends for the Service port at `index` named `name`; addresses whose
/// Endpoints carry no matching port are skipped.
fn backends_for(addrs: &[ReadyAddr], index: usize, name: Option<&str>) -> Vec<SocketAddrV4> {
    addrs
        .iter()
        .filter_map(|a| Some(SocketAddrV4::new(a.ip, a.port_for(index, name)?)))
        .collect()
}

/// nft script that flushes both DNAT chains and refills them: Service rules
/// first, then hostPorts, which on an unspecified hostIP take any local
/// address outside the Service range.
fn render_ruleset(table: &str, forwards: &[Forward], host_ports: &[HostPortForward]) -> String {
    let mut out = format!("flush chain ip {table} prerouting\nflush chain ip {table} output\n");
    for f in forwards {
        let Forward {
            cluster_ip,
            proto,
            port,
            ..
        } = f;
        let target = build_dnat_target(&f.backends, f.client_ip_affinity);
        out.push_str(&format!(
            "add rule ip {table} prerouting ip daddr {cluster_ip} {proto} dport {port} dnat to {target}\n"
        ));
        out.push_str(&format!(
            "add rule ip {table} output ip daddr {cluster_ip} {proto} dport {port} dnat to {target}\n"
        ));
        if let Some(node_port) = f.node_port {
            for chain in ["prerouting", "output"] {
                out.push_str(&format!(
                    "add rule ip {table} {chain} fib daddr type local ip daddr != {SERVICE_CIDR} {proto} dport {node_port} dnat to {target}\n"
                ));
            }
        }
        if f.load_balancer {
            out.push_str(&format!(
                "add rule ip {table} prerouting iifname != {BRIDGE_NAME} {proto} dport {port} dnat to {target}\n"
            ));
            out.push_str(&format!(
                "add rule ip {table} output {proto} dport {port} dnat to {target}\n"
            ));
        }
    }
    for h in host_ports {
        let HostPortForward {
            proto,
            host_port,
            backend,
            ..
        } = h;
        let dst = match h.host_ip {
            Some(ip) => format!("ip daddr {ip}"),
            None => format!("fib daddr type local ip daddr != {SERVICE_CIDR}"),
        };
        for chain in ["prerouting", "output"] {
            out.push_str(&format!(
                "add rule ip {table} {chain} {dst} {proto} dport {host_port} dnat to {backend}\n"
            ));
        }
    }
    out
}

pub fn cleanup(cluster: &str) {
    let table = nft_table_name(cluster);
    let _ = nft(&["delete", "table", "ip", &table]);
    tracing::info!(table, "nftables table removed");
}

/// `ip:port` for one backend, else a map of `ip . port` concatenations
/// picked round-robin, or by a hash of the client address when the Service
/// wants ClientIP affinity.
fn build_dnat_target(backends: &[SocketAddrV4], client_ip_affinity: bool) -> String {
    if let [only] = backends {
        return only.to_string();
    }
    let addrs: Vec<String> = backends
        .iter()
        .enumerate()
        .map(|(i, addr)| format!("{i} : {} . {}", addr.ip(), addr.port()))
        .collect();
    let pick = if client_ip_affinity {
        "jhash ip saddr"
    } else {
        "numgen inc"
    };
    format!(
        "{pick} mod {} map {{ {} }}",
        backends.len(),
        addrs.join(", ")
    )
}

fn nft(args: &[&str]) -> Result<()> {
    let output = Command::new("nft").args(args).output()?;
    if !output.status.success() {
        return Err(NetworkError::Command {
            cmd: "nft".to_string(),
            args: args.join(" "),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Apply `ruleset` as one `nft -f` transaction.
fn nft_apply(ruleset: &str) -> Result<()> {
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(ruleset.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(NetworkError::Command {
            cmd: "nft".to_string(),
            args: "-f -".to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use r8s_store::backend::ResourceRef;
    use serde_json::json;
    use std::net::Ipv4Addr;

    fn ip(n: u8) -> Ipv4Addr {
        Ipv4Addr::new(10, 244, 0, n)
    }

    fn sock(n: u8, port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(ip(n), port)
    }

    #[test]
    fn single_backend_is_a_plain_dnat_target() {
        assert_eq!(
            build_dnat_target(&[sock(10, 8080)], false),
            "10.244.0.10:8080"
        );
        assert_eq!(
            build_dnat_target(&[sock(10, 8080)], true),
            "10.244.0.10:8080"
        );
    }

    #[test]
    fn multiple_backends_load_balance_via_numgen_map() {
        let got = build_dnat_target(&[sock(10, 80), sock(11, 80), sock(12, 80)], false);
        assert_eq!(
            got,
            "numgen inc mod 3 map { 0 : 10.244.0.10 . 80, 1 : 10.244.0.11 . 80, 2 : 10.244.0.12 . 80 }"
        );
    }

    #[test]
    fn client_ip_affinity_hashes_the_source_address() {
        let got = build_dnat_target(&[sock(10, 80), sock(11, 80)], true);
        assert_eq!(
            got,
            "jhash ip saddr mod 2 map { 0 : 10.244.0.10 . 80, 1 : 10.244.0.11 . 80 }"
        );
    }

    #[test]
    fn node_port_forwards_on_every_local_address() {
        let forwards = vec![Forward {
            cluster_ip: "10.96.0.40".into(),
            proto: "tcp".into(),
            port: 80,
            load_balancer: false,
            node_port: Some(31000),
            client_ip_affinity: false,
            backends: vec![sock(5, 9376)],
        }];
        let got = render_ruleset("r8s_t", &forwards, &[]);
        assert!(got.contains(
            "add rule ip r8s_t prerouting fib daddr type local ip daddr != 10.96.0.0/16 tcp dport 31000 dnat to 10.244.0.5:9376\n"
        ));
        assert!(got.contains(
            "add rule ip r8s_t output fib daddr type local ip daddr != 10.96.0.0/16 tcp dport 31000 dnat to 10.244.0.5:9376\n"
        ));
    }

    #[test]
    fn ruleset_flushes_then_refills_both_chains() {
        let forwards = vec![
            Forward {
                cluster_ip: "10.96.0.10".into(),
                proto: "tcp".into(),
                port: 443,
                load_balancer: false,
                node_port: None,
                client_ip_affinity: false,
                backends: vec![sock(5, 6443)],
            },
            Forward {
                cluster_ip: "10.96.0.20".into(),
                proto: "udp".into(),
                port: 53,
                load_balancer: true,
                node_port: None,
                client_ip_affinity: false,
                backends: vec![sock(6, 5353), sock(7, 5353)],
            },
        ];
        let got = render_ruleset("r8s_t", &forwards, &[]);
        let expected = "\
flush chain ip r8s_t prerouting
flush chain ip r8s_t output
add rule ip r8s_t prerouting ip daddr 10.96.0.10 tcp dport 443 dnat to 10.244.0.5:6443
add rule ip r8s_t output ip daddr 10.96.0.10 tcp dport 443 dnat to 10.244.0.5:6443
add rule ip r8s_t prerouting ip daddr 10.96.0.20 udp dport 53 dnat to numgen inc mod 2 map { 0 : 10.244.0.6 . 5353, 1 : 10.244.0.7 . 5353 }
add rule ip r8s_t output ip daddr 10.96.0.20 udp dport 53 dnat to numgen inc mod 2 map { 0 : 10.244.0.6 . 5353, 1 : 10.244.0.7 . 5353 }
add rule ip r8s_t prerouting iifname != r8s0 udp dport 53 dnat to numgen inc mod 2 map { 0 : 10.244.0.6 . 5353, 1 : 10.244.0.7 . 5353 }
add rule ip r8s_t output udp dport 53 dnat to numgen inc mod 2 map { 0 : 10.244.0.6 . 5353, 1 : 10.244.0.7 . 5353 }
";
        assert_eq!(got, expected.replace("r8s0", BRIDGE_NAME));
    }

    #[test]
    fn empty_ruleset_only_flushes() {
        assert_eq!(
            render_ruleset("r8s_t", &[], &[]),
            "flush chain ip r8s_t prerouting\nflush chain ip r8s_t output\n"
        );
    }

    #[test]
    fn host_ports_dnat_on_both_chains() {
        let host_ports = vec![
            HostPortForward {
                host_ip: Some(Ipv4Addr::new(127, 0, 0, 1)),
                proto: "tcp".into(),
                host_port: 54321,
                backend: sock(5, 8080),
            },
            HostPortForward {
                host_ip: None,
                proto: "udp".into(),
                host_port: 54321,
                backend: sock(6, 8080),
            },
        ];
        let got = render_ruleset("r8s_t", &[], &host_ports);
        let expected = "\
flush chain ip r8s_t prerouting
flush chain ip r8s_t output
add rule ip r8s_t prerouting ip daddr 127.0.0.1 tcp dport 54321 dnat to 10.244.0.5:8080
add rule ip r8s_t output ip daddr 127.0.0.1 tcp dport 54321 dnat to 10.244.0.5:8080
add rule ip r8s_t prerouting fib daddr type local ip daddr != 10.96.0.0/16 udp dport 54321 dnat to 10.244.0.6:8080
add rule ip r8s_t output fib daddr type local ip daddr != 10.96.0.0/16 udp dport 54321 dnat to 10.244.0.6:8080
";
        assert_eq!(got, expected);
    }

    #[test]
    fn host_ports_come_from_running_pods_with_an_address() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        let pods = GroupVersionResource::pods();
        let pod = |name: &str, host_ip: &str, proto: &str, status: serde_json::Value| {
            json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "i", "ports": [
                    {"containerPort": 8080, "hostPort": 54321, "hostIP": host_ip, "protocol": proto}
                ]}]},
                "status": status,
            })
        };
        seed(
            &store,
            &pods,
            "tcp-local",
            pod(
                "tcp-local",
                "127.0.0.1",
                "TCP",
                json!({"phase": "Running", "podIP": "10.244.0.5"}),
            ),
        );
        seed(
            &store,
            &pods,
            "udp-any",
            pod(
                "udp-any",
                "",
                "UDP",
                json!({"phase": "Running", "podIP": "10.244.0.6"}),
            ),
        );
        seed(
            &store,
            &pods,
            "pending",
            pod("pending", "", "TCP", json!({"phase": "Pending"})),
        );
        seed(
            &store,
            &pods,
            "done",
            pod(
                "done",
                "",
                "TCP",
                json!({"phase": "Succeeded", "podIP": "10.244.0.7"}),
            ),
        );

        let mut got = collect_host_ports(&store);
        got.sort_by_key(|h| h.backend);
        assert_eq!(
            got,
            vec![
                HostPortForward {
                    host_ip: Some(Ipv4Addr::new(127, 0, 0, 1)),
                    proto: "tcp".into(),
                    host_port: 54321,
                    backend: sock(5, 8080),
                },
                HostPortForward {
                    host_ip: None,
                    proto: "udp".into(),
                    host_port: 54321,
                    backend: sock(6, 8080),
                },
            ]
        );
    }

    fn seed(store: &Store, gvr: &GroupVersionResource, name: &str, obj: serde_json::Value) {
        store
            .create(
                ResourceRef {
                    gvr,
                    namespace: Some("default"),
                    name,
                },
                &obj,
            )
            .unwrap();
    }

    #[test]
    fn named_target_port_uses_resolved_endpoint_port() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        seed(
            &store,
            &GroupVersionResource::services(),
            "metrics",
            json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "metrics", "namespace": "default"},
                "spec": {
                    "clusterIP": "10.96.0.30",
                    "ports": [
                        {"name": "https", "port": 443, "targetPort": "https"},
                        {"name": "admin", "port": 9000, "targetPort": 9001}
                    ]
                }
            }),
        );
        // Endpoints as the endpoints controller writes them: names resolved.
        seed(
            &store,
            &GroupVersionResource::endpoints(),
            "metrics",
            json!({
                "apiVersion": "v1", "kind": "Endpoints",
                "metadata": {"name": "metrics", "namespace": "default"},
                "subsets": [{
                    "addresses": [{"ip": "10.244.0.9"}],
                    "ports": [
                        {"name": "https", "port": 10250, "protocol": "TCP"},
                        {"name": "admin", "port": 9001, "protocol": "TCP"}
                    ]
                }]
            }),
        );
        let services: Vec<Service> = store
            .list_as(&GroupVersionResource::services(), None)
            .unwrap();
        let forwards = collect_forwards(&store, &services);
        assert_eq!(forwards.len(), 2);
        assert_eq!(forwards[0].port, 443);
        assert_eq!(forwards[0].backends, vec![sock(9, 10250)]);
        assert_eq!(forwards[1].port, 9000);
        assert_eq!(forwards[1].backends, vec![sock(9, 9001)]);
    }

    #[test]
    fn port_without_endpoint_port_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        seed(
            &store,
            &GroupVersionResource::services(),
            "web",
            json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "clusterIP": "10.96.0.31",
                    "ports": [{"name": "http", "port": 80}, {"name": "new", "port": 81}]
                }
            }),
        );
        seed(
            &store,
            &GroupVersionResource::endpoints(),
            "web",
            json!({
                "apiVersion": "v1", "kind": "Endpoints",
                "metadata": {"name": "web", "namespace": "default"},
                "subsets": [{
                    "addresses": [{"ip": "10.244.0.2"}],
                    "ports": [{"name": "http", "port": 8080}]
                }]
            }),
        );
        let services: Vec<Service> = store
            .list_as(&GroupVersionResource::services(), None)
            .unwrap();
        let forwards = collect_forwards(&store, &services);
        assert_eq!(forwards.len(), 1);
        assert_eq!(forwards[0].port, 80);
        assert_eq!(forwards[0].backends, vec![sock(2, 8080)]);
    }
}
