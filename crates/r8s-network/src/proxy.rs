use std::process::Command;

use r8s_store::Store;
use r8s_types::{Endpoints, GroupVersionResource, IntOrString, Service};

fn services_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "services")
}

fn endpoints_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "endpoints")
}

/// Create the r8s nftables table with NAT chains.
/// Idempotent -- safe to call if the table already exists.
pub fn setup_nat_table() -> anyhow::Result<()> {
    // Delete existing table first (ignore errors if it doesn't exist)
    let _ = nft(&["delete", "table", "ip", "r8s"]);

    nft(&["add", "table", "ip", "r8s"])?;
    nft(&[
        "add",
        "chain",
        "ip",
        "r8s",
        "prerouting",
        "{ type nat hook prerouting priority -100 ; }",
    ])?;
    nft(&[
        "add",
        "chain",
        "ip",
        "r8s",
        "output",
        "{ type nat hook output priority -100 ; }",
    ])?;
    nft(&[
        "add",
        "chain",
        "ip",
        "r8s",
        "postrouting",
        "{ type nat hook postrouting priority 100 ; }",
    ])?;
    // Masquerade outbound pod traffic for internet access
    nft(&[
        "add",
        "rule",
        "ip",
        "r8s",
        "postrouting",
        "ip",
        "saddr",
        "10.244.0.0/24",
        "masquerade",
    ])?;

    tracing::info!("nftables NAT table ready");
    Ok(())
}

/// Sync DNAT rules for all Services. Called periodically.
pub fn sync_service_rules(store: &Store) -> anyhow::Result<()> {
    // Flush DNAT chains (postrouting masquerade is not affected)
    let _ = nft(&["flush", "chain", "ip", "r8s", "prerouting"]);
    let _ = nft(&["flush", "chain", "ip", "r8s", "output"]);

    let services = store
        .list(&services_gvr(), None, None, None, None, None)
        .map(|r| r.items)
        .unwrap_or_default();

    for svc_value in &services {
        let svc: Service = match serde_json::from_value(svc_value.clone()) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let svc_name = match svc.metadata.name.as_deref() {
            Some(n) => n,
            None => continue,
        };
        let svc_ns = svc.metadata.namespace.as_deref();
        let cluster_ip = match svc.spec.as_ref().and_then(|s| s.cluster_ip.as_deref()) {
            Some(ip) if ip != "None" && !ip.is_empty() => ip,
            _ => continue,
        };

        let spec = match svc.spec.as_ref() {
            Some(s) => s,
            None => continue,
        };

        let ports = spec.ports.as_deref().unwrap_or_default();
        if ports.is_empty() {
            continue;
        }

        // Find endpoints for this service
        let ep_gvr = endpoints_gvr();
        let ep_ref = r8s_store::backend::ResourceRef {
            gvr: &ep_gvr,
            namespace: svc_ns,
            name: svc_name,
        };
        let endpoints: Option<Endpoints> = store
            .get(&ep_ref)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok());

        // Get all ready addresses from endpoints
        let pod_ips: Vec<&str> = endpoints
            .as_ref()
            .map(|ep| {
                ep.subsets
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .flat_map(|s| s.addresses.as_deref().unwrap_or_default().iter())
                    .map(|a| a.ip.as_str())
                    .collect()
            })
            .unwrap_or_default();

        if pod_ips.is_empty() {
            continue;
        }

        for port in ports {
            let svc_port = port.port as u64;
            let target_port = match &port.target_port {
                Some(IntOrString::Int(p)) => *p as u64,
                _ => svc_port,
            };
            let proto = port
                .protocol
                .as_deref()
                .unwrap_or("TCP")
                .to_lowercase();
            let svc_port_str = svc_port.to_string();

            // Build dnat target: single IP or nftables numgen round-robin
            let dnat_target = if pod_ips.len() == 1 {
                format!("{}:{target_port}", pod_ips[0])
            } else {
                // numgen round-robin across all endpoints
                let addrs: Vec<String> = pod_ips
                    .iter()
                    .map(|ip| format!("{ip}:{target_port}"))
                    .collect();
                format!(
                    "numgen inc mod {} map {{ {} }}",
                    pod_ips.len(),
                    addrs
                        .iter()
                        .enumerate()
                        .map(|(i, a)| format!("{i} : {a}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };

            // DNAT in prerouting (for traffic from other pods)
            let _ = nft(&[
                "add", "rule", "ip", "r8s", "prerouting",
                "ip", "daddr", cluster_ip,
                &proto, "dport", &svc_port_str,
                "dnat", "to", &dnat_target,
            ]);

            // DNAT in output (for traffic from the host itself)
            let _ = nft(&[
                "add", "rule", "ip", "r8s", "output",
                "ip", "daddr", cluster_ip,
                &proto, "dport", &svc_port_str,
                "dnat", "to", &dnat_target,
            ]);

            // LoadBalancer: DNAT external traffic (non-bridge) on the service port to the pod
            if spec.type_.as_deref() == Some("LoadBalancer") {
                let _ = nft(&[
                    "add", "rule", "ip", "r8s", "prerouting",
                    "iifname", "!=", "r8s0",
                    &proto, "dport", &svc_port_str,
                    "dnat", "to", &dnat_target,
                ]);
            }
        }
    }

    Ok(())
}

/// Clean up nftables table on shutdown.
pub fn cleanup() {
    let _ = nft(&["delete", "table", "ip", "r8s"]);
    tracing::info!("nftables table removed");
}

fn nft(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("nft").args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("nft {}: {stderr}", args.join(" "));
    }
    Ok(())
}
