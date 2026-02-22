use std::process::Command;

use r8s_store::Store;
use r8s_types::GroupVersionResource;

fn services_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "services")
}

fn endpoints_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "endpoints")
}

/// Create the r8s nftables table with NAT chains.
/// Idempotent — safe to call if the table already exists.
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

    for svc in &services {
        let svc_name = match svc["metadata"]["name"].as_str() {
            Some(n) => n,
            None => continue,
        };
        let svc_ns = svc["metadata"]["namespace"].as_str();
        let cluster_ip = match svc["spec"]["clusterIP"].as_str() {
            Some(ip) if ip != "None" && !ip.is_empty() => ip,
            _ => continue,
        };

        let ports = match svc["spec"]["ports"].as_array() {
            Some(p) => p,
            None => continue,
        };

        // Find endpoints for this service
        let ep_gvr = endpoints_gvr();
        let ep_ref = r8s_store::backend::ResourceRef {
            gvr: &ep_gvr,
            namespace: svc_ns,
            name: svc_name,
        };
        let endpoints = store.get(&ep_ref).ok().flatten();

        // Get first ready address from endpoints
        let pod_ip = endpoints.as_ref().and_then(|ep| {
            ep["subsets"]
                .as_array()?
                .iter()
                .flat_map(|s| s["addresses"].as_array().into_iter().flatten())
                .find_map(|a| a["ip"].as_str())
        });

        let pod_ip = match pod_ip {
            Some(ip) => ip,
            None => continue, // No ready endpoints
        };

        for port in ports {
            let svc_port = match port["port"].as_u64() {
                Some(p) => p,
                None => continue,
            };
            let target_port = port["targetPort"].as_u64().unwrap_or(svc_port);
            let proto = port["protocol"].as_str().unwrap_or("TCP").to_lowercase();

            let dnat_target = format!("{pod_ip}:{target_port}");
            let svc_port_str = svc_port.to_string();

            // DNAT in prerouting (for traffic from other pods)
            let _ = nft(&[
                "add",
                "rule",
                "ip",
                "r8s",
                "prerouting",
                "ip",
                "daddr",
                cluster_ip,
                &proto,
                "dport",
                &svc_port_str,
                "dnat",
                "to",
                &dnat_target,
            ]);

            // DNAT in output (for traffic from the host itself)
            let _ = nft(&[
                "add",
                "rule",
                "ip",
                "r8s",
                "output",
                "ip",
                "daddr",
                cluster_ip,
                &proto,
                "dport",
                &svc_port_str,
                "dnat",
                "to",
                &dnat_target,
            ]);
            // LoadBalancer: DNAT external traffic (non-bridge) on the service port to the pod
            let svc_type = svc["spec"]["type"].as_str().unwrap_or("ClusterIP");
            if svc_type == "LoadBalancer" {
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
