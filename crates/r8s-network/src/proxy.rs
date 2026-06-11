use std::process::Command;

use r8s_store::Store;
use r8s_types::{Endpoints, GroupVersionResource, IntOrString, Service};

/// nft identifier rules are `[A-Za-z][A-Za-z0-9_]*`; data-dir basenames can
/// contain dashes/dots. Return a per-cluster table name that always parses.
pub fn nft_table_name(cluster: &str) -> String {
    let sanitized: String = cluster
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("r8s_{sanitized}")
}

pub fn setup_nat_table(cluster: &str) -> anyhow::Result<()> {
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
        "10.244.0.0/24",
        "oifname",
        "!=",
        "r8s0",
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
        "10.244.0.0/24",
        "ip",
        "daddr",
        "10.244.0.0/24",
        "oifname",
        "r8s0",
        "masquerade",
    ])?;

    tracing::info!(table, "nftables NAT table ready");
    Ok(())
}

pub fn sync_service_rules(store: &Store, cluster: &str) -> anyhow::Result<()> {
    let table = nft_table_name(cluster);
    // Flush only DNAT chains -- postrouting masquerade rules live in a separate chain
    let _ = nft(&["flush", "chain", "ip", &table, "prerouting"]);
    let _ = nft(&["flush", "chain", "ip", &table, "output"]);

    let services: Vec<Service> = store
        .list_as::<Service>(&GroupVersionResource::services(), None)
        .unwrap_or_default();

    for svc in &services {
        let svc_name = match svc.metadata.name.as_deref() {
            Some(n) => n,
            None => continue,
        };
        let svc_ns = svc.metadata.namespace.as_deref();
        let spec = match svc.spec.as_ref() {
            Some(s) => s,
            None => continue,
        };
        let cluster_ip = match spec.cluster_ip.as_deref() {
            Some(ip) if ip != "None" && !ip.is_empty() => ip,
            _ => continue,
        };

        let ports = spec.ports.as_deref().unwrap_or_default();
        if ports.is_empty() {
            continue;
        }

        let pod_ips = endpoint_ips(store, svc_ns, svc_name);
        if pod_ips.is_empty() {
            continue;
        }

        let is_lb = spec.type_.as_deref() == Some("LoadBalancer");

        for port in ports {
            let svc_port = port.port as u64;
            let target_port = match &port.target_port {
                Some(IntOrString::Int(p)) => *p as u64,
                _ => svc_port,
            };
            let proto = port.protocol.as_deref().unwrap_or("TCP").to_lowercase();

            let dnat_target = build_dnat_target(&pod_ips, target_port);
            let svc_port_str = svc_port.to_string();

            // Prerouting: traffic from other pods
            let _ = nft(&[
                "add",
                "rule",
                "ip",
                &table,
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

            // Output: traffic originating on the host
            let _ = nft(&[
                "add",
                "rule",
                "ip",
                &table,
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

            if is_lb {
                let _ = nft(&[
                    "add",
                    "rule",
                    "ip",
                    &table,
                    "prerouting",
                    "iifname",
                    "!=",
                    "r8s0",
                    &proto,
                    "dport",
                    &svc_port_str,
                    "dnat",
                    "to",
                    &dnat_target,
                ]);
                let _ = nft(&[
                    "add",
                    "rule",
                    "ip",
                    &table,
                    "output",
                    &proto,
                    "dport",
                    &svc_port_str,
                    "dnat",
                    "to",
                    &dnat_target,
                ]);
            }
        }
    }

    Ok(())
}

pub fn cleanup(cluster: &str) {
    let table = nft_table_name(cluster);
    let _ = nft(&["delete", "table", "ip", &table]);
    tracing::info!(table, "nftables table removed");
}

fn endpoint_ips(store: &Store, namespace: Option<&str>, service_name: &str) -> Vec<String> {
    let gvr = GroupVersionResource::endpoints();
    let ep_ref = r8s_store::backend::ResourceRef {
        gvr: &gvr,
        namespace,
        name: service_name,
    };

    let ep: Endpoints = match store.get_as::<Endpoints>(&ep_ref) {
        Ok(Some(ep)) => ep,
        _ => return Vec::new(),
    };

    ep.subsets
        .as_deref()
        .unwrap_or_default()
        .iter()
        .flat_map(|s| s.addresses.as_deref().unwrap_or_default().iter())
        .map(|a| a.ip.clone())
        .collect()
}

fn build_dnat_target(pod_ips: &[String], target_port: u64) -> String {
    if pod_ips.len() == 1 {
        return format!("{}:{target_port}", pod_ips[0]);
    }
    let addrs: Vec<String> = pod_ips
        .iter()
        .enumerate()
        .map(|(i, ip)| format!("{i} : {ip}:{target_port}"))
        .collect();
    format!(
        "numgen inc mod {} map {{ {} }}",
        pod_ips.len(),
        addrs.join(", ")
    )
}

fn nft(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("nft").args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("nft {}: {stderr}", args.join(" "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nft_table_name_sanitizes() {
        assert_eq!(nft_table_name("default"), "r8s_default");
        assert_eq!(nft_table_name("my-cluster.1"), "r8s_my_cluster_1");
        assert_eq!(nft_table_name(""), "r8s_");
    }
}
