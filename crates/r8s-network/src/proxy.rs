use std::net::Ipv4Addr;
use std::process::Command;

use r8s_store::Store;
use r8s_types::{BRIDGE_NAME, GroupVersionResource, IntOrString, POD_CIDR, Service, nft_table_name};

use crate::endpoints::ready_endpoint_addresses;
use crate::error::{NetworkError, Result};

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

    tracing::info!(table, "nftables NAT table ready");
    Ok(())
}

pub fn sync_service_rules(store: &Store, cluster: &str) -> Result<()> {
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
                    BRIDGE_NAME,
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

fn endpoint_ips(store: &Store, namespace: Option<&str>, service_name: &str) -> Vec<Ipv4Addr> {
    ready_endpoint_addresses(store, namespace, service_name)
        .into_iter()
        .map(|a| a.ip)
        .collect()
}

fn build_dnat_target(pod_ips: &[Ipv4Addr], target_port: u64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(n: u8) -> Ipv4Addr {
        Ipv4Addr::new(10, 244, 0, n)
    }

    #[test]
    fn single_backend_is_a_plain_dnat_target() {
        assert_eq!(build_dnat_target(&[ip(10)], 8080), "10.244.0.10:8080");
    }

    #[test]
    fn multiple_backends_load_balance_via_numgen_map() {
        let got = build_dnat_target(&[ip(10), ip(11), ip(12)], 80);
        assert_eq!(
            got,
            "numgen inc mod 3 map { 0 : 10.244.0.10:80, 1 : 10.244.0.11:80, 2 : 10.244.0.12:80 }"
        );
    }
}
