//! Cluster network constants shared across r8s crates.

use std::net::Ipv4Addr;

/// The node's InternalIP: the r8s bridge gateway, owned by the host. This
/// single-node distribution runs the node, the bridge gateway, and every
/// in-cluster service on one machine, so this address is also the kubelet and
/// DNS bind address, the pod default route and `nameserver`, the downward-API
/// `status.hostIP`, and an apiserver certificate SAN.
pub const NODE_INTERNAL_IP: &str = "10.244.0.1";

/// Port the kubelet and its metrics server listen on, advertised via the node's
/// `daemonEndpoints`.
pub const KUBELET_PORT: u16 = 10250;

/// Linux bridge every pod veth attaches to.
pub const BRIDGE_NAME: &str = "r8s0";

/// Pod network. A single `/24` on one node — 254 usable hosts, gateway at
/// [`NODE_INTERNAL_IP`]. Raising the node count or this prefix would mean
/// rethinking IP allocation in the kubelet.
pub const POD_CIDR: &str = "10.244.0.0/24";
pub const POD_PREFIX: u8 = 24;

/// ClusterIP service range. Traffic to these addresses is DNAT'd to backing
/// pods by the nftables rules in `r8s-network::proxy`.
pub const SERVICE_CIDR: &str = "10.96.0.0/16";

/// Pod host range within [`POD_CIDR`]: skips `.0` (network), `.1` (gateway),
/// and `.255` (broadcast).
pub const IP_POOL_MIN: u32 = 2;
pub const IP_POOL_MAX: u32 = 254;

/// Pod IP for host number `host` within [`POD_CIDR`].
pub const fn pod_ip(host: u32) -> Ipv4Addr {
    Ipv4Addr::new(10, 244, 0, host as u8)
}

/// Inverse of [`pod_ip`]: the pool host number for `ip`, or `None` if it falls
/// outside the pod `/24` or the usable host range. Lets the kubelet recover an
/// allocation from a pod's `status.podIP` after a restart without hard-coding
/// the prefix itself.
pub fn pod_ip_host(ip: Ipv4Addr) -> Option<u32> {
    let [a, b, c, host] = ip.octets();
    if (a, b, c) != (10, 244, 0) {
        return None;
    }
    let host = host as u32;
    (IP_POOL_MIN..=IP_POOL_MAX).contains(&host).then_some(host)
}

/// nft identifier rules are `[A-Za-z][A-Za-z0-9_]*`; data-dir basenames (the
/// cluster name) can contain dashes/dots. Return a per-cluster table name that
/// always parses. Shared by `r8s-network::proxy` and the `r8s` CLI so the two
/// agree on which table to create and tear down.
pub fn nft_table_name(cluster: &str) -> String {
    let sanitized: String = cluster
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("r8s_{sanitized}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_ip_maps_host_into_pod_cidr() {
        assert_eq!(pod_ip(IP_POOL_MIN), Ipv4Addr::new(10, 244, 0, 2));
        assert_eq!(pod_ip(IP_POOL_MAX), Ipv4Addr::new(10, 244, 0, 254));
        assert_eq!(pod_ip(42), Ipv4Addr::new(10, 244, 0, 42));
    }

    #[test]
    fn pod_ip_host_inverts_pod_ip() {
        for host in [IP_POOL_MIN, 42, IP_POOL_MAX] {
            assert_eq!(pod_ip_host(pod_ip(host)), Some(host));
        }
    }

    #[test]
    fn pod_ip_host_rejects_out_of_range() {
        assert_eq!(pod_ip_host("10.244.0.1".parse().unwrap()), None); // gateway
        assert_eq!(pod_ip_host("10.244.0.255".parse().unwrap()), None); // broadcast
        assert_eq!(pod_ip_host("10.96.0.5".parse().unwrap()), None); // service CIDR
        assert_eq!(pod_ip_host("127.0.0.1".parse().unwrap()), None); // host
    }

    #[test]
    fn nft_table_name_sanitizes() {
        assert_eq!(nft_table_name("default"), "r8s_default");
        assert_eq!(nft_table_name("my-cluster.1"), "r8s_my_cluster_1");
        assert_eq!(nft_table_name(""), "r8s_");
    }
}
