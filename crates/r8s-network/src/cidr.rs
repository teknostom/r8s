use std::net::Ipv4Addr;

// /16 gives us 65k+ addresses. Should be well enough for any dev workload, and it's what standard
// k8s gives. Can be bumped later if someone runs out for some ungodly reason
// TODO: think about if Ipv6 would be usable. Might break stuff that expects v4.
pub const POD_CIDR: &str = "10.244.0.0/16";

pub const POD_PREFIX: u8 = 16;

pub const BRIDGE_IP: Ipv4Addr = Ipv4Addr::new(10, 244, 0, 1);

// Skips network, bridge, and broadcast.
pub const IP_POOL_MIN: u32 = 2;
pub const IP_POOL_MAX: u32 = 65534;

pub const fn pod_ip(host: u32) -> Ipv4Addr {
    Ipv4Addr::new(10, 244, (host >> 8) as u8, host as u8)
}
