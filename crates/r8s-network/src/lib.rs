//! Pod networking, service proxy, and embedded DNS.
//!
//! - `bridge`: bridge + veth pair setup for pod networking (shells out to `ip`/`nsenter`)
//! - `proxy`: nftables DNAT rules for Service ClusterIP (shells out to `nft`)
//! - `dns`: embedded UDP DNS server for cluster service discovery

pub mod bridge;
pub mod dns;
pub mod proxy;
