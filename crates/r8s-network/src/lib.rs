//! Pod networking, service proxy, and embedded DNS.
//!
//! - `cni`: bridge + veth pair setup for pod networking (via rtnetlink)
//! - `proxy`: nftables DNAT rules for Service ClusterIP/NodePort
//! - `dns`: embedded hickory-dns server for cluster service discovery
//! - `allocator`: IP and port allocation from configured CIDRs
