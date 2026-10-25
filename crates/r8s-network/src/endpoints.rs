use std::net::Ipv4Addr;

use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_types::{Endpoints, GroupVersionResource};

/// One ready endpoint address with the metadata the three consumers need:
/// DNS filters by `hostname`, the ingress proxy pairs `ip` with `port`, and the
/// service proxy takes `ip` alone.
pub(crate) struct ReadyAddr {
    pub ip: Ipv4Addr,
    pub hostname: Option<String>,
    /// The address's subset's first declared port, if any.
    pub port: Option<u16>,
}

/// Ready addresses for a Service's Endpoints, with IPs validated once at the
/// single read site. A malformed address is dropped rather than propagated as a
/// bogus DNS answer or nft rule.
pub(crate) fn ready_endpoint_addresses(
    store: &Store,
    namespace: Option<&str>,
    service: &str,
) -> Vec<ReadyAddr> {
    let gvr = GroupVersionResource::endpoints();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: service,
    };
    let Ok(Some(ep)) = store.get_as::<Endpoints>(&rref) else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for subset in ep.subsets.as_deref().unwrap_or_default() {
        let port = subset
            .ports
            .as_deref()
            .unwrap_or_default()
            .first()
            .map(|p| p.port as u16);
        for addr in subset.addresses.as_deref().unwrap_or_default() {
            if let Ok(ip) = addr.ip.parse::<Ipv4Addr>() {
                out.push(ReadyAddr {
                    ip,
                    hostname: addr.hostname.clone(),
                    port,
                });
            }
        }
    }
    out
}
