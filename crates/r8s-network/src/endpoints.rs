use std::net::Ipv4Addr;

use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_types::{Endpoints, GroupVersionResource};

/// One ready endpoint address: DNS filters by `hostname`, the ingress proxy
/// pairs `ip` with `port()`, the service proxy pairs it with `port_for`.
pub(crate) struct ReadyAddr {
    pub ip: Ipv4Addr,
    pub hostname: Option<String>,
    /// The address's subset's ports as `(name, port)`, in declared order.
    pub ports: Vec<(Option<String>, u16)>,
}

impl ReadyAddr {
    /// First declared port.
    pub fn port(&self) -> Option<u16> {
        self.ports.first().map(|(_, p)| *p)
    }

    /// Port for the Service port at `index` named `name`: the port at the same
    /// index when its name agrees, else the first port with that name.
    pub fn port_for(&self, index: usize, name: Option<&str>) -> Option<u16> {
        self.ports
            .get(index)
            .filter(|(n, _)| n.as_deref() == name)
            .or_else(|| self.ports.iter().find(|(n, _)| n.as_deref() == name))
            .map(|(_, p)| *p)
    }
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
        let ports: Vec<(Option<String>, u16)> = subset
            .ports
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter_map(|p| {
                let port = u16::try_from(p.port).ok()?;
                Some((
                    p.name
                        .as_deref()
                        .filter(|n| !n.is_empty())
                        .map(String::from),
                    port,
                ))
            })
            .collect();
        for addr in subset.addresses.as_deref().unwrap_or_default() {
            if let Ok(ip) = addr.ip.parse::<Ipv4Addr>() {
                out.push(ReadyAddr {
                    ip,
                    hostname: addr.hostname.clone(),
                    ports: ports.clone(),
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(ports: &[(Option<&str>, u16)]) -> ReadyAddr {
        ReadyAddr {
            ip: Ipv4Addr::new(10, 244, 0, 1),
            hostname: None,
            ports: ports
                .iter()
                .map(|(n, p)| (n.map(String::from), *p))
                .collect(),
        }
    }

    #[test]
    fn port_for_prefers_index_when_name_agrees() {
        let a = addr(&[(Some("http"), 8080), (Some("metrics"), 9090)]);
        assert_eq!(a.port_for(0, Some("http")), Some(8080));
        assert_eq!(a.port_for(1, Some("metrics")), Some(9090));
    }

    #[test]
    fn port_for_falls_back_to_name_when_order_differs() {
        let a = addr(&[(Some("metrics"), 9090), (Some("http"), 8080)]);
        assert_eq!(a.port_for(0, Some("http")), Some(8080));
        assert_eq!(a.port_for(1, Some("metrics")), Some(9090));
    }

    #[test]
    fn port_for_matches_unnamed_by_index() {
        let a = addr(&[(None, 8080), (None, 9090)]);
        assert_eq!(a.port_for(0, None), Some(8080));
        assert_eq!(a.port_for(1, None), Some(9090));
        assert_eq!(a.port_for(0, Some("http")), None);
    }
}
