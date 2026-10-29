use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_store::index::LabelSelector;
use r8s_store::watch::WatchEventType;
use r8s_types::{
    EndpointAddress, EndpointConditions, EndpointPort, EndpointSlice, EndpointSubset, Endpoints,
    GroupVersionResource, ObjectMeta, ObjectReference, Pod, Service, SliceEndpoint,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const MANAGED_BY_LABEL: &str = "endpointslice.kubernetes.io/managed-by";
const SKIP_MIRROR_LABEL: &str = "endpointslice.kubernetes.io/skip-mirror";
const SLICE_CONTROLLER: &str = "endpointslice-controller.k8s.io";
const MIRRORING_CONTROLLER: &str = "endpointslicemirroring-controller.k8s.io";

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("endpoints controller started");

    reconcile_all(&store);

    let mut svc_rx = store.watch(&GroupVersionResource::services());
    let mut pod_rx = store.watch(&GroupVersionResource::pods());
    let mut ep_rx = store.watch(&GroupVersionResource::endpoints());
    let mut slice_rx = store.watch(&GroupVersionResource::endpoint_slices());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("endpoints controller shutting down");
                return Ok(());
            }
            event = svc_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        let _ = reconcile_service(&store, &event.object);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) => {
                        let pod: Pod = match serde_json::from_value(event.object.as_ref().clone()) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        reconcile_services_for_pod(&store, &pod);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            // A managed slice someone deletes comes back from its source.
            event = slice_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let meta = event.object.get("metadata");
                        let label = |key: &str| {
                            meta.and_then(|m| m.get("labels"))
                                .and_then(|l| l.get(key))
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        };
                        let (Some(service), Some(controller)) =
                            (label(SERVICE_NAME_LABEL), label(MANAGED_BY_LABEL))
                        else {
                            continue;
                        };
                        let namespace = meta
                            .and_then(|m| m.get("namespace"))
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        let result = match controller.as_str() {
                            SLICE_CONTROLLER => {
                                let gvr = GroupVersionResource::services();
                                match store.get(&ResourceRef {
                                    gvr: &gvr,
                                    namespace: namespace.as_deref(),
                                    name: &service,
                                }) {
                                    Ok(Some(svc)) => reconcile_service(&store, &svc),
                                    Ok(None) => Ok(()),
                                    Err(e) => Err(e.into()),
                                }
                            }
                            MIRRORING_CONTROLLER => {
                                mirror_service_endpoints(&store, namespace.as_deref(), &service)
                            }
                            _ => Ok(()),
                        };
                        if let Err(e) = result {
                            tracing::warn!("endpointslice restore error for '{service}': {e}");
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            // Endpoints a user manages (no Service selector) are mirrored into
            // EndpointSlices; ours carry a Service owner reference and are not.
            event = ep_rx.recv() => {
                match event {
                    Ok(event) => {
                        let meta = event.object.get("metadata");
                        let service_owned = meta
                            .and_then(|m| m.get("ownerReferences"))
                            .and_then(|v| v.as_array())
                            .is_some_and(|refs| {
                                refs.iter().any(|r| r.get("kind").and_then(|k| k.as_str()) == Some("Service"))
                            });
                        if service_owned {
                            continue;
                        }
                        let (Some(name), namespace) = (
                            meta.and_then(|m| m.get("name")).and_then(|v| v.as_str()),
                            meta.and_then(|m| m.get("namespace")).and_then(|v| v.as_str()),
                        ) else {
                            continue;
                        };
                        let result = if matches!(event.event_type, WatchEventType::Deleted) {
                            delete_mirrored_slices(&store, namespace, name)
                        } else {
                            mirror_endpoints(&store, &event.object)
                        };
                        if let Err(e) = result {
                            tracing::warn!("endpoints mirror error for '{name}': {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn reconcile_all(store: &Store) {
    let result = match store.list(
        &GroupVersionResource::services(),
        None,
        None,
        None,
        None,
        None,
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("endpoints controller list error: {e}");
            return;
        }
    };
    for svc in &result.items {
        let _ = reconcile_service(store, svc);
    }
}

fn reconcile_services_for_pod(store: &Store, pod: &Pod) {
    let pod_ns = pod.metadata.namespace.as_deref();
    let pod_labels = pod.metadata.labels.as_ref();
    let svcs = match store.list(
        &GroupVersionResource::services(),
        pod_ns,
        None,
        None,
        None,
        None,
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("endpoints controller list error: {e}");
            return;
        }
    };
    for svc_value in &svcs.items {
        let svc: Service = match serde_json::from_value(svc_value.clone()) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let selector = match svc.spec.as_ref().and_then(|s| s.selector.as_ref()) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        let matches = match pod_labels {
            Some(labels) => selector.iter().all(|(k, v)| labels.get(k) == Some(v)),
            None => false,
        };
        if matches {
            let _ = reconcile_service(store, svc_value);
        }
    }
}

/// Resolve a Service port's `targetPort` to the concrete number the pods listen
/// on: an integer is used verbatim, a name is looked up in the selected pods'
/// `containerPorts`, and an absent target defaults to the Service `port`. A
/// named target that no pod exposes falls back to the Service port so at least
/// the legacy Endpoints object stays well-formed.
fn reconcile_service(store: &Store, service_value: &serde_json::Value) -> anyhow::Result<()> {
    let svc: Service = serde_json::from_value(service_value.clone())?;
    let svc_name = svc
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Service has no name"))?;
    let svc_ns = svc.metadata.namespace.as_deref();
    let svc_uid = svc.metadata.uid.as_deref().unwrap_or("");

    let svc_spec = match svc.spec.as_ref() {
        Some(s) => s,
        None => return Ok(()),
    };

    let selector = match svc_spec.selector.as_ref() {
        Some(s) if !s.is_empty() => s,
        // A Service without a selector takes the user's own Endpoints, mirrored.
        _ => return mirror_service_endpoints(store, svc_ns, svc_name),
    };

    let pod_gvr = GroupVersionResource::pods();
    let matching: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, svc_ns)?
        .into_iter()
        .filter(|pod| match pod.metadata.labels.as_ref() {
            Some(labels) => selector.iter().all(|(k, v)| labels.get(k) == Some(v)),
            None => false,
        })
        .collect();

    // `publishNotReadyAddresses` lifts the readiness gate: clustered
    // StatefulSets set it on their governing headless Service because a peer
    // must resolve its *own* per-pod DNS record before it can finish
    // bootstrapping and turn Ready (a KRaft kafka controller dials itself as
    // a quorum voter at startup). Without this the records never appear and
    // such pods deadlock at not-Ready.
    let publish_not_ready = svc_spec.publish_not_ready_addresses.unwrap_or(false);

    // One candidate per selected pod that has an IP and is Running: its
    // address, readiness, whether it is shutting down, and the Service ports
    // it serves — a named targetPort only where the pod has that port, as
    // upstream's FindPort resolves it. Pods serving the same ports share a
    // subset and a slice.
    struct Candidate {
        address: EndpointAddress,
        ready: bool,
        terminating: bool,
        ports: Vec<ResolvedPort>,
    }
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct ResolvedPort {
        name: Option<String>,
        port: i32,
        protocol: String,
    }

    let service_ports = svc_spec.ports.as_deref().unwrap_or_default();
    let candidates: Vec<Candidate> = matching
        .iter()
        .filter_map(|pod| {
            let status = pod.status.as_ref()?;
            let pod_ip = status.pod_ip.as_deref()?;
            if status.phase.as_deref() != Some("Running") {
                return None;
            }
            let ready = status
                .conditions
                .as_ref()
                .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
                .is_some_and(|c| c.status == "True");
            let pod_name = pod.metadata.name.as_deref()?;
            let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
            // Publish the pod's hostname only when its subdomain names this
            // Service — upstream k8s's gate. This is what scopes per-pod DNS
            // records (`<hostname>.<svc>.<ns>.svc.cluster.local`) to the
            // governing headless Service instead of every Service that happens
            // to select the pod.
            let hostname = pod.spec.as_ref().and_then(|s| {
                let hostname = s.hostname.as_deref()?;
                (s.subdomain.as_deref() == Some(svc_name)).then(|| hostname.to_string())
            });
            let ports: Vec<ResolvedPort> = service_ports
                .iter()
                .filter_map(|p| {
                    let protocol = p.protocol.clone().unwrap_or_else(|| "TCP".into());
                    let port = find_port(pod, p.target_port.as_ref(), p.port, &protocol)?;
                    Some(ResolvedPort {
                        name: p
                            .name
                            .as_deref()
                            .filter(|n| !n.is_empty())
                            .map(String::from),
                        port,
                        protocol,
                    })
                })
                .collect();
            if ports.is_empty() && !service_ports.is_empty() {
                return None;
            }
            Some(Candidate {
                address: EndpointAddress {
                    ip: pod_ip.to_string(),
                    hostname,
                    node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                    target_ref: Some(ObjectReference {
                        kind: Some("Pod".into()),
                        name: Some(pod_name.into()),
                        namespace: Some(pod_ns.into()),
                        uid: pod.metadata.uid.clone(),
                        resource_version: pod.metadata.resource_version.clone(),
                        ..Default::default()
                    }),
                },
                ready,
                terminating: pod.metadata.deletion_timestamp.is_some(),
                ports,
            })
        })
        .collect();

    let mut groups: std::collections::BTreeMap<Vec<ResolvedPort>, Vec<&Candidate>> =
        std::collections::BTreeMap::new();
    for candidate in &candidates {
        groups
            .entry(candidate.ports.clone())
            .or_default()
            .push(candidate);
    }

    // A pod is published as a ready endpoint when it's Ready, or unconditionally
    // when the Service publishes not-ready addresses (headless StatefulSet
    // bootstrap). The rest go to `notReadyAddresses` so consumers and
    // `kubectl get endpoints` can still see them.
    let ready_for_traffic = |c: &Candidate| publish_not_ready || c.ready;
    let subsets: Vec<EndpointSubset> = groups
        .iter()
        .map(|(ports, members)| {
            let ready: Vec<EndpointAddress> = members
                .iter()
                .filter(|c| ready_for_traffic(c))
                .map(|c| c.address.clone())
                .collect();
            let not_ready: Vec<EndpointAddress> = members
                .iter()
                .filter(|c| !ready_for_traffic(c))
                .map(|c| c.address.clone())
                .collect();
            EndpointSubset {
                addresses: Some(ready),
                not_ready_addresses: (!not_ready.is_empty()).then_some(not_ready),
                ports: Some(
                    ports
                        .iter()
                        .map(|p| EndpointPort {
                            name: p.name.clone(),
                            port: p.port,
                            protocol: Some(p.protocol.clone()),
                            ..Default::default()
                        })
                        .collect(),
                ),
            }
        })
        .collect();

    let owner_ref = crate::owner_reference("v1", "Service", svc_name, svc_uid);

    let ep = Endpoints {
        metadata: ObjectMeta {
            name: Some(svc_name.into()),
            namespace: svc_ns.map(String::from),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        subsets: Some(subsets),
    };

    let gvr = GroupVersionResource::endpoints();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    match store.get(&rref)? {
        Some(existing) => {
            let mut ep_value = serde_json::to_value(&ep)?;
            if let Some(rv) = existing
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|v| v.as_str())
                && let Some(meta) = ep_value.get_mut("metadata").and_then(|v| v.as_object_mut())
            {
                meta.insert("resourceVersion".to_string(), serde_json::json!(rv));
            }
            let _ = store.update(&rref, &ep_value);
        }
        None => {
            let ep_value = serde_json::to_value(&ep)?;
            let _ = store.create(rref, &ep_value);
        }
    }

    // One EndpointSlice per subset, the first named after the Service;
    // without pods a placeholder with neither endpoints nor ports.
    let slice_meta = |index: usize| ObjectMeta {
        name: Some(slice_name(svc_name, index)),
        namespace: svc_ns.map(String::from),
        labels: Some(std::collections::BTreeMap::from([
            (SERVICE_NAME_LABEL.into(), svc_name.into()),
            (MANAGED_BY_LABEL.into(), SLICE_CONTROLLER.into()),
        ])),
        owner_references: Some(vec![owner_ref.clone()]),
        ..Default::default()
    };
    let mut slices: Vec<EndpointSlice> = groups
        .iter()
        .enumerate()
        .map(|(index, (ports, members))| EndpointSlice {
            metadata: slice_meta(index),
            address_type: "IPv4".into(),
            // Every candidate — ready or not — becomes a slice endpoint carrying
            // its own conditions, so consumers can distinguish serving from
            // draining.
            endpoints: members
                .iter()
                .map(|c| SliceEndpoint {
                    addresses: vec![c.address.ip.clone()],
                    conditions: Some(EndpointConditions {
                        // Ready drives load-balancing: serving and not shutting
                        // down, or forced by publishNotReadyAddresses.
                        ready: Some(publish_not_ready || (c.ready && !c.terminating)),
                        // Serving tracks true readiness even while terminating,
                        // so graceful-shutdown proxies keep draining in-flight
                        // work.
                        serving: Some(c.ready),
                        terminating: Some(c.terminating),
                    }),
                    // The backing-pod reference: consumers (cert-manager's
                    // startup API check) resolve a Service's named port by
                    // walking slice endpoints and skip any without a targetRef.
                    target_ref: c.address.target_ref.clone(),
                    hostname: c.address.hostname.clone(),
                    node_name: c.address.node_name.clone(),
                    ..Default::default()
                })
                .collect(),
            ports: Some(
                ports
                    .iter()
                    .map(|p| k8s_openapi::api::discovery::v1::EndpointPort {
                        // Always set, "" when unnamed, as upstream writes it.
                        name: Some(p.name.clone().unwrap_or_default()),
                        port: Some(p.port),
                        protocol: Some(p.protocol.clone()),
                        ..Default::default()
                    })
                    .collect(),
            ),
        })
        .collect();
    if slices.is_empty() {
        slices.push(EndpointSlice {
            metadata: slice_meta(0),
            address_type: "IPv4".into(),
            endpoints: Vec::new(),
            ports: Some(Vec::new()),
        });
    }
    let slices_gvr = GroupVersionResource::endpoint_slices();
    for stale in managed_slices(store, svc_ns, svc_name, SLICE_CONTROLLER)?
        .into_iter()
        .filter(|existing| {
            !slices
                .iter()
                .any(|s| s.metadata.name.as_deref() == Some(existing))
        })
    {
        let _ = store.delete(&ResourceRef {
            gvr: &slices_gvr,
            namespace: svc_ns,
            name: &stale,
        });
    }
    for slice in &slices {
        write_slice(store, slice)?;
    }
    Ok(())
}

/// The slice of a Service's `index`th port set: the Service's name, then
/// `<name>-<index>`.
fn slice_name(service: &str, index: usize) -> String {
    if index == 0 {
        service.to_string()
    } else {
        format!("{service}-{index}")
    }
}

/// Upstream's FindPort: a numeric targetPort as it is, a named one as the
/// pod's container port of that name and protocol, `None` when the pod has
/// no such port.
fn find_port(
    pod: &Pod,
    target_port: Option<&IntOrString>,
    service_port: i32,
    protocol: &str,
) -> Option<i32> {
    match target_port {
        Some(IntOrString::Int(i)) => Some(*i),
        Some(IntOrString::String(name)) => pod
            .spec
            .as_ref()?
            .containers
            .iter()
            .filter_map(|c| c.ports.as_ref())
            .flatten()
            .find(|p| {
                p.name.as_deref() == Some(name)
                    && p.protocol.as_deref().unwrap_or("TCP") == protocol
            })
            .map(|p| p.container_port),
        None => Some(service_port),
    }
}

/// Creates or updates an EndpointSlice.
fn write_slice(store: &Store, slice: &EndpointSlice) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::endpoint_slices();
    let name = slice.metadata.name.as_deref().unwrap_or_default();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: slice.metadata.namespace.as_deref(),
        name,
    };
    let mut value = serde_json::to_value(slice)?;
    match store.get(&rref)? {
        Some(existing) => {
            if let Some(rv) = existing
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|v| v.as_str())
                && let Some(meta) = value.get_mut("metadata").and_then(|v| v.as_object_mut())
            {
                meta.insert("resourceVersion".to_string(), serde_json::json!(rv));
            }
            let _ = store.update(&rref, &value);
        }
        None => {
            let _ = store.create(rref, &value);
        }
    }
    Ok(())
}

/// Mirrors the Endpoints of a selector-less Service, if any.
fn mirror_service_endpoints(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::endpoints();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    match store.get(&rref)? {
        Some(endpoints) => mirror_endpoints(store, &endpoints),
        None => delete_mirrored_slices(store, namespace, name),
    }
}

/// Mirrors an Endpoints object a user manages into EndpointSlices, one per
/// subset, as upstream's mirroring controller does: the slices carry the
/// Endpoints' labels and annotations, the mirroring controller's managed-by
/// label and an owner reference to the Endpoints. Nothing is mirrored (and
/// earlier mirrors go) when the Service is missing, has a selector, or the
/// Endpoints carry the skip-mirror label.
fn mirror_endpoints(store: &Store, endpoints_value: &serde_json::Value) -> anyhow::Result<()> {
    let ep: Endpoints = serde_json::from_value(endpoints_value.clone())?;
    let name = ep
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Endpoints has no name"))?;
    let namespace = ep.metadata.namespace.as_deref();
    let skip = ep
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(SKIP_MIRROR_LABEL))
        .is_some_and(|v| v == "true");
    let svc_gvr = GroupVersionResource::services();
    let service: Option<Service> = store
        .get(&ResourceRef {
            gvr: &svc_gvr,
            namespace,
            name,
        })?
        .and_then(|v| serde_json::from_value(v).ok());
    let has_selector = service
        .as_ref()
        .and_then(|s| s.spec.as_ref())
        .and_then(|s| s.selector.as_ref())
        .is_some_and(|s| !s.is_empty());
    if skip || service.is_none() || has_selector {
        return delete_mirrored_slices(store, namespace, name);
    }

    let owner = crate::owner_reference(
        "v1",
        "Endpoints",
        name,
        ep.metadata.uid.as_deref().unwrap_or_default(),
    );
    let mut labels = ep.metadata.labels.clone().unwrap_or_default();
    labels.insert(SERVICE_NAME_LABEL.into(), name.into());
    labels.insert(MANAGED_BY_LABEL.into(), MIRRORING_CONTROLLER.into());
    let endpoint = |address: &EndpointAddress, ready: bool| SliceEndpoint {
        addresses: vec![address.ip.clone()],
        conditions: Some(EndpointConditions {
            ready: Some(ready),
            serving: Some(ready),
            terminating: Some(false),
        }),
        hostname: address.hostname.clone(),
        node_name: address.node_name.clone(),
        target_ref: address.target_ref.clone(),
        ..Default::default()
    };
    let is_ipv4 = |address: &&EndpointAddress| !address.ip.contains(':');
    let desired: Vec<EndpointSlice> = ep
        .subsets
        .iter()
        .flatten()
        .enumerate()
        .map(|(index, subset)| EndpointSlice {
            metadata: ObjectMeta {
                name: Some(format!("{name}-{index}")),
                namespace: namespace.map(String::from),
                labels: Some(labels.clone()),
                annotations: ep.metadata.annotations.clone(),
                owner_references: Some(vec![owner.clone()]),
                ..Default::default()
            },
            address_type: "IPv4".into(),
            endpoints: subset
                .addresses
                .iter()
                .flatten()
                .filter(is_ipv4)
                .map(|a| endpoint(a, true))
                .chain(
                    subset
                        .not_ready_addresses
                        .iter()
                        .flatten()
                        .filter(is_ipv4)
                        .map(|a| endpoint(a, false)),
                )
                .collect(),
            ports: Some(
                subset
                    .ports
                    .iter()
                    .flatten()
                    .map(|p| k8s_openapi::api::discovery::v1::EndpointPort {
                        // Always set, "" when unnamed, as upstream writes it.
                        name: Some(p.name.clone().unwrap_or_default()),
                        port: Some(p.port),
                        protocol: Some(p.protocol.clone().unwrap_or_else(|| "TCP".into())),
                        app_protocol: p.app_protocol.clone(),
                    })
                    .collect(),
            ),
        })
        .collect();

    let gvr = GroupVersionResource::endpoint_slices();
    for stale in mirrored_slices(store, namespace, name)?
        .into_iter()
        .filter(|existing| {
            !desired
                .iter()
                .any(|d| d.metadata.name.as_deref() == Some(existing))
        })
    {
        let _ = store.delete(&ResourceRef {
            gvr: &gvr,
            namespace,
            name: &stale,
        });
    }
    for slice in &desired {
        write_slice(store, slice)?;
    }
    Ok(())
}

/// The names of the slices mirrored from the Endpoints `name`.
fn mirrored_slices(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<Vec<String>> {
    managed_slices(store, namespace, name, MIRRORING_CONTROLLER)
}

/// The names of the slices `controller` manages for the Service `name`.
fn managed_slices(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    controller: &str,
) -> anyhow::Result<Vec<String>> {
    let selector = LabelSelector::parse(&format!(
        "{SERVICE_NAME_LABEL}={name},{MANAGED_BY_LABEL}={controller}"
    ))?;
    let result = store.list(
        &GroupVersionResource::endpoint_slices(),
        namespace,
        Some(&selector),
        None,
        None,
        None,
    )?;
    Ok(result
        .items
        .iter()
        .filter_map(|s| s.get("metadata")?.get("name")?.as_str().map(String::from))
        .collect())
}

fn delete_mirrored_slices(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::endpoint_slices();
    for slice in mirrored_slices(store, namespace, name)? {
        let _ = store.delete(&ResourceRef {
            gvr: &gvr,
            namespace,
            name: &slice,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_slices_carry_pod_uids_and_the_controller_label() {
        let (_d, store) = temp_store();
        let pods = GroupVersionResource::pods();
        store
            .create(
                ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "web-0",
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "web-0", "namespace": "default", "uid": "pod-uid-1",
                                 "labels": {"app": "web"}},
                    "spec": {"nodeName": "r8s-node",
                             "containers": [{"name": "web", "image": "web",
                                             "ports": [{"containerPort": 8080}]}]},
                    "status": {"phase": "Running", "podIP": "10.244.0.7",
                               "conditions": [{"type": "Ready", "status": "True"}]}
                }),
            )
            .unwrap();
        let svc = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "web", "namespace": "default", "uid": "svc-uid"},
            "spec": {"selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": 8080}]}
        });
        reconcile_service(&store, &svc).unwrap();

        let ep = get_endpoints(&store, "default", "web");
        let address = &ep.subsets.as_ref().unwrap()[0].addresses.as_ref().unwrap()[0];
        let target = address.target_ref.as_ref().unwrap();
        assert_eq!(target.uid.as_deref(), Some("pod-uid-1"));
        assert_eq!(address.node_name.as_deref(), Some("r8s-node"));

        let slice = get_slice(&store, "default", "web");
        let labels = slice.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[MANAGED_BY_LABEL], SLICE_CONTROLLER);
        assert_eq!(labels[SERVICE_NAME_LABEL], "web");
        let endpoint = &slice.endpoints[0];
        assert_eq!(
            endpoint.target_ref.as_ref().unwrap().uid.as_deref(),
            Some("pod-uid-1")
        );
        assert_eq!(endpoint.node_name.as_deref(), Some("r8s-node"));
    }

    #[test]
    fn pods_serving_different_named_ports_get_their_own_subset_and_slice() {
        let (_d, store) = temp_store();
        let pods = GroupVersionResource::pods();
        for (name, port_name, port) in [("pod1", "svc1", 100), ("pod2", "svc2", 101)] {
            store
                .create(
                    ResourceRef {
                        gvr: &pods,
                        namespace: Some("default"),
                        name,
                    },
                    &json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": name, "namespace": "default", "uid": format!("uid-{name}"),
                                     "labels": {"app": "multi"}},
                        "spec": {"containers": [{"name": "c", "image": "c",
                                 "ports": [{"name": port_name, "containerPort": port}]}]},
                        "status": {"phase": "Running", "podIP": format!("10.244.0.{port}"),
                                   "conditions": [{"type": "Ready", "status": "True"}]}
                    }),
                )
                .unwrap();
        }
        let svc = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "multi", "namespace": "default", "uid": "svc-uid"},
            "spec": {"selector": {"app": "multi"}, "ports": [
                {"name": "portname1", "port": 80, "targetPort": "svc1"},
                {"name": "portname2", "port": 81, "targetPort": "svc2"}
            ]}
        });
        reconcile_service(&store, &svc).unwrap();

        let ep = get_endpoints(&store, "default", "multi");
        let subsets = ep.subsets.unwrap();
        assert_eq!(subsets.len(), 2);
        for subset in &subsets {
            let ports = subset.ports.as_ref().unwrap();
            assert_eq!(ports.len(), 1);
            let address = &subset.addresses.as_ref().unwrap()[0];
            let expected = if ports[0].name.as_deref() == Some("portname1") {
                100
            } else {
                101
            };
            assert_eq!(ports[0].port, expected);
            assert_eq!(address.ip, format!("10.244.0.{expected}"));
        }
        let names = managed_slices(&store, Some("default"), "multi", SLICE_CONTROLLER).unwrap();
        assert_eq!(names, vec!["multi".to_string(), "multi-1".to_string()]);
        let first = get_slice(&store, "default", "multi");
        assert_eq!(first.ports.as_ref().unwrap().len(), 1);
        assert_eq!(first.endpoints.len(), 1);

        // No pods: one placeholder slice without ports, no subsets.
        let lonely = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "lonely", "namespace": "default", "uid": "svc-uid-2"},
            "spec": {"selector": {"app": "none"}, "ports": [{"port": 80}]}
        });
        reconcile_service(&store, &lonely).unwrap();
        assert!(
            get_endpoints(&store, "default", "lonely")
                .subsets
                .unwrap()
                .is_empty()
        );
        let placeholder = get_slice(&store, "default", "lonely");
        assert!(placeholder.endpoints.is_empty());
        assert!(placeholder.ports.unwrap().is_empty());
    }

    #[test]
    fn custom_endpoints_mirror_into_slices_per_subset() {
        let (_d, store) = temp_store();
        let services = GroupVersionResource::services();
        let endpoints = GroupVersionResource::endpoints();
        store
            .create(
                ResourceRef {
                    gvr: &services,
                    namespace: Some("ns"),
                    name: "custom",
                },
                &json!({"apiVersion": "v1", "kind": "Service",
                        "metadata": {"name": "custom", "namespace": "ns", "uid": "svc-uid"},
                        "spec": {"ports": [{"name": "example", "port": 80}]}}),
            )
            .unwrap();
        let ep = |subsets: serde_json::Value| {
            json!({"apiVersion": "v1", "kind": "Endpoints",
                   "metadata": {"name": "custom", "namespace": "ns", "uid": "ep-uid",
                                "labels": {"team": "a"}},
                   "subsets": subsets})
        };
        let one = ep(json!([{"ports": [{"port": 80}], "addresses": [{"ip": "10.1.2.3"}]}]));
        store
            .create(
                ResourceRef {
                    gvr: &endpoints,
                    namespace: Some("ns"),
                    name: "custom",
                },
                &one,
            )
            .unwrap();
        mirror_endpoints(&store, &one).unwrap();

        let slice = get_slice(&store, "ns", "custom-0");
        let labels = slice.metadata.labels.as_ref().unwrap();
        assert_eq!(labels[MANAGED_BY_LABEL], MIRRORING_CONTROLLER);
        assert_eq!(labels[SERVICE_NAME_LABEL], "custom");
        assert_eq!(labels["team"], "a");
        assert_eq!(slice.endpoints[0].addresses, vec!["10.1.2.3"]);
        assert_eq!(slice.ports.as_ref().unwrap()[0].port, Some(80));
        assert_eq!(
            slice.metadata.owner_references.as_ref().unwrap()[0].kind,
            "Endpoints"
        );

        let two = ep(json!([
            {"ports": [{"name": "port80", "port": 80}], "addresses": [{"ip": "10.1.2.3"}]},
            {"ports": [{"name": "port81", "port": 81}], "addresses": [{"ip": "10.1.2.4"}]}
        ]));
        mirror_endpoints(&store, &two).unwrap();
        assert_eq!(
            mirrored_slices(&store, Some("ns"), "custom").unwrap().len(),
            2
        );

        mirror_endpoints(&store, &one).unwrap();
        assert_eq!(
            mirrored_slices(&store, Some("ns"), "custom").unwrap(),
            vec!["custom-0".to_string()],
            "a dropped subset drops its slice"
        );

        delete_mirrored_slices(&store, Some("ns"), "custom").unwrap();
        assert!(
            mirrored_slices(&store, Some("ns"), "custom")
                .unwrap()
                .is_empty()
        );
    }
    use serde_json::json;

    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        (dir, store)
    }

    /// Seed one Running-but-not-Ready pod and a headless Service selecting it,
    /// reconcile, and return the resulting endpoint addresses.
    fn reconcile_not_ready_pod(publish_not_ready: bool) -> Vec<EndpointAddress> {
        let (_dir, store) = temp_store();

        let pod_gvr = GroupVersionResource::pods();
        store
            .create(
                ResourceRef {
                    gvr: &pod_gvr,
                    namespace: Some("kafka"),
                    name: "kafka-controller-0",
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {
                        "name": "kafka-controller-0",
                        "namespace": "kafka",
                        "labels": {"app": "kafka"}
                    },
                    "spec": {
                        "hostname": "kafka-controller-0",
                        "subdomain": "kafka-controller-headless",
                        "containers": [{"name": "kafka", "image": "kafka"}]
                    },
                    "status": {
                        "phase": "Running",
                        "podIP": "10.244.0.30",
                        "conditions": [{"type": "Ready", "status": "False"}]
                    }
                }),
            )
            .unwrap();

        let svc = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "kafka-controller-headless", "namespace": "kafka", "uid": "svc-uid"},
            "spec": {
                "clusterIP": "None",
                "selector": {"app": "kafka"},
                "publishNotReadyAddresses": publish_not_ready,
                "ports": [{"port": 9092}]
            }
        });
        let svc_gvr = GroupVersionResource::services();
        store
            .create(
                ResourceRef {
                    gvr: &svc_gvr,
                    namespace: Some("kafka"),
                    name: "kafka-controller-headless",
                },
                &svc,
            )
            .unwrap();

        reconcile_service(&store, &svc).unwrap();

        let ep_gvr = GroupVersionResource::endpoints();
        let ep: Endpoints = store
            .get_as(&ResourceRef {
                gvr: &ep_gvr,
                namespace: Some("kafka"),
                name: "kafka-controller-headless",
            })
            .unwrap()
            .expect("endpoints object should exist");
        ep.subsets
            .unwrap_or_default()
            .into_iter()
            .flat_map(|s| s.addresses.unwrap_or_default())
            .collect()
    }

    #[test]
    fn publish_not_ready_includes_unready_pod_with_hostname() {
        // The KRaft-bootstrap case: the pod must be resolvable (with its
        // per-pod hostname) while still not Ready.
        let addrs = reconcile_not_ready_pod(true);
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip, "10.244.0.30");
        assert_eq!(addrs[0].hostname.as_deref(), Some("kafka-controller-0"));
    }

    #[test]
    fn default_service_excludes_unready_pod() {
        let addrs = reconcile_not_ready_pod(false);
        assert!(addrs.is_empty());
    }

    fn get_endpoints(store: &Store, ns: &str, name: &str) -> Endpoints {
        let gvr = GroupVersionResource::endpoints();
        store
            .get_as(&ResourceRef {
                gvr: &gvr,
                namespace: Some(ns),
                name,
            })
            .unwrap()
            .expect("endpoints should exist")
    }

    fn get_slice(store: &Store, ns: &str, name: &str) -> EndpointSlice {
        let gvr = GroupVersionResource::endpoint_slices();
        store
            .get_as(&ResourceRef {
                gvr: &gvr,
                namespace: Some(ns),
                name,
            })
            .unwrap()
            .expect("endpointslice should exist")
    }

    /// Seed a Running/Ready pod exposing a named containerPort and a Service
    /// targeting that port by name, reconcile, and return the store.
    fn seed_named_port() -> (tempfile::TempDir, Store) {
        let (dir, store) = temp_store();
        let pods = GroupVersionResource::pods();
        store
            .create(
                ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "web-0",
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "web-0", "namespace": "default", "labels": {"app": "web"}},
                    "spec": {"containers": [{
                        "name": "web", "image": "web",
                        "ports": [{"name": "http", "containerPort": 8080}]
                    }]},
                    "status": {
                        "phase": "Running", "podIP": "10.0.0.1",
                        "conditions": [{"type": "Ready", "status": "True"}]
                    }
                }),
            )
            .unwrap();
        let svc = json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "web", "namespace": "default", "uid": "u"},
            "spec": {"selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": "http"}]}
        });
        let svcs = GroupVersionResource::services();
        store
            .create(
                ResourceRef {
                    gvr: &svcs,
                    namespace: Some("default"),
                    name: "web",
                },
                &svc,
            )
            .unwrap();
        reconcile_service(&store, &svc).unwrap();
        (dir, store)
    }

    #[test]
    fn named_target_port_resolves_from_container_port() {
        let (_d, store) = seed_named_port();
        let ep = get_endpoints(&store, "default", "web");
        let subset = &ep.subsets.unwrap()[0];
        let port = &subset.ports.as_ref().unwrap()[0];
        assert_eq!(
            port.port, 8080,
            "named targetPort resolves to containerPort"
        );
        // A single-port Service leaves the port name empty, not Some("").
        assert_eq!(port.name.as_deref(), None);
    }

    #[test]
    fn not_ready_pod_populates_not_ready_addresses() {
        let addrs = reconcile_not_ready_pod(false);
        assert!(addrs.is_empty(), "unready pod is not a ready address");

        // The same seed, fetched in full: it must appear as a not-ready address
        // and as a slice endpoint whose conditions say not-serving.
        let (_dir, store) = {
            // Re-run the seed so we can inspect the stored objects.
            let (dir, store) = temp_store();
            let pods = GroupVersionResource::pods();
            store
                .create(
                    ResourceRef {
                        gvr: &pods,
                        namespace: Some("kafka"),
                        name: "kafka-controller-0",
                    },
                    &json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": "kafka-controller-0", "namespace": "kafka", "labels": {"app": "kafka"}},
                        "spec": {"containers": [{"name": "kafka", "image": "kafka"}]},
                        "status": {"phase": "Running", "podIP": "10.244.0.30",
                                   "conditions": [{"type": "Ready", "status": "False"}]}
                    }),
                )
                .unwrap();
            let svc = json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": "svc", "namespace": "kafka", "uid": "svc-uid"},
                "spec": {"selector": {"app": "kafka"}, "ports": [{"port": 9092}]}
            });
            let svcs = GroupVersionResource::services();
            store
                .create(
                    ResourceRef {
                        gvr: &svcs,
                        namespace: Some("kafka"),
                        name: "svc",
                    },
                    &svc,
                )
                .unwrap();
            reconcile_service(&store, &svc).unwrap();
            (dir, store)
        };

        let ep = get_endpoints(&store, "kafka", "svc");
        let subset = &ep.subsets.unwrap()[0];
        assert!(subset.addresses.as_ref().unwrap().is_empty());
        assert_eq!(subset.not_ready_addresses.as_ref().unwrap().len(), 1);

        let slice = get_slice(&store, "kafka", "svc");
        let cond = slice.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(cond.ready, Some(false));
        assert_eq!(cond.serving, Some(false));
        assert_eq!(cond.terminating, Some(false));
    }

    #[test]
    fn ready_pod_slice_condition_is_serving() {
        let (_d, store) = seed_named_port();
        let slice = get_slice(&store, "default", "web");
        let cond = slice.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(cond.ready, Some(true));
        assert_eq!(cond.serving, Some(true));
        assert_eq!(cond.terminating, Some(false));
    }
}
