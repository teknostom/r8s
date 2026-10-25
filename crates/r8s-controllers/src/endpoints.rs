use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_store::watch::WatchEventType;
use r8s_types::{
    EndpointAddress, EndpointConditions, EndpointPort, EndpointSlice, EndpointSubset, Endpoints,
    GroupVersionResource, ObjectMeta, ObjectReference, Pod, Service, SliceEndpoint,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("endpoints controller started");

    reconcile_all(&store);

    let mut svc_rx = store.watch(&GroupVersionResource::services());
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

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
fn resolve_target_port(target_port: Option<&IntOrString>, service_port: i32, pods: &[Pod]) -> i32 {
    match target_port {
        Some(IntOrString::Int(i)) => *i,
        Some(IntOrString::String(name)) => resolve_named_port(name, pods).unwrap_or(service_port),
        None => service_port,
    }
}

/// First `containerPort` named `name` across the selected pods. Replicas share a
/// PodTemplate, so any pod that has the name resolves it.
fn resolve_named_port(name: &str, pods: &[Pod]) -> Option<i32> {
    pods.iter().find_map(|pod| {
        pod.spec
            .as_ref()?
            .containers
            .iter()
            .filter_map(|c| c.ports.as_ref())
            .flatten()
            .find(|p| p.name.as_deref() == Some(name))
            .map(|p| p.container_port)
    })
}

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
        _ => return Ok(()),
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

    // One candidate per selected pod that has an IP and is Running, tagged with
    // its readiness and whether it's shutting down. Endpoints splits these into
    // ready `addresses` and `notReadyAddresses`; EndpointSlice carries the
    // per-endpoint conditions.
    struct Candidate {
        address: EndpointAddress,
        ready: bool,
        terminating: bool,
    }

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
            Some(Candidate {
                address: EndpointAddress {
                    ip: pod_ip.to_string(),
                    hostname,
                    target_ref: Some(ObjectReference {
                        kind: Some("Pod".into()),
                        name: Some(pod_name.into()),
                        namespace: Some(pod_ns.into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ready,
                terminating: pod.metadata.deletion_timestamp.is_some(),
            })
        })
        .collect();

    // A pod is published as a ready endpoint when it's Ready, or unconditionally
    // when the Service publishes not-ready addresses (headless StatefulSet
    // bootstrap). The rest go to `notReadyAddresses` so consumers and
    // `kubectl get endpoints` can still see them.
    let ready_for_traffic = |c: &Candidate| publish_not_ready || c.ready;
    let ready_addresses: Vec<EndpointAddress> = candidates
        .iter()
        .filter(|c| ready_for_traffic(c))
        .map(|c| c.address.clone())
        .collect();
    let not_ready_addresses: Vec<EndpointAddress> = candidates
        .iter()
        .filter(|c| !ready_for_traffic(c))
        .map(|c| c.address.clone())
        .collect();

    // Resolve each Service port once; both the legacy and discovery forms reuse
    // it. An unnamed port stays `None`, not `Some("")` — a single-port Service
    // has no port name and consumers key on it.
    struct ResolvedPort {
        name: Option<String>,
        port: i32,
        protocol: String,
    }
    let resolved_ports: Vec<ResolvedPort> = svc_spec
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| ResolvedPort {
            name: p.name.as_deref().filter(|n| !n.is_empty()).map(String::from),
            port: resolve_target_port(p.target_port.as_ref(), p.port, &matching),
            protocol: p.protocol.clone().unwrap_or_else(|| "TCP".into()),
        })
        .collect();

    let ports: Vec<EndpointPort> = resolved_ports
        .iter()
        .map(|p| EndpointPort {
            name: p.name.clone(),
            port: p.port,
            protocol: Some(p.protocol.clone()),
            ..Default::default()
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
        subsets: Some(vec![EndpointSubset {
            addresses: Some(ready_addresses),
            not_ready_addresses: (!not_ready_addresses.is_empty()).then_some(not_ready_addresses),
            ports: Some(ports),
        }]),
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

    // Newer ingress controllers require EndpointSlice in addition to legacy Endpoints
    let es_gvr = GroupVersionResource::endpoint_slices();
    let es_ref = ResourceRef {
        gvr: &es_gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    let discovery_ports: Vec<k8s_openapi::api::discovery::v1::EndpointPort> = resolved_ports
        .iter()
        .map(|p| k8s_openapi::api::discovery::v1::EndpointPort {
            name: p.name.clone(),
            port: Some(p.port),
            protocol: Some(p.protocol.clone()),
            ..Default::default()
        })
        .collect();

    let es = EndpointSlice {
        metadata: ObjectMeta {
            name: Some(svc_name.into()),
            namespace: svc_ns.map(String::from),
            labels: Some(std::collections::BTreeMap::from([(
                "kubernetes.io/service-name".into(),
                svc_name.into(),
            )])),
            owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        address_type: "IPv4".into(),
        // Every candidate — ready or not — becomes a slice endpoint carrying its
        // own conditions, so consumers can distinguish serving from draining.
        endpoints: candidates
            .iter()
            .map(|c| SliceEndpoint {
                addresses: vec![c.address.ip.clone()],
                conditions: Some(EndpointConditions {
                    // Ready drives load-balancing: serving and not shutting down,
                    // or forced by publishNotReadyAddresses.
                    ready: Some(publish_not_ready || (c.ready && !c.terminating)),
                    // Serving tracks true readiness even while terminating, so
                    // graceful-shutdown proxies keep draining in-flight work.
                    serving: Some(c.ready),
                    terminating: Some(c.terminating),
                }),
                // Carry the backing-pod reference into the slice. Consumers
                // (e.g. cert-manager's startup API check) resolve a Service's
                // named port by walking EndpointSlice endpoints, and skip any
                // endpoint that lacks a targetRef — so without this they can't
                // resolve the port and report the Service as not ready.
                target_ref: c.address.target_ref.clone(),
                hostname: c.address.hostname.clone(),
                ..Default::default()
            })
            .collect(),
        ports: Some(discovery_ports),
    };
    match store.get(&es_ref)? {
        Some(existing) => {
            let mut es_value = serde_json::to_value(&es)?;
            if let Some(rv) = existing
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|v| v.as_str())
                && let Some(meta) = es_value.get_mut("metadata").and_then(|v| v.as_object_mut())
            {
                meta.insert("resourceVersion".to_string(), serde_json::json!(rv));
            }
            let _ = store.update(&es_ref, &es_value);
        }
        None => {
            let es_value = serde_json::to_value(&es)?;
            let _ = store.create(es_ref, &es_value);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(port.port, 8080, "named targetPort resolves to containerPort");
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
