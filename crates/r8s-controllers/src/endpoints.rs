use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_store::watch::WatchEventType;
use r8s_types::{
    EndpointAddress, EndpointPort, EndpointSlice, EndpointSubset, Endpoints,
    EndpointConditions, GroupVersionResource, ObjectMeta, ObjectReference, OwnerReference,
    Pod, Service, SliceEndpoint,
};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn svc_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "services")
}

fn ep_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "endpoints")
}

fn es_gvr() -> GroupVersionResource {
    GroupVersionResource::new("discovery.k8s.io", "v1", "endpointslices")
}

fn pods_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("endpoints controller started");

    reconcile_all(&store);

    let mut svc_rx = store.watch(&svc_gvr());
    let mut pod_rx = store.watch(&pods_gvr());

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
                        let pod: Pod = match serde_json::from_value(event.object) {
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
    let result = match store.list(&svc_gvr(), None, None, None, None, None) {
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

/// Only reconcile services whose selector matches the changed pod.
fn reconcile_services_for_pod(store: &Store, pod: &Pod) {
    let pod_ns = pod.metadata.namespace.as_deref();
    let pod_labels = pod.metadata.labels.as_ref();
    let svcs = match store.list(&svc_gvr(), pod_ns, None, None, None, None) {
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
        _ => return Ok(()), // no selector, skip
    };

    // Find matching pods
    let pods = store.list(&pods_gvr(), svc_ns, None, None, None, None)?;
    let matching: Vec<Pod> = pods
        .items
        .iter()
        .filter_map(|v| serde_json::from_value::<Pod>(v.clone()).ok())
        .filter(|pod| {
            match pod.metadata.labels.as_ref() {
                Some(labels) => selector.iter().all(|(k, v)| labels.get(k) == Some(v)),
                None => false,
            }
        })
        .collect();

    // Build addresses
    let addresses: Vec<EndpointAddress> = matching
        .iter()
        .filter_map(|pod| {
            let status = pod.status.as_ref()?;
            let pod_ip = status.pod_ip.as_deref()?;
            if status.phase.as_deref() != Some("Running") {
                return None;
            }
            let pod_name = pod.metadata.name.as_deref()?;
            let pod_ns = pod
                .metadata
                .namespace
                .as_deref()
                .unwrap_or("default");
            Some(EndpointAddress {
                ip: pod_ip.to_string(),
                target_ref: Some(ObjectReference {
                    kind: Some("Pod".into()),
                    name: Some(pod_name.into()),
                    namespace: Some(pod_ns.into()),
                    ..Default::default()
                }),
                ..Default::default()
            })
        })
        .collect();

    // Build ports from service spec
    let ports: Vec<EndpointPort> = svc_spec
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| {
            let target_port = p
                .target_port
                .as_ref()
                .and_then(|tp| match tp {
                    IntOrString::Int(i) => Some(*i),
                    _ => None,
                })
                .unwrap_or(p.port);
            EndpointPort {
                port: target_port,
                protocol: p.protocol.clone(),
                name: p.name.clone(),
                ..Default::default()
            }
        })
        .collect();

    let owner_ref = OwnerReference {
        api_version: "v1".into(),
        kind: "Service".into(),
        name: svc_name.into(),
        uid: svc_uid.into(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    };

    let ep = Endpoints {
        metadata: ObjectMeta {
            name: Some(svc_name.into()),
            namespace: svc_ns.map(String::from),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        subsets: Some(vec![EndpointSubset {
            addresses: Some(addresses.clone()),
            ports: Some(ports.clone()),
            ..Default::default()
        }]),
    };

    let ep_value = serde_json::to_value(&ep)?;
    let gvr = ep_gvr();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    match store.get(&rref)? {
        Some(_) => {
            let _ = store.update(&rref, &ep_value);
        }
        None => {
            let _ = store.create(rref, &ep_value);
        }
    }

    // Also create/update EndpointSlice (used by Traefik v3 and newer controllers)
    let es_gvr = es_gvr();
    let es_ref = ResourceRef {
        gvr: &es_gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    // Build discovery EndpointPort (different type from core EndpointPort)
    let discovery_ports: Vec<k8s_openapi::api::discovery::v1::EndpointPort> = svc_spec
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| {
            let target_port = p
                .target_port
                .as_ref()
                .and_then(|tp| match tp {
                    IntOrString::Int(i) => Some(*i),
                    _ => None,
                })
                .unwrap_or(p.port);
            k8s_openapi::api::discovery::v1::EndpointPort {
                port: Some(target_port),
                protocol: p.protocol.clone(),
                name: p.name.clone(),
                ..Default::default()
            }
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
        endpoints: addresses
            .iter()
            .map(|a| SliceEndpoint {
                addresses: vec![a.ip.clone()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect(),
        ports: Some(discovery_ports),
    };
    let es_value = serde_json::to_value(&es)?;
    match store.get(&es_ref)? {
        Some(_) => {
            let _ = store.update(&es_ref, &es_value);
        }
        None => {
            let _ = store.create(es_ref, &es_value);
        }
    }

    Ok(())
}
