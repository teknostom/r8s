use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_store::watch::WatchEventType;
use r8s_types::{
    EndpointAddress, EndpointConditions, EndpointPort, EndpointSlice, EndpointSubset, Endpoints,
    GroupVersionResource, ObjectMeta, ObjectReference, OwnerReference, Pod, Service, SliceEndpoint,
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

fn resolve_target_port(target_port: Option<&IntOrString>, default: i32) -> i32 {
    target_port
        .and_then(|tp| match tp {
            IntOrString::Int(i) => Some(*i),
            _ => None,
        })
        .unwrap_or(default)
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

    let addresses: Vec<EndpointAddress> = matching
        .iter()
        .filter_map(|pod| {
            let status = pod.status.as_ref()?;
            let pod_ip = status.pod_ip.as_deref()?;
            if status.phase.as_deref() != Some("Running") {
                return None;
            }
            let pod_name = pod.metadata.name.as_deref()?;
            let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
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

    let ports: Vec<EndpointPort> = svc_spec
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| EndpointPort {
            port: resolve_target_port(p.target_port.as_ref(), p.port),
            protocol: p.protocol.clone(),
            name: p.name.clone(),
            ..Default::default()
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

    let gvr = GroupVersionResource::endpoints();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    match store.get(&rref)? {
        Some(existing) => {
            let mut ep_value = serde_json::to_value(&ep)?;
            if let Some(rv) = existing["metadata"]["resourceVersion"].as_str() {
                ep_value["metadata"]["resourceVersion"] = serde_json::json!(rv);
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

    let discovery_ports: Vec<k8s_openapi::api::discovery::v1::EndpointPort> = svc_spec
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| k8s_openapi::api::discovery::v1::EndpointPort {
            port: Some(resolve_target_port(p.target_port.as_ref(), p.port)),
            protocol: p.protocol.clone(),
            name: p.name.clone(),
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
    match store.get(&es_ref)? {
        Some(existing) => {
            let mut es_value = serde_json::to_value(&es)?;
            if let Some(rv) = existing["metadata"]["resourceVersion"].as_str() {
                es_value["metadata"]["resourceVersion"] = serde_json::json!(rv);
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
