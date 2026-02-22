use r8s_store::Store;
use r8s_store::backend::ResourceRef;
use r8s_store::watch::WatchEventType;
use r8s_types::GroupVersionResource;
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
                    Ok(_) => reconcile_all(&store),
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

fn reconcile_service(store: &Store, service: &serde_json::Value) -> anyhow::Result<()> {
    let svc_name = service["metadata"]["name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Service has no name"))?;
    let svc_ns = service["metadata"]["namespace"].as_str();
    let svc_uid = service["metadata"]["uid"].as_str().unwrap_or("");

    let selector = match service["spec"]["selector"].as_object() {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(()), // no selector, skip
    };

    // Find matching pods
    let pods = store.list(&pods_gvr(), svc_ns, None, None, None, None)?;
    let matching: Vec<&serde_json::Value> = pods
        .items
        .iter()
        .filter(|pod| {
            let labels = pod["metadata"]["labels"].as_object();
            match labels {
                Some(l) => selector.iter().all(|(k, v)| l.get(k) == Some(v)),
                None => false,
            }
        })
        .collect();

    // Build addresses
    let addresses: Vec<serde_json::Value> = matching
        .iter()
        .filter_map(|pod| {
            let pod_ip = pod["status"]["podIP"].as_str()?;
            if pod["status"]["phase"].as_str() != Some("Running") {
                return None;
            }
            let pod_name = pod["metadata"]["name"].as_str()?;
            let pod_ns = pod["metadata"]["namespace"].as_str().unwrap_or("default");
            Some(serde_json::json!({
                "ip": pod_ip,
                "targetRef": {
                    "kind": "Pod",
                    "name": pod_name,
                    "namespace": pod_ns,
                },
            }))
        })
        .collect();

    // Build ports from service spec
    let ports: Vec<serde_json::Value> = service["spec"]["ports"]
        .as_array()
        .map(|ps| {
            ps.iter()
                .map(|p| {
                    serde_json::json!({
                        "port": p["targetPort"].as_u64().or(p["port"].as_u64()).unwrap_or(0),
                        "protocol": p["protocol"].as_str().unwrap_or("TCP"),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let ep = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": svc_name,
            "namespace": svc_ns,
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Service",
                "name": svc_name,
                "uid": svc_uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }],
        },
        "subsets": [{
            "addresses": addresses,
            "ports": ports,
        }],
    });

    let gvr = ep_gvr();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: svc_ns,
        name: svc_name,
    };

    match store.get(&rref)? {
        Some(_) => {
            let _ = store.update(&rref, &ep);
        }
        None => {
            let _ = store.create(rref, &ep);
        }
    }

    // Also create/update EndpointSlice (used by Traefik v3 and newer controllers)
    let es_gvr = es_gvr();
    let es_ref = ResourceRef {
        gvr: &es_gvr,
        namespace: svc_ns,
        name: svc_name,
    };
    let es = serde_json::json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSlice",
        "metadata": {
            "name": svc_name,
            "namespace": svc_ns,
            "labels": {
                "kubernetes.io/service-name": svc_name,
            },
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Service",
                "name": svc_name,
                "uid": svc_uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }],
        },
        "addressType": "IPv4",
        "endpoints": addresses.iter().map(|a| {
            serde_json::json!({
                "addresses": [a["ip"]],
                "conditions": {"ready": true},
            })
        }).collect::<Vec<_>>(),
        "ports": ports,
    });
    match store.get(&es_ref)? {
        Some(_) => {
            let _ = store.update(&es_ref, &es);
        }
        None => {
            let _ = store.create(es_ref, &es);
        }
    }

    Ok(())
}
