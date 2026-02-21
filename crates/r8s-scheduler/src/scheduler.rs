use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

fn pods_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

fn nodes_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "nodes")
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("scheduler started");

    register_node(&store)?;
    schedule_all(&store);

    let mut rx = store.watch(&pods_gvr());
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("scheduler shutting down");
                return Ok(());
            }
            event = rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        schedule_pod(&store, &event.object);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("scheduler lagged {n} events, re-syncing");
                        schedule_all(&store);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn register_node(store: &Store) -> anyhow::Result<()> {
    let gvr = nodes_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: NODE_NAME,
    };

    if store.get(&resource_ref)?.is_some() {
        tracing::info!("node '{NODE_NAME}' already registered");
        return Ok(());
    }

    let now = chrono::Utc::now().to_rfc3339();
    let node = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": NODE_NAME,
            "labels": {
                "kubernetes.io/hostname": NODE_NAME,
                "kubernetes.io/os": "linux",
                "kubernetes.io/arch": std::env::consts::ARCH,
            }
        },
        "status": {
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "lastHeartbeatTime": now,
                "lastTransitionTime": now,
                "reason": "KubeletReady",
                "message": "r8s node is ready",
            }],
            "nodeInfo": {
                "operatingSystem": "linux",
                "architecture": std::env::consts::ARCH,
                "kubeletVersion": "v1.32.0-r8s",
            },
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"},
        }
    });

    store.create(resource_ref, &node)?;
    tracing::info!("registered node '{NODE_NAME}'");
    Ok(())
}

fn schedule_all(store: &Store) {
    let result = match store.list(&pods_gvr(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("scheduler list error: {e}");
            return;
        }
    };
    for pod in &result.items {
        schedule_pod(store, pod);
    }
}

fn schedule_pod(store: &Store, pod: &serde_json::Value) {
    let node_name = pod["spec"]["nodeName"].as_str().unwrap_or("");
    if !node_name.is_empty() {
        return;
    }

    let pod_name = match pod["metadata"]["name"].as_str() {
        Some(n) => n,
        None => return,
    };
    let pod_ns = pod["metadata"]["namespace"].as_str();

    let gvr = pods_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns,
        name: pod_name,
    };

    let mut current = match store.get(&resource_ref) {
        Ok(Some(p)) => p,
        _ => return,
    };

    // Double-check still unscheduled after re-read
    let current_node = current["spec"]["nodeName"].as_str().unwrap_or("");
    if !current_node.is_empty() {
        return;
    }

    current["spec"]["nodeName"] = serde_json::json!(NODE_NAME);

    let now = chrono::Utc::now().to_rfc3339();
    if current["status"].is_null() {
        current["status"] = serde_json::json!({});
    }
    current["status"]["conditions"] = serde_json::json!([{
        "type": "PodScheduled",
        "status": "True",
        "lastTransitionTime": now,
    }]);

    match store.update(&resource_ref, &current) {
        Ok(_) => tracing::info!("scheduled pod '{pod_name}' to node '{NODE_NAME}'"),
        Err(e) => tracing::debug!("scheduler update conflict for '{pod_name}': {e}"),
    }
}
