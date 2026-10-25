use std::collections::BTreeMap;

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    DaemonEndpoint, GroupVersionResource, KUBELET_PORT, NODE_INTERNAL_IP, Node, NodeAddress,
    NodeCondition, NodeDaemonEndpoints, NodeSpec, NodeStatus, NodeSystemInfo, ObjectMeta,
    PodCondition, Quantity, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("scheduler started");

    register_node(&store)?;

    // Subscribe before the snapshot so a pod created during the initial scan
    // arrives as an event rather than vanishing in a list/watch gap.
    let (initial, mut rx) =
        store.list_and_watch(&GroupVersionResource::pods(), None, None, None)?;
    for pod in &initial.items {
        schedule_pod(&store, pod);
    }

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

#[allow(clippy::needless_update)]
fn register_node(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::nodes();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: NODE_NAME,
    };

    if store.get(&resource_ref)?.is_some() {
        tracing::info!("node '{NODE_NAME}' already registered");
        return Ok(());
    }

    let now = Time(chrono::Utc::now());
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let memory_ki = read_memtotal_ki().unwrap_or(8 * 1024 * 1024);
    let capacity = BTreeMap::from([
        ("cpu".into(), Quantity(cpu_count.to_string())),
        ("memory".into(), Quantity(format!("{memory_ki}Ki"))),
        ("pods".into(), Quantity("110".into())),
    ]);

    let node = Node {
        metadata: ObjectMeta {
            name: Some(NODE_NAME.into()),
            labels: Some(BTreeMap::from([
                ("kubernetes.io/hostname".into(), NODE_NAME.into()),
                ("kubernetes.io/os".into(), "linux".into()),
                ("kubernetes.io/arch".into(), std::env::consts::ARCH.into()),
            ])),
            ..Default::default()
        },
        spec: Some(NodeSpec::default()),
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                type_: "Ready".into(),
                status: "True".into(),
                last_heartbeat_time: Some(now.clone()),
                last_transition_time: Some(now),
                reason: Some("KubeletReady".into()),
                message: Some("r8s node is ready".into()),
                ..Default::default()
            }]),
            node_info: Some(NodeSystemInfo {
                operating_system: "linux".into(),
                architecture: std::env::consts::ARCH.into(),
                kubelet_version: "v1.32.0-r8s".into(),
                ..Default::default()
            }),
            capacity: Some(capacity.clone()),
            allocatable: Some(capacity),
            // Addresses + kubelet endpoint so metrics-server can locate and
            // scrape this node's /metrics/resource.
            addresses: Some(vec![
                NodeAddress {
                    type_: "InternalIP".into(),
                    address: NODE_INTERNAL_IP.into(),
                },
                NodeAddress {
                    type_: "Hostname".into(),
                    address: NODE_NAME.into(),
                },
            ]),
            daemon_endpoints: Some(NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint {
                    port: i32::from(KUBELET_PORT),
                }),
            }),
            ..Default::default()
        }),
    };

    store.create(resource_ref, &serde_json::to_value(&node)?)?;
    tracing::info!("registered node '{NODE_NAME}'");
    Ok(())
}

fn schedule_all(store: &Store) {
    let result = match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("failed to list pods: {e}");
            return;
        }
    };
    for pod in &result.items {
        schedule_pod(store, pod);
    }
}

fn is_scheduled(pod: &serde_json::Value) -> bool {
    pod.get("spec")
        .and_then(|spec| spec.get("nodeName"))
        .and_then(|name| name.as_str())
        .is_some_and(|name| !name.is_empty())
}

fn schedule_pod(store: &Store, pod_value: &serde_json::Value) {
    // Already bound — skip without a store round-trip. Kubelet status writes
    // re-fire this watch for pods we scheduled long ago.
    if is_scheduled(pod_value) {
        return;
    }

    let metadata = pod_value.get("metadata");
    let name = match metadata
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
    {
        Some(name) => name,
        None => return,
    };
    let namespace = metadata
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };

    // Re-read: the event may be stale, and the optimistic update needs the live
    // resourceVersion to detect a concurrent binding.
    let mut current = match store.get(&resource_ref) {
        Ok(Some(v)) => v,
        _ => return,
    };
    if is_scheduled(&current) {
        return;
    }

    // Mutate the raw Value to preserve fields not in our Pod struct
    if let Some(spec) = current.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("nodeName".to_string(), serde_json::json!(NODE_NAME));
    }
    if let Some(status) = current.get_mut("status").and_then(|v| v.as_object_mut()) {
        status.insert(
            "conditions".to_string(),
            serde_json::to_value(&[PodCondition {
                type_: "PodScheduled".into(),
                status: "True".into(),
                last_transition_time: Some(Time(chrono::Utc::now())),
                ..Default::default()
            }])
            .unwrap_or_default(),
        );
    } else if let Some(obj) = current.as_object_mut() {
        obj.insert(
            "status".to_string(),
            serde_json::json!({
                "conditions": serde_json::to_value(&[PodCondition {
                    type_: "PodScheduled".into(),
                    status: "True".into(),
                    last_transition_time: Some(Time(chrono::Utc::now())),
                    ..Default::default()
                }]).unwrap_or_default()
            }),
        );
    }

    match store.update(&resource_ref, &current) {
        Ok(_) => tracing::info!("scheduled pod '{name}' to node '{NODE_NAME}'"),
        Err(e) => tracing::debug!("scheduler update conflict for '{name}': {e}"),
    }
}

fn read_memtotal_ki() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = contents.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb_str = line.split_whitespace().nth(1)?;
    kb_str.parse().ok()
}
