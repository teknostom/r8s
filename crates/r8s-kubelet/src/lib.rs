use std::{sync::Arc, time::Duration};

use r8s_runtime::{
    ContainerRuntime,
    traits::{ContainerConfig, ContainerId},
};
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

fn pods_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

struct PodContainers {
    container_ids: Vec<ContainerId>,
}

pub async fn run<R: ContainerRuntime>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("kubelet started for node '{NODE_NAME}'");

    let mut pod_containers: FxHashMap<String, PodContainers> = FxHashMap::default();
    let mut next_pod_ip: u32 = 2;
    let rt = &*runtime;

    reconcile_all(&store, rt, &mut pod_containers, &mut next_pod_ip).await;

    let mut rx = store.watch(&pods_gvr());
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("kubelet shutting down");
                for pc in pod_containers.values() {
                    for cid in &pc.container_ids {
                        let _ = rt.stop_container(cid, Duration::from_secs(5)).await;
                        let _ = rt.remove_container(cid).await;
                    }
                }
                return Ok(());
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                reconcile_pod(&store, rt, &mut pod_containers, &mut next_pod_ip, &event.object).await;
                            }
                            WatchEventType::Deleted => {
                                handle_pod_deleted(rt, &mut pod_containers, &event.object).await;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("kubelet lagged, re-syncing");
                        reconcile_all(&store, rt, &mut pod_containers, &mut next_pod_ip).await;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

async fn reconcile_all<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    next_pod_ip: &mut u32,
) {
    let result = match store.list(&pods_gvr(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kubelet list error: {e}");
            return;
        }
    };
    for pod in &result.items {
        reconcile_pod(store, runtime, pod_containers, next_pod_ip, pod).await;
    }
}

async fn reconcile_pod<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    next_pod_ip: &mut u32,
    pod: &serde_json::Value,
) {
    // Only care about pods on our node
    let node_name = pod["spec"]["nodeName"].as_str().unwrap_or("");
    if node_name != NODE_NAME {
        return;
    }

    let pod_name = match pod["metadata"]["name"].as_str() {
        Some(n) => n,
        None => return,
    };
    let pod_ns = pod["metadata"]["namespace"].as_str();
    let pod_uid = match pod["metadata"]["uid"].as_str() {
        Some(u) => u.to_string(),
        None => return,
    };

    // Already tracked — just ensure status is up to date
    if pod_containers.contains_key(&pod_uid) {
        if pod["status"]["phase"].as_str() == Some("Running") {
            return; // Circuit breaker: don't re-update Running pods
        }
        update_pod_status(
            store,
            pod_containers,
            &pod_uid,
            pod_name,
            pod_ns,
            *next_pod_ip - 1,
        );
        return;
    }

    // New pod — start containers
    let containers = match pod["spec"]["containers"].as_array() {
        Some(c) => c,
        None => {
            tracing::warn!("pod '{pod_name}' has no containers in spec");
            return;
        }
    };

    let pod_ip_num = *next_pod_ip;
    *next_pod_ip += 1;
    let mut container_ids = Vec::new();

    for container_spec in containers {
        let container_name = container_spec["name"].as_str().unwrap_or("unnamed");
        let image = container_spec["image"].as_str().unwrap_or("unknown");

        // Pull image
        match runtime.pull_image(image).await {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("pod '{pod_name}': failed to pull image '{image}': {e}");
                return;
            }
        }

        // Build config
        let config = ContainerConfig {
            name: format!("{pod_name}_{container_name}"),
            image: image.to_string(),
            command: container_spec["command"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            args: container_spec["args"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            env: container_spec["env"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|e| {
                            let name = e["name"].as_str()?;
                            let value = e["value"].as_str().unwrap_or("");
                            Some((name.to_string(), value.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            working_dir: container_spec["workingDir"].as_str().map(String::from),
        };

        // Create and start
        let container_id = match runtime.create_container(&config).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(
                    "pod '{pod_name}': failed to create container '{container_name}': {e}"
                );
                return;
            }
        };

        if let Err(e) = runtime.start_container(&container_id).await {
            tracing::error!("pod '{pod_name}': failed to start container '{container_name}': {e}");
            return;
        }

        tracing::info!(
            "pod '{pod_name}': started container '{container_name}' (id={})",
            container_id.0
        );
        container_ids.push(container_id);
    }

    pod_containers.insert(pod_uid.clone(), PodContainers { container_ids });
    update_pod_status(
        store,
        pod_containers,
        &pod_uid,
        pod_name,
        pod_ns,
        pod_ip_num,
    );
}

fn update_pod_status(
    store: &Store,
    pod_containers: &FxHashMap<String, PodContainers>,
    pod_uid: &str,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_ip_num: u32,
) {
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

    let pc = match pod_containers.get(pod_uid) {
        Some(pc) => pc,
        None => return,
    };

    let now = chrono::Utc::now().to_rfc3339();
    let pod_ip = format!("10.244.0.{pod_ip_num}");

    let containers_spec = current["spec"]["containers"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    let container_statuses: Vec<serde_json::Value> = containers_spec
        .iter()
        .zip(pc.container_ids.iter())
        .map(|(spec, cid)| {
            let name = spec["name"].as_str().unwrap_or("unnamed");
            let image = spec["image"].as_str().unwrap_or("unknown");
            serde_json::json!({
                "name": name,
                "image": image,
                "imageID": format!("mock://{image}"),
                "containerID": format!("mock://{}", cid.0),
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {
                    "running": {
                        "startedAt": now,
                    }
                }
            })
        })
        .collect();

    // Preserve existing conditions (PodScheduled from scheduler), add new ones
    let mut conditions = current["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    if !conditions.iter().any(|c| c["type"] == "Ready") {
        conditions.push(serde_json::json!({
            "type": "Ready", "status": "True", "lastTransitionTime": now,
        }));
    }
    if !conditions.iter().any(|c| c["type"] == "Initialized") {
        conditions.push(serde_json::json!({
            "type": "Initialized", "status": "True", "lastTransitionTime": now,
        }));
    }
    if !conditions.iter().any(|c| c["type"] == "ContainersReady") {
        conditions.push(serde_json::json!({
            "type": "ContainersReady", "status": "True", "lastTransitionTime": now,
        }));
    }

    current["status"] = serde_json::json!({
        "phase": "Running",
        "hostIP": "127.0.0.1",
        "podIP": pod_ip,
        "podIPs": [{"ip": pod_ip}],
        "startTime": now,
        "conditions": conditions,
        "containerStatuses": container_statuses,
    });

    match store.update(&resource_ref, &current) {
        Ok(_) => tracing::info!("pod '{pod_name}': status updated to Running (ip={pod_ip})"),
        Err(e) => tracing::debug!("pod '{pod_name}': status update conflict: {e}"),
    }
}

async fn handle_pod_deleted<R: ContainerRuntime>(
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    pod: &serde_json::Value,
) {
    let node_name = pod["spec"]["nodeName"].as_str().unwrap_or("");
    if node_name != NODE_NAME {
        return;
    }

    let pod_uid = match pod["metadata"]["uid"].as_str() {
        Some(u) => u.to_string(),
        None => return,
    };
    let pod_name = pod["metadata"]["name"].as_str().unwrap_or("unknown");

    if let Some(pc) = pod_containers.remove(&pod_uid) {
        for cid in &pc.container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(10)).await;
            let _ = runtime.remove_container(cid).await;
        }
        tracing::info!(
            "pod '{pod_name}: cleaned up {} containers",
            pc.container_ids.len()
        );
    }
}
