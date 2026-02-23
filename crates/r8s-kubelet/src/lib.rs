use std::{path::PathBuf, sync::Arc, time::Duration};

use r8s_runtime::{
    ContainerRuntime,
    traits::{ContainerConfig, ContainerId, Mount},
};
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, Pod,
    pod::{
        ContainerState, ContainerStateRunning, ContainerStatusInfo, PodCondition, PodIP, PodStatus,
        Volume, VolumeMount,
    },
};
use rustc_hash::FxHashMap;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

fn pods_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

struct PodContainers {
    container_ids: Vec<ContainerId>,
    pod_ip_num: u32,
    pod_name: String,
    pod_ns: Option<String>,
}

/// Simple IP pool that reclaims freed addresses.
struct IpPool {
    free: Vec<u32>,
    next: u32,
}

impl IpPool {
    fn new() -> Self {
        Self {
            free: Vec::new(),
            next: 2,
        }
    }

    fn allocate(&mut self) -> Option<u32> {
        if let Some(ip) = self.free.pop() {
            return Some(ip);
        }
        if self.next > 254 {
            return None;
        }
        let ip = self.next;
        self.next += 1;
        Some(ip)
    }

    fn release(&mut self, ip: u32) {
        self.free.push(ip);
    }
}

pub async fn run<R: ContainerRuntime>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    tracing::info!("kubelet started for node '{NODE_NAME}'");

    let mut pod_containers: FxHashMap<String, PodContainers> = FxHashMap::default();
    let mut ip_pool = IpPool::new();
    let rt = &*runtime;

    reconcile_all(&store, rt, &mut pod_containers, &mut ip_pool, &data_dir).await;

    let mut rx = store.watch(&pods_gvr());
    let mut health_interval = tokio::time::interval(Duration::from_secs(10));
    health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
            _ = health_interval.tick() => {
                check_health(&store, rt, &mut pod_containers, &mut ip_pool, &data_dir).await;
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                reconcile_pod(&store, rt, &mut pod_containers, &mut ip_pool, &event.object, &data_dir).await;
                            }
                            WatchEventType::Deleted => {
                                handle_pod_deleted(rt, &mut pod_containers, &mut ip_pool, &event.object, &data_dir).await;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("kubelet lagged, re-syncing");
                        reconcile_all(&store, rt, &mut pod_containers, &mut ip_pool, &data_dir).await;
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
    ip_pool: &mut IpPool,
    data_dir: &PathBuf,
) {
    let result = match store.list(&pods_gvr(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kubelet list error: {e}");
            return;
        }
    };
    for pod in &result.items {
        reconcile_pod(store, runtime, pod_containers, ip_pool, pod, data_dir).await;
    }
}

async fn reconcile_pod<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    ip_pool: &mut IpPool,
    pod_value: &serde_json::Value,
    data_dir: &PathBuf,
) {
    let pod: Pod = match serde_json::from_value(pod_value.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    // Only care about pods on our node
    if pod.spec.node_name.as_deref() != Some(NODE_NAME) {
        return;
    }

    let pod_name = match pod.metadata.name.as_deref() {
        Some(n) => n,
        None => return,
    };
    let pod_ns = pod.metadata.namespace.as_deref();
    let pod_uid = match pod.metadata.uid.as_deref() {
        Some(u) => u.to_string(),
        None => return,
    };

    // Already tracked — just ensure status is up to date
    if let Some(pc) = pod_containers.get(&pod_uid) {
        if pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref()) == Some("Running")
        {
            return;
        }
        let ip_num = pc.pod_ip_num;
        update_pod_status(store, pod_containers, &pod_uid, pod_name, pod_ns, ip_num);
        return;
    }

    // New pod — start containers
    if pod.spec.containers.is_empty() {
        tracing::warn!("pod '{pod_name}' has no containers in spec");
        return;
    }

    // Prepare volumes before creating containers
    let volume_paths = match prepare_volumes(store, data_dir, &pod_uid, pod_ns, &pod.spec.volumes)
    {
        Ok(paths) => paths,
        Err(e) => {
            tracing::error!("pod '{pod_name}': volume preparation failed: {e}");
            return;
        }
    };

    let pod_ip_num = match ip_pool.allocate() {
        Some(ip) => ip,
        None => {
            tracing::error!("pod '{pod_name}': IP pool exhausted");
            return;
        }
    };
    let mut container_ids: Vec<ContainerId> = Vec::new();
    let mut failed = false;

    for container_spec in &pod.spec.containers {
        let container_name = &container_spec.name;
        let image = &container_spec.image;

        // Pull image
        if let Err(e) = runtime.pull_image(image).await {
            tracing::error!("pod '{pod_name}': failed to pull image '{image}': {e}");
            failed = true;
            break;
        }

        // Resolve volume mounts for this container
        let mounts = resolve_mounts(
            container_spec.volume_mounts.as_deref().unwrap_or_default(),
            &volume_paths,
        );

        // Build config
        let config = ContainerConfig {
            name: format!("{pod_name}_{container_name}"),
            image: image.to_string(),
            command: container_spec.command.clone().unwrap_or_default(),
            args: container_spec.args.clone().unwrap_or_default(),
            env: {
                let mut env: Vec<(String, String)> = container_spec
                    .env
                    .as_ref()
                    .map(|envs| {
                        envs.iter()
                            .map(|e| (e.name.clone(), e.value.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                env.push(("KUBERNETES_SERVICE_HOST".into(), "10.244.0.1".into()));
                env.push(("KUBERNETES_SERVICE_PORT".into(), "443".into()));
                env.push(("KUBERNETES_SERVICE_PORT_HTTPS".into(), "443".into()));
                env
            },
            working_dir: container_spec.working_dir.clone(),
            mounts,
        };

        // Create and start
        let container_id = match runtime.create_container(&config).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(
                    "pod '{pod_name}': failed to create container '{container_name}': {e}"
                );
                failed = true;
                break;
            }
        };

        if let Err(e) = runtime.start_container(&container_id).await {
            tracing::error!("pod '{pod_name}': failed to start container '{container_name}': {e}");
            failed = true;
            break;
        }

        tracing::info!(
            "pod '{pod_name}': started container '{container_name}' (id={})",
            container_id.0
        );
        container_ids.push(container_id);
    }

    // Rollback on partial failure: clean up any containers we started
    if failed {
        for cid in &container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
            let _ = runtime.remove_container(cid).await;
        }
        ip_pool.release(pod_ip_num);
        cleanup_pod_volumes(data_dir, &pod_uid);
        tracing::warn!("pod '{pod_name}': rolled back {} containers", container_ids.len());
        return;
    }

    if !container_ids.is_empty() {
        match runtime.container_pid(&container_ids[0]).await {
            Ok(pid) => {
                let pod_ip = format!("10.244.0.{pod_ip_num}");
                if let Err(e) = r8s_network::bridge::setup_pod_network(pid, &pod_ip, pod_name) {
                    tracing::error!("pod '{pod_name}': network setup failed: {e}");
                }
            }
            Err(e) => tracing::warn!("pod '{pod_name}': network setup failed: {e}"),
        }
    }

    pod_containers.insert(pod_uid.clone(), PodContainers {
        container_ids,
        pod_ip_num,
        pod_name: pod_name.to_string(),
        pod_ns: pod_ns.map(String::from),
    });
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

    // Deserialize the current pod to read spec containers and existing conditions
    let current_pod: Pod = match serde_json::from_value(current.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    let now = chrono::Utc::now().to_rfc3339();
    let pod_ip = format!("10.244.0.{pod_ip_num}");

    let container_statuses: Vec<ContainerStatusInfo> = current_pod
        .spec
        .containers
        .iter()
        .zip(pc.container_ids.iter())
        .map(|(spec, cid)| ContainerStatusInfo {
            name: spec.name.clone(),
            image: spec.image.clone(),
            image_id: spec.image.clone(),
            container_id: Some(cid.0.clone()),
            ready: true,
            started: true,
            restart_count: 0,
            state: Some(ContainerState {
                running: Some(ContainerStateRunning {
                    started_at: Some(now.clone()),
                }),
            }),
        })
        .collect();

    // Preserve existing conditions (PodScheduled from scheduler), add new ones
    let mut conditions = current_pod
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    if !conditions.iter().any(|c| c.type_ == "Ready") {
        conditions.push(PodCondition {
            type_: "Ready".into(),
            status: "True".into(),
            last_transition_time: Some(now.clone()),
        });
    }
    if !conditions.iter().any(|c| c.type_ == "Initialized") {
        conditions.push(PodCondition {
            type_: "Initialized".into(),
            status: "True".into(),
            last_transition_time: Some(now.clone()),
        });
    }
    if !conditions.iter().any(|c| c.type_ == "ContainersReady") {
        conditions.push(PodCondition {
            type_: "ContainersReady".into(),
            status: "True".into(),
            last_transition_time: Some(now.clone()),
        });
    }

    let status = PodStatus {
        phase: Some("Running".into()),
        host_ip: Some("127.0.0.1".into()),
        pod_ip: Some(pod_ip.clone()),
        pod_ips: Some(vec![PodIP { ip: pod_ip.clone() }]),
        start_time: Some(now),
        conditions,
        container_statuses,
    };
    current["status"] = serde_json::to_value(&status).unwrap_or_default();

    match store.update(&resource_ref, &current) {
        Ok(_) => tracing::info!("pod '{pod_name}': status updated to Running (ip={pod_ip})"),
        Err(e) => tracing::debug!("pod '{pod_name}': status update conflict: {e}"),
    }
}

async fn handle_pod_deleted<R: ContainerRuntime>(
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    ip_pool: &mut IpPool,
    pod_value: &serde_json::Value,
    data_dir: &PathBuf,
) {
    let pod: Pod = match serde_json::from_value(pod_value.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    if pod.spec.node_name.as_deref() != Some(NODE_NAME) {
        return;
    }

    let pod_uid = match pod.metadata.uid.as_deref() {
        Some(u) => u.to_string(),
        None => return,
    };
    let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");

    if let Some(pc) = pod_containers.remove(&pod_uid) {
        for cid in &pc.container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(10)).await;
            let _ = runtime.remove_container(cid).await;
        }
        ip_pool.release(pc.pod_ip_num);
        r8s_network::bridge::teardown_pod_network(pod_name);
        cleanup_pod_volumes(data_dir, &pod_uid);
        tracing::info!(
            "pod '{pod_name}: cleaned up {} containers, released IP 10.244.0.{}",
            pc.container_ids.len(),
            pc.pod_ip_num
        );
    }
}

async fn check_health<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    pod_containers: &mut FxHashMap<String, PodContainers>,
    ip_pool: &mut IpPool,
    data_dir: &PathBuf,
) {
    let mut crashed: Vec<String> = Vec::new();

    for (pod_uid, pc) in pod_containers.iter() {
        for cid in &pc.container_ids {
            match runtime.container_status(cid).await {
                Ok(status) if !status.running => {
                    tracing::warn!(
                        "pod '{}': container {} exited (code={:?})",
                        pc.pod_name,
                        cid.0,
                        status.exit_code
                    );
                    crashed.push(pod_uid.clone());
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "pod '{}': container {} status check failed: {e}",
                        pc.pod_name,
                        cid.0
                    );
                    crashed.push(pod_uid.clone());
                    break;
                }
                _ => {}
            }
        }
    }

    for pod_uid in crashed {
        let pc = match pod_containers.remove(&pod_uid) {
            Some(pc) => pc,
            None => continue,
        };

        // Clean up runtime resources
        for cid in &pc.container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
            let _ = runtime.remove_container(cid).await;
        }
        ip_pool.release(pc.pod_ip_num);
        r8s_network::bridge::teardown_pod_network(&pc.pod_name);
        cleanup_pod_volumes(data_dir, &pod_uid);

        // Update pod status to Failed, then delete so RS controller creates a replacement
        let gvr = pods_gvr();
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: pc.pod_ns.as_deref(),
            name: &pc.pod_name,
        };
        if let Ok(Some(mut pod)) = store.get(&resource_ref) {
            pod["status"]["phase"] = serde_json::json!("Failed");
            let _ = store.update(&resource_ref, &pod);
        }
        let _ = store.delete(&resource_ref);

        tracing::info!(
            "pod '{}': crashed, cleaned up and deleted (ip=10.244.0.{})",
            pc.pod_name,
            pc.pod_ip_num
        );
    }
}

fn configmaps_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "configmaps")
}

fn secrets_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "secrets")
}

/// Prepare host directories for each volume declared in the pod spec.
/// Returns a map of volume name → host path.
fn prepare_volumes(
    store: &Store,
    data_dir: &PathBuf,
    pod_uid: &str,
    pod_ns: Option<&str>,
    volumes: &[Volume],
) -> anyhow::Result<FxHashMap<String, String>> {
    let mut paths = FxHashMap::default();

    for vol in volumes {
        let host_path = if vol.empty_dir.is_some() {
            let dir = data_dir
                .join("pod-data")
                .join(pod_uid)
                .join(&vol.name);
            std::fs::create_dir_all(&dir)?;
            dir.to_string_lossy().to_string()
        } else if let Some(hp) = &vol.host_path {
            hp.path.clone()
        } else if let Some(cm_src) = &vol.config_map {
            let dir = data_dir
                .join("pod-data")
                .join(pod_uid)
                .join(&vol.name);
            std::fs::create_dir_all(&dir)?;
            project_configmap(store, pod_ns, &cm_src.name, &dir)?;
            dir.to_string_lossy().to_string()
        } else if let Some(secret_src) = &vol.secret {
            let dir = data_dir
                .join("pod-data")
                .join(pod_uid)
                .join(&vol.name);
            std::fs::create_dir_all(&dir)?;
            project_secret(store, pod_ns, &secret_src.secret_name, &dir)?;
            dir.to_string_lossy().to_string()
        } else {
            tracing::warn!("volume '{}': unsupported volume source, skipping", vol.name);
            continue;
        };

        paths.insert(vol.name.clone(), host_path);
    }

    Ok(paths)
}

/// Read a ConfigMap from the store and write its data entries as files.
fn project_configmap(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &std::path::Path,
) -> anyhow::Result<()> {
    let gvr = configmaps_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let cm = store
        .get(&resource_ref)?
        .ok_or_else(|| anyhow::anyhow!("configmap '{name}' not found"))?;
    if let Some(data) = cm["data"].as_object() {
        for (key, value) in data {
            if let Some(s) = value.as_str() {
                std::fs::write(dir.join(key), s)?;
            }
        }
    }
    Ok(())
}

/// Read a Secret from the store and write its data entries as files (base64-decoded).
fn project_secret(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &std::path::Path,
) -> anyhow::Result<()> {
    let gvr = secrets_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let secret = store
        .get(&resource_ref)?
        .ok_or_else(|| anyhow::anyhow!("secret '{name}' not found"))?;
    if let Some(data) = secret["data"].as_object() {
        for (key, value) in data {
            if let Some(b64) = value.as_str() {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .unwrap_or_else(|_| b64.as_bytes().to_vec());
                std::fs::write(dir.join(key), bytes)?;
            }
        }
    }
    Ok(())
}

/// Resolve a container's volumeMounts into runtime Mount entries.
fn resolve_mounts(
    volume_mounts: &[VolumeMount],
    volume_paths: &FxHashMap<String, String>,
) -> Vec<Mount> {
    volume_mounts
        .iter()
        .filter_map(|vm| {
            let host_path = volume_paths.get(&vm.name)?;
            Some(Mount {
                host_path: host_path.clone(),
                container_path: vm.mount_path.clone(),
                readonly: vm.read_only.unwrap_or(false),
            })
        })
        .collect()
}

/// Remove the pod's volume data directory.
fn cleanup_pod_volumes(data_dir: &PathBuf, pod_uid: &str) {
    let pod_data_dir = data_dir.join("pod-data").join(pod_uid);
    if pod_data_dir.exists() {
        let _ = std::fs::remove_dir_all(&pod_data_dir);
    }
}
