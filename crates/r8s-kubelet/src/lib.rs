use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::Engine;
use r8s_runtime::{ContainerConfig, ContainerId, ContainerRuntime, Mount, RegistryAuth};
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    ContainerState, ContainerStateRunning, ContainerStatus, GroupVersionResource, Pod,
    PodCondition, PodIP, PodStatus, Time, Volume, VolumeMount,
};
use rustc_hash::FxHashMap;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

struct TrackedPod {
    container_ids: Vec<ContainerId>,
    ip_num: u32,
    name: String,
    namespace: Option<String>,
}

struct PodState {
    pods: FxHashMap<String, TrackedPod>,
    ip_pool: IpPool,
}

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

pub async fn run<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    run_with_config(store, runtime, shutdown, data_dir, Duration::from_secs(10)).await
}

pub async fn run_with_config<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
    health_interval: Duration,
) -> anyhow::Result<()> {
    tracing::info!("kubelet started for node '{NODE_NAME}'");

    let state = Arc::new(Mutex::new(PodState {
        pods: FxHashMap::default(),
        ip_pool: IpPool::new(),
    }));

    {
        let mut s = state.lock().await;
        reconcile_all(&store, &*runtime, &mut s, &data_dir).await;
    }

    let mut rx = store.watch(&GroupVersionResource::pods());
    let mut ticker = tokio::time::interval(health_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("kubelet shutting down");
                let s = state.lock().await;
                for tracked in s.pods.values() {
                    for cid in &tracked.container_ids {
                        let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
                        let _ = runtime.remove_container(cid).await;
                    }
                }
                return Ok(());
            }
            _ = ticker.tick() => {
                let mut s = state.lock().await;
                check_health(&store, &*runtime, &mut s, &data_dir).await;
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let state = state.clone();
                        let store = store.clone();
                        let runtime = runtime.clone();
                        let data_dir = data_dir.clone();
                        tokio::spawn(async move {
                            let mut s = state.lock().await;
                            match event.event_type {
                                WatchEventType::Added | WatchEventType::Modified => {
                                    reconcile_pod(&store, &*runtime, &mut s, &event.object, &data_dir).await;
                                }
                                WatchEventType::Deleted => {
                                    handle_delete(&*runtime, &mut s, &event.object, &data_dir).await;
                                }
                            }
                        });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("kubelet lagged, re-syncing");
                        let mut s = state.lock().await;
                        reconcile_all(&store, &*runtime, &mut s, &data_dir).await;
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
    state: &mut PodState,
    data_dir: &Path,
) {
    let result = match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kubelet list error: {e}");
            return;
        }
    };
    for pod in &result.items {
        reconcile_pod(store, runtime, state, pod, data_dir).await;
    }
}

async fn reconcile_pod<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &mut PodState,
    pod_value: &serde_json::Value,
    data_dir: &Path,
) {
    let pod: Pod = match serde_json::from_value(pod_value.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    if pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(NODE_NAME) {
        return;
    }

    if let Some("Succeeded" | "Failed") = pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
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

    if let Some(tracked) = state.pods.get(&pod_uid) {
        if pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
            return;
        }
        update_pod_status(store, tracked);
        return;
    }

    let spec = match pod.spec.as_ref() {
        Some(s) => s,
        None => return,
    };

    if spec.containers.is_empty() {
        tracing::warn!("pod '{pod_name}' has no containers in spec");
        return;
    }

    let volumes = spec.volumes.as_deref().unwrap_or_default();
    let volume_paths = match prepare_volumes(store, data_dir, &pod_uid, pod_ns, volumes) {
        Ok(paths) => paths,
        Err(e) => {
            tracing::error!("pod '{pod_name}': volume preparation failed: {e}");
            return;
        }
    };

    let ip_num = match state.ip_pool.allocate() {
        Some(ip) => ip,
        None => {
            tracing::error!("pod '{pod_name}': IP pool exhausted");
            return;
        }
    };

    let mut container_ids: Vec<ContainerId> = Vec::new();
    let mut failed = false;

    for container_spec in &spec.containers {
        let container_name = &container_spec.name;
        let image = container_spec.image.as_deref().unwrap_or("unknown");

        let pull_policy = container_spec.image_pull_policy.as_deref().unwrap_or(
            if image.ends_with(":latest") || !image.contains(':') {
                "Always"
            } else {
                "IfNotPresent"
            },
        );

        let should_pull = match pull_policy {
            "Never" => false,
            "IfNotPresent" => !runtime.has_image(image).await,
            _ => true,
        };

        if should_pull {
            let auth = resolve_image_auth(store, pod_ns, pod_value, image);
            if let Err(e) = runtime.pull_image(image, auth.as_ref()).await {
                tracing::error!("pod '{pod_name}': failed to pull image '{image}': {e}");
                failed = true;
                break;
            }
        }

        let mounts = resolve_mounts(
            container_spec.volume_mounts.as_deref().unwrap_or_default(),
            &volume_paths,
        );

        let mut env: Vec<(String, String)> = container_spec
            .env
            .as_ref()
            .map(|envs| {
                envs.iter()
                    .map(|e| (e.name.clone(), e.value.clone().unwrap_or_default()))
                    .collect()
            })
            .unwrap_or_default();
        env.push(("KUBERNETES_SERVICE_HOST".into(), "10.244.0.1".into()));
        env.push(("KUBERNETES_SERVICE_PORT".into(), "443".into()));
        env.push(("KUBERNETES_SERVICE_PORT_HTTPS".into(), "443".into()));

        let config = ContainerConfig {
            name: format!("{pod_name}_{container_name}"),
            namespace: pod_ns.unwrap_or("default").to_string(),
            image: image.to_string(),
            command: container_spec.command.clone().unwrap_or_default(),
            args: container_spec.args.clone().unwrap_or_default(),
            env,
            working_dir: container_spec.working_dir.clone(),
            mounts,
        };

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

    if failed {
        for cid in &container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
            let _ = runtime.remove_container(cid).await;
        }
        state.ip_pool.release(ip_num);
        cleanup_pod_volumes(data_dir, &pod_uid);
        tracing::warn!(
            "pod '{pod_name}': rolled back {} containers",
            container_ids.len()
        );
        return;
    }

    if !container_ids.is_empty() {
        match runtime.container_pid(&container_ids[0]).await {
            Ok(pid) => {
                let pod_ip = format!("10.244.0.{ip_num}");
                if let Err(e) = r8s_network::bridge::setup_pod_network(pid, &pod_ip, pod_name) {
                    tracing::error!("pod '{pod_name}': network setup failed: {e}");
                }
            }
            Err(e) => tracing::warn!("pod '{pod_name}': network setup failed: {e}"),
        }
    }

    let tracked = TrackedPod {
        container_ids,
        ip_num,
        name: pod_name.to_string(),
        namespace: pod_ns.map(String::from),
    };
    update_pod_status(store, &tracked);
    state.pods.insert(pod_uid, tracked);
}

fn update_pod_status(store: &Store, tracked: &TrackedPod) {
    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: tracked.namespace.as_deref(),
        name: &tracked.name,
    };

    let mut current = match store.get(&resource_ref) {
        Ok(Some(p)) => p,
        _ => return,
    };

    let current_pod: Pod = match serde_json::from_value(current.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    let now = Time(chrono::Utc::now());
    let pod_ip = format!("10.244.0.{}", tracked.ip_num);

    let containers = current_pod
        .spec
        .as_ref()
        .map(|s| s.containers.as_slice())
        .unwrap_or_default();

    let container_statuses: Vec<ContainerStatus> = containers
        .iter()
        .zip(tracked.container_ids.iter())
        .map(|(spec, cid)| {
            let image = spec.image.clone().unwrap_or_default();
            ContainerStatus {
                name: spec.name.clone(),
                image: image.clone(),
                image_id: image,
                container_id: Some(cid.0.clone()),
                ready: true,
                started: Some(true),
                restart_count: 0,
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning {
                        started_at: Some(now.clone()),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }
        })
        .collect();

    let mut conditions = current_pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.clone())
        .unwrap_or_default();

    for type_name in ["Ready", "Initialized", "ContainersReady"] {
        if !conditions.iter().any(|c| c.type_ == type_name) {
            conditions.push(PodCondition {
                type_: type_name.into(),
                status: "True".into(),
                last_transition_time: Some(now.clone()),
                ..Default::default()
            });
        }
    }

    let status = PodStatus {
        phase: Some("Running".into()),
        host_ip: Some("127.0.0.1".into()),
        pod_ip: Some(pod_ip.clone()),
        pod_ips: Some(vec![PodIP { ip: pod_ip.clone() }]),
        start_time: Some(now),
        conditions: Some(conditions),
        container_statuses: Some(container_statuses),
        ..Default::default()
    };
    current["status"] = serde_json::to_value(&status).unwrap_or_default();

    match store.update(&resource_ref, &current) {
        Ok(_) => tracing::info!(
            "pod '{}': status updated to Running (ip={pod_ip})",
            tracked.name
        ),
        Err(e) => tracing::debug!("pod '{}': status update conflict: {e}", tracked.name),
    }
}

async fn handle_delete<R: ContainerRuntime>(
    runtime: &R,
    state: &mut PodState,
    pod_value: &serde_json::Value,
    data_dir: &Path,
) {
    let pod: Pod = match serde_json::from_value(pod_value.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };

    if pod.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(NODE_NAME) {
        return;
    }

    let pod_uid = match pod.metadata.uid.as_deref() {
        Some(u) => u.to_string(),
        None => return,
    };
    let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");

    let tracked = match state.pods.remove(&pod_uid) {
        Some(t) => t,
        None => return,
    };

    for cid in &tracked.container_ids {
        let _ = runtime.stop_container(cid, Duration::from_secs(10)).await;
        let _ = runtime.remove_container(cid).await;
    }
    state.ip_pool.release(tracked.ip_num);
    r8s_network::bridge::teardown_pod_network(pod_name);
    cleanup_pod_volumes(data_dir, &pod_uid);
    tracing::info!(
        "pod '{pod_name}': cleaned up {} containers, released IP 10.244.0.{}",
        tracked.container_ids.len(),
        tracked.ip_num
    );
}

async fn check_health<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &mut PodState,
    data_dir: &Path,
) {
    let mut crashed: Vec<(String, Option<i32>)> = Vec::new();

    for (pod_uid, tracked) in &state.pods {
        for cid in &tracked.container_ids {
            match runtime.container_status(cid).await {
                Ok(status) if !status.running => {
                    tracing::warn!(
                        "pod '{}': container {} exited (code={:?})",
                        tracked.name,
                        cid.0,
                        status.exit_code
                    );
                    crashed.push((pod_uid.clone(), status.exit_code));
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "pod '{}': container {} status check failed: {e}",
                        tracked.name,
                        cid.0
                    );
                    crashed.push((pod_uid.clone(), None));
                    break;
                }
                _ => {}
            }
        }
    }

    for (pod_uid, exit_code) in crashed {
        let tracked = match state.pods.remove(&pod_uid) {
            Some(t) => t,
            None => continue,
        };

        for cid in &tracked.container_ids {
            let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
            let _ = runtime.remove_container(cid).await;
        }
        state.ip_pool.release(tracked.ip_num);
        r8s_network::bridge::teardown_pod_network(&tracked.name);
        cleanup_pod_volumes(data_dir, &pod_uid);

        let gvr = GroupVersionResource::pods();
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: tracked.namespace.as_deref(),
            name: &tracked.name,
        };

        let restart_policy = store
            .get(&resource_ref)
            .ok()
            .flatten()
            .and_then(|p| p["spec"]["restartPolicy"].as_str().map(String::from))
            .unwrap_or_else(|| "Always".to_string());

        let succeeded = exit_code == Some(0);

        match restart_policy.as_str() {
            // Keep in store for Job controller to observe completion
            "Never" => {
                let phase = if succeeded { "Succeeded" } else { "Failed" };
                if let Ok(Some(mut pod)) = store.get(&resource_ref) {
                    pod["status"]["phase"] = serde_json::json!(phase);
                    let _ = store.update(&resource_ref, &pod);
                }
                tracing::info!(
                    "pod '{}': exited (code={:?}), phase={phase}, kept in store (restartPolicy=Never)",
                    tracked.name,
                    exit_code,
                );
            }
            "OnFailure" if succeeded => {
                if let Ok(Some(mut pod)) = store.get(&resource_ref) {
                    pod["status"]["phase"] = serde_json::json!("Succeeded");
                    let _ = store.update(&resource_ref, &pod);
                }
                tracing::info!(
                    "pod '{}': succeeded, kept in store (restartPolicy=OnFailure)",
                    tracked.name,
                );
            }
            // Delete so controller recreates
            _ => {
                if let Ok(Some(mut pod)) = store.get(&resource_ref) {
                    pod["status"]["phase"] = serde_json::json!("Failed");
                    let _ = store.update(&resource_ref, &pod);
                }
                let _ = store.delete(&resource_ref);
                tracing::info!(
                    "pod '{}': crashed (code={:?}), cleaned up and deleted (ip=10.244.0.{})",
                    tracked.name,
                    exit_code,
                    tracked.ip_num
                );
            }
        }
    }
}

fn registry_host(image: &str) -> &str {
    let host = image.split('/').next().unwrap_or(image);
    if host.contains('.') || host.contains(':') {
        host
    } else {
        "docker.io"
    }
}

fn resolve_image_auth(
    store: &Store,
    namespace: Option<&str>,
    pod_value: &serde_json::Value,
    image: &str,
) -> Option<RegistryAuth> {
    resolve_pull_secrets_auth(store, namespace, pod_value, image)
        .or_else(|| resolve_docker_config_auth(image))
}

fn resolve_pull_secrets_auth(
    store: &Store,
    namespace: Option<&str>,
    pod_value: &serde_json::Value,
    image: &str,
) -> Option<RegistryAuth> {
    let pull_secrets = pod_value["spec"]["imagePullSecrets"].as_array()?;
    let host = registry_host(image);
    let gvr = GroupVersionResource::secrets();

    for entry in pull_secrets {
        let secret_name = match entry["name"].as_str() {
            Some(n) => n,
            None => continue,
        };
        let rref = ResourceRef {
            gvr: &gvr,
            namespace,
            name: secret_name,
        };
        let secret = match store.get(&rref) {
            Ok(Some(s)) => s,
            _ => continue,
        };
        let b64 = match secret["data"][".dockerconfigjson"].as_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some(auth) = parse_docker_config_b64(b64, host) {
            return Some(auth);
        }
    }
    None
}

fn resolve_docker_config_auth(image: &str) -> Option<RegistryAuth> {
    let host = registry_host(image);
    let paths = [
        PathBuf::from("/root/.docker/config.json"),
        std::env::var("SUDO_USER")
            .ok()
            .map(|u| PathBuf::from(format!("/home/{u}/.docker/config.json")))
            .unwrap_or_default(),
    ];
    for path in &paths {
        if let Ok(content) = std::fs::read_to_string(path)
            && let Ok(config) = serde_json::from_str::<serde_json::Value>(&content)
            && let Some(auth) = find_registry_auth(&config, host)
        {
            return Some(auth);
        }
    }
    None
}

fn parse_docker_config_b64(b64: &str, host: &str) -> Option<RegistryAuth> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let config: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    find_registry_auth(&config, host)
}

fn find_registry_auth(config: &serde_json::Value, host: &str) -> Option<RegistryAuth> {
    let auths = config["auths"].as_object()?;
    let https_host = format!("https://{host}");
    let https_host_v1 = format!("https://{host}/v1/");
    for key in [host, https_host.as_str(), https_host_v1.as_str()] {
        if let Some(entry) = auths.get(key) {
            if let Some(auth_b64) = entry["auth"].as_str().filter(|s| !s.is_empty())
                && let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(auth_b64)
            {
                let decoded = String::from_utf8_lossy(&decoded);
                if let Some((u, p)) = decoded.split_once(':') {
                    return Some(RegistryAuth {
                        username: u.to_string(),
                        password: p.to_string(),
                    });
                }
            }
            let username = entry["username"].as_str().unwrap_or_default().to_string();
            let password = entry["password"].as_str().unwrap_or_default().to_string();
            if !username.is_empty() {
                return Some(RegistryAuth { username, password });
            }
        }
    }
    None
}

fn prepare_volumes(
    store: &Store,
    data_dir: &Path,
    pod_uid: &str,
    pod_ns: Option<&str>,
    volumes: &[Volume],
) -> anyhow::Result<FxHashMap<String, String>> {
    let mut paths = FxHashMap::default();
    let vol_dir = |name: &str| data_dir.join("pod-data").join(pod_uid).join(name);

    for vol in volumes {
        let host_path = if vol.empty_dir.is_some() {
            let dir = vol_dir(&vol.name);
            std::fs::create_dir_all(&dir)?;
            dir.to_string_lossy().to_string()
        } else if let Some(hp) = &vol.host_path {
            hp.path.clone()
        } else if let Some(cm_src) = &vol.config_map {
            let dir = vol_dir(&vol.name);
            std::fs::create_dir_all(&dir)?;
            project_configmap(store, pod_ns, &cm_src.name, &dir)?;
            dir.to_string_lossy().to_string()
        } else if let Some(secret_src) = &vol.secret {
            let dir = vol_dir(&vol.name);
            std::fs::create_dir_all(&dir)?;
            let secret_name = secret_src.secret_name.as_deref().unwrap_or_default();
            project_secret(store, pod_ns, secret_name, &dir)?;
            dir.to_string_lossy().to_string()
        } else {
            tracing::warn!("volume '{}': unsupported volume source, skipping", vol.name);
            continue;
        };

        paths.insert(vol.name.clone(), host_path);
    }

    Ok(paths)
}

fn project_configmap(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &Path,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::configmaps();
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

fn project_secret(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &Path,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::secrets();
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
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .unwrap_or_else(|_| b64.as_bytes().to_vec());
                std::fs::write(dir.join(key), bytes)?;
            }
        }
    }
    Ok(())
}

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

fn cleanup_pod_volumes(data_dir: &Path, pod_uid: &str) {
    let dir = data_dir.join("pod-data").join(pod_uid);
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
