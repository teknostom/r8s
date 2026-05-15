mod probe;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::Engine;
use r8s_runtime::{ContainerConfig, ContainerId, ContainerRuntime, Mount, RegistryAuth};
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting,
    ContainerStatus, GroupVersionResource, Pod, PodCondition, PodIP, PodSpec, PodStatus, Time,
    Volume, VolumeMount,
};
use rustc_hash::FxHashMap;
use tokio::sync::{Mutex, broadcast};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);

struct ProbeState {
    last_check: Instant,
    consecutive_failures: i32,
}

struct TrackedPod {
    container_ids: Vec<ContainerId>,
    ip_num: u32,
    name: String,
    namespace: Option<String>,
    restart_count: i32,
    last_restart: Option<Instant>,
    /// True when containers have crashed and we're waiting for backoff before restart.
    waiting_restart: bool,
    /// Exit code from the most recent crash (used for Terminated last_state).
    last_exit_code: Option<i32>,
    liveness: Option<ProbeState>,
    readiness: Option<ProbeState>,
    startup: Option<ProbeState>,
    started: bool, // true once startup probe succeeds (or immediately if no startup probe)
    ready: bool,
}

type SharedPod = Arc<Mutex<TrackedPod>>;

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

struct PodState {
    pods: Mutex<FxHashMap<String, SharedPod>>,
    ip_pool: Mutex<IpPool>,
}

impl PodState {
    fn new() -> Self {
        Self {
            pods: Mutex::new(FxHashMap::default()),
            ip_pool: Mutex::new(IpPool::new()),
        }
    }

    async fn get(&self, uid: &str) -> Option<SharedPod> {
        self.pods.lock().await.get(uid).cloned()
    }

    async fn insert(&self, uid: String, tracked: TrackedPod) -> SharedPod {
        let arc = Arc::new(Mutex::new(tracked));
        self.pods.lock().await.insert(uid, arc.clone());
        arc
    }

    async fn remove(&self, uid: &str) -> Option<SharedPod> {
        self.pods.lock().await.remove(uid)
    }

    async fn snapshot(&self) -> Vec<(String, SharedPod)> {
        self.pods
            .lock()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn allocate_ip(&self) -> Option<u32> {
        self.ip_pool.lock().await.allocate()
    }

    async fn release_ip(&self, ip: u32) {
        self.ip_pool.lock().await.release(ip);
    }
}

fn restart_backoff(restart_count: i32) -> Duration {
    if restart_count <= 0 {
        return Duration::ZERO;
    }
    let secs = INITIAL_BACKOFF
        .as_secs()
        .saturating_mul(1u64 << (restart_count - 1).min(30));
    Duration::from_secs(secs).min(MAX_BACKOFF)
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

    let state = Arc::new(PodState::new());

    reconcile_all(&store, &*runtime, &state, &data_dir).await;

    let mut rx = store.watch(&GroupVersionResource::pods());
    let mut ticker = tokio::time::interval(health_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("kubelet shutting down");
                let snap = state.snapshot().await;
                for (_, pod_arc) in snap {
                    let tracked = pod_arc.lock().await;
                    for cid in &tracked.container_ids {
                        let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
                        let _ = runtime.remove_container(cid).await;
                    }
                }
                return Ok(());
            }
            _ = ticker.tick() => {
                let state = state.clone();
                let runtime = runtime.clone();
                let store = store.clone();
                let data_dir = data_dir.clone();
                tokio::spawn(async move {
                    check_health(&store, &*runtime, &state, &data_dir).await;
                });
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let state = state.clone();
                        let store = store.clone();
                        let runtime = runtime.clone();
                        let data_dir = data_dir.clone();
                        tokio::spawn(async move {
                            match event.event_type {
                                WatchEventType::Added | WatchEventType::Modified => {
                                    reconcile_pod(&store, &*runtime, &state, &event.object, &data_dir).await;
                                }
                                WatchEventType::Deleted => {
                                    handle_delete(&*runtime, &state, &event.object, &data_dir).await;
                                }
                            }
                        });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("kubelet lagged, re-syncing");
                        reconcile_all(&store, &*runtime, &state, &data_dir).await;
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
    state: &PodState,
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
    state: &PodState,
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

    if let Some(pod_arc) = state.get(&pod_uid).await {
        let tracked = pod_arc.lock().await;
        if pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
            return;
        }
        update_pod_status(store, &tracked);
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

    let ip_num = match state.allocate_ip().await {
        Some(ip) => ip,
        None => {
            tracing::error!("pod '{pod_name}': IP pool exhausted");
            return;
        }
    };

    let container_ids = match start_containers(
        runtime,
        store,
        pod_name,
        pod_ns,
        pod_value,
        spec,
        &volume_paths,
    )
    .await
    {
        Ok(ids) => ids,
        Err(ids) => {
            for cid in &ids {
                let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
                let _ = runtime.remove_container(cid).await;
            }
            state.release_ip(ip_num).await;
            cleanup_pod_volumes(data_dir, &pod_uid);
            tracing::warn!("pod '{pod_name}': rolled back {} containers", ids.len());
            return;
        }
    };

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

    let has_liveness = spec.containers.iter().any(|c| c.liveness_probe.is_some());
    let has_readiness = spec.containers.iter().any(|c| c.readiness_probe.is_some());
    let has_startup = spec.containers.iter().any(|c| c.startup_probe.is_some());

    let tracked = TrackedPod {
        container_ids,
        ip_num,
        name: pod_name.to_string(),
        namespace: pod_ns.map(String::from),
        restart_count: 0,
        last_restart: None,
        waiting_restart: false,
        last_exit_code: None,
        liveness: if has_liveness {
            Some(ProbeState {
                last_check: Instant::now(),
                consecutive_failures: 0,
            })
        } else {
            None
        },
        readiness: if has_readiness {
            Some(ProbeState {
                last_check: Instant::now(),
                consecutive_failures: 0,
            })
        } else {
            None
        },
        startup: if has_startup {
            Some(ProbeState {
                last_check: Instant::now(),
                consecutive_failures: 0,
            })
        } else {
            None
        },
        started: !has_startup,
        ready: !has_readiness && !has_startup,
    };
    update_pod_status(store, &tracked);
    state.insert(pod_uid, tracked).await;
}

/// Start all containers for a pod. Returns Ok(ids) on success, Err(partial_ids) on failure.
async fn start_containers<R: ContainerRuntime>(
    runtime: &R,
    store: &Store,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_value: &serde_json::Value,
    spec: &PodSpec,
    volume_paths: &FxHashMap<String, String>,
) -> Result<Vec<ContainerId>, Vec<ContainerId>> {
    let mut container_ids: Vec<ContainerId> = Vec::new();

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
                return Err(container_ids);
            }
        } else if pull_policy == "IfNotPresent" {
            tracing::info!("pod '{pod_name}': image '{image}' present locally, skipping pull");
        }

        let mounts = resolve_mounts(
            container_spec.volume_mounts.as_deref().unwrap_or_default(),
            volume_paths,
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
                return Err(container_ids);
            }
        };

        container_ids.push(container_id.clone());

        if let Err(e) = runtime.start_container(&container_id).await {
            tracing::error!("pod '{pod_name}': failed to start container '{container_name}': {e}");
            return Err(container_ids);
        }

        tracing::info!(
            "pod '{pod_name}': started container '{container_name}' (id={})",
            container_id.0
        );
    }

    Ok(container_ids)
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

    let existing_statuses = current_pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref());

    let container_statuses: Vec<ContainerStatus> = containers
        .iter()
        .zip(tracked.container_ids.iter())
        .map(|(spec, cid)| {
            let image = spec.image.clone().unwrap_or_default();
            let existing = existing_statuses.and_then(|cs| cs.iter().find(|c| c.name == spec.name));

            if tracked.waiting_restart {
                // Container has crashed — show Waiting/CrashLoopBackOff state
                let terminated_state = ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: tracked.last_exit_code.unwrap_or(1),
                        reason: Some(
                            if tracked.last_exit_code == Some(0) {
                                "Completed"
                            } else {
                                "Error"
                            }
                            .into(),
                        ),
                        finished_at: Some(now.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let waiting_state = ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("CrashLoopBackOff".into()),
                        message: Some(format!(
                            "back-off {}s restarting failed container",
                            restart_backoff(tracked.restart_count).as_secs()
                        )),
                    }),
                    ..Default::default()
                };
                ContainerStatus {
                    name: spec.name.clone(),
                    image: image.clone(),
                    image_id: image,
                    container_id: Some(cid.0.clone()),
                    ready: false,
                    started: Some(false),
                    restart_count: tracked.restart_count,
                    state: Some(waiting_state),
                    last_state: Some(terminated_state),
                    ..Default::default()
                }
            } else {
                // Container is running normally
                let started_at = existing
                    .and_then(|c| c.state.as_ref())
                    .and_then(|s| s.running.as_ref())
                    .and_then(|r| r.started_at.clone())
                    .unwrap_or_else(|| now.clone());
                // Preserve last_state from previous crash if any
                let last_state = existing.and_then(|c| c.last_state.clone());
                ContainerStatus {
                    name: spec.name.clone(),
                    image: image.clone(),
                    image_id: image,
                    container_id: Some(cid.0.clone()),
                    ready: tracked.ready,
                    started: Some(true),
                    restart_count: tracked.restart_count,
                    state: Some(ContainerState {
                        running: Some(ContainerStateRunning {
                            started_at: Some(started_at),
                        }),
                        ..Default::default()
                    }),
                    last_state,
                    ..Default::default()
                }
            }
        })
        .collect();

    let mut conditions = current_pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.clone())
        .unwrap_or_default();

    let ready_status = if tracked.ready { "True" } else { "False" };
    for type_name in ["Initialized", "ContainersReady", "Ready"] {
        let status_val = if type_name == "Initialized" {
            "True"
        } else {
            ready_status
        };
        if let Some(cond) = conditions.iter_mut().find(|c| c.type_ == type_name) {
            if cond.status != status_val {
                cond.status = status_val.into();
                cond.last_transition_time = Some(now.clone());
            }
        } else {
            conditions.push(PodCondition {
                type_: type_name.into(),
                status: status_val.into(),
                last_transition_time: Some(now.clone()),
                ..Default::default()
            });
        }
    }

    let start_time = current_pod
        .status
        .as_ref()
        .and_then(|s| s.start_time.clone())
        .unwrap_or_else(|| now.clone());

    let status = PodStatus {
        phase: Some("Running".into()),
        host_ip: Some("127.0.0.1".into()),
        pod_ip: Some(pod_ip.clone()),
        pod_ips: Some(vec![PodIP { ip: pod_ip.clone() }]),
        start_time: Some(start_time),
        conditions: Some(conditions),
        container_statuses: Some(container_statuses),
        ..Default::default()
    };
    if let Some(obj) = current.as_object_mut() {
        obj.insert(
            "status".to_string(),
            serde_json::to_value(&status).unwrap_or_default(),
        );
    }

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
    state: &PodState,
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
    let pod_name = pod
        .metadata
        .name
        .as_deref()
        .unwrap_or("unknown")
        .to_string();

    let pod_arc = match state.remove(&pod_uid).await {
        Some(a) => a,
        None => return,
    };
    let tracked = pod_arc.lock().await;

    for cid in &tracked.container_ids {
        let _ = runtime.stop_container(cid, Duration::from_secs(10)).await;
        let _ = runtime.remove_container(cid).await;
    }
    state.release_ip(tracked.ip_num).await;
    r8s_network::bridge::teardown_pod_network(&pod_name);
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
    state: &PodState,
    data_dir: &Path,
) {
    let snap = state.snapshot().await;
    let mut crashed: Vec<(String, Option<i32>)> = Vec::new();

    for (pod_uid, pod_arc) in &snap {
        let tracked = pod_arc.lock().await;
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
        handle_crash(store, runtime, state, data_dir, &pod_uid, exit_code).await;
    }

    run_probes(store, runtime, state, data_dir).await;
}

async fn handle_crash<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    pod_uid: &str,
    exit_code: Option<i32>,
) {
    let pod_arc = match state.get(pod_uid).await {
        Some(a) => a,
        None => return,
    };

    let (pod_ns, pod_name) = {
        let t = pod_arc.lock().await;
        (t.namespace.clone(), t.name.clone())
    };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns.as_deref(),
        name: &pod_name,
    };
    let restart_policy = store
        .get(&resource_ref)
        .ok()
        .flatten()
        .and_then(|p| {
            p.get("spec")
                .and_then(|s| s.get("restartPolicy"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .unwrap_or_else(|| "Always".to_string());

    let succeeded = exit_code == Some(0);
    let should_restart = match restart_policy.as_str() {
        "Never" => false,
        "OnFailure" => !succeeded,
        _ => true, // "Always"
    };

    if should_restart {
        // Mark as crashed/waiting so status reflects CrashLoopBackOff during backoff
        if let Some(pod_arc) = state.get(pod_uid).await {
            let mut tracked = pod_arc.lock().await;
            tracked.waiting_restart = true;
            tracked.last_exit_code = exit_code;
            tracked.ready = false;
            update_pod_status(store, &tracked);
        }
        restart_in_place(store, runtime, state, data_dir, pod_uid).await;
    } else {
        terminate_pod(store, runtime, state, data_dir, pod_uid, exit_code).await;
    }
}

async fn restart_in_place<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    pod_uid: &str,
) {
    let pod_arc = match state.get(pod_uid).await {
        Some(a) => a,
        None => return,
    };

    // Snapshot what we need under the lock; release it for long awaits below.
    let (pod_name, pod_ns, ip_num, old_container_ids, prev_restart_count) = {
        let t = pod_arc.lock().await;
        let backoff = restart_backoff(t.restart_count);
        if let Some(last) = t.last_restart
            && last.elapsed() < backoff
        {
            return; // Not enough time elapsed, retry next tick
        }
        (
            t.name.clone(),
            t.namespace.clone(),
            t.ip_num,
            t.container_ids.clone(),
            t.restart_count,
        )
    };

    // Stop & remove old containers
    for cid in &old_container_ids {
        let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
        let _ = runtime.remove_container(cid).await;
    }

    // Tear down the dangling host-side veth so the next setup_pod_network can recreate it.
    r8s_network::bridge::teardown_pod_network(&pod_name);

    // Re-read pod spec from store
    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns.as_deref(),
        name: &pod_name,
    };
    let pod_value = match store.get(&resource_ref) {
        Ok(Some(v)) => v,
        _ => return,
    };
    let pod: Pod = match serde_json::from_value(pod_value.clone()) {
        Ok(p) => p,
        Err(_) => return,
    };
    let spec = match pod.spec.as_ref() {
        Some(s) => s,
        None => return,
    };

    let volumes = spec.volumes.as_deref().unwrap_or_default();
    let volume_paths = match prepare_volumes(store, data_dir, pod_uid, pod_ns.as_deref(), volumes) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("pod '{pod_name}': volume prep failed on restart: {e}");
            return;
        }
    };

    let new_ids = match start_containers(
        runtime,
        store,
        &pod_name,
        pod_ns.as_deref(),
        &pod_value,
        spec,
        &volume_paths,
    )
    .await
    {
        Ok(ids) => ids,
        Err(partial) => {
            for cid in &partial {
                let _ = runtime.stop_container(cid, Duration::from_secs(5)).await;
                let _ = runtime.remove_container(cid).await;
            }
            tracing::error!("pod '{pod_name}': restart failed, will retry next tick");
            return;
        }
    };

    if !new_ids.is_empty() {
        match runtime.container_pid(&new_ids[0]).await {
            Ok(pid) => {
                let pod_ip = format!("10.244.0.{ip_num}");
                if let Err(e) = r8s_network::bridge::setup_pod_network(pid, &pod_ip, &pod_name) {
                    tracing::error!("pod '{pod_name}': network setup failed on restart: {e}");
                }
            }
            Err(e) => tracing::warn!("pod '{pod_name}': network setup failed on restart: {e}"),
        }
    }

    let mut tracked = pod_arc.lock().await;
    tracked.container_ids = new_ids;
    tracked.restart_count = prev_restart_count + 1;
    tracked.last_restart = Some(Instant::now());
    tracked.waiting_restart = false;

    tracing::info!(
        "pod '{pod_name}': restarted in-place (restart_count={})",
        tracked.restart_count
    );
    update_pod_status(store, &tracked);
}

async fn terminate_pod<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    pod_uid: &str,
    exit_code: Option<i32>,
) {
    let pod_arc = match state.remove(pod_uid).await {
        Some(a) => a,
        None => return,
    };
    let tracked = pod_arc.lock().await;

    // Containers are already stopped (they crashed), but clean up
    for cid in &tracked.container_ids {
        let _ = runtime.stop_container(cid, Duration::from_secs(1)).await;
        let _ = runtime.remove_container(cid).await;
    }
    state.release_ip(tracked.ip_num).await;
    r8s_network::bridge::teardown_pod_network(&tracked.name);
    cleanup_pod_volumes(data_dir, pod_uid);

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: tracked.namespace.as_deref(),
        name: &tracked.name,
    };

    let phase = if exit_code == Some(0) {
        "Succeeded"
    } else {
        "Failed"
    };
    if let Ok(Some(mut pod)) = store.get(&resource_ref) {
        let now = Time(chrono::Utc::now());
        let terminated_state = ContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code: exit_code.unwrap_or(1),
                reason: Some(
                    if exit_code == Some(0) {
                        "Completed"
                    } else {
                        "Error"
                    }
                    .into(),
                ),
                finished_at: Some(now),
                ..Default::default()
            }),
            ..Default::default()
        };
        if let Some(status) = pod.get_mut("status").and_then(|v| v.as_object_mut()) {
            status.insert("phase".to_string(), serde_json::json!(phase));
            // Update each container status to show Terminated state
            if let Some(cs) = status
                .get_mut("containerStatuses")
                .and_then(|v| v.as_array_mut())
            {
                for c in cs.iter_mut() {
                    if let Some(obj) = c.as_object_mut() {
                        obj.insert("ready".into(), serde_json::json!(false));
                        obj.insert("started".into(), serde_json::json!(false));
                        obj.insert(
                            "state".into(),
                            serde_json::to_value(&terminated_state).unwrap_or_default(),
                        );
                    }
                }
            }
        }
        let _ = store.update(&resource_ref, &pod);
    }
    tracing::info!(
        "pod '{}': terminated (code={:?}, phase={phase})",
        tracked.name,
        exit_code,
    );
}

async fn run_probes<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
) {
    let now = Instant::now();
    let mut liveness_failures: Vec<String> = Vec::new();
    let mut readiness_changes: Vec<(String, bool)> = Vec::new();
    let mut startup_successes: Vec<String> = Vec::new();

    let snap = state.snapshot().await;

    for (pod_uid, pod_arc) in &snap {
        let mut guard = pod_arc.lock().await;
        let tracked = &mut *guard;

        let gvr = GroupVersionResource::pods();
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: tracked.namespace.as_deref(),
            name: &tracked.name,
        };
        let pod_value = match store.get(&resource_ref) {
            Ok(Some(v)) => v,
            _ => continue,
        };
        let pod: Pod = match serde_json::from_value(pod_value) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let spec = match pod.spec.as_ref() {
            Some(s) => s,
            None => continue,
        };
        let pod_ip = format!("10.244.0.{}", tracked.ip_num);

        let pid = match tracked.container_ids.first() {
            Some(cid) => match runtime.container_pid(cid).await {
                Ok(p) => p,
                Err(_) => continue,
            },
            None => continue,
        };

        let ports = spec
            .containers
            .first()
            .and_then(|c| c.ports.as_deref())
            .unwrap_or(&[]);

        // Startup probe — must succeed before liveness/readiness are enabled
        if let Some(ref mut probe_state) = tracked.startup
            && !tracked.started
            && let Some(probe_spec) = spec
                .containers
                .first()
                .and_then(|c| c.startup_probe.as_ref())
        {
            let period = Duration::from_secs(probe_spec.period_seconds.unwrap_or(10) as u64);
            let failure_threshold = probe_spec.failure_threshold.unwrap_or(3);
            if now.duration_since(probe_state.last_check) >= period {
                probe_state.last_check = now;
                let ok = probe::exec_probe(probe_spec, &pod_ip, pid, ports).await;
                if ok {
                    tracing::info!("pod '{}': startup probe succeeded", tracked.name);
                    startup_successes.push(pod_uid.clone());
                } else {
                    probe_state.consecutive_failures += 1;
                    if probe_state.consecutive_failures >= failure_threshold {
                        tracing::warn!(
                            "pod '{}': startup probe failed {} times, restarting",
                            tracked.name,
                            probe_state.consecutive_failures
                        );
                        probe_state.consecutive_failures = 0;
                        liveness_failures.push(pod_uid.clone());
                    }
                }
            }
            continue; // Don't run liveness/readiness until startup probe succeeds
        }

        // Liveness probe — first container's probe for simplicity
        if let Some(ref mut probe_state) = tracked.liveness
            && let Some(probe_spec) = spec
                .containers
                .first()
                .and_then(|c| c.liveness_probe.as_ref())
        {
            let period = Duration::from_secs(probe_spec.period_seconds.unwrap_or(10) as u64);
            let failure_threshold = probe_spec.failure_threshold.unwrap_or(3);
            if now.duration_since(probe_state.last_check) >= period {
                probe_state.last_check = now;
                let ok = probe::exec_probe(probe_spec, &pod_ip, pid, ports).await;
                if ok {
                    probe_state.consecutive_failures = 0;
                } else {
                    probe_state.consecutive_failures += 1;
                    if probe_state.consecutive_failures >= failure_threshold {
                        tracing::warn!(
                            "pod '{}': liveness probe failed {} times, restarting",
                            tracked.name,
                            probe_state.consecutive_failures
                        );
                        probe_state.consecutive_failures = 0;
                        liveness_failures.push(pod_uid.clone());
                    }
                }
            }
        }

        // Readiness probe
        if let Some(ref mut probe_state) = tracked.readiness
            && let Some(probe_spec) = spec
                .containers
                .first()
                .and_then(|c| c.readiness_probe.as_ref())
        {
            let period = Duration::from_secs(probe_spec.period_seconds.unwrap_or(10) as u64);
            let failure_threshold = probe_spec.failure_threshold.unwrap_or(3);
            if now.duration_since(probe_state.last_check) >= period {
                probe_state.last_check = now;
                let ok = probe::exec_probe(probe_spec, &pod_ip, pid, ports).await;
                if ok {
                    probe_state.consecutive_failures = 0;
                    if !tracked.ready {
                        readiness_changes.push((pod_uid.clone(), true));
                    }
                } else {
                    probe_state.consecutive_failures += 1;
                    if tracked.ready && probe_state.consecutive_failures >= failure_threshold {
                        readiness_changes.push((pod_uid.clone(), false));
                    }
                }
            }
        }
    }

    // Handle startup probe successes — mark pod as started, enable liveness/readiness
    for pod_uid in startup_successes {
        if let Some(pod_arc) = state.get(&pod_uid).await {
            let mut tracked = pod_arc.lock().await;
            tracked.started = true;
            tracked.startup = None;
            if tracked.readiness.is_none() {
                tracked.ready = true;
                update_pod_status(store, &tracked);
            }
        }
    }

    // Handle liveness failures — restart those pods
    for pod_uid in liveness_failures {
        restart_in_place(store, runtime, state, data_dir, &pod_uid).await;
    }

    // Handle readiness changes
    for (pod_uid, ready) in readiness_changes {
        if let Some(pod_arc) = state.get(&pod_uid).await {
            let mut tracked = pod_arc.lock().await;
            tracked.ready = ready;
            tracing::info!("pod '{}': readiness changed to {ready}", tracked.name);
            update_pod_status(store, &tracked);
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
    let pull_secrets = pod_value
        .get("spec")
        .and_then(|s| s.get("imagePullSecrets"))
        .and_then(|v| v.as_array())?;
    let host = registry_host(image);
    let gvr = GroupVersionResource::secrets();

    for entry in pull_secrets {
        let secret_name = match entry.get("name").and_then(|v| v.as_str()) {
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
        let b64 = match secret
            .get("data")
            .and_then(|d| d.get(".dockerconfigjson"))
            .and_then(|v| v.as_str())
        {
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
    let mut paths: Vec<PathBuf> = Vec::new();

    if let Ok(dir) = std::env::var("DOCKER_CONFIG") {
        paths.push(PathBuf::from(dir).join("config.json"));
    }
    if let Ok(sudo_user) = std::env::var("SUDO_USER") {
        paths.push(PathBuf::from(format!(
            "/home/{sudo_user}/.docker/config.json"
        )));
    }
    if let Ok(home) = std::env::var("HOME") {
        paths.push(PathBuf::from(home).join(".docker/config.json"));
    }
    paths.push(PathBuf::from("/root/.docker/config.json"));

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
    let auths = config.get("auths").and_then(|v| v.as_object())?;
    let https_host = format!("https://{host}");
    let https_host_v1 = format!("https://{host}/v1/");
    // Docker Hub's canonical auth key is `https://index.docker.io/v1/`, even
    // though the registry host is `docker.io`. `docker login` writes there.
    let mut keys: Vec<&str> = vec![host, https_host.as_str(), https_host_v1.as_str()];
    if host == "docker.io" {
        keys.push("https://index.docker.io/v1/");
        keys.push("index.docker.io");
    }
    for key in keys {
        if let Some(entry) = auths.get(key) {
            if let Some(auth_b64) = entry
                .get("auth")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
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
            let username = entry
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let password = entry
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
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
    if let Some(data) = cm.get("data").and_then(|v| v.as_object()) {
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
    if let Some(data) = secret.get("data").and_then(|v| v.as_object()) {
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
