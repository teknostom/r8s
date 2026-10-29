pub mod metrics_server;
mod probe;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::Engine;
use r8s_runtime::{
    ContainerConfig, ContainerId, ContainerRuntime, Mount, RegistryAuth, RuntimeError,
};
use r8s_store::{
    Store,
    backend::ResourceRef,
    events::{NORMAL, ObjectRef, Recorder, WARNING},
    watch::WatchEventType,
};
use r8s_types::{
    Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
    ContainerStateWaiting, ContainerStatus, DownwardAPIVolumeFile, GroupVersionResource,
    IP_POOL_MAX, IP_POOL_MIN, KeyToPath, Pod, PodCondition, PodIP, PodSpec, PodStatus,
    ServiceAccountTokenProjection, Time, Volume, VolumeMount, pod_ip,
};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{Mutex, broadcast};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";

/// The kubelet's event recorder, installed by `run_with_config`; before that
/// (unit tests) events are dropped.
static RECORDER: std::sync::OnceLock<Recorder> = std::sync::OnceLock::new();

/// Records `reason`/`message` about `object` as a kubelet event.
fn record(object: &ObjectRef, type_: &str, reason: &str, message: &str) {
    if let Some(recorder) = RECORDER.get() {
        recorder.event(object, type_, reason, message);
    }
}

/// A pod's event reference from its stored JSON.
fn pod_ref(pod_value: &serde_json::Value) -> ObjectRef {
    let meta = &pod_value["metadata"];
    ObjectRef::pod(
        meta["namespace"].as_str(),
        meta["name"].as_str().unwrap_or(""),
        meta["uid"].as_str().unwrap_or(""),
    )
}
const MAX_BACKOFF: Duration = Duration::from_secs(300);
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const POST_START_TIMEOUT: Duration = Duration::from_secs(60);

struct ProbeState {
    last_check: Instant,
    period: Duration,
    consecutive_failures: i32,
    consecutive_successes: i32,
}

impl ProbeState {
    /// Timed so the first check lands after `initialDelaySeconds`, then every
    /// `periodSeconds`.
    fn new(probe: &r8s_types::Probe) -> ProbeState {
        let initial = Duration::from_secs(probe.initial_delay_seconds.unwrap_or(0).max(0) as u64);
        let period = Duration::from_secs(probe.period_seconds.unwrap_or(10).max(1) as u64);
        let now = Instant::now();
        let last_check = if initial >= period {
            now + (initial - period)
        } else {
            now.checked_sub(period - initial).unwrap_or(now)
        };
        ProbeState {
            last_check,
            period,
            consecutive_failures: 0,
            consecutive_successes: 0,
        }
    }

    /// Whether the probe is due at `now`; marks it checked when it is.
    fn take_due(&mut self, now: Instant) -> bool {
        if now.duration_since(self.last_check) >= self.period {
            self.last_check = now;
            true
        } else {
            false
        }
    }
}

/// How often the probe loop looks for due probes; each probe still runs on
/// its own `periodSeconds`.
const PROBE_TICK: Duration = Duration::from_millis(500);
const EXIT_TICK: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeKind {
    Startup,
    Liveness,
    Readiness,
}

/// A probe due to run, with what it needs once the pod lock is released.
struct DueProbe {
    pod_uid: String,
    kind: ProbeKind,
    probe: r8s_types::Probe,
    pod_ip: String,
    container: ContainerId,
    ports: Vec<r8s_types::ContainerPort>,
    subject: ObjectRef,
    /// The pod's restart count when the probe was taken; a result for a
    /// container since restarted is dropped.
    restart_count: i32,
}

struct TrackedPod {
    container_ids: Vec<ContainerId>,
    /// Ephemeral containers started so far, `(name, id)`; they are never
    /// restarted.
    ephemeral: Vec<(String, ContainerId)>,
    /// Containers that exited and will not be restarted.
    finished: FxHashMap<String, Exit>,
    ip_num: u32,
    name: String,
    namespace: Option<String>,
    restart_count: i32,
    last_restart: Option<Instant>,
    /// Containers that exited and await their restart backoff.
    crashed: FxHashMap<String, Exit>,
    liveness: Option<ProbeState>,
    readiness: Option<ProbeState>,
    startup: Option<ProbeState>,
    started: bool, // true once startup probe succeeds (or immediately if no startup probe)
    ready: bool,
}

impl TrackedPod {
    /// The name of the container at `index`, from its id.
    fn crashed_container_name(&self, index: usize) -> Option<String> {
        Some(self.container_ids.get(index)?.container_name().to_string())
    }
}

/// How a container ended.
#[derive(Debug, Clone, Default)]
struct Exit {
    code: Option<i32>,
    /// The termination message, per the container's path and policy.
    message: Option<String>,
}

type SharedPod = Arc<Mutex<TrackedPod>>;

/// Pod IPs. Never-used addresses go first (gaps a restart left, then the
/// next fresh one); a released address waits behind them, oldest first, so
/// it is not handed to a new pod while the host's neighbor entry and
/// conntrack still point at the old one, as host-local IPAM avoids it.
struct IpPool {
    /// Below `next` but never allocated since start.
    free: Vec<u32>,
    /// Released, oldest first.
    released: std::collections::VecDeque<u32>,
    next: u32,
}

impl IpPool {
    fn new() -> Self {
        Self {
            free: Vec::new(),
            released: std::collections::VecDeque::new(),
            next: IP_POOL_MIN,
        }
    }

    fn allocate(&mut self) -> Option<u32> {
        if let Some(ip) = self.free.pop() {
            return Some(ip);
        }
        if self.next <= IP_POOL_MAX {
            let ip = self.next;
            self.next += 1;
            return Some(ip);
        }
        self.released.pop_front()
    }

    /// Mark a specific host as allocated — used to adopt a pod's existing IP
    /// after a restart so it can't also be handed out fresh. Idempotent: pushes
    /// the high-water mark past `ip` (freeing the gap it opens) or drops `ip`
    /// from the free list, whichever applies.
    fn reserve(&mut self, ip: u32) {
        if ip < IP_POOL_MIN {
            return;
        }
        if ip >= self.next {
            self.free.extend(self.next..ip);
            self.next = ip + 1;
        } else {
            self.free.retain(|&f| f != ip);
            self.released.retain(|&f| f != ip);
        }
    }

    fn release(&mut self, ip: u32) {
        if ip < IP_POOL_MIN {
            return;
        }
        // Idempotent: only a genuinely-allocated IP (below the high-water mark
        // and not already free) re-enters the pool. Without this a double
        // release would queue the same address twice and hand it to two pods.
        if ip < self.next && !self.free.contains(&ip) && !self.released.contains(&ip) {
            self.released.push_back(ip);
        }
    }
}

struct PodState {
    pods: Mutex<FxHashMap<String, SharedPod>>,
    ip_pool: Mutex<IpPool>,
    /// UIDs of untracked pods a reconcile is currently driving through start-up.
    /// A plain std mutex (held only for the insert/remove) so [`StartGuard`] can
    /// release it on drop across the `.await`s in `reconcile_pod`.
    starting: std::sync::Mutex<rustc_hash::FxHashSet<String>>,
    /// One lock per `<namespace>/<name>`: a pod's teardown and the start of
    /// its same-named successor (a StatefulSet replacement) share veth and
    /// container ids, so they never overlap.
    names: Mutex<FxHashMap<String, Arc<Mutex<()>>>>,
}

/// Released on drop, clearing the pod's UID from [`PodState::starting`] so a
/// later tick can retry a start that failed (e.g. an unbound PVC).
struct StartGuard<'a> {
    starting: &'a std::sync::Mutex<rustc_hash::FxHashSet<String>>,
    uid: String,
}

impl Drop for StartGuard<'_> {
    fn drop(&mut self) {
        self.starting
            .lock()
            .expect("starting poisoned")
            .remove(&self.uid);
    }
}

impl PodState {
    fn new() -> Self {
        Self {
            pods: Mutex::new(FxHashMap::default()),
            ip_pool: Mutex::new(IpPool::new()),
            starting: std::sync::Mutex::new(rustc_hash::FxHashSet::default()),
            names: Mutex::new(FxHashMap::default()),
        }
    }

    /// The lock serializing work on the pod named `key` (`<namespace>/<name>`).
    async fn name_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.names
            .lock()
            .await
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    /// Claim the right to start `uid`. Returns `None` if another reconcile is
    /// already starting it, so callers return without racing in the runtime.
    fn try_begin_start(&self, uid: &str) -> Option<StartGuard<'_>> {
        let mut set = self.starting.lock().expect("starting poisoned");
        if !set.insert(uid.to_string()) {
            return None;
        }
        Some(StartGuard {
            starting: &self.starting,
            uid: uid.to_string(),
        })
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

    async fn reserve_ip(&self, ip: u32) {
        self.ip_pool.lock().await.reserve(ip);
    }

    /// Re-reserve every IP in `used` under a single lock — the restart recovery
    /// pass, run before any allocation can race it.
    async fn reserve_ips(&self, used: &rustc_hash::FxHashSet<u32>) {
        let mut pool = self.ip_pool.lock().await;
        for &ip in used {
            pool.reserve(ip);
        }
    }

    async fn release_ip(&self, ip: u32) {
        self.ip_pool.lock().await.release(ip);
    }
}

/// hostNetwork pods report the node's own address; it lies outside the pool
/// so the IP pool never hands it out or takes it back.
const HOST_NETWORK_IP_NUM: u32 = 1;

fn restart_backoff(restart_count: i32) -> Duration {
    if restart_count <= 0 {
        return Duration::ZERO;
    }
    let secs = INITIAL_BACKOFF
        .as_secs()
        .saturating_mul(1u64 << (restart_count - 1).min(30));
    Duration::from_secs(secs).min(MAX_BACKOFF)
}

/// Stop then remove each container, ignoring errors. Teardown is idempotent — an
/// already-stopped or already-gone container is not a failure — so every cleanup
/// path (rollback, crash, delete, reap, shutdown) funnels through here. `grace`
/// is the stop timeout before the runtime kills the container.
async fn stop_and_remove<R: ContainerRuntime>(runtime: &R, ids: &[ContainerId], grace: Duration) {
    for cid in ids {
        let _ = runtime.stop_container(cid, grace).await;
        let _ = runtime.remove_container(cid).await;
    }
}

pub async fn run<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
    cluster: String,
) -> anyhow::Result<()> {
    run_with_config(
        store,
        runtime,
        shutdown,
        data_dir,
        cluster,
        Duration::from_secs(10),
    )
    .await
}

pub async fn run_with_config<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
    cluster: String,
    health_interval: Duration,
) -> anyhow::Result<()> {
    tracing::info!("kubelet started for node '{NODE_NAME}'");
    let _ = RECORDER.set(Recorder::new(store.clone(), "kubelet", NODE_NAME));

    let state = Arc::new(PodState::new());
    // Serializes the periodic sweep with itself — see the ticker arm below.
    let sweep_lock = Arc::new(Mutex::new(()));

    recover_ip_allocations(&store, &state).await;
    reconcile_all(&store, &*runtime, &state, &data_dir, &cluster).await;

    let mut rx = store.watch(&GroupVersionResource::pods());
    let mut ticker = tokio::time::interval(health_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut probe_ticker = tokio::time::interval(PROBE_TICK);
    probe_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let probe_lock = Arc::new(Mutex::new(()));
    let mut exit_ticker = tokio::time::interval(EXIT_TICK);
    exit_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // One exit pass at a time, so a crashed container is restarted once.
    let exit_lock = Arc::new(Mutex::new(()));
    let mut exit_events = runtime.exit_events();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("kubelet shutting down");
                let snap = state.snapshot().await;
                for (_, pod_arc) in snap {
                    let tracked = pod_arc.lock().await;
                    stop_and_remove(&*runtime, &all_container_ids(&tracked), Duration::from_secs(5)).await;
                }
                return Ok(());
            }
            _ = probe_ticker.tick() => {
                // One probe pass at a time; a pass waits on its slowest probe.
                let Ok(probe_guard) = probe_lock.clone().try_lock_owned() else {
                    continue;
                };
                let state = state.clone();
                let runtime = runtime.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    let _probe_guard = probe_guard;
                    run_probes(&store, &*runtime, &state).await;
                });
            }
            _ = exit_ticker.tick() => {
                spawn_exit_pass(&exit_lock, &store, &runtime, &state, &data_dir).await;
            }
            event = next_exit_event(&mut exit_events) => {
                if event.is_none() {
                    exit_events = None;
                    continue;
                }
                spawn_exit_pass(&exit_lock, &store, &runtime, &state, &data_dir).await;
            }
            _ = ticker.tick() => {
                // At most one sweep at a time; a sweep that outlives the tick
                // interval skips the next one.
                let Ok(sweep_guard) = sweep_lock.clone().try_lock_owned() else {
                    continue;
                };
                let state = state.clone();
                let runtime = runtime.clone();
                let store = store.clone();
                let data_dir = data_dir.clone();
                let cluster = cluster.clone();
                tokio::spawn(async move {
                    let _sweep_guard = sweep_guard;
                    // Retry pods scheduled here that haven't started yet — e.g.
                    // one whose first reconcile lost the race against the
                    // provisioner binding its PVC. check_health only inspects
                    // already-tracked pods, so without this periodic resync a
                    // transient prepare_volumes failure wedges the pod in
                    // Pending forever. The per-pod start guard keeps this safe
                    // even if a slow start spills across ticks.
                    reconcile_all(&store, &*runtime, &state, &data_dir, &cluster).await;
                    check_health(&store, &*runtime, &state, &data_dir).await;
                    reap_orphans(&store, &*runtime, &state, &data_dir, &cluster).await;
                });
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let state = state.clone();
                        let store = store.clone();
                        let runtime = runtime.clone();
                        let data_dir = data_dir.clone();
                        let cluster = cluster.clone();
                        tokio::spawn(async move {
                            match event.event_type {
                                WatchEventType::Added | WatchEventType::Modified => {
                                    reconcile_pod(&store, &*runtime, &state, &event.object, &data_dir, &cluster).await;
                                }
                                WatchEventType::Deleted => {
                                    handle_delete(&*runtime, &state, &event.object, &data_dir).await;
                                }
                            }
                        });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        tracing::warn!("kubelet lagged, re-syncing");
                        reconcile_all(&store, &*runtime, &state, &data_dir, &cluster).await;
                        reap_orphans(&store, &*runtime, &state, &data_dir, &cluster).await;
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
    cluster: &str,
) {
    let result = match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kubelet list error: {e}");
            return;
        }
    };
    for pod in &result.items {
        reconcile_pod(store, runtime, state, pod, data_dir, cluster).await;
    }
}

/// Re-reserve the IPs of pods already running on this node, recovered from their
/// `status.podIP`, before any reconcile can hand out a fresh address. The pool
/// is empty on every daemon start; without this a restart could allocate an IP a
/// surviving pod still holds, putting two pods on one address. `reconcile_pod`
/// then reuses each pod's own reserved IP (see [`existing_pod_ip_num`]), so
/// addresses stay stable across restarts rather than churning.
async fn recover_ip_allocations(store: &Store, state: &PodState) {
    let result = match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kubelet ip recovery list error: {e}");
            return;
        }
    };
    let used: rustc_hash::FxHashSet<u32> = result
        .items
        .iter()
        .filter(|v| v.pointer("/spec/nodeName").and_then(|n| n.as_str()) == Some(NODE_NAME))
        .filter_map(existing_pod_ip_num)
        .collect();
    if !used.is_empty() {
        tracing::info!("kubelet: recovered {} pod IP allocation(s)", used.len());
        state.reserve_ips(&used).await;
    }
}

/// The pool host number of a pod's already-assigned IP (`status.podIP`), if set
/// and within our pod range. A restart reuses this instead of churning the
/// address; `None` means allocate a fresh one.
fn existing_pod_ip_num(pod_value: &serde_json::Value) -> Option<u32> {
    pod_value
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())?
        .parse::<std::net::Ipv4Addr>()
        .ok()
        .and_then(r8s_types::pod_ip_host)
}

/// Reap containers whose pod no longer exists in the store. Discovers them by
/// asking the runtime for everything tagged as ours (`list_owned_containers`)
/// rather than by replaying our in-memory `state`: a container is labeled the
/// instant it's created, so this catches leaks a crash left out of `state`, and
/// `reconcile_all` only ever *creates* — a delete the watch never delivered
/// (broadcast overflow during a mass uninstall, or a delete across a daemon
/// restart) would otherwise leak forever. This was the root of the CI "context
/// deadline exceeded" failures.
///
/// The runtime filters by the cluster ownership label, so this can never touch
/// another cluster's containers in the shared containerd namespace. Containers
/// carry their pod uid, so a pod recreated under the same name gets a fresh
/// uid: the old container's uid is absent from the store and reaped, the new
/// one's uid is present and kept — no name-collision guard needed.
async fn reap_orphans<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    cluster: &str,
) {
    // What *should* exist: live pod uids in the store. Read the uid straight
    // from the JSON rather than deserializing the whole Pod — a pod whose body
    // we can't fully parse would otherwise fall out of this set and have its
    // live containers reaped. The uid is all the membership test needs.
    let live_uids: rustc_hash::FxHashSet<String> =
        match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
            Ok(r) => r
                .items
                .iter()
                .filter_map(|v| v.pointer("/metadata/uid").and_then(|u| u.as_str()))
                .map(String::from)
                .collect(),
            Err(e) => {
                tracing::warn!("kubelet reap list error: {e}");
                return;
            }
        };

    // What *does* exist on the container backend.
    let owned = match runtime.list_owned_containers().await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!("kubelet reap: list_owned_containers failed: {e}");
            return;
        }
    };

    // What *does* exist on the network backend: every host-side veth on the
    // r8s bridge whose alias carries our cluster tag. A crash between veth
    // create and container create leaves a labeled link with no container,
    // which the container-side enumeration alone cannot see.
    let owned_veths = r8s_network::bridge::list_owned_veths(cluster).unwrap_or_default();

    // Group by pod (uid + name). The pod name is used for teardown_pod_network
    // (it hashes pod_name -> veth name) and for log lines.
    let mut by_pod: FxHashMap<(String, String), (Vec<ContainerId>, Vec<String>)> =
        FxHashMap::default();
    for c in owned {
        by_pod
            .entry((c.pod_uid, c.pod_name))
            .or_default()
            .0
            .push(c.id);
    }
    for v in owned_veths {
        by_pod
            .entry((v.pod_uid, v.pod_name))
            .or_default()
            .1
            .push(v.link);
    }

    for ((uid, name), (cids, veths)) in by_pod {
        // Empty uid = a resource we can't attribute (e.g. created before
        // labeling); leave it for the operator rather than guess. A live pod
        // keeps everything.
        if uid.is_empty() || live_uids.contains(&uid) {
            continue;
        }
        stop_and_remove(runtime, &cids, Duration::from_secs(10)).await;
        // Drop in-memory tracking + release the IP if we still held it.
        if let Some(pod_arc) = state.remove(&uid).await {
            let tracked = pod_arc.lock().await;
            state.release_ip(tracked.ip_num).await;
        }
        // teardown_pod_network deletes the veth by name-hash. If a veth label
        // pointed at a different host-side link name (shouldn't happen, but a
        // pod_name collision across cluster restarts could in theory), nuke
        // the labeled links too.
        r8s_network::bridge::teardown_pod_network(&uid);
        for link in &veths {
            r8s_network::bridge::delete_link(link);
        }
        cleanup_pod_volumes(data_dir, &uid);
        tracing::info!(
            "reaped orphan pod '{name}' (uid={uid}): {} container(s), {} veth(s)",
            cids.len(),
            veths.len(),
        );
    }
}

async fn reconcile_pod<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    pod_value: &serde_json::Value,
    data_dir: &Path,
    cluster: &str,
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
        let started =
            start_ephemeral_containers(store, runtime, data_dir, &pod_arc, &pod, pod_value).await;
        let tracked = pod_arc.lock().await;
        if !started && pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
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

    if let Some(name) = forbidden_sysctl(spec) {
        tracing::warn!("pod '{pod_name}': sysctl '{name}' is not allowlisted");
        fail_pod_admission(
            store,
            &pod,
            "SysctlForbidden",
            &format!("forbidden sysctl: \"{name}\" not allowlisted"),
        );
        return;
    }

    if let Some(port) = host_port_conflict(store, &pod) {
        tracing::warn!("pod '{pod_name}': host port {port} is already in use on this node");
        fail_pod_admission(
            store,
            &pod,
            "PodFitsHostPorts",
            &format!(
                "Pod Predicate PodFitsHostPorts failed: node(s) didn't have free ports for the requested pod ports ({port})"
            ),
        );
        return;
    }

    // Only one reconcile may drive an untracked pod through start-up at a time.
    // The periodic pending-resync and a watch event can both fire for the same
    // pod; without this guard they race in start_containers and the loser hits
    // "container already exists", wedging the pod. The guard releases on drop,
    // so an attempt that bails early (e.g. a not-yet-bound PVC) is retried on a
    // later tick.
    let _start_guard = match state.try_begin_start(&pod_uid) {
        Some(g) => g,
        None => return,
    };
    // A same-named predecessor's teardown finishes first.
    let name_lock = state
        .name_lock(&format!("{}/{pod_name}", pod_ns.unwrap_or("default")))
        .await;
    let _name_guard = name_lock.lock().await;

    // Reuse the IP this pod already holds (recovered from a prior incarnation's
    // status.podIP across a daemon restart) so the address is stable; otherwise
    // draw a fresh one. recover_ip_allocations reserved every in-use IP up front
    // and reserve is idempotent, so neither path hands one address to two pods.
    let host_network = spec.host_network.unwrap_or(false);
    let existing_ip = existing_pod_ip_num(pod_value);
    let ip_num = if host_network {
        HOST_NETWORK_IP_NUM
    } else {
        match existing_ip {
            Some(ip) => {
                state.reserve_ip(ip).await;
                ip
            }
            None => match state.allocate_ip().await {
                Some(ip) => ip,
                None => {
                    tracing::error!("pod '{pod_name}': IP pool exhausted");
                    return;
                }
            },
        }
    };
    let pod_ip = pod_ip(ip_num).to_string();

    let volumes = spec.volumes.as_deref().unwrap_or_default();
    let volume_pod = VolumePod {
        name: pod_name,
        uid: &pod_uid,
        namespace: pod_ns,
        service_account: spec.service_account_name.as_deref().unwrap_or("default"),
        ip: &pod_ip,
        metadata: &pod_value["metadata"],
        spec,
    };
    let volume_paths = match prepare_volumes(store, data_dir, &volume_pod, volumes) {
        Ok(paths) => paths,
        Err(e) => {
            tracing::error!("pod '{pod_name}': volume preparation failed: {e}");
            if existing_ip.is_none() {
                state.release_ip(ip_num).await;
            }
            return;
        }
    };
    // The pod's network namespace is shared by its init and app containers.
    // Best effort: without it (mock-runtime tests) each container gets its own.
    let netns = if host_network {
        None
    } else {
        setup_pod_netns(&pod_uid, &pod_ip, pod_name, cluster)
    };

    // Init containers run to completion (in order) before the main containers.
    if let Err(failure) = run_init_containers(
        runtime,
        store,
        data_dir,
        pod_name,
        pod_ns,
        pod_value,
        spec,
        &volume_paths,
        &pod_ip,
        netns.as_deref(),
    )
    .await
    {
        r8s_network::bridge::teardown_pod_network(&pod_uid);
        state.release_ip(ip_num).await;
        cleanup_pod_volumes(data_dir, &pod_uid);
        match failure {
            InitFailure::Retry => {
                tracing::warn!("pod '{pod_name}': init containers failed, will retry next tick")
            }
            InitFailure::Fatal => tracing::warn!("pod '{pod_name}': init containers failed"),
        }
        return;
    }

    let container_ids = match start_containers(
        runtime,
        store,
        pod_name,
        pod_ns,
        pod_value,
        spec,
        &volume_paths,
        &pod_ip,
        netns.as_deref(),
    )
    .await
    {
        Ok(ids) => ids,
        Err(ids) => {
            stop_and_remove(runtime, &ids, Duration::from_secs(5)).await;
            r8s_network::bridge::teardown_pod_network(&pod_uid);
            state.release_ip(ip_num).await;
            cleanup_pod_volumes(data_dir, &pod_uid);
            tracing::warn!("pod '{pod_name}': rolled back {} containers", ids.len());
            return;
        }
    };

    // Probes watch the first container.
    let first_container = spec.containers.first();
    let has_readiness = first_container.is_some_and(|c| c.readiness_probe.is_some());
    let has_startup = first_container.is_some_and(|c| c.startup_probe.is_some());

    let tracked = TrackedPod {
        container_ids,
        ip_num,
        name: pod_name.to_string(),
        namespace: pod_ns.map(String::from),
        restart_count: 0,
        last_restart: None,
        ephemeral: Vec::new(),
        finished: FxHashMap::default(),
        crashed: FxHashMap::default(),
        liveness: first_container
            .and_then(|c| c.liveness_probe.as_ref())
            .map(ProbeState::new),
        readiness: first_container
            .and_then(|c| c.readiness_probe.as_ref())
            .map(ProbeState::new),
        startup: first_container
            .and_then(|c| c.startup_probe.as_ref())
            .map(ProbeState::new),
        started: !has_startup,
        ready: !has_readiness && !has_startup,
    };
    update_pod_status(store, &tracked);
    state.insert(pod_uid, tracked).await;
}

/// Every container id of a tracked pod, ephemeral ones included.
fn all_container_ids(tracked: &TrackedPod) -> Vec<ContainerId> {
    tracked
        .container_ids
        .iter()
        .cloned()
        .chain(tracked.ephemeral.iter().map(|(_, id)| id.clone()))
        .collect()
}

/// Start any ephemeral container in the spec that has not run yet, in the
/// pod's namespaces; true when one was started.
async fn start_ephemeral_containers<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    data_dir: &Path,
    pod_arc: &SharedPod,
    pod: &Pod,
    pod_value: &serde_json::Value,
) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    let (Some(pod_name), Some(pod_uid)) =
        (pod.metadata.name.as_deref(), pod.metadata.uid.as_deref())
    else {
        return false;
    };
    let pod_ns = pod.metadata.namespace.as_deref();
    let pending: Vec<Container> = {
        let tracked = pod_arc.lock().await;
        spec.ephemeral_containers
            .iter()
            .flatten()
            .filter(|ec| !tracked.ephemeral.iter().any(|(n, _)| *n == ec.name))
            // An ephemeral container has the common container fields.
            .filter_map(|ec| serde_json::from_value(serde_json::to_value(ec).ok()?).ok())
            .collect()
    };
    if pending.is_empty() {
        return false;
    }
    let ip_num = pod_arc.lock().await.ip_num;
    let pod_ip = pod_ip(ip_num).to_string();
    let volume_pod = VolumePod {
        name: pod_name,
        uid: pod_uid,
        namespace: pod_ns,
        service_account: spec.service_account_name.as_deref().unwrap_or("default"),
        ip: &pod_ip,
        metadata: &pod_value["metadata"],
        spec,
    };
    let volumes = spec.volumes.as_deref().unwrap_or_default();
    let volume_paths = match prepare_volumes(store, data_dir, &volume_pod, volumes) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("pod '{pod_name}': ephemeral container volumes: {e}");
            return false;
        }
    };
    let netns = existing_pod_netns(pod_uid);
    let mut started = false;
    for container in &pending {
        let name = container.name.clone();
        if let Err(e) = ensure_image(runtime, store, pod_name, pod_ns, pod_value, container).await {
            tracing::error!("pod '{pod_name}': ephemeral '{name}': {e}");
            continue;
        }
        let config = match build_container_config(
            store,
            pod_name,
            pod_ns,
            pod_value,
            spec,
            container,
            &volume_paths,
            &pod_ip,
            netns.as_deref(),
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("pod '{pod_name}': ephemeral '{name}': {e}");
                continue;
            }
        };
        let id = match runtime.create_container(&config).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("pod '{pod_name}': ephemeral '{name}': create failed: {e}");
                continue;
            }
        };
        if runtime.prepare_task(&id).await.is_err() || runtime.start_container(&id).await.is_err() {
            tracing::error!("pod '{pod_name}': ephemeral '{name}': failed to start");
            stop_and_remove(runtime, std::slice::from_ref(&id), Duration::from_secs(5)).await;
            continue;
        }
        tracing::info!(
            "pod '{pod_name}': started ephemeral container '{name}' (id={})",
            id.0
        );
        pod_arc.lock().await.ephemeral.push((name, id));
        started = true;
    }
    started
}

/// The pod's network namespace path, or None when it could not be set up.
fn setup_pod_netns(pod_uid: &str, pod_ip: &str, pod_name: &str, cluster: &str) -> Option<String> {
    match r8s_network::bridge::setup_pod_network(pod_uid, pod_ip, pod_name, cluster) {
        Ok(path) => Some(path),
        Err(e) => {
            tracing::warn!("pod '{pod_name}': network setup failed: {e}");
            None
        }
    }
}

/// Start all containers for a pod. Returns Ok(ids) on success, Err(partial_ids) on failure.
#[allow(clippy::too_many_arguments)]
async fn start_containers<R: ContainerRuntime>(
    runtime: &R,
    store: &Store,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_value: &serde_json::Value,
    spec: &PodSpec,
    volume_paths: &FxHashMap<String, String>,
    pod_ip: &str,
    netns_path: Option<&str>,
) -> Result<Vec<ContainerId>, Vec<ContainerId>> {
    let mut container_ids: Vec<ContainerId> = Vec::new();

    for container_spec in &spec.containers {
        let container_name = &container_spec.name;

        if let Err(e) =
            ensure_image(runtime, store, pod_name, pod_ns, pod_value, container_spec).await
        {
            tracing::error!("pod '{pod_name}': {e}");
            return Err(container_ids);
        }

        let config = match build_container_config(
            store,
            pod_name,
            pod_ns,
            pod_value,
            spec,
            container_spec,
            volume_paths,
            pod_ip,
            netns_path,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("pod '{pod_name}': container '{container_name}': {e}");
                write_config_error(store, pod_ns, pod_name, container_name, &e);
                return Err(container_ids);
            }
        };

        let subject = pod_ref(pod_value).container(container_name);
        let container_id = match runtime.create_container(&config).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(
                    "pod '{pod_name}': failed to create container '{container_name}': {e}"
                );
                record(&subject, WARNING, "Failed", &format!("Error: {e}"));
                return Err(container_ids);
            }
        };
        record(
            &subject,
            NORMAL,
            "Created",
            &format!("Created container {container_name}"),
        );

        container_ids.push(container_id.clone());

        let started = match runtime.prepare_task(&container_id).await {
            Ok(()) => runtime.start_container(&container_id).await,
            Err(e) => Err(e),
        };
        if let Err(e) = started {
            tracing::error!("pod '{pod_name}': failed to start container '{container_name}': {e}");
            record(&subject, WARNING, "Failed", &format!("Error: {e}"));
            return Err(container_ids);
        }

        tracing::info!(
            "pod '{pod_name}': started container '{container_name}' (id={})",
            container_id.0
        );
        record(
            &subject,
            NORMAL,
            "Started",
            &format!("Started container {container_name}"),
        );

        // The postStart hook runs right after start; a failed one kills the
        // container, as upstream does.
        if let Some(hook) = container_spec
            .lifecycle
            .as_ref()
            .and_then(|l| l.post_start.as_ref())
        {
            let ports = container_spec.ports.as_deref().unwrap_or(&[]);
            let ok = probe::run_lifecycle_handler(
                hook,
                pod_ip,
                runtime,
                &container_id,
                ports,
                POST_START_TIMEOUT,
            )
            .await;
            if !ok {
                tracing::error!("pod '{pod_name}': postStart hook of '{container_name}' failed");
                record(
                    &subject,
                    WARNING,
                    "FailedPostStartHook",
                    &format!("PostStartHook failed for container {container_name}"),
                );
                return Err(container_ids);
            }
        }
    }

    Ok(container_ids)
}

/// Pull a container's image per its `imagePullPolicy` (defaulting Always for
/// `:latest`/untagged, else IfNotPresent). Returns an error string on failure.
async fn ensure_image<R: ContainerRuntime>(
    runtime: &R,
    store: &Store,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_value: &serde_json::Value,
    container_spec: &Container,
) -> Result<(), String> {
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
    let subject = pod_ref(pod_value).container(&container_spec.name);
    if should_pull {
        let auth = resolve_image_auth(store, pod_ns, pod_value, image);
        record(
            &subject,
            NORMAL,
            "Pulling",
            &format!("Pulling image \"{image}\""),
        );
        let started = Instant::now();
        if let Err(e) = runtime.pull_image(image, auth.as_ref()).await {
            let message = format!("Failed to pull image \"{image}\": {e}");
            record(&subject, WARNING, "Failed", &message);
            return Err(message);
        }
        record(
            &subject,
            NORMAL,
            "Pulled",
            &format!(
                "Successfully pulled image \"{image}\" in {:.3}s",
                started.elapsed().as_secs_f64()
            ),
        );
    } else if pull_policy == "IfNotPresent" {
        tracing::info!("pod '{pod_name}': image '{image}' present locally, skipping pull");
        record(
            &subject,
            NORMAL,
            "Pulled",
            &format!("Container image \"{image}\" already present on machine"),
        );
    }
    Ok(())
}

/// Build the runtime config for one container: resolve env (literals with
/// `$(VAR)` expansion, downward-API fieldRefs, secret/configMap keyRefs),
/// command/args expansion, mounts, and securityContext UID/GID. Shared by both
/// init and main containers.
#[allow(clippy::too_many_arguments)]
fn build_container_config(
    store: &Store,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_value: &serde_json::Value,
    spec: &PodSpec,
    container_spec: &Container,
    volume_paths: &FxHashMap<String, String>,
    pod_ip: &str,
    netns_path: Option<&str>,
) -> Result<ContainerConfig, String> {
    let container_name = &container_spec.name;
    let image = container_spec.image.as_deref().unwrap_or("unknown");

    let pod_uid = pod_value
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sa_name = spec.service_account_name.as_deref().unwrap_or("default");

    // Service discovery variables come first; a container's own env wins.
    let mut env: Vec<(String, String)> = if spec.enable_service_links.unwrap_or(true) {
        service_env(store, pod_ns)
    } else {
        Vec::new()
    };
    for from in container_spec.env_from.iter().flatten() {
        let prefix = from.prefix.as_deref().unwrap_or("");
        let entries = if let Some(cm) = &from.config_map_ref {
            configmap_entries(store, pod_ns, &cm.name)
        } else if let Some(secret) = &from.secret_ref {
            secret_entries(store, pod_ns, &secret.name)
        } else {
            Vec::new()
        };
        for (key, value) in entries {
            let name = format!("{prefix}{key}");
            env.retain(|(n, _)| n != &name);
            env.push((name, value));
        }
    }
    if let Some(envs) = container_spec.env.as_ref() {
        for e in envs {
            // An empty value is unset, as protobuf clients send it beside valueFrom.
            let value = if let Some(v) = e.value.as_ref().filter(|v| !v.is_empty()) {
                expand_vars(v, &env)
            } else if let Some(src) = e.value_from.as_ref() {
                if let Some(f) = src.field_ref.as_ref() {
                    resolve_field_ref_in(
                        &f.field_path,
                        pod_name,
                        pod_ns.unwrap_or("default"),
                        pod_uid,
                        pod_ip,
                        sa_name,
                        &pod_value["metadata"],
                    )
                    .unwrap_or_default()
                } else if let Some(rf) = src.resource_field_ref.as_ref() {
                    resource_field_value(store, spec, container_spec, rf).unwrap_or_default()
                } else if let Some(sk) = src.secret_key_ref.as_ref() {
                    secret_value(store, pod_ns, &sk.name, &sk.key).unwrap_or_default()
                } else if let Some(ck) = src.config_map_key_ref.as_ref() {
                    configmap_value(store, pod_ns, &ck.name, &ck.key).unwrap_or_default()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            env.retain(|(name, _)| name != &e.name);
            env.push((e.name.clone(), value));
        }
    }
    // Point pods at the bootstrapped `kubernetes` Service ClusterIP, not the
    // bridge gateway. nftables DNAT rewrites 10.96.0.1:443 → 10.244.0.1:6443.
    for (name, value) in [
        ("KUBERNETES_SERVICE_HOST", "10.96.0.1"),
        ("KUBERNETES_SERVICE_PORT", "443"),
        ("KUBERNETES_SERVICE_PORT_HTTPS", "443"),
        ("KUBERNETES_PORT", "tcp://10.96.0.1:443"),
        ("KUBERNETES_PORT_443_TCP", "tcp://10.96.0.1:443"),
        ("KUBERNETES_PORT_443_TCP_PROTO", "tcp"),
        ("KUBERNETES_PORT_443_TCP_PORT", "443"),
        ("KUBERNETES_PORT_443_TCP_ADDR", "10.96.0.1"),
    ] {
        if !env.iter().any(|(n, _)| n == name) {
            env.push((name.into(), value.into()));
        }
    }

    let command: Vec<String> = container_spec
        .command
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|s| expand_vars(s, &env))
        .collect();
    let args: Vec<String> = container_spec
        .args
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|s| expand_vars(s, &env))
        .collect();

    let mounts = resolve_mounts(
        container_spec.volume_mounts.as_deref().unwrap_or_default(),
        volume_paths,
        &env,
    )?;

    let csc = container_spec.security_context.as_ref();
    let psc = spec.security_context.as_ref();
    let run_as_user = csc
        .and_then(|c| c.run_as_user)
        .or_else(|| psc.and_then(|p| p.run_as_user))
        .map(|u| u as u32);
    let run_as_group = csc
        .and_then(|c| c.run_as_group)
        .or_else(|| psc.and_then(|p| p.run_as_group))
        .map(|g| g as u32);

    Ok(ContainerConfig {
        name: ContainerId::derive(pod_ns.unwrap_or("default"), pod_name, container_name).0,
        namespace: pod_ns.unwrap_or("default").to_string(),
        pod_uid: pod_uid.to_string(),
        pod_name: pod_name.to_string(),
        container_name: container_name.to_string(),
        image: image.to_string(),
        command,
        args,
        env,
        working_dir: container_spec.working_dir.clone(),
        mounts,
        run_as_user,
        run_as_group,
        netns_path: netns_path.map(String::from),
        host_network: spec.host_network.unwrap_or(false),
        pod_ip: Some(pod_ip.to_string()),
        host_aliases: spec
            .host_aliases
            .iter()
            .flatten()
            .map(|a| (a.ip.clone(), a.hostnames.clone().unwrap_or_default()))
            .collect(),
        termination_message_path: Some(
            container_spec
                .termination_message_path
                .clone()
                .unwrap_or_else(|| "/dev/termination-log".to_string()),
        ),
        sysctls: spec
            .security_context
            .as_ref()
            .and_then(|sc| sc.sysctls.as_ref())
            .into_iter()
            .flatten()
            .map(|s| (s.name.clone(), s.value.clone()))
            .collect(),
        privileged: csc.and_then(|c| c.privileged).unwrap_or(false),
        read_only_root_fs: csc
            .and_then(|c| c.read_only_root_filesystem)
            .unwrap_or(false),
        no_new_privileges: csc.and_then(|c| c.allow_privilege_escalation) == Some(false),
        cap_add: csc
            .and_then(|c| c.capabilities.as_ref())
            .and_then(|caps| caps.add.clone())
            .unwrap_or_default(),
        cap_drop: csc
            .and_then(|c| c.capabilities.as_ref())
            .and_then(|caps| caps.drop.clone())
            .unwrap_or_default(),
        hostname: r8s_runtime::containerd::truncate_hostname(
            spec.hostname.as_deref().unwrap_or(pod_name),
        ),
        subdomain: spec.subdomain.clone().unwrap_or_default(),
        dns: pod_dns(spec, pod_ns.unwrap_or("default")),
    })
}

/// The pod's resolver configuration per its `dnsPolicy`, with `dnsConfig`
/// appended: ClusterFirst (the default) takes the cluster DNS with the
/// namespace search path, Default the node's resolvers, None only
/// `dnsConfig`; a hostNetwork pod needs ClusterFirstWithHostNet for the
/// cluster DNS.
fn pod_dns(spec: &PodSpec, namespace: &str) -> r8s_runtime::DnsConfig {
    let host_network = spec.host_network.unwrap_or(false);
    let mut dns = match spec.dns_policy.as_deref().unwrap_or("ClusterFirst") {
        "None" => r8s_runtime::DnsConfig::default(),
        "ClusterFirstWithHostNet" => cluster_dns(namespace),
        "ClusterFirst" if !host_network => cluster_dns(namespace),
        _ => node_dns(),
    };
    if let Some(config) = &spec.dns_config {
        for nameserver in config.nameservers.iter().flatten() {
            if !dns.nameservers.contains(nameserver) {
                dns.nameservers.push(nameserver.clone());
            }
        }
        for search in config.searches.iter().flatten() {
            if !dns.searches.contains(search) {
                dns.searches.push(search.clone());
            }
        }
        for option in config.options.iter().flatten() {
            let name = option.name.clone().unwrap_or_default();
            dns.options
                .retain(|o| o.split(':').next() != Some(name.as_str()));
            dns.options.push(match option.value.as_deref() {
                Some(value) if !value.is_empty() => format!("{name}:{value}"),
                _ => name,
            });
        }
    }
    dns
}

fn cluster_dns(namespace: &str) -> r8s_runtime::DnsConfig {
    r8s_runtime::DnsConfig {
        nameservers: vec![r8s_types::NODE_INTERNAL_IP.to_string()],
        searches: vec![
            format!("{namespace}.svc.cluster.local"),
            "svc.cluster.local".into(),
            "cluster.local".into(),
        ],
        options: vec!["ndots:5".into()],
    }
}

/// The node's resolvers: systemd-resolved's upstream list when it runs,
/// else /etc/resolv.conf.
fn node_dns() -> r8s_runtime::DnsConfig {
    let text = ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"]
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok())
        .unwrap_or_default();
    parse_resolv_conf(&text)
}

fn parse_resolv_conf(text: &str) -> r8s_runtime::DnsConfig {
    let mut dns = r8s_runtime::DnsConfig::default();
    for line in text.lines() {
        let mut words = line.split_whitespace();
        match words.next() {
            Some("nameserver") => dns.nameservers.extend(words.next().map(String::from)),
            Some("search") => dns.searches.extend(words.map(String::from)),
            Some("options") => dns.options.extend(words.map(String::from)),
            _ => {}
        }
    }
    dns
}

/// Record a container that cannot be configured: Waiting with
/// CreateContainerConfigError, as upstream reports it, until a later tick
/// succeeds.
fn write_config_error(
    store: &Store,
    pod_ns: Option<&str>,
    pod_name: &str,
    container_name: &str,
    message: &str,
) {
    let gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns,
        name: pod_name,
    };
    let Ok(Some(mut pod)) = store.get(&rref) else {
        return;
    };
    record(
        &pod_ref(&pod).container(container_name),
        WARNING,
        "Failed",
        &format!("Error: {message}"),
    );
    if !pod["status"].is_object() {
        pod["status"] = serde_json::json!({});
    }
    let waiting = serde_json::json!({"waiting": {
        "reason": "CreateContainerConfigError",
        "message": message,
    }});
    let statuses = pod["status"]["containerStatuses"]
        .as_array_mut()
        .map(std::mem::take)
        .unwrap_or_default();
    let mut found = false;
    let mut updated: Vec<serde_json::Value> = statuses
        .into_iter()
        .map(|mut c| {
            if c["name"] == container_name {
                found = true;
                c["state"] = waiting.clone();
                c["ready"] = serde_json::json!(false);
                c["started"] = serde_json::json!(false);
            }
            c
        })
        .collect();
    if !found {
        let image = pod["spec"]["containers"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|c| c["name"] == container_name)
            .and_then(|c| c["image"].as_str())
            .unwrap_or("")
            .to_string();
        updated.push(serde_json::json!({
            "name": container_name,
            "image": image,
            "imageID": "",
            "ready": false,
            "started": false,
            "restartCount": 0,
            "state": waiting,
        }));
    }
    pod["status"]["containerStatuses"] = serde_json::Value::Array(updated);
    if pod["status"]["phase"].is_null() {
        pod["status"]["phase"] = serde_json::json!("Pending");
    }
    if let Err(e) = store.update(&rref, &pod) {
        tracing::debug!("pod '{pod_name}': config error status skipped: {e}");
    }
}

/// A downward-API resource field: the container's limit or request (or the
/// node's allocatable when the limit is unset), in units of the divisor,
/// rounded up as upstream does.
fn resource_field_value(
    store: &Store,
    spec: &PodSpec,
    container: &Container,
    field: &r8s_types::ResourceFieldSelector,
) -> Option<String> {
    use r8s_types::quantity::parse_quantity;
    let target = match field.container_name.as_deref() {
        Some(name) if !name.is_empty() && name != container.name => {
            spec.containers.iter().find(|c| c.name == name)?
        }
        _ => container,
    };
    let (kind, resource) = field.resource.split_once('.')?;
    let quantity = |kind: &str| {
        let list = target.resources.as_ref().and_then(|r| match kind {
            "limits" => r.limits.as_ref(),
            "requests" => r.requests.as_ref(),
            _ => None,
        })?;
        parse_quantity(&list.get(resource)?.0)
    };
    let value = match kind {
        "limits" => quantity("limits")
            .or_else(|| node_allocatable(store, resource))
            .unwrap_or(0.0),
        "requests" => quantity("requests")
            .or_else(|| quantity("limits"))
            .unwrap_or(0.0),
        _ => return None,
    };
    let divisor = field
        .divisor
        .as_ref()
        .and_then(|d| parse_quantity(&d.0))
        .filter(|d| *d > 0.0)
        .unwrap_or(1.0);
    Some(format!("{}", (value / divisor).ceil() as i128))
}

/// The node's allocatable amount of `resource`, from its status.
fn node_allocatable(store: &Store, resource: &str) -> Option<f64> {
    let gvr = GroupVersionResource::nodes();
    let node = store
        .get(&ResourceRef {
            gvr: &gvr,
            namespace: None,
            name: NODE_NAME,
        })
        .ok()??;
    node["status"]["allocatable"][resource]
        .as_str()
        .and_then(r8s_types::quantity::parse_quantity)
}

/// Run the pod's init containers in order, each to completion, before the main
/// containers start. A non-zero exit fails the pod (caller retries with
/// backoff). Init containers share the pod's volumes (the emptyDir host dirs
/// are created once per pod), so an init container can stage files a main
/// container later reads — which is exactly how prometheus's config-reloader
/// expands its config Secret into the shared `config_out` dir.
#[allow(clippy::too_many_arguments)]
/// Why the init containers did not complete.
enum InitFailure {
    /// Retry on a later tick (restartPolicy Always/OnFailure).
    Retry,
    /// The pod has failed (restartPolicy Never).
    Fatal,
}

/// Run the init containers in order, each to completion, reporting their
/// progress in the pod status as upstream does: the pod stays Pending with
/// `Initialized: False` until they all succeed, a failed one is retried with
/// backoff under restartPolicy Always, and fails the pod under Never.
#[allow(clippy::too_many_arguments)]
async fn run_init_containers<R: ContainerRuntime>(
    runtime: &R,
    store: &Store,
    data_dir: &Path,
    pod_name: &str,
    pod_ns: Option<&str>,
    pod_value: &serde_json::Value,
    spec: &PodSpec,
    volume_paths: &FxHashMap<String, String>,
    pod_ip: &str,
    netns_path: Option<&str>,
) -> Result<(), InitFailure> {
    let inits = match spec.init_containers.as_deref() {
        Some(c) if !c.is_empty() => c,
        _ => return Ok(()),
    };
    let restart_never = spec.restart_policy.as_deref() == Some("Never");
    let now = || Time(chrono::Utc::now());
    let previous: Vec<ContainerStatus> = pod_value
        .pointer("/status/initContainerStatuses")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let mut statuses: Vec<ContainerStatus> = inits
        .iter()
        .map(|c| {
            let prev = previous.iter().find(|p| p.name == c.name);
            ContainerStatus {
                name: c.name.clone(),
                image: c.image.clone().unwrap_or_default(),
                image_id: c.image.clone().unwrap_or_default(),
                ready: false,
                started: Some(false),
                restart_count: prev.map(|p| p.restart_count).unwrap_or(0),
                last_state: prev.and_then(|p| p.last_state.clone()),
                state: Some(waiting_state("PodInitializing", None)),
                ..Default::default()
            }
        })
        .collect();
    write_init_status(store, pod_ns, pod_name, spec, &statuses, "Pending");

    for (index, container_spec) in inits.iter().enumerate() {
        let name = &container_spec.name;
        if let Err(e) =
            ensure_image(runtime, store, pod_name, pod_ns, pod_value, container_spec).await
        {
            tracing::error!("pod '{pod_name}': init '{name}': {e}");
            return Err(InitFailure::Retry);
        }
        let config = match build_container_config(
            store,
            pod_name,
            pod_ns,
            pod_value,
            spec,
            container_spec,
            volume_paths,
            pod_ip,
            netns_path,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("pod '{pod_name}': init '{name}': {e}");
                return Err(InitFailure::Retry);
            }
        };

        let mut subject = pod_ref(pod_value);
        subject.field_path = Some(format!("spec.initContainers{{{name}}}"));
        let id = match runtime.create_container(&config).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!("pod '{pod_name}': init '{name}': create failed: {e}");
                record(&subject, WARNING, "Failed", &format!("Error: {e}"));
                return Err(InitFailure::Retry);
            }
        };
        record(
            &subject,
            NORMAL,
            "Created",
            &format!("Created container {name}"),
        );
        let started = match runtime.prepare_task(&id).await {
            Ok(()) => runtime.start_container(&id).await,
            Err(e) => Err(e),
        };
        if let Err(e) = started {
            tracing::error!("pod '{pod_name}': init '{name}': failed to start: {e}");
            record(&subject, WARNING, "Failed", &format!("Error: {e}"));
            stop_and_remove(runtime, std::slice::from_ref(&id), Duration::from_secs(5)).await;
            return Err(InitFailure::Retry);
        }
        record(
            &subject,
            NORMAL,
            "Started",
            &format!("Started container {name}"),
        );
        statuses[index].container_id = Some(id.0.clone());
        statuses[index].started = Some(true);
        statuses[index].state = Some(ContainerState {
            running: Some(ContainerStateRunning {
                started_at: Some(now()),
            }),
            ..Default::default()
        });
        write_init_status(store, pod_ns, pod_name, spec, &statuses, "Pending");

        // Wait for the init container to exit. Init containers run to
        // completion; main containers don't start until they all succeed.
        let deadline = Instant::now() + Duration::from_secs(300);
        let code = loop {
            match runtime.container_status(&id).await {
                Ok(st) if !st.running => {
                    let code = st.exit_code.unwrap_or(0);
                    if code != 0 {
                        tracing::warn!("pod '{pod_name}': init '{name}' exited code={code}");
                    } else {
                        tracing::info!("pod '{pod_name}': init '{name}' completed");
                    }
                    break code;
                }
                Ok(_) => {
                    if Instant::now() > deadline {
                        tracing::warn!("pod '{pod_name}': init '{name}' timed out");
                        break 1;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                // Task gone (fast exit before first poll): init done.
                Err(RuntimeError::NotFound(_)) => break 0,
                // Transient backend error: retry until the deadline.
                Err(e) => {
                    if Instant::now() > deadline {
                        tracing::warn!("pod '{pod_name}': init '{name}' status unknown: {e}");
                        break 1;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };

        let exit = Exit {
            code: Some(code),
            message: termination_message(data_dir, &id, Some(container_spec), Some(code)),
        };
        stop_and_remove(runtime, std::slice::from_ref(&id), Duration::from_secs(5)).await;
        let terminated = terminated_state(&exit, now());
        statuses[index].started = Some(false);
        if code != 0 {
            if restart_never {
                statuses[index].state = Some(terminated);
                write_init_status(store, pod_ns, pod_name, spec, &statuses, "Failed");
                return Err(InitFailure::Fatal);
            }
            statuses[index].restart_count += 1;
            let backoff = restart_backoff(statuses[index].restart_count).as_secs();
            statuses[index].state = Some(waiting_state(
                "CrashLoopBackOff",
                Some(format!("back-off {backoff}s restarting failed container")),
            ));
            statuses[index].last_state = Some(terminated);
            write_init_status(store, pod_ns, pod_name, spec, &statuses, "Pending");
            return Err(InitFailure::Retry);
        }
        statuses[index].ready = true;
        statuses[index].state = Some(terminated);
        write_init_status(store, pod_ns, pod_name, spec, &statuses, "Pending");
    }
    Ok(())
}

fn waiting_state(reason: &str, message: Option<String>) -> ContainerState {
    ContainerState {
        waiting: Some(ContainerStateWaiting {
            reason: Some(reason.into()),
            message,
        }),
        ..Default::default()
    }
}

/// Set (or add) a pod condition, keeping its transition time while the
/// status holds.
fn set_pod_condition(
    conditions: &mut Vec<serde_json::Value>,
    type_: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
) {
    let now = serde_json::json!(Time(chrono::Utc::now()));
    let transition = conditions
        .iter()
        .find(|c| c["type"] == type_ && c["status"] == status)
        .map(|c| c["lastTransitionTime"].clone())
        .unwrap_or(now);
    conditions.retain(|c| c["type"] != type_);
    let mut cond = serde_json::json!({
        "type": type_,
        "status": status,
        "lastTransitionTime": transition,
    });
    if let Some(r) = reason {
        cond["reason"] = serde_json::json!(r);
    }
    if let Some(m) = message {
        cond["message"] = serde_json::json!(m);
    }
    conditions.push(cond);
}

/// Store the pod's status while init containers run: their statuses, the
/// app containers waiting on them, and the conditions that say so.
fn write_init_status(
    store: &Store,
    pod_ns: Option<&str>,
    pod_name: &str,
    spec: &PodSpec,
    inits: &[ContainerStatus],
    phase: &str,
) {
    let gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns,
        name: pod_name,
    };
    let Ok(Some(mut pod)) = store.get(&rref) else {
        return;
    };
    let incomplete: Vec<&str> = inits
        .iter()
        .filter(|s| {
            !s.state
                .as_ref()
                .and_then(|st| st.terminated.as_ref())
                .is_some_and(|t| t.exit_code == 0)
        })
        .map(|s| s.name.as_str())
        .collect();
    let app_names: Vec<&str> = spec.containers.iter().map(|c| c.name.as_str()).collect();
    let app_statuses: Vec<ContainerStatus> = spec
        .containers
        .iter()
        .map(|c| ContainerStatus {
            name: c.name.clone(),
            image: c.image.clone().unwrap_or_default(),
            image_id: c.image.clone().unwrap_or_default(),
            ready: false,
            started: Some(false),
            state: Some(waiting_state("PodInitializing", None)),
            ..Default::default()
        })
        .collect();
    if !pod["status"].is_object() {
        pod["status"] = serde_json::json!({});
    }
    let mut conditions = pod["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    set_pod_condition(
        &mut conditions,
        "Initialized",
        "False",
        Some("ContainersNotInitialized"),
        Some(&format!(
            "containers with incomplete status: [{}]",
            incomplete.join(" ")
        )),
    );
    let unready = format!("containers with unready status: [{}]", app_names.join(" "));
    set_pod_condition(
        &mut conditions,
        "Ready",
        "False",
        Some("ContainersNotReady"),
        Some(&unready),
    );
    set_pod_condition(
        &mut conditions,
        "ContainersReady",
        "False",
        Some("ContainersNotReady"),
        Some(&unready),
    );
    let status = &mut pod["status"];
    status["phase"] = serde_json::json!(phase);
    status["conditions"] = serde_json::Value::Array(conditions);
    status["initContainerStatuses"] = serde_json::to_value(inits).unwrap_or_default();
    status["containerStatuses"] = serde_json::to_value(app_statuses).unwrap_or_default();
    if status["startTime"].is_null() {
        status["startTime"] = serde_json::json!(Time(chrono::Utc::now()));
    }
    if let Err(e) = store.update(&rref, &pod) {
        tracing::debug!("pod '{pod_name}': init status update skipped: {e}");
    }
}

/// A hostPath volume's path, created for the `*OrCreate` types and required
/// to exist for the others (`""` is unchecked, as upstream).
fn prepare_host_path(path: &str, type_: Option<&str>) -> anyhow::Result<String> {
    let p = Path::new(path);
    match type_.unwrap_or("") {
        "DirectoryOrCreate" => {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
        }
        "FileOrCreate" => {
            if !p.exists() {
                if let Some(parent) = p.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::File::create(p)?;
            }
        }
        "" => {}
        kind => {
            if !p.exists() {
                anyhow::bail!("hostPath type check failed: {path} is not a {kind}");
            }
        }
    }
    Ok(path.to_string())
}

/// `(hostIP, hostPort, protocol)` for every container port with a hostPort.
fn host_ports(pod: &Pod) -> Vec<(String, i32, String)> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .flat_map(|c| c.ports.iter().flatten())
        .filter_map(|p| {
            let port = p.host_port.filter(|h| *h > 0)?;
            Some((
                p.host_ip.clone().unwrap_or_else(|| "0.0.0.0".into()),
                port,
                p.protocol.clone().unwrap_or_else(|| "TCP".into()),
            ))
        })
        .collect()
}

/// Upstream's PodFitsHostPorts admission: a hostPort held by another pod on
/// this node that is running or was created earlier. Returns `protocol/port`.
fn host_port_conflict(store: &Store, pod: &Pod) -> Option<String> {
    let wanted = host_ports(pod);
    if wanted.is_empty() {
        return None;
    }
    let others = store
        .list_as::<Pod>(&GroupVersionResource::pods(), None)
        .ok()?;
    for other in &others {
        if other.metadata.uid == pod.metadata.uid
            || other.spec.as_ref().and_then(|s| s.node_name.as_deref()) != Some(NODE_NAME)
            || other.metadata.deletion_timestamp.is_some()
        {
            continue;
        }
        let phase = other
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("");
        if matches!(phase, "Succeeded" | "Failed") {
            continue;
        }
        let earlier = other.metadata.creation_timestamp < pod.metadata.creation_timestamp;
        if phase != "Running" && !earlier {
            continue;
        }
        for (ip, port, protocol) in host_ports(other) {
            if wanted.iter().any(|(wip, wport, wproto)| {
                *wport == port
                    && *wproto == protocol
                    && (*wip == ip || ip == "0.0.0.0" || *wip == "0.0.0.0")
            }) {
                return Some(format!("{protocol}/{port}"));
            }
        }
    }
    None
}

/// Record an admission rejection: phase Failed with `reason` and `message`.
/// Upstream's safe sysctls, the only ones a pod may set without the kubelet
/// being configured to allow more.
const SAFE_SYSCTLS: [&str; 12] = [
    "kernel.shm_rmid_forced",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.tcp_syncookies",
    "net.ipv4.ping_group_range",
    "net.ipv4.ip_unprivileged_port_start",
    "net.ipv4.ip_local_reserved_ports",
    "net.ipv4.tcp_keepalive_time",
    "net.ipv4.tcp_fin_timeout",
    "net.ipv4.tcp_keepalive_intvl",
    "net.ipv4.tcp_keepalive_probes",
    "net.ipv4.tcp_rmem",
    "net.ipv4.tcp_wmem",
];

/// The first sysctl the pod asks for that is not on the safe list.
fn forbidden_sysctl(spec: &PodSpec) -> Option<String> {
    spec.security_context
        .as_ref()
        .and_then(|sc| sc.sysctls.as_ref())
        .into_iter()
        .flatten()
        .map(|s| s.name.clone())
        .find(|name| !SAFE_SYSCTLS.contains(&name.as_str()))
}

fn fail_pod_admission(store: &Store, pod: &Pod, reason: &str, message: &str) {
    let gvr = GroupVersionResource::pods();
    let name = pod.metadata.name.as_deref().unwrap_or("");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: pod.metadata.namespace.as_deref(),
        name,
    };
    let Ok(Some(mut value)) = store.get(&rref) else {
        return;
    };
    record(&pod_ref(&value), WARNING, reason, message);
    value["status"]["phase"] = serde_json::json!("Failed");
    value["status"]["reason"] = serde_json::json!(reason);
    value["status"]["message"] = serde_json::json!(message);
    if let Err(e) = store.update(&rref, &value) {
        tracing::warn!("pod '{name}': record admission failure: {e}");
    }
}

fn terminated_state(exit: &Exit, finished_at: Time) -> ContainerState {
    ContainerState {
        terminated: Some(ContainerStateTerminated {
            exit_code: exit.code.unwrap_or(1),
            reason: Some(
                if exit.code == Some(0) {
                    "Completed"
                } else {
                    "Error"
                }
                .into(),
            ),
            message: exit.message.clone().filter(|m| !m.is_empty()),
            finished_at: Some(finished_at),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Upstream's termination message: the file the container wrote at its
/// terminationMessagePath, else (with FallbackToLogsOnError, on failure)
/// the tail of its log. Capped at 4096 bytes.
fn termination_message(
    data_dir: &Path,
    cid: &ContainerId,
    container: Option<&Container>,
    exit_code: Option<i32>,
) -> Option<String> {
    const LIMIT: usize = 4096;
    let path = r8s_runtime::containerd::termination_message_file(data_dir, &cid.0);
    let written = std::fs::read(&path).ok().filter(|b| !b.is_empty());
    let _ = std::fs::remove_file(&path);
    if let Some(bytes) = written {
        let end = bytes.len().min(LIMIT);
        return Some(String::from_utf8_lossy(&bytes[..end]).into_owned());
    }
    let fallback = container.and_then(|c| c.termination_message_policy.as_deref())
        == Some("FallbackToLogsOnError");
    if !fallback || exit_code == Some(0) {
        return None;
    }
    let log = std::fs::read_to_string(data_dir.join("logs").join(format!("{}.log", cid.0))).ok()?;
    // Log lines are CRI-formatted: `<time> <stream> <P|F> <text>`.
    let lines: Vec<&str> = log.lines().collect();
    let tail = &lines[lines.len().saturating_sub(80)..];
    let text: String = tail
        .iter()
        .map(|l| l.splitn(4, ' ').nth(3).unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let start = text.len().saturating_sub(LIMIT);
    Some(text[start..].to_string())
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
    let pod_ip = pod_ip(tracked.ip_num).to_string();

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

            if let Some(exit) = tracked.finished.get(&cid.0) {
                ContainerStatus {
                    name: spec.name.clone(),
                    image: image.clone(),
                    image_id: image,
                    container_id: Some(cid.0.clone()),
                    ready: false,
                    started: Some(false),
                    restart_count: tracked.restart_count,
                    state: Some(terminated_state(exit, now.clone())),
                    ..Default::default()
                }
            } else if let Some(exit) = tracked.crashed.get(&cid.0) {
                // Container has crashed — show Waiting/CrashLoopBackOff state
                let terminated_state = terminated_state(exit, now.clone());
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
        host_ip: Some(r8s_types::NODE_INTERNAL_IP.into()),
        host_ips: Some(vec![r8s_types::HostIP {
            ip: r8s_types::NODE_INTERNAL_IP.into(),
        }]),
        qos_class: current_pod
            .status
            .as_ref()
            .and_then(|s| s.qos_class.clone()),
        init_container_statuses: current_pod
            .status
            .as_ref()
            .and_then(|s| s.init_container_statuses.clone()),
        ephemeral_container_statuses: (!tracked.ephemeral.is_empty()).then(|| {
            tracked
                .ephemeral
                .iter()
                .map(|(name, cid)| {
                    let image = current_pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.ephemeral_containers.as_ref())
                        .and_then(|ecs| ecs.iter().find(|ec| ec.name == *name))
                        .and_then(|ec| ec.image.clone())
                        .unwrap_or_default();
                    let state = match tracked.finished.get(&cid.0) {
                        Some(exit) => terminated_state(exit, now.clone()),
                        None => ContainerState {
                            running: Some(ContainerStateRunning {
                                started_at: Some(now.clone()),
                            }),
                            ..Default::default()
                        },
                    };
                    ContainerStatus {
                        name: name.clone(),
                        image: image.clone(),
                        image_id: image,
                        container_id: Some(cid.0.clone()),
                        ready: false,
                        started: Some(!tracked.finished.contains_key(&cid.0)),
                        state: Some(state),
                        ..Default::default()
                    }
                })
                .collect()
        }),
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
    // A same-named successor starts only once this teardown is done.
    let name_lock = state
        .name_lock(&format!(
            "{}/{pod_name}",
            pod.metadata.namespace.as_deref().unwrap_or("default")
        ))
        .await;
    let _name_guard = name_lock.lock().await;

    // Prefer the tracked container ids (authoritative), but if the pod isn't in
    // our in-memory state — a delete that arrived after a daemon restart, or a
    // watch that lagged and dropped the Added — fall back to ids derived from
    // the pod spec. Container ids are deterministic (`<pod>_<container>`), so
    // either path reaps the right containers; without the fallback an untracked
    // delete leaks the containers entirely.
    let (container_ids, ip_num) = match state.remove(&pod_uid).await {
        Some(pod_arc) => {
            let tracked = pod_arc.lock().await;
            (all_container_ids(&tracked), Some(tracked.ip_num))
        }
        None => (container_ids_from_spec(&pod, &pod_name), None),
    };

    // preStop hooks run before the containers are stopped, within the grace
    // period.
    if let (Some(spec), Some(ip)) = (pod.spec.as_ref(), ip_num) {
        let pod_ip = pod_ip(ip).to_string();
        let grace =
            Duration::from_secs(spec.termination_grace_period_seconds.unwrap_or(30).max(1) as u64);
        for (container, cid) in spec.containers.iter().zip(container_ids.iter()) {
            if let Some(hook) = container
                .lifecycle
                .as_ref()
                .and_then(|l| l.pre_stop.as_ref())
            {
                let ports = container.ports.as_deref().unwrap_or(&[]);
                if !probe::run_lifecycle_handler(hook, &pod_ip, runtime, cid, ports, grace).await {
                    tracing::warn!(
                        "pod '{pod_name}': preStop hook of '{}' failed",
                        container.name
                    );
                }
            }
        }
    }

    if let Some(spec) = pod.spec.as_ref() {
        let subject = pod_ref(pod_value);
        for container in spec.containers.iter().take(container_ids.len()) {
            record(
                &subject.clone().container(&container.name),
                NORMAL,
                "Killing",
                &format!("Stopping container {}", container.name),
            );
        }
    }
    stop_and_remove(runtime, &container_ids, Duration::from_secs(10)).await;
    if let Some(ip) = ip_num {
        state.release_ip(ip).await;
    }
    r8s_network::bridge::teardown_pod_network(&pod_uid);
    cleanup_pod_volumes(data_dir, &pod_uid);
    tracing::info!(
        "pod '{pod_name}': cleaned up {} containers",
        container_ids.len(),
    );
}

/// Derive a pod's container ids from its spec. Container ids are deterministic
/// (`<pod_name>_<container_name>`, see `build_container_config`), so this lets
/// us reap a pod's containers even when it isn't in our in-memory state — e.g.
/// a delete event for a pod created before the daemon last restarted, or whose
/// Added event was dropped by a lagging watch.
fn container_ids_from_spec(pod: &Pod, pod_name: &str) -> Vec<ContainerId> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };
    spec.init_containers
        .as_deref()
        .unwrap_or_default()
        .iter()
        .chain(spec.containers.iter())
        .map(|c| {
            ContainerId::derive(
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod_name,
                &c.name,
            )
        })
        .collect()
}

/// Runs `check_exits` in the background unless a pass is already running.
async fn spawn_exit_pass<R: ContainerRuntime + 'static>(
    lock: &Arc<Mutex<()>>,
    store: &Store,
    runtime: &Arc<R>,
    state: &Arc<PodState>,
    data_dir: &Path,
) {
    let Ok(guard) = lock.clone().try_lock_owned() else {
        return;
    };
    let (store, runtime, state, data_dir) = (
        store.clone(),
        runtime.clone(),
        state.clone(),
        data_dir.to_path_buf(),
    );
    tokio::spawn(async move {
        let _guard = guard;
        check_exits(&store, &*runtime, &state, &data_dir).await;
    });
}

/// The next exit event; never resolves without a subscription.
async fn next_exit_event(events: &mut Option<tokio::sync::mpsc::Receiver<()>>) -> Option<()> {
    match events {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Handles containers that exited since the last pass: restarts them per
/// restartPolicy once their backoff has passed, or records them finished.
async fn check_exits<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
) {
    let snap = state.snapshot().await;
    let mut crashed: Vec<(String, usize, Option<i32>)> = Vec::new();

    for (pod_uid, pod_arc) in &snap {
        let tracked = pod_arc.lock().await;
        let backing_off = tracked
            .last_restart
            .is_some_and(|last| last.elapsed() < restart_backoff(tracked.restart_count));
        for (index, cid) in tracked.container_ids.iter().enumerate() {
            if tracked.finished.contains_key(&cid.0)
                || (backing_off && tracked.crashed.contains_key(&cid.0))
            {
                continue;
            }
            match runtime.container_status(cid).await {
                Ok(status) if !status.running => {
                    tracing::warn!(
                        "pod '{}': container {} exited (code={:?})",
                        tracked.name,
                        cid.0,
                        status.exit_code
                    );
                    crashed.push((pod_uid.clone(), index, status.exit_code));
                }
                Err(RuntimeError::NotFound(_)) => {
                    crashed.push((pod_uid.clone(), index, None));
                }
                // Transient backend error: skip this sweep, re-poll next tick.
                Err(e) => {
                    tracing::warn!(
                        "pod '{}': container {} status check failed: {e}",
                        tracked.name,
                        cid.0
                    );
                }
                _ => {}
            }
        }
    }

    for (pod_uid, index, exit_code) in crashed {
        handle_crash(store, runtime, state, data_dir, &pod_uid, index, exit_code).await;
    }

    // Ephemeral containers just finish; nothing restarts them.
    for (_, pod_arc) in &snap {
        let mut tracked = pod_arc.lock().await;
        let mut changed = false;
        for (name, cid) in tracked.ephemeral.clone() {
            if tracked.finished.contains_key(&cid.0) {
                continue;
            }
            let exited = match runtime.container_status(&cid).await {
                Ok(status) if !status.running => Some(status.exit_code),
                Err(RuntimeError::NotFound(_)) => Some(None),
                _ => None,
            };
            if let Some(code) = exited {
                tracing::info!(
                    "pod '{}': ephemeral container '{name}' exited (code={code:?})",
                    tracked.name
                );
                tracked.finished.insert(
                    cid.0.clone(),
                    Exit {
                        code,
                        message: None,
                    },
                );
                changed = true;
            }
        }
        if changed {
            update_pod_status(store, &tracked);
        }
    }
}

/// Refreshes tracked pods' tokens and projected volumes, and ends those past
/// their active deadline.
async fn check_health<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
) {
    let snap = state.snapshot().await;
    let mut expired: Vec<String> = Vec::new();
    for (pod_uid, pod_arc) in &snap {
        let (name, namespace) = {
            let t = pod_arc.lock().await;
            (t.name.clone(), t.namespace.clone())
        };
        let gvr = GroupVersionResource::pods();
        if let Ok(Some(pod)) = store.get(&ResourceRef {
            gvr: &gvr,
            namespace: namespace.as_deref(),
            name: &name,
        }) {
            refresh_bound_tokens(store, data_dir, &pod);
            refresh_volume_projections(store, data_dir, &pod);
            if deadline_exceeded(&pod) {
                expired.push(pod_uid.clone());
            }
        }
    }
    for pod_uid in expired {
        tracing::warn!("pod {pod_uid}: active deadline exceeded");
        terminate_pod(
            store,
            runtime,
            state,
            data_dir,
            &pod_uid,
            None,
            Some((
                "DeadlineExceeded",
                "Pod was active on the node longer than the specified deadline",
            )),
        )
        .await;
    }
}

/// Whether `spec.activeDeadlineSeconds` has passed since the pod started.
fn deadline_exceeded(pod: &serde_json::Value) -> bool {
    let Some(deadline) = pod["spec"]["activeDeadlineSeconds"].as_i64() else {
        return false;
    };
    let Some(start) = pod["status"]["startTime"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
    else {
        return false;
    };
    chrono::Utc::now().timestamp() - start.timestamp() >= deadline
}

/// A container of a tracked pod exited: restart it per the pod's
/// restartPolicy, or record it as finished and end the pod once every
/// container has.
#[allow(clippy::too_many_arguments)]
async fn handle_crash<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    pod_uid: &str,
    index: usize,
    exit_code: Option<i32>,
) {
    let pod_arc = match state.get(pod_uid).await {
        Some(a) => a,
        None => return,
    };

    let (pod_ns, pod_name, cid) = {
        let t = pod_arc.lock().await;
        (
            t.namespace.clone(),
            t.name.clone(),
            t.container_ids.get(index).cloned(),
        )
    };
    let Some(cid) = cid else { return };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: pod_ns.as_deref(),
        name: &pod_name,
    };
    let pod_spec = store.get(&resource_ref).ok().flatten();
    let restart_policy = pod_spec
        .as_ref()
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
    let container_spec = pod_spec
        .as_ref()
        .and_then(|p| serde_json::from_value::<Pod>(p.clone()).ok())
        .and_then(|p| p.spec.and_then(|s| s.containers.get(index).cloned()));
    let exit = Exit {
        code: exit_code,
        message: termination_message(data_dir, &cid, container_spec.as_ref(), exit_code),
    };

    if should_restart {
        // Mark as crashed/waiting so status reflects CrashLoopBackOff during backoff
        {
            let mut tracked = pod_arc.lock().await;
            tracked.crashed.insert(cid.0.clone(), exit);
            tracked.ready = false;
            update_pod_status(store, &tracked);
        }
        restart_container(store, runtime, state, data_dir, pod_uid, index).await;
        return;
    }

    let pod_exit_code = {
        let mut tracked = pod_arc.lock().await;
        tracked.finished.insert(cid.0.clone(), exit);
        tracked.ready = false;
        let all_done = tracked
            .container_ids
            .iter()
            .all(|c| tracked.finished.contains_key(&c.0));
        if !all_done {
            tracing::info!(
                "pod '{pod_name}': container {} finished (code={exit_code:?})",
                cid.0
            );
            update_pod_status(store, &tracked);
            return;
        }
        tracked
            .finished
            .values()
            .map(|e| e.code)
            .find(|code| *code != Some(0))
            .unwrap_or(Some(0))
    };
    terminate_pod(
        store,
        runtime,
        state,
        data_dir,
        pod_uid,
        pod_exit_code,
        None,
    )
    .await;
}

/// The pod's network namespace path when it exists; a pod whose network
/// setup failed (or a mock runtime) gets `None` and an isolated namespace.
fn existing_pod_netns(pod_uid: &str) -> Option<String> {
    let path = r8s_network::bridge::netns_path(pod_uid);
    Path::new(&path).exists().then_some(path)
}

/// Replace one container of a running pod with a fresh one from the same
/// spec, after the restart backoff. The pod's other containers, IP, volumes
/// and network namespace stay as they are.
async fn restart_container<R: ContainerRuntime>(
    store: &Store,
    runtime: &R,
    state: &PodState,
    data_dir: &Path,
    pod_uid: &str,
    index: usize,
) {
    let pod_arc = match state.get(pod_uid).await {
        Some(a) => a,
        None => return,
    };

    // Snapshot what we need under the lock; release it for long awaits below.
    let (pod_name, pod_ns, ip_num, old_cid, prev_restart_count) = {
        let t = pod_arc.lock().await;
        let backoff = restart_backoff(t.restart_count);
        if let Some(last) = t.last_restart
            && last.elapsed() < backoff
        {
            if let Some(name) = t.crashed_container_name(index) {
                record(
                    &ObjectRef::pod(t.namespace.as_deref(), &t.name, pod_uid).container(&name),
                    WARNING,
                    "BackOff",
                    &format!(
                        "Back-off restarting failed container {name} in pod {}_{}({pod_uid})",
                        t.name,
                        t.namespace.as_deref().unwrap_or("default")
                    ),
                );
            }
            return; // Not enough time elapsed, retry next tick
        }
        (
            t.name.clone(),
            t.namespace.clone(),
            t.ip_num,
            t.container_ids.get(index).cloned(),
            t.restart_count,
        )
    };
    let Some(old_cid) = old_cid else { return };

    stop_and_remove(
        runtime,
        std::slice::from_ref(&old_cid),
        Duration::from_secs(5),
    )
    .await;

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
    let Some(container_spec) = spec.containers.get(index) else {
        return;
    };

    let pod_ip = pod_ip(ip_num).to_string();
    let volumes = spec.volumes.as_deref().unwrap_or_default();
    let volume_pod = VolumePod {
        name: &pod_name,
        uid: pod_uid,
        namespace: pod_ns.as_deref(),
        service_account: spec.service_account_name.as_deref().unwrap_or("default"),
        ip: &pod_ip,
        metadata: &pod_value["metadata"],
        spec,
    };
    let volume_paths = match prepare_volumes(store, data_dir, &volume_pod, volumes) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("pod '{pod_name}': volume prep failed on restart: {e}");
            return;
        }
    };
    let netns = existing_pod_netns(pod_uid);
    let config = match build_container_config(
        store,
        &pod_name,
        pod_ns.as_deref(),
        &pod_value,
        spec,
        container_spec,
        &volume_paths,
        &pod_ip,
        netns.as_deref(),
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("pod '{pod_name}': container '{}': {e}", container_spec.name);
            return;
        }
    };
    let subject = pod_ref(&pod_value).container(&container_spec.name);
    let new_id = match runtime.create_container(&config).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(
                "pod '{pod_name}': failed to recreate container '{}': {e}",
                container_spec.name
            );
            record(&subject, WARNING, "Failed", &format!("Error: {e}"));
            return;
        }
    };
    record(
        &subject,
        NORMAL,
        "Created",
        &format!("Created container {}", container_spec.name),
    );
    let started = match runtime.prepare_task(&new_id).await {
        Ok(()) => runtime.start_container(&new_id).await,
        Err(e) => Err(e),
    };
    if let Err(e) = started {
        tracing::error!(
            "pod '{pod_name}': failed to restart container '{}': {e}",
            container_spec.name
        );
        record(&subject, WARNING, "Failed", &format!("Error: {e}"));
        stop_and_remove(
            runtime,
            std::slice::from_ref(&new_id),
            Duration::from_secs(5),
        )
        .await;
        return;
    }

    let mut tracked = pod_arc.lock().await;
    if let Some(slot) = tracked.container_ids.get_mut(index) {
        *slot = new_id;
    }
    tracked.crashed.remove(&old_cid.0);
    tracked.restart_count = prev_restart_count + 1;
    tracked.last_restart = Some(Instant::now());

    // Reset readiness for the fresh container exactly as initial start does:
    // a container with no readiness/startup probe is Ready the moment it runs.
    // `handle_crash` set ready=false before the restart; without restoring it
    // here, a probe-less container (e.g. cert-manager-cainjector, which has no
    // readiness probe) that ever crashed stays stuck NotReady forever, since
    // no probe loop will flip it back true.
    let has_readiness = spec.containers.iter().any(|c| c.readiness_probe.is_some());
    let has_startup = spec.containers.iter().any(|c| c.startup_probe.is_some());
    tracked.started = !has_startup;
    tracked.ready = !has_readiness && !has_startup;
    // Probes start over, initial delay included.
    let first_container = spec.containers.first();
    tracked.liveness = first_container
        .and_then(|c| c.liveness_probe.as_ref())
        .map(ProbeState::new);
    tracked.readiness = first_container
        .and_then(|c| c.readiness_probe.as_ref())
        .map(ProbeState::new);
    tracked.startup = first_container
        .and_then(|c| c.startup_probe.as_ref())
        .map(ProbeState::new);

    tracing::info!(
        "pod '{pod_name}': restarted container '{}' (restart_count={})",
        container_spec.name,
        tracked.restart_count
    );
    record(
        &subject,
        NORMAL,
        "Started",
        &format!("Started container {}", container_spec.name),
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
    reason: Option<(&str, &str)>,
) {
    let pod_arc = match state.remove(pod_uid).await {
        Some(a) => a,
        None => return,
    };
    let tracked = pod_arc.lock().await;

    // Containers are already stopped (they crashed), but clean up
    stop_and_remove(
        runtime,
        &all_container_ids(&tracked),
        Duration::from_secs(1),
    )
    .await;
    state.release_ip(tracked.ip_num).await;
    r8s_network::bridge::teardown_pod_network(pod_uid);
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
        let pod_exit = Exit {
            code: exit_code,
            message: None,
        };
        if let Some(status) = pod.get_mut("status").and_then(|v| v.as_object_mut()) {
            status.insert("phase".to_string(), serde_json::json!(phase));
            if let Some((reason, message)) = reason {
                status.insert("reason".to_string(), serde_json::json!(reason));
                status.insert("message".to_string(), serde_json::json!(message));
            }
            let mut conditions = status
                .get("conditions")
                .and_then(|c| c.as_array())
                .cloned()
                .unwrap_or_default();
            for type_ in ["Ready", "ContainersReady"] {
                set_pod_condition(&mut conditions, type_, "False", Some("PodCompleted"), None);
            }
            status.insert(
                "conditions".to_string(),
                serde_json::Value::Array(conditions),
            );
            // Update each container status to show Terminated state
            if let Some(cs) = status
                .get_mut("containerStatuses")
                .and_then(|v| v.as_array_mut())
            {
                for c in cs.iter_mut() {
                    let exit = c["containerID"]
                        .as_str()
                        .and_then(|id| tracked.finished.get(id))
                        .unwrap_or(&pod_exit);
                    let state = terminated_state(exit, now.clone());
                    if let Some(obj) = c.as_object_mut() {
                        obj.insert("ready".into(), serde_json::json!(false));
                        obj.insert("started".into(), serde_json::json!(false));
                        obj.insert(
                            "state".into(),
                            serde_json::to_value(&state).unwrap_or_default(),
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

// TODO(probes): probe every container, not just the first. This is a pod-level
// approximation — the first container stands in for the whole pod — because the
// surrounding model is pod-level: `TrackedPod` holds one `ProbeState` per probe
// kind (not per container), `ready` is a single bool stamped onto every
// container status, and a liveness failure restarts the whole pod. Doing it
// properly is a cross-cutting change: per-container `ProbeState`, per-container
// readiness aggregated into the pod `Ready` condition, and container-granular
// restart in `restart_in_place` — touching `TrackedPod`, `run_probes`,
// `update_pod_status`, and the restart path together.
/// Runs every due startup, liveness and readiness probe of the tracked
/// pods, all at once, then applies the results: a startup success enables
/// the others, liveness failures past the threshold restart the container,
/// readiness flips after its success or failure threshold.
async fn run_probes<R: ContainerRuntime>(store: &Store, runtime: &R, state: &PodState) {
    let now = Instant::now();
    let mut due: Vec<DueProbe> = Vec::new();

    for (pod_uid, pod_arc) in &state.snapshot().await {
        let mut guard = pod_arc.lock().await;
        let tracked = &mut *guard;
        let startup_due =
            !tracked.started && tracked.startup.as_mut().is_some_and(|p| p.take_due(now));
        // Liveness and readiness wait for the startup probe.
        let gated = !tracked.started && tracked.startup.is_some();
        let liveness_due = !gated && tracked.liveness.as_mut().is_some_and(|p| p.take_due(now));
        let readiness_due = !gated && tracked.readiness.as_mut().is_some_and(|p| p.take_due(now));
        if !(startup_due || liveness_due || readiness_due) {
            continue;
        }

        let gvr = GroupVersionResource::pods();
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: tracked.namespace.as_deref(),
            name: &tracked.name,
        };
        let Some(pod) = store
            .get(&resource_ref)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value::<Pod>(v).ok())
        else {
            continue;
        };
        let Some(container_spec) = pod.spec.as_ref().and_then(|s| s.containers.first()) else {
            continue;
        };
        let Some(cid) = tracked.container_ids.first().cloned() else {
            continue;
        };
        if tracked.crashed.contains_key(&cid.0) || tracked.finished.contains_key(&cid.0) {
            continue;
        }
        let subject = ObjectRef::pod(tracked.namespace.as_deref(), &tracked.name, pod_uid)
            .container(&container_spec.name);
        let mut push = |kind: ProbeKind, probe: Option<&r8s_types::Probe>| {
            if let Some(probe) = probe {
                due.push(DueProbe {
                    pod_uid: pod_uid.clone(),
                    kind,
                    probe: probe.clone(),
                    pod_ip: pod_ip(tracked.ip_num).to_string(),
                    container: cid.clone(),
                    ports: container_spec.ports.clone().unwrap_or_default(),
                    subject: subject.clone(),
                    restart_count: tracked.restart_count,
                });
            }
        };
        if startup_due {
            push(ProbeKind::Startup, container_spec.startup_probe.as_ref());
        }
        if liveness_due {
            push(ProbeKind::Liveness, container_spec.liveness_probe.as_ref());
        }
        if readiness_due {
            push(
                ProbeKind::Readiness,
                container_spec.readiness_probe.as_ref(),
            );
        }
    }
    if due.is_empty() {
        return;
    }

    let results = futures_util::future::join_all(
        due.iter()
            .map(|d| probe::exec_probe(&d.probe, &d.pod_ip, runtime, &d.container, &d.ports)),
    )
    .await;

    let mut kills: Vec<ContainerId> = Vec::new();
    for (d, ok) in due.iter().zip(results) {
        let Some(pod_arc) = state.get(&d.pod_uid).await else {
            continue;
        };
        let mut tracked = pod_arc.lock().await;
        if tracked.restart_count != d.restart_count {
            continue;
        }
        let failure_threshold = d.probe.failure_threshold.unwrap_or(3).max(1);
        let success_threshold = d.probe.success_threshold.unwrap_or(1).max(1);
        let name = tracked.name.clone();
        match d.kind {
            ProbeKind::Startup => {
                let Some(probe_state) = tracked.startup.as_mut() else {
                    continue;
                };
                if ok {
                    tracing::info!("pod '{name}': startup probe succeeded");
                    tracked.started = true;
                    tracked.startup = None;
                    if tracked.readiness.is_none() {
                        tracked.ready = true;
                    }
                    update_pod_status(store, &tracked);
                } else {
                    probe_state.consecutive_failures += 1;
                    record(&d.subject, WARNING, "Unhealthy", "Startup probe failed");
                    if probe_state.consecutive_failures >= failure_threshold {
                        tracing::warn!(
                            "pod '{name}': startup probe failed {} times, killing the container",
                            probe_state.consecutive_failures
                        );
                        probe_state.consecutive_failures = 0;
                        kills.push(d.container.clone());
                    }
                }
            }
            ProbeKind::Liveness => {
                let Some(probe_state) = tracked.liveness.as_mut() else {
                    continue;
                };
                if ok {
                    probe_state.consecutive_failures = 0;
                } else {
                    probe_state.consecutive_failures += 1;
                    record(&d.subject, WARNING, "Unhealthy", "Liveness probe failed");
                    if probe_state.consecutive_failures >= failure_threshold {
                        tracing::warn!(
                            "pod '{name}': liveness probe failed {} times, killing the container",
                            probe_state.consecutive_failures
                        );
                        probe_state.consecutive_failures = 0;
                        kills.push(d.container.clone());
                    }
                }
            }
            ProbeKind::Readiness => {
                let ready = tracked.ready;
                let Some(probe_state) = tracked.readiness.as_mut() else {
                    continue;
                };
                let flip = if ok {
                    probe_state.consecutive_failures = 0;
                    probe_state.consecutive_successes += 1;
                    !ready && probe_state.consecutive_successes >= success_threshold
                } else {
                    probe_state.consecutive_successes = 0;
                    probe_state.consecutive_failures += 1;
                    record(&d.subject, WARNING, "Unhealthy", "Readiness probe failed");
                    ready && probe_state.consecutive_failures >= failure_threshold
                };
                if flip {
                    tracked.ready = !ready;
                    tracing::info!("pod '{name}': readiness changed to {}", !ready);
                    update_pod_status(store, &tracked);
                }
            }
        }
    }

    // Liveness probes watch the first container. Its exit goes through
    // check_exits, which restarts it per restartPolicy after the backoff.
    futures_util::future::join_all(
        kills
            .iter()
            .map(|cid| runtime.stop_container(cid, Duration::from_secs(5))),
    )
    .await;
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

/// The pod a volume is prepared for.
struct VolumePod<'a> {
    name: &'a str,
    uid: &'a str,
    namespace: Option<&'a str>,
    service_account: &'a str,
    ip: &'a str,
    /// `metadata`, for annotation and label field refs.
    metadata: &'a serde_json::Value,
    spec: &'a PodSpec,
}

fn prepare_volumes(
    store: &Store,
    data_dir: &Path,
    pod: &VolumePod<'_>,
    volumes: &[Volume],
) -> anyhow::Result<FxHashMap<String, String>> {
    let mut paths = FxHashMap::default();
    let pod_ns = pod.namespace;
    let pod_uid = pod.uid;
    let vol_dir = |name: &str| data_dir.join("pod-data").join(pod_uid).join(name);

    for vol in volumes {
        let host_path = if let Some(empty) = &vol.empty_dir {
            let dir = vol_dir(&vol.name);
            std::fs::create_dir_all(&dir)?;
            prepare_empty_dir(&dir, empty)?;
            dir.to_string_lossy().to_string()
        } else if let Some(hp) = &vol.host_path {
            prepare_host_path(&hp.path, hp.type_.as_deref())?
        } else if vol.config_map.is_some()
            || vol.secret.is_some()
            || vol.downward_api.is_some()
            || vol.projected.is_some()
        {
            let dir = vol_dir(&vol.name);
            std::fs::create_dir_all(&dir)?;
            project_volume(store, data_dir, pod, vol, &dir, true)?;
            dir.to_string_lossy().to_string()
        } else if let Some(pvc_src) = &vol.persistent_volume_claim {
            // Resolve PVC -> bound PV -> hostPath. An unbound claim or missing
            // PV is a hard error (not a silent skip): prepare_volumes fails, the
            // pod isn't started this pass, and the kubelet retries on its next
            // health tick once the provisioner has bound the claim.
            resolve_pvc_host_path(store, pod_ns, &pvc_src.claim_name)?
        } else {
            tracing::warn!("volume '{}': unsupported volume source, skipping", vol.name);
            continue;
        };

        paths.insert(vol.name.clone(), host_path);
    }

    Ok(paths)
}

/// Resolve a `persistentVolumeClaim` volume source to a host directory by
/// following PVC -> `spec.volumeName` -> PV -> `spec.hostPath.path`. Errors if
/// the claim is missing, not yet bound, or its PV has no hostPath.
fn resolve_pvc_host_path(
    store: &Store,
    namespace: Option<&str>,
    claim_name: &str,
) -> anyhow::Result<String> {
    let pvc_gvr = GroupVersionResource::persistent_volume_claims();
    let pvc = store
        .get(&ResourceRef {
            gvr: &pvc_gvr,
            namespace,
            name: claim_name,
        })?
        .ok_or_else(|| anyhow::anyhow!("persistentVolumeClaim '{claim_name}' not found"))?;

    let volume_name = pvc
        .pointer("/spec/volumeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("pvc '{claim_name}' is not bound yet"))?;

    let pv_gvr = GroupVersionResource::persistent_volumes();
    let pv = store
        .get(&ResourceRef {
            gvr: &pv_gvr,
            namespace: None,
            name: volume_name,
        })?
        .ok_or_else(|| anyhow::anyhow!("persistentVolume '{volume_name}' not found"))?;

    let path = pv
        .pointer("/spec/hostPath/path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("pv '{volume_name}' has no hostPath"))?;
    std::fs::create_dir_all(path)?;
    Ok(path.to_string())
}

/// Writes a configMap, secret, downwardAPI or projected volume into `dir`
/// and removes the files no source projects any more. A projected volume's
/// `defaultMode` applies to every file. `mint_tokens` mints the
/// serviceAccountToken sources (at start); otherwise the token stays as
/// `refresh_bound_tokens` keeps it. `Ok(false)` for any other source.
fn project_volume(
    store: &Store,
    data_dir: &Path,
    pod: &VolumePod<'_>,
    vol: &Volume,
    dir: &Path,
    mint_tokens: bool,
) -> anyhow::Result<bool> {
    let pod_ns = pod.namespace;
    let mut keep: Vec<String> = Vec::new();
    if let Some(cm) = &vol.config_map {
        keep = project_configmap(
            store,
            pod_ns,
            &cm.name,
            dir,
            cm.optional.unwrap_or(false),
            cm.default_mode,
            cm.items.as_deref(),
        )?;
    } else if let Some(sec) = &vol.secret {
        keep = project_secret(
            store,
            pod_ns,
            sec.secret_name.as_deref().unwrap_or_default(),
            dir,
            sec.optional.unwrap_or(false),
            sec.default_mode,
            sec.items.as_deref(),
        )?;
    } else if let Some(dapi) = &vol.downward_api {
        keep = project_downward_api(store, pod, dapi.items.as_deref(), dir, dapi.default_mode)?;
    } else if let Some(proj) = &vol.projected {
        for src in proj.sources.iter().flatten() {
            if let Some(s) = &src.secret {
                keep.extend(project_secret(
                    store,
                    pod_ns,
                    &s.name,
                    dir,
                    s.optional.unwrap_or(false),
                    proj.default_mode,
                    s.items.as_deref(),
                )?);
            } else if let Some(cm) = &src.config_map {
                keep.extend(project_configmap(
                    store,
                    pod_ns,
                    &cm.name,
                    dir,
                    cm.optional.unwrap_or(false),
                    proj.default_mode,
                    cm.items.as_deref(),
                )?);
            } else if let Some(token) = &src.service_account_token {
                if mint_tokens {
                    project_bound_token(
                        store,
                        data_dir,
                        pod,
                        token,
                        &dir.join(&token.path),
                        proj.default_mode,
                    )?;
                }
                keep.push(token.path.clone());
            } else if let Some(dapi) = &src.downward_api {
                keep.extend(project_downward_api(
                    store,
                    pod,
                    dapi.items.as_deref(),
                    dir,
                    proj.default_mode,
                )?);
            } else {
                tracing::warn!(
                    "volume '{}': unsupported projected source, skipping",
                    vol.name
                );
            }
        }
    } else {
        return Ok(false);
    }
    prune_projection(dir, &keep)?;
    Ok(true)
}

/// Removes the files under `dir` other than `keep` (paths relative to
/// `dir`), then the directories left empty.
fn prune_projection(dir: &Path, keep: &[String]) -> std::io::Result<()> {
    let keep: FxHashSet<PathBuf> = keep.iter().map(|k| dir.join(k)).collect();
    prune_dir(dir, &keep)
}

fn prune_dir(dir: &Path, keep: &FxHashSet<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            prune_dir(&path, keep)?;
            if std::fs::read_dir(&path)?.next().is_none() {
                std::fs::remove_dir(&path)?;
            }
        } else if !keep.contains(&path) {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Mint the pod's bound service account token at `path`: signed with the
/// cluster key, bound to this pod and node, for the source's audience and
/// lifetime (an hour unless asked otherwise, never under ten minutes).
fn project_bound_token(
    store: &Store,
    data_dir: &Path,
    pod: &VolumePod<'_>,
    source: &ServiceAccountTokenProjection,
    path: &Path,
    default_mode: Option<i32>,
) -> anyhow::Result<()> {
    use r8s_types::sa_token::{Binding, SigningKey, claims};
    let sa_gvr = GroupVersionResource::service_accounts();
    let sa = store
        .get(&ResourceRef {
            gvr: &sa_gvr,
            namespace: pod.namespace,
            name: pod.service_account,
        })?
        .ok_or_else(|| anyhow::anyhow!("serviceaccount '{}' not found yet", pod.service_account))?;
    let node_gvr = GroupVersionResource::nodes();
    let node_uid = store
        .get(&ResourceRef {
            gvr: &node_gvr,
            namespace: None,
            name: NODE_NAME,
        })?
        .and_then(|n| n["metadata"]["uid"].as_str().map(String::from))
        .unwrap_or_default();
    let binding = Binding {
        namespace: pod.namespace.unwrap_or("default").to_string(),
        service_account: pod.service_account.to_string(),
        service_account_uid: sa["metadata"]["uid"].as_str().unwrap_or("").to_string(),
        pod: Some((pod.name.to_string(), pod.uid.to_string())),
        node: Some((NODE_NAME.to_string(), node_uid)),
    };
    let audiences: Vec<String> = source.audience.iter().cloned().collect();
    let expiration = source.expiration_seconds.unwrap_or(3600).max(600);
    let key = SigningKey::load_or_create(data_dir)?;
    let token = key.mint(&claims(
        &binding,
        &audiences,
        expiration,
        chrono::Utc::now().timestamp(),
    ));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, token)?;
    set_projected_mode(path, default_mode)?;
    Ok(())
}

/// Re-mint the projected tokens of a running pod once they pass 80% of their
/// lifetime, as the kubelet rotates bound tokens.
fn refresh_bound_tokens(store: &Store, data_dir: &Path, pod_value: &serde_json::Value) {
    let Ok(pod) = serde_json::from_value::<Pod>(pod_value.clone()) else {
        return;
    };
    let (Some(spec), Some(name), Some(uid)) = (
        pod.spec.as_ref(),
        pod.metadata.name.as_deref(),
        pod.metadata.uid.as_deref(),
    ) else {
        return;
    };
    let ip = pod_value
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let volume_pod = VolumePod {
        name,
        uid,
        namespace: pod.metadata.namespace.as_deref(),
        service_account: spec.service_account_name.as_deref().unwrap_or("default"),
        ip,
        metadata: &pod_value["metadata"],
        spec,
    };
    let now = chrono::Utc::now().timestamp();
    for vol in spec.volumes.iter().flatten() {
        let Some(proj) = &vol.projected else {
            continue;
        };
        for source in proj.sources.iter().flatten() {
            let Some(token) = &source.service_account_token else {
                continue;
            };
            let path = data_dir
                .join("pod-data")
                .join(uid)
                .join(&vol.name)
                .join(&token.path);
            let Some(claims) = std::fs::read_to_string(&path)
                .ok()
                .and_then(|t| r8s_types::sa_token::unverified_claims(&t))
            else {
                continue;
            };
            let (Some(iat), Some(exp)) = (claims["iat"].as_i64(), claims["exp"].as_i64()) else {
                continue;
            };
            if now < iat + (exp - iat) * 4 / 5 {
                continue;
            }
            if let Err(e) = project_bound_token(
                store,
                data_dir,
                &volume_pod,
                token,
                &path,
                proj.default_mode,
            ) {
                tracing::warn!("pod '{name}': token refresh failed: {e}");
            }
        }
    }
}

/// Write downward-API items: each file holds the pod field it names.
fn project_downward_api(
    store: &Store,
    pod: &VolumePod<'_>,
    items: Option<&[DownwardAPIVolumeFile]>,
    dir: &Path,
    default_mode: Option<i32>,
) -> anyhow::Result<Vec<String>> {
    let mut written = Vec::new();
    for item in items.unwrap_or_default() {
        let value = if let Some(f) = &item.field_ref {
            resolve_field_ref_in(
                &f.field_path,
                pod.name,
                pod.namespace.unwrap_or("default"),
                pod.uid,
                pod.ip,
                pod.service_account,
                pod.metadata,
            )
            .unwrap_or_default()
        } else if let Some(rf) = &item.resource_field_ref {
            pod.spec
                .containers
                .iter()
                .find(|c| rf.container_name.as_deref() == Some(c.name.as_str()))
                .and_then(|c| resource_field_value(store, pod.spec, c, rf))
                .unwrap_or_default()
        } else {
            String::new()
        };
        let path = dir.join(&item.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, value)?;
        set_projected_mode(&path, item.mode.or(default_mode))?;
        written.push(item.path.clone());
    }
    Ok(written)
}

/// A projected file: relative path, contents, mode.
type ProjectedFile<'a> = (String, &'a [u8], Option<i32>);

/// The files a Secret or ConfigMap projects: every key, or just the `items`
/// listed, at their paths and modes.
fn projected_files<'a>(
    data: &'a BTreeMap<String, Vec<u8>>,
    items: Option<&'a [KeyToPath]>,
    optional: bool,
    what: &str,
) -> anyhow::Result<Vec<ProjectedFile<'a>>> {
    match items {
        None => Ok(data
            .iter()
            .map(|(k, v)| (k.clone(), v.as_slice(), None))
            .collect()),
        Some(items) => {
            let mut out = Vec::new();
            for item in items {
                match data.get(&item.key) {
                    Some(v) => out.push((item.path.clone(), v.as_slice(), item.mode)),
                    None if optional => {}
                    None => anyhow::bail!("{what} has no key '{}'", item.key),
                }
            }
            Ok(out)
        }
    }
}

/// Writes `contents`, or the `items` picked from it, under `dir`; the
/// relative paths written.
fn write_projected_files(
    dir: &Path,
    contents: &BTreeMap<String, Vec<u8>>,
    items: Option<&[KeyToPath]>,
    optional: bool,
    what: &str,
    default_mode: Option<i32>,
) -> anyhow::Result<Vec<String>> {
    let mut written = Vec::new();
    for (file, bytes, mode) in projected_files(contents, items, optional, what)? {
        let path = dir.join(&file);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, bytes)?;
        set_projected_mode(&path, mode.or(default_mode))?;
        written.push(file);
    }
    Ok(written)
}

/// Base64 bytes decoded; the text itself when it isn't base64.
fn decode_base64_or_raw(text: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .unwrap_or_else(|_| text.as_bytes().to_vec())
}

fn project_configmap(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &Path,
    optional: bool,
    default_mode: Option<i32>,
    items: Option<&[KeyToPath]>,
) -> anyhow::Result<Vec<String>> {
    let gvr = GroupVersionResource::configmaps();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let cm = match store.get(&resource_ref)? {
        Some(cm) => cm,
        // An `optional: true` source that doesn't exist is mounted empty, not
        // an error — matching k8s. Without this, charts that reference
        // not-yet-created TLS/config secrets (argo-cd) never start their pods.
        None if optional => return Ok(Vec::new()),
        None => anyhow::bail!("configmap '{name}' not found"),
    };
    let mut contents: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (key, value) in cm
        .get("data")
        .and_then(|v| v.as_object())
        .into_iter()
        .flatten()
    {
        if let Some(text) = value.as_str() {
            contents.insert(key.clone(), text.as_bytes().to_vec());
        }
    }
    for (key, value) in cm
        .get("binaryData")
        .and_then(|v| v.as_object())
        .into_iter()
        .flatten()
    {
        if let Some(b64) = value.as_str() {
            contents.insert(key.clone(), decode_base64_or_raw(b64));
        }
    }
    write_projected_files(dir, &contents, items, optional, "configmap", default_mode)
}

/// Apply a volume source's `defaultMode` to a projected file. Kubernetes
/// defaults to 0644; the mode matters because charts mount entrypoint and
/// probe scripts from ConfigMaps with `defaultMode: 0755` (bitnami redis runs
/// `/opt/bitnami/scripts/start-scripts/start-master.sh` straight from such a
/// mount) — without the exec bit those containers can't even start.
fn set_projected_mode(path: &Path, mode: Option<i32>) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = mode.unwrap_or(0o644) as u32 & 0o7777;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

fn project_secret(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    dir: &Path,
    optional: bool,
    default_mode: Option<i32>,
    items: Option<&[KeyToPath]>,
) -> anyhow::Result<Vec<String>> {
    let gvr = GroupVersionResource::secrets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let secret = match store.get(&resource_ref)? {
        Some(secret) => secret,
        // An `optional: true` source that doesn't exist is mounted empty, not
        // an error — matching k8s. argo-cd's server/repo-server mount optional
        // TLS secrets that only get created later, so failing here would wedge
        // every such pod in Pending.
        None if optional => return Ok(Vec::new()),
        None => anyhow::bail!("secret '{name}' not found"),
    };
    let mut contents: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (key, value) in secret
        .get("data")
        .and_then(|v| v.as_object())
        .into_iter()
        .flatten()
    {
        if let Some(b64) = value.as_str() {
            contents.insert(key.clone(), decode_base64_or_raw(b64));
        }
    }
    write_projected_files(dir, &contents, items, optional, "secret", default_mode)
}

/// Look up `valueFrom.secretKeyRef`: read `key` from the named Secret in the
/// pod's namespace and base64-decode it. `None` (→ empty value) if the secret,
/// key, or decode is missing — best-effort, matching the downward-API path.
/// Every `data` entry of a ConfigMap, for `envFrom`.
fn configmap_entries(store: &Store, namespace: Option<&str>, name: &str) -> Vec<(String, String)> {
    let gvr = GroupVersionResource::configmaps();
    store
        .get(&ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        })
        .ok()
        .flatten()
        .and_then(|cm| cm.get("data").and_then(|d| d.as_object()).cloned())
        .map(|data| {
            data.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Every `data` entry of a Secret, decoded, for `envFrom`.
fn secret_entries(store: &Store, namespace: Option<&str>, name: &str) -> Vec<(String, String)> {
    let gvr = GroupVersionResource::secrets();
    store
        .get(&ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        })
        .ok()
        .flatten()
        .and_then(|secret| secret.get("data").and_then(|d| d.as_object()).cloned())
        .map(|data| {
            data.iter()
                .filter_map(|(k, v)| {
                    let b64 = v.as_str()?;
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .unwrap_or_else(|_| b64.as_bytes().to_vec());
                    Some((k.clone(), String::from_utf8_lossy(&bytes).into_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn secret_value(store: &Store, namespace: Option<&str>, name: &str, key: &str) -> Option<String> {
    let gvr = GroupVersionResource::secrets();
    let secret = store
        .get(&ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        })
        .ok()??;
    let b64 = secret
        .get("data")
        .and_then(|d| d.get(key))
        .and_then(|v| v.as_str())?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    String::from_utf8(bytes).ok()
}

/// Look up `valueFrom.configMapKeyRef`: read `key` from the named ConfigMap in
/// the pod's namespace. `None` (→ empty value) if absent.
fn configmap_value(
    store: &Store,
    namespace: Option<&str>,
    name: &str,
    key: &str,
) -> Option<String> {
    let gvr = GroupVersionResource::configmaps();
    let cm = store
        .get(&ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        })
        .ok()??;
    cm.get("data")
        .and_then(|d| d.get(key))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Resolve a `valueFrom.fieldRef` field path against the downward API. Returns
/// `None` for paths r8s doesn't model (caller substitutes an empty string,
/// matching how kubelet treats unknown refs in practice for our subset).
/// `resolve_field_ref` with the pod's metadata, for
/// `metadata.annotations['key']` and `metadata.labels['key']`.
#[allow(clippy::too_many_arguments)]
fn resolve_field_ref_in(
    field_path: &str,
    pod_name: &str,
    pod_ns: &str,
    pod_uid: &str,
    pod_ip: &str,
    sa_name: &str,
    metadata: &serde_json::Value,
) -> Option<String> {
    if let Some(rest) = field_path.strip_prefix("metadata.")
        && let Some((map, key)) = rest.split_once("['")
        && let Some(key) = key.strip_suffix("']")
        && matches!(map, "annotations" | "labels")
    {
        return Some(metadata[map][key].as_str().unwrap_or("").to_string());
    }
    match field_path {
        "metadata.name" => Some(pod_name.into()),
        "metadata.namespace" => Some(pod_ns.into()),
        "metadata.uid" => Some(pod_uid.into()),
        // Whole maps, as downward-API volume files take them.
        "metadata.labels" | "metadata.annotations" => Some(format_map(
            &metadata[field_path.trim_start_matches("metadata.")],
        )),
        "status.podIP" | "status.podIPs" => Some(pod_ip.into()),
        // status.hostIP: the node is the bridge gateway host.
        "status.hostIP" | "status.hostIPs" => Some(r8s_types::NODE_INTERNAL_IP.into()),
        "spec.nodeName" => Some(NODE_NAME.into()),
        "spec.serviceAccountName" => Some(sa_name.into()),
        _ => None,
    }
}

/// A label or annotation map as a downward-API file: `key="value"` lines in
/// key order, the value Go-quoted, no trailing newline.
fn format_map(map: &serde_json::Value) -> String {
    let mut entries: Vec<(&String, &serde_json::Value)> =
        map.as_object().into_iter().flatten().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
        .iter()
        .map(|(k, v)| format!("{k}={}", go_quote(v.as_str().unwrap_or(""))))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `text` as Go's `%q` prints it.
fn go_quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c == '\x7f' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Expand `$(VAR)` references in `input` against `env`, per the k8s
/// container-environment contract: `$(VAR)` → the value of `VAR` if defined,
/// `$$(VAR)` → the literal `$(VAR)`, and an undefined `$(VAR)` is left as-is.
fn expand_vars(input: &str, env: &[(String, String)]) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    let mut chunk_start = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            // `$$(VAR)` -> literal `$(VAR)`
            if bytes[i + 1] == b'$'
                && i + 2 < bytes.len()
                && bytes[i + 2] == b'('
                && let Some(close) = bytes[i + 3..].iter().position(|&b| b == b')')
            {
                out.push_str(&input[chunk_start..i]);
                out.push_str(&input[i + 1..i + 3 + close + 1]);
                i += 3 + close + 1;
                chunk_start = i;
                continue;
            }
            // `$(VAR)` -> substitution
            if bytes[i + 1] == b'('
                && let Some(close) = bytes[i + 2..].iter().position(|&b| b == b')')
            {
                let name = &input[i + 2..i + 2 + close];
                out.push_str(&input[chunk_start..i]);
                if let Some((_, v)) = env.iter().find(|(k, _)| k == name) {
                    out.push_str(v);
                } else {
                    out.push_str(&input[i..i + 2 + close + 1]);
                }
                i += 2 + close + 1;
                chunk_start = i;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&input[chunk_start..]);
    out
}

/// The `<SVC>_SERVICE_HOST`-style variables for every ClusterIP Service in
/// the pod's namespace, as upstream's kubelet injects them.
fn service_env(store: &Store, pod_ns: Option<&str>) -> Vec<(String, String)> {
    let mut env = Vec::new();
    let services: Vec<r8s_types::Service> = store
        .list_as(&GroupVersionResource::services(), pod_ns)
        .unwrap_or_default();
    for svc in services {
        let (Some(name), Some(spec)) = (svc.metadata.name.as_deref(), svc.spec.as_ref()) else {
            continue;
        };
        let Some(ip) = spec
            .cluster_ip
            .as_deref()
            .filter(|ip| !ip.is_empty() && *ip != "None")
        else {
            continue;
        };
        if spec.type_.as_deref() == Some("ExternalName") || name == "kubernetes" {
            continue;
        }
        let prefix = name.to_uppercase().replace('-', "_");
        let ports = spec.ports.as_deref().unwrap_or_default();
        env.push((format!("{prefix}_SERVICE_HOST"), ip.to_string()));
        if let Some(first) = ports.first() {
            env.push((format!("{prefix}_SERVICE_PORT"), first.port.to_string()));
            let proto = first.protocol.as_deref().unwrap_or("TCP").to_lowercase();
            env.push((
                format!("{prefix}_PORT"),
                format!("{proto}://{ip}:{}", first.port),
            ));
        }
        for port in ports {
            let proto = port.protocol.as_deref().unwrap_or("TCP");
            let lower = proto.to_lowercase();
            if let Some(pname) = port.name.as_deref().filter(|n| !n.is_empty()) {
                env.push((
                    format!(
                        "{prefix}_SERVICE_PORT_{}",
                        pname.to_uppercase().replace('-', "_")
                    ),
                    port.port.to_string(),
                ));
            }
            let base = format!("{prefix}_PORT_{}_{}", port.port, proto.to_uppercase());
            env.push((base.clone(), format!("{lower}://{ip}:{}", port.port)));
            env.push((format!("{base}_PROTO"), lower.clone()));
            env.push((format!("{base}_PORT"), port.port.to_string()));
            env.push((format!("{base}_ADDR"), ip.to_string()));
        }
    }
    env
}

/// Mounts for a container's volumeMounts. `subPathExpr` is expanded with
/// the container's env; a subPath that is absolute or escapes the volume
/// is a configuration error.
fn resolve_mounts(
    volume_mounts: &[VolumeMount],
    volume_paths: &FxHashMap<String, String>,
    env: &[(String, String)],
) -> Result<Vec<Mount>, String> {
    let mut mounts = Vec::new();
    for vm in volume_mounts {
        let Some(base) = volume_paths.get(&vm.name) else {
            continue;
        };
        let sub_path = match (
            vm.sub_path.as_deref().filter(|s| !s.is_empty()),
            vm.sub_path_expr.as_deref().filter(|s| !s.is_empty()),
        ) {
            (Some(sub), _) => Some(sub.to_string()),
            (None, Some(expr)) => Some(expand_vars(expr, env)),
            (None, None) => None,
        };
        if let Some(sub) = sub_path.as_deref() {
            let path = Path::new(sub);
            if path.is_absolute() {
                return Err(format!(
                    "failed to resolve subPath \"{sub}\" of volume \"{}\": must not be an absolute path",
                    vm.name
                ));
            }
            if path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(format!(
                    "failed to resolve subPath \"{sub}\" of volume \"{}\": must not contain '..'",
                    vm.name
                ));
            }
        }
        let host_path = resolve_sub_path(base, sub_path.as_deref());
        mounts.push(Mount {
            host_path,
            container_path: vm.mount_path.clone(),
            readonly: vm.read_only.unwrap_or(false),
        });
    }
    Ok(mounts)
}

/// The host path of a volume, or of `sub_path` within it.
fn resolve_sub_path(base: &str, sub_path: Option<&str>) -> String {
    {
        {
            // `subPath` mounts a single file/dir from within the volume at the
            // mountPath, rather than the whole volume. Without honoring it, a
            // file-subPath mount (e.g. prometheus's web-config.yaml from a
            // Secret) bind-mounts the whole secret directory onto the file
            // path, so the target reads as a directory.
            match sub_path {
                Some(sub) => {
                    let joined = Path::new(base).join(sub);
                    // A subPath into a directory-backed volume (emptyDir / PVC /
                    // hostPath) must already exist on the host or runc fails the
                    // bind mount ("no such file or directory"); k8s materializes
                    // it on demand. Create it only when missing, so a subPath
                    // that targets an already-projected configMap/secret *file*
                    // is mounted as that file instead of being shadowed by a new
                    // directory. Best-effort: a failure here surfaces as the same
                    // runc mount error it does today.
                    if !joined.exists() {
                        let _ = std::fs::create_dir_all(&joined);
                    }
                    joined.to_string_lossy().into_owned()
                }
                None => base.to_string(),
            }
        }
    }
}

/// An emptyDir is world-writable (any user in the pod may use it); with
/// `medium: Memory` it is a tmpfs, sized by `sizeLimit` when given.
fn prepare_empty_dir(dir: &Path, empty: &r8s_types::EmptyDirVolumeSource) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if empty.medium.as_deref() == Some("Memory") && !is_mount_point(dir) {
        let mut options = String::from("mode=0777");
        if let Some(size) = empty
            .size_limit
            .as_ref()
            .and_then(|q| r8s_types::quantity::parse_quantity(&q.0))
            .filter(|b| *b > 0.0)
        {
            options.push_str(&format!(",size={}", size as u64));
        }
        let status = std::process::Command::new("mount")
            .args(["-t", "tmpfs", "-o", &options, "tmpfs"])
            .arg(dir)
            .status();
        match status {
            Ok(st) if st.success() => {}
            Ok(st) => tracing::warn!("emptyDir tmpfs mount at {} failed: {st}", dir.display()),
            Err(e) => tracing::warn!("emptyDir tmpfs mount at {}: {e}", dir.display()),
        }
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o777))?;
    Ok(())
}

/// Whether `dir` is a mount point (its device differs from its parent's).
fn is_mount_point(dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Some(parent) = dir.parent() else {
        return false;
    };
    match (std::fs::metadata(dir), std::fs::metadata(parent)) {
        (Ok(a), Ok(b)) => a.dev() != b.dev(),
        _ => false,
    }
}

fn cleanup_pod_volumes(data_dir: &Path, pod_uid: &str) {
    let dir = data_dir.join("pod-data").join(pod_uid);
    if !dir.exists() {
        return;
    }
    // tmpfs-backed emptyDirs are unmounted before the tree goes.
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if is_mount_point(&entry.path()) {
                let _ = std::process::Command::new("umount")
                    .arg(entry.path())
                    .status();
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Re-project the ConfigMap, Secret and downward-API files of a running
/// pod's volumes, so edits to their sources reach the pod as upstream's
/// kubelet does on its sync period. Tokens are refreshed separately.
fn refresh_volume_projections(store: &Store, data_dir: &Path, pod_value: &serde_json::Value) {
    let Ok(pod) = serde_json::from_value::<Pod>(pod_value.clone()) else {
        return;
    };
    let (Some(spec), Some(name), Some(uid)) = (
        pod.spec.as_ref(),
        pod.metadata.name.as_deref(),
        pod.metadata.uid.as_deref(),
    ) else {
        return;
    };
    let ip = pod_value
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let volume_pod = VolumePod {
        name,
        uid,
        namespace: pod.metadata.namespace.as_deref(),
        service_account: spec.service_account_name.as_deref().unwrap_or("default"),
        ip,
        metadata: &pod_value["metadata"],
        spec,
    };
    for vol in spec.volumes.iter().flatten() {
        let dir = data_dir.join("pod-data").join(uid).join(&vol.name);
        if !dir.exists() {
            continue;
        }
        if let Err(e) = project_volume(store, data_dir, &volume_pod, vol, &dir, false) {
            tracing::debug!("pod '{name}': volume '{}' refresh: {e}", vol.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{container_ids_from_spec, expand_vars, resolve_field_ref_in};

    #[test]
    fn container_ids_from_spec_covers_init_then_main() {
        // The untracked-delete and orphan-sweep paths reap by these derived ids,
        // so they must match exactly what `build_container_config` would create
        // via `ContainerId::derive`, init containers first.
        let pod: r8s_types::Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "web-0", "namespace": "default", "uid": "u1" },
            "spec": {
                "initContainers": [{ "name": "init-db", "image": "busybox" }],
                "containers": [
                    { "name": "app", "image": "nginx" },
                    { "name": "sidecar", "image": "envoy" }
                ]
            }
        }))
        .unwrap();
        let ids: Vec<String> = container_ids_from_spec(&pod, "web-0")
            .into_iter()
            .map(|c| c.0)
            .collect();
        assert_eq!(
            ids,
            [
                "default_web-0_init-db",
                "default_web-0_app",
                "default_web-0_sidecar"
            ]
        );
    }

    #[test]
    fn container_ids_from_spec_empty_without_spec() {
        let pod: r8s_types::Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "x" }
        }))
        .unwrap();
        assert!(container_ids_from_spec(&pod, "x").is_empty());
    }

    #[tokio::test]
    async fn reap_orphans_reaps_dead_pod_keeps_live_pod() {
        use super::{PodState, reap_orphans};
        use r8s_runtime::{ContainerConfig, ContainerRuntime, MockRuntime};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let runtime = MockRuntime::new();
        let state = PodState::new();
        let pods = GroupVersionResource::pods();

        let cfg = |pod: &str, uid: &str| ContainerConfig {
            name: format!("{pod}_main"),
            pod_uid: uid.to_string(),
            pod_name: pod.to_string(),
            container_name: "main".to_string(),
            ..Default::default()
        };

        // A live pod: present in the store, with its container in the backend.
        // The store assigns the uid on create (as the API server does), and the
        // kubelet labels the container with that same uid — so read it back.
        let created = store
            .create(
                ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "live",
                },
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "live", "namespace": "default" },
                    "spec": { "containers": [{ "name": "main", "image": "nginx" }] }
                }),
            )
            .unwrap();
        let live_uid = created
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let live_cid = runtime
            .create_container(&cfg("live", &live_uid))
            .await
            .unwrap();

        // An orphan: its container exists in the backend but no Pod object does
        // — exactly the crash/dropped-delete leak the sweep must catch.
        let dead_cid = runtime
            .create_container(&cfg("dead", "uid-dead"))
            .await
            .unwrap();

        reap_orphans(&store, &runtime, &state, dir.path(), "test").await;

        assert!(
            runtime.container_status(&dead_cid).await.is_err(),
            "orphaned pod's container must be reaped"
        );
        assert!(
            runtime.container_status(&live_cid).await.is_ok(),
            "live pod's container must survive"
        );
    }

    #[tokio::test]
    async fn reap_orphans_keeps_pod_whose_body_fails_to_parse() {
        // Liveness is decided from the uid alone. Reading the whole Pod and
        // dropping any that fail to deserialize would let one malformed field
        // reap a running pod's containers. Store a pod whose `spec` is the wrong
        // JSON type (so `from_value::<Pod>` fails) but whose uid is intact, and
        // confirm its container survives the sweep.
        use super::{PodState, reap_orphans};
        use r8s_runtime::{ContainerConfig, ContainerRuntime, MockRuntime};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let runtime = MockRuntime::new();
        let state = PodState::new();
        let pods = GroupVersionResource::pods();

        let created = store
            .create(
                ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "broken",
                },
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "broken", "namespace": "default" },
                    "spec": "this is not a pod spec"
                }),
            )
            .unwrap();
        let uid = created
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        assert!(
            serde_json::from_value::<r8s_types::Pod>(created).is_err(),
            "test premise: this pod must not fully deserialize"
        );

        let cid = runtime
            .create_container(&ContainerConfig {
                name: "broken_main".to_string(),
                pod_uid: uid,
                pod_name: "broken".to_string(),
                container_name: "main".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        reap_orphans(&store, &runtime, &state, dir.path(), "test").await;

        assert!(
            runtime.container_status(&cid).await.is_ok(),
            "container of a live pod must survive even if the pod body won't parse"
        );
    }

    #[test]
    fn pod_dns_follows_the_policy_and_appends_dns_config() {
        use super::{parse_resolv_conf, pod_dns};
        let spec = |extra: serde_json::Value| -> r8s_types::PodSpec {
            let mut v = serde_json::json!({"containers": []});
            for (k, val) in extra.as_object().unwrap() {
                v[k] = val.clone();
            }
            serde_json::from_value(v).unwrap()
        };
        let cluster = pod_dns(&spec(serde_json::json!({})), "dns-1");
        assert_eq!(cluster.nameservers, vec!["10.244.0.1"]);
        assert_eq!(cluster.searches[0], "dns-1.svc.cluster.local");
        assert_eq!(cluster.options, vec!["ndots:5"]);

        let none = pod_dns(
            &spec(serde_json::json!({"dnsPolicy": "None", "dnsConfig": {
                "nameservers": ["1.1.1.1"], "searches": ["resolv.conf.local"],
                "options": [{"name": "ndots", "value": "2"}, {"name": "debug"}]}})),
            "dns-1",
        );
        assert_eq!(none.nameservers, vec!["1.1.1.1"]);
        assert_eq!(none.searches, vec!["resolv.conf.local"]);
        assert_eq!(none.options, vec!["ndots:2", "debug"]);
        assert_eq!(
            none.render(),
            "nameserver 1.1.1.1\nsearch resolv.conf.local\noptions ndots:2 debug\n"
        );

        let merged = pod_dns(
            &spec(
                serde_json::json!({"dnsConfig": {"searches": ["extra.local"],
                                                   "options": [{"name": "ndots", "value": "1"}]}}),
            ),
            "dns-1",
        );
        assert_eq!(merged.searches.last().unwrap(), "extra.local");
        assert_eq!(merged.options, vec!["ndots:1"]);

        let host_net = pod_dns(&spec(serde_json::json!({"hostNetwork": true})), "dns-1");
        assert!(!host_net.nameservers.contains(&"10.244.0.1".to_string()));

        let parsed = parse_resolv_conf(
            "# comment\nnameserver 192.168.1.1\nsearch lan home\noptions edns0 trust-ad\n",
        );
        assert_eq!(parsed.nameservers, vec!["192.168.1.1"]);
        assert_eq!(parsed.searches, vec!["lan", "home"]);
        assert_eq!(parsed.options, vec!["edns0", "trust-ad"]);
    }

    #[test]
    fn whole_label_and_annotation_maps_project_in_key_order() {
        let meta = serde_json::json!({
            "labels": {"key2": "value2", "key1": "value1"},
            "annotations": {"builder": "bar", "quote": "a\"b"},
        });
        assert_eq!(
            resolve_field_ref_in("metadata.labels", "p", "ns", "u", "ip", "sa", &meta),
            Some("key1=\"value1\"\nkey2=\"value2\"".into())
        );
        assert_eq!(
            resolve_field_ref_in("metadata.annotations", "p", "ns", "u", "ip", "sa", &meta),
            Some("builder=\"bar\"\nquote=\"a\\\"b\"".into())
        );
        assert_eq!(
            resolve_field_ref_in(
                "metadata.labels",
                "p",
                "ns",
                "u",
                "ip",
                "sa",
                &serde_json::Value::Null
            ),
            Some(String::new())
        );
    }

    #[test]
    fn configmap_volumes_project_binary_data_and_drop_removed_keys() {
        use super::{prepare_volumes, project_volume};
        use base64::Engine;
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::{GroupVersionResource, Volume};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let configmaps = GroupVersionResource::configmaps();
        let rref = || ResourceRef {
            gvr: &configmaps,
            namespace: Some("ns"),
            name: "cm",
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode([0xde, 0xca, 0xfe]);
        store
            .create(
                rref(),
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "cm", "namespace": "ns"},
                    "data": {"data-1": "value-1", "data-2": "value-2"},
                    "binaryData": {"dump.bin": b64},
                }),
            )
            .unwrap();
        let volumes: Vec<Volume> = serde_json::from_value(serde_json::json!([
            {"name": "cfg", "configMap": {"name": "cm", "optional": true}}
        ]))
        .unwrap();
        let pod = super::VolumePod {
            name: "p",
            uid: "pod-uid",
            namespace: Some("ns"),
            service_account: "default",
            ip: "10.244.0.9",
            metadata: &serde_json::Value::Null,
            spec: &Default::default(),
        };
        let paths = prepare_volumes(&store, dir.path(), &pod, &volumes).unwrap();
        let vol_dir = std::path::PathBuf::from(&paths["cfg"]);
        assert_eq!(
            std::fs::read(vol_dir.join("dump.bin")).unwrap(),
            [0xde, 0xca, 0xfe]
        );
        assert_eq!(
            std::fs::read_to_string(vol_dir.join("data-2")).unwrap(),
            "value-2"
        );

        let mut cm = store.get(&rref()).unwrap().unwrap();
        cm["data"] = serde_json::json!({"data-1": "value-1b"});
        cm["binaryData"] = serde_json::json!({});
        store.update(&rref(), &cm).unwrap();
        project_volume(&store, dir.path(), &pod, &volumes[0], &vol_dir, false).unwrap();
        assert_eq!(
            std::fs::read_to_string(vol_dir.join("data-1")).unwrap(),
            "value-1b"
        );
        assert!(
            !vol_dir.join("data-2").exists(),
            "removed key leaves no file"
        );
        assert!(!vol_dir.join("dump.bin").exists());

        store.delete(&rref()).unwrap();
        project_volume(&store, dir.path(), &pod, &volumes[0], &vol_dir, false).unwrap();
        assert!(
            std::fs::read_dir(&vol_dir).unwrap().next().is_none(),
            "a deleted optional configmap empties the volume"
        );
    }

    #[test]
    fn prepare_volumes_projects_secret_source() {
        use super::prepare_volumes;
        use base64::Engine;
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::{GroupVersionResource, Volume};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();

        // Shaped like bitnami kafka's `kafka-user-passwords`: one key per
        // password file, base64-encoded as Secret data always is.
        let secrets = GroupVersionResource::secrets();
        let b64 = base64::engine::general_purpose::STANDARD.encode("r8s-integration");
        store
            .create(
                ResourceRef {
                    gvr: &secrets,
                    namespace: Some("kafka"),
                    name: "kafka-user-passwords",
                },
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Secret",
                    "metadata": { "name": "kafka-user-passwords", "namespace": "kafka" },
                    "data": { "client-passwords": b64 }
                }),
            )
            .unwrap();

        // A single-source projected volume, exactly the `kafka-sasl` shape.
        let volumes: Vec<Volume> = serde_json::from_value(serde_json::json!([
            {
                "name": "kafka-sasl",
                "projected": {
                    "sources": [ { "secret": { "name": "kafka-user-passwords" } } ]
                }
            }
        ]))
        .unwrap();

        let paths = prepare_volumes(
            &store,
            dir.path(),
            &super::VolumePod {
                name: "kafka-0",
                uid: "pod-uid",
                namespace: Some("kafka"),
                service_account: "default",
                ip: "10.244.0.9",
                metadata: &serde_json::Value::Null,
                spec: &Default::default(),
            },
            &volumes,
        )
        .unwrap();
        let vol_dir = paths
            .get("kafka-sasl")
            .expect("projected volume must resolve to a host dir");
        let contents =
            std::fs::read_to_string(std::path::Path::new(vol_dir).join("client-passwords"))
                .unwrap();
        assert_eq!(
            contents, "r8s-integration",
            "secret value must be decoded into the file"
        );
    }

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn expands_defined_var() {
        let e = env(&[("POD_NAMESPACE", "cert-manager")]);
        assert_eq!(
            expand_vars("--ns=$(POD_NAMESPACE)", &e),
            "--ns=cert-manager"
        );
        assert_eq!(
            expand_vars("svc.$(POD_NAMESPACE).svc", &e),
            "svc.cert-manager.svc"
        );
    }

    #[test]
    fn leaves_undefined_var_literal() {
        // k8s contract: an undefined reference passes through unchanged.
        assert_eq!(expand_vars("$(NOPE)", &env(&[])), "$(NOPE)");
    }

    #[test]
    fn double_dollar_escapes_to_literal() {
        let e = env(&[("VAR", "x")]);
        assert_eq!(expand_vars("$$(VAR)", &e), "$(VAR)");
    }

    #[test]
    fn no_refs_passthrough_and_multiple_refs() {
        let e = env(&[("A", "1"), ("B", "2")]);
        assert_eq!(expand_vars("plain text", &e), "plain text");
        assert_eq!(expand_vars("$(A)-$(B)", &e), "1-2");
    }

    #[test]
    fn empty_env_value_defers_to_value_from() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = r8s_store::Store::open(&dir.path().join("s.db")).unwrap();
        let pod = serde_json::json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {"containers": [{"name": "c", "image": "busybox", "env": [
                {"name": "POD_IP", "value": "", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                {"name": "EMPTY", "value": ""},
                {"name": "CPU_LIMIT", "value": "", "valueFrom": {"resourceFieldRef":
                    {"containerName": "", "resource": "limits.cpu", "divisor": "0"}}}
            ], "resources": {"limits": {"cpu": "2"}}}]}
        });
        let spec: super::PodSpec = serde_json::from_value(pod["spec"].clone()).unwrap();
        let config = super::build_container_config(
            &store,
            "p",
            Some("ns"),
            &pod,
            &spec,
            &spec.containers[0],
            &super::FxHashMap::default(),
            "10.244.0.9",
            None,
        )
        .unwrap();
        let env = |name: &str| {
            config
                .env
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(env("POD_IP"), Some("10.244.0.9"));
        assert_eq!(env("EMPTY"), Some(""));
        assert_eq!(env("CPU_LIMIT"), Some("2"));
    }

    #[test]
    fn field_ref_downward_api() {
        assert_eq!(
            resolve_field_ref_in(
                "metadata.namespace",
                "p",
                "cert-manager",
                "u",
                "10.244.0.5",
                "sa",
                &serde_json::Value::Null
            ),
            Some("cert-manager".into())
        );
        assert_eq!(
            resolve_field_ref_in(
                "status.podIP",
                "p",
                "ns",
                "u",
                "10.244.0.5",
                "sa",
                &serde_json::Value::Null
            ),
            Some("10.244.0.5".into())
        );
        // Annotation and label refs read the pod metadata; an absent key is "".
        assert_eq!(
            resolve_field_ref_in(
                "metadata.labels['x']",
                "p",
                "ns",
                "u",
                "ip",
                "sa",
                &serde_json::Value::Null
            ),
            Some(String::new())
        );
        let metadata = serde_json::json!({"labels": {"x": "y"}, "annotations": {"a": "b"}});
        assert_eq!(
            super::resolve_field_ref_in(
                "metadata.labels['x']",
                "p",
                "ns",
                "u",
                "ip",
                "sa",
                &metadata
            ),
            Some("y".into())
        );
        assert_eq!(
            super::resolve_field_ref_in(
                "metadata.annotations['a']",
                "p",
                "ns",
                "u",
                "ip",
                "sa",
                &metadata
            ),
            Some("b".into())
        );
    }

    #[test]
    fn start_guard_blocks_concurrent_starts_and_releases_on_drop() {
        use super::PodState;
        let state = PodState::new();

        // First claim succeeds; a second claim for the same pod is refused
        // while the first guard is alive — this is what stops a tick resync and
        // a watch event from both driving the same pod into start_containers.
        let g1 = state.try_begin_start("uid-a");
        assert!(g1.is_some());
        assert!(state.try_begin_start("uid-a").is_none());
        // A different pod is independent.
        assert!(state.try_begin_start("uid-b").is_some());

        // Dropping the guard releases the claim so a later tick can retry.
        drop(g1);
        assert!(
            state.try_begin_start("uid-a").is_some(),
            "claim must be retryable after the guard drops"
        );
    }

    #[test]
    fn subpath_mount_creates_missing_directory() {
        // bitnami/postgresql mounts one emptyDir at several mountPaths via
        // subPath (tmp-dir, app-conf-dir, ...). The subdir doesn't exist until
        // we make it, and runc refuses to bind-mount a missing source — which
        // wedged the pod in Pending. resolve_mounts must materialize it.
        use super::resolve_mounts;
        use rustc_hash::FxHashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let base = dir.path().join("empty-dir");
        std::fs::create_dir_all(&base).unwrap();

        let mut paths: FxHashMap<String, String> = FxHashMap::default();
        paths.insert("empty-dir".into(), base.to_string_lossy().into_owned());

        let vms: Vec<r8s_types::VolumeMount> = serde_json::from_value(serde_json::json!([
            { "name": "empty-dir", "mountPath": "/tmp", "subPath": "tmp-dir" }
        ]))
        .unwrap();

        let mounts = resolve_mounts(&vms, &paths, &[]).unwrap();
        let expected = base.join("tmp-dir");
        assert!(
            expected.is_dir(),
            "subPath dir should be created on the host"
        );
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, expected.to_string_lossy());
        assert_eq!(mounts[0].container_path, "/tmp");
    }

    #[test]
    fn subpath_mount_preserves_existing_file() {
        // A subPath that targets an already-projected configMap/secret *file*
        // (e.g. prometheus's web-config.yaml) must bind-mount that file, not be
        // shadowed by a freshly-created directory.
        use super::resolve_mounts;
        use rustc_hash::FxHashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let base = dir.path().join("secret-vol");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("web-config.yaml"), b"tls: {}").unwrap();

        let mut paths: FxHashMap<String, String> = FxHashMap::default();
        paths.insert("secret-vol".into(), base.to_string_lossy().into_owned());

        let vms: Vec<r8s_types::VolumeMount> = serde_json::from_value(serde_json::json!([
            { "name": "secret-vol", "mountPath": "/etc/web-config.yaml", "subPath": "web-config.yaml" }
        ]))
        .unwrap();

        let mounts = resolve_mounts(&vms, &paths, &[]).unwrap();
        let target = base.join("web-config.yaml");
        assert!(target.is_file(), "existing file subPath must remain a file");
        assert_eq!(mounts[0].host_path, target.to_string_lossy());
    }

    #[test]
    fn ip_pool_reserve_skips_reserved_and_fills_gaps() {
        use super::IpPool;
        // Reserving .5 (a restart-recovered IP) opens .2..=.4 as gaps. Fresh
        // allocations fill those, then jump past the reserved high-water mark —
        // never re-issuing .5.
        let mut pool = IpPool::new();
        pool.reserve(5);
        let mut got: Vec<u32> = std::iter::from_fn(|| pool.allocate()).take(4).collect();
        got.sort();
        assert_eq!(got, [2, 3, 4, 6]);
        assert!(!got.contains(&5), "reserved IP must never be allocated");
    }

    #[test]
    fn ip_pool_reserve_is_idempotent() {
        use super::IpPool;
        let mut pool = IpPool::new();
        pool.reserve(5);
        pool.reserve(5);
        let mut got: Vec<u32> = std::iter::from_fn(|| pool.allocate()).take(4).collect();
        got.sort();
        assert_eq!(got, [2, 3, 4, 6]);
    }

    #[test]
    fn ip_pool_release_is_idempotent() {
        use super::IpPool;
        use r8s_types::IP_POOL_MAX;
        let mut pool = IpPool::new();
        let a = pool.allocate().unwrap();
        pool.release(a);
        pool.release(a); // double release must not queue the address twice
        assert_eq!(pool.released.len(), 1);
        assert_ne!(
            pool.allocate(),
            Some(a),
            "a fresh address goes before a released one"
        );
        pool.next = IP_POOL_MAX + 1;
        assert_eq!(
            pool.allocate(),
            Some(a),
            "released IP is reused once exhausted"
        );
        assert_eq!(pool.allocate(), None, "but never handed out a second time");
    }

    #[test]
    fn ip_pool_release_ignores_never_allocated() {
        use super::IpPool;
        use r8s_types::IP_POOL_MIN;
        let mut pool = IpPool::new();
        pool.release(99); // never handed out -> ignored, not added to free
        assert_eq!(pool.allocate(), Some(IP_POOL_MIN));
    }

    #[test]
    fn host_path_types_create_or_require() {
        use super::prepare_host_path;
        let dir = tempfile::TempDir::new().unwrap();
        let sub = dir.path().join("a/b");
        let got = prepare_host_path(sub.to_str().unwrap(), Some("DirectoryOrCreate")).unwrap();
        assert!(std::path::Path::new(&got).is_dir());
        let file = dir.path().join("c/d.txt");
        prepare_host_path(file.to_str().unwrap(), Some("FileOrCreate")).unwrap();
        assert!(file.is_file());
        assert!(prepare_host_path("/definitely/missing", Some("Directory")).is_err());
        assert_eq!(
            prepare_host_path("/definitely/missing", None).unwrap(),
            "/definitely/missing"
        );
    }

    #[test]
    fn host_port_conflict_rejects_later_pod_only() {
        use super::host_port_conflict;
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::{GroupVersionResource, Pod};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let pods = GroupVersionResource::pods();
        let make = |name: &str, phase: &str, port: i64| {
            store
                .create(
                    ResourceRef {
                        gvr: &pods,
                        namespace: Some("default"),
                        name,
                    },
                    &serde_json::json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": name, "namespace": "default"},
                        "spec": {"nodeName": "r8s-node", "containers": [{
                            "name": "c", "image": "img",
                            "ports": [{"containerPort": port, "hostPort": port}]
                        }]},
                        "status": {"phase": phase}
                    }),
                )
                .unwrap()
        };
        let first = make("first", "Running", 21017);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = make("second", "Pending", 21017);
        let other = make("other", "Pending", 8080);
        let first: Pod = serde_json::from_value(first).unwrap();
        let second: Pod = serde_json::from_value(second).unwrap();
        let other: Pod = serde_json::from_value(other).unwrap();

        assert_eq!(
            host_port_conflict(&store, &second).as_deref(),
            Some("TCP/21017")
        );
        assert_eq!(host_port_conflict(&store, &other), None);
        // The running holder keeps its port even though another pod wants it.
        assert_eq!(host_port_conflict(&store, &first), None);
    }

    async fn tracked_ids(state: &super::PodState, uid: &str) -> Vec<r8s_runtime::ContainerId> {
        state
            .get(uid)
            .await
            .expect("pod tracked")
            .lock()
            .await
            .container_ids
            .clone()
    }

    #[test]
    fn deadline_counts_from_the_pod_start_time() {
        use super::deadline_exceeded;
        let old = (chrono::Utc::now() - chrono::Duration::seconds(100)).to_rfc3339();
        let pod = |deadline: Option<i64>| {
            let mut p = serde_json::json!({"spec": {}, "status": {"startTime": old}});
            if let Some(d) = deadline {
                p["spec"]["activeDeadlineSeconds"] = serde_json::json!(d);
            }
            p
        };
        assert!(!deadline_exceeded(&pod(None)));
        assert!(!deadline_exceeded(&pod(Some(1000))));
        assert!(deadline_exceeded(&pod(Some(50))));
        assert!(!deadline_exceeded(
            &serde_json::json!({"spec": {"activeDeadlineSeconds": 1}})
        ));
    }

    #[test]
    fn termination_message_prefers_the_file_then_the_log_tail() {
        use super::{ContainerId, termination_message};
        use r8s_types::Container;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let cid = ContainerId("p_c".into());
        let fallback: Container = serde_json::from_value(serde_json::json!({
            "name": "c", "terminationMessagePolicy": "FallbackToLogsOnError"
        }))
        .unwrap();
        let plain: Container = serde_json::from_value(serde_json::json!({"name": "c"})).unwrap();

        // Nothing written, no fallback: no message.
        assert_eq!(
            termination_message(dir.path(), &cid, Some(&plain), Some(1)),
            None
        );

        // The file the container wrote wins, and is consumed.
        let file = r8s_runtime::containerd::termination_message_file(dir.path(), &cid.0);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "DONE").unwrap();
        assert_eq!(
            termination_message(dir.path(), &cid, Some(&fallback), Some(0)).as_deref(),
            Some("DONE")
        );
        assert!(!file.exists());

        // Fallback on failure: the log tail without the CRI prefixes.
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        std::fs::write(
            dir.path().join("logs").join("p_c.log"),
            "2026-10-27T03:00:00.0Z stdout F first\n2026-10-27T03:00:01.0Z stderr F DONE\n",
        )
        .unwrap();
        assert_eq!(
            termination_message(dir.path(), &cid, Some(&fallback), Some(1)).as_deref(),
            Some("first\nDONE")
        );
        assert_eq!(
            termination_message(dir.path(), &cid, Some(&fallback), Some(0)),
            None
        );
        assert_eq!(
            termination_message(dir.path(), &cid, Some(&plain), Some(1)),
            None
        );
    }

    #[test]
    fn service_env_describes_cluster_ip_services() {
        use super::service_env;
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let services = GroupVersionResource::services();
        for (name, spec) in [
            (
                "foo-service",
                serde_json::json!({"clusterIP": "10.96.1.2",
                "ports": [{"name": "http", "port": 8765, "protocol": "TCP", "targetPort": 8080}]}),
            ),
            (
                "ext",
                serde_json::json!({"type": "ExternalName", "externalName": "x.example.com"}),
            ),
            (
                "headless",
                serde_json::json!({"clusterIP": "None", "ports": [{"port": 80}]}),
            ),
        ] {
            store
                .create(
                    ResourceRef {
                        gvr: &services,
                        namespace: Some("ns"),
                        name,
                    },
                    &serde_json::json!({"apiVersion": "v1", "kind": "Service",
                        "metadata": {"name": name, "namespace": "ns"}, "spec": spec}),
                )
                .unwrap();
        }
        let env = service_env(&store, Some("ns"));
        let get = |k: &str| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("FOO_SERVICE_SERVICE_HOST"), Some("10.96.1.2"));
        assert_eq!(get("FOO_SERVICE_SERVICE_PORT"), Some("8765"));
        assert_eq!(get("FOO_SERVICE_SERVICE_PORT_HTTP"), Some("8765"));
        assert_eq!(get("FOO_SERVICE_PORT"), Some("tcp://10.96.1.2:8765"));
        assert_eq!(
            get("FOO_SERVICE_PORT_8765_TCP"),
            Some("tcp://10.96.1.2:8765")
        );
        assert_eq!(get("FOO_SERVICE_PORT_8765_TCP_PROTO"), Some("tcp"));
        assert_eq!(get("FOO_SERVICE_PORT_8765_TCP_PORT"), Some("8765"));
        assert_eq!(get("FOO_SERVICE_PORT_8765_TCP_ADDR"), Some("10.96.1.2"));
        assert!(
            env.iter()
                .all(|(n, _)| !n.starts_with("EXT_") && !n.starts_with("HEADLESS_"))
        );
    }

    #[test]
    fn prepare_volumes_projects_a_bound_token_and_downward_api() {
        use super::{VolumePod, prepare_volumes};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::{GroupVersionResource, Volume};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let volumes: Vec<Volume> = serde_json::from_value(serde_json::json!([{
            "name": "kube-api-access",
            "projected": {"sources": [
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                {"downwardAPI": {"items": [
                    {"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}
                ]}}
            ]}
        }]))
        .unwrap();
        let pod = VolumePod {
            name: "p",
            uid: "u",
            namespace: Some("ns"),
            service_account: "builder",
            ip: "10.244.0.4",
            metadata: &serde_json::Value::Null,
            spec: &Default::default(),
        };

        let err = prepare_volumes(&store, dir.path(), &pod, &volumes).unwrap_err();
        assert!(
            err.to_string().contains("serviceaccount 'builder'"),
            "{err}"
        );

        let sas = GroupVersionResource::service_accounts();
        store
            .create(
                ResourceRef {
                    gvr: &sas,
                    namespace: Some("ns"),
                    name: "builder",
                },
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": "builder", "namespace": "ns", "uid": "sa-uid"}
                }),
            )
            .unwrap();
        let paths = prepare_volumes(&store, dir.path(), &pod, &volumes).unwrap();
        let vol = std::path::Path::new(&paths["kube-api-access"]);
        let token = std::fs::read_to_string(vol.join("token")).unwrap();
        let key = r8s_types::sa_token::SigningKey::load_or_create(dir.path()).unwrap();
        let claims = key.verify(&token, chrono::Utc::now().timestamp()).unwrap();
        assert_eq!(claims["sub"], "system:serviceaccount:ns:builder");
        assert_eq!(claims["kubernetes.io"]["pod"]["uid"], "u");
        assert_eq!(claims["kubernetes.io"]["serviceaccount"]["uid"], "sa-uid");
        assert_eq!(
            std::fs::read_to_string(vol.join("namespace")).unwrap(),
            "ns"
        );
    }

    #[tokio::test]
    async fn crashed_container_restarts_alone() {
        use super::{PodState, check_exits, reconcile_pod};
        use r8s_runtime::{ContainerRuntime, MockRuntime};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let runtime = MockRuntime::new();
        let state = PodState::new();
        let pods = GroupVersionResource::pods();
        let pod_ref = || ResourceRef {
            gvr: &pods,
            namespace: Some("default"),
            name: "web",
        };
        store
            .create(
                pod_ref(),
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "web", "namespace": "default", "uid": "u1" },
                    "spec": {
                        "nodeName": "r8s-node",
                        "containers": [
                            { "name": "app", "image": "nginx" },
                            { "name": "side", "image": "busybox" }
                        ]
                    }
                }),
            )
            .unwrap();
        let read = || store.get(&pod_ref()).unwrap().unwrap();
        reconcile_pod(&store, &runtime, &state, &read(), dir.path(), "test").await;
        let before = tracked_ids(&state, "u1").await;
        assert!(runtime.container_status(&before[0]).await.unwrap().running);

        runtime.stop_matching_with_code("web_side", 1);
        check_exits(&store, &runtime, &state, dir.path()).await;

        let after = tracked_ids(&state, "u1").await;
        assert_eq!(after[0], before[0], "the healthy container is left alone");
        assert_ne!(after[1], before[1], "the crashed container is recreated");
        assert!(runtime.container_status(&after[0]).await.unwrap().running);
        assert!(runtime.container_status(&after[1]).await.unwrap().running);
        let pod = read();
        assert_eq!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Running")
        );
        let statuses = pod.pointer("/status/containerStatuses").unwrap();
        assert_eq!(statuses[1]["restartCount"], 1);
        assert!(statuses[1]["state"]["running"].is_object());
        assert!(statuses[0]["state"]["running"].is_object());
    }

    #[tokio::test]
    async fn pod_ends_when_every_container_has_exited() {
        use super::{PodState, check_exits, reconcile_pod};
        use r8s_runtime::{ContainerRuntime, MockRuntime};
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let runtime = MockRuntime::new();
        let state = PodState::new();
        let pods = GroupVersionResource::pods();
        let pod_ref = || ResourceRef {
            gvr: &pods,
            namespace: Some("default"),
            name: "job",
        };
        store
            .create(
                pod_ref(),
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "job", "namespace": "default", "uid": "u2" },
                    "spec": {
                        "nodeName": "r8s-node",
                        "restartPolicy": "Never",
                        "containers": [
                            { "name": "app", "image": "nginx" },
                            { "name": "side", "image": "busybox" }
                        ]
                    }
                }),
            )
            .unwrap();
        let read = || store.get(&pod_ref()).unwrap().unwrap();
        reconcile_pod(&store, &runtime, &state, &read(), dir.path(), "test").await;
        let ids = tracked_ids(&state, "u2").await;
        let (app, side) = (ids[0].clone(), ids[1].clone());

        runtime.stop_matching_with_code("job_side", 0);
        check_exits(&store, &runtime, &state, dir.path()).await;
        let pod = read();
        assert_eq!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Running")
        );
        let statuses = pod.pointer("/status/containerStatuses").unwrap();
        assert_eq!(statuses[1]["state"]["terminated"]["exitCode"], 0);
        assert!(statuses[0]["state"]["running"].is_object());
        assert!(!runtime.container_status(&side).await.unwrap().running);
        assert!(runtime.container_status(&app).await.unwrap().running);

        // A second sweep leaves the finished container alone.
        check_exits(&store, &runtime, &state, dir.path()).await;
        assert!(!runtime.container_status(&side).await.unwrap().running);

        runtime.stop_matching_with_code("job_app", 3);
        check_exits(&store, &runtime, &state, dir.path()).await;
        let pod = read();
        assert_eq!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Failed")
        );
        assert!(state.get("u2").await.is_none());
    }

    #[tokio::test]
    async fn reconcile_pod_reuses_existing_status_pod_ip() {
        // After a daemon restart the in-memory pool is empty but the pod still
        // carries its old status.podIP. reconcile_pod must reuse that address,
        // not churn the pod onto a fresh one.
        use super::{PodState, reconcile_pod, recover_ip_allocations};
        use r8s_runtime::MockRuntime;
        use r8s_store::{Store, backend::ResourceRef};
        use r8s_types::GroupVersionResource;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let runtime = MockRuntime::new();
        let state = PodState::new();
        let pods = GroupVersionResource::pods();

        store
            .create(
                ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "web",
                },
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": "web", "namespace": "default" },
                    "spec": {
                        "nodeName": "r8s-node",
                        "containers": [{ "name": "app", "image": "nginx" }]
                    },
                    "status": { "phase": "Running", "podIP": "10.244.0.7" }
                }),
            )
            .unwrap();

        recover_ip_allocations(&store, &state).await;
        let read = || {
            store
                .get(&ResourceRef {
                    gvr: &pods,
                    namespace: Some("default"),
                    name: "web",
                })
                .unwrap()
                .unwrap()
        };
        let pod_value = read();
        reconcile_pod(&store, &runtime, &state, &pod_value, dir.path(), "test").await;

        assert_eq!(
            read().pointer("/status/podIP").and_then(|v| v.as_str()),
            Some("10.244.0.7"),
            "restart must preserve the pod's IP"
        );
    }
}
