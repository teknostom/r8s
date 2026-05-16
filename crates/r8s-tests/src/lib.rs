use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use r8s_controllers::ControllerManager;
use r8s_runtime::MockRuntime;
use r8s_store::backend::{ResourceRef, Store};
use r8s_types::{
    Container, CronJob, CronJobSpec, DaemonSet, DaemonSetSpec, Deployment, DeploymentSpec,
    GroupVersionResource, Job, JobSpec, JobTemplateSpec, LabelSelector, ObjectMeta, Pod, PodSpec,
    PodTemplateSpec, Service, ServicePort, ServiceSpec, StatefulSet, StatefulSetSpec,
    registry::ResourceRegistry,
};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// TestCluster
// ---------------------------------------------------------------------------

/// In-process test cluster with store, controllers, scheduler, and kubelet.
pub struct TestCluster {
    pub store: Store,
    pub runtime: Arc<MockRuntime>,
    pub registry: ResourceRegistry,
    shutdown: CancellationToken,
    _controller_manager: ControllerManager,
    _handles: Vec<JoinHandle<()>>,
    _temp_dir: TempDir,
}

impl TestCluster {
    /// Start a full in-process cluster with mock runtime.
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("failed to create temp dir");
        let store = Store::open(&dir.path().join("test.db")).expect("failed to open store");

        r8s_api::bootstrap::bootstrap_namespaces(&store).expect("bootstrap failed");

        let shutdown = CancellationToken::new();
        let registry = ResourceRegistry::default_mvp();

        let mut controller_manager = ControllerManager::new(
            store.clone(),
            shutdown.clone(),
            registry.clone(),
            String::new(),
        );
        controller_manager.start();

        let scheduler_handle = {
            let store = store.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                if let Err(e) = r8s_scheduler::run(store, shutdown).await {
                    tracing::error!("scheduler error: {e}");
                }
            })
        };

        let runtime = Arc::new(MockRuntime::new());
        let kubelet_handle = {
            let store = store.clone();
            let shutdown = shutdown.clone();
            let runtime = runtime.clone();
            let data_dir = dir.path().to_path_buf();
            tokio::spawn(async move {
                if let Err(e) = r8s_kubelet::run_with_config(
                    store,
                    runtime,
                    shutdown,
                    data_dir,
                    Duration::from_millis(500),
                )
                .await
                {
                    tracing::error!("kubelet error: {e}");
                }
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            store,
            runtime,
            registry,
            shutdown,
            _controller_manager: controller_manager,
            _handles: vec![scheduler_handle, kubelet_handle],
            _temp_dir: dir,
        }
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        for handle in self._handles {
            let _ = handle.await;
        }
        self._controller_manager.shutdown().await;
    }

    // -- Store convenience methods --

    /// Create a namespaced resource from a serializable value.
    pub fn create<T: serde::Serialize>(
        &self,
        gvr: &GroupVersionResource,
        ns: &str,
        name: &str,
        value: &T,
    ) {
        let rref = ResourceRef {
            gvr,
            namespace: Some(ns),
            name,
        };
        self.store
            .create(rref, &serde_json::to_value(value).unwrap())
            .unwrap();
    }

    /// Get a resource as raw JSON, panics if not found.
    pub fn get(&self, gvr: &GroupVersionResource, ns: &str, name: &str) -> serde_json::Value {
        let rref = ResourceRef {
            gvr,
            namespace: Some(ns),
            name,
        };
        self.store
            .get(&rref)
            .unwrap()
            .unwrap_or_else(|| panic!("{name} not found in {ns}"))
    }

    /// Get a resource, returning None if it doesn't exist.
    pub fn try_get(
        &self,
        gvr: &GroupVersionResource,
        ns: &str,
        name: &str,
    ) -> Option<serde_json::Value> {
        let rref = ResourceRef {
            gvr,
            namespace: Some(ns),
            name,
        };
        self.store.get(&rref).unwrap()
    }

    /// Update a namespaced resource.
    pub fn update(
        &self,
        gvr: &GroupVersionResource,
        ns: &str,
        name: &str,
        val: &serde_json::Value,
    ) {
        let rref = ResourceRef {
            gvr,
            namespace: Some(ns),
            name,
        };
        self.store.update(&rref, val).unwrap();
    }

    /// Delete a namespaced resource.
    pub fn delete(&self, gvr: &GroupVersionResource, ns: &str, name: &str) {
        let rref = ResourceRef {
            gvr,
            namespace: Some(ns),
            name,
        };
        self.store.delete(&rref).unwrap();
    }

    /// Build an API router for HTTP-level testing (no port binding needed).
    pub fn api_router(&self) -> axum::Router {
        r8s_api::server::ApiServer::new(
            self.store.clone(),
            self.registry.clone(),
            self._temp_dir.path().to_path_buf(),
        )
        .into_router()
    }

    /// Cluster's data directory (where logs/, certs/, etc. live).
    pub fn data_dir(&self) -> &std::path::Path {
        self._temp_dir.path()
    }

    /// List all resources of a GVR in a namespace.
    pub fn list(&self, gvr: &GroupVersionResource, ns: &str) -> Vec<serde_json::Value> {
        self.store
            .list(gvr, Some(ns), None, None, None, None)
            .unwrap()
            .items
    }

    /// Get the UID of a namespaced resource.
    pub fn uid(&self, gvr: &GroupVersionResource, ns: &str, name: &str) -> String {
        self.get(gvr, ns, name)["metadata"]["uid"]
            .as_str()
            .expect("resource missing uid")
            .to_string()
    }
}

// ---------------------------------------------------------------------------
// Wait utilities
// ---------------------------------------------------------------------------

/// Error returned when a wait times out, containing diagnostic info.
#[derive(Debug)]
pub struct TimeoutError {
    pub message: String,
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Core polling loop. Calls `check` every 50ms until it returns `Some(T)` or
/// the deadline is reached. Returns `Err(TimeoutError)` with the last
/// diagnostic from `describe_state`.
async fn poll_until<T, F, D>(
    timeout: Duration,
    mut check: F,
    describe_state: D,
) -> Result<T, TimeoutError>
where
    F: FnMut() -> Option<T>,
    D: Fn() -> String,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(val) = check() {
            return Ok(val);
        }
        if tokio::time::Instant::now() > deadline {
            return Err(TimeoutError {
                message: describe_state(),
            });
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Wait until a single named resource matches a condition. Returns the value.
///
/// Panics on timeout with a diagnostic message.
pub async fn wait_for<F>(
    store: &Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    name: &str,
    condition: F,
    timeout: Duration,
) -> bool
where
    F: Fn(&serde_json::Value) -> bool,
{
    let result = poll_until(
        timeout,
        || {
            let rref = ResourceRef {
                gvr,
                namespace: ns,
                name,
            };
            match store.get(&rref) {
                Ok(Some(val)) if condition(&val) => Some(val),
                _ => None,
            }
        },
        || {
            let rref = ResourceRef {
                gvr,
                namespace: ns,
                name,
            };
            match store.get(&rref) {
                Ok(Some(val)) => format!(
                    "timed out waiting for {}/{} to match condition; current value: {}",
                    ns.unwrap_or("(cluster)"),
                    name,
                    serde_json::to_string_pretty(&val).unwrap_or_default()
                ),
                Ok(None) => format!(
                    "timed out: {}/{} does not exist",
                    ns.unwrap_or("(cluster)"),
                    name
                ),
                Err(e) => format!("timed out with store error: {e}"),
            }
        },
    )
    .await;
    result.is_ok()
}

/// Wait until at least `count` resources match a filter. Returns true on success.
pub async fn wait_for_count<F>(
    store: &Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    filter: F,
    count: usize,
    timeout: Duration,
) -> bool
where
    F: Fn(&serde_json::Value) -> bool,
{
    let result = poll_until(
        timeout,
        || {
            let items = store.list(gvr, ns, None, None, None, None).ok()?;
            let matched: Vec<_> = items.items.into_iter().filter(|v| filter(v)).collect();
            if matched.len() >= count { Some(matched) } else { None }
        },
        || {
            let current = store
                .list(gvr, ns, None, None, None, None)
                .map(|r| r.items.len())
                .unwrap_or(0);
            format!(
                "timed out waiting for >= {count} matching resources in {}; total resources: {current}",
                ns.unwrap_or("(cluster)")
            )
        },
    )
    .await;
    result.is_ok()
}

/// Wait until exactly `count` resources match (stable across 2 consecutive polls).
pub async fn wait_for_exact_count<F>(
    store: &Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    filter: F,
    count: usize,
    timeout: Duration,
) -> bool
where
    F: Fn(&serde_json::Value) -> bool,
{
    let mut consecutive = 0u32;
    let result = poll_until(
        timeout,
        || {
            let items = store.list(gvr, ns, None, None, None, None).ok()?;
            let matched = items.items.iter().filter(|v| filter(v)).count();
            if matched == count {
                consecutive += 1;
                if consecutive >= 2 {
                    return Some(());
                }
            } else {
                consecutive = 0;
            }
            None
        },
        || {
            let matched = store
                .list(gvr, ns, None, None, None, None)
                .map(|r| r.items.iter().filter(|v| filter(v)).count())
                .unwrap_or(0);
            format!(
                "timed out waiting for exactly {count} matching resources in {}; found {matched}",
                ns.unwrap_or("(cluster)")
            )
        },
    )
    .await;
    result.is_ok()
}

/// Wait until zero resources match the filter.
pub async fn wait_for_zero<F>(
    store: &Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    filter: F,
    timeout: Duration,
) -> bool
where
    F: Fn(&serde_json::Value) -> bool,
{
    let result = poll_until(
        timeout,
        || {
            let items = store.list(gvr, ns, None, None, None, None).ok()?;
            let matched = items.items.iter().filter(|v| filter(v)).count();
            if matched == 0 { Some(()) } else { None }
        },
        || {
            let matched = store
                .list(gvr, ns, None, None, None, None)
                .map(|r| r.items.iter().filter(|v| filter(v)).count())
                .unwrap_or(0);
            format!(
                "timed out waiting for 0 matching resources in {}; still have {matched}",
                ns.unwrap_or("(cluster)")
            )
        },
    )
    .await;
    result.is_ok()
}

/// Wait until a named resource no longer exists.
pub async fn wait_for_deletion(
    store: &Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    name: &str,
    timeout: Duration,
) -> bool {
    let result = poll_until(
        timeout,
        || {
            let rref = ResourceRef {
                gvr,
                namespace: ns,
                name,
            };
            match store.get(&rref) {
                Ok(None) => Some(()),
                _ => None,
            }
        },
        || {
            format!(
                "timed out waiting for {}/{} to be deleted",
                ns.unwrap_or("(cluster)"),
                name,
            )
        },
    )
    .await;
    result.is_ok()
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

/// Check if a resource value has an ownerReference with the given UID.
pub fn is_owned_by(val: &serde_json::Value, owner_uid: &str) -> bool {
    val["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
}

/// Check if a resource has a condition with the given type and status.
pub fn has_condition(val: &serde_json::Value, cond_type: &str, status: &str) -> bool {
    val["status"]["conditions"]
        .as_array()
        .map(|cs| {
            cs.iter().any(|c| {
                c["type"].as_str() == Some(cond_type) && c["status"].as_str() == Some(status)
            })
        })
        .unwrap_or(false)
}

/// Check the phase of a pod.
pub fn has_phase(val: &serde_json::Value, phase: &str) -> bool {
    val["status"]["phase"].as_str() == Some(phase)
}

/// Check if a resource has a specific label value.
pub fn has_label(val: &serde_json::Value, key: &str, expected: &str) -> bool {
    val["metadata"]["labels"][key].as_str() == Some(expected)
}

// ---------------------------------------------------------------------------
// Resource factories — all create in "default" namespace
// ---------------------------------------------------------------------------

fn labels(app: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("app".into(), app.into())])
}

fn container(name: &str, image: &str) -> Container {
    Container {
        name: name.into(),
        image: Some(image.into()),
        ..Default::default()
    }
}

pub fn make_deployment(name: &str, replicas: i32, app_label: &str) -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels(app_label)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(app_label)),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container("app", "nginx:latest")],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_statefulset(name: &str, replicas: i32, app_label: &str) -> StatefulSet {
    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels(app_label)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(app_label)),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container("app", "nginx:latest")],
                    ..Default::default()
                }),
            },
            service_name: name.into(),
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_daemonset(name: &str, app_label: &str) -> DaemonSet {
    DaemonSet {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(labels(app_label)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(app_label)),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container("agent", "busybox:latest")],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_job(name: &str) -> Job {
    Job {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), name.into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container("worker", "busybox:latest")],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_cronjob(name: &str, schedule: &str) -> CronJob {
    CronJob {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule: schedule.into(),
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: None,
                        spec: Some(PodSpec {
                            containers: vec![container("cron-worker", "busybox:latest")],
                            restart_policy: Some("Never".into()),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_pod(name: &str, image: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![container("app", image)],
            ..Default::default()
        }),
        status: None,
    }
}

pub fn make_service(name: &str, selector: BTreeMap<String, String>, port: i32) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                port,
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}
