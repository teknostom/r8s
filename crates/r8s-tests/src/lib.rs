use std::{sync::Arc, time::Duration};

use r8s_controllers::ControllerManager;
use r8s_runtime::MockRuntime;
use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{GroupVersionResource, registry::ResourceRegistry};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// In-process test cluster with store, controllers, scheduler, and kubelet (mock runtime).
pub struct TestCluster {
    pub store: Store,
    pub runtime: Arc<MockRuntime>,
    shutdown: CancellationToken,
    _controller_manager: ControllerManager,
    _handles: Vec<JoinHandle<()>>,
    _temp_dir: TempDir,
}

impl TestCluster {
    /// Start a full in-process cluster with mock runtime.
    /// Controllers, scheduler, and kubelet are all running.
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("failed to create temp dir");
        let store = Store::open(&dir.path().join("test.db")).expect("failed to open store");

        // Bootstrap default namespaces + kubernetes service
        r8s_api::bootstrap::bootstrap_namespaces(&store).expect("bootstrap failed");

        let shutdown = CancellationToken::new();
        let registry = ResourceRegistry::default_mvp();

        // Start controllers BEFORE other subsystems (so watches are subscribed)
        let mut controller_manager =
            ControllerManager::new(store.clone(), shutdown.clone(), registry);
        controller_manager.start();

        // Start scheduler
        let scheduler_handle = {
            let store = store.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                if let Err(e) = r8s_scheduler::run(store, shutdown).await {
                    tracing::error!("scheduler error: {e}");
                }
            })
        };

        // Start kubelet with mock runtime and fast health checks (500ms)
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

        // Give subsystems a moment to initialize watches
        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            store,
            runtime,
            shutdown,
            _controller_manager: controller_manager,
            _handles: vec![scheduler_handle, kubelet_handle],
            _temp_dir: dir,
        }
    }

    /// Shut down the cluster cleanly.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        for handle in self._handles {
            let _ = handle.await;
        }
        self._controller_manager.shutdown().await;
    }
}

/// Poll the store until a condition is met or timeout expires.
/// Returns true if the condition was met, false on timeout.
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let resource_ref = ResourceRef { gvr, namespace: ns, name };
        if let Ok(Some(val)) = store.get(&resource_ref) {
            if condition(&val) {
                return true;
            }
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until at least `count` resources of a given GVR exist in a namespace
/// that match a filter.
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
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(result) = store.list(gvr, ns, None, None, None, None) {
            let matched = result.items.iter().filter(|v| filter(v)).count();
            if matched >= count {
                return true;
            }
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
