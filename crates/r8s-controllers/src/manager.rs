use std::sync::{Arc, RwLock};

use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Manages the lifecycle of all built-in controllers.
pub struct ControllerManager {
    store: Store,
    shutdown: CancellationToken,
    handles: Vec<JoinHandle<()>>,
    registry: Arc<RwLock<ResourceRegistry>>,
}

impl ControllerManager {
    pub fn new(
        store: Store,
        shutdown: CancellationToken,
        registry: Arc<RwLock<ResourceRegistry>>,
    ) -> Self {
        Self {
            store,
            shutdown,
            handles: Vec::new(),
            registry,
        }
    }

    pub fn start(&mut self) {
        macro_rules! spawn_controller {
            ($name:expr, $func:path) => {{
                let store = self.store.clone();
                let token = self.shutdown.clone();
                self.handles.push(tokio::spawn(async move {
                    if let Err(e) = $func(store, token).await {
                        tracing::error!("{} controller error: {e}", $name);
                    }
                }));
            }};
        }

        spawn_controller!("namespace", super::namespace::run);
        spawn_controller!("replicaset", super::replicaset::run);
        spawn_controller!("deployment", super::deployment::run);
        spawn_controller!("gc", super::gc::run);
        spawn_controller!("endpoints", super::endpoints::run);

        // CRD controller: watches for new CRDs and registers them in the
        // shared registry so the dynamic API routes can serve them.
        let store = self.store.clone();
        let token = self.shutdown.clone();
        let registry = self.registry.clone();
        self.handles.push(tokio::spawn(async move {
            if let Err(e) = super::crd::run(store, registry, token).await {
                tracing::error!("crd controller error: {e}");
            }
        }));

        tracing::info!(
            "controller manager started {} controllers",
            self.handles.len()
        );
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}
