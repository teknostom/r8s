use r8s_store::Store;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Manages the lifecycle of all built-in controllers.
pub struct ControllerManager {
    store: Store,
    shutdown: CancellationToken,
    handles: Vec<JoinHandle<()>>,
}

impl ControllerManager {
    pub fn new(store: Store, shutdown: CancellationToken) -> Self {
        Self {
            store,
            shutdown,
            handles: Vec::new(),
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
