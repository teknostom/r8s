use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct ControllerManager {
    store: Store,
    shutdown: CancellationToken,
    registry: ResourceRegistry,
    ca_pem: String,
    handles: Vec<JoinHandle<()>>,
}

impl ControllerManager {
    pub fn new(
        store: Store,
        shutdown: CancellationToken,
        registry: ResourceRegistry,
        ca_pem: String,
    ) -> Self {
        Self {
            store,
            shutdown,
            registry,
            ca_pem,
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
        {
            let store = self.store.clone();
            let token = self.shutdown.clone();
            let ca_pem = self.ca_pem.clone();
            self.handles.push(tokio::spawn(async move {
                if let Err(e) = super::serviceaccount::run(store, token, ca_pem).await {
                    tracing::error!("serviceaccount controller error: {e}");
                }
            }));
        }
        spawn_controller!("replicaset", super::replicaset::run);
        spawn_controller!("deployment", super::deployment::run);
        spawn_controller!("gc", super::gc::run);
        spawn_controller!("endpoints", super::endpoints::run);
        spawn_controller!("statefulset", super::statefulset::run);
        spawn_controller!("daemonset", super::daemonset::run);
        spawn_controller!("job", super::job::run);
        spawn_controller!("cronjob", super::cronjob::run);

        // CRD controller needs the registry
        {
            let store = self.store.clone();
            let token = self.shutdown.clone();
            let registry = self.registry.clone();
            self.handles.push(tokio::spawn(async move {
                if let Err(e) = super::crd::run(store, token, registry).await {
                    tracing::error!("crd controller error: {e}");
                }
            }));
        }

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
