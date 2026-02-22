use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use r8s_api::{ApiServer, bootstrap::bootstrap_namespaces};
use r8s_controllers::ControllerManager;
use r8s_runtime::{ContainerRuntime, MockRuntime, containerd::ContainerdRuntime};
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("r8s=info".parse()?))
        .init();

    tracing::info!("r8sd starting...");

    let data_dir =
        PathBuf::from(std::env::var("R8S_DATA_DIR").unwrap_or_else(|_| "/tmp/r8s".to_string()));

    std::fs::create_dir_all(&data_dir)?;
    let store = Store::open(&data_dir.join("store.db"))?;

    bootstrap_namespaces(&store)?;

    let shutdown = CancellationToken::new();

    // Start controllers before API server so watches are subscribed
    // before any API-driven mutations
    let mut controller_manager = ControllerManager::new(store.clone(), shutdown.clone());
    controller_manager.start();

    let scheduler_store = store.clone();
    let scheduler_shutdown = shutdown.clone();
    let scheduler_handle = tokio::spawn(async move {
        if let Err(e) = r8s_scheduler::run(scheduler_store, scheduler_shutdown).await {
            tracing::error!("scheduler error: {e}")
        }
    });

    let runtime_type = std::env::var("R8S_RUNTIME").unwrap_or_else(|_| "containerd".to_string());
    let kubelet_handle = match runtime_type.as_str() {
        "mock" => {
            tracing::info!("using mock container runtime");
            let runtime = Arc::new(MockRuntime::new());
            spawn_kubelet(store.clone(), runtime, shutdown.clone())
        }
        _ => {
            let socket = std::env::var("CONTAINERD_SOCKET")
                .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string());
            tracing::info!(socket, "using containerd runtime");
            let runtime = Arc::new(ContainerdRuntime::new(&socket).await?);
            spawn_kubelet(store.clone(), runtime, shutdown.clone())
        }
    };

    let registry = ResourceRegistry::default_mvp();
    let server = ApiServer::new(store, registry);
    let addr: SocketAddr = "127.0.0.1:6443".parse()?;

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.serve(addr).await {
            tracing::error!("API server error: {e}");
        }
    });

    tracing::info!("r8sd ready");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("r8sd shutting down...");
    shutdown.cancel();
    server_handle.abort();
    controller_manager.shutdown().await;
    let _ = scheduler_handle.await;
    let _ = kubelet_handle.await;

    Ok(())
}

fn spawn_kubelet<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = r8s_kubelet::run(store, runtime, shutdown).await {
            tracing::error!("kubelet error: {e}");
        }
    })
}
