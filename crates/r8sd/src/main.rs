use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use r8s_api::{
    ApiServer,
    bootstrap::{bootstrap_ingress_class, bootstrap_namespaces},
};
use r8s_controllers::ControllerManager;
use r8s_runtime::{ContainerRuntime, MockRuntime, containerd::ContainerdRuntime};
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "r8sd")]
struct Args {
    /// Data directory for this cluster
    #[arg(long, default_value = "/tmp/r8s")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("r8s=info".parse()?))
        .init();

    let args = Args::parse();
    let data_dir = args.data_dir;
    tracing::info!(data_dir = %data_dir.display(), "r8sd starting...");

    std::fs::create_dir_all(&data_dir)?;
    let store = Store::open(&data_dir.join("store.db"))?;

    bootstrap_namespaces(&store)?;
    bootstrap_ingress_class(&store)?;

    let shutdown = CancellationToken::new();
    let registry = ResourceRegistry::default_mvp();

    // Start controllers before API server so watches are subscribed
    // before any API-driven mutations
    let mut controller_manager =
        ControllerManager::new(store.clone(), shutdown.clone(), registry.clone());
    controller_manager.start();

    let scheduler_store = store.clone();
    let scheduler_shutdown = shutdown.clone();
    let scheduler_handle = tokio::spawn(async move {
        if let Err(e) = r8s_scheduler::run(scheduler_store, scheduler_shutdown).await {
            tracing::error!("scheduler error: {e}")
        }
    });

    r8s_network::bridge::setup_bridge(&data_dir)?;
    r8s_network::proxy::setup_nat_table()?;

    let dns_store = store.clone();
    let dns_shutdown = shutdown.clone();
    let dns_data_dir = data_dir.clone();
    let dns_handle = tokio::spawn(async move {
        if let Err(e) =
            r8s_network::dns::run_dns_server(dns_store, dns_shutdown, dns_data_dir).await
        {
            tracing::error!("DNS server error: {e}");
        }
    });

    let ingress_store = store.clone();
    let ingress_shutdown = shutdown.clone();
    let ingress_handle = tokio::spawn(async move {
        if let Err(e) =
            r8s_network::ingress::run_ingress_proxy(ingress_store, ingress_shutdown).await
        {
            tracing::error!("ingress proxy error: {e}");
        }
    });

    let proxy_store = store.clone();
    let proxy_shutdown = shutdown.clone();
    let proxy_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = proxy_shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    if let Err(e) = r8s_network::proxy::sync_service_rules(&proxy_store) {
                        tracing::warn!("service proxy sync: {e}");
                    }
                }
            }
        }
    });

    let runtime_type = std::env::var("R8S_RUNTIME").unwrap_or_else(|_| "containerd".to_string());
    let kubelet_handle = match runtime_type.as_str() {
        "mock" => {
            tracing::info!("using mock container runtime");
            let runtime = Arc::new(MockRuntime::new());
            spawn_kubelet(store.clone(), runtime, shutdown.clone(), data_dir.clone())
        }
        _ => {
            let socket = std::env::var("CONTAINERD_SOCKET")
                .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string());
            tracing::info!(socket, "using containerd runtime");
            let runtime = Arc::new(ContainerdRuntime::new(&socket, data_dir.clone()).await?);
            spawn_kubelet(store.clone(), runtime, shutdown.clone(), data_dir.clone())
        }
    };

    let server = ApiServer::new(store, registry, data_dir);
    let addr: SocketAddr = "0.0.0.0:6443".parse()?;
    let server_shutdown = shutdown.clone();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.serve(addr, server_shutdown).await {
            tracing::error!("API server error: {e}");
        }
    });

    tracing::info!("r8sd ready");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("r8sd shutting down...");
    shutdown.cancel();
    r8s_network::bridge::cleanup();
    r8s_network::proxy::cleanup();
    controller_manager.shutdown().await;
    let _ = scheduler_handle.await;
    let _ = kubelet_handle.await;
    let _ = dns_handle.await;
    let _ = ingress_handle.await;
    let _ = proxy_handle.await;
    let _ = server_handle.await;

    Ok(())
}

fn spawn_kubelet<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    shutdown: CancellationToken,
    data_dir: PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = r8s_kubelet::run(store, runtime, shutdown, data_dir).await {
            tracing::error!("kubelet error: {e}");
        }
    })
}
