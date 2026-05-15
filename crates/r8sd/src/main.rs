use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use r8s_api::{
    ApiServer,
    bootstrap::{bootstrap_ingress_class, bootstrap_namespaces},
};
use r8s_controllers::ControllerManager;
use r8s_runtime::{MockRuntime, containerd::ContainerdRuntime};
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio::task::JoinHandle;
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
    tracing::info!(data_dir = %data_dir.display(), "r8sd starting");

    std::fs::create_dir_all(&data_dir)?;
    let store = Store::open(&data_dir.join("store.db"))?;

    bootstrap_namespaces(&store)?;
    bootstrap_ingress_class(&store)?;

    let shutdown = CancellationToken::new();
    let registry = ResourceRegistry::default_mvp();

    // Controllers must subscribe to watches before the API server
    // accepts mutations, so start them first.
    let mut controller_manager =
        ControllerManager::new(store.clone(), shutdown.clone(), registry.clone());
    controller_manager.start();

    r8s_network::bridge::setup_bridge(&data_dir)?;
    r8s_network::proxy::setup_nat_table()?;

    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    spawn(
        &mut tasks,
        "scheduler",
        r8s_scheduler::run(store.clone(), shutdown.clone()),
    );
    spawn(
        &mut tasks,
        "dns",
        r8s_network::dns::run_dns_server(store.clone(), shutdown.clone(), data_dir.clone()),
    );
    spawn(
        &mut tasks,
        "ingress-proxy",
        r8s_network::ingress::run_ingress_proxy(store.clone(), shutdown.clone()),
    );
    spawn(
        &mut tasks,
        "service-proxy",
        run_service_proxy(store.clone(), shutdown.clone()),
    );

    let runtime_type = std::env::var("R8S_RUNTIME").unwrap_or_else(|_| "containerd".to_string());
    match runtime_type.as_str() {
        "mock" => {
            tracing::info!("using mock container runtime");
            let runtime = Arc::new(MockRuntime::new());
            spawn(
                &mut tasks,
                "kubelet",
                r8s_kubelet::run(store.clone(), runtime, shutdown.clone(), data_dir.clone()),
            );
        }
        _ => {
            let socket = std::env::var("CONTAINERD_SOCKET")
                .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string());
            if !std::path::Path::new(&socket).exists() {
                anyhow::bail!(
                    "containerd socket not found at '{socket}'. Is containerd running? \
                     (try: sudo systemctl start containerd)"
                );
            }
            tracing::info!(socket, "using containerd runtime");
            let runtime = Arc::new(ContainerdRuntime::new(&socket, data_dir.clone()).await?);
            spawn(
                &mut tasks,
                "kubelet",
                r8s_kubelet::run(store.clone(), runtime, shutdown.clone(), data_dir.clone()),
            );
        }
    }

    let api_server = ApiServer::new(store, registry, data_dir);
    let addr: SocketAddr = "0.0.0.0:6443".parse()?;
    spawn(
        &mut tasks,
        "api-server",
        api_server.serve(addr, shutdown.clone()),
    );

    tracing::info!("r8sd ready");

    wait_for_shutdown().await?;
    tracing::info!("r8sd shutting down");
    shutdown.cancel();
    r8s_network::bridge::cleanup();
    r8s_network::proxy::cleanup();
    controller_manager.shutdown().await;
    for handle in tasks {
        let _ = handle.await;
    }

    Ok(())
}

/// Wait for either SIGINT or SIGTERM. `r8s delete` sends SIGTERM via libc::kill,
/// so SIGINT-only handling (the default for `tokio::signal::ctrl_c`) would let
/// the cleanup block run only on Ctrl-C and leak state on every CLI-driven stop.
async fn wait_for_shutdown() -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = sigterm.recv() => Ok(()),
        _ = sigint.recv() => Ok(()),
    }
}

fn spawn(
    tasks: &mut Vec<JoinHandle<()>>,
    name: &'static str,
    fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    tasks.push(tokio::spawn(async move {
        if let Err(e) = fut.await {
            tracing::error!("{name} error: {e}");
        }
    }));
}

async fn run_service_proxy(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if let Err(e) = r8s_network::proxy::sync_service_rules(&store) {
                    tracing::warn!("service proxy sync: {e}");
                }
            }
        }
    }
}
