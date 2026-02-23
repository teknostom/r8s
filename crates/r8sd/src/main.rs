use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use r8s_api::{
    ApiServer,
    bootstrap::{bootstrap_ingress_class, bootstrap_namespaces},
};
use r8s_controllers::ControllerManager;
use r8s_runtime::{ContainerRuntime, MockRuntime, containerd::ContainerdRuntime};
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

// ---------------------------------------------------------------------------
// Subsystem trait — zero-cost (monomorphized), no dyn dispatch
// ---------------------------------------------------------------------------

trait Subsystem: Send + 'static {
    fn name(&self) -> &'static str;
    fn run(
        self,
        store: Store,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

struct SubsystemManager {
    store: Store,
    shutdown: CancellationToken,
    handles: Vec<(&'static str, JoinHandle<()>)>,
}

impl SubsystemManager {
    fn new(store: Store, shutdown: CancellationToken) -> Self {
        Self {
            store,
            shutdown,
            handles: Vec::new(),
        }
    }

    fn spawn(&mut self, subsystem: impl Subsystem) {
        let store = self.store.clone();
        let shutdown = self.shutdown.clone();
        let name = subsystem.name();
        self.handles.push((
            name,
            tokio::spawn(async move {
                if let Err(e) = subsystem.run(store, shutdown).await {
                    tracing::error!("{name} error: {e}");
                }
            }),
        ));
    }

    async fn shutdown(self) {
        for (_, handle) in self.handles {
            let _ = handle.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Subsystem implementations
// ---------------------------------------------------------------------------

struct Scheduler;

impl Subsystem for Scheduler {
    fn name(&self) -> &'static str {
        "scheduler"
    }
    async fn run(self, store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
        r8s_scheduler::run(store, shutdown).await
    }
}

struct DnsServer {
    data_dir: PathBuf,
}

impl Subsystem for DnsServer {
    fn name(&self) -> &'static str {
        "dns"
    }
    async fn run(self, store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
        r8s_network::dns::run_dns_server(store, shutdown, self.data_dir).await
    }
}

struct IngressProxy;

impl Subsystem for IngressProxy {
    fn name(&self) -> &'static str {
        "ingress-proxy"
    }
    async fn run(self, store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
        r8s_network::ingress::run_ingress_proxy(store, shutdown).await
    }
}

struct ServiceProxy;

impl Subsystem for ServiceProxy {
    fn name(&self) -> &'static str {
        "service-proxy"
    }
    async fn run(self, store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
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
}

struct Kubelet<R: ContainerRuntime> {
    runtime: Arc<R>,
    data_dir: PathBuf,
}

impl<R: ContainerRuntime + 'static> Subsystem for Kubelet<R> {
    fn name(&self) -> &'static str {
        "kubelet"
    }
    async fn run(self, store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
        r8s_kubelet::run(store, self.runtime, shutdown, self.data_dir).await
    }
}

struct HttpApiServer {
    server: ApiServer,
    addr: SocketAddr,
}

impl Subsystem for HttpApiServer {
    fn name(&self) -> &'static str {
        "api-server"
    }
    async fn run(self, _store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
        self.server.serve(self.addr, shutdown).await
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

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

    r8s_network::bridge::setup_bridge(&data_dir)?;
    r8s_network::proxy::setup_nat_table()?;

    let mut mgr = SubsystemManager::new(store.clone(), shutdown.clone());

    mgr.spawn(Scheduler);
    mgr.spawn(DnsServer {
        data_dir: data_dir.clone(),
    });
    mgr.spawn(IngressProxy);
    mgr.spawn(ServiceProxy);

    let runtime_type = std::env::var("R8S_RUNTIME").unwrap_or_else(|_| "containerd".to_string());
    match runtime_type.as_str() {
        "mock" => {
            tracing::info!("using mock container runtime");
            mgr.spawn(Kubelet {
                runtime: Arc::new(MockRuntime::new()),
                data_dir: data_dir.clone(),
            });
        }
        _ => {
            let socket = std::env::var("CONTAINERD_SOCKET")
                .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string());
            tracing::info!(socket, "using containerd runtime");
            let runtime = ContainerdRuntime::new(&socket, data_dir.clone()).await?;
            mgr.spawn(Kubelet {
                runtime: Arc::new(runtime),
                data_dir: data_dir.clone(),
            });
        }
    }

    mgr.spawn(HttpApiServer {
        server: ApiServer::new(store, registry, data_dir),
        addr: "0.0.0.0:6443".parse()?,
    });

    tracing::info!("r8sd ready");

    tokio::signal::ctrl_c().await?;
    tracing::info!("r8sd shutting down...");
    shutdown.cancel();
    r8s_network::bridge::cleanup();
    r8s_network::proxy::cleanup();
    controller_manager.shutdown().await;
    mgr.shutdown().await;

    Ok(())
}
