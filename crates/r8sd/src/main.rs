use std::{net::SocketAddr, path::PathBuf};

use r8s_api::{ApiServer, bootstrap::bootstrap_namespaces};
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;
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
    server_handle.abort();

    Ok(())
}
