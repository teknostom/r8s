use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("r8s=info".parse()?))
        .init();

    tracing::info!("r8sd starting...");

    // TODO: Phase 1 - Initialize store (redb)
    // TODO: Phase 2 - Start API server (axum)
    // TODO: Phase 3 - Bootstrap default resources (namespaces, node)
    // TODO: Phase 4 - Start kubelet (container runtime)
    // TODO: Phase 5 - Start controllers
    // TODO: Phase 6 - Start networking (proxy, DNS)

    tracing::info!("r8sd ready");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("r8sd shutting down...");

    Ok(())
}
