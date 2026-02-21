/// The K8s-compatible API server built on axum.
pub struct ApiServer {
    // TODO: store handle, router, TLS config
}

impl ApiServer {
    /// Start the API server on the given address.
    pub async fn serve(self, _addr: std::net::SocketAddr) -> anyhow::Result<()> {
        todo!("Phase 2: implement API server")
    }
}
