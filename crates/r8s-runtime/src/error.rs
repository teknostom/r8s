use thiserror::Error;

/// Errors from [`crate::ContainerRuntime`] operations.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The container or task no longer exists in the backend.
    #[error("container not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for RuntimeError {
    fn from(e: anyhow::Error) -> Self {
        RuntimeError::Other(e)
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(e: std::io::Error) -> Self {
        RuntimeError::Other(e.into())
    }
}

impl From<containerd_client::tonic::Status> for RuntimeError {
    fn from(e: containerd_client::tonic::Status) -> Self {
        RuntimeError::Other(e.into())
    }
}
