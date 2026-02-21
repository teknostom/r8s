use std::time::Duration;

/// Unique identifier for a container.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(pub String);

/// Unique identifier for a pulled image.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageId(pub String);

/// Container status information.
#[derive(Debug, Clone)]
pub struct ContainerStatus {
    pub id: ContainerId,
    pub running: bool,
    pub exit_code: Option<i32>,
}

/// Configuration for creating a container.
#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
}

/// Pluggable container runtime interface.
///
/// Implementations can use youki/libcontainer, containerd CRI, or a mock for testing.
pub trait ContainerRuntime: Send + Sync {
    fn pull_image(&self, image: &str) -> impl Future<Output = anyhow::Result<ImageId>> + Send;

    fn create_container(
        &self,
        config: &ContainerConfig,
    ) -> impl Future<Output = anyhow::Result<ContainerId>> + Send;

    fn start_container(&self, id: &ContainerId) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn stop_container(
        &self,
        id: &ContainerId,
        timeout: Duration,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn remove_container(&self, id: &ContainerId)
    -> impl Future<Output = anyhow::Result<()>> + Send;

    fn container_status(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = anyhow::Result<ContainerStatus>> + Send;
}
