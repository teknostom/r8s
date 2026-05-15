use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageId(pub String);

#[derive(Debug, Clone)]
pub struct ContainerStatus {
    pub id: ContainerId,
    pub running: bool,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct Mount {
    pub host_path: String,
    pub container_path: String,
    pub readonly: bool,
}

#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub name: String,
    pub namespace: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub mounts: Vec<Mount>,
}

#[derive(Debug, Clone)]
pub struct RegistryAuth {
    pub username: String,
    pub password: String,
}

pub trait ContainerRuntime: Send + Sync {
    fn has_image(&self, image: &str) -> impl Future<Output = bool> + Send;

    fn pull_image(
        &self,
        image: &str,
        auth: Option<&RegistryAuth>,
    ) -> impl Future<Output = anyhow::Result<ImageId>> + Send;

    fn create_container(
        &self,
        config: &ContainerConfig,
    ) -> impl Future<Output = anyhow::Result<ContainerId>> + Send;

    /// Prepare a container's task — fork the runc init process so namespaces
    /// (incl. the network namespace) exist and have a stable PID, but do NOT
    /// exec the user command yet. Letting the caller set up pod networking
    /// against the init PID before `start_container` closes the race where a
    /// fast-failing user process would tear down its netns before
    /// `setup_pod_network` could nsenter into it.
    fn prepare_task(&self, id: &ContainerId) -> impl Future<Output = anyhow::Result<()>> + Send;

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

    fn container_pid(&self, id: &ContainerId) -> impl Future<Output = anyhow::Result<u32>> + Send;
}
