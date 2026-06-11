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

/// A backend container tagged as belonging to this cluster, discovered by
/// querying the runtime's ownership labels rather than our store. Returned by
/// [`ContainerRuntime::list_owned_containers`] so teardown can reap orphans by
/// enumerating what actually exists — robust to a crash that left the store or
/// in-memory state incomplete.
#[derive(Debug, Clone)]
pub struct OwnedContainer {
    pub id: ContainerId,
    pub pod_uid: String,
    pub pod_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct ContainerConfig {
    pub name: String,
    pub namespace: String,
    /// Pod uid, stamped as an ownership label on the backend container so it
    /// can be reaped by querying the runtime, not by replaying our store.
    pub pod_uid: String,
    /// Pod name — ownership label, and used to tear down the pod network when
    /// reaping a container discovered from the backend.
    pub pod_name: String,
    /// Container name within the pod (`spec.containers[].name`).
    pub container_name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub mounts: Vec<Mount>,
    /// `securityContext.runAsUser` — UID the container process runs as. `None`
    /// means root (uid 0), the previous behavior.
    pub run_as_user: Option<u32>,
    /// `securityContext.runAsGroup` — GID the container process runs as.
    pub run_as_group: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct RegistryAuth {
    pub username: String,
    pub password: String,
}

// ─── Exec (`kubectl exec`) ──────────────────────────────────────────────────

/// What to run inside an already-running container.
#[derive(Debug, Clone)]
pub struct ExecConfig {
    pub container_id: ContainerId,
    pub command: Vec<String>,
    /// Allocate a PTY (`kubectl exec -t`). When set, stderr is merged into
    /// stdout and `ExecStreams::stderr_rx` is `None`.
    pub tty: bool,
    /// Initial terminal size `(cols, rows)`, if known at start.
    pub initial_size: Option<(u16, u16)>,
}

/// Bidirectional streams for a live exec session. The API server's WebSocket
/// handler pumps these against the k8s channel protocol.
pub struct ExecStreams {
    /// Bytes written here are forwarded to the process's stdin.
    pub stdin_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub stdout_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    /// `None` when `tty` is set (stderr is merged into stdout).
    pub stderr_rx: Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    /// Terminal resize `(cols, rows)`; a no-op when `tty` is false.
    pub resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>,
    /// Resolves with the process exit code once it terminates.
    pub exit_rx: tokio::sync::oneshot::Receiver<i32>,
}

/// Runs a command inside a running container. Object-safe (boxed future) so the
/// API server can hold it behind `Arc<dyn ExecRuntime>`.
pub trait ExecRuntime: Send + Sync {
    fn exec(
        &self,
        config: ExecConfig,
    ) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<ExecStreams>> + Send + '_>>;
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

    /// List the containers this cluster owns, discovered from backend ownership
    /// labels (`io.r8s.cluster`) rather than from our store. Lets teardown reap
    /// orphans by enumerating what actually exists — a container is labeled the
    /// instant it's created, so this finds leaks a crash left out of the store
    /// or in-memory state, and the cluster filter keeps it from touching other
    /// clusters that share the runtime.
    fn list_owned_containers(
        &self,
    ) -> impl Future<Output = anyhow::Result<Vec<OwnedContainer>>> + Send;

    /// Run a command inside a running container and return its exit code. Used
    /// by exec-style probes. Unlike a host-side `nsenter`, this runs with the
    /// container's real environment and all of its namespaces — so PATH-relative
    /// binaries (`pg_isready`) and loopback-bound checks behave exactly as they
    /// do for a process inside the container.
    fn exec_sync(
        &self,
        id: &ContainerId,
        command: &[String],
        timeout: Duration,
    ) -> impl Future<Output = anyhow::Result<i32>> + Send;
}
