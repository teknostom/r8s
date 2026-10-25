use std::time::Duration;

use crate::error::RuntimeError;

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

/// A backend container discovered from the runtime's ownership labels rather
/// than our store, so teardown can reap orphans by enumerating what exists.
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
    /// Pod uid, stamped as an ownership label on the backend container.
    pub pod_uid: String,
    /// Pod name — ownership label and the container's hostname.
    pub pod_name: String,
    /// Container name within the pod (`spec.containers[].name`).
    pub container_name: String,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub mounts: Vec<Mount>,
    /// `securityContext.runAsUser` — process UID. `None` means root (uid 0).
    pub run_as_user: Option<u32>,
    /// `securityContext.runAsGroup` — process GID.
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
// TODO(followup): no kill path — dropping this (client disconnect) or an
// `exec_sync` timeout leaves the exec'd process running and its tasks alive.
// Add a kill handle (or Drop guard firing KillRequest on the exec_id).
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
    ) -> impl Future<Output = Result<ImageId, RuntimeError>> + Send;

    fn create_container(
        &self,
        config: &ContainerConfig,
    ) -> impl Future<Output = Result<ContainerId, RuntimeError>> + Send;

    /// Fork the runc init process so its namespaces (incl. network) exist with a
    /// stable PID, but do NOT exec the user command yet — lets the caller set up
    /// pod networking against the init PID before `start_container`.
    fn prepare_task(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn start_container(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn stop_container(
        &self,
        id: &ContainerId,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn remove_container(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    fn container_status(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<ContainerStatus, RuntimeError>> + Send;

    fn container_pid(
        &self,
        id: &ContainerId,
    ) -> impl Future<Output = Result<u32, RuntimeError>> + Send;

    /// List this cluster's containers, discovered from backend ownership labels
    /// (`io.r8s.cluster`) rather than our store. The cluster filter scopes the
    /// reap to our own containers on a shared runtime.
    fn list_owned_containers(
        &self,
    ) -> impl Future<Output = Result<Vec<OwnedContainer>, RuntimeError>> + Send;

    /// Run a command inside a running container and return its exit code. Used
    /// by exec probes; runs with the container's namespaces and environment.
    fn exec_sync(
        &self,
        id: &ContainerId,
        command: &[String],
        timeout: Duration,
    ) -> impl Future<Output = Result<i32, RuntimeError>> + Send;
}
