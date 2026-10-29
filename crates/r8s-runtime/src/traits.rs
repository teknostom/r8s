use std::time::Duration;

use crate::error::RuntimeError;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(pub String);

/// Longest identifier containerd accepts.
const MAX_CONTAINER_ID_LEN: usize = 76;

impl ContainerId {
    /// `<namespace>_<pod>_<container>`, or when that exceeds containerd's
    /// limit, `<prefix>_<fnv1a32(namespace/pod)>_<container>`; pods of the
    /// same name in two namespaces never share one. Every component that
    /// names a backend container (kubelet, exec, orphan reaping) derives it
    /// from here. Container names hold no `_`, so the last `_`-separated part
    /// is always the container's name.
    pub fn derive(namespace: &str, pod_name: &str, container_name: &str) -> Self {
        let naive = format!("{namespace}_{pod_name}_{container_name}");
        if naive.len() <= MAX_CONTAINER_ID_LEN {
            return Self(naive);
        }
        let hash = format!(
            "{:08x}",
            fnv1a32(format!("{namespace}/{pod_name}").as_bytes())
        );
        let suffix_len = 1 + hash.len() + 1 + container_name.len();
        let budget = MAX_CONTAINER_ID_LEN.saturating_sub(suffix_len);
        let prefix: String = format!("{namespace}_{pod_name}")
            .chars()
            .take(budget)
            .collect();
        let prefix = prefix.trim_end_matches(['-', '.', '_']);
        Self(format!("{prefix}_{hash}_{container_name}"))
    }

    /// The container's name.
    pub fn container_name(&self) -> &str {
        self.0.rsplit('_').next().unwrap_or(&self.0)
    }
}

fn fnv1a32(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811c_9dc5_u32, |h, b| {
        (h ^ u32::from(*b)).wrapping_mul(0x0100_0193)
    })
}

#[cfg(test)]
mod container_id_tests {
    use super::ContainerId;

    /// containerd's identifier grammar: `^[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*$`,
    /// at most 76 characters.
    fn valid_identifier(id: &str) -> bool {
        let sep = |c: char| matches!(c, '.' | '_' | '-');
        let chars: Vec<char> = id.chars().collect();
        !chars.is_empty()
            && chars.len() <= 76
            && chars.iter().all(|c| c.is_ascii_alphanumeric() || sep(*c))
            && !sep(chars[0])
            && !sep(chars[chars.len() - 1])
            && !chars.windows(2).any(|w| sep(w[0]) && sep(w[1]))
    }

    #[test]
    fn short_names_are_joined_verbatim() {
        let id = ContainerId::derive("default", "web-0", "app");
        assert_eq!(id.0, "default_web-0_app");
        assert_eq!(id.container_name(), "app");
        assert_ne!(id, ContainerId::derive("other", "web-0", "app"));
    }

    #[test]
    fn long_names_fit_containerd_and_keep_the_container_name() {
        let pod = "sample-crd-conversion-webhook-deployment-5abea92ec6-3diep";
        let id = ContainerId::derive("crd-webhook-1234", pod, "sample-crd-conversion-webhook");
        assert!(valid_identifier(&id.0), "{}", id.0);
        assert_eq!(id.0.len(), 76, "{}", id.0);
        assert!(id.0.ends_with("_sample-crd-conversion-webhook"));
        assert!(id.0.starts_with("crd-webhook-1234_sample-"));
        assert_eq!(id.container_name(), "sample-crd-conversion-webhook");
        assert_eq!(
            id,
            ContainerId::derive("crd-webhook-1234", pod, "sample-crd-conversion-webhook")
        );
    }

    #[test]
    fn sibling_pods_with_a_shared_prefix_stay_distinct() {
        let container = "a-container-name-that-is-rather-long-but-still-valid-under-dns";
        let a = ContainerId::derive(
            "ns",
            "deployment-with-a-very-long-name-7c9f8d6b5a-aaaaa",
            container,
        );
        let b = ContainerId::derive(
            "ns",
            "deployment-with-a-very-long-name-7c9f8d6b5a-bbbbb",
            container,
        );
        assert_ne!(a, b);
        assert!(valid_identifier(&a.0), "{}", a.0);
        assert!(valid_identifier(&b.0), "{}", b.0);
    }
}

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
    /// Network namespace the container joins (the pod's); `None` gives the
    /// container a namespace of its own.
    pub netns_path: Option<String>,
    /// `spec.hostNetwork`: the container runs in the host's network namespace.
    pub host_network: bool,
    /// The pod's address, for the managed `/etc/hosts`.
    pub pod_ip: Option<String>,
    /// `spec.hostAliases`: extra `/etc/hosts` entries, `(ip, hostnames)`.
    pub host_aliases: Vec<(String, Vec<String>)>,
    /// `terminationMessagePath`: where the container writes its last words.
    pub termination_message_path: Option<String>,
    /// `spec.securityContext.sysctls`, applied in the container's namespaces.
    pub sysctls: Vec<(String, String)>,
    /// `securityContext.privileged`: every capability.
    pub privileged: bool,
    /// `securityContext.readOnlyRootFilesystem`.
    pub read_only_root_fs: bool,
    /// `securityContext.allowPrivilegeEscalation: false`.
    pub no_new_privileges: bool,
    /// `securityContext.capabilities.add`, upstream names (`NET_ADMIN`).
    pub cap_add: Vec<String>,
    /// `securityContext.capabilities.drop`.
    pub cap_drop: Vec<String>,
    /// The pod's hostname (`spec.hostname`, else the pod name), at most 63
    /// characters as upstream truncates it.
    pub hostname: String,
    /// `spec.subdomain`: names the pod's
    /// `<hostname>.<subdomain>.<namespace>.svc.cluster.local` hosts entry.
    pub subdomain: String,
    /// The pod's resolv.conf.
    pub dns: DnsConfig,
}

/// A resolver configuration: resolv.conf's nameserver, search and options
/// lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsConfig {
    pub nameservers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

impl DnsConfig {
    /// resolv.conf text.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for nameserver in &self.nameservers {
            out.push_str(&format!("nameserver {nameserver}\n"));
        }
        if !self.searches.is_empty() {
            out.push_str(&format!("search {}\n", self.searches.join(" ")));
        }
        if !self.options.is_empty() {
            out.push_str(&format!("options {}\n", self.options.join(" ")));
        }
        out
    }
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

    /// A channel that receives a message whenever one of our containers'
    /// tasks exits; `None` when the backend has no exit events.
    fn exit_events(&self) -> Option<tokio::sync::mpsc::Receiver<()>> {
        None
    }
}
