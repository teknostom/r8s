use crate::error::RuntimeError;
use crate::traits::*;
use containerd_client::{
    connect,
    services::v1::{
        container::Runtime,
        containers_client::ContainersClient,
        content_client::ContentClient,
        images_client::ImagesClient,
        snapshots::{
            MountsRequest, PrepareSnapshotRequest, RemoveSnapshotRequest, StatSnapshotRequest,
            snapshots_client::SnapshotsClient,
        },
        tasks_client::TasksClient,
        transfer_client::TransferClient,
        *,
    },
    to_any,
    tonic::{Request, transport::Channel},
    types::{Platform, transfer::*, v1::Status as TaskStatus},
    with_namespace,
};
use oci_spec::runtime::{
    Capability, LinuxBuilder, LinuxCapabilitiesBuilder, LinuxNamespaceBuilder, LinuxNamespaceType,
    Mount as OciMount, MountBuilder, ProcessBuilder, RootBuilder, SpecBuilder, UserBuilder,
    get_default_mounts,
};
use sha2::{Digest, Sha256};

const NAMESPACE: &str = "r8s";
const SNAPSHOTTER: &str = "overlayfs";

// Ownership labels stamped on every container so teardown can enumerate the
// backend instead of replaying our store. The containerd namespace is shared
// across r8s clusters; the cluster label scopes a reap to our own containers.
const LABEL_CLUSTER: &str = "io.r8s.cluster";
const LABEL_POD_UID: &str = "io.r8s.pod-uid";
const LABEL_POD_NAME: &str = "io.r8s.pod-name";
const LABEL_CONTAINER: &str = "io.r8s.container";

pub struct ContainerdRuntime {
    channel: Channel,
    data_dir: std::path::PathBuf,
    /// This cluster's name, stamped into the `io.r8s.cluster` ownership label.
    cluster: String,
    // Serializes image pulls across all callers.
    pull_lock: tokio::sync::Mutex<()>,
}

impl ContainerdRuntime {
    pub async fn new(
        socket_path: &str,
        data_dir: std::path::PathBuf,
        cluster: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            channel: connect(socket_path).await?,
            data_dir,
            cluster,
            pull_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// A `kubectl exec` backend sharing this runtime's containerd connection.
    pub fn exec_handle(&self) -> crate::exec::ContainerdExec {
        crate::exec::ContainerdExec::new(self.channel.clone(), self.data_dir.clone())
    }

    async fn cleanup_stale(&self, name: &str) {
        let mut tasks = TasksClient::new(self.channel.clone());
        let kill = KillRequest {
            container_id: name.to_string(),
            signal: 9,
            ..Default::default()
        };
        let _ = tasks.kill(with_namespace!(kill, NAMESPACE)).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let del = DeleteTaskRequest {
            container_id: name.to_string(),
        };
        let _ = tasks.delete(with_namespace!(del, NAMESPACE)).await;
        let del = DeleteContainerRequest {
            id: name.to_string(),
        };
        let _ = ContainersClient::new(self.channel.clone())
            .delete(with_namespace!(del, NAMESPACE))
            .await;
        let rm = RemoveSnapshotRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: name.to_string(),
        };
        let _ = SnapshotsClient::new(self.channel.clone())
            .remove(with_namespace!(rm, NAMESPACE))
            .await;
    }

    async fn image_info(&self, image_ref: &str) -> anyhow::Result<ImageInfo> {
        let req = GetImageRequest {
            name: image_ref.to_string(),
        };
        let resp = ImagesClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await?;
        let image = resp
            .into_inner()
            .image
            .ok_or_else(|| anyhow::anyhow!("image not found: {image_ref}"))?;
        let target = image
            .target
            .ok_or_else(|| anyhow::anyhow!("image has no target descriptor"))?;

        // Resolve multi-arch manifest index to the platform-specific manifest
        let manifest_digest =
            if target.media_type.contains("index") || target.media_type.contains("manifest.list") {
                let index_bytes = self.read_content(&target.digest).await?;
                let index: serde_json::Value = serde_json::from_slice(&index_bytes)?;
                let arch = current_oci_arch();
                index
                    .get("manifests")
                    .and_then(|v| v.as_array())
                    .and_then(|manifests| {
                        manifests.iter().find(|m| {
                            m.get("platform")
                                .and_then(|p| p.get("os"))
                                .and_then(|v| v.as_str())
                                == Some("linux")
                                && m.get("platform")
                                    .and_then(|p| p.get("architecture"))
                                    .and_then(|v| v.as_str())
                                    == Some(arch)
                        })
                    })
                    .and_then(|m| m.get("digest").and_then(|v| v.as_str()))
                    .ok_or_else(|| anyhow::anyhow!("no manifest for linux/{arch}"))?
                    .to_string()
            } else {
                target.digest
            };

        let manifest_bytes = self.read_content(&manifest_digest).await?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
        let config_digest = manifest
            .get("config")
            .and_then(|c| c.get("digest"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("manifest has no config digest"))?;

        let config_bytes = self.read_content(config_digest).await?;
        let config: serde_json::Value = serde_json::from_slice(&config_bytes)?;

        let diff_ids: Vec<String> = config
            .get("rootfs")
            .and_then(|r| r.get("diff_ids"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("config has no rootfs.diff_ids"))?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let chain_id = compute_chain_id(&diff_ids)?;
        let img_config = config.get("config").unwrap_or(&serde_json::Value::Null);

        Ok(ImageInfo {
            chain_id,
            entrypoint: json_string_array(
                img_config
                    .get("Entrypoint")
                    .unwrap_or(&serde_json::Value::Null),
            ),
            cmd: json_string_array(img_config.get("Cmd").unwrap_or(&serde_json::Value::Null)),
            env: json_string_array(img_config.get("Env").unwrap_or(&serde_json::Value::Null)),
            working_dir: img_config
                .get("WorkingDir")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from),
        })
    }

    async fn read_content(&self, digest: &str) -> anyhow::Result<Vec<u8>> {
        let req = ReadContentRequest {
            digest: digest.to_string(),
            ..Default::default()
        };
        let resp = ContentClient::new(self.channel.clone())
            .read(with_namespace!(req, NAMESPACE))
            .await?;

        let mut data = Vec::new();
        let mut stream = resp.into_inner();
        while let Some(chunk) = stream.message().await? {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }
}

struct ImageInfo {
    chain_id: String,
    entrypoint: Vec<String>,
    cmd: Vec<String>,
    env: Vec<String>,
    working_dir: Option<String>,
}

fn json_string_array(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_image_ref(image: &str) -> String {
    // Docker Hub official images live under `library/`; containerd's resolver
    // doesn't insert it for bare names, so match the Docker CLI and do it here.
    let with_registry = if let Some(rest) = image.strip_prefix("docker.io/") {
        if rest.contains('/') {
            image.to_string()
        } else {
            format!("docker.io/library/{rest}")
        }
    } else {
        // The component before the first `/` is a registry only if it looks like
        // a host (contains `.` or `:`, or is `localhost`); otherwise it's a
        // Docker Hub namespace under docker.io.
        match image.split_once('/') {
            Some((first, _))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                image.to_string()
            }
            Some(_) => format!("docker.io/{image}"),
            None => format!("docker.io/library/{image}"),
        }
    };

    if with_registry
        .split('/')
        .next_back()
        .unwrap_or("")
        .contains(':')
    {
        with_registry
    } else {
        format!("{with_registry}:latest")
    }
}

fn current_oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

/// Map a task-lookup gRPC error: containerd returns `NotFound` once the task is
/// gone, which callers treat as terminal rather than a transient backend error.
fn task_lookup_err(id: &str, status: containerd_client::tonic::Status) -> RuntimeError {
    if status.code() == containerd_client::tonic::Code::NotFound {
        RuntimeError::NotFound(id.to_string())
    } else {
        RuntimeError::Other(status.into())
    }
}

/// chain_id[0] = diff_id[0]
/// chain_id[i] = sha256(chain_id[i-1] + " " + diff_id[i])
fn compute_chain_id(diff_ids: &[String]) -> anyhow::Result<String> {
    let mut chain_id = diff_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("image has no layers"))?
        .clone();

    for diff_id in &diff_ids[1..] {
        let mut hasher = Sha256::new();
        hasher.update(format!("{chain_id} {diff_id}"));
        chain_id = format!("sha256:{:x}", hasher.finalize());
    }

    Ok(chain_id)
}

fn build_container_mounts(
    config: &ContainerConfig,
    data_dir: &std::path::Path,
) -> anyhow::Result<Vec<OciMount>> {
    let mut mounts = get_default_mounts();
    let pod_name = config.pod_name.as_str();

    let resolv_dir = data_dir.join("resolv");
    std::fs::create_dir_all(&resolv_dir)?;
    let resolv_path = resolv_dir.join(format!("{}.conf", config.name));
    // 10.244.0.1 = r8s_types::NODE_INTERNAL_IP, inlined to avoid the dep.
    std::fs::write(
        &resolv_path,
        format!(
            "nameserver 10.244.0.1\nsearch {ns}.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n",
            ns = config.namespace,
        ),
    )?;
    mounts.push(
        MountBuilder::default()
            .destination("/etc/resolv.conf")
            .source(resolv_path)
            .typ("bind")
            .options(vec!["rbind".into(), "ro".into()])
            .build()?,
    );

    // /etc/hosts: map localhost and the pod hostname.
    let hosts_dir = data_dir.join("hosts");
    std::fs::create_dir_all(&hosts_dir)?;
    let hosts_path = hosts_dir.join(format!("{}.hosts", config.name));
    std::fs::write(
        &hosts_path,
        format!("127.0.0.1\tlocalhost\n::1\tlocalhost\n127.0.0.1\t{pod_name}\n"),
    )?;
    mounts.push(
        MountBuilder::default()
            .destination("/etc/hosts")
            .source(hosts_path)
            .typ("bind")
            .options(vec!["rbind".into(), "ro".into()])
            .build()?,
    );

    mounts.push(
        MountBuilder::default()
            .destination("/var/run/secrets/kubernetes.io/serviceaccount")
            .source(data_dir.join("serviceaccount"))
            .typ("bind")
            .options(vec!["rbind".into(), "ro".into()])
            .build()?,
    );

    for m in &config.mounts {
        mounts.push(
            MountBuilder::default()
                .destination(&m.container_path)
                .source(&m.host_path)
                .typ("bind")
                .options(if m.readonly {
                    vec!["rbind".into(), "ro".into()]
                } else {
                    vec!["rbind".into(), "rw".into()]
                })
                .build()?,
        );
    }

    Ok(mounts)
}

#[allow(clippy::disallowed_types)] // oci_spec Capabilities requires std HashSet
fn build_oci_spec(
    config: &ContainerConfig,
    image: &ImageInfo,
    data_dir: &std::path::Path,
) -> anyhow::Result<Vec<u8>> {
    // K8s semantics: command overrides ENTRYPOINT, args overrides CMD
    let args = if !config.command.is_empty() {
        let mut args = config.command.clone();
        args.extend(config.args.iter().cloned());
        args
    } else if !config.args.is_empty() {
        let mut args = image.entrypoint.clone();
        args.extend(config.args.iter().cloned());
        args
    } else {
        let mut args = image.entrypoint.clone();
        args.extend(image.cmd.iter().cloned());
        if args.is_empty() {
            vec!["/bin/sh".to_string()]
        } else {
            args
        }
    };

    let env: Vec<String> = image
        .env
        .iter()
        .cloned()
        .chain(config.env.iter().map(|(k, v)| format!("{k}={v}")))
        .collect();

    // Empty `workingDir` means unset (→ `/`); runc rejects a literal "" Cwd.
    // Protobuf clients send the field present-but-empty, so `None` isn't enough.
    let cwd = config
        .working_dir
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(image.working_dir.as_deref().filter(|s| !s.is_empty()))
        .unwrap_or("/");

    // Docker-default capabilities
    let default_caps = [
        Capability::AuditWrite,
        Capability::Chown,
        Capability::DacOverride,
        Capability::Fowner,
        Capability::Fsetid,
        Capability::Kill,
        Capability::Mknod,
        Capability::NetBindService,
        Capability::NetRaw,
        Capability::Setfcap,
        Capability::Setgid,
        Capability::Setpcap,
        Capability::Setuid,
        Capability::SysChroot,
    ]
    .into_iter()
    .collect::<std::collections::HashSet<_>>();
    // Ambient caps survive the switch to a non-root user — a runAsUser != 0
    // process needs them to keep e.g. NET_BIND_SERVICE. Irrelevant for root.
    let ambient: std::collections::HashSet<Capability> = if config.run_as_user.unwrap_or(0) != 0 {
        default_caps.clone()
    } else {
        Default::default()
    };
    let capabilities = LinuxCapabilitiesBuilder::default()
        .bounding(default_caps.clone())
        .effective(default_caps.clone())
        .permitted(default_caps.clone())
        .inheritable(default_caps)
        .ambient(ambient)
        .build()?;

    let hostname = config.pod_name.as_str();
    let mounts = build_container_mounts(config, data_dir)?;

    let spec = SpecBuilder::default()
        .hostname(hostname)
        .root(
            RootBuilder::default()
                .path("rootfs")
                .readonly(false)
                .build()?,
        )
        .process({
            let mut pb = ProcessBuilder::default()
                .args(args)
                .env(env)
                .cwd(cwd)
                .terminal(false)
                .capabilities(capabilities);
            if let Some(uid) = config.run_as_user {
                pb = pb.user(
                    UserBuilder::default()
                        .uid(uid)
                        .gid(config.run_as_group.unwrap_or(0))
                        .build()?,
                );
            }
            pb.build()?
        })
        .mounts(mounts)
        .linux(
            LinuxBuilder::default()
                .namespaces(vec![
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Pid)
                        .build()?,
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Network)
                        .build()?,
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Ipc)
                        .build()?,
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Uts)
                        .build()?,
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Mount)
                        .build()?,
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Cgroup)
                        .build()?,
                ])
                .build()?,
        )
        .build()?;

    Ok(serde_json::to_vec(&spec)?)
}

impl ContainerRuntime for ContainerdRuntime {
    async fn has_image(&self, image: &str) -> bool {
        let full_ref = normalize_image_ref(image);
        let req = GetImageRequest { name: full_ref };
        ImagesClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await
            .is_ok()
    }

    async fn pull_image(
        &self,
        image: &str,
        auth: Option<&RegistryAuth>,
    ) -> Result<ImageId, RuntimeError> {
        let _pull_guard = self.pull_lock.lock().await;
        let full_image_ref = normalize_image_ref(image);
        let resolver = auth.map(|a| {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", a.username, a.password));
            containerd_client::types::transfer::RegistryResolver {
                headers: [("Authorization".into(), format!("Basic {encoded}"))].into(),
                ..Default::default()
            }
        });
        let oci_registry = OciRegistry {
            reference: full_image_ref.clone(),
            resolver,
        };
        let architecture = current_oci_arch().to_string();
        let platform = Platform {
            os: "linux".to_string(),
            architecture,
            variant: "".to_string(),
            os_version: "".to_string(),
        };
        let image_store = ImageStore {
            name: full_image_ref.clone(),
            platforms: vec![platform.clone()],
            unpacks: vec![UnpackConfiguration {
                platform: Some(platform),
                snapshotter: SNAPSHOTTER.to_string(),
            }],
            ..Default::default()
        };
        let request = TransferRequest {
            source: Some(to_any(&oci_registry)),
            destination: Some(to_any(&image_store)),
            options: Some(TransferOptions::default()),
        };
        tracing::info!(image = full_image_ref, "pulling image");
        TransferClient::new(self.channel.clone())
            .transfer(with_namespace!(request, NAMESPACE))
            .await?;
        Ok(ImageId(full_image_ref))
    }

    async fn create_container(
        &self,
        config: &ContainerConfig,
    ) -> Result<ContainerId, RuntimeError> {
        let image_ref = normalize_image_ref(&config.image);
        let image_info = self.image_info(&image_ref).await?;
        self.cleanup_stale(&config.name).await;

        // Wait for image unpack to complete
        let snapshot_key = config.name.clone();
        let mut snapshots = SnapshotsClient::new(self.channel.clone());
        for i in 0..50 {
            let req = StatSnapshotRequest {
                snapshotter: SNAPSHOTTER.to_string(),
                key: image_info.chain_id.clone(),
            };
            match snapshots.stat(with_namespace!(req, NAMESPACE)).await {
                Ok(_) => break,
                Err(_) if i < 49 => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "image snapshot '{}' not ready after 10s: {e}",
                        image_info.chain_id
                    )
                    .into());
                }
            }
        }

        let req = PrepareSnapshotRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: snapshot_key.clone(),
            parent: image_info.chain_id.clone(),
            labels: Default::default(),
        };
        snapshots.prepare(with_namespace!(req, NAMESPACE)).await?;

        let spec_json = build_oci_spec(config, &image_info, &self.data_dir)?;
        let spec = prost_types::Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: spec_json,
        };

        // `Container.labels` is a protobuf map → std HashMap, not FxHashMap.
        #[allow(clippy::disallowed_types)]
        let labels = std::collections::HashMap::from([
            (LABEL_CLUSTER.to_string(), self.cluster.clone()),
            (LABEL_POD_UID.to_string(), config.pod_uid.clone()),
            (LABEL_POD_NAME.to_string(), config.pod_name.clone()),
            (LABEL_CONTAINER.to_string(), config.container_name.clone()),
        ]);
        let container = Container {
            id: config.name.clone(),
            image: image_ref,
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
            spec: Some(spec),
            snapshotter: SNAPSHOTTER.to_string(),
            snapshot_key,
            labels,
            ..Default::default()
        };
        let req = CreateContainerRequest {
            container: Some(container),
        };
        ContainersClient::new(self.channel.clone())
            .create(with_namespace!(req, NAMESPACE))
            .await?;

        tracing::info!(name = config.name, "created container");
        Ok(ContainerId(config.name.clone()))
    }

    async fn prepare_task(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        let mut snapshots = SnapshotsClient::new(self.channel.clone());
        let mut mounts = Vec::new();
        for i in 0..50 {
            let req = MountsRequest {
                snapshotter: SNAPSHOTTER.to_string(),
                key: id.0.clone(),
            };
            match snapshots.mounts(with_namespace!(req, NAMESPACE)).await {
                Ok(resp) => {
                    mounts = resp.into_inner().mounts;
                    break;
                }
                Err(_) if i < 49 => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("snapshot '{}' not found after 10s: {e}", id.0).into());
                }
            }
        }

        let log_dir = self.data_dir.join("logs");
        std::fs::create_dir_all(&log_dir)?;

        // CRI-style logging. Pumps must start *before* tasks.create() so the
        // fifo opens unblock on both ends.
        let log_paths = crate::log_pump::LogPaths::for_container(&log_dir, &id.0);
        crate::log_pump::start(&log_paths)?;

        let req = CreateTaskRequest {
            container_id: id.0.clone(),
            rootfs: mounts,
            stdout: log_paths.stdout_fifo.to_string_lossy().to_string(),
            stderr: log_paths.stderr_fifo.to_string_lossy().to_string(),
            ..Default::default()
        };
        // On failure the pump threads would block on their fifo open forever.
        if let Err(e) = TasksClient::new(self.channel.clone())
            .create(with_namespace!(req, NAMESPACE))
            .await
        {
            crate::log_pump::abort(&log_paths);
            return Err(e.into());
        }
        Ok(())
    }

    async fn start_container(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        let req = StartRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        TasksClient::new(self.channel.clone())
            .start(with_namespace!(req, NAMESPACE))
            .await?;

        tracing::info!(id = id.0, "started container");
        Ok(())
    }

    async fn stop_container(
        &self,
        id: &ContainerId,
        timeout: std::time::Duration,
    ) -> Result<(), RuntimeError> {
        let mut tasks = TasksClient::new(self.channel.clone());

        let req = KillRequest {
            container_id: id.0.clone(),
            signal: 15,
            all: true,
            ..Default::default()
        };
        if let Err(e) = tasks.kill(with_namespace!(req, NAMESPACE)).await {
            tracing::debug!(id = id.0, error = %e, "SIGTERM failed, task may already be stopped");
        }

        let wait_req = WaitRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let exited =
            tokio::time::timeout(timeout, tasks.wait(with_namespace!(wait_req, NAMESPACE))).await;

        if exited.is_err() {
            tracing::warn!(id = id.0, "container did not stop in time, sending SIGKILL");
            let req = KillRequest {
                container_id: id.0.clone(),
                signal: 9,
                all: true,
                ..Default::default()
            };
            let _ = tasks.kill(with_namespace!(req, NAMESPACE)).await;

            let wait_req = WaitRequest {
                container_id: id.0.clone(),
                ..Default::default()
            };
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tasks.wait(with_namespace!(wait_req, NAMESPACE)),
            )
            .await;
        }

        let req = DeleteTaskRequest {
            container_id: id.0.clone(),
        };
        let _ = tasks.delete(with_namespace!(req, NAMESPACE)).await;

        tracing::info!(id = id.0, "stopped container");
        Ok(())
    }

    async fn remove_container(&self, id: &ContainerId) -> Result<(), RuntimeError> {
        let req = DeleteTaskRequest {
            container_id: id.0.clone(),
        };
        let _ = TasksClient::new(self.channel.clone())
            .delete(with_namespace!(req, NAMESPACE))
            .await;

        let req = DeleteContainerRequest { id: id.0.clone() };
        let _ = ContainersClient::new(self.channel.clone())
            .delete(with_namespace!(req, NAMESPACE))
            .await;

        let req = RemoveSnapshotRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: id.0.clone(),
        };
        let _ = SnapshotsClient::new(self.channel.clone())
            .remove(with_namespace!(req, NAMESPACE))
            .await;

        // Task delete closed the shim's writer fds, so the pumps have exited.
        // Unlink the fifos; keep <id>.log for post-mortem `kubectl logs`.
        let log_paths =
            crate::log_pump::LogPaths::for_container(&self.data_dir.join("logs"), &id.0);
        let _ = std::fs::remove_file(&log_paths.stdout_fifo);
        let _ = std::fs::remove_file(&log_paths.stderr_fifo);

        tracing::info!(id = id.0, "removed container");
        Ok(())
    }

    async fn container_status(&self, id: &ContainerId) -> Result<ContainerStatus, RuntimeError> {
        let req = GetRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let resp = TasksClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await
            .map_err(|s| task_lookup_err(&id.0, s))?;
        let process = resp
            .into_inner()
            .process
            .ok_or_else(|| RuntimeError::NotFound(id.0.clone()))?;

        let status = TaskStatus::try_from(process.status).unwrap_or(TaskStatus::Unknown);

        Ok(ContainerStatus {
            id: id.clone(),
            running: matches!(status, TaskStatus::Running),
            exit_code: match status {
                TaskStatus::Stopped => Some(process.exit_status as i32),
                _ => None,
            },
        })
    }

    async fn container_pid(&self, id: &ContainerId) -> Result<u32, RuntimeError> {
        let req = GetRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let resp = TasksClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await
            .map_err(|s| task_lookup_err(&id.0, s))?;
        let process = resp
            .into_inner()
            .process
            .ok_or_else(|| RuntimeError::NotFound(id.0.clone()))?;
        Ok(process.pid)
    }

    async fn list_owned_containers(&self) -> Result<Vec<OwnedContainer>, RuntimeError> {
        // Server-side filter to this cluster — the containerd namespace is
        // shared across clusters.
        let req = ListContainersRequest {
            filters: vec![format!("labels.\"{LABEL_CLUSTER}\"==\"{}\"", self.cluster)],
        };
        let resp = ContainersClient::new(self.channel.clone())
            .list(with_namespace!(req, NAMESPACE))
            .await?
            .into_inner();
        let owned = resp
            .containers
            .into_iter()
            .map(|c| OwnedContainer {
                id: ContainerId(c.id),
                pod_uid: c.labels.get(LABEL_POD_UID).cloned().unwrap_or_default(),
                pod_name: c.labels.get(LABEL_POD_NAME).cloned().unwrap_or_default(),
            })
            .collect();
        Ok(owned)
    }

    async fn exec_sync(
        &self,
        id: &ContainerId,
        command: &[String],
        timeout: std::time::Duration,
    ) -> Result<i32, RuntimeError> {
        use crate::traits::{ExecConfig, ExecRuntime};

        let streams = self
            .exec_handle()
            .exec(ExecConfig {
                container_id: id.clone(),
                command: command.to_vec(),
                tty: false,
                initial_size: None,
            })
            .await?;

        // Drain stdout/stderr so a full channel never wedges the exec'd process.
        let mut stdout_rx = streams.stdout_rx;
        tokio::spawn(async move { while stdout_rx.recv().await.is_some() {} });
        if let Some(mut stderr_rx) = streams.stderr_rx {
            tokio::spawn(async move { while stderr_rx.recv().await.is_some() {} });
        }

        match tokio::time::timeout(timeout, streams.exit_rx).await {
            Ok(Ok(code)) => Ok(code),
            Ok(Err(_)) => Err(anyhow::anyhow!("exec wait channel closed").into()),
            Err(_) => Err(anyhow::anyhow!("exec '{}' timed out", id.0).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_chain_id, normalize_image_ref, task_lookup_err};
    use crate::error::RuntimeError;
    use containerd_client::tonic::Status;

    #[test]
    fn chain_id_single_layer_is_diff_id() {
        assert_eq!(
            compute_chain_id(&["sha256:aaa".to_string()]).unwrap(),
            "sha256:aaa"
        );
    }

    #[test]
    fn chain_id_empty_is_error() {
        let empty: &[String] = &[];
        assert!(compute_chain_id(empty).is_err());
    }

    #[test]
    fn chain_id_folds_layers_left_to_right() {
        // Vectors computed independently: sha256("<prev> <diff>") per OCI.
        assert_eq!(
            compute_chain_id(&["sha256:aaa".to_string(), "sha256:bbb".to_string()]).unwrap(),
            "sha256:56efb1d4f6c79b745d37d6eff87e3ed8dd2be28104e124ba73fd6e6c4892c792"
        );
        assert_eq!(
            compute_chain_id(&[
                "sha256:aaa".to_string(),
                "sha256:bbb".to_string(),
                "sha256:ccc".to_string(),
            ])
            .unwrap(),
            "sha256:5f37250437ffd4d0a708296d64527afcea82547c8f3683aed2e2e1abbc1999c2"
        );
    }

    #[test]
    fn task_lookup_err_classifies_notfound() {
        assert!(matches!(
            task_lookup_err("c1", Status::not_found("gone")),
            RuntimeError::NotFound(id) if id == "c1"
        ));
        assert!(matches!(
            task_lookup_err("c1", Status::unavailable("backend down")),
            RuntimeError::Other(_)
        ));
    }

    #[test]
    fn normalizes_image_refs() {
        // Bare name -> docker.io/library + :latest
        assert_eq!(
            normalize_image_ref("traefik"),
            "docker.io/library/traefik:latest"
        );
        assert_eq!(
            normalize_image_ref("ubuntu:22.04"),
            "docker.io/library/ubuntu:22.04"
        );

        // Docker Hub *namespaced* short ref (the ingress-nginx echo backend):
        // `hashicorp` is a namespace, not a registry, so it lives under docker.io.
        assert_eq!(
            normalize_image_ref("hashicorp/http-echo:0.2.3"),
            "docker.io/hashicorp/http-echo:0.2.3"
        );

        // Explicit registries (have a dot / port / localhost) are left alone.
        assert_eq!(
            normalize_image_ref("quay.io/jetstack/cert-manager-controller:v1.20.2"),
            "quay.io/jetstack/cert-manager-controller:v1.20.2"
        );
        assert_eq!(
            normalize_image_ref("registry.k8s.io/ingress-nginx/controller:v1.11.0"),
            "registry.k8s.io/ingress-nginx/controller:v1.11.0"
        );
        assert_eq!(
            normalize_image_ref("localhost:5000/my/app:dev"),
            "localhost:5000/my/app:dev"
        );

        // Explicit docker.io prefix: official images get library/, others kept.
        assert_eq!(
            normalize_image_ref("docker.io/redis:7"),
            "docker.io/library/redis:7"
        );
        assert_eq!(
            normalize_image_ref("docker.io/bitnami/redis:7"),
            "docker.io/bitnami/redis:7"
        );
    }
}
