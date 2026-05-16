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
    Mount as OciMount, MountBuilder, ProcessBuilder, RootBuilder, SpecBuilder, get_default_mounts,
};
use sha2::{Digest, Sha256};

const NAMESPACE: &str = "r8s";
const SNAPSHOTTER: &str = "overlayfs";

pub struct ContainerdRuntime {
    channel: Channel,
    data_dir: std::path::PathBuf,
    // Serializes image pulls across all callers (matches kubelet's
    // --serialize-image-pulls=true default).
    pull_lock: tokio::sync::Mutex<()>,
}

impl ContainerdRuntime {
    pub async fn new(socket_path: &str, data_dir: std::path::PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            channel: connect(socket_path).await?,
            data_dir,
            pull_lock: tokio::sync::Mutex::new(()),
        })
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
    // Docker Hub's official images live under `library/`; the Docker CLI inserts
    // it automatically for bare names, but containerd's resolver doesn't.
    // Match that behavior here so charts that say `docker.io/traefik:v3.7.1`
    // (or just `traefik:v3.7.1`) resolve to `docker.io/library/traefik:v3.7.1`.
    let with_registry = if let Some(rest) = image.strip_prefix("docker.io/") {
        if rest.contains('/') {
            image.to_string()
        } else {
            format!("docker.io/library/{rest}")
        }
    } else if image.contains('/') {
        image.to_string()
    } else {
        format!("docker.io/library/{image}")
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
    let pod_name = config
        .name
        .rsplit_once('_')
        .map(|(p, _)| p)
        .unwrap_or(&config.name);

    let resolv_dir = data_dir.join("resolv");
    std::fs::create_dir_all(&resolv_dir)?;
    let resolv_path = resolv_dir.join(format!("{}.conf", config.name));
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

    // /etc/hosts maps the pod hostname to localhost for InetAddress.getLocalHost() compatibility
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

    let cwd = config
        .working_dir
        .as_deref()
        .or(image.working_dir.as_deref())
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
    let capabilities = LinuxCapabilitiesBuilder::default()
        .bounding(default_caps.clone())
        .effective(default_caps.clone())
        .permitted(default_caps.clone())
        .inheritable(default_caps)
        .build()?;

    let hostname = config
        .name
        .rsplit_once('_')
        .map(|(p, _)| p)
        .unwrap_or(&config.name);
    let mounts = build_container_mounts(config, data_dir)?;

    let spec = SpecBuilder::default()
        .hostname(hostname)
        .root(
            RootBuilder::default()
                .path("rootfs")
                .readonly(false)
                .build()?,
        )
        .process(
            ProcessBuilder::default()
                .args(args)
                .env(env)
                .cwd(cwd)
                .terminal(false)
                .capabilities(capabilities)
                .build()?,
        )
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
    ) -> anyhow::Result<ImageId> {
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

    async fn create_container(&self, config: &ContainerConfig) -> anyhow::Result<ContainerId> {
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
                    anyhow::bail!(
                        "image snapshot '{}' not ready after 10s: {e}",
                        image_info.chain_id
                    );
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

    async fn prepare_task(&self, id: &ContainerId) -> anyhow::Result<()> {
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
                    anyhow::bail!("snapshot '{}' not found after 10s: {e}", id.0);
                }
            }
        }

        let log_dir = self.data_dir.join("logs");
        std::fs::create_dir_all(&log_dir)?;

        // CRI-style logging: containerd writes to two named pipes, our pump
        // threads merge them into <id>.log with per-line timestamp + stream tag.
        // Threads must be started *before* tasks.create() so the fifo opens
        // unblock simultaneously on both ends.
        let log_paths = crate::log_pump::LogPaths::for_container(&log_dir, &id.0);
        crate::log_pump::start(&log_paths)?;

        let req = CreateTaskRequest {
            container_id: id.0.clone(),
            rootfs: mounts,
            stdout: log_paths.stdout_fifo.to_string_lossy().to_string(),
            stderr: log_paths.stderr_fifo.to_string_lossy().to_string(),
            ..Default::default()
        };
        TasksClient::new(self.channel.clone())
            .create(with_namespace!(req, NAMESPACE))
            .await?;
        Ok(())
    }

    async fn start_container(&self, id: &ContainerId) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<()> {
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

    async fn remove_container(&self, id: &ContainerId) -> anyhow::Result<()> {
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

        // The task delete above already closed the shim's writer fds, so the
        // pump threads have hit EOF and exited. Unlink the fifo paths so they
        // don't accumulate. Leave <id>.log for post-mortem `kubectl logs`.
        let log_paths =
            crate::log_pump::LogPaths::for_container(&self.data_dir.join("logs"), &id.0);
        let _ = std::fs::remove_file(&log_paths.stdout_fifo);
        let _ = std::fs::remove_file(&log_paths.stderr_fifo);

        tracing::info!(id = id.0, "removed container");
        Ok(())
    }

    async fn container_status(&self, id: &ContainerId) -> anyhow::Result<ContainerStatus> {
        let req = GetRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let resp = TasksClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await?;
        let process = resp
            .into_inner()
            .process
            .ok_or_else(|| anyhow::anyhow!("no process info for container {}", id.0))?;

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

    async fn container_pid(&self, id: &ContainerId) -> anyhow::Result<u32> {
        let req = GetRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let resp = TasksClient::new(self.channel.clone())
            .get(with_namespace!(req, NAMESPACE))
            .await?;
        let process = resp
            .into_inner()
            .process
            .ok_or_else(|| anyhow::anyhow!("no process info"))?;
        Ok(process.pid)
    }
}
