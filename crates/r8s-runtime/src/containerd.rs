use crate::traits::*;
use containerd_client::{
    connect,
    services::v1::{
        container::Runtime,
        containers_client::ContainersClient,
        content_client::ContentClient,
        images_client::ImagesClient,
        snapshots::{
            MountsRequest, PrepareSnapshotRequest, RemoveSnapshotRequest,
            StatSnapshotRequest, snapshots_client::SnapshotsClient,
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
    MountBuilder, ProcessBuilder, RootBuilder, SpecBuilder, get_default_mounts,
};
use sha2::{Digest, Sha256};

const NAMESPACE: &str = "r8s";
const SNAPSHOTTER: &str = "overlayfs";

pub struct ContainerdRuntime {
    channel: Channel,
    data_dir: std::path::PathBuf,
}

impl ContainerdRuntime {
    pub async fn new(socket_path: &str, data_dir: std::path::PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            channel: connect(socket_path).await?,
            data_dir,
        })
    }

    /// Remove any stale task, container, and snapshot for a given name.
    /// All errors are ignored — the resources may not exist.
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

    /// Read the image config from containerd's content store.
    ///
    /// Returns the chain ID (for snapshot parent) and the image defaults
    /// (entrypoint, cmd, env, working dir).
    async fn image_info(&self, image_ref: &str) -> anyhow::Result<ImageInfo> {
        // 1. Get image metadata to find the target descriptor
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

        // 2. If it's a manifest index (multi-arch), resolve to platform-specific manifest
        let manifest_digest =
            if target.media_type.contains("index") || target.media_type.contains("manifest.list") {
                let index_bytes = self.read_content(&target.digest).await?;
                let index: serde_json::Value = serde_json::from_slice(&index_bytes)?;
                let arch = current_oci_arch();
                index["manifests"]
                    .as_array()
                    .and_then(|manifests| {
                        manifests.iter().find(|m| {
                            m["platform"]["os"].as_str() == Some("linux")
                                && m["platform"]["architecture"].as_str() == Some(arch)
                        })
                    })
                    .and_then(|m| m["digest"].as_str())
                    .ok_or_else(|| anyhow::anyhow!("no manifest for linux/{arch}"))?
                    .to_string()
            } else {
                target.digest
            };

        // 3. Read manifest to get config digest
        let manifest_bytes = self.read_content(&manifest_digest).await?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
        let config_digest = manifest["config"]["digest"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("manifest has no config digest"))?;

        // 4. Read config to get rootfs.diff_ids and image defaults
        let config_bytes = self.read_content(config_digest).await?;
        let config: serde_json::Value = serde_json::from_slice(&config_bytes)?;

        let diff_ids: Vec<String> = config["rootfs"]["diff_ids"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("config has no rootfs.diff_ids"))?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let chain_id = compute_chain_id(&diff_ids)?;

        // Extract image defaults from config.config (ENTRYPOINT, CMD, ENV, WorkingDir)
        let img_config = &config["config"];
        let entrypoint = json_string_array(&img_config["Entrypoint"]);
        let cmd = json_string_array(&img_config["Cmd"]);
        let env = json_string_array(&img_config["Env"]);
        let working_dir = img_config["WorkingDir"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);

        Ok(ImageInfo {
            chain_id,
            entrypoint,
            cmd,
            env,
            working_dir,
        })
    }

    /// Read a blob from containerd's content store by digest.
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
    let with_registry = if image.contains('/') {
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

/// Compute the chain ID of the topmost layer from diff_ids.
///
/// chain_id\[0\] = diff_id\[0\]
/// chain_id\[i\] = "sha256:" + hex(sha256(chain_id\[i-1\] + " " + diff_id\[i\]))
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

#[allow(clippy::disallowed_types)] // oci_spec::Capabilities requires std HashSet
fn build_oci_spec(
    config: &ContainerConfig,
    image: &ImageInfo,
    data_dir: &std::path::Path,
) -> anyhow::Result<Vec<u8>> {
    // K8s semantics: pod command overrides ENTRYPOINT, pod args overrides CMD.
    // If neither is set, use image defaults.
    let args = if !config.command.is_empty() {
        let mut args = config.command.clone();
        args.extend(config.args.iter().cloned());
        args
    } else if !config.args.is_empty() {
        // Pod args only: use image entrypoint + pod args
        let mut args = image.entrypoint.clone();
        args.extend(config.args.iter().cloned());
        args
    } else {
        // Neither set: use image entrypoint + image cmd
        let mut args = image.entrypoint.clone();
        args.extend(image.cmd.iter().cloned());
        if args.is_empty() {
            vec!["/bin/sh".to_string()]
        } else {
            args
        }
    };

    // Start with image env, then layer pod env on top
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

    // Default capabilities matching Docker defaults
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

    let spec = SpecBuilder::default()
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
        .mounts({
            let mut mounts = get_default_mounts();

            // Generate per-container resolv.conf with the pod's namespace in the search path
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
            mounts
        })
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
    async fn pull_image(&self, image: &str, auth: Option<&RegistryAuth>) -> anyhow::Result<ImageId> {
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

        // Read image config to get chain ID and default command/env
        let image_info = self.image_info(&image_ref).await?;

        // Clean up any stale state from a previous run (task → container → snapshot)
        self.cleanup_stale(&config.name).await;

        // Wait for image snapshot to be ready (unpack may still be in progress)
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

        // Prepare a writable snapshot for this container
        let req = PrepareSnapshotRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: snapshot_key.clone(),
            parent: image_info.chain_id.clone(),
            labels: Default::default(),
        };
        snapshots
            .prepare(with_namespace!(req, NAMESPACE))
            .await?;

        // Build OCI runtime spec and wrap in protobuf Any
        let spec_json = build_oci_spec(config, &image_info, &self.data_dir)?;
        let spec = prost_types::Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: spec_json,
        };

        // Create container metadata in containerd
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

    async fn start_container(&self, id: &ContainerId) -> anyhow::Result<()> {
        // Get rootfs mounts from the prepared snapshot
        let req = MountsRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: id.0.clone(),
        };
        let resp = SnapshotsClient::new(self.channel.clone())
            .mounts(with_namespace!(req, NAMESPACE))
            .await?;
        let mounts = resp.into_inner().mounts;

        // Set up log files for stdout/stderr (must exist before task creation)
        let log_dir = self.data_dir.join("logs");
        std::fs::create_dir_all(&log_dir)?;
        let stdout_path = log_dir.join(format!("{}.stdout", id.0));
        let stderr_path = log_dir.join(format!("{}.stderr", id.0));
        std::fs::File::create(&stdout_path)?;
        std::fs::File::create(&stderr_path)?;

        // Create task (prepare the runtime shim + rootfs)
        let req = CreateTaskRequest {
            container_id: id.0.clone(),
            rootfs: mounts,
            stdout: stdout_path.to_string_lossy().to_string(),
            stderr: stderr_path.to_string_lossy().to_string(),
            ..Default::default()
        };
        TasksClient::new(self.channel.clone())
            .create(with_namespace!(req, NAMESPACE))
            .await?;

        // Start task (exec the container process)
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

        // Send SIGTERM
        let req = KillRequest {
            container_id: id.0.clone(),
            signal: 15, // SIGTERM
            all: true,
            ..Default::default()
        };
        if let Err(e) = tasks.kill(with_namespace!(req, NAMESPACE)).await {
            tracing::debug!(id = id.0, error = %e, "kill SIGTERM failed (task may already be stopped)");
        }

        // Wait for exit with timeout
        let wait_req = WaitRequest {
            container_id: id.0.clone(),
            ..Default::default()
        };
        let wait_result =
            tokio::time::timeout(timeout, tasks.wait(with_namespace!(wait_req, NAMESPACE))).await;

        if wait_result.is_err() {
            // Timeout — force kill
            tracing::warn!(id = id.0, "container did not stop in time, sending SIGKILL");
            let req = KillRequest {
                container_id: id.0.clone(),
                signal: 9, // SIGKILL
                all: true,
                ..Default::default()
            };
            let _ = tasks.kill(with_namespace!(req, NAMESPACE)).await;

            // Brief wait for SIGKILL to take effect
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

        // Delete the task
        let req = DeleteTaskRequest {
            container_id: id.0.clone(),
        };
        let _ = tasks.delete(with_namespace!(req, NAMESPACE)).await;

        tracing::info!(id = id.0, "stopped container");
        Ok(())
    }

    async fn remove_container(&self, id: &ContainerId) -> anyhow::Result<()> {
        // Delete task (ignore errors — may not exist or already deleted)
        let req = DeleteTaskRequest {
            container_id: id.0.clone(),
        };
        let _ = TasksClient::new(self.channel.clone())
            .delete(with_namespace!(req, NAMESPACE))
            .await;

        // Delete container metadata
        let req = DeleteContainerRequest { id: id.0.clone() };
        let _ = ContainersClient::new(self.channel.clone())
            .delete(with_namespace!(req, NAMESPACE))
            .await;

        // Remove snapshot
        let req = RemoveSnapshotRequest {
            snapshotter: SNAPSHOTTER.to_string(),
            key: id.0.clone(),
        };
        let _ = SnapshotsClient::new(self.channel.clone())
            .remove(with_namespace!(req, NAMESPACE))
            .await;

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
