//! `kubectl exec` backend: spawn a new process inside a running container's
//! namespaces via containerd's ExecProcess API, and bridge containerd's stdio
//! FIFOs to mpsc channels the API server's WebSocket handler can pump from.
//!
//! Wire flow:
//!   API server WS ⇄ mpsc channels (here) ⇄ FIFOs ⇄ containerd shim ⇄ runc
//!
//! When `tty` is true, containerd's runc shim allocates a PTY and merges the
//! exec'd process's stderr into stdout — so `stderr_rx` is `None` and resizes
//! are honored via `ResizePtyRequest`.

use std::pin::Pin;
use std::time::Duration;

use containerd_client::{
    services::v1::{
        CloseIoRequest, DeleteProcessRequest, ExecProcessRequest, GetContainerRequest,
        ResizePtyRequest, StartRequest, WaitRequest, containers_client::ContainersClient,
        tasks_client::TasksClient,
    },
    tonic::{Request, transport::Channel},
    with_namespace,
};
use oci_spec::runtime::ProcessBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::traits::{ExecConfig, ExecRuntime, ExecStreams};

/// The containerd grpc namespace r8s uses for all of its tasks.
const NAMESPACE: &str = "r8s";

/// Single 8KB chunk is a good balance for terminal-paced traffic.
const PIPE_CHUNK: usize = 8 * 1024;

pub struct ContainerdExec {
    channel: Channel,
    data_dir: std::path::PathBuf,
}

impl ContainerdExec {
    pub fn new(channel: Channel, data_dir: std::path::PathBuf) -> Self {
        Self { channel, data_dir }
    }
}

impl ExecRuntime for ContainerdExec {
    fn exec(
        &self,
        config: ExecConfig,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ExecStreams>> + Send + '_>> {
        Box::pin(start_exec(
            self.channel.clone(),
            self.data_dir.clone(),
            config,
        ))
    }
}

#[derive(Clone)]
struct ExecPaths {
    stdin: std::path::PathBuf,
    stdout: std::path::PathBuf,
    stderr: std::path::PathBuf,
}

impl ExecPaths {
    fn for_session(data_dir: &std::path::Path, container_id: &str, exec_id: &str) -> Self {
        let dir = data_dir.join("exec");
        Self {
            stdin: dir.join(format!("{container_id}.{exec_id}.stdin")),
            stdout: dir.join(format!("{container_id}.{exec_id}.stdout")),
            stderr: dir.join(format!("{container_id}.{exec_id}.stderr")),
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.stdin);
        let _ = std::fs::remove_file(&self.stdout);
        let _ = std::fs::remove_file(&self.stderr);
    }
}

fn mkfifo(path: &std::path::Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let _ = std::fs::remove_file(path);
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // 0o600 — only r8sd reads/writes these.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn start_exec(
    channel: Channel,
    data_dir: std::path::PathBuf,
    config: ExecConfig,
) -> anyhow::Result<ExecStreams> {
    let exec_id = format!(
        "exec-{}-{}",
        std::process::id(),
        // Unique per exec; containerd needs uniqueness within the container.
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    let paths = ExecPaths::for_session(&data_dir, &config.container_id.0, &exec_id);
    std::fs::create_dir_all(paths.stdin.parent().expect("parent dir"))?;

    // Tear down a half-built session on setup failure.
    match build_exec_session(&channel, &paths, &config, &exec_id).await {
        Ok(streams) => Ok(streams),
        Err(e) => {
            teardown_failed_exec(&channel, &config.container_id.0, &exec_id, &paths).await;
            Err(e)
        }
    }
}

/// Delete the exec record and unlink the fifos.
async fn teardown_failed_exec(
    channel: &Channel,
    container_id: &str,
    exec_id: &str,
    paths: &ExecPaths,
) {
    let del = DeleteProcessRequest {
        container_id: container_id.to_string(),
        exec_id: exec_id.to_string(),
    };
    let _ = TasksClient::new(channel.clone())
        .delete_process(with_namespace!(del, NAMESPACE))
        .await;
    paths.cleanup();
}

async fn build_exec_session(
    channel: &Channel,
    paths: &ExecPaths,
    config: &ExecConfig,
    exec_id: &str,
) -> anyhow::Result<ExecStreams> {
    let exec_id = exec_id.to_owned();

    mkfifo(&paths.stdin)?;
    mkfifo(&paths.stdout)?;
    if !config.tty {
        mkfifo(&paths.stderr)?;
    }

    // Minimal OCI Process spec — namespaces and capabilities are inherited from
    // the container's task. Env is pulled from the container's spec so exec'd
    // processes resolve binaries against the same PATH.
    let mut env = fetch_container_env(channel.clone(), &config.container_id.0).await;
    if config.tty {
        env.push("TERM=xterm-256color".to_string());
    }
    let process = ProcessBuilder::default()
        .args(config.command.clone())
        .env(env)
        .cwd("/")
        .terminal(config.tty)
        .build()?;
    let spec_any = prost_types::Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: serde_json::to_vec(&process)?,
    };

    let mut tasks = TasksClient::new(channel.clone());

    let exec_req = ExecProcessRequest {
        container_id: config.container_id.0.clone(),
        stdin: paths.stdin.to_string_lossy().to_string(),
        stdout: paths.stdout.to_string_lossy().to_string(),
        stderr: if config.tty {
            String::new()
        } else {
            paths.stderr.to_string_lossy().to_string()
        },
        terminal: config.tty,
        spec: Some(spec_any),
        exec_id: exec_id.clone(),
    };
    tasks.exec(with_namespace!(exec_req, NAMESPACE)).await?;

    // Bridge mpsc → stdin FIFO. Open our write end before Start so the shim's
    // read open doesn't block on a missing writer.
    let (stdin_tx, mut stdin_rx) = mpsc::channel::<Vec<u8>>(16);
    let stdin_path = paths.stdin.clone();
    let stdin_channel = channel.clone();
    let stdin_cid = config.container_id.0.clone();
    let stdin_eid = exec_id.clone();
    tokio::spawn(async move {
        let file = match tokio::fs::OpenOptions::new()
            .write(true)
            .open(&stdin_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "exec stdin open failed");
                return;
            }
        };
        let mut writer = tokio::io::BufWriter::new(file);
        while let Some(chunk) = stdin_rx.recv().await {
            if writer.write_all(&chunk).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
        // Client closed stdin. Dropping our FIFO writer isn't enough — the shim
        // holds the stdin FIFO open, so EOF needs an explicit CloseIO.
        drop(writer);
        let mut tasks = TasksClient::new(stdin_channel);
        let close_req = CloseIoRequest {
            container_id: stdin_cid,
            exec_id: stdin_eid,
            stdin: true,
        };
        if let Err(e) = tasks.close_io(with_namespace!(close_req, NAMESPACE)).await {
            // A command that already exited has no process left to close.
            if e.message().contains("process does not exist") {
                tracing::debug!(error = %e, "exec CloseIO(stdin): process already exited");
            } else {
                tracing::warn!(error = %e, "exec CloseIO(stdin) failed");
            }
        }
    });

    // FIFO → mpsc readers for stdout / stderr.
    let (stdout_tx, stdout_rx) = mpsc::channel::<Vec<u8>>(16);
    spawn_pipe_reader(paths.stdout.clone(), stdout_tx);

    let stderr_rx_opt = if config.tty {
        None
    } else {
        let (stderr_tx, stderr_rx) = mpsc::channel::<Vec<u8>>(16);
        spawn_pipe_reader(paths.stderr.clone(), stderr_tx);
        Some(stderr_rx)
    };

    // Start the exec'd process now that all FIFOs are wired.
    let start_req = StartRequest {
        container_id: config.container_id.0.clone(),
        exec_id: exec_id.clone(),
    };
    tasks.start(with_namespace!(start_req, NAMESPACE)).await?;

    // Optional initial PTY size.
    if let (true, Some((cols, rows))) = (config.tty, config.initial_size) {
        let req = ResizePtyRequest {
            container_id: config.container_id.0.clone(),
            exec_id: exec_id.clone(),
            width: cols as u32,
            height: rows as u32,
        };
        let _ = tasks.resize_pty(with_namespace!(req, NAMESPACE)).await;
    }

    // Forward (cols, rows) to containerd while the session lives. No-op when
    // tty=false, kept for a uniform caller interface.
    let (resize_tx, mut resize_rx) = mpsc::channel::<(u16, u16)>(4);
    {
        let channel = channel.clone();
        let container_id = config.container_id.0.clone();
        let exec_id = exec_id.clone();
        let tty = config.tty;
        tokio::spawn(async move {
            let mut tasks = TasksClient::new(channel);
            while let Some((cols, rows)) = resize_rx.recv().await {
                if !tty {
                    continue;
                }
                let req = ResizePtyRequest {
                    container_id: container_id.clone(),
                    exec_id: exec_id.clone(),
                    width: cols as u32,
                    height: rows as u32,
                };
                let _ = tasks.resize_pty(with_namespace!(req, NAMESPACE)).await;
            }
        });
    }

    // Wait for the process to exit, then ship the exit code and clean up
    // FIFOs + the exec'd process record.
    let (exit_tx, exit_rx) = oneshot::channel::<i32>();
    {
        let channel = channel.clone();
        let container_id = config.container_id.0.clone();
        let exec_id = exec_id.clone();
        let paths = paths.clone();
        tokio::spawn(async move {
            let mut tasks = TasksClient::new(channel);
            let wait_req = WaitRequest {
                container_id: container_id.clone(),
                exec_id: exec_id.clone(),
            };
            let exit_code = match tasks.wait(with_namespace!(wait_req, NAMESPACE)).await {
                Ok(resp) => resp.into_inner().exit_status as i32,
                Err(e) => {
                    tracing::warn!(error = %e, "exec wait failed");
                    -1
                }
            };
            let _ = exit_tx.send(exit_code);

            // Free the exec record, else the next Wait can return stale state.
            let del_req = DeleteProcessRequest {
                container_id,
                exec_id,
            };
            let _ = tasks
                .delete_process(with_namespace!(del_req, NAMESPACE))
                .await;

            // Let the FIFO readers drain their last bytes, then unlink.
            tokio::time::sleep(Duration::from_millis(50)).await;
            paths.cleanup();
        });
    }

    Ok(ExecStreams {
        stdin_tx,
        stdout_rx,
        stderr_rx: stderr_rx_opt,
        resize_tx,
        exit_rx,
    })
}

/// Read the container's environment from its stored OCI spec so exec'd
/// processes resolve binaries like the main process. Falls back to a minimal
/// PATH on failure.
async fn fetch_container_env(channel: Channel, container_id: &str) -> Vec<String> {
    let fallback = || {
        vec![
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
            "HOME=/root".to_string(),
        ]
    };

    let req = GetContainerRequest {
        id: container_id.to_string(),
    };
    let spec = match ContainersClient::new(channel)
        .get(with_namespace!(req, NAMESPACE))
        .await
    {
        Ok(resp) => resp.into_inner().container.and_then(|c| c.spec),
        Err(_) => None,
    };
    let Some(spec) = spec else {
        return fallback();
    };
    let env: Vec<String> = serde_json::from_slice::<serde_json::Value>(&spec.value)
        .ok()
        .as_ref()
        .and_then(|v| v.get("process").and_then(|p| p.get("env")))
        .and_then(|e| e.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    if env.is_empty() { fallback() } else { env }
}

fn spawn_pipe_reader(path: std::path::PathBuf, tx: mpsc::Sender<Vec<u8>>) {
    tokio::spawn(async move {
        // O_RDONLY blocks until the shim opens its write end; tokio::fs runs it
        // on the blocking pool.
        let file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "exec pipe open failed");
                return;
            }
        };
        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = vec![0u8; PIPE_CHUNK];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break, // writer closed → EOF
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}
