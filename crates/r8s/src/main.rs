mod config;
mod helm;

use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "r8s", about = "Lightweight single-node Kubernetes in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a new cluster
    Create {
        /// Cluster name
        name: Option<String>,
    },
    /// Start a cluster
    Up {
        /// Cluster name
        name: Option<String>,
        /// Run in foreground instead of daemonizing
        #[arg(long)]
        foreground: bool,
    },
    /// Stop a cluster (preserves state)
    Down {
        /// Cluster name
        name: Option<String>,
    },
    /// Delete a cluster and all its data
    Delete {
        /// Cluster name
        name: String,
    },
    /// List all clusters
    List,
    /// Show cluster status
    Status {
        /// Cluster name
        name: Option<String>,
    },
    /// Print kubeconfig path for a cluster
    Kubeconfig {
        /// Cluster name
        name: Option<String>,
    },
    /// Show store statistics
    Stats {
        /// Cluster name
        name: Option<String>,
    },
    /// Tail daemon logs
    Logs {
        /// Cluster name
        name: Option<String>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Manage environment from r8s.toml
    Env {
        #[command(subcommand)]
        action: EnvCmd,
    },
}

#[derive(Subcommand)]
enum EnvCmd {
    /// Start environment: deploy deps and app from r8s.toml if not already running
    Up {
        /// Path to r8s.toml
        #[arg(short, long, default_value = "r8s.toml")]
        config: PathBuf,
    },
    /// Stop the cluster (state is preserved)
    Down,
    /// Uninstall all helm releases from r8s.toml
    Nuke {
        /// Path to r8s.toml
        #[arg(short, long, default_value = "r8s.toml")]
        config: PathBuf,
    },
}

fn base_dir() -> PathBuf {
    PathBuf::from("/var/lib/r8s/clusters")
}

fn cluster_dir(name: &str) -> PathBuf {
    base_dir().join(name)
}

fn pid_file(dir: &Path) -> PathBuf {
    dir.join("r8sd.pid")
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("cluster name cannot be empty");
    }
    if name.contains('/') || name.contains('\\') || name == "." || name == ".." {
        anyhow::bail!("invalid cluster name '{name}'");
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!("cluster name must be alphanumeric (with - or _)");
    }
    Ok(())
}

fn resolve_name(name: Option<String>) -> anyhow::Result<String> {
    if let Some(n) = name {
        validate_name(&n)?;
        return Ok(n);
    }

    let base = base_dir();
    if !base.exists() {
        anyhow::bail!("no clusters exist. Create one with: r8s create <name>");
    }

    let entries: Vec<String> = std::fs::read_dir(&base)?
        .filter_map(|e| {
            let e = e.ok()?;
            if e.file_type().ok()?.is_dir() {
                Some(e.file_name().to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();

    match entries.len() {
        0 => anyhow::bail!("no clusters exist. Create one with: r8s create <name>"),
        1 => Ok(entries.into_iter().next().unwrap()),
        _ => anyhow::bail!(
            "multiple clusters exist ({}). Specify which one.",
            entries.join(", ")
        ),
    }
}

fn create_cluster_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir.join("logs"))?;
    std::fs::create_dir_all(dir.join("serviceaccount"))?;
    Ok(())
}

fn write_kubeconfig(dir: &Path, name: &str) -> anyhow::Result<()> {
    let ca_path = dir.join("certs").join("ca.crt");
    let content = format!(
        r#"apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
    certificate-authority: {ca}
  name: r8s-{name}
contexts:
- context:
    cluster: r8s-{name}
    user: r8s-admin
  name: r8s-{name}
current-context: r8s-{name}
users:
- name: r8s-admin
  user: {{}}
"#,
        ca = ca_path.display()
    );
    std::fs::write(dir.join("kubeconfig"), content)?;
    Ok(())
}

fn read_pid(dir: &Path) -> Option<u32> {
    std::fs::read_to_string(pid_file(dir))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn is_running(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

fn daemon_running(dir: &Path) -> bool {
    read_pid(dir).is_some_and(is_running)
}

fn r8sd_binary() -> PathBuf {
    let self_path = std::env::current_exe().unwrap_or_default();
    let sibling = self_path.parent().unwrap_or(Path::new(".")).join("r8sd");
    if sibling.exists() {
        return sibling;
    }
    PathBuf::from("r8sd")
}

fn spawn_daemon(dir: &Path, name: &str) -> anyhow::Result<u32> {
    write_kubeconfig(dir, name)?;

    let log_file = std::fs::File::create(dir.join("r8sd.log"))?;
    let stderr_file = log_file.try_clone()?;
    let mut child = Command::new(r8sd_binary())
        .arg("--data-dir")
        .arg(dir)
        .stdout(log_file)
        .stderr(stderr_file)
        .spawn()?;

    let pid = child.id();
    std::fs::write(pid_file(dir), pid.to_string())?;

    let api_addr: SocketAddr = "127.0.0.1:6443".parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            let _ = std::fs::remove_file(pid_file(dir));
            let log = std::fs::read_to_string(dir.join("r8sd.log")).unwrap_or_default();
            let tail: String = log
                .lines()
                .rev()
                .take(10)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            anyhow::bail!("r8sd exited immediately ({status}):\n{tail}");
        }
        if TcpStream::connect_timeout(&api_addr, Duration::from_millis(100)).is_ok() {
            println!("Cluster '{name}' started (pid {pid}).");
            return Ok(pid);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("Cluster '{name}' started (pid {pid}), waiting for API server...");
    Ok(pid)
}

/// Returns true if a daemon was running and was stopped.
fn stop_daemon(dir: &Path) -> anyhow::Result<bool> {
    let pid = match read_pid(dir) {
        Some(pid) if is_running(pid) => pid,
        _ => return Ok(false),
    };

    let ret = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    if ret != 0 {
        anyhow::bail!("failed to send SIGTERM to pid {pid}");
    }

    for _ in 0..100 {
        if !is_running(pid) {
            let _ = std::fs::remove_file(pid_file(dir));
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("warning: r8sd (pid {pid}) did not exit, sending SIGKILL");
    let _ = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    let _ = std::fs::remove_file(pid_file(dir));
    Ok(true)
}

fn check_root() -> anyhow::Result<()> {
    if unsafe { libc::getuid() } != 0 {
        anyhow::bail!("r8s requires root. Run with sudo.");
    }
    Ok(())
}

fn ctr_list(resource: &str) -> Vec<String> {
    Command::new("ctr")
        .args(["-n", "r8s", resource, "ls", "-q"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

fn cleanup_containerd() {
    let tasks = ctr_list("tasks");
    for id in &tasks {
        let _ = Command::new("ctr")
            .args(["-n", "r8s", "tasks", "kill", "-s", "9", id])
            .output();
    }
    if !tasks.is_empty() {
        std::thread::sleep(Duration::from_millis(500));
    }
    for id in &tasks {
        let _ = Command::new("ctr")
            .args(["-n", "r8s", "tasks", "rm", id])
            .output();
    }

    for id in &ctr_list("containers") {
        let _ = Command::new("ctr")
            .args(["-n", "r8s", "containers", "rm", id])
            .output();
    }

    for id in &ctr_list("images") {
        let _ = Command::new("ctr")
            .args(["-n", "r8s", "images", "rm", id])
            .output();
    }

    println!("Cleaned up containerd resources.");
}

fn tail_follow(path: &Path) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let lines: Vec<&str> = content.lines().collect();
    let start = lines.len().saturating_sub(20);
    for line in &lines[start..] {
        println!("{line}");
    }

    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::End(0))?;
    let mut reader = BufReader::new(file);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => std::thread::sleep(Duration::from_millis(100)),
            Ok(_) => print!("{line}"),
            Err(e) => {
                eprintln!("error reading log: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Cmd::Create { name } => {
            let name = name.unwrap_or_else(|| "default".to_string());
            validate_name(&name)?;
            let dir = cluster_dir(&name);
            if dir.exists() {
                anyhow::bail!("cluster '{name}' already exists");
            }
            create_cluster_dir(&dir)?;
            println!("Cluster '{name}' created.");
            println!("Start it with: sudo r8s up {name}");
        }

        Cmd::Up { name, foreground } => {
            check_root()?;
            let name = resolve_name(name)?;
            let dir = cluster_dir(&name);
            if !dir.exists() {
                anyhow::bail!("cluster '{name}' does not exist. Create it with: r8s create {name}");
            }
            if daemon_running(&dir) {
                anyhow::bail!("cluster '{name}' is already running");
            }

            if foreground {
                write_kubeconfig(&dir, &name)?;
                println!("Starting cluster '{name}' in foreground...");
                let status = Command::new(r8sd_binary())
                    .arg("--data-dir")
                    .arg(&dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("r8sd exited with {status}");
                }
            } else {
                spawn_daemon(&dir, &name)?;
                println!("export KUBECONFIG={}", dir.join("kubeconfig").display());
            }
        }

        Cmd::Down { name } => {
            check_root()?;
            let name = resolve_name(name)?;
            let dir = cluster_dir(&name);
            if !stop_daemon(&dir)? {
                println!("Cluster '{name}' is not running.");
                return Ok(());
            }
            println!("Cluster '{name}' stopped.");
        }

        Cmd::Delete { name } => {
            validate_name(&name)?;
            let dir = cluster_dir(&name);
            if !dir.exists() {
                anyhow::bail!("cluster '{name}' does not exist");
            }
            if daemon_running(&dir) {
                check_root()?;
                stop_daemon(&dir)?;
            }
            std::fs::remove_dir_all(&dir)?;
            println!("Cluster '{name}' deleted.");
        }

        Cmd::List => {
            let base = base_dir();
            if !base.exists() {
                println!("No clusters.");
                return Ok(());
            }
            let mut entries: Vec<(String, &str)> = Vec::new();
            for entry in std::fs::read_dir(&base)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                let status = if daemon_running(&entry.path()) {
                    "running"
                } else {
                    "stopped"
                };
                entries.push((name, status));
            }
            if entries.is_empty() {
                println!("No clusters.");
            } else {
                println!("{:<20} STATUS", "NAME");
                for (name, status) in &entries {
                    println!("{:<20} {}", name, status);
                }
            }
        }

        Cmd::Status { name } => {
            let name = resolve_name(name)?;
            let dir = cluster_dir(&name);
            if !dir.exists() {
                anyhow::bail!("cluster '{name}' does not exist");
            }
            if let Some(pid) = read_pid(&dir).filter(|&pid| is_running(pid)) {
                println!("Cluster '{name}': running (pid {pid})");
                println!("Kubeconfig: {}", dir.join("kubeconfig").display());
            } else {
                println!("Cluster '{name}': stopped");
            }
        }

        Cmd::Kubeconfig { name } => {
            let name = resolve_name(name)?;
            let kc = cluster_dir(&name).join("kubeconfig");
            if !kc.exists() {
                anyhow::bail!("kubeconfig not found. Start the cluster first: sudo r8s up {name}");
            }
            println!("{}", kc.display());
        }

        Cmd::Stats { name } => {
            let name = resolve_name(name)?;
            let dir = cluster_dir(&name);
            let db_path = dir.join("store.db");
            if !db_path.exists() {
                anyhow::bail!("store not found for cluster '{name}'");
            }
            let (rev, rev_entries, resources) = r8s_store::stats(&db_path)?;
            let size = std::fs::metadata(&db_path)?.len();
            println!("Cluster: {name}");
            println!("Current revision:      {rev}");
            println!("Revision table entries: {rev_entries}");
            println!("Resource count:         {resources}");
            println!("Store size:             {:.1} KB", size as f64 / 1024.0);
        }

        Cmd::Env { action } => match action {
            EnvCmd::Up { config } => env_up(config)?,
            EnvCmd::Down => env_down()?,
            EnvCmd::Nuke { config } => env_nuke(config)?,
        },

        Cmd::Logs { name, follow } => {
            let name = resolve_name(name)?;
            let log_path = cluster_dir(&name).join("r8sd.log");
            if !log_path.exists() {
                anyhow::bail!("no logs for cluster '{name}'");
            }
            if follow {
                tail_follow(&log_path)?;
            } else {
                print!("{}", std::fs::read_to_string(&log_path)?);
            }
        }
    }

    Ok(())
}

fn env_up(config: PathBuf) -> anyhow::Result<()> {
    check_root()?;
    let config_path = std::fs::canonicalize(&config)
        .map_err(|e| anyhow::anyhow!("cannot find {}: {e}", config.display()))?;
    let project_dir = config_path.parent().unwrap();
    let cfg = config::load(&config_path)?;

    let name = cfg
        .cluster
        .as_ref()
        .and_then(|c| c.name.clone())
        .unwrap_or_else(|| "default".to_string());
    validate_name(&name)?;
    let dir = cluster_dir(&name);

    if !dir.exists() {
        create_cluster_dir(&dir)?;
        println!("Cluster '{name}' created.");
    }

    if !daemon_running(&dir) {
        spawn_daemon(&dir, &name)?;
    } else {
        println!("Cluster '{name}' already running.");
    }

    let kubeconfig = dir.join("kubeconfig");
    helm::check_helm()?;

    let default_ns = cfg
        .cluster
        .as_ref()
        .and_then(|c| c.namespace.as_deref())
        .unwrap_or("default");

    for dep in &cfg.dependencies {
        println!("Applying dependency '{}'...", dep.name);
        helm::install_release(dep, &kubeconfig, project_dir, default_ns)?;
    }
    if let Some(app) = &cfg.app {
        println!("Applying app '{}'...", app.name);
        helm::install_release(app, &kubeconfig, project_dir, default_ns)?;
    }

    println!("Environment ready.");
    println!("export KUBECONFIG={}", kubeconfig.display());
    Ok(())
}

fn env_down() -> anyhow::Result<()> {
    check_root()?;
    let name = resolve_name(None)?;
    let dir = cluster_dir(&name);
    if !stop_daemon(&dir)? {
        println!("Environment '{name}' is not running.");
        return Ok(());
    }
    println!("Environment '{name}' stopped. State preserved.");
    Ok(())
}

fn env_nuke(config: PathBuf) -> anyhow::Result<()> {
    check_root()?;
    let config_path = std::fs::canonicalize(&config)
        .map_err(|e| anyhow::anyhow!("cannot find {}: {e}", config.display()))?;
    let cfg = config::load(&config_path)?;

    let name = cfg
        .cluster
        .as_ref()
        .and_then(|c| c.name.clone())
        .unwrap_or_else(|| "default".to_string());
    validate_name(&name)?;
    let dir = cluster_dir(&name);

    if !dir.exists() {
        println!("Environment '{name}' does not exist.");
        return Ok(());
    }

    stop_daemon(&dir)?;
    cleanup_containerd();
    std::fs::remove_dir_all(&dir)?;
    println!("Environment '{name}' nuked.");
    Ok(())
}
