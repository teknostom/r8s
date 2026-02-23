use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

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
}

fn base_dir() -> PathBuf {
    PathBuf::from("/var/lib/r8s/clusters")
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("cluster name cannot be empty");
    }
    if name.contains('/') || name.contains('\\') || name == "." || name == ".." {
        anyhow::bail!("invalid cluster name '{name}'");
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        anyhow::bail!("cluster name must be alphanumeric (with - or _)");
    }
    Ok(())
}

fn cluster_dir(name: &str) -> PathBuf {
    base_dir().join(name)
}

/// Resolve cluster name: use provided name, or if omitted, pick the only existing cluster.
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

fn pid_file(dir: &Path) -> PathBuf {
    dir.join("r8sd.pid")
}

fn read_pid(dir: &Path) -> Option<u32> {
    std::fs::read_to_string(pid_file(dir))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn is_running(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

fn write_kubeconfig(dir: &Path, name: &str) -> anyhow::Result<()> {
    let content = format!(
        r#"apiVersion: v1
kind: Config
clusters:
- cluster:
    server: http://127.0.0.1:6443
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
"#
    );
    std::fs::write(dir.join("kubeconfig"), content)?;
    Ok(())
}

fn r8sd_binary() -> PathBuf {
    // Look next to the r8s binary first
    let self_path = std::env::current_exe().unwrap_or_default();
    let sibling = self_path.parent().unwrap_or(Path::new(".")).join("r8sd");
    if sibling.exists() {
        return sibling;
    }
    // Fall back to PATH
    PathBuf::from("r8sd")
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
            std::fs::create_dir_all(dir.join("logs"))?;
            std::fs::create_dir_all(dir.join("serviceaccount"))?;
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

            // Check if already running
            if let Some(pid) = read_pid(&dir) {
                if is_running(pid) {
                    anyhow::bail!("cluster '{name}' is already running (pid {pid})");
                }
            }

            write_kubeconfig(&dir, &name)?;

            if foreground {
                println!("Starting cluster '{name}' in foreground...");
                let status = Command::new(r8sd_binary())
                    .arg("--data-dir")
                    .arg(&dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("r8sd exited with {status}");
                }
            } else {
                let log_file = std::fs::File::create(dir.join("r8sd.log"))?;
                let stderr_file = log_file.try_clone()?;
                let mut child = Command::new(r8sd_binary())
                    .arg("--data-dir")
                    .arg(&dir)
                    .stdout(log_file)
                    .stderr(stderr_file)
                    .spawn()?;

                std::fs::write(pid_file(&dir), child.id().to_string())?;

                // Wait for boot: check process alive + API ready (up to 2s)
                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_secs(10);
                let api_addr: std::net::SocketAddr = "127.0.0.1:6443".parse().unwrap();
                let mut ready = false;

                while std::time::Instant::now() < deadline {
                    match child.try_wait()? {
                        Some(status) => {
                            let _ = std::fs::remove_file(pid_file(&dir));
                            let log = std::fs::read_to_string(dir.join("r8sd.log"))
                                .unwrap_or_default();
                            let tail: Vec<&str> = log.lines().rev().take(10).collect();
                            let tail: String = tail.into_iter().rev()
                                .collect::<Vec<_>>().join("\n");
                            anyhow::bail!(
                                "r8sd exited immediately ({status}):\n{tail}"
                            );
                        }
                        None => {}
                    }
                    if std::net::TcpStream::connect_timeout(
                        &api_addr,
                        std::time::Duration::from_millis(100),
                    ).is_ok() {
                        ready = true;
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }

                let kc = dir.join("kubeconfig");
                if ready {
                    println!("Cluster '{name}' started (pid {}).", child.id());
                } else {
                    println!(
                        "Cluster '{name}' started (pid {}), waiting for API server...",
                        child.id()
                    );
                }
                println!("export KUBECONFIG={}", kc.display());
            }
        }

        Cmd::Down { name } => {
            check_root()?;
            let name = resolve_name(name)?;
            let dir = cluster_dir(&name);

            let pid = match read_pid(&dir) {
                Some(pid) if is_running(pid) => pid,
                _ => {
                    println!("Cluster '{name}' is not running.");
                    return Ok(());
                }
            };

            // Send SIGTERM
            let ret = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if ret != 0 {
                anyhow::bail!("failed to send SIGTERM to pid {pid}");
            }

            // Wait for exit (up to 10 seconds)
            for _ in 0..100 {
                if !is_running(pid) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            if is_running(pid) {
                eprintln!("warning: r8sd (pid {pid}) did not exit, sending SIGKILL");
                let _ = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
            }

            let _ = std::fs::remove_file(pid_file(&dir));
            println!("Cluster '{name}' stopped.");
        }

        Cmd::Delete { name } => {
            validate_name(&name)?;
            let dir = cluster_dir(&name);
            if !dir.exists() {
                anyhow::bail!("cluster '{name}' does not exist");
            }

            // Stop if running
            if let Some(pid) = read_pid(&dir) {
                if is_running(pid) {
                    check_root()?;
                    unsafe {
                        libc::kill(pid as i32, libc::SIGTERM);
                    }
                    for _ in 0..50 {
                        if !is_running(pid) {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
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
                let dir = entry.path();
                let status = match read_pid(&dir) {
                    Some(pid) if is_running(pid) => "running",
                    _ => "stopped",
                };
                entries.push((name, status));
            }
            if entries.is_empty() {
                println!("No clusters.");
            } else {
                println!("{:<20} {}", "NAME", "STATUS");
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
            match read_pid(&dir) {
                Some(pid) if is_running(pid) => {
                    println!("Cluster '{name}': running (pid {pid})");
                    println!("Kubeconfig: {}", dir.join("kubeconfig").display());
                }
                _ => {
                    println!("Cluster '{name}': stopped");
                }
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

        Cmd::Logs { name, follow } => {
            let name = resolve_name(name)?;
            let log_path = cluster_dir(&name).join("r8sd.log");
            if !log_path.exists() {
                anyhow::bail!("no logs for cluster '{name}'");
            }

            if follow {
                let mut file = std::fs::File::open(&log_path)?;
                // Seek to end, then tail
                file.seek(SeekFrom::End(0))?;
                let mut reader = BufReader::new(file);
                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) => std::thread::sleep(std::time::Duration::from_millis(100)),
                        Ok(_) => print!("{line}"),
                        Err(e) => {
                            eprintln!("error reading log: {e}");
                            break;
                        }
                    }
                }
            } else {
                print!("{}", std::fs::read_to_string(&log_path)?);
            }
        }
    }

    Ok(())
}

fn check_root() -> anyhow::Result<()> {
    if unsafe { libc::getuid() } != 0 {
        anyhow::bail!("r8s requires root. Run with sudo.");
    }
    Ok(())
}
