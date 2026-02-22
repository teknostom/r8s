use std::process::Command;

const BRIDGE_NAME: &str = "r8s0";
const BRIDGE_CIDR: &str = "10.244.0.1/24";

/// Create the r8s0 bridge and enable IP forwarding.
/// Idempotent — safe to call if the bridge already exists.
pub fn setup_bridge() -> anyhow::Result<()> {
    // Create resolv.conf for containers (must exist before any container is created)
    let resolv_dir = std::path::Path::new("/tmp/r8s");
    std::fs::create_dir_all(resolv_dir)?;

    // Preserve host resolver for DNS forwarding
    let host_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    let upstream = host_resolv
        .lines()
        .find_map(|l| l.strip_prefix("nameserver ").map(|s| s.trim().to_string()))
        .unwrap_or_else(|| "1.1.1.1".to_string());
    std::fs::write(resolv_dir.join("upstream_dns"), &upstream)?;

    std::fs::write(
        resolv_dir.join("resolv.conf"),
        "nameserver 10.244.0.1\nsearch default.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n",
    )?;

    // Create bridge (ignore "already exists")
    run_ignore_exists("ip", &["link", "add", BRIDGE_NAME, "type", "bridge"])?;
    run_ignore_exists("ip", &["addr", "add", BRIDGE_CIDR, "dev", BRIDGE_NAME])?;
    run("ip", &["link", "set", BRIDGE_NAME, "up"])?;

    // Enable IP forwarding
    run("sysctl", &["-w", "net.ipv4.ip_forward=1"])?;

    tracing::info!("bridge {BRIDGE_NAME} ready");
    Ok(())
}

/// Create a veth pair, attach to bridge, configure IP in the container's netns.
pub fn setup_pod_network(pid: u32, pod_ip: &str, pod_name: &str) -> anyhow::Result<()> {
    let veth_host = veth_name(pod_name);
    let pid_str = pid.to_string();

    // Create veth pair
    run(
        "ip",
        &[
            "link", "add", &veth_host, "type", "veth", "peer", "name", "eth0",
        ],
    )?;

    // Move eth0 into container netns
    run("ip", &["link", "set", "eth0", "netns", &pid_str])?;

    // Attach host side to bridge and bring up
    run("ip", &["link", "set", &veth_host, "master", BRIDGE_NAME])?;
    run("ip", &["link", "set", &veth_host, "up"])?;

    // Configure container side
    let ip_cidr = format!("{pod_ip}/24");
    nsenter(pid, &["ip", "addr", "add", &ip_cidr, "dev", "eth0"])?;
    nsenter(pid, &["ip", "link", "set", "eth0", "up"])?;
    nsenter(pid, &["ip", "link", "set", "lo", "up"])?;
    nsenter(pid, &["ip", "route", "add", "default", "via", "10.244.0.1"])?;

    tracing::info!(pod_name, pod_ip, pid, "pod network configured");
    Ok(())
}

/// Delete the veth pair for a pod. The peer end is auto-deleted.
pub fn teardown_pod_network(pod_name: &str) {
    let veth_host = veth_name(pod_name);
    if let Err(e) = run("ip", &["link", "delete", &veth_host]) {
        tracing::debug!(pod_name, "veth cleanup (may already be gone): {e}");
    }
}

/// Clean up bridge and nftables on shutdown.
pub fn cleanup() {
    let _ = run("ip", &["link", "delete", BRIDGE_NAME]);
    tracing::info!("bridge {BRIDGE_NAME} removed");
}

/// Sanitized veth name — max 15 chars for Linux interface names.
fn veth_name(pod_name: &str) -> String {
    let sanitized: String = pod_name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(10)
        .collect();
    format!("veth{sanitized}")
}

fn run(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new(cmd).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("{cmd} {}: {stderr}", args.join(" "));
    }
    Ok(())
}

fn run_ignore_exists(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new(cmd).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("File exists") {
            anyhow::bail!("{cmd} {}: {stderr}", args.join(" "));
        }
    }
    Ok(())
}

fn nsenter(pid: u32, cmd_args: &[&str]) -> anyhow::Result<()> {
    let pid_str = pid.to_string();
    let mut args = vec!["-t", &pid_str, "-n", "--"];
    args.extend_from_slice(cmd_args);
    run("nsenter", &args)
}
