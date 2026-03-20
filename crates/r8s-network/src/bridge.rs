use std::process::Command;

const BRIDGE_NAME: &str = "r8s0";
const BRIDGE_CIDR: &str = "10.244.0.1/24";

pub fn setup_bridge(data_dir: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(data_dir)?;

    let host_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
    let upstream = host_resolv
        .lines()
        .find_map(|l| l.strip_prefix("nameserver ").map(|s| s.trim().to_string()))
        .unwrap_or_else(|| "1.1.1.1".to_string());
    std::fs::write(data_dir.join("upstream_dns"), &upstream)?;

    std::fs::write(
        data_dir.join("resolv.conf"),
        "nameserver 10.244.0.1\nsearch default.svc.cluster.local svc.cluster.local cluster.local\noptions ndots:5\n",
    )?;

    run_ignore_exists("ip", &["link", "add", BRIDGE_NAME, "type", "bridge"])?;
    run_ignore_exists("ip", &["addr", "add", BRIDGE_CIDR, "dev", BRIDGE_NAME])?;
    run("ip", &["link", "set", BRIDGE_NAME, "up"])?;

    run("sysctl", &["-w", "net.ipv4.ip_forward=1"])?;
    // Prevent bridge traffic between pods from being filtered by iptables/nftables
    let _ = run("sysctl", &["-w", "net.bridge.bridge-nf-call-iptables=0"]);
    let _ = run("sysctl", &["-w", "net.bridge.bridge-nf-call-ip6tables=0"]);

    // Dummy interface sinks ClusterIP traffic so the kernel accepts it
    // and routes it through prerouting DNAT
    let _ = run("ip", &["link", "add", "r8s-svc", "type", "dummy"]);
    let _ = run("ip", &["link", "set", "r8s-svc", "up"]);
    let _ = run("ip", &["addr", "add", "10.96.0.0/16", "dev", "r8s-svc"]);

    // Docker sets the FORWARD chain policy to DROP
    let _ = run(
        "iptables",
        &["-I", "FORWARD", "1", "-i", BRIDGE_NAME, "-j", "ACCEPT"],
    );
    let _ = run(
        "iptables",
        &["-I", "FORWARD", "1", "-o", BRIDGE_NAME, "-j", "ACCEPT"],
    );

    tracing::info!("bridge {BRIDGE_NAME} ready");
    Ok(())
}

pub fn setup_pod_network(pid: u32, pod_ip: &str, pod_name: &str) -> anyhow::Result<()> {
    let veth_host = veth_name(pod_name);
    let veth_peer = format!("{veth_host}p");
    let pid_str = pid.to_string();

    run(
        "ip",
        &[
            "link", "add", &veth_host, "type", "veth", "peer", "name", &veth_peer,
        ],
    )?;

    run("ip", &["link", "set", &veth_peer, "netns", &pid_str])?;
    nsenter(pid, &["ip", "link", "set", &veth_peer, "name", "eth0"])?;

    run("ip", &["link", "set", &veth_host, "master", BRIDGE_NAME])?;
    run("ip", &["link", "set", &veth_host, "up"])?;

    let ip_cidr = format!("{pod_ip}/24");
    nsenter(pid, &["ip", "addr", "add", &ip_cidr, "dev", "eth0"])?;
    nsenter(pid, &["ip", "link", "set", "eth0", "up"])?;
    nsenter(pid, &["ip", "link", "set", "lo", "up"])?;
    nsenter(pid, &["ip", "route", "add", "default", "via", "10.244.0.1"])?;

    tracing::info!(pod_name, pod_ip, pid, "pod network configured");
    Ok(())
}

pub fn teardown_pod_network(pod_name: &str) {
    let veth_host = veth_name(pod_name);
    if let Err(e) = run("ip", &["link", "delete", &veth_host]) {
        tracing::debug!(pod_name, "veth cleanup (may already be gone): {e}");
    }
}

pub fn cleanup() {
    let _ = run(
        "iptables",
        &["-D", "FORWARD", "-i", BRIDGE_NAME, "-j", "ACCEPT"],
    );
    let _ = run(
        "iptables",
        &["-D", "FORWARD", "-o", BRIDGE_NAME, "-j", "ACCEPT"],
    );
    let _ = run("ip", &["link", "delete", BRIDGE_NAME]);
    let _ = run("ip", &["link", "delete", "r8s-svc"]);
    tracing::info!("bridge {BRIDGE_NAME} removed");
}

/// Linux interface names are limited to 15 characters.
fn veth_name(pod_name: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    pod_name.hash(&mut hasher);
    let hash = hasher.finish();
    format!("veth{hash:010x}", hash = hash & 0xff_ffff_ffff)
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
        if !stderr.contains("File exists") && !stderr.contains("already") {
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
