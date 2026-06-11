use std::path::Path;
use std::process::Command;

const BRIDGE_NAME: &str = "r8s0";
const BRIDGE_CIDR: &str = "10.244.0.1/24";
const VETH_ALIAS_PREFIX: &str = "r8s:";

pub fn setup_bridge(data_dir: &Path) -> anyhow::Result<()> {
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

    // Docker sets the FORWARD chain policy to DROP. -C checks for existence
    // first so repeated r8sd starts don't stack duplicate rules.
    ensure_iptables_forward("-i");
    ensure_iptables_forward("-o");

    tracing::info!("bridge {BRIDGE_NAME} ready");
    Ok(())
}

/// Configure the host-side veth and its peer inside the pod's netns.
///
/// `cluster` and `pod_uid` are written into the host-side veth's interface
/// alias so `list_owned_veths` can sweep stale links after a crash without
/// needing to trust the store.
pub fn setup_pod_network(
    pid: u32,
    pod_ip: &str,
    pod_name: &str,
    cluster: &str,
    pod_uid: &str,
) -> anyhow::Result<()> {
    let veth_host = veth_name(pod_name);
    let veth_peer = format!("{veth_host}p");
    let pid_str = pid.to_string();

    run(
        "ip",
        &[
            "link", "add", &veth_host, "type", "veth", "peer", "name", &veth_peer,
        ],
    )?;

    // Tag the host-side veth with cluster+pod ownership before any further
    // setup, so even a mid-create crash leaves a sweepable link.
    let alias = format_alias(cluster, pod_uid, pod_name);
    if let Err(e) = run("ip", &["link", "set", &veth_host, "alias", &alias]) {
        tracing::warn!(pod_name, "failed to set veth alias: {e}");
    }

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

/// A host-side veth attached to the r8s bridge and tagged with our ownership
/// alias. Returned by `list_owned_veths`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedVeth {
    pub link: String,
    pub pod_uid: String,
    pub pod_name: String,
}

/// Enumerate veth devices currently attached to the r8s bridge and return
/// those whose alias carries our cluster tag. This is the network-side mirror
/// of `ContainerRuntime::list_owned_containers`: teardown reads the live
/// kernel state rather than replaying what the store thinks exists.
///
/// Returns an empty vec if the bridge is gone (cluster never came up, or
/// already torn down) — never an error in that case.
pub fn list_owned_veths(cluster: &str) -> anyhow::Result<Vec<OwnedVeth>> {
    let brif_dir = Path::new("/sys/class/net").join(BRIDGE_NAME).join("brif");
    let entries = match std::fs::read_dir(&brif_dir) {
        Ok(e) => e,
        Err(_) => return Ok(Vec::new()),
    };

    let mut out = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let alias_path = Path::new("/sys/class/net").join(&name).join("ifalias");
        let alias = std::fs::read_to_string(&alias_path).unwrap_or_default();
        if let Some(owned) = parse_alias(&name, alias.trim(), cluster) {
            out.push(owned);
        }
    }
    Ok(out)
}

fn format_alias(cluster: &str, pod_uid: &str, pod_name: &str) -> String {
    format!("{VETH_ALIAS_PREFIX}cluster={cluster};uid={pod_uid};pod={pod_name}")
}

fn parse_alias(link: &str, alias: &str, want_cluster: &str) -> Option<OwnedVeth> {
    let rest = alias.strip_prefix(VETH_ALIAS_PREFIX)?;
    let mut cluster = "";
    let mut uid = String::new();
    let mut pod = String::new();
    for kv in rest.split(';') {
        let Some((k, v)) = kv.split_once('=') else {
            continue;
        };
        match k {
            "cluster" => cluster = v,
            "uid" => uid = v.to_string(),
            "pod" => pod = v.to_string(),
            _ => {}
        }
    }
    if cluster != want_cluster {
        return None;
    }
    Some(OwnedVeth {
        link: link.to_string(),
        pod_uid: uid,
        pod_name: pod,
    })
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

fn ensure_iptables_forward(direction: &str) {
    let check = Command::new("iptables")
        .args(["-C", "FORWARD", direction, BRIDGE_NAME, "-j", "ACCEPT"])
        .output();
    let present = matches!(check, Ok(o) if o.status.success());
    if !present {
        let _ = run(
            "iptables",
            &["-I", "FORWARD", "1", direction, BRIDGE_NAME, "-j", "ACCEPT"],
        );
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alias_round_trips() {
        let a = format_alias("alpha", "uid-1", "web-0");
        let parsed = parse_alias("veth0", &a, "alpha").expect("alpha cluster matches");
        assert_eq!(
            parsed,
            OwnedVeth {
                link: "veth0".to_string(),
                pod_uid: "uid-1".to_string(),
                pod_name: "web-0".to_string(),
            }
        );
    }

    #[test]
    fn alias_filters_foreign_cluster() {
        let a = format_alias("alpha", "uid-1", "web-0");
        assert!(parse_alias("veth0", &a, "beta").is_none());
    }

    #[test]
    fn alias_rejects_unowned_link() {
        assert!(parse_alias("veth0", "some other alias", "alpha").is_none());
        assert!(parse_alias("veth0", "", "alpha").is_none());
    }
}
