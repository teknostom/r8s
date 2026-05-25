//! Kubelet resource-metrics endpoint (`/metrics/resource`).
//!
//! metrics-server scrapes each node's kubelet over HTTPS for the Prometheus
//! "resource metrics" — per-container and node CPU/memory — then serves the
//! `metrics.k8s.io` API the aggregator proxies to. r8s has no real kubelet, so
//! we stand up a tiny HTTPS server here that reads the same numbers from
//! cgroup v2 (per container, via its PID) and `/proc` (node total).
//!
//! Bind address / port must match the node's `status.addresses` +
//! `daemonEndpoints` (see r8s-scheduler's register_node).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, extract::State, response::IntoResponse, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use r8s_runtime::{ContainerId, ContainerRuntime};
use r8s_store::Store;
use r8s_types::GroupVersionResource;

/// Node InternalIP / kubelet port — must match r8s-scheduler::register_node.
const BIND_IP: &str = "10.244.0.1";
const BIND_PORT: u16 = 10250;
/// USER_HZ on Linux (clock ticks per second for /proc/stat).
const USER_HZ: f64 = 100.0;

struct MetricsState<R: ContainerRuntime> {
    store: Store,
    runtime: Arc<R>,
}

pub async fn run<R: ContainerRuntime + 'static>(
    store: Store,
    runtime: Arc<R>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
) -> anyhow::Result<()> {
    // rustls needs a process-wide crypto provider. The API server installs one
    // too, but this server starts first — install_default is idempotent (Err
    // if already set), so the `let _` is safe either way.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let config = RustlsConfig::from_pem(cert_pem, key_pem).await?;
    let state = Arc::new(MetricsState { store, runtime });
    let app = Router::new()
        .route("/metrics/resource", get(metrics_resource))
        // metrics-server also probes /healthz on some paths; be friendly.
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state);
    let addr: SocketAddr = format!("{BIND_IP}:{BIND_PORT}").parse()?;
    tracing::info!(%addr, "kubelet metrics server listening");
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn metrics_resource<R: ContainerRuntime + 'static>(
    State(state): State<Arc<MetricsState<R>>>,
) -> impl IntoResponse {
    let ts = chrono::Utc::now().timestamp_millis();
    let mut out = String::new();

    // ── node ────────────────────────────────────────────────────────────
    out.push_str("# HELP node_cpu_usage_seconds_total [ALPHA] Cumulative cpu time consumed by the node in core-seconds\n");
    out.push_str("# TYPE node_cpu_usage_seconds_total counter\n");
    if let Some(cpu) = node_cpu_seconds() {
        out.push_str(&format!("node_cpu_usage_seconds_total {cpu} {ts}\n"));
    }
    out.push_str("# HELP node_memory_working_set_bytes [ALPHA] Current working set of the node in bytes\n");
    out.push_str("# TYPE node_memory_working_set_bytes gauge\n");
    if let Some(mem) = node_memory_working_set() {
        out.push_str(&format!("node_memory_working_set_bytes {mem} {ts}\n"));
    }

    // ── containers ──────────────────────────────────────────────────────
    out.push_str("# HELP container_cpu_usage_seconds_total [ALPHA] Cumulative cpu time consumed by the container in core-seconds\n");
    out.push_str("# TYPE container_cpu_usage_seconds_total counter\n");
    let mut mem_lines = String::new();
    mem_lines.push_str("# HELP container_memory_working_set_bytes [ALPHA] Current working set of the container in bytes\n");
    mem_lines.push_str("# TYPE container_memory_working_set_bytes gauge\n");

    for (namespace, pod, container, id) in running_containers(&state.store) {
        let Ok(pid) = state.runtime.container_pid(&ContainerId(id)).await else {
            continue;
        };
        let Some((cpu, mem)) = container_cgroup_stats(pid) else {
            continue;
        };
        let labels = format!("{{container=\"{container}\",namespace=\"{namespace}\",pod=\"{pod}\"}}");
        out.push_str(&format!("container_cpu_usage_seconds_total{labels} {cpu} {ts}\n"));
        mem_lines.push_str(&format!("container_memory_working_set_bytes{labels} {mem} {ts}\n"));
    }
    out.push_str(&mem_lines);

    (
        [("content-type", "text/plain; version=0.0.4")],
        out,
    )
}

/// (namespace, pod, container, container_id) for every running container.
fn running_containers(store: &Store) -> Vec<(String, String, String, String)> {
    let gvr = GroupVersionResource::pods();
    let Ok(list) = store.list(&gvr, None, None, None, None, None) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for pod in &list.items {
        if pod
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|v| v.as_str())
            != Some("Running")
        {
            continue;
        }
        let meta = pod.get("metadata");
        let Some(pod_name) = meta.and_then(|m| m.get("name")).and_then(|v| v.as_str()) else {
            continue;
        };
        let namespace = meta
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let containers = pod
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array());
        for c in containers.into_iter().flatten() {
            if let Some(cname) = c.get("name").and_then(|v| v.as_str()) {
                out.push((
                    namespace.to_string(),
                    pod_name.to_string(),
                    cname.to_string(),
                    format!("{pod_name}_{cname}"),
                ));
            }
        }
    }
    out
}

/// Node cumulative CPU core-seconds from `/proc/stat` (total minus idle/iowait).
fn node_cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let line = stat.lines().next()?; // "cpu  user nice system idle iowait irq softirq steal ..."
    let vals: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if vals.len() < 5 {
        return None;
    }
    let total: u64 = vals.iter().sum();
    let idle = vals[3] + vals.get(4).copied().unwrap_or(0); // idle + iowait
    Some((total - idle) as f64 / USER_HZ)
}

/// Node working set: MemTotal - MemAvailable, in bytes.
fn node_memory_working_set() -> Option<u64> {
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let read_kb = |key: &str| -> Option<u64> {
        info.lines()
            .find(|l| l.starts_with(key))?
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()
    };
    let total = read_kb("MemTotal:")?;
    let avail = read_kb("MemAvailable:")?;
    Some(total.saturating_sub(avail) * 1024)
}

/// (cpu_core_seconds, memory_working_set_bytes) for a process's cgroup v2.
fn container_cgroup_stats(pid: u32) -> Option<(f64, u64)> {
    // cgroup v2: /proc/<pid>/cgroup is a single `0::/<path>` line.
    let cg = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    let rel = cg.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    let dir = format!("/sys/fs/cgroup{rel}");

    // cpu.stat: `usage_usec <n>`
    let cpu_stat = std::fs::read_to_string(format!("{dir}/cpu.stat")).ok()?;
    let usage_usec: u64 = cpu_stat
        .lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let cpu_seconds = usage_usec as f64 / 1_000_000.0;

    // working set = memory.current - inactive_file (from memory.stat)
    let current: u64 = std::fs::read_to_string(format!("{dir}/memory.current"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    let inactive_file: u64 = std::fs::read_to_string(format!("{dir}/memory.stat"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("inactive_file "))
                .and_then(|v| v.trim().parse().ok())
        })
        .unwrap_or(0);
    let working_set = current.saturating_sub(inactive_file);

    Some((cpu_seconds, working_set))
}
