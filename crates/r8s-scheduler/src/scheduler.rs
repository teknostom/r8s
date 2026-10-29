use std::collections::BTreeMap;

use r8s_store::{
    Store,
    backend::ResourceRef,
    events::{NORMAL, ObjectRef, Recorder, WARNING},
    watch::WatchEventType,
};

use crate::fit::{self, Misfit};
use r8s_types::{
    DaemonEndpoint, GroupVersionResource, KUBELET_PORT, NODE_INTERNAL_IP, Node, NodeAddress,
    NodeCondition, NodeDaemonEndpoints, NodeSpec, NodeStatus, NodeSystemInfo, ObjectMeta,
    PodCondition, Quantity, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const NODE_NAME: &str = "r8s-node";
const SCHEDULER_NAME: &str = "default-scheduler";

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("scheduler started");

    register_node(&store)?;
    let recorder = Recorder::new(store.clone(), SCHEDULER_NAME, NODE_NAME);

    // Subscribe before the snapshot so a pod created during the initial scan
    // arrives as an event rather than vanishing in a list/watch gap.
    let (initial, mut rx) =
        store.list_and_watch(&GroupVersionResource::pods(), None, None, None)?;
    for pod in &initial.items {
        schedule_pod(&store, &recorder, pod);
    }

    // The node's Lease heartbeat, as a kubelet renews it every 10s.
    renew_node_lease(&store);
    let mut lease_tick = tokio::time::interval(std::time::Duration::from_secs(10));
    lease_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Unschedulable pods are retried as the node frees up.
    let mut retry_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("scheduler shutting down");
                return Ok(());
            }
            _ = lease_tick.tick() => renew_node_lease(&store),
            _ = retry_tick.tick() => schedule_all(&store, &recorder),
            event = rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        schedule_pod(&store, &recorder, &event.object);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("scheduler lagged {n} events, re-syncing");
                        schedule_all(&store, &recorder);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

/// `kube-node-lease/<node>` with a fresh `renewTime`; created on first use.
fn renew_node_lease(store: &Store) {
    let gvr = GroupVersionResource::new("coordination.k8s.io", "v1", "leases");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("kube-node-lease"),
        name: NODE_NAME,
    };
    let spec = serde_json::json!({
        "holderIdentity": NODE_NAME,
        "leaseDurationSeconds": 40,
        "renewTime": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
    });
    let result = match store.get(&rref) {
        Ok(Some(mut lease)) => {
            lease["spec"] = spec;
            store.update(&rref, &lease).map(|_| ())
        }
        Ok(None) => store
            .create(
                rref,
                &serde_json::json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {"name": NODE_NAME, "namespace": "kube-node-lease"},
                    "spec": spec,
                }),
            )
            .map(|_| ()),
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        tracing::warn!("node lease renew failed: {e}");
    }
}

#[allow(clippy::needless_update)]
fn register_node(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::nodes();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: NODE_NAME,
    };

    if let Some(mut existing) = store.get(&resource_ref)? {
        tracing::info!("node '{NODE_NAME}' already registered");
        // A node registered by an older daemon takes the current nodeInfo
        // and well-known labels.
        let before = existing.clone();
        existing["status"]["nodeInfo"] = serde_json::to_value(node_info())?;
        if !existing["metadata"]["labels"].is_object() {
            existing["metadata"]["labels"] = serde_json::json!({});
        }
        for (key, value) in [
            ("kubernetes.io/hostname", NODE_NAME),
            ("kubernetes.io/os", "linux"),
            ("kubernetes.io/arch", go_arch()),
        ] {
            existing["metadata"]["labels"][key] = serde_json::json!(value);
        }
        if existing != before {
            store.update(&resource_ref, &existing)?;
        }
        return Ok(());
    }

    let now = Time(chrono::Utc::now());
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let memory_ki = read_memtotal_ki().unwrap_or(8 * 1024 * 1024);
    let capacity = BTreeMap::from([
        ("cpu".into(), Quantity(cpu_count.to_string())),
        ("memory".into(), Quantity(format!("{memory_ki}Ki"))),
        ("pods".into(), Quantity("110".into())),
    ]);

    let node = Node {
        metadata: ObjectMeta {
            name: Some(NODE_NAME.into()),
            labels: Some(BTreeMap::from([
                ("kubernetes.io/hostname".into(), NODE_NAME.into()),
                ("kubernetes.io/os".into(), "linux".into()),
                ("kubernetes.io/arch".into(), go_arch().into()),
            ])),
            ..Default::default()
        },
        spec: Some(NodeSpec::default()),
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                type_: "Ready".into(),
                status: "True".into(),
                last_heartbeat_time: Some(now.clone()),
                last_transition_time: Some(now),
                reason: Some("KubeletReady".into()),
                message: Some("r8s node is ready".into()),
                ..Default::default()
            }]),
            node_info: Some(node_info()),
            capacity: Some(capacity.clone()),
            allocatable: Some(capacity),
            // Addresses + kubelet endpoint so metrics-server can locate and
            // scrape this node's /metrics/resource.
            addresses: Some(vec![
                NodeAddress {
                    type_: "InternalIP".into(),
                    address: NODE_INTERNAL_IP.into(),
                },
                NodeAddress {
                    type_: "Hostname".into(),
                    address: NODE_NAME.into(),
                },
            ]),
            daemon_endpoints: Some(NodeDaemonEndpoints {
                kubelet_endpoint: Some(DaemonEndpoint {
                    port: i32::from(KUBELET_PORT),
                }),
            }),
            ..Default::default()
        }),
    };

    store.create(resource_ref, &serde_json::to_value(&node)?)?;
    tracing::info!("registered node '{NODE_NAME}'");
    Ok(())
}

/// The node's `status.nodeInfo`; the proxy runs inside the daemon, so it
/// shares the kubelet's version.
fn node_info() -> NodeSystemInfo {
    NodeSystemInfo {
        operating_system: "linux".into(),
        architecture: go_arch().into(),
        kubelet_version: "v1.32.0-r8s".into(),
        kube_proxy_version: "v1.32.0-r8s".into(),
        container_runtime_version: "containerd://2".into(),
        os_image: os_image(),
        kernel_version: std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|s| s.trim().to_string())
            .unwrap_or_default(),
        ..Default::default()
    }
}

/// The architecture as Go names it, which `kubernetes.io/arch` uses.
fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64le",
        other => other,
    }
}

/// `PRETTY_NAME` from /etc/os-release.
fn os_image() -> String {
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|l| l.strip_prefix("PRETTY_NAME="))
                .map(|v| v.trim_matches('"').to_string())
        })
        .unwrap_or_else(|| "Linux".into())
}

fn schedule_all(store: &Store, recorder: &Recorder) {
    let result = match store.list(&GroupVersionResource::pods(), None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("failed to list pods: {e}");
            return;
        }
    };
    for pod in &result.items {
        schedule_pod(store, recorder, pod);
    }
}

fn is_scheduled(pod: &serde_json::Value) -> bool {
    pod.get("spec")
        .and_then(|spec| spec.get("nodeName"))
        .and_then(|name| name.as_str())
        .is_some_and(|name| !name.is_empty())
}

fn schedule_pod(store: &Store, recorder: &Recorder, pod_value: &serde_json::Value) {
    // Already bound — skip without a store round-trip. Kubelet status writes
    // re-fire this watch for pods we scheduled long ago.
    if is_scheduled(pod_value) || !ours(pod_value) {
        return;
    }

    let metadata = pod_value.get("metadata");
    let name = match metadata
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
    {
        Some(name) => name,
        None => return,
    };
    let namespace = metadata
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };

    // Re-read: the event may be stale, and the optimistic update needs the live
    // resourceVersion to detect a concurrent binding.
    let mut current = match store.get(&resource_ref) {
        Ok(Some(v)) => v,
        _ => return,
    };
    if is_scheduled(&current) || !ours(&current) {
        return;
    }

    let node_gvr = GroupVersionResource::nodes();
    let Ok(Some(node)) = store.get(&ResourceRef {
        gvr: &node_gvr,
        namespace: None,
        name: NODE_NAME,
    }) else {
        return;
    };
    // Controllers create pods in-process, past the Priority admission
    // plugin: resolve each pod's class here.
    let classes = priority_classes(store);
    fill_priority(&classes, &mut current);
    let all: Vec<serde_json::Value> = store
        .list(&gvr, None, None, None, None, None)
        .map(|r| r.items)
        .unwrap_or_default()
        .into_iter()
        .map(|mut p| {
            fill_priority(&classes, &mut p);
            p
        })
        .collect();
    let others: Vec<&serde_json::Value> = all
        .iter()
        .filter(|p| fit::holds_resources(p, NODE_NAME))
        .collect();
    // Room a preemption freed is held for its preemptor: pending pods
    // nominated here with at least this pod's priority count as placed.
    let my_uid = &current["metadata"]["uid"];
    let mut reserved = others.clone();
    reserved.extend(all.iter().filter(|p| {
        p["metadata"]["uid"] != *my_uid
            && !is_scheduled(p)
            && p["status"]["nominatedNodeName"].as_str() == Some(NODE_NAME)
            && fit::priority(p) >= fit::priority(&current)
    }));

    let verdict = fit::node_fits(&current, &node)
        .and_then(|()| fit::ports_fit(&current, &others))
        .and_then(|()| fit::resources_fit(&current, &node, &reserved));
    if let Err(misfit) = verdict {
        // A preemption already under way finishes before another starts.
        let waiting_on_victims = current["status"]["nominatedNodeName"].as_str() == Some(NODE_NAME)
            && others
                .iter()
                .any(|p| !p["metadata"]["deletionTimestamp"].is_null());
        if matches!(misfit, Misfit::Resources(_))
            && !waiting_on_victims
            && let Some(victims) = fit::victims(&current, &node, &others)
        {
            preempt(store, recorder, &current, &victims);
            current["status"]["nominatedNodeName"] = serde_json::json!(NODE_NAME);
        }
        mark_unschedulable(store, recorder, &resource_ref, current, &misfit.message());
        return;
    }

    if !current["status"].is_object() {
        current["status"] = serde_json::json!({});
    }
    set_condition(&mut current, "PodScheduled", "True", None, None);
    if let Some(spec) = current.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("nodeName".to_string(), serde_json::json!(NODE_NAME));
    }
    if let Some(status) = current.get_mut("status").and_then(|v| v.as_object_mut()) {
        status.remove("nominatedNodeName");
    }

    match store.update(&resource_ref, &current) {
        Ok(_) => {
            tracing::info!("scheduled pod '{name}' to node '{NODE_NAME}'");
            if let Some(subject) = ObjectRef::of(&current) {
                recorder.event(
                    &subject,
                    NORMAL,
                    "Scheduled",
                    &format!(
                        "Successfully assigned {}/{name} to {NODE_NAME}",
                        namespace.unwrap_or("default")
                    ),
                );
            }
        }
        Err(e) => tracing::debug!("scheduler update conflict for '{name}': {e}"),
    }
}

/// PriorityClass values by name.
fn priority_classes(store: &Store) -> rustc_hash::FxHashMap<String, (i64, Option<String>)> {
    let gvr = GroupVersionResource::new("scheduling.k8s.io", "v1", "priorityclasses");
    store
        .list(&gvr, None, None, None, None, None)
        .map(|r| r.items)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| {
            let name = c["metadata"]["name"].as_str()?.to_string();
            let value = c["value"].as_i64().unwrap_or(0);
            let policy = c["preemptionPolicy"].as_str().map(String::from);
            Some((name, (value, policy)))
        })
        .collect()
}

/// `spec.priority` (and preemptionPolicy) from the pod's class when unset.
fn fill_priority(
    classes: &rustc_hash::FxHashMap<String, (i64, Option<String>)>,
    pod: &mut serde_json::Value,
) {
    if !pod["spec"]["priority"].is_null() {
        return;
    }
    let Some((value, policy)) = pod["spec"]["priorityClassName"]
        .as_str()
        .and_then(|name| classes.get(name))
        .cloned()
    else {
        return;
    };
    pod["spec"]["priority"] = serde_json::json!(value);
    if pod["spec"]["preemptionPolicy"].is_null()
        && let Some(policy) = policy
    {
        pod["spec"]["preemptionPolicy"] = serde_json::json!(policy);
    }
}

/// A pod this scheduler places: its schedulerName (or none), no scheduling
/// gates, not being deleted.
fn ours(pod: &serde_json::Value) -> bool {
    let spec = &pod["spec"];
    spec["schedulerName"]
        .as_str()
        .is_none_or(|s| s.is_empty() || s == SCHEDULER_NAME)
        && spec["schedulingGates"]
            .as_array()
            .is_none_or(|g| g.is_empty())
        && pod["metadata"]["deletionTimestamp"].is_null()
}

/// `status.conditions[type]` set, its transition time kept unless the
/// status changes.
fn set_condition(
    pod: &mut serde_json::Value,
    type_: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
) {
    if !pod["status"].is_object() {
        pod["status"] = serde_json::json!({});
    }
    let mut conditions = pod["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let since = conditions
        .iter()
        .find(|c| c["type"] == type_ && c["status"] == status)
        .map(|c| c["lastTransitionTime"].clone())
        .filter(|t| !t.is_null())
        .unwrap_or_else(|| serde_json::json!(Time(chrono::Utc::now())));
    conditions.retain(|c| c["type"] != type_);
    let mut condition = serde_json::to_value(PodCondition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.map(String::from),
        message: message.map(String::from),
        ..Default::default()
    })
    .unwrap_or_default();
    condition["lastTransitionTime"] = since;
    if condition.get("lastProbeTime").is_some_and(|v| v.is_null())
        && let Some(obj) = condition.as_object_mut()
    {
        obj.remove("lastProbeTime");
    }
    conditions.push(condition);
    pod["status"]["conditions"] = serde_json::Value::Array(conditions);
}

/// PodScheduled False, reason Unschedulable, with a FailedScheduling event;
/// written only when the verdict changes.
fn mark_unschedulable(
    store: &Store,
    recorder: &Recorder,
    rref: &ResourceRef,
    mut pod: serde_json::Value,
    message: &str,
) {
    let before = pod.clone();
    let unchanged = pod["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|c| {
            c["type"] == "PodScheduled"
                && c["status"] == "False"
                && c["reason"] == "Unschedulable"
                && c["message"] == message
        });
    if !unchanged {
        set_condition(
            &mut pod,
            "PodScheduled",
            "False",
            Some("Unschedulable"),
            Some(message),
        );
    }
    if pod == before {
        return;
    }
    let name = rref.name;
    match store.update(rref, &pod) {
        Ok(_) => {
            tracing::info!("pod '{name}' unschedulable: {message}");
            if !unchanged && let Some(subject) = ObjectRef::of(&pod) {
                recorder.event(&subject, WARNING, "FailedScheduling", message);
            }
        }
        Err(e) => tracing::debug!("scheduler update conflict for '{name}': {e}"),
    }
}

/// Evict `victims` for `preemptor`: each gets a DisruptionTarget condition
/// and is deleted (marked terminating while finalizers hold it).
fn preempt(
    store: &Store,
    recorder: &Recorder,
    preemptor: &serde_json::Value,
    victims: &[&serde_json::Value],
) {
    let gvr = GroupVersionResource::pods();
    let preemptor_uid = preemptor["metadata"]["uid"].as_str().unwrap_or("");
    let preemptor_ns = preemptor["metadata"]["namespace"]
        .as_str()
        .unwrap_or("default");
    let preemptor_name = preemptor["metadata"]["name"].as_str().unwrap_or("");
    for victim in victims {
        let (Some(name), namespace) = (
            victim["metadata"]["name"].as_str(),
            victim["metadata"]["namespace"].as_str(),
        ) else {
            continue;
        };
        let rref = ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        };
        let Ok(Some(mut live)) = store.get(&rref) else {
            continue;
        };
        set_condition(
            &mut live,
            "DisruptionTarget",
            "True",
            Some("PreemptionByScheduler"),
            Some(&format!(
                "{SCHEDULER_NAME}: preempting to accommodate a higher priority pod"
            )),
        );
        let has_finalizers = live["metadata"]["finalizers"]
            .as_array()
            .is_some_and(|f| !f.is_empty());
        let grace = live["spec"]["terminationGracePeriodSeconds"]
            .as_i64()
            .unwrap_or(30);
        live["metadata"]["deletionTimestamp"] = serde_json::json!(Time(chrono::Utc::now()));
        live["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(grace);
        if let Err(e) = store.update(&rref, &live) {
            tracing::warn!("preempt '{name}': {e}");
            continue;
        }
        if let Some(subject) = ObjectRef::of(&live) {
            recorder.event(
                &subject,
                NORMAL,
                "Preempted",
                &format!("Preempted by pod {preemptor_uid} on node {NODE_NAME}"),
            );
        }
        tracing::info!("preempted pod '{name}' for '{preemptor_ns}/{preemptor_name}'");
        if !has_finalizers {
            let _ = store.delete(&rref);
        }
    }
}

fn read_memtotal_ki() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = contents.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb_str = line.split_whitespace().nth(1)?;
    kb_str.parse().ok()
}
