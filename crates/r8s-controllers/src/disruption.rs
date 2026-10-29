//! PodDisruptionBudget controller: keeps `status` (healthy, desired, expected,
//! disruptionsAllowed) current for the pods a budget selects. The eviction
//! endpoint consumes `disruptionsAllowed` and records `disruptedPods`.

use std::collections::BTreeMap;

use r8s_store::{Store, StoreError, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    Condition, GroupVersionResource, IntOrString, Pod, PodDisruptionBudget,
    PodDisruptionBudgetStatus, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{pod_ready, pod_terminating, selector_matches};

/// How long an evicted pod stays counted as disrupted while it terminates.
const DELETION_TIMEOUT_SECS: i64 = 2 * 60;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("disruption controller started");
    let gvr = GroupVersionResource::pod_disruption_budgets();

    reconcile_all(&store, None);

    let mut pdb_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("disruption controller shutting down");
                return Ok(());
            }
            event = pdb_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_pdb(&store, &event.object) {
                            tracing::warn!("pdb reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store, None),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) => {
                        let ns = event
                            .object
                            .get("metadata")
                            .and_then(|m| m.get("namespace"))
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        reconcile_all(&store, ns.as_deref());
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store, None),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn reconcile_all(store: &Store, namespace: Option<&str>) {
    let gvr = GroupVersionResource::pod_disruption_budgets();
    let result = match store.list(&gvr, namespace, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("disruption controller list error: {e}");
            return;
        }
    };
    for pdb in &result.items {
        if let Err(e) = reconcile_pdb(store, pdb) {
            tracing::warn!("pdb reconcile error: {e}");
        }
    }
}

fn reconcile_pdb(store: &Store, pdb_value: &serde_json::Value) -> anyhow::Result<()> {
    let pdb: PodDisruptionBudget = serde_json::from_value(pdb_value.clone())?;
    let name = pdb
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("PodDisruptionBudget has no name"))?;
    let namespace = pdb.metadata.namespace.as_deref();
    let gvr = GroupVersionResource::pod_disruption_budgets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let Some(current) = store.get_as::<PodDisruptionBudget>(&rref)? else {
        return Ok(());
    };
    let spec = current.spec.clone().unwrap_or_default();
    let previous = current.status.clone().unwrap_or_default();
    let generation = current.metadata.generation;

    let pods: Vec<Pod> = store
        .list_as::<Pod>(&GroupVersionResource::pods(), namespace)?
        .into_iter()
        .filter(|p| {
            spec.selector
                .as_ref()
                .is_some_and(|s| selector_matches(s, p.metadata.labels.as_ref()))
        })
        .collect();

    let now = chrono::Utc::now();
    let disrupted: BTreeMap<String, Time> = previous
        .disrupted_pods
        .iter()
        .flatten()
        .filter(|(pod_name, since)| {
            pods.iter().any(|p| {
                p.metadata.name.as_deref() == Some(pod_name.as_str()) && !pod_terminating(p)
            }) && since.0 + chrono::Duration::seconds(DELETION_TIMEOUT_SECS) > now
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let current_healthy = pods
        .iter()
        .filter(|p| {
            !pod_terminating(p)
                && pod_ready(p)
                && !p
                    .metadata
                    .name
                    .as_deref()
                    .is_some_and(|n| disrupted.contains_key(n))
        })
        .count() as i32;

    let mut status = PodDisruptionBudgetStatus {
        current_healthy,
        observed_generation: generation,
        disrupted_pods: (!disrupted.is_empty()).then_some(disrupted),
        ..Default::default()
    };
    let (allowed_reason, allowed) = match expected_and_desired(store, &spec, &pods, namespace) {
        Ok((expected, desired)) => {
            status.expected_pods = expected;
            status.desired_healthy = desired;
            let allowed = if expected <= 0 {
                0
            } else {
                (current_healthy - desired).max(0)
            };
            status.disruptions_allowed = allowed;
            (
                if allowed > 0 {
                    "SufficientPods"
                } else {
                    "InsufficientPods"
                },
                allowed > 0,
            )
        }
        Err(e) => {
            tracing::warn!("pdb '{name}': {e}");
            ("SyncFailed", false)
        }
    };
    let unchanged = previous
        .conditions
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|c| c.type_ == "DisruptionAllowed")
        .filter(|c| c.status == if allowed { "True" } else { "False" })
        .map(|c| c.last_transition_time.clone());
    status.conditions = Some(vec![Condition {
        type_: "DisruptionAllowed".into(),
        status: if allowed { "True" } else { "False" }.into(),
        reason: allowed_reason.into(),
        message: String::new(),
        observed_generation: generation,
        last_transition_time: unchanged.unwrap_or(Time(now)),
    }]);

    write_status(store, &rref, status)
}

/// Upstream `getExpectedPodCount`: an integer `minAvailable` measures the
/// selected pods themselves; a percentage or `maxUnavailable` needs the scale
/// of the controllers owning them.
fn expected_and_desired(
    store: &Store,
    spec: &k8s_openapi::api::policy::v1::PodDisruptionBudgetSpec,
    pods: &[Pod],
    namespace: Option<&str>,
) -> anyhow::Result<(i32, i32)> {
    if let Some(max_unavailable) = spec.max_unavailable.as_ref() {
        let expected = expected_scale(store, pods, namespace)?;
        let unavailable = scaled(max_unavailable, expected, true);
        return Ok((expected, (expected - unavailable).max(0)));
    }
    match spec.min_available.as_ref() {
        Some(IntOrString::Int(n)) => Ok((pods.len() as i32, *n)),
        Some(percent @ IntOrString::String(_)) => {
            let expected = expected_scale(store, pods, namespace)?;
            Ok((expected, scaled(percent, expected, true)))
        }
        None => Ok((pods.len() as i32, 0)),
    }
}

fn scaled(v: &IntOrString, total: i32, round_up: bool) -> i32 {
    match v {
        IntOrString::Int(n) => *n,
        IntOrString::String(s) => {
            let percent: f64 = s.trim_end_matches('%').trim().parse().unwrap_or(0.0);
            let raw = percent / 100.0 * f64::from(total);
            if round_up {
                raw.ceil() as i32
            } else {
                raw.floor() as i32
            }
        }
    }
}

/// Sum of the replica counts of the controllers owning `pods`; an unmanaged pod
/// is an error, as upstream.
fn expected_scale(store: &Store, pods: &[Pod], namespace: Option<&str>) -> anyhow::Result<i32> {
    let mut scales: BTreeMap<String, i32> = BTreeMap::new();
    for pod in pods {
        let pod_name = pod.metadata.name.as_deref().unwrap_or("");
        let Some(owner) = pod
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|r| r.controller == Some(true))
        else {
            anyhow::bail!("found no controllers for pod \"{pod_name}\"");
        };
        if scales.contains_key(&owner.uid) {
            continue;
        }
        let Some((uid, scale)) = controller_scale(store, namespace, &owner.kind, &owner.name)?
        else {
            anyhow::bail!("found no controllers for pod \"{pod_name}\"");
        };
        scales.insert(uid, scale);
    }
    Ok(scales.values().sum())
}

/// `(uid, spec.replicas)` of a scalable controller; a ReplicaSet owned by a
/// Deployment reports the Deployment's.
fn controller_scale(
    store: &Store,
    namespace: Option<&str>,
    kind: &str,
    name: &str,
) -> anyhow::Result<Option<(String, i32)>> {
    let gvr = match kind {
        "ReplicaSet" => GroupVersionResource::replica_sets(),
        "ReplicationController" => GroupVersionResource::replication_controllers(),
        "StatefulSet" => GroupVersionResource::stateful_sets(),
        "Deployment" => GroupVersionResource::deployments(),
        _ => return Ok(None),
    };
    let Some(obj) = store.get(&ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    })?
    else {
        return Ok(None);
    };
    if kind == "ReplicaSet"
        && let Some(deploy) = obj
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|r| r.as_array())
            .and_then(|refs| {
                refs.iter()
                    .find(|r| r["controller"] == true && r["kind"] == "Deployment")
            })
        && let Some(name) = deploy.get("name").and_then(|n| n.as_str())
        && let Some(scale) = controller_scale(store, namespace, "Deployment", name)?
    {
        return Ok(Some(scale));
    }
    let uid = obj["metadata"]["uid"].as_str().unwrap_or("").to_string();
    let replicas = obj["spec"]["replicas"].as_i64().unwrap_or(1) as i32;
    Ok(Some((uid, replicas)))
}

fn write_status(
    store: &Store,
    rref: &ResourceRef,
    status: PodDisruptionBudgetStatus,
) -> anyhow::Result<()> {
    let Some(mut current) = store.get(rref)? else {
        return Ok(());
    };
    let new_status = serde_json::to_value(&status)?;
    if current.get("status") == Some(&new_status) {
        return Ok(());
    }
    current["status"] = new_status;
    match store.update(rref, &current) {
        Ok(_) | Err(StoreError::Conflict { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn rref<'a>(gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: Some("default"),
            name,
        }
    }

    fn create_pdb(store: &Store, spec: Value) -> Value {
        store
            .create(
                rref(&GroupVersionResource::pod_disruption_budgets(), "foo"),
                &json!({
                    "apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                    "metadata": {"name": "foo", "namespace": "default", "generation": 1},
                    "spec": spec,
                }),
            )
            .unwrap()
    }

    fn create_pod(store: &Store, name: &str, ready: bool, owner: Option<(&str, &str, &str)>) {
        let mut meta = json!({"name": name, "namespace": "default", "labels": {"foo": "bar"}});
        if let Some((kind, oname, uid)) = owner {
            meta["ownerReferences"] = json!([{"apiVersion": "apps/v1", "kind": kind, "name": oname, "uid": uid, "controller": true}]);
        }
        store
            .create(
                rref(&GroupVersionResource::pods(), name),
                &json!({
                    "apiVersion": "v1", "kind": "Pod", "metadata": meta,
                    "spec": {"containers": []},
                    "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": if ready {"True"} else {"False"}}]}
                }),
            )
            .unwrap();
    }

    fn status(store: &Store) -> PodDisruptionBudgetStatus {
        store
            .get_as::<PodDisruptionBudget>(&rref(
                &GroupVersionResource::pod_disruption_budgets(),
                "foo",
            ))
            .unwrap()
            .unwrap()
            .status
            .unwrap()
    }

    #[test]
    fn integer_min_available_counts_selected_pods() {
        let (_d, store) = store();
        let pdb = create_pdb(
            &store,
            json!({"selector": {"matchLabels": {"foo": "bar"}}, "minAvailable": 1}),
        );
        for i in 0..3 {
            create_pod(&store, &format!("pod-{i}"), true, None);
        }
        reconcile_pdb(&store, &pdb).unwrap();
        let s = status(&store);
        assert_eq!(
            (s.expected_pods, s.desired_healthy, s.current_healthy),
            (3, 1, 3)
        );
        assert_eq!(s.disruptions_allowed, 2);
        assert_eq!(s.observed_generation, Some(1));
        let c = &s.conditions.unwrap()[0];
        assert_eq!(
            (c.type_.as_str(), c.status.as_str(), c.reason.as_str()),
            ("DisruptionAllowed", "True", "SufficientPods")
        );
    }

    #[test]
    fn max_unavailable_uses_controller_scale_and_unmanaged_pods_fail_sync() {
        let (_d, store) = store();
        let pdb = create_pdb(
            &store,
            json!({"selector": {"matchLabels": {"foo": "bar"}}, "maxUnavailable": 0}),
        );
        let rs = store
            .create(
                rref(&GroupVersionResource::replica_sets(), "rs"),
                &json!({"apiVersion": "apps/v1", "kind": "ReplicaSet",
                        "metadata": {"name": "rs", "namespace": "default"},
                        "spec": {"replicas": 3, "selector": {"matchLabels": {"foo": "bar"}}}}),
            )
            .unwrap();
        let uid = rs["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod(&store, "a", true, Some(("ReplicaSet", "rs", &uid)));
        create_pod(&store, "b", false, Some(("ReplicaSet", "rs", &uid)));
        reconcile_pdb(&store, &pdb).unwrap();
        let s = status(&store);
        assert_eq!(
            (
                s.expected_pods,
                s.desired_healthy,
                s.current_healthy,
                s.disruptions_allowed
            ),
            (3, 3, 1, 0)
        );

        create_pod(&store, "orphan", true, None);
        reconcile_pdb(&store, &pdb).unwrap();
        let s = status(&store);
        assert_eq!(s.disruptions_allowed, 0);
        assert_eq!(s.conditions.unwrap()[0].reason, "SyncFailed");
    }

    #[test]
    fn disrupted_pods_are_not_healthy_until_gone() {
        let (_d, store) = store();
        create_pdb(
            &store,
            json!({"selector": {"matchLabels": {"foo": "bar"}}, "minAvailable": 1}),
        );
        create_pod(&store, "a", true, None);
        create_pod(&store, "b", true, None);
        let gvr = GroupVersionResource::pod_disruption_budgets();
        let mut v = store.get(&rref(&gvr, "foo")).unwrap().unwrap();
        v["status"] = json!({"disruptedPods": {"a": Time(chrono::Utc::now())}});
        store.update(&rref(&gvr, "foo"), &v).unwrap();
        reconcile_pdb(&store, &v).unwrap();
        let s = status(&store);
        assert_eq!(s.current_healthy, 1);
        assert_eq!(s.disruptions_allowed, 0);
        assert!(s.disrupted_pods.unwrap().contains_key("a"));
    }
}
