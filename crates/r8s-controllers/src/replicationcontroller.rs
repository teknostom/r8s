//! ReplicationController controller. Structurally identical to the
//! ReplicaSet controller (compare alongside `replicaset.rs`); the difference
//! is the RC spec shape — `spec.selector` is a flat `map[string]string`
//! instead of a `LabelSelector{matchLabels}` — and owner references use the
//! `core/v1 ReplicationController` kind.
//!
//! Conformance tests (sig-api-machinery / Garbage collector / ResourceQuota
//! life-of-rc) exercise create/scale/delete of RCs, which is all this
//! controller needs to drive correctly. Rolling updates aren't an RC concept,
//! so there's no template-hash bookkeeping.

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, LabelSelector, ObjectMeta, Pod, PodTemplateSpec};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("replicationcontroller controller started");
    let gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
    let pod_gvr = GroupVersionResource::pods();

    reconcile_all(&store);

    let mut rc_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&pod_gvr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("rc controller shutting down");
                return Ok(());
            }
            event = rc_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_rc(&store, &event.object) {
                            tracing::warn!("rc reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Added) => {
                        let pod: Pod = match serde_json::from_value(event.object.as_ref().clone()) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        if let Some(owner) =
                            crate::find_owner(&pod.metadata, "ReplicationController")
                        {
                            let rref = ResourceRef {
                                gvr: &gvr,
                                namespace: pod.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(rc)) = store.get(&rref) {
                                let _ = reconcile_rc(&store, &rc);
                            }
                        } else {
                            // An RC selector is a flat label map.
                            for rc in crate::orphan_candidates(&store, &gvr, &pod, |rc| {
                                let labels = rc.get("spec")?.get("selector")?.clone();
                                Some(LabelSelector {
                                    match_labels: serde_json::from_value(labels).ok(),
                                    match_expressions: None,
                                })
                            }) {
                                let _ = reconcile_rc(&store, &rc);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn reconcile_all(store: &Store) {
    let gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("rc controller list error: {e}");
            return;
        }
    };
    for rc in &result.items {
        if let Err(e) = reconcile_rc(store, rc) {
            tracing::warn!("rc reconcile error: {e}");
        }
    }
}

fn reconcile_rc(store: &Store, rc_value: &Value) -> anyhow::Result<()> {
    let rc_name = rc_value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("RC has no name"))?
        .to_string();
    let rc_ns = rc_value
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());
    let rc_uid = rc_value
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if rc_uid.is_empty() {
        // Ownership is matched by uid; an empty uid would adopt unrelated pods.
        tracing::warn!("rc '{rc_name}': missing uid; skipping");
        return Ok(());
    }

    let gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: rc_ns,
        name: &rc_name,
    };
    let current = match store.get(&rref)? {
        Some(rc) => rc,
        None => return Ok(()),
    };
    // Don't create replacements while the RC is being deleted — the
    // foreground GC reconciler is draining pods.
    if current
        .get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .is_some_and(|v| !v.is_null())
    {
        return Ok(());
    }
    let current_spec = current
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("RC has no spec"))?;
    let desired = current_spec
        .get("replicas")
        .and_then(|v| v.as_i64())
        .unwrap_or(1)
        .max(0) as u64;

    let template_value = current_spec
        .get("template")
        .ok_or_else(|| anyhow::anyhow!("RC has no template"))?;
    let template: PodTemplateSpec = serde_json::from_value(template_value.clone())?;

    // An RC selector is a flat label map; when empty it defaults to the
    // template's labels, as the apiserver defaults it.
    let selector: std::collections::BTreeMap<String, String> = current_spec
        .get("selector")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .filter(|m: &std::collections::BTreeMap<String, String>| !m.is_empty())
        .or_else(|| template.metadata.as_ref().and_then(|m| m.labels.clone()))
        .unwrap_or_default();
    let pod_gvr = GroupVersionResource::pods();
    let owner = crate::owner_reference("v1", "ReplicationController", &rc_name, &rc_uid);
    let terminating = rc_value
        .get("metadata")
        .and_then(|m| m.get("deletionTimestamp"))
        .is_some();
    let owned = crate::claim_pods(store, rc_ns, &owner, terminating, |p| {
        !selector.is_empty()
            && selector
                .iter()
                .all(|(k, v)| p.metadata.labels.as_ref().and_then(|l| l.get(k)) == Some(v))
    })?;
    let current_count = owned.len() as u64;

    let mut create_failure: Option<String> = None;
    if current_count < desired {
        let to_create = desired - current_count;
        let mut created = 0;
        for _ in 0..to_create {
            match create_pod(store, &rc_name, &rc_uid, rc_ns, &template) {
                Ok(()) => created += 1,
                Err(e) => {
                    tracing::warn!("rc '{rc_name}': create pod: {e}");
                    create_failure = Some(format!("Error creating: {e}"));
                    break;
                }
            }
        }
        tracing::info!("rc '{rc_name}': created {created} pods ({current_count} -> {desired})");
    } else if current_count > desired {
        let to_delete = current_count - desired;
        for pod in owned.iter().rev().take(to_delete as usize) {
            if let Some(pod_name) = pod.metadata.name.as_deref() {
                let pod_ref = ResourceRef {
                    gvr: &pod_gvr,
                    namespace: rc_ns,
                    name: pod_name,
                };
                store.delete(&pod_ref)?;
            }
        }
        tracing::info!("rc '{rc_name}': deleted {to_delete} pods ({current_count} -> {desired})");
    }

    // Recompute status with the final pod set.
    let final_pods: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, rc_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, &rc_uid))
        .collect();
    let replicas = final_pods.len() as i32;
    let ready = final_pods.iter().filter(|p| crate::pod_ready(p)).count() as i32;
    update_rc_status(
        store,
        &rc_name,
        rc_ns,
        replicas,
        ready,
        create_failure.as_deref(),
    )?;

    Ok(())
}

fn create_pod(
    store: &Store,
    rc_name: &str,
    rc_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{rc_name}-{}", crate::random_suffix());
    let labels = template.metadata.as_ref().and_then(|m| m.labels.clone());
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels,
            owner_references: Some(vec![crate::owner_reference(
                "v1",
                "ReplicationController",
                rc_name,
                rc_uid,
            )]),
            ..Default::default()
        },
        spec: template.spec.clone(),
        status: None,
    };

    let gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &pod_name,
    };
    let mut pod_value = serde_json::to_value(&pod)?;
    crate::pod_admission::admit_pod(store, &mut pod_value);
    if let Some(ns) = namespace
        && let Err(msg) = crate::quota::check_admission(store, &gvr, ns, &pod_value)
    {
        anyhow::bail!("pods \"{pod_name}\" is forbidden: {msg}");
    }
    store.create(rref, &pod_value)?;
    Ok(())
}

/// Store the counts, `observedGeneration`, and a `ReplicaFailure` condition
/// while pod creation is being refused (cleared once it succeeds again).
fn update_rc_status(
    store: &Store,
    rc_name: &str,
    rc_ns: Option<&str>,
    replicas: i32,
    ready: i32,
    create_failure: Option<&str>,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: rc_ns,
        name: rc_name,
    };
    let current = match store.get(&rref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    let mut new_status = serde_json::json!({
        "replicas": replicas,
        "readyReplicas": ready,
        "availableReplicas": ready,
        "observedGeneration": current["metadata"]["generation"].as_i64().unwrap_or(0),
    });
    if let Some(message) = create_failure {
        new_status["conditions"] =
            serde_json::json!([crate::replica_failure_condition(&current, message)]);
    }
    if current.get("status") == Some(&new_status) {
        return Ok(());
    }
    let mut updated = current;
    if let Some(obj) = updated.as_object_mut() {
        obj.insert("status".to_string(), new_status);
    }
    match store.update(&rref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("rc status update conflict for '{rc_name}': {e}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn quota_refusal_surfaces_replica_failure_until_satisfied() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let quota_gvr = GroupVersionResource::resource_quotas();
        store
            .create(
                ResourceRef {
                    gvr: &quota_gvr,
                    namespace: Some("default"),
                    name: "q",
                },
                &json!({"apiVersion": "v1", "kind": "ResourceQuota",
                        "metadata": {"name": "q", "namespace": "default"},
                        "spec": {"hard": {"pods": "1"}}}),
            )
            .unwrap();
        let rc_gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
        let rref = ResourceRef {
            gvr: &rc_gvr,
            namespace: Some("default"),
            name: "rc",
        };
        let rc = store
            .create(
                ResourceRef {
                    gvr: &rc_gvr,
                    namespace: Some("default"),
                    name: "rc",
                },
                &json!({"apiVersion": "v1", "kind": "ReplicationController",
                        "metadata": {"name": "rc", "namespace": "default", "generation": 1},
                        "spec": {"replicas": 2, "selector": {"app": "x"},
                                 "template": {"metadata": {"labels": {"app": "x"}},
                                              "spec": {"containers": [{"name": "c", "image": "img"}]}}}}),
            )
            .unwrap();

        reconcile_rc(&store, &rc).unwrap();
        let after = store.get(&rref).unwrap().unwrap();
        assert_eq!(after["status"]["replicas"], 1);
        assert_eq!(after["status"]["observedGeneration"], 1);
        let cond = &after["status"]["conditions"][0];
        assert_eq!(cond["type"], "ReplicaFailure");
        assert_eq!(cond["reason"], "FailedCreate");
        assert!(cond["message"].as_str().unwrap().contains("exceeded quota"));

        // A reconcile that changes nothing must not write (it would trigger
        // itself through the RC watch forever).
        reconcile_rc(&store, &after).unwrap();
        let again = store.get(&rref).unwrap().unwrap();
        assert_eq!(
            again["metadata"]["resourceVersion"], after["metadata"]["resourceVersion"],
            "status: {} vs {}",
            after["status"], again["status"]
        );

        let mut scaled = after.clone();
        scaled["spec"]["replicas"] = json!(1);
        scaled["metadata"]["generation"] = json!(2);
        let scaled = store.update(&rref, &scaled).unwrap();
        reconcile_rc(&store, &scaled).unwrap();
        let after = store.get(&rref).unwrap().unwrap();
        assert_eq!(after["status"]["observedGeneration"], 2);
        assert!(
            after["status"].get("conditions").is_none(),
            "condition cleared"
        );
    }
}
