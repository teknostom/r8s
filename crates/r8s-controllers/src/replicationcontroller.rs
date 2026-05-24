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
use r8s_types::{GroupVersionResource, ObjectMeta, OwnerReference, Pod, PodTemplateSpec};
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
                        let pod: Pod = match serde_json::from_value(event.object) {
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

    let pod_gvr = GroupVersionResource::pods();
    let owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, rc_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, &rc_uid))
        .collect();
    let current_count = owned.len() as u64;

    if current_count < desired {
        let to_create = desired - current_count;
        for _ in 0..to_create {
            create_pod(store, &rc_name, &rc_uid, rc_ns, &template)?;
        }
        tracing::info!(
            "rc '{rc_name}': created {to_create} pods ({current_count} -> {desired})"
        );
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
        tracing::info!(
            "rc '{rc_name}': deleted {to_delete} pods ({current_count} -> {desired})"
        );
    }

    // Recompute status with the final pod set.
    let final_pods: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, rc_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, &rc_uid))
        .collect();
    let replicas = final_pods.len() as i32;
    let ready = final_pods
        .iter()
        .filter(|pod| {
            pod.status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
                .is_some_and(|c| c.status == "True")
        })
        .count() as i32;
    update_rc_status(store, &rc_name, rc_ns, replicas, ready)?;

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
            owner_references: Some(vec![OwnerReference {
                api_version: "v1".into(),
                kind: "ReplicationController".into(),
                name: rc_name.into(),
                uid: rc_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
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
    crate::pod_admission::inject_sa_token(store, &mut pod_value);
    store.create(rref, &pod_value)?;
    Ok(())
}

fn update_rc_status(
    store: &Store,
    rc_name: &str,
    rc_ns: Option<&str>,
    replicas: i32,
    ready: i32,
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
    let new_status = serde_json::json!({
        "replicas": replicas,
        "readyReplicas": ready,
        "availableReplicas": ready,
    });
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
