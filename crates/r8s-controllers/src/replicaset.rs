use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, ObjectMeta, OwnerReference, Pod, PodTemplateSpec, ReplicaSetStatus,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("replicaset controller started");
    let gvr = GroupVersionResource::replica_sets();

    reconcile_all(&store);

    let mut rs_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("replicaset controller shutting down");
                return Ok(());
            }
            event = rs_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_rs(&store, &event.object) {
                            tracing::warn!("rs reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let pod: Pod = match serde_json::from_value(event.object) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        if let Some(owner) = crate::find_owner(&pod.metadata, "ReplicaSet") {
                            let resource_ref = ResourceRef {
                                gvr: &GroupVersionResource::replica_sets(),
                                namespace: pod.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(rs)) = store.get(&resource_ref) {
                                let _ = reconcile_rs(&store, &rs);
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
    let gvr = GroupVersionResource::replica_sets();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("rs controller list error: {e}");
            return;
        }
    };
    for rs in &result.items {
        if let Err(e) = reconcile_rs(store, rs) {
            tracing::warn!("rs reconcile error: {e}");
        }
    }
}

fn reconcile_rs(store: &Store, rs_value: &serde_json::Value) -> anyhow::Result<()> {
    let rs: r8s_types::ReplicaSet = serde_json::from_value(rs_value.clone())?;
    let rs_name = rs
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("RS has no name"))?;
    let rs_ns = rs.metadata.namespace.as_deref();
    let rs_uid = rs.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::replica_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: rs_ns,
        name: rs_name,
    };
    let current: r8s_types::ReplicaSet = match store.get_as(&resource_ref)? {
        Some(rs) => rs,
        None => return Ok(()),
    };
    let current_uid = current.metadata.uid.as_deref().unwrap_or(rs_uid);
    let current_spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("RS has no spec"))?;

    let desired = current_spec.replicas.unwrap_or(1) as u64;
    let template = current_spec
        .template
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("RS has no template"))?;

    let pod_gvr = GroupVersionResource::pods();
    let owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, rs_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();
    let current_count = owned.len() as u64;

    if current_count < desired {
        let to_create = desired - current_count;
        for _ in 0..to_create {
            create_pod(store, rs_name, current_uid, rs_ns, template)?;
        }
        tracing::info!("rs '{rs_name}': created {to_create} pods ({current_count} -> {desired})");
    } else if current_count > desired {
        let to_delete = current_count - desired;
        for pod in owned.iter().rev().take(to_delete as usize) {
            if let Some(pod_name) = pod.metadata.name.as_deref() {
                let pod_ref = ResourceRef {
                    gvr: &pod_gvr,
                    namespace: rs_ns,
                    name: pod_name,
                };
                store.delete(&pod_ref)?;
            }
        }
        tracing::info!("rs '{rs_name}': deleted {to_delete} pods ({current_count} -> {desired})");
    }

    let final_count = store
        .list_as::<Pod>(&pod_gvr, rs_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .count() as i32;
    update_rs_status(store, rs_name, rs_ns, final_count)?;

    Ok(())
}

fn create_pod(
    store: &Store,
    rs_name: &str,
    rs_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{}-{}", rs_name, crate::random_suffix());
    let labels = template.metadata.as_ref().and_then(|m| m.labels.clone());
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels,
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".into(),
                kind: "ReplicaSet".into(),
                name: rs_name.into(),
                uid: rs_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: template.spec.clone(),
        status: None,
    };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &pod_name,
    };
    store.create(resource_ref, &serde_json::to_value(&pod)?)?;
    Ok(())
}

fn update_rs_status(
    store: &Store,
    rs_name: &str,
    rs_ns: Option<&str>,
    replica_count: i32,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::replica_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: rs_ns,
        name: rs_name,
    };

    let current = match store.get(&resource_ref)? {
        Some(rs) => rs,
        None => return Ok(()),
    };

    let status = ReplicaSetStatus {
        replicas: replica_count,
        ready_replicas: Some(replica_count),
        available_replicas: Some(replica_count),
        ..Default::default()
    };
    let new_status_val = serde_json::to_value(&status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    let mut updated = current;
    updated["status"] = new_status_val;

    match store.update(&resource_ref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("rs status update conflict for '{rs_name}': {e}");
            Ok(())
        }
    }
}
