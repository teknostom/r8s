use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, ObjectMeta, OwnerReference, Pod, PodTemplateSpec, StatefulSetStatus,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("statefulset controller started");
    let gvr = GroupVersionResource::stateful_sets();

    reconcile_all(&store);

    let mut sts_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("statefulset controller shutting down");
                return Ok(());
            }
            event = sts_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_sts(&store, &event.object) {
                            tracing::warn!("sts reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted | WatchEventType::Modified) => {
                        let pod: Pod = match serde_json::from_value(event.object) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        if let Some(owner) = crate::find_owner(&pod.metadata, "StatefulSet") {
                            let resource_ref = ResourceRef {
                                gvr: &GroupVersionResource::stateful_sets(),
                                namespace: pod.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(sts)) = store.get(&resource_ref) {
                                let _ = reconcile_sts(&store, &sts);
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
    let gvr = GroupVersionResource::stateful_sets();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("sts controller list error: {e}");
            return;
        }
    };
    for sts in &result.items {
        if let Err(e) = reconcile_sts(store, sts) {
            tracing::warn!("sts reconcile error: {e}");
        }
    }
}

fn reconcile_sts(store: &Store, sts_value: &serde_json::Value) -> anyhow::Result<()> {
    let sts: r8s_types::StatefulSet = serde_json::from_value(sts_value.clone())?;
    let sts_name = sts
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("StatefulSet has no name"))?;
    let sts_ns = sts.metadata.namespace.as_deref();
    let sts_uid = sts.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::stateful_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: sts_ns,
        name: sts_name,
    };
    let current: r8s_types::StatefulSet = match store.get_as(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    let current_uid = current.metadata.uid.as_deref().unwrap_or(sts_uid);
    let current_spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("StatefulSet has no spec"))?;

    let desired = current_spec.replicas.unwrap_or(1) as u64;
    let template = &current_spec.template;

    let pod_gvr = GroupVersionResource::pods();
    let mut owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, sts_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();

    owned.sort_by_key(pod_ordinal);

    let current_count = owned.len() as u64;

    if current_count < desired {
        let existing_ordinals: rustc_hash::FxHashSet<u64> = owned.iter().map(pod_ordinal).collect();
        let mut created = 0u64;
        let mut ordinal = 0u64;
        while created < desired - current_count {
            if !existing_ordinals.contains(&ordinal) {
                create_pod(store, sts_name, current_uid, sts_ns, template, ordinal)?;
                created += 1;
            }
            ordinal += 1;
        }
        tracing::info!("sts '{sts_name}': created {created} pods ({current_count} -> {desired})");
    } else if current_count > desired {
        let to_delete = current_count - desired;
        for pod in owned.iter().rev().take(to_delete as usize) {
            if let Some(pod_name) = pod.metadata.name.as_deref() {
                let pod_ref = ResourceRef {
                    gvr: &pod_gvr,
                    namespace: sts_ns,
                    name: pod_name,
                };
                store.delete(&pod_ref)?;
            }
        }
        tracing::info!("sts '{sts_name}': deleted {to_delete} pods ({current_count} -> {desired})");
    }

    let final_owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, sts_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();
    let total = final_owned.len() as i32;
    let ready = final_owned
        .iter()
        .filter(|p| {
            p.status
                .as_ref()
                .is_some_and(|s| s.phase.as_deref() == Some("Running"))
        })
        .count() as i32;
    update_sts_status(store, sts_name, sts_ns, total, ready)?;

    Ok(())
}

fn pod_ordinal(pod: &Pod) -> u64 {
    pod.metadata
        .name
        .as_deref()
        .and_then(|n| n.rsplit('-').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX)
}

fn create_pod(
    store: &Store,
    sts_name: &str,
    sts_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
    ordinal: u64,
) -> anyhow::Result<()> {
    let pod_name = format!("{sts_name}-{ordinal}");
    let mut labels = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert(
        "statefulset.kubernetes.io/pod-name".into(),
        pod_name.clone(),
    );

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels: Some(labels),
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".into(),
                kind: "StatefulSet".into(),
                name: sts_name.into(),
                uid: sts_uid.into(),
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

fn update_sts_status(
    store: &Store,
    sts_name: &str,
    sts_ns: Option<&str>,
    total: i32,
    ready: i32,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::stateful_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: sts_ns,
        name: sts_name,
    };

    let current = match store.get(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };

    let new_status = StatefulSetStatus {
        replicas: total,
        ready_replicas: Some(ready),
        available_replicas: Some(ready),
        ..Default::default()
    };
    let new_status_val = serde_json::to_value(&new_status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    let mut updated = current;
    if let Some(obj) = updated.as_object_mut() {
        obj.insert("status".to_string(), new_status_val);
    }

    match store.update(&resource_ref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("sts status update conflict for '{sts_name}': {e}");
            Ok(())
        }
    }
}
