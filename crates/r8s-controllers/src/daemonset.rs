use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    DaemonSetStatus, GroupVersionResource, ObjectMeta, OwnerReference, Pod, PodTemplateSpec,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("daemonset controller started");
    let gvr = GroupVersionResource::daemon_sets();

    reconcile_all(&store);

    let mut ds_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("daemonset controller shutting down");
                return Ok(());
            }
            event = ds_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_ds(&store, &event.object) {
                            tracing::warn!("ds reconcile error: {e}");
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
                        if let Some(owner) = crate::find_owner(&pod.metadata, "DaemonSet") {
                            let resource_ref = ResourceRef {
                                gvr: &GroupVersionResource::daemon_sets(),
                                namespace: pod.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(ds)) = store.get(&resource_ref) {
                                let _ = reconcile_ds(&store, &ds);
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
    let gvr = GroupVersionResource::daemon_sets();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("ds controller list error: {e}");
            return;
        }
    };
    for ds in &result.items {
        if let Err(e) = reconcile_ds(store, ds) {
            tracing::warn!("ds reconcile error: {e}");
        }
    }
}

fn reconcile_ds(store: &Store, ds_value: &serde_json::Value) -> anyhow::Result<()> {
    let ds: r8s_types::DaemonSet = serde_json::from_value(ds_value.clone())?;
    let ds_name = ds
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("DaemonSet has no name"))?;
    let ds_ns = ds.metadata.namespace.as_deref();
    let ds_uid = ds.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::daemon_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: ds_ns,
        name: ds_name,
    };
    let current: r8s_types::DaemonSet = match store.get_as(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    let current_uid = current.metadata.uid.as_deref().unwrap_or(ds_uid);
    let current_spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("DaemonSet has no spec"))?;
    let template = &current_spec.template;

    let pod_gvr = GroupVersionResource::pods();
    let owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, ds_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();

    // Single node — desired is always 1
    let current_count = owned.len();

    if current_count == 0 {
        create_pod(store, ds_name, current_uid, ds_ns, template)?;
        tracing::info!("ds '{ds_name}': created pod (0 -> 1)");
    } else if current_count > 1 {
        for pod in owned.iter().skip(1) {
            if let Some(pod_name) = pod.metadata.name.as_deref() {
                let pod_ref = ResourceRef {
                    gvr: &pod_gvr,
                    namespace: ds_ns,
                    name: pod_name,
                };
                store.delete(&pod_ref)?;
            }
        }
        tracing::info!("ds '{ds_name}': deleted {} extra pods", current_count - 1);
    }

    let final_owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, ds_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();
    let current_num = final_owned.len() as i32;
    let ready = final_owned
        .iter()
        .filter(|p| {
            p.status
                .as_ref()
                .is_some_and(|s| s.phase.as_deref() == Some("Running"))
        })
        .count() as i32;
    update_ds_status(store, ds_name, ds_ns, current_num, ready)?;

    Ok(())
}

fn create_pod(
    store: &Store,
    ds_name: &str,
    ds_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{ds_name}-{}", crate::random_suffix());
    let labels = template.metadata.as_ref().and_then(|m| m.labels.clone());
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels,
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".into(),
                kind: "DaemonSet".into(),
                name: ds_name.into(),
                uid: ds_uid.into(),
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

fn update_ds_status(
    store: &Store,
    ds_name: &str,
    ds_ns: Option<&str>,
    current_number: i32,
    ready: i32,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::daemon_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: ds_ns,
        name: ds_name,
    };

    let current = match store.get(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };

    let new_status = DaemonSetStatus {
        desired_number_scheduled: 1,
        current_number_scheduled: current_number,
        number_ready: ready,
        number_available: Some(ready),
        number_misscheduled: 0,
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
            tracing::debug!("ds status update conflict for '{ds_name}': {e}");
            Ok(())
        }
    }
}
