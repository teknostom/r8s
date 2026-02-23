use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, ObjectMeta, OwnerReference, Pod,
    replicaset::ReplicaSetStatus,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn rs_gvr() -> GroupVersionResource {
    GroupVersionResource::new("apps", "v1", "replicasets")
}

fn pods_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

fn random_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvxyz0123456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

fn is_owned_by(meta: &ObjectMeta, owner_uid: &str) -> bool {
    meta.owner_references.iter().any(|r| r.uid == owner_uid)
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("replicaset controller started");
    let gvr = rs_gvr();

    reconcile_all(&store);

    let mut rs_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&pods_gvr());

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
                    if let Some(owner) = pod.metadata.owner_references.iter().find(|r| r.kind == "ReplicaSet") {
                        let resource_ref = ResourceRef {
                            gvr: &rs_gvr(),
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
    let gvr = rs_gvr();
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

    let gvr = rs_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: rs_ns,
        name: rs_name,
    };
    let current_value = match store.get(&resource_ref)? {
        Some(rs) => rs,
        None => return Ok(()),
    };
    let current: r8s_types::ReplicaSet = serde_json::from_value(current_value)?;
    let current_uid = current.metadata.uid.as_deref().unwrap_or(rs_uid);

    let desired = current.spec.replicas;
    let template = &current.spec.template;

    let pod_gvr = pods_gvr();
    let pods = store.list(&pod_gvr, rs_ns, None, None, None, None)?;
    let owned: Vec<Pod> = pods
        .items
        .iter()
        .filter_map(|v| serde_json::from_value::<Pod>(v.clone()).ok())
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();
    let current_count = owned.len() as u64;

    if current_count < desired {
        let to_create = desired - current_count;
        for _ in 0..to_create {
            create_pod_from_template(store, rs_name, current_uid, rs_ns, template)?;
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

    let final_pods = store.list(&pod_gvr, rs_ns, None, None, None, None)?;
    let final_count = final_pods
        .items
        .iter()
        .filter_map(|v| serde_json::from_value::<Pod>(v.clone()).ok())
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .count() as u64;
    update_rs_status(store, rs_name, rs_ns, final_count)?;

    Ok(())
}

fn create_pod_from_template(
    store: &Store,
    rs_name: &str,
    rs_uid: &str,
    namespace: Option<&str>,
    template: &r8s_types::PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{}-{}", rs_name, random_suffix());
    let mut labels = template.metadata.labels.clone();
    let pod = Pod {
        api_version: "v1".into(),
        kind: "Pod".into(),
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels: std::mem::take(&mut labels),
            owner_references: vec![OwnerReference {
                api_version: "apps/v1".into(),
                kind: "ReplicaSet".into(),
                name: rs_name.into(),
                uid: rs_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }],
            ..Default::default()
        },
        spec: template.spec.clone(),
        status: None,
    };

    let gvr = pods_gvr();
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
    replica_count: u64,
) -> anyhow::Result<()> {
    let gvr = rs_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: rs_ns,
        name: rs_name,
    };

    let mut current = match store.get(&resource_ref)? {
        Some(rs) => rs,
        None => return Ok(()),
    };

    let status = ReplicaSetStatus {
        replicas: replica_count,
        ready_replicas: replica_count,
        available_replicas: replica_count,
    };
    current["status"] = serde_json::to_value(&status)?;

    match store.update(&resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("rs status update conflict for '{rs_name}', will retry: {e}");
            Ok(())
        }
    }
}
