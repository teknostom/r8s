use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
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

fn is_owned_by(object: &serde_json::Value, owner_uid: &str) -> bool {
    object["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
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
                    if let Some((rs_name, rs_ns)) = find_owner_rs(&event.object) {
                        let resource_ref = ResourceRef {
                            gvr: &rs_gvr(),
                            namespace: rs_ns.as_deref(),
                            name: &rs_name,
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

fn find_owner_rs(pod: &serde_json::Value) -> Option<(String, Option<String>)> {
    let refs = pod["metadata"]["ownerReferences"].as_array()?;
    let owner = refs
        .iter()
        .find(|r| r["kind"].as_str() == Some("ReplicaSet"))?;
    let name = owner["name"].as_str()?.to_string();
    let ns = pod["metadata"]["namespace"].as_str().map(|s| s.to_string());
    Some((name, ns))
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

fn reconcile_rs(store: &Store, rs: &serde_json::Value) -> anyhow::Result<()> {
    let rs_name = rs["metadata"]["name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("RS has no name"))?;
    let rs_ns = rs["metadata"]["namespace"].as_str();
    let rs_uid = rs["metadata"]["uid"].as_str().unwrap_or("");

    let gvr = rs_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: rs_ns,
        name: rs_name,
    };
    let current_rs = match store.get(&resource_ref)? {
        Some(rs) => rs,
        None => return Ok(()),
    };

    let desired: u64 = current_rs["spec"]["replicas"].as_u64().unwrap_or(1);
    let template = &current_rs["spec"]["template"];
    let current_uid = current_rs["metadata"]["uid"].as_str().unwrap_or(rs_uid);

    let pod_gvr = pods_gvr();
    let pods = store.list(&pod_gvr, rs_ns, None, None, None, None)?;
    let owned: Vec<&serde_json::Value> = pods
        .items
        .iter()
        .filter(|p| is_owned_by(p, current_uid))
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
            if let Some(pod_name) = pod["metadata"]["name"].as_str() {
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
        .filter(|p| is_owned_by(p, current_uid))
        .count() as u64;
    update_rs_status(store, &current_rs, final_count)?;

    Ok(())
}

fn create_pod_from_template(
    store: &Store,
    rs_name: &str,
    rs_uid: &str,
    namespace: Option<&str>,
    template: &serde_json::Value,
) -> anyhow::Result<()> {
    let pod_name = format!("{}-{}", rs_name, random_suffix());
    let mut pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": template["metadata"]["labels"],
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": rs_name,
                "uid": rs_uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }],
        },
        "spec": template["spec"],
    });
    if let Some(ns) = namespace {
        pod["metadata"]["namespace"] = serde_json::json!(ns);
    }

    let gvr = pods_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &pod_name,
    };
    store.create(resource_ref, &pod)?;
    Ok(())
}

fn update_rs_status(
    store: &Store,
    rs: &serde_json::Value,
    replica_count: u64,
) -> anyhow::Result<()> {
    let rs_name = rs["metadata"]["name"].as_str().unwrap_or("");
    let rs_ns = rs["metadata"]["namespace"].as_str();
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

    current["status"] = serde_json::json!({
        "replicas": replica_count,
        "readyReplicas": replica_count,
        "avaliableReplicas": replica_count,
    });

    match store.update(&resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("rs status update conflict for '{rs_name}', will retry: {e}");
            Ok(())
        }
    }
}
