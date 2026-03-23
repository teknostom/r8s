use std::hash::{Hash, Hasher};

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    Deployment, DeploymentCondition, DeploymentStatus, GroupVersionResource, ObjectMeta,
    OwnerReference, ReplicaSet, ReplicaSetSpec,
};
use rustc_hash::FxHasher;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

fn template_hash(template: &serde_json::Value) -> String {
    let canonical = serde_json::to_string(template).unwrap_or_default();
    let mut hasher = FxHasher::default();
    canonical.hash(&mut hasher);
    format!("{:010x}", hasher.finish() & 0xff_ffff_ffff)
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("deployment controller started");
    let gvr = GroupVersionResource::deployments();

    reconcile_all(&store);

    let mut deploy_rx = store.watch(&gvr);
    let mut rs_rx = store.watch(&GroupVersionResource::replica_sets());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("deployment controller shutting down");
                return Ok(());
            }
            event = deploy_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_deployment(&store, &event.object) {
                            tracing::warn!("deployment reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = rs_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Modified) => {
                        let rs: ReplicaSet = match serde_json::from_value(event.object) {
                            Ok(r) => r,
                            Err(_) => continue,
                        };
                        if let Some(owner) = crate::find_owner(&rs.metadata, "Deployment") {
                            let d_gvr = GroupVersionResource::deployments();
                            let rref = ResourceRef {
                                gvr: &d_gvr,
                                namespace: rs.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(deploy)) = store.get(&rref) {
                                let _ = update_deploy_status(&store, &deploy);
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
    let gvr = GroupVersionResource::deployments();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("deployment controller list error: {e}");
            return;
        }
    };
    for deploy in &result.items {
        if let Err(e) = reconcile_deployment(store, deploy) {
            tracing::warn!("deployment reconcile error: {e}");
        }
    }
}

fn reconcile_deployment(store: &Store, deploy_value: &serde_json::Value) -> anyhow::Result<()> {
    let deploy: Deployment = serde_json::from_value(deploy_value.clone())?;
    let deploy_name = deploy
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Deployment has no name"))?;
    let deploy_ns = deploy.metadata.namespace.as_deref();
    let deploy_uid = deploy.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::deployments();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: deploy_ns,
        name: deploy_name,
    };
    let current_value = match store.get(&resource_ref)? {
        Some(d) => d,
        None => return Ok(()),
    };
    let current: Deployment = serde_json::from_value(current_value.clone())?;
    let current_uid = current.metadata.uid.as_deref().unwrap_or(deploy_uid);
    let current_spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Deployment has no spec"))?;

    let desired_replicas = current_spec.replicas.unwrap_or(1);
    let template_value = serde_json::to_value(&current_spec.template)?;
    let hash = template_hash(&template_value);
    let rs_name = format!("{deploy_name}-{hash}");

    let rs_gvr = GroupVersionResource::replica_sets();
    let owned_rs: Vec<ReplicaSet> = store
        .list_as::<ReplicaSet>(&rs_gvr, deploy_ns)?
        .into_iter()
        .filter(|rs| is_owned_by(&rs.metadata, current_uid))
        .collect();

    let matching = owned_rs
        .iter()
        .find(|rs| rs.metadata.name.as_deref() == Some(rs_name.as_str()));

    if let Some(existing_rs) = matching {
        let existing_replicas = existing_rs
            .spec
            .as_ref()
            .and_then(|s| s.replicas)
            .unwrap_or(1);
        if existing_replicas != desired_replicas {
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name: &rs_name,
            };
            if let Ok(Some(mut rs_value)) = store.get(&rs_rref) {
                if let Some(spec) = rs_value.get_mut("spec").and_then(|v| v.as_object_mut()) {
                    spec.insert("replicas".to_string(), serde_json::json!(desired_replicas));
                }
                let _ = store.update(&rs_rref, &rs_value);
                tracing::info!(
                    "deployment '{deploy_name}': scaled RS '{rs_name}' to {desired_replicas}"
                );
            }
        }
    } else {
        let mut labels = current_spec
            .selector
            .match_labels
            .clone()
            .unwrap_or_default();
        labels.insert("pod-template-hash".into(), hash.clone());

        let rs = ReplicaSet {
            metadata: ObjectMeta {
                name: Some(rs_name.clone()),
                namespace: deploy_ns.map(String::from),
                labels: Some(labels.clone()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "apps/v1".into(),
                    kind: "Deployment".into(),
                    name: deploy_name.into(),
                    uid: current_uid.into(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: Some(ReplicaSetSpec {
                replicas: Some(desired_replicas),
                selector: current_spec.selector.clone(),
                template: Some(current_spec.template.clone()),
                ..Default::default()
            }),
            status: None,
        };

        let rs_rref = ResourceRef {
            gvr: &rs_gvr,
            namespace: deploy_ns,
            name: &rs_name,
        };
        store.create(rs_rref, &serde_json::to_value(&rs)?)?;
        tracing::info!(
            "deployment '{deploy_name}': created RS '{rs_name}' with {desired_replicas} replicas"
        );
    }

    for old_rs in &owned_rs {
        let name = old_rs.metadata.name.as_deref().unwrap_or("");
        let old_replicas = old_rs.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
        if name != rs_name && old_replicas > 0 {
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name,
            };
            if let Ok(Some(mut rs_value)) = store.get(&rs_rref) {
                if let Some(spec) = rs_value.get_mut("spec").and_then(|v| v.as_object_mut()) {
                    spec.insert("replicas".to_string(), serde_json::json!(0));
                }
                let _ = store.update(&rs_rref, &rs_value);
                tracing::info!("deployment '{deploy_name}': scaled down old RS '{name}' to 0");
            }
        }
    }

    update_deploy_status(store, &current_value)?;

    Ok(())
}

fn update_deploy_status(store: &Store, deploy_value: &serde_json::Value) -> anyhow::Result<()> {
    let deploy: Deployment = serde_json::from_value(deploy_value.clone())?;
    let deploy_name = deploy.metadata.name.as_deref().unwrap_or("");
    let deploy_ns = deploy.metadata.namespace.as_deref();
    let deploy_uid = deploy.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::deployments();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: deploy_ns,
        name: deploy_name,
    };

    let mut current = match store.get(&resource_ref)? {
        Some(d) => d,
        None => return Ok(()),
    };

    let rs_gvr = GroupVersionResource::replica_sets();
    let owned_rs: Vec<ReplicaSet> = store
        .list_as::<ReplicaSet>(&rs_gvr, deploy_ns)?
        .into_iter()
        .filter(|rs| is_owned_by(&rs.metadata, deploy_uid))
        .collect();
    let mut total_replicas: i32 = 0;
    let mut ready_replicas: i32 = 0;
    for rs in &owned_rs {
        let status = rs.status.clone().unwrap_or_default();
        total_replicas += status.replicas;
        ready_replicas += status.ready_replicas.unwrap_or(0);
    }

    let status = DeploymentStatus {
        replicas: Some(total_replicas),
        ready_replicas: Some(ready_replicas),
        available_replicas: Some(ready_replicas),
        conditions: Some(vec![DeploymentCondition {
            type_: "Available".into(),
            status: if ready_replicas > 0 {
                "True".into()
            } else {
                "False".into()
            },
            reason: Some("MinimumReplicasAvailable".into()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let new_status_val = serde_json::to_value(&status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    if let Some(obj) = current.as_object_mut() {
        obj.insert("status".to_string(), new_status_val);
    }

    match store.update(&resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("deploy status update conflict for '{deploy_name}': {e}");
            Ok(())
        }
    }
}
