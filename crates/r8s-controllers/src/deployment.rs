use std::hash::{Hash, Hasher};

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    Deployment, GroupVersionResource, ObjectMeta, OwnerReference, ReplicaSet,
    deployment::{DeploymentCondition, DeploymentStatus},
    replicaset::ReplicaSetSpec,
};
use rustc_hash::FxHasher;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn deploy_gvr() -> GroupVersionResource {
    GroupVersionResource::new("apps", "v1", "deployments")
}

fn rs_gvr() -> GroupVersionResource {
    GroupVersionResource::new("apps", "v1", "replicasets")
}

fn is_owned_by(meta: &ObjectMeta, owner_uid: &str) -> bool {
    meta.owner_references.iter().any(|r| r.uid == owner_uid)
}

fn template_hash(template: &serde_json::Value) -> String {
    let canonical = serde_json::to_string(template).unwrap_or_default();
    let mut hasher = FxHasher::default();
    canonical.hash(&mut hasher);
    format!("{:010x}", hasher.finish() & 0xff_ffff_ffff)
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("deployment controller started");
    let gvr = deploy_gvr();

    reconcile_all(&store);

    let mut deploy_rx = store.watch(&gvr);
    let mut rs_rx = store.watch(&rs_gvr());

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
                        if let Some(owner) = rs.metadata.owner_references.iter().find(|r| r.kind == "Deployment") {
                            let d_gvr = deploy_gvr();
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
    let gvr = deploy_gvr();
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

    let gvr = deploy_gvr();
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

    let desired_replicas = current.spec.replicas;
    let template_value = serde_json::to_value(&current.spec.template)?;
    let hash = template_hash(&template_value);
    let rs_name = format!("{deploy_name}-{hash}");

    // List RSes owned by this deployment
    let rs_gvr = rs_gvr();
    let all_rs = store.list(&rs_gvr, deploy_ns, None, None, None, None)?;
    let owned_rs: Vec<ReplicaSet> = all_rs
        .items
        .iter()
        .filter_map(|v| serde_json::from_value::<ReplicaSet>(v.clone()).ok())
        .filter(|rs| is_owned_by(&rs.metadata, current_uid))
        .collect();

    // Find or create the matching RS
    let matching = owned_rs
        .iter()
        .find(|rs| rs.metadata.name.as_deref() == Some(rs_name.as_str()));

    if let Some(existing_rs) = matching {
        // Ensure replicas match
        if existing_rs.spec.replicas != desired_replicas {
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name: &rs_name,
            };
            if let Ok(Some(mut rs_value)) = store.get(&rs_rref) {
                rs_value["spec"]["replicas"] = serde_json::json!(desired_replicas);
                let _ = store.update(&rs_rref, &rs_value);
                tracing::info!(
                    "deployment '{deploy_name}': scaled RS '{rs_name}' to {desired_replicas}"
                );
            }
        }
    } else {
        // Create new RS
        let mut labels = current
            .spec
            .selector
            .match_labels
            .clone()
            .unwrap_or_default();
        labels.insert("pod-template-hash".into(), hash.clone());

        let rs = ReplicaSet {
            api_version: "apps/v1".into(),
            kind: "ReplicaSet".into(),
            metadata: ObjectMeta {
                name: Some(rs_name.clone()),
                namespace: deploy_ns.map(String::from),
                labels: labels.clone(),
                owner_references: vec![OwnerReference {
                    api_version: "apps/v1".into(),
                    kind: "Deployment".into(),
                    name: deploy_name.into(),
                    uid: current_uid.into(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }],
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: desired_replicas,
                selector: current.spec.selector.clone(),
                template: current.spec.template.clone(),
            },
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

    // Scale down old RSes
    for old_rs in &owned_rs {
        let name = old_rs.metadata.name.as_deref().unwrap_or("");
        if name != rs_name && old_rs.spec.replicas > 0 {
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name,
            };
            if let Ok(Some(mut rs_value)) = store.get(&rs_rref) {
                rs_value["spec"]["replicas"] = serde_json::json!(0);
                let _ = store.update(&rs_rref, &rs_value);
                tracing::info!("deployment '{deploy_name}': scaled down old RS '{name}' to 0");
            }
        }
    }

    // Update deployment status
    update_deploy_status(store, &current_value)?;

    Ok(())
}

fn update_deploy_status(store: &Store, deploy_value: &serde_json::Value) -> anyhow::Result<()> {
    let deploy: Deployment = serde_json::from_value(deploy_value.clone())?;
    let deploy_name = deploy.metadata.name.as_deref().unwrap_or("");
    let deploy_ns = deploy.metadata.namespace.as_deref();
    let deploy_uid = deploy.metadata.uid.as_deref().unwrap_or("");

    let gvr = deploy_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: deploy_ns,
        name: deploy_name,
    };

    let mut current = match store.get(&resource_ref)? {
        Some(d) => d,
        None => return Ok(()),
    };

    let rs_gvr = rs_gvr();
    let all_rs = store.list(&rs_gvr, deploy_ns, None, None, None, None)?;
    let mut total_replicas: u64 = 0;
    let mut ready_replicas: u64 = 0;
    for rs_value in &all_rs.items {
        let rs: ReplicaSet = match serde_json::from_value(rs_value.clone()) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if is_owned_by(&rs.metadata, deploy_uid) {
            let status = rs.status.unwrap_or_default();
            total_replicas += status.replicas;
            ready_replicas += status.ready_replicas;
        }
    }

    let status = DeploymentStatus {
        replicas: total_replicas,
        ready_replicas,
        available_replicas: ready_replicas,
        conditions: vec![DeploymentCondition {
            type_: "Available".into(),
            status: if ready_replicas > 0 {
                "True".into()
            } else {
                "False".into()
            },
            reason: Some("MinimumReplicasAvailable".into()),
        }],
    };
    let new_status_val = serde_json::to_value(&status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    current["status"] = new_status_val;

    match store.update(&resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("deploy status update conflict for '{deploy_name}': {e}");
            Ok(())
        }
    }
}
