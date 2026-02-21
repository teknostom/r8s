use std::hash::{Hash, Hasher};

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use rustc_hash::FxHasher;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn deploy_gvr() -> GroupVersionResource {
    GroupVersionResource::new("apps", "v1", "deployments")
}

fn rs_gvr() -> GroupVersionResource {
    GroupVersionResource::new("apps", "v1", "replicasets")
}

fn is_owned_by(object: &serde_json::Value, owner_uid: &str) -> bool {
    object["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
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
                        // RS status changed — find parent deploy and update its status
                        if let Some((deploy_name, deploy_ns)) = find_owner_deploy(&event.object) {
                            let d_gvr = deploy_gvr();
                            let rref = ResourceRef {
                                gvr: &d_gvr,
                                namespace: deploy_ns.as_deref(),
                                name: &deploy_name,
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

fn find_owner_deploy(rs: &serde_json::Value) -> Option<(String, Option<String>)> {
    let refs = rs["metadata"]["ownerReferences"].as_array()?;
    let owner = refs
        .iter()
        .find(|r| r["kind"].as_str() == Some("Deployment"))?;
    let name = owner["name"].as_str()?.to_string();
    let ns = rs["metadata"]["namespace"].as_str().map(|s| s.to_string());
    Some((name, ns))
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

fn reconcile_deployment(store: &Store, deploy: &serde_json::Value) -> anyhow::Result<()> {
    let deploy_name = deploy["metadata"]["name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Deployment has no name"))?;
    let deploy_ns = deploy["metadata"]["namespace"].as_str();
    let deploy_uid = deploy["metadata"]["uid"].as_str().unwrap_or("");

    let gvr = deploy_gvr();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: deploy_ns,
        name: deploy_name,
    };
    let current_deploy = match store.get(&resource_ref)? {
        Some(d) => d,
        None => return Ok(()),
    };
    let current_uid = current_deploy["metadata"]["uid"]
        .as_str()
        .unwrap_or(deploy_uid);

    let desired_replicas = current_deploy["spec"]["replicas"].as_u64().unwrap_or(1);
    let template = &current_deploy["spec"]["template"];
    let selector = &current_deploy["spec"]["selector"];
    let hash = template_hash(template);
    let rs_name = format!("{deploy_name}-{hash}");

    // List RSes owned by this deployment
    let rs_gvr = rs_gvr();
    let all_rs = store.list(&rs_gvr, deploy_ns, None, None, None, None)?;
    let owned_rs: Vec<&serde_json::Value> = all_rs
        .items
        .iter()
        .filter(|rs| is_owned_by(rs, current_uid))
        .collect();

    // Find or create the matching RS
    let matching = owned_rs
        .iter()
        .find(|rs| rs["metadata"]["name"].as_str() == Some(rs_name.as_str()));

    if let Some(existing_rs) = matching {
        // Ensure replicas match
        let current_replicas = existing_rs["spec"]["replicas"].as_u64().unwrap_or(0);
        if current_replicas != desired_replicas {
            let mut updated = (*existing_rs).clone();
            updated["spec"]["replicas"] = serde_json::json!(desired_replicas);
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name: &rs_name,
            };
            let _ = store.update(&rs_rref, &updated);
            tracing::info!(
                "deployment '{deploy_name}': scaled RS '{rs_name}' to {desired_replicas}"
            );
        }
    } else {
        // Create new RS
        let mut labels = serde_json::Map::new();
        if let Some(match_labels) = selector["matchLabels"].as_object() {
            labels.extend(match_labels.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        labels.insert("pod-template-hash".to_string(), serde_json::json!(hash));

        let rs = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": rs_name,
                "namespace": deploy_ns,
                "labels": labels,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": deploy_name,
                    "uid": current_uid,
                    "controller": true,
                    "blockOwnerDeletion": true,
                }],
            },
            "spec": {
                "replicas": desired_replicas,
                "selector": selector,
                "template": template,
            },
        });

        let rs_rref = ResourceRef {
            gvr: &rs_gvr,
            namespace: deploy_ns,
            name: &rs_name,
        };
        store.create(rs_rref, &rs)?;
        tracing::info!(
            "deployment '{deploy_name}': created RS '{rs_name}' with {desired_replicas} replicas"
        );
    }

    // Scale down old RSes
    for old_rs in &owned_rs {
        let name = old_rs["metadata"]["name"].as_str().unwrap_or("");
        if name != rs_name && old_rs["spec"]["replicas"].as_u64().unwrap_or(0) > 0 {
            let mut updated = (*old_rs).clone();
            updated["spec"]["replicas"] = serde_json::json!(0);
            let rs_rref = ResourceRef {
                gvr: &rs_gvr,
                namespace: deploy_ns,
                name,
            };
            let _ = store.update(&rs_rref, &updated);
            tracing::info!("deployment '{deploy_name}': scaled down old RS '{name}' to 0");
        }
    }

    // Update deployment status
    update_deploy_status(store, &current_deploy)?;

    Ok(())
}

fn update_deploy_status(store: &Store, deploy: &serde_json::Value) -> anyhow::Result<()> {
    let deploy_name = deploy["metadata"]["name"].as_str().unwrap_or("");
    let deploy_ns = deploy["metadata"]["namespace"].as_str();
    let deploy_uid = deploy["metadata"]["uid"].as_str().unwrap_or("");

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
    for rs in &all_rs.items {
        if is_owned_by(rs, deploy_uid) {
            total_replicas += rs["status"]["replicas"].as_u64().unwrap_or(0);
            ready_replicas += rs["status"]["readyReplicas"].as_u64().unwrap_or(0);
        }
    }

    current["status"] = serde_json::json!({
          "replicas": total_replicas,
          "readyReplicas": ready_replicas,
          "availableReplicas": ready_replicas,
          "conditions": [{
              "type": "Available",
              "status": if ready_replicas > 0 { "True" } else { "False" },
              "reason": "MinimumReplicasAvailable",
          }],
    });

    match store.update(&resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("deploy status update conflict for '{deploy_name}': {e}");
            Ok(())
        }
    }
}
