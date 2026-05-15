use std::hash::{Hash, Hasher};

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    Deployment, DeploymentCondition, DeploymentSpec, DeploymentStatus, GroupVersionResource,
    IntOrString, ObjectMeta, OwnerReference, ReplicaSet, ReplicaSetSpec,
};
use rustc_hash::FxHasher;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

const DEFAULT_MAX_SURGE_PERCENT: f64 = 25.0;
const DEFAULT_MAX_UNAVAILABLE_PERCENT: f64 = 25.0;

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
                            if let Ok(Some(deploy)) = store.get(&rref)
                                && let Err(e) = reconcile_deployment(&store, &deploy)
                            {
                                tracing::warn!("deployment reconcile error: {e}");
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
    let new_rs_name = format!("{deploy_name}-{hash}");

    let rs_gvr = GroupVersionResource::replica_sets();
    let owned_rs: Vec<ReplicaSet> = store
        .list_as::<ReplicaSet>(&rs_gvr, deploy_ns)?
        .into_iter()
        .filter(|rs| is_owned_by(&rs.metadata, current_uid))
        .collect();

    let strategy_type = current_spec
        .strategy
        .as_ref()
        .and_then(|s| s.type_.as_deref());
    let is_recreate = strategy_type == Some("Recreate");

    if is_recreate {
        reconcile_recreate(
            store,
            deploy_name,
            deploy_ns,
            current_uid,
            current_spec,
            &owned_rs,
            &new_rs_name,
            &hash,
            desired_replicas,
        )?;
    } else {
        reconcile_rolling(
            store,
            deploy_name,
            deploy_ns,
            current_uid,
            current_spec,
            &owned_rs,
            &new_rs_name,
            &hash,
            desired_replicas,
        )?;
    }

    update_deploy_status(store, &current_value)?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reconcile_recreate(
    store: &Store,
    deploy_name: &str,
    deploy_ns: Option<&str>,
    deploy_uid: &str,
    spec: &DeploymentSpec,
    owned_rs: &[ReplicaSet],
    new_rs_name: &str,
    hash: &str,
    desired_replicas: i32,
) -> anyhow::Result<()> {
    if let Some(existing_rs) = owned_rs
        .iter()
        .find(|rs| rs.metadata.name.as_deref() == Some(new_rs_name))
    {
        let existing_replicas = existing_rs
            .spec
            .as_ref()
            .and_then(|s| s.replicas)
            .unwrap_or(1);
        if existing_replicas != desired_replicas {
            scale_rs(store, deploy_ns, new_rs_name, desired_replicas)?;
            tracing::info!(
                "deployment '{deploy_name}': scaled RS '{new_rs_name}' to {desired_replicas}"
            );
        }
    } else {
        create_new_rs(
            store,
            deploy_name,
            deploy_ns,
            deploy_uid,
            spec,
            new_rs_name,
            hash,
            desired_replicas,
        )?;
        tracing::info!(
            "deployment '{deploy_name}': created RS '{new_rs_name}' with {desired_replicas} replicas"
        );
    }

    for old_rs in owned_rs {
        let name = old_rs.metadata.name.as_deref().unwrap_or("");
        let old_replicas = old_rs.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
        if name != new_rs_name && old_replicas > 0 {
            scale_rs(store, deploy_ns, name, 0)?;
            tracing::info!("deployment '{deploy_name}': scaled down old RS '{name}' to 0");
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reconcile_rolling(
    store: &Store,
    deploy_name: &str,
    deploy_ns: Option<&str>,
    deploy_uid: &str,
    spec: &DeploymentSpec,
    owned_rs: &[ReplicaSet],
    new_rs_name: &str,
    hash: &str,
    desired_replicas: i32,
) -> anyhow::Result<()> {
    let (max_surge, max_unavailable) = rolling_params(spec, desired_replicas);

    let new_rs = owned_rs
        .iter()
        .find(|rs| rs.metadata.name.as_deref() == Some(new_rs_name));
    let new_rs_spec_replicas = new_rs
        .and_then(|rs| rs.spec.as_ref().and_then(|s| s.replicas))
        .unwrap_or(0);
    let new_rs_available = new_rs
        .and_then(|rs| rs.status.as_ref())
        .and_then(|s| s.available_replicas)
        .unwrap_or(0);

    let total_spec_replicas: i32 = owned_rs
        .iter()
        .map(|rs| rs.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0))
        .sum();

    let max_total_pods = desired_replicas + max_surge;
    let scale_up_room = (max_total_pods - total_spec_replicas).max(0);
    let new_target = (new_rs_spec_replicas + scale_up_room).min(desired_replicas);

    let (new_rs_spec_after, total_spec_after) = if new_rs.is_none() {
        create_new_rs(
            store,
            deploy_name,
            deploy_ns,
            deploy_uid,
            spec,
            new_rs_name,
            hash,
            new_target,
        )?;
        tracing::info!(
            "deployment '{deploy_name}': created RS '{new_rs_name}' with {new_target} replicas (rolling)"
        );
        (new_target, total_spec_replicas + new_target)
    } else if new_target != new_rs_spec_replicas {
        scale_rs(store, deploy_ns, new_rs_name, new_target)?;
        tracing::info!(
            "deployment '{deploy_name}': scaled new RS '{new_rs_name}' from {new_rs_spec_replicas} to {new_target}"
        );
        (
            new_target,
            total_spec_replicas + (new_target - new_rs_spec_replicas),
        )
    } else {
        (new_rs_spec_replicas, total_spec_replicas)
    };

    let min_available = (desired_replicas - max_unavailable).max(0);
    let new_unavailable = (new_rs_spec_after - new_rs_available).max(0);
    let max_scaled_down = (total_spec_after - min_available - new_unavailable).max(0);

    let mut remaining = max_scaled_down;
    for old in owned_rs {
        if remaining <= 0 {
            break;
        }
        let name = match old.metadata.name.as_deref() {
            Some(n) if n != new_rs_name => n,
            _ => continue,
        };
        let cur = old.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
        if cur <= 0 {
            continue;
        }
        let reduce_by = cur.min(remaining);
        let target = cur - reduce_by;
        scale_rs(store, deploy_ns, name, target)?;
        tracing::info!(
            "deployment '{deploy_name}': scaled old RS '{name}' from {cur} to {target} (rolling)"
        );
        remaining -= reduce_by;
    }

    Ok(())
}

fn rolling_params(spec: &DeploymentSpec, desired: i32) -> (i32, i32) {
    let ru = spec
        .strategy
        .as_ref()
        .and_then(|s| s.rolling_update.as_ref());
    let surge = ru
        .and_then(|r| r.max_surge.as_ref())
        .map(|v| resolve_intstr(v, desired, true))
        .unwrap_or_else(|| ((desired as f64) * (DEFAULT_MAX_SURGE_PERCENT / 100.0)).ceil() as i32)
        .max(0);
    let unavail = ru
        .and_then(|r| r.max_unavailable.as_ref())
        .map(|v| resolve_intstr(v, desired, false))
        .unwrap_or_else(|| {
            ((desired as f64) * (DEFAULT_MAX_UNAVAILABLE_PERCENT / 100.0)).floor() as i32
        })
        .max(0);
    // Both at zero is an invalid combination per k8s validation; nudge surge
    // to 1 so the rollout can make progress instead of deadlocking.
    if surge == 0 && unavail == 0 {
        (1, 0)
    } else {
        (surge, unavail)
    }
}

fn resolve_intstr(v: &IntOrString, desired: i32, round_up: bool) -> i32 {
    match v {
        IntOrString::Int(n) => *n,
        IntOrString::String(s) => {
            if let Some(stripped) = s.strip_suffix('%')
                && let Ok(p) = stripped.trim().parse::<f64>()
            {
                let raw = (p / 100.0) * (desired as f64);
                return if round_up {
                    raw.ceil() as i32
                } else {
                    raw.floor() as i32
                };
            }
            s.parse::<i32>().unwrap_or(0)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_new_rs(
    store: &Store,
    deploy_name: &str,
    deploy_ns: Option<&str>,
    deploy_uid: &str,
    spec: &DeploymentSpec,
    rs_name: &str,
    hash: &str,
    replicas: i32,
) -> anyhow::Result<()> {
    let mut labels = spec.selector.match_labels.clone().unwrap_or_default();
    labels.insert("pod-template-hash".into(), hash.to_string());

    let rs = ReplicaSet {
        metadata: ObjectMeta {
            name: Some(rs_name.to_string()),
            namespace: deploy_ns.map(String::from),
            labels: Some(labels),
            owner_references: Some(vec![OwnerReference {
                api_version: "apps/v1".into(),
                kind: "Deployment".into(),
                name: deploy_name.into(),
                uid: deploy_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: Some(ReplicaSetSpec {
            replicas: Some(replicas),
            selector: spec.selector.clone(),
            template: Some(spec.template.clone()),
            ..Default::default()
        }),
        status: None,
    };

    let rs_gvr = GroupVersionResource::replica_sets();
    let rs_rref = ResourceRef {
        gvr: &rs_gvr,
        namespace: deploy_ns,
        name: rs_name,
    };
    store.create(rs_rref, &serde_json::to_value(&rs)?)?;
    Ok(())
}

fn scale_rs(
    store: &Store,
    deploy_ns: Option<&str>,
    rs_name: &str,
    replicas: i32,
) -> anyhow::Result<()> {
    let rs_gvr = GroupVersionResource::replica_sets();
    let rs_rref = ResourceRef {
        gvr: &rs_gvr,
        namespace: deploy_ns,
        name: rs_name,
    };
    let mut rs_value = match store.get(&rs_rref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    if let Some(spec) = rs_value.get_mut("spec").and_then(|v| v.as_object_mut()) {
        spec.insert("replicas".to_string(), serde_json::json!(replicas));
    }
    let _ = store.update(&rs_rref, &rs_value);
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
