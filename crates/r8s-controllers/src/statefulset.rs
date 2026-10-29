//! StatefulSet controller: ordinal-indexed pods with stable identity, per-pod
//! PVCs, ControllerRevision history and partitioned rolling updates.

use std::cmp::Reverse;
use std::collections::BTreeSet;

use r8s_store::{Store, StoreError, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    ControllerRevision, GroupVersionResource, ObjectMeta, PersistentVolumeClaim,
    PersistentVolumeClaimVolumeSource, Pod, PodTemplateSpec, StatefulSet, StatefulSetSpec,
    StatefulSetStatus, Volume,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::history::{self, Parent};
use crate::{is_owned_by, pod_available, pod_phase, pod_running_and_ready, pod_terminating};

const POD_NAME_LABEL: &str = "statefulset.kubernetes.io/pod-name";
const POD_INDEX_LABEL: &str = "apps.kubernetes.io/pod-index";
const DEFAULT_REVISION_HISTORY_LIMIT: i32 = 10;

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
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = handle_sts_deleted(&store, &event.object) {
                            tracing::warn!("sts deletion cleanup error: {e}");
                        }
                    }
                    Ok(event) => {
                        if let Err(e) = reconcile_sts(&store, &event.object) {
                            tracing::warn!("sts reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted | WatchEventType::Modified) => {
                        let pod: Pod = match serde_json::from_value(event.object.as_ref().clone()) {
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

/// Where new pods take their template from. `partition: None` is an explicit
/// RollingUpdate without a `rollingUpdate` block, which upstream leaves
/// undefaulted and resolves against `status.currentReplicas` instead.
struct Rollout {
    rolling: bool,
    partition: Option<i32>,
}

impl Rollout {
    fn of(spec: &StatefulSetSpec) -> Self {
        let Some(strategy) = spec.update_strategy.as_ref() else {
            return Rollout {
                rolling: true,
                partition: Some(0),
            };
        };
        let rolling = strategy.type_.as_deref().unwrap_or("RollingUpdate") == "RollingUpdate";
        let partition = match (&strategy.rolling_update, &strategy.type_) {
            (Some(r), _) => Some(r.partition.unwrap_or(0)),
            (None, None) => Some(0),
            (None, Some(_)) => None,
        };
        Rollout { rolling, partition }
    }

    fn update_min(&self) -> u64 {
        self.partition.unwrap_or(0).max(0) as u64
    }
}

fn reconcile_sts(store: &Store, sts_value: &serde_json::Value) -> anyhow::Result<()> {
    let sts: StatefulSet = serde_json::from_value(sts_value.clone())?;
    let sts_name = sts
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("StatefulSet has no name"))?;
    let sts_ns = sts.metadata.namespace.as_deref();

    let gvr = GroupVersionResource::stateful_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: sts_ns,
        name: sts_name,
    };
    let Some(current) = store.get_as::<StatefulSet>(&resource_ref)? else {
        return Ok(());
    };
    let uid = current.metadata.uid.as_deref().unwrap_or("");
    if uid.is_empty() {
        tracing::warn!("sts '{sts_name}': missing uid; skipping");
        return Ok(());
    }
    let spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("StatefulSet has no spec"))?;
    let parent = Parent {
        api_version: "apps/v1",
        kind: "StatefulSet",
        name: sts_name,
        namespace: sts_ns,
        uid,
    };
    let status_before = current.status.clone().unwrap_or_default();

    let (revisions, current_rev, update_rev, collision_count) =
        resolve_revisions(store, &parent, &current, spec)?;
    let current_name = history::name_of(&current_rev).to_string();
    let update_name = history::name_of(&update_rev).to_string();
    let current_template =
        history::template_of(&current_rev).unwrap_or_else(|| spec.template.clone());

    let desired = spec.replicas.unwrap_or(1).max(0) as u64;
    let vcts = spec.volume_claim_templates.as_deref().unwrap_or_default();
    // OrderedReady (the default) acts on one pod per pass and only while every
    // lower ordinal is healthy; Parallel acts on every ordinal at once.
    let monotonic = spec.pod_management_policy.as_deref() != Some("Parallel");
    let min_ready = spec.min_ready_seconds.unwrap_or(0);
    let rollout = Rollout::of(spec);
    let when_scaled = spec
        .persistent_volume_claim_retention_policy
        .as_ref()
        .and_then(|p| p.when_scaled.as_deref())
        .unwrap_or("Retain");

    let owned = member_pods(store, sts_ns, sts_name, uid)?;
    let replicas: Vec<Option<&Pod>> = (0..desired)
        .map(|o| owned.iter().find(|p| pod_ordinal(p) == o))
        .collect();
    let mut condemned: Vec<&Pod> = owned.iter().filter(|p| pod_ordinal(p) >= desired).collect();
    condemned.sort_by_key(|p| Reverse(pod_ordinal(p)));
    let first_unhealthy: Option<&Pod> = replicas
        .iter()
        .flatten()
        .copied()
        .find(|p| !is_healthy(p))
        .or_else(|| condemned.iter().rev().copied().find(|p| !is_healthy(p)));

    let mut status = compute_status(&owned, &current_name, &update_name, min_ready);
    status.current_revision = Some(current_name.clone());
    status.update_revision = Some(update_name.clone());
    status.collision_count = Some(collision_count);
    status.observed_generation = Some(current.metadata.generation.unwrap_or(0).max(1));
    status.conditions = status_before.conditions.clone();

    if current.metadata.deletion_timestamp.is_some() {
        return write_status(store, &resource_ref, status);
    }

    let mut halted = false;
    for (ordinal, slot) in replicas.iter().enumerate() {
        let ordinal = ordinal as u64;
        match slot {
            Some(pod) if matches!(pod_phase(pod), "Failed" | "Succeeded") => {
                if !pod_terminating(pod) {
                    delete_pod(store, sts_ns, pod)?;
                    tracing::info!(
                        "sts '{sts_name}': deleted {} pod ordinal {ordinal}",
                        pod_phase(pod)
                    );
                }
                halted = true;
                break;
            }
            None => {
                let use_current = rollout.rolling
                    && match rollout.partition {
                        Some(p) => ordinal < p.max(0) as u64,
                        None => ordinal < status_before.current_replicas.unwrap_or(0).max(0) as u64,
                    };
                let (template, revision) = if use_current {
                    (&current_template, current_name.as_str())
                } else {
                    (&spec.template, update_name.as_str())
                };
                create_pod(
                    store,
                    &parent,
                    template,
                    vcts,
                    ordinal,
                    &spec.service_name,
                    revision,
                )?;
                tracing::info!("sts '{sts_name}': created pod ordinal {ordinal} ({revision})");
                if monotonic {
                    halted = true;
                    break;
                }
            }
            Some(pod) => {
                if monotonic
                    && (pod_terminating(pod)
                        || !pod_running_and_ready(pod)
                        || !pod_available(pod, min_ready))
                {
                    halted = true;
                    break;
                }
            }
        }
    }

    if !halted {
        for pod in &condemned {
            if pod_terminating(pod) {
                if monotonic {
                    halted = true;
                    break;
                }
                continue;
            }
            let is_first_unhealthy =
                first_unhealthy.is_some_and(|f| f.metadata.name == pod.metadata.name);
            if monotonic
                && !is_first_unhealthy
                && (!pod_running_and_ready(pod) || !pod_available(pod, min_ready))
            {
                halted = true;
                break;
            }
            scale_down_pod(store, pod, sts_ns, sts_name, vcts, when_scaled)?;
            if monotonic {
                halted = true;
                break;
            }
        }
    }

    let fresh = member_pods(store, sts_ns, sts_name, uid)?;
    let counts = compute_status(&fresh, &current_name, &update_name, min_ready);
    status.replicas = counts.replicas;
    status.ready_replicas = counts.ready_replicas;
    status.available_replicas = counts.available_replicas;
    status.current_replicas = counts.current_replicas;
    status.updated_replicas = counts.updated_replicas;

    // Rolling update: from the highest ordinal down to the partition, delete
    // the first pod not at the update revision, one per pass, waiting on
    // unhealthy pods above it.
    if !halted && rollout.rolling {
        let update_min = rollout.update_min();
        let mut targets: Vec<&Pod> = fresh.iter().filter(|p| pod_ordinal(p) < desired).collect();
        targets.sort_by_key(|p| Reverse(pod_ordinal(p)));
        for pod in targets {
            if pod_ordinal(pod) < update_min {
                break;
            }
            if pod_revision(pod) != update_name && !pod_terminating(pod) {
                delete_pod(store, sts_ns, pod)?;
                tracing::info!(
                    "sts '{sts_name}': deleted pod ordinal {} for update to {update_name}",
                    pod_ordinal(pod)
                );
                status.current_replicas = status.current_replicas.map(|c| (c - 1).max(0));
                break;
            }
            if !is_healthy(pod) {
                break;
            }
        }
    }

    if rollout.rolling
        && status.updated_replicas == Some(status.replicas)
        && status.ready_replicas == Some(status.replicas)
    {
        status.current_replicas = status.updated_replicas;
        status.current_revision = Some(update_name.clone());
    }

    write_status(store, &resource_ref, status)?;

    let mut live: BTreeSet<&str> = fresh.iter().map(pod_revision).collect();
    live.insert(&current_name);
    live.insert(&update_name);
    history::truncate(
        store,
        &parent,
        &revisions,
        spec.revision_history_limit
            .unwrap_or(DEFAULT_REVISION_HISTORY_LIMIT),
        |r| live.contains(history::name_of(r)),
    )?;
    Ok(())
}

/// The update revision for the current template (reusing or renumbering an
/// equal one, else creating it) and the current revision named by status.
fn resolve_revisions(
    store: &Store,
    parent: &Parent,
    sts: &StatefulSet,
    spec: &StatefulSetSpec,
) -> anyhow::Result<(
    Vec<ControllerRevision>,
    ControllerRevision,
    ControllerRevision,
    i32,
)> {
    let revisions = history::owned_revisions(store, parent)?;
    let mut collision_count = sts
        .status
        .as_ref()
        .and_then(|s| s.collision_count)
        .unwrap_or(0);
    let data = history::template_patch(&spec.template);
    let next = revisions.last().map_or(1, |r| r.revision + 1);
    let equal: Vec<&ControllerRevision> = revisions
        .iter()
        .filter(|r| history::data_of(r) == Some(&data))
        .collect();
    let update_rev = match (revisions.last(), equal.last()) {
        (Some(last), Some(eq)) if history::name_of(last) == history::name_of(eq) => last.clone(),
        (_, Some(eq)) => history::set_revision_number(store, parent, eq, next)?,
        _ => history::create_revision(
            store,
            parent,
            spec.template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.clone())
                .unwrap_or_default(),
            sts.metadata.annotations.clone(),
            data,
            next,
            &mut collision_count,
        )?,
    };
    let current_rev = sts
        .status
        .as_ref()
        .and_then(|s| s.current_revision.as_deref())
        .and_then(|name| revisions.iter().find(|r| history::name_of(r) == name))
        .cloned()
        .unwrap_or_else(|| update_rev.clone());
    Ok((revisions, current_rev, update_rev, collision_count))
}

/// Pods owned by the set and named `<set>-<ordinal>`, by ordinal.
fn member_pods(
    store: &Store,
    namespace: Option<&str>,
    sts_name: &str,
    uid: &str,
) -> anyhow::Result<Vec<Pod>> {
    let prefix = format!("{sts_name}-");
    let mut pods: Vec<Pod> = store
        .list_as::<Pod>(&GroupVersionResource::pods(), namespace)?
        .into_iter()
        .filter(|p| {
            is_owned_by(&p.metadata, uid)
                && p.metadata
                    .name
                    .as_deref()
                    .and_then(|n| n.strip_prefix(&prefix))
                    .is_some_and(|s| s.parse::<u64>().is_ok())
        })
        .collect();
    pods.sort_by_key(pod_ordinal);
    Ok(pods)
}

fn pod_ordinal(pod: &Pod) -> u64 {
    pod.metadata
        .name
        .as_deref()
        .and_then(|n| n.rsplit('-').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX)
}

fn pod_revision(pod: &Pod) -> &str {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(history::REVISION_HASH_LABEL))
        .map(String::as_str)
        .unwrap_or("")
}

fn is_healthy(pod: &Pod) -> bool {
    pod_running_and_ready(pod) && !pod_terminating(pod)
}

fn compute_status(
    pods: &[Pod],
    current_name: &str,
    update_name: &str,
    min_ready: i32,
) -> StatefulSetStatus {
    let mut status = StatefulSetStatus::default();
    let (mut ready, mut available, mut current, mut updated) = (0, 0, 0, 0);
    for pod in pods {
        status.replicas += 1;
        if pod_running_and_ready(pod) {
            ready += 1;
            if pod_available(pod, min_ready) {
                available += 1;
            }
        }
        if !pod_terminating(pod) {
            let revision = pod_revision(pod);
            if revision == current_name {
                current += 1;
            }
            if revision == update_name {
                updated += 1;
            }
        }
    }
    status.ready_replicas = Some(ready);
    status.available_replicas = Some(available);
    status.current_replicas = Some(current);
    status.updated_replicas = Some(updated);
    status
}

fn delete_pod(store: &Store, namespace: Option<&str>, pod: &Pod) -> anyhow::Result<()> {
    let Some(name) = pod.metadata.name.as_deref() else {
        return Ok(());
    };
    store.delete(&ResourceRef {
        gvr: &GroupVersionResource::pods(),
        namespace,
        name,
    })?;
    Ok(())
}

/// Delete a pod scaled beyond the replica count, and — when
/// `persistentVolumeClaimRetentionPolicy.whenScaled` is `Delete` — its per-ordinal
/// PVCs. By default PVCs are retained so the data survives a later scale-up.
fn scale_down_pod(
    store: &Store,
    pod: &Pod,
    namespace: Option<&str>,
    sts_name: &str,
    vcts: &[PersistentVolumeClaim],
    when_scaled: &str,
) -> anyhow::Result<()> {
    let Some(pod_name) = pod.metadata.name.as_deref() else {
        return Ok(());
    };
    let ordinal = pod_ordinal(pod);
    delete_pod(store, namespace, pod)?;

    if when_scaled == "Delete" {
        let pvc_gvr = GroupVersionResource::persistent_volume_claims();
        for vct in vcts {
            if let Some(tmpl) = vct.metadata.name.as_deref() {
                let pvc_name = format!("{tmpl}-{sts_name}-{ordinal}");
                let _ = store.delete(&ResourceRef {
                    gvr: &pvc_gvr,
                    namespace,
                    name: &pvc_name,
                });
            }
        }
        tracing::info!("sts '{sts_name}': scaled down {pod_name} + PVCs (whenScaled=Delete)");
    } else {
        tracing::info!("sts '{sts_name}': scaled down {pod_name}");
    }
    Ok(())
}

/// Create `<set>-<ordinal>` from `template` at `revision`, with a PVC per
/// volumeClaimTemplate and the hostname/subdomain that give it a stable DNS
/// name under the governing service.
fn create_pod(
    store: &Store,
    parent: &Parent,
    template: &PodTemplateSpec,
    volume_claim_templates: &[PersistentVolumeClaim],
    ordinal: u64,
    service_name: &str,
    revision: &str,
) -> anyhow::Result<()> {
    let pod_name = format!("{}-{ordinal}", parent.name);
    let mut labels = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert(POD_NAME_LABEL.into(), pod_name.clone());
    labels.insert(POD_INDEX_LABEL.into(), ordinal.to_string());
    labels.insert(history::REVISION_HASH_LABEL.into(), revision.to_string());

    let mut spec = template.spec.clone();
    if let Some(spec) = spec.as_mut() {
        spec.hostname = Some(pod_name.clone());
        if !service_name.is_empty() {
            spec.subdomain = Some(service_name.to_string());
        }
    }

    // PVCs are not owned by the set, so they outlive their pods.
    for vct in volume_claim_templates {
        let Some(tmpl_name) = vct.metadata.name.as_deref() else {
            continue;
        };
        let pvc_name = format!("{tmpl_name}-{}-{ordinal}", parent.name);
        ensure_pvc(store, parent.namespace, &pvc_name, vct)?;
        if let Some(spec) = spec.as_mut() {
            let vols = spec.volumes.get_or_insert_with(Vec::new);
            if !vols.iter().any(|v| v.name == tmpl_name) {
                vols.push(Volume {
                    name: tmpl_name.to_string(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: pvc_name.clone(),
                        read_only: None,
                    }),
                    ..Default::default()
                });
            }
        }
    }

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: parent.namespace.map(String::from),
            labels: Some(labels),
            annotations: template
                .metadata
                .as_ref()
                .and_then(|m| m.annotations.clone()),
            owner_references: Some(vec![crate::owner_reference(
                parent.api_version,
                parent.kind,
                parent.name,
                parent.uid,
            )]),
            ..Default::default()
        },
        spec,
        status: None,
    };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: parent.namespace,
        name: &pod_name,
    };
    let mut pod_value = serde_json::to_value(&pod)?;
    crate::pod_admission::admit_pod(store, &mut pod_value);
    store.create(resource_ref, &pod_value)?;
    Ok(())
}

/// Store `status` unless it already matches; a concurrent write wins.
fn write_status(
    store: &Store,
    resource_ref: &ResourceRef,
    status: StatefulSetStatus,
) -> anyhow::Result<()> {
    let Some(mut current) = store.get(resource_ref)? else {
        return Ok(());
    };
    let new_status = serde_json::to_value(&status)?;
    if current.get("status") == Some(&new_status) {
        return Ok(());
    }
    if let Some(obj) = current.as_object_mut() {
        obj.insert("status".to_string(), new_status);
    }
    match store.update(resource_ref, &current) {
        Ok(_) => Ok(()),
        Err(StoreError::Conflict { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn handle_sts_deleted(store: &Store, sts_value: &serde_json::Value) -> anyhow::Result<()> {
    let sts: r8s_types::StatefulSet = serde_json::from_value(sts_value.clone())?;
    let spec = match sts.spec.as_ref() {
        Some(s) => s,
        None => return Ok(()),
    };
    let when_deleted = spec
        .persistent_volume_claim_retention_policy
        .as_ref()
        .and_then(|p| p.when_deleted.as_deref())
        .unwrap_or("Retain");
    if when_deleted != "Delete" {
        return Ok(());
    }
    let Some(sts_name) = sts.metadata.name.as_deref() else {
        return Ok(());
    };
    let ns = sts.metadata.namespace.as_deref();
    let vcts = spec.volume_claim_templates.as_deref().unwrap_or_default();

    let pvc_gvr = GroupVersionResource::persistent_volume_claims();
    let all_pvcs = store.list_as::<PersistentVolumeClaim>(&pvc_gvr, ns)?;
    for vct in vcts {
        let Some(tmpl) = vct.metadata.name.as_deref() else {
            continue;
        };
        let prefix = format!("{tmpl}-{sts_name}-");
        for pvc in &all_pvcs {
            if let Some(name) = pvc.metadata.name.as_deref()
                && name
                    .strip_prefix(&prefix)
                    .is_some_and(|suffix| suffix.parse::<u64>().is_ok())
            {
                let _ = store.delete(&ResourceRef {
                    gvr: &pvc_gvr,
                    namespace: ns,
                    name,
                });
                tracing::info!("sts '{sts_name}': deleted PVC {name} (whenDeleted=Delete)");
            }
        }
    }
    Ok(())
}

/// Create a PVC from a volumeClaimTemplate if it doesn't already exist.
/// Idempotent: on pod recreation the PVC is found and reused, preserving data.
fn ensure_pvc(
    store: &Store,
    namespace: Option<&str>,
    pvc_name: &str,
    template: &PersistentVolumeClaim,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::persistent_volume_claims();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: pvc_name,
    };
    if store.get(&resource_ref)?.is_some() {
        return Ok(());
    }

    let mut pvc = template.clone();
    pvc.metadata.name = Some(pvc_name.to_string());
    pvc.metadata.namespace = namespace.map(String::from);
    pvc.metadata.uid = None;
    pvc.metadata.resource_version = None;
    pvc.status = None;

    let mut value = serde_json::to_value(&pvc)?;
    if let Some(obj) = value.as_object_mut() {
        obj.insert("apiVersion".into(), serde_json::json!("v1"));
        obj.insert("kind".into(), serde_json::json!("PersistentVolumeClaim"));
    }
    store.create(resource_ref, &value)?;
    tracing::info!("sts pvc '{pvc_name}' created");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn sts_gvr() -> GroupVersionResource {
        GroupVersionResource::stateful_sets()
    }
    fn pods_gvr() -> GroupVersionResource {
        GroupVersionResource::pods()
    }
    fn pvcs_gvr() -> GroupVersionResource {
        GroupVersionResource::persistent_volume_claims()
    }

    fn create_sts(store: &Store, name: &str, spec: Value) -> Value {
        store
            .create(
                ResourceRef {
                    gvr: &sts_gvr(),
                    namespace: Some("default"),
                    name,
                },
                &json!({
                    "apiVersion": "apps/v1", "kind": "StatefulSet",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": spec,
                }),
            )
            .unwrap()
    }

    fn create_owned_pod(store: &Store, name: &str, uid: &str, ready: bool) {
        store
            .create(
                ResourceRef {
                    gvr: &pods_gvr(),
                    namespace: Some("default"),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {
                        "name": name, "namespace": "default",
                        "ownerReferences": [{
                            "apiVersion": "apps/v1", "kind": "StatefulSet",
                            "name": "web", "uid": uid, "controller": true
                        }]
                    },
                    "spec": {"containers": []},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": if ready {"True"} else {"False"}}]
                    }
                }),
            )
            .unwrap();
    }

    fn create_pvc(store: &Store, name: &str) {
        store
            .create(
                ResourceRef {
                    gvr: &pvcs_gvr(),
                    namespace: Some("default"),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": {"resources": {"requests": {"storage": "1Gi"}}}
                }),
            )
            .unwrap();
    }

    fn exists(store: &Store, gvr: &GroupVersionResource, name: &str) -> bool {
        store
            .get(&ResourceRef {
                gvr,
                namespace: Some("default"),
                name,
            })
            .unwrap()
            .is_some()
    }

    fn owned_pod_names(store: &Store, uid: &str) -> Vec<String> {
        let mut v: Vec<String> = store
            .list_as::<Pod>(&pods_gvr(), Some("default"))
            .unwrap()
            .iter()
            .filter(|p| is_owned_by(&p.metadata, uid))
            .filter_map(|p| p.metadata.name.clone())
            .collect();
        v.sort();
        v
    }

    fn spec(replicas: i32, extra: Value) -> Value {
        let mut base = json!({
            "replicas": replicas,
            "serviceName": "svc",
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{"name": "c", "image": "img"}]}
            }
        });
        if let (Some(b), Some(e)) = (base.as_object_mut(), extra.as_object()) {
            for (k, val) in e {
                b.insert(k.clone(), val.clone());
            }
        }
        base
    }

    fn uid_of(sts: &Value) -> String {
        sts["metadata"]["uid"].as_str().unwrap().to_string()
    }

    #[test]
    fn ordered_scale_up_creates_first_ordinal_only() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(3, json!({})));
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid_of(&sts)), vec!["web-0"]);
    }

    #[test]
    fn ordered_scale_up_advances_when_predecessor_ready() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(3, json!({})));
        let uid = uid_of(&sts);
        create_owned_pod(&store, "web-0", &uid, true);
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0", "web-1"]);
    }

    #[test]
    fn ordered_scale_up_waits_when_predecessor_not_ready() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(3, json!({})));
        let uid = uid_of(&sts);
        create_owned_pod(&store, "web-0", &uid, false);
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0"]);
    }

    #[test]
    fn parallel_creates_all_at_once() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(3, json!({"podManagementPolicy": "Parallel"})),
        );
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(
            owned_pod_names(&store, &uid_of(&sts)),
            vec!["web-0", "web-1", "web-2"]
        );
    }

    #[test]
    fn when_scaled_delete_removes_pvc() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(
                1,
                json!({
                    "volumeClaimTemplates": [{
                        "metadata": {"name": "data"},
                        "spec": {"resources": {"requests": {"storage": "1Gi"}}}
                    }],
                    "persistentVolumeClaimRetentionPolicy": {
                        "whenScaled": "Delete", "whenDeleted": "Retain"
                    }
                }),
            ),
        );
        let uid = uid_of(&sts);
        create_owned_pod(&store, "web-0", &uid, true);
        create_owned_pod(&store, "web-1", &uid, true);
        create_pvc(&store, "data-web-1");

        reconcile_sts(&store, &sts).unwrap();

        assert!(
            !exists(&store, &pods_gvr(), "web-1"),
            "highest ordinal reaped"
        );
        assert!(
            exists(&store, &pods_gvr(), "web-0"),
            "kept replica survives"
        );
        assert!(
            !exists(&store, &pvcs_gvr(), "data-web-1"),
            "whenScaled=Delete reaps the PVC"
        );
    }

    #[test]
    fn when_deleted_delete_removes_all_pvcs() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(
                2,
                json!({
                    "volumeClaimTemplates": [{
                        "metadata": {"name": "data"},
                        "spec": {"resources": {"requests": {"storage": "1Gi"}}}
                    }],
                    "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}
                }),
            ),
        );
        create_pvc(&store, "data-web-0");
        create_pvc(&store, "data-web-1");

        handle_sts_deleted(&store, &sts).unwrap();

        assert!(!exists(&store, &pvcs_gvr(), "data-web-0"));
        assert!(!exists(&store, &pvcs_gvr(), "data-web-1"));
    }

    #[test]
    fn retain_is_the_default_on_scale_down() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(
                1,
                json!({
                    "volumeClaimTemplates": [{
                        "metadata": {"name": "data"},
                        "spec": {"resources": {"requests": {"storage": "1Gi"}}}
                    }]
                }),
            ),
        );
        let uid = uid_of(&sts);
        create_owned_pod(&store, "web-0", &uid, true);
        create_owned_pod(&store, "web-1", &uid, true);
        create_pvc(&store, "data-web-1");

        reconcile_sts(&store, &sts).unwrap();

        assert!(!exists(&store, &pods_gvr(), "web-1"));
        assert!(
            exists(&store, &pvcs_gvr(), "data-web-1"),
            "default retention keeps the PVC"
        );
    }

    fn create_member_pod(store: &Store, name: &str, uid: &str, ready: bool, revision: &str) {
        store
            .create(
                ResourceRef {
                    gvr: &pods_gvr(),
                    namespace: Some("default"),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {
                        "name": name, "namespace": "default",
                        "labels": {"controller-revision-hash": revision},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1", "kind": "StatefulSet",
                            "name": "web", "uid": uid, "controller": true
                        }]
                    },
                    "spec": {"containers": []},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": if ready {"True"} else {"False"}}]
                    }
                }),
            )
            .unwrap();
    }

    fn set_ready(store: &Store, name: &str, ready: bool) {
        let rref = ResourceRef {
            gvr: &pods_gvr(),
            namespace: Some("default"),
            name,
        };
        let mut pod = store.get(&rref).unwrap().unwrap();
        pod["status"]["conditions"] =
            json!([{"type": "Ready", "status": if ready {"True"} else {"False"}}]);
        store.update(&rref, &pod).unwrap();
    }

    fn sts_status(store: &Store) -> StatefulSetStatus {
        store
            .get_as::<StatefulSet>(&ResourceRef {
                gvr: &sts_gvr(),
                namespace: Some("default"),
                name: "web",
            })
            .unwrap()
            .unwrap()
            .status
            .unwrap()
    }

    fn revisions(store: &Store) -> Vec<ControllerRevision> {
        let mut r = store
            .list_as::<ControllerRevision>(
                &GroupVersionResource::controller_revisions(),
                Some("default"),
            )
            .unwrap();
        r.sort_by_key(|r| r.revision);
        r
    }

    fn set_image(store: &Store, image: &str) -> Value {
        let rref = ResourceRef {
            gvr: &sts_gvr(),
            namespace: Some("default"),
            name: "web",
        };
        let mut sts = store.get(&rref).unwrap().unwrap();
        sts["spec"]["template"]["spec"]["containers"][0]["image"] = json!(image);
        store.update(&rref, &sts).unwrap()
    }

    fn pod_label(store: &Store, name: &str) -> String {
        store
            .get_as::<Pod>(&ResourceRef {
                gvr: &pods_gvr(),
                namespace: Some("default"),
                name,
            })
            .unwrap()
            .unwrap()
            .metadata
            .labels
            .unwrap()["controller-revision-hash"]
            .clone()
    }

    #[test]
    fn pods_carry_the_revision_named_in_status() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(1, json!({})));
        reconcile_sts(&store, &sts).unwrap();

        let revs = revisions(&store);
        assert_eq!(revs.len(), 1);
        assert_eq!(revs[0].revision, 1);
        let status = sts_status(&store);
        assert_eq!(
            status.current_revision.as_deref(),
            Some(history::name_of(&revs[0]))
        );
        assert_eq!(status.update_revision, status.current_revision);
        assert_eq!(status.replicas, 1);
        assert_eq!(status.current_replicas, Some(1));
        assert_eq!(status.updated_replicas, Some(1));
        assert_eq!(status.collision_count, Some(0));
        assert_eq!(pod_label(&store, "web-0"), history::name_of(&revs[0]));
        let pod = store
            .get_as::<Pod>(&ResourceRef {
                gvr: &pods_gvr(),
                namespace: Some("default"),
                name: "web-0",
            })
            .unwrap()
            .unwrap();
        let labels = pod.metadata.labels.unwrap();
        assert_eq!(labels["statefulset.kubernetes.io/pod-name"], "web-0");
        assert_eq!(labels["apps.kubernetes.io/pod-index"], "0");
    }

    #[test]
    fn ordered_scale_down_halts_while_a_higher_pod_is_unhealthy() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(0, json!({})));
        let uid = uid_of(&sts);
        for name in ["web-0", "web-1", "web-2"] {
            create_member_pod(&store, name, &uid, false, "old");
        }
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(
            owned_pod_names(&store, &uid),
            vec!["web-0", "web-1", "web-2"],
            "nothing is deleted while the condemned pods are unhealthy"
        );

        for name in ["web-0", "web-1", "web-2"] {
            set_ready(&store, name, true);
        }
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0", "web-1"]);
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0"]);
    }

    #[test]
    fn ordered_scale_down_deletes_the_first_unhealthy_pod() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(0, json!({})));
        let uid = uid_of(&sts);
        create_member_pod(&store, "web-0", &uid, false, "old");
        reconcile_sts(&store, &sts).unwrap();
        assert!(owned_pod_names(&store, &uid).is_empty());
    }

    #[test]
    fn parallel_scale_down_ignores_health() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(0, json!({"podManagementPolicy": "Parallel"})),
        );
        let uid = uid_of(&sts);
        for name in ["web-0", "web-1", "web-2"] {
            create_member_pod(&store, name, &uid, false, "old");
        }
        reconcile_sts(&store, &sts).unwrap();
        assert!(owned_pod_names(&store, &uid).is_empty());
    }

    #[test]
    fn rolling_update_replaces_the_highest_ordinal_first() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(2, json!({})));
        let uid = uid_of(&sts);
        create_member_pod(&store, "web-0", &uid, true, "old");
        create_member_pod(&store, "web-1", &uid, true, "old");
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0"]);
        let status = sts_status(&store);
        assert_ne!(status.update_revision.as_deref(), Some("old"));
        assert_eq!(status.current_replicas, Some(0));
        assert_eq!(status.updated_replicas, Some(0));

        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0", "web-1"]);
        let update = sts_status(&store).update_revision.unwrap();
        assert_eq!(pod_label(&store, "web-1"), update);
        assert_eq!(
            pod_label(&store, "web-0"),
            "old",
            "waits for web-1 to be ready"
        );
    }

    #[test]
    fn partition_keeps_lower_ordinals_on_the_current_revision() {
        let (_d, store) = store();
        let sts = create_sts(
            &store,
            "web",
            spec(
                3,
                json!({"updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"partition": 2}}}),
            ),
        );
        reconcile_sts(&store, &sts).unwrap();
        let uid = uid_of(&sts);
        for name in ["web-0", "web-1", "web-2"] {
            let _ = store.delete(&ResourceRef {
                gvr: &pods_gvr(),
                namespace: Some("default"),
                name,
            });
        }
        let first = sts_status(&store).current_revision.unwrap();
        for name in ["web-0", "web-1", "web-2"] {
            create_member_pod(&store, name, &uid, true, &first);
        }
        let sts = set_image(&store, "img:2");

        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(owned_pod_names(&store, &uid), vec!["web-0", "web-1"]);
        let status = sts_status(&store);
        assert_eq!(status.current_revision.as_deref(), Some(first.as_str()));
        let update = status.update_revision.unwrap();
        assert_ne!(update, first);

        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(pod_label(&store, "web-2"), update);
        set_ready(&store, "web-2", true);
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(
            owned_pod_names(&store, &uid),
            vec!["web-0", "web-1", "web-2"]
        );
        assert_eq!(pod_label(&store, "web-0"), first);
        assert_eq!(pod_label(&store, "web-1"), first);

        // A pod below the partition is recreated from the current revision.
        store
            .delete(&ResourceRef {
                gvr: &pods_gvr(),
                namespace: Some("default"),
                name: "web-0",
            })
            .unwrap();
        reconcile_sts(&store, &sts).unwrap();
        assert_eq!(pod_label(&store, "web-0"), first);
    }

    #[test]
    fn rollback_renumbers_the_earlier_revision() {
        let (_d, store) = store();
        let sts = create_sts(&store, "web", spec(1, json!({})));
        reconcile_sts(&store, &sts).unwrap();
        let sts = set_image(&store, "img:2");
        reconcile_sts(&store, &sts).unwrap();
        let revs = revisions(&store);
        assert_eq!(revs.len(), 2);
        assert_eq!((revs[0].revision, revs[1].revision), (1, 2));

        let sts = set_image(&store, "img");
        reconcile_sts(&store, &sts).unwrap();
        let revs = revisions(&store);
        assert_eq!(revs.len(), 2);
        assert_eq!((revs[0].revision, revs[1].revision), (2, 3));
        assert_eq!(
            sts_status(&store).update_revision.as_deref(),
            Some(history::name_of(&revs[1]))
        );
    }
}
