use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, ObjectMeta, PersistentVolumeClaim,
    PersistentVolumeClaimVolumeSource, Pod, PodTemplateSpec, StatefulSetStatus, Volume,
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
    let vcts = current_spec
        .volume_claim_templates
        .as_deref()
        .unwrap_or_default();

    let pod_gvr = GroupVersionResource::pods();
    let mut owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, sts_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();

    owned.sort_by_key(pod_ordinal);

    let current_count = owned.len() as u64;
    // OrderedReady (the default) rolls out one pod at a time, gated on the
    // predecessor's readiness; Parallel acts on every ordinal at once.
    let ordered = current_spec.pod_management_policy.as_deref() != Some("Parallel");
    let when_scaled = current_spec
        .persistent_volume_claim_retention_policy
        .as_ref()
        .and_then(|p| p.when_scaled.as_deref())
        .unwrap_or("Retain");

    if current_count < desired {
        if ordered {
            // Walk ordinals in order: create the first gap, but only once every
            // lower ordinal is Ready. A not-ready predecessor stops the rollout
            // here — the next pod's Modified event resumes it.
            for ordinal in 0..desired {
                match owned.iter().find(|p| pod_ordinal(p) == ordinal) {
                    Some(pod) if crate::pod_ready(pod) => continue,
                    Some(_) => break,
                    None => {
                        create_pod(
                            store,
                            sts_name,
                            current_uid,
                            sts_ns,
                            template,
                            vcts,
                            ordinal,
                            &current_spec.service_name,
                        )?;
                        tracing::info!("sts '{sts_name}': created pod ordinal {ordinal} (ordered)");
                        break;
                    }
                }
            }
        } else {
            let existing: rustc_hash::FxHashSet<u64> = owned.iter().map(pod_ordinal).collect();
            let mut created = 0u64;
            for ordinal in 0..desired {
                if !existing.contains(&ordinal) {
                    create_pod(
                        store,
                        sts_name,
                        current_uid,
                        sts_ns,
                        template,
                        vcts,
                        ordinal,
                        &current_spec.service_name,
                    )?;
                    created += 1;
                }
            }
            tracing::info!("sts '{sts_name}': created {created} pods (parallel)");
        }
    } else if current_count > desired {
        // Everything at ordinal >= desired must go, highest first.
        let mut removable: Vec<&Pod> = owned.iter().filter(|p| pod_ordinal(p) >= desired).collect();
        removable.sort_by_key(|p| pod_ordinal(p));
        if ordered {
            if let Some(pod) = removable.last() {
                scale_down_pod(store, pod, sts_ns, sts_name, vcts, when_scaled)?;
            }
        } else {
            for pod in &removable {
                scale_down_pod(store, pod, sts_ns, sts_name, vcts, when_scaled)?;
            }
        }
    }

    let final_owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, sts_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();
    let total = final_owned.len() as i32;
    let ready = final_owned
        .iter()
        .filter(|p| crate::pod_ready(p))
        .count() as i32;
    update_sts_status(
        store,
        sts_name,
        sts_ns,
        total,
        ready,
        current.metadata.generation,
    )?;

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
    let pod_gvr = GroupVersionResource::pods();
    store.delete(&ResourceRef {
        gvr: &pod_gvr,
        namespace,
        name: pod_name,
    })?;

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

/// On StatefulSet deletion, honor `persistentVolumeClaimRetentionPolicy.whenDeleted`.
/// PVCs aren't owned by the STS (so they survive pod churn), so `Delete` must reap
/// them here explicitly; the default `Retain` leaves them.
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

#[allow(clippy::too_many_arguments)]
fn create_pod(
    store: &Store,
    sts_name: &str,
    sts_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
    volume_claim_templates: &[PersistentVolumeClaim],
    ordinal: u64,
    service_name: &str,
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

    let mut spec = template.spec.clone();

    // Stable network identity, mirroring the real StatefulSet controller: the
    // pod becomes addressable as `<pod>.<serviceName>.<ns>.svc.cluster.local`.
    // The endpoints controller only publishes a per-pod hostname when the
    // pod's `subdomain` names the Service, so both fields are needed for the
    // DNS record to exist — and that record is how StatefulSet peers find each
    // other (e.g. redis replicas dialing `redis-master-0.redis-headless...`).
    if let Some(spec) = spec.as_mut() {
        spec.hostname = Some(pod_name.clone());
        if !service_name.is_empty() {
            spec.subdomain = Some(service_name.to_string());
        }
    }

    // Each volumeClaimTemplate gets a per-ordinal PVC (`<tmpl>-<sts>-<ordinal>`)
    // and a matching `persistentVolumeClaim` volume injected into the pod. The
    // PVCs are intentionally *not* owned by the StatefulSet, so they (and their
    // data) outlive pod deletion — that's what makes the data stable.
    for vct in volume_claim_templates {
        let Some(tmpl_name) = vct.metadata.name.as_deref() else {
            continue;
        };
        let pvc_name = format!("{tmpl_name}-{sts_name}-{ordinal}");
        ensure_pvc(store, namespace, &pvc_name, vct)?;
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
            namespace: namespace.map(String::from),
            labels: Some(labels),
            owner_references: Some(vec![crate::owner_reference(
                "apps/v1",
                "StatefulSet",
                sts_name,
                sts_uid,
            )]),
            ..Default::default()
        },
        spec,
        status: None,
    };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &pod_name,
    };
    let mut pod_value = serde_json::to_value(&pod)?;
    crate::pod_admission::inject_sa_token(store, &mut pod_value);
    store.create(resource_ref, &pod_value)?;
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

fn update_sts_status(
    store: &Store,
    sts_name: &str,
    sts_ns: Option<&str>,
    total: i32,
    ready: i32,
    generation: Option<i64>,
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

    // `helm --wait` (and kstatus) gate StatefulSet readiness on more than
    // readyReplicas: observedGeneration must be set and >= generation, and
    // under the default RollingUpdate strategy updatedReplicas must reach the
    // replica count. r8s doesn't track revisions, so currentRevision/
    // updateRevision are left equal (both unset) to clear the rollout check.
    let new_status = StatefulSetStatus {
        replicas: total,
        ready_replicas: Some(ready),
        available_replicas: Some(ready),
        current_replicas: Some(total),
        updated_replicas: Some(total),
        observed_generation: Some(generation.unwrap_or(0).max(1)),
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
        let sts = create_sts(&store, "web", spec(3, json!({"podManagementPolicy": "Parallel"})));
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

        assert!(!exists(&store, &pods_gvr(), "web-1"), "highest ordinal reaped");
        assert!(exists(&store, &pods_gvr(), "web-0"), "kept replica survives");
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
}
