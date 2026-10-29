//! DaemonSet controller. r8s is single-node, so a DaemonSet wants one pod when
//! the node satisfies the template's nodeSelector and node affinity, kept at
//! the current ControllerRevision and replaced when the template changes
//! (RollingUpdate) or the pod fails.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::NodeSelectorTerm;
use r8s_store::{Store, StoreError, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    ControllerRevision, DaemonSet, DaemonSetSpec, DaemonSetStatus, GroupVersionResource, Node,
    ObjectMeta, Pod, PodTemplateSpec,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::history::{self, Parent};
use crate::{is_owned_by, pod_available, pod_phase, pod_ready, pod_terminating};

const DEFAULT_REVISION_HISTORY_LIMIT: i32 = 10;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("daemonset controller started");
    let gvr = GroupVersionResource::daemon_sets();

    reconcile_all(&store);

    let mut ds_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());
    let mut node_rx = store.watch(&GroupVersionResource::nodes());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("daemonset controller shutting down");
                return Ok(());
            }
            event = node_rx.recv() => {
                match event {
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
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
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted | WatchEventType::Modified) => {
                        let pod: Pod = match serde_json::from_value(event.object.as_ref().clone()) {
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
    let ds: DaemonSet = serde_json::from_value(ds_value.clone())?;
    let ds_name = ds
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("DaemonSet has no name"))?;
    let ds_ns = ds.metadata.namespace.as_deref();

    let gvr = GroupVersionResource::daemon_sets();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: ds_ns,
        name: ds_name,
    };
    let Some(current) = store.get_as::<DaemonSet>(&resource_ref)? else {
        return Ok(());
    };
    let uid = current.metadata.uid.as_deref().unwrap_or("");
    if uid.is_empty() {
        tracing::warn!("ds '{ds_name}': missing uid; skipping");
        return Ok(());
    }
    if current.metadata.deletion_timestamp.is_some() {
        return Ok(());
    }
    let spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("DaemonSet has no spec"))?;
    let parent = Parent {
        api_version: "apps/v1",
        kind: "DaemonSet",
        name: ds_name,
        namespace: ds_ns,
        uid,
    };
    let status_before = current.status.clone().unwrap_or_default();

    let (old, cur, collision_count) = construct_history(store, &parent, &current, spec)?;
    let hash = history::hash_of(&cur).to_string();

    let node = store
        .list_as::<Node>(&GroupVersionResource::nodes(), None)?
        .into_iter()
        .next();
    let should_run = node_should_run(&spec.template, node.as_ref());

    let pods = owned_pods(store, ds_ns, uid)?;
    let mut running: Vec<&Pod> = Vec::new();
    for pod in &pods {
        match pod_phase(pod) {
            _ if !should_run => {
                delete_pod(store, ds_ns, pod)?;
                tracing::info!(
                    "ds '{ds_name}': deleted pod {}: node no longer matches",
                    pod.metadata.name.as_deref().unwrap_or("")
                );
            }
            phase @ ("Failed" | "Succeeded") => {
                delete_pod(store, ds_ns, pod)?;
                tracing::info!(
                    "ds '{ds_name}': deleted {phase} pod {}",
                    pod.metadata.name.as_deref().unwrap_or("")
                );
            }
            _ => running.push(pod),
        }
    }

    let mut created = false;
    if !should_run {
    } else if running.is_empty() {
        create_pod(store, &parent, &spec.template, &hash)?;
        tracing::info!("ds '{ds_name}': created pod ({hash})");
        created = true;
    } else {
        for extra in &running[1..] {
            delete_pod(store, ds_ns, extra)?;
        }
        if running.len() > 1 {
            tracing::info!("ds '{ds_name}': deleted {} extra pods", running.len() - 1);
        }
    }

    let rolling = spec
        .update_strategy
        .as_ref()
        .and_then(|s| s.type_.as_deref())
        .unwrap_or("RollingUpdate")
        == "RollingUpdate";
    if rolling && !created && running.len() == 1 && pod_hash(running[0]) != hash {
        delete_pod(store, ds_ns, running[0])?;
        tracing::info!(
            "ds '{ds_name}': deleted pod {} for update to {hash}",
            running[0].metadata.name.as_deref().unwrap_or("")
        );
    }

    let fresh = owned_pods(store, ds_ns, uid)?;
    let oldest = fresh.first().filter(|_| should_run);
    let min_ready = spec.min_ready_seconds.unwrap_or(0);
    let ready = oldest.is_some_and(pod_ready);
    let available = ready && oldest.is_some_and(|p| pod_available(p, min_ready));
    let desired = i32::from(should_run);
    let status = DaemonSetStatus {
        desired_number_scheduled: desired,
        current_number_scheduled: i32::from(should_run && !fresh.is_empty()),
        number_misscheduled: i32::from(!should_run && !fresh.is_empty()),
        number_ready: i32::from(ready),
        number_available: Some(i32::from(available)),
        number_unavailable: Some(desired - i32::from(available)),
        updated_number_scheduled: Some(i32::from(oldest.is_some_and(|p| pod_hash(p) == hash))),
        observed_generation: Some(current.metadata.generation.unwrap_or(0).max(1)),
        collision_count: Some(collision_count),
        conditions: status_before.conditions,
    };
    write_status(store, &resource_ref, status)?;

    let live: BTreeSet<&str> = fresh.iter().map(pod_hash).collect();
    history::truncate(
        store,
        &parent,
        &old,
        spec.revision_history_limit
            .unwrap_or(DEFAULT_REVISION_HISTORY_LIMIT),
        |r| live.contains(history::hash_of(r)),
    )?;
    Ok(())
}

/// Whether the node runs this template: its labels satisfy `nodeSelector` and
/// one of the required node affinity terms. Without a Node object the single
/// node is assumed eligible.
fn node_should_run(template: &PodTemplateSpec, node: Option<&Node>) -> bool {
    let Some(node) = node else {
        return true;
    };
    let labels = node.metadata.labels.clone().unwrap_or_default();
    let spec = template.spec.as_ref();
    if let Some(selector) = spec.and_then(|s| s.node_selector.as_ref())
        && selector.iter().any(|(k, v)| labels.get(k) != Some(v))
    {
        return false;
    }
    let terms = spec
        .and_then(|s| s.affinity.as_ref())
        .and_then(|a| a.node_affinity.as_ref())
        .and_then(|n| {
            n.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .map(|r| r.node_selector_terms.as_slice())
        .unwrap_or_default();
    terms.is_empty() || terms.iter().any(|t| term_matches(t, node, &labels))
}

/// A node selector term: every expression (on labels) and field (on
/// `metadata.name`) must hold.
fn term_matches(term: &NodeSelectorTerm, node: &Node, labels: &BTreeMap<String, String>) -> bool {
    let name = node.metadata.name.clone().unwrap_or_default();
    let fields = BTreeMap::from([("metadata.name".to_string(), name)]);
    let holds = |reqs: Option<&Vec<k8s_openapi::api::core::v1::NodeSelectorRequirement>>,
                 values: &BTreeMap<String, String>| {
        reqs.into_iter().flatten().all(|req| {
            let actual = values.get(&req.key);
            let wanted = req.values.as_deref().unwrap_or_default();
            match req.operator.as_str() {
                "In" => actual.is_some_and(|a| wanted.contains(a)),
                "NotIn" => !actual.is_some_and(|a| wanted.contains(a)),
                "Exists" => actual.is_some(),
                "DoesNotExist" => actual.is_none(),
                "Gt" | "Lt" => {
                    let (Some(a), Some(w)) = (
                        actual.and_then(|a| a.parse::<i64>().ok()),
                        wanted.first().and_then(|w| w.parse::<i64>().ok()),
                    ) else {
                        return false;
                    };
                    if req.operator == "Gt" { a > w } else { a < w }
                }
                _ => false,
            }
        })
    };
    holds(term.match_expressions.as_ref(), labels) && holds(term.match_fields.as_ref(), &fields)
}

/// Split the owned revisions into those recording the current template and
/// older ones, and return the single current revision: created if missing,
/// deduplicated (pods relabelled to the survivor's hash) if several, and
/// renumbered above every old one.
fn construct_history(
    store: &Store,
    parent: &Parent,
    ds: &DaemonSet,
    spec: &DaemonSetSpec,
) -> anyhow::Result<(Vec<ControllerRevision>, ControllerRevision, i32)> {
    let data = history::template_patch(&spec.template);
    let (mut current, old): (Vec<ControllerRevision>, Vec<ControllerRevision>) =
        history::owned_revisions(store, parent)?
            .into_iter()
            .partition(|r| history::data_of(r) == Some(&data));
    let next = old.iter().map(|r| r.revision).max().unwrap_or(0) + 1;
    let mut collision_count = ds
        .status
        .as_ref()
        .and_then(|s| s.collision_count)
        .unwrap_or(0);

    let Some(keep) = current.pop() else {
        let rev = history::create_revision(
            store,
            parent,
            spec.template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.clone())
                .unwrap_or_default(),
            ds.metadata.annotations.clone(),
            data,
            next,
            &mut collision_count,
        )?;
        return Ok((old, rev, collision_count));
    };
    for dup in &current {
        relabel_pods(
            store,
            parent,
            history::hash_of(dup),
            history::hash_of(&keep),
        )?;
        history::delete_revision(store, parent, dup)?;
    }
    let cur = if keep.revision < next {
        history::set_revision_number(store, parent, &keep, next)?
    } else {
        keep
    };
    Ok((old, cur, collision_count))
}

fn relabel_pods(store: &Store, parent: &Parent, from: &str, to: &str) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::pods();
    for pod in owned_pods(store, parent.namespace, parent.uid)? {
        if pod_hash(&pod) != from {
            continue;
        }
        let Some(name) = pod.metadata.name.as_deref() else {
            continue;
        };
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: parent.namespace,
            name,
        };
        let Some(mut value) = store.get(&rref)? else {
            continue;
        };
        value["metadata"]["labels"][history::REVISION_HASH_LABEL] = serde_json::json!(to);
        store.update(&rref, &value)?;
    }
    Ok(())
}

/// Owned pods not being deleted, oldest first.
fn owned_pods(store: &Store, namespace: Option<&str>, uid: &str) -> anyhow::Result<Vec<Pod>> {
    let mut pods: Vec<Pod> = store
        .list_as::<Pod>(&GroupVersionResource::pods(), namespace)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, uid) && !pod_terminating(p))
        .collect();
    pods.sort_by(|a, b| {
        a.metadata
            .creation_timestamp
            .cmp(&b.metadata.creation_timestamp)
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });
    Ok(pods)
}

fn pod_hash(pod: &Pod) -> &str {
    pod.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(history::REVISION_HASH_LABEL))
        .map(String::as_str)
        .unwrap_or("")
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

fn create_pod(
    store: &Store,
    parent: &Parent,
    template: &PodTemplateSpec,
    hash: &str,
) -> anyhow::Result<()> {
    let pod_name = format!("{}-{}", parent.name, crate::random_suffix());
    let mut labels = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    labels.insert(history::REVISION_HASH_LABEL.into(), hash.to_string());
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
        spec: template.spec.clone(),
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
    status: DaemonSetStatus,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn ds_ref<'a>(gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: Some("default"),
            name,
        }
    }

    fn create_ds(store: &Store, image: &str) -> Value {
        store
            .create(
                ds_ref(&GroupVersionResource::daemon_sets(), "agent"),
                &json!({
                    "apiVersion": "apps/v1", "kind": "DaemonSet",
                    "metadata": {"name": "agent", "namespace": "default"},
                    "spec": {
                        "selector": {"matchLabels": {"app": "agent"}},
                        "template": {
                            "metadata": {"labels": {"app": "agent"}},
                            "spec": {"containers": [{"name": "c", "image": image}]}
                        }
                    }
                }),
            )
            .unwrap()
    }

    fn pods(store: &Store) -> Vec<Pod> {
        store
            .list_as::<Pod>(&GroupVersionResource::pods(), Some("default"))
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

    fn set_pod_status(store: &Store, name: &str, status: Value) {
        let gvr = GroupVersionResource::pods();
        let rref = ds_ref(&gvr, name);
        let mut pod = store.get(&rref).unwrap().unwrap();
        pod["status"] = status;
        store.update(&rref, &pod).unwrap();
    }

    fn ds_status(store: &Store) -> DaemonSetStatus {
        store
            .get_as::<DaemonSet>(&ds_ref(&GroupVersionResource::daemon_sets(), "agent"))
            .unwrap()
            .unwrap()
            .status
            .unwrap()
    }

    #[test]
    fn creates_one_pod_labelled_with_the_revision_hash() {
        let (_d, store) = store();
        let ds = create_ds(&store, "img:1");
        reconcile_ds(&store, &ds).unwrap();

        let revs = revisions(&store);
        assert_eq!(revs.len(), 1);
        assert_eq!(revs[0].revision, 1);
        let hash = history::hash_of(&revs[0]).to_string();
        assert_eq!(history::name_of(&revs[0]), format!("agent-{hash}"));

        let created = pods(&store);
        assert_eq!(created.len(), 1);
        assert_eq!(pod_hash(&created[0]), hash);

        let status = ds_status(&store);
        assert_eq!(status.desired_number_scheduled, 1);
        assert_eq!(status.current_number_scheduled, 1);
        assert_eq!(status.number_ready, 0);
        assert_eq!(status.updated_number_scheduled, Some(1));
        assert_eq!(status.number_unavailable, Some(1));

        // A second pass is a no-op.
        reconcile_ds(&store, &ds).unwrap();
        assert_eq!(pods(&store).len(), 1);
        assert_eq!(revisions(&store).len(), 1);
    }

    #[test]
    fn status_follows_pod_readiness_and_keeps_conditions() {
        let (_d, store) = store();
        let ds = create_ds(&store, "img:1");
        reconcile_ds(&store, &ds).unwrap();
        let name = pods(&store)[0].metadata.name.clone().unwrap();
        set_pod_status(
            &store,
            &name,
            json!({"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}),
        );
        let gvr = GroupVersionResource::daemon_sets();
        let rref = ds_ref(&gvr, "agent");
        let mut value = store.get(&rref).unwrap().unwrap();
        value["status"]["conditions"] = json!([{"type": "StatusUpdate", "status": "True"}]);
        store.update(&rref, &value).unwrap();

        reconcile_ds(&store, &ds).unwrap();
        let status = ds_status(&store);
        assert_eq!(status.number_ready, 1);
        assert_eq!(status.number_available, Some(1));
        assert_eq!(status.number_unavailable, Some(0));
        assert_eq!(status.conditions.unwrap()[0].type_, "StatusUpdate");
    }

    #[test]
    fn template_change_rolls_the_pod_and_keeps_history() {
        let (_d, store) = store();
        let ds = create_ds(&store, "img:1");
        reconcile_ds(&store, &ds).unwrap();
        let old_pod = pods(&store)[0].metadata.name.clone().unwrap();
        let old_hash = pod_hash(&pods(&store)[0]).to_string();

        let gvr = GroupVersionResource::daemon_sets();
        let rref = ds_ref(&gvr, "agent");
        let mut value = store.get(&rref).unwrap().unwrap();
        value["spec"]["template"]["spec"]["containers"][0]["image"] = json!("img:2");
        let ds = store.update(&rref, &value).unwrap();

        reconcile_ds(&store, &ds).unwrap();
        assert!(pods(&store).is_empty(), "old pod deleted for update");
        let revs = revisions(&store);
        assert_eq!(revs.len(), 2, "old revision kept");
        assert_eq!(revs[1].revision, 2);

        reconcile_ds(&store, &ds).unwrap();
        let new_pods = pods(&store);
        assert_eq!(new_pods.len(), 1);
        assert_ne!(new_pods[0].metadata.name.as_deref(), Some(old_pod.as_str()));
        assert_ne!(pod_hash(&new_pods[0]), old_hash);
        assert_eq!(pod_hash(&new_pods[0]), history::hash_of(&revs[1]));

        // Rolling back reuses the first revision, renumbered to 3.
        let mut value = store.get(&rref).unwrap().unwrap();
        value["spec"]["template"]["spec"]["containers"][0]["image"] = json!("img:1");
        let ds = store.update(&rref, &value).unwrap();
        reconcile_ds(&store, &ds).unwrap();
        let revs = revisions(&store);
        assert_eq!(revs.len(), 2);
        assert_eq!(revs[1].revision, 3);
        assert_eq!(history::hash_of(&revs[1]), old_hash);
    }

    #[test]
    fn failed_pod_is_replaced() {
        let (_d, store) = store();
        let ds = create_ds(&store, "img:1");
        reconcile_ds(&store, &ds).unwrap();
        let name = pods(&store)[0].metadata.name.clone().unwrap();
        set_pod_status(&store, &name, json!({"phase": "Failed"}));

        reconcile_ds(&store, &ds).unwrap();
        let after = pods(&store);
        assert_eq!(after.len(), 1);
        assert_ne!(after[0].metadata.name.as_deref(), Some(name.as_str()));
    }

    #[test]
    fn node_selector_gates_the_pod() {
        let (_d, store) = store();
        store
            .create(
                ResourceRef {
                    gvr: &GroupVersionResource::nodes(),
                    namespace: None,
                    name: "r8s-node",
                },
                &json!({"apiVersion": "v1", "kind": "Node",
                        "metadata": {"name": "r8s-node", "labels": {"color": "green"}}}),
            )
            .unwrap();
        let gvr = GroupVersionResource::daemon_sets();
        let mut ds = create_ds(&store, "img:1");
        ds["spec"]["template"]["spec"]["nodeSelector"] = json!({"color": "blue"});
        let ds = store.update(&ds_ref(&gvr, "agent"), &ds).unwrap();

        reconcile_ds(&store, &ds).unwrap();
        assert!(pods(&store).is_empty(), "node does not match");
        let status = ds_status(&store);
        assert_eq!(status.desired_number_scheduled, 0);
        assert_eq!(status.number_unavailable, Some(0));

        let nodes = GroupVersionResource::nodes();
        let nref = ResourceRef {
            gvr: &nodes,
            namespace: None,
            name: "r8s-node",
        };
        let mut node = store.get(&nref).unwrap().unwrap();
        node["metadata"]["labels"]["color"] = json!("blue");
        store.update(&nref, &node).unwrap();
        reconcile_ds(&store, &ds).unwrap();
        assert_eq!(pods(&store).len(), 1);
        assert_eq!(ds_status(&store).desired_number_scheduled, 1);

        let mut node = store.get(&nref).unwrap().unwrap();
        node["metadata"]["labels"]["color"] = json!("red");
        store.update(&nref, &node).unwrap();
        reconcile_ds(&store, &ds).unwrap();
        assert!(
            pods(&store).is_empty(),
            "pod removed when the node stops matching"
        );
    }

    #[test]
    fn duplicate_current_revisions_collapse_onto_the_newest() {
        let (_d, store) = store();
        let ds = create_ds(&store, "img:1");
        reconcile_ds(&store, &ds).unwrap();
        let first = revisions(&store).remove(0);
        let uid = ds["metadata"]["uid"].as_str().unwrap().to_string();

        let gvr = GroupVersionResource::controller_revisions();
        let mut dup = serde_json::to_value(&first).unwrap();
        dup["metadata"]["name"] = json!("agent-handmade");
        dup["metadata"]["labels"][history::REVISION_HASH_LABEL] = json!("handmade");
        dup["metadata"]["ownerReferences"][0]["uid"] = json!(uid);
        dup["metadata"].as_object_mut().unwrap().remove("uid");
        dup["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("resourceVersion");
        dup["revision"] = json!(2);
        store.create(ds_ref(&gvr, "agent-handmade"), &dup).unwrap();

        reconcile_ds(&store, &ds).unwrap();
        let revs = revisions(&store);
        assert_eq!(revs.len(), 1);
        assert_eq!(history::name_of(&revs[0]), "agent-handmade");
        assert_eq!(pod_hash(&pods(&store)[0]), "handmade");
    }
}
