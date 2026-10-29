pub mod certs;
pub mod crd;
pub mod cronjob;
pub mod daemonset;
pub mod deployment;
pub mod disruption;
pub mod endpoints;
pub mod foreground_gc;
pub mod gc;
pub mod history;
pub mod job;
pub mod manager;
pub mod namespace;
pub mod pod_admission;
pub mod provisioner;
pub mod quantity;
pub mod quota;
pub mod replicaset;
pub mod replicationcontroller;
pub mod serviceaccount;
pub mod statefulset;
pub mod taint_eviction;

pub use manager::ControllerManager;

use std::collections::BTreeMap;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{GroupVersionResource, LabelSelector, ObjectMeta, OwnerReference, Pod};

/// Status value shared by every k8s condition when the condition holds.
pub(crate) const STATUS_TRUE: &str = "True";
/// `type` of the pod condition that gates readiness gates, endpoint membership,
/// and rollout progress.
pub(crate) const CONDITION_READY: &str = "Ready";
/// `Secret.type` the serviceaccount controller stamps on the token Secret it
/// mints; pod admission selects the token to mount by matching it.
pub(crate) const SA_TOKEN_SECRET_TYPE: &str = "kubernetes.io/service-account-token";

pub(crate) fn is_owned_by(meta: &ObjectMeta, owner_uid: &str) -> bool {
    meta.owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|r| r.uid == owner_uid)
}

pub(crate) fn find_owner<'a>(meta: &'a ObjectMeta, kind: &str) -> Option<&'a OwnerReference> {
    meta.owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|r| r.kind == kind)
}

/// A controller owner reference — `controller: true, blockOwnerDeletion: true` —
/// as stamped on pods/replicasets/jobs the controllers create. Callers own the
/// UID/name lookup; this only removes the boilerplate literal that was otherwise
/// copy-pasted across every controller.
pub(crate) fn owner_reference(
    api_version: &str,
    kind: &str,
    name: &str,
    uid: &str,
) -> OwnerReference {
    OwnerReference {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// A pod counts as ready only when its `Ready` condition is `True` — not merely
/// when its phase is `Running`. A crash-looping container is `Running`/`0/1`,
/// and counting it as ready makes `kubectl get` lie and lets `helm --wait`
/// believe a broken rollout succeeded.
pub fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| c.type_ == CONDITION_READY && c.status == STATUS_TRUE)
        })
}

pub(crate) fn pod_phase(pod: &Pod) -> &str {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("")
}

pub(crate) fn pod_running_and_ready(pod: &Pod) -> bool {
    pod_phase(pod) == "Running" && pod_ready(pod)
}

pub(crate) fn pod_terminating(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_some()
}

/// Ready for at least `min_ready_seconds` (upstream `IsPodAvailable`).
pub(crate) fn pod_available(pod: &Pod, min_ready_seconds: i32) -> bool {
    if !pod_ready(pod) {
        return false;
    }
    if min_ready_seconds <= 0 {
        return true;
    }
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == CONDITION_READY))
        .and_then(|c| c.last_transition_time.as_ref())
        .is_some_and(|t| {
            t.0 + chrono::Duration::seconds(i64::from(min_ready_seconds)) < chrono::Utc::now()
        })
}

/// True when `job` carries a `status.conditions` entry of one of `types` set to
/// `True`. `["Complete", "Failed"]` tests terminality; `["Complete"]` tests
/// success. Shared by the job and cronjob controllers so both read job outcome
/// the same way.
pub(crate) fn job_condition_true(job: &r8s_types::Job, types: &[&str]) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or_default()
        .iter()
        .any(|c| types.contains(&c.type_.as_str()) && c.status == STATUS_TRUE)
}

/// `metav1.LabelSelector` against a label map: every `matchLabels` entry must
/// be present with that value and every `matchExpressions` requirement must
/// hold; an absent key satisfies `NotIn` and `DoesNotExist`. An empty selector
/// matches nothing, so no controller ever claims a whole namespace.
pub fn selector_matches(
    selector: &LabelSelector,
    labels: Option<&BTreeMap<String, String>>,
) -> bool {
    let empty = BTreeMap::new();
    let labels = labels.unwrap_or(&empty);
    let match_labels = selector.match_labels.as_ref().filter(|m| !m.is_empty());
    let requirements = selector.match_expressions.as_deref().unwrap_or_default();
    if match_labels.is_none() && requirements.is_empty() {
        return false;
    }
    if let Some(wanted) = match_labels
        && wanted.iter().any(|(k, v)| labels.get(k) != Some(v))
    {
        return false;
    }
    requirements.iter().all(|req| {
        let actual = labels.get(&req.key);
        let values = req.values.as_deref().unwrap_or_default();
        match req.operator.as_str() {
            "In" => actual.is_some_and(|a| values.contains(a)),
            "NotIn" => !actual.is_some_and(|a| values.contains(a)),
            "Exists" => actual.is_some(),
            "DoesNotExist" => actual.is_none(),
            _ => false,
        }
    })
}

/// Controllers of `gvr` in the pod's namespace whose selector (as read by
/// `selector_of`) matches an orphan pod's labels: the ones that would adopt it.
pub(crate) fn orphan_candidates(
    store: &Store,
    gvr: &GroupVersionResource,
    pod: &Pod,
    selector_of: impl Fn(&serde_json::Value) -> Option<LabelSelector>,
) -> Vec<serde_json::Value> {
    if pod.metadata.deletion_timestamp.is_some()
        || pod
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|r| r.controller == Some(true))
    {
        return Vec::new();
    }
    store
        .list(
            gvr,
            pod.metadata.namespace.as_deref(),
            None,
            None,
            None,
            None,
        )
        .map(|r| r.items)
        .unwrap_or_default()
        .into_iter()
        .filter(|owner| {
            selector_of(owner).is_some_and(|s| selector_matches(&s, pod.metadata.labels.as_ref()))
        })
        .collect()
}

/// `spec.selector` of a workload object as a `LabelSelector`.
pub(crate) fn spec_selector(owner: &serde_json::Value) -> Option<LabelSelector> {
    serde_json::from_value(owner.get("spec")?.get("selector")?.clone()).ok()
}

/// The pods in `namespace` that `owner` controls once orphans matching the
/// selector are adopted and owned pods that stopped matching are released.
pub(crate) fn claim_pods(
    store: &Store,
    namespace: Option<&str>,
    owner: &OwnerReference,
    owner_terminating: bool,
    matches: impl Fn(&Pod) -> bool,
) -> anyhow::Result<Vec<Pod>> {
    claim(
        store,
        &GroupVersionResource::pods(),
        namespace,
        owner,
        owner_terminating,
        matches,
    )
}

/// The objects of `gvr` in `namespace` that `owner` controls once orphans
/// matching `matches` are adopted and owned objects that stopped matching are
/// released (upstream's ControllerRefManager). Objects controlled by another
/// owner are left alone, and nothing is adopted while either side is
/// terminating.
pub(crate) fn claim<T>(
    store: &Store,
    gvr: &GroupVersionResource,
    namespace: Option<&str>,
    owner: &OwnerReference,
    owner_terminating: bool,
    matches: impl Fn(&T) -> bool,
) -> anyhow::Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + k8s_openapi::Metadata<Ty = ObjectMeta>,
{
    let who = format!("{} '{}'", owner.kind.to_lowercase(), owner.name);
    let what = gvr.resource.trim_end_matches('s');
    let mut owned = Vec::new();
    for mut value in store.list(gvr, namespace, None, None, None, None)?.items {
        let Ok(obj) = serde_json::from_value::<T>(value.clone()) else {
            continue;
        };
        let meta = obj.metadata();
        let refs = meta.owner_references.as_deref().unwrap_or_default();
        // Only a `controller: true` reference makes an object owned; a plain
        // owner reference is just a GC edge, as upstream's GetControllerOf.
        let controller = refs.iter().find(|r| r.controller == Some(true));
        let name = meta.name.clone().unwrap_or_default();
        let terminating = meta.deletion_timestamp.is_some();
        let rref = ResourceRef {
            gvr,
            namespace,
            name: &name,
        };
        match controller {
            Some(c) if c.uid == owner.uid => {
                if matches(&obj) {
                    owned.push(obj);
                } else if !terminating {
                    let kept: Vec<OwnerReference> = refs
                        .iter()
                        .filter(|r| r.uid != owner.uid)
                        .cloned()
                        .collect();
                    set_owner_references(&mut value, &kept)?;
                    match store.update(&rref, &value) {
                        Ok(_) => tracing::info!("{who}: released {what} '{name}'"),
                        Err(e) => tracing::warn!("{who}: release {what} '{name}': {e}"),
                    }
                }
            }
            Some(_) => {}
            None => {
                if owner_terminating || terminating || !matches(&obj) {
                    continue;
                }
                // A plain reference to this owner becomes the controller reference.
                let mut kept: Vec<OwnerReference> = refs
                    .iter()
                    .filter(|r| r.uid != owner.uid)
                    .cloned()
                    .collect();
                kept.push(owner.clone());
                set_owner_references(&mut value, &kept)?;
                match store.update(&rref, &value) {
                    Ok(updated) => {
                        tracing::info!("{who}: adopted {what} '{name}'");
                        if let Ok(o) = serde_json::from_value::<T>(updated) {
                            owned.push(o);
                        }
                    }
                    Err(e) => tracing::warn!("{who}: adopt {what} '{name}': {e}"),
                }
            }
        }
    }
    Ok(owned)
}

fn set_owner_references(
    value: &mut serde_json::Value,
    refs: &[OwnerReference],
) -> anyhow::Result<()> {
    let meta = value
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
        .ok_or_else(|| anyhow::anyhow!("object has no metadata"))?;
    if refs.is_empty() {
        meta.remove("ownerReferences");
    } else {
        meta.insert("ownerReferences".to_string(), serde_json::to_value(refs)?);
    }
    Ok(())
}

/// A `ReplicaFailure=True/FailedCreate` condition for `owner`'s status, keeping
/// the transition time of an existing one.
pub(crate) fn replica_failure_condition(
    owner: &serde_json::Value,
    message: &str,
) -> serde_json::Value {
    // An existing condition with the same status and reason stays as it is,
    // as upstream's SetCondition does; the message carries a generated pod
    // name, so refreshing it would make every refused create a status write.
    let existing = owner["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| {
            c["type"] == "ReplicaFailure" && c["status"] == "True" && c["reason"] == "FailedCreate"
        });
    if let Some(existing) = existing {
        return existing.clone();
    }
    serde_json::json!({
        "type": "ReplicaFailure",
        "status": "True",
        "reason": "FailedCreate",
        "message": message,
        "lastTransitionTime": r8s_types::Time(chrono::Utc::now()),
    })
}

pub(crate) fn random_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

#[cfg(test)]
mod orphan_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn orphan_candidates_are_selector_matches_without_a_controller() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let gvr = GroupVersionResource::jobs();
        for (name, labels) in [
            ("match", json!({"job": "a"})),
            ("other", json!({"job": "b"})),
        ] {
            store
                .create(
                    ResourceRef {
                        gvr: &gvr,
                        namespace: Some("ns"),
                        name,
                    },
                    &json!({"metadata": {"name": name, "namespace": "ns"},
                            "spec": {"selector": {"matchLabels": labels}}}),
                )
                .unwrap();
        }
        let orphan: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "labels": {"job": "a"}}
        }))
        .unwrap();
        let found = orphan_candidates(&store, &gvr, &orphan, spec_selector);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["metadata"]["name"], "match");

        let owned: Pod = serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "labels": {"job": "a"},
                         "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job",
                                              "name": "x", "uid": "u", "controller": true}]}
        }))
        .unwrap();
        assert!(orphan_candidates(&store, &gvr, &owned, spec_selector).is_empty());
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn put_pod(store: &Store, name: &str, labels: serde_json::Value, owners: serde_json::Value) {
        store
            .create(
                ResourceRef {
                    gvr: &GroupVersionResource::pods(),
                    namespace: Some("ns"),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": name, "namespace": "ns", "labels": labels, "ownerReferences": owners},
                    "spec": {"containers": [{"name": "c", "image": "x"}]}
                }),
            )
            .unwrap();
    }

    fn controller_uid(store: &Store, name: &str) -> Option<String> {
        let pod: Pod = store
            .get_as(&ResourceRef {
                gvr: &GroupVersionResource::pods(),
                namespace: Some("ns"),
                name,
            })
            .unwrap()
            .unwrap();
        pod.metadata
            .owner_references
            .unwrap_or_default()
            .into_iter()
            .find(|r| r.controller == Some(true))
            .map(|r| r.uid)
    }

    fn app_selector(value: &str) -> LabelSelector {
        LabelSelector {
            match_labels: Some(BTreeMap::from([("app".to_string(), value.to_string())])),
            ..Default::default()
        }
    }

    #[test]
    fn selector_semantics() {
        let labels = BTreeMap::from([("app".to_string(), "web".to_string())]);
        assert!(selector_matches(&app_selector("web"), Some(&labels)));
        assert!(!selector_matches(&app_selector("db"), Some(&labels)));
        assert!(
            !selector_matches(&LabelSelector::default(), Some(&labels)),
            "empty matches nothing"
        );
        let not_in: LabelSelector = serde_json::from_value(json!({"matchExpressions": [
            {"key": "tier", "operator": "NotIn", "values": ["cache"]}]}))
        .unwrap();
        assert!(
            selector_matches(&not_in, Some(&labels)),
            "absent key satisfies NotIn"
        );
        assert!(selector_matches(&not_in, None));
    }

    #[test]
    fn adopts_orphans_releases_strays_ignores_others() {
        let (_d, store) = store();
        let owner = owner_reference("apps/v1", "ReplicaSet", "rs", "rs-uid");
        let other = json!([{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "x", "uid": "other-uid", "controller": true}]);
        put_pod(&store, "orphan-match", json!({"app": "web"}), json!([]));
        put_pod(&store, "orphan-nomatch", json!({"app": "db"}), json!([]));
        put_pod(
            &store,
            "owned-stray",
            json!({"app": "db"}),
            serde_json::to_value(vec![owner.clone()]).unwrap(),
        );
        put_pod(
            &store,
            "owned-ok",
            json!({"app": "web"}),
            serde_json::to_value(vec![owner.clone()]).unwrap(),
        );
        put_pod(&store, "theirs", json!({"app": "web"}), other);
        put_pod(
            &store,
            "plain-ref-match",
            json!({"app": "web"}),
            json!([{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"}]),
        );

        let sel = app_selector("web");
        let owned = claim_pods(&store, Some("ns"), &owner, false, |p| {
            selector_matches(&sel, p.metadata.labels.as_ref())
        })
        .unwrap();
        let mut names: Vec<_> = owned
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, ["orphan-match", "owned-ok", "plain-ref-match"]);
        let plain = store
            .get_as::<Pod>(&ResourceRef {
                gvr: &GroupVersionResource::pods(),
                namespace: Some("ns"),
                name: "plain-ref-match",
            })
            .unwrap()
            .unwrap();
        let refs = plain.metadata.owner_references.unwrap();
        assert_eq!(
            refs.len(),
            1,
            "the plain reference became the controller one"
        );
        assert_eq!(refs[0].controller, Some(true));
        assert_eq!(
            controller_uid(&store, "orphan-match").as_deref(),
            Some("rs-uid")
        );
        assert_eq!(controller_uid(&store, "owned-stray"), None, "released");
        assert_eq!(controller_uid(&store, "orphan-nomatch"), None);
        assert_eq!(
            controller_uid(&store, "theirs").as_deref(),
            Some("other-uid")
        );
    }

    #[test]
    fn terminating_owner_adopts_nothing() {
        let (_d, store) = store();
        let owner = owner_reference("apps/v1", "ReplicaSet", "rs", "rs-uid");
        put_pod(&store, "orphan", json!({"app": "web"}), json!([]));
        let sel = app_selector("web");
        let owned = claim_pods(&store, Some("ns"), &owner, true, |p| {
            selector_matches(&sel, p.metadata.labels.as_ref())
        })
        .unwrap();
        assert!(owned.is_empty());
        assert_eq!(controller_uid(&store, "orphan"), None);
    }
}
