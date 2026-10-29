//! Deployment controller: one ReplicaSet per pod template, rolled out under
//! RollingUpdate surge/unavailability bounds or by Recreate, and scaled
//! proportionally across ReplicaSets while a rollout is in progress.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use r8s_store::{Store, StoreError, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    Deployment, DeploymentCondition, DeploymentSpec, DeploymentStatus, GroupVersionResource,
    IntOrString, ObjectMeta, Pod, PodTemplateSpec, ReplicaSet, ReplicaSetSpec,
};
use rustc_hash::FxHasher;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{is_owned_by, pod_phase, selector_matches};

const REVISION_ANNOTATION: &str = "deployment.kubernetes.io/revision";
const REVISION_HISTORY_ANNOTATION: &str = "deployment.kubernetes.io/revision-history";
const DESIRED_REPLICAS_ANNOTATION: &str = "deployment.kubernetes.io/desired-replicas";
const MAX_REPLICAS_ANNOTATION: &str = "deployment.kubernetes.io/max-replicas";
const LAST_APPLIED_ANNOTATION: &str = "kubectl.kubernetes.io/last-applied-configuration";
const POD_TEMPLATE_HASH: &str = "pod-template-hash";
const DEFAULT_REVISION_HISTORY_LIMIT: i32 = 10;
const DEFAULT_MAX_SURGE_PERCENT: f64 = 25.0;
const DEFAULT_MAX_UNAVAILABLE_PERCENT: f64 = 25.0;

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
                            log_reconcile_error(&e);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = rs_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Modified | WatchEventType::Deleted) => {
                        let rs: ReplicaSet = match serde_json::from_value(event.object.as_ref().clone()) {
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
                                log_reconcile_error(&e);
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

/// A reconcile error; the object vanishing mid-reconcile is not one worth a
/// warning.
fn log_reconcile_error(e: &anyhow::Error) {
    if matches!(
        e.downcast_ref::<r8s_store::StoreError>(),
        Some(r8s_store::StoreError::NotFound { .. })
    ) {
        tracing::debug!("deployment reconcile: {e}");
    } else {
        tracing::warn!("deployment reconcile error: {e}");
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
            log_reconcile_error(&e);
        }
    }
}

/// The deployment's identity and resolved rollout bounds for one pass.
struct Params<'a> {
    name: &'a str,
    namespace: Option<&'a str>,
    uid: &'a str,
    spec: &'a DeploymentSpec,
    desired: i32,
    rolling: bool,
    max_surge: i32,
    max_unavailable: i32,
}

impl Params<'_> {
    fn new<'a>(
        name: &'a str,
        namespace: Option<&'a str>,
        uid: &'a str,
        spec: &'a DeploymentSpec,
    ) -> Params<'a> {
        let desired = spec.replicas.unwrap_or(1).max(0);
        let rolling = spec
            .strategy
            .as_ref()
            .and_then(|s| s.type_.as_deref())
            .unwrap_or("RollingUpdate")
            == "RollingUpdate";
        let (surge, unavailable) = resolve_fenceposts(spec, desired);
        Params {
            name,
            namespace,
            uid,
            spec,
            desired,
            rolling,
            max_surge: if rolling { surge } else { 0 },
            max_unavailable: if rolling && desired > 0 {
                unavailable.min(desired)
            } else {
                0
            },
        }
    }

    fn min_available(&self) -> i32 {
        self.desired - self.max_unavailable
    }

    fn max_replicas(&self) -> i32 {
        self.desired + self.max_surge
    }

    fn rs_ref<'a>(&'a self, gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: self.namespace,
            name,
        }
    }
}

/// maxSurge (rounded up) and maxUnavailable (rounded down) as counts, each
/// defaulting to 25%; both zero becomes one unavailable so a rollout can move.
fn resolve_fenceposts(spec: &DeploymentSpec, desired: i32) -> (i32, i32) {
    let ru = spec
        .strategy
        .as_ref()
        .and_then(|s| s.rolling_update.as_ref());
    let surge = ru
        .and_then(|r| r.max_surge.as_ref())
        .map(|v| resolve_intstr(v, desired, true))
        .unwrap_or_else(|| ((desired as f64) * (DEFAULT_MAX_SURGE_PERCENT / 100.0)).ceil() as i32)
        .max(0);
    let unavailable = ru
        .and_then(|r| r.max_unavailable.as_ref())
        .map(|v| resolve_intstr(v, desired, false))
        .unwrap_or_else(|| {
            ((desired as f64) * (DEFAULT_MAX_UNAVAILABLE_PERCENT / 100.0)).floor() as i32
        })
        .max(0);
    if surge == 0 && unavailable == 0 {
        (0, 1)
    } else {
        (surge, unavailable)
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

fn rs_name(rs: &ReplicaSet) -> &str {
    rs.metadata.name.as_deref().unwrap_or("")
}

fn rs_replicas(rs: &ReplicaSet) -> i32 {
    rs.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1)
}

fn rs_status_replicas(rs: &ReplicaSet) -> i32 {
    rs.status.as_ref().map(|s| s.replicas).unwrap_or(0)
}

fn rs_ready(rs: &ReplicaSet) -> i32 {
    rs.status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0)
}

fn rs_available(rs: &ReplicaSet) -> i32 {
    rs.status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0)
}

fn rs_annotation<'a>(rs: &'a ReplicaSet, key: &str) -> Option<&'a str> {
    rs.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .map(String::as_str)
}

fn rs_revision(rs: &ReplicaSet) -> i64 {
    rs_annotation(rs, REVISION_ANNOTATION)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

fn rs_annotation_i32(rs: &ReplicaSet, key: &str) -> Option<i32> {
    rs_annotation(rs, key)
        .and_then(|s| s.parse::<i32>().ok())
        .filter(|n| *n >= 0)
}

fn is_active(rs: &ReplicaSet) -> bool {
    rs_replicas(rs) > 0
}

fn by_creation(a: &ReplicaSet, b: &ReplicaSet) -> Ordering {
    a.metadata
        .creation_timestamp
        .cmp(&b.metadata.creation_timestamp)
        .then_with(|| a.metadata.name.cmp(&b.metadata.name))
}

fn template_hash(template: &PodTemplateSpec) -> String {
    let canonical = serde_json::to_string(&template_without_hash(template)).unwrap_or_default();
    let mut hasher = FxHasher::default();
    canonical.hash(&mut hasher);
    format!("{:010x}", hasher.finish() & 0xff_ffff_ffff)
}

fn template_without_hash(template: &PodTemplateSpec) -> serde_json::Value {
    let mut value = serde_json::to_value(template).unwrap_or_default();
    if let Some(labels) = value
        .get_mut("metadata")
        .and_then(|m| m.get_mut("labels"))
        .and_then(|l| l.as_object_mut())
    {
        labels.remove(POD_TEMPLATE_HASH);
    }
    value
}

/// The ReplicaSet whose template equals the deployment's (ignoring the hash
/// label), oldest first on ties, and the rest oldest first.
fn split_new_old(
    mut rs_list: Vec<ReplicaSet>,
    template: &PodTemplateSpec,
) -> (Option<ReplicaSet>, Vec<ReplicaSet>) {
    rs_list.sort_by(by_creation);
    let wanted = template_without_hash(template);
    let idx = rs_list.iter().position(|rs| {
        rs.spec
            .as_ref()
            .and_then(|s| s.template.as_ref())
            .is_some_and(|t| template_without_hash(t) == wanted)
    });
    let new_rs = idx.map(|i| rs_list.remove(i));
    (new_rs, rs_list)
}

fn reconcile_deployment(store: &Store, deploy_value: &serde_json::Value) -> anyhow::Result<()> {
    let deploy: Deployment = serde_json::from_value(deploy_value.clone())?;
    let name = deploy
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Deployment has no name"))?;
    let namespace = deploy.metadata.namespace.as_deref();

    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace,
        name,
    };
    let Some(current) = store.get_as::<Deployment>(&rref)? else {
        return Ok(());
    };
    let uid = current.metadata.uid.as_deref().unwrap_or("");
    if uid.is_empty() {
        tracing::warn!("deployment '{name}': missing uid; skipping");
        return Ok(());
    }
    let spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Deployment has no spec"))?;
    let p = Params::new(name, namespace, uid, spec);
    let terminating = current.metadata.deletion_timestamp.is_some();

    let owner = crate::owner_reference("apps/v1", "Deployment", name, uid);
    let rs_list: Vec<ReplicaSet> = crate::claim(
        store,
        &GroupVersionResource::replica_sets(),
        namespace,
        &owner,
        terminating,
        |rs: &ReplicaSet| selector_matches(&spec.selector, rs.metadata.labels.as_ref()),
    )?;
    if terminating {
        sync_status(store, &rref, &p)?;
        return Ok(());
    }

    let (mut new_rs, old_rss) = split_new_old(rs_list, &spec.template);
    if let Some(rs) = new_rs.take() {
        new_rs = Some(sync_new_rs(store, &p, rs, &old_rss)?);
    }

    if is_scaling_event(&p, new_rs.as_ref(), &old_rss) {
        scale(store, &p, new_rs.as_ref(), &old_rss, &current)?;
        sync_status(store, &rref, &p)?;
        return Ok(());
    }

    let status = if p.rolling {
        rollout_rolling(store, &rref, &p, new_rs, old_rss)?
    } else {
        rollout_recreate(store, &rref, &p, new_rs, old_rss)?
    };
    if let Some(status) = status
        && complete(&p, &status)
    {
        cleanup_old_rss(store, &p)?;
    }
    Ok(())
}

/// Any active ReplicaSet recorded a different desired count: the deployment
/// was scaled, which takes precedence over rolling out.
fn is_scaling_event(p: &Params, new_rs: Option<&ReplicaSet>, old_rss: &[ReplicaSet]) -> bool {
    old_rss
        .iter()
        .chain(new_rs)
        .filter(|rs| is_active(rs))
        .filter_map(|rs| rs_annotation_i32(rs, DESIRED_REPLICAS_ANNOTATION))
        .any(|desired| desired != p.desired)
}

fn rollout_rolling(
    store: &Store,
    rref: &ResourceRef,
    p: &Params,
    new_rs: Option<ReplicaSet>,
    old_rss: Vec<ReplicaSet>,
) -> anyhow::Result<Option<DeploymentStatus>> {
    let new_rs = match new_rs {
        Some(rs) => rs,
        None => create_new_rs(store, p, &old_rss)?,
    };
    if reconcile_new_rs(store, p, &new_rs, &old_rss)? {
        sync_status(store, rref, p)?;
        return Ok(None);
    }
    let active_old: Vec<ReplicaSet> = old_rss.into_iter().filter(is_active).collect();
    if reconcile_old_rss(store, p, &new_rs, active_old)? {
        sync_status(store, rref, p)?;
        return Ok(None);
    }
    sync_status(store, rref, p).map(Some)
}

/// Grow the new ReplicaSet up to the surge bound (or shrink it to the desired
/// count). True when it changed.
fn reconcile_new_rs(
    store: &Store,
    p: &Params,
    new_rs: &ReplicaSet,
    old_rss: &[ReplicaSet],
) -> anyhow::Result<bool> {
    let replicas = rs_replicas(new_rs);
    if replicas == p.desired {
        return Ok(false);
    }
    let target = if replicas > p.desired {
        p.desired
    } else {
        new_rs_replicas(p, new_rs, old_rss)
    };
    Ok(scale_rs(store, p, new_rs, target)?.is_some())
}

/// Upstream `NewRSNewReplicas`: how many replicas the new ReplicaSet may have
/// given the pods that already exist across all ReplicaSets.
fn new_rs_replicas(p: &Params, new_rs: &ReplicaSet, old_rss: &[ReplicaSet]) -> i32 {
    if !p.rolling {
        return p.desired;
    }
    let current = rs_replicas(new_rs) + old_rss.iter().map(rs_replicas).sum::<i32>();
    let max_total = p.max_replicas();
    if current >= max_total {
        return rs_replicas(new_rs);
    }
    rs_replicas(new_rs) + (max_total - current).min(p.desired - rs_replicas(new_rs))
}

/// Shrink old ReplicaSets as far as the unavailability bound allows: unhealthy
/// replicas first, then available ones once the new pods cover for them. True
/// when anything was scaled down.
fn reconcile_old_rss(
    store: &Store,
    p: &Params,
    new_rs: &ReplicaSet,
    mut old_rss: Vec<ReplicaSet>,
) -> anyhow::Result<bool> {
    let old_pods: i32 = old_rss.iter().map(rs_replicas).sum();
    if old_pods == 0 {
        return Ok(false);
    }
    let all_pods = old_pods + rs_replicas(new_rs);
    let new_unavailable = rs_replicas(new_rs) - rs_available(new_rs);
    let max_scaled_down = all_pods - p.min_available() - new_unavailable;
    if max_scaled_down <= 0 {
        return Ok(false);
    }
    old_rss.sort_by(by_creation);

    let mut cleaned = 0;
    for rs in old_rss.iter_mut() {
        if cleaned >= max_scaled_down {
            break;
        }
        let replicas = rs_replicas(rs);
        if replicas == 0 || replicas == rs_available(rs) {
            continue;
        }
        let count = (max_scaled_down - cleaned).min(replicas - rs_available(rs));
        if let Some(updated) = scale_rs(store, p, rs, replicas - count)? {
            *rs = updated;
        }
        cleaned += count;
    }

    let available: i32 = rs_available(new_rs) + old_rss.iter().map(rs_available).sum::<i32>();
    let mut scaled_down = 0;
    if available > p.min_available() {
        let total = available - p.min_available();
        for rs in &old_rss {
            if scaled_down >= total {
                break;
            }
            let replicas = rs_replicas(rs);
            if replicas == 0 {
                continue;
            }
            let count = replicas.min(total - scaled_down);
            scale_rs(store, p, rs, replicas - count)?;
            scaled_down += count;
        }
    }
    Ok(cleaned + scaled_down > 0)
}

fn rollout_recreate(
    store: &Store,
    rref: &ResourceRef,
    p: &Params,
    new_rs: Option<ReplicaSet>,
    old_rss: Vec<ReplicaSet>,
) -> anyhow::Result<Option<DeploymentStatus>> {
    let mut scaled = false;
    for rs in old_rss.iter().filter(|rs| is_active(rs)) {
        scaled |= scale_rs(store, p, rs, 0)?.is_some();
    }
    if scaled || old_pods_running(store, p, new_rs.as_ref(), &old_rss)? {
        sync_status(store, rref, p)?;
        return Ok(None);
    }
    let new_rs = match new_rs {
        Some(rs) => rs,
        None => create_new_rs(store, p, &old_rss)?,
    };
    scale_rs(store, p, &new_rs, p.desired)?;
    sync_status(store, rref, p).map(Some)
}

/// Pods of the old ReplicaSets still exist (by their status, or as pods not
/// yet terminated).
fn old_pods_running(
    store: &Store,
    p: &Params,
    new_rs: Option<&ReplicaSet>,
    old_rss: &[ReplicaSet],
) -> anyhow::Result<bool> {
    if old_rss.iter().map(rs_status_replicas).sum::<i32>() > 0 {
        return Ok(true);
    }
    let new_uid = new_rs.and_then(|rs| rs.metadata.uid.as_deref());
    let old_uids: Vec<&str> = old_rss
        .iter()
        .filter_map(|rs| rs.metadata.uid.as_deref())
        .collect();
    let pods = store.list_as::<Pod>(&GroupVersionResource::pods(), p.namespace)?;
    Ok(pods.iter().any(|pod| {
        let controller = crate::find_owner(&pod.metadata, "ReplicaSet").map(|r| r.uid.as_str());
        controller.is_some_and(|c| old_uids.contains(&c) && Some(c) != new_uid)
            && !matches!(pod_phase(pod), "Failed" | "Succeeded")
    }))
}

/// Scale on a replica-count change: the one active ReplicaSet takes the whole
/// count; several active ones share the change in proportion to their size
/// (upstream `scale`).
fn scale(
    store: &Store,
    p: &Params,
    new_rs: Option<&ReplicaSet>,
    old_rss: &[ReplicaSet],
    deploy: &Deployment,
) -> anyhow::Result<()> {
    if let Some(target) = find_active_or_latest(new_rs, old_rss) {
        if rs_replicas(target) != p.desired {
            scale_rs(store, p, target, p.desired)?;
        }
        return Ok(());
    }
    if new_rs.is_some_and(|rs| is_saturated(p, rs)) {
        for rs in old_rss.iter().filter(|rs| is_active(rs)) {
            scale_rs(store, p, rs, 0)?;
        }
        return Ok(());
    }
    if !p.rolling {
        return Ok(());
    }

    let mut all: Vec<&ReplicaSet> = old_rss
        .iter()
        .chain(new_rs)
        .filter(|rs| is_active(rs))
        .collect();
    let all_replicas: i32 = all.iter().map(|rs| rs_replicas(rs)).sum();
    let allowed = if p.desired > 0 { p.max_replicas() } else { 0 };
    let to_add = allowed - all_replicas;
    match to_add.cmp(&0) {
        // Bigger first; ties go to the newer set when growing, the older when shrinking.
        Ordering::Greater => all.sort_by(|a, b| {
            rs_replicas(b)
                .cmp(&rs_replicas(a))
                .then_with(|| by_creation(b, a))
        }),
        Ordering::Less => all.sort_by(|a, b| {
            rs_replicas(b)
                .cmp(&rs_replicas(a))
                .then_with(|| by_creation(a, b))
        }),
        Ordering::Equal => {}
    }
    let status_replicas = deploy.status.as_ref().and_then(|s| s.replicas).unwrap_or(0);
    let mut added = 0;
    let mut sizes: Vec<i32> = Vec::with_capacity(all.len());
    for rs in &all {
        let proportion = if to_add != 0 {
            proportion(p, rs, to_add, added, status_replicas)
        } else {
            0
        };
        sizes.push(rs_replicas(rs) + proportion);
        added += proportion;
    }
    for (i, rs) in all.iter().enumerate() {
        if i == 0 && to_add != 0 {
            sizes[i] = (sizes[i] + to_add - added).max(0);
        }
        scale_rs(store, p, rs, sizes[i])?;
    }
    Ok(())
}

fn find_active_or_latest<'a>(
    new_rs: Option<&'a ReplicaSet>,
    old_rss: &'a [ReplicaSet],
) -> Option<&'a ReplicaSet> {
    if new_rs.is_none() && old_rss.is_empty() {
        return None;
    }
    let active: Vec<&ReplicaSet> = old_rss
        .iter()
        .chain(new_rs)
        .filter(|rs| is_active(rs))
        .collect();
    match active.len() {
        0 => new_rs.or_else(|| old_rss.iter().max_by(|a, b| by_creation(a, b))),
        1 => Some(active[0]),
        _ => None,
    }
}

fn is_saturated(p: &Params, rs: &ReplicaSet) -> bool {
    rs_annotation_i32(rs, DESIRED_REPLICAS_ANNOTATION) == Some(p.desired)
        && rs_replicas(rs) == p.desired
        && rs_available(rs) == p.desired
}

/// This ReplicaSet's share of `to_add`, bounded by what is still unassigned.
fn proportion(p: &Params, rs: &ReplicaSet, to_add: i32, added: i32, status_replicas: i32) -> i32 {
    if rs_replicas(rs) == 0 || to_add == 0 || to_add == added {
        return 0;
    }
    let fraction = rs_fraction(p, rs, status_replicas);
    let allowed = to_add - added;
    if to_add > 0 {
        fraction.min(allowed)
    } else {
        fraction.max(allowed)
    }
}

/// How many replicas this ReplicaSet gains (or loses) to keep its share when
/// the deployment's maximum moves from the recorded `max-replicas` to the new
/// one.
fn rs_fraction(p: &Params, rs: &ReplicaSet, status_replicas: i32) -> i32 {
    let replicas = rs_replicas(rs);
    if p.desired == 0 {
        return -replicas;
    }
    let before = rs_annotation_i32(rs, MAX_REPLICAS_ANNOTATION)
        .filter(|n| *n != 0)
        .unwrap_or(status_replicas);
    if before == 0 {
        return 0;
    }
    let size = f64::from(replicas * p.max_replicas()) / f64::from(before);
    size.round() as i32 - replicas
}

/// Set `spec.replicas` and the desired/max annotations; None when nothing
/// changed.
fn scale_rs(
    store: &Store,
    p: &Params,
    rs: &ReplicaSet,
    new_scale: i32,
) -> anyhow::Result<Option<ReplicaSet>> {
    let old_scale = rs_replicas(rs);
    let annotations_current = rs_annotation_i32(rs, DESIRED_REPLICAS_ANNOTATION) == Some(p.desired)
        && rs_annotation_i32(rs, MAX_REPLICAS_ANNOTATION) == Some(p.max_replicas());
    if old_scale == new_scale && annotations_current {
        return Ok(None);
    }
    let gvr = GroupVersionResource::replica_sets();
    let rref = p.rs_ref(&gvr, rs_name(rs));
    let Some(mut value) = store.get(&rref)? else {
        return Ok(None);
    };
    value["spec"]["replicas"] = serde_json::json!(new_scale);
    set_replicas_annotations(&mut value, p);
    let updated = store.update(&rref, &value)?;
    if old_scale != new_scale {
        tracing::info!(
            "deployment '{}': scaled {} replica set '{}' from {old_scale} to {new_scale}",
            p.name,
            if old_scale < new_scale { "up" } else { "down" },
            rs_name(rs)
        );
    }
    Ok(Some(serde_json::from_value(updated)?))
}

fn set_replicas_annotations(value: &mut serde_json::Value, p: &Params) {
    let meta = &mut value["metadata"];
    if !meta.get("annotations").is_some_and(|a| a.is_object()) {
        meta["annotations"] = serde_json::json!({});
    }
    meta["annotations"][DESIRED_REPLICAS_ANNOTATION] = serde_json::json!(p.desired.to_string());
    meta["annotations"][MAX_REPLICAS_ANNOTATION] = serde_json::json!(p.max_replicas().to_string());
}

fn max_revision(rss: &[ReplicaSet]) -> i64 {
    rss.iter().map(rs_revision).max().unwrap_or(0)
}

/// Deployment annotations that propagate to its ReplicaSets.
fn inherited_annotations(spec_owner: &Deployment) -> BTreeMap<String, String> {
    spec_owner
        .metadata
        .annotations
        .iter()
        .flatten()
        .filter(|(k, _)| {
            ![
                LAST_APPLIED_ANNOTATION,
                REVISION_ANNOTATION,
                REVISION_HISTORY_ANNOTATION,
                DESIRED_REPLICAS_ANNOTATION,
                MAX_REPLICAS_ANNOTATION,
            ]
            .contains(&k.as_str())
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Keep the new ReplicaSet's revision above every old one and its
/// minReadySeconds in step with the deployment.
fn sync_new_rs(
    store: &Store,
    p: &Params,
    rs: ReplicaSet,
    old_rss: &[ReplicaSet],
) -> anyhow::Result<ReplicaSet> {
    let next = max_revision(old_rss) + 1;
    let revision_stale = rs_revision(&rs) < next;
    let min_ready_stale =
        rs.spec.as_ref().and_then(|s| s.min_ready_seconds) != p.spec.min_ready_seconds;
    if !revision_stale && !min_ready_stale {
        return Ok(rs);
    }
    let gvr = GroupVersionResource::replica_sets();
    let rref = p.rs_ref(&gvr, rs_name(&rs));
    let Some(mut value) = store.get(&rref)? else {
        return Ok(rs);
    };
    if revision_stale {
        let meta = &mut value["metadata"];
        if !meta.get("annotations").is_some_and(|a| a.is_object()) {
            meta["annotations"] = serde_json::json!({});
        }
        meta["annotations"][REVISION_ANNOTATION] = serde_json::json!(next.to_string());
    }
    match p.spec.min_ready_seconds {
        Some(s) => value["spec"]["minReadySeconds"] = serde_json::json!(s),
        None => {
            if let Some(spec) = value["spec"].as_object_mut() {
                spec.remove("minReadySeconds");
            }
        }
    }
    Ok(serde_json::from_value(store.update(&rref, &value)?)?)
}

/// Create `<deployment>-<hash>` for the current template at the next revision,
/// sized by the rollout bounds.
fn create_new_rs(store: &Store, p: &Params, old_rss: &[ReplicaSet]) -> anyhow::Result<ReplicaSet> {
    let hash = template_hash(&p.spec.template);
    let name = format!("{}-{hash}", p.name);

    let mut template = p.spec.template.clone();
    let mut tmpl_meta = template.metadata.unwrap_or_default();
    let mut tmpl_labels = tmpl_meta.labels.unwrap_or_default();
    tmpl_labels.insert(POD_TEMPLATE_HASH.into(), hash.clone());
    tmpl_meta.labels = Some(tmpl_labels.clone());
    template.metadata = Some(tmpl_meta);

    let mut selector = p.spec.selector.clone();
    let mut match_labels = selector.match_labels.unwrap_or_default();
    match_labels.insert(POD_TEMPLATE_HASH.into(), hash.clone());
    selector.match_labels = Some(match_labels);

    let gvr = GroupVersionResource::deployments();
    let deploy: Option<Deployment> = store.get_as(&ResourceRef {
        gvr: &gvr,
        namespace: p.namespace,
        name: p.name,
    })?;
    let mut annotations = deploy
        .as_ref()
        .map(inherited_annotations)
        .unwrap_or_default();
    annotations.insert(
        REVISION_ANNOTATION.into(),
        (max_revision(old_rss) + 1).to_string(),
    );
    annotations.insert(DESIRED_REPLICAS_ANNOTATION.into(), p.desired.to_string());
    annotations.insert(MAX_REPLICAS_ANNOTATION.into(), p.max_replicas().to_string());

    let mut rs = ReplicaSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: p.namespace.map(String::from),
            labels: Some(tmpl_labels),
            annotations: Some(annotations),
            owner_references: Some(vec![crate::owner_reference(
                "apps/v1",
                "Deployment",
                p.name,
                p.uid,
            )]),
            ..Default::default()
        },
        spec: Some(ReplicaSetSpec {
            replicas: Some(0),
            min_ready_seconds: p.spec.min_ready_seconds,
            selector,
            template: Some(template),
        }),
        status: None,
    };
    let replicas = new_rs_replicas(p, &rs, old_rss);
    if let Some(spec) = rs.spec.as_mut() {
        spec.replicas = Some(replicas);
    }

    let rs_gvr = GroupVersionResource::replica_sets();
    let rref = p.rs_ref(&rs_gvr, &name);
    let created = match store.create(rref, &serde_json::to_value(&rs)?) {
        Ok(v) => v,
        Err(StoreError::AlreadyExists { .. }) => {
            let existing: Option<ReplicaSet> = store.get_as(&p.rs_ref(&rs_gvr, &name))?;
            match existing {
                Some(existing)
                    if is_owned_by(&existing.metadata, p.uid)
                        && existing
                            .spec
                            .as_ref()
                            .and_then(|s| s.template.as_ref())
                            .is_some_and(|t| {
                                template_without_hash(t) == template_without_hash(&p.spec.template)
                            }) =>
                {
                    return Ok(existing);
                }
                _ => anyhow::bail!("replica set '{name}' exists with a different template"),
            }
        }
        Err(e) => return Err(e.into()),
    };
    tracing::info!(
        "deployment '{}': created replica set '{name}' with {replicas} replicas",
        p.name
    );
    Ok(serde_json::from_value(created)?)
}

fn complete(p: &Params, status: &DeploymentStatus) -> bool {
    status.updated_replicas == Some(p.desired)
        && status.replicas == Some(p.desired)
        && status.available_replicas == Some(p.desired)
}

/// Delete fully scaled-down old ReplicaSets beyond `revisionHistoryLimit`,
/// oldest revision first.
fn cleanup_old_rss(store: &Store, p: &Params) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::replica_sets();
    let owned: Vec<ReplicaSet> = store
        .list_as::<ReplicaSet>(&gvr, p.namespace)?
        .into_iter()
        .filter(|rs| is_owned_by(&rs.metadata, p.uid) && rs.metadata.deletion_timestamp.is_none())
        .collect();
    let (_, mut old) = split_new_old(owned, &p.spec.template);
    let limit = p
        .spec
        .revision_history_limit
        .unwrap_or(DEFAULT_REVISION_HISTORY_LIMIT);
    if limit < 0 {
        return Ok(());
    }
    let excess = old.len().saturating_sub(limit as usize);
    if excess == 0 {
        return Ok(());
    }
    old.sort_by(|a, b| {
        rs_revision(a)
            .cmp(&rs_revision(b))
            .then_with(|| by_creation(a, b))
    });
    for rs in old.iter().take(excess) {
        if rs_replicas(rs) != 0 || rs_status_replicas(rs) != 0 {
            continue;
        }
        store.delete(&p.rs_ref(&gvr, rs_name(rs)))?;
        tracing::info!(
            "deployment '{}': deleted old replica set '{}'",
            p.name,
            rs_name(rs)
        );
    }
    Ok(())
}

/// Recompute status from the owned ReplicaSets and store it with the new
/// ReplicaSet's revision annotation.
fn sync_status(store: &Store, rref: &ResourceRef, p: &Params) -> anyhow::Result<DeploymentStatus> {
    let owned: Vec<ReplicaSet> = store
        .list_as::<ReplicaSet>(&GroupVersionResource::replica_sets(), p.namespace)?
        .into_iter()
        .filter(|rs| is_owned_by(&rs.metadata, p.uid))
        .collect();
    let (new_rs, old_rss) = split_new_old(owned, &p.spec.template);
    let all = old_rss.iter().chain(new_rs.as_ref());

    let Some(mut current) = store.get(rref)? else {
        return Ok(DeploymentStatus::default());
    };
    let previous: DeploymentStatus = current
        .get("status")
        .cloned()
        .and_then(|s| serde_json::from_value(s).ok())
        .unwrap_or_default();

    let replicas: i32 = all.clone().map(rs_status_replicas).sum();
    let ready: i32 = all.clone().map(rs_ready).sum();
    let available: i32 = all.clone().map(rs_available).sum();
    let spec_total: i32 = all.clone().map(rs_replicas).sum();
    let updated = new_rs.as_ref().map(rs_status_replicas).unwrap_or(0);
    let mut status = DeploymentStatus {
        replicas: Some(replicas),
        updated_replicas: Some(updated),
        ready_replicas: Some(ready),
        available_replicas: Some(available),
        unavailable_replicas: Some((spec_total - available).max(0)),
        observed_generation: current
            .get("metadata")
            .and_then(|m| m.get("generation"))
            .and_then(|g| g.as_i64()),
        collision_count: previous.collision_count,
        conditions: None,
    };
    let new_name = new_rs.as_ref().map(rs_name).unwrap_or("");
    let mut conditions = Vec::new();
    conditions.push(condition(
        "Available",
        available >= p.min_available(),
        "MinimumReplicasAvailable",
        "MinimumReplicasUnavailable",
        "Deployment has minimum availability.",
        "Deployment does not have minimum availability.",
    ));
    conditions.push(if complete(p, &status) {
        condition(
            "Progressing",
            true,
            "NewReplicaSetAvailable",
            "",
            &format!("ReplicaSet \"{new_name}\" has successfully progressed."),
            "",
        )
    } else {
        condition(
            "Progressing",
            true,
            "ReplicaSetUpdated",
            "",
            &format!("ReplicaSet \"{new_name}\" is progressing."),
            "",
        )
    });
    status.conditions = Some(conditions);

    let new_status = serde_json::to_value(&status)?;
    let revision = new_rs.as_ref().map(|rs| rs_revision(rs).to_string());
    let stored_revision = current
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(REVISION_ANNOTATION))
        .and_then(|v| v.as_str())
        .map(String::from);
    let revision_changed = revision
        .as_ref()
        .is_some_and(|r| Some(r) != stored_revision.as_ref());
    if current.get("status") == Some(&new_status) && !revision_changed {
        return Ok(status);
    }
    current["status"] = new_status;
    if let Some(revision) = revision.filter(|_| revision_changed) {
        let meta = &mut current["metadata"];
        if !meta.get("annotations").is_some_and(|a| a.is_object()) {
            meta["annotations"] = serde_json::json!({});
        }
        meta["annotations"][REVISION_ANNOTATION] = serde_json::json!(revision);
    }
    match store.update(rref, &current) {
        Ok(_) | Err(StoreError::Conflict { .. }) => Ok(status),
        Err(e) => Err(e.into()),
    }
}

fn condition(
    type_: &str,
    is_true: bool,
    true_reason: &str,
    false_reason: &str,
    true_message: &str,
    false_message: &str,
) -> DeploymentCondition {
    DeploymentCondition {
        type_: type_.into(),
        status: if is_true { "True" } else { "False" }.into(),
        reason: Some(if is_true { true_reason } else { false_reason }.into()),
        message: Some(if is_true { true_message } else { false_message }.into()),
        ..Default::default()
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

    fn rref<'a>(gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: Some("default"),
            name,
        }
    }

    fn create_deploy(
        store: &Store,
        name: &str,
        replicas: i32,
        image: &str,
        strategy: Value,
    ) -> Value {
        let mut spec = json!({
            "replicas": replicas,
            "selector": {"matchLabels": {"name": name}},
            "template": {
                "metadata": {"labels": {"name": name}},
                "spec": {"containers": [{"name": "c", "image": image}]}
            }
        });
        if !strategy.is_null() {
            spec["strategy"] = strategy;
        }
        store
            .create(
                rref(&GroupVersionResource::deployments(), name),
                &json!({
                    "apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": spec,
                }),
            )
            .unwrap()
    }

    fn get_deploy(store: &Store, name: &str) -> Value {
        store
            .get(&rref(&GroupVersionResource::deployments(), name))
            .unwrap()
            .unwrap()
    }

    fn update_deploy(store: &Store, name: &str, f: impl FnOnce(&mut Value)) -> Value {
        let gvr = GroupVersionResource::deployments();
        let mut d = get_deploy(store, name);
        f(&mut d);
        store.update(&rref(&gvr, name), &d).unwrap()
    }

    fn reconcile(store: &Store, name: &str) {
        let d = get_deploy(store, name);
        reconcile_deployment(store, &d).unwrap();
    }

    fn owned_rss(store: &Store, deploy: &Value) -> Vec<ReplicaSet> {
        let uid = deploy["metadata"]["uid"].as_str().unwrap();
        let mut rss: Vec<ReplicaSet> = store
            .list_as::<ReplicaSet>(&GroupVersionResource::replica_sets(), Some("default"))
            .unwrap()
            .into_iter()
            .filter(|rs| is_owned_by(&rs.metadata, uid))
            .collect();
        rss.sort_by_key(rs_revision);
        rss
    }

    fn rs_by_name(store: &Store, name: &str) -> ReplicaSet {
        store
            .get_as::<ReplicaSet>(&rref(&GroupVersionResource::replica_sets(), name))
            .unwrap()
            .unwrap()
    }

    fn set_rs_status(store: &Store, name: &str, replicas: i32, ready: i32, available: i32) {
        let gvr = GroupVersionResource::replica_sets();
        let mut rs = store.get(&rref(&gvr, name)).unwrap().unwrap();
        rs["status"] = json!({
            "replicas": replicas, "readyReplicas": ready, "availableReplicas": available
        });
        store.update(&rref(&gvr, name), &rs).unwrap();
    }

    fn create_orphan_rs(
        store: &Store,
        name: &str,
        labels: Value,
        image: &str,
        revision: Option<&str>,
    ) {
        let mut meta = json!({"name": name, "namespace": "default", "labels": labels});
        if let Some(r) = revision {
            meta["annotations"] = json!({REVISION_ANNOTATION: r});
        }
        store
            .create(
                rref(&GroupVersionResource::replica_sets(), name),
                &json!({
                    "apiVersion": "apps/v1", "kind": "ReplicaSet",
                    "metadata": meta,
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": labels},
                        "template": {
                            "metadata": {"labels": labels},
                            "spec": {"containers": [{"name": "c", "image": image}]}
                        }
                    },
                    "status": {"replicas": 1, "readyReplicas": 1, "availableReplicas": 1}
                }),
            )
            .unwrap();
    }

    fn deploy_status(store: &Store, name: &str) -> DeploymentStatus {
        serde_json::from_value(get_deploy(store, name)["status"].clone()).unwrap()
    }

    #[test]
    fn creates_new_rs_with_hash_and_annotations() {
        let (_d, store) = store();
        let d = create_deploy(&store, "web", 3, "img:1", Value::Null);
        reconcile(&store, "web");

        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 1);
        let rs = &rss[0];
        let hash = rs.metadata.labels.as_ref().unwrap()[POD_TEMPLATE_HASH].clone();
        assert_eq!(rs_name(rs), format!("web-{hash}"));
        assert_eq!(rs_replicas(rs), 3);
        let spec = rs.spec.as_ref().unwrap();
        assert_eq!(
            spec.selector.match_labels.as_ref().unwrap()[POD_TEMPLATE_HASH],
            hash
        );
        assert_eq!(
            spec.template
                .as_ref()
                .unwrap()
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap()[POD_TEMPLATE_HASH],
            hash
        );
        assert_eq!(rs_annotation(rs, REVISION_ANNOTATION), Some("1"));
        assert_eq!(rs_annotation(rs, DESIRED_REPLICAS_ANNOTATION), Some("3"));
        assert_eq!(rs_annotation(rs, MAX_REPLICAS_ANNOTATION), Some("4"));
        assert_eq!(
            get_deploy(&store, "web")["metadata"]["annotations"][REVISION_ANNOTATION],
            "1"
        );
        let status = deploy_status(&store, "web");
        assert_eq!(status.replicas, Some(0));
        assert_eq!(status.unavailable_replicas, Some(3));
        let conditions = status.conditions.unwrap();
        assert_eq!(conditions[0].type_, "Available");
        assert_eq!(conditions[0].status, "False");
    }

    #[test]
    fn rolling_update_respects_surge_and_unavailability_then_scales_proportionally() {
        let (_d, store) = store();
        let d = create_deploy(
            &store,
            "web",
            10,
            "img:1",
            json!({"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 3, "maxUnavailable": 2}}),
        );
        reconcile(&store, "web");
        let rs1 = rs_name(&owned_rss(&store, &d)[0]).to_string();
        set_rs_status(&store, &rs1, 10, 10, 10);
        reconcile(&store, "web");
        assert_eq!(deploy_status(&store, "web").available_replicas, Some(10));

        update_deploy(&store, "web", |d| {
            d["spec"]["template"]["spec"]["containers"][0]["image"] = json!("img:2");
        });
        reconcile(&store, "web");
        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 2);
        let rs2 = rs_name(&rss[1]).to_string();
        assert_eq!(rs_replicas(&rss[1]), 3, "new RS takes the surge");
        assert_eq!(rs_replicas(&rss[0]), 8, "old RS down to minAvailable");
        assert_eq!(rs_annotation(&rss[1], REVISION_ANNOTATION), Some("2"));

        set_rs_status(&store, &rs1, 8, 8, 8);
        set_rs_status(&store, &rs2, 3, 0, 0);
        reconcile(&store, "web");
        assert_eq!(
            rs_replicas(&rs_by_name(&store, &rs2)),
            5,
            "new RS fills the freed room"
        );

        set_rs_status(&store, &rs2, 5, 0, 0);
        reconcile(&store, "web");
        assert_eq!(
            rs_replicas(&rs_by_name(&store, &rs1)),
            8,
            "unavailable new pods block further scale-down"
        );
        let status = deploy_status(&store, "web");
        assert_eq!(status.available_replicas, Some(8));
        assert_eq!(status.updated_replicas, Some(5));

        update_deploy(&store, "web", |d| d["spec"]["replicas"] = json!(30));
        reconcile(&store, "web");
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs1)), 20);
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs2)), 13);
    }

    #[test]
    fn recreate_waits_for_old_pods_before_creating_the_new_rs() {
        let (_d, store) = store();
        let d = create_deploy(&store, "web", 1, "img:1", json!({"type": "Recreate"}));
        reconcile(&store, "web");
        let rs1 = rs_name(&owned_rss(&store, &d)[0]).to_string();
        set_rs_status(&store, &rs1, 1, 1, 1);

        update_deploy(&store, "web", |d| {
            d["spec"]["template"]["spec"]["containers"][0]["image"] = json!("img:2");
        });
        reconcile(&store, "web");
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs1)), 0);
        assert_eq!(
            owned_rss(&store, &d).len(),
            1,
            "no new RS while old pods run"
        );
        reconcile(&store, "web");
        assert_eq!(owned_rss(&store, &d).len(), 1);

        set_rs_status(&store, &rs1, 0, 0, 0);
        reconcile(&store, "web");
        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 2);
        assert_eq!(rs_replicas(&rss[1]), 1);
        assert_eq!(rs_annotation(&rss[1], REVISION_ANNOTATION), Some("2"));
    }

    #[test]
    fn adopts_a_matching_replicaset_and_continues_its_revision() {
        let (_d, store) = store();
        create_orphan_rs(
            &store,
            "old",
            json!({"name": "web", "pod": "httpd"}),
            "httpd",
            Some("3546343826724305832"),
        );
        let d = create_deploy(&store, "web", 1, "agnhost", Value::Null);
        reconcile(&store, "web");

        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 2);
        assert_eq!(rs_name(&rss[0]), "old");
        assert_eq!(
            rs_annotation(&rss[1], REVISION_ANNOTATION),
            Some("3546343826724305833")
        );
        assert_eq!(
            get_deploy(&store, "web")["metadata"]["annotations"][REVISION_ANNOTATION],
            "3546343826724305833"
        );
    }

    #[test]
    fn rollover_scales_down_the_unavailable_new_rs_first() {
        let (_d, store) = store();
        create_orphan_rs(
            &store,
            "old",
            json!({"name": "web", "pod": "httpd"}),
            "httpd",
            None,
        );
        let d = create_deploy(
            &store,
            "web",
            1,
            "bad:img",
            json!({"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}),
        );
        reconcile(&store, "web");
        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 2);
        let rs1 = rs_name(&rss[1]).to_string();
        assert_eq!(rs_replicas(&rs_by_name(&store, "old")), 1);
        assert_eq!(rs_replicas(&rss[1]), 1);
        set_rs_status(&store, &rs1, 1, 0, 0);
        reconcile(&store, "web");
        assert_eq!(
            rs_replicas(&rs_by_name(&store, "old")),
            1,
            "old stays while new is unavailable"
        );

        update_deploy(&store, "web", |d| {
            d["spec"]["template"]["spec"]["containers"][0]["image"] = json!("good:img");
        });
        reconcile(&store, "web");
        assert_eq!(
            rs_replicas(&rs_by_name(&store, &rs1)),
            0,
            "unhealthy old RS cleaned up first"
        );
        let rss = owned_rss(&store, &d);
        assert_eq!(rss.len(), 3);
        let rs2 = rs_name(&rss[2]).to_string();
        assert_eq!(rs_annotation(&rss[2], REVISION_ANNOTATION), Some("2"));

        set_rs_status(&store, &rs1, 0, 0, 0);
        reconcile(&store, "web");
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs2)), 1);
        set_rs_status(&store, &rs2, 1, 1, 1);
        reconcile(&store, "web");
        assert_eq!(rs_replicas(&rs_by_name(&store, "old")), 0);
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs1)), 0);
        assert_eq!(rs_replicas(&rs_by_name(&store, &rs2)), 1);
        assert_eq!(
            get_deploy(&store, "web")["metadata"]["annotations"][REVISION_ANNOTATION],
            "2"
        );
    }
}
