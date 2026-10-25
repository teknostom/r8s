//! ResourceQuota controller.
//!
//! Watches every ResourceQuota plus the resource types it enforces (pods,
//! services, configmaps, secrets, pvcs, resourcequotas, replicasets, …). On any
//! change in namespace N, recomputes `status.used` for every ResourceQuota in N
//! so clients see live usage, and gates admission via [`check_admission`].
//!
//! Fields handled:
//!   * Object counts — built-in shortcuts (`pods`, `services`, `secrets`,
//!     `persistentvolumeclaims`, `services.nodeports`, `services.loadbalancers`,
//!     …) and the generic `count/<resource>.<group>` form (group omitted for
//!     core).
//!   * Compute — `cpu`/`memory`/`ephemeral-storage` and their `requests.*` /
//!     `limits.*` forms, summed across pod containers (init containers via the
//!     `max(init, sum(regular))` rule).
//!   * Storage — `requests.storage` / `storage` and the per-class
//!     `<class>.storageclass.storage.k8s.io/{requests.storage,persistentvolumeclaims}`
//!     forms, charged against PVC `spec.resources.requests.storage`.
//!   * Scopes — `Terminating`/`BestEffort` and their negations.
//!
//! A hard field we can't interpret still appears in `status.used` as 0 so the
//! status struct stays well-formed.

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::GroupVersionResource;
use rustc_hash::FxHashSet;
use serde_json::{Map, Value};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::quantity::Quantity;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("resourcequota controller started");
    let quota_gvr = GroupVersionResource::resource_quotas();

    // Resources whose changes should retrigger a quota recompute. The
    // controller fans in events from each watch into a single mpsc so the
    // main loop only has to await one channel.
    let watched: Vec<GroupVersionResource> = vec![
        quota_gvr.clone(),
        GroupVersionResource::pods(),
        GroupVersionResource::services(),
        GroupVersionResource::secrets(),
        GroupVersionResource::configmaps(),
        GroupVersionResource::replica_sets(),
        GroupVersionResource::new("", "v1", "persistentvolumeclaims"),
        GroupVersionResource::new("", "v1", "replicationcontrollers"),
    ];

    reconcile_all(&store, &quota_gvr);

    // `Some(ns)` = the namespace that changed; `None` = broadcast lag,
    // re-sync everything.
    let (tx, mut rx) = mpsc::channel::<Option<String>>(256);
    for gvr in &watched {
        let mut sub = store.watch(gvr);
        let tx = tx.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    res = sub.recv() => match res {
                        Ok(event) => {
                            let ns = event
                                .object
                                .get("metadata")
                                .and_then(|m| m.get("namespace"))
                                .and_then(|v| v.as_str())
                                .map(String::from);
                            if tx.send(ns).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            if tx.send(None).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                }
            }
        });
    }
    drop(tx);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("resourcequota controller shutting down");
                return Ok(());
            }
            msg = rx.recv() => match msg {
                Some(Some(ns)) => recompute_for_namespace(&store, &quota_gvr, &ns),
                Some(None) => reconcile_all(&store, &quota_gvr),
                None => return Ok(()), // all watch tasks exited
            },
        }
    }
}

/// Infix separating a storage-class name from its quota suffix, e.g.
/// `standard.storageclass.storage.k8s.io/requests.storage`.
const STORAGECLASS_INFIX: &str = ".storageclass.storage.k8s.io/";

/// What a `spec.hard` field name charges against.
enum FieldKind {
    /// Sum of PVC `spec.resources.requests.storage`, optionally scoped to a
    /// storage class.
    Storage { class: Option<String> },
    /// Count of PVCs scoped to a storage class.
    PvcCount { class: String },
    /// Pod compute (cpu/memory/ephemeral-storage/gpu; `requests.*`/`limits.*`).
    Compute,
    /// Discrete object count.
    Count,
}

fn classify_field(field: &str) -> FieldKind {
    if let Some((class, suffix)) = field.split_once(STORAGECLASS_INFIX) {
        return match suffix {
            "requests.storage" => FieldKind::Storage {
                class: Some(class.to_string()),
            },
            "persistentvolumeclaims" => FieldKind::PvcCount {
                class: class.to_string(),
            },
            _ => FieldKind::Count,
        };
    }
    match field {
        "storage" | "requests.storage" => FieldKind::Storage { class: None },
        _ if is_pod_compute_field(field) => FieldKind::Compute,
        _ => FieldKind::Count,
    }
}

fn is_pod_compute_field(field: &str) -> bool {
    matches!(
        field,
        "cpu" | "memory" | "ephemeral-storage" | "pods.cpu" | "pods.memory"
    ) || field.starts_with("requests.")
        || field.starts_with("limits.")
}

/// A resolved usage or delta, tagged so it renders in the right units: a count
/// as a bare integer (`pods=3`), a quantity in canonical form (`cpu=500m`).
struct Usage {
    quantity: Quantity,
    is_count: bool,
}

impl Usage {
    fn count(n: u64) -> Self {
        Self {
            quantity: Quantity::from_millis(n as i128 * 1000),
            is_count: true,
        }
    }

    fn amount(quantity: Quantity) -> Self {
        Self {
            quantity,
            is_count: false,
        }
    }

    fn display(&self) -> String {
        if self.is_count {
            (self.quantity.millis() / 1000).to_string()
        } else {
            self.quantity.to_canonical_string()
        }
    }
}

/// Current usage of `field` in `namespace`.
fn used_for_field(store: &Store, namespace: &str, field: &str, scopes: &[String]) -> Usage {
    match classify_field(field) {
        FieldKind::Storage { class } => {
            Usage::amount(storage_used(store, namespace, class.as_deref()))
        }
        FieldKind::PvcCount { class } => {
            Usage::count(pvc_count(store, namespace, Some(&class)))
        }
        FieldKind::Compute => Usage::amount(compute_for_field(store, namespace, field, scopes)),
        FieldKind::Count => Usage::count(count_for_field(store, namespace, field, scopes)),
    }
}

fn pvc_storage_class(pvc: &Value) -> &str {
    pvc.pointer("/spec/storageClassName")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn pvc_storage_request(pvc: &Value) -> Quantity {
    pvc.pointer("/spec/resources/requests/storage")
        .and_then(|v| v.as_str())
        .and_then(Quantity::parse)
        .unwrap_or_else(Quantity::zero)
}

/// Sum requested storage across the namespace's PVCs, optionally filtered to a
/// storage class (exact match on `spec.storageClassName`).
fn storage_used(store: &Store, namespace: &str, class: Option<&str>) -> Quantity {
    let pvc_gvr = GroupVersionResource::new("", "v1", "persistentvolumeclaims");
    let items = match store.list(&pvc_gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return Quantity::zero(),
    };
    let mut total = Quantity::zero();
    for pvc in &items {
        if class.is_some_and(|c| pvc_storage_class(pvc) != c) {
            continue;
        }
        total = total + pvc_storage_request(pvc);
    }
    total
}

fn pvc_count(store: &Store, namespace: &str, class: Option<&str>) -> u64 {
    let pvc_gvr = GroupVersionResource::new("", "v1", "persistentvolumeclaims");
    let items = match store.list(&pvc_gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return 0,
    };
    match class {
        None => items.len() as u64,
        Some(class) => items
            .iter()
            .filter(|p| pvc_storage_class(p) == class)
            .count() as u64,
    }
}

/// Admission gate: is `object` permitted to land in `namespace`? Walks every
/// ResourceQuota in the namespace, computes the hypothetical `used` after the
/// new object is added, and rejects if any hard limit would be exceeded.
///
/// `mutating_gvr` is the type of `object`; used to scope which hard fields
/// receive a non-zero delta from this admission. Returns Ok if every quota
/// remains within its hard limits, Err with an upstream-style message otherwise.
///
/// Like upstream, this is a check-then-write with no cross-request lock, so two
/// concurrent creates can both pass against the same `used` snapshot and jointly
/// overshoot; the next `recompute_one` still records the true usage.
pub fn check_admission(
    store: &Store,
    mutating_gvr: &GroupVersionResource,
    namespace: &str,
    object: &Value,
) -> Result<(), String> {
    let quota_gvr = GroupVersionResource::resource_quotas();
    let quotas = match store.list(&quota_gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return Ok(()),
    };
    for q in &quotas {
        let spec = q.get("spec");
        let hard = match spec.and_then(|s| s.get("hard")).and_then(|h| h.as_object()) {
            Some(h) => h,
            None => continue,
        };
        let scopes: Vec<String> = spec
            .and_then(|s| s.get("scopes"))
            .and_then(|s| s.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // If this object is a pod and the quota's scopes don't accept it,
        // the quota neither counts nor enforces against it.
        let pod_passes_scopes = if mutating_gvr.resource == "pods" {
            pod_matches_scopes(object, &scopes)
        } else {
            scopes.is_empty()
        };
        let name = q
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        for (field, hard_val) in hard {
            let hard_q = match hard_val.as_str().and_then(Quantity::parse) {
                Some(q) => q,
                None => continue,
            };
            let current = used_for_field(store, namespace, field, &scopes);
            let delta = if mutating_gvr.resource == "pods" && !pod_passes_scopes {
                Usage {
                    quantity: Quantity::zero(),
                    is_count: current.is_count,
                }
            } else {
                delta_for_object(field, mutating_gvr, object)
            };
            let projected = current.quantity + delta.quantity;
            if projected.cmp(&hard_q) == std::cmp::Ordering::Greater {
                return Err(format!(
                    "exceeded quota: {name}, requested: {field}={requested}, used: {used}, limit: {limit}",
                    requested = delta.display(),
                    used = current.display(),
                    limit = hard_val.as_str().unwrap_or("?"),
                ));
            }
        }
    }
    Ok(())
}

/// How much would `object` contribute to `field` if admitted? A create adds 1
/// to a count field it matches, its request to a compute/storage field, and
/// nothing to fields for other resource types.
fn delta_for_object(field: &str, mutating_gvr: &GroupVersionResource, object: &Value) -> Usage {
    match classify_field(field) {
        FieldKind::Storage { class } => {
            if mutating_gvr.resource != "persistentvolumeclaims" {
                return Usage::amount(Quantity::zero());
            }
            if class.is_some_and(|c| pvc_storage_class(object) != c) {
                return Usage::amount(Quantity::zero());
            }
            Usage::amount(pvc_storage_request(object))
        }
        FieldKind::PvcCount { class } => {
            if mutating_gvr.resource != "persistentvolumeclaims"
                || pvc_storage_class(object) != class
            {
                return Usage::count(0);
            }
            Usage::count(1)
        }
        FieldKind::Compute => {
            if mutating_gvr.resource != "pods" {
                return Usage::amount(Quantity::zero());
            }
            let (kind, key) = resolve_compute_target(field);
            Usage::amount(pod_compute(object, kind, key))
        }
        FieldKind::Count => {
            let target = match resolve_count_target(field) {
                Some(t) => t,
                None => return Usage::count(0),
            };
            if target.resource != mutating_gvr.resource || target.group != mutating_gvr.group {
                return Usage::count(0);
            }
            // Service field variants only count services of the matching type.
            if target.resource == "services" && !service_matches_field(object, field) {
                return Usage::count(0);
            }
            Usage::count(1)
        }
    }
}

fn reconcile_all(store: &Store, quota_gvr: &GroupVersionResource) {
    let result = match store.list(quota_gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("quota controller list error: {e}");
            return;
        }
    };
    let mut namespaces: FxHashSet<String> = FxHashSet::default();
    for q in &result.items {
        if let Some(ns) = q
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
        {
            namespaces.insert(ns.to_string());
        }
    }
    for ns in namespaces {
        recompute_for_namespace(store, quota_gvr, &ns);
    }
}

fn recompute_for_namespace(store: &Store, quota_gvr: &GroupVersionResource, namespace: &str) {
    let quotas = match store.list(quota_gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return,
    };
    for quota in quotas {
        recompute_one(store, quota_gvr, namespace, quota);
    }
}

fn recompute_one(store: &Store, quota_gvr: &GroupVersionResource, namespace: &str, quota: Value) {
    let name = match quota
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
    {
        Some(n) => n.to_string(),
        None => return,
    };
    let spec = quota.get("spec");
    let hard = spec
        .and_then(|s| s.get("hard"))
        .and_then(|h| h.as_object())
        .cloned()
        .unwrap_or_default();
    let scopes = spec
        .and_then(|s| s.get("scopes"))
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default();
    let scopes: Vec<String> = scopes
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let mut used = Map::new();
    for (field, _limit) in &hard {
        // Keep the field even if we can't count it — clients expect every hard
        // field to appear in used. Unknown fields resolve to a 0 count.
        let usage = used_for_field(store, namespace, field, &scopes);
        used.insert(field.clone(), Value::String(usage.display()));
    }

    let mut current = quota.clone();
    if let Some(obj) = current.as_object_mut() {
        let status = serde_json::json!({
            "hard": hard,
            "used": used,
        });
        if obj.get("status") == Some(&status) {
            return;
        }
        obj.insert("status".to_string(), status);
    }
    let rref = ResourceRef {
        gvr: quota_gvr,
        namespace: Some(namespace),
        name: &name,
    };
    if let Err(e) = store.update(&rref, &current) {
        tracing::debug!("quota '{namespace}/{name}': status update skipped: {e}");
    }
}

/// Sum the requested (or limited, for `limits.*`) amount of a compute resource
/// across all pods in the namespace whose state matches the quota's `scopes`.
/// Init containers use the upstream rule:
/// each compute field is `max(initContainersMax, sum(regularContainers))`.
fn compute_for_field(store: &Store, namespace: &str, field: &str, scopes: &[String]) -> Quantity {
    let pods_gvr = GroupVersionResource::pods();
    let items = match store.list(&pods_gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return Quantity::zero(),
    };
    let (kind, resource_key) = resolve_compute_target(field);
    let mut total = Quantity::zero();
    for pod in items {
        if !pod_matches_scopes(&pod, scopes) {
            continue;
        }
        total = total + pod_compute(&pod, kind, resource_key);
    }
    total
}

/// True if `pod` satisfies every scope in `scopes`. Pod-scope-aware quotas
/// only count/compute the subset of pods passing this predicate. Quotas
/// without scopes match every pod.
fn pod_matches_scopes(pod: &Value, scopes: &[String]) -> bool {
    scopes.iter().all(|s| pod_matches_scope(pod, s))
}

fn pod_matches_scope(pod: &Value, scope: &str) -> bool {
    match scope {
        "Terminating" => has_active_deadline(pod),
        "NotTerminating" => !has_active_deadline(pod),
        "BestEffort" => is_best_effort(pod),
        "NotBestEffort" => !is_best_effort(pod),
        // Unknown / unsupported scope (PriorityClass, CrossNamespacePodAffinity)
        // — be permissive so the quota at least sees every pod rather than
        // silently dropping all of them.
        _ => true,
    }
}

fn has_active_deadline(pod: &Value) -> bool {
    pod.get("spec")
        .and_then(|s| s.get("activeDeadlineSeconds"))
        .is_some_and(|v| v.is_number() && v.as_i64().unwrap_or(0) > 0)
}

fn is_best_effort(pod: &Value) -> bool {
    let spec = match pod.get("spec") {
        Some(s) => s,
        None => return true,
    };
    let any_with_resources = |list: Option<&Value>| -> bool {
        list.and_then(|v| v.as_array())
            .is_some_and(|arr| arr.iter().any(container_has_resources))
    };
    !any_with_resources(spec.get("containers")) && !any_with_resources(spec.get("initContainers"))
}

fn container_has_resources(c: &Value) -> bool {
    let res = match c.get("resources") {
        Some(r) => r,
        None => return false,
    };
    for k in ["requests", "limits"] {
        if let Some(map) = res.get(k).and_then(|v| v.as_object()) {
            // Any non-empty value counts. Upstream considers a container with
            // any of cpu/memory/etc. requested or limited as "not BestEffort".
            if map
                .values()
                .any(|v| v.as_str().is_some_and(|s| !s.is_empty()))
            {
                return true;
            }
        }
    }
    false
}

#[derive(Clone, Copy)]
enum ComputeKind {
    Requests,
    Limits,
}

/// Translate a quota field name into (requests-or-limits, container-resource-key).
/// For instance `"cpu"` → (Requests, "cpu"), `"limits.memory"` → (Limits, "memory"),
/// `"requests.nvidia.com/gpu"` → (Requests, "nvidia.com/gpu"), and the storage
/// class variants peel the class off and look up `requests.storage` (we don't
/// scope by class yet, so they all aggregate together — Wave 3).
fn resolve_compute_target(field: &str) -> (ComputeKind, &str) {
    if let Some(rest) = field.strip_prefix("requests.") {
        return (ComputeKind::Requests, rest);
    }
    if let Some(rest) = field.strip_prefix("limits.") {
        return (ComputeKind::Limits, rest);
    }
    // Bare shortcuts default to requests.* upstream.
    (ComputeKind::Requests, field)
}

fn pod_compute(pod: &Value, kind: ComputeKind, resource: &str) -> Quantity {
    let spec = match pod.get("spec") {
        Some(s) => s,
        None => return Quantity::zero(),
    };
    let init_sum = sum_containers(spec.get("initContainers"), kind, resource);
    let regular_sum = sum_containers(spec.get("containers"), kind, resource);
    // Upstream's effective request/limit for a pod = max(initContainers, regular).
    if init_sum.cmp(&regular_sum) == std::cmp::Ordering::Greater {
        init_sum
    } else {
        regular_sum
    }
}

fn sum_containers(list: Option<&Value>, kind: ComputeKind, resource: &str) -> Quantity {
    let arr = match list.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Quantity::zero(),
    };
    let key = match kind {
        ComputeKind::Requests => "requests",
        ComputeKind::Limits => "limits",
    };
    let mut total = Quantity::zero();
    for c in arr {
        if let Some(v) = c
            .get("resources")
            .and_then(|r| r.get(key))
            .and_then(|r| r.get(resource))
            .and_then(|v| v.as_str())
            && let Some(q) = Quantity::parse(v)
        {
            total = total + q;
        }
    }
    total
}

/// Resolve a `spec.hard` field name to a current count. Returns 0 for fields
/// we don't (yet) know how to count, which keeps the status well-formed.
/// When the quota declares scopes and the field counts pods, applies the
/// scope predicate; other resource types ignore scopes (they aren't
/// applicable per upstream).
fn count_for_field(store: &Store, namespace: &str, field: &str, scopes: &[String]) -> u64 {
    let gvr = match resolve_count_target(field) {
        Some(g) => g,
        None => return 0,
    };
    let items = match store.list(&gvr, Some(namespace), None, None, None, None) {
        Ok(r) => r.items,
        Err(_) => return 0,
    };
    if gvr.resource == "pods" && !scopes.is_empty() {
        items
            .iter()
            .filter(|p| pod_matches_scopes(p, scopes))
            .count() as u64
    } else if gvr.resource == "services" {
        items
            .iter()
            .filter(|s| service_matches_field(s, field))
            .count() as u64
    } else {
        items.len() as u64
    }
}

/// Service quota fields that filter by Service.spec.type:
///   * `services` counts all services.
///   * `services.nodeports` counts services that allocate a NodePort —
///     `NodePort` and `LoadBalancer` (LB always provisions a NodePort too).
///   * `services.loadbalancers` counts only `LoadBalancer` services.
fn service_matches_field(svc: &Value, field: &str) -> bool {
    let svc_type = svc
        .get("spec")
        .and_then(|s| s.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("ClusterIP");
    match field {
        "services" => true,
        "services.nodeports" => svc_type == "NodePort" || svc_type == "LoadBalancer",
        "services.loadbalancers" => svc_type == "LoadBalancer",
        _ => true,
    }
}

/// Map a quota field name to the GVR that should be counted. Returns None
/// for fields whose semantics aren't a simple object count (compute,
/// scoped pods, NodePort/LoadBalancer service variants, etc.).
fn resolve_count_target(field: &str) -> Option<GroupVersionResource> {
    // Generic `count/<resource>.<group>` syntax. Empty group means core.
    if let Some(rest) = field.strip_prefix("count/") {
        return Some(match rest.split_once('.') {
            Some((resource, group)) => GroupVersionResource::new(group, "v1", resource),
            None => GroupVersionResource::new("", "v1", rest),
        });
    }
    // Built-in shortcuts upstream's quota controller hardcodes.
    Some(match field {
        "pods" => GroupVersionResource::pods(),
        "services" => GroupVersionResource::services(),
        "configmaps" => GroupVersionResource::configmaps(),
        "secrets" => GroupVersionResource::secrets(),
        "resourcequotas" => GroupVersionResource::resource_quotas(),
        "persistentvolumeclaims" => GroupVersionResource::new("", "v1", "persistentvolumeclaims"),
        "replicationcontrollers" => GroupVersionResource::new("", "v1", "replicationcontrollers"),
        // Conditional service counts: same GVR, predicate applied later in
        // `service_matches_field`.
        "services.nodeports" | "services.loadbalancers" => GroupVersionResource::services(),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn open() -> (TempDir, Store) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        (dir, store)
    }

    fn pvc_gvr() -> GroupVersionResource {
        GroupVersionResource::new("", "v1", "persistentvolumeclaims")
    }

    fn create_pvc(store: &Store, ns: &str, name: &str, class: &str, size: &str) {
        let gvr = pvc_gvr();
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some(ns),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": { "name": name, "namespace": ns },
                    "spec": {
                        "storageClassName": class,
                        "resources": { "requests": { "storage": size } },
                    },
                }),
            )
            .unwrap();
    }

    fn create_pod(store: &Store, ns: &str, name: &str) {
        let gvr = GroupVersionResource::pods();
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some(ns),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": { "name": name, "namespace": ns },
                    "spec": { "containers": [] },
                }),
            )
            .unwrap();
    }

    fn create_quota(store: &Store, ns: &str, name: &str, hard: Value) {
        let gvr = GroupVersionResource::resource_quotas();
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some(ns),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "ResourceQuota",
                    "metadata": { "name": name, "namespace": ns },
                    "spec": { "hard": hard },
                }),
            )
            .unwrap();
    }

    fn used(store: &Store, ns: &str, name: &str, field: &str) -> String {
        let gvr = GroupVersionResource::resource_quotas();
        let q = store
            .get(&ResourceRef {
                gvr: &gvr,
                namespace: Some(ns),
                name,
            })
            .unwrap()
            .unwrap();
        // Direct key lookup, not a JSON pointer: the per-class field names
        // contain `/`, which `pointer` would treat as a path separator.
        q.get("status")
            .and_then(|s| s.get("used"))
            .and_then(|u| u.get(field))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    #[test]
    fn storage_used_sums_pvc_requests() {
        let (_d, store) = open();
        let quota_gvr = GroupVersionResource::resource_quotas();
        create_pvc(&store, "default", "a", "standard", "1Gi");
        create_pvc(&store, "default", "b", "standard", "2Gi");
        create_quota(&store, "default", "q", json!({ "requests.storage": "10Gi" }));

        recompute_for_namespace(&store, &quota_gvr, "default");

        let got = used(&store, "default", "q", "requests.storage");
        assert_eq!(
            Quantity::parse(&got),
            Quantity::parse("3Gi"),
            "storage must sum PVC requests, got {got}"
        );
    }

    #[test]
    fn storage_used_filters_by_class() {
        let (_d, store) = open();
        let quota_gvr = GroupVersionResource::resource_quotas();
        create_pvc(&store, "default", "a", "standard", "1Gi");
        create_pvc(&store, "default", "b", "fast", "5Gi");
        create_quota(
            &store,
            "default",
            "q",
            json!({ "standard.storageclass.storage.k8s.io/requests.storage": "10Gi" }),
        );

        recompute_for_namespace(&store, &quota_gvr, "default");

        let got = used(
            &store,
            "default",
            "q",
            "standard.storageclass.storage.k8s.io/requests.storage",
        );
        assert_eq!(
            Quantity::parse(&got),
            Quantity::parse("1Gi"),
            "per-class storage must exclude other classes, got {got}"
        );
    }

    #[test]
    fn count_used_renders_as_integer() {
        let (_d, store) = open();
        let quota_gvr = GroupVersionResource::resource_quotas();
        create_pod(&store, "default", "p1");
        create_quota(&store, "default", "q", json!({ "pods": "5" }));

        recompute_for_namespace(&store, &quota_gvr, "default");

        assert_eq!(used(&store, "default", "q", "pods"), "1");
    }

    #[test]
    fn admission_count_message_uses_integers() {
        let (_d, store) = open();
        create_pod(&store, "default", "p1");
        create_quota(&store, "default", "q", json!({ "pods": "1" }));

        let new_pod = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "p2", "namespace": "default" },
            "spec": { "containers": [] },
        });
        let err = check_admission(&store, &GroupVersionResource::pods(), "default", &new_pod)
            .expect_err("second pod must exceed pods=1");
        assert!(err.contains("pods=1"), "message should read pods=1: {err}");
        assert!(
            !err.contains("1000m"),
            "counts must not render as milli-units: {err}"
        );
    }

    #[test]
    fn admission_enforces_storage() {
        let (_d, store) = open();
        create_pvc(&store, "default", "a", "standard", "8Gi");
        create_quota(&store, "default", "q", json!({ "requests.storage": "10Gi" }));

        let big = json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": { "name": "big", "namespace": "default" },
            "spec": { "resources": { "requests": { "storage": "5Gi" } } },
        });
        assert!(
            check_admission(&store, &pvc_gvr(), "default", &big).is_err(),
            "8Gi + 5Gi must exceed 10Gi"
        );

        let small = json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": { "name": "small", "namespace": "default" },
            "spec": { "resources": { "requests": { "storage": "1Gi" } } },
        });
        assert!(
            check_admission(&store, &pvc_gvr(), "default", &small).is_ok(),
            "8Gi + 1Gi fits within 10Gi"
        );
    }
}
