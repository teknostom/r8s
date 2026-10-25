pub mod certs;
pub mod crd;
pub mod cronjob;
pub mod daemonset;
pub mod deployment;
pub mod endpoints;
pub mod foreground_gc;
pub mod gc;
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

pub use manager::ControllerManager;

use r8s_types::{ObjectMeta, OwnerReference, Pod};

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
pub(crate) fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| c.type_ == CONDITION_READY && c.status == STATUS_TRUE)
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

pub(crate) fn random_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}
