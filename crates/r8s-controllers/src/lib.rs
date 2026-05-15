pub mod certs;
pub mod crd;
pub mod cronjob;
pub mod daemonset;
pub mod deployment;
pub mod endpoints;
pub mod gc;
pub mod job;
pub mod manager;
pub mod namespace;
pub mod pod_admission;
pub mod replicaset;
pub mod serviceaccount;
pub mod statefulset;

pub use manager::ControllerManager;

use r8s_types::{ObjectMeta, OwnerReference};

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

pub(crate) fn random_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvxyz0123456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}
