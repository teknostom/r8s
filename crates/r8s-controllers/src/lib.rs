pub mod crd;
pub mod deployment;
pub mod endpoints;
pub mod gc;
pub mod manager;
pub mod namespace;
pub mod replicaset;
pub mod statefulset;

pub use manager::ControllerManager;

use r8s_types::ObjectMeta;

pub(crate) fn is_owned_by(meta: &ObjectMeta, owner_uid: &str) -> bool {
    meta.owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|r| r.uid == owner_uid)
}
