//! Background cascading deletion — the low-latency fast path.
//!
//! When an owner (Deployment, ReplicaSet, Job, …) is deleted with the default
//! background policy, its dependents must be reaped. This controller does that
//! immediately off the owner's `Deleted` watch event, so a two-level cascade
//! (Deployment → ReplicaSet → pods) collapses in a couple of events rather than
//! waiting on a polling scan.
//!
//! It is deliberately best-effort: it only fires on live events and drops
//! everything on `Lagged`. Correctness is guaranteed elsewhere —
//! [`crate::foreground_gc`]'s absent-owner pass relists on a timer and reaps any
//! dependent whose owners have all vanished, with a debounce that this fast path
//! can't afford. So anything this controller misses (a lagged channel, a
//! multi-owner dependent it declines to touch) is caught there within a tick.

use r8s_store::{
    Store,
    backend::ResourceRef,
    watch::{WatchEvent, WatchEventType},
};
use r8s_types::{GroupVersionResource, ObjectMeta};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize)]
struct MetadataOnly {
    metadata: ObjectMeta,
}

/// Outcome of handling one watch event: whether the run loop should keep going.
enum Flow {
    Continue,
    Stop,
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("gc controller started");
    let deploy_gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let rc_gvr = GroupVersionResource::new("", "v1", "replicationcontrollers");
    let sts_gvr = GroupVersionResource::stateful_sets();
    let ds_gvr = GroupVersionResource::daemon_sets();
    let job_gvr = GroupVersionResource::jobs();
    let cj_gvr = GroupVersionResource::cron_jobs();
    let pods_gvr = GroupVersionResource::pods();

    let mut deploy_rx = store.watch(&deploy_gvr);
    let mut rs_rx = store.watch(&rs_gvr);
    let mut rc_rx = store.watch(&rc_gvr);
    let mut sts_rx = store.watch(&sts_gvr);
    let mut ds_rx = store.watch(&ds_gvr);
    let mut job_rx = store.watch(&job_gvr);
    let mut cj_rx = store.watch(&cj_gvr);

    loop {
        let flow = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("gc controller shutting down");
                return Ok(());
            }
            event = deploy_rx.recv() => on_owner_deleted(&store, event, &rs_gvr),
            event = rs_rx.recv() => on_owner_deleted(&store, event, &pods_gvr),
            event = rc_rx.recv() => on_owner_deleted(&store, event, &pods_gvr),
            event = sts_rx.recv() => on_owner_deleted(&store, event, &pods_gvr),
            event = ds_rx.recv() => on_owner_deleted(&store, event, &pods_gvr),
            event = cj_rx.recv() => on_owner_deleted(&store, event, &job_gvr),
            event = job_rx.recv() => on_owner_deleted(&store, event, &pods_gvr),
        };
        if let Flow::Stop = flow {
            return Ok(());
        }
    }
}

/// On an owner's `Deleted` event, reap the dependents it solely owned. `Lagged`
/// and non-delete events are ignored — foreground-gc's timed scan is the
/// correctness backstop. `Closed` means the store is gone: stop.
fn on_owner_deleted(
    store: &Store,
    event: Result<WatchEvent, broadcast::error::RecvError>,
    child_gvr: &GroupVersionResource,
) -> Flow {
    match event {
        Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
            let meta: MetadataOnly = match serde_json::from_value(event.object.as_ref().clone()) {
                Ok(m) => m,
                Err(_) => return Flow::Continue,
            };
            if let Some(uid) = meta.metadata.uid.as_deref().filter(|u| !u.is_empty()) {
                delete_owned(store, child_gvr, uid);
            }
            Flow::Continue
        }
        Err(broadcast::error::RecvError::Closed) => Flow::Stop,
        _ => Flow::Continue,
    }
}

/// True when `owner_uid` is the object's one and only owner. We reap on that
/// fast path only; a dependent with additional owners might still be held by a
/// live one, so we defer it to foreground-gc's absent-owner pass, which checks
/// every ref before reaping.
fn is_sole_owner(meta: &ObjectMeta, owner_uid: &str) -> bool {
    matches!(
        meta.owner_references.as_deref(),
        Some([oref]) if oref.uid == owner_uid
    )
}

fn delete_owned(store: &Store, child_gvr: &GroupVersionResource, owner_uid: &str) {
    let children = match store.list_as::<MetadataOnly>(child_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("gc list error: {e}");
            return;
        }
    };
    for meta in &children {
        if is_sole_owner(&meta.metadata, owner_uid) {
            let name = meta.metadata.name.as_deref().unwrap_or("");
            let namespace = meta.metadata.namespace.as_deref();
            let resource_ref = ResourceRef {
                gvr: child_gvr,
                namespace,
                name,
            };
            match store.delete(&resource_ref) {
                Ok(_) => tracing::info!(
                    "gc: deleted {}/{} (owner uid={})",
                    child_gvr.resource,
                    name,
                    owner_uid
                ),
                Err(e) => {
                    tracing::warn!("gc: failed to delete {}/{}: {e}", child_gvr.resource, name)
                }
            }
        }
    }
}
