//! Taint-based eviction: a pod on a node whose NoExecute taints it does not
//! tolerate is deleted at once; one that tolerates them with a
//! `tolerationSeconds` is deleted after the shortest such seconds among the
//! tolerations in use; removing the taint cancels the pending eviction.

use r8s_store::{
    Store,
    backend::ResourceRef,
    events::{NORMAL, ObjectRef, Recorder},
    watch::WatchEventType,
};
use r8s_types::GroupVersionResource;
use rustc_hash::FxHashMap;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const TICK: Duration = Duration::from_secs(1);
const COMPONENT: &str = "taint-eviction-controller";

/// A pod's scheduled eviction.
struct Eviction {
    namespace: Option<String>,
    name: String,
    due: Instant,
}

/// Pending evictions by pod uid, and the NoExecute taints last seen per node.
#[derive(Default)]
struct State {
    pending: FxHashMap<String, Eviction>,
    node_taints: FxHashMap<String, Vec<Value>>,
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("taint eviction controller started");
    let recorder = Recorder::new(store.clone(), COMPONENT, "");
    let mut state = State::default();
    let mut node_rx = store.watch(&GroupVersionResource::nodes());
    let mut pod_rx = store.watch(&GroupVersionResource::pods());
    resync(&store, &mut state);

    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("taint eviction controller shutting down");
                return Ok(());
            }
            _ = tick.tick() => evict_due(&store, &recorder, &mut state),
            event = node_rx.recv() => match event {
                Ok(event) => {
                    let Some(node) = event.object["metadata"]["name"].as_str() else { continue };
                    let taints = if matches!(event.event_type, WatchEventType::Deleted) {
                        Vec::new()
                    } else {
                        no_execute_taints(&event.object)
                    };
                    if state.node_taints.get(node) == Some(&taints) {
                        continue;
                    }
                    state.node_taints.insert(node.to_string(), taints.clone());
                    for pod in pods_on(&store, node) {
                        schedule(&mut state, &pod, &taints);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => resync(&store, &mut state),
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
            event = pod_rx.recv() => match event {
                Ok(event) => {
                    let uid = event.object["metadata"]["uid"].as_str().unwrap_or("");
                    if matches!(event.event_type, WatchEventType::Deleted) {
                        state.pending.remove(uid);
                        continue;
                    }
                    let Some(node) = event.object["spec"]["nodeName"].as_str() else { continue };
                    let taints = state.node_taints.get(node).cloned().unwrap_or_default();
                    schedule(&mut state, &event.object, &taints);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => resync(&store, &mut state),
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
        }
    }
}

/// Rebuilds the state from the store: every node's taints, every pod's
/// eviction.
fn resync(store: &Store, state: &mut State) {
    let Ok(nodes) = store.list(&GroupVersionResource::nodes(), None, None, None, None, None) else {
        return;
    };
    let Ok(pods) = store.list(&GroupVersionResource::pods(), None, None, None, None, None) else {
        return;
    };
    state.node_taints = nodes
        .items
        .iter()
        .filter_map(|n| {
            let name = n["metadata"]["name"].as_str()?;
            Some((name.to_string(), no_execute_taints(n)))
        })
        .collect();
    let live: rustc_hash::FxHashSet<&str> = pods
        .items
        .iter()
        .filter_map(|p| p["metadata"]["uid"].as_str())
        .collect();
    state.pending.retain(|uid, _| live.contains(uid.as_str()));
    for pod in &pods.items {
        let Some(node) = pod["spec"]["nodeName"].as_str() else {
            continue;
        };
        let taints = state.node_taints.get(node).cloned().unwrap_or_default();
        schedule(state, pod, &taints);
    }
}

/// Pods bound to `node`.
fn pods_on(store: &Store, node: &str) -> Vec<Value> {
    store
        .list(&GroupVersionResource::pods(), None, None, None, None, None)
        .map(|r| {
            r.items
                .into_iter()
                .filter(|p| p["spec"]["nodeName"].as_str() == Some(node))
                .collect()
        })
        .unwrap_or_default()
}

/// Schedules, keeps or cancels `pod`'s eviction for `taints`. A pending
/// eviction keeps its due time; one that no longer tolerates the taints at
/// all is due now.
fn schedule(state: &mut State, pod: &Value, taints: &[Value]) {
    let meta = &pod["metadata"];
    let Some(uid) = meta["uid"].as_str() else {
        return;
    };
    if meta.get("deletionTimestamp").is_some() {
        state.pending.remove(uid);
        return;
    }
    match eviction_delay(pod, taints) {
        None => {
            state.pending.remove(uid);
        }
        Some(delay) => {
            if !delay.is_zero() && state.pending.contains_key(uid) {
                return;
            }
            state.pending.insert(
                uid.to_string(),
                Eviction {
                    namespace: meta["namespace"].as_str().map(String::from),
                    name: meta["name"].as_str().unwrap_or("").to_string(),
                    due: Instant::now() + delay,
                },
            );
        }
    }
}

/// Deletes the pods whose eviction is due.
fn evict_due(store: &Store, recorder: &Recorder, state: &mut State) {
    let now = Instant::now();
    let due: Vec<String> = state
        .pending
        .iter()
        .filter(|(_, e)| e.due <= now)
        .map(|(uid, _)| uid.clone())
        .collect();
    let gvr = GroupVersionResource::pods();
    for uid in due {
        let Some(eviction) = state.pending.remove(&uid) else {
            continue;
        };
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: eviction.namespace.as_deref(),
            name: &eviction.name,
        };
        let Ok(Some(pod)) = store.get(&rref) else {
            continue;
        };
        if pod["metadata"]["uid"].as_str() != Some(uid.as_str()) {
            continue;
        }
        let ns = eviction.namespace.as_deref().unwrap_or("default");
        tracing::info!(
            "evicting pod '{ns}/{}': NoExecute taint not tolerated",
            eviction.name
        );
        recorder.event(
            &ObjectRef::pod(eviction.namespace.as_deref(), &eviction.name, &uid),
            NORMAL,
            "TaintManagerEviction",
            &format!("Marking for deletion Pod {ns}/{}", eviction.name),
        );
        if let Err(e) = store.delete(&rref) {
            tracing::warn!("evict pod '{ns}/{}': {e}", eviction.name);
        }
    }
}

/// A node's taints with effect NoExecute.
fn no_execute_taints(node: &Value) -> Vec<Value> {
    node["spec"]["taints"]
        .as_array()
        .map(|taints| {
            taints
                .iter()
                .filter(|t| t["effect"].as_str() == Some("NoExecute"))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `toleration` tolerates `taint`: an empty key with operator Exists
/// matches every taint; otherwise the keys match and, for operator Equal
/// (the default), so do the values; an empty effect matches every effect.
fn tolerates(toleration: &Value, taint: &Value) -> bool {
    let str_of = |v: &Value, key: &str| v[key].as_str().unwrap_or("").to_string();
    let effect_ok = {
        let effect = str_of(toleration, "effect");
        effect.is_empty() || effect == str_of(taint, "effect")
    };
    if !effect_ok {
        return false;
    }
    let exists = str_of(toleration, "operator") == "Exists";
    let key = str_of(toleration, "key");
    if key.is_empty() {
        return exists;
    }
    if key != str_of(taint, "key") {
        return false;
    }
    exists || str_of(toleration, "value") == str_of(taint, "value")
}

/// How long `pod` may stay with `taints`: `None` when it tolerates all of
/// them without a `tolerationSeconds`, zero when it does not tolerate one of
/// them, else the shortest `tolerationSeconds` among the tolerations in use.
fn eviction_delay(pod: &Value, taints: &[Value]) -> Option<Duration> {
    if taints.is_empty() {
        return None;
    }
    let tolerations = pod["spec"]["tolerations"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut used: Vec<&Value> = Vec::new();
    for taint in taints {
        let matching: Vec<&Value> = tolerations.iter().filter(|t| tolerates(t, taint)).collect();
        if matching.is_empty() {
            return Some(Duration::ZERO);
        }
        used.extend(matching);
    }
    let mut shortest: Option<i64> = None;
    for toleration in used {
        if let Some(secs) = toleration["tolerationSeconds"].as_i64() {
            if secs <= 0 {
                return Some(Duration::ZERO);
            }
            shortest = Some(shortest.map_or(secs, |s| s.min(secs)));
        }
    }
    shortest.map(|s| Duration::from_secs(s as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn taint(key: &str, value: &str) -> Value {
        json!({"key": key, "value": value, "effect": "NoExecute"})
    }

    fn pod(tolerations: Value) -> Value {
        json!({"metadata": {"name": "p", "uid": "u"}, "spec": {"tolerations": tolerations}})
    }

    #[test]
    fn untolerated_taints_evict_at_once() {
        let taints = [taint("k", "v")];
        assert_eq!(
            eviction_delay(&pod(json!([])), &taints),
            Some(Duration::ZERO)
        );
        let wrong_value = pod(json!([{"key": "k", "value": "other", "effect": "NoExecute"}]));
        assert_eq!(eviction_delay(&wrong_value, &taints), Some(Duration::ZERO));
        let no_schedule_only = pod(json!([{"key": "k", "value": "v", "effect": "NoSchedule"}]));
        assert_eq!(
            eviction_delay(&no_schedule_only, &taints),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn tolerations_without_seconds_last_forever() {
        let taints = [taint("k", "v")];
        let exact = pod(json!([{"key": "k", "value": "v", "effect": "NoExecute"}]));
        assert_eq!(eviction_delay(&exact, &taints), None);
        let any = pod(json!([{"operator": "Exists"}]));
        assert_eq!(eviction_delay(&any, &taints), None);
        let any_effect = pod(json!([{"key": "k", "operator": "Exists"}]));
        assert_eq!(eviction_delay(&any_effect, &taints), None);
        assert_eq!(eviction_delay(&pod(json!([])), &[]), None);
    }

    #[test]
    fn the_shortest_toleration_seconds_wins() {
        let taints = [taint("a", "1"), taint("b", "2")];
        let p = pod(json!([
            {"key": "a", "value": "1", "effect": "NoExecute", "tolerationSeconds": 25},
            {"key": "b", "value": "2", "effect": "NoExecute", "tolerationSeconds": 5},
        ]));
        assert_eq!(eviction_delay(&p, &taints), Some(Duration::from_secs(5)));
        let partly_forever = pod(json!([
            {"key": "a", "value": "1", "effect": "NoExecute"},
            {"key": "b", "value": "2", "effect": "NoExecute", "tolerationSeconds": 10},
        ]));
        assert_eq!(
            eviction_delay(&partly_forever, &taints),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn removing_the_taint_cancels_a_pending_eviction() {
        let mut state = State::default();
        let p = json!({"metadata": {"name": "p", "namespace": "ns", "uid": "u"},
                       "spec": {"nodeName": "n", "tolerations": [
                           {"key": "k", "value": "v", "effect": "NoExecute", "tolerationSeconds": 10}]}});
        schedule(&mut state, &p, &[taint("k", "v")]);
        let due = state.pending["u"].due;
        assert!(due > Instant::now() + Duration::from_secs(8));
        schedule(&mut state, &p, &[taint("k", "v")]);
        assert_eq!(
            state.pending["u"].due, due,
            "a pending eviction keeps its time"
        );
        schedule(&mut state, &p, &[]);
        assert!(state.pending.is_empty());
    }
}
