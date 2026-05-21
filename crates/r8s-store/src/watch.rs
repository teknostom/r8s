use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rustc_hash::FxHashMap;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub resource_version: u64,
    pub object: serde_json::Value,
}

/// Per-GVR history + live broadcast pair. The history buffer keeps recent
/// events so clients that open a watch with `resourceVersion=N` can be caught
/// up on what happened after N without doing a fresh list — matching the
/// "watch cache" behaviour upstream's apiserver implements via etcd revisions.
struct GvrChannel {
    sender: broadcast::Sender<WatchEvent>,
    /// Newest-last. Bounded by `HISTORY_CAP`; oldest are evicted on push.
    history: VecDeque<WatchEvent>,
}

/// How many past events to retain per GVR. Sized so the typical
/// create-then-delete-then-watch primer used by apiextensions tests always
/// finds its target event; tune up if a watch test starts flaking on a
/// short replay window.
const HISTORY_CAP: usize = 4096;

#[derive(Default, Clone)]
pub struct WatchHub {
    channels: Arc<Mutex<FxHashMap<String, GvrChannel>>>,
}

/// Returned when a client asks to replay from a resourceVersion older than the
/// oldest event still in the history buffer. The HTTP layer maps this to a
/// 410 Gone, telling the client to fall back to list-then-watch.
pub struct TooOld {
    pub oldest_available: u64,
    pub requested: u64,
}

impl WatchHub {
    /// Subscribe to live events from now onwards. Used by controllers and by
    /// the watch handler when no resourceVersion was supplied.
    pub fn subscribe(&self, gvr_prefix: &str) -> broadcast::Receiver<WatchEvent> {
        let mut map = self.channels.lock().expect("watch channels lock poisoned");
        Self::ensure(&mut map, gvr_prefix).sender.subscribe()
    }

    /// Subscribe with history: returns every buffered event whose
    /// `resourceVersion > since_rv` (in order) plus a live receiver. The
    /// snapshot and the subscribe happen under one lock, so no events can be
    /// lost between the two — and any event sent after this call goes to the
    /// returned receiver, not the history slice. If `since_rv` is older than
    /// the oldest event in the buffer, returns `TooOld` so the caller can
    /// reject with 410 Gone.
    pub fn subscribe_with_history(
        &self,
        gvr_prefix: &str,
        since_rv: u64,
    ) -> Result<(Vec<WatchEvent>, broadcast::Receiver<WatchEvent>), TooOld> {
        let mut map = self.channels.lock().expect("watch channels lock poisoned");
        let chan = Self::ensure(&mut map, gvr_prefix);

        // since_rv=0 means "I want everything I can get" — semantically the
        // same as a fresh subscriber that's also being told the early history.
        // The HTTP layer treats rv=0 specially anyway (it does a list first),
        // so we never need to return TooOld for it.
        if since_rv > 0 {
            if let Some(oldest) = chan.history.front() {
                if oldest.resource_version > since_rv {
                    return Err(TooOld {
                        oldest_available: oldest.resource_version,
                        requested: since_rv,
                    });
                }
            }
            // If the buffer is empty we cannot prove that nothing happened
            // since `since_rv`, but no information has been lost either — let
            // the caller subscribe and start fresh. This is the same posture
            // upstream takes for a freshly-created resource type.
        }

        let history: Vec<WatchEvent> = chan
            .history
            .iter()
            .filter(|e| e.resource_version > since_rv)
            .cloned()
            .collect();
        let receiver = chan.sender.subscribe();
        Ok((history, receiver))
    }

    /// Append `event` to the per-GVR history (evicting the oldest if at cap)
    /// and broadcast it to live subscribers. Buffer write and broadcast send
    /// happen under one lock so `subscribe_with_history` sees a consistent
    /// cut: every event with rv ≤ snapshot-time is in the returned history;
    /// every event after is delivered live.
    pub fn notify(&self, gvr_prefix: &str, event: WatchEvent) {
        let mut map = self.channels.lock().expect("watch channels lock poisoned");
        let chan = Self::ensure(&mut map, gvr_prefix);
        if chan.history.len() == HISTORY_CAP {
            chan.history.pop_front();
        }
        chan.history.push_back(event.clone());
        let _ = chan.sender.send(event);
    }

    fn ensure<'a>(
        map: &'a mut FxHashMap<String, GvrChannel>,
        gvr_prefix: &str,
    ) -> &'a mut GvrChannel {
        map.entry(gvr_prefix.to_string()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(1024);
            GvrChannel {
                sender,
                history: VecDeque::with_capacity(64),
            }
        })
    }
}
