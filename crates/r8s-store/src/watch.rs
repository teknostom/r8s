use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Per-channel buffer of events not yet drained by a subscriber. A subscriber
/// that falls further behind than this receives `Lagged` and must relist.
const BROADCAST_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEventType {
    #[serde(rename = "ADDED")]
    Added,
    #[serde(rename = "MODIFIED")]
    Modified,
    #[serde(rename = "DELETED")]
    Deleted,
}

impl WatchEventType {
    pub fn as_k8s_str(self) -> &'static str {
        match self {
            WatchEventType::Added => "ADDED",
            WatchEventType::Modified => "MODIFIED",
            WatchEventType::Deleted => "DELETED",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub resource_version: u64,
    pub object: Arc<serde_json::Value>,
    /// The object before a MODIFIED event; lets a filtered watcher tell an
    /// object leaving its selector (DELETED) from one changing inside it.
    pub prev_object: Option<Arc<serde_json::Value>>,
}

/// Live fan-out of mutations, keyed by GVR prefix. Replay of past events is
/// served durably from the store's revision table, not from here.
#[derive(Default, Clone)]
pub struct WatchHub {
    channels: Arc<Mutex<FxHashMap<String, broadcast::Sender<WatchEvent>>>>,
}

impl WatchHub {
    pub fn subscribe(&self, gvr_prefix: &str) -> broadcast::Receiver<WatchEvent> {
        let mut channels = self.channels.lock();
        Self::sender(&mut channels, gvr_prefix).subscribe()
    }

    pub fn notify(&self, gvr_prefix: &str, event: WatchEvent) {
        let mut channels = self.channels.lock();
        let _ = Self::sender(&mut channels, gvr_prefix).send(event);
    }

    fn sender<'a>(
        channels: &'a mut FxHashMap<String, broadcast::Sender<WatchEvent>>,
        gvr_prefix: &str,
    ) -> &'a mut broadcast::Sender<WatchEvent> {
        channels
            .entry(gvr_prefix.to_string())
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
    }
}
