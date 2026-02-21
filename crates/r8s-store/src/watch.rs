use std::collections::hash_map::Entry::*;
use std::sync::Mutex;

use rustc_hash::FxHashMap;
use tokio::sync::broadcast;

/// Watch event types matching K8s watch semantics.
#[derive(Debug, Clone)]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
}

/// A watch event containing the event type, resource version, and serialized object.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub event_type: WatchEventType,
    pub resource_version: u64,
    pub object: serde_json::Value,
}

#[derive(Default)]
pub struct WatchHub {
    channels: Mutex<FxHashMap<String, broadcast::Sender<WatchEvent>>>,
}

impl WatchHub {
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(FxHashMap::default()),
        }
    }

    pub fn subscribe(&self, gvr_prefix: &str) -> broadcast::Receiver<WatchEvent> {
        let mut map = self
            .channels
            .lock()
            .expect("Unable to lock channels mutex. FATAL");
        match map.entry(gvr_prefix.to_string()) {
            Occupied(entry) => entry.get().subscribe(),
            Vacant(slot) => {
                let (sender, receiver) = broadcast::channel(1024);
                slot.insert(sender);
                receiver
            }
        }
    }

    pub fn notify(&self, gvr_prefix: &str, event: WatchEvent) {
        let mut map = self
            .channels
            .lock()
            .expect("Unable to lock channels mutex. FATAL");
        if let Occupied(sender) = map.entry(gvr_prefix.to_string()) {
            let _ = sender.get().send(event); // Ignore no receivers error
        }
    }
}
