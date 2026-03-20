use std::collections::hash_map::Entry::*;
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

#[derive(Default, Clone)]
pub struct WatchHub {
    channels: Arc<Mutex<FxHashMap<String, broadcast::Sender<WatchEvent>>>>,
}

impl WatchHub {
    pub fn subscribe(&self, gvr_prefix: &str) -> broadcast::Receiver<WatchEvent> {
        let mut map = self.channels.lock().expect("watch channels lock poisoned");
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
        let mut map = self.channels.lock().expect("watch channels lock poisoned");
        if let Occupied(sender) = map.entry(gvr_prefix.to_string()) {
            let _ = sender.get().send(event);
        }
    }
}
