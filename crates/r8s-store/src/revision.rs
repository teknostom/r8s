use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Tracks the global monotonic resource version counter.
/// Every mutation increments this and the new value becomes the resource's resourceVersion.
#[derive(Clone)]
pub struct RevisionCounter {
    revision_id: Arc<AtomicU64>,
}

impl RevisionCounter {
    pub fn new(initial: u64) -> RevisionCounter {
        Self {
            revision_id: Arc::new(AtomicU64::new(initial)),
        }
    }

    pub fn next(&self) -> u64 {
        self.revision_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn current(&self) -> u64 {
        self.revision_id.load(Ordering::Relaxed)
    }
}
