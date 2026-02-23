use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Tracks the global monotonic resource version counter.
/// Every mutation increments this and the new value becomes the resource's resourceVersion.
///
/// Note: the counter is incremented before the DB transaction commits, so a failed
/// commit leaves a gap. This matches real K8s behavior where resourceVersion gaps are
/// normal and expected. Consumers must not assume contiguous revision sequences.
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
        self.revision_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn current(&self) -> u64 {
        self.revision_id.load(Ordering::SeqCst)
    }
}
