use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

// Incremented before the DB commit, so failed transactions leave gaps.
// This matches K8s where revision gaps are normal and expected.
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
