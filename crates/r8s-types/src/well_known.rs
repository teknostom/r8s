//! Well-known protocol tokens shared between the API server and the
//! controllers. These are load-bearing strings both sides must agree on
//! verbatim; keeping a single definition here stops the producer and consumer
//! from drifting apart.

/// Finalizer the API server stamps when a client deletes an object with
/// `propagationPolicy=Foreground`. The foreground-GC reconciler strips it once
/// every dependent is gone, at which point the object is actually removed.
pub const FOREGROUND_FINALIZER: &str = "foregroundDeletion";
