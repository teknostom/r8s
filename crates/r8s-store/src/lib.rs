pub mod backend;
pub mod error;
pub mod index;
pub mod revision;
pub mod watch;

// Re-export the main store interface
pub use backend::Store;
