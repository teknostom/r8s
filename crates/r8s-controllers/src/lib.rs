pub mod crd;
pub mod deployment;
pub mod endpoints;
pub mod gc;
pub mod manager;
pub mod namespace;
pub mod replicaset;

pub use manager::ControllerManager;
