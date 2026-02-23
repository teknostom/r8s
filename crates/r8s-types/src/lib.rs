pub mod registry;
pub mod resource;

// K8s resource types
pub mod meta;
pub mod pod;
pub mod service;
pub mod endpoints;
pub mod endpointslice;
pub mod deployment;
pub mod replicaset;
pub mod ingress;
pub mod ingressclass;
pub mod namespace;
pub mod serviceaccount;
pub mod node;
pub mod crd;

pub use resource::{GroupVersionResource, ResourceType};

// Re-export top-level resource types
pub use deployment::Deployment;
pub use endpoints::Endpoints;
pub use endpointslice::EndpointSlice;
pub use ingress::Ingress;
pub use ingressclass::IngressClass;
pub use meta::{LabelSelector, ObjectMeta, ObjectReference, OwnerReference, PodTemplateSpec};
pub use namespace::Namespace;
pub use node::Node;
pub use pod::Pod;
pub use replicaset::ReplicaSet;
pub use service::Service;
pub use serviceaccount::ServiceAccount;
pub use crd::CustomResourceDefinition;
