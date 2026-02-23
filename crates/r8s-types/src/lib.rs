pub mod registry;
pub mod resource;

pub use resource::{GroupVersionResource, ResourceType};

// --- Re-exports from k8s-openapi ---

// Core v1 types
pub use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, ContainerState,
    ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus,
    EmptyDirVolumeSource, EndpointAddress, EndpointPort, EndpointSubset, Endpoints, EnvVar,
    EnvVarSource, HostPathVolumeSource, LocalObjectReference, Namespace, NamespaceStatus, Node,
    NodeCondition, NodeSpec, NodeStatus, NodeSystemInfo, ObjectReference, Pod, PodCondition, PodIP,
    PodSpec, PodStatus, PodTemplateSpec, Secret, SecretVolumeSource, Service, ServiceAccount,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};

// Resource types
pub use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

// Time type
pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

// Apps v1 types
pub use k8s_openapi::api::apps::v1::{
    DaemonSet, DaemonSetSpec, DaemonSetStatus, Deployment, DeploymentCondition, DeploymentSpec,
    DeploymentStatus, ReplicaSet, ReplicaSetSpec, ReplicaSetStatus, StatefulSet, StatefulSetSpec,
    StatefulSetStatus,
};

// Batch v1 types
pub use k8s_openapi::api::batch::v1::{
    CronJob, CronJobSpec, CronJobStatus, Job, JobCondition, JobSpec, JobStatus, JobTemplateSpec,
};

// Networking v1 types
pub use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressClass, IngressClassSpec,
    IngressRule, IngressSpec, IngressServiceBackend, ServiceBackendPort,
};

// Discovery v1 types
pub use k8s_openapi::api::discovery::v1::{
    Endpoint as SliceEndpoint, EndpointConditions, EndpointSlice,
};

// Meta v1 types
pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelector, ObjectMeta, OwnerReference,
};

// Utility types
pub use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

// CRD types
pub use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, CustomResourceDefinitionNames,
    CustomResourceDefinitionSpec, CustomResourceDefinitionVersion,
};
