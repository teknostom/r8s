pub mod registry;
pub mod resource;

pub use resource::{GroupVersionResource, ResourceType};

pub use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, ContainerState,
    ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus,
    EmptyDirVolumeSource, EndpointAddress, EndpointPort, EndpointSubset, Endpoints, EnvVar,
    EnvVarSource, ExecAction, HTTPGetAction, HostPathVolumeSource, LocalObjectReference, Namespace,
    NamespaceStatus, Node, NodeCondition, NodeSpec, NodeStatus, NodeSystemInfo, ObjectReference,
    Pod, PodCondition, PodIP, PodSpec, PodStatus, PodTemplateSpec, Probe, Secret,
    SecretVolumeSource, Service, ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction, Volume,
    VolumeMount,
};

pub use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

pub use k8s_openapi::api::apps::v1::{
    DaemonSet, DaemonSetSpec, DaemonSetStatus, Deployment, DeploymentCondition, DeploymentSpec,
    DeploymentStatus, ReplicaSet, ReplicaSetSpec, ReplicaSetStatus, StatefulSet, StatefulSetSpec,
    StatefulSetStatus,
};

pub use k8s_openapi::api::batch::v1::{
    CronJob, CronJobSpec, CronJobStatus, Job, JobCondition, JobSpec, JobStatus, JobTemplateSpec,
};

pub use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressClass, IngressClassSpec,
    IngressRule, IngressServiceBackend, IngressSpec, ServiceBackendPort,
};

pub use k8s_openapi::api::discovery::v1::{
    Endpoint as SliceEndpoint, EndpointConditions, EndpointSlice,
};

pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelector, ObjectMeta, OwnerReference,
};

pub use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

pub use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
    CustomResourceDefinitionVersion,
};
