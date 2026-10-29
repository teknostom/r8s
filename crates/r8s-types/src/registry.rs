use std::sync::{Arc, RwLock};

use rustc_hash::FxHashMap;

use crate::{GroupVersionResource, ResourceType};

struct RegistryInner {
    by_gvr: FxHashMap<GroupVersionResource, Arc<ResourceType>>,
    by_resource: FxHashMap<String, Arc<ResourceType>>,
}

/// Thread-safe registry of resource types; CRDs register at runtime.
#[derive(Clone)]
pub struct ResourceRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                by_gvr: FxHashMap::default(),
                by_resource: FxHashMap::default(),
            })),
        }
    }

    pub fn register(&self, rt: ResourceType) {
        let rt = Arc::new(rt);
        let mut inner = self.inner.write().expect("registry lock poisoned");
        inner.by_gvr.insert(rt.gvr.clone(), Arc::clone(&rt));
        inner.by_resource.insert(rt.gvr.resource.clone(), rt);
    }

    pub fn unregister(&self, gvr: &GroupVersionResource) {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        inner.by_gvr.remove(gvr);
        // `by_resource` is keyed by plural alone, which several GVRs can share
        // (e.g. autoscaling/v1 and /v2 horizontalpodautoscalers, or core and
        // events.k8s.io events). Only drop the entry if it still points at THIS
        // gvr; then repoint to a surviving version with the same plural so the
        // resource stays resolvable by name.
        if inner
            .by_resource
            .get(&gvr.resource)
            .is_some_and(|rt| &rt.gvr == gvr)
        {
            inner.by_resource.remove(&gvr.resource);
            if let Some(rt) = inner
                .by_gvr
                .values()
                .find(|rt| rt.gvr.resource == gvr.resource)
                .cloned()
            {
                inner.by_resource.insert(gvr.resource.clone(), rt);
            }
        }
    }

    pub fn get_by_gvr(&self, gvr: &GroupVersionResource) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_gvr.get(gvr).cloned()
    }

    pub fn get_by_resource(&self, resource: &str) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_resource.get(resource).cloned()
    }

    pub fn iter(&self) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_gvr.values().cloned().collect()
    }

    pub fn resources_for_group_version(
        &self,
        group: &str,
        version: &str,
    ) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner
            .by_gvr
            .values()
            .filter(|rt| rt.gvr.group == group && rt.gvr.version == version)
            .cloned()
            .collect()
    }

    pub fn default_mvp() -> Self {
        let r = Self::new();
        for b in builtins() {
            let schema = crate::openapi::schema_for(&b.gvr.group, &b.gvr.version, b.kind);
            let shared_events = b.gvr.group == "events.k8s.io" && b.gvr.resource == "events";
            r.register(ResourceType {
                gvr: b.gvr,
                kind: b.kind.to_string(),
                namespaced: b.namespaced,
                singular: b.singular.to_string(),
                short_names: b.short_names.iter().map(|s| s.to_string()).collect(),
                subresources: b.subresources.iter().map(|s| s.to_string()).collect(),
                schema,
                // events.k8s.io Events are stored as core Events.
                storage_gvr: shared_events.then(GroupVersionResource::events),
            });
        }
        r
    }
}

/// A built-in resource type registered by [`ResourceRegistry::default_mvp`].
/// The GVR uses a [`GroupVersionResource`] constructor where one exists, so each
/// (group, version, resource) triple is defined in exactly one place.
struct Builtin {
    gvr: GroupVersionResource,
    kind: &'static str,
    namespaced: bool,
    singular: &'static str,
    short_names: &'static [&'static str],
    subresources: &'static [&'static str],
}

#[rustfmt::skip]
fn builtins() -> Vec<Builtin> {
    use GroupVersionResource as G;
    vec![
        Builtin { gvr: G::pods(), kind: "Pod", namespaced: true, singular: "pod", short_names: &["po"], subresources: &[] },
        Builtin { gvr: G::namespaces(), kind: "Namespace", namespaced: false, singular: "namespace", short_names: &["ns"], subresources: &[] },
        Builtin { gvr: G::configmaps(), kind: "ConfigMap", namespaced: true, singular: "configmap", short_names: &["cm"], subresources: &[] },
        Builtin { gvr: G::secrets(), kind: "Secret", namespaced: true, singular: "secret", short_names: &[], subresources: &[] },
        Builtin { gvr: G::services(), kind: "Service", namespaced: true, singular: "service", short_names: &["svc"], subresources: &[] },
        Builtin { gvr: G::service_accounts(), kind: "ServiceAccount", namespaced: true, singular: "serviceaccount", short_names: &["sa"], subresources: &[] },
        Builtin { gvr: G::endpoints(), kind: "Endpoints", namespaced: true, singular: "endpoints", short_names: &["ep"], subresources: &[] },
        Builtin { gvr: G::nodes(), kind: "Node", namespaced: false, singular: "node", short_names: &["no"], subresources: &[] },
        Builtin { gvr: G::new("", "v1", "events"), kind: "Event", namespaced: true, singular: "event", short_names: &["ev"], subresources: &[] },
        Builtin { gvr: G::new("", "v1", "limitranges"), kind: "LimitRange", namespaced: true, singular: "limitrange", short_names: &["limits"], subresources: &[] },
        Builtin { gvr: G::resource_quotas(), kind: "ResourceQuota", namespaced: true, singular: "resourcequota", short_names: &["quota"], subresources: &[] },
        Builtin { gvr: G::pod_templates(), kind: "PodTemplate", namespaced: true, singular: "podtemplate", short_names: &[], subresources: &[] },
        Builtin { gvr: G::deployments(), kind: "Deployment", namespaced: true, singular: "deployment", short_names: &["deploy"], subresources: &["scale"] },
        Builtin { gvr: G::replica_sets(), kind: "ReplicaSet", namespaced: true, singular: "replicaset", short_names: &["rs"], subresources: &["scale"] },
        Builtin { gvr: G::stateful_sets(), kind: "StatefulSet", namespaced: true, singular: "statefulset", short_names: &["sts"], subresources: &["scale"] },
        Builtin { gvr: G::daemon_sets(), kind: "DaemonSet", namespaced: true, singular: "daemonset", short_names: &["ds"], subresources: &[] },
        Builtin { gvr: G::controller_revisions(), kind: "ControllerRevision", namespaced: true, singular: "controllerrevision", short_names: &[], subresources: &[] },
        Builtin { gvr: G::ingresses(), kind: "Ingress", namespaced: true, singular: "ingress", short_names: &["ing"], subresources: &[] },
        Builtin { gvr: G::ingress_classes(), kind: "IngressClass", namespaced: false, singular: "ingressclass", short_names: &[], subresources: &[] },
        // Inert: no CNI policy enforcement.
        Builtin { gvr: G::new("networking.k8s.io", "v1", "networkpolicies"), kind: "NetworkPolicy", namespaced: true, singular: "networkpolicy", short_names: &["netpol"], subresources: &[] },
        Builtin { gvr: G::endpoint_slices(), kind: "EndpointSlice", namespaced: true, singular: "endpointslice", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("policy", "v1", "poddisruptionbudgets"), kind: "PodDisruptionBudget", namespaced: true, singular: "poddisruptionbudget", short_names: &["pdb"], subresources: &[] },
        Builtin { gvr: G::jobs(), kind: "Job", namespaced: true, singular: "job", short_names: &[], subresources: &[] },
        Builtin { gvr: G::cron_jobs(), kind: "CronJob", namespaced: true, singular: "cronjob", short_names: &["cj"], subresources: &[] },
        Builtin { gvr: G::new("autoscaling", "v2", "horizontalpodautoscalers"), kind: "HorizontalPodAutoscaler", namespaced: true, singular: "horizontalpodautoscaler", short_names: &["hpa"], subresources: &[] },
        Builtin { gvr: G::crds(), kind: "CustomResourceDefinition", namespaced: false, singular: "customresourcedefinition", short_names: &["crd", "crds"], subresources: &[] },
        Builtin { gvr: G::new("rbac.authorization.k8s.io", "v1", "roles"), kind: "Role", namespaced: true, singular: "role", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("rbac.authorization.k8s.io", "v1", "rolebindings"), kind: "RoleBinding", namespaced: true, singular: "rolebinding", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("rbac.authorization.k8s.io", "v1", "clusterroles"), kind: "ClusterRole", namespaced: false, singular: "clusterrole", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("rbac.authorization.k8s.io", "v1", "clusterrolebindings"), kind: "ClusterRoleBinding", namespaced: false, singular: "clusterrolebinding", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("coordination.k8s.io", "v1", "leases"), kind: "Lease", namespaced: true, singular: "lease", short_names: &[], subresources: &[] },
        // Inert: stored only so watches resolve.
        Builtin { gvr: G::new("apiregistration.k8s.io", "v1", "apiservices"), kind: "APIService", namespaced: false, singular: "apiservice", short_names: &[], subresources: &[] },
        // Stored; admission webhooks are not dialed yet.
        Builtin { gvr: G::new("admissionregistration.k8s.io", "v1", "validatingwebhookconfigurations"), kind: "ValidatingWebhookConfiguration", namespaced: false, singular: "validatingwebhookconfiguration", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("admissionregistration.k8s.io", "v1", "validatingadmissionpolicies"), kind: "ValidatingAdmissionPolicy", namespaced: false, singular: "validatingadmissionpolicy", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("admissionregistration.k8s.io", "v1", "validatingadmissionpolicybindings"), kind: "ValidatingAdmissionPolicyBinding", namespaced: false, singular: "validatingadmissionpolicybinding", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("admissionregistration.k8s.io", "v1", "mutatingwebhookconfigurations"), kind: "MutatingWebhookConfiguration", namespaced: false, singular: "mutatingwebhookconfiguration", short_names: &[], subresources: &[] },
        // Inert: registered only to appear in discovery.
        Builtin { gvr: G::new("authentication.k8s.io", "v1", "tokenreviews"), kind: "TokenReview", namespaced: false, singular: "tokenreview", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("authorization.k8s.io", "v1", "selfsubjectaccessreviews"), kind: "SelfSubjectAccessReview", namespaced: false, singular: "selfsubjectaccessreview", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("authorization.k8s.io", "v1", "selfsubjectrulesreviews"), kind: "SelfSubjectRulesReview", namespaced: false, singular: "selfsubjectrulesreview", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("authorization.k8s.io", "v1", "subjectaccessreviews"), kind: "SubjectAccessReview", namespaced: false, singular: "subjectaccessreview", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("authorization.k8s.io", "v1", "localsubjectaccessreviews"), kind: "LocalSubjectAccessReview", namespaced: true, singular: "localsubjectaccessreview", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("autoscaling", "v1", "horizontalpodautoscalers"), kind: "HorizontalPodAutoscaler", namespaced: true, singular: "horizontalpodautoscaler", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("certificates.k8s.io", "v1", "certificatesigningrequests"), kind: "CertificateSigningRequest", namespaced: false, singular: "certificatesigningrequest", short_names: &["csr"], subresources: &["approval"] },
        Builtin { gvr: G::new("events.k8s.io", "v1", "events"), kind: "Event", namespaced: true, singular: "event", short_names: &["ev"], subresources: &[] },
        Builtin { gvr: G::new("node.k8s.io", "v1", "runtimeclasses"), kind: "RuntimeClass", namespaced: false, singular: "runtimeclass", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("scheduling.k8s.io", "v1", "priorityclasses"), kind: "PriorityClass", namespaced: false, singular: "priorityclass", short_names: &["pc"], subresources: &[] },
        // Stored only: extension apiservers' flow-control informers must sync before they report ready.
        Builtin { gvr: G::new("flowcontrol.apiserver.k8s.io", "v1", "flowschemas"), kind: "FlowSchema", namespaced: false, singular: "flowschema", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("flowcontrol.apiserver.k8s.io", "v1", "prioritylevelconfigurations"), kind: "PriorityLevelConfiguration", namespaced: false, singular: "prioritylevelconfiguration", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("storage.k8s.io", "v1", "csinodes"), kind: "CSINode", namespaced: false, singular: "csinode", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("storage.k8s.io", "v1", "csidrivers"), kind: "CSIDriver", namespaced: false, singular: "csidriver", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("storage.k8s.io", "v1", "csistoragecapacities"), kind: "CSIStorageCapacity", namespaced: true, singular: "csistoragecapacity", short_names: &[], subresources: &[] },
        Builtin { gvr: G::new("storage.k8s.io", "v1", "volumeattachments"), kind: "VolumeAttachment", namespaced: false, singular: "volumeattachment", short_names: &[], subresources: &[] },
        Builtin { gvr: G::persistent_volume_claims(), kind: "PersistentVolumeClaim", namespaced: true, singular: "persistentvolumeclaim", short_names: &["pvc"], subresources: &[] },
        Builtin { gvr: G::persistent_volumes(), kind: "PersistentVolume", namespaced: false, singular: "persistentvolume", short_names: &["pv"], subresources: &[] },
        Builtin { gvr: G::storage_classes(), kind: "StorageClass", namespaced: false, singular: "storageclass", short_names: &["sc"], subresources: &[] },
        Builtin { gvr: G::new("", "v1", "replicationcontrollers"), kind: "ReplicationController", namespaced: true, singular: "replicationcontroller", short_names: &["rc"], subresources: &[] },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(group: &str, version: &str, resource: &str) -> ResourceType {
        ResourceType {
            gvr: GroupVersionResource::new(group, version, resource),
            kind: "K".into(),
            namespaced: false,
            singular: String::new(),
            short_names: vec![],
            subresources: vec![],
            schema: None,
            storage_gvr: None,
        }
    }

    #[test]
    fn unregister_preserves_sibling_version_by_resource() {
        let r = ResourceRegistry::new();
        r.register(rt("autoscaling", "v1", "horizontalpodautoscalers"));
        r.register(rt("autoscaling", "v2", "horizontalpodautoscalers"));
        // `by_resource` now points at whichever registered last (v2).
        r.unregister(&GroupVersionResource::new(
            "autoscaling",
            "v2",
            "horizontalpodautoscalers",
        ));
        let got = r.get_by_resource("horizontalpodautoscalers");
        assert!(got.is_some(), "the sibling version must remain resolvable");
        assert_eq!(got.unwrap().gvr.version, "v1");
    }

    #[test]
    fn unregister_removes_resource_when_no_sibling() {
        let r = ResourceRegistry::new();
        r.register(rt("g", "v1", "widgets"));
        r.unregister(&GroupVersionResource::new("g", "v1", "widgets"));
        assert!(r.get_by_resource("widgets").is_none());
    }
}
