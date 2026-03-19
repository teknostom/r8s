use std::sync::{Arc, atomic::AtomicU32};

use axum::{
    extract::{Path, State},
    response::Response,
};

use rustc_hash::FxHashSet;

use crate::response::object_response;
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;

pub type AppState = Arc<ApiState>;

pub struct ApiState {
    pub store: Store,
    pub registry: ResourceRegistry,
    pub data_dir: std::path::PathBuf,
    /// Counter for allocating ClusterIPs in the 10.96.0.0/16 range.
    /// Starts at 2 (10.96.0.1 is reserved for the kubernetes service).
    pub next_cluster_ip: AtomicU32,
}

impl ApiState {
    pub fn allocate_cluster_ip(&self) -> String {
        let n = self.next_cluster_ip.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let b3 = (n >> 8) as u8;
        let b4 = n as u8;
        format!("10.96.{b3}.{b4}")
    }
}

pub async fn get_version() -> Response {
    object_response(&serde_json::json!({
        "major": "1",
        "minor": "32",
        "gitVersion": "v1.32.0-r8s",
        "platform": format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
    }))
}

pub async fn get_api_versions() -> Response {
    object_response(&serde_json::json!({
        "kind": "APIVersions",
        "versions": ["v1"],
        "serverAddressByClientCIDRs": [
            {"clientCIDR": "0.0.0.0/0", "serverAddress": "127.0.0.1:6443"}
        ],
    }))
}

pub async fn get_api_groups(State(state): State<AppState>) -> Response {
    let mut seen = FxHashSet::default();
    let mut groups = Vec::new();

    for rt in state.registry.iter() {
        if rt.gvr.group.is_empty() || !seen.insert(rt.gvr.group.clone()) {
            continue;
        }
        let gv = format!("{}/{}", rt.gvr.group, rt.gvr.version);
        groups.push(serde_json::json!({
            "name": rt.gvr.group,
            "versions": [{"groupVersion": gv, "version": rt.gvr.version}],
            "preferredVersion": {"groupVersion": gv, "version": rt.gvr.version},
        }));
    }

    // Always include authorization.k8s.io for RBAC checks (k9s, etc.)
    groups.push(serde_json::json!({
        "name": "authorization.k8s.io",
        "versions": [{"groupVersion": "authorization.k8s.io/v1", "version": "v1"}],
        "preferredVersion": {"groupVersion": "authorization.k8s.io/v1", "version": "v1"},
    }));

    groups.push(serde_json::json!({
        "name": "apiextensions.k8s.io",
        "versions": [{"groupVersion": "apiextensions.k8s.io/v1", "version": "v1"}],
        "preferredVersion": {"groupVersion": "apiextensions.k8s.io/v1", "version": "v1"},
    }));

    object_response(&serde_json::json!({
        "kind": "APIGroupList",
        "apiVersion": "v1",
        "groups": groups,
    }))
}

pub async fn get_core_v1_resources(State(state): State<AppState>) -> Response {
    api_resource_list(&state, "", "v1")
}

pub async fn get_group_version_resources(
    State(state): State<AppState>,
    Path((group, version)): Path<(String, String)>,
) -> Response {
    api_resource_list(&state, &group, &version)
}

fn api_resource_list(state: &ApiState, group: &str, version: &str) -> Response {
    // Handle authorization.k8s.io specially — not stored in registry
    if group == "authorization.k8s.io" && version == "v1" {
        return object_response(&serde_json::json!({
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": "authorization.k8s.io/v1",
            "resources": [
                {
                    "name": "selfsubjectaccessreviews",
                    "singularName": "",
                    "namespaced": false,
                    "kind": "SelfSubjectAccessReview",
                    "verbs": ["create"],
                },
                {
                    "name": "selfsubjectrulesreviews",
                    "singularName": "",
                    "namespaced": false,
                    "kind": "SelfSubjectRulesReview",
                    "verbs": ["create"],
                },
            ],
        }));
    }

    let group_version = if group.is_empty() {
        version.to_string()
    } else {
        format!("{group}/{version}")
    };

    let resources: Vec<serde_json::Value> = state
        .registry
        .resources_for_group_version(group, version)
        .into_iter()
        .map(|rt| {
            serde_json::json!({
                "name": rt.gvr.resource,
                "singularName": rt.singular,
                "namespaced": rt.namespaced,
                "kind": rt.kind,
                "verbs": ["create","delete","get","list","patch","update","watch"],
                "shortNames": rt.short_names,
            })
        })
        .collect();

    object_response(&serde_json::json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": group_version,
        "resources": resources,
    }))
}
