use std::sync::{Arc, atomic::AtomicU32};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, header},
    response::Response,
};

use rustc_hash::FxHashSet;

use crate::response::object_response;
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;

/// Media type clients send to request the aggregated discovery shape
/// (`apidiscovery.k8s.io/v2 APIGroupDiscoveryList`). When that media type
/// appears in the `Accept` header on `/api` or `/apis`, the response switches
/// from the legacy `APIGroupList`/`APIVersions` shape to the aggregated one
/// and the response Content-Type echoes the negotiated value back. Real k8s
/// also serves `v2beta1`; we accept either to be lenient with older clients.
const AGGREGATED_MEDIA_TYPE: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";

fn wants_aggregated_discovery(headers: &HeaderMap) -> Option<&'static str> {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .find_map(|part| {
            let part = part.trim();
            let has = |k| part.contains(k);
            if has("g=apidiscovery.k8s.io")
                && has("as=APIGroupDiscoveryList")
                && (has("v=v2") || has("v=v2beta1"))
            {
                Some(if has("v=v2beta1") {
                    "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList"
                } else {
                    AGGREGATED_MEDIA_TYPE
                })
            } else {
                None
            }
        })
}

fn aggregated_response(media_type: &'static str, body: serde_json::Value) -> Response {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, media_type)
        .body(Body::from(bytes))
        .expect("valid response")
}

pub type AppState = Arc<ApiState>;

pub struct ApiState {
    pub store: Store,
    pub registry: ResourceRegistry,
    pub data_dir: std::path::PathBuf,
    /// Starts at 2 because 10.96.0.1 is reserved for the kubernetes service.
    pub next_cluster_ip: AtomicU32,
}

impl ApiState {
    pub fn allocate_cluster_ip(&self) -> String {
        let n = self
            .next_cluster_ip
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

pub async fn get_api_versions(headers: HeaderMap, State(state): State<AppState>) -> Response {
    if let Some(media) = wants_aggregated_discovery(&headers) {
        return aggregated_response(media, aggregated_legacy_group(&state));
    }
    object_response(&serde_json::json!({
        "kind": "APIVersions",
        "versions": ["v1"],
        "serverAddressByClientCIDRs": [
            {"clientCIDR": "0.0.0.0/0", "serverAddress": "127.0.0.1:6443"}
        ],
    }))
}

pub async fn get_api_groups(headers: HeaderMap, State(state): State<AppState>) -> Response {
    if let Some(media) = wants_aggregated_discovery(&headers) {
        return aggregated_response(media, aggregated_non_legacy_groups(&state));
    }
    let mut seen = FxHashSet::default();
    let mut groups = Vec::new();

    // A group may register multiple versions (e.g. autoscaling/v1 + /v2).
    // Collect them all so discovery advertises every served version.
    let mut versions_by_group: rustc_hash::FxHashMap<String, Vec<String>> = Default::default();
    for rt in state.registry.iter() {
        if rt.gvr.group.is_empty() {
            continue;
        }
        versions_by_group
            .entry(rt.gvr.group.clone())
            .or_default()
            .push(rt.gvr.version.clone());
    }

    for rt in state.registry.iter() {
        if rt.gvr.group.is_empty() || !seen.insert(rt.gvr.group.clone()) {
            continue;
        }
        let mut versions: Vec<String> =
            versions_by_group.get(&rt.gvr.group).cloned().unwrap_or_default();
        versions.sort();
        versions.dedup();
        let version_objs: Vec<serde_json::Value> = versions
            .iter()
            .map(|v| {
                let gv = format!("{}/{}", rt.gvr.group, v);
                serde_json::json!({"groupVersion": gv, "version": v})
            })
            .collect();
        let preferred = versions.last().cloned().unwrap_or_else(|| rt.gvr.version.clone());
        let preferred_gv = format!("{}/{preferred}", rt.gvr.group);
        groups.push(serde_json::json!({
            "name": rt.gvr.group,
            "versions": version_objs,
            "preferredVersion": {"groupVersion": preferred_gv, "version": preferred},
        }));
    }

    object_response(&serde_json::json!({
        "kind": "APIGroupList",
        "apiVersion": "v1",
        "groups": groups,
    }))
}

pub async fn get_core_v1_resources(State(state): State<AppState>) -> Response {
    api_resource_list(&state, "", "v1")
}

/// `GET /apis/{group}` — returns the single APIGroup object for `group`,
/// listing every served version. The legacy core group is reached via
/// `GET /api`; this handler does not serve `""`.
pub async fn get_single_api_group(
    State(state): State<AppState>,
    Path(group): Path<String>,
) -> Response {
    let mut versions: Vec<String> = state
        .registry
        .iter()
        .into_iter()
        .filter(|rt| rt.gvr.group == group)
        .map(|rt| rt.gvr.version.clone())
        .collect();
    if versions.is_empty() {
        return crate::response::status_error(
            axum::http::StatusCode::NOT_FOUND,
            "NotFound",
            &format!("no API group '{group}'"),
        );
    }
    versions.sort();
    versions.dedup();
    let version_objs: Vec<serde_json::Value> = versions
        .iter()
        .map(|v| {
            let gv = format!("{group}/{v}");
            serde_json::json!({"groupVersion": gv, "version": v})
        })
        .collect();
    let preferred = versions.last().cloned().unwrap_or_default();
    let preferred_gv = format!("{group}/{preferred}");
    object_response(&serde_json::json!({
        "kind": "APIGroup",
        "apiVersion": "v1",
        "name": group,
        "versions": version_objs,
        "preferredVersion": {"groupVersion": preferred_gv, "version": preferred},
    }))
}

pub async fn get_group_version_resources(
    State(state): State<AppState>,
    Path((group, version)): Path<(String, String)>,
) -> Response {
    api_resource_list(&state, &group, &version)
}

/// Aggregated-discovery body for `/api` (legacy "" group only).
fn aggregated_legacy_group(state: &ApiState) -> serde_json::Value {
    let items: Vec<serde_json::Value> = aggregated_items_for_group(state, "");
    serde_json::json!({
        "apiVersion": "apidiscovery.k8s.io/v2",
        "kind": "APIGroupDiscoveryList",
        "items": items,
    })
}

/// Aggregated-discovery body for `/apis` (every non-empty group).
fn aggregated_non_legacy_groups(state: &ApiState) -> serde_json::Value {
    let mut seen = FxHashSet::default();
    let mut groups: Vec<String> = Vec::new();
    for rt in state.registry.iter() {
        if rt.gvr.group.is_empty() {
            continue;
        }
        if seen.insert(rt.gvr.group.clone()) {
            groups.push(rt.gvr.group.clone());
        }
    }
    groups.sort();
    let items: Vec<serde_json::Value> = groups
        .iter()
        .flat_map(|g| aggregated_items_for_group(state, g))
        .collect();
    serde_json::json!({
        "apiVersion": "apidiscovery.k8s.io/v2",
        "kind": "APIGroupDiscoveryList",
        "items": items,
    })
}

/// Build the `items` entry for a single group, collapsing all served versions
/// and listing each resource with the metadata clients need to route to it.
fn aggregated_items_for_group(state: &ApiState, group: &str) -> Vec<serde_json::Value> {
    let mut versions_map: rustc_hash::FxHashMap<String, Vec<serde_json::Value>> =
        Default::default();
    for rt in state.registry.iter() {
        if rt.gvr.group != group {
            continue;
        }
        let resource = serde_json::json!({
            "resource": rt.gvr.resource,
            "responseKind": {
                "group": "",
                "version": "",
                "kind": rt.kind,
            },
            "scope": if rt.namespaced { "Namespaced" } else { "Cluster" },
            "singularResource": rt.singular,
            "shortNames": rt.short_names,
            "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"],
        });
        versions_map
            .entry(rt.gvr.version.clone())
            .or_default()
            .push(resource);
    }
    let mut versions: Vec<(String, Vec<serde_json::Value>)> = versions_map.into_iter().collect();
    versions.sort_by(|a, b| a.0.cmp(&b.0));
    let versions_json: Vec<serde_json::Value> = versions
        .into_iter()
        .map(|(v, resources)| {
            serde_json::json!({
                "version": v,
                "resources": resources,
                "freshness": "Current",
            })
        })
        .collect();

    if versions_json.is_empty() {
        return Vec::new();
    }
    vec![serde_json::json!({
        "metadata": {"name": group},
        "versions": versions_json,
    })]
}

fn api_resource_list(state: &ApiState, group: &str, version: &str) -> Response {
    let group_version = if group.is_empty() {
        version.to_string()
    } else {
        format!("{group}/{version}")
    };

    let mut resources: Vec<serde_json::Value> = Vec::new();
    for rt in state.registry.resources_for_group_version(group, version) {
        resources.push(serde_json::json!({
            "name": rt.gvr.resource,
            "singularName": rt.singular,
            "namespaced": rt.namespaced,
            "kind": rt.kind,
            "verbs": ["create","delete","get","list","patch","update","watch"],
            "shortNames": rt.short_names,
        }));
        for sub in &rt.subresources {
            // The `scale` subresource is special: it's defined as a Scale kind
            // in the autoscaling/v1 group, but advertised under the parent's
            // group/version so kubectl can route to it.
            if sub == "scale" {
                resources.push(serde_json::json!({
                    "name": format!("{}/scale", rt.gvr.resource),
                    "singularName": "",
                    "namespaced": rt.namespaced,
                    "group": "autoscaling",
                    "version": "v1",
                    "kind": "Scale",
                    "verbs": ["get", "patch", "update"],
                }));
            }
        }
    }

    object_response(&serde_json::json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": group_version,
        "resources": resources,
    }))
}
