use chrono::Utc;
use serde_json::{Value, json};

use crate::response::object_response;
use axum::response::Response;

pub struct ColumnDef {
    pub name: &'static str,
    pub col_type: &'static str,
    pub format: &'static str,
    pub description: &'static str,
    pub priority: i32,
}

fn col(
    name: &'static str,
    col_type: &'static str,
    format: &'static str,
    description: &'static str,
    priority: i32,
) -> ColumnDef {
    ColumnDef {
        name,
        col_type,
        format,
        description,
        priority,
    }
}

pub fn columns_for(resource: &str) -> Vec<ColumnDef> {
    match resource {
        "pods" => vec![
            col("Name", "string", "name", "Name of the pod", 0),
            col("Ready", "string", "", "Ready containers", 0),
            col("Status", "string", "", "Pod phase", 0),
            col("Restarts", "integer", "", "Restart count", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "deployments" => vec![
            col("Name", "string", "name", "Name of the deployment", 0),
            col("Ready", "string", "", "Ready replicas", 0),
            col("Up-to-date", "integer", "", "Updated replicas", 0),
            col("Available", "integer", "", "Available replicas", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "replicasets" => vec![
            col("Name", "string", "name", "Name of the replica set", 0),
            col("Desired", "integer", "", "Desired replicas", 0),
            col("Current", "integer", "", "Current replicas", 0),
            col("Ready", "integer", "", "Ready replicas", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "services" => vec![
            col("Name", "string", "name", "Name of the service", 0),
            col("Type", "string", "", "Service type", 0),
            col("Cluster-IP", "string", "", "Cluster IP", 0),
            col("External-IP", "string", "", "External IP", 0),
            col("Port(s)", "string", "", "Service ports", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "namespaces" => vec![
            col("Name", "string", "name", "Name of the namespace", 0),
            col("Status", "string", "", "Namespace phase", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "nodes" => vec![
            col("Name", "string", "name", "Name of the node", 0),
            col("Status", "string", "", "Node status", 0),
            col("Roles", "string", "", "Node roles", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
            col("Version", "string", "", "Kubelet version", 0),
        ],
        "configmaps" => vec![
            col("Name", "string", "name", "Name of the configmap", 0),
            col("Data", "integer", "", "Number of data keys", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "secrets" => vec![
            col("Name", "string", "name", "Name of the secret", 0),
            col("Type", "string", "", "Secret type", 0),
            col("Data", "integer", "", "Number of data keys", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "endpoints" => vec![
            col("Name", "string", "name", "Name of the endpoints", 0),
            col("Endpoints", "string", "", "Endpoint addresses", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "events" => vec![
            col("Last Seen", "string", "", "Last seen time", 0),
            col("Type", "string", "", "Event type", 0),
            col("Reason", "string", "", "Event reason", 0),
            col("Object", "string", "", "Involved object", 0),
            col("Message", "string", "", "Event message", 0),
        ],
        "statefulsets" => vec![
            col("Name", "string", "name", "Name of the statefulset", 0),
            col("Ready", "string", "", "Ready replicas", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "daemonsets" => vec![
            col("Name", "string", "name", "Name of the daemonset", 0),
            col("Desired", "integer", "", "Desired pods", 0),
            col("Current", "integer", "", "Current pods", 0),
            col("Ready", "integer", "", "Ready pods", 0),
            col("Up-to-date", "integer", "", "Updated pods", 0),
            col("Available", "integer", "", "Available pods", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        "serviceaccounts" => vec![
            col("Name", "string", "name", "Name of the service account", 0),
            col("Secrets", "integer", "", "Number of secrets", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
        _ => vec![
            col("Name", "string", "name", "Name", 0),
            col("Age", "string", "", "CreationTimestamp", 0),
        ],
    }
}

fn val<'a>(obj: &'a Value, keys: &[&str]) -> &'a Value {
    let mut current = obj;
    for &key in keys {
        match current.get(key) {
            Some(v) => current = v,
            None => return &Value::Null,
        }
    }
    current
}

fn str_val<'a>(obj: &'a Value, keys: &[&str]) -> Option<&'a str> {
    val(obj, keys).as_str()
}

pub fn extract_cells(resource: &str, obj: &Value) -> Vec<Value> {
    match resource {
        "pods" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let statuses = val(obj, &["status", "containerStatuses"]).as_array();
            let total = val(obj, &["spec", "containers"])
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0);
            let ready = statuses
                .map(|s| {
                    s.iter()
                        .filter(|c| c.get("ready").and_then(|v| v.as_bool()) == Some(true))
                        .count()
                })
                .unwrap_or(0);
            let phase = str_val(obj, &["status", "phase"]).unwrap_or("Pending");
            let restarts: u64 = statuses
                .map(|s| {
                    s.iter()
                        .map(|c| c.get("restartCount").and_then(|v| v.as_u64()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(format!("{ready}/{total}")),
                json!(phase),
                json!(restarts),
                json!(age),
            ]
        }
        "deployments" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let desired = val(obj, &["spec", "replicas"]).as_u64().unwrap_or(0);
            let ready = val(obj, &["status", "readyReplicas"]).as_u64().unwrap_or(0);
            let updated = val(obj, &["status", "updatedReplicas"])
                .as_u64()
                .unwrap_or(0);
            let available = val(obj, &["status", "availableReplicas"])
                .as_u64()
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(format!("{ready}/{desired}")),
                json!(updated),
                json!(available),
                json!(age),
            ]
        }
        "replicasets" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let desired = val(obj, &["spec", "replicas"]).as_u64().unwrap_or(0);
            let current = val(obj, &["status", "replicas"]).as_u64().unwrap_or(0);
            let ready = val(obj, &["status", "readyReplicas"]).as_u64().unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(desired),
                json!(current),
                json!(ready),
                json!(age),
            ]
        }
        "services" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let svc_type = str_val(obj, &["spec", "type"]).unwrap_or("ClusterIP");
            let cluster_ip = str_val(obj, &["spec", "clusterIP"]).unwrap_or("<none>");
            let external_ip = str_val(obj, &["spec", "externalIP"]).unwrap_or("<none>");
            let ports = val(obj, &["spec", "ports"])
                .as_array()
                .map(|ps| {
                    ps.iter()
                        .map(|p| {
                            let port = p.get("port").and_then(|v| v.as_u64()).unwrap_or(0);
                            let proto = p.get("protocol").and_then(|v| v.as_str()).unwrap_or("TCP");
                            format!("{port}/{proto}")
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_else(|| "<none>".to_string());
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(svc_type),
                json!(cluster_ip),
                json!(external_ip),
                json!(ports),
                json!(age),
            ]
        }
        "namespaces" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let phase = str_val(obj, &["status", "phase"]).unwrap_or("Active");
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(phase), json!(age)]
        }
        "nodes" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let conditions = val(obj, &["status", "conditions"]).as_array();
            let status = conditions
                .and_then(|c| {
                    c.iter()
                        .find(|c| c.get("type").and_then(|v| v.as_str()) == Some("Ready"))
                })
                .map(|c| {
                    if c.get("status").and_then(|v| v.as_str()) == Some("True") {
                        "Ready"
                    } else {
                        "NotReady"
                    }
                })
                .unwrap_or("Unknown");
            let roles = str_val(
                obj,
                &[
                    "metadata",
                    "labels",
                    "node-role.kubernetes.io/control-plane",
                ],
            )
            .map(|_| "control-plane")
            .unwrap_or("<none>");
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            let version = str_val(obj, &["status", "nodeInfo", "kubeletVersion"]).unwrap_or("");
            vec![
                json!(name),
                json!(status),
                json!(roles),
                json!(age),
                json!(version),
            ]
        }
        "configmaps" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let data_count = obj
                .get("data")
                .and_then(|v| v.as_object())
                .map(|m| m.len())
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(data_count), json!(age)]
        }
        "secrets" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let secret_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("Opaque");
            let data_count = obj
                .get("data")
                .and_then(|v| v.as_object())
                .map(|m| m.len())
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(secret_type),
                json!(data_count),
                json!(age),
            ]
        }
        "endpoints" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let addrs = obj
                .get("subsets")
                .and_then(|v| v.as_array())
                .map(|subsets| {
                    subsets
                        .iter()
                        .flat_map(|s| {
                            s.get("addresses")
                                .and_then(|v| v.as_array())
                                .into_iter()
                                .flatten()
                        })
                        .filter_map(|a| a.get("ip").and_then(|v| v.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "<none>".to_string());
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(addrs), json!(age)]
        }
        "events" => {
            let last_seen = format_age(obj.get("lastTimestamp").and_then(|v| v.as_str()));
            let event_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("Normal");
            let reason = obj.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let kind = str_val(obj, &["involvedObject", "kind"]).unwrap_or("");
            let obj_name = str_val(obj, &["involvedObject", "name"]).unwrap_or("");
            let object = format!("{kind}/{obj_name}");
            let message = obj.get("message").and_then(|v| v.as_str()).unwrap_or("");
            vec![
                json!(last_seen),
                json!(event_type),
                json!(reason),
                json!(object),
                json!(message),
            ]
        }
        "statefulsets" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let desired = val(obj, &["spec", "replicas"]).as_u64().unwrap_or(0);
            let ready = val(obj, &["status", "readyReplicas"]).as_u64().unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(format!("{ready}/{desired}")), json!(age)]
        }
        "daemonsets" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let desired = val(obj, &["status", "desiredNumberScheduled"])
                .as_u64()
                .unwrap_or(0);
            let current = val(obj, &["status", "currentNumberScheduled"])
                .as_u64()
                .unwrap_or(0);
            let ready = val(obj, &["status", "numberReady"]).as_u64().unwrap_or(0);
            let updated = val(obj, &["status", "updatedNumberScheduled"])
                .as_u64()
                .unwrap_or(0);
            let available = val(obj, &["status", "numberAvailable"])
                .as_u64()
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![
                json!(name),
                json!(desired),
                json!(current),
                json!(ready),
                json!(updated),
                json!(available),
                json!(age),
            ]
        }
        "serviceaccounts" => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let secrets = obj
                .get("secrets")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(secrets), json!(age)]
        }
        _ => {
            let name = str_val(obj, &["metadata", "name"]).unwrap_or("");
            let age = format_age(str_val(obj, &["metadata", "creationTimestamp"]));
            vec![json!(name), json!(age)]
        }
    }
}

fn format_age(timestamp: Option<&str>) -> String {
    let ts = match timestamp.and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()) {
        Some(t) => t,
        None => return "<unknown>".to_string(),
    };
    let secs = Utc::now().signed_duration_since(ts).num_seconds();
    if secs < 0 {
        return "0s".to_string();
    }
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        return format!("{}m", secs / 60);
    }
    if secs < 86400 {
        return format!("{}h", secs / 3600);
    }
    format!("{}d", secs / 86400)
}

pub fn table_response(
    columns: &[ColumnDef],
    items: &[Value],
    resource: &str,
    resource_version: Option<u64>,
) -> Response {
    let column_defs: Vec<Value> = columns
        .iter()
        .map(|c| {
            json!({
                "name": c.name,
                "type": c.col_type,
                "format": c.format,
                "description": c.description,
                "priority": c.priority,
            })
        })
        .collect();

    let rows: Vec<Value> = items
        .iter()
        .map(|obj| {
            let cells = extract_cells(resource, obj);
            json!({
                "cells": cells,
                "object": {
                    "apiVersion": "meta.k8s.io/v1",
                    "kind": "PartialObjectMetadata",
                    "metadata": obj.get("metadata").cloned().unwrap_or(Value::Null),
                }
            })
        })
        .collect();

    let metadata = match resource_version {
        Some(rv) => json!({"resourceVersion": rv.to_string()}),
        None => json!({}),
    };

    let body = json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": metadata,
        "columnDefinitions": column_defs,
        "rows": rows,
    });

    object_response(&body)
}

pub fn single_object_table_response(
    columns: &[ColumnDef],
    obj: &Value,
    resource: &str,
) -> Response {
    table_response(columns, std::slice::from_ref(obj), resource, None)
}
