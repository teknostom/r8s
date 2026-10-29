//! Pod creation-time mutations:
//! - Inject the ServiceAccount token volume so containers see
//!   `/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`.
//! - Stamp the initial status: phase Pending and the QoS class, which every
//!   stored pod carries from creation on.

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{GroupVersionResource, ServiceAccount};
use serde_json::{Value, json};

const SA_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const SA_VOLUME_NAME: &str = "kube-api-access";

/// What a controller does to a pod before storing it, as the apiserver's
/// admission does for API-created pods: the token volume and the initial
/// status.
pub fn admit_pod(store: &Store, pod: &mut Value) {
    inject_sa_token(store, pod);
    set_initial_status(pod);
}

/// `status.qosClass` from the containers' requests and limits, and
/// `status.phase` Pending when unset.
pub fn set_initial_status(pod: &mut Value) {
    let containers: Vec<Value> = ["containers", "initContainers"]
        .iter()
        .flat_map(|list| {
            pod.get("spec")
                .and_then(|s| s.get(*list))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    let quantity = |c: &Value, kind: &str, res: &str| -> Option<Value> {
        c.get("resources")?
            .get(kind)?
            .get(res)
            .filter(|q| q.as_str().is_some_and(|q| !q.is_empty()) || q.is_number())
            .cloned()
    };
    let any = containers.iter().any(|c| {
        ["requests", "limits"].iter().any(|k| {
            ["cpu", "memory"]
                .iter()
                .any(|r| quantity(c, k, r).is_some())
        })
    });
    let guaranteed = !containers.is_empty()
        && containers.iter().all(|c| {
            ["cpu", "memory"]
                .iter()
                .all(|r| match quantity(c, "limits", r) {
                    Some(limit) => quantity(c, "requests", r).is_none_or(|req| req == limit),
                    None => false,
                })
        });
    let class = if guaranteed {
        "Guaranteed"
    } else if any {
        "Burstable"
    } else {
        "BestEffort"
    };
    let Some(obj) = pod.as_object_mut() else {
        return;
    };
    let status = obj.entry("status").or_insert_with(|| json!({}));
    if !status.is_object() {
        *status = json!({});
    }
    if let Some(status) = status.as_object_mut() {
        status.insert("qosClass".into(), json!(class));
        if status.get("phase").is_none_or(|p| p.is_null()) {
            status.insert("phase".into(), json!("Pending"));
        }
    }
}

/// Mutates a Pod JSON value in place to project the ServiceAccount token.
///
/// No-op if any of: `automountServiceAccountToken` is false on the pod or SA;
/// the SA has no token Secret yet; a volume with the conventional name
/// already exists; a container already mounts the SA path.
pub fn inject_sa_token(store: &Store, pod: &mut Value) {
    let spec = match pod.get_mut("spec").and_then(|v| v.as_object_mut()) {
        Some(s) => s,
        None => return,
    };

    // The pod's own setting wins; the ServiceAccount's applies when the pod
    // says nothing.
    let pod_automount = spec
        .get("automountServiceAccountToken")
        .and_then(|v| v.as_bool());
    if pod_automount == Some(false) {
        return;
    }

    if let Some(vols) = spec.get("volumes").and_then(|v| v.as_array())
        && vols
            .iter()
            .any(|v| v.get("name").and_then(|n| n.as_str()) == Some(SA_VOLUME_NAME))
    {
        return;
    }

    let sa_name = spec
        .get("serviceAccountName")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let namespace = pod
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    let sa_gvr = GroupVersionResource::service_accounts();
    let sa_rref = ResourceRef {
        gvr: &sa_gvr,
        namespace: Some(&namespace),
        name: &sa_name,
    };
    let sa: ServiceAccount = match store.get(&sa_rref) {
        Ok(Some(v)) => match serde_json::from_value(v) {
            Ok(s) => s,
            Err(_) => return,
        },
        _ => return,
    };

    if pod_automount.is_none() && sa.automount_service_account_token == Some(false) {
        return;
    }

    let spec = match pod.get_mut("spec").and_then(|v| v.as_object_mut()) {
        Some(s) => s,
        None => return,
    };

    let volumes = spec
        .entry("volumes".to_string())
        .or_insert_with(|| json!([]));
    if volumes.is_null() {
        *volumes = json!([]);
    }
    if let Some(arr) = volumes.as_array_mut() {
        arr.push(json!({
            "name": SA_VOLUME_NAME,
            "projected": {
                "defaultMode": 420,
                "sources": [
                    {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                    {"configMap": {
                        "name": "kube-root-ca.crt",
                        "items": [{"key": "ca.crt", "path": "ca.crt"}],
                    }},
                    {"downwardAPI": {"items": [
                        {"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}
                    ]}},
                ],
            },
        }));
    }

    // Both regular and init containers need the token — an init container that
    // talks to the API server (e.g. waits for a dependency) reads the same files.
    for list in ["initContainers", "containers"] {
        if let Some(containers) = spec.get_mut(list).and_then(|v| v.as_array_mut()) {
            for container in containers {
                mount_sa_token(container);
            }
        }
    }
}

/// Add the SA-token volume mount to one container, unless the path is already
/// mounted.
fn mount_sa_token(container: &mut Value) {
    let Some(obj) = container.as_object_mut() else {
        return;
    };
    let mounts = obj
        .entry("volumeMounts".to_string())
        .or_insert_with(|| json!([]));
    let already_mounted = mounts.as_array().is_some_and(|arr| {
        arr.iter()
            .any(|m| m.get("mountPath").and_then(|v| v.as_str()) == Some(SA_MOUNT_PATH))
    });
    if already_mounted {
        return;
    }
    if let Some(arr) = mounts.as_array_mut() {
        arr.push(json!({
            "name": SA_VOLUME_NAME,
            "mountPath": SA_MOUNT_PATH,
            "readOnly": true,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn put(store: &Store, gvr: &GroupVersionResource, ns: &str, name: &str, obj: Value) {
        store
            .create(
                ResourceRef {
                    gvr,
                    namespace: Some(ns),
                    name,
                },
                &obj,
            )
            .unwrap();
    }

    fn mount_paths(container: &Value) -> Vec<String> {
        container
            .get("volumeMounts")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|m| {
                        m.get("mountPath")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn initial_status_has_phase_and_qos_class() {
        let mut best_effort = json!({"spec": {"containers": [{"name": "a"}]}});
        set_initial_status(&mut best_effort);
        assert_eq!(best_effort["status"]["phase"], "Pending");
        assert_eq!(best_effort["status"]["qosClass"], "BestEffort");

        let mut guaranteed = json!({"spec": {"containers": [{"name": "a", "resources": {
            "limits": {"cpu": "1", "memory": "1Gi"}}}]}, "status": {"phase": "Running"}});
        set_initial_status(&mut guaranteed);
        assert_eq!(guaranteed["status"]["qosClass"], "Guaranteed");
        assert_eq!(guaranteed["status"]["phase"], "Running");

        let mut burstable = json!({"spec": {"containers": [{"name": "a", "resources": {
            "requests": {"cpu": "100m"}}}]}, "status": null});
        set_initial_status(&mut burstable);
        assert_eq!(burstable["status"]["qosClass"], "Burstable");
    }

    #[test]
    fn injects_token_typed_secret_into_all_containers() {
        let (_d, store) = store();
        let secrets = GroupVersionResource::secrets();
        // A non-token secret listed first, then the real token secret.
        put(
            &store,
            &secrets,
            "default",
            "docker-cfg",
            json!({"apiVersion":"v1","kind":"Secret","type":"kubernetes.io/dockerconfigjson",
                   "metadata":{"name":"docker-cfg","namespace":"default"}}),
        );
        put(
            &store,
            &secrets,
            "default",
            "default-token",
            json!({"apiVersion":"v1","kind":"Secret","type":"kubernetes.io/service-account-token",
                   "metadata":{"name":"default-token","namespace":"default"}}),
        );
        put(
            &store,
            &GroupVersionResource::service_accounts(),
            "default",
            "default",
            json!({"apiVersion":"v1","kind":"ServiceAccount",
                   "metadata":{"name":"default","namespace":"default"},
                   "secrets":[{"name":"docker-cfg"},{"name":"default-token"}]}),
        );

        let mut pod = json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{
                "initContainers":[{"name":"init","image":"busybox"}],
                "containers":[{"name":"main","image":"nginx"}]
            }
        });
        inject_sa_token(&store, &mut pod);

        let vol = &pod["spec"]["volumes"][0];
        assert_eq!(
            vol["projected"]["sources"][0]["serviceAccountToken"]["path"],
            "token"
        );
        // Both the init and main containers get the mount.
        assert!(
            mount_paths(&pod["spec"]["initContainers"][0]).contains(&SA_MOUNT_PATH.to_string())
        );
        assert!(mount_paths(&pod["spec"]["containers"][0]).contains(&SA_MOUNT_PATH.to_string()));
    }

    #[test]
    fn pod_automount_setting_overrides_the_service_accounts() {
        let (_d, store) = store();
        put(
            &store,
            &GroupVersionResource::secrets(),
            "default",
            "nomount-token",
            json!({"apiVersion":"v1","kind":"Secret","type":"kubernetes.io/service-account-token",
                   "metadata":{"name":"nomount-token","namespace":"default"}}),
        );
        put(
            &store,
            &GroupVersionResource::service_accounts(),
            "default",
            "nomount",
            json!({"apiVersion":"v1","kind":"ServiceAccount",
                   "metadata":{"name":"nomount","namespace":"default"},
                   "automountServiceAccountToken": false,
                   "secrets":[{"name":"nomount-token"}]}),
        );
        let pod = |automount: Option<bool>| {
            let mut spec = json!({"serviceAccountName": "nomount",
                                  "containers":[{"name":"main","image":"nginx"}]});
            if let Some(a) = automount {
                spec["automountServiceAccountToken"] = json!(a);
            }
            json!({"apiVersion":"v1","kind":"Pod",
                   "metadata":{"name":"p","namespace":"default"}, "spec": spec})
        };

        let mut unset = pod(None);
        inject_sa_token(&store, &mut unset);
        assert!(unset["spec"].get("volumes").is_none(), "SA opts out");

        let mut forced = pod(Some(true));
        inject_sa_token(&store, &mut forced);
        assert_eq!(forced["spec"]["volumes"][0]["name"], SA_VOLUME_NAME);

        let mut refused = pod(Some(false));
        inject_sa_token(&store, &mut refused);
        assert!(refused["spec"].get("volumes").is_none(), "pod opts out");
    }

    #[test]
    fn null_volumes_list_still_gets_the_token_volume() {
        let (_d, store) = store();
        put(
            &store,
            &GroupVersionResource::secrets(),
            "default",
            "default-token",
            json!({"apiVersion":"v1","kind":"Secret","type":"kubernetes.io/service-account-token",
                   "metadata":{"name":"default-token","namespace":"default"}}),
        );
        put(
            &store,
            &GroupVersionResource::service_accounts(),
            "default",
            "default",
            json!({"apiVersion":"v1","kind":"ServiceAccount",
                   "metadata":{"name":"default","namespace":"default"},
                   "secrets":[{"name":"default-token"}]}),
        );
        let mut pod = json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{"volumes": null, "containers":[{"name":"main","image":"nginx"}]}
        });
        inject_sa_token(&store, &mut pod);
        assert_eq!(pod["spec"]["volumes"][0]["name"], SA_VOLUME_NAME);
    }

    #[test]
    fn projects_a_bound_token_without_any_token_secret() {
        let (_d, store) = store();
        put(
            &store,
            &GroupVersionResource::service_accounts(),
            "default",
            "default",
            json!({"apiVersion":"v1","kind":"ServiceAccount",
                   "metadata":{"name":"default","namespace":"default"}}),
        );

        let mut pod = json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{"containers":[{"name":"main","image":"nginx"}]}
        });
        inject_sa_token(&store, &mut pod);

        let sources = &pod["spec"]["volumes"][0]["projected"]["sources"];
        assert_eq!(sources[0]["serviceAccountToken"]["path"], "token");
        assert_eq!(sources[1]["configMap"]["name"], "kube-root-ca.crt");
        assert_eq!(sources[2]["downwardAPI"]["items"][0]["path"], "namespace");
    }
}
