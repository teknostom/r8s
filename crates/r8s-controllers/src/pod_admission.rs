//! Pod creation-time mutations. Currently:
//! - Inject the ServiceAccount token volume so containers see
//!   `/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`.

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{GroupVersionResource, ServiceAccount};
use serde_json::{Value, json};

const SA_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const SA_VOLUME_NAME: &str = "kube-api-access";

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

    if spec
        .get("automountServiceAccountToken")
        .and_then(|v| v.as_bool())
        == Some(false)
    {
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

    if sa.automount_service_account_token == Some(false) {
        return;
    }

    let token_secret = match resolve_token_secret(store, &namespace, &sa_name, &sa) {
        Some(n) => n,
        None => return,
    };

    let spec = match pod.get_mut("spec").and_then(|v| v.as_object_mut()) {
        Some(s) => s,
        None => return,
    };

    let volumes = spec
        .entry("volumes".to_string())
        .or_insert_with(|| json!([]));
    if let Some(arr) = volumes.as_array_mut() {
        arr.push(json!({
            "name": SA_VOLUME_NAME,
            "secret": {
                "secretName": token_secret,
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

/// The name of the SA's token Secret. Prefers a `service-account-token`-typed
/// Secret the SA references; falls back to the conventional `<sa>-token` name so
/// admission still works in the window after the Secret is minted but before the
/// serviceaccount controller has linked it onto the SA.
fn resolve_token_secret(
    store: &Store,
    namespace: &str,
    sa_name: &str,
    sa: &ServiceAccount,
) -> Option<String> {
    let secret_gvr = GroupVersionResource::secrets();
    let is_token = |name: &str| {
        matches!(
            store.get(&ResourceRef {
                gvr: &secret_gvr,
                namespace: Some(namespace),
                name,
            }),
            Ok(Some(v)) if v.get("type").and_then(|t| t.as_str()) == Some(crate::SA_TOKEN_SECRET_TYPE)
        )
    };

    if let Some(secrets) = sa.secrets.as_ref() {
        for s in secrets {
            if let Some(name) = s.name.as_deref()
                && is_token(name)
            {
                return Some(name.to_string());
            }
        }
    }

    let conventional = format!("{sa_name}-token");
    is_token(&conventional).then_some(conventional)
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
                    .filter_map(|m| m.get("mountPath").and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default()
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

        // The volume must reference the token-typed secret, not the first one.
        let vol = &pod["spec"]["volumes"][0];
        assert_eq!(vol["secret"]["secretName"], "default-token");
        // Both the init and main containers get the mount.
        assert!(mount_paths(&pod["spec"]["initContainers"][0]).contains(&SA_MOUNT_PATH.to_string()));
        assert!(mount_paths(&pod["spec"]["containers"][0]).contains(&SA_MOUNT_PATH.to_string()));
    }

    #[test]
    fn falls_back_to_conventional_name_when_sa_unlinked() {
        let (_d, store) = store();
        // Token secret exists but the SA doesn't reference it yet.
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
                   "metadata":{"name":"default","namespace":"default"}}),
        );

        let mut pod = json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"default"},
            "spec":{"containers":[{"name":"main","image":"nginx"}]}
        });
        inject_sa_token(&store, &mut pod);

        assert_eq!(pod["spec"]["volumes"][0]["secret"]["secretName"], "default-token");
    }
}
