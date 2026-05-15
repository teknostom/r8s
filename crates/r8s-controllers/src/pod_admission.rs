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

    let token_secret = match sa
        .secrets
        .as_ref()
        .and_then(|s| s.first())
        .and_then(|s| s.name.clone())
    {
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

    let containers = match spec.get_mut("containers").and_then(|v| v.as_array_mut()) {
        Some(c) => c,
        None => return,
    };
    for container in containers {
        let obj = match container.as_object_mut() {
            Some(o) => o,
            None => continue,
        };
        let mounts = obj
            .entry("volumeMounts".to_string())
            .or_insert_with(|| json!([]));
        let already_mounted = mounts
            .as_array()
            .map(|arr| {
                arr.iter()
                    .any(|m| m.get("mountPath").and_then(|v| v.as_str()) == Some(SA_MOUNT_PATH))
            })
            .unwrap_or(false);
        if already_mounted {
            continue;
        }
        if let Some(arr) = mounts.as_array_mut() {
            arr.push(json!({
                "name": SA_VOLUME_NAME,
                "mountPath": SA_MOUNT_PATH,
                "readOnly": true,
            }));
        }
    }
}
