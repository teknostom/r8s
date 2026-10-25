//! Ensures every ServiceAccount has a paired token Secret so pods that
//! mount it find `/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`.
//! r8s does not enforce auth, so the token bytes are placeholders — what
//! matters is that the files exist with the right names.

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use r8s_store::{Store, StoreError, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ServiceAccount};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{SA_TOKEN_SECRET_TYPE, random_suffix};

const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";
const SA_UID_ANNOTATION: &str = "kubernetes.io/service-account.uid";

/// Conventional token-Secret name for a ServiceAccount. Deterministic so a
/// re-reconcile finds the existing Secret instead of minting a duplicate.
fn token_secret_name(sa_name: &str) -> String {
    format!("{sa_name}-token")
}

pub async fn run(store: Store, shutdown: CancellationToken, ca_pem: String) -> anyhow::Result<()> {
    tracing::info!("serviceaccount controller started");
    let sa_gvr = GroupVersionResource::service_accounts();

    reconcile_all(&store, &sa_gvr, &ca_pem);

    let mut rx = store.watch(&sa_gvr);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("serviceaccount controller shutting down");
                return Ok(());
            }
            event = rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = ensure_token_secret(&store, &event.object, &ca_pem) {
                            tracing::warn!("sa token secret error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("sa controller lagged {n} events, re-syncing");
                        reconcile_all(&store, &sa_gvr, &ca_pem);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn reconcile_all(store: &Store, sa_gvr: &GroupVersionResource, ca_pem: &str) {
    let result = match store.list(sa_gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("sa controller list error: {e}");
            return;
        }
    };
    for sa in &result.items {
        if let Err(e) = ensure_token_secret(store, sa, ca_pem) {
            tracing::warn!("sa token secret error: {e}");
        }
    }
}

fn ensure_token_secret(
    store: &Store,
    sa_value: &serde_json::Value,
    ca_pem: &str,
) -> anyhow::Result<()> {
    let sa: ServiceAccount = serde_json::from_value(sa_value.clone())?;
    let name = sa
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("SA has no name"))?;
    let namespace = sa
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("SA '{name}' has no namespace"))?;
    let uid = sa.metadata.uid.as_deref().unwrap_or("");

    // If the SA already references a token secret that exists, nothing to do.
    let secret_gvr = GroupVersionResource::secrets();
    if let Some(secrets) = sa.secrets.as_ref() {
        for s in secrets {
            let s_name = match s.name.as_deref() {
                Some(n) => n,
                None => continue,
            };
            let rref = ResourceRef {
                gvr: &secret_gvr,
                namespace: Some(namespace),
                name: s_name,
            };
            if matches!(store.get(&rref), Ok(Some(_))) {
                return Ok(());
            }
        }
    }

    let secret_name = token_secret_name(name);
    let secret_rref = ResourceRef {
        gvr: &secret_gvr,
        namespace: Some(namespace),
        name: &secret_name,
    };

    // Create the token Secret if it's not already there. The deterministic name
    // makes this idempotent: a re-reconcile finds it (or races to AlreadyExists)
    // rather than minting a second Secret every time the link update below loses
    // an optimistic-concurrency race.
    if store.get(&secret_rref)?.is_none() {
        // Token bytes are placeholders — r8s does not validate them server-side.
        // Real k8s would mint a JWT signed by the apiserver; here it just needs
        // to exist so containers don't crash reading the file.
        let token = format!("r8s-placeholder-token-{}", random_suffix());
        let mut data = BTreeMap::new();
        data.insert(
            "token".to_string(),
            serde_json::json!(B64.encode(token.as_bytes())),
        );
        data.insert(
            "namespace".to_string(),
            serde_json::json!(B64.encode(namespace.as_bytes())),
        );
        data.insert(
            "ca.crt".to_string(),
            serde_json::json!(B64.encode(ca_pem.as_bytes())),
        );

        let mut annotations = BTreeMap::new();
        annotations.insert(SA_NAME_ANNOTATION.to_string(), name.to_string());
        if !uid.is_empty() {
            annotations.insert(SA_UID_ANNOTATION.to_string(), uid.to_string());
        }

        let secret_body = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "type": SA_TOKEN_SECRET_TYPE,
            "metadata": {
                "name": secret_name,
                "namespace": namespace,
                "annotations": annotations,
            },
            "data": data,
        });
        match store.create(secret_rref, &secret_body) {
            Ok(_) => {
                tracing::info!("created token Secret '{secret_name}' for SA '{namespace}/{name}'")
            }
            Err(StoreError::AlreadyExists { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }

    link_secret(store, namespace, name, &secret_name)
}

/// Reference the token Secret from the SA's `secrets` list, retrying on
/// optimistic-concurrency conflicts. Giving up here (as the old code did on the
/// first conflict) left the SA unlinked, so every later reconcile minted a fresh
/// Secret.
fn link_secret(
    store: &Store,
    namespace: &str,
    name: &str,
    secret_name: &str,
) -> anyhow::Result<()> {
    let sa_gvr = GroupVersionResource::service_accounts();
    let sa_rref = ResourceRef {
        gvr: &sa_gvr,
        namespace: Some(namespace),
        name,
    };
    for _ in 0..5 {
        let Some(mut current) = store.get(&sa_rref)? else {
            return Ok(());
        };
        let already_linked = current
            .get("secrets")
            .and_then(|v| v.as_array())
            .is_some_and(|arr| {
                arr.iter()
                    .any(|s| s.get("name").and_then(|n| n.as_str()) == Some(secret_name))
            });
        if already_linked {
            return Ok(());
        }
        if let Some(obj) = current.as_object_mut() {
            let secrets = obj
                .entry("secrets".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if let Some(arr) = secrets.as_array_mut() {
                arr.push(serde_json::json!({"name": secret_name}));
            }
        }
        match store.update(&sa_rref, &current) {
            Ok(_) => return Ok(()),
            Err(StoreError::Conflict { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    tracing::warn!("sa '{namespace}/{name}': gave up linking token secret after retries");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn create_sa(store: &Store, ns: &str, name: &str) -> serde_json::Value {
        store
            .create(
                ResourceRef {
                    gvr: &GroupVersionResource::service_accounts(),
                    namespace: Some(ns),
                    name,
                },
                &json!({
                    "apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": name, "namespace": ns}
                }),
            )
            .unwrap()
    }

    fn token_secret_count(store: &Store, ns: &str) -> usize {
        store
            .list(&GroupVersionResource::secrets(), Some(ns), None, None, None, None)
            .unwrap()
            .items
            .iter()
            .filter(|s| s.get("type").and_then(|t| t.as_str()) == Some(SA_TOKEN_SECRET_TYPE))
            .count()
    }

    #[test]
    fn token_secret_creation_is_idempotent() {
        let (_d, store) = store();
        let sa = create_sa(&store, "default", "sa");

        // Reconcile twice: deterministic naming must yield exactly one Secret.
        ensure_token_secret(&store, &sa, "ca-pem").unwrap();
        ensure_token_secret(&store, &sa, "ca-pem").unwrap();

        assert_eq!(token_secret_count(&store, "default"), 1);
        assert!(
            store
                .get(&ResourceRef {
                    gvr: &GroupVersionResource::secrets(),
                    namespace: Some("default"),
                    name: "sa-token",
                })
                .unwrap()
                .is_some(),
            "secret uses the deterministic <sa>-token name"
        );
    }

    #[test]
    fn secret_is_linked_back_on_the_sa() {
        let (_d, store) = store();
        let sa = create_sa(&store, "default", "sa");
        ensure_token_secret(&store, &sa, "ca-pem").unwrap();

        let stored = store
            .get(&ResourceRef {
                gvr: &GroupVersionResource::service_accounts(),
                namespace: Some("default"),
                name: "sa",
            })
            .unwrap()
            .unwrap();
        let linked = stored["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "sa-token");
        assert!(linked, "SA must reference its token secret");
    }
}
