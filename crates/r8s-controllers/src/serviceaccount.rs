//! Ensures every ServiceAccount has a paired token Secret so pods that
//! mount it find `/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`.
//! r8s does not enforce auth, so the token bytes are placeholders — what
//! matters is that the files exist with the right names.

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ServiceAccount};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::random_suffix;

const TOKEN_SECRET_TYPE: &str = "kubernetes.io/service-account-token";
const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";
const SA_UID_ANNOTATION: &str = "kubernetes.io/service-account.uid";

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

    let secret_name = format!("{name}-token-{}", random_suffix());

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
        "type": TOKEN_SECRET_TYPE,
        "metadata": {
            "name": secret_name,
            "namespace": namespace,
            "annotations": annotations,
        },
        "data": data,
    });

    let secret_rref = ResourceRef {
        gvr: &secret_gvr,
        namespace: Some(namespace),
        name: &secret_name,
    };
    store.create(secret_rref, &secret_body)?;
    tracing::info!("created token Secret '{secret_name}' for SA '{namespace}/{name}'");

    // Link the secret back on the SA.
    let sa_gvr = GroupVersionResource::service_accounts();
    let sa_rref = ResourceRef {
        gvr: &sa_gvr,
        namespace: Some(namespace),
        name,
    };
    if let Some(mut current) = store.get(&sa_rref)? {
        let entry = serde_json::json!({"name": secret_name});
        if let Some(obj) = current.as_object_mut() {
            let secrets = obj
                .entry("secrets".to_string())
                .or_insert_with(|| serde_json::json!([]));
            if let Some(arr) = secrets.as_array_mut() {
                arr.push(entry);
            }
        }
        if let Err(e) = store.update(&sa_rref, &current) {
            tracing::debug!("sa secrets-list update conflict for '{namespace}/{name}': {e}");
        }
    }

    Ok(())
}
