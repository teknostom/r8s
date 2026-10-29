//! CRD conversion: an object stored in one served version is converted to the
//! version a request names, through the CRD's `spec.conversion.webhook` when
//! the strategy is `Webhook`, by rewriting `apiVersion` when it is `None`.

use std::net::IpAddr;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::GroupVersionResource;
use serde_json::{Value, json};

use crate::admission::{self, AdmissionError};
use crate::handler::RouteContext;

#[derive(Clone, Debug)]
pub struct Conversion {
    /// `group/version` the CRD stores objects in.
    pub storage_version: String,
    webhook: Option<Webhook>,
}

#[derive(Clone, Debug)]
struct Webhook {
    service: Option<(String, String, String, u16)>, // namespace, name, path, port
    url: Option<String>,
    ca_pem: Option<Vec<u8>>,
    review_version: String,
}

impl Conversion {
    /// Conversion settings of the CRD serving `ctx`; None for built-ins or
    /// when the CRD is gone.
    pub fn for_resource(store: &Store, ctx: &RouteContext) -> Option<Conversion> {
        let rt = &ctx.resource_type;
        rt.storage_gvr.as_ref()?;
        let crd_gvr =
            GroupVersionResource::new("apiextensions.k8s.io", "v1", "customresourcedefinitions");
        let name = format!("{}.{}", rt.gvr.resource, rt.gvr.group);
        let crd = store
            .get(&ResourceRef {
                gvr: &crd_gvr,
                namespace: None,
                name: &name,
            })
            .ok()
            .flatten()?;
        let spec = crd.get("spec")?;
        let storage_version = spec
            .get("versions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|v| v.get("storage").and_then(Value::as_bool) == Some(true))
            .and_then(|v| v.get("name").and_then(Value::as_str))
            .map(|v| format!("{}/{v}", rt.gvr.group))
            .unwrap_or_else(|| rt.gvr.api_version());
        let conversion = spec.get("conversion");
        let webhook = match conversion
            .and_then(|c| c.get("strategy"))
            .and_then(Value::as_str)
        {
            Some("Webhook") => {
                let hook = conversion.and_then(|c| c.get("webhook"));
                let client = hook.and_then(|h| h.get("clientConfig"));
                let service = client.and_then(|c| c.get("service")).map(|s| {
                    (
                        s.get("namespace")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        s.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        s.get("path")
                            .and_then(Value::as_str)
                            .unwrap_or("/")
                            .to_string(),
                        s.get("port").and_then(Value::as_u64).unwrap_or(443) as u16,
                    )
                });
                let review_version = hook
                    .and_then(|h| h.get("conversionReviewVersions"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .find(|v| matches!(*v, "v1" | "v1beta1"))
                    .unwrap_or("v1")
                    .to_string();
                Some(Webhook {
                    service,
                    url: client
                        .and_then(|c| c.get("url"))
                        .and_then(Value::as_str)
                        .map(String::from),
                    ca_pem: admission::resolve_ca(
                        store,
                        client.and_then(|c| c.get("caBundle")),
                        None,
                    ),
                    review_version,
                })
            }
            _ => None,
        };
        Some(Conversion {
            storage_version,
            webhook,
        })
    }

    /// True when serving `from` as `to` needs the webhook (both `group/version`).
    pub fn needs_webhook(&self, from: &str, to: &str) -> bool {
        self.webhook.is_some() && from != to
    }

    /// `objects`, each converted to `desired`. Objects already in that version
    /// pass through; the rest go to the webhook in one review per source
    /// version, or just have `apiVersion` rewritten when there is no webhook.
    pub async fn convert(
        &self,
        store: &Store,
        objects: Vec<Value>,
        desired: &str,
    ) -> Result<Vec<Value>, String> {
        let mut out: Vec<Option<Value>> = Vec::with_capacity(objects.len());
        let mut pending: Vec<(usize, Value)> = Vec::new();
        for (i, obj) in objects.into_iter().enumerate() {
            let version = obj.get("apiVersion").and_then(Value::as_str).unwrap_or("");
            if version == desired || version.is_empty() {
                out.push(Some(obj));
            } else if self.webhook.is_none() {
                let mut obj = obj;
                obj["apiVersion"] = json!(desired);
                out.push(Some(obj));
            } else {
                out.push(None);
                pending.push((i, obj));
            }
        }
        if !pending.is_empty() {
            let converted = self
                .call(
                    store,
                    pending.iter().map(|(_, o)| o.clone()).collect(),
                    desired,
                )
                .await?;
            for ((i, _), obj) in pending.into_iter().zip(converted) {
                out[i] = Some(obj);
            }
        }
        Ok(out.into_iter().flatten().collect())
    }

    pub async fn convert_one(
        &self,
        store: &Store,
        object: Value,
        desired: &str,
    ) -> Result<Value, String> {
        self.convert(store, vec![object], desired)
            .await?
            .pop()
            .ok_or_else(|| "conversion webhook returned no object".to_string())
    }

    async fn call(
        &self,
        store: &Store,
        objects: Vec<Value>,
        desired: &str,
    ) -> Result<Vec<Value>, String> {
        let webhook = self.webhook.as_ref().ok_or("no conversion webhook")?;
        let uid = admission::uuid_like();
        let review = json!({
            "apiVersion": format!("apiextensions.k8s.io/{}", webhook.review_version),
            "kind": "ConversionReview",
            "request": {"uid": uid, "desiredAPIVersion": desired, "objects": objects},
        });
        let body = serde_json::to_vec(&review).map_err(|e| e.to_string())?;
        let count = objects_len(&review);

        let (ip, port, host, path) = match (&webhook.service, &webhook.url) {
            (Some((ns, name, path, svc_port)), _) => {
                let (ip, port) = admission::resolve_service_endpoint(store, ns, name, *svc_port)
                    .map_err(admission_text)?;
                (ip, port, format!("{name}.{ns}.svc"), path.clone())
            }
            (None, Some(url)) => resolve_url(url).await?,
            (None, None) => return Err("conversion webhook has no service or url".into()),
        };
        let raw = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            admission::do_https_post(ip, port, &host, &path, &body, webhook.ca_pem.as_deref()),
        )
        .await
        .map_err(|_| format!("Post \"https://{host}:{port}{path}\": context deadline exceeded"))?
        .map_err(|e| format!("Post \"https://{host}:{port}{path}\": {e}"))?;

        let parsed: Value =
            serde_json::from_slice(&raw).map_err(|e| format!("response json: {e}"))?;
        let response = parsed
            .get("response")
            .ok_or_else(|| "response missing".to_string())?;
        if response.get("uid").and_then(Value::as_str) != Some(uid.as_str()) {
            return Err("response uid mismatch".into());
        }
        let status = response
            .get("result")
            .and_then(|r| r.get("status"))
            .and_then(Value::as_str);
        if status != Some("Success") {
            let message = response
                .get("result")
                .and_then(|r| r.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("conversion failed");
            return Err(message.to_string());
        }
        let converted = response
            .get("convertedObjects")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if converted.len() != count {
            return Err(format!(
                "expected {count} converted objects, got {}",
                converted.len()
            ));
        }
        for obj in &converted {
            if obj.get("apiVersion").and_then(Value::as_str) != Some(desired) {
                return Err(format!(
                    "converted object has apiVersion {}, expected {desired}",
                    obj.get("apiVersion").cloned().unwrap_or(Value::Null)
                ));
            }
        }
        Ok(converted)
    }
}

fn objects_len(review: &Value) -> usize {
    review["request"]["objects"].as_array().map_or(0, Vec::len)
}

fn admission_text(e: AdmissionError) -> String {
    match e {
        AdmissionError::Internal(s) => s,
        other => format!("{other:?}"),
    }
}

/// `(ip, port, host, path)` for a `url` client config.
async fn resolve_url(url: &str) -> Result<(IpAddr, u16, String, String), String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("unsupported webhook url {url}"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|e| e.to_string())?),
        None => (authority.to_string(), 443),
    };
    let ip = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| format!("resolve {host}: {e}"))?
        .next()
        .map(|a| a.ip())
        .ok_or_else(|| format!("no address for {host}"))?;
    Ok((ip, port, host, path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use r8s_types::ResourceType;
    use std::sync::Arc;

    fn ctx() -> RouteContext {
        RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::new("stable.example.com", "v2", "foos"),
                kind: "Foo".into(),
                namespaced: true,
                singular: "foo".into(),
                short_names: vec![],
                subresources: vec![],
                schema: None,
                storage_gvr: Some(GroupVersionResource::storage("stable.example.com", "foos")),
            }),
        }
    }

    fn store_with_crd(strategy: Value) -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let gvr =
            GroupVersionResource::new("apiextensions.k8s.io", "v1", "customresourcedefinitions");
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: None,
                    name: "foos.stable.example.com",
                },
                &json!({
                    "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
                    "metadata": {"name": "foos.stable.example.com"},
                    "spec": {
                        "group": "stable.example.com",
                        "names": {"plural": "foos", "kind": "Foo"},
                        "scope": "Namespaced",
                        "versions": [{"name": "v1", "served": true, "storage": true},
                                     {"name": "v2", "served": true, "storage": false}],
                        "conversion": strategy,
                    }
                }),
            )
            .unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn strategy_none_rewrites_api_version_only() {
        let (_d, store) = store_with_crd(json!({"strategy": "None"}));
        let conv = Conversion::for_resource(&store, &ctx()).unwrap();
        assert_eq!(conv.storage_version, "stable.example.com/v1");
        assert!(!conv.needs_webhook("stable.example.com/v1", "stable.example.com/v2"));
        let out = conv
            .convert(
                &store,
                vec![json!({"apiVersion": "stable.example.com/v1", "kind": "Foo", "hostPort": "h:1"})],
                "stable.example.com/v2",
            )
            .await
            .unwrap();
        assert_eq!(out[0]["apiVersion"], "stable.example.com/v2");
        assert_eq!(out[0]["hostPort"], "h:1");
    }

    #[tokio::test]
    async fn webhook_strategy_is_needed_across_versions() {
        let (_d, store) = store_with_crd(json!({
            "strategy": "Webhook",
            "webhook": {"clientConfig": {"service": {"namespace": "ns", "name": "hook", "path": "/convert", "port": 9443}},
                        "conversionReviewVersions": ["v1"]}
        }));
        let conv = Conversion::for_resource(&store, &ctx()).unwrap();
        assert!(conv.needs_webhook("stable.example.com/v1", "stable.example.com/v2"));
        assert!(!conv.needs_webhook("stable.example.com/v2", "stable.example.com/v2"));
        // Objects already in the desired version never reach the webhook.
        let out = conv
            .convert(
                &store,
                vec![json!({"apiVersion": "stable.example.com/v2"})],
                "stable.example.com/v2",
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        // A conversion with no reachable service fails with the endpoint error.
        let err = conv
            .convert(
                &store,
                vec![json!({"apiVersion": "stable.example.com/v1"})],
                "stable.example.com/v2",
            )
            .await
            .unwrap_err();
        assert!(err.contains("service ns/hook not found"), "{err}");
    }
}
