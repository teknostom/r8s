//! Converters from Kubernetes protobuf-decoded messages to JSON `Value`s
//! matching the wire shape served by kube-apiserver.
//!
//! The prost-generated types don't carry the exact JSON field names k8s clients
//! expect (`openAPIV3Schema`, `x-kubernetes-preserve-unknown-fields`, etc.), so
//! each resource type that needs protobuf round-trip gets a hand-rolled walker
//! here. We start with `CustomResourceDefinition` — the only protobuf path
//! exercised by sig-api-machinery conformance today.

use prost::Message;
use serde_json::{Map, Value, json};

use crate::k8s_pb::admissionregistration_v1 as pbadmission;
use crate::k8s_pb::apiextensions_v1 as pbapi;
use crate::k8s_pb::coordination_v1 as pbcoord;
use crate::k8s_pb::core_v1 as pbcore;
use crate::k8s_pb::meta_v1 as pbmeta;
use crate::k8s_pb::runtime as pbruntime;

/// Magic prefix every k8s protobuf body starts with (`k8s\0`).
const K8S_MAGIC: &[u8] = b"k8s\0";

/// Decode a kubernetes protobuf request body into a JSON `Value` matching the
/// wire shape kube-apiserver serves. Returns `None` if the body doesn't have
/// the k8s envelope, isn't for a known GVK, or fails to decode.
///
/// Currently handles `apiextensions.k8s.io/v1 CustomResourceDefinition` only;
/// every other GVK falls through to `None`, which callers should treat as a
/// 415 Unsupported Media Type.
pub fn decode_k8s_protobuf_to_json(body: &[u8]) -> Option<Value> {
    if !body.starts_with(K8S_MAGIC) {
        return None;
    }
    let envelope = &body[K8S_MAGIC.len()..];

    // `runtime.Unknown` envelope: field 1 = TypeMeta, field 2 = raw resource.
    // NB: `runtime.TypeMeta` has (apiVersion=1, kind=2) — opposite of the
    // namesake `meta.v1.TypeMeta` (kind=1, apiVersion=2). Decoding the
    // envelope with the wrong one silently swaps the fields.
    let type_meta_bytes = read_message_field(envelope, 1)?;
    let raw = read_message_field(envelope, 2)?;
    let type_meta = pbruntime::TypeMeta::decode(type_meta_bytes).ok()?;
    let api_version = type_meta.api_version.as_deref().unwrap_or("");
    let kind = type_meta.kind.as_deref().unwrap_or("");

    match (api_version, kind) {
        ("apiextensions.k8s.io/v1", "CustomResourceDefinition") => {
            let crd = pbapi::CustomResourceDefinition::decode(raw).ok()?;
            Some(crd_to_json(&crd, api_version, kind))
        }
        ("v1", "Secret") => {
            let secret = pbcore::Secret::decode(raw).ok()?;
            Some(secret_to_json(&secret, api_version, kind))
        }
        ("coordination.k8s.io/v1", "Lease") => {
            let lease = pbcoord::Lease::decode(raw).ok()?;
            Some(lease_to_json(&lease, api_version, kind))
        }
        ("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration") => {
            let c = pbadmission::ValidatingWebhookConfiguration::decode(raw).ok()?;
            Some(webhook_config_to_json(
                c.metadata.as_ref(),
                c.webhooks.iter().map(validating_webhook_to_json).collect(),
                api_version,
                kind,
            ))
        }
        ("admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration") => {
            let c = pbadmission::MutatingWebhookConfiguration::decode(raw).ok()?;
            Some(webhook_config_to_json(
                c.metadata.as_ref(),
                c.webhooks.iter().map(mutating_webhook_to_json).collect(),
                api_version,
                kind,
            ))
        }
        _ => None,
    }
}

// ─── Secret walker ──────────────────────────────────────────────────────────
//
// Helm 3 stores its release info as a Secret via the typed client, which
// serializes as protobuf. Decoder must preserve `data` losslessly (it carries
// the gzip'd chart) — the old metadata-only path silently dropped it and
// broke `helm uninstall`.

fn secret_to_json(s: &pbcore::Secret, api_version: &str, kind: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = s.metadata.as_ref() {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if let Some(t) = s.r#type.as_deref() {
        obj.insert("type".into(), json!(t));
    }
    if let Some(im) = s.immutable {
        obj.insert("immutable".into(), json!(im));
    }
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    if !s.data.is_empty() {
        let mut data = Map::new();
        for (k, v) in &s.data {
            data.insert(k.clone(), json!(b64.encode(v)));
        }
        obj.insert("data".into(), Value::Object(data));
    }
    // `stringData` is a write-side convenience: per the k8s API contract the
    // server folds each entry into `data` (base64-encoding the UTF-8 bytes)
    // and the persisted object only has `data`. Mirror that here so callers
    // see a single canonical shape.
    if !s.string_data.is_empty() {
        let data_entry = obj.entry("data".to_string()).or_insert_with(|| {
            Value::Object(Map::new())
        });
        if let Some(data_obj) = data_entry.as_object_mut() {
            for (k, v) in &s.string_data {
                data_obj.insert(k.clone(), json!(b64.encode(v.as_bytes())));
            }
        }
    }
    Value::Object(obj)
}

// ─── Lease walker ───────────────────────────────────────────────────────────

// Leader-election clients (controller-runtime, client-go) write the lock
// Lease as protobuf. The store treats Lease as opaque, so all we have to do is
// faithfully round-trip the spec — `renewTime`/`acquireTime` in particular,
// since holders compare them against `leaseDurationSeconds` to decide whether
// the lease has expired.
fn lease_to_json(l: &pbcoord::Lease, api_version: &str, kind: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = l.metadata.as_ref() {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if let Some(spec) = l.spec.as_ref() {
        let mut s = Map::new();
        if let Some(v) = spec.holder_identity.as_deref() {
            s.insert("holderIdentity".into(), json!(v));
        }
        if let Some(v) = spec.lease_duration_seconds {
            s.insert("leaseDurationSeconds".into(), json!(v));
        }
        if let Some(t) = spec.acquire_time.as_ref() {
            s.insert("acquireTime".into(), micro_time_to_json(t));
        }
        if let Some(t) = spec.renew_time.as_ref() {
            s.insert("renewTime".into(), micro_time_to_json(t));
        }
        if let Some(v) = spec.lease_transitions {
            s.insert("leaseTransitions".into(), json!(v));
        }
        if let Some(v) = spec.strategy.as_deref() {
            s.insert("strategy".into(), json!(v));
        }
        if let Some(v) = spec.preferred_holder.as_deref() {
            s.insert("preferredHolder".into(), json!(v));
        }
        obj.insert("spec".into(), Value::Object(s));
    }
    Value::Object(obj)
}

// ─── Admission webhook config walker ────────────────────────────────────────

// cert-manager's cainjector writes the webhook config's `caBundle` via a full
// protobuf Update. We must round-trip the whole object — especially the
// `webhooks` array (clientConfig.service, rules, selectors) that r8s's own
// admission dispatch reads — or the Update would clobber it. Validating and
// Mutating configs share everything except per-webhook extras, so the wrapper
// is shared and the per-webhook converters differ.
fn webhook_config_to_json(
    metadata: Option<&pbmeta::ObjectMeta>,
    webhooks: Vec<Value>,
    api_version: &str,
    kind: &str,
) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = metadata {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if !webhooks.is_empty() {
        obj.insert("webhooks".into(), Value::Array(webhooks));
    }
    Value::Object(obj)
}

fn validating_webhook_to_json(w: &pbadmission::ValidatingWebhook) -> Value {
    let mut m = Map::new();
    insert_common_webhook_fields(
        &mut m,
        w.name.as_deref(),
        w.client_config.as_ref(),
        &w.rules,
        w.failure_policy.as_deref(),
        w.match_policy.as_deref(),
        w.namespace_selector.as_ref(),
        w.object_selector.as_ref(),
        w.side_effects.as_deref(),
        w.timeout_seconds,
        &w.admission_review_versions,
        &w.match_conditions,
    );
    Value::Object(m)
}

fn mutating_webhook_to_json(w: &pbadmission::MutatingWebhook) -> Value {
    let mut m = Map::new();
    insert_common_webhook_fields(
        &mut m,
        w.name.as_deref(),
        w.client_config.as_ref(),
        &w.rules,
        w.failure_policy.as_deref(),
        w.match_policy.as_deref(),
        w.namespace_selector.as_ref(),
        w.object_selector.as_ref(),
        w.side_effects.as_deref(),
        w.timeout_seconds,
        &w.admission_review_versions,
        &w.match_conditions,
    );
    if let Some(v) = w.reinvocation_policy.as_deref() {
        m.insert("reinvocationPolicy".into(), json!(v));
    }
    Value::Object(m)
}

#[allow(clippy::too_many_arguments)]
fn insert_common_webhook_fields(
    m: &mut Map<String, Value>,
    name: Option<&str>,
    client_config: Option<&pbadmission::WebhookClientConfig>,
    rules: &[pbadmission::RuleWithOperations],
    failure_policy: Option<&str>,
    match_policy: Option<&str>,
    namespace_selector: Option<&pbmeta::LabelSelector>,
    object_selector: Option<&pbmeta::LabelSelector>,
    side_effects: Option<&str>,
    timeout_seconds: Option<i32>,
    admission_review_versions: &[String],
    match_conditions: &[pbadmission::MatchCondition],
) {
    if let Some(v) = name {
        m.insert("name".into(), json!(v));
    }
    if let Some(cc) = client_config {
        m.insert("clientConfig".into(), webhook_client_config_to_json(cc));
    }
    if !rules.is_empty() {
        let rules: Vec<Value> = rules.iter().map(rule_with_operations_to_json).collect();
        m.insert("rules".into(), Value::Array(rules));
    }
    if let Some(v) = failure_policy {
        m.insert("failurePolicy".into(), json!(v));
    }
    if let Some(v) = match_policy {
        m.insert("matchPolicy".into(), json!(v));
    }
    if let Some(s) = namespace_selector {
        m.insert("namespaceSelector".into(), label_selector_to_json(s));
    }
    if let Some(s) = object_selector {
        m.insert("objectSelector".into(), label_selector_to_json(s));
    }
    if let Some(v) = side_effects {
        m.insert("sideEffects".into(), json!(v));
    }
    if let Some(v) = timeout_seconds {
        m.insert("timeoutSeconds".into(), json!(v));
    }
    if !admission_review_versions.is_empty() {
        m.insert("admissionReviewVersions".into(), json!(admission_review_versions));
    }
    if !match_conditions.is_empty() {
        let mc: Vec<Value> = match_conditions
            .iter()
            .map(|c| {
                let mut o = Map::new();
                if let Some(n) = c.name.as_deref() {
                    o.insert("name".into(), json!(n));
                }
                if let Some(e) = c.expression.as_deref() {
                    o.insert("expression".into(), json!(e));
                }
                Value::Object(o)
            })
            .collect();
        m.insert("matchConditions".into(), Value::Array(mc));
    }
}

fn webhook_client_config_to_json(cc: &pbadmission::WebhookClientConfig) -> Value {
    let mut o = Map::new();
    if let Some(url) = cc.url.as_deref() {
        o.insert("url".into(), json!(url));
    }
    if let Some(svc) = cc.service.as_ref() {
        let mut s = Map::new();
        if let Some(v) = svc.namespace.as_deref() {
            s.insert("namespace".into(), json!(v));
        }
        if let Some(v) = svc.name.as_deref() {
            s.insert("name".into(), json!(v));
        }
        if let Some(v) = svc.path.as_deref() {
            s.insert("path".into(), json!(v));
        }
        if let Some(v) = svc.port {
            s.insert("port".into(), json!(v));
        }
        o.insert("service".into(), Value::Object(s));
    }
    // caBundle is base64-encoded PEM in JSON — this is the field cainjector
    // is actually setting.
    if let Some(ca) = cc.ca_bundle.as_ref()
        && !ca.is_empty()
    {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(ca);
        o.insert("caBundle".into(), json!(b64));
    }
    Value::Object(o)
}

// RuleWithOperations embeds a Rule; in JSON the two are flattened into one
// object (operations + apiGroups/apiVersions/resources/scope side by side).
fn rule_with_operations_to_json(r: &pbadmission::RuleWithOperations) -> Value {
    let mut o = Map::new();
    if !r.operations.is_empty() {
        o.insert("operations".into(), json!(r.operations));
    }
    if let Some(rule) = r.rule.as_ref() {
        if !rule.api_groups.is_empty() {
            o.insert("apiGroups".into(), json!(rule.api_groups));
        }
        if !rule.api_versions.is_empty() {
            o.insert("apiVersions".into(), json!(rule.api_versions));
        }
        if !rule.resources.is_empty() {
            o.insert("resources".into(), json!(rule.resources));
        }
        if let Some(scope) = rule.scope.as_deref() {
            o.insert("scope".into(), json!(scope));
        }
    }
    Value::Object(o)
}

fn label_selector_to_json(s: &pbmeta::LabelSelector) -> Value {
    let mut o = Map::new();
    if !s.match_labels.is_empty() {
        o.insert("matchLabels".into(), string_map_to_json(&s.match_labels));
    }
    if !s.match_expressions.is_empty() {
        let exprs: Vec<Value> = s
            .match_expressions
            .iter()
            .map(|e| {
                let mut m = Map::new();
                if let Some(k) = e.key.as_deref() {
                    m.insert("key".into(), json!(k));
                }
                if let Some(op) = e.operator.as_deref() {
                    m.insert("operator".into(), json!(op));
                }
                if !e.values.is_empty() {
                    m.insert("values".into(), json!(e.values));
                }
                Value::Object(m)
            })
            .collect();
        o.insert("matchExpressions".into(), Value::Array(exprs));
    }
    Value::Object(o)
}

// ─── CRD walker ─────────────────────────────────────────────────────────────

fn crd_to_json(crd: &pbapi::CustomResourceDefinition, api_version: &str, kind: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = crd.metadata.as_ref() {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if let Some(spec) = crd.spec.as_ref() {
        obj.insert("spec".into(), crd_spec_to_json(spec));
    }
    if let Some(status) = crd.status.as_ref() {
        obj.insert("status".into(), crd_status_to_json(status));
    }
    Value::Object(obj)
}

fn crd_spec_to_json(spec: &pbapi::CustomResourceDefinitionSpec) -> Value {
    let mut obj = Map::new();
    obj.insert("group".into(), json!(spec.group.clone().unwrap_or_default()));
    if let Some(names) = spec.names.as_ref() {
        obj.insert("names".into(), crd_names_to_json(names));
    }
    obj.insert("scope".into(), json!(spec.scope.clone().unwrap_or_default()));
    if !spec.versions.is_empty() {
        let versions: Vec<Value> = spec.versions.iter().map(crd_version_to_json).collect();
        obj.insert("versions".into(), Value::Array(versions));
    }
    if let Some(conversion) = spec.conversion.as_ref() {
        obj.insert("conversion".into(), crd_conversion_to_json(conversion));
    }
    if let Some(p) = spec.preserve_unknown_fields {
        obj.insert("preserveUnknownFields".into(), json!(p));
    }
    Value::Object(obj)
}

fn crd_names_to_json(n: &pbapi::CustomResourceDefinitionNames) -> Value {
    let mut obj = Map::new();
    obj.insert("plural".into(), json!(n.plural.clone().unwrap_or_default()));
    if let Some(s) = &n.singular {
        obj.insert("singular".into(), json!(s));
    }
    if !n.short_names.is_empty() {
        obj.insert("shortNames".into(), json!(n.short_names));
    }
    obj.insert("kind".into(), json!(n.kind.clone().unwrap_or_default()));
    if let Some(lk) = &n.list_kind {
        obj.insert("listKind".into(), json!(lk));
    }
    if !n.categories.is_empty() {
        obj.insert("categories".into(), json!(n.categories));
    }
    Value::Object(obj)
}

fn crd_version_to_json(v: &pbapi::CustomResourceDefinitionVersion) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(v.name.clone().unwrap_or_default()));
    obj.insert("served".into(), json!(v.served.unwrap_or(false)));
    obj.insert("storage".into(), json!(v.storage.unwrap_or(false)));
    if let Some(d) = v.deprecated {
        obj.insert("deprecated".into(), json!(d));
    }
    if let Some(msg) = &v.deprecation_warning {
        obj.insert("deprecationWarning".into(), json!(msg));
    }
    if let Some(schema) = v.schema.as_ref() {
        obj.insert("schema".into(), crd_validation_to_json(schema));
    }
    if let Some(sub) = v.subresources.as_ref() {
        obj.insert("subresources".into(), crd_subresources_to_json(sub));
    }
    if !v.additional_printer_columns.is_empty() {
        let cols: Vec<Value> = v
            .additional_printer_columns
            .iter()
            .map(crd_column_to_json)
            .collect();
        obj.insert("additionalPrinterColumns".into(), Value::Array(cols));
    }
    if !v.selectable_fields.is_empty() {
        let sf: Vec<Value> = v
            .selectable_fields
            .iter()
            .map(|f| json!({ "jsonPath": f.json_path.clone().unwrap_or_default() }))
            .collect();
        obj.insert("selectableFields".into(), Value::Array(sf));
    }
    Value::Object(obj)
}

fn crd_validation_to_json(v: &pbapi::CustomResourceValidation) -> Value {
    let mut obj = Map::new();
    if let Some(s) = v.open_apiv3_schema.as_ref() {
        obj.insert("openAPIV3Schema".into(), json_schema_props_to_json(s));
    }
    Value::Object(obj)
}

fn crd_subresources_to_json(s: &pbapi::CustomResourceSubresources) -> Value {
    let mut obj = Map::new();
    if s.status.is_some() {
        obj.insert("status".into(), json!({}));
    }
    if let Some(scale) = s.scale.as_ref() {
        let mut so = Map::new();
        so.insert(
            "specReplicasPath".into(),
            json!(scale.spec_replicas_path.clone().unwrap_or_default()),
        );
        so.insert(
            "statusReplicasPath".into(),
            json!(scale.status_replicas_path.clone().unwrap_or_default()),
        );
        if let Some(sp) = &scale.label_selector_path {
            so.insert("labelSelectorPath".into(), json!(sp));
        }
        obj.insert("scale".into(), Value::Object(so));
    }
    Value::Object(obj)
}

fn crd_column_to_json(c: &pbapi::CustomResourceColumnDefinition) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(c.name.clone().unwrap_or_default()));
    obj.insert("type".into(), json!(c.r#type.clone().unwrap_or_default()));
    obj.insert("jsonPath".into(), json!(c.json_path.clone().unwrap_or_default()));
    if let Some(d) = &c.description {
        obj.insert("description".into(), json!(d));
    }
    if let Some(f) = &c.format {
        obj.insert("format".into(), json!(f));
    }
    if let Some(p) = c.priority {
        obj.insert("priority".into(), json!(p));
    }
    Value::Object(obj)
}

fn crd_conversion_to_json(c: &pbapi::CustomResourceConversion) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "strategy".into(),
        json!(c.strategy.clone().unwrap_or_default()),
    );
    if let Some(wc) = c.webhook.as_ref() {
        let mut wo = Map::new();
        if let Some(cc) = wc.client_config.as_ref() {
            let mut co = Map::new();
            if let Some(u) = &cc.url {
                co.insert("url".into(), json!(u));
            }
            if let Some(svc) = cc.service.as_ref() {
                let mut so = Map::new();
                so.insert("namespace".into(), json!(svc.namespace.clone().unwrap_or_default()));
                so.insert("name".into(), json!(svc.name.clone().unwrap_or_default()));
                if let Some(p) = &svc.path {
                    so.insert("path".into(), json!(p));
                }
                if let Some(port) = svc.port {
                    so.insert("port".into(), json!(port));
                }
                co.insert("service".into(), Value::Object(so));
            }
            if let Some(ca) = cc.ca_bundle.as_deref()
                && !ca.is_empty()
            {
                use base64::Engine;
                co.insert(
                    "caBundle".into(),
                    json!(base64::engine::general_purpose::STANDARD.encode(ca)),
                );
            }
            wo.insert("clientConfig".into(), Value::Object(co));
        }
        if !wc.conversion_review_versions.is_empty() {
            wo.insert(
                "conversionReviewVersions".into(),
                json!(wc.conversion_review_versions),
            );
        }
        obj.insert("webhook".into(), Value::Object(wo));
    }
    Value::Object(obj)
}

fn crd_status_to_json(s: &pbapi::CustomResourceDefinitionStatus) -> Value {
    let mut obj = Map::new();
    if !s.conditions.is_empty() {
        let conds: Vec<Value> = s
            .conditions
            .iter()
            .map(|c| {
                let mut co = Map::new();
                co.insert("type".into(), json!(c.r#type.clone().unwrap_or_default()));
                co.insert("status".into(), json!(c.status.clone().unwrap_or_default()));
                if let Some(t) = c.last_transition_time.as_ref() {
                    co.insert("lastTransitionTime".into(), time_to_json(t));
                }
                if let Some(r) = &c.reason {
                    co.insert("reason".into(), json!(r));
                }
                if let Some(m) = &c.message {
                    co.insert("message".into(), json!(m));
                }
                Value::Object(co)
            })
            .collect();
        obj.insert("conditions".into(), Value::Array(conds));
    }
    if let Some(names) = s.accepted_names.as_ref() {
        obj.insert("acceptedNames".into(), crd_names_to_json(names));
    }
    if !s.stored_versions.is_empty() {
        obj.insert("storedVersions".into(), json!(s.stored_versions));
    }
    Value::Object(obj)
}

// ─── ObjectMeta walker (subset) ─────────────────────────────────────────────

fn object_meta_to_json(m: &pbmeta::ObjectMeta) -> Value {
    let mut obj = Map::new();
    if let Some(v) = &m.name {
        obj.insert("name".into(), json!(v));
    }
    if let Some(v) = &m.generate_name {
        obj.insert("generateName".into(), json!(v));
    }
    if let Some(v) = &m.namespace {
        obj.insert("namespace".into(), json!(v));
    }
    if let Some(v) = &m.uid {
        obj.insert("uid".into(), json!(v));
    }
    if let Some(v) = &m.resource_version {
        obj.insert("resourceVersion".into(), json!(v));
    }
    if let Some(t) = m.creation_timestamp.as_ref() {
        obj.insert("creationTimestamp".into(), time_to_json(t));
    }
    if let Some(t) = m.deletion_timestamp.as_ref() {
        obj.insert("deletionTimestamp".into(), time_to_json(t));
    }
    if !m.labels.is_empty() {
        obj.insert("labels".into(), string_map_to_json(&m.labels));
    }
    if !m.annotations.is_empty() {
        obj.insert("annotations".into(), string_map_to_json(&m.annotations));
    }
    if !m.owner_references.is_empty() {
        let refs: Vec<Value> = m
            .owner_references
            .iter()
            .map(|o| {
                let mut r = Map::new();
                r.insert(
                    "apiVersion".into(),
                    json!(o.api_version.clone().unwrap_or_default()),
                );
                r.insert("kind".into(), json!(o.kind.clone().unwrap_or_default()));
                r.insert("name".into(), json!(o.name.clone().unwrap_or_default()));
                r.insert("uid".into(), json!(o.uid.clone().unwrap_or_default()));
                if let Some(c) = o.controller {
                    r.insert("controller".into(), json!(c));
                }
                if let Some(b) = o.block_owner_deletion {
                    r.insert("blockOwnerDeletion".into(), json!(b));
                }
                Value::Object(r)
            })
            .collect();
        obj.insert("ownerReferences".into(), Value::Array(refs));
    }
    if !m.finalizers.is_empty() {
        obj.insert("finalizers".into(), json!(m.finalizers));
    }
    Value::Object(obj)
}

// ─── JSONSchemaProps walker ─────────────────────────────────────────────────

fn json_schema_props_to_json(s: &pbapi::JsonSchemaProps) -> Value {
    let mut obj = Map::new();

    // Scalar primitives mirrored 1:1 ────────────────────────────────────────
    macro_rules! str_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = &s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }
    // JSONSchemaProps uses `omitempty` semantics for its bool fields — false
    // is the zero value and is dropped on the wire. Some proto generators
    // (Go's gogoproto with bare `bool` Go fields) emit explicit `false`
    // anyway; upstream's OpenAPI converter discards those before serving.
    // Mirror that or the conformance schema-equality check fails.
    macro_rules! bool_opt {
        ($field:ident, $json:literal) => {
            if let Some(true) = s.$field {
                obj.insert($json.into(), json!(true));
            }
        };
    }
    macro_rules! f64_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }
    macro_rules! i64_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }

    str_opt!(id, "id");
    str_opt!(schema, "$schema");
    str_opt!(r#ref, "$ref");
    str_opt!(description, "description");
    str_opt!(r#type, "type");
    str_opt!(format, "format");
    str_opt!(title, "title");
    str_opt!(pattern, "pattern");
    f64_opt!(maximum, "maximum");
    bool_opt!(exclusive_maximum, "exclusiveMaximum");
    f64_opt!(minimum, "minimum");
    bool_opt!(exclusive_minimum, "exclusiveMinimum");
    i64_opt!(max_length, "maxLength");
    i64_opt!(min_length, "minLength");
    i64_opt!(max_items, "maxItems");
    i64_opt!(min_items, "minItems");
    bool_opt!(unique_items, "uniqueItems");
    f64_opt!(multiple_of, "multipleOf");
    i64_opt!(max_properties, "maxProperties");
    i64_opt!(min_properties, "minProperties");
    if !s.required.is_empty() {
        obj.insert("required".into(), json!(s.required));
    }

    if let Some(d) = s.default.as_ref() {
        obj.insert("default".into(), raw_json_to_value(d.raw.as_deref().unwrap_or(&[])));
    }

    if !s.r#enum.is_empty() {
        let arr: Vec<Value> = s.r#enum.iter().map(|j| raw_json_to_value(j.raw.as_deref().unwrap_or(&[]))).collect();
        obj.insert("enum".into(), Value::Array(arr));
    }

    if let Some(items) = s.items.as_ref() {
        if let Some(schema) = items.schema.as_ref() {
            obj.insert("items".into(), json_schema_props_to_json(schema));
        } else if !items.j_son_schemas.is_empty() {
            obj.insert(
                "items".into(),
                Value::Array(
                    items
                        .j_son_schemas
                        .iter()
                        .map(json_schema_props_to_json)
                        .collect(),
                ),
            );
        }
    }

    if !s.all_of.is_empty() {
        obj.insert(
            "allOf".into(),
            Value::Array(s.all_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if !s.one_of.is_empty() {
        obj.insert(
            "oneOf".into(),
            Value::Array(s.one_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if !s.any_of.is_empty() {
        obj.insert(
            "anyOf".into(),
            Value::Array(s.any_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if let Some(not) = s.not.as_ref() {
        obj.insert("not".into(), json_schema_props_to_json(not));
    }

    if !s.properties.is_empty() {
        let mut props = Map::new();
        for (k, v) in &s.properties {
            props.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("properties".into(), Value::Object(props));
    }
    if let Some(ap) = s.additional_properties.as_ref() {
        if let Some(schema) = ap.schema.as_deref() {
            obj.insert("additionalProperties".into(), json_schema_props_to_json(schema));
        } else if let Some(allows) = ap.allows {
            obj.insert("additionalProperties".into(), json!(allows));
        }
    }
    if !s.pattern_properties.is_empty() {
        let mut props = Map::new();
        for (k, v) in &s.pattern_properties {
            props.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("patternProperties".into(), Value::Object(props));
    }
    if !s.dependencies.is_empty() {
        let mut deps = Map::new();
        for (k, v) in &s.dependencies {
            if let Some(schema) = v.schema.as_ref() {
                deps.insert(k.clone(), json_schema_props_to_json(schema));
            } else if !v.property.is_empty() {
                deps.insert(k.clone(), json!(v.property));
            }
        }
        obj.insert("dependencies".into(), Value::Object(deps));
    }
    if let Some(ai) = s.additional_items.as_ref() {
        if let Some(schema) = ai.schema.as_deref() {
            obj.insert("additionalItems".into(), json_schema_props_to_json(schema));
        } else if let Some(allows) = ai.allows {
            obj.insert("additionalItems".into(), json!(allows));
        }
    }
    if !s.definitions.is_empty() {
        let mut defs = Map::new();
        for (k, v) in &s.definitions {
            defs.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("definitions".into(), Value::Object(defs));
    }

    bool_opt!(nullable, "nullable");

    if let Some(ed) = s.external_docs.as_ref() {
        let mut e = Map::new();
        if let Some(d) = &ed.description {
            e.insert("description".into(), json!(d));
        }
        if let Some(u) = &ed.url {
            e.insert("url".into(), json!(u));
        }
        obj.insert("externalDocs".into(), Value::Object(e));
    }
    if let Some(ex) = s.example.as_ref() {
        obj.insert("example".into(), raw_json_to_value(ex.raw.as_deref().unwrap_or(&[])));
    }

    // x-kubernetes-* extensions ─────────────────────────────────────────────
    bool_opt!(x_kubernetes_preserve_unknown_fields, "x-kubernetes-preserve-unknown-fields");
    bool_opt!(x_kubernetes_embedded_resource, "x-kubernetes-embedded-resource");
    bool_opt!(x_kubernetes_int_or_string, "x-kubernetes-int-or-string");
    if !s.x_kubernetes_list_map_keys.is_empty() {
        obj.insert(
            "x-kubernetes-list-map-keys".into(),
            json!(s.x_kubernetes_list_map_keys),
        );
    }
    str_opt!(x_kubernetes_list_type, "x-kubernetes-list-type");
    str_opt!(x_kubernetes_map_type, "x-kubernetes-map-type");
    if !s.x_kubernetes_validations.is_empty() {
        let arr: Vec<Value> = s
            .x_kubernetes_validations
            .iter()
            .map(|v| {
                let mut o = Map::new();
                o.insert("rule".into(), json!(v.rule.clone().unwrap_or_default()));
                if let Some(m) = &v.message {
                    o.insert("message".into(), json!(m));
                }
                if let Some(me) = &v.message_expression {
                    o.insert("messageExpression".into(), json!(me));
                }
                if let Some(r) = &v.reason {
                    o.insert("reason".into(), json!(r));
                }
                if let Some(p) = &v.field_path {
                    o.insert("fieldPath".into(), json!(p));
                }
                if let Some(o2) = v.optional_old_self {
                    o.insert("optionalOldSelf".into(), json!(o2));
                }
                Value::Object(o)
            })
            .collect();
        obj.insert("x-kubernetes-validations".into(), Value::Array(arr));
    }

    Value::Object(obj)
}

// ─── Primitive helpers ──────────────────────────────────────────────────────

/// k8s `meta.v1.Time` has proto fields `seconds: i64` + `nanos: i32` but the
/// JSON form is an RFC3339 timestamp string. Empty timestamps serialize as
/// `null`, matching upstream.
fn time_to_json(t: &pbmeta::Time) -> Value {
    use chrono::TimeZone;
    let secs = t.seconds.unwrap_or(0);
    let nanos = t.nanos.unwrap_or(0);
    if secs == 0 && nanos == 0 {
        return Value::Null;
    }
    match chrono::Utc.timestamp_opt(secs, nanos as u32) {
        chrono::offset::LocalResult::Single(dt) => json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        _ => Value::Null,
    }
}

/// Like [`time_to_json`] but for `meta.v1.MicroTime`, which k8s serializes at
/// microsecond precision — leader-election renewal math relies on the
/// sub-second component, so we must not truncate it to whole seconds.
fn micro_time_to_json(t: &pbmeta::MicroTime) -> Value {
    use chrono::TimeZone;
    let secs = t.seconds.unwrap_or(0);
    let nanos = t.nanos.unwrap_or(0);
    if secs == 0 && nanos == 0 {
        return Value::Null;
    }
    match chrono::Utc.timestamp_opt(secs, nanos as u32) {
        chrono::offset::LocalResult::Single(dt) => {
            json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        }
        _ => Value::Null,
    }
}

fn string_map_to_json(m: &std::collections::HashMap<String, String>) -> Value {
    let mut o = Map::new();
    for (k, v) in m {
        o.insert(k.clone(), json!(v));
    }
    Value::Object(o)
}

/// `runtime.RawExtension` (used by JSONSchemaProps.default, .example, .enum
/// items) carries a raw JSON byte slice. Parse it back into a `Value`.
fn raw_json_to_value(raw: &[u8]) -> Value {
    serde_json::from_slice(raw).unwrap_or(Value::Null)
}

// ─── Internal varint helpers (just enough to locate envelope sub-messages) ──

fn read_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &b) in data.iter().enumerate() {
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn read_message_field(data: &[u8], target: u32) -> Option<&[u8]> {
    let mut pos = 0;
    while pos < data.len() {
        let (tag, n) = read_varint(&data[pos..])?;
        pos += n;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        if wire != 2 {
            // Skip non-length-delimited fields.
            let skip = match wire {
                0 => read_varint(&data[pos..]).map(|(_, n)| n)?,
                1 => 8,
                5 => 4,
                _ => return None,
            };
            pos += skip;
            continue;
        }
        let (len, n) = read_varint(&data[pos..])?;
        pos += n;
        let end = pos + len as usize;
        if end > data.len() {
            return None;
        }
        if field == target {
            return Some(&data[pos..end]);
        }
        pos = end;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s_pb::runtime as pbruntime;
    use base64::Engine;
    use prost::Message;

    /// Wrap a raw resource in the k8s `Unknown` envelope (magic + TypeMeta + raw),
    /// the exact framing client-go sends.
    fn envelope(api_version: &str, kind: &str, raw: Vec<u8>) -> Vec<u8> {
        let unknown = pbruntime::Unknown {
            type_meta: Some(pbruntime::TypeMeta {
                api_version: Some(api_version.into()),
                kind: Some(kind.into()),
            }),
            raw: Some(raw),
            ..Default::default()
        };
        let mut body = K8S_MAGIC.to_vec();
        body.extend(unknown.encode_to_vec());
        body
    }

    #[test]
    fn decodes_validating_webhook_config_round_trip() {
        let config = pbadmission::ValidatingWebhookConfiguration {
            metadata: Some(pbmeta::ObjectMeta {
                name: Some("cert-manager-webhook".into()),
                ..Default::default()
            }),
            webhooks: vec![pbadmission::ValidatingWebhook {
                name: Some("webhook.cert-manager.io".into()),
                client_config: Some(pbadmission::WebhookClientConfig {
                    service: Some(pbadmission::ServiceReference {
                        namespace: Some("cert-manager".into()),
                        name: Some("cert-manager-webhook".into()),
                        path: Some("/validate".into()),
                        port: Some(443),
                    }),
                    ca_bundle: Some(b"PEMDATA".to_vec()),
                    url: None,
                }),
                rules: vec![pbadmission::RuleWithOperations {
                    operations: vec!["CREATE".into(), "UPDATE".into()],
                    rule: Some(pbadmission::Rule {
                        api_groups: vec!["cert-manager.io".into()],
                        api_versions: vec!["v1".into()],
                        resources: vec!["certificates".into()],
                        scope: Some("*".into()),
                    }),
                }],
                side_effects: Some("None".into()),
                ..Default::default()
            }],
        };
        let body = envelope(
            "admissionregistration.k8s.io/v1",
            "ValidatingWebhookConfiguration",
            config.encode_to_vec(),
        );

        let v = decode_k8s_protobuf_to_json(&body).expect("decode");
        assert_eq!(v["kind"], "ValidatingWebhookConfiguration");
        assert_eq!(v["metadata"]["name"], "cert-manager-webhook");

        let wh = &v["webhooks"][0];
        assert_eq!(wh["name"], "webhook.cert-manager.io");
        assert_eq!(wh["clientConfig"]["service"]["name"], "cert-manager-webhook");
        assert_eq!(wh["clientConfig"]["service"]["path"], "/validate");
        assert_eq!(wh["clientConfig"]["service"]["port"], 443);
        // The whole point: caBundle must survive (base64 of "PEMDATA").
        let expected_ca = base64::engine::general_purpose::STANDARD.encode(b"PEMDATA");
        assert_eq!(wh["clientConfig"]["caBundle"], expected_ca);
        // RuleWithOperations must flatten operations + rule fields together.
        assert_eq!(wh["rules"][0]["operations"][0], "CREATE");
        assert_eq!(wh["rules"][0]["apiGroups"][0], "cert-manager.io");
        assert_eq!(wh["rules"][0]["resources"][0], "certificates");
        assert_eq!(wh["rules"][0]["scope"], "*");
        assert_eq!(wh["sideEffects"], "None");
    }

    #[test]
    fn decodes_mutating_webhook_reinvocation_policy() {
        let config = pbadmission::MutatingWebhookConfiguration {
            metadata: Some(pbmeta::ObjectMeta {
                name: Some("cert-manager-webhook".into()),
                ..Default::default()
            }),
            webhooks: vec![pbadmission::MutatingWebhook {
                name: Some("webhook.cert-manager.io".into()),
                reinvocation_policy: Some("Never".into()),
                ..Default::default()
            }],
        };
        let body = envelope(
            "admissionregistration.k8s.io/v1",
            "MutatingWebhookConfiguration",
            config.encode_to_vec(),
        );
        let v = decode_k8s_protobuf_to_json(&body).expect("decode");
        assert_eq!(v["webhooks"][0]["reinvocationPolicy"], "Never");
    }

    #[test]
    fn decodes_lease_spec() {
        let lease = pbcoord::Lease {
            metadata: Some(pbmeta::ObjectMeta {
                name: Some("cert-manager-cainjector-leader-election".into()),
                ..Default::default()
            }),
            spec: Some(pbcoord::LeaseSpec {
                holder_identity: Some("holder-1".into()),
                lease_duration_seconds: Some(15),
                ..Default::default()
            }),
        };
        let body = envelope("coordination.k8s.io/v1", "Lease", lease.encode_to_vec());
        let v = decode_k8s_protobuf_to_json(&body).expect("decode");
        assert_eq!(v["spec"]["holderIdentity"], "holder-1");
        assert_eq!(v["spec"]["leaseDurationSeconds"], 15);
    }
}
