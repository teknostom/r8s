//! ValidatingAdmissionPolicy: each binding whose policy and match resources
//! select the request runs the policy's validations in CEL; a failing one
//! with the Deny action rejects the request.

// cel-interpreter builds its maps from std HashMaps.
#![allow(clippy::disallowed_types)]

use std::collections::HashMap;

use cel_interpreter::Value as Cel;
use r8s_store::backend::ResourceRef;
use r8s_types::GroupVersionResource;
use serde_json::{Value, json};

use crate::admission::{AdmissionCtx, AdmissionError};
use crate::cel::{self, Activation};

const GROUP: &str = "admissionregistration.k8s.io";

/// Rejects the request when a bound policy denies it.
pub fn validate(
    ctx: &AdmissionCtx<'_>,
    object: &Value,
    old_object: Option<&Value>,
) -> Result<(), AdmissionError> {
    if ctx.gvr.group == GROUP {
        return Ok(());
    }
    let list = |resource: &str| {
        ctx.store
            .list(
                &GroupVersionResource::new(GROUP, "v1", resource),
                None,
                None,
                None,
                None,
                None,
            )
            .map(|r| r.items)
            .unwrap_or_default()
    };
    let bindings = list("validatingadmissionpolicybindings");
    if bindings.is_empty() {
        return Ok(());
    }
    let policies: HashMap<String, Value> = list("validatingadmissionpolicies")
        .into_iter()
        .filter_map(|p| Some((p["metadata"]["name"].as_str()?.to_string(), p)))
        .collect();
    let (object, old_object) = match ctx.operation.as_str() {
        "DELETE" => (&Value::Null, old_object.or(Some(object))),
        _ => (object, old_object),
    };
    for binding in &bindings {
        let Some(policy) = binding["spec"]["policyName"]
            .as_str()
            .and_then(|n| policies.get(n))
        else {
            continue;
        };
        if !selects(
            ctx,
            &policy["spec"]["matchConstraints"],
            object,
            old_object,
            true,
        ) || !selects(
            ctx,
            &binding["spec"]["matchResources"],
            object,
            old_object,
            false,
        ) {
            continue;
        }
        let actions: Vec<&str> = binding["spec"]["validationActions"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str())
            .collect();
        if !actions.contains(&"Deny") {
            continue;
        }
        if let Some(message) = run_policy(ctx, policy, object, old_object) {
            let policy_name = policy["metadata"]["name"].as_str().unwrap_or("");
            let binding_name = binding["metadata"]["name"].as_str().unwrap_or("");
            return Err(AdmissionError::PolicyDenied {
                message: format!(
                    "ValidatingAdmissionPolicy '{policy_name}' with binding '{binding_name}' denied request: {message}"
                ),
                reason: message_reason(policy),
            });
        }
    }
    Ok(())
}

/// The reason a denial carries; Invalid unless a validation names another.
fn message_reason(policy: &Value) -> String {
    policy["spec"]["validations"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|v| v["reason"].as_str())
        .unwrap_or("Invalid")
        .to_string()
}

/// Whether `match_resources` (a policy's constraints or a binding's match)
/// selects the request. A binding without resource rules takes the policy's.
fn selects(
    ctx: &AdmissionCtx<'_>,
    match_resources: &Value,
    object: &Value,
    old_object: Option<&Value>,
    rules_required: bool,
) -> bool {
    let subject = if object.is_null() {
        old_object
    } else {
        Some(object)
    };
    let rules = &match_resources["resourceRules"];
    if (rules.is_array() || rules_required) && !crate::admission::rules_match(rules, ctx, subject) {
        return false;
    }
    if match_resources["excludeResourceRules"].is_array()
        && crate::admission::rules_match(&match_resources["excludeResourceRules"], ctx, subject)
    {
        return false;
    }
    crate::admission::selectors_match(
        ctx,
        match_resources.get("namespaceSelector"),
        match_resources.get("objectSelector"),
        subject,
        old_object,
    )
}

/// The first failing validation's message, or `None` when all pass (or
/// their errors are ignored by the failure policy).
fn run_policy(
    ctx: &AdmissionCtx<'_>,
    policy: &Value,
    object: &Value,
    old_object: Option<&Value>,
) -> Option<String> {
    let spec = &policy["spec"];
    let fail_closed = spec["failurePolicy"].as_str() != Some("Ignore");
    let mut activation = Activation::default()
        .with("object", cel::from_json(object))
        .with(
            "oldObject",
            old_object.map(cel::from_json).unwrap_or(Cel::Null),
        )
        .with(
            "request",
            cel::from_json(&crate::admission::request_json(ctx)),
        )
        .with("params", Cel::Null)
        .with("namespaceObject", cel::from_json(&namespace_object(ctx)));

    for condition in spec["matchConditions"].as_array().into_iter().flatten() {
        let expression = condition["expression"].as_str().unwrap_or("");
        match cel::check(expression, &activation) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(e) if fail_closed => {
                return Some(format!("expression '{expression}' resulted in error: {e}"));
            }
            Err(_) => return None,
        }
    }

    let mut variables: HashMap<String, Cel> = HashMap::new();
    for variable in spec["variables"].as_array().into_iter().flatten() {
        let name = variable["name"].as_str().unwrap_or("").to_string();
        let expression = variable["expression"].as_str().unwrap_or("");
        activation.set("variables", Cel::from(variables.clone()));
        let value = match cel::compile(expression).and_then(|p| cel::evaluate(&p, &activation)) {
            Ok(v) => v,
            Err(e) if fail_closed => {
                return Some(format!(
                    "variable '{name}' with expression '{expression}' resulted in error: {e}"
                ));
            }
            Err(_) => return None,
        };
        variables.insert(name, value);
    }
    activation.set("variables", Cel::from(variables));

    for validation in spec["validations"].as_array().into_iter().flatten() {
        let expression = validation["expression"].as_str().unwrap_or("");
        match cel::check(expression, &activation) {
            Ok(true) => {}
            Ok(false) => {
                let from_expression = validation["messageExpression"]
                    .as_str()
                    .and_then(|m| cel::compile(m).ok())
                    .and_then(|p| cel::evaluate_string(&p, &activation).ok())
                    .filter(|m| !m.trim().is_empty());
                return Some(
                    from_expression
                        .or_else(|| validation["message"].as_str().map(String::from))
                        .unwrap_or_else(|| format!("failed expression: {expression}")),
                );
            }
            Err(e) if fail_closed => {
                return Some(format!("expression '{expression}' resulted in error: {e}"));
            }
            Err(_) => {}
        }
    }
    None
}

/// The request's Namespace object; `null` for cluster-scoped requests.
fn namespace_object(ctx: &AdmissionCtx<'_>) -> Value {
    let Some(ns) = ctx.namespace.filter(|n| !n.is_empty()) else {
        return Value::Null;
    };
    ctx.store
        .get(&ResourceRef {
            gvr: &GroupVersionResource::namespaces(),
            namespace: None,
            name: ns,
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| json!({"metadata": {"name": ns}}))
}

/// Compile errors in a webhook configuration's or a policy's expressions, as
/// `(field path, expression, error)`.
pub fn compile_errors(group: &str, resource: &str, body: &Value) -> Vec<(String, String, String)> {
    let mut fields: Vec<(String, &Value)> = Vec::new();
    match (group, resource) {
        (GROUP, "validatingwebhookconfigurations" | "mutatingwebhookconfigurations") => {
            for (i, webhook) in body["webhooks"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                for (j, c) in webhook["matchConditions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    fields.push((
                        format!("webhooks[{i}].matchConditions[{j}].expression"),
                        &c["expression"],
                    ));
                }
            }
        }
        (GROUP, "validatingadmissionpolicies") => {
            let spec = &body["spec"];
            for (list, key) in [
                ("matchConditions", "expression"),
                ("variables", "expression"),
                ("validations", "expression"),
                ("validations", "messageExpression"),
                ("auditAnnotations", "valueExpression"),
            ] {
                for (i, item) in spec[list].as_array().into_iter().flatten().enumerate() {
                    fields.push((format!("spec.{list}[{i}].{key}"), &item[key]));
                }
            }
        }
        _ => {}
    }
    fields
        .into_iter()
        .filter_map(|(path, value)| {
            let expression = value.as_str()?;
            let error = cel::compile(expression).err()?;
            Some((path, expression.to_string(), error))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_errors_name_the_field() {
        let webhooks = json!({"webhooks": [{"name": "a", "matchConditions": [
            {"name": "ok", "expression": "true"},
            {"name": "bad", "expression": "... [] bad expression"}
        ]}]});
        let errors = compile_errors(GROUP, "validatingwebhookconfigurations", &webhooks);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "webhooks[0].matchConditions[1].expression");
        let policy = json!({"spec": {"validations": [{"expression": "object.spec.replicas > 1",
                                                      "messageExpression": "'x' +"}]}});
        let errors = compile_errors(GROUP, "validatingadmissionpolicies", &policy);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "spec.validations[0].messageExpression");
    }
}
