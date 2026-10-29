//! CEL for admission: webhook `matchConditions` and ValidatingAdmissionPolicy
//! expressions, through the cel-interpreter crate.

// cel-interpreter builds its maps from std HashMaps.
#![allow(clippy::disallowed_types)]

use std::collections::HashMap;
use std::sync::Arc;

use cel_interpreter::{Context, Program, Value};

/// A compiled expression, or the compiler's message. The parser panics on
/// some malformed input; that is a compile error too.
pub fn compile(expression: &str) -> Result<Program, String> {
    quiet_parser_panics();
    COMPILING.with(|c| c.set(true));
    let result = std::panic::catch_unwind(|| Program::compile(expression));
    COMPILING.with(|c| c.set(false));
    match result {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => Err(format!("ERROR: <input>: syntax error in {expression:?}")),
    }
}

thread_local! {
    static COMPILING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The default panic hook, except for the parser's panics during
/// `compile`, which become compile errors.
fn quiet_parser_panics() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !COMPILING.with(|c| c.get()) {
                previous(info);
            }
        }));
    });
}

/// A JSON value as CEL sees it: integers that fit stay `int`, maps keyed by
/// string, `null` for null.
pub fn from_json(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => Value::Int(i),
            (None, Some(u), _) => Value::UInt(u),
            (_, _, Some(f)) => Value::Float(f),
            _ => Value::Null,
        },
        serde_json::Value::String(s) => Value::String(Arc::new(s.clone())),
        serde_json::Value::Array(items) => {
            Value::List(Arc::new(items.iter().map(from_json).collect()))
        }
        serde_json::Value::Object(map) => {
            let entries: HashMap<String, Value> =
                map.iter().map(|(k, v)| (k.clone(), from_json(v))).collect();
            Value::from(entries)
        }
    }
}

/// Variables an expression runs with.
#[derive(Default)]
pub struct Activation {
    vars: Vec<(String, Value)>,
}

impl Activation {
    pub fn with(mut self, name: &str, value: Value) -> Self {
        self.vars.push((name.to_string(), value));
        self
    }

    pub fn set(&mut self, name: &str, value: Value) {
        self.vars.retain(|(n, _)| n != name);
        self.vars.push((name.to_string(), value));
    }

    fn context(&self) -> Context<'_> {
        let mut ctx = Context::default();
        for (name, value) in &self.vars {
            ctx.add_variable_from_value(name.clone(), value.clone());
        }
        ctx
    }
}

/// The expression's value.
pub fn evaluate(program: &Program, activation: &Activation) -> Result<Value, String> {
    program
        .execute(&activation.context())
        .map_err(|e| e.to_string())
}

/// The expression's value, which must be a bool.
pub fn evaluate_bool(program: &Program, activation: &Activation) -> Result<bool, String> {
    match evaluate(program, activation)? {
        Value::Bool(b) => Ok(b),
        other => Err(format!("expected bool, got {:?}", other.type_of())),
    }
}

/// The expression's value, which must be a string.
pub fn evaluate_string(program: &Program, activation: &Activation) -> Result<String, String> {
    match evaluate(program, activation)? {
        Value::String(s) => Ok(s.to_string()),
        other => Err(format!("expected string, got {:?}", other.type_of())),
    }
}

/// Compiles and evaluates `expression` to a bool.
pub fn check(expression: &str, activation: &Activation) -> Result<bool, String> {
    evaluate_bool(&compile(expression)?, activation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn object(o: serde_json::Value) -> Activation {
        Activation::default().with("object", from_json(&o))
    }

    #[test]
    fn evaluates_against_json_objects() {
        let deployment = object(json!({"metadata": {"name": "web"}, "spec": {"replicas": 3}}));
        assert_eq!(check("object.spec.replicas > 1", &deployment), Ok(true));
        assert_eq!(
            check("object.spec.replicas % 2 == 1", &deployment),
            Ok(true)
        );
        assert_eq!(
            check("object.metadata.name != 'skip-me'", &deployment),
            Ok(true)
        );
        assert_eq!(check("has(object.metadata.labels)", &deployment), Ok(false));
        let skip = object(json!({"metadata": {"name": "skip-me"}}));
        assert_eq!(check("object.metadata.name != 'skip-me'", &skip), Ok(false));
    }

    #[test]
    fn bad_expressions_fail_to_compile() {
        assert!(compile("... [] bad expression").is_err());
        assert!(compile("object.spec.replicas > 1").is_ok());
    }

    #[test]
    fn missing_fields_and_type_confusion_are_errors() {
        let deployment = object(json!({"spec": {"replicas": 1}}));
        assert!(check("object.spec.missing > 1", &deployment).is_err());
        let message = compile("'wants replicas > 1, got ' + object.spec.replicas").unwrap();
        assert!(evaluate_string(&message, &deployment).is_err());
        let ok = compile("'wants replicas > 1, got ' + string(object.spec.replicas)").unwrap();
        assert_eq!(
            evaluate_string(&ok, &deployment),
            Ok("wants replicas > 1, got 1".to_string())
        );
    }

    #[test]
    fn variables_are_a_map() {
        let mut activation = object(json!({"spec": {"replicas": 3}}));
        let replicas = evaluate(&compile("object.spec.replicas").unwrap(), &activation).unwrap();
        let mut vars = HashMap::new();
        vars.insert("replicas".to_string(), replicas);
        activation.set("variables", Value::from(vars));
        assert_eq!(check("variables.replicas > 1", &activation), Ok(true));
    }
}
