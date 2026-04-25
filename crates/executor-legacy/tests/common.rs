use strata_core::Value;
use strata_executor_legacy::Strata;

pub(crate) fn create_strata() -> Strata {
    Strata::cache().unwrap()
}

pub(crate) fn event_payload(key: &str, value: Value) -> Value {
    Value::object([(key.to_string(), value)].into_iter().collect())
}
