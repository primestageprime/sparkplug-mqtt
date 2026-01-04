use crate::payload::payload::Metric;

use super::types::{MetricValue, Timestamp};

/// Create a SparkPlug B metric from a name and typed value.
#[must_use]
pub fn create_metric(name: impl Into<String>, value: MetricValue) -> Metric {
    let (proto_value, datatype) = value.to_proto();
    Metric {
        name: Some(name.into()),
        value: Some(proto_value),
        datatype: Some(datatype),
        timestamp: Some(Timestamp::now().0),
        ..Default::default()
    }
}

/// Build a SparkPlug B payload from a list of metrics and an optional timestamp.
#[must_use]
pub fn create_payload(
    metrics: Vec<Metric>,
    timestamp: Option<Timestamp>,
) -> crate::payload::Payload {
    let timestamp = timestamp.unwrap_or_else(Timestamp::now);

    crate::payload::Payload {
        timestamp: Some(timestamp.0),
        metrics,
        seq: Some(1),
        body: None,
        uuid: None,
    }
}

/// Create a node birth certificate payload (infallible).
#[must_use]
pub fn create_birth_certificate() -> crate::payload::Payload {
    let metric = create_metric("Node Control/Rebirth", MetricValue::Float(0.0));
    crate::payload::Payload {
        timestamp: Some(crate::util::get_current_timestamp()),
        metrics: vec![metric],
        seq: Some(0),
        body: None,
        uuid: None,
    }
}

/// Create a device birth certificate payload (infallible).
#[must_use]
pub fn create_device_birth_certificate() -> crate::payload::Payload {
    let metric = create_metric("Device Control/Rebirth", MetricValue::Float(0.0));
    crate::payload::Payload {
        timestamp: Some(crate::util::get_current_timestamp()),
        metrics: vec![metric],
        seq: Some(0),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::encode_type;
    use crate::payload::payload::metric;

    #[test]
    fn create_metric_float() {
        let m = create_metric("temp", MetricValue::Float(42.5));
        assert_eq!(m.value, Some(metric::Value::DoubleValue(42.5)));
        assert_eq!(m.datatype, Some(encode_type("DOUBLE")));
    }

    #[test]
    fn create_metric_string() {
        let m = create_metric("mode", MetricValue::String("AUTO".to_string()));
        assert_eq!(
            m.value,
            Some(metric::Value::StringValue("AUTO".to_string()))
        );
        assert_eq!(m.datatype, Some(encode_type("STRING")));
    }

    #[test]
    fn create_metric_bool() {
        let m = create_metric("active", MetricValue::Bool(true));
        assert_eq!(m.value, Some(metric::Value::BooleanValue(true)));
        assert_eq!(m.datatype, Some(encode_type("BOOLEAN")));
    }

    #[test]
    fn create_metric_int() {
        let m = create_metric("count", MetricValue::Int(99));
        assert_eq!(m.value, Some(metric::Value::LongValue(99)));
        assert_eq!(m.datatype, Some(encode_type("UINT64")));
    }

    #[test]
    fn create_payload_uses_provided_timestamp() {
        let ts = Timestamp(1_700_000_000_000);
        let p = create_payload(vec![], Some(ts));
        assert_eq!(p.timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn create_payload_generates_timestamp_when_none() {
        let p = create_payload(vec![], None);
        assert!(p.timestamp.expect("should have timestamp") > 0);
    }

    #[test]
    fn create_birth_certificate_is_infallible() {
        let p = create_birth_certificate();
        assert_eq!(p.seq, Some(0));
        assert_eq!(p.metrics.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Protobuf round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn metric_value_float_roundtrip() {
        use prost::Message;
        let metric = create_metric("temperature", MetricValue::Float(42.5));
        let payload = create_payload(vec![metric], Some(Timestamp(1_700_000_000_000)));
        let bytes = payload.encode_to_vec();
        let decoded = crate::payload::decode_payload(&bytes).expect("should decode payload");
        assert_eq!(decoded.metrics.len(), 1);
        let m = &decoded.metrics[0];
        assert_eq!(m.name.as_deref(), Some("temperature"));
        assert!(matches!(
            m.value,
            Some(metric::Value::DoubleValue(v)) if (v - 42.5).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn metric_value_string_roundtrip() {
        use prost::Message;
        let metric = create_metric("vessel_type", MetricValue::String("AUTO".into()));
        let payload = create_payload(vec![metric], Some(Timestamp(1_000)));
        let bytes = payload.encode_to_vec();
        let decoded = crate::payload::decode_payload(&bytes).expect("should decode payload");
        assert!(matches!(
            &decoded.metrics[0].value,
            Some(metric::Value::StringValue(s)) if s == "AUTO"
        ));
    }

    #[test]
    fn metric_value_bool_roundtrip() {
        use prost::Message;
        let metric = create_metric("active", MetricValue::Bool(true));
        let payload = create_payload(vec![metric], Some(Timestamp(1_000)));
        let bytes = payload.encode_to_vec();
        let decoded = crate::payload::decode_payload(&bytes).expect("should decode payload");
        assert!(matches!(
            decoded.metrics[0].value,
            Some(metric::Value::BooleanValue(true))
        ));
    }

    #[test]
    fn metric_value_int_roundtrip() {
        use prost::Message;
        let metric = create_metric("count", MetricValue::Int(99));
        let payload = create_payload(vec![metric], Some(Timestamp(1_000)));
        let bytes = payload.encode_to_vec();
        let decoded = crate::payload::decode_payload(&bytes).expect("should decode payload");
        assert!(matches!(
            decoded.metrics[0].value,
            Some(metric::Value::LongValue(99))
        ));
    }
}
