use crate::payload::payload::Metric;

use super::types::{MetricValue, Timestamp};

/// Create a SparkPlug B metric from a name, a typed value and an instant.
///
/// The caller states the instant. This builder reads no clock, so a test can
/// pin what it expects.
#[must_use]
pub fn create_metric(name: impl Into<String>, value: MetricValue, timestamp: Timestamp) -> Metric {
    let (proto_value, datatype) = value.to_proto();
    Metric {
        name: Some(name.into()),
        value: Some(proto_value),
        datatype: Some(datatype.code()),
        timestamp: Some(timestamp.0),
        ..Default::default()
    }
}

/// Build a SparkPlug B payload from a list of metrics and an instant.
#[must_use]
pub fn create_payload(metrics: Vec<Metric>, timestamp: Timestamp) -> crate::payload::Payload {
    crate::payload::Payload {
        timestamp: Some(timestamp.0),
        metrics,
        seq: Some(1),
        body: None,
        uuid: None,
    }
}

/// Create a node birth certificate payload (infallible).
///
/// The payload and its one metric carry the same instant.
#[must_use]
pub fn create_birth_certificate(timestamp: Timestamp) -> crate::payload::Payload {
    let metric = create_metric("Node Control/Rebirth", MetricValue::Float(0.0), timestamp);
    crate::payload::Payload {
        timestamp: Some(timestamp.0),
        metrics: vec![metric],
        seq: Some(0),
        body: None,
        uuid: None,
    }
}

/// Create a device birth certificate payload (infallible).
///
/// The payload and its one metric carry the same instant.
#[must_use]
pub fn create_device_birth_certificate(timestamp: Timestamp) -> crate::payload::Payload {
    let metric = create_metric("Device Control/Rebirth", MetricValue::Float(0.0), timestamp);
    crate::payload::Payload {
        timestamp: Some(timestamp.0),
        metrics: vec![metric],
        seq: Some(0),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::payload::metric;
    use crate::sparkplug::DataType;

    const AT: Timestamp = Timestamp(1_700_000_000_000);

    #[test]
    fn create_metric_float() {
        let m = create_metric("temp", MetricValue::Float(42.5), AT);
        assert_eq!(m.value, Some(metric::Value::DoubleValue(42.5)));
        assert_eq!(m.datatype, Some(DataType::Double.code()));
    }

    #[test]
    fn create_metric_string() {
        let m = create_metric("mode", MetricValue::String("AUTO".to_string()), AT);
        assert_eq!(
            m.value,
            Some(metric::Value::StringValue("AUTO".to_string()))
        );
        assert_eq!(m.datatype, Some(DataType::String.code()));
    }

    #[test]
    fn create_metric_bool() {
        let m = create_metric("active", MetricValue::Bool(true), AT);
        assert_eq!(m.value, Some(metric::Value::BooleanValue(true)));
        assert_eq!(m.datatype, Some(DataType::Boolean.code()));
    }

    #[test]
    fn create_metric_int() {
        let m = create_metric("count", MetricValue::Int(99), AT);
        assert_eq!(m.value, Some(metric::Value::LongValue(99)));
        assert_eq!(m.datatype, Some(DataType::UInt64.code()));
    }

    #[test]
    fn create_metric_stamps_the_instant_the_caller_states() {
        // Before this took a Timestamp it read the clock itself, so this
        // test could only assert the value was above zero.
        assert_eq!(
            create_metric("temp", MetricValue::Float(1.0), AT).timestamp,
            Some(1_700_000_000_000)
        );
    }

    #[test]
    fn create_payload_uses_the_instant_the_caller_states() {
        assert_eq!(
            create_payload(vec![], AT).timestamp,
            Some(1_700_000_000_000)
        );
    }

    #[test]
    fn a_birth_certificate_stamps_its_payload_and_its_metric_alike() {
        // Two clock reads used to build one birth, so the payload and its
        // metric could land on different milliseconds.
        for payload in [
            create_birth_certificate(AT),
            create_device_birth_certificate(AT),
        ] {
            assert_eq!(payload.timestamp, Some(AT.0));
            assert_eq!(payload.metrics[0].timestamp, Some(AT.0));
        }
    }

    #[test]
    fn create_birth_certificate_is_infallible() {
        let p = create_birth_certificate(AT);
        assert_eq!(p.seq, Some(0));
        assert_eq!(p.metrics.len(), 1);
    }

    #[test]
    fn timestamp_now_reads_a_clock_after_the_epoch() {
        // `util::get_current_timestamp` is gone; `Timestamp::now` is the one
        // route to the clock.
        assert!(Timestamp::now() > Timestamp(1_700_000_000_000));
    }

    // -----------------------------------------------------------------------
    // Protobuf round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn metric_value_float_roundtrip() {
        use prost::Message;
        let metric = create_metric("temperature", MetricValue::Float(42.5), AT);
        let payload = create_payload(vec![metric], AT);
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
        let metric = create_metric("vessel_type", MetricValue::String("AUTO".into()), AT);
        let payload = create_payload(vec![metric], AT);
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
        let metric = create_metric("active", MetricValue::Bool(true), AT);
        let payload = create_payload(vec![metric], AT);
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
        let metric = create_metric("count", MetricValue::Int(99), AT);
        let payload = create_payload(vec![metric], AT);
        let bytes = payload.encode_to_vec();
        let decoded = crate::payload::decode_payload(&bytes).expect("should decode payload");
        assert!(matches!(
            decoded.metrics[0].value,
            Some(metric::Value::LongValue(99))
        ));
    }
}
