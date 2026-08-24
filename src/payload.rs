use prost::Message;

include!(concat!(env!("OUT_DIR"), "/_.rs"));

// Re-export the generated protobuf types
pub use payload::*;

// Define the input structures
pub fn decode_payload(bytes: &[u8]) -> Result<Payload, prost::DecodeError> {
    Payload::decode(bytes)
}

impl Metric {
    /// Extract numeric value from a SparkplugB metric for plotting
    ///
    /// # Arguments
    /// * `self` - The SparkplugB metric to extract value from
    ///
    /// # Returns
    /// `Some(f64)` if the metric contains a numeric value, `None` otherwise
    #[must_use]
    pub fn numeric_value(&self) -> Option<f64> {
        if let Some(ref value) = self.value {
            match value {
                metric::Value::IntValue(v) => Some(*v as f64),
                metric::Value::LongValue(v) => Some(*v as f64),
                metric::Value::FloatValue(v) => Some(*v as f64),
                metric::Value::DoubleValue(v) => Some(*v),
                metric::Value::BooleanValue(v) => Some(if *v { 1.0 } else { 0.0 }),
                _ => None, // String and other types can't be plotted as numbers
            }
        } else {
            None
        }
    }

    /// Format a SparkplugB metric value for display
    ///
    /// # Arguments
    /// * `self` - The metric to format
    ///
    /// # Returns
    /// Human-readable string representation of the metric value
    #[must_use]
    pub fn format_value(&self) -> String {
        if let Some(ref value) = self.value {
            match value {
                metric::Value::IntValue(v) => format!("{}", v),
                metric::Value::LongValue(v) => format!("{}", v),
                metric::Value::FloatValue(v) => format!("{}", v),
                metric::Value::DoubleValue(v) => format!("{}", v),
                metric::Value::BooleanValue(v) => format!("{}", v),
                metric::Value::StringValue(v) => format!("\"{}\"", v),
                metric::Value::BytesValue(v) => format!("{:?}", v),
                _ => "Unknown value type".to_string(),
            }
        } else {
            "No value".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric_with(value: metric::Value) -> Metric {
        Metric {
            value: Some(value),
            ..Default::default()
        }
    }

    #[test]
    fn numeric_value_reads_every_number_variant() {
        assert_eq!(
            metric_with(metric::Value::IntValue(7)).numeric_value(),
            Some(7.0)
        );
        assert_eq!(
            metric_with(metric::Value::LongValue(7)).numeric_value(),
            Some(7.0)
        );
        assert_eq!(
            metric_with(metric::Value::FloatValue(7.5)).numeric_value(),
            Some(7.5)
        );
        assert_eq!(
            metric_with(metric::Value::DoubleValue(7.5)).numeric_value(),
            Some(7.5)
        );
    }

    #[test]
    fn numeric_value_reads_a_boolean_as_one_or_zero() {
        assert_eq!(
            metric_with(metric::Value::BooleanValue(true)).numeric_value(),
            Some(1.0)
        );
        assert_eq!(
            metric_with(metric::Value::BooleanValue(false)).numeric_value(),
            Some(0.0)
        );
    }

    #[test]
    fn numeric_value_rejects_what_it_cannot_plot() {
        // amygdala-rs gates on this None to fall through to its string path,
        // so a String that returned a number would change what it stores.
        assert_eq!(
            metric_with(metric::Value::StringValue("AUTO".to_owned())).numeric_value(),
            None
        );
        assert_eq!(
            metric_with(metric::Value::BytesValue(vec![1, 2])).numeric_value(),
            None
        );
        assert_eq!(Metric::default().numeric_value(), None);
    }

    #[test]
    fn format_value_quotes_a_string_and_leaves_a_number_bare() {
        assert_eq!(
            metric_with(metric::Value::StringValue("AUTO".to_owned())).format_value(),
            "\"AUTO\""
        );
        assert_eq!(
            metric_with(metric::Value::DoubleValue(7.5)).format_value(),
            "7.5"
        );
        assert_eq!(
            metric_with(metric::Value::BooleanValue(true)).format_value(),
            "true"
        );
    }

    #[test]
    fn format_value_says_so_when_there_is_no_value() {
        assert_eq!(Metric::default().format_value(), "No value");
    }

    #[test]
    fn decode_payload_reads_back_what_was_encoded() {
        let payload = Payload {
            timestamp: Some(1_700_000_000_000),
            metrics: vec![Metric {
                name: Some("temperature".to_owned()),
                value: Some(metric::Value::DoubleValue(42.5)),
                datatype: Some(10),
                ..Default::default()
            }],
            ..Default::default()
        };
        let decoded = decode_payload(&payload.encode_to_vec()).expect("should decode payload");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn decode_payload_rejects_bytes_that_are_not_a_payload() {
        assert!(decode_payload(&[0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
