use prost::Message;

include!(concat!(env!("OUT_DIR"), "/_.rs"));

// Re-export the generated protobuf types
pub use payload::*;

// Define the input structures
pub fn decode_payload(bytes: &[u8]) -> Result<Payload, prost::DecodeError> {
    Payload::decode(bytes)
}

#[must_use]
pub fn encode_type(type_str: &str) -> u32 {
    match type_str.to_uppercase().as_str() {
        "INT8" => 1,
        "INT16" => 2,
        "INT32" | "INT" => 3,
        "INT64" | "LONG" => 4,
        "UINT8" => 5,
        "UINT16" => 6,
        "UINT32" => 7,
        "UINT64" => 8,
        "FLOAT" => 9,
        "DOUBLE" => 10,
        "BOOLEAN" => 11,
        "STRING" => 12,
        "DATETIME" => 13,
        "TEXT" => 14,
        "UUID" => 15,
        "DATASET" => 16,
        "BYTES" => 17,
        "FILE" => 18,
        "TEMPLATE" => 19,
        "PROPERTYSET" => 20,
        "PROPERTYSETLIST" => 21,
        _ => 0,
    }
}

/// Decode SparkplugB datatype number to human-readable name
///
/// # Arguments
/// * `datatype` - SparkplugB datatype number
///
/// # Returns
/// Human-readable string name for the datatype
#[must_use]
pub fn decode_type(datatype: u32) -> &'static str {
    match datatype {
        0 => "Unknown",
        1 => "Int8",
        2 => "Int16",
        3 => "Int32",
        4 => "Int64",
        5 => "UInt8",
        6 => "UInt16",
        7 => "UInt32",
        8 => "UInt64",
        9 => "Float32",
        10 => "Float64",
        11 => "Boolean",
        12 => "String",
        13 => "DateTime",
        14 => "Text",
        15 => "UUID",
        16 => "DataSet",
        17 => "Bytes",
        18 => "File",
        19 => "Template",
        20 => "PropertySet",
        21 => "PropertySetList",
        _ => "Unknown",
    }
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

    #[test]
    fn encode_type_known_types() {
        assert_eq!(encode_type("DOUBLE"), 10);
        assert_eq!(encode_type("STRING"), 12);
        assert_eq!(encode_type("BOOLEAN"), 11);
        assert_eq!(encode_type("INT64"), 4);
        assert_eq!(encode_type("FLOAT"), 9);
        assert_eq!(encode_type("INT32"), 3);
    }

    #[test]
    fn encode_type_unknown_returns_zero() {
        assert_eq!(encode_type("GARBAGE"), 0);
        assert_eq!(encode_type(""), 0);
    }

    #[test]
    fn decode_type_known_types() {
        assert_eq!(decode_type(10), "Float64");
        assert_eq!(decode_type(12), "String");
        assert_eq!(decode_type(11), "Boolean");
        assert_eq!(decode_type(4), "Int64");
        assert_eq!(decode_type(9), "Float32");
    }

    #[test]
    fn decode_type_unknown_returns_unknown() {
        assert_eq!(decode_type(0), "Unknown");
        assert_eq!(decode_type(999), "Unknown");
    }
}
