use crate::error::SparkplugError;
use crate::payload::encode_type;
use crate::payload::payload::metric;
use std::str::FromStr;

pub const VERSION: &str = "spBv1.0";

// ---------------------------------------------------------------------------
// MetricValue
// ---------------------------------------------------------------------------

/// Type-safe metric value for SparkPlug B payloads.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MetricValue {
    Float(f64),
    String(String),
    Bool(bool),
    Int(u64),
}

impl MetricValue {
    /// Convert to the protobuf `metric::Value` and its datatype code.
    #[must_use]
    pub(super) fn to_proto(&self) -> (metric::Value, u32) {
        match self {
            MetricValue::Float(v) => (metric::Value::DoubleValue(*v), encode_type("DOUBLE")),
            MetricValue::String(v) => {
                (metric::Value::StringValue(v.clone()), encode_type("STRING"))
            }
            MetricValue::Bool(v) => (metric::Value::BooleanValue(*v), encode_type("BOOLEAN")),
            MetricValue::Int(v) => (metric::Value::LongValue(*v), encode_type("UINT64")),
        }
    }
}

// ---------------------------------------------------------------------------
// SparkplugTopic / MessageType
// ---------------------------------------------------------------------------

/// Parsed SparkplugB topic information
#[derive(Debug, Clone, PartialEq)]
pub struct SparkplugTopic {
    pub group_id: String,
    pub node_id: String,
    pub device_id: Option<String>,
    pub message_type: MessageType,
}

/// SparkplugB message types
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MessageType {
    NBIRTH, // Node birth
    NDATA,  // Node data
    NDEATH, // Node death
    DBIRTH, // Device birth
    DDATA,  // Device data
    DDEATH, // Device death
    NCMD,   // Node command
    DCMD,   // Device command
    STATE,  // State message
}

impl MessageType {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::NBIRTH => "NBIRTH",
            MessageType::NDATA => "NDATA",
            MessageType::NDEATH => "NDEATH",
            MessageType::DBIRTH => "DBIRTH",
            MessageType::DDATA => "DDATA",
            MessageType::DDEATH => "DDEATH",
            MessageType::NCMD => "NCMD",
            MessageType::DCMD => "DCMD",
            MessageType::STATE => "STATE",
        }
    }
}

impl FromStr for MessageType {
    type Err = SparkplugError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NBIRTH" => Ok(MessageType::NBIRTH),
            "NDATA" => Ok(MessageType::NDATA),
            "NDEATH" => Ok(MessageType::NDEATH),
            "DBIRTH" => Ok(MessageType::DBIRTH),
            "DDATA" => Ok(MessageType::DDATA),
            "DDEATH" => Ok(MessageType::DDEATH),
            "NCMD" => Ok(MessageType::NCMD),
            "DCMD" => Ok(MessageType::DCMD),
            "STATE" => Ok(MessageType::STATE),
            other => Err(SparkplugError::UnknownMessageType(other.to_string())),
        }
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Timestamp / TimestampToMetrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp(pub u64);

impl Timestamp {
    #[must_use]
    pub fn now() -> Self {
        Self(crate::util::get_current_timestamp())
    }
}

#[derive(Debug, Clone)]
pub struct TimestampToMetrics {
    timestamp: Timestamp,
    metrics: Vec<crate::payload::payload::Metric>,
}

impl TimestampToMetrics {
    pub fn new(timestamp: Timestamp, metrics: Vec<crate::payload::payload::Metric>) -> Self {
        Self { timestamp, metrics }
    }

    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    #[must_use]
    pub fn metrics(&self) -> &[crate::payload::payload::Metric] {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_from_str() {
        let mt: MessageType = "DDATA".parse().expect("should parse DDATA");
        assert_eq!(mt, MessageType::DDATA);
    }

    #[test]
    fn message_type_roundtrip() {
        let all = [
            MessageType::NBIRTH,
            MessageType::NDATA,
            MessageType::NDEATH,
            MessageType::DBIRTH,
            MessageType::DDATA,
            MessageType::DDEATH,
            MessageType::NCMD,
            MessageType::DCMD,
            MessageType::STATE,
        ];
        for mt in &all {
            let s = mt.as_str();
            let parsed: MessageType = s.parse().expect("roundtrip should succeed");
            assert_eq!(&parsed, mt);
        }
    }
}
