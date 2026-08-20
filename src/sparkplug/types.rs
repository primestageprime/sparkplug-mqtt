use crate::error::SparkplugError;
use crate::payload::encode_type;
use crate::payload::payload::metric;
use std::str::FromStr;

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
// Shape / MessageType
// ---------------------------------------------------------------------------

/// Which segment layout a topic uses.
///
/// The message type fixes the shape, so a caller never chooses one. Match on
/// [`super::SparkplugTopic::shape`] to learn which identity accessors carry a
/// value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Shape {
    /// `{namespace}/{group}/{type}/{node}`
    Node,
    /// `{namespace}/{group}/{type}/{node}/{device}`
    Device,
    /// `{namespace}/STATE/{host}`
    Host,
}

/// SparkplugB message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Which topic shape this message type requires.
    #[must_use]
    pub fn shape(&self) -> Shape {
        match self {
            MessageType::NBIRTH | MessageType::NDATA | MessageType::NDEATH | MessageType::NCMD => {
                Shape::Node
            }
            MessageType::DBIRTH | MessageType::DDATA | MessageType::DDEATH | MessageType::DCMD => {
                Shape::Device
            }
            MessageType::STATE => Shape::Host,
        }
    }

    /// The QoS the Sparkplug specification fixes for this message type.
    ///
    /// This states the specification. It does not state what this crate
    /// sends — a client that publishes on behalf of edge nodes it does not
    /// own may keep QoS 1 so its callers can confirm delivery.
    #[must_use]
    pub fn spec_qos(&self) -> rumqttc::QoS {
        match self {
            MessageType::NDEATH | MessageType::STATE => rumqttc::QoS::AtLeastOnce,
            _ => rumqttc::QoS::AtMostOnce,
        }
    }

    /// Whether the Sparkplug specification retains this message type.
    ///
    /// Only STATE is retained, so a host application that connects later
    /// still learns whether the primary host is online.
    #[must_use]
    pub fn spec_retain(&self) -> bool {
        matches!(self, MessageType::STATE)
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
// Timestamp
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp(pub u64);

impl Timestamp {
    #[must_use]
    pub fn now() -> Self {
        Self(crate::util::get_current_timestamp())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [MessageType; 9] = [
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

    #[test]
    fn message_type_from_str() {
        let mt: MessageType = "DDATA".parse().expect("should parse DDATA");
        assert_eq!(mt, MessageType::DDATA);
    }

    #[test]
    fn message_type_roundtrip() {
        for mt in &ALL {
            let parsed: MessageType = mt.as_str().parse().expect("roundtrip should succeed");
            assert_eq!(&parsed, mt);
        }
    }

    #[test]
    fn node_messages_take_the_node_shape() {
        for mt in [
            MessageType::NBIRTH,
            MessageType::NDATA,
            MessageType::NDEATH,
            MessageType::NCMD,
        ] {
            assert_eq!(mt.shape(), Shape::Node, "{mt} is a node message");
        }
    }

    #[test]
    fn device_messages_take_the_device_shape() {
        for mt in [
            MessageType::DBIRTH,
            MessageType::DDATA,
            MessageType::DDEATH,
            MessageType::DCMD,
        ] {
            assert_eq!(mt.shape(), Shape::Device, "{mt} is a device message");
        }
    }

    #[test]
    fn state_takes_the_host_shape() {
        assert_eq!(MessageType::STATE.shape(), Shape::Host);
    }

    #[test]
    fn only_ndeath_and_state_use_qos_one() {
        for mt in ALL {
            let expected = match mt {
                MessageType::NDEATH | MessageType::STATE => rumqttc::QoS::AtLeastOnce,
                _ => rumqttc::QoS::AtMostOnce,
            };
            assert_eq!(mt.spec_qos(), expected, "{mt} carries the wrong spec QoS");
        }
    }

    #[test]
    fn only_state_is_retained() {
        for mt in ALL {
            assert_eq!(
                mt.spec_retain(),
                mt == MessageType::STATE,
                "{mt} carries the wrong spec retain flag"
            );
        }
    }
}
