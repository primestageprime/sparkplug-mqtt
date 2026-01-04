//! Consolidated error types for the sparkplug-mqtt crate.

use std::time::Duration;

/// Errors that can occur when using sparkplug-mqtt.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SparkplugError {
    /// MQTT connection failed (bad URL, auth rejected, etc.)
    #[error("connection failed: {0}")]
    Connection(String),

    /// Timed out waiting for initial MQTT connection.
    #[error("connection timed out after {0:?}")]
    ConnectionTimeout(Duration),

    /// MQTT publish operation failed (connection lost, queue full, etc.)
    #[error("publish failed: {0}")]
    Publish(#[from] rumqttc::ClientError),

    /// Failed to encode a SparkPlug B protobuf payload.
    #[error("payload encode failed: {0}")]
    Encode(#[from] prost::EncodeError),

    /// Failed to decode a SparkPlug B protobuf payload.
    #[error("payload decode failed: {0}")]
    Decode(#[from] prost::DecodeError),

    /// Topic string does not match the SparkPlug B format
    /// `spBv1.0/<group>/<message_type>/<node>[/<device>]`.
    #[error("invalid topic format: {0}")]
    InvalidTopic(String),

    /// The message type segment of a topic is not a known SparkPlug B type.
    #[error("unknown message type: {0}")]
    UnknownMessageType(String),
}
