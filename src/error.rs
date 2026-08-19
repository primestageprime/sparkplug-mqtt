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

    /// A disconnect discarded publishes the broker never acknowledged.
    ///
    /// Sparkplug requires `clean_session = true`, so the broker keeps no
    /// session state and the client cannot resend them. The caller must
    /// decide whether to publish them again.
    #[error("{count} publish(es) were discarded when the MQTT connection dropped")]
    PublishLost { count: u32 },

    /// The client refused the DISCONNECT request that `shutdown` sent.
    ///
    /// The event loop is already gone, so it cannot write the packet. Every
    /// publish still reached the broker, because `shutdown` flushes first.
    #[error("disconnect request failed: {0}")]
    Disconnect(#[source] rumqttc::ClientError),

    /// `shutdown` could not confirm that the DISCONNECT packet went out.
    ///
    /// Every publish reached the broker, so nothing is lost. The event loop
    /// stops anyway, so no task leaks. Expect this when the broker is
    /// already unreachable.
    #[error("shutdown could not confirm the DISCONNECT packet within {after:?}")]
    ShutdownTimeout { after: Duration },

    /// `flush` gave up waiting for the broker to acknowledge.
    #[error("flush timed out after {after:?} with {unacked} publish(es) unacknowledged")]
    FlushTimeout { after: Duration, unacked: u32 },
}
