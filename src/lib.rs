//! SparkPlug B MQTT client library for industrial IoT messaging.
//!
//! This crate provides MQTT client functionality with SparkPlug B protocol
//! support, designed for real-time industrial telemetry pipelines.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use sparkplug_mqtt::{MqttConfig, SparkplugClient, MetricValue};
//! use rumqttc::Transport;
//!
//! # async fn example() -> Result<(), sparkplug_mqtt::SparkplugError> {
//! let config = MqttConfig {
//!     broker_url: "localhost".to_owned(),
//!     broker_port: 1883,
//!     transport: Transport::Tcp,
//!     username: "user".to_owned(),
//!     password: "pass".to_owned(),
//!     group_id: "MyGroup".to_owned(),
//!     node_id: "node1".to_owned(),
//!     version: "spBv1.0".to_owned(),
//! };
//!
//! let client = SparkplugClient::connect(&config).await?;
//! client.publish_birth("MyGroup", "device1").await?;
//! client
//!     .publish_metric("MyGroup", "node1", "device1", "temperature", MetricValue::Float(23.5), 0)
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod error;
#[allow(clippy::module_inception)]
pub mod payload;
pub mod sparkplug;
pub mod util;

// Re-export commonly used types
pub use client::{MqttConfig, generate_client_id, mqtt_options, mqtt_parts, tls_transport};
pub use error::SparkplugError;
pub use payload::{
    Metric, Payload, decode_metric_value_to_string, decode_payload, decode_type, encode_type,
    metric,
};
pub use sparkplug::{
    DeliveryTracker, DeviceId, EdgeNodeId, GroupId, Health, HostId, MessageType, MetricValue,
    Namespace, PublishTicket, Shape, SparkplugClient, SparkplugTopic, Timestamp,
    create_birth_certificate, create_device_birth_certificate, create_metric, create_payload,
};

// Re-export rumqttc types for convenience
pub use rumqttc::Transport;
