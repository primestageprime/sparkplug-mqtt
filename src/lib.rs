//! SparkPlug B MQTT client library for industrial IoT messaging.
//!
//! This crate provides MQTT client functionality with SparkPlug B protocol
//! support, designed for real-time industrial telemetry pipelines.
//!
//! # Quick start
//!
//! A client connects in one of two roles. A publisher client takes the edge
//! node of each publish per call. An edge node client names one edge node at
//! connect, and it is the client that announces a birth.
//!
//! ```rust,no_run
//! use sparkplug_mqtt::{
//!     DEFAULT_CONNECT_TIMEOUT, DeviceId, EdgeNode, EdgeNodeId, GroupId, MetricValue, MqttConfig,
//!     SparkplugClient,
//! };
//! use rumqttc::Transport;
//!
//! # async fn example() -> Result<(), sparkplug_mqtt::SparkplugError> {
//! let config = MqttConfig {
//!     broker_url: "localhost".to_owned(),
//!     broker_port: 1883,
//!     transport: Transport::Tcp,
//!     username: "user".to_owned(),
//!     password: "pass".to_owned(),
//!     // `None` takes a generated client id. The client id names this MQTT
//!     // connection, and it names no edge node.
//!     client_id: None,
//!     version: "spBv1.0".to_owned(),
//! };
//!
//! // This client speaks for one edge node: the group and the node below.
//! let edge_node = EdgeNode::new(GroupId::new("MyGroup")?, EdgeNodeId::new("node1")?);
//! let client =
//!     SparkplugClient::connect_as_edge_node(&config, edge_node, DEFAULT_CONNECT_TIMEOUT).await?;
//!
//! // The birth names that edge node, so the births and the data below share
//! // one seq count.
//! client.publish_birth(DeviceId::new("device1")?).await?;
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

// Re-export commonly used types
pub use client::{MqttConfig, generate_client_id, mqtt_options, mqtt_parts, tls_transport};
pub use error::SparkplugError;
pub use payload::{Metric, Payload, decode_payload, metric};
pub use sparkplug::{
    DEFAULT_CONNECT_TIMEOUT, DataType, DeliveryTracker, DeviceId, EdgeNode, EdgeNodeId, GroupId,
    Health, HostId, MessageType, MetricValue, Namespace, PublishTicket, Role, Shape,
    SparkplugClient, SparkplugTopic, Timestamp,
};

// Re-export rumqttc types for convenience
pub use rumqttc::Transport;
