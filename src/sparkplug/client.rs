use crate::client::{MqttClient, MqttConfig};
use crate::error::SparkplugError;
use crate::payload::payload::Metric;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use super::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_payload,
};
use super::types::{MetricValue, Timestamp};

/// Maximum consecutive event-loop errors before escalating from warn to error.
const MAX_CONSECUTIVE_ERRORS: u32 = 5;

/// A self-contained SparkPlug B client that owns an `AsyncClient` and a
/// background event loop.
///
/// # Example
///
/// ```rust,no_run
/// use sparkplug_mqtt::{MqttConfig, SparkplugClient, MetricValue};
/// use rumqttc::Transport;
///
/// # async fn example() -> Result<(), sparkplug_mqtt::SparkplugError> {
/// let config = MqttConfig {
///     broker_url: "localhost".to_owned(),
///     broker_port: 1883,
///     transport: Transport::Tcp,
///     username: "user".to_owned(),
///     password: "pass".to_owned(),
///     group_id: "MyGroup".to_owned(),
///     node_id: "node1".to_owned(),
///     version: "spBv1.0".to_owned(),
/// };
///
/// let client = SparkplugClient::connect(&config).await?;
/// client.publish_birth("MyGroup", "device1").await?;
/// client
///     .publish_metric("MyGroup", "node1", "device1", "temperature", MetricValue::Float(23.5), 0)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// The `node_id` parameter on `publish_metric`/`publish_metrics` is explicit
/// so a single client can publish on behalf of multiple Sparkplug edge nodes
/// (e.g. one connection, many assets).
pub struct SparkplugClient {
    client: rumqttc::AsyncClient,
    version: Arc<str>,
    node_id: Arc<str>,
    event_loop_handle: tokio::task::JoinHandle<()>,
}

impl Drop for SparkplugClient {
    fn drop(&mut self) {
        self.event_loop_handle.abort();
    }
}

impl SparkplugClient {
    /// Connect to an MQTT broker and return a ready-to-use client.
    ///
    /// Waits up to 5 seconds for the initial connection. The background
    /// event loop reconnects automatically if the connection drops later.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::ConnectionTimeout`] if the broker doesn't
    /// respond within 5 seconds.
    pub async fn connect(config: &MqttConfig) -> Result<Self, SparkplugError> {
        let (mqtt_client, mut eventloop) = MqttClient::new(config.clone());
        let async_client = mqtt_client.into_async_client();

        let version: Arc<str> = Arc::from(config.version.as_str());
        let node_id: Arc<str> = Arc::from(config.node_id.as_str());

        let (tx, rx) = oneshot::channel::<()>();
        let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

        let tx_clone = tx.clone();
        let event_loop_handle = tokio::spawn(async move {
            let mut consecutive_errors: u32 = 0;
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                        tracing::info!("SparkplugClient connected");
                        consecutive_errors = 0;
                        if let Some(tx) = tx_clone.lock().await.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)) => {
                        tracing::warn!("SparkplugClient disconnected");
                    }
                    Ok(_) => {
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            tracing::error!(
                                "SparkplugClient event loop error ({} consecutive): {}",
                                consecutive_errors,
                                e
                            );
                        } else {
                            tracing::warn!(
                                "SparkplugClient event loop error ({} consecutive): {}",
                                consecutive_errors,
                                e
                            );
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        let timeout_duration = Duration::from_secs(5);
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(_) => {
                tracing::info!("SparkplugClient connected successfully");
            }
            Err(_) => {
                event_loop_handle.abort();
                return Err(SparkplugError::ConnectionTimeout(timeout_duration));
            }
        }

        Ok(Self {
            client: async_client,
            version,
            node_id,
            event_loop_handle,
        })
    }

    /// Publish a single metric as a DDATA message on behalf of `node_id`.
    ///
    /// The Sparkplug topic is `{version}/{group_id}/DDATA/{node_id}/{device_id}`,
    /// so the caller controls the edge-node identity per publish. A single
    /// client can therefore publish as many different edge nodes — useful when
    /// a back-end service injects historical or synthetic metrics on behalf of
    /// multiple assets.
    pub async fn publish_metric(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
        metric_name: &str,
        value: MetricValue,
        timestamp_ms: u64,
    ) -> Result<(), SparkplugError> {
        let (proto_value, datatype) = value.to_proto();
        let metric = Metric {
            name: Some(metric_name.to_string()),
            value: Some(proto_value),
            datatype: Some(datatype),
            timestamp: Some(timestamp_ms),
            ..Default::default()
        };

        let payload = create_payload(vec![metric], Some(Timestamp(timestamp_ms)));
        let topic = ddata_topic(&self.version, group_id, node_id, device_id);

        self.client
            .publish(
                &topic,
                rumqttc::QoS::AtLeastOnce,
                false,
                payload.encode_to_vec(),
            )
            .await?;
        Ok(())
    }

    /// Publish a batch of metrics as a single DDATA message on behalf of `node_id`.
    pub async fn publish_metrics(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
        metrics: Vec<(String, MetricValue, u64)>,
    ) -> Result<(), SparkplugError> {
        let proto_metrics: Vec<Metric> = metrics
            .into_iter()
            .map(|(name, value, ts)| {
                let (proto_value, datatype) = value.to_proto();
                Metric {
                    name: Some(name),
                    value: Some(proto_value),
                    datatype: Some(datatype),
                    timestamp: Some(ts),
                    ..Default::default()
                }
            })
            .collect();

        let payload = create_payload(proto_metrics, None);
        let topic = ddata_topic(&self.version, group_id, node_id, device_id);

        self.client
            .publish(
                &topic,
                rumqttc::QoS::AtLeastOnce,
                false,
                payload.encode_to_vec(),
            )
            .await?;
        Ok(())
    }

    /// Publish NBIRTH + DBIRTH for a device.
    pub async fn publish_birth(
        &self,
        group_id: &str,
        device_id: &str,
    ) -> Result<(), SparkplugError> {
        // NBIRTH
        let nbirth_topic = format!("{}/{}/NBIRTH/{}", self.version, group_id, self.node_id);
        let nbirth_payload = create_birth_certificate();
        self.client
            .publish(
                &nbirth_topic,
                rumqttc::QoS::AtLeastOnce,
                false,
                nbirth_payload.encode_to_vec(),
            )
            .await?;

        // DBIRTH
        let dbirth_topic = format!(
            "{}/{}/DBIRTH/{}/{}",
            self.version, group_id, self.node_id, device_id
        );
        let dbirth_payload = create_device_birth_certificate();
        self.client
            .publish(
                &dbirth_topic,
                rumqttc::QoS::AtLeastOnce,
                false,
                dbirth_payload.encode_to_vec(),
            )
            .await?;

        Ok(())
    }
}

/// Build the DDATA topic for a metric publish.
///
/// Extracted as a pure function so callers can assert the topic shape in
/// tests without needing an MQTT broker.
fn ddata_topic(version: &str, group_id: &str, node_id: &str, device_id: &str) -> String {
    format!("{version}/{group_id}/DDATA/{node_id}/{device_id}")
}

#[cfg(test)]
mod tests {
    use super::ddata_topic;

    #[test]
    fn ddata_topic_uses_caller_provided_node_id() {
        let topic = ddata_topic("spBv1.0", "Stax", "xbox7-1", "xbox7-1");
        assert_eq!(topic, "spBv1.0/Stax/DDATA/xbox7-1/xbox7-1");
    }

    #[test]
    fn ddata_topic_distinguishes_node_and_device() {
        // Real deployments usually set node_id == device_id for single-asset
        // nodes, but the topic must carry both independently.
        let topic = ddata_topic("spBv1.0", "Stax", "edge-node-A", "device-1");
        assert_eq!(topic, "spBv1.0/Stax/DDATA/edge-node-A/device-1");
    }
}
