use crate::client::MqttClient;
use crate::error::SparkplugError;
use crate::payload::payload::{Metric, metric};
use prost::Message;
use std::sync::Arc;

use super::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_payload,
};
use super::types::TimestampToMetrics;

// ---------------------------------------------------------------------------
// SparkplugPublisher (legacy)
// ---------------------------------------------------------------------------

#[deprecated(note = "use SparkplugClient instead")]
#[derive(Clone)]
pub struct SparkplugPublisher {
    mqtt_client: Arc<MqttClient>,
}

#[allow(deprecated)]
impl SparkplugPublisher {
    pub fn new(mqtt_client: Arc<MqttClient>) -> Self {
        Self { mqtt_client }
    }

    pub async fn publish_metrics(
        &self,
        group_id: &str,
        asset_id: &str,
        metrics_data: Vec<TimestampToMetrics>,
    ) -> Result<(), SparkplugError> {
        for metric_frame in &metrics_data {
            let data_topic = format!(
                "{}/{}/DDATA/{}/{}",
                self.mqtt_client.version(),
                group_id,
                asset_id,
                asset_id,
            );

            let metrics: Vec<Metric> = metric_frame
                .metrics()
                .iter()
                .map(|m| Metric {
                    name: Some(m.name.clone().unwrap_or_default()),
                    value: m.value.clone(),
                    datatype: m.datatype,
                    timestamp: Some(metric_frame.timestamp().0),
                    ..Default::default()
                })
                .collect();

            let data_payload =
                create_payload(metrics, Some(metric_frame.timestamp())).encode_to_vec();

            self.mqtt_client.publish(&data_topic, &data_payload).await?;
        }
        Ok(())
    }

    pub async fn publish_births(
        &self,
        group_id: &str,
        asset_id: &str,
    ) -> Result<(), SparkplugError> {
        tracing::debug!("Publishing birth certificates for asset {}:", asset_id);

        // Send Node Birth
        let node_birth_topic = format!(
            "{}/{}/NBIRTH/{}",
            self.mqtt_client.version(),
            group_id,
            asset_id,
        );
        let node_birth_payload = create_birth_certificate().encode_to_vec();
        tracing::debug!("  -> Sending NBIRTH on topic: {}", node_birth_topic);
        self.mqtt_client
            .publish(&node_birth_topic, &node_birth_payload)
            .await?;

        // Send Device Birth
        let device_birth_topic = format!(
            "{}/{}/DBIRTH/{}/{}",
            self.mqtt_client.version(),
            group_id,
            asset_id,
            asset_id,
        );
        let device_birth_payload = create_device_birth_certificate().encode_to_vec();
        tracing::debug!("  -> Sending DBIRTH on topic: {}", device_birth_topic);
        self.mqtt_client
            .publish(&device_birth_topic, &device_birth_payload)
            .await?;

        tracing::debug!(
            "Birth certificates published successfully for asset {}",
            asset_id,
        );
        Ok(())
    }

    pub fn handle_command(&self, topic: &str, payload: &[u8], asset_id: &str) -> bool {
        // Extract node ID from topic
        let topic_parts: Vec<&str> = topic.split('/').collect();
        if topic_parts.len() < 4 {
            return false;
        }

        // Check if this command is for our node
        if topic_parts[3] != asset_id {
            return false;
        }

        if let Ok(payload) = crate::payload::decode_payload(payload) {
            payload.metrics.iter().any(|m| {
                m.name.as_deref() == Some("Node Control/Rebirth")
                    && m.value == Some(metric::Value::FloatValue(1.0))
            })
        } else {
            false
        }
    }
}
