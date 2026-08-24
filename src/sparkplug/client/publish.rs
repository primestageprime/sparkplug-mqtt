//! Everything that puts a Sparkplug message on the wire.
//!
//! Split from `client.rs` so that file stays under the module size limit.
//! The concern here is one question: given a topic and this client's role,
//! how does the message go out, and does the delivery tracker count it?

use prost::Message;

use crate::error::SparkplugError;
use crate::payload::payload::Metric;

use super::SparkplugClient;
use crate::sparkplug::delivery::DeliveryTracker;
use crate::sparkplug::ids::{DeviceId, EdgeNodeId, GroupId};
use crate::sparkplug::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_payload,
};
use crate::sparkplug::topic::SparkplugTopic;
use crate::sparkplug::types::{MessageType, MetricValue, Timestamp, WireOptions, wire_options};

impl SparkplugClient {
    ///
    /// The Sparkplug topic is `{version}/{group_id}/DDATA/{node_id}/{device_id}`,
    /// so the caller controls the edge-node identity per publish. A single
    /// client can therefore publish as many different edge nodes — useful when
    /// a back-end service injects historical or synthetic metrics on behalf of
    /// multiple assets.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// Do not wrap one publish in `tokio::time::timeout`. Do not put one
    /// publish in a `select!` branch that another branch can cancel. Publish
    /// the whole batch, then bound the wait with the `timeout` argument of
    /// [`Self::flush`].
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
        let topic = self.ddata_topic(group_id, node_id, device_id)?;

        self.publish_message(&topic, payload.encode_to_vec()).await
    }

    /// Publish a batch of metrics as a single DDATA message on behalf of `node_id`.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// Do not wrap one publish in `tokio::time::timeout`. Do not put one
    /// publish in a `select!` branch that another branch can cancel. Publish
    /// the whole batch, then bound the wait with the `timeout` argument of
    /// [`Self::flush`].
    pub async fn publish_metrics(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
        metrics: Vec<(String, MetricValue, u64)>,
    ) -> Result<(), SparkplugError> {
        let payload = create_payload(proto_metrics(metrics), None);
        let topic = self.ddata_topic(group_id, node_id, device_id)?;

        self.publish_message(&topic, payload.encode_to_vec()).await
    }

    /// Publish NBIRTH + DBIRTH for a device.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// Do not wrap one publish in `tokio::time::timeout`. Do not put one
    /// publish in a `select!` branch that another branch can cancel. Publish
    /// the whole batch, then bound the wait with the `timeout` argument of
    /// [`Self::flush`].
    pub async fn publish_birth(
        &self,
        group_id: &str,
        device_id: &str,
    ) -> Result<(), SparkplugError> {
        // Both births name this client's own edge node, while
        // `publish_metric` takes the edge node per call. Where they differ,
        // births and data land on different edge nodes.
        let nbirth_topic = self
            .namespace
            .nbirth(GroupId::new(group_id)?, EdgeNodeId::new(&self.node_id)?);
        let nbirth_payload = create_birth_certificate();
        self.publish_message(&nbirth_topic, nbirth_payload.encode_to_vec())
            .await?;

        let dbirth_topic = self.namespace.dbirth(
            GroupId::new(group_id)?,
            EdgeNodeId::new(&self.node_id)?,
            DeviceId::new(device_id)?,
        );
        let dbirth_payload = create_device_birth_certificate();
        self.publish_message(&dbirth_topic, dbirth_payload.encode_to_vec())
            .await
    }

    /// Publish one metric to a topic the caller already built.
    ///
    /// Prefer this to [`Self::publish_metric`]. The identity segments are
    /// checked when their types are built, so a group, an edge node and a
    /// device cannot be passed in the wrong order.
    ///
    /// ```rust,no_run
    /// # use sparkplug_mqtt::{SparkplugClient, MetricValue, GroupId, EdgeNodeId, DeviceId};
    /// # async fn example(client: &SparkplugClient) -> Result<(), sparkplug_mqtt::SparkplugError> {
    /// let ns = client.namespace();
    /// let topic = ns.ddata(
    ///     GroupId::new("PlantFloor")?,
    ///     EdgeNodeId::new("edge_node_1")?,
    ///     DeviceId::new("pump_3")?,
    /// );
    /// client.publish_metric_to(&topic, "temperature", MetricValue::Float(23.5), 0).await
    /// # }
    /// ```
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe. See [`Self::publish_metric`].
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::Publish`] when the client refuses the
    /// message, and [`SparkplugError::Encode`] when the payload will not
    /// encode. The topic cannot fail — it was checked when it was built.
    pub async fn publish_metric_to(
        &self,
        topic: &SparkplugTopic,
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
        self.publish_message(topic, payload.encode_to_vec()).await
    }

    /// Publish a batch of metrics to a topic the caller already built.
    ///
    /// The batch goes out as one message. See [`Self::publish_metric_to`].
    ///
    /// # Cancel safety
    ///
    /// Not cancel-safe. See [`Self::publish_metric`].
    ///
    /// # Errors
    ///
    /// See [`Self::publish_metric_to`].
    pub async fn publish_metrics_to(
        &self,
        topic: &SparkplugTopic,
        metrics: Vec<(String, MetricValue, u64)>,
    ) -> Result<(), SparkplugError> {
        let payload = create_payload(proto_metrics(metrics), None);
        self.publish_message(topic, payload.encode_to_vec()).await
    }

    /// Build a DDATA topic from unchecked identifiers.
    fn ddata_topic(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
    ) -> Result<SparkplugTopic, SparkplugError> {
        Ok(self.namespace.ddata(
            GroupId::new(group_id)?,
            EdgeNodeId::new(node_id)?,
            DeviceId::new(device_id)?,
        ))
    }

    /// Publish one message the way this client's role requires.
    ///
    /// The topic names its own message type, so this is the one place that
    /// reads the QoS, the retain flag, and whether the delivery tracker may
    /// count the publish. An untracked message never reaches the tracker,
    /// which is what stops a QoS 0 publish from holding a later flush open.
    ///
    /// A tracked publish is not cancel-safe. A cancelled call leaves
    /// `unacked` one too high, so every later flush reports `FlushTimeout`.
    async fn publish_message(
        &self,
        topic: &SparkplugTopic,
        payload: Vec<u8>,
    ) -> Result<(), SparkplugError> {
        let options = wire_options(self.role, topic.message_type());

        publish_with_options(
            &self.delivery,
            options,
            self.client
                .publish(topic.to_string(), options.qos, options.retain, payload),
        )
        .await
    }

    /// Whether [`Self::flush`] can confirm what this client publishes.
    ///
    /// A [`Role::Publisher`] client publishes at QoS 1, so the broker
    /// acknowledges every message and `flush` reports what a disconnect
    /// discarded. A [`Role::EdgeNode`] client publishes at the QoS the
    /// specification fixes, which is QoS 0 for everything this crate can
    /// send today — the broker acknowledges none of it, so `flush` returns
    /// `Ok` without confirming anything.
    ///
    /// Assert this once at startup before gating a durable write on
    /// [`Self::flush`]. A `false` reading means the confirmation a caller
    /// wants is not available in this role, not that a publish failed.
    #[must_use]
    pub fn tracks_delivery(&self) -> bool {
        wire_options(self.role, MessageType::DDATA).tracked
    }
}

/// Map caller metrics onto their protobuf form.
pub(super) fn proto_metrics(metrics: Vec<(String, MetricValue, u64)>) -> Vec<Metric> {
    metrics
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
        .collect()
}

/// Hand one message to the client, counting it only when the broker will
/// acknowledge it.
///
/// Split out as a free function so a test can drive both branches with no
/// broker, the same way [`record_around_publish`] is.
pub(super) async fn publish_with_options(
    delivery: &DeliveryTracker,
    options: WireOptions,
    publish: impl Future<Output = Result<(), rumqttc::ClientError>>,
) -> Result<(), SparkplugError> {
    match options.tracked {
        true => record_around_publish(delivery, publish).await,
        false => publish.await.map_err(SparkplugError::from),
    }
}

/// Count a publish, hand it to the client, then roll the count back when
/// the client refuses it.
///
/// The count must rise before `publish` runs. `publish_metric` and `flush`
/// both take `&self`, so a task that shares this client can flush while a
/// publish is still in flight. A disconnect in that window would record no
/// loss, and an early PubAck would leave a count that never clears.
/// Over-counting only delays a flush, so it is the safe direction.
///
/// The rollback carries the ticket from the matching `record_publish`, so a
/// refusal can never cancel a count that a disconnect cleared or that
/// belongs to a publish from another task.
///
/// Split out as a free function so a test can drive the ordering without a
/// broker.
pub(super) async fn record_around_publish(
    delivery: &DeliveryTracker,
    publish: impl Future<Output = Result<(), rumqttc::ClientError>>,
) -> Result<(), SparkplugError> {
    let ticket = delivery.record_publish();
    match publish.await {
        Ok(()) => Ok(()),
        Err(error) => {
            delivery.record_publish_failed(ticket);
            Err(SparkplugError::from(error))
        }
    }
}
