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
use crate::sparkplug::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_metric, create_payload,
};
use crate::sparkplug::seq::SeqReservation;
use crate::sparkplug::topic::SparkplugTopic;
use crate::sparkplug::topic::ids::{DeviceId, EdgeNodeId, GroupId};
use crate::sparkplug::types::{MessageType, MetricValue, Timestamp, WireOptions, wire_options};

impl SparkplugClient {
    /// Publish a single metric as a DDATA message for the edge node the
    /// caller names.
    ///
    /// The Sparkplug topic is `{version}/{group_id}/DDATA/{node_id}/{device_id}`,
    /// so the caller controls the edge-node identity per publish. A single
    /// client can therefore publish as many different edge nodes — useful when
    /// a back-end service injects historical or synthetic metrics on behalf of
    /// multiple assets.
    ///
    /// # Cancellation
    ///
    /// This draws the `seq` of the edge node under that edge node's gate,
    /// and holds the gate until the client takes the message. A cancelled
    /// call drops the reservation, which writes the previous number back
    /// under the gate, and releases the delivery count of a tracked
    /// publish. So a cancelled publish leaves no gap for a host application
    /// to read, and no count to hold a later [`Self::flush`] open.
    ///
    /// One window stays open. The client can take the message out of the
    /// publish future in the same instant that the caller cancels the call.
    /// That message reaches the broker, and the next message for the edge
    /// node carries its number a second time.
    pub async fn publish_metric(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
        metric_name: &str,
        value: MetricValue,
        timestamp_ms: u64,
    ) -> Result<(), SparkplugError> {
        let metric = create_metric(metric_name, value, Timestamp(timestamp_ms));
        let topic = self.ddata_topic(group_id, node_id, device_id)?;

        self.publish_metrics_on(&topic, vec![metric], Timestamp(timestamp_ms))
            .await
    }

    /// Publish a batch of metrics as one DDATA message for the edge node
    /// the caller names.
    ///
    /// # Cancellation
    ///
    /// A cancelled call restores the `seq` of its edge node and releases the
    /// delivery count. See [`Self::publish_metric`].
    pub async fn publish_metrics(
        &self,
        group_id: &str,
        node_id: &str,
        device_id: &str,
        metrics: Vec<(String, MetricValue, u64)>,
    ) -> Result<(), SparkplugError> {
        let topic = self.ddata_topic(group_id, node_id, device_id)?;

        self.publish_metrics_on(&topic, proto_metrics(metrics), Timestamp::now())
            .await
    }

    /// Publish NBIRTH + DBIRTH for a device of this client's edge node.
    ///
    /// The group and the edge node come from the identity this client took
    /// at connect, and nothing can change them afterwards. So the births and
    /// the data of that edge node name one edge node and share one `seq`
    /// count.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::NotAnEdgeNode`] when this client connected
    /// as a publisher. Such a client owns no edge node, so it has none to
    /// announce — connect through
    /// [`SparkplugClient::connect_as_edge_node`] instead.
    ///
    /// Returns [`SparkplugError::Publish`] when the client refuses a
    /// message, and [`SparkplugError::Encode`] when a payload will not
    /// encode.
    ///
    /// # Cancellation
    ///
    /// One reservation covers both births, so a cancelled call and a DBIRTH
    /// the client refuses both leave the count where the NBIRTH found it.
    /// See [`Self::publish_metric`].
    pub async fn publish_birth(&self, device: DeviceId<'_>) -> Result<(), SparkplugError> {
        let edge_node = self.edge_node().ok_or(SparkplugError::NotAnEdgeNode)?;

        // One birth event, so both payloads carry one instant.
        let at = Timestamp::now();

        // One reservation covers both births. It reads the message type of
        // the NBIRTH topic and sets the count of its edge node back to 0, so
        // the NBIRTH carries 0 and the DBIRTH beside it carries 1. The
        // reservation holds the gate of that edge node across both
        // publishes, so no other publish for it takes a number between them.
        let nbirth_topic = self
            .namespace
            .nbirth(edge_node.group(), edge_node.edge_node_id());
        let mut seq = self.reserve_seq(&nbirth_topic).await?;

        let nbirth_payload = create_birth_certificate(at, *seq);
        self.publish_message(&nbirth_topic, nbirth_payload.encode_to_vec())
            .await?;

        let dbirth_topic =
            self.namespace
                .dbirth(edge_node.group(), edge_node.edge_node_id(), device);
        let dbirth_payload = create_device_birth_certificate(at, seq.next());
        self.publish_message(&dbirth_topic, dbirth_payload.encode_to_vec())
            .await?;

        seq.commit();
        Ok(())
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
    /// # Cancellation
    ///
    /// A cancelled call moves no count. See [`Self::publish_metric`].
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::Publish`] when the client refuses the
    /// message, and [`SparkplugError::Encode`] when the payload will not
    /// encode. Returns [`SparkplugError::InvalidTopic`] when the topic names
    /// no edge node, because a metric belongs to the count of one edge node.
    /// The segments themselves cannot fail — the topic checked them when it
    /// was built.
    pub async fn publish_metric_to(
        &self,
        topic: &SparkplugTopic,
        metric_name: &str,
        value: MetricValue,
        timestamp_ms: u64,
    ) -> Result<(), SparkplugError> {
        let metric = create_metric(metric_name, value, Timestamp(timestamp_ms));

        self.publish_metrics_on(topic, vec![metric], Timestamp(timestamp_ms))
            .await
    }

    /// Publish a batch of metrics to a topic the caller already built.
    ///
    /// The batch goes out as one message. See [`Self::publish_metric_to`].
    ///
    /// # Cancellation
    ///
    /// A cancelled call moves no count. See [`Self::publish_metric`].
    ///
    /// # Errors
    ///
    /// See [`Self::publish_metric_to`].
    pub async fn publish_metrics_to(
        &self,
        topic: &SparkplugTopic,
        metrics: Vec<(String, MetricValue, u64)>,
    ) -> Result<(), SparkplugError> {
        self.publish_metrics_on(topic, proto_metrics(metrics), Timestamp::now())
            .await
    }

    /// Draw the `seq` of the edge node `topic` names, publish the metrics
    /// under it, and keep the number only when the client takes the message.
    ///
    /// Every publish that carries metrics passes through here. The
    /// reservation holds the gate of that edge node from the draw until the
    /// client answers, so the order the numbers are drawn in and the order
    /// the messages reach the client agree.
    ///
    /// # Errors
    ///
    /// See [`Self::reserve_seq`].
    async fn publish_metrics_on(
        &self,
        topic: &SparkplugTopic,
        metrics: Vec<Metric>,
        at: Timestamp,
    ) -> Result<(), SparkplugError> {
        let seq = self.reserve_seq(topic).await?;
        let payload = create_payload(metrics, at, *seq);

        self.publish_message(topic, payload.encode_to_vec()).await?;

        seq.commit();
        Ok(())
    }

    /// Reserve the `seq` that the next message on `topic` carries.
    ///
    /// One place reads the counter, so every publish path counts alike.
    /// The group and the edge node come from the topic, and that pair names
    /// the edge node the count belongs to. A node topic and a device topic
    /// both carry the pair, so both draw from one count.
    ///
    /// The message type of the topic decides which way the count moves. An
    /// NBIRTH declares a new session for its edge node, so it sets that
    /// count back to 0. Every other message adds 1. A caller who builds an
    /// NBIRTH topic and publishes it through [`Self::publish_metric_to`]
    /// therefore restarts the count, the same as [`Self::publish_birth`]
    /// does.
    ///
    /// The topic hands back checked identifiers, so nothing here checks a
    /// segment again — see ADR-0002.
    ///
    /// The reservation holds the gate of the edge node until it drops. The
    /// caller must therefore publish and commit, or drop it — see ADR-0005.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::InvalidTopic`] when the topic names no edge
    /// node. A host topic has that shape, and a STATE message carries no
    /// `seq`.
    async fn reserve_seq(&self, topic: &SparkplugTopic) -> Result<SeqReservation, SparkplugError> {
        let (group, node) = topic.group_id().zip(topic.node_id()).ok_or_else(|| {
            SparkplugError::InvalidTopic(format!(
                "{topic} names no edge node, so it holds no seq count"
            ))
        })?;

        Ok(match topic.message_type() {
            MessageType::NBIRTH => self.seq.reserve_reset(group, node).await,
            _ => self.seq.reserve(group, node).await,
        })
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
    /// A cancelled call releases whatever a tracked publish counted, so it
    /// holds no later flush open.
    async fn publish_message(
        &self,
        topic: &SparkplugTopic,
        payload: Vec<u8>,
    ) -> Result<(), SparkplugError> {
        let options = wire_options(self.role(), topic.message_type());

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
    /// A [`Role::Publisher`](crate::Role::Publisher) client publishes at QoS
    /// 1, so the broker acknowledges every message and `flush` reports what a
    /// disconnect discarded. A [`Role::EdgeNode`](crate::Role::EdgeNode)
    /// client publishes at the QoS the
    /// specification fixes, which is QoS 0 for everything this crate can
    /// send today — the broker acknowledges none of it, so `flush` returns
    /// `Ok` without confirming anything.
    ///
    /// Assert this once at startup before gating a durable write on
    /// [`Self::flush`]. A `false` reading means the confirmation a caller
    /// wants is not available in this role, not that a publish failed.
    #[must_use]
    pub fn tracks_delivery(&self) -> bool {
        wire_options(self.role(), MessageType::DDATA).tracked
    }
}

/// Map caller metrics onto their protobuf form.
///
/// Each metric carries the instant the caller gives beside it, so a batch
/// can hold values read at different times.
pub(super) fn proto_metrics(metrics: Vec<(String, MetricValue, u64)>) -> Vec<Metric> {
    metrics
        .into_iter()
        .map(|(name, value, ts)| create_metric(name, value, Timestamp(ts)))
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

/// Count a publish, hand it to the client, and release the count for any
/// message that does not reach the client.
///
/// The count must rise before `publish` runs. `publish_metric` and `flush`
/// both take `&self`, so a task that shares this client can flush while a
/// publish is still in flight. A disconnect in that window would record no
/// loss, and an early PubAck would leave a count that never clears.
/// Over-counting only delays a flush, so it is the safe direction.
///
/// `PublishInFlight` holds the count. It releases the count when it drops,
/// which covers the refusal below and the cancellation that drops this
/// whole future. The guard carries the ticket of its own publish, so it can
/// never release a count that a disconnect cleared or that belongs to a
/// publish from another task.
///
/// Split out as a free function so a test can drive the ordering without a
/// broker.
pub(super) async fn record_around_publish(
    delivery: &DeliveryTracker,
    publish: impl Future<Output = Result<(), rumqttc::ClientError>>,
) -> Result<(), SparkplugError> {
    let in_flight = delivery.publish_in_flight();

    match publish.await {
        Ok(()) => {
            in_flight.keep();
            Ok(())
        }
        // `in_flight` drops here, which releases the count.
        Err(error) => Err(SparkplugError::from(error)),
    }
}
