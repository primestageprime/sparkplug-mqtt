//! Everything that puts a Sparkplug message on the wire.
//!
//! Split from `client.rs` so that file stays under the module size limit.
//! The concern here is one question: given a topic and this client's role,
//! how does the message go out, and does the delivery tracker count it?

use prost::Message;

use crate::error::SparkplugError;
use crate::payload::Payload;
use crate::payload::payload::Metric;

use super::SparkplugClient;
use crate::sparkplug::delivery::DeliveryTracker;
use crate::sparkplug::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_metric, create_payload,
};
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
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// It draws the `seq` of the edge node before the same `await`. A
    /// cancelled call, and a message the client refuses, both leave that
    /// number off the wire. The next message carries the number after it, so
    /// a host application reads a gap and asks the edge node for a rebirth.
    /// The counter does not roll back: another task can publish for the same
    /// edge node while this call waits, so a rollback would give one number
    /// to two messages. A host application recovers from a gap, because it
    /// asks for a rebirth. It cannot recover from two messages that carry
    /// one number.
    ///
    /// The same order applies to two tasks that publish for one edge node
    /// on one client. This method draws the number, then
    /// `AsyncClient::publish` waits for room in a bounded channel. The task
    /// that drew 5 can wait while the task that drew 6 finds room, so 6
    /// enters the channel first and the broker sends 6 before 5. A host
    /// application reads that order as a gap and asks the edge node for a
    /// rebirth. Publish for one edge node from one task to hold the order.
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
        let metric = create_metric(metric_name, value, Timestamp(timestamp_ms));

        let topic = self.ddata_topic(group_id, node_id, device_id)?;
        let payload = self.payload_for(&topic, vec![metric], Timestamp(timestamp_ms))?;

        self.publish_message(&topic, payload.encode_to_vec()).await
    }

    /// Publish a batch of metrics as one DDATA message for the edge node
    /// the caller names.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// It draws the `seq` of the edge node before the same `await`. A
    /// cancelled call, and a message the client refuses, both leave that
    /// number off the wire. The next message carries the number after it, so
    /// a host application reads a gap and asks the edge node for a rebirth.
    /// The counter does not roll back: another task can publish for the same
    /// edge node while this call waits, so a rollback would give one number
    /// to two messages. A host application recovers from a gap, because it
    /// asks for a rebirth. It cannot recover from two messages that carry
    /// one number.
    ///
    /// The same order applies to two tasks that publish for one edge node
    /// on one client. This method draws the number, then
    /// `AsyncClient::publish` waits for room in a bounded channel. The task
    /// that drew 5 can wait while the task that drew 6 finds room, so 6
    /// enters the channel first and the broker sends 6 before 5. A host
    /// application reads that order as a gap and asks the edge node for a
    /// rebirth. Publish for one edge node from one task to hold the order.
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
        let topic = self.ddata_topic(group_id, node_id, device_id)?;
        let payload = self.payload_for(&topic, proto_metrics(metrics), Timestamp::now())?;

        self.publish_message(&topic, payload.encode_to_vec()).await
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
    /// # Cancel safety
    ///
    /// This method is not cancel-safe. It counts the publish before it hands
    /// the message to the client, and it holds that count across an `await`.
    /// A cancelled call drops the count without a rollback. `unacked` then
    /// stays one too high until the next disconnect, so every later
    /// [`Self::flush`] reports [`SparkplugError::FlushTimeout`].
    ///
    /// It draws the `seq` of the edge node before the same `await`. A
    /// cancelled call, and a message the client refuses, both leave that
    /// number off the wire. The next message carries the number after it, so
    /// a host application reads a gap and asks the edge node for a rebirth.
    /// The counter does not roll back: another task can publish for the same
    /// edge node while this call waits, so a rollback would give one number
    /// to two messages. A host application recovers from a gap, because it
    /// asks for a rebirth. It cannot recover from two messages that carry
    /// one number.
    ///
    /// The same order applies to two tasks that publish for one edge node
    /// on one client. This method draws the number, then
    /// `AsyncClient::publish` waits for room in a bounded channel. The task
    /// that drew 5 can wait while the task that drew 6 finds room, so 6
    /// enters the channel first and the broker sends 6 before 5. A host
    /// application reads that order as a gap and asks the edge node for a
    /// rebirth. Publish for one edge node from one task to hold the order.
    ///
    /// Do not wrap one publish in `tokio::time::timeout`. Do not put one
    /// publish in a `select!` branch that another branch can cancel. Publish
    /// the whole batch, then bound the wait with the `timeout` argument of
    /// [`Self::flush`].
    pub async fn publish_birth(&self, device: DeviceId<'_>) -> Result<(), SparkplugError> {
        let edge_node = self.edge_node().ok_or(SparkplugError::NotAnEdgeNode)?;

        // One birth event, so both payloads carry one instant.
        let at = Timestamp::now();

        // Both births draw their count through `seq_for`, the one place
        // that reads the counter. It reads the message type of the topic:
        // an NBIRTH sets the count of its edge node back to 0, so the
        // NBIRTH carries 0, and the DBIRTH beside it adds 1 and carries 1.
        // The group and the edge node id together name the edge node that
        // owns the count.
        let nbirth_topic = self
            .namespace
            .nbirth(edge_node.group(), edge_node.edge_node_id());
        let nbirth_payload = create_birth_certificate(at, self.seq_for(&nbirth_topic)?);
        self.publish_message(&nbirth_topic, nbirth_payload.encode_to_vec())
            .await?;

        let dbirth_topic =
            self.namespace
                .dbirth(edge_node.group(), edge_node.edge_node_id(), device);
        let dbirth_payload = create_device_birth_certificate(at, self.seq_for(&dbirth_topic)?);
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

        let payload = self.payload_for(topic, vec![metric], Timestamp(timestamp_ms))?;
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
        let payload = self.payload_for(topic, proto_metrics(metrics), Timestamp::now())?;
        self.publish_message(topic, payload.encode_to_vec()).await
    }

    /// Build a payload for `topic`, and draw the `seq` its edge node
    /// gives the message.
    ///
    /// Every publish that carries metrics passes through here.
    ///
    /// # Errors
    ///
    /// See [`Self::seq_for`].
    fn payload_for(
        &self,
        topic: &SparkplugTopic,
        metrics: Vec<Metric>,
        at: Timestamp,
    ) -> Result<Payload, SparkplugError> {
        Ok(create_payload(metrics, at, self.seq_for(topic)?))
    }

    /// Draw the `seq` that the next message on `topic` carries.
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
    /// # Errors
    ///
    /// Returns [`SparkplugError::InvalidTopic`] when the topic names no edge
    /// node. A host topic has that shape, and a STATE message carries no
    /// `seq`.
    fn seq_for(&self, topic: &SparkplugTopic) -> Result<u8, SparkplugError> {
        let (group, node) = topic.group_id().zip(topic.node_id()).ok_or_else(|| {
            SparkplugError::InvalidTopic(format!(
                "{topic} names no edge node, so it holds no seq count"
            ))
        })?;

        Ok(match topic.message_type() {
            MessageType::NBIRTH => self.seq.reset(group, node),
            _ => self.seq.next(group, node),
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
    /// A tracked publish is not cancel-safe. A cancelled call leaves
    /// `unacked` one too high, so every later flush reports `FlushTimeout`.
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
