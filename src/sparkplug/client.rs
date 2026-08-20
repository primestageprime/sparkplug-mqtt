use crate::client::{MqttConfig, mqtt_parts};
use crate::error::SparkplugError;
use crate::payload::payload::Metric;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use super::delivery::DeliveryTracker;
use super::eventloop::Health;
use super::ids::{DeviceId, EdgeNodeId, GroupId};
use super::payload_helpers::{
    create_birth_certificate, create_device_birth_certificate, create_payload,
};
use super::topic::{Namespace, SparkplugTopic};

mod shutdown;
use super::types::{MetricValue, Timestamp};
use shutdown::{disconnect_outcome, shutdown_outcome};

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
    namespace: Namespace,
    node_id: Arc<str>,
    delivery: Arc<DeliveryTracker>,
    health: tokio::sync::watch::Sender<Health>,
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
        Self::connect_with_timeout(config, DEFAULT_CONNECT_TIMEOUT).await
    }

    /// Connect, waiting up to `timeout` for the broker to answer.
    ///
    /// [`Self::connect`] uses five seconds. Raise it when the broker may be
    /// restarting alongside this process.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::ConnectionTimeout`] if the broker does not
    /// answer within `timeout`. Returns
    /// [`SparkplugError::InvalidNamespace`] when `config.version` cannot
    /// stand as a topic segment.
    pub async fn connect_with_timeout(
        config: &MqttConfig,
        timeout: Duration,
    ) -> Result<Self, SparkplugError> {
        let (async_client, eventloop) = mqtt_parts(config);

        let namespace = Namespace::new(&config.version)?;
        let node_id: Arc<str> = Arc::from(config.node_id.as_str());

        let (tx, rx) = oneshot::channel::<()>();
        let delivery = Arc::new(DeliveryTracker::new());
        let (health, _) = tokio::sync::watch::channel(Health::Disconnected);
        let event_loop_handle = tokio::spawn(super::eventloop::run(
            eventloop,
            delivery.clone(),
            health.clone(),
            tx,
        ));

        match tokio::time::timeout(timeout, rx).await {
            Ok(_) => {
                tracing::info!("SparkplugClient connected successfully");
            }
            Err(_) => {
                event_loop_handle.abort();
                return Err(SparkplugError::ConnectionTimeout(timeout));
            }
        }

        Ok(Self {
            client: async_client,
            namespace,
            node_id,
            delivery,
            health,
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

        self.publish_tracked(&topic, payload.encode_to_vec()).await
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

        self.publish_tracked(&topic, payload.encode_to_vec()).await
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
        self.publish_tracked(&nbirth_topic, nbirth_payload.encode_to_vec())
            .await?;

        let dbirth_topic = self.namespace.dbirth(
            GroupId::new(group_id)?,
            EdgeNodeId::new(&self.node_id)?,
            DeviceId::new(device_id)?,
        );
        let dbirth_payload = create_device_birth_certificate();
        self.publish_tracked(&dbirth_topic, dbirth_payload.encode_to_vec())
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
        self.publish_tracked(topic, payload.encode_to_vec()).await
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
        self.publish_tracked(topic, payload.encode_to_vec()).await
    }

    /// The namespace this client publishes on.
    ///
    /// Use it to build topics for [`Self::publish_metric_to`].
    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
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

    /// Publish one QoS 1 message and keep the delivery count honest.
    ///
    /// This is not cancel-safe. A cancelled call leaves `unacked` one too
    /// high, so every later flush reports `FlushTimeout`.
    async fn publish_tracked(
        &self,
        topic: &SparkplugTopic,
        payload: Vec<u8>,
    ) -> Result<(), SparkplugError> {
        record_around_publish(
            &self.delivery,
            self.client
                .publish(topic.to_string(), rumqttc::QoS::AtLeastOnce, false, payload),
        )
        .await
    }

    /// Wait until the broker has acknowledged every publish from this client.
    ///
    /// Call this before recording that a message was delivered — for example
    /// before stamping a database row as published, or before a short-lived
    /// process exits.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::PublishLost`] when a disconnect discarded
    /// publishes since the last flush. Sparkplug requires `clean_session`, so
    /// the broker keeps no session state and those messages cannot be resent
    /// automatically. Returns [`SparkplugError::FlushTimeout`] when the broker
    /// stays silent.
    ///
    /// A successful flush clears the loss record, so each call reports only
    /// what happened since the previous one.
    pub async fn flush(&self, timeout: Duration) -> Result<(), SparkplugError> {
        self.delivery.flush(timeout).await
    }

    /// How many publishes the broker has not acknowledged yet.
    #[must_use]
    pub fn unacked(&self) -> u32 {
        self.delivery.unacked()
    }

    /// Watch whether the client holds a broker connection.
    ///
    /// This reports the link only. A `Connected` reading does not prove that
    /// any particular message arrived — use [`Self::flush`] for that.
    #[must_use]
    pub fn health(&self) -> tokio::sync::watch::Receiver<Health> {
        self.health.subscribe()
    }

    /// Flush, disconnect cleanly, and stop the event loop.
    ///
    /// Prefer this to dropping the client. [`Drop`] aborts the event loop
    /// immediately, so a short-lived process can lose whatever had not reached
    /// the wire.
    ///
    /// `timeout` bounds the whole sequence, not each step. The flush runs
    /// first and can use the full budget, which leaves the disconnect
    /// unconfirmed. This method never runs past `timeout` by more than the
    /// time one lock takes.
    ///
    /// # Errors
    ///
    /// Returns whatever [`Self::flush`] returns. Returns
    /// [`SparkplugError::Disconnect`] when the client refuses the DISCONNECT
    /// request, and [`SparkplugError::ShutdownTimeout`] when the flush
    /// succeeds but the event loop does not confirm the DISCONNECT packet
    /// before the deadline. The client stops on any of these errors, so the
    /// caller learns what happened without leaking the task.
    pub async fn shutdown(self, timeout: Duration) -> Result<(), SparkplugError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let flushed = self.flush(timeout).await;

        // Subscribe before the request, so the signal cannot slip past.
        let health = self.health.subscribe();
        let requested = tokio::time::timeout_at(deadline, self.client.disconnect()).await;

        shutdown_outcome(
            flushed,
            disconnect_outcome(requested, health, deadline).await,
            timeout,
        )
    }
}

/// Map caller metrics onto their protobuf form.
fn proto_metrics(metrics: Vec<(String, MetricValue, u64)>) -> Vec<Metric> {
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

/// How long [`SparkplugClient::connect`] waits for the broker to answer.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
async fn record_around_publish(
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

#[cfg(test)]
mod tests;
