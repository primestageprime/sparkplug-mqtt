use crate::client::{MqttConfig, mqtt_parts};
use crate::error::SparkplugError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use super::delivery::DeliveryTracker;
use super::eventloop::Health;
use super::topic::Namespace;
use super::types::Role;

mod publish;
mod shutdown;
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
    role: Role,
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
        Self::connect_as(config, Role::Publisher, DEFAULT_CONNECT_TIMEOUT).await
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
        Self::connect_as(config, Role::Publisher, timeout).await
    }

    /// Connect in a chosen role, waiting up to `timeout` for the broker.
    ///
    /// The role fixes the QoS and the retain flag every publish uses, and
    /// with them whether [`Self::flush`] can confirm anything. Read
    /// [`Role`] before choosing, and [`Self::tracks_delivery`] after.
    ///
    /// [`Self::connect`] and [`Self::connect_with_timeout`] both call this
    /// with [`Role::Publisher`], which is why raising the crate version
    /// changes no existing caller's behaviour. Pass
    /// [`DEFAULT_CONNECT_TIMEOUT`] for the timeout those two use.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::ConnectionTimeout`] if the broker does not
    /// answer within `timeout`. Returns
    /// [`SparkplugError::InvalidNamespace`] when `config.version` cannot
    /// stand as a topic segment.
    pub async fn connect_as(
        config: &MqttConfig,
        role: Role,
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
            role,
            event_loop_handle,
        })
    }

    /// Publish a single metric as a DDATA message on behalf of `node_id`.
    /// The namespace this client publishes on.
    ///
    /// Use it to build topics for [`Self::publish_metric_to`].
    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
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

/// How long [`SparkplugClient::connect`] waits for the broker to answer.
///
/// Pass this to [`SparkplugClient::connect_as`] to get the same wait that
/// [`SparkplugClient::connect`] uses.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests;
