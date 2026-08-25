use crate::client::{MqttConfig, mqtt_parts};
use crate::error::SparkplugError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use super::delivery::DeliveryTracker;
use super::eventloop::Health;
use super::seq::SeqCounters;
use super::topic::Namespace;
use super::topic::ids::{EdgeNode, OwnedEdgeNode};
use super::types::Role;

mod publish;
mod shutdown;
use shutdown::{disconnect_outcome, shutdown_outcome};

/// Which Sparkplug identity a client holds, fixed when it connects.
///
/// The identity and the role are one value, so a client in the edge node
/// role always names the edge node it speaks for. Nothing can build the
/// role without a checked pair beside it — see ADR-0004.
enum Identity {
    /// Publishes on behalf of edge nodes it does not own.
    ///
    /// It names no edge node of its own, so it announces no birth and no
    /// death. Every publish carries the edge node the caller gives it.
    Publisher,

    /// Speaks for one edge node, named here at connect.
    ///
    /// [`OwnedEdgeNode`] holds the group and the edge node id together, as
    /// Sparkplug requires. Its fields are private and its only constructor
    /// takes an [`EdgeNode`], so this variant cannot hold a segment that no
    /// check passed. [`SparkplugClient::edge_node`] borrows the pair back
    /// without a second check.
    EdgeNode(OwnedEdgeNode),
}

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
///     client_id: None,
///     version: "spBv1.0".to_owned(),
/// };
///
/// let client = SparkplugClient::connect(&config).await?;
/// client
///     .publish_metric("MyGroup", "node1", "device1", "temperature", MetricValue::Float(23.5), 0)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Self::connect`] gives a publisher client, which takes the edge node of
/// each publish per call — one connection can publish for many edge nodes.
/// Call [`Self::connect_as_edge_node`] to speak for one edge node named at
/// connect, which is the client [`Self::publish_birth`] needs.
pub struct SparkplugClient {
    client: rumqttc::AsyncClient,
    namespace: Namespace,
    /// Which Sparkplug identity this client holds. It decides the role, so
    /// the two cannot disagree.
    identity: Identity,
    delivery: Arc<DeliveryTracker>,
    /// The `seq` count of each edge node this client publishes for.
    seq: Arc<SeqCounters>,
    health: tokio::sync::watch::Sender<Health>,
    event_loop_handle: tokio::task::JoinHandle<()>,
}

impl Drop for SparkplugClient {
    fn drop(&mut self) {
        self.event_loop_handle.abort();
    }
}

impl SparkplugClient {
    /// Connect to an MQTT broker and return a ready-to-use publisher client.
    ///
    /// Waits up to 5 seconds for the initial connection. The background
    /// event loop reconnects automatically if the connection drops later.
    ///
    /// The client takes the edge node of each publish per call and owns
    /// none, so it announces no birth. Call [`Self::connect_as_edge_node`]
    /// for a client that speaks for one edge node.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::ConnectionTimeout`] if the broker doesn't
    /// respond within 5 seconds.
    pub async fn connect(config: &MqttConfig) -> Result<Self, SparkplugError> {
        Self::connect_with(config, Identity::Publisher, DEFAULT_CONNECT_TIMEOUT).await
    }

    /// Connect as a publisher, waiting up to `timeout` for the broker to
    /// answer.
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
        Self::connect_with(config, Identity::Publisher, timeout).await
    }

    /// Connect as one edge node, named here and fixed for the session.
    ///
    /// `edge_node` names the Sparkplug entity this client speaks for.
    /// `config.client_id` names the MQTT connection. The two are separate:
    /// one process can hold several connections, each with its own client
    /// id, and only some of them speak for an edge node.
    ///
    /// The client publishes in [`Role::EdgeNode`], which takes the QoS and
    /// the retain flag the specification fixes. The broker acknowledges
    /// nothing at QoS 0, so [`Self::flush`] confirms nothing — read
    /// [`Self::tracks_delivery`] before you gate a durable write on it.
    ///
    /// Pass [`DEFAULT_CONNECT_TIMEOUT`] for the wait [`Self::connect`] uses.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::ConnectionTimeout`] if the broker does not
    /// answer within `timeout`. Returns
    /// [`SparkplugError::InvalidNamespace`] when `config.version` cannot
    /// stand as a topic segment.
    pub async fn connect_as_edge_node(
        config: &MqttConfig,
        edge_node: EdgeNode<'_>,
        timeout: Duration,
    ) -> Result<Self, SparkplugError> {
        Self::connect_with(config, Identity::EdgeNode(edge_node.into()), timeout).await
    }

    /// Open the connection and start the event loop, whichever identity the
    /// client holds.
    ///
    /// Every public constructor runs this body, so they differ in the
    /// identity alone.
    ///
    /// # Errors
    ///
    /// See [`Self::connect_as_edge_node`].
    async fn connect_with(
        config: &MqttConfig,
        identity: Identity,
        timeout: Duration,
    ) -> Result<Self, SparkplugError> {
        let (async_client, eventloop) = mqtt_parts(config);

        let namespace = Namespace::new(&config.version)?;

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
            identity,
            delivery,
            seq: Arc::new(SeqCounters::new()),
            health,
            event_loop_handle,
        })
    }

    /// Which role this client publishes in.
    ///
    /// The identity decides it, so an edge node role always has an edge
    /// node beside it.
    fn role(&self) -> Role {
        match self.identity {
            Identity::Publisher => Role::Publisher,
            Identity::EdgeNode { .. } => Role::EdgeNode,
        }
    }

    /// The edge node this client speaks for, or `None` for a publisher.
    ///
    /// The stored pair passed its check where it entered [`OwnedEdgeNode`],
    /// so this borrows it rather than checking it again — see ADR-0002.
    fn edge_node(&self) -> Option<EdgeNode<'_>> {
        match &self.identity {
            Identity::Publisher => None,
            Identity::EdgeNode(edge_node) => Some(edge_node.into()),
        }
    }

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
/// Pass this to [`SparkplugClient::connect_as_edge_node`] to get the same
/// wait that [`SparkplugClient::connect`] uses.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests;
