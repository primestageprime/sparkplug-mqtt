//! The background MQTT poll loop.
//!
//! The loop reads one event at a time from an [`EventSource`] and applies it
//! to the delivery tracker and the health signal. `rumqttc::EventLoop` is the
//! adapter in production. Tests supply a scripted adapter, so the whole
//! session lifecycle runs without a broker.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::delivery::DeliveryTracker;

/// Maximum consecutive event-loop errors before escalating from warn to error.
const MAX_CONSECUTIVE_ERRORS: u32 = 5;

/// How long the loop waits after a poll error before it polls again.
const ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// A stream of MQTT events the loop can poll.
///
/// This is the seam under [`run`]. `rumqttc::EventLoop` satisfies it in
/// production; a scripted adapter satisfies it in tests. The `Send` bound is
/// what lets [`super::SparkplugClient::connect`] spawn the loop.
pub(super) trait EventSource {
    fn poll(
        &mut self,
    ) -> impl Future<Output = Result<rumqttc::Event, rumqttc::ConnectionError>> + Send;
}

impl EventSource for rumqttc::EventLoop {
    async fn poll(&mut self) -> Result<rumqttc::Event, rumqttc::ConnectionError> {
        rumqttc::EventLoop::poll(self).await
    }
}

/// Apply one polled event to the delivery tracker.
fn apply_event(
    delivery: &DeliveryTracker,
    event: &Result<rumqttc::Event, rumqttc::ConnectionError>,
) {
    match event {
        Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))) => delivery.record_ack(),
        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)) | Err(_) => {
            delivery.record_disconnect();
        }
        _ => {}
    }
}

/// Whether the client currently holds a broker connection.
///
/// This reports the link, not delivery. Use [`super::SparkplugClient::flush`]
/// to learn whether specific messages arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Connected,
    Disconnected,
}

/// Apply one polled event to the health signal.
///
/// `rumqttc` reports `Outgoing::Disconnect` only after it writes and flushes
/// the DISCONNECT packet, so that event proves the packet left the socket.
/// [`super::SparkplugClient::shutdown`] waits for it.
fn apply_health(
    health: &tokio::sync::watch::Sender<Health>,
    event: &Result<rumqttc::Event, rumqttc::ConnectionError>,
) {
    let next = match event {
        Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => Health::Connected,
        Ok(
            rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)
            | rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect),
        )
        | Err(_) => Health::Disconnected,
        _ => return,
    };
    health.send_replace(next);
}

/// Poll the event source until the task is aborted.
///
/// The loop never returns on its own. A poll error is not fatal — it backs
/// off and polls again, because `rumqttc` reconnects underneath.
pub(super) async fn run<S: EventSource>(
    mut source: S,
    delivery: Arc<DeliveryTracker>,
    health: tokio::sync::watch::Sender<Health>,
    ready: oneshot::Sender<()>,
) {
    let mut ready = Some(ready);
    let mut consecutive_errors: u32 = 0;

    loop {
        let event = source.poll().await;
        apply_event(&delivery, &event);
        apply_health(&health, &event);

        match event {
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                tracing::info!("SparkplugClient connected");
                consecutive_errors = 0;
                if let Some(tx) = ready.take() {
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
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
