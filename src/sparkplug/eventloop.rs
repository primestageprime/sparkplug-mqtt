//! The background MQTT poll loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::delivery::DeliveryTracker;

/// Maximum consecutive event-loop errors before escalating from warn to error.
const MAX_CONSECUTIVE_ERRORS: u32 = 5;

/// Apply one polled event to the delivery tracker.
///
/// Split out as a pure function so the classification is testable without
/// a broker.
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

/// Poll the MQTT event loop until the task is aborted.
pub(super) async fn run(
    mut eventloop: rumqttc::EventLoop,
    delivery: Arc<DeliveryTracker>,
    health: tokio::sync::watch::Sender<Health>,
    ready: oneshot::Sender<()>,
) {
    let mut ready = Some(ready);
    let mut consecutive_errors: u32 = 0;

    loop {
        let event = eventloop.poll().await;
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
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_puback_clears_one_publish() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        apply_event(
            &tracker,
            &Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(
                rumqttc::PubAck::new(1),
            ))),
        );
        assert_eq!(tracker.unacked(), 0);
    }

    #[test]
    fn a_poll_error_loses_everything_outstanding() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        apply_event(&tracker, &Err(rumqttc::ConnectionError::RequestsDone));
        assert_eq!(
            tracker.unacked(),
            0,
            "a disconnect clears the outstanding count"
        );
    }

    #[test]
    fn an_incoming_disconnect_loses_everything_outstanding() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        apply_event(
            &tracker,
            &Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)),
        );
        assert_eq!(tracker.unacked(), 0);
    }

    #[test]
    fn an_unrelated_event_changes_nothing() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        apply_event(
            &tracker,
            &Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)),
        );
        assert_eq!(tracker.unacked(), 1);
    }

    #[test]
    fn connack_reports_connected_and_errors_report_disconnected() {
        let (tx, rx) = tokio::sync::watch::channel(Health::Disconnected);

        apply_health(
            &tx,
            &Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck::new(rumqttc::ConnectReturnCode::Success, false),
            ))),
        );
        assert_eq!(*rx.borrow(), Health::Connected);

        apply_health(&tx, &Err(rumqttc::ConnectionError::RequestsDone));
        assert_eq!(*rx.borrow(), Health::Disconnected);
    }

    #[test]
    fn a_written_disconnect_reports_disconnected() {
        let (tx, rx) = tokio::sync::watch::channel(Health::Connected);
        apply_health(
            &tx,
            &Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)),
        );
        assert_eq!(*rx.borrow(), Health::Disconnected);
    }

    #[test]
    fn an_unrelated_event_leaves_health_alone() {
        let (tx, rx) = tokio::sync::watch::channel(Health::Connected);
        apply_health(
            &tx,
            &Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)),
        );
        assert_eq!(*rx.borrow(), Health::Connected);
    }
}
