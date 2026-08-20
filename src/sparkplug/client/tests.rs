//! Unit tests for [`super`].
//!
//! They live in their own file so `client.rs` stays under the
//! module size limit.

use super::shutdown::{Disconnected, disconnect_outcome, shutdown_outcome, wait_for_disconnect};
use super::*;

#[tokio::test]
async fn a_publish_is_counted_before_the_client_sees_it() {
    let delivery = DeliveryTracker::new();
    let counted_in_flight = std::cell::Cell::new(0);

    let sent = record_around_publish(&delivery, async {
        counted_in_flight.set(delivery.unacked());
        Ok(())
    })
    .await;

    assert!(sent.is_ok());
    assert_eq!(
        counted_in_flight.get(),
        1,
        "a flush that runs while the publish is in flight must see it outstanding"
    );
}

#[tokio::test]
async fn a_refused_publish_leaves_no_phantom_count() {
    let delivery = DeliveryTracker::new();

    let sent = record_around_publish(&delivery, async {
        Err(rumqttc::ClientError::Request(rumqttc::Request::Disconnect(
            rumqttc::Disconnect,
        )))
    })
    .await;

    assert!(matches!(sent, Err(SparkplugError::Publish(_))));
    assert_eq!(
        delivery.unacked(),
        0,
        "a message the client refused must not hold a later flush open"
    );
}

#[tokio::test]
async fn a_refused_publish_cannot_cancel_another_publish() {
    let delivery = DeliveryTracker::new();

    // The client refuses this publish, but only after a disconnect
    // clears the count and a second task starts a publish of its own.
    let refused = record_around_publish(&delivery, async {
        delivery.record_disconnect();
        record_around_publish(&delivery, async { Ok(()) })
            .await
            .expect("the second publish must succeed");
        Err(rumqttc::ClientError::Request(rumqttc::Request::Disconnect(
            rumqttc::Disconnect,
        )))
    })
    .await;

    assert!(matches!(refused, Err(SparkplugError::Publish(_))));
    assert_eq!(
        delivery.unacked(),
        1,
        "a refused publish must not cancel the count of a publish that is still in flight"
    );
}

#[tokio::test]
async fn an_unwritten_disconnect_is_not_confirmed() {
    let (_health, receiver) = tokio::sync::watch::channel(Health::Connected);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    assert!(
        !wait_for_disconnect(receiver, deadline).await,
        "a silent event loop must not count as a written DISCONNECT"
    );
}

#[tokio::test]
async fn a_written_disconnect_is_confirmed() {
    let (health, receiver) = tokio::sync::watch::channel(Health::Connected);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

    let waiting = tokio::spawn(wait_for_disconnect(receiver, deadline));
    tokio::time::sleep(Duration::from_millis(20)).await;
    health.send_replace(Health::Disconnected);

    assert!(waiting.await.expect("the waiting task must not panic"));
}

#[test]
fn shutdown_reports_the_lost_publishes_before_the_disconnect() {
    let outcome = shutdown_outcome(
        Err(SparkplugError::PublishLost { count: 3 }),
        Disconnected::Unconfirmed,
        Duration::from_secs(5),
    );
    assert!(matches!(
        outcome,
        Err(SparkplugError::PublishLost { count: 3 })
    ));
}

#[test]
fn shutdown_reports_an_unconfirmed_disconnect() {
    let outcome = shutdown_outcome(Ok(()), Disconnected::Unconfirmed, Duration::from_secs(5));
    assert!(matches!(
        outcome,
        Err(SparkplugError::ShutdownTimeout { .. })
    ));
}

#[test]
fn shutdown_reports_the_refused_disconnect_rather_than_a_timeout() {
    let refused = Disconnected::Refused(rumqttc::ClientError::Request(
        rumqttc::Request::Disconnect(rumqttc::Disconnect),
    ));
    let outcome = shutdown_outcome(Ok(()), refused, Duration::from_secs(5));
    assert!(
        matches!(outcome, Err(SparkplugError::Disconnect(_))),
        "a refused request must name its own cause, not the deadline"
    );
}

#[tokio::test]
async fn a_refused_disconnect_does_not_wait_for_the_event_loop() {
    let (_health, receiver) = tokio::sync::watch::channel(Health::Connected);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let refused = Ok(Err(rumqttc::ClientError::Request(
        rumqttc::Request::Disconnect(rumqttc::Disconnect),
    )));

    let outcome = disconnect_outcome(refused, receiver, deadline).await;
    assert!(matches!(outcome, Disconnected::Refused(_)));
}

#[tokio::test]
async fn a_request_that_misses_the_deadline_is_unconfirmed() {
    let (_health, receiver) = tokio::sync::watch::channel(Health::Connected);
    let deadline = tokio::time::Instant::now();
    let late = tokio::time::timeout(Duration::from_nanos(1), std::future::pending::<()>())
        .await
        .map(|()| Ok(()));

    let outcome = disconnect_outcome(late, receiver, deadline).await;
    assert!(matches!(outcome, Disconnected::Unconfirmed));
}

#[test]
fn shutdown_reports_success_once_the_disconnect_is_written() {
    assert!(shutdown_outcome(Ok(()), Disconnected::Written, Duration::from_secs(5)).is_ok());
}

#[test]
fn the_batch_mapping_keeps_each_metric_name_value_and_timestamp() {
    let mapped = proto_metrics(vec![
        ("temperature".to_string(), MetricValue::Float(23.5), 100),
        ("mode".to_string(), MetricValue::String("AUTO".into()), 200),
    ]);

    assert_eq!(mapped.len(), 2);
    assert_eq!(mapped[0].name.as_deref(), Some("temperature"));
    assert_eq!(mapped[0].timestamp, Some(100));
    assert_eq!(mapped[1].name.as_deref(), Some("mode"));
    assert_eq!(mapped[1].timestamp, Some(200));
}

#[test]
fn a_checked_identifier_cannot_reach_the_wrong_segment() {
    // The types are the guard. This records what the topic looks like when
    // the edge node and the device differ, which is the pair a caller used
    // to be able to swap silently.
    let ns = Namespace::sparkplug_b();
    let topic = ns.ddata(
        GroupId::new("Stax").expect("Stax is a usable group id"),
        EdgeNodeId::new("edge-node-A").expect("usable edge node id"),
        DeviceId::new("device-1").expect("usable device id"),
    );
    assert_eq!(topic.to_string(), "spBv1.0/Stax/DDATA/edge-node-A/device-1");
}

#[test]
fn a_bad_namespace_fails_the_connect_rather_than_every_publish() {
    let err = Namespace::new("spB/v1.0").expect_err("a namespace with a slash addresses nothing");
    assert!(matches!(err, SparkplugError::InvalidNamespace(_)));
}
