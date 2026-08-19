//! Unit tests for [`super`].
//!
//! They live in their own file so `client.rs` stays under the
//! module size limit.

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
fn ddata_topic_uses_caller_provided_node_id() {
    let topic = ddata_topic("spBv1.0", "Stax", "xbox7-1", "xbox7-1");
    assert_eq!(topic, "spBv1.0/Stax/DDATA/xbox7-1/xbox7-1");
}

#[test]
fn ddata_topic_distinguishes_node_and_device() {
    // Real deployments usually set node_id == device_id for single-asset
    // nodes, but the topic must carry both independently.
    let topic = ddata_topic("spBv1.0", "Stax", "edge-node-A", "device-1");
    assert_eq!(topic, "spBv1.0/Stax/DDATA/edge-node-A/device-1");
}
