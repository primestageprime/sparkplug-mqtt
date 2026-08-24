//! Unit tests for [`super`].
//!
//! They live in their own file so `client.rs` stays under the
//! module size limit.

use super::publish::{proto_metrics, publish_with_options, record_around_publish};
use super::shutdown::{Disconnected, disconnect_outcome, shutdown_outcome, wait_for_disconnect};
use super::*;
use crate::sparkplug::topic::SparkplugTopic;
use crate::sparkplug::topic::ids::{DeviceId, EdgeNodeId, GroupId, HostId};
use crate::sparkplug::types::{MessageType, MetricValue, wire_options};

/// What `seq` reaches the wire. Its own file, so this one stays under the
/// module size limit.
mod seq;

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

// ---------------------------------------------------------------------------
// What reaches the wire
// ---------------------------------------------------------------------------
//
// `rumqttc::AsyncClient::from_senders` builds a client over a channel the
// test owns. Its own documentation names this use: "mostly useful for
// creating a test instance where you can listen on the corresponding
// receiver." So these run the real publish path and read the QoS and retain
// flag the message actually carries — not a table that says what it should.

/// Build a client whose publishes land in a channel instead of a socket.
///
/// The event loop task parks forever. `Drop` aborts it, so nothing leaks.
fn wired(role: Role) -> (SparkplugClient, flume::Receiver<rumqttc::Request>) {
    let (tx, rx) = flume::bounded(16);
    let (health, _) = tokio::sync::watch::channel(Health::Disconnected);

    let client = SparkplugClient {
        client: rumqttc::AsyncClient::from_senders(tx),
        namespace: Namespace::sparkplug_b(),
        node_id: Arc::from("edge1"),
        delivery: Arc::new(DeliveryTracker::new()),
        seq: Arc::new(SeqCounters::new()),
        health,
        role,
        event_loop_handle: tokio::spawn(std::future::pending::<()>()),
    };

    (client, rx)
}

fn ddata_topic(client: &SparkplugClient) -> SparkplugTopic {
    client.namespace().ddata(
        GroupId::new("PlantFloor").expect("group id"),
        EdgeNodeId::new("edge_node_1").expect("edge node id"),
        DeviceId::new("pump_3").expect("device id"),
    )
}

/// Read the one publish the client sent.
fn sent(requests: &flume::Receiver<rumqttc::Request>) -> rumqttc::Publish {
    match requests
        .recv()
        .expect("the client must have sent a request")
    {
        rumqttc::Request::Publish(publish) => publish,
        other => panic!("expected a publish, got {other:?}"),
    }
}

#[tokio::test]
async fn a_publisher_client_sends_data_at_qos_one_and_counts_it() {
    let (client, requests) = wired(Role::Publisher);

    client
        .publish_metric_to(
            &ddata_topic(&client),
            "temperature",
            MetricValue::Float(1.0),
            0,
        )
        .await
        .expect("the publish must be accepted");

    let publish = sent(&requests);
    assert_eq!(publish.qos, rumqttc::QoS::AtLeastOnce);
    assert!(!publish.retain);
    assert_eq!(
        client.unacked(),
        1,
        "a QoS 1 publish is outstanding until the broker acknowledges it"
    );
}

#[tokio::test]
async fn an_edge_node_client_sends_data_at_qos_zero_and_counts_nothing() {
    let (client, requests) = wired(Role::EdgeNode);

    client
        .publish_metric_to(
            &ddata_topic(&client),
            "temperature",
            MetricValue::Float(1.0),
            0,
        )
        .await
        .expect("the publish must be accepted");

    let publish = sent(&requests);
    assert_eq!(
        publish.qos,
        rumqttc::QoS::AtMostOnce,
        "DDATA takes QoS 0 under the specification"
    );
    assert!(!publish.retain);
    assert_eq!(
        client.unacked(),
        0,
        "the broker never acknowledges QoS 0, so counting it would hold every \
         later flush open"
    );
}

#[tokio::test]
async fn an_untracked_publish_leaves_flush_with_nothing_to_wait_for() {
    let (client, _requests) = wired(Role::EdgeNode);

    client
        .publish_metric_to(
            &ddata_topic(&client),
            "temperature",
            MetricValue::Float(1.0),
            0,
        )
        .await
        .expect("the publish must be accepted");

    client
        .flush(Duration::from_millis(50))
        .await
        .expect("an untracked publish must not hold a flush open");
}

#[tokio::test]
async fn the_publish_path_reads_the_message_type_from_the_topic() {
    // NCMD is a node message and carries metrics, which is how a host
    // application sends `Node Control/Rebirth`. Under the specification it
    // takes QoS 0, the same as DDATA — so this proves the options come from
    // the topic rather than from a constant on the data path.
    let (client, requests) = wired(Role::EdgeNode);
    let topic = client.namespace().ncmd(
        GroupId::new("PlantFloor").expect("group id"),
        EdgeNodeId::new("edge_node_1").expect("edge node id"),
    );

    client
        .publish_metric_to(&topic, "Node Control/Rebirth", MetricValue::Bool(true), 0)
        .await
        .expect("the publish must be accepted");

    let publish = sent(&requests);
    assert_eq!(publish.topic, topic.to_string());
    assert_eq!(publish.qos, rumqttc::QoS::AtMostOnce);
}

#[tokio::test]
async fn tracks_delivery_reports_the_role() {
    let (publisher, _p) = wired(Role::Publisher);
    let (edge_node, _e) = wired(Role::EdgeNode);

    assert!(
        publisher.tracks_delivery(),
        "a publisher client publishes at QoS 1, so flush can confirm it"
    );
    assert!(
        !edge_node.tracks_delivery(),
        "an edge node client publishes at QoS 0, so flush confirms nothing"
    );
}

#[tokio::test]
async fn an_untracked_publish_that_the_client_refuses_still_reports_the_error() {
    let (tx, rx) = flume::bounded::<rumqttc::Request>(1);
    drop(rx);
    let delivery = DeliveryTracker::new();
    let client = rumqttc::AsyncClient::from_senders(tx);

    let options = wire_options(Role::EdgeNode, MessageType::DDATA);
    let sent = publish_with_options(
        &delivery,
        options,
        client.publish("t", options.qos, false, vec![]),
    )
    .await;

    assert!(
        matches!(sent, Err(SparkplugError::Publish(_))),
        "a refused publish must reach the caller even when it is untracked"
    );
    assert_eq!(delivery.unacked(), 0);
}
