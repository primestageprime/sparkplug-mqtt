//! Broker-backed checks. Run them with a local broker:
//!
//! ```text
//! docker run --rm -p 1883:1883 eclipse-mosquitto:2 \
//!     mosquitto -c /mosquitto-no-auth.conf
//! cargo test --test delivery -- --ignored
//! ```

use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use sparkplug_mqtt::{
    DEFAULT_CONNECT_TIMEOUT, DeviceId, EdgeNode, EdgeNodeId, GroupId, MetricValue, MqttConfig,
    SparkplugClient,
};

/// The connection these tests open. `client_id` is `None`, so each run takes
/// a generated client id and two runs cannot collide on the broker.
fn local_config() -> MqttConfig {
    MqttConfig {
        broker_url: "localhost".to_owned(),
        broker_port: 1883,
        transport: Transport::Tcp,
        username: String::new(),
        password: String::new(),
        client_id: None,
        version: "spBv1.0".to_owned(),
    }
}

#[tokio::test]
#[ignore = "needs a broker on localhost:1883"]
async fn flush_confirms_a_publish_against_a_real_broker() {
    let client = SparkplugClient::connect(&local_config())
        .await
        .expect("should connect to the local broker");

    client
        .publish_metric(
            "PlanTest",
            "edge1",
            "pump1",
            "temperature",
            MetricValue::Float(42.5),
            0,
        )
        .await
        .expect("DDATA should enqueue");

    client
        .flush(Duration::from_secs(5))
        .await
        .expect("the broker should acknowledge the publish");

    assert_eq!(client.unacked(), 0);

    client
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown should flush clean");
}

#[tokio::test]
#[ignore = "needs a broker on localhost:1883"]
async fn an_edge_node_client_publishes_at_qos_zero_and_flushes_clean() {
    // The QoS 0 path is only fully true against a real broker: the message
    // goes out, the broker sends no PubAck, and the flush must still return
    // rather than wait for one that never comes.
    let edge_node = EdgeNode::new(
        GroupId::new("PlanTest").expect("group id"),
        EdgeNodeId::new("edge1").expect("edge node id"),
    );
    let client =
        SparkplugClient::connect_as_edge_node(&local_config(), edge_node, DEFAULT_CONNECT_TIMEOUT)
            .await
            .expect("should connect to the local broker");

    assert!(
        !client.tracks_delivery(),
        "an edge node client publishes at the spec QoS, so flush confirms nothing"
    );

    let topic = client.namespace().ddata(
        GroupId::new("PlanTest").expect("group id"),
        EdgeNodeId::new("edge1").expect("edge node id"),
        DeviceId::new("pump1").expect("device id"),
    );

    client
        .publish_metric_to(&topic, "temperature", MetricValue::Float(42.5), 0)
        .await
        .expect("DDATA should enqueue");

    assert_eq!(
        client.unacked(),
        0,
        "a QoS 0 publish is never acknowledged, so it must not be counted"
    );

    client
        .flush(Duration::from_secs(5))
        .await
        .expect("a flush must not wait for an acknowledgement that cannot come");

    client
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown should complete for an untracked client");
}

/// Read the topic of every message a subscriber of `spBv1.0/#` receives.
///
/// This returns once the broker confirms the subscription, so a message
/// published after the call cannot slip past. A task then polls the event
/// loop and sends each topic on.
async fn topic_listener() -> tokio::sync::mpsc::UnboundedReceiver<String> {
    // A broker treats two connections that share a client id as a takeover,
    // so a fixed name lets one run of these tests disconnect another. The
    // random suffix keeps two runs apart.
    let client_id = format!(
        "sparkplug-mqtt-topic-listener-{:08x}",
        rand::random::<u32>()
    );
    let mut options = MqttOptions::new(client_id, "localhost", 1883);
    options.set_clean_session(true);
    let (client, mut eventloop) = AsyncClient::new(options, 16);

    client
        .subscribe("spBv1.0/#", QoS::AtLeastOnce)
        .await
        .expect("the listener should subscribe");

    loop {
        match eventloop
            .poll()
            .await
            .expect("the listener should reach the broker")
        {
            Event::Incoming(Packet::SubAck(_)) => break,
            _ => continue,
        }
    }

    let (topics, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        // Hold the client, or dropping it closes the connection.
        let _client = client;
        while let Ok(event) = eventloop.poll().await {
            match event {
                Event::Incoming(Packet::Publish(publish)) => {
                    if topics.send(publish.topic).is_err() {
                        break;
                    }
                }
                _ => continue,
            }
        }
    });

    receiver
}

/// Wait for the first topic that carries `message_type`.
async fn topic_of(
    topics: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    message_type: &str,
) -> String {
    let arrived = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let topic = topics
                .recv()
                .await
                .expect("the listener should stay connected");
            if topic.contains(message_type) {
                return topic;
            }
        }
    });

    arrived
        .await
        .unwrap_or_else(|_| panic!("no {message_type} reached the bus within 5 s"))
}

#[tokio::test]
#[ignore = "needs a broker on localhost:1883"]
async fn a_birth_names_the_edge_node_of_the_connect_call_on_a_real_bus() {
    // The client id and the edge node carry different names here, and a
    // second client reads what the broker actually distributes.
    let mut topics = topic_listener().await;

    let config = MqttConfig {
        client_id: Some("stax-publisher-7".to_owned()),
        ..local_config()
    };
    let edge_node = EdgeNode::new(
        GroupId::new("PlanTest").expect("group id"),
        EdgeNodeId::new("edge_of_record").expect("edge node id"),
    );

    let client = SparkplugClient::connect_as_edge_node(&config, edge_node, DEFAULT_CONNECT_TIMEOUT)
        .await
        .expect("should connect to the local broker");

    client
        .publish_birth(DeviceId::new("pump1").expect("device id"))
        .await
        .expect("the births should enqueue");

    assert_eq!(
        topic_of(&mut topics, "NBIRTH").await,
        "spBv1.0/PlanTest/NBIRTH/edge_of_record",
        "the birth must name the edge node given at connect, not the client id"
    );
    assert_eq!(
        topic_of(&mut topics, "DBIRTH").await,
        "spBv1.0/PlanTest/DBIRTH/edge_of_record/pump1"
    );

    client
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown should complete");
}
