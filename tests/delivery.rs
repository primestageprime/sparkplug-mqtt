//! Broker-backed checks. Run them with a local broker:
//!
//! ```text
//! docker run --rm -p 1883:1883 eclipse-mosquitto:2 \
//!     mosquitto -c /mosquitto-no-auth.conf
//! cargo test --test delivery -- --ignored
//! ```

use std::time::Duration;

use rumqttc::Transport;
use sparkplug_mqtt::{
    DEFAULT_CONNECT_TIMEOUT, DeviceId, EdgeNodeId, GroupId, MetricValue, MqttConfig, Role,
    SparkplugClient,
};

fn local_config() -> MqttConfig {
    MqttConfig {
        broker_url: "localhost".to_owned(),
        broker_port: 1883,
        transport: Transport::Tcp,
        username: String::new(),
        password: String::new(),
        group_id: "PlanTest".to_owned(),
        node_id: "edge1".to_owned(),
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
    let client =
        SparkplugClient::connect_as(&local_config(), Role::EdgeNode, DEFAULT_CONNECT_TIMEOUT)
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
