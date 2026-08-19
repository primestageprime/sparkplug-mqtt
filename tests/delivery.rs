//! Broker-backed checks. Run them with a local broker:
//!
//! ```text
//! docker run --rm -p 1883:1883 eclipse-mosquitto:2 \
//!     mosquitto -c /mosquitto-no-auth.conf
//! cargo test --test delivery -- --ignored
//! ```

use std::time::Duration;

use rumqttc::Transport;
use sparkplug_mqtt::{MetricValue, MqttConfig, SparkplugClient};

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
