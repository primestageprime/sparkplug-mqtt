//! Publish a metric to an MQTT broker every 30 seconds.
//!
//! Usage:
//!   cargo run --example publish_every_30s
//!
//! Requires an MQTT broker running on localhost:1883.

use sparkplug_mqtt::{MqttConfig, MetricValue, SparkplugClient};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), sparkplug_mqtt::SparkplugError> {
    tracing_subscriber::fmt::init();

    let config = MqttConfig {
        broker_url: "localhost".to_owned(),
        broker_port: 1883,
        transport: rumqttc::Transport::Tcp,
        username: String::new(),
        password: String::new(),
        group_id: "Example".to_owned(),
        node_id: "example-node".to_owned(),
        version: "spBv1.0".to_owned(),
    };

    let client = SparkplugClient::connect(&config).await?;

    client.publish_birth("Example", "sensor1").await?;
    println!("Published birth certificates for sensor1");

    loop {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_millis() as u64;

        client
            .publish_metric(
                "Example",
                "sensor1",
                "temperature",
                MetricValue::Float(20.0 + rand::random::<f64>() * 10.0),
                timestamp_ms,
            )
            .await?;

        println!("Published temperature reading at {timestamp_ms}");
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}
