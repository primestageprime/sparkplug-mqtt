# sparkplug-mqtt

SparkPlug B protocol client for MQTT — connect, publish, and subscribe to industrial IoT metrics.

## Quick Start

```toml
[dependencies]
sparkplug-mqtt = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

```rust
use sparkplug_mqtt::{MqttConfig, SparkplugClient, MetricValue};
use rumqttc::Transport;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), sparkplug_mqtt::SparkplugError> {
    let config = MqttConfig {
        broker_url: "localhost".to_owned(),
        broker_port: 1883,
        transport: Transport::Tcp,
        username: String::new(),
        password: String::new(),
        group_id: "Plant1".to_owned(),
        node_id: "edge1".to_owned(),
        version: "spBv1.0".to_owned(),
    };

    // Waits up to 5 s for ConnAck; returns Err on timeout.
    let client = SparkplugClient::connect(&config).await?;

    client.publish_birth("Plant1", "pump1").await?;

    loop {
        client.publish_metric(
            "Plant1", "pump1", "temperature",
            MetricValue::Float(42.5),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_millis() as u64,
        ).await?;

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
```

## Features

- **SparkPlug B protocol** — full protobuf encoding/decoding per the Eclipse SparkPlug B spec
- **Topic parsing** — parse and construct `spBv1.0/<group>/<type>/<node>[/<device>]` topics
- **Multiple metric types** — Float (f64), String, Bool, Int (u64)
- **Connection pooling** — `MqttClientManager` maintains per-group client connections
- **TLS support** — `tls_transport()` loads system root certificates
- **Robust event loop** — automatic reconnection with backoff on errors
- **Async/await** — built on tokio and rumqttc

## Architecture

| Module | Purpose |
|--------|---------|
| `client` | MQTT connection management and pooling |
| `sparkplug` | SparkPlug B protocol types, topic parsing, `SparkplugClient` |
| `payload` | Protobuf message encoding/decoding (generated from `sparkplugb.proto`) |
| `error` | `SparkplugError` — all error types in one enum |

## Known Limitations

- **No connection health signal.** `publish()` enqueues messages into an internal buffer and returns `Ok(())` even if the MQTT connection is currently down. The background event loop will retry delivery, but there is no way for callers to detect a permanently broken connection. A health-check mechanism (e.g., a `watch` channel) is planned for a future release.
- **Sequence numbers are hardcoded.** The SparkPlug B spec requires `seq` to increment 0-255. This crate currently hardcodes `seq` to 0 (births) or 1 (data). A proper sequence counter is planned for a future release.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
