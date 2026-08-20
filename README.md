# sparkplug-mqtt

SparkPlug B protocol client for MQTT — connect, publish, and subscribe to industrial IoT metrics.

## Quick Start

```toml
[dependencies]
sparkplug-mqtt = "0.4"
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
            "Plant1",       // group
            "edge1",        // edge node
            "pump1",        // device
            "temperature",  // metric
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
- **Topic namespace** — `Namespace` builds and parses every Sparkplug topic, including
  the three-segment `spBv1.0/STATE/<host>` form. Parsing is strict: a topic addressed
  with the wrong shape for its message type is reported, not reinterpreted
- **Identifiers that cannot be swapped** — `GroupId`, `EdgeNodeId` and `DeviceId` are
  checked once and are not interchangeable, so a transposed argument stops compiling
- **Multiple metric types** — Float (f64), String, Bool, Int (u64)
- **Delivery you can confirm** — `flush` waits for the broker and reports what a
  reconnect discarded, instead of reporting an enqueued message as delivered
- **TLS support** — `tls_transport()` loads system root certificates
- **Robust event loop** — automatic reconnection with backoff on errors
- **Async/await** — built on tokio and rumqttc

## Publishing without transposing an identifier

`publish_metric` takes four adjacent `&str`s — group, edge node, device, metric
name. Nothing stops you passing them in the wrong order. The checked path does:

```rust
use sparkplug_mqtt::{GroupId, EdgeNodeId, DeviceId, MetricValue};

// group is usually fixed for a batch, so check it once
let group = GroupId::new("Plant1")?;

for reading in readings {
    let node = EdgeNodeId::new(&reading.asset_id)?;
    let device = DeviceId::new(&reading.device_id)?;

    let topic = client.namespace().ddata(group, node, device);
    client
        .publish_metric_to(&topic, "temperature", MetricValue::Float(reading.value), reading.ts)
        .await?;
}
```

`ddata` cannot fail — each identifier was checked when it was built — and it
will not compile with its arguments transposed. The six-argument
`publish_metric` still works and is not deprecated.

## Confirming delivery

`publish_metric` returns when the message enters the internal queue, not when
the broker has it. Call `flush` before you record the message as delivered.

```rust
for row in rows {
    client.publish_metric(&group, &node, &device, &row.name, row.value, row.ts).await?;
}

// Only record the rows as published once the broker has them.
match client.flush(Duration::from_secs(5)).await {
    Ok(()) => mark_published(&rows).await?,
    Err(e) => tracing::warn!("not recording as published: {e}"),
}
```

The publish methods are not cancel-safe. A cancelled publish leaves `unacked`
one too high, so every later `flush` reports `SparkplugError::FlushTimeout`.
Only a disconnect clears that count. Do not wrap one publish in
`tokio::time::timeout`. Do not put one publish in a `select!` branch that
another branch can cancel. Apply the deadline around the whole batch, and use
the `timeout` argument of `flush`.

Call `client.shutdown(Duration::from_secs(5))` before a short-lived process
exits. `shutdown` flushes, sends a DISCONNECT, and then stops the event loop.
A plain drop aborts the event loop at once, so the last messages can go
missing. The timeout bounds the whole sequence. `shutdown` waits for the
event loop to write the DISCONNECT packet, and reports
`SparkplugError::ShutdownTimeout` when it cannot confirm the packet in time.
`shutdown` reports `SparkplugError::Disconnect` when the client refuses the
DISCONNECT request, which names the cause instead of the deadline.

## Architecture

| Module | Purpose |
|--------|---------|
| `client` | MQTT connection setup — options, transport, `mqtt_parts` |
| `sparkplug` | SparkPlug B protocol types, the topic namespace, `SparkplugClient` |
| `payload` | Protobuf message encoding/decoding (generated from `sparkplugb.proto`) |
| `error` | `SparkplugError` — all error types in one enum |

## Known Limitations

- **A publish is not a delivery.** `publish_metric` and `publish_metrics` put the message in an internal queue and return `Ok(())`, even when the connection is down. Call `flush(timeout)` to wait until the broker acknowledges every message. Call `shutdown(timeout)` to drain the queue before a short-lived process exits. Sparkplug needs `clean_session = true`, so a disconnect discards the queued messages. `flush` reports that loss as `SparkplugError::PublishLost` instead of hiding it. Use `health()` to watch the link state.
- **Sequence numbers are hardcoded.** The SparkPlug B spec requires `seq` to increment 0-255. This crate currently hardcodes `seq` to 0 (births) or 1 (data). A proper sequence counter is planned for a future release.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
