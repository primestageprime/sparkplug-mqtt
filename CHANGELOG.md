# Changelog

## 0.3.0

Every change in this release is additive. `connect`, `publish_metric`,
`publish_metrics`, and `publish_birth` keep their signatures, and `MqttConfig`
keeps its eight fields.

### Added

- `SparkplugClient::flush(timeout)` waits until the broker acknowledges every
  publish from this client. Call it before you record a message as delivered.
- `SparkplugClient::unacked()` reports how many publishes the broker has not
  acknowledged yet.
- `SparkplugClient::shutdown(timeout)` flushes, sends a DISCONNECT, and stops
  the event loop. Call it before a short-lived process exits.
- `SparkplugClient::connect_with_timeout(config, timeout)` replaces the
  hardcoded five-second connect timeout. `connect` still uses five seconds.
- `SparkplugClient::health()` returns a `tokio::sync::watch::Receiver<Health>`
  that reports `Health::Connected` or `Health::Disconnected`.
- `DeliveryTracker` counts unacknowledged publishes behind the client.
- `SparkplugError::PublishLost { count }` reports the publishes a disconnect
  discarded. Sparkplug needs `clean_session = true`, so the broker keeps no
  session state and the client cannot resend them.
- `SparkplugError::FlushTimeout { after, unacked }` reports that the broker
  stayed silent.
- `SparkplugError::ShutdownTimeout { after }` reports that `shutdown` could not
  confirm the DISCONNECT packet. The `timeout` argument bounds the whole
  shutdown sequence, and `shutdown` waits for the event loop to write the
  packet rather than for a fixed delay.
- `SparkplugError::Disconnect(ClientError)` reports that the client refused the
  DISCONNECT request that `shutdown` sent. `shutdown` used to report this as
  `ShutdownTimeout`, which hid the cause.
- `PublishTicket` pairs a rollback with the publish that raised the count.
  `DeliveryTracker::record_publish` returns one, and
  `DeliveryTracker::record_publish_failed` takes it. A refused publish
  therefore cancels only its own count, never a count that a disconnect
  cleared or that belongs to a publish from another task.
- The publish methods document their cancel-safety contract. A cancelled
  publish leaves `unacked` one too high, so every later `flush` reports
  `FlushTimeout`. Apply the deadline around a whole batch instead.

## 0.2.0

### Breaking changes

- `SparkplugClient::publish_metric` and `publish_metrics` now take `node_id` as
  an explicit argument rather than using the `MqttConfig.node_id` baked in at
  connect time. A single client can therefore publish DDATA on behalf of many
  edge nodes.
- `publish_birth` still uses `self.node_id` (the connected client's own
  identity) for its NBIRTH and DBIRTH topics.

### Migration

Before:

```rust
client.publish_metric(&group, &device, metric).await?;
```

After:

```rust
client.publish_metric(&group, &node_id, &device, metric).await?;
```

Pass `client.node_id()` (or the value used at connect time) to preserve the
previous behavior.

## 0.1.0

- Initial release: SparkPlug B protocol client for MQTT.
