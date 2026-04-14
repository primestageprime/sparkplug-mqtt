# Changelog

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
