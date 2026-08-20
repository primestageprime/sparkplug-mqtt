# Changelog

## 0.4.0

A breaking release. `connect`, `publish_metric`, `publish_metrics`, and
`publish_birth` keep their signatures, and `MqttConfig` keeps its eight
fields — but the deprecated `SparkplugPublisher`, the `MqttClient` wrapper
and its manager are removed, and the Sparkplug topic namespace moves behind
one module.

### Breaking changes

- `SparkplugPublisher` is removed. 0.2.0 deprecated it. It ran a second
  publish path with no delivery tracking, so `flush` could not confirm a
  message it accepted. Use `SparkplugClient`.
- `TimestampToMetrics` is removed. `SparkplugPublisher::publish_metrics` was
  its only caller.
- `MqttClient` is removed. It was shallow — five of its eight methods passed
  straight through to `rumqttc`. Use `mqtt_parts(&config)`, which returns the
  `(AsyncClient, EventLoop)` pair that `MqttClient::new` plus
  `into_async_client` used to produce in two steps. Read the namespace from
  your own `MqttConfig`.
- `MqttClientManager` is removed, with `MessageCallback` and
  `set_message_callback`. It ran a second event loop that duplicated the one
  behind `SparkplugClient`, without delivery tracking or a health signal.
  Open one `SparkplugClient` per group instead.
- `SparkplugPublisher::handle_command` goes with its type. It matched a
  rebirth request against `FloatValue(1.0)`, but the spec defines
  `Node Control/Rebirth` as Boolean, so it never matched a conformant host
  application. Command handling returns in a later release.
- `parse_sparkplug_topic` is removed. Use `Namespace::parse`, which validates
  a topic against the namespace you configured rather than a hardcoded
  `spBv1.0`.
- `VERSION` is removed. Nothing read it. Use `Namespace::sparkplug_b()`.
- `SparkplugTopic` no longer exposes public fields. Read it through
  `group_id()`, `node_id()`, `device_id()` and `host_id()`, which each return
  `Option<&str>` because the shape decides which identities a topic carries.
  It can only be built through `Namespace`, so holding one means the topic is
  addressable.

### Changed

- **Topic parsing is strict.** A topic whose segments do not match the shape
  its message type requires is now rejected instead of reinterpreted.
  `spBv1.0/g/NBIRTH/n/d` used to parse as an NBIRTH carrying a device; it now
  reports `SparkplugError::WrongTopicShape`. That error is distinct from
  `InvalidTopic`, so a host application can log a misaddressed message from a
  real publisher separately from unrelated traffic.
- `spBv1.0/STATE/<host_id>` now parses. It has three segments and names its
  message type second, so the old parser rejected every STATE topic.
- The nine `Namespace` constructors take checked identifiers and no longer
  return `Result`. Every identifier is checked when its type is built, so
  there is nothing left for a constructor to reject.

### Added

- `Namespace` owns the topic namespace in both directions. It builds every
  topic — `nbirth`, `ndata`, `ndeath`, `ncmd`, `dbirth`, `ddata`, `ddeath`,
  `dcmd` and `state` — and parses them back. Build one at connect time.
- `Shape` reports which segment layout a topic uses: `Node`, `Device` or
  `Host`. Read it through `SparkplugTopic::shape()`.
- `MessageType::shape()` gives the shape a message type requires.
- `MessageType::spec_qos()` and `MessageType::spec_retain()` state what the
  Sparkplug specification fixes for a message type. They state the
  specification, not what this crate currently sends.
- `SparkplugError::InvalidNamespace` reports a namespace that cannot stand as
  a topic segment. `connect` returns it when `MqttConfig.version` is empty or
  contains `/`, `+` or `#`, instead of publishing to a topic no broker
  accepts.
- `SparkplugError::WrongTopicShape` reports a well-formed topic that its
  publisher addressed with the wrong shape.
- `MessageType` is now `Copy`.
- `GroupId`, `EdgeNodeId`, `DeviceId` and `HostId` are checked identifiers.
  Each wraps a borrowed string that can stand as a topic segment. They are
  `Copy` and allocate nothing. They exist to stop a positional swap: a group,
  an edge node and a device are all `&str`, sit next to each other in a call,
  and mean entirely different things.
- `SparkplugClient::publish_metric_to(&topic, name, value, ts)` and
  `publish_metrics_to(&topic, metrics)` publish to a topic the caller built.
  Four arguments instead of six, and the identity segments cannot be passed
  in the wrong order. Prefer these to `publish_metric` and `publish_metrics`,
  which keep their signatures and are not deprecated.
- `SparkplugClient::namespace()` returns the namespace the client publishes
  on, so a caller can build topics for the methods above.
- `mqtt_parts(&config)` returns the `(AsyncClient, EventLoop)` pair for a
  Sparkplug connection. Use it when you drive your own event loop.
- `mqtt_options(&config)` returns the `MqttOptions` those parts are built
  from, so the settings can be read back without opening a socket.

### Migration

Before:

```rust
let publisher = SparkplugPublisher::new(mqtt_client);
publisher.publish_births(&group, &asset).await?;
publisher.publish_metrics(&group, &asset, frames).await?;
```

After:

```rust
let client = SparkplugClient::connect(&config).await?;
client.publish_birth(&group, &asset).await?;
client.publish_metrics(&group, &asset, &asset, metrics).await?;
client.flush(Duration::from_secs(5)).await?;
```

`metrics` is a `Vec<(String, MetricValue, u64)>` — one entry per metric, each
carrying its own timestamp. This replaces the per-timestamp grouping that
`TimestampToMetrics` provided. The `flush` call is the gain: the old path
could not tell an enqueued message from a delivered one.

**Check your edge node identity before you migrate.** `publish_births` used
`asset` as both the edge node and the device, so it published to
`{namespace}/{group}/NBIRTH/{asset}`. `SparkplugClient::publish_birth` uses
the `node_id` from `MqttConfig` instead, so it publishes to
`{namespace}/{group}/NBIRTH/{node_id}`. Where `asset` and `node_id` differ,
births move to a different edge node while `publish_metrics` keeps sending
data to the one you name per call. Set `node_id` to `asset` at connect time,
or open one client per edge node.

For topic parsing, before:

```rust
let parsed = parse_sparkplug_topic(topic)?;
let group = &parsed.group_id;
let device = parsed.device_id.as_deref();
```

After:

```rust
let ns = Namespace::sparkplug_b();   // build once, keep it
let parsed = ns.parse(topic)?;
let group = parsed.group_id();       // Option<&str>
let device = parsed.device_id();     // Option<&str>
```

`group_id()` and `node_id()` return `None` on a STATE topic, which the parser
now accepts. Handle that arm rather than unwrapping.

To publish through the checked path, before:

```rust
client.publish_metric(&group, &node, &device, "temperature", value, ts).await?;
```

After:

```rust
let group = GroupId::new(&group)?;        // loop-invariant — build it once
let node = EdgeNodeId::new(&node)?;
let device = DeviceId::new(&device)?;

let topic = client.namespace().ddata(group, node, device);
client.publish_metric_to(&topic, "temperature", value, ts).await?;
```

The old call still works and is not deprecated. The gain is that
`ddata(group, node, device)` will not compile with its arguments transposed,
where the six-argument form accepted any order of four `&str`s.

For a self-driven event loop, before:

```rust
let (client, eventloop) = MqttClient::new(config.clone());
let async_client = client.into_async_client();
```

After:

```rust
let (async_client, eventloop) = mqtt_parts(&config);
```

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
