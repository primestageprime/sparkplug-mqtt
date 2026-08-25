# Changelog

## 0.5.0

A breaking release. `connect`, `connect_with_timeout`, `publish_metric`,
`publish_metrics`, `publish_metric_to` and `publish_metrics_to` keep their
signatures — but `MqttConfig` loses `node_id` and `group_id`,
`publish_birth` takes one argument, `seq` counts for each edge node instead
of holding a literal, and the payload builders, the datatype tables, the
hand-rolled byte decoder and `pub mod util` leave the crate interface.

### Breaking changes

- `MqttConfig.node_id` and `MqttConfig.group_id` are removed, and
  `client_id: Option<String>` takes their place. `node_id` named two things
  at once: `generate_client_id` put it in the MQTT client id, which names one
  connection, and `publish_birth` announced an edge node under it, which
  names a Sparkplug entity. `publish_metric` took the edge node per call, so
  where the two differed the births announced one edge node and the data
  carried another — and the `seq` counts of the two ran apart, which a host
  application reads as a gap in both streams. `group_id` only ever prefixed
  the client id; every publish took its own group. Set `client_id` to name a
  connection yourself, which one process with several connections must do,
  or leave it `None` for a generated one. See ADR-0004.
- `generate_client_id` takes `&str`, the version, rather than `&MqttConfig`,
  and formats `{version}_{random 32-bit hex}` — for example
  `spBv1.0_9f3c1ba7`. The group and the edge node left the config, so neither
  can prefix a client id, and `version` is the same string for every caller.
  The suffix therefore carries all of the difference between two generated
  ids. A broker reads two connections with one client id as a takeover and
  disconnects the earlier session, which the event loop then reconnects.
  `mqtt_options` calls this only when `client_id` is `None`.
- `SparkplugClient::connect_as(config, role, timeout)` is removed. It is
  unreleased — it was added and removed inside this cycle, so no entry below
  adds it — and it could reach `Role::EdgeNode` with no edge node beside it,
  which is the state ADR-0004 removes.
  `connect_as_edge_node(config, edge_node, timeout)` replaces it and takes
  the identity the role needs. `connect` and `connect_with_timeout` keep
  their signatures and their behaviour.
- `SparkplugClient::publish_birth` takes one argument, `DeviceId<'_>`. The
  group and the edge node come from the identity the client took at connect,
  so the births and the data of that edge node name one edge node and draw
  from one `seq` count. A client that connected as a publisher owns no edge
  node and returns the new `SparkplugError::NotAnEdgeNode`. That stays a
  runtime check: refusing the call at compile time needs the client-type
  split ADR-0003 deferred.
- `EdgeNode` is a new public type: the checked pair of a `GroupId` and an
  `EdgeNodeId` that names one edge node, read back through `group()` and
  `edge_node_id()`. `connect_as_edge_node` takes it. Sparkplug names an edge
  node by both segments, so the pair travels as one value — the same
  reasoning ADR-0002 gives for `GroupId` and `EdgeNodeId` themselves. A
  client in the edge node role stores that pair and nothing weaker, so no
  route in the crate reaches that role with a segment that failed the
  check.
- `SparkplugError::NotAnEdgeNode` is a new variant. The enum is
  `#[non_exhaustive]`, so a caller that matches with a wildcard arm needs no
  change.
- `decode_metric_value_to_string` is removed, with its crate re-export. It
  decoded big-endian bytes for Int8 through String, which protobuf already
  carries as typed `oneof` variants. Its only caller reached it when the
  value was `bytes_value`, and the specification fills that field for Bytes
  and File alone — so its Int, Float and Boolean arms either never ran or
  misread their input. This closes G13 of
  `docs/sparkplug-conformance-gaps.md`.
- `Metric::decode_value_to_string` is removed. It dispatched to the function
  above for a `bytes_value` and to `format_value` for everything else. Call
  `Metric::format_value`, which reads the protobuf variants directly.
- `encode_type` and `decode_type` are removed, with their crate re-exports.
  They held the datatype table twice, in opposite directions, with nothing
  keeping the two in step. `encode_type` took a string, so a misspelt name
  returned `0` — Unknown — and nothing reported it. Use `DataType` below.
  Neither function had a caller outside this crate.
- `pub mod util` is removed, with `get_current_timestamp`. `Timestamp::now`
  reads the clock now, and it is the only route to it in the crate. The
  module exported one five-line function, so its interface was as wide as
  its implementation, and two payload builders called it directly rather
  than through `Timestamp::now` — one clock with two routes.
- `create_metric`, `create_payload`, `create_birth_certificate` and
  `create_device_birth_certificate` leave the crate interface. They built
  the payloads of the publish path, and that path is the one caller they
  have: no caller exists in amygdala-rs, in amygdala-stax-rs, or in
  `examples/publish_every_30s.rs`. `create_metric` is now private to
  `payload_helpers`, and the other three are visible inside `sparkplug`
  alone. Build a payload through `SparkplugClient::publish_metric_to` or
  `publish_metrics_to`.
- The same four builders changed shape as they moved. Each takes a
  `Timestamp` instead of reading the clock, `create_payload` takes a
  `Timestamp` rather than an `Option<Timestamp>`, and the three payload
  builders take a `seq` that only the client can supply. Every payload
  builder is now a pure function of its arguments, so a test states the
  instant it expects instead of asserting the value is above zero. No caller
  outside the crate can reach them, so no caller outside the crate changes.
- `SparkplugTopic::group_id`, `node_id`, `device_id` and `host_id` return
  `Option<GroupId<'_>>`, `Option<EdgeNodeId<'_>>`, `Option<DeviceId<'_>>` and
  `Option<HostId<'_>>` rather than `Option<&str>`. A topic holds segments
  that passed their check when it was built, so an accessor that hands back
  `&str` makes the next caller check them again — and ADR-0002 checks a value
  once, where it enters its type. The client reads the group and the edge
  node of every publish to draw its `seq`, and it now handles no error that
  cannot happen. Call `as_str()` on the identifier for the value the
  accessor used to return.

`Metric::numeric_value` and `Metric::format_value` are unchanged.

### Fixed

- `seq` was three literals: `create_payload` wrote 1, and the two birth
  builders wrote 0. `seq` counts the messages of one edge node, so no literal
  can be right. A host application reads the count to find a message it did
  not receive, and a literal writes one value in every message, so no count
  can show a gap. `SeqCounters` now holds one count for each edge node. An
  NBIRTH sets that count to 0, every message after it adds 1, and the count
  wraps from 255 back to 0. The pair `group_id/edge_node_id` names the edge
  node that owns the count, as Sparkplug requires: the same node id in two
  groups names two edge nodes, and each one counts alone. The client draws
  the count in one place, from the topic of the message, so two edge nodes
  that share one client keep separate counts. That one place reads the
  message type of the topic, so every publish path restarts the count at an
  NBIRTH — including a caller who builds an NBIRTH topic and sends it
  through `publish_metric_to`. A publish the client refuses,
  and a call the caller cancels, each leave one number off the wire, so a
  host application reads a gap and asks for a rebirth. The counter does not
  roll back, because a rollback races a publish from another task and would
  give one number to two messages. Two tasks that publish for one edge node
  on one client can still reach the broker in the other order, because the
  client draws the number before it waits for room in a bounded channel — so
  publish for one edge node from one task. This closes G5 of
  `docs/sparkplug-conformance-gaps.md`. `bdSeq`, the NDEATH Will and the
  rebirth after a reconnect stay open — see G1, G2 and G3.
- `publish_birth` read the clock twice, so an NBIRTH and the DBIRTH beside
  it could carry different milliseconds. One birth event now carries one
  instant, and each birth payload stamps its own metric to match.

### Added

- `Role`, with the variants `Publisher` and `EdgeNode`. It is fixed when a
  client connects and decides the QoS and the retain flag of every publish,
  and with them whether `flush` can confirm a message. See ADR-0003.
- `SparkplugClient::connect_as_edge_node(config, edge_node, timeout)`, which
  connects in `Role::EdgeNode` for the edge node named there. `connect` and
  `connect_with_timeout` keep their signatures and connect as a publisher,
  so no existing caller changes behaviour.
- `SparkplugClient::tracks_delivery`. It reports whether `flush` can confirm
  what this client publishes. Assert it once at startup before gating a
  durable write on `flush`. A `Role::EdgeNode` client publishes at QoS 0,
  which the broker never acknowledges, so its `flush` returns `Ok` without
  confirming anything.
- `DEFAULT_CONNECT_TIMEOUT` is now public, so the wait `connect` uses can be
  named when calling `connect_as_edge_node`.
- `DataType`, an enum of the 21 datatypes the Sparkplug B specification
  numbers. `DataType::code` writes the wire number and `DataType::from_code`
  reads one back, returning `None` for a number no datatype claims. One
  table generates both directions, so adding a datatype is one line. This
  unblocks G10 of `docs/sparkplug-conformance-gaps.md`.

### Changed

- `MessageType::spec_qos` and `MessageType::spec_retain` now have a caller
  inside the crate. They stated the specification's rules while the publish
  path hardcoded QoS 1 and retain false, so the rules were tested and the
  wire ignored them. Both stay public and unchanged.
- The delivery tracker no longer counts a publish the broker will not
  acknowledge. Its own documentation asked for this — "Do not call this for
  QoS 0" — and nothing enforced it.
- Both GitHub workflows now start an `eclipse-mosquitto:2` container and run
  `cargo test -- --ignored`. The broker-backed tests in `tests/delivery.rs`
  carry `#[ignore]`, so a bare `cargo test` skipped them, and one of them
  holds the only proof that a birth names the edge node of the connect call
  on a real bus.
- The listener in `tests/delivery.rs` takes a random client id. It used a
  fixed one, so two overlapping runs disconnected each other.

This does not close G4 of the conformance audit. Every client that exists
today is a `Role::Publisher` and still publishes at QoS 1. G4 closes when an
`EdgeNode` client runs the session lifecycle, which is separate work.

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
