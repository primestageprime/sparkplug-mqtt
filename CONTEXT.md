# sparkplug-mqtt

A Rust client for the Eclipse Sparkplug B protocol over MQTT. This glossary
fixes the vocabulary of the Sparkplug domain as this crate uses it. Several
Sparkplug terms collide with MQTT terms that sound the same, so the
distinctions below are load-bearing.

## Language

### Addressing

**Namespace**:
The root segment of every Sparkplug topic, which names the protocol version.
The standard value is `spBv1.0`.
_Avoid_: version, prefix, protocol

**Topic**:
The full path a Sparkplug message is published to, made of a namespace and
the segments that address a group, an edge node, a device, or a host.
_Avoid_: channel, subject, path

**Shape**:
Which segment layout a topic uses. A node topic carries a group and an edge
node. A device topic adds a device. A host topic carries a host id alone.
_Avoid_: kind, variant, form, level

**Group**:
A named collection of edge nodes that share a bus. It appears as one topic
segment and has no meaning beyond addressing.
_Avoid_: tenant, site, namespace

**Identifier**:
A checked value that can stand as one topic segment: non-empty, and free of
`/`, `+` and `#`. Each kind of identity has its own identifier type, so one
cannot be passed where another is meant.
_Avoid_: id string, key, name

### Identities

**Edge node**:
A Sparkplug entity that owns one MQTT session, announces its own birth and
death, and publishes on behalf of its devices.
_Avoid_: node, client, asset, gateway

**Device**:
An asset that reports metrics through an edge node. It has no session of its
own and depends on its edge node's session for its lifecycle.
_Avoid_: sensor, thing, endpoint, asset

**Host id**:
The identifier of a host application, carried by a STATE topic. It is not a
group, an edge node, or a device, and never appears alongside them.
_Avoid_: host name, application id, consumer id

**Client id**:
The MQTT-level identifier for one connection. Distinct from an edge node —
one process may open several connections, each needing its own client id,
without any of them being an edge node.
_Avoid_: node id, connection name

**Host application**:
A consumer that subscribes across the bus and reassembles state from births,
deaths, and data. A Primary Host Application is the one an edge node watches
to decide whether the bus is live.
_Avoid_: subscriber, listener, consumer

### Roles this crate offers

**Role**:
Which of the two roles below a client fills. It is fixed when the client
connects, and it decides the QoS and the retain flag of every publish. The
type is `Role`, with the variants `EdgeNode` and `Publisher`.
_Avoid_: mode, kind, profile

**Edge node client**:
A client that runs the full Sparkplug session lifecycle for its own
configured edge node — death registration, births, commands, and the QoS the
specification fixes.

`Role::EdgeNode` selects the QoS and the retain flag today. Every client
also counts `seq` for each edge node it publishes for, and the pair
`group_id/edge_node_id` names that edge node. Death registration, `bdSeq`,
and the rebirth after a reconnect are not implemented, so a client in that
role is not yet a conformant edge node. The term names the destination; the role is
where the rest lands.

**Publisher client**:
A client that publishes data on behalf of edge nodes it does not own. It runs
no lifecycle and must never announce a death, because the identity is
borrowed per publish. It publishes at QoS 1 rather than the QoS the
specification fixes, so its caller can confirm delivery. This is `Role::Publisher`, and it
is the default.
_Avoid_: producer, writer

### Messages

**Message type**:
What a message means in the session lifecycle — birth, death, data, or
command — at either node or device level. It fixes the topic shape, the QoS,
and the retain flag.
_Avoid_: verb, command, event type

**Birth**:
The message that declares an entity is online and lists every metric it will
report, with datatypes and initial values. No data may reference a metric
that a birth did not declare.
_Avoid_: announce, register, hello

**Death**:
The message that declares an entity is offline. An edge node's death is
registered with the broker at connect time, so the broker sends it if the
session drops without warning.
_Avoid_: goodbye, offline, disconnect

**Rebirth**:
A fresh birth published after a reconnect or on a host's request, because a
new session invalidates everything the previous births declared.

**Metric**:
A named, typed, timestamped value carried in a payload.
_Avoid_: tag, point, reading, signal

### Delivery

**Enqueued**:
A publish the in-process client has accepted but the broker has not
acknowledged. This crate's publish methods return once a message is
enqueued, not once it is delivered.
_Avoid_: sent, published, submitted

**Lost**:
A publish the broker never acknowledged and never will, because a reconnect
discarded it. Sparkplug requires a clean session, so the broker keeps no
state to resend from and only the caller can publish it again.
_Avoid_: dropped, failed, missed

**Flush**:
Waiting until the broker has acknowledged every publish since the last such
wait, and reporting anything lost in that window.
_Avoid_: drain, sync, commit

**Tracked**:
Whether the crate counts a publish toward a flush. A publish is tracked when
the broker will acknowledge it, which means QoS 1. A QoS 0 publish is
untracked: no acknowledgement can arrive, so counting it would hold every
later flush open. The role decides this for every message a client sends —
read it with `tracks_delivery`.
_Avoid_: confirmed, guaranteed, reliable
