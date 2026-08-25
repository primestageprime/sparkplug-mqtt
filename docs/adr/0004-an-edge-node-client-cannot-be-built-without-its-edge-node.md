---
status: accepted
---

# An edge node client cannot be built without its edge node

`SparkplugClient` holds a private `Identity`, which is either `Publisher` or
`EdgeNode(OwnedEdgeNode)`. The role is read off that value rather than
stored beside it. `connect_as_edge_node(config, edge_node, timeout)` takes
the edge node as one checked pair, `EdgeNode`, and `publish_birth` announces
that pair. `MqttConfig` keeps the connection alone and gains
`client_id: Option<String>`.

## Why the decision was needed

`MqttConfig.node_id` did two jobs. `generate_client_id` put it in the MQTT
client id, which names one connection. `publish_birth` announced an edge node
under it, which names a Sparkplug entity. `publish_metric` took the edge node
per call. So one field named a connection and an entity at once, and the two
publish paths read different sources for the same thing.

Where the two differed, the births announced one edge node and the data
carried another. `seq` counts the messages of one edge node, and the pair
`group_id/edge_node_id` names the edge node that owns the count, so the
births and the data counted apart. A host application then reads a gap in
both streams and asks for a rebirth it will read the same way.

Only a comment in `publish_birth` stated the rule:

> Both births name this client's own edge node, while `publish_metric` takes
> the edge node per call. Where they differ, births and data land on
> different edge nodes.

A survey of the two consuming repos shows the fields differ in practice.
`amygdala-stax-rs` opens five connections from one process, each with its own
`node_id` suffix — `{host_id}-publisher`, `{host_id}-override-reconciler`,
and three more — so that the broker does not read them as a takeover. Its own
configuration says so: "`node_id` is the MQTT-level client identifier."
Real DDATA goes out under a different per-publish `node_id`. So every one of
those five connections would announce a birth for an edge node that no data
ever names.

## Why the identity sits in the constructor

An edge node client speaks for one edge node for the length of its session.
The specification ties the death it registers, the births it announces, and
the `seq` it counts to that one entity, so the identity cannot vary per
publish and stay conformant. A value that cannot change after connect belongs
in the constructor.

`EdgeNode` carries the group and the edge node id together, as one `Copy`
pair of ADR-0002 newtypes. Sparkplug names an edge node by both segments, so
a caller that takes one segment from one place and the other from another
names an edge node that may not exist. The pair travels as one value to stop
that, on the same reasoning ADR-0002 gives for the segment types themselves.

`EdgeNode` borrows, and a client keeps its identity for the length of a
session, so the client cannot store that type. The identity holds
`OwnedEdgeNode` instead: the same pair as two `Arc<str>`, with private
fields and one constructor, `From<EdgeNode<'_>>`. The owned type is what
makes the guarantee a compile-time one. A pair of bare `Arc<str>` fields
would take any string, so a route that skipped `GroupId::new` could reach
the edge node role with an empty segment or one holding a `/`. A release
build then publishes to `spBv1.0/Plant/Floor/NBIRTH/` and keys `seq` on
`Plant/Floor/`, which is the key corruption the pair exists to prevent. With
`OwnedEdgeNode` there is no such route to write. `From<&OwnedEdgeNode>`
hands an `EdgeNode` back without a second check — ADR-0002 checks a value
once, where it enters its type — and `wrap_checked` stays
`pub(in crate::sparkplug::topic)`, so the module wall, not a convention,
keeps its `debug_assert` true.

The `role` field goes, and `fn role(&self)` reads the identity instead. This
is the load-bearing part. `tests.rs` builds `SparkplugClient` field by field,
which is the widest route into a client the crate has, so a guarantee that
only a constructor holds is no guarantee at all. With one field, the edge
node role has nowhere to exist without a checked edge node beside it, and
the widest route proves it rather than working around it: `wired_edge_node`
takes an `EdgeNode`, because `Identity::EdgeNode` accepts nothing else.

## Considered options

**Validate `connect_as`** — keep the constructor ADR-0003 added and reject
`Role::EdgeNode` when the config names no edge node. Rejected. `Role` is a
fieldless enum, so `Role::EdgeNode` with no identity is exactly the state
this decision removes; a runtime check leaves it representable and adds an
error for a state the type system can refuse. `connect_as` is unreleased —
ADR-0003 landed it on this branch and no version carries it — so removing it
breaks no caller.

The session conformance plan settles it. It registers an NDEATH Will in the
CONNECT packet of an edge node client, and a Will must name a topic before
the socket opens. A constructor that can reach the edge node role with no
identity has no topic to register, so that plan would have to add the same
argument later and break every caller a second time.

**An `edge_node` field on `MqttConfig`** — rejected on ADR-0003's reasoning
against a `role` field there. The struct has struct-literal sites across two
consumer repos, and a field that only one of the two roles reads makes the
other role's callers name a value the client ignores.

**Keep `group_id` on `MqttConfig`** — rejected. It only ever prefixed the
client id; every publish took its own group. A caller that supplies a client
id needs neither `group_id` nor `node_id`, and an edge node client names its
group in `EdgeNode` instead. So `generate_client_id` takes the version alone
and formats `{version}_{random 32-bit hex}`, and `client_id` lets a caller
name a connection itself. `version` is the same string for every caller, so
the suffix carries all of the difference and must be wide: a broker reads
two connections with one client id as a takeover and disconnects the earlier
session, which the event loop reconnects. `amygdala-stax-rs` builds five
distinct names today by hand; `client_id` is where they now go.

## Consequences

This is a breaking change for every caller. `MqttConfig` loses two fields and
gains one, `publish_birth` takes a `DeviceId` alone, and `connect_as` is
gone. `connect` and `connect_with_timeout` keep their signatures and behave
as before.

**`publish_birth` on a publisher client stays a runtime check.** It returns
the new `SparkplugError::NotAnEdgeNode`. The compiler cannot refuse the call,
because one type serves both roles and the method sits on that type. Making
it unrepresentable needs the client-type split ADR-0003 deferred — an
`EdgeNode` client that has `publish_birth` and no `flush`, and a `Publisher`
client that has the reverse. This decision does not deliver that. What it
delivers is narrower and worth stating plainly: a client in the edge node
role always names an edge node whose group and edge node id both passed the
segment check, and no route in the crate can build one that does not. So a
birth can never announce an entity the data does not carry, and no publish
of such a client can land on a topic with a missing segment. A publisher
client can still call `publish_birth`, and it learns at runtime.

`Role` keeps its exact public shape — fieldless, `Copy`, two variants —
because `wire_options` reads it and ADR-0003 fixes what it decides. It is now
derived from the identity rather than stored, so the two cannot disagree.

The `seq` defect closes with the interface. The births and the data of one
edge node draw from one count, and a test reads all three numbers — 0, 1, 2 —
off the wire. G1, G2 and G3 of `docs/sparkplug-conformance-gaps.md` stay
open: `bdSeq`, the NDEATH Will and the rebirth after a reconnect are still
missing, so an edge node client is not yet a conformant edge node. CONTEXT.md
records that gap.
