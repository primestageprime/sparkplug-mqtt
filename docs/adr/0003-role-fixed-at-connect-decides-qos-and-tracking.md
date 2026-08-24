---
status: accepted
---

# A role fixed at connect decides QoS, retain, and delivery tracking

`Role` is chosen once, when a client connects. It fixes the QoS and the
retain flag of every publish, and with them whether the delivery tracker
counts the message. `Role::Publisher` publishes at QoS 1 and is the default.
`Role::EdgeNode` publishes at the QoS the specification fixes.

One function, `wire_options(role, message_type)`, answers all three
questions. The publish path calls it once per message.

## Why the decision was needed

`MessageType::spec_qos` and `MessageType::spec_retain` stated the
specification's rules and reached no call site outside their own tests, while
the publish path hardcoded QoS 1 and retain false for every message. The
rules were tested and the wire ignored them, which is G4 of the conformance
audit.

Two facts that must agree also sat apart. The QoS lived in `types.rs`.
Whether the tracker may count a publish lived in `delivery.rs`, as a doc
comment: "Do not call this for QoS 0. The broker never acknowledges QoS 0, so
a counted message would never clear." Nothing enforced it.

## The conflict this resolves

The specification sets DDATA to QoS 0. The last two releases built a delivery
guarantee — `flush`, `unacked`, `PublishLost` — on QoS 1 acknowledgements.
dside #32865 step 3 gates a database write on `flush` for DDATA carrying
operator overrides, because today STAX stamps a row when the publish is only
enqueued.

So conformance and the delivery guarantee are in direct conflict, on the same
message type. A role separates them: the traffic that needs confirmation is
published by a client that does not claim to be an edge node.

## Considered options

**Per-publish QoS** — every publish method gains an argument. Rejected on
ADR-0002's reasoning: three traits in `amygdala-stax-rs` are implemented on
`publish_metric`, and that signature stays pinned. The QoS also follows the
role rather than varying message by message, so the argument would be
loop-invariant at every call site — the same finding that sank the publish
handle.

**Always QoS 1** — the status quo. Rejected: it leaves G4 open by decision
and gives a conformant edge node no route at all.

**Always spec QoS** — conformant. Rejected: it silently breaks the guarantee
#32865 is built on, in the release where consumers first wire it up.

**A `role` field on `MqttConfig`** — rejected. The struct has eight public
fields and five struct-literal sites across the two consumer repos, so a
ninth field breaks all five. It would also collide with the separate proposal
to split the client id from the edge node identity (dside #32942).

**Splitting the client type**, so an `EdgeNode` client has no `flush` — the
strongest answer, and the house preference for compile-time proof. Deferred,
not rejected. It doubles the client type for a role whose lifecycle the crate
cannot run yet. **Revisit when the session conformance plan lands the NDEATH
Will, `bdSeq`, `seq`, and the rebirth** — at that point an `EdgeNode` client
is a real thing and the split pays for itself.

## Consequences

`connect` and `connect_with_timeout` keep their signatures and delegate with
`Role::Publisher`, so raising the crate version changes no existing caller's
behaviour. `connect_as(config, role, timeout)` is the single connect path,
and `DEFAULT_CONNECT_TIMEOUT` becomes public so the default stays nameable.

`flush` returns `Ok` for a client that tracks nothing, because nothing is
outstanding. That is true but easy to misread as confirmation, so
`tracks_delivery` states the role's rule directly. A caller that gates a
durable write on `flush` asserts it once at startup.

G4 does not close here. This lands the mechanism; G4 closes when an
`EdgeNode` client actually runs, which the session conformance plan builds.
Flipping the default instead would ship QoS 0 to consumers that have raised
their pin but not yet wired the guarantee they raised it for.

`Role::EdgeNode` spends a glossary term early. CONTEXT.md defines an edge
node client as one that runs the full session lifecycle, and this role runs
one quarter of it. The term names the destination deliberately, and CONTEXT.md
records the gap so the name is not read as a promise the code keeps.

`flume` joins the dev-dependencies. `rumqttc::AsyncClient::from_senders` takes
its `Sender`, which lets a test read the QoS and retain flag a publish
actually carries instead of asserting a table. `rumqttc` already depends on
it, so no extra crate compiles.
