---
status: accepted
---

# A per-edge-node gate holds the `seq` across the publish

`SeqCounters` holds one gate for each edge node — a `tokio::sync::Mutex<u8>`
behind an `Arc`, in a `std::sync::Mutex<HashMap<String, _>>`. `reserve` and
`reserve_reset` take the gate of one edge node, write the new count, and
return a `SeqReservation` that holds the gate. The reservation derefs to the
number the message carries. `commit` keeps that number, because the client
took the message. Every other end — a refusal, and the cancellation that
drops the publish future — drops the reservation, which writes the previous
number back under the gate.

This is the first lock in the crate that a future holds across an `await`.

## Why the decision was needed

`payload_for` drew a number from the counter and then awaited
`AsyncClient::publish`. Three losses came from that order, and the cancel
safety blocks of `publish.rs` stated all three:

1. A publish the client refused burned its number. The next message carried
   the number after it.
2. A cancelled call burned its number the same way, and leaked its delivery
   count. Every later `flush` then reported `FlushTimeout`.
3. Two tasks that published for one edge node could reach the client out of
   order. The task that drew 5 waited for room while the task that drew 6
   found room, so the broker sent 6 before 5.

A host application reads a gap and asks the edge node for a rebirth, so each
one costs a rebirth per event. The old documentation refused a rollback for a
reason it stated plainly: another task can publish for the same edge node
while the first call waits, so a rollback alone would give one number to two
messages. The gate is what removes that objection. The rollback runs under
the gate, so no other publish for that edge node can read the number in
between, and the same guard covers a refusal, a cancellation and the order.

## Why this lock crosses an `await`

`delivery.rs` and `eventloop.rs` set the convention this breaks. Both take a
`std::sync::Mutex`, read or write a few fields, and drop the guard in the
same statement list. Neither holds one across an `await`.

`AsyncClient::publish` resolves when the request enters `rumqttc`'s channel,
not when the broker sends a PubAck. That channel holds ten requests by
default. So the gate is held for the time the event loop needs to drain a
slot, and no part of that hold waits for the broker to answer. The gate also
guards one edge node, so a publish for another edge node takes another gate
and does not wait.

The bound is the channel, not a promise of speed. A client with no connection
fills those ten slots, and the publish then waits for the reconnect. Every
publish for that one edge node waits behind it, which is the same order the
wire needs anyway. A caller that wants a bound of its own wraps the publish
in `tokio::time::timeout`: the cancellation now restores the number and
releases the delivery count, which is what makes that safe to do.

The map guard is `std::sync` and the gate is `tokio::sync`, so the order of
the two matters. `gate_of` takes the map guard, clones one `Arc`, and returns
— the guard drops there, before `reserve_with` awaits the gate. Holding both
would make every publish future not `Send`, which `SparkplugClient::connect`
needs for the event loop task, and clippy's `await_holding_lock` names the
same fault.

## Considered options

**A rollback with no gate** — rejected, and the previous documentation says
why. Two tasks that publish for one edge node would then give one number to
two messages. A host application recovers from a gap by asking for a rebirth
and cannot recover from a repeated number, so a rollback without the order is
worse than the gap it removes.

**One gate over the whole map** — rejected. A publisher client publishes for
many edge nodes on one connection, and `amygdala-stax-rs` does exactly that.
One gate would hold every edge node behind whichever publish waits for room,
which turns a stall on one asset into a stall on the bus. The count of one
edge node is the only thing two publishes for that edge node share, so the
gate guards that.

**An atomic counter** — rejected. A `compare_exchange` restores the number
only when nothing else moved it, and it fixes neither the order the messages
reach the client in nor the delivery count a cancelled publish leaks.

**A task that owns the counts and the publishes** — the message-passing
answer, and the house preference for shared state. Rejected for now. It moves
the publish off the caller's task, so a caller that cancels no longer cancels
the publish, and the client would need one task per connection to keep the
order. The gate reaches the same guarantee inside the call the caller already
awaits.

**Keeping the gap and documenting it** — the status quo. Rejected: the
documentation told a caller not to use `timeout` and not to use `select!`
around a publish, which is a rule no caller can hold across a codebase.

## Consequences

`SeqCounters::new` stays a plain function, so `connect_with` and the test
constructor that builds the client field by field are unchanged. The publish
path is now the only async part: `seq_for` becomes `reserve_seq`, and every
publish that carries metrics runs through one function, `publish_metrics_on`,
which reserves, publishes, and commits.

`publish_birth` holds one reservation across both births. The NBIRTH carries
0 and the DBIRTH carries 1, no publish for that edge node draws a number
between them, and a DBIRTH the client refuses leaves the count where the
NBIRTH found it.

A `Role::Publisher` publish also holds a `PublishTicket` across the same
`await`. `DeliveryTracker::publish_in_flight` returns a guard that carries
the ticket and releases it through `record_publish_failed` when it drops, so
a cancelled publish releases its delivery count on the same path that
restores its number. A `Role::EdgeNode` data publish is QoS 0 and untracked
per ADR-0003, so it holds no ticket and only the number goes back.

**One window stays open.** `rumqttc` sends on a `flume` channel, and a
receiver takes the message out of the send future before that future
resolves. A cancellation that lands in that instant leaves the message on its
way to the broker while the reservation writes the number back, so the next
message for that edge node carries the number a second time. Nothing inside
this crate can close that window, because the publish future owns the
message. It is far narrower than the gap it replaces, which every refusal and
every cancellation produced.

The tests state what they can prove. `two_tasks_that_publish_for_one_edge_node_reach_the_wire_in_order`
runs on a single-threaded runtime with a channel that holds one request, so
it reads one interleaving every run and locks the order the numbers reach the
wire in. That runtime cannot produce the reorder itself, because a publish
with no gate draws its number and reaches the channel in one poll — the
reorder needs two threads. `one_edge_node_does_not_wait_behind_another` is
what holds the gate to one edge node rather than the map.

G1, G2 and G3 of `docs/sparkplug-conformance-gaps.md` stay open. `bdSeq`, the
NDEATH Will and the rebirth after a reconnect are still missing, so this
makes the count of an edge node client correct without making the client a
conformant edge node.
