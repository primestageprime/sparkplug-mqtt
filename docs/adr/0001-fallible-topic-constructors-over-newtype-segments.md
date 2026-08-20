---
status: superseded by ADR-0002
---

# Fallible topic constructors instead of validated segment newtypes

The topic module holds one invariant: a `SparkplugTopic` that exists is
well-formed. A topic segment must therefore be non-empty and must not embed
a `/`. We validate that inside the constructors, which return
`Result<SparkplugTopic, SparkplugError>`, rather than making each segment a
newtype that cannot hold a bad value.

This deviates from the house preference for compile-time proof over runtime
validation, so the reason matters.

## Considered options

**Validated newtypes** — `GroupId`, `EdgeNodeId`, `DeviceId`, `HostId`, each
rejecting bad input at construction. Construction of a topic then cannot
fail. Rejected for now: the newtypes do not stop at this module. They reach
outward through `SparkplugClient::publish_metric`, whose six-argument
signature is pinned — three traits in `amygdala-stax-rs` are implemented on
it, and the Sparkplug session conformance plan depends on it not changing.
Adopting newtypes means changing that signature and every caller in two
downstream repos, in the same release that already carries several breaking
changes.

**Infallible constructors, documented requirement** — cheapest, and
rejected outright. It leaves the invariant unenforced, which removes the
reason the type is opaque at all. The module would be able to mint a topic
that its own parser refuses to read.

## Consequences

Nine constructors return `Result`. This costs nothing at the call sites:
`publish_metric`, `publish_metrics`, and `publish_birth` all already return
`Result<(), SparkplugError>`, so the `?` lands in code that has an error path
today. The failure reuses `SparkplugError::InvalidTopic`, because a bad
segment is exactly what makes a topic invalid.

Newtype segments remain the intended destination. Revisit when the
`publish_metric` signature is next open — the deepened publish handle
described as C5 in the 2026-08-20 architecture review is the natural moment,
because it reshapes those arguments anyway. Until then, do not re-propose
newtypes as a standalone change; the cost is in the downstream signature,
not in this module.
