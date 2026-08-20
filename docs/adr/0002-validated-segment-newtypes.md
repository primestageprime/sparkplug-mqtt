---
status: accepted
---

# Validated segment newtypes for topic identifiers

`GroupId`, `EdgeNodeId`, `DeviceId` and `HostId` wrap a checked topic
segment. `Namespace` takes them instead of `&str`, which makes its nine
constructors infallible — every identifier is already checked by the time a
constructor sees it. This reverses ADR-0001.

## Why the reversal

ADR-0001 chose fallible constructors over newtypes and said to revisit when
the `publish_metric` signature was next open. That trigger has not fired, and
the signature is still pinned. A different fact changed the decision.

A survey of the two consuming repos found that a caller had already paid for
the missing type safety by hand. `amygdala-stax-rs/src/vessel_call_publisher.rs`
defines a named struct rather than a tuple, with this reason in the source:

> Named fields, not a tuple: `node_id` and `device_id` are both `&str` and
> carry different meanings … so a positional swap would be silent.

It carries a regression test guarding that swap. Its two sibling modules do
the same job with a bare tuple and no guard. So the risk is not theoretical,
one consumer has mitigated it locally, and two have not.

ADR-0001 deferred on the grounds that no caller had felt the pain. A caller
had.

## Considered options

**A publishing handle bound to `(group, node, device)`** — the original
proposal, from the 2026-08-20 architecture review. Rejected on evidence. The
same survey showed `group_id` is loop-invariant at five of six call sites and
`device_id` equals `node_id` at all six unless a process-wide override is set.
The identity that actually varies per publish is `node_id` — the one a handle
would have bound. It would have been rebuilt every iteration and bought
nothing.

**Owned newtypes** (`GroupId(String)`) — rejected. Every caller holds these
as `&str` from a database row, a config field, or a const. Owning would force
an allocation per publish for text the caller already owns. The types borrow
and are `Copy`.

## Consequences

The `Namespace` constructors no longer return `Result`. Validation happens
once, where the value enters its type, instead of once per topic built.

The six-argument `publish_metric` and `publish_metrics` are unchanged and
undeprecated. Three traits in `amygdala-stax-rs` are implemented on the
former, and deprecating it now would warn on every downstream build for an
interface those repos cannot leave until they raise their version pin.
`publish_metric_to` and `publish_metrics_to` take a `&SparkplugTopic`
alongside them. Deprecate the pair once a consumer has migrated.

The crate does **not** absorb `device_id_override.unwrap_or(asset_id)`, which
appears in four copies downstream. That is one deployment's convention, not a
Sparkplug rule, and it belongs in that repo.
