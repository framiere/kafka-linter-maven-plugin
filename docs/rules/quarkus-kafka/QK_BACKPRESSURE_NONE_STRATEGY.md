# QK_BACKPRESSURE_NONE_STRATEGY

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: annotation
**Tagline**: @OnOverflow(NONE) is reactive in name only.

## TL;DR

`@OnOverflow(OnOverflow.Strategy.NONE)` tells SmallRye to skip back-pressure entirely. Sends push to downstream regardless of demand. If the downstream can't keep up (most do), this either crashes the subscriber or — worse — silently allocates unbounded memory in whatever buffer the downstream happens to use.

## What's happening (the mechanism)

Reactive Streams demand is the back-pressure protocol: a subscriber says "I can take N more" via `request(N)`, the publisher emits up to N. `NONE` skips this — emissions are pushed regardless of `request` state.

SmallRye's docs:
> Ignores back-pressure; downstream must handle it.

If the downstream is well-behaved, it'll buffer or fail explicitly. In practice, the downstream is often a Kafka producer that buffers internally with its own bounded queue, and exceeding that queue throws or blocks. You've moved the OOM from "obvious" (UNBOUNDED_BUFFER) to "buried in Kafka client config."

## Operational impact

- Possible OOM via internal Kafka producer buffer (controlled by `buffer.memory`, default 32 MB).
- Possible `BufferExhaustedException` from the producer if `block.on.buffer.full` is false.
- No back-pressure signal to the caller — looks healthy until the explosion.

## How to fix

```java
// BAD
@Inject @Channel("orders")
@OnOverflow(OnOverflow.Strategy.NONE)
Emitter<Order> emitter;

// GOOD — explicit bounded buffer with failure path
@Inject @Channel("orders")
@OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 10_000)
Emitter<Order> emitter;
```

## When this might be a false positive

- Emitter feeding a downstream that already implements its own back-pressure (custom Multi processor).
- Test code measuring raw throughput.

## Detection strategy

- Annotation: `@OnOverflow(Strategy.NONE)` on any Emitter/MutinyEmitter.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/emitter/
- https://quarkus.io/blog/reactive-messaging-emitter/
