# QK_EMITTER_UNBOUNDED_BUFFER

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation
**Tagline**: UNBOUNDED_BUFFER is the docs' own [DANGER ZONE]. Read that as: OOM.

## TL;DR

`@OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)` removes the back-pressure safety valve. A producer faster than the broker fills the heap until the JVM dies — and the only signal is `OutOfMemoryError`.

## What's happening (the mechanism)

`UNBOUNDED_BUFFER` does exactly what it says: enqueues without a cap. The SmallRye docs flag it `[DANGER ZONE]` because:

- A slow / unreachable broker means messages pile up indefinitely.
- A producer thread that doesn't yield (e.g., a tight REST loop) fills the queue faster than Kafka can drain it.
- The JVM has no warning until allocation fails. The crash dump points at unrelated code.

## Operational impact

- Heap usage climbs linearly with load until OOM.
- `kafka_producer_record_send_total` rises while `kafka_producer_record_queue_time_avg` skyrockets.
- Pod gets OOMKilled by the kubelet; restarts; in-flight messages lost.
- Postmortem: "what allocated all this memory?" — answer: an emitter buffer that nobody can see in profilers (it's just an `ArrayDeque` in a SmallRye internal).

## How to fix

```java
// BAD
@Inject @Channel("orders")
@OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
Emitter<Order> emitter;

// GOOD — bounded buffer sized for observed burst, with metric or DROP visibility
@Inject @Channel("orders")
@OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 50_000)
Emitter<Order> emitter;
```

If you genuinely have an unbounded ingestion appetite, the right fix is upstream back-pressure (the caller waits), not an unbounded buffer downstream.

## When this might be a false positive

- Bench/load test code where you want to measure raw throughput without flow control.
- One-shot scripts processing a finite, small input.

## Detection strategy

- Annotation: `@OnOverflow` with `value=UNBOUNDED_BUFFER` on any `Emitter`/`MutinyEmitter` field.
- ASM: AnnotationNode with `value` = `Lorg/eclipse/microprofile/reactive/messaging/OnOverflow$Strategy;` enum value `UNBOUNDED_BUFFER`.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/emitter/ ("[DANGER ZONE]")
- https://quarkus.io/blog/reactive-messaging-emitter/
