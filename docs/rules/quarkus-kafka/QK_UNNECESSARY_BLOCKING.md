# QK_UNNECESSARY_BLOCKING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: annotation + bytecode
**Tagline**: @Blocking on pure compute is a free thread hop you pay for.

## TL;DR

A method annotated `@Blocking` that does no blocking I/O (no JDBC, no synchronous HTTP, no `Thread.sleep`, no `*.await()`) wastes a worker-pool dispatch on every message. At high throughput this is measurable.

## What's happening (the mechanism)

`@Blocking` schedules the method on the Vert.x worker pool. Every dispatch incurs:
- A task submission to the executor.
- A context switch to a worker thread.
- A return hop to the event loop to deliver the ack.

For pure transforms (CPU work, in-memory cache lookups, lookups against already-loaded data), this is overhead with no benefit. The event loop would have run the method in nanoseconds; the worker pool adds microseconds of scheduling plus contention with genuinely blocking work.

## Operational impact

- Worker-pool saturation under high message rates — the pool exists for blocking I/O, not transforms.
- Increased latency under load (the pool queue grows; transforms wait behind real I/O).
- `vertx.pools.worker.queue-size` metric grows.

## How to fix

```java
// BAD — pure compute on the worker pool
@Incoming("events")
@Outgoing("normalized")
@Blocking
public Event normalize(Event e) {
    return e.toLowerCase();
}

// GOOD — keep it on the event loop
@Incoming("events")
@Outgoing("normalized")
public Event normalize(Event e) {
    return e.toLowerCase();
}
```

## When this might be a false positive

- The method is "small now, big later" — operator knowingly preallocated `@Blocking` for upcoming changes.
- Method calls a third-party library whose blocking behavior the linter can't see.
- `@Blocking(ordered = false)` is being used specifically to enable concurrent processing across partitions, not for blocking semantics.

## Detection strategy

- Bytecode: method has `@Blocking` AND no invocations in the known-blocking set (JDBC, JPA, Thread.sleep, Future.get, sync RestClient, `*.await()`).
- Confidence: MEDIUM — heuristic; operator override expected.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/blocking/
- https://quarkus.io/guides/kafka (Blocking processing)
