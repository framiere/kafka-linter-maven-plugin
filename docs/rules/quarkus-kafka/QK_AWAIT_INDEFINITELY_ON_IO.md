# QK_AWAIT_INDEFINITELY_ON_IO

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: annotation + bytecode
**Tagline**: await().indefinitely() in a reactive pipeline is a deadlock waiting for tomorrow.

## TL;DR

Calling `Uni.await().indefinitely()` inside an `@Incoming`/`@Outgoing` method that runs on the IO thread blocks the very thread responsible for completing the `Uni`. Deadlock — or, with the BlockedThreadChecker, a forced shutdown.

## What's happening (the mechanism)

The Vert.x event loop is single-threaded per "context." A `Uni<RecordMetadata>` returned by a Kafka producer completes via a callback dispatched back onto the same event loop. If your reactive handler calls `.await().indefinitely()`:

1. The event-loop thread enters the await — parked.
2. Kafka's response arrives and is scheduled on the event-loop thread.
3. That thread is still parked. The completion never runs. The `await()` never returns.
4. After `quarkus.vertx.max-event-loop-execute-time` (default 2 s), the BlockedThreadChecker logs a warning. After enough, it kills the thread.

Even on a worker thread, `await().indefinitely()` is a code smell: you've taken a reactive API and made it synchronous, paying both costs.

## Operational impact

- `Thread Thread[vert.x-eventloop-thread-N,...] has been blocked for NNN ms` warnings.
- Channel halts; new records never delivered.
- Other channels sharing the same event loop also stall.

## How to fix

```java
// BAD — blocks the IO thread
@Incoming("orders")
public void handle(Order o) {
    producer.send(o).await().indefinitely();
}

// GOOD — chain reactively, return the Uni
@Incoming("orders")
public Uni<Void> handle(Order o) {
    return producer.send(o).replaceWithVoid();
}

// GOOD — if you really must block, hop to a worker
@Incoming("orders")
@Blocking
public void handle(Order o) {
    producer.send(o).await().indefinitely();  // OK on worker thread
}
```

`await().atMost(Duration)` is safer than `indefinitely()` because it surfaces the deadlock as a timeout exception instead of an indefinite hang — but the deeper fix is to return the `Uni`.

## When this might be a false positive

- Method is annotated `@Blocking` or `@RunOnVirtualThread` — already off the event loop.
- Method is a startup hook (`@Startup`, `@PostConstruct`) — runs on the main thread, not the event loop.
- Test code.

## Detection strategy

- Bytecode: `INVOKEVIRTUAL io/smallrye/mutiny/groups/UniAwait.indefinitely` (or `atMost`) within a method annotated `@Incoming` / `@Outgoing` / `@Channel`-injected handler.
- Annotation: suppress if method has `@Blocking` or `@RunOnVirtualThread`.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/blocking/
- https://quarkus.io/guides/messaging-virtual-threads
