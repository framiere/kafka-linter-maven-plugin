# QK_BLOCKING_ON_IO_THREAD

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + annotation
**Tagline**: @Blocking on the IO thread isn't reactive — it's surrender.

## TL;DR

An `@Incoming` method that does blocking I/O (JDBC, JPA, HTTP client without async, `Thread.sleep`, `Future.get`) but is not annotated with `@Blocking` or `@RunOnVirtualThread` blocks the Vert.x event loop. This stalls every other channel and HTTP request sharing that thread.

## What's happening (the mechanism)

SmallRye Reactive Messaging dispatches `@Incoming` methods on the Vert.x event-loop thread by default. The event loop is shared across all channels and the HTTP server. A blocking call there freezes everything.

Two opt-outs exist:
- `@Blocking` (either `io.smallrye.common.annotation.Blocking` or `io.smallrye.reactive.messaging.annotations.Blocking`) — offloads to the worker pool.
- `@RunOnVirtualThread` (Quarkus, requires JDK 21+) — offloads to a virtual thread, preserves ordering by default.

Quarkus auto-treats `@Transactional` methods as blocking. Everything else is on you.

Bytecode signals of blocking work in a non-blocking method:
- `INVOKEINTERFACE java/sql/Connection.*` — JDBC.
- `INVOKEVIRTUAL jakarta/persistence/EntityManager.*` / Panache repository calls.
- `INVOKESTATIC java/lang/Thread.sleep`.
- `INVOKEVIRTUAL java/util/concurrent/Future.get`.
- `INVOKEVIRTUAL java/util/concurrent/CompletableFuture.join`.
- `INVOKEVIRTUAL io/smallrye/mutiny/Uni.await ... indefinitely`.
- Synchronous REST clients (`@RegisterRestClient` without `Uni`/`CompletionStage` return).

## Operational impact

- Quarkus logs warnings like `Thread Thread[vert.x-eventloop-thread-X,...] has been blocked for NNN ms`.
- HTTP latency p99 spikes for unrelated endpoints.
- Other Kafka channels lag — they share the same event loop.
- Eventually the BlockedThreadChecker kills the thread (default `quarkus.vertx.warning-exception-time=2s`, `quarkus.vertx.max-event-loop-execute-time=2s`).

## How to fix

```java
// BAD — JPA on the event loop
@Incoming("orders")
public void persist(Order o) {
    em.persist(o);  // blocks
}

// GOOD — explicit thread pool hop
@Incoming("orders")
@Blocking
@Transactional
public void persist(Order o) {
    em.persist(o);
}

// MODERN — virtual thread, ordering preserved
@Incoming("orders")
@RunOnVirtualThread
@Transactional
public void persist(Order o) {
    em.persist(o);
}
```

## When this might be a false positive

- Calls into reactive equivalents (Mutiny Panache `*.persist().await()` is still blocking; `*.persist()` returning `Uni` is not).
- A "blocking-looking" call wrapped in `Uni.createFrom().item(() -> ...)` with `runSubscriptionOn(workerPool)`.
- Method declared blocking via class-level `@Blocking` (rare; check the enclosing class).

## Detection strategy

- Bytecode: scan `@Incoming` methods for invokes against `java/sql/*`, `jakarta/persistence/*`, `java/lang/Thread.sleep`, `*.await().indefinitely()`.
- Annotation: check for `Lio/smallrye/common/annotation/Blocking;`, `Lio/smallrye/reactive/messaging/annotations/Blocking;`, `Lio/smallrye/common/annotation/RunOnVirtualThread;`, `Ljakarta/transaction/Transactional;`.
- Confidence: HIGH when JDBC / JPA / Thread.sleep detected; MEDIUM for arbitrary InvokeVirtual sequences that might block.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/blocking/
- https://quarkus.io/guides/messaging-virtual-threads
- https://github.com/quarkusio/quarkus/discussions/25992
