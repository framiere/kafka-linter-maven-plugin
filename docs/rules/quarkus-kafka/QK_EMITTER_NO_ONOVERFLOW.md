# QK_EMITTER_NO_ONOVERFLOW

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: bytecode + annotation
**Tagline**: 256 messages is not your back-pressure plan.

## TL;DR

An `Emitter<T>` or `MutinyEmitter<T>` injected with `@Channel` but no `@OnOverflow` uses the default `BUFFER` strategy with `bufferSize=256`. The 257th unconsumed message throws `BackPressureFailure`; production traffic blows past 256 in milliseconds.

## What's happening (the mechanism)

SmallRye Reactive Messaging Emitters buffer between the calling thread and the downstream subscriber to bridge the imperative-to-reactive boundary. The buffer is:

- 256 entries by default.
- `BUFFER` strategy by default — when full, `send()` throws or the returned `CompletionStage`/`Uni` completes exceptionally.

Available strategies:
- `BUFFER` — bounded queue, fail when full. Safe but `send()` becomes lossy under burst.
- `UNBOUNDED_BUFFER` — flagged `[DANGER ZONE]` in the docs; OOM risk.
- `DROP` — newest messages dropped silently when full.
- `LATEST` — older messages dropped (keep only the latest).
- `FAIL` — like BUFFER but doesn't buffer at all; fails immediately on lack of demand.
- `NONE` — ignore back-pressure, push to downstream regardless (will likely crash the subscriber).

Without explicit `@OnOverflow`, you've chosen `BUFFER@256` by accident.

## Operational impact

- Burst from REST → emitter → Kafka: under load, `send()` throws `io.smallrye.mutiny.subscription.BackPressureFailure`. The HTTP request fails 500.
- Returned `CompletionStage` completes exceptionally — if the caller doesn't handle it, the failure is silently dropped.
- No metric pre-warns; you only learn at the 257th in-flight message.

## How to fix

```java
// BAD — implicit 256-entry buffer
@Inject
@Channel("orders")
Emitter<Order> emitter;

// GOOD — explicit bounded buffer sized for your burst pattern
@Inject
@Channel("orders")
@OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 10_000)
Emitter<Order> emitter;

// GOOD — explicit drop-newest with metric for visibility
@Inject
@Channel("metrics")
@OnOverflow(OnOverflow.Strategy.DROP)
Emitter<Metric> emitter;
```

For Mutiny, prefer the typed `MutinyEmitter<T>` and ALWAYS subscribe to the returned `Uni`:

```java
emitter.send(order)
    .onFailure().invoke(err -> log.error("emit failed", err))
    .subscribe().with(__ -> {}, err -> {});
```

## When this might be a false positive

- Truly tiny applications where 256 is enough headroom and `send()` failure is acceptable.
- Tests / scaffolds.

## Detection strategy

- Bytecode: detect `@Inject` fields of type `Lorg/eclipse/microprofile/reactive/messaging/Emitter;` or `Lio/smallrye/reactive/messaging/MutinyEmitter;` (or constructor-injected via parameter).
- Annotation: absence of `Lorg/eclipse/microprofile/reactive/messaging/OnOverflow;` on the same field/parameter.
- Confidence: HIGH — the default is rarely intentional once you know it exists.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/emitter/
- https://quarkus.io/blog/reactive-messaging-emitter/
