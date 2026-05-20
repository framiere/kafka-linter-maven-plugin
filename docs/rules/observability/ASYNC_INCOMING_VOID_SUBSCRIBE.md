# ASYNC_INCOMING_VOID_SUBSCRIBE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: `@Incoming` returning `void` while calling `.subscribe()` on a `Multi` is fire-and-forget — and the framework can't ack what it doesn't see.

## TL;DR

The linter flags Quarkus / SmallRye `@Incoming`-annotated methods that return `void` AND invoke `Multi.subscribe()` / `Uni.subscribe()` / `Uni.subscribeAsCompletionStage()` inside the body. SmallRye Reactive Messaging tracks message ack/nack through the reactive chain it sees as the method return value; `.subscribe()` detaches a new pipeline that the framework can't observe. The original message is auto-acked immediately on `void` return, before the detached pipeline has done anything. Subscribed-side errors become silent.

## The setup

Engineer writes `@Incoming("orders") public void handle(Order order)` and inside calls `webClient.post(order).subscribe()` (or `Uni.createFrom().item(order).subscribe()`) because "I don't need to wait for the response." The `Multi`/`Uni` runs in the background. The method returns immediately. SmallRye acks the Kafka message. The background work then fails — no nack, no DLQ, no retry. The Kafka pipeline has lost the message.

## What's actually happening

SmallRye Reactive Messaging supports several signatures for `@Incoming`:

| Signature | Ack policy | Framework awareness |
|-----------|-----------|---------------------|
| `void method(T)` | Auto-ack on return | None — fire-and-forget |
| `CompletionStage<Void> method(T)` | Ack when stage completes | Yes |
| `Uni<Void> method(T)` | Ack when Uni terminates | Yes |
| `Multi<Out> method(T)` (stream transformation) | Per-record | Yes |
| `Message<T> method(Message<T>)` | Manual ack via `Message.ack()` | Yes |

A `void`-returning method tells SmallRye "I'm synchronous; ack as soon as I return." If you spawn an async pipeline via `.subscribe()`, that pipeline runs detached. SmallRye doesn't see it. The message is acked. The async work can fail without consequence to the Kafka pipeline.

In contrast, if you return the `Uni`/`Multi`/`CompletionStage`, SmallRye threads the framework's ack/nack through the reactive type:
- Pipeline succeeds → message acked.
- Pipeline fails → message nacked → `failure-strategy` kicks in (DLQ, fail, ignore).

## Why this is subtle

- `subscribe()` is the "normal" way to consume a Mutiny pipeline in Java. Engineers from Reactor/RxJava bring the habit.
- The compiler accepts the signature. Tests pass (especially synchronous ones).
- The `failure-strategy` is configured, looks correct, never fires.
- Throwing inside the `.subscribe()` lambda doesn't surface to the framework — it propagates to Mutiny's default error handler, which prints to stderr unless configured otherwise.

## Operational impact

- Messages acked before the work is done. On crash, in-flight work lost.
- Errors don't trigger `failure-strategy` — DLQ stays empty, "fail" doesn't fail.
- Concurrency: nothing throttles the spawned subscribers; a burst on Kafka can launch thousands of detached pipelines.

## Failure scenarios (walkthrough)

1. **The crash-loss window.** Method receives 100 messages per second. Each spawns a `Uni.subscribe()` that POSTs to an HTTP endpoint with a 200ms latency. Pod gets SIGTERM. Kafka offsets are at the latest message (acks happened immediately on return). The 200 messages currently in-flight in spawned pipelines are lost — the pod terminates before they complete.

2. **The silent HTTP failure.** Same pattern. Downstream HTTP is down. Every spawned `Uni` fails. Errors land in Mutiny's default handler — printed to stderr if you're lucky, swallowed if a custom subscriber was provided. `failure-strategy=dead-letter-queue` configured — DLQ stays empty.

## How to fix

Return the reactive type so SmallRye can observe it.

```java
// BAD — void + subscribe
@Incoming("orders")
public void handle(Order order) {
    webClient.post(order).subscribe().with(
        ok -> log.info("posted"),
        ex -> log.error("failed", ex));
}

// GOOD — return Uni<Void>
@Incoming("orders")
public Uni<Void> handle(Order order) {
    return webClient.post(order)
        .onItem().ignore().andContinueWithNull();
}

// GOOD — return CompletionStage<Void>
@Incoming("orders")
public CompletionStage<Void> handle(Order order) {
    return downstream.postAsync(order);
}

// GOOD — Message-level manual ack
@Incoming("orders")
public Uni<Void> handle(Message<Order> msg) {
    return webClient.post(msg.getPayload())
        .onItem().transformToUni(r -> Uni.createFrom().completionStage(msg.ack()))
        .onFailure().recoverWithUni(ex -> Uni.createFrom().completionStage(msg.nack(ex)));
}
```

## When this might be a false positive

- The `subscribe()` is on a hot stream that's started at `@PostConstruct` and the `@Incoming` method body is genuinely synchronous on top of it.
- A test fixture using `.subscribe().asCompletionStage().get()` — that's a synchronous bridge, not detached.

## Detection strategy

- bytecode: visit methods annotated `@org.eclipse.microprofile.reactive.messaging.Incoming`.
- Check the return type: `void`.
- Walk the body for `INVOKE` on:
  - `io.smallrye.mutiny.Multi.subscribe`
  - `io.smallrye.mutiny.Uni.subscribe`
  - `io.smallrye.mutiny.Uni.subscribeAsCompletionStage`
  - `reactor.core.publisher.Mono.subscribe`
  - `reactor.core.publisher.Flux.subscribe`
- Flag if any such call is found and the return type is `void`.
- Confidence HIGH — annotation + return type + subscribe call is exact.

## References

- Quarkus — Reactive Messaging method signatures: https://quarkus.io/guides/kafka#message-consumption
- SmallRye Reactive Messaging — Incoming method signatures: https://smallrye.io/smallrye-reactive-messaging/latest/concepts/incoming/
- Mutiny — subscribe vs return semantics: https://smallrye.io/smallrye-mutiny/latest/guides/subscribing/
- Related: `QK_INCOMING_RETURNS_VOID` (subset — void method with no async return at all)
- Related: `QK_MUTINY_UNI_NEVER_SUBSCRIBED` (the inverse — Uni built but never returned)
