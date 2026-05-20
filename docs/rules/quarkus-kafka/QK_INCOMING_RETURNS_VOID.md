# QK_INCOMING_RETURNS_VOID

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Void return = pre-processing ack = at-most-once if you blink wrong.

## TL;DR

`@Incoming` on a `void` method taking a plain payload triggers SmallRye's `POST_PROCESSING` auto-ack — but only if the method returns normally. Any throw, async escape, or framework misconfiguration turns the channel into at-most-once. For data that matters, take a `Message<T>` and ack explicitly, or return `CompletionStage<Void>` / `Uni<Void>`.

## What's happening (the mechanism)

For a synchronous `void` return:
- Method returns normally → SmallRye acks → offset eventually committed.
- Method throws → SmallRye nacks → `failure-strategy` kicks in.

That sounds fine. The trap is async work:

```java
// BAD — fire-and-forget inside a void @Incoming
@Incoming("orders")
public void handle(Order o) {
    externalApi.sendAsync(o);  // returns CompletableFuture, ignored
    // void returns now → SmallRye acks → offset committed
    // ...but the actual work hasn't finished. Crash here = data loss.
}
```

The framework can't tell that you have async work in flight. It commits before the work completes. This is at-most-once dressed up as at-least-once.

## Operational impact

- Looks healthy under normal load.
- Crashes mid-processing lose in-flight messages.
- Slow downstream becomes invisible — no back-pressure signal because the method has returned.

## How to fix

```java
// GOOD — return the CompletionStage so SmallRye waits for it
@Incoming("orders")
public CompletionStage<Void> handle(Order o) {
    return externalApi.sendAsync(o).thenApply(__ -> null);
}

// GOOD — Uni equivalent
@Incoming("orders")
public Uni<Void> handle(Order o) {
    return Uni.createFrom().completionStage(externalApi.sendAsync(o)).replaceWithVoid();
}

// GOOD — fully synchronous, void is fine
@Incoming("orders")
public void handle(Order o) {
    repository.save(o);
}
```

## When this might be a false positive

- Genuinely synchronous handlers — the rule should only flag void methods that *also* invoke async APIs (CompletionStage / Uni / Future) without returning them.
- Auto-ack mode explicitly opted out via `@Acknowledgment(Acknowledgment.Strategy.NONE)`.

## Detection strategy

- Bytecode: `@Incoming` method with return type `V` (void). Scan instructions for invokes returning `CompletionStage` / `CompletableFuture` / `Uni` whose result is popped or stored in an unused local.
- Confidence: MEDIUM — interprocedural truth.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/acknowledgement/
- https://quarkus.io/guides/kafka
