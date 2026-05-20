# QK_MUTINY_UNI_NEVER_SUBSCRIBED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A Uni you don't subscribe to is a message you don't send.

## TL;DR

`MutinyEmitter.send(payload)` returns a `Uni<Void>`. Mutiny is lazy: nothing happens until something subscribes. If the result is discarded (popped off the stack with no `subscribe()`, `await()`, or `onItem()...` chain consumed by a subscriber), the message is never emitted.

## What's happening (the mechanism)

Mutiny `Uni<T>` is a deferred computation. The send happens inside the subscription, not at `.send()` call time. Common bytecode shapes that hit this bug:

```java
// BAD — Uni discarded
emitter.send(order);  // returns Uni<Void>, immediately popped → POP instruction

// BAD — assigned and never used
Uni<Void> u = emitter.send(order);

// BAD — chained but never subscribed
emitter.send(order).onFailure().invoke(...);
```

Correct shapes:

```java
// GOOD — async fire-and-forget (errors logged)
emitter.send(order)
    .subscribe().with(__ -> {}, err -> log.error("send failed", err));

// GOOD — explicit forget helper
emitter.sendAndForget(order);

// GOOD — block-and-wait (only off the IO thread!)
emitter.sendAndAwait(order);

// GOOD — returned to caller who subscribes
public Uni<Void> publish(Order o) { return emitter.send(o); }
```

## Operational impact

- Producer metrics show zero throughput on the channel.
- No error logs. The application looks healthy; downstream consumers see nothing.
- Hard to diagnose: developers see `send()` called and assume it worked.

## How to fix

Either subscribe explicitly, use `sendAndForget` / `sendAndAwait`, or return the `Uni` up the call chain to a subscriber that will subscribe (e.g., a `@Outgoing` method returning `Uni<Void>`, or a REST endpoint returning `Uni<Response>`).

## When this might be a false positive

- The method's caller subscribes — interprocedural analysis can't always see this. Detect only when the `Uni` is discarded *within the same method* (POP after the INVOKEVIRTUAL).

## Detection strategy

- Bytecode: find `INVOKEVIRTUAL io/smallrye/reactive/messaging/MutinyEmitter.send` returning `Uni`, followed by `POP` within N instructions without an `INVOKEVIRTUAL io/smallrye/mutiny/Uni.subscribe` / `await` / `await.indefinitely` / `await.atMost` on the same value.
- Also flag local-variable assignments where the local is never read.
- Confidence: HIGH for in-method POP; LOW if the value escapes (return / store-to-field).

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/emitter/
- https://quarkus.io/blog/reactive-messaging-emitter/
