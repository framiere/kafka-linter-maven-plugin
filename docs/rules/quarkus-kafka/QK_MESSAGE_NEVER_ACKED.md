# QK_MESSAGE_NEVER_ACKED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A Message<T> you don't ack is a partition you'll never advance.

## TL;DR

An `@Incoming` method taking `Message<T>` runs in MANUAL ack mode. If the code path doesn't call `msg.ack()` or `msg.nack(...)` on every branch, the offset never commits — the channel stalls within `throttled.unprocessed-record-max-age.ms` (default 60 s) and turns unhealthy.

## What's happening (the mechanism)

SmallRye Reactive Messaging picks an ack strategy by the consumer signature:
- Plain payload `T` → `POST_PROCESSING` (auto-ack after method return).
- `Record<K,V>` → `POST_PROCESSING`.
- `Message<T>` / `KafkaRecord<K,V>` → `MANUAL` — you own ack/nack.

In MANUAL mode, returning normally is not an ack. The throttled commit tracker sits on the un-acked offset. After `throttled.unprocessed-record-max-age.ms` (60 s default), the channel is marked unhealthy and stops polling.

Common bytecode bugs:

```java
// BAD — exception path doesn't nack
@Incoming("orders")
public CompletionStage<Void> handle(Message<Order> msg) {
    try {
        process(msg.getPayload());
        return msg.ack();
    } catch (Exception e) {
        return CompletableFuture.completedFuture(null);  // never ack/nack!
    }
}

// BAD — early return path doesn't ack
@Incoming("orders")
public void handle(Message<Order> msg) {
    if (msg.getPayload().isStale()) return;  // never ack/nack!
    process(msg.getPayload());
    msg.ack();
}
```

## Operational impact

- After 60 s of an un-acked message, channel becomes UNREADY.
- `smallrye-health` readiness probe goes DOWN — Kubernetes stops routing traffic to the pod.
- `consumer_lag` for the partition grows without bound.
- No exception, no crash — just a quiet stall.

## How to fix

```java
// GOOD — ack on success, nack on failure, always
@Incoming("orders")
public CompletionStage<Void> handle(Message<Order> msg) {
    try {
        process(msg.getPayload());
        return msg.ack();
    } catch (Exception e) {
        return msg.nack(e);
    }
}

// GOOD — let the framework handle ack: take the payload, not Message
@Incoming("orders")
public void handle(Order o) { process(o); }  // auto-ack on return
```

## When this might be a false positive

- The method returns a `Message<T>` (via `@Outgoing`) — downstream is responsible for ack propagation.
- The `Message` is stored in a field/queue and acked elsewhere — interprocedural; flag with LOW confidence.

## Detection strategy

- Bytecode: method has `@Incoming`, parameter type `Lorg/eclipse/microprofile/reactive/messaging/Message;` or `Lio/smallrye/reactive/messaging/kafka/KafkaRecord;`. Walk all return paths. If any path returns/exits without an `INVOKEINTERFACE Message.ack` or `Message.nack`, flag.
- Confidence: HIGH for void/CompletionStage returns with no escape; LOWER when the Message is passed to other methods (interprocedural).

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/concepts/acknowledgement/
- https://quarkus.io/guides/kafka (Acknowledgement section)
