# QK_FAIL_ON_DESER_NO_DLQ

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: One poison pill, one dead channel — unless you've planned for it.

## TL;DR

The SmallRye default `fail-on-deserialization-failure=true` means a single malformed record kills the channel. Without `failure-strategy=dead-letter-queue` paired with `fail-on-deserialization-failure=false`, the consumer crashloops on every restart.

## What's happening (the mechanism)

When the configured deserializer throws (corrupt bytes, schema mismatch, wrong codec), the connector decides:

- `fail-on-deserialization-failure=true` (default): the channel fails. With default `failure-strategy=fail`, this halts the channel; offset not committed; restart re-reads the same poison pill; crashloop.
- `fail-on-deserialization-failure=false`: a `null` value is forwarded to the downstream method, and a key/value-deserialization-failure-handler (if configured) can intercept. Combined with `failure-strategy=dead-letter-queue`, the failed bytes go to the DLQ along with headers like `deserialization-failure-reason`, `deserialization-failure-cause`, `deserialization-failure-data`.

The trap: many ops teams set up a DLQ, configure `failure-strategy=dead-letter-queue`, and assume deserialization failures route there too. They don't — they kill the channel first, before the failure-strategy gets a chance.

## Operational impact

- Poison pill produced by an upstream service → consumer enters crashloop.
- `smallrye-health` liveness DOWN.
- Manual remediation: change `auto.offset.reset` or seek past the offending offset.
- This is the canonical "Friday-evening incident."

## How to fix

```properties
# GOOD — route deserialization failures to DLQ, keep the channel alive
mp.messaging.incoming.orders.fail-on-deserialization-failure=false
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders-dlq
```

For more control, register a custom handler:

```java
@ApplicationScoped
@Identifier("orders-value-deser-failure")
public class OrderValueDeserHandler implements DeserializationFailureHandler<Order> {
    @Override
    public Order decorateDeserialization(...) {
        // log, route, or null
    }
}
```

```properties
mp.messaging.incoming.orders.value-deserialization-failure-handler=orders-value-deser-failure
```

## When this might be a false positive

- Strict pipelines where any deserialization failure MUST page a human before processing continues (regulated/financial).
- Test consumers verifying upstream schema enforcement.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.connector=smallrye-kafka` AND `fail-on-deserialization-failure` unset (or `true`) AND `failure-strategy` is not `dead-letter-queue`/`delayed-retry-topic` AND no `*-deserialization-failure-handler` is set.
- Confidence: HIGH for production profiles.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://github.com/quarkusio/quarkus/pull/40804 (docs PR clarifying deserialization failure handling)
