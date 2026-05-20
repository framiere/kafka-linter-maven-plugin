# QK_THROTTLED_HEALTH_DISABLED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Disabling unprocessed-record-max-age is welcoming the OOM in.

## TL;DR

`mp.messaging.incoming.<channel>.throttled.unprocessed-record-max-age.ms` set to `0` or a negative number disables the stuck-record health check. The SmallRye docs explicitly warn this "might lead to running out of memory if there are 'poison pill' messages."

## What's happening (the mechanism)

The `throttled` commit strategy buffers in-flight records to compute the highest *consecutive* acked offset. If a record is never acked (handler hangs, deadlock, infinite retry), the buffer grows.

The unprocessed-record-max-age check is the safety valve:
- Default 60000 ms.
- If a record sits un-acked for longer than this, the channel is marked unhealthy → liveness DOWN → pod restart.

Setting this to `0` (or negative) disables the check. The buffer grows without bound, and:
- Memory pressure increases.
- The reactive pipeline keeps polling Kafka, accumulating more records.
- The JVM eventually OOMs.

There's no upside to disabling this. The check is precisely the kind of "fail loud" safety net you want.

## Operational impact

- Heap growth on stuck channels.
- OOMKilled pods with no clear cause; postmortem blames "memory leak."
- Hidden because the channel still polls and acks most records — until it doesn't.

## How to fix

```properties
# BAD
mp.messaging.incoming.orders.throttled.unprocessed-record-max-age.ms=0

# GOOD — default
# (omit; default is 60000)

# GOOD — tuned higher for very slow handlers (e.g., LLM calls)
mp.messaging.incoming.orders.throttled.unprocessed-record-max-age.ms=600000
```

If your handler is legitimately slow (>60s), increase the threshold rather than disable the check.

## When this might be a false positive

- Almost never. The docs warn explicitly. Disable is a "footgun by intent" pattern.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.throttled.unprocessed-record-max-age.ms` set to `0` or negative.
- Confidence: HIGH.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/blog/kafka-commit-strategies/
