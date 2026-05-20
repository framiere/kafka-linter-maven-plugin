# PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: delivery.timeout.ms < request.timeout.ms + linger.ms doesn't even start.

## TL;DR

The linter flags producer configurations where `delivery.timeout.ms` is less than `request.timeout.ms + linger.ms`. Kafka rejects this at construction with `ConfigException`.

## What's happening (the mechanism)

`delivery.timeout.ms` (KIP-91, default 120000) is the upper bound on the total time from `send()` to terminal success/failure callback. Internally it must cover at least one full in-flight attempt:

```
delivery.timeout.ms ≥ request.timeout.ms + linger.ms
```

If the inequality is violated, the producer throws `ConfigException` during `new KafkaProducer(props)`. The constraint exists so that at least one send attempt has time to complete after batching delay.

The trap is upgrade-time: teams raise `request.timeout.ms` to "be safer" without touching `delivery.timeout.ms`, and the next deploy fails to start.

## Operational impact

- Producer cannot be constructed — startup fails. Best caught at lint time, second-best at IT time, worst in prod canary.
- The error message is clear but the root cause (which knob to lower / which to raise) is often misdiagnosed; teams flail between the three values.

## How to fix

```properties
# BAD
request.timeout.ms=60000
linger.ms=20
delivery.timeout.ms=30000
# 30000 < 60000 + 20  → ConfigException

# GOOD
request.timeout.ms=30000
linger.ms=20
delivery.timeout.ms=120000
```

Rule of thumb: `delivery.timeout.ms ≥ 2 × request.timeout.ms + linger.ms` to allow at least one retry.

## When this might be a false positive

- None. This always fails at runtime.

## Detection strategy

- Config: parse all three keys from the same Properties source. Compute the inequality; flag violations. HIGH.
- Bytecode: more involved — extract all three `Properties.put` calls in the same method body, compute, flag.
- Use defaults for missing keys: `request.timeout.ms=30000`, `linger.ms=5` (Kafka 4.0) / `linger.ms=0` (Kafka 3.x), `delivery.timeout.ms=120000`.

## References

- KIP-91 — delivery.timeout.ms: https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
- Apache Kafka producer configs — `delivery.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_delivery.timeout.ms
- AWS MSK best practices — producer timeouts: https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html
