# QK_TRACING_DISABLED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: tracing-enabled=false is "I solemnly swear to debug from logs alone."

## TL;DR

`mp.messaging.incoming.<channel>.tracing-enabled=false` (or the outgoing equivalent) cuts the OpenTelemetry span propagation through the channel. Distributed traces stop at the Kafka boundary; correlation across producer→broker→consumer is gone.

## What's happening (the mechanism)

By default, SmallRye injects OpenTelemetry trace context into outgoing Kafka headers (`traceparent`, `tracestate`) and extracts it on incoming records, so a single trace spans producer service → Kafka → consumer service. Disabling tracing on either side drops the span and the propagation header.

Setting this to `false` is a performance micro-optimization (a few microseconds per record) at the cost of observability that's usually worth orders of magnitude more during incidents.

## Operational impact

- Jaeger / Tempo / Honeycomb traces show producer ends abruptly at "kafka.send"; consumer trace starts fresh with no parent.
- Root-cause analysis for cross-service latency becomes a join across log timestamps.
- The savings (microseconds, mostly span allocation) are invisible in any throughput benchmark.

## How to fix

```properties
# BAD
mp.messaging.incoming.orders.tracing-enabled=false

# GOOD — defaults; just don't override
# (omit; default is true)
```

If you genuinely need to disable for a specific channel (e.g., extremely high-rate telemetry channel where span allocation matters), document the rationale in the same properties file:

```properties
# Intentional: 100k+ msg/s firehose, span allocation dominates
mp.messaging.incoming.metrics-firehose.tracing-enabled=false
```

## When this might be a false positive

- Very high-cardinality telemetry channels where span allocation is measurable cost.
- Channels where the operator has decided that span data isn't shipped to a backend anyway.

## Detection strategy

- Config: `mp.messaging.incoming.<channel>.tracing-enabled=false` OR `mp.messaging.outgoing.<channel>.tracing-enabled=false`.
- Confidence: MEDIUM — legitimate disables exist, but they should be rare and documented.

## References

- https://smallrye.io/smallrye-reactive-messaging/latest/kafka/receiving-kafka-records/
- https://quarkus.io/guides/opentelemetry
