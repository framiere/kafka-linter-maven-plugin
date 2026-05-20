# STREAMS_PROD_HANDLER_BLANKET_CONTINUE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: A `ProductionExceptionHandler` that always returns `CONTINUE` swallows every produce failure — including the ones that mean the broker is gone.

## TL;DR

The linter flags `ProductionExceptionHandler` implementations whose `handle()` always returns `CONTINUE`. Production-side errors include `RecordTooLargeException` (transient, safe to drop with a metric), but also `TimeoutException`, `OutOfOrderSequenceException`, and ad-hoc broker-cluster failures that should fail the stream loudly. Blanket continue means the topology runs forward while every output write is silently failing — by the time you notice, the output topic is hours behind reality.

## The setup

Team encounters `RecordTooLargeException` killing the topology. They follow the pattern from `STREAMS_PRODUCTION_HANDLER_MISSING` but go too far: their custom handler returns `CONTINUE` for every exception, not just `RecordTooLargeException`. Behavior is now: every produce failure → log + continue. Topology happily runs while no output is being written.

## What's actually happening

`ProductionExceptionHandler.handle()` is invoked from the producer's `Callback.onCompletion` when the broker reports a send failure. The handler decides `CONTINUE` (drop, log at DEBUG, advance topology) or `FAIL` (log at ERROR, shut down).

The framework only invokes the handler for non-fatal exceptions — `ProducerFencedException` and certain auth errors bypass it. But the set of recoverable exceptions includes things you do *not* want to silently drop:

- `TimeoutException` — could mean the broker is unreachable.
- `OutOfOrderSequenceException` — idempotence violation, likely a producer bug.
- `UnknownProducerIdException` — transactional state lost.
- `NetworkException` — partition unreachable.
- `KafkaException` — generic catch-all.

`RecordTooLargeException` is one of the few that's "safe to log+drop with a metric". Everything else either means "broker is angry, retry" or "your topology is broken, stop".

## Why this is subtle

- The advice "write a custom production handler" gets internalized as "make it not fail."
- The CONTINUE/FAIL enum has only two values — there's no nuanced "RETRY" — which pushes implementers toward CONTINUE to avoid the topology-shutdown sledgehammer.
- The producer side has its own retry loop already (`retries`, `retry.backoff.ms`); by the time your handler sees the exception, retries are exhausted. CONTINUE there means "I accept the loss."
- Without a per-exception-type metric, the loss is invisible.

## Operational impact

- Output topic stops receiving records, source topic offsets keep advancing → permanent data loss for the time the broker was unavailable.
- Aggregates / joins computed correctly internally, but the materialized result topic is stale.
- "Streams is healthy" — all internal state machines look fine, app doesn't restart.
- Customer-visible: downstream consumers see no new events, then a flood when the broker recovers (if producer-side retry succeeded in those interim records).

## Failure scenarios (walkthrough)

1. **The broker outage.** Brokers go down for 20 minutes. Source records keep being consumed (still committed offsets via the input partition, which… well, also can't commit if broker is down — but on partial outages of *output* topic only). For 20 minutes the topology consumes, processes, and tries to produce. Every produce fails with `TimeoutException`. Blanket CONTINUE: every record dropped. After recovery: 20 minutes of data permanently gone, no DLT, no log alert.

2. **The misconfigured ACL.** Service account loses write permission on output topic. Every produce → `TopicAuthorizationException`. (This one is usually pre-filtered as fatal, but the model still applies for similar permission errors.) Blanket CONTINUE: silently drop.

## How to fix

Dispatch on exception type. Be explicit about what you drop.

```java
public class SmartProductionHandler implements ProductionExceptionHandler {
    private final MeterRegistry meter;
    private final KafkaProducer<byte[], byte[]> dltProducer;  // optional

    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            meter.counter("streams.produce.drop", "reason", "oversized",
                "topic", record.topic()).increment();
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
        // network / broker / sequence: not safe to drop
        meter.counter("streams.produce.fail",
            "exception", exception.getClass().getSimpleName(),
            "topic", record.topic()).increment();
        return ProductionExceptionHandlerResponse.FAIL;
    }
    @Override public void configure(Map<String, ?> configs) {}
}
```

## Consult a friend?

> 🤝 **Slow down.** This is exactly-once-adjacent. With `processing.guarantee=exactly_once_v2`:
> - `CONTINUE` on a produce failure means the input record's offset will *still* be committed in the next transaction.
> - You silently lose the output. EOS now guarantees "at most once" for those records.
> - Verify with your tech lead that this is acceptable for the affected topics.

## When this might be a false positive

- Handler returns CONTINUE for one or two specific exception types, FAIL for the default — that's the correct shape.

## Detection strategy

- bytecode: scan implementations of `org.apache.kafka.streams.errors.ProductionExceptionHandler`.
- For the `handle` method, verify there exists at least one return path returning `FAIL`. If every path returns `CONTINUE`, flag.
- Confidence HIGH.

## References

- Apache Kafka — `DefaultProductionExceptionHandler`: https://kafka.apache.org/27/javadoc/org/apache/kafka/streams/errors/DefaultProductionExceptionHandler.html
- KIP-210 — Custom error handling for Streams production: https://cwiki.apache.org/confluence/display/KAFKA/KIP-210+-+Provide+for+custom+error+handling++when+Kafka+Streams+fails+to+produce
