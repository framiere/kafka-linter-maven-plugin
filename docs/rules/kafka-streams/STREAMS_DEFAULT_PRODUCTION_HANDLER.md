# STREAMS_DEFAULT_PRODUCTION_HANDLER

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: A `RecordTooLargeException` shouldn't kill your topology — set a production handler.

## TL;DR

The linter flags Streams configs where `default.production.exception.handler` is not set. The default fails the StreamThread on any producer-side error (e.g. `RecordTooLargeException`, `UnknownTopicOrPartitionException`).

## What's happening (the mechanism)

Symmetric to the deserialization handler: when the embedded producer's `Callback` reports a non-retriable error, the production exception handler decides between `FAIL` and `CONTINUE`. The default `DefaultProductionExceptionHandler` returns `FAIL` for every error. One oversized record (over `message.max.bytes`), one missing topic, one ACL change — and the thread dies.

`RecordTooLargeException` is special: it's not retriable (the record will never fit, no matter how many retries) and it's commonly thrown by aggregations that accumulate state into a value that grows beyond the broker limit. With the default handler, your aggregation pipeline halts; with a `CONTINUE` handler that DLQs the oversized record, you stay alive.

## Operational impact

- A single oversized record on a sink topic stops the entire StreamThread.
- Internal repartition / changelog topics also use this handler — a misconfigured broker (e.g. `message.max.bytes` smaller than your store value size) takes down the topology silently.
- `kafka.streams:type=stream-metrics,client-id=...:failed-stream-threads` ticks up. Replace-thread handlers will rebuild and hit the same poison record. Infinite restart loop.

## How to fix

```java
// BAD — implicit default (FAIL)

// GOOD — built-in handler that DLQs and continues for RecordTooLarge specifically
public class TolerantProductionHandler implements ProductionExceptionHandler {
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception ex) {
        if (ex instanceof RecordTooLargeException) {
            log.error("Dropping oversized record key={}, size={}", record.key(), record.value().length);
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
        return ProductionExceptionHandlerResponse.FAIL;
    }
    public void configure(Map<String, ?> configs) {}
}

props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
          TolerantProductionHandler.class);
```

KIP-1033 renamed to `production.exception.handler` (no `default.` prefix). Both keys work.

## When this might be a false positive

- Strict-pipelines where any producer error must halt the system. Acceptable; suppress per-module.

## Detection strategy

- Config files: neither `default.production.exception.handler` nor `production.exception.handler` is set. Flag with MEDIUM confidence (sometimes default is intentional).
- Bytecode: no `Properties.put` for either key.
- Confidence: MEDIUM — fail-fast on producer errors is a defensible default for some teams.

## References

- Streams config — default.production.exception.handler: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#default-production-exception-handler
- KIP-1033 — Add Kafka Streams exception handler for exceptions occurring during processing: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § Production exception handler.
