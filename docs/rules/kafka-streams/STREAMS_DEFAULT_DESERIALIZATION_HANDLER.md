# STREAMS_DEFAULT_DESERIALIZATION_HANDLER

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: One poison pill kills the whole topology by default.

## TL;DR

The linter flags Streams configs where `default.deserialization.exception.handler` (or the new `deserialization.exception.handler`) is not set. The default is `LogAndFailExceptionHandler`: the first malformed record on any source topic kills the stream thread.

## What's happening (the mechanism)

When Kafka Streams pulls a record and the configured `Serde` throws while deserializing, control transfers to the deserialization exception handler. Three built-ins ship with Kafka:

- `LogAndFailExceptionHandler` — *default*. Logs the error then returns `FAIL`. The StreamThread is torn down. With one thread, the instance dies.
- `LogAndContinueExceptionHandler` — logs the error and skips the record. Safe-ish, but you lose data silently.
- A custom handler that routes to a DLQ topic and returns `CONTINUE`.

If you ship to prod without setting this, a single non-Avro record on an Avro topic — or a schema-registry hiccup, or a producer using a newer schema you haven't deployed yet — takes down every instance one by one as each thread polls the bad record. Rebalances repeatedly re-assign the partition to a fresh victim.

Note: KIP-1033 renamed the property to `deserialization.exception.handler` (dropping the `default.` prefix) and added context. The old key still works, deprecated.

## Operational impact

- One poison-pill record → all instances cascade-fail. `alive-stream-threads` drops to 0 across the fleet.
- Lag on the affected partition grows linearly with input rate.
- Logs show the exact stack trace once per instance, then silence. Operators wake up to "all my pods restarting in a loop".
- `kafka.streams:type=stream-thread-metrics,...:dropped-records-rate` is what you'd see with `LogAndContinue` — it stays at 0 with the default Fail handler because no records were "dropped"; the thread just died.

## How to fix

For a managed DLQ pattern:

```java
// GOOD — built-in continue (data loss, but topology stays alive)
props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogAndContinueExceptionHandler.class);

// BETTER — custom handler that publishes the raw bytes to a DLQ
props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          DlqDeserializationExceptionHandler.class.getName());
```

```java
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private Producer<byte[], byte[]> dlqProducer;
    private String dlqTopic;
    public DeserializationHandlerResponse handle(ProcessorContext ctx, ConsumerRecord<byte[], byte[]> record, Exception ex) {
        dlqProducer.send(new ProducerRecord<>(dlqTopic, record.key(), record.value()));
        return DeserializationHandlerResponse.CONTINUE;
    }
    // configure() / close() omitted
}
```

For 4.x: prefer the new name `deserialization.exception.handler` (KIP-1033).

## When this might be a false positive

- Single-topic toy apps where data loss is acceptable and `LogAndFail` is fine for "early loud failure".
- Apps that already wrap their Serde in a try/catch returning a sentinel — strongly discouraged but possible.

## Detection strategy

- Config files: neither `default.deserialization.exception.handler` nor `deserialization.exception.handler` is set. Flag.
- Bytecode: no `Properties.put` for either key, and no `StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG` setter.
- Confidence: HIGH (the default is dangerous in production).

## References

- Streams config — default.deserialization.exception.handler: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#default-deserialization-exception-handler
- KIP-1033 — Custom processing exception handler: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing
- Confluent — Handle deserialization errors: https://docs.confluent.io/platform/current/streams/faq.html#handling-corrupted-records-and-deserialization-errors-poison-pill-records

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/production-hardening.md § Deserialization exception handler, kafka-streams-programming/SKILL.md § Invariant Checklist #7.
