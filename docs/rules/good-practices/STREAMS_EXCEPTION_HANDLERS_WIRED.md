# STREAMS_EXCEPTION_HANDLERS_WIRED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Every Streams app needs three handlers wired, not one missing.

## TL;DR

A production Streams app must explicitly configure all three exception
handlers: `default.deserialization.exception.handler`,
`default.production.exception.handler`, and `StreamsUncaughtExceptionHandler`
(set programmatically via `kafkaStreams.setUncaughtExceptionHandler(...)`).
The defaults are not safe choices — they're conservative for backward
compatibility. The good-practice form is to wire deliberate handlers
that route to a DLT and surface metrics.

## The setup

Three independent failure surfaces:

1. **Deserialization** — a poison record at the input. Handled by
   `DeserializationExceptionHandler` (the
   `default.deserialization.exception.handler` config). Default
   `LogAndFailExceptionHandler` — first poison record stops the app.
2. **Production** — a downstream write fails (broker unavailable,
   record too big, etc.). Handled by `ProductionExceptionHandler` (the
   `default.production.exception.handler` config). Default
   `DefaultProductionExceptionHandler` — always FAILs, even on retriable
   errors. Anti-pattern doc:
   [STREAMS_PRODUCTION_HANDLER_MISSING](../observability/STREAMS_PRODUCTION_HANDLER_MISSING.md).
3. **Processing** — an uncaught exception in a processor / transformer.
   Handled by `StreamsUncaughtExceptionHandler` (KIP-671, set
   programmatically). Default: kills the entire JVM instance.

The anti-pattern catalog covers the failure modes (handler missing or
blanket-CONTINUE). The good-practice catalog says: **wire all three**,
with deliberate routing (DLT for deser, retry/fail for production,
REPLACE_THREAD for transient processor errors).

## Cross-reference

The anti-pattern docs already cover the mechanisms in detail:

- [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](../kafka-streams/STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md)
- [STREAMS_DEFAULT_PRODUCTION_HANDLER](../kafka-streams/STREAMS_DEFAULT_PRODUCTION_HANDLER.md)
- [STREAMS_PRODUCTION_HANDLER_MISSING](../observability/STREAMS_PRODUCTION_HANDLER_MISSING.md)
- [STREAMS_DESER_HANDLER_BLANKET_CONTINUE](../observability/STREAMS_DESER_HANDLER_BLANKET_CONTINUE.md)
- [STREAMS_PROD_HANDLER_BLANKET_CONTINUE](../observability/STREAMS_PROD_HANDLER_BLANKET_CONTINUE.md)
- [STREAMS_UNCAUGHT_HANDLER_MISSING](../kafka-streams/STREAMS_UNCAUGHT_HANDLER_MISSING.md)
- [STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API](../observability/STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md)

## What the good-practice adds

Wiring discipline. The rule fires if ALL of these are simultaneously
absent / default:

- `default.deserialization.exception.handler` unset (defaults to
  LogAndFail).
- `default.production.exception.handler` unset.
- No `kafkaStreams.setUncaughtExceptionHandler(...)` in the startup
  code.

If any one is explicitly configured but the others are default, defer
to the per-handler anti-pattern rules. The combined "all three default"
is the signal that nobody on the team has thought about failure
handling yet.

```java
// yes — all three wired
Properties p = new Properties();
p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      DltDeserializationExceptionHandler.class.getName());
p.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
      RetriableProductionExceptionHandler.class.getName());

KafkaStreams streams = new KafkaStreams(topology, p);
streams.setUncaughtExceptionHandler(ex -> {
    if (ex instanceof TransientException) {
        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
});
```

## Detection strategy

- **Config files:** flag if all three are default/missing.
- **Bytecode:** detect `KafkaStreams` startup paths without a
  `setUncaughtExceptionHandler` call.
- **Confidence: HIGH** — provable from the absence of three constants.

## References

See the linked anti-pattern docs. KIP-210, KIP-671, KIP-1033.
