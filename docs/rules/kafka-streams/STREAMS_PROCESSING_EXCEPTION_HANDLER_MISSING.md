# STREAMS_PROCESSING_EXCEPTION_HANDLER_MISSING
**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: A KIP-1034 handler is the only thing between a lambda NPE and a fleet-wide restart loop.
**Source**: Confluent agent-skills — kafka-streams-programming/references/production-hardening.md § Error Handling (Layer 2), kafka-streams-programming/SKILL.md § Invariant Checklist #4.

## TL;DR

The linter flags Kafka Streams configs (Kafka 3.9+ / CP 7.8+) where `processing.exception.handler` is unset. KIP-1034 added a dedicated handler for exceptions thrown **inside** topology operators (map / filter / aggregate lambdas, join value joiners). Without it, the default behavior is to fail the stream thread on any such exception — the same crash-loop cascade as a missing deserialization handler, just one layer up.

## What's happening (the mechanism)

Kafka Streams now has four layers of error handling. Each catches a different class of failure:

1. `DeserializationExceptionHandler` — corrupt input bytes (KIP-161, see `STREAMS_DEFAULT_DESERIALIZATION_HANDLER`).
2. **`ProcessingExceptionHandler` (KIP-1034, Kafka 3.9)** — user-code exceptions in topology lambdas.
3. `ProductionExceptionHandler` — failures writing output / changelog records (see `STREAMS_PRODUCTION_HANDLER_MISSING`).
4. `StreamsUncaughtExceptionHandler` — anything that escaped the other three (see `STREAMS_UNCAUGHT_HANDLER_MISSING`).

The default `ProcessingExceptionHandler` returns `FAIL` — any NullPointerException, ArithmeticException, or business-logic throw from inside `mapValues(...)`, `filter(...)`, an `Aggregator`, or a join `ValueJoiner` shuts the thread down. Kubernetes restarts the pod; the same record comes back; the thread dies again. Crash-loop.

Confluent ships two built-ins:
- `LogAndContinueProcessingExceptionHandler` — log and skip (dev default).
- `DeadLetterQueueExceptionHandler` (Kafka 4.x) — route the offending record to a DLQ topic auto-named `<application.id>-<source-topic>-dlq`. Production default.

## Operational impact

- One unhandled `null` field in a `mapValues(rec -> rec.getNested().getId())` lambda DDoSes the entire deployment until the bad record is purged or skipped.
- Same operational shape as a missing deserialization handler, but the root cause is harder to find — operators expect deserialization to be the fragile boundary, not the topology body.
- Pre-KIP-1034 Streams users would set `default.deserialization.exception.handler` and call it done. Post-KIP-1034 that's no longer enough — there's a second handler to wire up.

## How to fix (bad → good code)

```properties
# BAD — only the deserialization handler is set; topology-internal exceptions FAIL
default.deserialization.exception.handler=org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
# (no processing.exception.handler)

# GOOD (dev) — log and continue on lambda exceptions
processing.exception.handler=org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler

# GOOD (prod) — route bad records to a DLQ
processing.exception.handler=org.apache.kafka.streams.errors.DeadLetterQueueExceptionHandler
```

The DLQ handler creates `<application.id>-<source-topic>-dlq` headers carrying original topic/partition/offset, the thrown exception, and a timestamp. **Pre-create the DLQ topic** — production clusters typically run with `auto.create.topics.enable=false`.

## Consult a friend?

Yes if you're enabling `DeadLetterQueueExceptionHandler` on a financial / order pipeline. A handler that "continues" past a topology exception is silent data loss by another name — make sure the team understands that DLQ records still represent records the topology *did not process correctly*, and that the DLQ topic is monitored (lag, rate alarms).

## When this might be a false positive

- Pre-Kafka-3.9 Streams: the property does not exist. Detect this via the `kafka-streams` dependency version.
- A topology that intentionally lets failures bubble to `StreamsUncaughtExceptionHandler` for a max-failures-per-window response (some teams prefer this for unambiguous loud-fail semantics on stateless apps). Document the choice.

## Detection strategy

- Config: `processing.exception.handler` is absent or empty.
- Dependency: `org.apache.kafka:kafka-streams` >= 3.9.0 — the property exists from that version.
- Confidence: HIGH. The default (FAIL) is the dangerous one.
- Bytecode (companion): if a custom handler is set via `props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ...)`, verify it doesn't blanket-return `CONTINUE` — same pathology as `STREAMS_DESER_HANDLER_BLANKET_CONTINUE`.

## References

- KIP-1034 — Add Kafka Streams exception handler for exceptions occurring during processing: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034
- Confluent agent-skills — kafka-streams-programming/references/production-hardening.md § Error Handling (Layer 2)
- Confluent agent-skills — kafka-streams-programming/SKILL.md § Invariant Checklist #4
- Related rules: [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md), [STREAMS_UNCAUGHT_HANDLER_MISSING](STREAMS_UNCAUGHT_HANDLER_MISSING.md), [STREAMS_PRODUCTION_HANDLER_MISSING](../observability/STREAMS_PRODUCTION_HANDLER_MISSING.md)
