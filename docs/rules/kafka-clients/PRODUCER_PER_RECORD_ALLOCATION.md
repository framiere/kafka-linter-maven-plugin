# PRODUCER_PER_RECORD_ALLOCATION

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode
**Tagline**: Producers are oxygen — share one, breathe easy.

## TL;DR

The linter flags methods that allocate a fresh `new KafkaProducer<>(...)` per call rather than reusing a long-lived instance. Producer construction is expensive (metadata fetch, network thread, sender thread) and instances are designed to be shared across the JVM.

## What's happening (the mechanism)

`new KafkaProducer<>(props)` is heavy:
- Creates a `Sender` thread.
- Creates a `NetworkClient` with broker connections (TCP handshake + TLS if applicable + SASL if applicable).
- Fetches initial cluster metadata (blocking up to `max.block.ms`).
- Allocates a `RecordAccumulator` of `buffer.memory` bytes.

`KafkaProducer` is thread-safe and is explicitly recommended to be shared across threads — that's the entire point of the accumulator + sender thread design.

Building a producer per `sendOne(...)` call:
- Costs ~50–200ms per construction (metadata + TLS handshake).
- Forks an extra OS thread per call.
- Allocates and immediately discards 32 MiB of buffer (default).
- Defeats batching (each producer's accumulator sees one record).
- Holds N broker connections briefly, churning broker connection metrics.

This is distinct from `PRODUCER_IN_LOOP` (already covered) — that rule flags `send()` inside loops; this one flags `new KafkaProducer()` inside frequently-called methods.

## Operational impact

- 100–1000x throughput collapse vs. shared producer.
- Broker connection churn: `kafka.server:type=socket-server-metrics:connection-count` saw-tooth pattern.
- Per-request latency dominated by producer construction.
- File descriptor leaks if `close()` is also missing.

## How to fix

```java
// BAD — producer per request
@PostMapping("/event")
public void publish(@RequestBody Event e) {
    try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
        p.send(new ProducerRecord<>("events", e.id, e.json));
    }
}

// GOOD — share a single instance
private final KafkaProducer<String, String> producer; // field, injected or lazy singleton

@PostMapping("/event")
public void publish(@RequestBody Event e) {
    producer.send(new ProducerRecord<>("events", e.id, e.json));
}
```

## When this might be a false positive

- Throw-away tooling (one-shot batch jobs).
- Tests using ephemeral producers around a Testcontainers cluster.

## Detection strategy

- Bytecode: detect `new KafkaProducer(...)` allocations whose enclosing method is a request handler (heuristics: annotated with `@RequestMapping`, `@GetMapping`, `@PostMapping`, JAX-RS `@Path`, MicroProfile `@Inject` injection point, Quarkus REST endpoint), OR is called from a loop. HIGH in those contexts; MEDIUM otherwise.
- Suppress if the result is stored in a static field or returned from a singleton accessor.

## References

- KafkaProducer javadoc — "The producer is thread safe and sharing a single producer instance across threads will generally be faster than having multiple instances.": https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
- Confluent — Producer best practices: https://docs.confluent.io/platform/current/clients/producer.html
