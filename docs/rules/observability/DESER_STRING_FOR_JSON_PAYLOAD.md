# DESER_STRING_FOR_JSON_PAYLOAD

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: `StringDeserializer` on a JSON topic catches no structural errors — your "deserializer" is a UTF-8 decoder.

## TL;DR

The linter flags consumers configured with `org.apache.kafka.common.serialization.StringDeserializer` whose listener body parses the string as JSON (`ObjectMapper.readValue`, `Jackson`, `Gson`, `JsonParser`). `StringDeserializer` does the wire-level decode and stops there; JSON validity is checked later by user code. That deferral means the framework's poison-pill machinery (DLT, error handler, `ErrorHandlingDeserializer`) never fires for structural JSON errors — they surface as ordinary listener exceptions, get caught/logged, and the structural problem is invisible to the platform.

## The setup

Team starts simple: `StringDeserializer` for the value, parse the JSON manually in the listener. It works. They add `ErrorHandlingDeserializer` over `StringDeserializer` for "poison pill protection." That wrapper does nothing useful — `StringDeserializer` accepts any byte sequence as a valid String, so there's no exception to wrap. The "poison pill" path is silent.

## What's actually happening

`StringDeserializer.deserialize(byte[])` returns `new String(bytes, configured charset)`. Any byte sequence is a valid input. There's no notion of "this is malformed" at the deserializer level.

The JSON parse happens later, in the listener body:
```java
Order o = mapper.readValue(record.value(), Order.class);  // can throw JsonProcessingException
```

When this throws, it's an ordinary listener exception. Spring's `CommonErrorHandler` does see it. *But*:
- `ErrorHandlingDeserializer` is no help — the deserializer didn't fail.
- `DeserializationException` is not the exception type → fatal-exception heuristics in `DefaultErrorHandler` treat JSON parse errors as retryable, retrying the same poison record 10 times before sending to DLT (or dropping if no recoverer — see `SPRING_DEH_NO_DLT_RECOVERER`).
- Per-poison-pill metrics counters that look at deserializer-level failures stay at zero.

A real JSON deserializer (`JsonDeserializer`, `JacksonJsonDeserializer`, custom `Deserializer<Order>`) wraps the parse in `Deserializer.deserialize` and throws a `SerializationException` / `DeserializationException`. With `ErrorHandlingDeserializer`, this becomes a header-stamped null record that the container's machinery handles via the right path.

## Why this is subtle

- "It works in dev" — every JSON record parses, no exceptions.
- The listener exception path *does* fire on parse errors, so engineers think their error handling is working.
- "It's just a string" sounds simpler than "it's a JSON value." Conflating the two saves a line of config but loses framework integration.
- The pattern is contagious: one engineer does it, the rest of the team copies it.

## Operational impact

- Poison pills cause the same 10-immediate-retry storm as any listener exception (rule `SPRING_DEH_DEFAULT_BACKOFF`).
- `ErrorHandlingDeserializer` wrapping does nothing useful → false sense of security.
- Poison-pill metrics that look at "deserializer exceptions" are zero — under-reporting.
- DLT entries (if configured) are tagged with `JsonProcessingException` instead of `DeserializationException` — DLT replayers that filter on deserialization errors miss them.

## Failure scenarios (walkthrough)

1. **The producer encoding bug.** Producer accidentally writes UTF-16 to a UTF-8 topic. `StringDeserializer` reads bytes-as-UTF-8 (valid, garbage out). JSON parse later throws `JsonParseException`. Listener retries 10x, eventually DLT'd or dropped. Metric: zero deserialization errors.

2. **The double-encoding.** Producer sends `JSON.stringify(JSON.stringify(obj))`. Outer is a valid JSON string. `StringDeserializer` decodes to bytes. Parse expects `{"...": ...}` but gets `"{\"...\": ...}"`. Throws. Same retry storm.

## How to fix

Use the right deserializer at the right level.

```java
// BAD
@Bean ConsumerFactory<String, String> cf() { ... StringDeserializer for value ... }

@KafkaListener
public void handle(String json) {
    Order order = mapper.readValue(json, Order.class);
    process(order);
}

// GOOD — JsonDeserializer with explicit type, wrapped in EHD
@Bean ConsumerFactory<String, Order> cf() {
    JsonDeserializer<Order> json = new JsonDeserializer<>(Order.class);
    json.addTrustedPackages("com.example");
    json.ignoreTypeHeaders();
    return new DefaultKafkaConsumerFactory<>(props,
        new StringDeserializer(),
        new ErrorHandlingDeserializer<>(json));
}

@KafkaListener
public void handle(Order order) {
    process(order);
}
```

If you genuinely need `String` (e.g., the payload isn't always JSON), keep `StringDeserializer` but mark the rule as suppressed and document why.

## When this might be a false positive

- The topic genuinely carries strings (logs, plain text, CSV) — JSON parsing is intentional and the rule doesn't apply.
- Schema-on-read pattern with multiple JSON variants where a single typed deserializer doesn't fit — but consider `JsonDeserializer` with `JsonTypeInfo` mappings.
- Test fixtures.

## Detection strategy

- bytecode: identify consumers using `StringDeserializer` (constant config value or class reference).
- bytecode: in the same `@KafkaListener` method (or downstream from `record.value()`), look for `ObjectMapper.readValue`, `JsonParser`, `Gson.fromJson`, `JsonbBuilder.create().fromJson`, etc.
- Confidence MEDIUM — many legitimate uses of `StringDeserializer` exist; the heuristic relies on detecting JSON-parse calls.

## References

- Apache Kafka — `StringDeserializer`: https://kafka.apache.org/40/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html
- Spring Kafka — `JsonDeserializer`: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- Confluent — Handling deserialization errors: https://www.confluent.io/blog/spring-kafka-can-your-kafka-consumers-handle-a-poison-pill/
