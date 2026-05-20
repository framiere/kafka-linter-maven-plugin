# QK_NATIVE_REFLECTION_MISSING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: annotation + bytecode
**Tagline**: GraalVM doesn't read your mind — register that class.

## TL;DR

Custom Kafka `Serializer`/`Deserializer`/`Partitioner`/`ConsumerInterceptor` classes referenced only by string in config (`value.deserializer=com.example.FooDeserializer`) get reflectively instantiated by the Kafka client. Without `@RegisterForReflection` (or `reflect-config.json`), native image strips them.

## What's happening (the mechanism)

Quarkus's native image build performs reachability analysis from bytecode. Classes referenced only as strings (`Class.forName("...")`) are NOT reachable from static analysis. Kafka's client config loads:

- `key.deserializer` / `value.deserializer`
- `key.serializer` / `value.serializer`
- `partitioner.class`
- `interceptor.classes`
- `key-deserialization-failure-handler` / `value-deserialization-failure-handler` (via CDI, mostly safe)

via `Class.forName(configValue).newInstance()`. The class is in your jar, but native-image has discarded its constructor metadata.

Symptoms in native mode: `KafkaException: Could not find a public no-argument constructor for com.example.FooDeserializer`.

## Operational impact

- JVM build works perfectly. Native build deploys and crashes on first record.
- Stack traces are unhelpful (`ReflectiveOperationException` with no class context in some Quarkus versions).
- Root cause is buried in `reflect-config.json` discovery.

## How to fix

```java
// GOOD — annotate the class
@RegisterForReflection
public class FooDeserializer implements Deserializer<Foo> { ... }

// OR — register from a separate scanner class
@RegisterForReflection(targets = { FooDeserializer.class, FooPartitioner.class })
public class KafkaReflectionConfig {}
```

For Quarkus + Kafka, the `quarkus-messaging-kafka` extension auto-registers known serializers, but custom ones must be opt-in.

## When this might be a false positive

- JVM-only builds (no native).
- Class is referenced directly from bytecode elsewhere — already reachable.
- Quarkus build step already registered it (some extensions do this for known patterns).

## Detection strategy

- Config: scan `mp.messaging.*.{value,key}.{serializer,deserializer}`, `*.partitioner.class`, `*.interceptor.classes` for fully-qualified class names.
- Bytecode: check whether the named class has `@RegisterForReflection` (or is in a `reflect-config.json` we can detect).
- Confidence: MEDIUM — needs cross-reference of config string → class metadata.

## References

- https://quarkus.io/guides/writing-native-applications-tips
- https://quarkus.io/guides/kafka (Native mode)
