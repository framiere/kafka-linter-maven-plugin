# SPRING_JSON_DESERIALIZER_NO_TRUSTED_PACKAGES

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: JsonDeserializer without trusted packages will refuse your own classes — until someone widens it to `*`.

## TL;DR

The linter flags configurations using `org.springframework.kafka.support.serializer.JsonDeserializer` without setting `spring.json.trusted.packages` (or programmatic `addTrustedPackages` / `trustedPackages`). The default is to trust only `java.util` and `java.lang`, which means production deserialization fails — and the typical "fix" applied under pressure is to set the property to `*`, which is a deserialization-attack surface.

## What's happening (the mechanism)

`JsonDeserializer` reads a `__TypeId__` header (or a configured default type) and deserializes the payload via Jackson. Before instantiating any class from the type header, it checks whether the class's package is in the *trusted packages* list. By default that list is `[java.util, java.lang]` only.

Without configuration:
- Your `com.example.Order` is not trusted. Deserialization fails with `IllegalArgumentException: The class 'com.example.Order' is not in the trusted packages`.
- The poison record blocks the poll loop until an `ErrorHandlingDeserializer` wrapper is in place.

The typical fix: add `spring.json.trusted.packages=*`. That removes the package check entirely — any class on the classpath can be instantiated from a producer-controlled type header, which is the classic Java deserialization gadget surface. See `SPRING_JSON_DESERIALIZER_TRUST_STAR` for that follow-on rule.

The right fix is to list your own packages explicitly.

## Operational impact

- Listener fails to deserialize on first record. Without `ErrorHandlingDeserializer`, the container loops on the same offset forever (the failure is inside `poll()`, before Spring's error handler sees it).
- With `ErrorHandlingDeserializer`, the record goes to the recoverer / DLT — but every record from a producer using type headers fails the same way.
- On-call response: "just set it to `*`, ship it" — turns a denial-of-service into a remote-code-execution surface.

## How to fix

```properties
# BAD
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# GOOD
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.orders,com.example.shared.events
spring.kafka.consumer.properties.spring.json.value.default.type=com.example.orders.Order
```

```java
// GOOD — programmatic
JsonDeserializer<Order> deser = new JsonDeserializer<>(Order.class);
deser.addTrustedPackages("com.example.orders", "com.example.shared.events");
```

Even better, use *type mappings* so the wire format carries a short token rather than a FQCN:

```properties
spring.kafka.consumer.properties.spring.json.type.mapping=order:com.example.orders.Order,refund:com.example.orders.Refund
spring.kafka.producer.properties.spring.json.type.mapping=order:com.example.orders.Order,refund:com.example.orders.Refund
```

This decouples wire schema from internal class names and dodges the trusted-packages question for the typed cases.

## When this might be a false positive

- The application sets `spring.kafka.consumer.value-deserializer=...JsonDeserializer` but every consumer factory bean overrides it programmatically with `addTrustedPackages`. Bean-graph reasoning is needed; downgrade to INFO when bytecode shows the call.
- Tests that use a typed `JsonDeserializer<Specific>` constructor with no need for type headers.

## Detection strategy

- Config: find `spring.kafka.consumer.value-deserializer` or `key-deserializer` equal to `JsonDeserializer` (FQCN or short).
- Config: check for the companion `spring.json.trusted.packages` (in `spring.kafka.consumer.properties[...]` form).
- Bytecode: when configuring `JsonDeserializer` programmatically (`new JsonDeserializer<>(...)`), look for `INVOKEVIRTUAL addTrustedPackages` / `trustedPackages`.
- Flag when no trusted packages are configured.
- Confidence: MEDIUM.

## References

- Spring Kafka — Serialization & Deserialization (JSON): https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- Spring Kafka — `JsonDeserializer.TRUSTED_PACKAGES` constant: https://docs.spring.io/spring-kafka/docs/current/api/org/springframework/kafka/support/serializer/JsonDeserializer.html
