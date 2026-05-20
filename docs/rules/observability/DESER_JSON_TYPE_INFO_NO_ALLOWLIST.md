# DESER_JSON_TYPE_INFO_NO_ALLOWLIST

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + config-file
**Tagline**: `JsonDeserializer` with `__TypeId__` headers and no trusted-packages allowlist is RCE via a Kafka header.

## TL;DR

The linter flags `org.springframework.kafka.support.serializer.JsonDeserializer` usage where (a) `USE_TYPE_INFO_HEADERS=true` (the default) and (b) `TRUSTED_PACKAGES` is unset or includes `*`. Spring's default behavior is to read the class name from the record header `__TypeId__` and instantiate it via Jackson's polymorphic deserialization. An attacker who can publish to the topic can put any class name in the header — and any class on the classpath becomes instantiable. CVE-2023-34040 is the formal record of this exact pattern.

## The setup

Team uses `JsonDeserializer` with the convenient pattern: producer writes a `Map<String, Object>` and the deserializer figures out the type from the header. They never set `TRUSTED_PACKAGES`. The pattern works for years until either (a) a security audit catches it or (b) someone exploits it.

## What's actually happening

`JsonSerializer` (default `ADD_TYPE_INFO_HEADERS=true`) writes the fully-qualified class name of the payload into a Kafka header named `__TypeId__`:

```
__TypeId__: com.example.Order
__KeyTypeId__: java.lang.String
```

`JsonDeserializer` (default `USE_TYPE_INFO_HEADERS=true`) reads `__TypeId__`, loads that class via `Class.forName`, and asks Jackson to deserialize into it. The check that gates this lookup is `TRUSTED_PACKAGES`:

- Default: `java.util,java.lang` (very restrictive — won't even work for your own classes).
- Common bad pattern: `*` — trust everything.
- Common bad pattern: unset, fixed by writing application code to call `deserializer.trustedPackages("*")`.

With `*`, an attacker who can publish to the topic can set `__TypeId__: org.apache.commons.collections.functors.InvokerTransformer` (or any deserialization gadget chain) and Jackson will happily instantiate it. CVE-2023-34040 documents a related vector specifically through the `springDeserializerExceptionKey/Value` header when `ErrorHandlingDeserializer` is not configured — the deserialization header itself can carry a malicious payload.

`ErrorHandlingDeserializer` removes such headers before they reach the deserializer (this is the canonical fix), but the broader pattern — trust + polymorphic type from headers — remains a class of vulnerabilities.

## Why this is subtle

- The pattern is convenient. Single deserializer handles many types.
- Most "internal" topics are assumed to have only trusted producers — until an internal compromise turns them into attack channels.
- `trustedPackages("*")` is documented and supported. It's not flagged as deprecated or dangerous by the API itself.
- The deserialization vulnerability surfaces only when an attacker can write to the topic — which, for internal pipelines, requires either a compromised producer service or a broker ACL gap.

## Operational impact

- Worst case: RCE in the consumer process.
- Realistic case: DoS via instantiating an OOM-y class (`java.util.HashMap` with a giant header).
- Audit/compliance: trivial finding, "remote code execution via untrusted deserialization."

## Failure scenarios (walkthrough)

1. **The lateral-movement compromise.** Attacker compromises an internal microservice that has produce permission to topic `internal-events`. They craft a record with `__TypeId__` pointing at a Java deserialization gadget (e.g., `org.springframework.beans.factory.config.BeanFactoryReference` style). The consumer service (running with `trustedPackages("*")`) instantiates it. Code executes in the consumer's process — which has different (and more sensitive) permissions than the producer.

2. **The bypass via `ErrorHandlingDeserializer` absence.** Per CVE-2023-34040: an attacker sets `springDeserializerExceptionValue` header on a normal record. With `checkDeserExWhenValueNull=true` and no `ErrorHandlingDeserializer`, Spring tries to deserialize the header content as a `DeserializationException` — Java deserialization, all gadgets available.

## How to fix

```java
// BAD
JsonDeserializer<Order> deser = new JsonDeserializer<>();
deser.addTrustedPackages("*");

// BAD (subtle) — relies on type headers + no allowlist set
JsonDeserializer<Order> deser = new JsonDeserializer<>(Order.class);
// without trustedPackages, default is java.util,java.lang only — Order won't deserialize
// engineers often "fix" by adding "*"

// GOOD — narrow allowlist + concrete target type
JsonDeserializer<Order> deser = new JsonDeserializer<>(Order.class);
deser.addTrustedPackages("com.example.orders", "com.example.shared");
deser.ignoreTypeHeaders();  // USE_TYPE_INFO_HEADERS = false
// type comes from constructor, headers are ignored entirely
```

```properties
# GOOD — config-file
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.orders,com.example.shared
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=com.example.orders.Order
```

The truly safe pattern: disable type headers (`ignoreTypeHeaders`), specify the target type explicitly. This removes the attack surface entirely — the deserializer can't be tricked into instantiating an arbitrary class because it isn't reading the class name from the wire.

Always pair with `ErrorHandlingDeserializer` — it strips malicious deserialization-exception headers per CVE-2023-34040.

## Consult a friend?

> 🤝 **Slow down.** Security boundaries around Kafka are easy to misjudge.
> 1. Identify exactly who can write to the topic. ACLs on the broker, not "we assume only our services do."
> 2. If multiple services publish, identify the smallest set of payload types and use a type allowlist with `TYPE_MAPPINGS` rather than free-form class names.
> 3. Upgrade to Spring for Apache Kafka ≥ 3.0.10 (or 2.9.11) for the CVE-2023-34040 fix.
> 4. If genuinely polymorphic, use Avro/Protobuf with schema registry instead — schema-validated payloads cannot be turned into RCE gadgets.

## When this might be a false positive

- Topic ACLs restrict produce to a single, fully-trusted service with no end-user input.
- The deserializer is in a test fixture.
- The deserializer is wrapped in `ErrorHandlingDeserializer` AND a narrow `TRUSTED_PACKAGES` is set AND `USE_TYPE_INFO_HEADERS=false`.

## Detection strategy

- bytecode: instantiation of `JsonDeserializer` (and `JacksonJsonDeserializer` in spring-kafka 4.x).
- bytecode: trace `trustedPackages("*")`, `addTrustedPackages("*")`. Always flag.
- bytecode: trace absence of `ignoreTypeHeaders()` AND absence of `trustedPackages(...specific...)`.
- config-file: scan for `spring.json.trusted.packages=*` (flag), absent + presence of `spring.json.use.type.headers` not explicitly `false` (flag).
- Confidence HIGH — string `"*"` is unambiguous.

## References

- Spring Kafka — `JsonDeserializer`: https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/serializer/JsonDeserializer.html
- Spring Kafka — serdes reference: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- CVE-2023-34040 — Spring Kafka deserialization vulnerability: https://spring.io/security/cve-2023-34040/
- Baeldung — Trusted packages: https://www.baeldung.com/spring-kafka-trusted-packages-feature
