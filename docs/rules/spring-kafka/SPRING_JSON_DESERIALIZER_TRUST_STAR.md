# SPRING_JSON_DESERIALIZER_TRUST_STAR

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: combination
**Tagline**: `spring.json.trusted.packages=*` is `eval()` over the wire.

## TL;DR

The linter flags `spring.json.trusted.packages=*` (or programmatic `addTrustedPackages("*")`). It disables the package-allowlist check and lets any class on the classpath be instantiated from a producer-controlled `__TypeId__` header — the classic Java deserialization gadget surface.

## What's happening (the mechanism)

`JsonDeserializer` reads the FQCN from the message's `__TypeId__` header (or whatever type-mapper resolves) and asks Jackson to deserialize into that class. The trusted-packages allowlist is the only gate between the wire and `Class.forName(...)` of an attacker-chosen class.

`*` removes that gate. Any class on the classpath is instantiable. Combined with Jackson's polymorphic deserialization and any class with a side-effecting setter / constructor (the "gadget" patterns from CVE-2019-12086 and similar), this is a remote code execution surface.

The threat model is not theoretical:
- A compromised producer can send records with crafted type headers.
- A cross-tenant Kafka cluster where any tenant can publish to a shared topic.
- Replay of a captured record from a previously-trusted producer that was later compromised.
- A DLT consumer that reads records from arbitrary upstream sources (the DLT is by definition a graveyard of records that broke type assumptions).

Spring Kafka's docs explicitly warn against `*`: *"Setting `*` disables the check — only use it in trusted environments."* In practice, "trusted environment" is a fragile claim that erodes as the org grows.

## Operational impact

- Direct: remote code execution surface, severity depends on classpath gadgets.
- Indirect: an audit finds `*` and demands an emergency change; a hurried switch to specific packages breaks producers using FQCN type headers from outside the allowlist.
- Compliance: many infosec frameworks (SOC2, ISO 27001) call this out explicitly under "deserialization of untrusted data".

## How to fix

```properties
# BAD
spring.kafka.consumer.properties.spring.json.trusted.packages=*

# GOOD — explicit allowlist
spring.kafka.consumer.properties.spring.json.trusted.packages=com.example.events,com.example.shared

# BEST — type mappings, no FQCNs on the wire
spring.kafka.consumer.properties.spring.json.type.mapping=order:com.example.Order,refund:com.example.Refund
spring.kafka.producer.properties.spring.json.type.mapping=order:com.example.Order,refund:com.example.Refund
# (and no trusted.packages needed beyond java.* defaults, because the wire carries tokens)
```

```java
// BAD
deser.addTrustedPackages("*");

// GOOD
deser.addTrustedPackages("com.example.events");
```

If the topic carries records from many internal services and you cannot enumerate packages, switch to type mappings — the producer side declares the mapping, the consumer side does too, and the wire carries `order` not `com.acme.crm.events.shared.Order`. No allowlist needed.

## When this might be a false positive

- None for production. Strict suppression by file path (e.g., `application-local.yml`) is acceptable but the security team should still sign off.

## Detection strategy

- Config: find `spring.json.trusted.packages` (under any of the various Spring Boot paths) with value `*` or containing `*` as a comma-separated entry.
- Bytecode: find `INVOKEVIRTUAL JsonDeserializer.addTrustedPackages` or `trustedPackages` with a `LDC "*"` argument.
- Confidence: HIGH — the pattern is unambiguous.

## References

- Spring Kafka — JSON Deserialization Security: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
- CWE-502: Deserialization of Untrusted Data: https://cwe.mitre.org/data/definitions/502.html
- Spring Kafka — Type Mappings: https://docs.spring.io/spring-kafka/reference/kafka/serdes.html
