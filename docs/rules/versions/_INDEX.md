# version & dependency rules

Build-time anti-pattern checks for the **`pom.xml`** of any Kafka-touching project: client / library version EOL, CVE exposure, framework BOM mismatches, transitive dependency hygiene. These rules read the Maven object model (`MavenProject`) — no bytecode involved.

The checks fall into six families:

1. **Kafka client and Streams versions** — EOL, KIP-679 era defaults, deprecated config keys.
2. **CVE exposure** — Kafka clients lines with known unpatched vulnerabilities.
3. **Spring Boot / spring-kafka** — Boot EOL, kafka-clients override, spring-kafka × Boot major mismatch, duplicate declarations.
4. **Quarkus** — Quarkus EOL, BOM-override traps, missing companion extensions, renamed artifacts.
5. **Serdes & schema registries** — Confluent Platform line, Apicurio v1 vs v2, mixed-vendor clients.
6. **Build hygiene** — JDK level, test artifacts in production scope, multi-module drift.

## Legend

**Severity**
- `ERROR` — almost always a bug. Build won't run, will hard-fail at startup, exposes a remotely-exploitable CVE, or commits to an EOL line whose security path is closed.
- `WARNING` — likely a bug. Legitimate exceptions exist; commercial support contracts, deliberate broker pinning, library-internal compatibility shims.

**Confidence**
- `HIGH` — the linter reads the resolved version directly; the EOL table or CVE range is unambiguous.
- `MEDIUM` — depends on a cross-module assumption or on inferring intent from coupled dependencies.
- `CONTEXT` — depends on deployment context (broker version, commercial support contract) the linter cannot see.

**Detection** — `pom-dependency`, `pom-property`, or a combination (` + `, alphabetical).

## Catalog (28)

### Apache Kafka client & Streams

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [KAFKA_CLIENTS_EOL](./KAFKA_CLIENTS_EOL.md) | ERROR | HIGH | pom-dependency | A Kafka client older than the bugs you'd file against it. |
| [KAFKA_CLIENTS_PRE_KIP679](./KAFKA_CLIENTS_PRE_KIP679.md) | WARNING | HIGH | pom-dependency | A client from before durable defaults — your producer is fire-and-forget unless you said otherwise. |
| [KAFKA_CLIENT_BROKER_LAG](./KAFKA_CLIENT_BROKER_LAG.md) | WARNING | CONTEXT | pom-property | Your client is forward-compatible with a broker eighteen months in the future — but missing four bug fixes. |
| [KAFKA_SCALA_BUNDLE_IMPORTED](./KAFKA_SCALA_BUNDLE_IMPORTED.md) | ERROR | HIGH | pom-dependency | You wanted a Kafka client. You pulled in Scala, the broker, and a small star system. |
| [KAFKA_CLIENT_TYPO_GROUP](./KAFKA_CLIENT_TYPO_GROUP.md) | ERROR | HIGH | pom-dependency | There is no io.confluent:kafka-clients. There never has been. |
| [KAFKA_CLIENTS_SNAPSHOT](./KAFKA_CLIENTS_SNAPSHOT.md) | WARNING | HIGH | pom-dependency | A SNAPSHOT or milestone client in production is a contract you didn't sign. |
| [KAFKA_STREAMS_CACHE_CONFIG_RENAMED](./KAFKA_STREAMS_CACHE_CONFIG_RENAMED.md) | WARNING | HIGH | bytecode + config-file + pom-dependency | cache.max.bytes.buffering was renamed in Streams 3.4 — using the old name silently disables your cache. |

### Apache Kafka CVE exposure

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER](./KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER.md) | ERROR | HIGH | pom-dependency | CVE-2025-27817 — your kafka-clients can be told to read any file on disk. |
| [KAFKA_CLIENTS_CVE_JNDI_LDAP](./KAFKA_CLIENTS_CVE_JNDI_LDAP.md) | ERROR | HIGH | pom-dependency | CVE-2023-25194 / CVE-2025-27818 — the Kafka spiritual successor to Log4Shell. |
| [KAFKA_CLIENTS_CVE_CONFIG_PROVIDER](./KAFKA_CLIENTS_CVE_CONFIG_PROVIDER.md) | ERROR | HIGH | pom-dependency | CVE-2024-31141 — ConfigProvider was happy to read whatever file you named. |
| [KAFKA_CLIENTS_CVE_BUFFER_POOL](./KAFKA_CLIENTS_CVE_BUFFER_POOL.md) | ERROR | HIGH | pom-dependency | CVE-2026-35554 — your producer ships messages to the wrong topic and never tells you. |
| [KAFKA_CLIENTS_CVE_SCRAM_REPLAY](./KAFKA_CLIENTS_CVE_SCRAM_REPLAY.md) | WARNING | HIGH | pom-dependency | CVE-2024-56128 — SCRAM without TLS is a replay attack waiting to happen. |

### Spring Boot / spring-kafka

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [SPRING_BOOT_EOL](./SPRING_BOOT_EOL.md) | ERROR | HIGH | pom-dependency | An EOL Spring Boot means an EOL kafka-clients underneath, and you don't get to pick. |
| [SPRING_KAFKA_EOL](./SPRING_KAFKA_EOL.md) | ERROR | HIGH | pom-dependency | spring-kafka's OSS life is tied to Spring Boot's — and Boot 2.x stopped getting patches three years ago. |
| [SPRING_KAFKA_BOOT_MISMATCH](./SPRING_KAFKA_BOOT_MISMATCH.md) | ERROR | HIGH | pom-dependency + pom-property | Mixing spring-kafka and Spring Boot major versions is a slow-motion classpath collision. |
| [SPRING_BOOT_KAFKA_CLIENT_OVERRIDE](./SPRING_BOOT_KAFKA_CLIENT_OVERRIDE.md) | WARNING | HIGH | pom-dependency + pom-property | Boot picks the kafka-clients spring-kafka was tested against; pinning your own is signing a private contract. |
| [SPRING_KAFKA_DUPLICATE_DECLARATION](./SPRING_KAFKA_DUPLICATE_DECLARATION.md) | WARNING | HIGH | pom-dependency | Declaring spring-kafka and spring-boot-starter-kafka is one of them too many. |

### Quarkus

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [QUARKUS_EOL](./QUARKUS_EOL.md) | ERROR | HIGH | pom-dependency | Quarkus releases monthly; if you're not on an LTS, you're EOL within four weeks. |
| [QUARKUS_KAFKA_EXTENSION_RENAMED](./QUARKUS_KAFKA_EXTENSION_RENAMED.md) | WARNING | HIGH | pom-dependency | quarkus-smallrye-reactive-messaging-kafka was renamed to quarkus-messaging-kafka — use the new name. |
| [QUARKUS_KAFKA_CLIENT_OVERRIDE](./QUARKUS_KAFKA_CLIENT_OVERRIDE.md) | WARNING | HIGH | pom-dependency | The Quarkus BOM pins kafka-clients; overriding it can derail native compilation. |
| [QUARKUS_STREAMS_MISSING_CLIENT](./QUARKUS_STREAMS_MISSING_CLIENT.md) | ERROR | HIGH | pom-dependency | quarkus-kafka-streams requires quarkus-kafka-client — and won't tell you nicely. |
| [SMALLRYE_RM_OVERRIDE](./SMALLRYE_RM_OVERRIDE.md) | WARNING | HIGH | pom-dependency | Quarkus 3 ships SmallRye Reactive Messaging 4 — pinning your own version breaks the build steps. |

### Serdes & schema registries

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [CONFLUENT_AVRO_SERDE_EOL](./CONFLUENT_AVRO_SERDE_EOL.md) | WARNING | HIGH | pom-dependency | kafka-avro-serializer 5.x belongs to Confluent Platform 5 — twelve releases out of date. |
| [CONFLUENT_CLIENT_MIX](./CONFLUENT_CLIENT_MIX.md) | WARNING | MEDIUM | pom-dependency | Mixing Confluent Platform clients with Apache Kafka clients is asking two vendors to share one classpath. |
| [APICURIO_SERDE_V1](./APICURIO_SERDE_V1.md) | WARNING | HIGH | pom-dependency | Apicurio Registry Serdes 1.x is on a different package — and its lights went off in 2022. |

### Build hygiene

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [JAVA_VERSION_TOO_LOW](./JAVA_VERSION_TOO_LOW.md) | ERROR | HIGH | pom-property | Kafka 4 needs JDK 17. Spring 6 needs 17. Quarkus 3 needs 17. JDK 11 is not enough anymore. |
| [KAFKA_TEST_UTILS_RUNTIME_SCOPE](./KAFKA_TEST_UTILS_RUNTIME_SCOPE.md) | WARNING | HIGH | pom-dependency | kafka-streams-test-utils in runtime scope ships an embedded broker into production. |
| [KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE](./KAFKA_CLIENTS_DRIFT_IN_MULTIMODULE.md) | WARNING | MEDIUM | pom-dependency | Two modules in the same reactor on two different kafka-clients is half a fix away from a classloader fight. |

---

## Why version checks matter — the BOM trap

The hardest bug in this entire family is one Maven doesn't tell you about: **you almost never know what version of `kafka-clients` you're running.**

### The chain of resolution

A typical Spring Boot Kafka project's pom doesn't mention `kafka-clients` at all. It declares:

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>
</parent>
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-kafka</artifactId>
    </dependency>
</dependencies>
```

Under the hood:

1. The `spring-boot-starter-parent` parent imports `spring-boot-dependencies`, a BOM that declares hundreds of `<dependencyManagement>` entries — one of which pins `org.apache.kafka:kafka-clients` to a specific version.
2. `spring-boot-starter-kafka` is a one-line starter that transitively depends on `org.springframework.kafka:spring-kafka`.
3. `spring-kafka` transitively depends on `org.apache.kafka:kafka-clients` — *without* declaring a version (the version comes from the BOM).
4. Maven's resolver walks this graph, applies `<dependencyManagement>` from the parent's BOM, and produces a resolved version for every artifact.

So **the actual `kafka-clients` version is determined by Spring Boot's BOM**, three levels removed from anything the project's author wrote. The same is true for Quarkus (`quarkus-bom`) and Confluent (`io.confluent:kafka-schema-registry-parent`).

### Where the trap fires

Three failure modes:

**(1) Stale Boot, stale client.** Spring Boot 2.7 is OSS-EOL, but it still pins `kafka-clients` 3.1.x — also EOL. The project pom looks tidy; the dependency tree carries five-year-old code with multiple unfixed CVEs.

**(2) "I'll override kafka-clients".** A team reads about a CVE and adds an explicit `<dependency>` entry pinning a newer kafka-clients. Maven's nearest-wins beats the BOM. But `spring-kafka` was compiled and tested against the BOM's version; the override creates an unsanctioned combination. See [SPRING_BOOT_KAFKA_CLIENT_OVERRIDE](./SPRING_BOOT_KAFKA_CLIENT_OVERRIDE.md).

**(3) "I'll bump kafka-clients via the property".** The right approach with Spring Boot is to set `<properties><kafka.version>3.9.2</kafka.version></properties>` — Boot's BOM uses that property and propagates the bump to all kafka-* artifacts in the constellation. But this pattern is poorly documented; teams confuse it with the dependency-override path and end up with version skew between `kafka-clients` and `kafka-streams`.

### What the linter has to do

For every rule in this catalog the linter reads **both**:

- `project.getArtifacts()` — the resolved version after BOM and conflict resolution. This is the version that actually runs.
- `project.getDependencies()` and `project.getDependencyManagement()` — the declared / managed versions. This is the version the human thought they were pinning.

When the two disagree, that disagreement is itself a signal — it means BOM resolution is doing something the developer may not realize. The linter surfaces both numbers in violation messages so the user can see the gap.

### The Quarkus variant

Quarkus has the same pattern with `io.quarkus.platform:quarkus-bom`. The Quarkus BOM pins `kafka-clients`, `kafka-streams`, the SmallRye Reactive Messaging Kafka connector, and the GraalVM substitutions for all of them. Overriding any one in isolation tears the carefully-tested combination apart. Native image compilation is particularly unforgiving — see [QUARKUS_KAFKA_CLIENT_OVERRIDE](./QUARKUS_KAFKA_CLIENT_OVERRIDE.md) and [SMALLRYE_RM_OVERRIDE](./SMALLRYE_RM_OVERRIDE.md).

### The Confluent variant

Confluent ships its own `kafka-clients` with a `-ccs` qualifier. Mixing it with the Apache build in the same project is technically two artifacts at the same `groupId:artifactId` — Maven picks one via nearest-wins. See [CONFLUENT_CLIENT_MIX](./CONFLUENT_CLIENT_MIX.md). The behaviour is *usually* benign; the surface area is undocumented.

### The diagnostic developers should learn

Whenever the linter flags a versioning issue, the canonical diagnostic for the developer is:

```sh
mvn dependency:tree -Dincludes=org.apache.kafka:kafka-clients
mvn help:effective-pom | grep -A2 kafka
```

If neither shows what the rule reports, file a bug — the linter and Maven should always agree.

## References

- [Apache Kafka EOL on endoflife.date](https://endoflife.date/apache-kafka)
- [Spring Boot support page](https://spring.io/projects/spring-boot#support)
- [Quarkus EOL on endoflife.date](https://endoflife.date/quarkus)
- [Apache Kafka CVE list](https://kafka.apache.org/cve-list)
- [Spring for Apache Kafka compatibility matrix](https://spring.io/projects/spring-kafka/)
- [Confluent Platform supported versions](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)
- [Conduktor Kafka Config Advisor](https://kafka-options-explorer.conduktor.io/config-advisor/)
- [KIP-679 — durable producer defaults](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679)
- [KIP-770 — Streams cache config rename](https://cwiki.apache.org/confluence/display/KAFKA/KIP-770)
- [KIP-447 — EOS v2](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447)
- [KIP-896 — Remove old client protocol API versions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896)
