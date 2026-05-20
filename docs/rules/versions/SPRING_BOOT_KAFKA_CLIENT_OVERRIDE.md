# SPRING_BOOT_KAFKA_CLIENT_OVERRIDE

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency + pom-property
**Tagline**: Boot picks the kafka-clients spring-kafka was tested against; pinning your own is signing a private contract.

## TL;DR

Spring Boot's BOM pins `org.apache.kafka:kafka-clients` to the exact patch version `spring-kafka` was compiled against. Pinning your own `kafka-clients` version in the project's pom — or its `<dependencyManagement>` — overrides Boot's BOM. The combination is not tested by the Spring team; subtle behavior changes between Kafka client minors can break the listener container, the transactional manager, or the new-consumer-group protocol negotiation.

## The setup

A team reads about a `kafka-clients` CVE or a new feature (KRaft, KIP-848 new consumer protocol, a buffer-pool fix) and adds:

```xml
<properties>
    <kafka.version>3.9.2</kafka.version>
</properties>
```

— a property that Spring Boot's BOM uses (good intent: align with Boot's mechanism). Or worse:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

— a direct override that wins via Maven nearest-wins. Now `spring-kafka` 3.3.x (which was compiled against `kafka-clients` 3.8.1) is running with `kafka-clients` 3.9.2. Sometimes fine; sometimes not.

## What's actually happening

Spring Boot's `spring-boot-dependencies` BOM contains a `<kafka.version>` property that pins `org.apache.kafka:kafka-clients` and a small constellation of related artifacts (`kafka-streams`, `kafka-streams-test-utils`, `kafka-clients` test-jar, `connect-api`, `connect-json`). `spring-kafka` is built and tested against that specific version.

Examples of breaking diffs across patch lines:

- **3.7 → 3.8**: new consumer protocol (KIP-848) entered "preview". Test paths in `spring-kafka` that rely on the old protocol's coordinator behavior have shifted timing.
- **3.8 → 3.9**: changes to `ProducerConfig` validation (the OAUTHBEARER allow-list system property — see `KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER`). Spring-kafka's `KafkaTransactionManager` initialization path can throw on startup if OAUTHBEARER is in use and the allow-list isn't set.
- **3.9 → 4.0**: ZooKeeper-related APIs deleted. `spring-kafka` 3.3.x calls `AdminClient.describeCluster()` against a code path that 4.0 collapsed.

The Spring team explicitly recommend the override pattern only via `<properties>`:

```xml
<properties>
    <kafka.version>3.9.2</kafka.version>
</properties>
```

This **does** update the entire constellation of kafka-* artifacts together (so `kafka-streams` stays aligned with `kafka-clients`). Bumping just `kafka-clients` with an explicit `<dependency>` block does **not** update `kafka-streams` — and now the two artifacts disagree.

## Why this is subtle

- The pom looks responsible — bumping for a CVE looks like good hygiene.
- The build succeeds, runs tests, and starts up. The breakage shows up in production-only code paths: real broker, real auth, real transactions.
- Boot's documentation buries the warning. The pattern most people learn is "set `kafka.version` and you're fine" — which is true for property-based overrides but breaks if you write `<dependency>...<version>`.

## Operational impact

- **Startup failures** from missing classes / removed methods on the new client version.
- **Subtle behavior changes** in the listener container — e.g. rebalance protocol mismatches, transaction commit-offset semantics.
- **Streams / clients version skew** — `kafka-streams` 3.7 with `kafka-clients` 3.9 has been a known cause of "store not found" errors during rebalance.
- **Stuck on the override** — once you pin, you have to keep the pin in sync with every Boot upgrade, which most teams forget.

## How to fix

```xml
<!-- BEST: don't override; bump Spring Boot itself -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>   <!-- already pins kafka-clients 3.9.x -->
</parent>

<!-- ACCEPTABLE: property override (updates the whole kafka constellation) -->
<properties>
    <kafka.version>3.9.2</kafka.version>
</properties>

<!-- BAD: explicit dependency override -->
<dependencies>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.9.2</version>   <!-- breaks alignment with kafka-streams etc -->
    </dependency>
</dependencies>
```

## When this might be a false positive

- A property-based `<kafka.version>` override moving forward by one minor at most, where you've verified `kafka-streams` and `connect-api` came along (Boot's BOM uses the property consistently).
- A patch-level bump within the same minor (3.8.1 → 3.8.2). Boot is unlikely to break, but it's still an unsanctioned combination.

## Detection strategy

1. Detect Spring Boot in `getDependencyManagement()` / parent.
2. Find the Boot-pinned `kafka.version` (read it from the resolved BOM or compute it from the Boot release).
3. Walk `project.getDependencies()` (raw, not resolved):
   - If `org.apache.kafka:kafka-clients` has an explicit `<version>` → flag as `<dependency>`-override. ERROR-leaning WARNING.
   - If `project.getProperties().get("kafka.version")` is set and **differs from** Boot's pinned value → flag as property override, WARNING.
4. If `project.getArtifacts()` shows resolved `kafka-clients` at a version other than Boot's pinned one, but no explicit override appears in this pom, it came from a transitive dependency — different rule, surface it as `SPRING_BOOT_KAFKA_CLIENT_DRIFT`.

## References

- [Spring Kafka — Override Kafka Client version](https://docs.spring.io/spring-kafka/reference/appendix.html#override-boot-dependencies)
- [Spring Boot dependency management — managed versions](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#dependency-versions.coordinates)
- [Spring Kafka release notes — kafka-clients alignment](https://spring.io/blog/category/spring-kafka)
