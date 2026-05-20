# KAFKA_CLIENTS_EOL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: A Kafka client older than the bugs you'd file against it.

## TL;DR

The linter flags `org.apache.kafka:kafka-clients` resolved at a release line that the Apache Kafka project no longer maintains. As of May 2026 the only community-supported lines are **4.2, 4.1, 4.0 and 3.9**. Everything below 3.9 is EOL.

## The setup

A team picked a `kafka-clients` version when they first wired the project up — perhaps three years ago. The pom never changed because "it works". They are not necessarily on an old broker; in fact most managed Kafka services (MSK, Confluent Cloud, Aiven) have been silently rolling forward. The client is the part that never gets touched until something breaks.

## What's actually happening

Apache Kafka follows a roughly four-month minor cadence and keeps "the latest few" minor lines on patch maintenance. The EOL boundary moves. A client version that was supported in 2023 is no longer in scope for security backports or behavior corrections in 2026:

| Line | Released | Community EOL | Status (May 2026) |
|------|----------|---------------|-------------------|
| 0.x – 2.7 | 2015–2020 | ≤ 2021 | EOL — security fixes not backported |
| 2.8 | Apr 2021 | Sep 2022 | EOL — last AK 2.x line |
| 3.0 – 3.5 | 2021–2023 | ≤ Oct 2023 | EOL |
| 3.6 – 3.8 | 2023–2024 | ≤ Nov 2024 | EOL |
| 3.9 | Nov 2024 | Feb 2027 | Supported |
| 4.0 – 4.2 | 2025–2026 | 2027–2028 | Supported |

Beyond "no security patches", every line below 3.0 also predates the KIP-679 producer hardening (durable defaults, idempotence-on by default — see `KAFKA_CLIENTS_PRE_KIP679`).

## Why this is subtle

Three things conspire to keep teams on a dead version:

1. **Maven's nearest-wins resolution** means a transitive `kafka-clients` from spring-kafka or quarkus-kafka-client is what actually ships, not the version the developer "remembers picking". `mvn dependency:tree` reveals it; the pom alone often does not.
2. **Spring Boot's curated BOM** pins `kafka-clients` to whatever Boot was built against. If you're on Spring Boot 2.7 (itself OSS-EOL) the BOM pins kafka-clients 3.1.x — which is also EOL.
3. **The client is forward compatible with newer brokers.** Things keep working. The signal that should make you upgrade — runtime breakage — never fires.

## Operational impact

- **No security patches.** New CVEs against `kafka-clients` (see `KAFKA_CLIENTS_CVE_*` rules) are only fixed on the supported lines. You inherit the issue.
- **Known bugs stay un-fixed.** Producer pool corruption (`KAFKA_CLIENTS_CVE_2026_35554`), consumer rebalance edge cases, MM2 leaks — all need a client bump.
- **Confluent Platform 8.0 / KIP-896** sets the new client baseline at the Kafka 2.1.0 protocol level. Pre-2.1 clients will be hard-rejected by future brokers.
- **Library compatibility shrinks.** Newer spring-kafka / quarkus-kafka / streams releases stop testing against your client version.

## How to fix

```xml
<!-- BAD: pinning a 2.x line, or letting the BOM pick a 3.0–3.8 line -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>2.8.2</version>
</dependency>

<!-- GOOD: latest patch of an active line -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>

<!-- BETTER: let your framework BOM manage it and bump the BOM
     (Spring Boot 3.5.x / Quarkus 3.27.x LTS will pull a supported line) -->
```

Severity matrix the linter applies:

| Detected version | Severity |
|------------------|----------|
| < 3.0 | ERROR |
| 3.0–3.8 | WARNING |
| 3.9, 4.x | OK |

## When this might be a false positive

- You are deliberately pinned for compatibility with a legacy broker that hasn't been upgraded (rare in 2026; managed clusters are on 3.7+).
- You maintain an internal fork that backports security patches — declare that via a suppression annotation.
- Test-only modules using an embedded broker pinned to an older version intentionally.

## Detection strategy

- Walk `project.getArtifacts()` looking for `groupId=org.apache.kafka, artifactId=kafka-clients`. This gives the **resolved** version after BOM and conflict resolution.
- Also walk `project.getDependencies()` to spot explicit overrides (treat an explicit declaration as more confident than a transitive resolution — surface both in the violation message).
- Parse the version with a tolerant `ComparableVersion` (the Maven shared util) so `3.7.1`, `3.7.1-redhat-00001`, `3.9.0-ce`, `3.7.1.Final` all sort sensibly.
- Strip `-SNAPSHOT`, `-RC*`, `-Mn` for the EOL comparison but mention the qualifier in the message — e.g. `3.0.0-SNAPSHOT` counts as 3.0.0 for EOL purposes but is independently suspect.

## References

- [Apache Kafka EOL table on endoflife.date](https://endoflife.date/apache-kafka)
- [Apache Kafka downloads / release notes](https://kafka.apache.org/downloads)
- [KIP-896 — removal of legacy client API support in CP 8.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896)
