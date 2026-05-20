# KAFKA_CLIENT_BROKER_LAG

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: pom-property
**Tagline**: Your client is forward-compatible with a broker eighteen months in the future — but missing four bug fixes.

## TL;DR

Kafka clients are forward-compatible with newer brokers since 0.10, so `kafka-clients` 3.2 talking to a 4.0 broker "works". But every quarter you stay behind costs you fixed bugs, new producer/consumer behaviors gated on KIP version negotiation, and improved client-side metrics. The linter flags the gap when the project declares both the client version and a broker version (in a `<kafka.broker.version>` property, in compose files, or in a documented connection string).

## The setup

Many internal projects keep a `kafka.version` Maven property — sometimes used in tests against a Testcontainers Confluent image, sometimes as a doc breadcrumb. Sometimes the production cluster is referenced in a `README` or in a `docker-compose.yml` adjacent to the pom. The linter can compare any of these signals against the declared/resolved `kafka-clients` version.

## What's actually happening

Compatibility is asymmetric by design:

- Client `< broker` is **supported** (forward compatibility). The client negotiates an API version the broker also supports.
- Client `> broker` is **partially supported** — some newer API requests fall back; some KIPs disable themselves on older brokers (idempotence requires broker 0.11+; transactional producers require 0.11+; cooperative rebalance requires 2.4+; etc.).

The risk surface of a stale client:

- **Missed bug fixes** — every minor release ships consumer-group, rebalance, and metric fixes. Staying two minors behind = two minors of bugs.
- **Missed defaults** — KIP-679 (durable producer defaults, Kafka 3.0+), KIP-770 (state-store cache key rename, Streams 3.4+), KIP-844 (committed offsets in transaction, Streams 3.5+).
- **Wire-format efficiency** — the broker advertises newer protocol versions; an old client cannot use them, missing batch-format improvements and idle-connection optimizations.
- **Observability gap** — newer client versions expose more JMX metrics. You can't dashboard what doesn't exist.

## Why this is subtle

- Things appear to work. The signal is absence, not failure.
- "We're forward compatible, so we'll upgrade when we have time" becomes a permanent state.
- The broker version is often not visible from the application code — it has to be fetched from `AdminClient.describeCluster()` at runtime, which most teams don't surface.

## Operational impact

- **Slow degradation** — unfixed consumer-group bugs accumulate, producers ride older idempotence semantics, metric blind spots widen.
- **Upgrade rollouts get harder over time** — a 4-minor jump touches more behavior than four 1-minor jumps.
- **Confluent Platform 8.0 protocol baseline** (Kafka 2.1, per KIP-896) eventually rejects clients below the baseline; staying way behind eventually becomes a hard incompatibility.

## How to fix

Keep the client within one or two minor versions of the broker:

```xml
<!-- BAD: broker is 3.9, client is 3.1 — 8 minors behind -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.1.0</version>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

Document the broker version explicitly in a Maven property so the linter (and humans) can compare:

```xml
<properties>
    <kafka.broker.version>3.9.2</kafka.broker.version>
    <kafka.client.version>3.9.2</kafka.client.version>
</properties>
```

## When this might be a false positive

- A library project that doesn't know its deployment's broker version — skip the rule or treat as informational.
- A test-only module pinned to an older client to verify backwards compatibility against an older broker fixture.
- An organizational policy to lag the broker by one quarter (some banks do this). Document and suppress.

## Detection strategy

- Look for a `kafka.broker.version` (or `kafka.broker`, `confluent.platform.version`) property in `project.getProperties()`.
- Compare against the resolved `kafka-clients` version.
- Flag at **`>= 3 minor versions`** behind: WARNING.
- Flag at **`>= 5 minor versions`** behind: WARNING with stronger language.
- If no broker version is declared anywhere, skip — the linter shouldn't invent a target.

## References

- [Apache Kafka compatibility matrix](https://kafka.apache.org/protocol)
- [KIP-35 — Retrieve protocol version](https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version)
- [KIP-896 — Remove old client protocol API versions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896)
