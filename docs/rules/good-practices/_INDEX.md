# good-practices rules

This catalog is the **inverse** of the rest of the linter. Most other directories flag concrete anti-patterns: a thing in the code is wrong and should be removed. This one flags the **absence of recommended patterns**: a thing is missing, and even though nothing is breaking right now, you're one production incident away from wishing you had it.

## What "good-practices" means here

A rule belongs in this catalog if:

- The recommended pattern (a config key, a code idiom, an annotation, a dependency) is the well-known production default in the Kafka / Spring / Quarkus communities.
- Its absence is **not a bug** — the application starts, the tests pass, the dev-cluster smoke check is green.
- Its absence shows up as cost, latency, observability gap, or surprise-rebalance under production conditions that staging won't reproduce.

That's why every rule in this catalog is **WARNING, not ERROR**. Absence isn't a bug. You can ship without it. The rule is a nudge — fire it, surface it to the team, let them decide.

## Format

Each rule follows the didactic template used across the other catalogs:

- Severity / Confidence / Detection / Tagline header
- TL;DR
- The setup (what the recommended pattern is, KIPs cited)
- The mechanism (wire / metric / config diff with vs without)
- Operational impact (cost / latency / throughput / toil / observability)
- How to fix (no → yes inline code blocks)
- `Consult a friend?` block (only on EOS / transactions / subtle topics)
- When this might be a false positive
- Detection strategy (what the linter actually scans)
- References (KIPs, official docs, Conduktor advisor, blogs)

## Legend

**Severity** — `WARNING` only (absence isn't a bug).

**Confidence**
- `HIGH` — provable from absence of one constant (e.g. `INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY`, `OBS_SPRING_CLIENT_ID_PREFIX`).
- `MEDIUM` — combo / cross-key inference (e.g. the producer durability / throughput / transactional bundles).
- `CONTEXT` — depends on deployment shape the linter can't see (multi-AZ for `client.rack`, EOS workload intent for `STREAMS_EOS_V2_ENABLED`).

**Detection** — `bytecode`, `config-file`, `pom-dependency`, or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (24)

### Networking / topology

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [FOLLOWER_FETCHING_CLIENT_RACK](./FOLLOWER_FETCHING_CLIENT_RACK.md) | WARNING | CONTEXT | config-file | Cross-AZ fetches are billed by the gigabyte. `client.rack` is the line that turns that bill into local traffic. |

### Producer bundles

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [PRODUCER_DURABILITY_BUNDLE](./PRODUCER_DURABILITY_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file + pom-dependency | Durability is a five-key chord. Miss one note and the whole song goes flat. |
| [PRODUCER_THROUGHPUT_BUNDLE](./PRODUCER_THROUGHPUT_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file | A producer with default batching settings is one-record-at-a-time pretending to be a stream. |
| [PRODUCER_TRANSACTIONAL_BUNDLE](./PRODUCER_TRANSACTIONAL_BUNDLE.md) | WARNING | MEDIUM | bytecode + config-file | A `transactional.id` is a contract with the broker. Sign it properly or don't sign it. 🤝 |

### Consumer bundles

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [CONSUMER_STATIC_MEMBERSHIP](./CONSUMER_STATIC_MEMBERSHIP.md) | WARNING | CONTEXT | bytecode + config-file | Static membership turns N rolling-restart rebalances into zero. |
| [CONSUMER_EOS_READ_COMMITTED](./CONSUMER_EOS_READ_COMMITTED.md) | WARNING | MEDIUM | bytecode + config-file | Reading from a transactional producer with the default isolation is reading drafts. 🤝 |
| [CONSUMER_POLL_TUNING_EXPLICIT](./CONSUMER_POLL_TUNING_EXPLICIT.md) | WARNING | CONTEXT | config-file | `max.poll.records` and `max.poll.interval.ms` should be deliberate, not whatever defaults you forgot were there. |
| [CONSUMER_KIP_848_AWARENESS](./CONSUMER_KIP_848_AWARENESS.md) | WARNING | CONTEXT | config-file | KIP-848 is the future. It's just not your present yet. |

### Kafka Streams bundles

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [STREAMS_REPLICATION_FACTOR_EXPLICIT](./STREAMS_REPLICATION_FACTOR_EXPLICIT.md) | WARNING | MEDIUM | config-file | Replication factor of 3 (or broker default of -1) is the floor for a Streams app worth restarting. |
| [STREAMS_NUM_THREADS_EXPLICIT](./STREAMS_NUM_THREADS_EXPLICIT.md) | WARNING | MEDIUM | config-file | `num.stream.threads` should be a sized choice, not a forgotten default. |
| [STREAMS_EOS_V2_ENABLED](./STREAMS_EOS_V2_ENABLED.md) | WARNING | CONTEXT | config-file | `exactly_once_v2` is one config line and an entire set of operational obligations. 🤝 |
| [STREAMS_APP_ID_STABLE_AND_VERSIONED](./STREAMS_APP_ID_STABLE_AND_VERSIONED.md) | WARNING | MEDIUM | config-file | `application.id` is your stream's identity. Treat it like a deployment artifact, not a runtime variable. |
| [STREAMS_EXCEPTION_HANDLERS_WIRED](./STREAMS_EXCEPTION_HANDLERS_WIRED.md) | WARNING | HIGH | config-file | Every Streams app needs three handlers wired, not one missing. |
| [STREAMS_NAMED_OPERATORS](./STREAMS_NAMED_OPERATORS.md) | WARNING | HIGH | bytecode | Every stateful operator gets a stable name, or your topology is one edit away from data loss. |

### Observability

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [OBS_CLIENT_ID_SET](./OBS_CLIENT_ID_SET.md) | WARNING | MEDIUM | bytecode + config-file | `client.id` is the difference between `producer-1` and `orders-svc-prod-pod-3` in broker logs. |
| [OBS_METRIC_REPORTERS_CONFIGURED](./OBS_METRIC_REPORTERS_CONFIGURED.md) | WARNING | CONTEXT | bytecode + config-file + pom-dependency | Kafka exposes 100+ metrics. Configure one reporter, or you publish to /dev/null. |
| [OBS_INTERCEPTORS_CONFIGURED](./OBS_INTERCEPTORS_CONFIGURED.md) | WARNING | CONTEXT | bytecode + config-file | Interceptors are how the trace context survives the partition boundary. |
| [OBS_SPRING_CLIENT_ID_PREFIX](./OBS_SPRING_CLIENT_ID_PREFIX.md) | WARNING | HIGH | config-file | Spring Boot has a `client-id` knob. Use it before the broker logs see `producer-1` again. |
| [OBS_QUARKUS_CLIENT_ID_PREFIX](./OBS_QUARKUS_CLIENT_ID_PREFIX.md) | WARNING | HIGH | config-file | Quarkus has a `client-id` knob too. SmallRye RM won't write one for you. |

### Versioning hygiene

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [VERSIONS_LET_BOM_MANAGE_CLIENTS](./VERSIONS_LET_BOM_MANAGE_CLIENTS.md) | WARNING | HIGH | pom-dependency | Spring Boot, Quarkus, and Confluent all ship a BOM. Use it, don't fight it. |
| [VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED](./VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED.md) | WARNING | MEDIUM | pom-dependency | Schema Registry serdes must match the broker family, not your random Maven cache. |

### Infrastructure respect

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST](./INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST.md) | WARNING | MEDIUM | config-file | `localhost` in `src/main` is a deploy-time outage waiting for Friday. |
| [INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED](./INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED.md) | WARNING | HIGH | config-file | Dev Services live in `%dev.` and `%test.`. Anywhere else is a sidecar Kafka nobody told ops about. |
| [INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY](./INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY.md) | WARNING | HIGH | bytecode + pom-dependency | `EmbeddedKafkaBroker` is a test fixture. It does not belong in `src/main`. |

## Cross-references to anti-pattern catalogs

A number of practices in this catalog are the inverse of an existing anti-pattern. We don't re-write the mechanism; the good-practice doc cross-links and adds the convention / discipline layer.

| Good-practice | Inverse anti-pattern |
|---|---|
| CONSUMER_STATIC_MEMBERSHIP | [CONSUMER_GROUP_INSTANCE_ID_MISSING](../kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md) |
| CONSUMER_EOS_READ_COMMITTED | [CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN](../kafka-clients/CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md) |
| STREAMS_REPLICATION_FACTOR_EXPLICIT | [STREAMS_REPLICATION_FACTOR_ONE](../kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md) |
| STREAMS_NUM_THREADS_EXPLICIT | [STREAMS_NUM_THREADS_ONE](../kafka-streams/STREAMS_NUM_THREADS_ONE.md) |
| STREAMS_APP_ID_STABLE_AND_VERSIONED | [STREAMS_APP_ID_UNSTABLE](../kafka-streams/STREAMS_APP_ID_UNSTABLE.md) |
| STREAMS_EXCEPTION_HANDLERS_WIRED | [STREAMS_PRODUCTION_HANDLER_MISSING](../kafka-streams/STREAMS_PRODUCTION_HANDLER_MISSING.md), [STREAMS_UNCAUGHT_HANDLER_MISSING](../kafka-streams/STREAMS_UNCAUGHT_HANDLER_MISSING.md), [STREAMS_DEFAULT_DESERIALIZATION_HANDLER](../kafka-streams/STREAMS_DEFAULT_DESERIALIZATION_HANDLER.md) |
| STREAMS_NAMED_OPERATORS | [STREAMS_UNNAMED_MATERIALIZED](../kafka-streams/STREAMS_UNNAMED_MATERIALIZED.md) |
| STREAMS_EOS_V2_ENABLED | [STREAMS_EOS_V1_DEPRECATED](../kafka-streams/STREAMS_EOS_V1_DEPRECATED.md) |
| OBS_CLIENT_ID_SET | [CLIENT_ID_MISSING](../kafka-clients/CLIENT_ID_MISSING.md) |
| OBS_METRIC_REPORTERS_CONFIGURED | [OBS_NO_METRIC_REPORTERS](../observability/OBS_NO_METRIC_REPORTERS.md) |
| OBS_INTERCEPTORS_CONFIGURED | [OBS_NO_PRODUCER_INTERCEPTORS](../observability/OBS_NO_PRODUCER_INTERCEPTORS.md), [OBS_NO_CONSUMER_INTERCEPTORS](../observability/OBS_NO_CONSUMER_INTERCEPTORS.md) |
| INFRA_BOOTSTRAP_SERVERS_NOT_LOCALHOST | [SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST](../spring-kafka/SPRING_BOOT_BOOTSTRAP_SERVERS_LOCALHOST.md), [QK_LOCALHOST_IN_PROD](../quarkus-kafka/QK_LOCALHOST_IN_PROD.md) |
| INFRA_QUARKUS_DEVSERVICES_PROFILE_SCOPED | [QK_DEVSERVICES_IN_PROD](../quarkus-kafka/QK_DEVSERVICES_IN_PROD.md) |
| INFRA_SPRING_EMBEDDED_KAFKA_TEST_ONLY | [KAFKA_TEST_UTILS_RUNTIME_SCOPE](../versions/KAFKA_TEST_UTILS_RUNTIME_SCOPE.md) |
| PRODUCER_DURABILITY_BUNDLE | [PRODUCER_ACKS_ZERO](../kafka-clients/PRODUCER_ACKS_ZERO.md), [PRODUCER_ACKS_ONE](../kafka-clients/PRODUCER_ACKS_ONE.md), [PRODUCER_IDEMPOTENCE_DISABLED](../kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md), [PRODUCER_RETRIES_ZERO](../kafka-clients/PRODUCER_RETRIES_ZERO.md), [KAFKA_CLIENTS_PRE_KIP679](../versions/KAFKA_CLIENTS_PRE_KIP679.md) |
| PRODUCER_THROUGHPUT_BUNDLE | [PRODUCER_LINGER_ZERO_NO_BATCH](../kafka-clients/PRODUCER_LINGER_ZERO_NO_BATCH.md), [PRODUCER_COMPRESSION_NONE_EXPLICIT](../kafka-clients/PRODUCER_COMPRESSION_NONE_EXPLICIT.md) |
| PRODUCER_TRANSACTIONAL_BUNDLE | [PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE](../kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md), [SPRING_TRANSACTIONAL_WITHOUT_KTM](../spring-kafka/SPRING_TRANSACTIONAL_WITHOUT_KTM.md) |
| CONSUMER_POLL_TUNING_EXPLICIT | [CONSUMER_MAX_POLL_INTERVAL_TOO_LOW](../kafka-clients/CONSUMER_MAX_POLL_INTERVAL_TOO_LOW.md), [CONSUMER_MAX_POLL_RECORDS_TOO_HIGH](../kafka-clients/CONSUMER_MAX_POLL_RECORDS_TOO_HIGH.md) |
| VERSIONS_LET_BOM_MANAGE_CLIENTS | [SPRING_BOOT_KAFKA_CLIENT_OVERRIDE](../versions/SPRING_BOOT_KAFKA_CLIENT_OVERRIDE.md), [QUARKUS_KAFKA_CLIENT_OVERRIDE](../versions/QUARKUS_KAFKA_CLIENT_OVERRIDE.md), [SMALLRYE_RM_OVERRIDE](../versions/SMALLRYE_RM_OVERRIDE.md), [SPRING_KAFKA_DUPLICATE_DECLARATION](../versions/SPRING_KAFKA_DUPLICATE_DECLARATION.md) |
| VERSIONS_SCHEMA_REGISTRY_SERDE_PINNED | [CONFLUENT_AVRO_SERDE_EOL](../versions/CONFLUENT_AVRO_SERDE_EOL.md), [APICURIO_SERDE_V1](../versions/APICURIO_SERDE_V1.md) |

## Notes for the implementer

- All rules default to WARNING, never ERROR. The user explicitly asked for this: absence of a good practice is a nudge, not a bug.
- The CONTEXT-tier rules (`FOLLOWER_FETCHING_CLIENT_RACK`, `CONSUMER_STATIC_MEMBERSHIP`, `CONSUMER_POLL_TUNING_EXPLICIT`, `STREAMS_EOS_V2_ENABLED`, `OBS_METRIC_REPORTERS_CONFIGURED`, `OBS_INTERCEPTORS_CONFIGURED`, `CONSUMER_KIP_848_AWARENESS`) should default to print-only and require an opt-in flag to fail the build.
- `STREAMS_EOS_V2_ENABLED`, `CONSUMER_EOS_READ_COMMITTED`, and `PRODUCER_TRANSACTIONAL_BUNDLE` ship with the `Consult a friend?` block the user mandated — "familiarity is not understanding" framed around the KIP-447 / KIP-129 reading list.
- The bundles (`PRODUCER_DURABILITY_BUNDLE`, etc.) need cross-key inference: the linter must read multiple properties from the same source and reason about the combination. Reuse the same `RuleContext` "Properties group" abstraction the anti-pattern bundle rules already need.
- The Conduktor Config Advisor (<https://kafka-options-explorer.conduktor.io/config-advisor/>) covers the producer durability / consumer durability / consumer throughput / consumer low-latency profiles in great detail. It does NOT cover `client.rack` or follower fetching — that part of this catalog is original ground.
