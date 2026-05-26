# kafka-connect rules

Build-time anti-pattern checks for Apache Kafka Connect connector configurations — sink and source connectors, transformations, predicates, error-handling, DLQ semantics, and Debezium-specific footguns. Rules scan connector `.properties` and `.json` payloads, identified primarily by the presence of `connector.class`. Targets Apache Kafka Connect 3.x and 4.x plus the Confluent / Aiven / Debezium connector ecosystem.

## Status

This category currently has **92 rules registered in the runtime** (`src/main/java/io/conductor/kafkalinter/RuleId.java`) but **no per-rule standalone doc files yet**. Each rule's full didactic prose — tagline, mechanism, operational impact, and how-to-fix — lives **inline** in its `RuleId.message(...)` block and is carried verbatim into the violation message that the linter prints. An operator hitting one of these rules in the Maven log will see the full explanation without needing to open a doc file.

The standalone doc backlog (one `*.md` per rule, mirroring the structure used in `kafka-clients/`, `kafka-streams/`, `spring-kafka/`, etc.) is tracked under the doc-tree-vs-registry drift section of [`../_CATALOG.md`](../_CATALOG.md#known-drift-doc-tree-vs-registry). Until that backlog clears, `RuleId.java` is the source of truth for rule prose in this category.

## Rule families

The 92 rules cluster into the following families. Each family targets a specific connector class or configuration surface.

### Connector framework basics

`CONNECT_NAME_MISSING`, `CONNECT_TASKS_MAX_ABSENT`, `CONNECT_TASKS_MAX_LESS_THAN_ONE`, `CONNECT_REST_ADVERTISED_HOST_NAME_LOCALHOST`, `CONNECT_WORKER_INTERNAL_TOPIC_REPLICATION_FACTOR_LOW` — connector identity, parallelism, and worker-cluster baseline.

### Error handling & DLQ contract

`CONNECT_ERRORS_TOLERANCE_ABSENT`, `CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ`, `CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG`, `CONNECT_ERRORS_RETRY_TIMEOUT_ABSENT`, `CONNECT_DLQ_TOPIC_EQUALS_INPUT_TOPIC`, `CONNECT_DLQ_REPLICATION_FACTOR_LOW`, `CONNECT_DLQ_CONTEXT_HEADERS_DISABLED` — the KIP-298 retry-with-tolerance contract and DLQ wiring. These are the rules most likely to fire on a real-world connector config.

### Converters & schema

`CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE`, `CONNECT_SCHEMA_REGISTRY_CONVERTER_MISSING_URL`, `CONNECT_JSON_CONVERTER_SCHEMAS_ENABLE_UNSET` — converter-side correctness; auto-register-schemas in production is the most common footgun.

### Source vs sink offset & commit semantics

`CONNECT_SINK_AUTO_COMMIT_TRUE`, `CONNECT_SINK_CONSUMER_AUTO_OFFSET_RESET_LATEST`, `CONNECT_SOURCE_PRODUCER_ACKS_NOT_ALL`, `CONNECT_PRODUCER_ENABLE_IDEMPOTENCE_FALSE`, `CONNECT_CONSUMER_OVERRIDE_GROUP_ID` — connector-runtime overrides that silently break Connect's offset-management contract.

### Topic-routing & SMT correctness

`CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET`, `CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_NEITHER_SET`, `CONNECT_TRANSFORM_*`, `CONNECT_PREDICATE_*`, `CONNECT_TOPIC_CREATION_GROUP_DEFINED_BUT_NOT_LISTED` — Connect's `transforms` / `predicates` chain wiring is famously easy to misconfigure; these catch the canonical mistakes.

### Storage sinks (S3 / GCS / HDFS)

`CONNECT_GCS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE`, `CONNECT_HDFS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE`, `CONNECT_STORAGE_SINK_*` — partitioner / rotate-interval / path-format combinations that produce either no files or fragmented files.

### JDBC sink & source

`CONNECT_JDBC_SINK_*`, `CONNECT_JDBC_SOURCE_*` — pk-mode / auto-evolve / table-name-format / poll-interval / column-mode pitfalls; the JDBC connectors have the most options-per-line of any connector family.

### Debezium (CDC)

`CONNECT_DEBEZIUM_*` (40+ rules across MySQL, PostgreSQL, Oracle, SQL Server) — snapshot mode, schema history, publication / slot sharing, tasks.max constraints, decimal & time precision handling, key/value converter mismatches, tombstones-on-delete. Debezium's surface is the largest in the connector ecosystem; the rules here encode the operational gotchas accumulated across years of CDC pipelines.

### Demo / misuse guards

`CONNECT_FILE_STREAM_DEMO_CONNECTOR` — flags the `FileStream*Connector` shipped with Apache Kafka for tutorials but unsuitable for production.

### Config provider hygiene

`CONNECT_CONFIG_PROVIDER_REFERENCE_UNDEFINED` — `${provider:path:key}` references that point at a `config.providers` entry that was never declared.

## How to find a rule's prose

```bash
grep -A12 'RuleId.<CONNECT_RULE_NAME>' src/main/java/io/conductor/kafkalinter/RuleId.java
```

The `.message(...)` block carries the full didactic body. The `.tagline(...)`, `.mechanism(...)`, `.impact(...)`, and `.whyMatters(...)` blocks decompose that prose into the four canonical didactic blocks if a rule was authored with them split out.
