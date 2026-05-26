# quarkus rules

Build-time anti-pattern checks for Quarkus SmallRye Reactive Messaging Kafka connector configuration — the `mp.messaging.incoming.<channel>.*` and `mp.messaging.outgoing.<channel>.*` property surface. This category is **distinct from** [`quarkus-kafka`](../quarkus-kafka/_INDEX.md): the `quarkus-kafka` category covers the broader Quarkus + Kafka integration (`@Incoming` / `@Outgoing` Java code, `Emitter` injection, dev-services configuration, observability), while this `quarkus` category targets a specific cluster of channel-attribute footguns that live purely in `application.properties` / `application.yml`.

## Status

This category currently has **6 rules registered in the runtime** (`src/main/java/io/conductor/kafkalinter/RuleId.java`) but **no per-rule standalone doc files yet**. Each rule's full didactic prose — tagline, mechanism, operational impact, and how-to-fix — lives **inline** in its `RuleId.message(...)` block and is carried verbatim into the violation message that the linter prints. An operator hitting one of these rules in the Maven log will see the full explanation without needing to open a doc file.

The standalone doc backlog is tracked under the doc-tree-vs-registry drift section of [`../_CATALOG.md`](../_CATALOG.md#known-drift-doc-tree-vs-registry). Until that backlog clears, `RuleId.java` is the source of truth for rule prose in this category.

## Catalog (6)

All rules are prefixed `QK_` for "Quarkus channel". They split into incoming-channel and outgoing-channel concerns.

### Incoming channels

- **`QK_INCOMING_BATCH_TRUE`** — `mp.messaging.incoming.<channel>.batch=true` changes the consumer's per-record dispatch to a batched dispatch where the @Incoming method receives `KafkaRecordBatch<K, V>` instead of one record at a time. Often set during throughput tuning without realizing it requires a different method signature and different commit semantics; throws at startup if the signature is wrong.
- **`QK_INCOMING_PAUSE_IF_NO_REQUESTS_FALSE`** — disables SmallRye's reactive back-pressure pause. With `false`, the consumer keeps polling even when the downstream subscriber has not requested more — records pile up in the channel's internal queue with no flow-control signal, eventually leading to memory pressure or rebalances on `max.poll.interval.ms`.
- **`QK_INCOMING_RETRY_TRUE`** — enables SmallRye's per-record retry. The retry is in-process and blocks the channel; if the failure cause is persistent (poison message), the channel halts forever. The correct posture is usually `failure-strategy=dead-letter-queue` plus an external retry strategy, not in-channel retry.

### Outgoing channels

- **`QK_OUTGOING_MERGE_TRUE`** — `mp.messaging.outgoing.<channel>.merge=true` allows multiple producers to write to the same outgoing channel. Convenient for fan-in but produces a shared underlying `KafkaProducer` whose `client.id` no longer corresponds to a single subsystem; observability gets noisier and back-pressure semantics get muddier.
- **`QK_OUTGOING_KEY_LITERAL`** — `mp.messaging.outgoing.<channel>.key=<literal-string>` pins every produced record to a single literal key, which collapses all writes into one partition. Almost always a misuse of the property (the operator meant to set a key extractor expression, or forgot to provide a `Message<K, V>` with the key payload from code).
- **`QK_LAZY_CLIENT_TRUE`** — `quarkus.kafka.devservices.enabled=false` plus `lazy-client=true` delays connector initialization until the first message. Startup looks healthy but the actual Kafka misconfiguration (wrong bootstrap, bad credentials, missing topic) surfaces only at first message — frequently long after deployment readiness probes have already passed.

## How to find a rule's prose

```bash
grep -A12 'RuleId.QK_<RULE_NAME>' src/main/java/io/conductor/kafkalinter/RuleId.java
```

The `.message(...)` block carries the full didactic body.
