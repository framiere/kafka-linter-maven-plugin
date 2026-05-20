# STREAMS_EOS_V2_ENABLED

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: `exactly_once_v2` is one config line and an entire set of operational obligations.

## TL;DR

The linter flags Streams apps that handle financial/order/state data
with `processing.guarantee=at_least_once` (the default). The good
practice is `exactly_once_v2`. But this is the rule where familiarity
is not understanding — the linter emits the warning with a strong
CONSULT note attached. Don't enable EOS v2 in production without
reading KIP-447 and KIP-129 and understanding what your topology will
look like under it.

## The setup

KIP-129 (Kafka 0.11) introduced exactly_once for Streams using
per-task producers. KIP-447 (Kafka 2.5) introduced `exactly_once_v2`
using a single producer per Streams instance — vastly more efficient,
same guarantees. KIP-732 (Kafka 3.0) deprecated `exactly_once` (v1) in
favor of `exactly_once_v2`.

In 2026, `exactly_once_v2` is the only EOS option you should be
choosing. The default remains `at_least_once` because EOS has real
costs: longer commit cycles, transactional broker state, more careful
operational handling.

## What's actually happening (the mechanism)

`at_least_once` (default):
- Streams uses normal idempotent producers.
- Offset commits happen periodically (`commit.interval.ms`, default
  30s; for EOS v2 it's 100ms).
- On crash, records processed but not committed are re-read and
  re-processed. Downstream effects (writes to other topics, state
  store updates) may be duplicated.

`exactly_once_v2`:
- All output to Kafka topics (downstream sinks, repartition,
  changelog) is bundled into a transaction.
- The offset commit for input partitions is included in the same
  transaction via `producer.sendOffsetsToTransaction(...)`.
- The transaction is committed every `commit.interval.ms` (defaults to
  100ms under EOS, configurable).
- Downstream consumers with `isolation.level=read_committed` see
  records only after the transaction commits.

Failure modes:
- Open transactions hold the LSO of output topics. A crashed instance
  blocks downstream consumers until `transaction.timeout.ms` expires
  (default 60s, configurable). Tune this for your tolerance.
- Per-task EOS v1 used one transactional producer per task → did not
  scale beyond ~1000 tasks. v2 uses one producer per Streams instance →
  scales much better but cross-task atomicity is per-instance, not
  per-task.

JMX: `kafka.streams:type=stream-thread-metrics:commit-rate` increases
~300× under EOS v2 (every 100ms vs every 30s). Broker
`transaction-coordinator-metrics` becomes important.

## Operational impact

- **Cost.** A handful of extra commit-marker writes per second per
  Streams instance. Negligible on a sized broker.
- **Latency.** End-to-end latency increases by ~100-200ms (commit
  interval + downstream read_committed delivery). Acceptable for most
  pipelines; not for sub-50ms real-time.
- **Throughput.** Slight reduction (~5-15%). Far less under v2 than v1.
- **Operational toil.** Increased. Open transactions, stuck LSO, zombie
  producer fencing — all real failure modes that need monitoring.

## Consult a friend?

**Yes. This is the rule with the strongest CONSULT obligation.**

Familiarity is not understanding. The common misconceptions:

- **"EOS means I can't lose data."** EOS prevents *processing*
  duplicates within Streams. It does NOT prevent loss when the broker
  topic configuration is wrong (`min.insync.replicas=1`, `acks` not
  forced to `all`, RF=1 on changelogs). The same broker hygiene that
  makes `at_least_once` durable is required for EOS to be durable.
- **"EOS means downstream sees exactly one delivery."** Only if
  downstream is `read_committed`. Cross-link
  [CONSUMER_EOS_READ_COMMITTED](./CONSUMER_EOS_READ_COMMITTED.md).
- **"EOS works across external systems."** No. EOS is Kafka-to-Kafka.
  Any side effect to an external DB, HTTP call, or non-Kafka system is
  outside the transaction and can duplicate on retry. Idempotent
  external writes are still your problem.
- **"EOS handles all failure modes."** A crashed Streams instance holds
  its transaction open for `transaction.timeout.ms`. During that window,
  downstream `read_committed` consumers stall. Watch for stuck LSO.
- **"`exactly_once` is the same as `exactly_once_v2`."** No.
  `exactly_once` (v1) is deprecated in 3.0 and removed in 4.0. Cross-link
  [STREAMS_EOS_V1_DEPRECATED](../kafka-streams/STREAMS_EOS_V1_DEPRECATED.md).

If your team has not shipped EOS Streams before, schedule a paired
reading session of KIP-447 + KIP-129 + the Confluent EOS in Kafka
Streams blog post before enabling.

## How to fix (no → yes)

```properties
# no — at_least_once on a financial pipeline
processing.guarantee=at_least_once

# yes — EOS v2
processing.guarantee=exactly_once_v2
# default commit.interval.ms is 100ms under EOS v2 — usually correct
# raise transaction.timeout.ms if your tasks have long external calls
producer.transaction.timeout.ms=120000
```

```java
// yes
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
          StreamsConfig.EXACTLY_ONCE_V2);
```

Downstream consumers must use `isolation.level=read_committed` or they
see uncommitted writes (cross-link CONSUMER_EOS_READ_COMMITTED).

Broker side, which the linter can't verify:
- `min.insync.replicas=2` on changelog topics
- `transaction.max.timeout.ms` >= producer `transaction.timeout.ms`
- Sufficient `__transaction_state` partitions for the number of
  transactional producers in the cluster.

## When this might be a false positive

- The team has explicitly chosen `at_least_once` for cost / latency
  reasons after measurement. Document it in a comment near the config
  and suppress.
- The Streams app does only stateless transformations with idempotent
  downstream sinks (already-keyed `KStream.to()` with idempotent
  consumers). EOS adds cost without changing the semantics.
- Streams app reading from non-transactional input, writing to a
  non-Kafka sink — EOS is irrelevant; Streams' transaction would just
  cover the offset commit.

## Detection strategy

- **Config files:** flag any Streams app where `processing.guarantee`
  is unset or `at_least_once` AND the project has a hint of financial /
  ordering / dedup intent (heuristics: topic names containing
  `orders|payments|transactions|ledger`, presence of `Avro`/`Protobuf`
  schemas with `correlationId`/`txId` fields). This is necessarily
  fuzzy.
- **Anti-pattern cross-links:**
  [STREAMS_EOS_V1_DEPRECATED](../kafka-streams/STREAMS_EOS_V1_DEPRECATED.md)
  for v1 → v2 migration.
- **Confidence: CONTEXT.** This rule depends on workload intent. Default
  to INFO-tier print-only with the consult message attached.

## References

- KIP-447 — Producer scalability for exactly once semantics:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
- KIP-129 — Streams exactly-once:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics
- KIP-732 — Deprecate eos-alpha and replace with eos-v2:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-732
- Confluent — Enabling Exactly-Once in Kafka Streams:
  https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/
- Apache Kafka Streams configs — `processing.guarantee`:
  https://kafka.apache.org/documentation/#streamsconfigs_processing.guarantee
