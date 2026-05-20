# PRODUCER_TRANSACTIONAL_BUNDLE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: A `transactional.id` is a contract with the broker. Sign it properly or don't sign it.

## TL;DR

When a producer sets `transactional.id`, the linter checks the rest of
the contract: `enable.idempotence=true` (now the default once
`transactional.id` is set, but verify the version), and a
`transaction.timeout.ms` that fits inside the broker's
`transaction.max.timeout.ms` (default 15 minutes). Also flag a
`transactional.id` that isn't stable across restarts.

## The setup

KIP-98 (Kafka 0.11) added transactions. KIP-447 (Kafka 2.5) added
`exactly_once_v2` and tied many of the constraints to defaults.

A correct transactional producer has:

- `transactional.id` — stable, unique per producer instance. Used by the
  transaction coordinator to fence zombie producers (an instance that
  crashed mid-transaction; the new instance with the same id increments
  the epoch and aborts the zombie's open txn).
- `enable.idempotence=true` — required. Without it, the producer can't
  even initialize: setting `transactional.id` implies idempotence since
  3.0, but on older clients you must set it yourself.
- `transaction.timeout.ms` — client-side cap on a transaction's
  duration. Must be `<= broker.transaction.max.timeout.ms` (default
  900000 ms / 15 min). Default 60s is fine for most pipelines.
- `acks=all` (forced by idempotence).
- `max.in.flight.requests.per.connection<=5` (forced by idempotence).
- Consumer side: `isolation.level=read_committed` (see the related
  CONSUMER_EOS_READ_COMMITTED practice).

## What's actually happening (the mechanism)

The transactional producer wire flow:

1. `InitProducerIdRequest(transactional.id)` → coordinator assigns
   PID + epoch. Any previous epoch is fenced (zombie producers throw
   `ProducerFencedException` on next request).
2. `beginTransaction()` — local marker.
3. `send(...)` writes batches with the transactional flag set.
4. `sendOffsetsToTransaction()` — for consume-process-produce flows.
5. `commitTransaction()` → coordinator writes a commit marker to every
   partition involved.
6. Consumers with `isolation.level=read_committed` see records only
   after the commit marker.

If the producer crashes before step 5, the coordinator waits up to
`transaction.timeout.ms` then aborts the transaction. During that
window, downstream `read_committed` consumers stall at the open
transaction's start offset on affected partitions — Last Stable Offset
(LSO) doesn't advance.

The big subtle one: `transactional.id` must be **stable across
restarts**. If it's `"my-app-" + UUID.randomUUID()`, every restart
creates a fresh transactional identity, no fencing happens, and a
crashed instance's open transaction blocks consumers for the full
`transaction.timeout.ms` because nothing comes back to abort it.

JMX: `kafka.producer:type=producer-metrics,client-id=*:txn-abort-rate`,
`txn-commit-rate`. Broker-side
`kafka.server:type=transaction-coordinator-metrics:OpenTransactionCount`.

## Operational impact

- **Cost.** A few extra requests per transaction (begin, commit). Stable
  cost.
- **Latency.** ~5-10 ms per commit (coordinator round-trip + marker
  writes). Compounded with `read_committed` consumer-side delivery
  delay.
- **Throughput.** Transactions per second is the bottleneck. Aim for
  larger transactions covering more records to amortize the per-commit
  overhead.
- **Operational toil.** Zombie transactions, stuck LSO, fenced
  producers — failure modes the linter cannot fix but can keep
  consistent.

## Consult a friend?

Yes. Familiarity is not understanding. Common misconceptions:

- **"Idempotence and transactions are the same thing."** No. Idempotence
  is per-partition de-duplication on a single producer's retries.
  Transactions add atomicity across partitions and consumer offset
  commits. Transactions require idempotence; idempotence does not require
  transactions.
- **"I can use `transactional.id` for any unique string."** It must be
  stable across restarts of the *same logical instance*. Two patterns
  work: `(app, partition)` derived from a partition assignment (Streams
  does this internally), or `(app, instance-id)` where instance-id is
  pinned to a pod ordinal / k8s StatefulSet name. UUID-per-startup is
  wrong.
- **"`read_committed` means I can't lose data."** It means you don't
  read uncommitted data. You can still skip data if the upstream
  producer never finishes — LSO sits, lag climbs, eventually the
  transaction times out and the records are aborted (lost). EOS is a
  property of the whole pipeline, not just `read_committed`.
- **"Transactions speed things up because of larger batches."** No.
  They slow things down per commit; the gain comes from amortizing the
  commit overhead over many records, not from batching itself.

If the team is shipping a transactional producer for the first time,
schedule a 30-min reading session covering KIP-98 and KIP-447 with
someone who has shipped one before. Production transaction debugging is
not something to learn from logs alone.

## How to fix (no → yes)

```java
// no — transactional.id is unstable
p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
       "my-app-" + UUID.randomUUID());

// no — manually disabled idempotence under transactional.id
p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-app-prod");
p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // ConfigException at startup

// yes — stable per-instance id, defaults handle the rest
String instanceId = System.getenv("POD_NAME"); // or StatefulSet ordinal
p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-tx-producer-" + instanceId);
p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
// enable.idempotence, acks, max.in.flight: defaults are correct
```

Verify the broker:

```properties
# broker — must be >= producer's transaction.timeout.ms
transaction.max.timeout.ms=900000
```

## When this might be a false positive

- The project doesn't use transactions at all (no `transactional.id` set
  anywhere). The rule does not fire — that's the design.
- Spring `KafkaTransactionManager` manages the bundle; if Spring is
  detected and `spring.kafka.producer.transaction-id-prefix` is set,
  Spring derives a stable id per instance — suppress the manual rule.
- Kafka Streams sets `transactional.id` automatically when
  `processing.guarantee=exactly_once_v2`. Suppress the manual rule on
  Streams configs.

## Detection strategy

- **Config files & bytecode (combined):**
  - If `transactional.id` is set:
    - Verify `enable.idempotence` is not explicitly `false`.
    - Verify `transaction.timeout.ms` is set; warn if missing (defaults
      to 60s, often too short for batch jobs).
    - Verify `transactional.id` value: HIGH-confidence flag for
      `UUID.randomUUID()`, `System.currentTimeMillis()`, or env vars not
      typically stable (cross-link
      [PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE](../kafka-clients/PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.md)
      for the idempotence-off variant).
  - If `transactional.id` is NOT set and the project uses transactions
    via Spring `KafkaTransactionManager`: cross-link
    [SPRING_TRANSACTIONAL_WITHOUT_KTM](../spring-kafka/SPRING_TRANSACTIONAL_WITHOUT_KTM.md)
    for the inverse case.
- **Confidence: MEDIUM** for the bundle. HIGH on the unstable-id pattern.

## References

- KIP-98 — Exactly Once Delivery and Transactional Messaging:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- KIP-447 — Producer scalability for exactly-once semantics:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
- Confluent — Transactions in Apache Kafka:
  https://www.confluent.io/blog/transactions-apache-kafka/
- Apache Kafka producer configs — `transactional.id`:
  https://kafka.apache.org/documentation/#producerconfigs_transactional.id
- Conduktor Config Advisor — producer durability/EOS profile:
  https://kafka-options-explorer.conduktor.io/config-advisor/
