# CONSUMER_EOS_READ_COMMITTED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: Reading from a transactional producer with the default isolation is reading drafts.

## TL;DR

The linter flags a consumer that lacks `isolation.level=read_committed`
when the same project (or its upstream, when known) writes via a
transactional producer (`transactional.id` set). Without
`read_committed`, the consumer reads records that may later be aborted
— breaking exactly-once semantics end-to-end.

## The setup

The producer-side rule (PRODUCER_TRANSACTIONAL_BUNDLE) handles one half
of EOS. The consumer side is just as important and easier to forget:
the default `isolation.level=read_uncommitted` returns *every* record
including ones that belong to in-flight or eventually-aborted
transactions.

KIP-98 added `read_committed`. The broker holds back records past the
Last Stable Offset (LSO) — the lowest offset still belonging to an
open transaction — and only delivers them to `read_committed`
consumers after the commit marker arrives.

The pair is enforced as a contract, not by Kafka: a developer can ship
a transactional producer without `read_committed` consumers and Kafka
won't complain. The linter does.

## What's actually happening (the mechanism)

`read_uncommitted` (default): consumer receives every record as soon
as it's persisted on the broker, including records in open or
eventually-aborted transactions. Downstream processing acts on
not-yet-committed and aborted data.

`read_committed`: broker delivers records only up to the LSO. If an
upstream transaction is open, the consumer pauses at that offset until
the transaction commits or aborts:

- Commit marker: records are delivered.
- Abort marker: records are filtered out, consumer advances past them.

JMX: `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*:records-lag-max`
behaves differently — under `read_committed` it includes records held
back by open transactions, so a "stuck" lag often means an upstream
producer is holding a transaction open.

## Operational impact

- **Cost.** None directly.
- **Latency.** End-to-end latency includes the upstream transaction
  commit window (typically 5-50 ms; up to seconds for batch jobs that
  commit only at end).
- **Throughput.** Same. Records arrive in batches when commits land.
- **Correctness.** This is the whole point. Without it, downstream sees
  aborted transactions as "valid" data.
- **Failure modes.** A crashed transactional producer holds a
  transaction open for up to `transaction.timeout.ms` (default 60s);
  during that window the `read_committed` consumer stalls at the LSO.

## Consult a friend?

Yes. Two confusions are common:

- **"`read_committed` makes me exactly-once on its own."** No. It
  filters aborted records on the read side. It does nothing about
  duplicates in your *own* processing logic (e.g., your consumer crashes
  after side effects but before committing offsets). EOS requires:
  upstream transactional producer + `read_committed` consumer +
  application-side idempotent processing OR transactional offset commit
  (consume-process-produce pattern with `sendOffsetsToTransaction`).
- **"My consumer reads from a non-transactional topic, so the default
  is fine."** True — for that topic. But the moment ANY producer
  writing to that topic uses transactions, your consumer reads aborted
  data. Default-on `read_committed` is the safer choice on any topic
  that could become transactional later.

## How to fix (no → yes)

```properties
# no — paired with a transactional producer in the same project
spring.kafka.producer.transaction-id-prefix=orders-tx-
spring.kafka.consumer.isolation-level=read_uncommitted  # default, wrong

# yes
spring.kafka.consumer.isolation-level=read_committed
```

```java
// yes — Java client
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```

```properties
# yes — Quarkus
mp.messaging.incoming.orders.isolation.level=read_committed
```

Streams: handled automatically when `processing.guarantee=exactly_once_v2`.

## When this might be a false positive

- Consumer reads only from non-transactional topics (raw event streams,
  log aggregation). `read_uncommitted` is the right (and faster) choice.
- Stream-stream join where both sides are non-transactional.
- Cross-link to the existing anti-pattern
  [CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN](../kafka-clients/CONSUMER_ISOLATION_READ_UNCOMMITTED_WITH_TXN.md)
  for the inverse framing — the anti-pattern fires when we can
  *prove* the upstream is transactional.

## Detection strategy

- **Config files & bytecode (combined):**
  - In the same project, check whether any producer has `transactional.id`
    set (or Spring `transaction-id-prefix`, Streams `exactly_once_v2`).
  - If yes, and any consumer in the same project has `isolation.level`
    unset or set to `read_uncommitted`, flag.
  - Cannot detect upstream-only transactional producers (different
    project). Confidence is MEDIUM at best.
- **Confidence: MEDIUM.** Some legitimate cases exist (consumer reads
  from a non-transactional topic that happens to share a project with a
  transactional producer on a *different* topic — the linter can't tell).

## References

- KIP-98 — Exactly-Once Delivery and Transactional Messaging:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- Apache Kafka consumer configs — `isolation.level`:
  https://kafka.apache.org/documentation/#consumerconfigs_isolation.level
- Conduktor Config Advisor — consumer durability profile:
  https://kafka-options-explorer.conduktor.io/config-advisor/
