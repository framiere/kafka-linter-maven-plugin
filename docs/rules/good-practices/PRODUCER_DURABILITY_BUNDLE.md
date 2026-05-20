# PRODUCER_DURABILITY_BUNDLE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: Durability is a five-key chord. Miss one note and the whole song goes flat.

## TL;DR

The linter checks that a producer config has the full durability bundle:
`acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection<=5`,
`retries=Integer.MAX_VALUE`, and a `delivery.timeout.ms` that bounds the
retry window without being too tight. Since Kafka 3.0 (KIP-679) these
are the defaults — but the linter still flags any one that is explicitly
overridden away from the safe value, and warns when an older client
version is on the classpath where the defaults differ.

## The setup

KIP-679 (Kafka 3.0, Sept 2021) made the producer durable by default:

| key | pre-3.0 default | 3.0+ default |
|---|---|---|
| `acks` | `1` | `all` |
| `enable.idempotence` | `false` | `true` |
| `max.in.flight.requests.per.connection` | `5` | `5` |
| `retries` | `0` (some builds) → `Integer.MAX_VALUE` | `Integer.MAX_VALUE` |
| `delivery.timeout.ms` | `120000` | `120000` |

Two failure modes still ship to production in 2026:

1. The project is on `kafka-clients` 2.x or 3.0-pre and assumes 3.x
   defaults.
2. The project is on 3.x but someone explicitly set `acks=1` or
   `enable.idempotence=false` for a long-forgotten reason ("we had retries off
   for the demo").

The bundle is also a system: setting any one to an incompatible value
breaks the others. `acks=1` + `enable.idempotence=true` throws
`ConfigException` at startup. `max.in.flight=6` + idempotence throws
`ConfigException`. `enable.idempotence=true` forces `acks=all` and
`retries>0` anyway.

## What's actually happening (the mechanism)

Each key contributes one piece of the at-least-once-with-no-reorder
guarantee:

- **`acks=all`** — the leader waits for every in-sync replica to fsync
  the batch before acknowledging. Paired with broker
  `min.insync.replicas>=2`, this is the only setting that survives a
  leader crash without data loss.
- **`enable.idempotence=true`** — the producer is assigned a Producer
  ID (PID); every batch carries a sequence number per partition. On
  retry the broker dedups by `(PID, partition, sequence)`, so a network
  timeout that causes a resend does not double-write.
- **`max.in.flight.requests.per.connection<=5`** — the broker only
  remembers the last 5 sequence numbers per `(PID, partition)`. With 6+
  in flight, a retry of batch N may arrive after batch N+5 has been
  acknowledged, and the dedup window has rolled past. Higher values are
  rejected client-side with `ConfigException`.
- **`retries=Integer.MAX_VALUE`** — let `delivery.timeout.ms` govern the
  retry window instead of a count. A leader election lasting 8 seconds
  must not exhaust retries.
- **`delivery.timeout.ms`** — the upper bound on how long a record
  lingers in the accumulator before `send()` either succeeds or fails
  permanently. 120s (default) tolerates rolling restarts; 300s
  tolerates extended controller failover.

On the wire: idempotent producers do an `InitProducerId` handshake at
startup. Each `ProduceRequest` carries the PID, producer epoch, and
base sequence number. `kafka.producer:type=producer-metrics,client-id=*:record-error-rate`
will surface `OUT_OF_ORDER_SEQUENCE_NUMBER` if the bundle is broken.

## Operational impact

- **Cost.** None directly; the durability tax is a few extra milliseconds
  per produce.
- **Latency.** `acks=all` adds the ISR replication round-trip (5-50 ms
  typically). Acceptable for almost every workload.
- **Throughput.** Idempotence + `acks=all` has been within 5-10% of the
  fire-and-forget mode since Kafka 2.5 — measurable but rarely a deciding
  factor.
- **Operational toil.** Without the bundle: silent data loss on every
  leader failure, silent duplicates on every network blip. With it: zero
  events for the same incidents.
- **Observability gain.** `record-error-rate` becomes a true error rate
  rather than a duplicate-generation rate.

## How to fix (no → yes)

```properties
# no — quietly disabled durability ("had a reason once")
acks=1
enable.idempotence=false
retries=3

# yes — the bundle
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=2147483647
delivery.timeout.ms=120000
```

```java
// yes — Java client, post-KIP-679 defaults
Properties p = new Properties();
p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
// acks, enable.idempotence, max.in.flight, retries, delivery.timeout.ms
// are all already correct since 3.0 — do not override.
p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
new KafkaProducer<>(p);
```

If the project is still on `kafka-clients` 2.x:

```java
// yes — explicit on a pre-3.0 client
p.put(ProducerConfig.ACKS_CONFIG, "all");
p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
p.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
```

Broker side, which the linter can't see but the developer must
arrange: `min.insync.replicas>=2` on durability-critical topics.
`acks=all` only waits for the current ISR; with `min.insync.replicas=1`
a one-replica ISR is enough for an ack, and the guarantee degrades to
`acks=1`.

## When this might be a false positive

- Genuinely lossy / throughput-extreme topics (clickstream, log
  aggregation) where the team has made an explicit at-most-once decision.
  Mark the topic intent with a comment near the config and suppress.
- Latency-extreme front doors (real-time recommendation features
  shipping to an in-memory ML model) where `acks=1` is a measured
  tradeoff. Rare; document the reasoning.
- A producer that's not the source of record — e.g. re-emitting an
  already-durable upstream into a temporary scratch topic.

## Detection strategy

- **Config files & bytecode (combined):** for every producer-scope
  property source, check all five keys. Flag if any of:
  - `acks` is set to `0` or `1` (cross-link
    [PRODUCER_ACKS_ZERO](../kafka-clients/PRODUCER_ACKS_ZERO.md),
    [PRODUCER_ACKS_ONE](../kafka-clients/PRODUCER_ACKS_ONE.md))
  - `enable.idempotence=false` (cross-link
    [PRODUCER_IDEMPOTENCE_DISABLED](../kafka-clients/PRODUCER_IDEMPOTENCE_DISABLED.md))
  - `max.in.flight.requests.per.connection > 5`
  - `retries=0` (cross-link
    [PRODUCER_RETRIES_ZERO](../kafka-clients/PRODUCER_RETRIES_ZERO.md))
- **Version-aware:** if `kafka-clients` < 3.0 is on the classpath and
  none of these keys are present, emit the bundle warning anyway —
  defaults are unsafe pre-3.0 (cross-link
  [KAFKA_CLIENTS_PRE_KIP679](../versions/KAFKA_CLIENTS_PRE_KIP679.md)).
- **Confidence: MEDIUM.** The linter cannot tell whether a topic
  intentionally trades durability for throughput.

## References

- KIP-679 — Producer will enable the strongest delivery guarantee by default:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default
- KIP-98 — Exactly Once Delivery and Transactional Messaging:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- Apache Kafka producer configs:
  https://kafka.apache.org/documentation/#producerconfigs
- Conduktor Config Advisor — producer durability profile:
  https://kafka-options-explorer.conduktor.io/config-advisor/
