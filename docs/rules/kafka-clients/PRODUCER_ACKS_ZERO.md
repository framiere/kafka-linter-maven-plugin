# PRODUCER_ACKS_ZERO

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: both
**Tagline**: acks=0 is fire-and-pray.

## TL;DR

The linter flags producers configured with `acks=0` — the broker never acknowledges writes, so any in-flight record dropped by the network or the leader is silently lost.

## What's happening (the mechanism)

With `acks=0` the producer marks a record as delivered as soon as the OS write call returns. No `ProduceResponse` is awaited from the partition leader, no commit is required to followers, and no error path triggers a retry. The producer does not learn about leader elections, ISR shrinkage, throttling, broker restarts, or even the broker dropping the TCP connection mid-batch.

Defaults: since Apache Kafka 3.0 the producer default is `acks=all` and `enable.idempotence=true` (KIP-679). Explicitly setting `acks=0` is therefore a deliberate downgrade from the safe default. Idempotence is incompatible with `acks=0` and will fail validation if both are explicit.

State machine: with `acks=0` retries are effectively disabled (there is no failure signal to retry on), so the records lost are gone — there is no `Callback` exception to handle them.

## Operational impact

- Silent message loss during leader failover, network blips, or broker restarts. The `record-error-rate` JMX metric stays at zero because the producer never sees the errors.
- `Callback.onCompletion` always reports `metadata` with `offset=-1` and `RecordMetadata` whose offset/timestamp are meaningless.
- No correlation between `producer-metrics:record-send-rate` and broker-side `MessagesInPerSec` — gap = lost data.
- The pager goes off in the downstream service ("missing events") long after the producer-side incident is over and unrecoverable.
- Cost-of-discovery is brutal because logs look healthy on the producer side.

## How to fix

```java
// BAD
props.put(ProducerConfig.ACKS_CONFIG, "0");

// GOOD — accept the default (acks=all since 3.0)
// (omit the property entirely)

// GOOD — if you must be explicit
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

If throughput is the worry, prefer `acks=all` plus larger `batch.size` / `linger.ms` rather than weakening durability — the throughput delta from `acks=all` is small once batching is on.

## When this might be a false positive

- True best-effort telemetry sinks where loss is explicitly acceptable (server-side application metrics, click-tracking previews). Document the choice and downgrade the rule for that module.
- Local development clusters where durability does not matter.

## Detection strategy

- Config files (`application.properties`, `application.yml`, `*.properties`): key `acks` (or `spring.kafka.producer.acks`, `kafka.producer.acks`, `mp.messaging.outgoing.*.acks`) with value `0`. HIGH confidence.
- Bytecode: a `Properties.put` / `Map.put` call whose first arg is an `LDC "acks"` (or `ProducerConfig.ACKS_CONFIG` resolved at compile time to `"acks"`) and whose second arg is `LDC "0"`. HIGH confidence when both LDCs are constant.
- Confidence: HIGH for explicit `"0"`. MEDIUM if `acks` is read from environment/config without a default (the value cannot be evaluated statically).

## References

- Apache Kafka producer configs — `acks`: https://kafka.apache.org/documentation/#producerconfigs_acks
- KIP-679 — Producer will enable the strongest delivery guarantee by default: https://cwiki.apache.org/confluence/display/KAFKA/KIP-679
- Confluent docs — Producer durability: https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
