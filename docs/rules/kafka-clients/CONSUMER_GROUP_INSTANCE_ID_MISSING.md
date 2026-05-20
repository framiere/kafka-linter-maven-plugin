# CONSUMER_GROUP_INSTANCE_ID_MISSING

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: bytecode
**Tagline**: Without group.instance.id, every rolling restart is a full rebalance you didn't need.

## TL;DR

The linter flags consumers whose `group.id` is set but `group.instance.id` is not. For stateful or stable deployments (k8s pods, VMs), static membership (KIP-345) avoids unnecessary rebalances on restart.

## What's happening (the mechanism)

Without `group.instance.id`, each consumer is a "dynamic" member: on startup it gets a fresh `member.id`, and on graceful shutdown it sends a `LeaveGroup` request that triggers an immediate rebalance. A rolling restart of N consumers therefore triggers up to 2N rebalances (one on leave, one on rejoin per instance).

With `group.instance.id` (KIP-345, Kafka 2.3+), the consumer:
- Keeps its member identity across restarts.
- Does NOT send `LeaveGroup` on graceful shutdown — the coordinator waits up to `session.timeout.ms` for the same instance to come back before rebalancing.
- Reclaims its previous partition assignment on rejoin.

For stateful consumers (Streams stores, local caches, idempotent processors with hot state), this dramatically reduces partition churn.

## Operational impact

- N pods rolling restart → N rebalances (best case Eager protocol, slightly better with Cooperative), each interrupting downstream throughput for seconds.
- Hot caches / RocksDB state stores rebuild every cycle.
- `kafka.consumer:type=consumer-coordinator-metrics:partition-revoked-latency-avg` accumulates across rolling deploys.

## How to fix

```java
// BAD — dynamic membership; every restart = rebalance
props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-svc");
// no group.instance.id

// GOOD — static membership keyed to the pod
String podName = System.getenv("HOSTNAME");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-svc");
props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "orders-svc-" + podName);
// also raise session.timeout.ms so brief restarts don't trigger eviction
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
```

The `group.instance.id` must be unique per consumer instance. Reusing the same id across two live consumers in the same group will fence one of them (`FencedInstanceIdException`).

## When this might be a false positive

- Truly ephemeral consumers (CI test runners, ad hoc scripts) where rebalances on restart are the right behavior.
- Auto-scaled consumers where pod identity is not stable.
- Connecting to brokers older than 2.3 — `group.instance.id` is rejected.

## Detection strategy

- Bytecode: find Properties where `group.id` is set but `group.instance.id` is not, then passed to `new KafkaConsumer(...)`. CONTEXT confidence (legitimate reasons exist).
- Suppress if the framework manages this (Kafka Streams sets it automatically since 3.0 via `application.id`).
- Suggest pairing with raised `session.timeout.ms` (30–60s) so brief restarts don't blow the window.

## References

- KIP-345 — Static membership: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345
- Apache Kafka consumer configs — `group.instance.id`: https://kafka.apache.org/documentation/#consumerconfigs_group.instance.id
- Confluent — Static Consumer Group Membership: https://www.confluent.io/en-gb/blog/dynamic-vs-static-kafka-consumer-rebalancing/
