# CLIENT_ID_MISSING

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: both
**Tagline**: No client.id is a faceless caller — broker logs see "producer-1" and so do you.

## TL;DR

The linter flags Kafka producer and consumer constructions where `client.id` is not set. Without it, the broker generates a generic id (`producer-1`, `consumer-2`) which is useless for quotas, audit, and incident triage.

## What's happening (the mechanism)

`client.id` is included in every request to the broker (`ProduceRequest`, `FetchRequest`, etc.) and surfaces in:

- Broker request logs (`kafka.request.logger`)
- Broker quota enforcement (`kafka.server:type=ClientQuotaMetrics,user=*,client-id=*`)
- JMX metrics on both sides
- `kafka-consumer-groups.sh --describe` output
- Authorizer audit logs

Default: if unset, the producer/consumer generates a synthetic `producer-N` / `consumer-N` per JVM. Two replicas of the same service look indistinguishable in broker logs.

## Operational impact

- Client quotas (`quota.window.size.seconds` / `producer_byte_rate`) cannot be applied per-application — they hit the synthetic id, which is shared by accident across unrelated workloads on the same JVM.
- Incident triage: "which one of the 40 services hammered partition 7?" — answer not available.
- `__consumer_offsets` audit shows no application name.
- Compliance / audit logs miss the actor identity.

## How to fix

```java
// BAD
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "...");
// no client.id

// GOOD
props.put(ProducerConfig.CLIENT_ID_CONFIG, "orders-api-producer-" + hostname);
```

For consumers: include the consumer group plus the instance — `client.id = orders-svc-consumer-${POD_NAME}`. Pair with `group.instance.id` for static membership.

## When this might be a false positive

- Throw-away CLI tools and one-shot scripts.
- Test fixtures.

## Detection strategy

- Bytecode: detect `new KafkaProducer(props)` / `new KafkaConsumer(props)` where the `props` Properties is built in the same method and there is no `props.put("client.id", ...)` and no `ProducerConfig.CLIENT_ID_CONFIG` / `ConsumerConfig.CLIENT_ID_CONFIG` reference. MEDIUM (can be set externally).
- Config: warn if any framework-managed properties source declares `bootstrap.servers` but no `client.id`.
- Confidence: drop to LOW if the props object is passed in from outside the analyzed method (cannot prove absence).

## References

- Apache Kafka producer configs — `client.id`: https://kafka.apache.org/documentation/#producerconfigs_client.id
- Apache Kafka — quotas: https://kafka.apache.org/documentation/#design_quotas
- KIP-371 — propagate client.id to broker: https://cwiki.apache.org/confluence/display/KAFKA/KIP-371
