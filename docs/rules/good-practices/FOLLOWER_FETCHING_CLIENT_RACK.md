# FOLLOWER_FETCHING_CLIENT_RACK

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: Cross-AZ fetches are billed by the gigabyte. `client.rack` is the line that turns that bill into local traffic.

## TL;DR

The linter flags consumers (and Streams `main.consumer.*` /
`restore.consumer.*` configs) that omit `client.rack`. With it set, KIP-392
lets the consumer fetch from an in-AZ follower instead of the
cross-AZ leader. On AWS / GCP / Azure that change is the difference between
a cross-AZ egress line item and zero.

## The setup

Before KIP-392 (Kafka 2.4), consumers always fetched from the partition
leader. In a 3-AZ cluster, two thirds of consumers always paid cross-AZ
egress to read.

KIP-392 introduced "Fetch From Closest Replica":

- The consumer sends its `client.rack` (the AZ it lives in) on every
  metadata and fetch request.
- The broker, configured with a `replica.selector.class` (typically
  `org.apache.kafka.common.replica.RackAwareReplicaSelector`), picks an
  in-sync follower in the same rack as the client and tells the consumer
  to fetch from it.
- If no same-rack replica is in-sync, the consumer falls back to the
  leader.

The linter checks the client side only — the broker side
(`replica.selector.class`, broker `broker.rack`) is operational config
the linter can't see. When the client side is missing, surface that
fact and suggest checking the broker.

## What's actually happening (the mechanism)

With `client.rack` unset:
1. Consumer asks for metadata → broker returns the leader.
2. Every `FetchRequest` goes to the leader.
3. If the leader is in another AZ, every fetched byte crosses AZ.

With `client.rack=eu-west-1a` and a rack-aware selector on the broker:
1. Consumer's metadata request includes the rack.
2. Broker selects a same-rack ISR follower.
3. Consumer's `FetchRequest`s go to that follower.
4. The follower already has the data (replicated from leader, billed once
   on the producer side); serving it to the same-AZ consumer is free.

On the wire, the only difference is the `client.rack` field in the
`MetadataRequest` and an alternate broker host returned in the
`MetadataResponse`. From JMX: `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*:fetch-rate`
stays the same, but the broker-side `kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec`
re-allocates from the leader to followers.

## Operational impact

- **Cost.** On a 3-AZ AWS deployment, cross-AZ egress is around
  $0.01/GB on EC2-to-EC2 (it has not moved much since 2020 — verify
  current pricing). A 50 MB/s consumer reading from a cross-AZ leader
  costs about $13k/year per consumer; same-AZ follower fetching cuts it
  to zero (you still pay producer-side cross-AZ replication, which you'd
  pay anyway).
- **Latency.** Same-AZ RTT (sub-ms) replaces inter-AZ RTT
  (1-2 ms in a region). Modest absolute saving, but compounds on poll
  loops that do many small fetches.
- **Throughput.** Same. Followers serve fetches as well as leaders for
  consumers (producers still write to the leader).
- **Observability gain.** Per-broker `BytesOutPerSec` now reflects the
  AZ distribution of consumers, useful for capacity planning.

## How to fix (no → yes)

```properties
# no — every fetch potentially crosses AZ
bootstrap.servers=b1:9092,b2:9092,b3:9092
group.id=orders-svc

# yes — feed the AZ in from the deployment
bootstrap.servers=b1:9092,b2:9092,b3:9092
group.id=orders-svc
client.rack=${AVAILABILITY_ZONE}
```

```java
// yes — Java client
String az = System.getenv("AWS_AVAILABILITY_ZONE"); // or AZURE_REGION, GCP_ZONE
props.put(ConsumerConfig.CLIENT_RACK_CONFIG, az);
```

```yaml
# yes — Spring Boot
spring:
  kafka:
    consumer:
      properties:
        client.rack: ${AVAILABILITY_ZONE}
```

```properties
# yes — Quarkus
kafka.client.rack=${AVAILABILITY_ZONE}
# or per-channel
mp.messaging.incoming.orders.client.rack=${AVAILABILITY_ZONE}
```

Streams: set both consumers.

```properties
main.consumer.client.rack=${AVAILABILITY_ZONE}
restore.consumer.client.rack=${AVAILABILITY_ZONE}
```

On Kubernetes the AZ is in the downward API:
`topology.kubernetes.io/zone` on the node, surfaced to the pod via
`fieldRef: spec.nodeName` + a side-car or via the
`topology.kubernetes.io/zone` label propagated through env.

The broker side, which the linter can't verify, must also be configured:

```properties
# broker — for completeness
broker.rack=eu-west-1a
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
```

## When this might be a false positive

- Single-AZ cluster (deliberate cost / simplicity tradeoff): no follower
  in another rack to fetch from, the feature is a no-op.
- On-prem / single-DC deployments where there is no AZ concept.
- Producers (the rule does not apply — producers always write to the leader).
- The brokers do not have `replica.selector.class` configured — the
  feature degrades to leader fetching, no harm done but also no benefit.
- A managed Kafka where the provider already injects `client.rack` via
  the client library (some Confluent Cloud and AWS MSK Java clients do
  this).

## Detection strategy

- **Config files:** scan for `client.rack` (case-insensitive) in every
  consumer-scope config source:
  - `application.properties`, `application.yml`
  - `spring.kafka.consumer.properties.client.rack`,
    `spring.kafka.streams.properties.client.rack`
  - Quarkus `kafka.client.rack`,
    `mp.messaging.incoming.<channel>.client.rack`
  - Streams `main.consumer.client.rack`, `restore.consumer.client.rack`.
- **Bytecode:** detect `Properties.put(ConsumerConfig.CLIENT_RACK_CONFIG, ...)`
  or `props.put("client.rack", ...)`. The constant lives on
  `ConsumerConfig.CLIENT_RACK_CONFIG` since 2.4.
- **Confidence: CONTEXT.** The rule cannot prove that the deployment is
  multi-AZ. Default to print-only; promote to WARNING only if the project
  declares a cloud profile.

## References

- KIP-392 — Allow consumers to fetch from closest replica:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica
- Apache Kafka consumer configs — `client.rack`:
  https://kafka.apache.org/documentation/#consumerconfigs_client.rack
- AWS — Reducing Cross-AZ data transfer with Kafka follower fetching:
  https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
- Confluent — Multi-region clusters and follower fetching:
  https://docs.confluent.io/platform/current/multi-dc-deployments/multi-region.html
