# REQUEST_TIMEOUT_LT_REPLICA_LAG

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: A request timeout shorter than replica lag is an SLA against physics.

## TL;DR

The linter flags producer `request.timeout.ms` set very low (< 10000) with `acks=all`. Replication to the ISR can take longer than that under load; the broker's `replica.lag.time.max.ms` (default 30s) bounds how slow a follower can be before being kicked out. Setting the producer timeout below this guarantees timeouts during normal replication lag.

## What's happening (the mechanism)

`request.timeout.ms` (default 30000) bounds how long the producer waits for a single produce request response. With `acks=all`, the response only fires after the partition leader has received acks from all in-sync replicas. The broker tolerates ISR followers up to `replica.lag.time.max.ms` (default 30000) behind before kicking them out.

If `request.timeout.ms` < `replica.lag.time.max.ms`, then any time a follower is slow but still in the ISR (well within the broker's tolerance), the producer times out. The broker continues replicating, eventually succeeds, and the producer retries (and may succeed or duplicate, depending on idempotence).

## Operational impact

- Producer timeouts under normal ISR lag (broker is fine, follower is "slow but in ISR").
- Retry storms during partial degradation.
- `producer-metrics:record-error-rate` spikes correlated with broker `kafka.server:type=ReplicaFetcherManager:MaxLag` going up — even though the broker is operating within spec.

## How to fix

```java
// BAD
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

// GOOD
props.put(ProducerConfig.ACKS_CONFIG, "all");
// default request.timeout.ms=30000 matches replica.lag.time.max.ms default
// (omit, or set explicitly to ≥ 30000)
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
```

## When this might be a false positive

- Latency-critical systems where the operator has deliberately tightened the timeout and accepts the retry cost. Document and downgrade.

## Detection strategy

- Config: `request.timeout.ms < 10000` paired with `acks=all` (or `acks` unset, i.e., default `all` since 3.0). MEDIUM.

## References

- Apache Kafka producer configs — `request.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_request.timeout.ms
- Apache Kafka broker configs — `replica.lag.time.max.ms`: https://kafka.apache.org/documentation/#brokerconfigs_replica.lag.time.max.ms
- AWS MSK best practices — producer timeouts: https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html
