# WARPSTREAM_BATCH_AND_LINGER_DEFAULTS
**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: Default `batch.size=16384` + `linger.ms=0` against object storage = one S3 PUT per record.
**Source**: Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Java Client Overrides, § Quick Checklist.

## TL;DR

When the target is WarpStream, the linter flags producer configurations that leave `batch.size`, `linger.ms`, `compression.type`, and `buffer.memory` at Apache-Kafka defaults. WarpStream Agents flush batches to object storage; each object-store write has a fixed per-request cost (latency + dollar cost). With default `batch.size=16384` and `linger.ms=0`, every record can trigger its own S3 PUT — turning a high-throughput topic into hundreds of thousands of S3 requests per second. The Confluent recommendation is `batch.size=100000`, `linger.ms=100`, `compression.type=lz4`, `buffer.memory=128000000`.

## What's happening (the mechanism)

Apache Kafka producer batching is optimized for low latency on disk-backed brokers — small batches keep latency low, the broker absorbs the I/O cost. The defaults reflect that: `batch.size=16384` bytes (16 KB), `linger.ms=0` (flush immediately), `compression.type=none`, `buffer.memory=33554432` (32 MB).

WarpStream Agents write to object storage. The cost model inverts:
- Each S3 PUT carries fixed latency (~250ms p50) and a per-request dollar charge (~$0.005 per 1000 PUTs on standard S3).
- Throughput is bound by request count per Agent, not by bytes/sec.
- Therefore: large batches are strictly better than small batches.

Specifically:
- `batch.size=100000` (100 KB): groups more records per object-storage write.
- `linger.ms=100`: gives the batch time to fill, even when traffic is bursty.
- `compression.type=lz4`: Agents decompress + recompress for storage; LZ4 is the right tradeoff for client CPU vs. wire bytes.
- `buffer.memory=128000000` (128 MB): the producer keeps more records buffered while in-flight requests are large.

On a busy topic, going from 16 KB batches at `linger.ms=0` to 100 KB at `linger.ms=100` typically yields a 10x reduction in S3 PUT count and a 5-10x improvement in producer throughput.

## Operational impact

- **Cost**: at 100k records/sec with default batching, you may issue ~30k S3 PUTs/sec — at $5 per million PUTs that's $13/hour just in S3 request fees, per producer cluster.
- **Throughput ceiling**: each Agent can only handle a finite number of in-flight S3 writes. Small batches saturate the request quota before saturating the byte quota.
- **Tail latency**: because every record triggers a PUT, the p99 latency is dominated by S3 tail latency. Larger batches amortize this.

## How to fix (bad → good code)

```properties
# BAD on WarpStream — Apache Kafka defaults
# batch.size=16384
# linger.ms=0
# compression.type=none
# buffer.memory=33554432

# GOOD on WarpStream (Confluent-recommended)
batch.size=100000
linger.ms=100
compression.type=lz4
buffer.memory=128000000
max.request.size=64000000
request.timeout.ms=30000
metadata.max.age.ms=60000
metadata.recovery.strategy=rebootstrap

# Pair with idempotence off + high in-flight (see WARPSTREAM_IDEMPOTENCE_ENABLED)
enable.idempotence=false
max.in.flight.requests.per.connection=1000
```

For Kafka Streams, set these via the `producer.` prefix:

```properties
producer.batch.size=100000
producer.linger.ms=100
producer.compression.type=lz4
producer.buffer.memory=128000000
```

For latency-sensitive workloads, reduce `linger.ms` to 10-25ms — but only if the topic's traffic shape sustains the batch even at the smaller window.

## When this might be a false positive

- The target is not WarpStream. The Apache Kafka defaults are correct on a disk-backed cluster — bigger batches there increase produce latency for no S3-cost benefit.
- The application is intentionally latency-optimized and `linger.ms=0` is required. In that case, configure the WarpStream Agent's `WARPSTREAM_BATCH_TIMEOUT` instead, and inform the user of the cost.
- A low-volume control topic where batch efficiency doesn't matter.

## Detection strategy

- WarpStream context signal (see `WARPSTREAM_IDEMPOTENCE_ENABLED` for detection criteria).
- Config-file check: any of these is at the Apache Kafka default OR missing:
  - `batch.size` < 50000 (or unset)
  - `linger.ms` < 50 (or unset)
  - `compression.type` is `none` or unset
  - `buffer.memory` < 67108864 (or unset)
- Flag with the specific knob that's misconfigured.
- Confidence: CONTEXT — the rule is only relevant when WarpStream targeting is established.

## Consult a friend?

> 🤝 **Slow down.** Cranking `batch.size` and `linger.ms` is the right move on WarpStream, but the same config on a *non*-WarpStream cluster (dev environment using Apache Kafka, integration tests against a local broker, fallback to MSK during a WarpStream outage) is now a 100ms latency tax on every record for no benefit.
> - Is the WarpStream targeting expressed in code/config, or is it implicit in `bootstrap.servers`? If a dev environment shares the same `application.properties` with a different broker URL, the tuned config follows. Use profile-scoped properties so the tune only applies when the target actually warrants it.
> - `linger.ms=100` adds up to 100ms to the *first* record in each batch — for latency-sensitive paths (request/response, user-facing acks) that's a real SLO impact. Confirm which topics are latency-critical and consider per-topic `KafkaTemplate` beans for those.
> - The math on cost only holds at scale. For low-volume topics (control plane, audit), the larger `buffer.memory` is just RAM that sits empty. Right-size per app, not per cluster.
> - On the broker side, `max.request.size=64000000` (64 MB) only works if the broker / Agent also accepts that. Check WarpStream Agent's `WARPSTREAM_MAX_MSG_SIZE_BYTES` and the topic's `max.message.bytes` before deploying the producer change.

## References

- Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Java Client Overrides, § Latency Expectations and Tuning, § Quick Checklist
- WarpStream docs — Sticky partitioning and batching: https://docs.warpstream.com/warpstream/reference/configuration/client-configuration-recommendations
