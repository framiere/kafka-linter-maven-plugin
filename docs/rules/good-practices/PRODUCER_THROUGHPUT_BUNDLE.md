# PRODUCER_THROUGHPUT_BUNDLE

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: combination
**Tagline**: A producer with default batching settings is one-record-at-a-time pretending to be a stream.

## TL;DR

The linter flags producer configs where none of `linger.ms`,
`batch.size`, or `compression.type` are tuned away from defaults. The
defaults (`linger.ms=5` since Kafka 4.0, `batch.size=16384`, `compression.type=none`)
are conservative; any production workload moving more than a trickle
benefits from explicit values. Absence of *any* tuning is the signal —
not the individual value.

## The setup

Kafka producer batching has three controls:

- `linger.ms` — how long the sender waits to fill a batch before
  dispatching. Pre-4.0 default was `0` (no wait). 4.0+ default is `5`
  (KIP-1027). Either way, `5-20 ms` is the typical production sweet
  spot.
- `batch.size` — max bytes per partition batch. Default 16 KB. For
  bigger records or higher rates, 64-512 KB is common.
- `compression.type` — `none` by default. Production almost always
  benefits from `lz4`, `snappy`, or `zstd`.

A producer left at defaults sends most records individually,
uncompressed. On a high-rate topic that means CPU and network burned
on per-record overhead instead of doing work.

## What's actually happening (the mechanism)

The producer accumulator buffers records per `(topic, partition)`. The
sender thread dispatches a batch when either:

- The batch reaches `batch.size`, or
- `linger.ms` elapses since the first record entered the batch.

With `linger.ms=0` (pre-4.0), the sender wakes the instant a record
arrives. At low rates this means batches of one record. At high rates
it works out because the accumulator naturally fills between sender
ticks — but you're paying the per-batch protocol overhead on every
record.

With `linger.ms=10`:

- Low rate: 10 ms is added to publish latency, but batches still hold
  one or two records.
- High rate: 10 ms is enough to gather hundreds of records into one
  batch. Throughput climbs near-linearly with rate until `batch.size`
  caps it.

`compression.type=lz4` (or `zstd`) compresses the assembled batch
before send. A 200 KB batch of JSON compresses to ~20 KB. The CPU cost
is a few microseconds; the network and broker disk savings are an
order of magnitude. Broker stores the batch already compressed.

JMX metrics that move when this bundle is set:
`kafka.producer:type=producer-metrics,client-id=*:batch-size-avg` (up),
`record-send-rate` (up),
`compression-rate-avg` (drops below 1.0 when compression is on),
`request-rate` (down — same data in fewer requests).

## Operational impact

- **Cost.** Cross-AZ replication and downstream bandwidth fall
  proportionally to the compression ratio (2-10× for JSON, 1.5-2× for
  Avro/Protobuf which are already compact). Broker disk usage same.
- **Latency.** `linger.ms=10` adds up to 10 ms of producer-side latency
  per record (less on average — first record waits the full window, last
  records in a batch wait nothing). For latency-sensitive paths, see
  PRODUCER_LINGER_ZERO_NO_BATCH for the inverse warning.
- **Throughput.** 5-20× headroom on most JSON-heavy workloads moving
  from defaults to `linger.ms=20, batch.size=131072, compression.type=lz4`.
- **Operational toil.** Reduced. Fewer broker requests, smaller log
  segments per byte ingested.

## How to fix (no → yes)

```properties
# no — all defaults
bootstrap.servers=...
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer

# yes — conservative throughput tuning
linger.ms=10
batch.size=65536
compression.type=lz4
```

```properties
# yes — heavy-throughput pipeline (ETL, log ingestion)
linger.ms=20
batch.size=262144
compression.type=zstd
buffer.memory=67108864
```

Pair with the durability bundle (`PRODUCER_DURABILITY_BUNDLE`) — these
are orthogonal axes, set both.

Compression choice:

- `lz4` — fastest CPU, ~2× ratio on JSON. Default recommendation.
- `snappy` — historically common, slightly worse than lz4 on both
  metrics. Migrate to lz4.
- `zstd` — best ratio (~3× on JSON), 30-50% more CPU than lz4. Worth it
  on bandwidth-bound pipelines.
- `gzip` — don't. Slow, no advantage over zstd.

## When this might be a false positive

- Already-compressed payloads (JPEG, protobuf with packed encoding,
  precompressed binary blobs). Setting `compression.type` is wasted CPU.
- Real-time control plane producers where p99 latency under 5 ms is a
  product requirement — `linger.ms=0` is correct.
- Extremely low rate topics (< 1 record/sec) where batching never
  triggers anyway.

## Detection strategy

- **Config files & bytecode (combined):** for every producer config
  source, flag if NONE of `linger.ms`, `batch.size`, `compression.type`
  are set. This is the "no thinking applied" signal.
- Already-existing related rules (cross-link, don't duplicate):
  - [PRODUCER_LINGER_ZERO_NO_BATCH](../kafka-clients/PRODUCER_LINGER_ZERO_NO_BATCH.md) — linger=0 explicit
  - [PRODUCER_COMPRESSION_NONE_EXPLICIT](../kafka-clients/PRODUCER_COMPRESSION_NONE_EXPLICIT.md) — compression=none explicit
  - [PRODUCER_BUFFER_MEMORY_MISCONFIG](../kafka-clients/PRODUCER_BUFFER_MEMORY_MISCONFIG.md) — buffer.memory vs batch.size
- **Confidence: MEDIUM.** Defaults are valid for some workloads.

## References

- Apache Kafka producer configs — batching:
  https://kafka.apache.org/documentation/#producerconfigs_batch.size
- KIP-1027 — Change default `linger.ms` to 5 ms:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-1027
- Confluent — Optimizing Producers for Throughput:
  https://docs.confluent.io/platform/current/clients/producer.html#throughput
- Conduktor Config Advisor — producer high throughput profile:
  https://kafka-options-explorer.conduktor.io/config-advisor/
