# WARPSTREAM_IDEMPOTENCE_ENABLED
**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: `enable.idempotence=true` against ~250ms object-storage latency = 5 in-flight × 4 round-trips/sec = 20 RPS.
**Source**: Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Idempotent Producers and EOS.

## TL;DR

When the target is WarpStream, the linter flags `enable.idempotence=true` or `processing.guarantee=exactly_once_v2` configurations. WarpStream's produce path goes through object storage (S3 / GCS / equivalent) with ~250ms p50 latency. The Apache Kafka idempotent-producer protocol caps `max.in.flight.requests.per.connection` to 5. Multiplying 5 in-flight × ~4 round-trips/sec, each connection delivers ~20 RPS — a 10-20x throughput regression versus the WarpStream-tuned defaults.

## What's happening (the mechanism)

Idempotent producers exist to prevent duplicate writes during retries. The protocol assigns each (producer-id, partition) tuple a strictly-monotonic sequence number. To keep the bookkeeping bounded, the broker enforces `max.in.flight.requests.per.connection <= 5`.

On Apache Kafka, this isn't a problem: typical produce latency is 5-15ms, so 5 in-flight requests sustain hundreds of records/sec/connection easily.

On WarpStream:
- Agents are stateless. A produce request batches a payload, then writes it to object storage. S3 PUT p50 is ~200-250ms.
- 5 in-flight × ~250ms = 5 / 0.25 = 20 produce calls/sec per connection.
- Multiply by batch size, and the absolute throughput is workable for low-volume topics — but it's 10-20x lower than the throughput-optimized default of `max.in.flight.requests.per.connection=1000`.

Additionally, the Confluent guide notes that idempotent producers on WarpStream see retriable `KAFKA_STORAGE_ERROR` errors more often, because partition ownership shifts between Agents frequently (Agents are stateless and any Agent can serve any partition — the "owner" is a transient assignment).

For Kafka Streams: `processing.guarantee=exactly_once_v2` internally turns on `enable.idempotence=true` for the embedded producer. The Confluent recommendation is to prefer `at_least_once` with downstream deduplication, unless EOS is a hard requirement.

## Operational impact

- 10-20x lower per-connection throughput. For a topic that processes 10k records/sec, the upstream producer needs proportionally more parallelism (more partitions, more producer instances).
- Increased `KAFKA_STORAGE_ERROR` retries inflate latency tail.
- Cost: object-storage backends bill per request. Smaller-but-more-frequent batches (forced by the in-flight cap interacting with `linger.ms`) raise the S3 PUT count and therefore the monthly storage bill.

## How to fix (bad → good code)

```properties
# BAD on WarpStream (works, but slow and expensive)
enable.idempotence=true
processing.guarantee=exactly_once_v2

# GOOD for throughput on WarpStream
enable.idempotence=false
max.in.flight.requests.per.connection=1000
processing.guarantee=at_least_once
# (consider downstream deduplication if your business logic needs it)
```

If EOS is a hard requirement (e.g., the downstream sink doesn't tolerate duplicates and idempotent writes aren't possible), keep `enable.idempotence=true` and accept the throughput cost. Plan capacity accordingly.

## When this might be a false positive

- The target is **not** WarpStream — the rule is irrelevant on Apache Kafka, MSK, Confluent Cloud, Redpanda. Detect via bootstrap-server hostname (`*.warpstream.com`, `*.serverless.warpstream.com`), explicit `warpstream.profile=true`, or `ws_az=` / `ws_sle=` markers in `client.id`.
- The application processes <1k records/sec and the throughput hit is irrelevant. Print-only in that case.
- The data contract requires EOS — the dev is making a conscious tradeoff. Lint should still print so reviewers see it.

## Detection strategy

- Project-level context check first: is the target WarpStream? Signals (any of):
  - Bootstrap-server hostname contains `warpstream` or `serverless.warpstream`.
  - A project-level config flag (`warpstream.profile=true`, `target.platform=warpstream`).
  - `client.id` includes `ws_az=` / `ws_sle=` / `ws_dfat=` markers.
- Then config-file check: any of:
  - `enable.idempotence=true`
  - `processing.guarantee=exactly_once_v2` (or `exactly_once`)
- Confidence: CONTEXT — print-only by default; bump to WARNING if a WarpStream-target signal is explicit in the project config.

## References

- Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Idempotent Producers and EOS, § Quick Checklist
- WarpStream docs — Client configuration recommendations: https://docs.warpstream.com/warpstream/reference/configuration/client-configuration-recommendations
- Apache Kafka KIP-679: Idempotence default behavior
