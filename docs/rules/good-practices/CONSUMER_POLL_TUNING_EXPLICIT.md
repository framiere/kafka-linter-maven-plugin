# CONSUMER_POLL_TUNING_EXPLICIT

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: `max.poll.records` and `max.poll.interval.ms` should be deliberate, not whatever defaults you forgot were there.

## TL;DR

The linter flags consumer configs where neither `max.poll.records` nor
`max.poll.interval.ms` is set explicitly. The defaults (500 records, 5
minutes) work for some workloads and break others; absence of any
value usually means "we never measured." The good-practice form is to
set both based on measured processing time per record.

## The setup

`max.poll.records` (default 500) is the upper bound on records returned
per `poll()`. `max.poll.interval.ms` (default 300000) is the upper
bound on time between `poll()` calls before the consumer is considered
dead and its partitions are reassigned.

The constraint that ties them together:

> `max.poll.records × per-record-processing-time < max.poll.interval.ms`

If processing 500 records takes more than 5 minutes, the consumer is
evicted mid-batch, the offsets aren't committed, the partitions are
reassigned, the new owner reprocesses the same 500 records — and
takes another 5 minutes. This is the rebalance perpetual motion
machine. Cross-link the existing anti-pattern
[CONSUMER_MAX_POLL_INTERVAL_TOO_LOW](../kafka-clients/CONSUMER_MAX_POLL_INTERVAL_TOO_LOW.md).

The good-practice form is two-step:

1. Measure p99 processing time per record (via metrics or a profiling
   run).
2. Set `max.poll.records` to a value where
   `max.poll.records × p99 < 0.5 × max.poll.interval.ms` (50% headroom).

The linter cannot measure for you. What it can do is detect the absence
of any tuning combined with the absence of any
poll-latency metric — the proxy for "nobody is watching this
relationship."

## What's actually happening (the mechanism)

Each `poll()` returns up to `max.poll.records` records buffered from
fetches. The consumer thread processes them, then calls `poll()` again,
which also sends heartbeats and updates the group coordinator that the
consumer is alive.

If the processing loop exceeds `max.poll.interval.ms`, the consumer's
next `poll()` triggers `LeaveGroup` and a rebalance. The records
already processed but not yet committed are reprocessed by whoever gets
the partition next.

JMX to watch:
- `kafka.consumer:type=consumer-coordinator-metrics:last-rebalance-seconds-ago`
- `kafka.consumer:type=consumer-fetch-manager-metrics:records-consumed-rate`
- A custom timer around your processing loop. There is no built-in
  per-record processing latency metric — the team has to add one.

## Operational impact

- **Cost.** Reprocessing storms re-bill cross-AZ replication egress on
  the broker side.
- **Latency.** Spurious rebalances pause all consumers in the group for
  seconds — partition reassignment cost is global.
- **Throughput.** Capped to whatever `max.poll.records × poll loop time`
  produces. Wrong sizing leaves throughput on the floor or invites
  evictions.
- **Operational toil.** Mysterious "consumer keeps rebalancing" pages
  with no obvious trigger.

## How to fix (no → yes)

```properties
# no — defaults, no measurement
group.id=orders-svc

# yes — measured, with headroom
group.id=orders-svc
max.poll.records=200           # measured p99 = 200ms, batch = 40s
max.poll.interval.ms=120000    # 3× headroom over batch time
session.timeout.ms=45000
heartbeat.interval.ms=15000
```

Pair with a metric or log line that records actual batch processing
time. A 30-second batch on a 5-minute interval has 10× headroom — fine.
A 4-minute batch on a 5-minute interval is one DB slowdown from a
rebalance.

For high-latency external calls (DB writes, third-party APIs), prefer:

- Smaller `max.poll.records` (50-100) so partial-progress reprocessing
  costs are bounded.
- Larger `max.poll.interval.ms` (10-30 min) only if your team can
  justify the increased dead-consumer detection window.
- Or move the work onto an executor and use the
  pause/resume pattern instead of blocking the poll thread.

## When this might be a false positive

- Default values are fine for the workload (in-memory transformation,
  Streams without external I/O). Suppress on Streams configs — the
  Streams app handles this internally.
- Spring `@KafkaListener` with `concurrency`: Spring Boot's defaults
  are tuned, the linter should defer to them when no override exists.

## Detection strategy

- **Config files:** flag if both `max.poll.records` AND
  `max.poll.interval.ms` are absent AND the project does not declare a
  poll-latency metric (no Micrometer `Timer.builder("kafka.consumer.poll")`
  bytecode reference, no OpenTelemetry instrumentation).
- **Bytecode:** absence of a `Timer` or `Counter` around the
  `poll()` → process loop is a weak signal — many teams instrument
  externally. Treat as CONTEXT.
- **Confidence: CONTEXT.** This is a coverage rule, not a correctness
  rule. Default to INFO-tier print-only.

## References

- Apache Kafka consumer configs — `max.poll.records`:
  https://kafka.apache.org/documentation/#consumerconfigs_max.poll.records
- Apache Kafka consumer configs — `max.poll.interval.ms`:
  https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
- Confluent — Tuning Kafka consumers for throughput and latency:
  https://docs.confluent.io/platform/current/clients/consumer.html#message-handling
- Conduktor Config Advisor — consumer high-throughput profile:
  https://kafka-options-explorer.conduktor.io/config-advisor/
