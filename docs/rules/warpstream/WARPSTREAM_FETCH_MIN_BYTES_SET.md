# WARPSTREAM_FETCH_MIN_BYTES_SET
**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: `fetch.min.bytes` is silently ignored on WarpStream — use `fetch.max.wait.ms` to get the batching you wanted.
**Source**: Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Things That Don't Apply on WarpStream, § Consumer.

## TL;DR

The linter flags consumer configurations that set `fetch.min.bytes` or `consumer.fetch.min.bytes` when the target is WarpStream. WarpStream does not implement this Kafka protocol option — it accepts the request and ignores the value. Code written assuming "the consumer will only return when X bytes are available" gets back a stream of small fetches instead, with no warning. The correct knob on WarpStream is `fetch.max.wait.ms` (and large `fetch.max.bytes` / `max.partition.fetch.bytes`).

## What's happening (the mechanism)

On Apache Kafka, `fetch.min.bytes` (default `1`) tells the broker: "don't reply to a Fetch request until at least N bytes are available, or `fetch.max.wait.ms` has elapsed." Setting `fetch.min.bytes=1048576` is a common throughput tuning — it batches consumer reads.

On WarpStream, this option is in the list of unsupported configs (per the "Things That Don't Apply" table in the optimization guide). The Agent receives the option, ignores it, and replies as soon as it has anything for the consumer. The effect:

- Consumer's intent — "I want at least 1 MB or wait 10 sec" — collapses to "give me whatever you have right now".
- Consumer CPU goes up (more Fetch calls, more record-iterations).
- Consumer throughput drops compared to the tuned target.
- No log line, no error — the misconfig is invisible.

The Confluent guide prescribes the alternative: set `fetch.max.wait.ms=10000` (10 seconds) and large `fetch.max.bytes` (50 MB) / `max.partition.fetch.bytes` (50 MB). The Agent will then batch fetches up to those byte ceilings and reply after at most 10 seconds.

## Operational impact

- Consumer side: more Fetch calls means more S3 GET round-trips on the Agent side, raising cost and latency.
- The intent of the config is lost. The lint surfaces a property the dev believed was doing something — and is not.
- Throughput regression is silent — only visible in metrics (high `records-consumed-rate` variance, low average batch size).

## How to fix (bad → good code)

```properties
# BAD on WarpStream
fetch.min.bytes=1048576
fetch.max.wait.ms=500

# GOOD on WarpStream — use fetch.max.wait.ms + large fetch ceilings
# (do NOT set fetch.min.bytes at all)
fetch.max.bytes=50242880
max.partition.fetch.bytes=50242880
fetch.max.wait.ms=10000
```

For Kafka Streams (consumer-prefix overrides):

```properties
consumer.fetch.max.bytes=50242880
consumer.max.partition.fetch.bytes=50242880
consumer.fetch.max.wait.ms=10000
# Do NOT set consumer.fetch.min.bytes
```

## When this might be a false positive

- The target is not WarpStream — the property is correct on Apache Kafka, MSK, Confluent Cloud, Redpanda.
- A config file is shared between WarpStream and non-WarpStream targets, with `fetch.min.bytes` set for the non-WarpStream case only. The lint should print, not block, in that case.

## Detection strategy

- WarpStream context signal (see `WARPSTREAM_IDEMPOTENCE_ENABLED` for detection criteria).
- Config-file: any property file or YAML containing `fetch.min.bytes=` or `consumer.fetch.min.bytes=` (with a non-default value, i.e., not `1`).
- Confidence: HIGH — the unsupported-config list is explicit and exhaustive in the Confluent guide.

## References

- Confluent agent-skills — kafka-streams-programming/references/warpstream-optimization.md § Things That Don't Apply on WarpStream, § Consumer
- WarpStream docs — Client configuration recommendations: https://docs.warpstream.com/warpstream/reference/configuration/client-configuration-recommendations
