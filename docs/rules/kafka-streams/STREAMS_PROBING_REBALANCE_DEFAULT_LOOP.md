# STREAMS_PROBING_REBALANCE_DEFAULT_LOOP
**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: combination (config-file + topology analysis)
**Tagline**: Stateful app with standbys that never catch up → 10-minute rebalance loop forever.
**Source**: Confluent agent-skills — kafka-streams-programming/references/debugging.md § 10-Minute Rebalance Loop (Probing Rebalance).

## TL;DR

The linter flags Kafka Streams applications that combine `num.standby.replicas >= 1` (or `max.warmup.replicas >= 1`) with `acceptable.recovery.lag` left at the default AND `probing.rebalance.interval.ms` left at the default 600000ms (10 min). On large stateful topologies whose standbys never fully catch up, the probing rebalance fires every 10 minutes forever — visible on dashboards as a perfectly periodic spike in `rebalance-rate-per-hour`.

## What's happening (the mechanism)

`probing.rebalance.interval.ms` (default 600000ms) tells Streams: "Every 10 minutes, do a probing rebalance to see if any standby has caught up enough to take over from an active task and improve the assignment." It's intended to be self-healing — standbys catch up, an active task moves to a better-placed instance, the assignment improves, no more probing needed.

The pathological case from Confluent's debugging guide:

> When standby replicas never fully catch up, the probing rebalance fires to redistribute tasks, but nothing changes — the same pattern repeats.

Symptoms:
1. App stabilizes, runs for exactly 10 minutes, rebalances again. Repeats indefinitely.
2. `rebalance-rate-per-hour` is exactly 6 (one every 10 min).
3. The rebalance doesn't move tasks (no improvement is possible because standbys are still behind).
4. Each rebalance pauses processing for a few seconds — bounded but visible.

Confluent's fix is two-fold:
- Raise `acceptable.recovery.lag` so a standby is considered "caught up" sooner.
- Or raise `probing.rebalance.interval.ms` to 86400000 (24h) if probing isn't actually needed.
- Or fix the root cause — changelog topic compaction is behind and the standby has more bytes to replay than it can sustain.

## Operational impact

- Every 10 minutes, downstream consumers see a brief lag spike that exactly correlates with rebalance.
- Customer-facing dashboards show "flickering" data — usually misdiagnosed as a backend bug.
- The KS instance's `process-rate` drops to 0 during the rebalance window then recovers.
- The whole pattern is invisible without dashboards correlated to `rebalance-rate-per-hour`.

## How to fix (bad → good code)

```properties
# BAD — stateful app with default probing interval and warm-up settings
num.standby.replicas=1
# (probing.rebalance.interval.ms left at 600000ms default)
# (acceptable.recovery.lag left at 10000 default)

# GOOD — tune probing for the workload
num.standby.replicas=1
# Either raise the probing interval if the assignment doesn't benefit from probing:
probing.rebalance.interval.ms=86400000   # 24h
# Or raise the recovery lag so standbys are considered ready sooner:
acceptable.recovery.lag=100000
```

If standby replicas are not needed at all, removing them is often the right answer (`num.standby.replicas=0`) — and on KIP-1071 (`group.protocol=streams`) standby replicas aren't even supported (see `STREAMS_GROUP_PROTOCOL_STREAMS_INCOMPAT`).

## When this might be a false positive

- App is stateless or has small state — standbys catch up easily, probing finishes its work.
- `num.standby.replicas` is explicitly `0`. Acceptable, but the lint should not fire.
- Operator has already tuned `acceptable.recovery.lag` to a workload-appropriate value.

## Detection strategy

- Config: `num.standby.replicas >= 1` (or unset on classic protocol with a stateful topology) AND `probing.rebalance.interval.ms` either unset or set to default 600000 AND `acceptable.recovery.lag` either unset or at default 10000.
- Topology heuristic: stateful (at least one `Materialized` / `groupByKey` / `windowedBy` operator).
- Confidence: CONTEXT — the lint can detect the configuration but not whether standbys actually fail to catch up. Default to print-only with the diagnostic note.

## References

- Confluent agent-skills — kafka-streams-programming/references/debugging.md § 10-Minute Rebalance Loop (Probing Rebalance)
- Apache Kafka docs — `probing.rebalance.interval.ms`: https://kafka.apache.org/documentation/#streamsconfigs_probing.rebalance.interval.ms
- KIP-441 — Smooth scaling out for Kafka Streams: https://cwiki.apache.org/confluence/display/KAFKA/KIP-441
