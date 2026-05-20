# STREAMS_EOS_COMMIT_INTERVAL_SET
**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Setting `commit.interval.ms` under EOS overrides a correctness-critical default.
**Source**: Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § EOS Configuration ("Properties to NOT Set").

## TL;DR

The linter flags Kafka Streams configs that combine `processing.guarantee=exactly_once_v2` with any explicit value of `commit.interval.ms`. Under EOS v2, Kafka Streams hard-codes the commit interval to 100ms internally because the transaction boundary depends on it. Overriding the value either widens the transaction (more state held open, larger blast radius on crash) or has no effect — both are bug-shaped.

## What's happening (the mechanism)

In `at_least_once` mode, `commit.interval.ms` defaults to 30000ms (30s) — a throughput tradeoff. Larger intervals = fewer offset commits = better throughput, but more re-processing on crash.

EOS v2 changes the contract. Every commit interval is also a transaction boundary: all output records, state-store updates, and consumer offset commits in that interval are bundled into one Kafka transaction. The transaction is committed atomically or aborted atomically. Confluent's skill says:

> Do NOT set commit.interval.ms for EOS apps. EOS overrides this to 100ms internally for correctness. OMIT this line entirely when using exactly_once_v2.

If a user sets `commit.interval.ms=30000` under EOS, they are asking for a 30-second transaction window. Downstream `read_committed` consumers see no records until each transaction commits, so end-to-end latency jumps from ~100ms to ~30s. On crash, the entire 30s of work is rolled back and replayed. The cache also holds 30s of state-store updates in memory.

(Internally, some Streams versions still respect a user-provided value — others overwrite it. The skill's instruction is "omit the line entirely" precisely because the behavior is version-dependent and surprising either way.)

## Operational impact

- End-to-end latency goes from ~100-200ms to whatever the user-set value is (often 30s — the at-least-once default copied without thinking).
- Transaction rollback on crash discards a larger window of work → longer recovery, more downstream `read_committed` stall.
- `commit-rate` JMX metric is dramatically lower than expected — the operator's "commits are happening every 100ms" mental model is wrong.

## How to fix (bad → good code)

```properties
# BAD — at_least_once default copied into EOS config
processing.guarantee=exactly_once_v2
commit.interval.ms=30000

# GOOD — omit commit.interval.ms; EOS uses 100ms internally
processing.guarantee=exactly_once_v2
# (no commit.interval.ms)
transaction.timeout.ms=60000      # tune this instead, if needed
```

If you genuinely need a different EOS commit interval (rare), consult the team — see Consult below.

## Consult a friend?

Yes. EOS commit intervals are one of the rare cases where "tune for throughput" is actively wrong. Cross-link the EOS checklist in `config-baseline.md` and `STREAMS_EOS_V2_ENABLED` (good-practices) before raising the interval.

## When this might be a false positive

- The user has set `commit.interval.ms=100` explicitly to match the EOS default — harmless, but the lint message still applies: the line is noise.
- Pre-EOS v2 (`exactly_once`, `exactly_once_beta`) — already flagged by `STREAMS_EOS_V1_DEPRECATED`; this rule shouldn't double-fire.

## Detection strategy

- Config: `processing.guarantee` equals `exactly_once_v2` (or `exactly_once` / `exactly_once_beta`, but those are caught by the v1-deprecated rule), AND `commit.interval.ms` is set to any value.
- Confidence: HIGH. The combination is unambiguous; the recommendation is in Confluent's own EOS checklist.

## References

- Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § EOS Configuration (Properties to NOT Set)
- Apache Kafka docs — `processing.guarantee`: https://kafka.apache.org/documentation/#streamsconfigs_processing.guarantee
- KIP-447 — Producer scalability for EOS: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447
- Related rules: [STREAMS_EOS_V1_DEPRECATED](STREAMS_EOS_V1_DEPRECATED.md), [STREAMS_EOS_V2_ENABLED](../good-practices/STREAMS_EOS_V2_ENABLED.md)
