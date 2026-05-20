# STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT
**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: 10s `transaction.timeout.ms` under EOS is a state-wipe cascade waiting for the first slow record.
**Source**: Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § EOS Configuration, kafka-streams-programming/references/debugging.md § Transaction timeout cascade.

## TL;DR

The linter flags Kafka Streams configs with `processing.guarantee=exactly_once_v2` that leave `transaction.timeout.ms` at its 10000ms default. The skill calls out 60000ms (60s) as the recommended starting point and 300000ms (5min) for apps doing slow external lookups. The 10s default exists for non-Streams transactional producers; for a Streams app, it's just enough rope for a single GC pause or a slow downstream broker to abort the transaction, fence the producer, and trigger a state-wipe + restoration cascade.

## What's happening (the mechanism)

Under `exactly_once_v2`, every batch of records is bundled into a Kafka transaction. The producer sends `BeginTxn`, writes output records, sends `CommitTxn`, all within `transaction.timeout.ms` (default 10s, broker-side ceiling `transaction.max.timeout.ms`).

If processing one batch exceeds the timeout — slow REST call, long RocksDB compaction, broker leader change, even a long GC pause — the transaction coordinator aborts the transaction and fences the producer. From Confluent's debugging guide:

> Processing or state restoration takes longer than `transaction.timeout.ms` (default 10s). The transaction coordinator aborts the transaction, the producer is fenced, a rebalance is triggered, state must be restored, and the cycle repeats.

The cascade compounds: fencing → rebalance → state restoration → restoration itself takes longer than the timeout if state is large → cycle. The skill's debugging guide explicitly names this as "Transaction timeout cascade" with `Completed rollback of ongoing transaction ... due to timeout` as the broker-side fingerprint.

The recommended values from `config-baseline.md` § EOS Configuration:

| Workload shape | `transaction.timeout.ms` |
|---|---|
| Light stateful, fast lookups | 60000 (60s) — starting point |
| Slow external lookups, larger state | 300000 (5 min) |
| Extreme processing time, very large state | 900000 (15 min) |

## Operational impact

- Broker logs: `Completed rollback of ongoing transaction for transactionalId <app-id>-<uuid>-<thread> due to timeout`.
- Client logs: `InvalidProducerEpochException`, `Detected that the thread is being fenced`.
- Each cycle wipes local state and re-restores from changelog — for large stores, this is 30-90 minutes per cycle. A multi-hour outage from one slow record.
- Once in the cascade, the loop is self-sustaining until either the timeout is raised, the workload changes, or the user manually escalates to `at_least_once` (see Confluent's "Emergency procedure" in debugging.md).

## How to fix (bad → good code)

```properties
# BAD — EOS with default 10s timeout
processing.guarantee=exactly_once_v2
# (no transaction.timeout.ms — falls back to 10s default)

# GOOD — 60s starting point per Confluent skill
processing.guarantee=exactly_once_v2
transaction.timeout.ms=60000

# Also align poll interval with transaction timeout — see STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT
consumer.max.poll.interval.ms=600000
```

Broker-side, the cluster operator must ensure `transaction.max.timeout.ms` (default 15 min on AK) is at least the client's `transaction.timeout.ms`. On Confluent Cloud this is typically already correct.

## Consult a friend?

Yes. EOS troubleshooting requires understanding the transactional producer state machine. If the team has not shipped EOS before, walk through KIP-447 and the EOS checklist in `config-baseline.md` together — particularly the eight-item checklist that ends with "consumer.max.poll.interval.ms >= transaction.timeout.ms".

## When this might be a false positive

- Apps with `processing.guarantee=at_least_once` (default) — this rule should not fire.
- A test config where the default is fine — the user is running `TopologyTestDriver` and no broker is involved.

## Detection strategy

- Config: `processing.guarantee` equals `exactly_once_v2` AND `transaction.timeout.ms` is unset OR set to a value below 30000.
- Confidence: MEDIUM. The 10s default is a footgun, but the "correct" value depends on the workload — 60s is a starting recommendation, not a hard cutoff.

## References

- Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § EOS Configuration
- Confluent agent-skills — kafka-streams-programming/references/debugging.md § Transaction timeout cascade
- KIP-447: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447
- Apache Kafka docs — `transaction.timeout.ms`: https://kafka.apache.org/documentation/#producerconfigs_transaction.timeout.ms
- Related rules: [STREAMS_EOS_V2_ENABLED](../good-practices/STREAMS_EOS_V2_ENABLED.md), [STREAMS_EOS_COMMIT_INTERVAL_SET](STREAMS_EOS_COMMIT_INTERVAL_SET.md)
