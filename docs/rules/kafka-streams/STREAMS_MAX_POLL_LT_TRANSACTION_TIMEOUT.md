# STREAMS_MAX_POLL_LT_TRANSACTION_TIMEOUT
**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Under EOS, `max.poll.interval.ms` smaller than `transaction.timeout.ms` is a guaranteed cascade.
**Source**: Confluent agent-skills — kafka-streams-programming/references/debugging.md § Rebalancing Issues (Config relationship rules), kafka-streams-programming/references/config-baseline.md § EOS Checklist (#9).

## TL;DR

The linter flags Kafka Streams configs where `processing.guarantee=exactly_once_v2` AND `consumer.max.poll.interval.ms < transaction.timeout.ms` (or `max.poll.interval.ms` when set globally). The skill's EOS checklist names this as item #9, and the debugging guide states the invariant as a config relationship rule. Violating it guarantees that any single slow batch triggers a consumer eviction *before* the transaction can time out — producing exactly the cascade the longer transaction timeout was supposed to prevent.

## What's happening (the mechanism)

Under EOS, every poll-process-commit cycle is wrapped in a Kafka transaction:

```
poll() → process records → write to output topic → commitTransaction()
```

Two independent timeouts can interrupt this:

- `transaction.timeout.ms` — broker-side cap on how long a transaction can stay open. Default 10s; recommended 60s+ for Streams (see `STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT`).
- `max.poll.interval.ms` — client-side cap on time between two `poll()` calls before the consumer is evicted from the group. Default 5 min (300000ms).

If `max.poll.interval.ms < transaction.timeout.ms`, the consumer is kicked out of the group *before* the transaction can finish — leaving an open transaction that the broker then aborts on its own timer. Net effect: every batch that's slow enough to need the transaction timeout's full budget will trip the poll interval first.

Confluent's debugging guide names the invariant explicitly under "Rebalancing Issues":

> `request.timeout.ms` >= `max.poll.interval.ms`
> For EOS: `transaction.timeout.ms` <= `max.poll.interval.ms`

And the EOS checklist in `config-baseline.md`:

> 9. `consumer.max.poll.interval.ms` >= `transaction.timeout.ms`

## Operational impact

- Symptom: `Member consumer-... has exceeded the timeout` followed immediately by transaction abort and state restoration.
- Each slow batch triggers a rebalance — the rebalance triggers state restoration — restoration is the long step → the cycle repeats. Same shape as `STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT`, except this one is mechanical, not workload-dependent.
- Often shipped together: the user raises `transaction.timeout.ms` to 60s but forgets to bump `consumer.max.poll.interval.ms`, which still inherits the broker default. The lint catches the combination.

## How to fix (bad → good code)

```properties
# BAD — transaction.timeout > max.poll.interval
processing.guarantee=exactly_once_v2
transaction.timeout.ms=300000
consumer.max.poll.interval.ms=60000   # 60s < 300s — backwards

# GOOD — poll interval >= transaction timeout
processing.guarantee=exactly_once_v2
transaction.timeout.ms=300000
consumer.max.poll.interval.ms=600000  # 10 min — >= 5 min
consumer.request.timeout.ms=600000    # match for symmetry (Confluent baseline)
consumer.session.timeout.ms=45000
```

## When this might be a false positive

- `processing.guarantee=at_least_once` — the rule does not apply.
- `transaction.timeout.ms` left at default (10s) AND `max.poll.interval.ms` left at default (300000ms) — the invariant holds by coincidence. Acceptable, but ship both values explicitly under EOS for clarity.

## Detection strategy

- Config: `processing.guarantee=exactly_once_v2` AND `transaction.timeout.ms` numerically > `consumer.max.poll.interval.ms` (or > the global `max.poll.interval.ms` when present).
- Treat unset values as their documented defaults: `transaction.timeout.ms=10000`, `max.poll.interval.ms=300000`. Only flag when an explicit value violates the inequality.
- Confidence: HIGH. The relationship is mechanical and the invariant is in Confluent's own checklist.

## References

- Confluent agent-skills — kafka-streams-programming/references/debugging.md § Rebalancing Issues
- Confluent agent-skills — kafka-streams-programming/references/config-baseline.md § EOS Checklist (#9)
- KIP-447 — Producer scalability for exactly once semantics: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447
- Related rules: [STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT](STREAMS_EOS_TRANSACTION_TIMEOUT_DEFAULT.md), [STREAMS_EOS_V2_ENABLED](../good-practices/STREAMS_EOS_V2_ENABLED.md)
