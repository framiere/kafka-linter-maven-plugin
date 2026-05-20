# STREAMS_COMMIT_INTERVAL_TOO_LOW

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Committing every millisecond means the broker commits a million times a millisecond.

## TL;DR

The linter flags `commit.interval.ms` set unrealistically low (typically `< 100`). The default is 30000ms for ALOS and 100ms for EOS-v2; values below 100ms cause runaway broker CPU on `__consumer_offsets` and `__transaction_state`.

## What's happening (the mechanism)

`commit.interval.ms` is how often each StreamThread flushes its state (RocksDB + cache) and commits its consumer offsets / transaction. The defaults are calibrated against broker capacity:

- ALOS (`at_least_once`): 30000 ms — coarse commits, low broker overhead.
- EOS-v2: 100 ms — fine commits because each commit is a transaction; lowering this raises the transaction churn rate.

People set `commit.interval.ms=1` or `=10` thinking it improves "real-time-ness" or "exactly-once-ness". It does neither. It triggers a flush + commit on every poll loop, which:
- Drives `__consumer_offsets` writes to thousands per second per instance.
- Under EOS-v2, multiplies transactional state churn — every commit is a `BeginTransaction` + `EndTransaction` write.
- Effectively defeats the in-memory cache (it flushes on every commit), so the cache benefits documented in `STREAMS_CACHE_DISABLED` are nullified.

## Operational impact

- Broker CPU pegged. `kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs` and request-queue-time grow.
- `__consumer_offsets` and `__transaction_state` topic write rate is far above input rate; replication backlog.
- `kafka.streams:type=stream-task-metrics,...:commit-rate` is 1000s/sec instead of dozens.
- Higher tail latency in processing because commit overhead dominates.
- For EOS-v2, `kafka.streams:type=stream-thread-metrics,...:transaction-time-avg` skyrockets.

## How to fix

```properties
# BAD
commit.interval.ms=1
commit.interval.ms=10

# GOOD — defaults are fine
# (omit, or set explicitly)
commit.interval.ms=30000        # ALOS default
commit.interval.ms=100          # EOS-v2 default
```

If you need lower end-to-end latency, tune `cache.max.bytes` smaller or use `Suppressed` to emit on demand — don't crank the commit interval.

## When this might be a false positive

- Microbenchmark / test code that needs deterministic per-record commits.
- Niche low-volume topologies where commit overhead is negligible. Suppress per-module.

## Detection strategy

- Config files: `commit.interval.ms` value < 100.
- Bytecode: `Properties.put(...COMMIT_INTERVAL_MS_CONFIG..., LDC <value>)` where value is statically resolvable to < 100.
- Confidence: MEDIUM. Threshold tunable.

## Consult a friend?

> 🤝 **Slow down.** Raising `commit.interval.ms` back to defaults is the right move, but the team probably set it low to fix a *real* latency problem — bumping it without addressing the underlying cause just hides the symptom on the broker side.
> - Why was the interval lowered? "Downstream wasn't seeing records fast enough" is usually a cache problem, not a commit problem — `cache.max.bytes` defaults to ~10MB per thread, and the cache only flushes on commit *or* on overflow. Lowering the cache (or using `Suppressed.untilWindowCloses(...)`) is the right knob.
> - Under EOS, each commit is a transaction commit — broker load is `txn_rate × num_partitions`. Have you checked `__transaction_state` write rate after the bump? It should drop by orders of magnitude; if it doesn't, something else is committing more than it should.
> - The cache benefits (`STREAMS_CACHE_DISABLED` rule) are only real with a sane commit interval. After fixing this, re-check that cache hit rate is non-trivial — if it's still near zero, the commit interval wasn't the only thing wrong.

## References

- Streams config — commit.interval.ms: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#commit-interval-ms
- KIP-447 — Producer scalability for exactly once: https://cwiki.apache.org/confluence/display/KAFKA/KIP-447:+Producer+scalability+for+exactly+once+semantics

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/config-baseline.md § EOS v2 invariants, kafka-streams-programming/references/debugging.md § Excessive commits.
