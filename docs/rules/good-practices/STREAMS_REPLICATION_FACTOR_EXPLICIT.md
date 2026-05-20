# STREAMS_REPLICATION_FACTOR_EXPLICIT

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Replication factor of 3 (or broker default of -1) is the floor for a Streams app worth restarting.

## TL;DR

The good-practice form of
[STREAMS_REPLICATION_FACTOR_ONE](../kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md).
A production Streams app should either omit `replication.factor` (so
the broker default applies — `-1` since KIP-464 in 2.4) or set it
explicitly to `3` (or whatever matches `min.insync.replicas + 1`). The
anti-pattern flags `=1`; the good-practice doc reminds you to think
about it.

## Cross-reference

See [STREAMS_REPLICATION_FACTOR_ONE](../kafka-streams/STREAMS_REPLICATION_FACTOR_ONE.md)
for the mechanism. KIP-464.

## What the good-practice adds

- Prefer omission over hard-coding. Since Kafka 2.4 the default is `-1`
  ("use broker default"), and the broker is the right place to decide.
- If you set it explicitly, make it match the broker's RF for its
  default topics — usually `3` in a multi-AZ deployment. A mismatch
  (Streams creates RF=2 changelogs on a broker that uses RF=3 for
  everything else) makes capacity planning weird.
- Tie `min.insync.replicas` to the same value at the topic level (RF=3
  + min.ISR=2 is the durable standard).

```properties
# yes — defer to broker (preferred)
# (no replication.factor key at all)

# yes — explicit, matches broker default
replication.factor=3
```

## References

See the linked anti-pattern doc, KIP-464, and the Streams operational
guide.
