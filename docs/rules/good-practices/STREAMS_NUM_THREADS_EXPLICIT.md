# STREAMS_NUM_THREADS_EXPLICIT

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `num.stream.threads` should be a sized choice, not a forgotten default.

## TL;DR

The good-practice form of
[STREAMS_NUM_THREADS_ONE](../kafka-streams/STREAMS_NUM_THREADS_ONE.md).
A production Streams app should set `num.stream.threads` to
`min(cores_available, max_input_partitions)`, not ship with the default
of `1`.

## Cross-reference

See [STREAMS_NUM_THREADS_ONE](../kafka-streams/STREAMS_NUM_THREADS_ONE.md)
for the mechanism, JMX (`alive-stream-threads`), and detection.

## What the good-practice adds

- The good-practice is to set the value **explicitly** as a deliberate
  capacity decision, not to rely on environment-derived auto-tuning. The
  number lives in source control, code review notices it, and capacity
  planning has a single line to read.
- Pair with the
  [STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API](../observability/STREAMS_UNCAUGHT_HANDLER_DEPRECATED_API.md)
  practice: a `StreamsUncaughtExceptionHandler` returning
  `REPLACE_THREAD` means a transient failure in one thread doesn't kill
  the whole instance — the value of having multiple threads is now
  realized.

```properties
# yes — explicit, capacity-planned
num.stream.threads=4
```

## References

See the linked anti-pattern doc and KIP-671.
