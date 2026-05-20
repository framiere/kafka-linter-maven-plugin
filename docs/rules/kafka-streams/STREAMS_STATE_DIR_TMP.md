# STREAMS_STATE_DIR_TMP

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: RocksDB on /tmp is state-by-coincidence.

## TL;DR

The linter flags `state.dir` pointing under `/tmp` (or unset, since the default is `${java.io.tmpdir}/kafka-streams` which on Linux is `/tmp/kafka-streams`). Reboots, tmpfs eviction, and systemd `PrivateTmp` will wipe your state.

## What's happening (the mechanism)

`state.dir` is where each Streams instance keeps its local RocksDB stores. After a restart, Streams checks the local state and replays only the *delta* missing from the changelog topic. If the directory is gone, every store does a full restore from offset 0 of every changelog partition assigned to this instance.

`/tmp` is hostile to that contract:
- On Linux it's commonly `tmpfs` — gone on reboot.
- `systemd-tmpfiles` may delete unaccessed files after a configurable age (often 10 days).
- Containers running with `--tmpfs /tmp` or `tmpfs` volumes lose state on every restart.
- `PrivateTmp=true` in systemd units creates a per-service /tmp that's wiped at unit restart.

The default value is `${java.io.tmpdir}/kafka-streams` which is `/tmp/kafka-streams` on almost every Linux JVM — *not* a real location for production state.

## Operational impact

- Every restart triggers a full state restore. `kafka.streams:type=stream-task-metrics,task-id=...:restore-records-rate` saturates, restoration time grows with changelog size — minutes to hours for large stores.
- During restore, the task is not `RUNNING`, so interactive queries return `InvalidStateStoreException` and the topology emits nothing.
- Aggregations look correct *after* restoration completes; users see "wrong" values during the restore window.
- Multiple Streams instances per node may race for `/tmp/kafka-streams/<app.id>` lock files.
- Disk usage on `/tmp` (or tmpfs RAM!) grows with state size — pages OOM-killer.

## How to fix

```properties
# BAD — default
# state.dir=  (defaults to /tmp/kafka-streams)

# BAD — explicit
state.dir=/tmp/kafka-streams

# GOOD — persistent volume
state.dir=/var/lib/kafka-streams

# GOOD — Kubernetes PVC mount
state.dir=/data/kafka-streams
```

```java
// GOOD
props.put(StreamsConfig.STATE_DIR_CONFIG, "/var/lib/kafka-streams/" + appId);
```

In Kubernetes, mount a `PersistentVolumeClaim` at this path; pair with `StatefulSet` so the pod re-attaches to the same volume after rescheduling.

## When this might be a false positive

- `TopologyTestDriver` and local dev tests (no real state needed). Suppress per-test-classpath.
- Pure stateless topologies (no `Materialized`, no joins, no windows) — but then `state.dir` is barely used; still cleaner to point it at a real location.

## Detection strategy

- Config files: `state.dir` value starts with `/tmp/` or equals `/tmp`. Also flag absent (default).
- Bytecode: `Properties.put("state.dir", LDC "/tmp/...")` or `GETSTATIC StreamsConfig.STATE_DIR_CONFIG` paired with a tmp-pattern LDC.
- Special-case: paths starting with `${java.io.tmpdir}` (templating in yaml).
- Confidence: HIGH for explicit `/tmp`. MEDIUM for absent (default may still be a packaging mistake).

## References

- Streams config — state.dir: https://kafka.apache.org/documentation/streams/developer-guide/config-streams.html#state-dir
- systemd-tmpfiles man page: https://www.freedesktop.org/software/systemd/man/systemd-tmpfiles.html

## Cross-reference

Also recommended by Confluent agent-skills — see kafka-streams-programming/references/config-baseline.md § Persistence and state directory, kafka-streams-programming/references/production-hardening.md § State directory placement.
