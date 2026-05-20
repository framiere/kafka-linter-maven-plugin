# WarpStream-Specific Anti-Patterns

This catalog covers anti-patterns for Kafka clients targeting WarpStream — a Kafka-protocol-compatible backend where stateless Agents write directly to object storage (e.g., S3) instead of broker-local disks. WarpStream's performance profile is fundamentally different from Apache Kafka: produce latency is ~250ms p50 instead of single-digit ms, several Kafka configs are inert (`replication.factor`, `min.insync.replicas`), and some are outright unsupported (`fetch.min.bytes`).

Rules here apply only when the target environment is WarpStream. Detection signals: bootstrap servers pointing at a WarpStream cluster, the `ws_az=` / `ws_sle=` / `ws_dfat=` markers in `client.id`, or an explicit project-level configuration switch (e.g., a `warpstream.profile=true` flag, environment selector in CI). On non-WarpStream deployments, these rules degrade to CONTEXT or are skipped.

## Legend

**Severity**
- `WARNING` — suboptimal — works but 10-20x slower or more expensive than necessary.

**Confidence**
- `HIGH` — mechanical detection of the misconfig in a known-WarpStream context.
- `MEDIUM` — heuristic — needs project-level signals to confirm WarpStream targeting.
- `CONTEXT` — the rule fires only when WarpStream is selected as the target; print-only by default.

**Detection** — `config-file` (plus `pom-dependency` for one rule), or a combination (` + `, alphabetical).

A trailing 🤝 in the tagline means the rule's doc carries a `## Consult a friend?` block — read it before changing prod config.

## Catalog (4)

### WarpStream client overrides

| Rule | Severity | Confidence | Detection | Tagline |
|---|---|---|---|---|
| [WARPSTREAM_IDEMPOTENCE_ENABLED](./WARPSTREAM_IDEMPOTENCE_ENABLED.md) | WARNING | CONTEXT | config-file | `enable.idempotence=true` against ~250ms object-storage latency = 5 in-flight × 4 round-trips/sec = 20 RPS. 🤝 |
| [WARPSTREAM_BATCH_AND_LINGER_DEFAULTS](./WARPSTREAM_BATCH_AND_LINGER_DEFAULTS.md) | WARNING | CONTEXT | config-file | Default `batch.size=16384` + `linger.ms=0` against object storage = one S3 PUT per record. 🤝 |
| [WARPSTREAM_FETCH_MIN_BYTES_SET](./WARPSTREAM_FETCH_MIN_BYTES_SET.md) | WARNING | HIGH | config-file | `fetch.min.bytes` is silently ignored on WarpStream — use `fetch.max.wait.ms` to get the batching you wanted. |
| [WARPSTREAM_CLIENT_ID_NO_AZ](./WARPSTREAM_CLIENT_ID_NO_AZ.md) | WARNING | CONTEXT | config-file | No `ws_az=<az>` in `client.id` — every byte routed cross-AZ is $0.05/GB, paid silently in your AWS bill. |

## Cross-cutting themes

- **WarpStream needs larger batches**: object storage has high per-request cost. Default Kafka client batches (16 KB) become S3 PUTs at single-record granularity. The Confluent guidance is `batch.size=100000` and `linger.ms=100`.
- **WarpStream rejects some Kafka tunables silently**: `replication.factor` returns `3` regardless of input, `min.insync.replicas` always `1`, `fetch.min.bytes` quietly ignored. The lint flags the ones that matter (currently `fetch.min.bytes` — the others are cosmetic, not bugs).
- **Cross-AZ traffic is the silent budget killer**: without `ws_az=` in `client.id`, WarpStream may route a producer to an Agent in another AZ, and the cross-AZ NAT charge is invisible until the AWS invoice arrives.
- **EOS works but costs**: `processing.guarantee=exactly_once_v2` forces `enable.idempotence=true` which caps in-flight requests to 5. On WarpStream, with ~250ms produce latency, that's 20 requests/sec of throughput. The Confluent guide explicitly recommends `at_least_once` with downstream deduplication unless EOS is required.

## References

- Confluent agent-skills — `kafka-streams-programming/references/warpstream-optimization.md`
- WarpStream configuration recommendations: <https://docs.warpstream.com/warpstream/reference/configuration/client-configuration-recommendations>
