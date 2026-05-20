# STREAMS_TOPOLOGY_OPTIMIZATION_DISABLED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Default is `none`. You're leaving free CPU and bandwidth on the table.

## TL;DR

The linter flags Streams configs with `topology.optimization=none` (the default) when the topology contains source-KTables and/or chained key-changers — both of which optimization can collapse, saving internal topics.

## What's happening (the mechanism)

KIP-295 introduced `topology.optimization`. The default is `none` for backward compatibility (so users upgrading from pre-2.0 don't accidentally end up with a different topology shape). Setting it to `all` enables:

- **Source-KTable optimization**: when `builder.table("foo")` is used, Streams normally creates a separate changelog topic for the KTable. With `all`, it reuses the source topic as the changelog — saves a topic and 2x the write traffic.
- **Repartition merging**: when multiple operators each repartition by the same key, the optimizer can collapse them into a single repartition topic.
- (KIP-862) Single-store self-join optimization for stream-stream self-joins.

The companion change in KIP-312 means you must also use `StreamsBuilder.build(Properties)` rather than `build()` for the optimizer to see your config.

For new applications, `all` is essentially free. The reason the default is `none` is historical, not technical.

## Operational impact

- Extra internal topics on the broker (`<app-id>-...-changelog`) for every source KTable.
- Double the bandwidth on KTable input — once to populate the KTable, again to populate its changelog.
- Operators see "phantom" topics they didn't expect.
- Slower restart (more changelogs to restore).

## How to fix

```properties
# BAD — default
topology.optimization=none

# GOOD — enable all
topology.optimization=all
```

```java
Properties props = new Properties();
props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

// BAD — build() without properties; optimizer doesn't run
Topology t = builder.build();

// GOOD — pass props so the optimizer sees the config
Topology t = builder.build(props);
KafkaStreams streams = new KafkaStreams(t, props);
```

Beware: enabling optimization changes the topology layout. For an existing application, a deploy with `all` invalidates internal-topic names → state loss equivalent to `STREAMS_APP_ID_UNSTABLE`. Use the application reset tool, or stage with a new `application.id`.

## When this might be a false positive

- Existing app with state on disk and changelogs — switching to `all` mid-life is dangerous. Flag, but recommend new applications enable it from day one.
- Apps using KIP-705 to selectively disable specific optimizations — partially-optimized.

## Detection strategy

- Config files: `topology.optimization=none` or absent (default). Flag with MEDIUM severity (depends on topology shape).
- Bytecode: `StreamsBuilder.build()` with no `Properties` argument, when the same module sets `topology.optimization=all` — the optimizer won't see it. HIGH confidence pairing.
- Confidence: MEDIUM.

## References

- KIP-295 — Topology optimization: https://cwiki.apache.org/confluence/display/KAFKA/KIP-295:+Add+Streams+Configuration+Allowing+for+Optional+Topology+Optimization
- KIP-312 — StreamsBuilder.build(Properties): https://cwiki.apache.org/confluence/display/KAFKA/KIP-312:+Add+Overloaded+StreamsBuilder+Build+Method+to+Accept+java.util.Properties
- KIP-705 — Selectively Disable Topology Optimizations: https://cwiki.apache.org/confluence/display/KAFKA/KIP-705:+Selectively+Disable+Topology+Optimizations
- Confluent — Optimizing topologies: https://docs.confluent.io/platform/current/streams/developer-guide/optimizing-streams.html
