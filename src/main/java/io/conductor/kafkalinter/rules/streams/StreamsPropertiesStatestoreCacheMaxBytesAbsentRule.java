package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires on a Kafka Streams .properties file that
 * does NOT set {@code statestore.cache.max.bytes} (KIP-770, Apache
 * Kafka 3.4, February 2023 — the rename of {@code
 * cache.max.bytes.buffering}). The Streams default is {@code 10485760}
 * (10 MiB) TOTAL for the entire instance, shared across ALL
 * state-stores AND ALL StreamThreads. For production-scale topologies
 * (8+ state-stores × 8+ threads), the per-store-per-thread effective
 * budget drops to kilobytes — too small to coalesce repeated puts, so
 * RocksDB write-amp jumps 3-10×, changelog topic write rate jumps the
 * same, and downstream operators see flooded intermediate updates that
 * should have been coalesced. INFO severity because the default is
 * defensible for trivial topologies (1-2 stores, 1-2 threads).
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesStatestoreCacheMaxBytesAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String STATESTORE_CACHE_MAX_BYTES = "statestore.cache.max.bytes";

    private final Severity severity;

    public StreamsPropertiesStatestoreCacheMaxBytesAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_STATESTORE_CACHE_MAX_BYTES_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(STATESTORE_CACHE_MAX_BYTES))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_STATESTORE_CACHE_MAX_BYTES_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + STATESTORE_CACHE_MAX_BYTES, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + STATESTORE_CACHE_MAX_BYTES + "`. KIP-770 "
                            + "(Apache Kafka 3.4, February 2023) renamed "
                            + "the older `cache.max.bytes.buffering` to "
                            + "`" + STATESTORE_CACHE_MAX_BYTES + "` AND "
                            + "changed the SCOPE of the value: pre-3.4 "
                            + "was PER-StreamThread (multiplied internally "
                            + "by num.stream.threads to derive the "
                            + "instance-wide budget); post-3.4 is the "
                            + "TOTAL cache budget for the entire instance, "
                            + "shared across ALL state-stores AND ALL "
                            + "StreamThreads. The cache sits in front of "
                            + "every materialized state-store (KTable, "
                            + "GlobalKTable, aggregate-result store, "
                            + "windowed-result store, joined-result store) "
                            + "and serves as a write-behind buffer that "
                            + "COALESCES repeated puts to the same key "
                            + "within a single commit interval. Within "
                            + "the interval: every `put(key, value)` "
                            + "writes to the in-memory cache; if the same "
                            + "key is updated N times in the interval, "
                            + "only the FINAL value is flushed to (a) "
                            + "the underlying RocksDB store, (b) the "
                            + "changelog topic, (c) any downstream "
                            + "operator subscribing to the KTable. The "
                            + "cache is the PRIMARY MECHANISM by which "
                            + "Streams reduces write amplification on "
                            + "stateful topologies. The default `10485760` "
                            + "(10 MiB) is calibrated for SMALL topologies "
                            + "(1-2 state-stores, 1-2 threads). For "
                            + "PRODUCTION-SCALE topologies — multiple "
                            + "aggregations, joins materializing "
                            + "intermediate KTables, windowed stores, "
                            + "GlobalKTables, all on a 4-or-more-thread "
                            + "instance — the 10 MiB total budget is "
                            + "GROSSLY INSUFFICIENT and is divided so "
                            + "finely across state-stores × threads that "
                            + "each effective per-store-per-thread budget "
                            + "is in the kilobytes. At that size, the "
                            + "cache evicts entries faster than it can "
                            + "coalesce repeated updates; coalescing "
                            + "effectiveness drops from typical 80-99% "
                            + "to 10-30%; RocksDB write-amp jumps 3-10×; "
                            + "changelog topic write rate jumps the same; "
                            + "downstream consumers are flooded with "
                            + "redundant intermediate updates that should "
                            + "have been coalesced. Bug shape: operator "
                            + "deploys a production Streams app with 8 "
                            + "state-stores and 8 StreamThreads; default "
                            + "10 MiB is used; per-store-per-thread "
                            + "effective budget is 10 MiB ÷ 8 stores ÷ 8 "
                            + "threads = ~160 KiB — too small to absorb "
                            + "anything but the highest-frequency hot "
                            + "keys; topology runs at 5-10× the necessary "
                            + "RocksDB write rate; RocksDB compaction "
                            + "can't keep up; L0 stalls become frequent; "
                            + "throughput drops by 3-5× from theoretical; "
                            + "operator profiles and finds RocksDB "
                            + "write-amp as the bottleneck but doesn't "
                            + "realize it's a CACHE SIZING ISSUE. "
                            + "Migration hazard from pre-3.4: the rename "
                            + "is NOT 1-to-1 because the scope changed — "
                            + "if pre-3.4 you had `cache.max.bytes."
                            + "buffering=10485760` on a 4-thread instance "
                            + "(actual budget was 40 MiB), post-3.4 you "
                            + "must set `" + STATESTORE_CACHE_MAX_BYTES
                            + "=41943040` to preserve the same per-thread "
                            + "budget. Fix: for trivial topologies (1-2 "
                            + "stores, 1-2 threads), set `"
                            + STATESTORE_CACHE_MAX_BYTES + "=10485760` "
                            + "EXPLICITLY (10 MiB — documents the "
                            + "deliberate choice). For medium topologies "
                            + "(4-8 stores, 4-8 threads), set `"
                            + STATESTORE_CACHE_MAX_BYTES + "=134217728` "
                            + "(128 MiB — provides ~2-4 MiB per "
                            + "store-per-thread). For large topologies "
                            + "(16+ stores, 16+ threads), set `"
                            + STATESTORE_CACHE_MAX_BYTES + "=1073741824` "
                            + "(1 GiB — provides ~4 MiB per "
                            + "store-per-thread). For memory-constrained "
                            + "instances, size the cache as a percentage "
                            + "of the JVM heap (typical 5-15% of -Xmx)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
