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
 * does NOT set {@code default.dsl.store} (KIP-591, Apache Kafka 3.2,
 * May 2022). The Streams default is {@code "rocksdb"} (persistent,
 * off-heap, p99 0.5-5 ms); alternative is {@code "in_memory"}
 * (heap-only, lost on restart, p99 5-50 μs — 100-1000× faster). For
 * low-latency hot-key topologies (real-time fraud detection,
 * ad-bidding, pricing) where each record must complete in &lt;10 ms,
 * in_memory is the right answer. INFO severity because the default
 * is correct for most production workloads (large state, PVC-backed
 * restart, tolerable p99 latency).
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesDefaultDslStoreAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEFAULT_DSL_STORE = "default.dsl.store";

    private final Severity severity;

    public StreamsPropertiesDefaultDslStoreAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_DEFAULT_DSL_STORE_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(DEFAULT_DSL_STORE))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_DEFAULT_DSL_STORE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + DEFAULT_DSL_STORE, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + DEFAULT_DSL_STORE + "`. KIP-591 (Apache "
                            + "Kafka 3.2, May 2022) introduced this knob "
                            + "as the GLOBAL DEFAULT for which state-store "
                            + "implementation the DSL operators "
                            + "(`groupByKey().aggregate()`, `KTable` "
                            + "materializations, windowed stores, "
                            + "`KStream.join(KTable)`, etc.) use when "
                            + "the operator does NOT explicitly call "
                            + "`Materialized.as(Stores."
                            + "inMemoryKeyValueStore(...))` or "
                            + "`Materialized.withStoreType(...)`. The two "
                            + "built-in options: (1) `rocksdb` (default) "
                            + "— every materialized state-store is backed "
                            + "by RocksDB, an embedded LSM-tree KV store; "
                            + "state is PERSISTED to disk (via `state.dir`); "
                            + "memory is OFF-HEAP (native write-buffer, "
                            + "block-cache, index/filter blocks); p99 "
                            + "get/put is 0.5-5 ms; warm restart from "
                            + "disk is near-instant. (2) `in_memory` — "
                            + "every state-store is backed by a "
                            + "TreeMap/ConcurrentSkipListMap; state is "
                            + "HEAP-ONLY, lost on JVM restart, must be "
                            + "fully restored from the changelog topic on "
                            + "every start; p99 get/put is 5-50 μs "
                            + "(100-1000× faster than RocksDB). The "
                            + "trade-off: rocksdb is right for the "
                            + "MAJORITY of production workloads (large "
                            + "state, persistent restarts via "
                            + "Kubernetes PVCs, tolerable p99 latency); "
                            + "in_memory is the right answer for: "
                            + "(a) LOW-LATENCY hot-key topologies "
                            + "(real-time fraud detection, ad-bidding, "
                            + "pricing where each record must complete "
                            + "in <10 ms); (b) SMALL-STATE topologies "
                            + "(in-memory total under 1 GiB) on "
                            + "memory-rich instances (16+ GiB heap); "
                            + "(c) EPHEMERAL-DEPLOYMENT topologies "
                            + "(no PVC, fast scale-up/down). Bug shape: "
                            + "operator deploys a low-latency Streams "
                            + "topology with a <10 ms SLO without "
                            + "setting `" + DEFAULT_DSL_STORE + "`; "
                            + "default `rocksdb` is used; each record's "
                            + "4 state-store accesses cost 0.5-5 ms p99 "
                            + "each = 2-20 ms inside the topology alone; "
                            + "SLO violated frequently; operator profiles "
                            + "and sees RocksDB JNI calls dominating CPU "
                            + "but doesn't realize the DSL Materialized "
                            + "default is the lever. Setting "
                            + "`" + DEFAULT_DSL_STORE + "=in_memory` "
                            + "drops each access to 5-50 μs; 4 accesses "
                            + "= 20-200 μs total; SLO comfortably met. "
                            + "Counter-bug: operator switches to "
                            + "`in_memory` on a large-state topology "
                            + "(100k+ keys × 4 KiB values = 400 MB+ "
                            + "state) on a heap-constrained instance "
                            + "(4 GiB heap); the in-memory state plus "
                            + "GC overhead OOMs the JVM. Fix: for default "
                            + "persistent state stores, set "
                            + "`" + DEFAULT_DSL_STORE + "=rocksdb` "
                            + "EXPLICITLY (documents the deliberate "
                            + "choice; insulates from future framework "
                            + "default changes). For low-latency or "
                            + "ephemeral topologies, set "
                            + "`" + DEFAULT_DSL_STORE + "=in_memory` "
                            + "(cuts state-access latency 100-1000× at "
                            + "the cost of heap memory and slower cold-"
                            + "start from changelog replay). For mixed-"
                            + "profile topologies, leave the global "
                            + "default and override per-operator with "
                            + "`Materialized.withStoreType(Materialized."
                            + "StoreType.IN_MEMORY)` (KIP-825, Apache "
                            + "Kafka 3.6+) or "
                            + "`Materialized.as(Stores."
                            + "inMemoryKeyValueStore(\"store-name\"))` "
                            + "(older API)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
