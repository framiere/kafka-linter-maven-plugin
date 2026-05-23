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
 * Project-scoped rule. Fires on a Kafka Streams .properties file
 * (identified by top-level {@code application.id}, excluding Connect
 * configs via {@code connector.class}) that does NOT set
 * {@code topology.optimization}.
 *
 * <p>Streams defaults {@code topology.optimization} to {@code none}
 * (declared in {@code StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG},
 * {@code ConfigDef.Type.STRING}). With {@code none}, the topology builder
 * generates internal CHANGELOG and REPARTITION topics conservatively:
 * every {@code KTable.toStream()} materialization creates a separate
 * changelog topic; every {@code groupBy().*} after a key-changing
 * operation creates a separate repartition topic; every {@code KTable}
 * source materialization creates a changelog DUPLICATING the source
 * topic. For non-trivial topologies, this generates 5-15 EXTRA internal
 * topics — doubling broker resource consumption and cold-start restore
 * time.
 *
 * <p>KIP-295 (Apache Kafka 2.1, November 2018) introduced
 * {@code topology.optimization=all} to consolidate these. The default
 * {@code none} exists for backward compatibility with pre-KIP-295
 * deployments; for new Streams applications, {@code all} is the
 * production default.
 *
 * <p>INFO severity because the failure mode is resource-overhead, not
 * data loss or operational failure — but it's a high-impact opportunity
 * for resource efficiency on every non-trivial Streams app.
 *
 * <p>Complementary to {@link io.conductor.kafkalinter.rules.config.ConfigKeyValueRule}-based
 * {@code STREAMS_TOPOLOGY_OPTIMIZATION_NONE} which catches the
 * bytecode-side explicit {@code =none} value.
 */
public final class StreamsPropertiesTopologyOptimizationAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TOPOLOGY_OPTIMIZATION = "topology.optimization";

    private final Severity severity;

    public StreamsPropertiesTopologyOptimizationAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_TOPOLOGY_OPTIMIZATION_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(TOPOLOGY_OPTIMIZATION))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_TOPOLOGY_OPTIMIZATION_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + TOPOLOGY_OPTIMIZATION, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + TOPOLOGY_OPTIMIZATION + "`. Streams defaults `"
                            + TOPOLOGY_OPTIMIZATION + "` to `none` — the "
                            + "topology builder generates internal CHANGELOG "
                            + "and REPARTITION topics CONSERVATIVELY: every "
                            + "`KTable.toStream()` materialization creates a "
                            + "separate changelog topic; every `groupBy().*` "
                            + "after a key-changing operation creates a "
                            + "separate repartition topic; every `KTable` "
                            + "source materialization creates a changelog "
                            + "DUPLICATING the source topic. For non-trivial "
                            + "topologies (a join chain, a multi-stage "
                            + "aggregation, a foreign-key join), this "
                            + "generates 5-15 EXTRA internal topics that all "
                            + "need broker partitions, replication-factor "
                            + "durability, separate compaction lifecycles, "
                            + "and separate restore-time changelog replay on "
                            + "startup. KIP-295 (Apache Kafka 2.1, November "
                            + "2018) introduced `" + TOPOLOGY_OPTIMIZATION
                            + "=all` to consolidate these: (a) "
                            + "`reuse.ktable.source.topics` — a KTable from a "
                            + "source topic uses the SOURCE TOPIC AS ITS "
                            + "CHANGELOG instead of creating a duplicate "
                            + "internal `<app-id>-<store-name>-changelog`; "
                            + "(b) `merge.repartition.topics` — consecutive "
                            + "`groupBy().*` / `selectKey().*` chains share a "
                            + "SINGLE repartition topic instead of one-per-"
                            + "operator; (c) `single.store.self.join` (KIP-862, "
                            + "Apache Kafka 3.4) — a self-join on a KStream "
                            + "creates ONE state store instead of two. For a "
                            + "typical non-trivial Streams app (one "
                            + "aggregation, two joins, three repartition-"
                            + "inducing operators), `" + TOPOLOGY_OPTIMIZATION
                            + "=all` reduces internal topic count from ~12 "
                            + "to ~6 — halving broker partition count, "
                            + "halving replication overhead, halving restore-"
                            + "replay time on cold start. Specific impacts: "
                            + "(a) **aggregation app with KTable source** — "
                            + "the source topic IS the changelog under "
                            + "`reuse.ktable.source.topics`; saves 10M extra "
                            + "messages written daily plus 5-10 extra minutes "
                            + "of restore time per pod restart on a 10M-record "
                            + "KTable. (b) **multi-stage aggregation with "
                            + "chained groupBy** — two repartition topics "
                            + "merge into one under `merge.repartition.topics`; "
                            + "halves broker resource cost on intermediate "
                            + "datasets. (c) **foreign-key join materialized "
                            + "view** — the operator's 4-5 internal topics "
                            + "reduce under repartition consolidation; "
                            + "gigabytes saved on broker storage for 100M-"
                            + "record joins. (d) **self-join** — two state "
                            + "stores collapse to one; halves state-store "
                            + "size and restore time. (e) **Spring Boot "
                            + "Streams autoconfig** — default `application.yml`"
                            + " template omits this knob; every Spring Boot "
                            + "Streams app inherits the conservative default. "
                            + "Fix: ONE LINE. For NEW applications: `"
                            + TOPOLOGY_OPTIMIZATION + "=all` (the production "
                            + "default — three documented sub-optimizations, "
                            + "halves internal topic count, halves restore-"
                            + "replay time, halves broker resource "
                            + "consumption). For EXISTING applications: set "
                            + "explicit `" + TOPOLOGY_OPTIMIZATION + "=none` "
                            + "and document the migration constraint — "
                            + "changing optimization on a running app "
                            + "reshuffles internal topic identities (new "
                            + "changelog names, new sub-topology IDs); the "
                            + "OLD changelog topics become orphaned and the "
                            + "app must REBUILD state from scratch on first "
                            + "deploy after the change; this is a one-time "
                            + "state-rebuild window that must be planned. "
                            + "Migration sub-options exist (`"
                            + TOPOLOGY_OPTIMIZATION + "=reuse.ktable.source."
                            + "topics` alone, or any comma-separated subset "
                            + "of the three named sub-optimizations) for "
                            + "operators who want to enable optimizations "
                            + "incrementally. Sibling rule [[streams-"
                            + "topology-optimization-none]] catches the "
                            + "bytecode-detected explicit `=none` in code-"
                            + "side `Properties.put` calls; this rule "
                            + "catches the .properties-file absent case "
                            + "where the operator never set the knob at all. "
                            + "Runtime semantics are unchanged — `"
                            + TOPOLOGY_OPTIMIZATION + "` only affects the "
                            + "INTERNAL topic layout, never the observable "
                            + "processing behavior."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
