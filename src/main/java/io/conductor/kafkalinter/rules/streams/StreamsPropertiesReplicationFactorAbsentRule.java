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
 * does NOT set {@code replication.factor}. The Streams default is
 * {@code -1} — 'use the broker's default'. In dev / sandbox clusters
 * the broker default is almost always 1, silently creating single-
 * broker dependencies for changelog and repartition topics. WARNING
 * severity because the failure mode is data loss on broker reboot,
 * not throughput.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesReplicationFactorAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String REPLICATION_FACTOR = "replication.factor";

    private final Severity severity;

    public StreamsPropertiesReplicationFactorAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_REPLICATION_FACTOR_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(REPLICATION_FACTOR))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_REPLICATION_FACTOR_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + REPLICATION_FACTOR, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + REPLICATION_FACTOR + "`. Streams defaults `"
                            + REPLICATION_FACTOR + "` to `-1` — which means "
                            + "'use the broker's `default.replication.factor`'. "
                            + "In dev / sandbox / freshly-installed clusters "
                            + "that broker default is almost always `1`, "
                            + "creating single-broker dependencies for every "
                            + "internal topic (changelog topics for state "
                            + "stores, repartition topics for keyed re-"
                            + "grouping). The .properties file gives ZERO "
                            + "local signal of this — operators discover it "
                            + "during a broker reboot, when state stores "
                            + "cannot restore from changelog topics whose "
                            + "leader partitions were on the rebooted broker. "
                            + "Fix: set `" + REPLICATION_FACTOR + "=3` for "
                            + "production. The broker default is a portability "
                            + "convenience that hides a critical durability "
                            + "decision behind broker-side config the Streams "
                            + "app cannot inspect."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
