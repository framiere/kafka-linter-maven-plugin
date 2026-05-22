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
 * does NOT set {@code processing.guarantee}. The Streams default is
 * {@code at_least_once} — silently right for idempotent downstream
 * workloads, silently wrong for workloads that need EOS (financial
 * settlement, inventory decrement, external API calls). INFO severity
 * because the default is workload-dependent.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesProcessingGuaranteeAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PROCESSING_GUARANTEE = "processing.guarantee";

    private final Severity severity;

    public StreamsPropertiesProcessingGuaranteeAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_PROCESSING_GUARANTEE_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(PROCESSING_GUARANTEE))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_PROCESSING_GUARANTEE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + PROCESSING_GUARANTEE, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + PROCESSING_GUARANTEE + "`. Streams defaults `"
                            + PROCESSING_GUARANTEE + "` to `at_least_once` — "
                            + "on every rebalance and on every restart, in-"
                            + "flight records may be reprocessed and re-emitted "
                            + "to output topics. Silently right for idempotent "
                            + "downstream (key-keyed upserts, set-valued state, "
                            + "monitoring dashboards), silently wrong for non-"
                            + "idempotent downstream (financial settlement, "
                            + "inventory decrement, external API calls) where "
                            + "duplicate emission causes double-counting bugs "
                            + "discovered in production by reconciliation. Fix: "
                            + "set `" + PROCESSING_GUARANTEE + "=at_least_once` "
                            + "EXPLICITLY to document the at-least-once posture, "
                            + "OR set `" + PROCESSING_GUARANTEE + "=exactly_"
                            + "once_v2` for EOS (requires broker 2.5+, costs "
                            + "~30% throughput, pins commit.interval.ms=100). "
                            + "The point is to commit to one deliberately."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
