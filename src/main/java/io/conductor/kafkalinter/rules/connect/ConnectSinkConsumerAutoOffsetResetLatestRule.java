package io.conductor.kafkalinter.rules.connect;

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
 * Project-scoped rule. Fires when a Kafka Connect SINK connector
 * .properties file explicitly sets
 * {@code consumer.override.auto.offset.reset=latest} (or, defensively,
 * the un-namespaced {@code auto.offset.reset=latest}).
 *
 * <p>Sink connectors are consumer-side workloads. With {@code latest},
 * a fresh deployment (or any restart that has no committed offsets in
 * {@code __consumer_offsets}: renamed connector, deleted-and-recreated,
 * Connect 3.6+ {@code DELETE /connectors/<name>/offsets} reset) silently
 * SKIPS every existing record in the source topic — the most common
 * "where's my data?" Connect incident. Sinks must onboard the existing
 * topic backlog; the correct value is {@code earliest} (or absent, to
 * inherit the worker's default).
 *
 * <p>Sink-connector identification heuristics:
 * <ul>
 *   <li>{@code connector.class} contains the substring {@code "Sink"}
 *       (covers Confluent's S3/GCS/HDFS/JDBC/Elasticsearch sinks and
 *       virtually every community sink connector), OR</li>
 *   <li>{@code topics} or {@code topics.regex} is set (these are
 *       sink-only Connect properties — source connectors define their
 *       output topic differently, not via {@code topics}).</li>
 * </ul>
 *
 * <p>Emits one ERROR-severity violation per offending file.
 */
public final class ConnectSinkConsumerAutoOffsetResetLatestRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TOPICS = "topics";
    private static final String TOPICS_REGEX = "topics.regex";
    private static final String CONSUMER_OVERRIDE_RESET = "consumer.override.auto.offset.reset";
    private static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    private final Severity severity;

    public ConnectSinkConsumerAutoOffsetResetLatestRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SINK_CONSUMER_AUTO_OFFSET_RESET_LATEST;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!isSinkConnector(connectorClass, p)) continue;

            String overrideValue = trimOrNull(p.getProperty(CONSUMER_OVERRIDE_RESET));
            String plainValue = trimOrNull(p.getProperty(AUTO_OFFSET_RESET));

            String offendingKey = null;
            String offendingValue = null;
            if (overrideValue != null && "latest".equalsIgnoreCase(overrideValue)) {
                offendingKey = CONSUMER_OVERRIDE_RESET;
                offendingValue = overrideValue;
            } else if (plainValue != null && "latest".equalsIgnoreCase(plainValue)) {
                offendingKey = AUTO_OFFSET_RESET;
                offendingValue = plainValue;
            }
            if (offendingKey == null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_SINK_CONSUMER_AUTO_OFFSET_RESET_LATEST, severity,
                    ctx.relativize(e.getKey()), "key:" + offendingKey, 0,
                    "Connect SINK connector " + connectorClass + " sets " + offendingKey + "="
                            + offendingValue + " — on first deploy (or any restart with no "
                            + "committed offsets in __consumer_offsets: renamed connector, "
                            + "deleted-and-recreated, Connect 3.6+ DELETE /connectors/<name>/offsets), "
                            + "the consumer starts at the END of the topic and SILENTLY SKIPS every "
                            + "existing record. The sink writes nothing to its destination "
                            + "(database, search index, object store, etc.) until brand-new events "
                            + "arrive after the deploy. There is no error log, no warning — just "
                            + "empty downstream destinations and engineers chasing networking, IAM, "
                            + "and schema-registry issues for hours. Sinks must onboard the existing "
                            + "topic backlog: change to " + offendingKey + "=earliest, OR remove the "
                            + "override entirely to inherit the worker's default (which is earliest "
                            + "in Confluent's default worker config). The latest setting is "
                            + "appropriate ONLY for rare 'start-from-now' read-only mirrors and "
                            + "should be documented explicitly when intentional."));
        }
        return out;
    }

    private static boolean isSinkConnector(String connectorClass, Properties p) {
        if (connectorClass.contains("Sink")) return true;
        if (trimOrNull(p.getProperty(TOPICS)) != null) return true;
        if (trimOrNull(p.getProperty(TOPICS_REGEX)) != null) return true;
        return false;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
