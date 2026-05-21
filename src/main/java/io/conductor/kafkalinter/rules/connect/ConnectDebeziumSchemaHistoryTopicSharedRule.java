package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when two or more Debezium connector
 * .properties files declare the SAME schema-history topic.
 *
 * <p>The schema-history topic is a per-connector internal log of DDL
 * events. Sharing it between connectors corrupts schema recovery on
 * restart and forces a full re-snapshot of the source database.
 *
 * <p>Honors both Debezium 2.x ({@code schema.history.internal.kafka.topic})
 * and the legacy 1.x ({@code database.history.kafka.topic}). Emits one
 * violation per file in a sharing group.
 */
public final class ConnectDebeziumSchemaHistoryTopicSharedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_PREFIX = "io.debezium.connector.";
    private static final String SCHEMA_HISTORY_2X = "schema.history.internal.kafka.topic";
    private static final String SCHEMA_HISTORY_1X = "database.history.kafka.topic";

    private final Severity severity;

    public ConnectDebeziumSchemaHistoryTopicSharedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_TOPIC_SHARED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Map<String, List<HistoryUsage>> byTopic = new LinkedHashMap<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_PREFIX)) continue;

            String key = SCHEMA_HISTORY_2X;
            String topic = trimOrNull(p.getProperty(SCHEMA_HISTORY_2X));
            if (topic == null) {
                topic = trimOrNull(p.getProperty(SCHEMA_HISTORY_1X));
                key = SCHEMA_HISTORY_1X;
            }
            if (topic == null) continue;

            byTopic.computeIfAbsent(topic, t -> new ArrayList<>())
                    .add(new HistoryUsage(e.getKey(), key, connectorClass,
                            trimOrNull(p.getProperty("name"))));
        }

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<String, List<HistoryUsage>> group : byTopic.entrySet()) {
            List<HistoryUsage> usages = group.getValue();
            if (usages.size() < 2) continue;

            for (HistoryUsage u : usages) {
                StringBuilder peers = new StringBuilder();
                for (HistoryUsage other : usages) {
                    if (other == u) continue;
                    if (peers.length() > 0) peers.append(", ");
                    peers.append(ctx.relativize(other.file));
                    if (other.connectorName != null) {
                        peers.append(" (name=").append(other.connectorName).append(")");
                    }
                }
                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_TOPIC_SHARED, severity,
                        ctx.relativize(u.file), "key:" + u.keyUsed, 0,
                        "Debezium connector " + u.connectorClass
                                + (u.connectorName != null ? " (name=" + u.connectorName + ")" : "")
                                + " declares " + u.keyUsed + "=" + group.getKey()
                                + " which is ALSO used by: " + peers + ". The schema-history "
                                + "topic is a per-connector internal log of DDL events used "
                                + "for schema recovery on restart; sharing it between "
                                + "connectors causes each to read the other's DDL events, "
                                + "corrupting schema state, triggering 'schema-history-"
                                + "recovery-failed' errors, and forcing a full re-snapshot "
                                + "of the source database (hours of locked production "
                                + "tables). Give each Debezium connector a unique topic, "
                                + "e.g., " + u.keyUsed + "=<connector-name>.schema-history."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static final class HistoryUsage {
        final Path file;
        final String keyUsed;
        final String connectorClass;
        final String connectorName;

        HistoryUsage(Path file, String keyUsed, String connectorClass, String connectorName) {
            this.file = file;
            this.keyUsed = keyUsed;
            this.connectorClass = connectorClass;
            this.connectorName = connectorName;
        }
    }
}
