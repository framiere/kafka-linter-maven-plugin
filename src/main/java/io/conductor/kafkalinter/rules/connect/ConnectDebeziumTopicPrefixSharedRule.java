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
 * Project-scoped rule. Fires when two or more Debezium connectors
 * declare the same {@code topic.prefix} (Debezium 2.x) or the same
 * {@code database.server.name} (Debezium 1.x).
 *
 * <p>This value is the single namespace for all of a Debezium
 * connector's CDC topics, transaction-metadata topic, schema-change
 * topic, heartbeat topic suffix, source.name field, and JMX metric
 * tags. Two connectors sharing it interleave every output topic and
 * merge every metric — catastrophic namespace collision.
 *
 * <p>Detection covers the entire Debezium connector family (any
 * {@code io.debezium.connector.} class-name prefix), prefers the
 * 2.x key with 1.x fallback, and emits one violation per file in
 * any sharing group.
 */
public final class ConnectDebeziumTopicPrefixSharedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_PREFIX = "io.debezium.connector.";
    private static final String TOPIC_PREFIX_2X = "topic.prefix";
    private static final String DATABASE_SERVER_NAME_1X = "database.server.name";

    private final Severity severity;

    public ConnectDebeziumTopicPrefixSharedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_TOPIC_PREFIX_SHARED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Map<String, List<PrefixUsage>> byPrefix = new LinkedHashMap<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_PREFIX)) continue;

            String keyUsed = TOPIC_PREFIX_2X;
            String prefix = trimOrNull(p.getProperty(TOPIC_PREFIX_2X));
            if (prefix == null) {
                prefix = trimOrNull(p.getProperty(DATABASE_SERVER_NAME_1X));
                keyUsed = DATABASE_SERVER_NAME_1X;
            }
            if (prefix == null) continue;

            byPrefix.computeIfAbsent(prefix, s -> new ArrayList<>())
                    .add(new PrefixUsage(e.getKey(), keyUsed, connectorClass, trimOrNull(p.getProperty("name"))));
        }

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<String, List<PrefixUsage>> group : byPrefix.entrySet()) {
            List<PrefixUsage> usages = group.getValue();
            if (usages.size() < 2) continue;

            for (PrefixUsage u : usages) {
                StringBuilder peers = new StringBuilder();
                for (PrefixUsage other : usages) {
                    if (other == u) continue;
                    if (peers.length() > 0) peers.append(", ");
                    peers.append(ctx.relativize(other.file));
                    if (other.connectorName != null) {
                        peers.append(" (name=").append(other.connectorName).append(")");
                    }
                }
                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_TOPIC_PREFIX_SHARED, severity,
                        ctx.relativize(u.file), "key:" + u.keyUsed, 0,
                        "Debezium connector "
                                + (u.connectorName != null ? "(name=" + u.connectorName + ") " : "")
                                + u.keyUsed + "=" + group.getKey()
                                + " — the same value is used by: " + peers
                                + ". `topic.prefix` (Debezium 2.x) / `database.server.name` "
                                + "(Debezium 1.x) is the SINGLE NAMESPACE for ALL of the "
                                + "connector's CDC topics, transaction-metadata topic, "
                                + "schema-change topic, heartbeat topic suffix, source.name "
                                + "field, and JMX metric tags. Two connectors sharing it "
                                + "INTERLEAVE every CDC topic (events from different sources "
                                + "land in the same Kafka topic), merge JMX metrics (you "
                                + "cannot graph one connector's lag separately), and break "
                                + "transaction-metadata and schema-change semantics. Give "
                                + "each Debezium connector a unique " + u.keyUsed + ", "
                                + "e.g., " + u.keyUsed + "=cdc.<service-name> or "
                                + u.keyUsed + "=<env>.<service-name>."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static final class PrefixUsage {
        final Path file;
        final String keyUsed;
        final String connectorClass;
        final String connectorName;

        PrefixUsage(Path file, String keyUsed, String connectorClass, String connectorName) {
            this.file = file;
            this.keyUsed = keyUsed;
            this.connectorClass = connectorClass;
            this.connectorName = connectorName;
        }
    }
}
