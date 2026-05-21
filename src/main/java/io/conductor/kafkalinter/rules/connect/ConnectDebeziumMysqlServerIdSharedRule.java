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
 * Project-scoped rule. Fires when two or more Debezium MySQL connectors
 * declare the same {@code database.server.id}.
 *
 * <p>MySQL's row-based replication protocol requires every replica
 * (and every Debezium connector counts as a replica for binlog
 * purposes) to register a UNIQUE 32-bit unsigned integer
 * {@code server_id}. The primary uses this id to demultiplex binlog
 * dump connections; collisions cause one connector to be silently
 * disconnected mid-stream, leading to a connect/disconnect storm.
 *
 * <p>Connectors that omit {@code database.server.id} are NOT flagged
 * by this rule — Debezium will allocate a random id in that case,
 * which is a different (and separately checked) anti-pattern.
 *
 * <p>Emits one violation per file in a sharing group.
 */
public final class ConnectDebeziumMysqlServerIdSharedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector";
    private static final String SERVER_ID = "database.server.id";

    private final Severity severity;

    public ConnectDebeziumMysqlServerIdSharedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_SHARED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Map<String, List<ServerIdUsage>> byServerId = new LinkedHashMap<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!MYSQL_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String serverId = trimOrNull(p.getProperty(SERVER_ID));
            if (serverId == null) continue;

            byServerId.computeIfAbsent(serverId, s -> new ArrayList<>())
                    .add(new ServerIdUsage(e.getKey(), trimOrNull(p.getProperty("name"))));
        }

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<String, List<ServerIdUsage>> group : byServerId.entrySet()) {
            List<ServerIdUsage> usages = group.getValue();
            if (usages.size() < 2) continue;

            for (ServerIdUsage u : usages) {
                StringBuilder peers = new StringBuilder();
                for (ServerIdUsage other : usages) {
                    if (other == u) continue;
                    if (peers.length() > 0) peers.append(", ");
                    peers.append(ctx.relativize(other.file));
                    if (other.connectorName != null) {
                        peers.append(" (name=").append(other.connectorName).append(")");
                    }
                }
                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_MYSQL_SERVER_ID_SHARED, severity,
                        ctx.relativize(u.file), "key:" + SERVER_ID, 0,
                        "Debezium MySQL connector "
                                + (u.connectorName != null ? "(name=" + u.connectorName + ") " : "")
                                + "database.server.id=" + group.getKey()
                                + " — the same server id is used by: " + peers
                                + ". MySQL's row-based replication protocol requires every "
                                + "binlog reader (replica or Debezium connector) to register "
                                + "a UNIQUE 32-bit unsigned integer server_id. The MySQL "
                                + "primary uses this id to demultiplex binlog dump "
                                + "connections; on collision, the primary intermittently "
                                + "disconnects whichever reader matches the duplicate-id "
                                + "signature, throwing 'A slave with the same server_uuid/"
                                + "server_id as this slave has connected to the master'. "
                                + "Both connectors then enter a connect/disconnect storm, "
                                + "causing event loss and unbounded lag. Give each MySQL "
                                + "Debezium connector a globally unique database.server.id "
                                + "(coordinate via an org-wide server-id allocation registry "
                                + "shared with your DBA team's MySQL replicas)."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static final class ServerIdUsage {
        final Path file;
        final String connectorName;

        ServerIdUsage(Path file, String connectorName) {
            this.file = file;
            this.connectorName = connectorName;
        }
    }
}
