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
 * Project-scoped rule. Fires when two or more Debezium Postgres
 * connectors declare the same {@code publication.name}.
 *
 * <p>A Postgres logical-replication publication is a shared server-wide
 * named object that controls which tables are exposed to logical
 * replication subscribers. Unlike a replication slot (exclusive), a
 * publication is SHARED — multiple connectors that share a publication
 * name fight over its table-list (in {@code filtered} mode, the LAST
 * connector to start overwrites the others' filter), leak data across
 * connector boundaries (in {@code all_tables} mode), and break each
 * other on DROP PUBLICATION.
 *
 * <p>If {@code publication.name} is absent, Debezium's default
 * ({@code dbz_publication}) is used. Connectors that ALL omit it
 * therefore share the default and are equally susceptible — they are
 * grouped together.
 *
 * <p>Emits one violation per file in a sharing group.
 */
public final class ConnectDebeziumPostgresPublicationNameSharedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String PUBLICATION_NAME = "publication.name";
    private static final String DEFAULT_PUBLICATION_NAME = "dbz_publication";

    private final Severity severity;

    public ConnectDebeziumPostgresPublicationNameSharedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_NAME_SHARED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Map<String, List<PublicationUsage>> byPublication = new LinkedHashMap<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!POSTGRES_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String configured = trimOrNull(p.getProperty(PUBLICATION_NAME));
            String pub = configured != null ? configured : DEFAULT_PUBLICATION_NAME;
            boolean usedDefault = configured == null;

            byPublication.computeIfAbsent(pub, s -> new ArrayList<>())
                    .add(new PublicationUsage(e.getKey(), usedDefault, trimOrNull(p.getProperty("name"))));
        }

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<String, List<PublicationUsage>> group : byPublication.entrySet()) {
            List<PublicationUsage> usages = group.getValue();
            if (usages.size() < 2) continue;

            for (PublicationUsage u : usages) {
                StringBuilder peers = new StringBuilder();
                for (PublicationUsage other : usages) {
                    if (other == u) continue;
                    if (peers.length() > 0) peers.append(", ");
                    peers.append(ctx.relativize(other.file));
                    if (other.connectorName != null) {
                        peers.append(" (name=").append(other.connectorName).append(")");
                    }
                }
                String source = u.usedDefault
                        ? "publication.name is not set, defaulting to '" + DEFAULT_PUBLICATION_NAME + "'"
                        : "publication.name=" + group.getKey();
                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_POSTGRES_PUBLICATION_NAME_SHARED, severity,
                        ctx.relativize(u.file), "key:" + PUBLICATION_NAME, 0,
                        "Debezium Postgres connector "
                                + (u.connectorName != null ? "(name=" + u.connectorName + ") " : "")
                                + source + " — the same publication name is used by: " + peers
                                + ". Postgres publications are SHARED server-wide named "
                                + "objects: in publication.autocreate.mode=filtered each "
                                + "connector REPLACES the publication's table list on every "
                                + "startup (LAST connector wins, others' filters silently "
                                + "OVERWRITTEN — missed events GONE, not deferred); in "
                                + "all_tables mode (default) both connectors leak table data "
                                + "across boundaries (each pays the WAL-decode cost of every "
                                + "other connector's tables); any DROP PUBLICATION or ALTER "
                                + "PUBLICATION DROP TABLE by an operator decommissioning one "
                                + "connector silently breaks every sharing connector. Give "
                                + "each Postgres Debezium connector a unique publication.name, "
                                + "e.g., publication.name=dbz_publication_<connector-name> or "
                                + "publication.name=dbz_pub_<database-server-name>."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static final class PublicationUsage {
        final Path file;
        final boolean usedDefault;
        final String connectorName;

        PublicationUsage(Path file, boolean usedDefault, String connectorName) {
            this.file = file;
            this.usedDefault = usedDefault;
            this.connectorName = connectorName;
        }
    }
}
