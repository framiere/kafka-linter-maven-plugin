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
 * Project-scoped rule. Fires when a Debezium Postgres connector has
 * {@code publication.autocreate.mode=filtered} but none of the four
 * filter-list configs ({@code schema.include.list},
 * {@code schema.exclude.list}, {@code table.include.list},
 * {@code table.exclude.list}) are set.
 *
 * <p>The {@code filtered} mode tells Debezium to issue
 * {@code CREATE PUBLICATION <name> FOR TABLE <derived-list>;}, with the
 * derived list computed from the connector's filter configuration. With all
 * four filter lists empty, the derived list is empty: either the SQL is
 * invalid (startup failure) or Debezium creates an empty publication
 * (silently captures zero events). There is no Debezium scenario where the
 * operator's intent is 'create a publication filtered to capture nothing'.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumPublicationFilteredMissingFilterListRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String PUBLICATION_AUTOCREATE_MODE = "publication.autocreate.mode";
    private static final String FILTERED_MODE = "filtered";

    private static final String[] FILTER_LIST_KEYS = {
            "table.include.list",
            "table.exclude.list",
            "schema.include.list",
            "schema.exclude.list"
    };

    private final Severity severity;

    public ConnectDebeziumPublicationFilteredMissingFilterListRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_PUBLICATION_FILTERED_MISSING_FILTER_LIST;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!POSTGRES_CONNECTOR.equals(connectorClass)) continue;

            String mode = trimOrNull(p.getProperty(PUBLICATION_AUTOCREATE_MODE));
            if (mode == null || !FILTERED_MODE.equalsIgnoreCase(mode)) continue;

            boolean anyFilterSet = false;
            for (String key : FILTER_LIST_KEYS) {
                if (trimOrNull(p.getProperty(key)) != null) {
                    anyFilterSet = true;
                    break;
                }
            }
            if (anyFilterSet) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_PUBLICATION_FILTERED_MISSING_FILTER_LIST, severity,
                    ctx.relativize(e.getKey()), "key:" + PUBLICATION_AUTOCREATE_MODE, 0,
                    "Debezium Postgres connector sets publication.autocreate.mode=filtered "
                            + "but NONE of table.include.list, table.exclude.list, "
                            + "schema.include.list, or schema.exclude.list is set. The filtered "
                            + "mode tells Debezium to issue `CREATE PUBLICATION <name> FOR TABLE "
                            + "<derived-list>;` where <derived-list> is computed from the "
                            + "connector's filter configs. With all four filter lists empty, the "
                            + "derived list is empty: depending on the Debezium version, either "
                            + "the SQL is invalid (startup fails with a Postgres syntax error) "
                            + "or Debezium creates an EMPTY publication (connector starts cleanly "
                            + "but captures ZERO events — no events ever flow to Kafka, no error "
                            + "in the connector log, just silence). There is no Debezium "
                            + "configuration where this is the operator's intent. Set at least "
                            + "one of: table.include.list=<schema>.<table>,... (most common); "
                            + "schema.include.list=<schema> (capture all tables in a schema); or "
                            + "the exclude variants for broad capture with specific exclusions."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
