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
import java.util.TreeSet;

/**
 * Project-scoped rule. Fires when a Debezium connector (non-Postgres)
 * has any property key starting with the legacy Debezium 1.x prefix
 * {@code database.history.}.
 *
 * <p>In Debezium 2.0, the entire {@code database.history.*} config
 * namespace was RENAMED to {@code schema.history.internal.*}. The
 * legacy keys are silently ignored in 2.x (Connect accepts unknown
 * keys and passes them through; the connector ignores them).
 *
 * <p>Postgres connectors are skipped: Postgres does not use a
 * schema-history topic (DDL is replayed inline via the logical-
 * replication publication), so legacy keys on Postgres are
 * leftover-cruft rather than active misconfiguration.
 *
 * <p>Emits one violation per offending file, naming every legacy key
 * present.
 */
public final class ConnectDebeziumLegacySchemaHistoryKeysRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String LEGACY_KEY_PREFIX = "database.history.";

    private final Severity severity;

    public ConnectDebeziumLegacySchemaHistoryKeysRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_LEGACY_SCHEMA_HISTORY_KEYS;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;
            if (POSTGRES_CONNECTOR.equals(connectorClass)) continue;

            TreeSet<String> legacyKeys = new TreeSet<>();
            for (String name : p.stringPropertyNames()) {
                if (name.startsWith(LEGACY_KEY_PREFIX)) {
                    legacyKeys.add(name);
                }
            }
            if (legacyKeys.isEmpty()) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_LEGACY_SCHEMA_HISTORY_KEYS, severity,
                    ctx.relativize(e.getKey()), "key:" + legacyKeys.first(), 0,
                    "Debezium connector uses LEGACY Debezium 1.x schema-history "
                            + "config key(s): " + String.join(", ", legacyKeys)
                            + " — in Debezium 2.0 the entire database.history.* "
                            + "namespace was RENAMED to schema.history.internal.*. "
                            + "The legacy keys are NOT in Debezium 2.x's ConfigDef; "
                            + "Connect's general behavior with unknown keys is to "
                            + "silently accept them (the worker passes them in the "
                            + "originals map but the connector ignores them — the "
                            + "REST-API PUT-validate endpoint does NOT flag the "
                            + "legacy keys). The connector then either fails at "
                            + "start with ConfigException: "
                            + "schema.history.internal.kafka.bootstrap.servers is "
                            + "required for KafkaSchemaHistory (when the migration "
                            + "drops the bootstrap-servers key entirely), OR runs "
                            + "degraded (schema-history topic absent/non-functional; "
                            + "the connector cannot recover from a restart because "
                            + "the schema-history replay is broken). Fix: rename "
                            + "each legacy key mechanically by replacing the prefix "
                            + "database.history with schema.history.internal — "
                            + "database.history.kafka.bootstrap.servers becomes "
                            + "schema.history.internal.kafka.bootstrap.servers; "
                            + "database.history.kafka.topic becomes "
                            + "schema.history.internal.kafka.topic; "
                            + "database.history.skip.unparseable.ddl becomes "
                            + "schema.history.internal.skip.unparseable.ddl; etc. "
                            + "The suffix patterns are preserved; only the prefix "
                            + "changes. The migration has NO downstream impact "
                            + "(the schema-history topic's contents and consumer "
                            + "semantics are identical) and no Kafka-side changes "
                            + "(the existing schema-history topic can be reused "
                            + "under the new key name — pointing the new config at "
                            + "the same topic name preserves continuity). Common "
                            + "origins: (a) Debezium 1.x config carried forward to "
                            + "a 2.x deployment without rename; (b) copy-paste from "
                            + "a 2020-era tutorial that predates the 2.0 rename; "
                            + "(c) a Helm chart from the 1.x era that was "
                            + "'upgraded' by bumping only the image version. Note: "
                            + "this rule does NOT fire on Postgres connectors — "
                            + "Postgres does not use a schema-history topic (DDL is "
                            + "replayed inline via the logical-replication "
                            + "publication), so legacy keys on Postgres are "
                            + "leftover-cruft rather than active misconfiguration."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
