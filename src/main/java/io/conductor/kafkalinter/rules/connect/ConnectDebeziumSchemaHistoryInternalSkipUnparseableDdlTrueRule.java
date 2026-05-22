package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * sets {@code schema.history.internal.skip.unparseable.ddl=true}.
 *
 * <p>The {@code true} value tells the connector's SchemaHistory layer
 * to silently skip DDL statements that the DDL parser cannot parse.
 * The schema cache then diverges from the source database's actual
 * schema; subsequent row events for the affected table fail to decode
 * and cascade to the {@code event.processing.failure.handling.mode}
 * setting — either crashing the connector later or silently dropping
 * every row event forever.
 *
 * <p>The legacy key {@code database.history.skip.unparseable.ddl}
 * is intentionally NOT checked here — it is caught by
 * {@link ConnectDebeziumLegacySchemaHistoryKeysRule}, which flags
 * the legacy key prefix regardless of value.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumSchemaHistoryInternalSkipUnparseableDdlTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CONNECTOR_CLASS_PREFIX = "io.debezium.connector.";
    private static final String SKIP_UNPARSEABLE_DDL_KEY = "schema.history.internal.skip.unparseable.ddl";

    private final Severity severity;

    public ConnectDebeziumSchemaHistoryInternalSkipUnparseableDdlTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_CONNECTOR_CLASS_PREFIX)) continue;

            String value = trimOrNull(p.getProperty(SKIP_UNPARSEABLE_DDL_KEY));
            if (value == null) continue;
            if (!"true".equals(value.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL_TRUE, severity,
                    ctx.relativize(e.getKey()), "key:" + SKIP_UNPARSEABLE_DDL_KEY, 0,
                    "Debezium connector (" + connectorClass + ") has schema.history.internal.skip."
                            + "unparseable.ddl=true. This is a silent-schema-drift switch: when the "
                            + "connector's DDL parser fails to parse a source-database DDL (vendor-"
                            + "syntax extension, a new DDL feature not yet supported by the parser, a "
                            + "parser bug), the `true` value tells the SchemaHistory layer to LOG-AND-"
                            + "SKIP-AND-CONTINUE. The DDL's schema change is NOT applied to the "
                            + "connector's in-memory schema cache; the cache silently diverges from the "
                            + "source database's actual schema. The NEXT time a row event arrives for "
                            + "the affected table, the connector tries to deserialize the row using its "
                            + "stale schema cache; the column count or column types DO NOT MATCH; "
                            + "deserialization FAILS. The failure then cascades to event.processing."
                            + "failure.handling.mode: with the default `fail`, the connector starts "
                            + "failing on every subsequent row event for the affected table (operator "
                            + "is paged days later, but the schema-history topic has already stored the "
                            + "unparseable DDL, and recovery requires manually editing the topic); with "
                            + "`skip`/`warn`, EVERY ROW EVENT for the affected table is silently dropped "
                            + "forever (the table's CDC stream goes dead with no signal). The default "
                            + "`false` is correct for production: stop the connector, page the operator, "
                            + "give them the chance to upgrade Debezium (most DDL-parser issues are "
                            + "fixed in later releases), rewrite the DDL in parser-compatible syntax, or "
                            + "manually advance the schema history. Fix: remove this line (default "
                            + "`false` is correct), or set schema.history.internal.skip.unparseable.ddl="
                            + "false explicitly. If parser-stumbles are happening repeatedly, address "
                            + "the root cause: upgrade Debezium, work with the DBA to use compatible "
                            + "DDL syntax, or use schema.history.internal.store.only.captured.tables.ddl"
                            + "=true to filter the schema-history to only the captured tables (so DDLs "
                            + "on unrelated tables don't affect the connector)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
