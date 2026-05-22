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
 * Project-scoped rule. Fires when a Debezium Postgres connector
 * (`io.debezium.connector.postgresql.PostgresConnector`) is configured
 * with {@code tasks.max} greater than 1.
 *
 * <p>Debezium Postgres is fundamentally single-task by architectural
 * constraint: it attaches a SINGLE PostgreSQL logical-replication slot
 * to a SINGLE plugin subscription, producing a SINGLE strictly-ordered
 * WAL stream. The connector's {@code taskConfigs(int maxTasks)} returns
 * a one-element list regardless of {@code maxTasks}. Setting tasks.max
 * to anything > 1 is a silent no-op that confuses operators into
 * believing they have parallelism.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumPostgresTasksMaxGreaterThanOneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public ConnectDebeziumPostgresTasksMaxGreaterThanOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_TASKS_MAX_GREATER_THAN_ONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!POSTGRES_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String raw = trimOrNull(p.getProperty(TASKS_MAX));
            if (raw == null) continue;
            Integer parsed = parseIntOrNull(raw);
            if (parsed == null) continue;
            if (parsed <= 1) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_POSTGRES_TASKS_MAX_GREATER_THAN_ONE, severity,
                    ctx.relativize(e.getKey()), "key:" + TASKS_MAX, 0,
                    "Debezium Postgres connector has tasks.max=" + parsed + " — silently "
                            + "ignored. The Debezium Postgres connector is FUNDAMENTALLY "
                            + "single-task: it attaches a SINGLE PostgreSQL logical-replication "
                            + "slot to a SINGLE plugin subscription, producing a SINGLE "
                            + "strictly-ordered WAL stream that must be consumed sequentially "
                            + "to preserve transactional ordering. Logical-replication slots "
                            + "are EXCLUSIVE (only one consumer process may attach at a time), "
                            + "so the connector's taskConfigs(int maxTasks) HARDCODES "
                            + "List.of(taskProps) — the maxTasks parameter is received but "
                            + "ignored, and Connect schedules exactly 1 task regardless of "
                            + "tasks.max. The remaining " + (parsed - 1) + " 'task slot(s)' are "
                            + "never used. Operators who set tasks.max>1 to 'scale CDC "
                            + "throughput' get NO parallelism — the connector silently runs 1 "
                            + "task and the operator's capacity planning is based on a false "
                            + "premise. Fix: set tasks.max=1 (or omit, Connect's default is "
                            + "1). For true scaling of CDC throughput on Postgres sources, the "
                            + "options are (a) horizontal sharding of tables across multiple "
                            + "connectors (each with unique slot.name and disjoint "
                            + "table.include.list), (b) heap/CPU scaling of the single "
                            + "connector's worker thread, (c) Debezium 2.x incremental "
                            + "snapshot for parallel initial-snapshot phase. None involve "
                            + "raising tasks.max."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static Integer parseIntOrNull(String v) {
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
