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
 * Project-scoped rule. Fires when a Debezium DB2 connector
 * (`io.debezium.connector.db2.Db2Connector`) is configured with
 * {@code tasks.max} greater than 1.
 *
 * <p>Debezium DB2 is fundamentally single-task by architectural
 * constraint: it polls the DB2 ASN CD (Change Data) tables in COMMITSEQ
 * order, producing a single strictly-ordered stream of change-data rows
 * that must be processed sequentially. The connector's
 * {@code taskConfigs(int maxTasks)} returns a one-element list
 * regardless of {@code maxTasks}.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumDb2TasksMaxGreaterThanOneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DB2_CONNECTOR = "io.debezium.connector.db2.Db2Connector";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public ConnectDebeziumDb2TasksMaxGreaterThanOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_DB2_TASKS_MAX_GREATER_THAN_ONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!DB2_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String raw = trimOrNull(p.getProperty(TASKS_MAX));
            if (raw == null) continue;
            Integer parsed = parseIntOrNull(raw);
            if (parsed == null) continue;
            if (parsed <= 1) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_DB2_TASKS_MAX_GREATER_THAN_ONE, severity,
                    ctx.relativize(e.getKey()), "key:" + TASKS_MAX, 0,
                    "Debezium DB2 connector has tasks.max=" + parsed + " — silently "
                            + "ignored. The Debezium DB2 connector is FUNDAMENTALLY "
                            + "single-task: it polls the DB2 ASN CD (Change Data) tables "
                            + "in IBMSNAP_COMMITSEQ order, producing a SINGLE strictly-"
                            + "ordered stream of change-data rows that must be processed "
                            + "sequentially to preserve transactional ordering. A single "
                            + "DB2 transaction touching multiple tables produces multiple "
                            + "CD-table rows sharing ONE COMMITSEQ — emitting them out-of-"
                            + "order or as separate transactions corrupts downstream "
                            + "consumer state. Splitting work across tasks is "
                            + "architecturally impossible: every task would either poll "
                            + "the same CD tables (causing duplicate emission), or "
                            + "partition CD tables across tasks (losing cross-CD-table "
                            + "transactional grouping). The connector's taskConfigs(int "
                            + "maxTasks) HARDCODES List.of(taskProps) — the maxTasks "
                            + "parameter is received but ignored, and Connect schedules "
                            + "exactly 1 task regardless of tasks.max. The remaining "
                            + (parsed - 1) + " 'task slot(s)' are never used. Operators "
                            + "who set tasks.max>1 to 'scale CDC throughput' get NO "
                            + "parallelism — the connector silently runs 1 task and the "
                            + "operator's capacity planning is based on a false premise. "
                            + "Fix: set tasks.max=1 (or omit, Connect's default is 1). "
                            + "For true scaling of CDC throughput on DB2 sources, the "
                            + "options are (a) horizontal sharding of tables across "
                            + "multiple connectors (each polling a disjoint "
                            + "table.include.list), (b) DB2-side tuning of the ASN "
                            + "Capture program (ASNCAP priority, CD-table retention, "
                            + "CD-table indexes), (c) heap/CPU scaling of the single "
                            + "connector's worker thread. None involve raising tasks.max."));
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
