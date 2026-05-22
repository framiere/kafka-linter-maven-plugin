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
 * Project-scoped rule. Fires when a Debezium Oracle connector
 * (`io.debezium.connector.oracle.OracleConnector`) is configured with
 * {@code tasks.max} greater than 1.
 *
 * <p>Debezium Oracle is fundamentally single-task by architectural
 * constraint: both adapter modes (LogMiner and XStream) capture from a
 * single source-side mining session producing a single strictly-ordered
 * redo-log stream. The connector's {@code taskConfigs(int maxTasks)}
 * returns a one-element list regardless of {@code maxTasks}. Setting
 * tasks.max to anything > 1 is a silent no-op.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumOracleTasksMaxGreaterThanOneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ORACLE_CONNECTOR = "io.debezium.connector.oracle.OracleConnector";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public ConnectDebeziumOracleTasksMaxGreaterThanOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_ORACLE_TASKS_MAX_GREATER_THAN_ONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!ORACLE_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String raw = trimOrNull(p.getProperty(TASKS_MAX));
            if (raw == null) continue;
            Integer parsed = parseIntOrNull(raw);
            if (parsed == null) continue;
            if (parsed <= 1) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_ORACLE_TASKS_MAX_GREATER_THAN_ONE, severity,
                    ctx.relativize(e.getKey()), "key:" + TASKS_MAX, 0,
                    "Debezium Oracle connector has tasks.max=" + parsed + " — silently "
                            + "ignored. The Debezium Oracle connector is FUNDAMENTALLY "
                            + "single-task: BOTH adapter modes (LogMiner and XStream) "
                            + "capture from a SINGLE source-side mining session producing "
                            + "a SINGLE strictly-ordered stream of redo-log change events "
                            + "that must be consumed sequentially to preserve transactional "
                            + "ordering. For LogMiner, splitting the work is architecturally "
                            + "impossible: every task would need its own LogMiner session "
                            + "(sessions are per-connection and V$LOGMNR_CONTENTS is "
                            + "per-session), each session would mine the SAME redo logs "
                            + "producing the SAME events (causing duplicate emission to "
                            + "Kafka), AND each session would independently hold a "
                            + "redo-log-retention latch on the Oracle server, MULTIPLYING "
                            + "the source-database load by N. For XStream, the outbound "
                            + "server is a server-side singleton — the client connects "
                            + "ONCE per connector. The connector's taskConfigs(int "
                            + "maxTasks) HARDCODES List.of(taskProps) — the maxTasks "
                            + "parameter is received but ignored, and Connect schedules "
                            + "exactly 1 task regardless of tasks.max. The remaining "
                            + (parsed - 1) + " 'task slot(s)' are never used. Operators "
                            + "who set tasks.max>1 to 'scale CDC throughput' get NO "
                            + "parallelism — the connector silently runs 1 task and the "
                            + "operator's capacity planning is based on a false premise. "
                            + "Fix: set tasks.max=1 (or omit, Connect's default is 1). "
                            + "For true scaling of CDC throughput on Oracle sources, the "
                            + "options are (a) horizontal sharding of tables across "
                            + "multiple connectors (each capturing a disjoint "
                            + "table.include.list), (b) Oracle-side tuning of LogMiner "
                            + "batch parameters (log.mining.batch.size.max, "
                            + "log.mining.view.fetch.size), (c) heap/CPU scaling of the "
                            + "single connector's worker thread, (d) one-connector-per-PDB "
                            + "sharding for multitenant container databases. None involve "
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
