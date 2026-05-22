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
 * Project-scoped rule. Fires on a Kafka Connect connector .properties file
 * (identified by {@code connector.class}) that does NOT set
 * {@code tasks.max}. Connect defaults this to {@code 1}, meaning the
 * connector runs with EXACTLY ONE worker task regardless of input
 * partition count — silently under-provisioning parallelism for most
 * sink connectors and parallelizable source connectors.
 *
 * <p>INFO severity because some connectors (Debezium MySQL/Postgres/
 * Oracle/DB2) ARE single-task by architectural constraint and the
 * default-1 is correct. The plugin cannot distinguish without an
 * exhaustive class allowlist.
 */
public final class ConnectTasksMaxAbsentRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TASKS_MAX = "tasks.max";

    private final Severity severity;

    public ConnectTasksMaxAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TASKS_MAX_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(TASKS_MAX))) continue;
            out.add(new Violation(
                    RuleId.CONNECT_TASKS_MAX_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + TASKS_MAX, 0,
                    "Kafka Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " does NOT set `" + TASKS_MAX + "`. Connect "
                            + "defaults this to 1 — meaning the connector runs "
                            + "with EXACTLY ONE worker task regardless of input "
                            + "partition count. Sink connectors consuming a "
                            + "multi-partition topic will serialize ALL "
                            + "partitions through one task; parallelizable "
                            + "source connectors (JDBC source polling many "
                            + "tables, S3 source reading many prefixes) will "
                            + "run with one task instead of N. Common bug "
                            + "shape: team deploys a sink to a 24-partition "
                            + "topic expecting throughput to scale with "
                            + "partition count; one task consumes all 24 "
                            + "partitions serially, throughput is 24× under "
                            + "expectation. Fix: set `" + TASKS_MAX
                            + "=<intended-parallelism>` explicitly. "
                            + "Recommended values: 1 for single-task-by-design "
                            + "connectors (Debezium variants — explicit 1 "
                            + "documents the deliberate choice), "
                            + "min(<partition-count>, <worker-thread-budget>) "
                            + "for sink connectors, <table-count> or "
                            + "<prefix-count> for parallelizable source "
                            + "connectors."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
