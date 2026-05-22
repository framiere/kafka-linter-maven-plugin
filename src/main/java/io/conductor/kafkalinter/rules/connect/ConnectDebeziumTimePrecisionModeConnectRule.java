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
 * Project-scoped rule. Fires when a Debezium connector (any class under
 * {@code io.debezium.connector.}) sets {@code time.precision.mode=connect}.
 *
 * <p>The {@code connect} mode downgrades every temporal column to Kafka
 * Connect's built-in {@code Timestamp}/{@code Time}/{@code Date} logical
 * types — millisecond precision only — silently truncating any
 * sub-millisecond precision present in the source. Postgres TIMESTAMP(6),
 * MySQL 5.7+ TIMESTAMP(6), Oracle TIMESTAMP(9), SQL Server datetime2(7)
 * all carry sub-millisecond precision that this mode discards. For
 * event-sourcing, distributed tracing, audit, and HFT systems that rely
 * on monotonic strict-order microsecond timestamps, this breaks ordering
 * invariants. The default {@code adaptive*} modes preserve source
 * precision via the {@code io.debezium.time.*} semantic types.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumTimePrecisionModeConnectRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String TIME_PRECISION_MODE = "time.precision.mode";
    private static final String CONNECT_MODE = "connect";

    private final Severity severity;

    public ConnectDebeziumTimePrecisionModeConnectRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_TIME_PRECISION_MODE_CONNECT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;
            String mode = trimOrNull(p.getProperty(TIME_PRECISION_MODE));
            if (mode == null) continue;
            if (!CONNECT_MODE.equalsIgnoreCase(mode)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_TIME_PRECISION_MODE_CONNECT, severity,
                    ctx.relativize(e.getKey()), "key:" + TIME_PRECISION_MODE, 0,
                    "Debezium connector " + connectorClass
                            + " sets time.precision.mode=connect — every TIMESTAMP/TIME/"
                            + "DATE/TIMESTAMPTZ value is silently truncated to millisecond "
                            + "precision at the connector, BEFORE the record reaches Kafka. "
                            + "Postgres microseconds, SQL Server 100ns, Oracle nanoseconds, "
                            + "MySQL 5.7+ microseconds — all lost. Two events that occurred "
                            + "50 microseconds apart on the source arrive with IDENTICAL "
                            + "millisecond timestamps in Kafka, breaking strict-order replay, "
                            + "distributed-tracing causal ordering, event-sourcing "
                            + "determinism, and audit-trail uniqueness. The truncation is "
                            + "silent — no error, no schema indication that precision was "
                            + "lost — until a downstream system that depends on "
                            + "sub-millisecond ordering finds collisions. Set "
                            + "time.precision.mode=adaptive (default — preserves source "
                            + "precision via io.debezium.time.MicroTimestamp / NanoTimestamp / "
                            + "MicroTime / NanoTime semantic types) or "
                            + "time.precision.mode=adaptive_time_microseconds (MySQL-specific "
                            + "micro-precision for TIME columns)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
