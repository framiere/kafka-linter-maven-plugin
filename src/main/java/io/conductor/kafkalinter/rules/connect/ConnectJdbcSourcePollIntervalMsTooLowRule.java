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
 * Project-scoped rule. Fires when a Confluent JDBC source connector
 * ({@code io.confluent.connect.jdbc.JdbcSourceConnector}) declares
 * {@code poll.interval.ms} set to a positive value less than 1000 ms
 * (1 second).
 *
 * <p>Sub-second polling means more than one SELECT per second per
 * task against the source DB — saturating the connection pool,
 * generating excess WAL/redo activity, and competing for row locks
 * with application traffic. The Confluent default is 5000 ms.
 *
 * <p>The rule does NOT fire on absent / zero / negative values.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSourcePollIntervalMsTooLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS_KEY = "connector.class";
    private static final String JDBC_SOURCE_CONNECTOR_CLASS =
            "io.confluent.connect.jdbc.JdbcSourceConnector";
    private static final String POLL_INTERVAL_MS_KEY = "poll.interval.ms";
    private static final long THRESHOLD_MS = 1000L;

    private final Severity severity;

    public ConnectJdbcSourcePollIntervalMsTooLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SOURCE_POLL_INTERVAL_MS_TOO_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS_KEY));
            if (!JDBC_SOURCE_CONNECTOR_CLASS.equals(connectorClass)) continue;

            Long pollIntervalMs = tryParseLong(trimOrNull(p.getProperty(POLL_INTERVAL_MS_KEY)));
            if (pollIntervalMs == null || pollIntervalMs <= 0L || pollIntervalMs >= THRESHOLD_MS) continue;

            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SOURCE_POLL_INTERVAL_MS_TOO_LOW, severity,
                    ctx.relativize(e.getKey()), "key:" + POLL_INTERVAL_MS_KEY, 0,
                    "Confluent JDBC source connector declares poll.interval.ms="
                            + pollIntervalMs + " ms — BELOW the plugin's threshold of "
                            + THRESHOLD_MS + " ms (1 second). poll.interval.ms is the "
                            + "per-task sleep between successive polls of the source "
                            + "database; sub-second values mean MORE THAN ONE SQL "
                            + "SELECT PER SECOND PER TASK. At realistic tasks.max=4-8, "
                            + "the connector issues 4-80 SELECTs/sec sustained against "
                            + "the source DB — saturating connection pools, generating "
                            + "excess WAL/redo-log activity, competing for shared row "
                            + "locks with OLTP traffic, and preventing PostgreSQL "
                            + "vacuum from advancing (MVCC snapshot stays open). The "
                            + "Confluent default is 5000 ms. Typical wrong-value "
                            + "origins: (a) 'I want real-time data' misconception — "
                            + "operator does not realize the freshness floor is the "
                            + "application's commit cadence, not the poll interval; "
                            + "(b) debug-mode setting (100ms or 50ms) never reverted; "
                            + "(c) tutorial copy-paste at scale (tutorial used 100ms "
                            + "on a tiny single-task connector); (d) sub-second "
                            + "freshness requirement that should instead be addressed "
                            + "with Debezium (CDC against the DB's WAL/binlog). Fix: "
                            + "(1) raise poll.interval.ms to 5000-30000 ms (the "
                            + "Confluent default's range); (2) for genuine sub-second "
                            + "freshness, switch to Debezium — which delivers sub-"
                            + "second propagation latency WITHOUT polling; (3) if "
                            + "sub-second polling is genuinely required, suppress "
                            + "this rule with OFF in the .properties documenting the "
                            + "DBA's signoff, connection-pool sizing, and WAL/redo-"
                            + "log retention impact."));
        }
        return out;
    }

    private static Long tryParseLong(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
