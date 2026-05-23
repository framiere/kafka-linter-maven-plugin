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
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>{@code errors.tolerance=all} (case-insensitive), AND</li>
 *   <li>{@code errors.log.enable} either ABSENT or set to a non-truthy value
 *       (case-insensitive {@code true} is the only accepted truthy value).</li>
 * </ul>
 *
 * <p>With {@code errors.tolerance=all}, the Connect runtime swallows per-record
 * processing exceptions and continues. Without {@code errors.log.enable=true},
 * the framework writes NO log line for those swallowed exceptions — the only
 * signals are the JMX {@code errors.tolerated} counter ticking up and (if a DLQ
 * is configured) the DLQ topic gradually filling. For operators alerting on
 * connector log patterns, this is invisible silent failure.
 *
 * <p>Apache Kafka's default for {@code errors.log.enable} is {@code false}
 * (backward compatibility with pre-KIP-298 tooling), but every Connect
 * deployment using {@code errors.tolerance=all} should set it to {@code true}
 * to preserve the log-channel observability KIP-298 was designed around.
 *
 * <p>Independent of [[connect-errors-tolerance-all-no-dlq]] which catches the
 * complementary DLQ-channel gap. Both rules can fire on the same .properties.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectErrorsToleranceAllNoLogRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ERRORS_TOLERANCE = "errors.tolerance";
    private static final String ERRORS_LOG_ENABLE = "errors.log.enable";

    private final Severity severity;

    public ConnectErrorsToleranceAllNoLogRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String tolerance = p.getProperty(ERRORS_TOLERANCE);
            if (tolerance == null || !"all".equalsIgnoreCase(tolerance.trim())) continue;

            String logEnable = p.getProperty(ERRORS_LOG_ENABLE);
            if (isTrue(logEnable)) continue;

            String state = (logEnable == null || logEnable.trim().isEmpty())
                    ? "not set (defaults to false)"
                    : "set to '" + logEnable.trim() + "'";

            out.add(new Violation(
                    RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG, severity,
                    ctx.relativize(e.getKey()), "key:" + ERRORS_LOG_ENABLE, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " sets " + ERRORS_TOLERANCE + "=all but "
                            + ERRORS_LOG_ENABLE + " is " + state
                            + " — the Connect runtime SILENTLY swallows per-record "
                            + "processing exceptions (converter, SMT, sink-task, "
                            + "source-producer failures) with NO log line written "
                            + "to the connector's own log stream. The ONLY signals "
                            + "of failure are the JMX `errors.tolerated` counter "
                            + "ticking up and (if a DLQ is configured) the DLQ "
                            + "topic gradually filling. Operators tailing connector "
                            + "logs (`kubectl logs`, centralized log aggregators, "
                            + "alert rules keyed on `level=ERROR`) see NOTHING. "
                            + "The connector LOOKS healthy when it is in fact "
                            + "silently failing record after record. Real-world "
                            + "impact: (1) **JDBC sink with malformed upstream "
                            + "records** — DLQ fills at thousands/sec while logs "
                            + "stay silent; on-call alert (keyed on connector "
                            + "log ERROR lines) never fires; bug only discovered "
                            + "6 hours later when downstream-warehouse daily-load "
                            + "alert catches the missing rows. (2) **S3 sink with "
                            + "intermittent S3 5xx** — slow trickle of failures "
                            + "into DLQ, invisible to log-based monitoring, "
                            + "discovered downstream when consumer notices missing "
                            + "records. (3) **Debezium CDC schema-drift** — events "
                            + "fail silently; CDC topic gaps go unnoticed; "
                            + "materialized views drift from source. (4) **MM2 "
                            + "replication** — silently drops events on per-record "
                            + "errors; cluster divergence accumulates undetected. "
                            + "(5) **Connect-on-K8s operator templates** ship "
                            + "`errors.tolerance=all` without the log knob — "
                            + "every connector deployed via the template has "
                            + "structural log-blindness. KIP-298 (Apache Kafka "
                            + "2.0, July 2018) explicitly designed `errors.tolerance` "
                            + "to work with TWO complementary observability channels: "
                            + "(a) the DLQ topic — record-level, durable, "
                            + "replayable; and (b) the in-process log — "
                            + "connector-level, real-time, grep-able from the "
                            + "connector's own log stream. Setting "
                            + "`errors.tolerance=all` without the log channel "
                            + "breaks half of the KIP-298 contract. Fix: add `"
                            + ERRORS_LOG_ENABLE + "=true` and "
                            + "`errors.log.include.messages=true` (the latter "
                            + "optional when records contain PII the team "
                            + "doesn't want in log aggregators — exception "
                            + "class+message+stacktrace+source-coordinates "
                            + "still log without record bytes). The default "
                            + "`false` exists for backward compatibility with "
                            + "pre-KIP-298 tooling, NOT as a recommended "
                            + "production posture. Sibling rule "
                            + "[[connect-errors-tolerance-all-no-dlq]] catches "
                            + "the complementary DLQ-channel gap; both rules "
                            + "can fire on the same .properties when the "
                            + "operator set ONLY `errors.tolerance=all` and "
                            + "neither observability channel."));
        }
        return out;
    }

    private static boolean isTrue(String v) {
        return v != null && "true".equalsIgnoreCase(v.trim());
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
