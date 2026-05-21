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
 *   <li>{@code consumer.override.enable.auto.commit=true} (case-insensitive value).</li>
 * </ul>
 *
 * <p>Connect's {@code WorkerSinkTask} hardcodes {@code enable.auto.commit=false}
 * on the inbound consumer because Connect manages offsets through its own
 * {@code preCommit} / {@code offset.flush.interval.ms} lifecycle. Setting
 * {@code consumer.override.enable.auto.commit=true} (KIP-458) re-enables the
 * consumer's heartbeat-driven auto-commit, which advances offsets BEFORE the
 * sink-side {@code put()} confirms delivery. A task crash silently loses every
 * record between the last auto-commit and the crash.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectSinkAutoCommitTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String AUTO_COMMIT_OVERRIDE = "consumer.override.enable.auto.commit";

    private final Severity severity;

    public ConnectSinkAutoCommitTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SINK_AUTO_COMMIT_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String autoCommit = p.getProperty(AUTO_COMMIT_OVERRIDE);
            if (autoCommit == null || !"true".equalsIgnoreCase(autoCommit.trim())) continue;

            out.add(new Violation(
                    RuleId.CONNECT_SINK_AUTO_COMMIT_TRUE, severity,
                    ctx.relativize(e.getKey()), "key:" + AUTO_COMMIT_OVERRIDE, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " sets consumer.override.enable.auto.commit=true — this OVERRIDES the "
                            + "Connect runtime's hardcoded enable.auto.commit=false and re-enables "
                            + "heartbeat-driven auto-commit. Offsets advance BEFORE SinkTask.put() "
                            + "confirms the downstream write; a task crash mid-batch silently loses "
                            + "every record between the last auto-commit and the crash. Remove this "
                            + "override; Connect's offset.flush.interval.ms / preCommit hook is the "
                            + "only correct commit path."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
