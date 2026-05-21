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
 *   <li>NO {@code errors.deadletterqueue.topic.name} (or the value is blank).</li>
 * </ul>
 *
 * <p>This is KIP-298's canonical anti-pattern: {@code errors.tolerance=all}
 * tells the Connect runtime to swallow per-record processing exceptions, and
 * {@code errors.deadletterqueue.topic.name} is the ONLY built-in mechanism to
 * preserve the failing record bytes for later replay or forensic analysis.
 * Setting the first without the second silently drops every failing record.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectErrorsToleranceAllNoDlqRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ERRORS_TOLERANCE = "errors.tolerance";
    private static final String DLQ_TOPIC = "errors.deadletterqueue.topic.name";

    private final Severity severity;

    public ConnectErrorsToleranceAllNoDlqRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String tolerance = p.getProperty(ERRORS_TOLERANCE);
            if (tolerance == null || !"all".equalsIgnoreCase(tolerance.trim())) continue;
            if (notBlank(p.getProperty(DLQ_TOPIC))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ, severity,
                    ctx.relativize(e.getKey()), "key:" + ERRORS_TOLERANCE, 0,
                    "Connect connector " + e.getValue().getProperty(CONNECTOR_CLASS)
                            + " sets errors.tolerance=all but has no errors.deadletterqueue.topic.name — "
                            + "every record that fails conversion, transformation, or write is SILENTLY DROPPED. "
                            + "Add errors.deadletterqueue.topic.name=<dlq-topic>, "
                            + "errors.deadletterqueue.topic.replication.factor=3, "
                            + "errors.deadletterqueue.context.headers.enable=true, "
                            + "errors.log.enable=true, errors.log.include.messages=true."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
