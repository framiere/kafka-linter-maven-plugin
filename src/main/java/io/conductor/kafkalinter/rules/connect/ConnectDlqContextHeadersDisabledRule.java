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
 * Project-scoped rule. Fires when a Kafka Connect sink connector .properties
 * file configures {@code errors.deadletterqueue.topic.name=} but does NOT
 * also enable {@code errors.deadletterqueue.context.headers.enable=true}.
 *
 * <p>Without context headers, DLQ records carry only the original record's
 * key+value bytes — no source topic, partition, offset, exception class,
 * exception message, or stack trace. The DLQ becomes undebuggable.
 *
 * <p>Apache Kafka's default for the context-headers knob is {@code false}
 * (backwards-compatibility with pre-KIP-298 tooling), but every operational
 * DLQ should set it to {@code true}.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDlqContextHeadersDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DLQ_TOPIC = "errors.deadletterqueue.topic.name";
    private static final String CONTEXT_HEADERS_ENABLE = "errors.deadletterqueue.context.headers.enable";

    private final Severity severity;

    public ConnectDlqContextHeadersDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DLQ_CONTEXT_HEADERS_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            if (!notBlank(p.getProperty(DLQ_TOPIC))) continue;

            String headersValue = p.getProperty(CONTEXT_HEADERS_ENABLE);
            if (isTrue(headersValue)) continue;

            String state = (headersValue == null || headersValue.trim().isEmpty())
                    ? "not set (defaults to false)"
                    : "set to '" + headersValue.trim() + "'";

            out.add(new Violation(
                    RuleId.CONNECT_DLQ_CONTEXT_HEADERS_DISABLED, severity,
                    ctx.relativize(e.getKey()), "key:" + DLQ_TOPIC, 0,
                    "Connect sink connector " + p.getProperty(CONNECTOR_CLASS)
                            + " configures " + DLQ_TOPIC + "="
                            + p.getProperty(DLQ_TOPIC).trim()
                            + " but " + CONTEXT_HEADERS_ENABLE + " is " + state
                            + " — DLQ records will arrive with ONLY the original record bytes; "
                            + "no source topic, no partition, no offset, no exception class, no "
                            + "exception message, no stack trace, no connector name, no task ID. "
                            + "The DLQ becomes undebuggable: operators cannot triage failures, "
                            + "re-process records, or correlate DLQ entries to source records. "
                            + "Set " + CONTEXT_HEADERS_ENABLE + "=true (the default is false for "
                            + "backwards compatibility with pre-KIP-298 tooling, but ANY "
                            + "operational DLQ should enable headers)."));
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
