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
 * file declares BOTH {@code topics=} and {@code topics.regex=} — Connect's
 * {@code SinkConnectorConfig} enforces exactly one of these keys and throws
 * {@code ConfigException} at startup with
 * "Must configure one of topics or topics.regex".
 *
 * <p>The two keys are MUTUALLY EXCLUSIVE by spec (KIP-185, Kafka 1.0+) — there
 * is no merge semantic, no precedence rule, and no warning when their values
 * overlap. A connector with both set never reaches its {@code start()}
 * lifecycle method; the task is quarantined; the worker emits a FAILED status.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectSinkTopicsAndTopicsRegexBothSetRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TOPICS = "topics";
    private static final String TOPICS_REGEX = "topics.regex";

    private final Severity severity;

    public ConnectSinkTopicsAndTopicsRegexBothSetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            if (!notBlank(p.getProperty(TOPICS))) continue;
            if (!notBlank(p.getProperty(TOPICS_REGEX))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET, severity,
                    ctx.relativize(e.getKey()), "key:" + TOPICS_REGEX, 0,
                    "Connect sink connector " + p.getProperty(CONNECTOR_CLASS)
                            + " declares BOTH topics=" + p.getProperty(TOPICS).trim()
                            + " AND topics.regex=" + p.getProperty(TOPICS_REGEX).trim()
                            + " — Connect's SinkConnectorConfig enforces exactly one and "
                            + "throws ConfigException at startup: 'Must configure one of topics "
                            + "or topics.regex'. The connector never reaches its start() "
                            + "lifecycle method; the task is quarantined; the worker reports "
                            + "FAILED status. Delete whichever line does NOT match your "
                            + "intent — the two keys are MUTUALLY EXCLUSIVE by spec (KIP-185), "
                            + "with no merge fallback and no precedence rule."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
